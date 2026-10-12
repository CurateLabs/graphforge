//! #1985: a query whose join or aggregate state exceeds the query memory
//! budget completes with the result it has under a large budget, instead of
//! exhausting the pool. A pool that grants no reservation past its budget makes
//! "completes" the proof that the peak stayed within it.

use super::*;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::displayable;
use graphforge_ir::RuntimeCatalog;
use std::path::PathBuf;
use tempfile::TempDir;

const MIB: u64 = 1024 * 1024;
/// Rows per table: 800k keys with a 100-byte payload is about 86 MiB a table.
const ROWS: i64 = 800_000;

fn payload_table(keys: &[i64], batch_rows: usize) -> MemTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]));
    let batches = keys
        .chunks(batch_rows)
        .map(|chunk| {
            let pad = StringArray::from_iter_values(chunk.iter().map(|k| format!("{k:0>100}")));
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(chunk.to_vec())), Arc::new(pad)],
            )
            .expect("payload batch")
        })
        .collect::<Vec<_>>();
    MemTable::try_new(schema, vec![batches]).expect("payload table")
}

/// The keys `0..ROWS`, in order.
fn sequential_keys() -> Vec<i64> {
    (0..ROWS).collect()
}

/// `ROWS` keys of which `hot_percent` percent hash into partition 0 of
/// `partitions` hash partitions on DataFusion's repartitioning hash, the rest
/// into the other partitions, so that one partition of a grouped aggregate
/// receives that share of the groups.
fn skewed_keys(partitions: usize, hot_percent: i64) -> Vec<i64> {
    use datafusion::common::hash_utils::create_hashes;
    use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;

    let hot = ROWS * hot_percent / 100;
    let (mut hot_keys, mut cold_keys) = (Vec::new(), Vec::new());
    let mut next = 0_i64;
    while (hot_keys.len() as i64) < hot || (cold_keys.len() as i64) < ROWS - hot {
        let candidates: arrow::array::ArrayRef =
            Arc::new(Int64Array::from_iter_values(next..next + 65_536));
        let mut hashes = vec![0_u64; candidates.len()];
        create_hashes(
            [&candidates],
            &REPARTITION_RANDOM_STATE.random_state(),
            &mut hashes,
        )
        .expect("hash candidate keys");
        for (key, hash) in (next..next + 65_536).zip(hashes) {
            let into_hot = hash % partitions as u64 == 0;
            if into_hot && (hot_keys.len() as i64) < hot {
                hot_keys.push(key);
            } else if !into_hot && (cold_keys.len() as i64) < ROWS - hot {
                cold_keys.push(key);
            }
        }
        next += 65_536;
    }
    hot_keys.extend(cold_keys);
    hot_keys.sort_unstable();
    hot_keys
}

fn session(budget: u64, partitions: usize, spill: &TempDir) -> (ExecutionSession, TempDir) {
    session_over(budget, partitions, spill, &sequential_keys())
}

/// A session whose tables `a` and `b` both hold `keys`.
fn session_over(
    budget: u64,
    partitions: usize,
    spill: &TempDir,
    keys: &[i64],
) -> (ExecutionSession, TempDir) {
    let dir = TempDir::new().unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let resources = SessionResourceConfig {
        target_partitions: partitions,
        memory_budget_bytes: budget,
        spill_enabled: true,
        spill_directory: Some(spill.path().to_path_buf()),
        ..SessionResourceConfig::default()
    };
    let session = ExecutionSession::build(
        catalog,
        None,
        PathBuf::new(),
        OntologyMode::Exploratory,
        None,
        OrdinalIdentityConfig::default(),
        &resources,
    )
    .unwrap();
    for name in ["a", "b"] {
        session
            .ctx
            .register_table(name, Arc::new(payload_table(keys, resources.batch_size)))
            .unwrap();
    }
    (session, dir)
}

/// Run `sql`; its rendered rows and the physical plan with its metrics.
async fn run(
    session: &ExecutionSession,
    sql: &str,
) -> Result<(Vec<String>, Arc<dyn ExecutionPlan>), DataFusionError> {
    let plan = session.ctx.sql(sql).await?.create_physical_plan().await?;
    let batches =
        datafusion::physical_plan::collect(Arc::clone(&plan), session.ctx.task_ctx()).await?;
    let rows = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                (0..batch.num_columns())
                    .map(|col| {
                        arrow::util::display::array_value_to_string(batch.column(col), row).unwrap()
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
        })
        .collect();
    Ok((rows, plan))
}

fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(false).to_string()
}

/// Spill operations performed by every operator of `plan` named `operator`.
fn spills(plan: &Arc<dyn ExecutionPlan>, operator: &str) -> usize {
    let own = if plan.name() == operator {
        plan.metrics()
            .and_then(|metrics| metrics.spill_count())
            .unwrap_or(0)
    } else {
        0
    };
    own + plan
        .children()
        .into_iter()
        .map(|child| spills(child, operator))
        .sum::<usize>()
}

const JOIN: &str = "SELECT count(*), sum(a.k), sum(length(a.pad)), sum(length(b.pad)) \
                    FROM a JOIN b ON a.k = b.k";

/// Both sides are about 86 MiB, so a hash join's build side cannot fit a
/// 48 MiB pool beside the probe side and its hash table. It must be planned as a spilling sort-merge join and return
/// what the hash join returns under a pool that holds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_whose_build_side_exceeds_the_budget_spills_and_matches_the_large_budget() {
    let spill = TempDir::new().unwrap();
    let (small, _small_dir) = session(48 * MIB, 1, &spill);
    let (small_rows, small_plan) = run(&small, JOIN)
        .await
        .expect("a join over the budget must spill, not exhaust the pool");
    let small_text = plan_text(&small_plan);
    assert!(
        small_text.contains("SortMergeJoinExec") && !small_text.contains("HashJoinExec"),
        "{small_text}"
    );
    assert!(
        spills(&small_plan, "SortExec") > 0,
        "the sorts feeding the join exceed the budget, so they must have spilled:\n{small_text}"
    );

    let (large, _large_dir) = session(2048 * MIB, 1, &spill);
    let (large_rows, large_plan) = run(&large, JOIN).await.unwrap();
    let large_text = plan_text(&large_plan);
    assert!(
        large_text.contains("HashJoinExec") && !large_text.contains("SortMergeJoinExec"),
        "a build side that fits keeps the hash join:\n{large_text}"
    );
    assert_eq!(small_rows, large_rows);
    assert_eq!(
        small_rows,
        [format!(
            "{ROWS},{},{},{}",
            ROWS * (ROWS - 1) / 2,
            ROWS * 100,
            ROWS * 100
        )]
    );
}

const AGGREGATE: &str = "SELECT count(*), sum(k), sum(length(m)) \
                         FROM (SELECT k, max(pad) AS m FROM a GROUP BY k)";

/// The aggregate over `keys` in a `budget` pool shared by `partitions`
/// partitions: it must spill and return what it returns under a large pool.
async fn aggregate_spills_and_matches_the_large_budget(
    budget: u64,
    partitions: usize,
    keys: &[i64],
) {
    let spill = TempDir::new().unwrap();
    let (small, _small_dir) = session_over(budget, partitions, &spill, keys);
    let (small_rows, small_plan) = run(&small, AGGREGATE)
        .await
        .expect("an aggregate over the budget must spill, not exhaust the pool");
    assert!(
        spills(&small_plan, "AggregateExec") > 0,
        "the groups exceed the budget, so the aggregate must have spilled:\n{}",
        plan_text(&small_plan)
    );
    let (large, _large_dir) = session_over(2048 * MIB, partitions, &spill, keys);
    let (large_rows, large_plan) = run(&large, AGGREGATE).await.unwrap();
    assert_eq!(spills(&large_plan, "AggregateExec"), 0);
    assert_eq!(small_rows, large_rows);
    assert_eq!(
        small_rows,
        [format!(
            "{ROWS},{},{}",
            keys.iter().sum::<i64>(),
            ROWS * 100
        )]
    );
}

/// 800k groups of a 100-byte maximum are about 100 MiB of group state, past a
/// 64 MiB pool shared by the two partitions it is sized for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aggregate_whose_groups_exceed_the_budget_spills_and_matches_the_large_budget() {
    aggregate_spills_and_matches_the_large_budget(64 * MIB, 2, &sequential_keys()).await;
}

/// Nine tenths of the groups hash into one of the two partitions, so that
/// partition's state alone exceeds the pool while the other holds the rest.
/// Each partition must still be able to reserve the memory it sorts its state
/// in to spill it, and to restart afterwards, whichever way they interleave.
/// Before the pool kept a spilling aggregate's released memory for that sort
/// and restart, a partition that ran into the budget while the other held
/// memory failed with `Failed to reserve memory for sort during spill` or
/// `Additional allocation failed for GroupedHashAggregateStream` (merge group
/// run 38245644578).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aggregate_with_one_dominant_partition_spills_and_matches_the_large_budget() {
    aggregate_spills_and_matches_the_large_budget(64 * MIB, 2, &skewed_keys(2, 90)).await;
}
