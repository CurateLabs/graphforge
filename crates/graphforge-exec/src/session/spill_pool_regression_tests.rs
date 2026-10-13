//! #1985: a hash exchange that spills under a tight memory pool finishes.
//!
//! DataFusion 54.1.0's `RepartitionExec` could deadlock when its spill pool held
//! two open files: the reader waited on a drained file whose writer had moved on,
//! while the batch it was owed sat in a newer file, and nothing woke it
//! (apache/datafusion#24883). The workspace resolves `datafusion-physical-plan`
//! from `vendor/datafusion-physical-plan`, which carries the upstream fix
//! (apache/datafusion#24891). This runs the upstream reproduction through the
//! session's own query memory pool and fails, instead of hanging, if the
//! exchange stops delivering.

use super::*;
use graphforge_ir::RuntimeCatalog;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

/// Memory pool budget of the reproduction.
const POOL_BYTES: u64 = 2 * 1024 * 1024;
/// Rows of the reproduction table.
const ROWS: i64 = 40_000;
/// Distinct `g` values.
const GROUPS: usize = 13;
/// Distinct `trace_id` values in every group: `v % 13` and `v % 337` are
/// independent because 13 and 337 are coprime, and 40 000 rows cover every pair
/// that 13 * 337 = 4 381 admits.
const TRACES_PER_GROUP: i64 = 337;
/// Times the aggregate runs. The deadlock needs a particular interleaving of the
/// exchange's writers and its reader, so one run proves little.
const RUNS: usize = 20;
/// A run takes well under a second; this bounds a hang, it is not a tuning knob.
const RUN_DEADLINE: Duration = Duration::from_secs(60);

fn session(spill: &TempDir) -> (ExecutionSession, TempDir) {
    let dir = TempDir::new().unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let resources = SessionResourceConfig {
        target_partitions: 2,
        batch_size: 64,
        memory_budget_bytes: POOL_BYTES,
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
    (session, dir)
}

/// Spill operations performed by every `RepartitionExec` of `plan`.
fn exchange_spills(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let own = if plan.name() == "RepartitionExec" {
        plan.metrics()
            .and_then(|metrics| metrics.spill_count())
            .unwrap_or(0)
    } else {
        0
    };
    own + plan
        .children()
        .into_iter()
        .map(exchange_spills)
        .sum::<usize>()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hash_exchange_spilling_under_a_tight_pool_delivers_every_group() {
    let spill = TempDir::new().unwrap();
    let (session, _dir) = session(&spill);
    session
        .ctx
        .sql(&format!(
            "CREATE TABLE t AS SELECT v % {GROUPS} AS g, \
             md5(CAST(v % {TRACES_PER_GROUP} AS VARCHAR)) AS trace_id \
             FROM generate_series(1, {ROWS}) AS s(v)"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .expect("the table must load within the pool");

    let mut exchange_spills_seen = 0;
    for run in 0..RUNS {
        let plan = session
            .ctx
            .sql("SELECT g, count(DISTINCT trace_id) AS traces FROM t GROUP BY g")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let batches = tokio::time::timeout(
            RUN_DEADLINE,
            datafusion::physical_plan::collect(Arc::clone(&plan), session.ctx.task_ctx()),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("run {run}: the aggregate did not finish within {RUN_DEADLINE:?}: a spilling exchange deadlocked")
        })
        .expect("a spilling exchange must deliver, not exhaust the pool");

        let mut rows = batches
            .iter()
            .flat_map(|batch| {
                let groups = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("g is an Int64");
                let traces = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("count is an Int64");
                (0..batch.num_rows())
                    .map(|row| (groups.value(row), traces.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort_unstable();
        let expected = (0..GROUPS as i64)
            .map(|group| (group, TRACES_PER_GROUP))
            .collect::<Vec<_>>();
        assert_eq!(rows, expected, "run {run}: every group counts every trace");
        exchange_spills_seen += exchange_spills(&plan);
    }
    assert!(
        exchange_spills_seen > 0,
        "the pool must be tight enough that an exchange spills, or this proves nothing"
    );
}
