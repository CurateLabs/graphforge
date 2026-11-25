//! A query whose sort spills keeps to its query memory budget, returns the rows
//! it returns under a large budget, and leaves no spill file behind, whether it
//! completes, is dropped part way, or is refused.
//!
//! Stock DataFusion owns its spill writers, readers and merges, so the order
//! and size of their reservations are not asserted here (#2357). The outcomes
//! are those of the query: completion, exact rows, the pool's peak, typed
//! refusal and cleanup.

use super::*;
use arrow::array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, FixedSizeBinaryArray, Float64Array, Int8Array,
    Int64Array, ListArray, StringArray, StringViewArray, StructArray, TimestampMillisecondArray,
};
use arrow::compute::{concat_batches, sort_to_indices, take_record_batch};
use arrow::datatypes::{DataType, Field, Fields, Int8Type, Int64Type, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use futures::StreamExt;
use graphforge_ir::RuntimeCatalog;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use tempfile::TempDir;

const MIB: u64 = 1024 * 1024;
/// A budget no query here comes near, for the reference answer.
const LARGE_BUDGET: u64 = 1024 * MIB;

/// A one-partition session over `table` as `t`, spilling into `spill` under
/// `spill_max_bytes`.
fn session_over(
    table: &[RecordBatch],
    budget: u64,
    spill: &TempDir,
    spill_max_bytes: Option<u64>,
) -> (ExecutionSession, TempDir) {
    let dir = TempDir::new().unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let resources = SessionResourceConfig {
        target_partitions: 1,
        memory_budget_bytes: budget,
        spill_enabled: true,
        spill_directory: Some(spill.path().to_path_buf()),
        spill_max_bytes,
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
    let mem = MemTable::try_new(table[0].schema(), vec![table.to_vec()]).unwrap();
    session.ctx.register_table("t", Arc::new(mem)).unwrap();
    (session, dir)
}

/// The files under `dir`, at any depth.
fn files_under(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                files_under(&entry.path())
            } else {
                1
            }
        })
        .sum()
}

/// The spill metrics of every operator in `plan`: files written and rows
/// written.
fn spills(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
    let metrics = plan.metrics().unwrap_or_default();
    let own = (
        metrics.spill_count().unwrap_or(0),
        metrics.spilled_rows().unwrap_or(0),
    );
    plan.children()
        .into_iter()
        .map(spills)
        .fold(own, |(files, rows), (more_files, more_rows)| {
            (files + more_files, rows + more_rows)
        })
}

/// What a query did under one budget.
struct Run {
    /// Its rows, as one batch.
    rows: RecordBatch,
    spill_files: usize,
    spilled_rows: usize,
    /// The query pool's high-water mark.
    peak: usize,
    /// Files left in the spill directory once the query has finished.
    left_behind: usize,
    /// Bytes left reserved in the pool once the query has finished.
    left_reserved: usize,
}

async fn run(table: &[RecordBatch], sql: &str, budget: u64) -> Run {
    let spill = TempDir::new().unwrap();
    let (session, _dir) = session_over(table, budget, &spill, None);
    let plan = session
        .ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), session.ctx.task_ctx())
        .await
        .unwrap_or_else(|error| panic!("{sql} under {budget} bytes: {error}"));
    let (spill_files, spilled_rows) = spills(&plan);
    let schema = plan.schema();
    drop(plan);
    Run {
        rows: concat_batches(&schema, &batches).unwrap(),
        spill_files,
        spilled_rows,
        peak: session.memory_peak.load(Ordering::Relaxed),
        left_behind: files_under(spill.path()),
        left_reserved: session.ctx.runtime_env().memory_pool.reserved(),
    }
}

/// Runs `sql` under `budget` and under a large budget and checks that the
/// constrained run spilled, kept to its budget, returned the large-budget rows
/// and cleaned up after itself. Returns the constrained run.
async fn assert_spills_to_the_large_budget_rows(
    table: &[RecordBatch],
    sql: &str,
    budget: u64,
) -> Run {
    let large = run(table, sql, LARGE_BUDGET).await;
    assert_eq!(large.spill_files, 0, "{sql}: the reference must not spill");
    let tight = run(table, sql, budget).await;
    assert!(
        tight.spill_files >= 2,
        "{sql}: the input exceeds {budget} bytes, so it spills more than once: {} files",
        tight.spill_files
    );
    assert!(
        tight.peak <= usize::try_from(budget).unwrap(),
        "{sql}: the query is admitted within its budget: peak {} of {budget}",
        tight.peak
    );
    assert!(
        tight.rows == large.rows,
        "{sql}: rows differ from the large budget"
    );
    for (name, finished) in [("constrained", &tight), ("large", &large)] {
        assert_eq!(
            finished.left_behind, 0,
            "{sql}: {name} run left spill files"
        );
        assert_eq!(
            finished.left_reserved, 0,
            "{sql}: {name} run left reservations"
        );
    }
    tight
}

/// 262144 rows in descending order of a Utf8 key `k` and an Int64 key `j`,
/// each with a 100-byte payload.
fn keyed_rows() -> Vec<RecordBatch> {
    const KEYS: i64 = 262_144;
    const BATCH: i64 = 8_192;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("j", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]));
    let pad = "x".repeat(100);
    (0..KEYS / BATCH)
        .map(|batch| {
            let top = KEYS - 1 - batch * BATCH;
            let keys = (top - BATCH + 1..=top).rev().collect::<Vec<_>>();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from_iter_values(
                        keys.iter().map(|key| format!("{key:010}")),
                    )),
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(StringArray::from(vec![pad.as_str(); BATCH as usize])),
                ],
            )
            .unwrap()
        })
        .collect()
}

/// The merge reads its spill files back by row-encoded keys (two keys), by a
/// primitive column (`j`) and by a string column (`k`); each returns the
/// large-budget rows within its budget.
#[tokio::test]
async fn a_spilling_sort_returns_the_large_budget_rows_for_every_key_shape() {
    let table = keyed_rows();
    for order in ["k, j", "j", "k"] {
        let sql = format!("SELECT k, j, pad FROM t ORDER BY {order}");
        let tight = assert_spills_to_the_large_budget_rows(&table, &sql, 16 * MIB).await;
        assert_eq!(
            tight.rows.num_rows(),
            table.iter().map(RecordBatch::num_rows).sum::<usize>(),
            "{sql}"
        );
    }
}

/// With more spill files than one merge pass can read within the budget, the
/// merge writes intermediate files (rows are spilled more than once) and still
/// returns the large-budget rows within its budget.
#[tokio::test]
async fn a_sort_whose_spill_files_outgrow_one_merge_pass_returns_the_large_budget_rows() {
    let table = keyed_rows();
    let rows = table.iter().map(RecordBatch::num_rows).sum::<usize>();
    for order in ["k, j", "j", "k"] {
        let sql = format!("SELECT k, j, pad FROM t ORDER BY {order}");
        let tight = assert_spills_to_the_large_budget_rows(&table, &sql, 4 * MIB).await;
        assert!(
            tight.spilled_rows > rows,
            "{sql}: intermediate merge passes respill rows: {} of {rows}",
            tight.spilled_rows
        );
    }
}

/// 262144 rows of 262144 distinct groups `g` with a 100-byte payload: the
/// group state is about 30 MiB.
fn grouped_rows() -> Vec<RecordBatch> {
    const GROUPS: i64 = 262_144;
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]));
    (0..GROUPS)
        .step_by(8_192)
        .map(|first| {
            let groups = (first..first + 8_192)
                .map(|row| row * 7_919 % GROUPS)
                .collect::<Vec<_>>();
            let pads = groups
                .iter()
                .map(|group| format!("{group:0100}"))
                .collect::<Vec<_>>();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(groups)),
                    Arc::new(StringArray::from(pads)),
                ],
            )
            .unwrap()
        })
        .collect()
}

/// A grouped aggregate whose groups exceed its budget spills its state, merges
/// it back within the budget, returns the large-budget groups and leaves
/// nothing behind.
#[tokio::test]
async fn a_spilling_aggregate_returns_the_large_budget_groups_within_its_budget() {
    let table = grouped_rows();
    let tight = assert_spills_to_the_large_budget_rows(
        &table,
        "SELECT g, max(pad) AS m FROM t GROUP BY g ORDER BY g",
        24 * MIB,
    )
    .await;
    assert_eq!(tight.rows.num_rows(), 262_144);
}

const NESTED_ROWS: usize = 120_000;

/// Rows with every column family a spill file must carry back: nullable
/// booleans, float bit patterns (signed zeros, a NaN payload), a dictionary
/// that every batch reuses, nullable lists with empty and null cells, a
/// nullable struct with a timestamp, string views both inline and out of line,
/// strings across the row format's 8- and 32-byte block boundaries, and fixed
/// binary. `key` is a permutation of `0..NESTED_ROWS`.
fn nested_rows() -> Vec<RecordBatch> {
    let record_fields = Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("at", DataType::Timestamp(TimeUnit::Millisecond, None), true),
    ]);
    let dictionary_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let list_type = DataType::new_list(DataType::Int64, true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, true),
        Field::new("float", DataType::Float64, false),
        Field::new("dict", dictionary_type, true),
        Field::new("nested", list_type, true),
        Field::new("record", DataType::Struct(record_fields.clone()), true),
        Field::new("view", DataType::Utf8View, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("fixed", DataType::FixedSizeBinary(16), true),
    ]));
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("shared-dictionary-value".repeat(16)),
        None,
        Some("second".to_owned()),
    ]));
    (0..NESTED_ROWS)
        .step_by(8_192)
        .map(|first| {
            let rows = first..(first + 8_192).min(NESTED_ROWS);
            let keys = rows
                .clone()
                .map(|row| i64::try_from(row * 7_919 % NESTED_ROWS).unwrap())
                .collect::<Vec<_>>();
            let flag = BooleanArray::from(
                keys.iter()
                    .map(|key| (key % 3 != 0).then_some(key % 2 != 0))
                    .collect::<Vec<_>>(),
            );
            let float = Float64Array::from(
                keys.iter()
                    .map(|key| match key % 4 {
                        0 => -0.0_f64,
                        1 => 0.0_f64,
                        2 => f64::from_bits(0x7ff8_0000_0000_0001),
                        _ => *key as f64,
                    })
                    .collect::<Vec<_>>(),
            );
            let dict = DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(
                    keys.iter()
                        .map(|key| (key % 5 != 0).then_some(i8::try_from(key % 3).unwrap()))
                        .collect::<Vec<_>>(),
                ),
                Arc::clone(&values),
            )
            .unwrap();
            let nested =
                ListArray::from_iter_primitive::<Int64Type, _, _>(keys.iter().map(|key| {
                    match key % 3 {
                        0 => Some(vec![Some(*key), None]),
                        1 => None,
                        _ => Some(vec![]),
                    }
                }));
            let record = StructArray::try_new(
                record_fields.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        keys.iter()
                            .map(|key| (key % 7 != 0).then_some(-key))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(TimestampMillisecondArray::from(
                        keys.iter()
                            .map(|key| (key % 11 != 0).then_some(1_700_000_000_000 + key))
                            .collect::<Vec<_>>(),
                    )),
                ],
                Some(keys.iter().map(|key| key % 13 != 0).collect()),
            )
            .unwrap();
            let view = StringViewArray::from(
                keys.iter()
                    .map(|key| match key % 3 {
                        0 => Some(format!("view-payload-{key}-{}", "v".repeat(24))),
                        1 => None,
                        _ => Some(format!("in{}", key % 100)),
                    })
                    .collect::<Vec<_>>(),
            );
            let text = StringArray::from(
                keys.iter()
                    .map(|key| {
                        let length = usize::try_from(key % 70).unwrap();
                        (key % 17 != 0).then(|| "t".repeat(length))
                    })
                    .collect::<Vec<_>>(),
            );
            let fixed = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                keys.iter().map(|key| {
                    (key % 19 != 0).then(|| {
                        let mut bytes = [0_u8; 16];
                        bytes[8..].copy_from_slice(&key.to_be_bytes());
                        bytes
                    })
                }),
                16,
            )
            .unwrap();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(flag),
                    Arc::new(float),
                    Arc::new(dict),
                    Arc::new(nested),
                    Arc::new(record),
                    Arc::new(view),
                    Arc::new(text),
                    Arc::new(fixed),
                ],
            )
            .unwrap()
        })
        .collect()
}

/// Every column family comes back from the spill files exactly: ordered by a
/// unique primitive key the rows are the input sorted by Arrow's own kernel,
/// and ordered by a mix of nullable variable-length, boolean and float keys
/// they are the large-budget rows.
#[tokio::test]
async fn a_spilling_sort_returns_nested_view_and_dictionary_columns_exactly() {
    let table = nested_rows();
    let by_key =
        assert_spills_to_the_large_budget_rows(&table, "SELECT * FROM t ORDER BY key", 8 * MIB)
            .await;
    let input = concat_batches(&table[0].schema(), &table).unwrap();
    let order = sort_to_indices(input.column(0), None, None).unwrap();
    let expected = take_record_batch(&input, &order).unwrap();
    assert!(
        by_key.rows == expected,
        "the spilled rows differ from the input sorted by key"
    );
    assert_spills_to_the_large_budget_rows(
        &table,
        "SELECT * FROM t ORDER BY text DESC NULLS FIRST, flag, float, key",
        8 * MIB,
    )
    .await;
}

/// A query dropped after its sort has spilled and begun to merge releases
/// every reservation and removes every spill file when it is dropped.
#[tokio::test]
async fn a_query_dropped_while_its_sort_merges_releases_its_memory_and_spill_files() {
    let table = keyed_rows();
    let spill = TempDir::new().unwrap();
    let (session, _dir) = session_over(&table, 4 * MIB, &spill, None);
    let plan = session
        .ctx
        .sql("SELECT k, j, pad FROM t ORDER BY k, j")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), session.ctx.task_ctx())
            .unwrap();
    let first = stream
        .next()
        .await
        .expect("the sort emits rows")
        .expect("the first merged batch");
    assert_eq!(
        first
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert!(
        files_under(spill.path()) > 0,
        "the merge reads spill files while it emits"
    );
    drop(first);
    drop(stream);
    drop(plan);
    assert_eq!(session.ctx.runtime_env().memory_pool.reserved(), 0);
    assert_eq!(
        files_under(spill.path()),
        0,
        "spill files outlived the query"
    );
}

/// A sort refused by its spill cap after it has written spill files fails with
/// the cap's refusal, and releases every reservation and spill file it held.
#[tokio::test]
async fn a_sort_refused_by_its_spill_cap_releases_its_memory_and_spill_files() {
    let table = keyed_rows();
    let spill = TempDir::new().unwrap();
    let (session, _dir) = session_over(&table, 4 * MIB, &spill, Some(8 * MIB));
    let plan = session
        .ctx
        .sql("SELECT k, j, pad FROM t ORDER BY k, j")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let error = datafusion::physical_plan::collect(Arc::clone(&plan), session.ctx.task_ctx())
        .await
        .expect_err("the sort needs more spill than its cap");
    assert!(
        error
            .to_string()
            .contains("exceeded the allowable limit of 8.0 MB"),
        "{error}"
    );
    assert!(
        spills(&plan).0 > 0,
        "the cap is reached after the sort has written spill files"
    );
    drop(plan);
    assert_eq!(session.ctx.runtime_env().memory_pool.reserved(), 0);
    assert_eq!(
        files_under(spill.path()),
        0,
        "spill files outlived the refusal"
    );
}
