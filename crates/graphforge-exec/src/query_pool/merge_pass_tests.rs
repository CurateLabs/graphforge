//! A merge pass is admitted in one selection for everything its participants
//! can hold at once; after it no reader, writer or copy asks the pool (#2173).
//! These controls run real spill files through `StreamingMergeBuilder`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::config::SpillCompression;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use datafusion::physical_plan::spill::{SpillManager, reader_pass_bound};
use futures::StreamExt;

use super::tests::{exchange, spilled};
use super::*;

const MIB: usize = 1024 * 1024;
const SPONSOR: &str = "merge pass sponsor";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]))
}

/// Rows of the sorted key sequence `first, first + step, ...`, `rows` of them.
fn batch(first: i64, step: i64, rows: usize, pad: usize) -> RecordBatch {
    let keys = (0..rows as i64)
        .map(|i| first + i * step)
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(keys.clone())),
            Arc::new(StringArray::from_iter_values(
                keys.iter().map(|key| format!("{key:0>pad$}")),
            )),
        ],
    )
    .unwrap()
}

/// Records every request that reserves bytes, by consumer name, and the
/// largest size the merge's sponsor reservation reaches.
#[derive(Debug)]
struct Recorder {
    inner: Arc<dyn MemoryPool>,
    requests: Mutex<Vec<(String, usize)>>,
    registered: Mutex<Vec<String>>,
    sponsor_peak: AtomicUsize,
}

impl Recorder {
    fn new(inner: Arc<dyn MemoryPool>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            requests: Mutex::default(),
            registered: Mutex::default(),
            sponsor_peak: AtomicUsize::new(0),
        })
    }
    fn fresh(&self, prefix: &str) -> Vec<(String, usize)> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, bytes)| name.starts_with(prefix) && *bytes > 0)
            .cloned()
            .collect()
    }
}

impl std::fmt::Display for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "recorder({})", self.inner)
    }
}

impl MemoryPool for Recorder {
    fn name(&self) -> &str {
        "merge-pass-recorder"
    }
    fn register(&self, consumer: &MemoryConsumer) {
        self.registered
            .lock()
            .unwrap()
            .push(consumer.name().to_string());
        self.requests
            .lock()
            .unwrap()
            .push((format!("register:{}", consumer.name()), 0));
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let name = reservation.consumer().name();
        self.requests
            .lock()
            .unwrap()
            .push((name.to_string(), additional));
        let result = self.inner.try_grow(reservation, additional);
        if result.is_ok() && name == SPONSOR {
            self.sponsor_peak
                .fetch_max(reservation.size() + additional, Ordering::SeqCst);
        }
        result
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// `files` sorted spill files of `batches` batches of `rows` rows each, file
/// `f` holding the keys `f, f + files, f + 2 * files, ...`. `declared` turns
/// the true largest batch of a file into the size the merge is told.
struct Files {
    files: Vec<SortedSpillFile>,
    maximum: usize,
    total_rows: usize,
}

fn spill_files(
    manager: &SpillManager,
    files: usize,
    batches: usize,
    rows: usize,
    declared: impl Fn(usize) -> usize,
) -> Files {
    let mut out = Vec::new();
    let mut maximum = 0;
    for file in 0..files {
        let mut writer = manager
            .create_in_progress_file("merge pass control")
            .unwrap();
        let mut largest = 0;
        for index in 0..batches {
            let first = (file + index * rows * files) as i64;
            largest = largest.max(
                writer
                    .append_batch(&batch(first, files as i64, rows, 100))
                    .unwrap(),
            );
        }
        maximum = maximum.max(largest);
        out.push(SortedSpillFile {
            file: writer.finish().unwrap().unwrap(),
            max_record_batch_memory: declared(largest),
        });
    }
    Files {
        files: out,
        maximum,
        total_rows: files * batches * rows,
    }
}

fn manager(pool: &Arc<dyn MemoryPool>, directory: &tempfile::TempDir) -> SpillManager {
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool))
        .with_temp_file_path(directory.path().to_path_buf())
        .build_arc()
        .unwrap();
    SpillManager::new(
        env,
        SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        schema(),
    )
}

fn merge(
    manager: SpillManager,
    files: Vec<SortedSpillFile>,
    sponsor: MemoryReservation,
    batch_size: usize,
) -> SendableRecordBatchStream {
    let order =
        LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0)))]).unwrap();
    StreamingMergeBuilder::new()
        .with_sorted_spill_files(files)
        .with_spill_manager(manager)
        .with_schema(schema())
        .with_expressions(&order)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(batch_size)
        .with_reservation(sponsor)
        .build()
        .unwrap()
}

/// Drains the merge and checks that it delivered every key once, in order.
async fn drain(mut stream: SendableRecordBatchStream, rows: usize) -> Result<()> {
    let mut expected = 0_i64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for key in keys.values() {
            assert_eq!(*key, expected);
            expected += 1;
        }
    }
    assert_eq!(expected as usize, rows);
    Ok(())
}

/// A reader of a bounded merge pass draws on the credit its pass was admitted
/// and on nothing else. The files here are declared a quarter of their true
/// size, so the pass credits its readers a quarter of what they decode; the
/// pool has plenty of room, and a reader that fell through to a fresh request
/// would be served. It must fail with the error of the credit instead, and
/// register nothing beyond it.
#[tokio::test]
async fn a_reader_with_credit_never_requests_fresh() {
    let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(256 * MIB)));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let manager = manager(&dynamic, &directory);
    let files = spill_files(&manager, 2, 12, 2048, |largest| largest / 4);
    let sponsor = MemoryConsumer::new(SPONSOR).register(&dynamic);
    let stream = merge(manager, files.files, sponsor, 2048);
    let error = drain(stream, files.total_rows)
        .await
        .expect_err("the readers outgrow the credit their pass was admitted");
    assert!(
        error
            .to_string()
            .contains("spill reader exceeded its admitted credit"),
        "{error}"
    );
    let fresh = pool.fresh("SpillReader");
    assert!(fresh.is_empty(), "readers asked the pool: {fresh:?}");
}

/// The pass bound covers every batch a reader can have alive, however the
/// merge interleaves them, and not a byte more: pools of exactly the bytes of
/// the pass admit the merge (any batch outside the bound would fail it with the
/// error of the credit), and a pool one byte short is refused at selection,
/// before a reader exists. Input batches are smaller than the merge's batch,
/// so every output batch has rows pending in several input batches.
#[tokio::test]
async fn the_pass_bound_covers_every_live_batch() {
    const FILES: usize = 2;
    const BATCHES: usize = 24;
    const ROWS: usize = 100;
    const BATCH_SIZE: usize = 256;
    let run = |limit: usize| async move {
        let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(limit)));
        let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
        let directory = tempfile::tempdir().unwrap();
        let manager = manager(&dynamic, &directory);
        let files = spill_files(&manager, FILES, BATCHES, ROWS, |largest| largest);
        let maximum = files.maximum;
        let sponsor = MemoryConsumer::new(SPONSOR).register(&dynamic);
        let stream = merge(manager, files.files, sponsor, BATCH_SIZE);
        let result = drain(stream, files.total_rows).await;
        (result, pool, maximum)
    };

    let (result, roomy, maximum) = run(256 * MIB).await;
    result.unwrap();
    // A roomy pool selects two buffered batches per reader.
    let two = roomy.sponsor_peak.load(Ordering::SeqCst);
    let bound = |buffer: usize| {
        reader_pass_bound(
            &schema(),
            maximum,
            BATCH_SIZE,
            SpillCompression::Uncompressed,
            buffer,
        )
        .unwrap()
        .expect("a flat schema has a reader bound")
    };
    // One buffered batch per reader takes one batch less from each.
    let one = two - FILES * (bound(2) - bound(1));
    assert!(roomy.fresh("SpillReader").is_empty());

    for (limit, peak) in [(two, two), (one, one)] {
        let (result, pool, _) = run(limit).await;
        result.unwrap_or_else(|error| panic!("a pool of {limit} bytes: {error}"));
        assert_eq!(pool.sponsor_peak.load(Ordering::SeqCst), peak);
        assert!(pool.fresh("SpillReader").is_empty());
    }
    // Short of the two-batch pass by a byte, the merge narrows to one batch per
    // reader; short of the one-batch pass it cannot start.
    let (result, pool, _) = run(two - 1).await;
    result.unwrap();
    assert_eq!(pool.sponsor_peak.load(Ordering::SeqCst), one);
    let (result, pool, _) = run(one - 1).await;
    let error = result.expect_err("two readers at one batch each do not fit");
    assert!(error.to_string().contains("Resources exhausted"), "{error}");
    assert!(
        !pool
            .registered
            .lock()
            .unwrap()
            .iter()
            .any(|name| name.starts_with("SpillReader")),
        "the refusal came after a reader existed"
    );
}

/// The merge of a spilled aggregate beside a full exchange starts, and asks
/// the pool for nothing once its first reader exists. The exchange has taken
/// every byte it is admitted, so the merge has what the pool keeps for the
/// aggregate: its floor and the loan its last spill armed. Its first selection
/// (two buffered batches per reader, the intermediate file's writer included)
/// is served by the loan, the second file's is refused, and the merge narrows
/// to one buffered batch per reader and starts; every byte it asks, it asks
/// before its first reader registers.
#[tokio::test]
async fn a_spilled_aggregate_merges_beside_a_full_exchange_and_asks_for_nothing_after_its_readers()
{
    let query: Arc<dyn MemoryPool> = Arc::new(QueryMemoryPool::new(64 * MIB));
    let pool = Recorder::new(Arc::clone(&query));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let manager = manager(&dynamic, &directory);
    let files = spill_files(&manager, 2, 4, 8192, |largest| largest);
    assert!(files.maximum > 900_000 && files.maximum < 1_100_000);

    let busy = spilled(&dynamic, 0);
    let idle = spilled(&dynamic, 1);
    let repartition = exchange(&dynamic, 0);
    busy.try_grow(8 * MIB).unwrap();
    while repartition.try_grow(MIB).is_ok() {}
    let error = repartition.try_grow(MIB).unwrap_err().to_string();
    assert!(error.contains("0.0 B remain available"), "{error}");
    let filled = pool.requests.lock().unwrap().len();

    let stream = merge(manager, files.files, idle, 8192);
    drain(stream, files.total_rows).await.unwrap();
    let requests = pool.requests.lock().unwrap()[filled..].to_vec();
    let first_reader = requests
        .iter()
        .position(|(name, _)| name.starts_with("register:SpillReader"))
        .expect("the merge reads its spill files");
    let late = requests[first_reader..]
        .iter()
        .filter(|(_, bytes)| *bytes > 0)
        .collect::<Vec<_>>();
    assert!(late.is_empty(), "asked after its readers: {late:?}");
    let selection = requests[..first_reader]
        .iter()
        .filter(|(_, bytes)| *bytes > 0)
        .count();
    assert!(
        selection >= 2,
        "the first selection was served whole: {requests:?}"
    );
    assert!(pool.reserved() <= 64 * MIB, "{}", pool.reserved());
}
