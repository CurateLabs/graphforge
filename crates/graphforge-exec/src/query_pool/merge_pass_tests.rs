//! A merge pass is admitted in one selection for everything its participants
//! can hold at once; after it no reader, writer or copy asks the pool (#2173).
//! These controls run real spill files through `StreamingMergeBuilder`.

use std::collections::HashMap;
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
    /// Requests the inner pool refused.
    refused: Mutex<Vec<(String, usize)>>,
    /// Shrinks of the sponsor: the number of requests made so far, the size it
    /// had before and the bytes released.
    sponsor_shrinks: Mutex<Vec<(usize, usize, usize)>>,
    registered: Mutex<Vec<String>>,
    sponsor_peak: AtomicUsize,
    /// Bytes each consumer holds, by name.
    held: Mutex<HashMap<String, usize>>,
}

impl Recorder {
    fn new(inner: Arc<dyn MemoryPool>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            requests: Mutex::default(),
            refused: Mutex::default(),
            sponsor_shrinks: Mutex::default(),
            registered: Mutex::default(),
            sponsor_peak: AtomicUsize::new(0),
            held: Mutex::default(),
        })
    }
    fn held(&self, name: &str) -> usize {
        self.held.lock().unwrap().get(name).copied().unwrap_or(0)
    }
    fn hold(&self, reservation: &MemoryReservation, more: usize, less: usize) {
        let mut held = self.held.lock().unwrap();
        let bytes = held
            .entry(reservation.consumer().name().to_string())
            .or_default();
        *bytes = (*bytes + more) - less;
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
        self.hold(reservation, additional, 0);
        self.inner.grow(reservation, additional);
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.hold(reservation, 0, shrink);
        if reservation.consumer().name() == SPONSOR {
            let made = self.requests.lock().unwrap().len();
            self.sponsor_shrinks
                .lock()
                .unwrap()
                .push((made, reservation.size(), shrink));
        }
        self.inner.shrink(reservation, shrink);
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let name = reservation.consumer().name();
        self.requests
            .lock()
            .unwrap()
            .push((name.to_string(), additional));
        let result = self.inner.try_grow(reservation, additional);
        if result.is_err() {
            self.refused
                .lock()
                .unwrap()
                .push((name.to_string(), additional));
        }
        if result.is_ok() {
            self.hold(reservation, additional, 0);
        }
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

fn spill_manager(pool: &Arc<dyn MemoryPool>, directory: &tempfile::TempDir) -> SpillManager {
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
    merge_with_bound(manager, files, sponsor, batch_size).0
}

/// The merge, and the bytes of its narrowest pass (what the consumer of a
/// spilled aggregate admits in one request before it builds the merge).
fn merge_with_bound(
    manager: SpillManager,
    files: Vec<SortedSpillFile>,
    sponsor: MemoryReservation,
    batch_size: usize,
) -> (SendableRecordBatchStream, usize) {
    let order =
        LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0)))]).unwrap();
    let builder = StreamingMergeBuilder::new()
        .with_sorted_spill_files(files)
        .with_spill_manager(manager)
        .with_schema(schema())
        .with_expressions(&order)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(batch_size);
    let bound = builder.minimum_pass_bound().unwrap();
    (builder.with_reservation(sponsor).build().unwrap(), bound)
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
    let manager = spill_manager(&dynamic, &directory);
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
        let manager = spill_manager(&dynamic, &directory);
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

/// The bytes of the narrowest pass of `files` sorted files of `batches`
/// batches, as the consumer of a merge computes them.
fn narrowest_pass(files: usize, batches: usize, rows: usize, batch_size: usize) -> usize {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 * MIB));
    let directory = tempfile::tempdir().unwrap();
    let manager = spill_manager(&pool, &directory);
    let set = spill_files(&manager, files, batches, rows, |largest| largest);
    let sponsor = MemoryConsumer::new(SPONSOR).register(&pool);
    merge_with_bound(manager, set.files, sponsor, batch_size).1
}

/// The narrowest pass is the selection of two files at one buffered batch
/// each, with an intermediate writer only when a third file exists (#2173): a
/// pool of exactly the bytes `minimum_pass_bound` names admits the merge, with
/// the sponsor peaking at that size; a byte less is refused at selection,
/// before a reader exists. The two-file pass is the output pass and has no
/// writer to charge.
#[tokio::test]
async fn the_minimum_pass_bound_is_the_selection_of_two_files_at_one_batch() {
    const ROWS: usize = 8192;
    const BATCH_SIZE: usize = 8192;
    const BATCHES: usize = 3;
    for files in [2, 3] {
        let bound = narrowest_pass(files, BATCHES, ROWS, BATCH_SIZE);
        let run = |limit: usize| async move {
            let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(limit)));
            let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
            let directory = tempfile::tempdir().unwrap();
            let manager = spill_manager(&dynamic, &directory);
            let set = spill_files(&manager, files, BATCHES, ROWS, |largest| largest);
            let sponsor = MemoryConsumer::new(SPONSOR).register(&dynamic);
            let stream = merge(manager, set.files, sponsor, BATCH_SIZE);
            (drain(stream, set.total_rows).await, pool)
        };
        let (result, pool) = run(bound).await;
        result.unwrap_or_else(|error| panic!("{files} files in {bound} bytes: {error}"));
        assert_eq!(
            pool.sponsor_peak.load(Ordering::SeqCst),
            bound,
            "{files} files: the narrowest pass is what the selection takes at its second file"
        );
        assert!(pool.fresh("SpillReader").is_empty());
        let (result, pool) = run(bound - 1).await;
        let error = result.expect_err("one byte short of the narrowest pass");
        assert!(error.to_string().contains("Resources exhausted"), "{error}");
        assert!(
            !pool
                .registered
                .lock()
                .unwrap()
                .iter()
                .any(|name| name.starts_with("SpillReader")),
            "{files} files: the refusal came after a reader existed"
        );
    }
    // Three files leave one for a later pass, so their narrowest pass writes.
    let two = narrowest_pass(2, BATCHES, ROWS, BATCH_SIZE);
    let three = narrowest_pass(3, BATCHES, ROWS, BATCH_SIZE);
    assert!(three > two + 1000, "{two} {three}");
}

/// A narrower retry returns to what the pass held when its selection began,
/// not to nothing (#2173): the consumer that pre-reserved its narrowest pass
/// keeps it when the two-batch attempt is refused, so the retry at one batch
/// is covered and asks the pool for nothing.
#[tokio::test]
async fn a_narrower_retry_keeps_the_pass_pre_reserve() {
    const ROWS: usize = 256;
    const BATCH_SIZE: usize = 256;
    let bound = narrowest_pass(2, 24, ROWS, BATCH_SIZE);
    let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(bound + 1)));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let manager = spill_manager(&dynamic, &directory);
    let set = spill_files(&manager, 2, 24, ROWS, |largest| largest);
    let sponsor = MemoryConsumer::new(SPONSOR).register(&dynamic);
    sponsor.try_grow(bound).unwrap();
    pool.requests.lock().unwrap().clear();
    let stream = merge(manager, set.files, sponsor, BATCH_SIZE);
    drain(stream, set.total_rows).await.unwrap();
    let refused = pool.refused.lock().unwrap().clone();
    assert_eq!(
        refused.len(),
        1,
        "only the two-batch attempt is refused: {refused:?}"
    );
    // Until the merge starts reading, the sponsor returns nothing it holds.
    let started = pool
        .requests
        .lock()
        .unwrap()
        .iter()
        .position(|(name, _)| name.starts_with("register:SpillReader"))
        .expect("the merge reads its spill files");
    let shrinks = pool.sponsor_shrinks.lock().unwrap().clone();
    let early = shrinks
        .iter()
        .filter(|(made, _, _)| *made < started)
        .collect::<Vec<_>>();
    assert!(
        early.is_empty(),
        "the retry released the pre-reserve: {early:?}"
    );
    let asked = pool
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|(name, bytes)| name == SPONSOR && *bytes > 0)
        .count();
    assert_eq!(asked, 1, "the retry asked again");
}

/// Every pass of a multi-level merge opens with its narrowest form as one
/// request (#2173): six files in a pool whose half admits four of them at two
/// batches (where a pass stops growing) write one intermediate file, and the
/// second pass (two files and the intermediate) is admitted by one request
/// before its first reader exists, not one request per file.
#[tokio::test]
async fn a_later_pass_opens_with_its_minimum_in_one_request() {
    const ROWS: usize = 256;
    const BATCH_SIZE: usize = 256;
    // Four files in one pass, then the writer of the intermediate file.
    let four = {
        let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(256 * MIB)));
        let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
        let directory = tempfile::tempdir().unwrap();
        let manager = spill_manager(&dynamic, &directory);
        let set = spill_files(&manager, 4, 24, ROWS, |largest| largest);
        let sponsor = MemoryConsumer::new(SPONSOR).register(&dynamic);
        drain(
            merge(manager, set.files, sponsor, BATCH_SIZE),
            set.total_rows,
        )
        .await
        .unwrap();
        pool.sponsor_peak.load(Ordering::SeqCst)
    };
    let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(256 * MIB)));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let manager = spill_manager(&dynamic, &directory);
    let writer = datafusion::physical_plan::spill::writer_pass_bound(
        &schema(),
        spill_files(&manager, 1, 24, ROWS, |largest| largest).maximum,
        BATCH_SIZE,
        SpillCompression::Uncompressed,
    )
    .unwrap()
    .expect("a flat schema has a writer bound");
    drop(pool);
    let limit = 2 * (four + writer);
    let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(limit)));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let manager = spill_manager(&dynamic, &directory);
    let set = spill_files(&manager, 6, 24, ROWS, |largest| largest);
    let sponsor = MemoryConsumer::new(SPONSOR).register(&dynamic);
    drain(
        merge(manager, set.files, sponsor, BATCH_SIZE),
        set.total_rows,
    )
    .await
    .unwrap();
    let requests = pool.requests.lock().unwrap().clone();
    let readers = requests
        .iter()
        .enumerate()
        .filter(|(_, (name, _))| name == "register:SpillReaderMetadata")
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert_eq!(readers.len(), 4 + 3, "a pass of four files, then of three");
    let opening = requests[readers[3]..readers[4]]
        .iter()
        .filter(|(name, bytes)| name == SPONSOR && *bytes > 0)
        .collect::<Vec<_>>();
    // The pass opens with its narrowest form (two files and the intermediate
    // one: three files, so a writer) in one request, and only then selects.
    let minimum = narrowest_pass(3, 24, ROWS, BATCH_SIZE);
    assert_eq!(
        opening.first().map(|(_, bytes)| *bytes),
        Some(minimum),
        "the second pass did not open with its narrowest form: {opening:?}"
    );
}

/// The merge of a spilled aggregate beside a full exchange (#2173). The
/// exchange has taken every byte it is admitted and a busy partition holds a
/// quarter of the pool; the idle aggregate has spilled, so its last bulk shrink
/// armed a loan, the one request the pool guarantees it past its floor.
/// `pre_reserve` makes the aggregate's request: its state bound and the
/// narrowest pass of its two files together, which the loan serves although it
/// is past the floor of 10 MiB. Without it the merge selects on its own, and
/// the same loan has to carry the whole selection. Either way the merge asks
/// the pool for nothing once its first reader exists.
async fn merge_beside_a_full_exchange(pre_reserve: bool) {
    let query: Arc<dyn MemoryPool> = Arc::new(QueryMemoryPool::new(64 * MIB));
    let pool = Recorder::new(Arc::clone(&query));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let manager = spill_manager(&dynamic, &directory);
    let set = spill_files(&manager, 2, 4, 8192, |largest| largest);
    assert!(set.maximum > 900_000 && set.maximum < 1_100_000);

    let busy = spilled(&dynamic, 0);
    let idle = spilled(&dynamic, 1);
    let repartition = exchange(&dynamic, 0);
    busy.try_grow(26 * MIB).unwrap();
    while repartition.try_grow(MIB).is_ok() {}
    let error = repartition.try_grow(MIB).unwrap_err().to_string();
    assert!(error.contains("0.0 B remain available"), "{error}");
    let filled = pool.requests.lock().unwrap().len();

    // The state a merging aggregate holds beside its pass (two largest batches
    // and 64 bytes a group), admitted within its floor right after its spill.
    let state = 2 * set.maximum + 64 * (8192 + 1);
    let sponsor = if pre_reserve {
        let narrowest = narrowest_pass(2, 4, 8192, 8192);
        assert!(state + narrowest > 10 * MIB, "{narrowest}");
        idle.try_grow(state + narrowest)
            .expect("the loan serves the one request");
        idle.split(narrowest)
    } else {
        idle.try_grow(state).unwrap();
        idle.split(0)
    };
    let stream = merge(manager, set.files, sponsor, 8192);
    drain(stream, set.total_rows).await.unwrap();
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
    assert!(pool.reserved() <= 64 * MIB, "{}", pool.reserved());
}

#[tokio::test]
async fn a_spilled_aggregate_merges_beside_a_full_exchange_on_one_request() {
    merge_beside_a_full_exchange(true).await;
}

#[tokio::test]
async fn a_merge_without_a_pre_reserve_beside_a_full_exchange_is_carried_by_the_loan() {
    merge_beside_a_full_exchange(false).await;
}

/// Spill files whose batches are narrower than the output batch. A merge of
/// two files of 24 batches of 100 rows at a batch size of 8192 would hold every
/// batch of both files before its first output, while the pass credits each
/// reader four batches: the merge emits what it holds before a stream's second
/// refill instead, so no output is built from more than two batches of a
/// stream, and the readers stay within the bank without asking the pool. (An
/// output wider than the largest input batch is a copy past the working set,
/// whose excess asks the pool as every copy did before the working set.)
#[tokio::test]
async fn a_merge_of_batches_narrower_than_its_output_stays_within_its_pass() {
    const FILES: usize = 2;
    const ROWS: usize = 100;
    let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(256 * MIB)));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let manager = spill_manager(&dynamic, &directory);
    let files = spill_files(&manager, FILES, 24, ROWS, |largest| largest);
    let sponsor = MemoryConsumer::new(SPONSOR).register(&dynamic);
    let mut stream = merge(manager, files.files, sponsor, 8192);
    let mut expected = 0_i64;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        assert!(
            batch.num_rows() <= 2 * FILES * ROWS,
            "an output of {} rows was built from more than two batches of a stream",
            batch.num_rows()
        );
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
    assert_eq!(expected as usize, files.total_rows);
    let fresh = pool.fresh("SpillReader");
    assert!(fresh.is_empty(), "readers asked the pool: {fresh:?}");
}

/// An output the merge's consumer keeps is charged, as the merge builds the
/// next one, to a consumer that cannot spill, `MergeOutput`, and leaves the
/// sponsor: the sponsor's reservation peaks where it does when every output
/// is dropped, whatever the consumer retains, and the retained result is
/// refused only at the pool's limit, not at the limit less the floors the
/// pool keeps from a consumer that can spill (a sort's `collect`, #2173). The
/// outputs the working set carries beside the copies' workspace stay within
/// it, as they did, until the merge retires with them.
#[tokio::test]
async fn an_output_the_consumer_retains_is_charged_to_the_output_consumer() {
    const FILES: usize = 2;
    const BATCHES: usize = 12;
    const ROWS: usize = 2048;
    let output = "MergeOutput";
    let run = |retain: bool| {
        async move {
            let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(256 * MIB)));
            let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
            let directory = tempfile::tempdir().unwrap();
            let manager = spill_manager(&dynamic, &directory);
            let files = spill_files(&manager, FILES, BATCHES, ROWS, |largest| largest);
            let sponsor = MemoryConsumer::new(SPONSOR)
                .with_can_spill(true)
                .register(&dynamic);
            let mut stream = merge(manager, files.files, sponsor, ROWS);
            let mut retained = Vec::new();
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                rows += batch.num_rows();
                if retain {
                    retained.push(batch);
                }
            }
            assert_eq!(rows, files.total_rows);
            let kept = retained
                .iter()
                .map(|batch| batch.get_array_memory_size())
                .sum::<usize>();
            drop(stream);
            // The merge has retired: the sponsor holds what its working set
            // carries (two copies of the largest batch: the last outputs,
            // their claims and their headers' metadata) and the output
            // consumer's registration; the output consumer holds the rest.
            let working_set = 4 * files.maximum;
            assert!(
                pool.held(SPONSOR) <= working_set + 64 * 1024,
                "the sponsor holds {} beyond its working set of {working_set}",
                pool.held(SPONSOR)
            );
            assert!(
                pool.held(output) + pool.held(SPONSOR) >= kept,
                "{} and {} hold less than the {kept} retained bytes",
                pool.held(output),
                pool.held(SPONSOR)
            );
            assert_eq!(
                pool.fresh(output).is_empty(),
                !retain,
                "the output consumer asked the pool: {:?}",
                pool.fresh(output)
            );
            drop(retained);
            assert_eq!(pool.held(output), 0);
            assert_eq!(pool.held(SPONSOR), 0);
            assert_eq!(dynamic.reserved(), 0);
            pool.sponsor_peak.load(Ordering::SeqCst)
        }
    };
    let dropped = run(false).await;
    let retained = run(true).await;
    assert_eq!(
        retained, dropped,
        "retaining every output raised the sponsor's peak from {dropped} to {retained}"
    );
}

/// Beyond its minimum a pass grows within half the pool, and leaves the other
/// half to a sibling's narrowest pass or merge reserve, which is fatal when
/// refused: six files in a pool twice the four-file pass are merged four at a
/// time, then the remaining two and the intermediate file, where a pass that
/// grew to the pool's limit would read all six at once (and did, in a join of
/// two spilling sorts over a 48 MiB pool, whose second sort could then not
/// open its pass).
#[tokio::test]
async fn a_pass_grows_beyond_its_minimum_within_half_the_pool() {
    const ROWS: usize = 256;
    const BATCH_SIZE: usize = 256;
    let four = {
        let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(256 * MIB)));
        let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
        let directory = tempfile::tempdir().unwrap();
        let manager = spill_manager(&dynamic, &directory);
        let set = spill_files(&manager, 4, 24, ROWS, |largest| largest);
        let sponsor = MemoryConsumer::new(SPONSOR).register(&dynamic);
        drain(
            merge(manager, set.files, sponsor, BATCH_SIZE),
            set.total_rows,
        )
        .await
        .unwrap();
        pool.sponsor_peak.load(Ordering::SeqCst)
    };
    let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(256 * MIB)));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let manager = spill_manager(&dynamic, &directory);
    let writer = datafusion::physical_plan::spill::writer_pass_bound(
        &schema(),
        spill_files(&manager, 1, 24, ROWS, |largest| largest).maximum,
        BATCH_SIZE,
        SpillCompression::Uncompressed,
    )
    .unwrap()
    .expect("a flat schema has a writer bound");
    drop(pool);
    // Half the pool admits four files and the intermediate writer; the pool
    // admits six.
    let limit = 2 * (four + writer);
    let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(limit)));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let manager = spill_manager(&dynamic, &directory);
    let set = spill_files(&manager, 6, 24, ROWS, |largest| largest);
    let sponsor = MemoryConsumer::new(SPONSOR).register(&dynamic);
    drain(
        merge(manager, set.files, sponsor, BATCH_SIZE),
        set.total_rows,
    )
    .await
    .unwrap();
    let readers = pool
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|(name, _)| name == "register:SpillReaderMetadata")
        .count();
    assert_eq!(readers, 4 + 3, "a pass of four files, then of three");
    assert!(
        pool.sponsor_peak.load(Ordering::SeqCst) <= limit / 2,
        "the pass grew past half the pool: {} of {limit}",
        pool.sponsor_peak.load(Ordering::SeqCst)
    );
    assert!(pool.refused.lock().unwrap().is_empty());
}

/// A reader's share of the bank goes back to the pool once the reader is
/// exhausted and the merge holds none of its batches, while the readers still
/// reading keep theirs: files whose key ranges do not overlap are read one
/// after another, and the sponsor's reservation falls by a reader's share as
/// each file ends. (The sort test of #2173 reads twelve such files, whose
/// readers end at 6%, 16%, ... 99% of its output; main's readers release what
/// they asked per batch as they go.)
#[tokio::test]
async fn an_exhausted_reader_gives_its_share_back() {
    const FILES: usize = 4;
    const BATCHES: usize = 8;
    const ROWS: usize = 1024;
    let pool = Recorder::new(Arc::new(GreedyMemoryPool::new(256 * MIB)));
    let dynamic: Arc<dyn MemoryPool> = Arc::clone(&pool) as Arc<dyn MemoryPool>;
    let directory = tempfile::tempdir().unwrap();
    let manager = spill_manager(&dynamic, &directory);
    // File `f` holds the keys from `f * BATCHES * ROWS`, sorted: the merge
    // exhausts file 0, then file 1, ...
    let mut files = Vec::new();
    let mut maximum = 0;
    for file in 0..FILES {
        let mut writer = manager.create_in_progress_file("share control").unwrap();
        let mut largest = 0;
        for index in 0..BATCHES {
            let first = ((file * BATCHES + index) * ROWS) as i64;
            largest = largest.max(writer.append_batch(&batch(first, 1, ROWS, 100)).unwrap());
        }
        maximum = maximum.max(largest);
        files.push(SortedSpillFile {
            file: writer.finish().unwrap().unwrap(),
            max_record_batch_memory: largest,
        });
    }
    // A roomy pool selects two buffered batches a reader.
    let share = reader_pass_bound(&schema(), maximum, ROWS, SpillCompression::Uncompressed, 2)
        .unwrap()
        .expect("a flat schema has a reader bound");
    let sponsor = MemoryConsumer::new(SPONSOR).register(&dynamic);
    let mut stream = merge(manager, files, sponsor, ROWS);
    let mut expected = 0_i64;
    let mut held = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for key in keys.values() {
            assert_eq!(*key, expected);
            expected += 1;
        }
        held.push(pool.held(SPONSOR));
    }
    assert_eq!(expected as usize, FILES * BATCHES * ROWS);
    let start = held[0];
    let end = *held.last().unwrap();
    // Every file but the last ended before the last output, and the merge
    // held nothing of theirs by the output after: their shares are out.
    assert!(
        end + (FILES - 1) * share <= start,
        "the sponsor held {start} at the first output and {end} at the last, with a reader's share of {share}"
    );
    // Nothing is given back twice: the sponsor never falls below the live
    // readers' shares.
    assert!(
        end >= share,
        "the sponsor held {end} with a reader still live, whose share is {share}"
    );
    let fresh = pool.fresh("SpillReader");
    assert!(fresh.is_empty(), "readers asked the pool: {fresh:?}");
}
