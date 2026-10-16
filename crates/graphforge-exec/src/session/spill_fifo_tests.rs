//! A real exchange must hand dead encoding workspace to its FIFO reader.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result, utils::memory::get_record_batch_memory_size};
use datafusion::execution::TaskContext;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use futures::StreamExt;
use tokio::sync::oneshot;

const ROWS: usize = 512;
const TIGHT_POOL: usize = 4 * 1024 * 1024;
const ROOMY_POOL: usize = 16 * 1024 * 1024;
const WRITER_GAP: usize = 64 * 1024;
// This finite fixture needs a fresh reader deficit as well as its donor.
// Its checked phase bound below includes construction and owner controls;
// 32 KiB covers R-W and remains smaller than its 71,816 B wire body.
const FIRST_READER_GAP: usize = 32 * 1024;
const BANK_CONTROL_CEILING: usize = 512;
const EVENT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

async fn wait_event(event: oneshot::Receiver<()>, description: &str) {
    tokio::time::timeout(EVENT_DEADLINE, event)
        .await
        .unwrap_or_else(|_| panic!("{description} did not arrive before the test deadline"))
        .unwrap_or_else(|_| panic!("{description} was dropped before delivery"));
}

fn known_control_target() -> bool {
    cfg!(any(
        target_os = "linux",
        all(target_os = "windows", not(target_vendor = "win7")),
        all(target_os = "macos", target_pointer_width = "64")
    ))
}

#[derive(Debug)]
struct GatedPartition {
    schema: SchemaRef,
    state: Mutex<Option<SourceState>>,
}

#[derive(Debug)]
struct SourceState {
    batches: std::vec::IntoIter<RecordBatch>,
    sent: Option<oneshot::Sender<()>>,
    resume: Option<oneshot::Receiver<()>>,
    yielded: usize,
}

impl PartitionStream for GatedPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let state = self.state.lock().unwrap().take().unwrap();
        let stream = futures::stream::unfold(state, |mut state| async move {
            if state.yielded == 1 {
                // Repartition polls again only after its synchronous spill and
                // the first marker send have completed. The consumer has not
                // been polled again, so no reader body exists yet.
                state.sent.take().unwrap().send(()).ok();
                state.resume.take().unwrap().await.ok();
            }
            let batch = state.batches.next()?;
            state.yielded += 1;
            Some((Ok(batch), state))
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

fn text(row: usize) -> String {
    format!("{row:08}:{}", "x".repeat(119))
}

fn input_batch() -> Result<RecordBatch> {
    let full_rows = 16 * 1024;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let full = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..full_rows as i64)),
            Arc::new(StringArray::from_iter_values((0..full_rows).map(text))),
        ],
    )?;
    // Both input batches share these same physical arrays. Their visible
    // payload is small, while each exchange queue entry would retain the full
    // independently sponsored source backing.
    Ok(full.slice(ROWS, ROWS))
}

// An upper bound over BOTH initialization and first decode, deliberately
// summing their temporary allocations even where the source scopes do not
// overlap. This is a fixture prerequisite, not a runtime admission estimator.
fn first_reader_peak(schema: &Schema) -> usize {
    let word = std::mem::size_of::<usize>();
    let alignment = std::mem::align_of::<MemoryReservation>()
        .max(std::mem::align_of::<std::sync::atomic::AtomicUsize>())
        .max(std::mem::align_of::<SchemaRef>());
    // SharedSchema contains SchemaRef + Budget. Budget contains the stock
    // MemoryReservation, Option<Arc<Credit>> and AtomicUsize; add Arc's two
    // words and four maximum-alignment padding slots across both structs.
    let schema_owner_control = std::mem::size_of::<MemoryReservation>()
        .checked_add(word.checked_mul(5).unwrap())
        .and_then(|n| n.checked_add(alignment.checked_mul(4)?))
        .unwrap();
    let body = 71_816usize;
    let initial_buf_reader = 8192usize;
    let scan_metadata = 2usize.checked_mul(2).unwrap().checked_mul(1024).unwrap();
    let schema_message_headers = 4096usize;
    // 4096 + two nodes * 512 + five buffers * 256.
    let record_message_headers = 4096usize
        .checked_add(2usize.checked_mul(512).unwrap())
        .and_then(|n| n.checked_add(5usize.checked_mul(256)?))
        .unwrap();
    // Keep BOTH the reader's complete schema construction grant and the
    // decode work's schema base; do not rely on the new resident shrink.
    let schema_fee = schema
        .fields()
        .iter()
        .try_fold(4096usize, |n, field| {
            n.checked_add(field.size().checked_mul(8)?)
        })
        .unwrap();
    assert!(schema_fee <= 8 * 1024);
    let both_schema_grants = schema_fee.checked_mul(2).unwrap();
    let persistent_message_highwater = 2usize.checked_mul(1024).unwrap();
    // This is added while `initial` still owns its BufReader allowance.
    let persistent_buf_reader = 8192usize;
    [
        body,
        initial_buf_reader,
        scan_metadata,
        schema_message_headers,
        record_message_headers,
        both_schema_grants,
        schema_owner_control,
        persistent_message_highwater,
        persistent_buf_reader,
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)
    .unwrap()
}

fn fixture_window_prerequisites(batch: &RecordBatch) -> Result<()> {
    use arrow::ipc::{MessageHeader, MetadataVersion, root_as_message};
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
    use datafusion::physical_plan::spill::SpillManager;
    use std::io::{Read, Seek, SeekFrom};

    assert_eq!(batch.num_rows(), ROWS);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.column(0).data_type(), &DataType::Int64);
    assert_eq!(batch.column(1).data_type(), &DataType::Utf8);
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| field.metadata().is_empty())
    );
    assert!(batch.schema().metadata().is_empty());
    // Pinned construction fee, used only to establish this fixture's static
    // prerequisites. The test's primary oracle is real IPC/exchange execution.
    let schema_bound = 4096
        + batch
            .schema()
            .fields()
            .iter()
            .map(|field| 8 * field.size())
            .sum::<usize>();
    assert!(schema_bound <= 8 * 1024);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(ROOMY_POOL));
    let source = MemoryConsumer::new("FIFO reference source backing").register(&pool);
    source.try_grow(get_record_batch_memory_size(batch))?;
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()?;
    let manager = SpillManager::new(
        env,
        SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        batch.schema(),
    );
    let file = manager
        .spill_record_batch_and_finish(
            std::slice::from_ref(batch),
            "FIFO reference frame prerequisites",
        )?
        .unwrap();
    let mut input = std::fs::File::open(file.path())?;
    let mut schemas = 0;
    let mut records = 0;
    loop {
        let mut prefix = [0; 4];
        input.read_exact(&mut prefix)?;
        if prefix == [255; 4] {
            input.read_exact(&mut prefix)?;
        }
        let len = i32::from_le_bytes(prefix);
        if len == 0 {
            break;
        }
        assert!(
            (0..=1024).contains(&len),
            "fixture message metadata must be at most 1 KiB"
        );
        let mut metadata = vec![0; len as usize];
        input.read_exact(&mut metadata)?;
        let message = root_as_message(&metadata).unwrap();
        assert_eq!(message.version(), MetadataVersion::V5);
        match message.header_type() {
            MessageHeader::Schema => schemas += 1,
            MessageHeader::RecordBatch => {
                records += 1;
                assert_eq!(message.bodyLength(), 71_816);
                assert!(FIRST_READER_GAP < message.bodyLength() as usize);
            }
            other => panic!("unexpected fixture IPC message: {other:?}"),
        }
        assert!(message.bodyLength() >= 0);
        input.seek(SeekFrom::Current(message.bodyLength()))?;
    }
    assert_eq!((schemas, records), (1, 1));
    drop(input);
    drop(file);
    drop(manager);
    drop(source);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

fn physical_bytes(array: &ArrayRef) -> usize {
    let data = array.to_data();
    let mut owners = HashMap::<usize, usize>::new();
    for buffer in data
        .buffers()
        .iter()
        .chain(data.nulls().map(|nulls| nulls.inner().inner()).into_iter())
    {
        if buffer.capacity() != 0 {
            owners
                .entry(buffer.data_ptr().as_ptr() as usize)
                .and_modify(|capacity| *capacity = (*capacity).max(buffer.capacity()))
                .or_insert(buffer.capacity());
        }
    }
    owners.values().sum()
}

async fn fifo_execution(limit: usize, rotate: bool) -> Result<Vec<(i64, String)>> {
    let batch = input_batch()?;
    fixture_window_prerequisites(&batch)?;
    let source_bytes = get_record_batch_memory_size(&batch);
    assert!(source_bytes > TIGHT_POOL / 2);
    let observed = Arc::new(InstallingPool {
        inner: GreedyMemoryPool::new(limit),
        candidates: None,
        arrives: std::sync::atomic::AtomicUsize::new(0),
        encoders: Mutex::new(HashMap::new()),
    });
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let source = MemoryConsumer::new("independently sponsored FIFO source").register(&pool);
    source.try_grow(source_bytes)?;
    let directory = tempfile::tempdir()?;
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_temp_file_path(directory.path().to_path_buf())
        .build_arc()?;
    let mut config = SessionConfig::new().with_batch_size(ROWS);
    if rotate {
        config.options_mut().execution.max_spill_file_size_bytes = 1;
    }
    let context = SessionContext::new_with_config_rt(config, env);
    let (sent, ready) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    let schema = batch.schema();
    let partition: Arc<dyn PartitionStream> = Arc::new(GatedPartition {
        schema: Arc::clone(&schema),
        state: Mutex::new(Some(SourceState {
            batches: vec![batch.clone(), batch].into_iter(),
            sent: Some(sent),
            resume: Some(resumed),
            yielded: 0,
        })),
    });
    let input = StreamingTableExec::try_new(schema, vec![partition], None, [], false, None)?;
    let exchange = RepartitionExec::try_new(Arc::new(input), Partitioning::RoundRobinBatch(1))?;
    let mut output = exchange.execute(0, context.task_ctx())?;
    // A current-thread runtime cannot run the newly spawned input task until
    // this first consumer poll returns Pending.
    assert!(futures::poll!(output.next()).is_pending());
    wait_event(ready, "first FIFO write").await;
    let initial_encoding_authority = observed.encoder_total();

    let pressure = MemoryConsumer::new("independent FIFO reader pressure").register(&pool);
    if limit == TIGHT_POOL {
        assert!(exchange.metrics().unwrap().spill_count().unwrap() > 0);
        assert!(
            initial_encoding_authority
                .checked_add(FIRST_READER_GAP)
                .unwrap()
                >= first_reader_peak(&exchange.schema())
                    .checked_add(BANK_CONTROL_CEILING)
                    .unwrap()
        );
        // Fund real R-W, but leave less than one full legacy reader body.
        // This does not require the donated writer window to cover all of R.
        pressure.try_grow(limit - pool.reserved() - FIRST_READER_GAP)?;
    }
    let mut rows = Vec::new();
    let first = output
        .next()
        .await
        .expect("the first spilled marker must produce a batch")
        .expect("dead encoding workspace must remain usable by the FIFO reader");
    append_rows(&first, &mut rows);
    let leaf = first.column(1).slice(0, 1);
    let body_bytes = physical_bytes(&leaf);
    drop(first);
    drop(leaf);
    if limit == TIGHT_POOL {
        assert!(body_bytes > 64 * 1024, "a leaf owns the full IPC body");
        // A full fresh writer window cannot fit in this gap. Returned reader
        // tickets must also be usable by the next real encoding operation.
        let free = limit - pool.reserved();
        if free < WRITER_GAP {
            pressure.shrink(WRITER_GAP - free);
        } else {
            pressure.try_grow(free - WRITER_GAP)?;
        }
        assert_eq!(limit - pool.reserved(), WRITER_GAP);
    }
    resume.send(()).unwrap();
    let mut final_leaf = None;
    while let Some(batch) = output.next().await {
        let batch = batch?;
        append_rows(&batch, &mut rows);
        if limit == TIGHT_POOL {
            final_leaf = Some(batch.column(1).slice(0, 1));
        }
    }
    assert_eq!(rows.len(), 2 * ROWS);
    if limit == TIGHT_POOL {
        assert_eq!(
            exchange.metrics().unwrap().spilled_rows().unwrap(),
            2 * ROWS
        );
        if rotate {
            assert_eq!(exchange.metrics().unwrap().spill_count().unwrap(), 2);
        }
    }
    // Keep the exhausted OUTER stream and plan alive: EOF itself must retire
    // unused credit instead of relying on eventual container destruction.
    if let Some(leaf) = final_leaf {
        let full_body = physical_bytes(&leaf);
        assert!(full_body > 64 * 1024);
        let escaped_authority = observed.encoder_total();
        assert!(escaped_authority > 0);
        assert!(
            escaped_authority < initial_encoding_authority,
            "EOF must retire unused encoding authority while the outer stream survives"
        );
        assert!(pool.reserved() >= source.size() + pressure.size() + full_body);
        assert!(output.next().await.is_none());
        drop(leaf);
        assert_eq!(
            observed.encoder_total(),
            0,
            "the last physical leaf must release its donor registration and control"
        );
    }
    drop(output);
    drop(exchange);
    drop(context);
    drop(pressure);
    drop(source);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    Ok(rows)
}

fn append_rows(batch: &RecordBatch, rows: &mut Vec<(i64, String)>) {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for row in 0..batch.num_rows() {
        rows.push((keys.value(row), values.value(row).to_owned()));
    }
}

#[tokio::test]
async fn spill_fifo_repartition_reuses_dead_writer_workspace_for_reading_and_the_next_write()
-> Result<()> {
    if !known_control_target() {
        return unproved_fifo_refuses_before_reading().await;
    }
    let expected = (0..2)
        .flat_map(|_| (ROWS..2 * ROWS).map(|row| (row as i64, text(row))))
        .collect::<Vec<_>>();
    assert_eq!(fifo_execution(ROOMY_POOL, false).await?, expected);
    assert_eq!(fifo_execution(TIGHT_POOL, false).await?, expected);
    Ok(())
}

#[tokio::test]
async fn spill_fifo_repartition_keeps_one_consumer_root_across_file_rotations() -> Result<()> {
    if !known_control_target() {
        return unproved_fifo_refuses_before_reading().await;
    }
    let expected = (0..2)
        .flat_map(|_| (ROWS..2 * ROWS).map(|row| (row as i64, text(row))))
        .collect::<Vec<_>>();
    assert_eq!(fifo_execution(TIGHT_POOL, true).await?, expected);
    Ok(())
}

#[derive(Debug)]
struct InstallingPool {
    inner: GreedyMemoryPool,
    candidates: Option<Arc<CandidateGate>>,
    arrives: std::sync::atomic::AtomicUsize,
    encoders: Mutex<HashMap<usize, usize>>,
}

#[derive(Debug, Default)]
struct CandidateGate {
    cancelled: Mutex<bool>,
    changed: std::sync::Condvar,
}
impl CandidateGate {
    fn wait(&self, arrives: &std::sync::atomic::AtomicUsize) -> Result<()> {
        use datafusion::common::DataFusionError;
        let deadline = std::time::Instant::now() + EVENT_DEADLINE;
        let mut cancelled = self.cancelled.lock().unwrap();
        loop {
            if *cancelled {
                return Err(DataFusionError::Internal(
                    "FIFO candidate test gate cancelled before peer arrival".into(),
                ));
            }
            if arrives.load(std::sync::atomic::Ordering::SeqCst) >= 2 {
                self.changed.notify_all();
                return Ok(());
            }
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "FIFO candidate peer did not reach the test gate before its deadline"
                            .into(),
                    )
                })?;
            let (guard, _) = self.changed.wait_timeout(cancelled, remaining).unwrap();
            cancelled = guard;
        }
    }
}
struct ReleaseCandidates(Arc<CandidateGate>);
impl Drop for ReleaseCandidates {
    fn drop(&mut self) {
        *self.0.cancelled.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}
impl std::fmt::Display for InstallingPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "concurrent FIFO donor control")
    }
}
impl InstallingPool {
    fn encoder_total(&self) -> usize {
        self.encoders.lock().unwrap().values().sum()
    }
    fn change(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        bytes: usize,
        grow: bool,
    ) {
        if reservation.consumer().name() == "SpillWriterEncode" {
            let mut entries = self.encoders.lock().unwrap();
            let held = entries.entry(reservation.consumer().id()).or_default();
            if grow {
                *held += bytes;
            } else {
                *held -= bytes;
            }
        }
    }
}
impl MemoryPool for InstallingPool {
    fn name(&self) -> &str {
        "concurrent FIFO donor control"
    }
    fn grow(&self, r: &datafusion::execution::memory_pool::MemoryReservation, n: usize) {
        self.inner.grow(r, n);
        self.change(r, n, true);
    }
    fn try_grow(
        &self,
        r: &datafusion::execution::memory_pool::MemoryReservation,
        n: usize,
    ) -> Result<()> {
        if self.candidates.is_some()
            && r.consumer().name() == "SpillWriterEncode"
            && n == 0
            && r.size() > 0
            && self
                .arrives
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                < 2
        {
            // Stock try_grow(0) still invokes this callback. Candidate control
            // construction reaches it after the absent-bank snapshot, with
            // neither file/outer/bank nor underlying pool lock held.
            self.candidates.as_ref().unwrap().wait(&self.arrives)?;
        }
        self.inner.try_grow(r, n)?;
        self.change(r, n, true);
        Ok(())
    }
    fn shrink(&self, r: &datafusion::execution::memory_pool::MemoryReservation, n: usize) {
        self.inner.shrink(r, n);
        self.change(r, n, false);
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spill_fifo_repartition_installs_one_donor_from_concurrent_owned_receipts() -> Result<()> {
    if !known_control_target() {
        return unproved_fifo_refuses_before_reading().await;
    }
    let candidates = Arc::new(CandidateGate::default());
    // Assertion failures or early returns must release any waiting peer.
    let _release = ReleaseCandidates(Arc::clone(&candidates));
    let observed = Arc::new(InstallingPool {
        inner: GreedyMemoryPool::new(TIGHT_POOL),
        candidates: Some(candidates),
        arrives: std::sync::atomic::AtomicUsize::new(0),
        encoders: Mutex::new(HashMap::new()),
    });
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let batch = input_batch()?;
    let source = MemoryConsumer::new("shared concurrent FIFO source").register(&pool);
    source.try_grow(get_record_batch_memory_size(&batch))?;
    let schema = batch.schema();
    let mut partitions: Vec<Arc<dyn PartitionStream>> = Vec::new();
    let mut readies = Vec::new();
    let mut resumes = Vec::new();
    for _ in 0..2 {
        let (sent, ready) = oneshot::channel();
        let (resume, resumed) = oneshot::channel();
        partitions.push(Arc::new(GatedPartition {
            schema: Arc::clone(&schema),
            state: Mutex::new(Some(SourceState {
                batches: vec![batch.clone(), batch.clone()].into_iter(),
                sent: Some(sent),
                resume: Some(resumed),
                yielded: 0,
            })),
        }));
        readies.push(ready);
        resumes.push(resume);
    }
    drop(batch);
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_temp_file_path(directory.path().to_path_buf())
        .build_arc()?;
    let context =
        SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(ROWS), runtime);
    let input = StreamingTableExec::try_new(schema, partitions, None, [], false, None)?;
    let exchange = RepartitionExec::try_new(Arc::new(input), Partitioning::RoundRobinBatch(1))?;
    let mut output = exchange.execute(0, context.task_ctx())?;
    let mut rows = Vec::new();
    if let std::task::Poll::Ready(item) = futures::poll!(output.next()) {
        append_rows(&item.expect("writers are still open")?, &mut rows);
    }
    let ready = futures::future::try_join_all(readies);
    tokio::pin!(ready);
    tokio::time::timeout(EVENT_DEADLINE, async {
        loop {
            tokio::select! {
                result = &mut ready => {
                    result.expect("both concurrent FIFO source events must arrive");
                    break;
                }
                batch = output.next() => {
                    // The one-output distribution gate needs receiver progress
                    // before the second producer's marker send can finish.
                    append_rows(&batch.expect("sources remain gated open")?, &mut rows);
                }
            }
        }
        Ok::<_, datafusion::common::DataFusionError>(())
    })
    .await
    .expect("both concurrent FIFO writes finish before the deadline")?;
    assert_eq!(
        observed.arrives.load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert_eq!(
        observed
            .encoders
            .lock()
            .unwrap()
            .values()
            .filter(|size| **size != 0)
            .count(),
        1,
        "the losing owned receipt must be released instead of growing the winning bank"
    );
    for resume in resumes {
        resume.send(()).unwrap();
    }
    while let Some(batch) = output.next().await {
        append_rows(&batch?, &mut rows);
    }
    rows.sort_unstable();
    let mut expected = (0..4)
        .flat_map(|_| (ROWS..2 * ROWS).map(|row| (row as i64, text(row))))
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(rows, expected);
    assert!(exchange.metrics().unwrap().spill_count().unwrap() >= 2);
    assert_eq!(
        exchange.metrics().unwrap().spilled_rows().unwrap(),
        4 * ROWS
    );
    drop(output);
    drop(exchange);
    drop(context);
    drop(source);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

#[derive(Debug)]
struct SequencePartition {
    schema: SchemaRef,
    state: Mutex<Option<SequenceState>>,
}
#[derive(Debug)]
struct SequenceState {
    batches: std::vec::IntoIter<RecordBatch>,
    gates: std::collections::VecDeque<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
    yielded: usize,
    dropped: Option<oneshot::Sender<()>>,
}
impl Drop for SequenceState {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            dropped.send(()).ok();
        }
    }
}
impl PartitionStream for SequencePartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let state = self.state.lock().unwrap().take().unwrap();
        let stream = futures::stream::unfold(state, |mut state| async move {
            if state.yielded != 0
                && let Some((sent, resume)) = state.gates.pop_front()
            {
                sent.send(()).ok();
                resume.await.ok();
            }
            let batch = state.batches.next()?;
            state.yielded += 1;
            Some((Ok(batch), state))
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}
fn varied_text(row: usize) -> String {
    let width = if (2 * ROWS..3 * ROWS).contains(&row) {
        1024
    } else {
        128
    };
    format!("{row:08}:{}", "x".repeat(width - 9))
}
fn varied_input() -> Result<(RecordBatch, RecordBatch)> {
    use arrow::array::StringBuilder;
    let n = 16 * 1024;
    let mut strings = StringBuilder::with_capacity(n, n * 128 + ROWS * (1024 - 128));
    for row in 0..n {
        strings.append_value(varied_text(row));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let full = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..n as i64)),
            Arc::new(strings.finish()),
        ],
    )?;
    Ok((full.slice(ROWS, ROWS), full.slice(2 * ROWS, ROWS)))
}

#[tokio::test]
async fn spill_fifo_repartition_larger_frame_refuses_before_decode_and_retires_on_error()
-> Result<()> {
    if !known_control_target() {
        return unproved_fifo_refuses_before_reading().await;
    }
    use datafusion::common::DataFusionError;
    let (small, large) = varied_input()?;
    fixture_window_prerequisites(&small)?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(TIGHT_POOL));
    let source = MemoryConsumer::new("larger FIFO frame source").register(&pool);
    source.try_grow(get_record_batch_memory_size(&small))?;
    let (sent1, ready1) = oneshot::channel();
    let (resume1, resumed1) = oneshot::channel();
    let (sent2, ready2) = oneshot::channel();
    let (resume2, resumed2) = oneshot::channel();
    let (dropped, done) = oneshot::channel();
    let schema = small.schema();
    let partition: Arc<dyn PartitionStream> = Arc::new(SequencePartition {
        schema: Arc::clone(&schema),
        state: Mutex::new(Some(SequenceState {
            batches: vec![small, large].into_iter(),
            gates: [(sent1, resumed1), (sent2, resumed2)].into(),
            yielded: 0,
            dropped: Some(dropped),
        })),
    });
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_temp_file_path(directory.path().to_path_buf())
        .build_arc()?;
    let context =
        SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(ROWS), runtime);
    let input = StreamingTableExec::try_new(schema, vec![partition], None, [], false, None)?;
    let exchange = RepartitionExec::try_new(Arc::new(input), Partitioning::RoundRobinBatch(1))?;
    let mut output = exchange.execute(0, context.task_ctx())?;
    assert!(futures::poll!(output.next()).is_pending());
    wait_event(ready1, "small FIFO write").await;
    let pressure = MemoryConsumer::new("larger FIFO frame independent pressure").register(&pool);
    pressure.try_grow(TIGHT_POOL - pool.reserved() - FIRST_READER_GAP)?;
    let first = output.next().await.unwrap()?;
    let mut rows = Vec::new();
    append_rows(&first, &mut rows);
    assert_eq!(
        rows,
        (ROWS..2 * ROWS)
            .map(|row| (row as i64, varied_text(row)))
            .collect::<Vec<_>>()
    );
    let leaf = first.column(1).slice(0, 1);
    let full_body = physical_bytes(&leaf);
    drop(first);
    pressure.free();
    resume1.send(()).unwrap();
    // A second actual write completes before the test fills ordinary free
    // space again. The donor remains the small first frame's owned workspace.
    wait_event(ready2, "larger FIFO write").await;
    assert_eq!(
        exchange.metrics().unwrap().spilled_rows().unwrap(),
        2 * ROWS
    );
    pressure.try_grow(TIGHT_POOL - pool.reserved() - 1024)?;
    let error = output
        .next()
        .await
        .unwrap()
        .expect_err("a larger frame needs real fresh authority");
    assert!(
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert!(error.to_string().contains("SpillReaderDecode"), "{error}");
    assert!(
        output.next().await.is_none(),
        "a fatal private FIFO read is terminal"
    );
    assert!(pool.reserved() >= source.size() + pressure.size() + full_body);
    // Error retirement has already happened with the exhausted outer stream
    // alive. Cancel the gated producer and wait for its actual data drop.
    drop(resume2);
    drop(output);
    drop(exchange);
    drop(context);
    wait_event(done, "failed FIFO producer data drop").await;
    drop(pressure);
    drop(source);
    assert!(pool.reserved() >= full_body);
    drop(leaf);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

#[derive(Debug)]
struct WorkerGate {
    release: (Mutex<bool>, std::sync::Condvar),
    started: tokio::sync::Notify,
}
struct ReleaseWorker(Arc<WorkerGate>);
impl Drop for ReleaseWorker {
    fn drop(&mut self) {
        *self.0.release.0.lock().unwrap() = true;
        self.0.release.1.notify_all();
    }
}
#[derive(Debug)]
struct CancellingPool {
    inner: InstallingPool,
    gate: Arc<WorkerGate>,
    blocked: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}
impl std::fmt::Display for CancellingPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FIFO active worker cancellation control")
    }
}
impl MemoryPool for CancellingPool {
    fn name(&self) -> &str {
        "FIFO active worker cancellation control"
    }
    fn grow(&self, r: &datafusion::execution::memory_pool::MemoryReservation, n: usize) {
        self.inner.grow(r, n);
    }
    fn try_grow(
        &self,
        r: &datafusion::execution::memory_pool::MemoryReservation,
        n: usize,
    ) -> Result<()> {
        self.inner.try_grow(r, n)?;
        if r.consumer().name() == "SpillReaderDecode"
            && !self.blocked.swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            self.gate.started.notify_one();
            let mut released = self.gate.release.0.lock().unwrap();
            while !*released {
                released = self.gate.release.1.wait(released).unwrap();
            }
        }
        Ok(())
    }
    fn shrink(&self, r: &datafusion::execution::memory_pool::MemoryReservation, n: usize) {
        self.inner.shrink(r, n);
        self.changed.notify_one();
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

#[tokio::test]
async fn spill_fifo_repartition_cancel_keeps_the_root_until_the_actual_worker_exits() -> Result<()>
{
    if !known_control_target() {
        return unproved_fifo_refuses_before_reading().await;
    }
    let gate = Arc::new(WorkerGate {
        release: (Mutex::new(false), std::sync::Condvar::new()),
        started: tokio::sync::Notify::new(),
    });
    // An assertion unwind must not strand a deliberately blocked worker.
    let release = ReleaseWorker(Arc::clone(&gate));
    let observed = Arc::new(CancellingPool {
        inner: InstallingPool {
            inner: GreedyMemoryPool::new(TIGHT_POOL),
            candidates: None,
            arrives: std::sync::atomic::AtomicUsize::new(0),
            encoders: Mutex::new(HashMap::new()),
        },
        gate: Arc::clone(&gate),
        blocked: std::sync::atomic::AtomicBool::new(false),
        changed: tokio::sync::Notify::new(),
    });
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let batch = input_batch()?;
    let source = MemoryConsumer::new("cancelled FIFO source").register(&pool);
    source.try_grow(get_record_batch_memory_size(&batch))?;
    let (sent, ready) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    let (dropped, done) = oneshot::channel();
    let schema = batch.schema();
    let partition: Arc<dyn PartitionStream> = Arc::new(SequencePartition {
        schema: Arc::clone(&schema),
        state: Mutex::new(Some(SequenceState {
            batches: vec![batch.clone(), batch].into_iter(),
            gates: [(sent, resumed)].into(),
            yielded: 0,
            dropped: Some(dropped),
        })),
    });
    let directory = tempfile::tempdir()?;
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_temp_file_path(directory.path().to_path_buf())
        .build_arc()?;
    let context =
        SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(ROWS), env);
    let input = StreamingTableExec::try_new(schema, vec![partition], None, [], false, None)?;
    let exchange = RepartitionExec::try_new(Arc::new(input), Partitioning::RoundRobinBatch(1))?;
    let mut output = exchange.execute(0, context.task_ctx())?;
    assert!(futures::poll!(output.next()).is_pending());
    wait_event(ready, "cancellable FIFO write").await;
    let seed = observed.inner.encoder_total();
    assert!(seed > 0);
    let pressure = MemoryConsumer::new("cancelled FIFO independent pressure").register(&pool);
    pressure.try_grow(TIGHT_POOL - pool.reserved() - 1024)?;
    {
        let next = output.next();
        tokio::pin!(next);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _=gate.started.notified()=>{},
                result=&mut next=>panic!("reader completed before worker barrier: {result:?}"),
            }
        })
        .await
        .expect("the real FIFO reader reaches its admitted worker boundary");
    }
    drop(output);
    drop(exchange);
    drop(context);
    // This notification is used on the same current-thread runtime. The
    // cancelled source's synchronous destructor finishes before we resume.
    wait_event(done, "cancelled FIFO producer data drop").await;
    drop(resume);
    assert_eq!(
        observed.inner.encoder_total(),
        seed,
        "dropping the consumer cannot retire the still-running worker's root"
    );
    drop(release);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let changed = observed.changed.notified();
            if observed.inner.encoder_total() == 0 {
                break;
            }
            changed.await;
        }
    })
    .await
    .expect("actual worker exit returns its tickets and retires unused authority");
    drop(pressure);
    drop(source);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

async fn unproved_fifo_refuses_before_reading() -> Result<()> {
    use datafusion::common::DataFusionError;
    let batch = input_batch()?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(TIGHT_POOL));
    let source = MemoryConsumer::new("unproved FIFO target source").register(&pool);
    source.try_grow(get_record_batch_memory_size(&batch))?;
    let (sent, ready) = oneshot::channel();
    let (_resume, resumed) = oneshot::channel();
    let (dropped, done) = oneshot::channel();
    let schema = batch.schema();
    let partition: Arc<dyn PartitionStream> = Arc::new(SequencePartition {
        schema: Arc::clone(&schema),
        state: Mutex::new(Some(SequenceState {
            batches: vec![batch.clone(), batch].into_iter(),
            gates: [(sent, resumed)].into(),
            yielded: 0,
            dropped: Some(dropped),
        })),
    });
    let directory = tempfile::tempdir()?;
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_temp_file_path(directory.path().to_path_buf())
        .build_arc()?;
    let context =
        SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(ROWS), env);
    let input = StreamingTableExec::try_new(schema, vec![partition], None, [], false, None)?;
    let exchange = RepartitionExec::try_new(Arc::new(input), Partitioning::RoundRobinBatch(1))?;
    let mut output = exchange.execute(0, context.task_ctx())?;
    assert!(futures::poll!(output.next()).is_pending());
    wait_event(ready, "unproved-target FIFO write").await;
    assert!(exchange.metrics().unwrap().spill_count().unwrap() > 0);
    let pressure = MemoryConsumer::new("unproved FIFO target pressure").register(&pool);
    pressure.try_grow(TIGHT_POOL - pool.reserved() - 1024)?;
    let error = output
        .next()
        .await
        .unwrap()
        .expect_err("an unproved mutex target must retain ordinary fresh reader admission");
    assert!(
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert!(error.to_string().contains("SpillReaderMetadata"), "{error}");
    drop(output);
    drop(exchange);
    drop(context);
    wait_event(done, "unproved-target FIFO producer data drop").await;
    drop(pressure);
    drop(source);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}
