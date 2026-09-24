//! Test-support-only spill/memory-pool experiment (#1507), never selected by
//! default.
//!
//! `GF_SHAPE_SPILL_SPIKE` selects how a fixed-width partition is sorted:
//!
//! * unset or `baseline` — the resident load and sort, which refuses a
//!   partition whose materialization exceeds the recorded
//!   `max_partition_bytes`;
//! * `datafusion` — hybrid: partitions the baseline admits stay resident, and
//!   a partition the baseline would *refuse* is sorted by DataFusion's
//!   external `SortExec` under a memory pool of `max_partition_bytes`, spilling
//!   to a DataFusion `DiskManager`;
//! * `datafusion-always` — every fixed-width partition goes through the
//!   external sort, which measures the operator's cost where the baseline
//!   needs no spill.
//!
//! Recorded splitters, routing, GraphForge's own spill segments, publication,
//! receipts and recovery are unchanged. DataFusion owns only the transient
//! sort runs it writes while one partition is sorted. Those runs are execution
//! scratch: path-based `tempfile`s, never fsynced, never checksummed, read back
//! with Arrow validation disabled, deleted on drop, and invisible to recovery
//! and to the allocation ledger. They are not durable checkpoints, so this
//! module never lets them outlive the partition that created them and never
//! treats them as resume state.
//!
//! Because DataFusion does not detect a changed spill payload, the adapter
//! carries its own guard: an order-independent checksum of the multiset of
//! records fed in, compared with the records coming out before the coordinator
//! can publish. `GF_SHAPE_SPILL_GUARD=off` disables it so the tests can show
//! what DataFusion alone lets through.
//!
//! The sort phase (consuming input, sorting, spilling runs) runs on the load
//! worker. The merge phase streams on the coordinator as it writes the
//! partition, so no fully sorted partition is ever resident.
use super::partition::admit_materialization;
use super::partition_load::PartitionLoadCounters;
use super::{merge_cache_release_evidence, open_fixed_reader, read_run_record, storage};
use crate::construction_detail_codec::DetailCodec;
use crate::construction_directory::ConstructionDirectory as StableDirectory;
use crate::corruption_checksum::Checksum;
use arrow::array::{
    Array, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::arrow::compute::SortOptions;
use datafusion::execution::TaskContext;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use graphforge_core::GfError;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Most records per input batch handed to `SortExec`, and its output batch
/// size.
const MAX_BATCH_RECORDS: usize = 8192;

/// Fewest records per batch; below this per-batch overhead dominates.
const MIN_BATCH_RECORDS: usize = 64;

/// DataFusion buffers whole input batches under the pool and needs room for a
/// sorted copy plus the merge reservation, so a batch far larger than the
/// pool can never be admitted. Size batches to an eighth of the pool.
fn batch_records<const N: usize>(pool_bytes: usize) -> usize {
    (pool_bytes / (8 * N)).clamp(MIN_BATCH_RECORDS, MAX_BATCH_RECORDS)
}

/// Upper bound on DataFusion's merge reservation, which it sets aside before
/// sorting so the merge can proceed after a spill. Capped at a quarter of the
/// pool so a small pool still leaves room to buffer input.
const MAX_MERGE_RESERVATION_BYTES: usize = 10 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode {
    Baseline,
    OnRefusal,
    Always,
}

pub(super) fn mode() -> Result<Mode, GfError> {
    match std::env::var("GF_SHAPE_SPILL_SPIKE") {
        Err(std::env::VarError::NotPresent) => Ok(Mode::Baseline),
        Ok(mode) if mode == "baseline" => Ok(Mode::Baseline),
        Ok(mode) if mode == "datafusion" => Ok(Mode::OnRefusal),
        Ok(mode) if mode == "datafusion-always" => Ok(Mode::Always),
        _ => Err(storage("invalid shape spill experiment mode")),
    }
}

fn bytes_env(name: &str) -> Result<Option<u64>, GfError> {
    match std::env::var(name) {
        Ok(value) => value.parse::<u64>().map(Some).map_err(storage),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(storage(error)),
    }
}

/// The resident materialization estimate `load_fixed_partition` admits, so
/// the hybrid routes exactly the partitions the baseline would refuse.
fn resident_bytes<const N: usize>(
    expected_records: Option<u64>,
    codec: Option<DetailCodec>,
    spill_bytes: u64,
) -> Option<u64> {
    let count = expected_records.unwrap_or(
        spill_bytes
            / if codec.is_some() {
                (N - 255) as u64
            } else {
                N as u64
            },
    );
    if codec.is_some() {
        count
            .checked_mul(std::mem::size_of::<usize>() as u64)
            .and_then(|offsets| spill_bytes.checked_add(offsets))
    } else {
        count.checked_mul(N as u64)
    }
}

/// Whether this partition takes the external path under `mode`. The segment
/// length is read only when the answer depends on it, so the baseline mode
/// performs exactly the baseline's filesystem work.
pub(super) fn selects_external<const N: usize>(
    mode: Mode,
    expected_records: Option<u64>,
    codec: Option<DetailCodec>,
    spill_bytes: impl FnOnce() -> Result<u64, GfError>,
    max_partition_bytes: u64,
) -> Result<bool, GfError> {
    Ok(match mode {
        Mode::Baseline => false,
        Mode::Always => true,
        Mode::OnRefusal => admit_materialization(
            resident_bytes::<N>(expected_records, codec, spill_bytes()?),
            max_partition_bytes,
        )
        .is_err(),
    })
}

/// A `GreedyMemoryPool` that also records its high-water reservation, which
/// the plain pool does not keep.
#[derive(Debug)]
struct PeakPool {
    inner: GreedyMemoryPool,
    peak: AtomicUsize,
}

impl PeakPool {
    fn observe(&self) {
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
    }
}

impl fmt::Display for PeakPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl MemoryPool for PeakPool {
    fn name(&self) -> &'static str {
        "graphforge-spill-spike-peak"
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.observe();
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }
    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.observe();
        Ok(())
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// Order-independent checksum of a record multiset: a count plus two wrapping
/// sums of differently seeded 64-bit record checksums. Accidental corruption
/// detection only, in the sense of `corruption_checksum`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Multiset {
    records: u64,
    sum: u64,
    salted: u64,
}

impl Multiset {
    fn add(&mut self, record: &[u8]) {
        let mut plain = Checksum::new();
        plain.update(record);
        let mut salted = Checksum::new();
        salted.update(b"gf-1507");
        salted.update(record);
        self.records += 1;
        self.sum = self.sum.wrapping_add(plain.finish());
        self.salted = self.salted.wrapping_add(salted.finish().rotate_left(17));
    }
}

/// Test-only fault injected once, after the sort phase has spilled and before
/// the merge reads the runs back. `abort` kills the process there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Flip,
    Truncate,
    Abort,
}

fn fault() -> Result<Option<Fault>, GfError> {
    match std::env::var("GF_SHAPE_SPILL_FAULT") {
        Err(std::env::VarError::NotPresent) => Ok(None),
        Ok(fault) if fault == "flip" => Ok(Some(Fault::Flip)),
        Ok(fault) if fault == "truncate" => Ok(Some(Fault::Truncate)),
        Ok(fault) if fault == "abort" => Ok(Some(Fault::Abort)),
        _ => Err(storage("invalid shape spill fault")),
    }
}

/// Faults fire once per process, on the first partition that spilled.
static FAULT_FIRED: AtomicBool = AtomicBool::new(false);

/// Set once an external sort in this process has finished its input with
/// spilled runs on disk, so a test can cancel while those runs exist.
#[cfg(test)]
static SPILL_OBSERVED: AtomicBool = AtomicBool::new(false);

/// Whether any external sort in this process has spilled.
#[cfg(test)]
pub(crate) fn spill_observed() -> bool {
    SPILL_OBSERVED.load(Ordering::Acquire)
}

fn spill_files(directories: &[PathBuf]) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for directory in directories {
        if let Ok(entries) = std::fs::read_dir(directory) {
            files.extend(
                entries
                    .filter_map(Result::ok)
                    .map(|entry| entry.path())
                    .filter(|path| path.is_file()),
            );
        }
    }
    files.sort();
    files
}

/// Mutate the spill run that holds `needle` (a record fed to the sort), inside
/// the record's payload bytes, never Arrow metadata or offsets: the reader
/// runs with validation disabled, so a metadata fault is not a safe test.
fn inject(fault: Fault, directories: &[PathBuf], needle: &[u8]) -> Result<(), GfError> {
    if fault == Fault::Abort {
        std::process::abort();
    }
    for path in spill_files(directories) {
        let mut bytes = std::fs::read(&path).map_err(storage)?;
        let Some(at) = bytes
            .windows(needle.len())
            .position(|window| window == needle)
        else {
            continue;
        };
        match fault {
            Fault::Flip => {
                // The last byte of the record: never the UUID prefix, so the
                // corrupted record still sorts where it did.
                bytes[at + needle.len() - 1] ^= 0x5a;
                std::fs::write(&path, &bytes).map_err(storage)?;
            }
            Fault::Truncate => {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .map_err(storage)?;
                file.set_len((at + needle.len() / 2) as u64)
                    .map_err(storage)?;
            }
            Fault::Abort => unreachable!(),
        }
        return Ok(());
    }
    Err(storage(
        "spill fault found no spilled run holding its record",
    ))
}

/// The input side, read from GraphForge's own sealed spill segments. Readers
/// are opened by the load worker through the construction directory's
/// descriptor authority and handed to the operator already open.
struct SegmentSource<const N: usize> {
    schema: SchemaRef,
    codec: Option<DetailCodec>,
    batch_records: usize,
    readers: Mutex<Option<Vec<SegmentReader>>>,
    state: Arc<SourceState>,
}

type SegmentReader = (
    std::io::BufReader<super::CountingRead<graphforge_filesystem::FileCacheReleasingReader>>,
    super::IoCounter,
);

#[derive(Default)]
struct SourceState {
    fed: Mutex<Multiset>,
    counters: Mutex<PartitionLoadCounters>,
    first_record: Mutex<Option<Vec<u8>>>,
    peak_disk_bytes: AtomicU64,
    peak_disk_files: AtomicUsize,
    runtime: Mutex<Option<Arc<RuntimeEnv>>>,
}

impl SourceState {
    fn observe_disk(&self) {
        if let Some(runtime) = self.runtime.lock().ok().and_then(|slot| slot.clone()) {
            let progress = runtime.disk_manager.spilling_progress();
            self.peak_disk_bytes
                .fetch_max(progress.current_bytes, Ordering::Relaxed);
            self.peak_disk_files
                .fetch_max(progress.active_files_count, Ordering::Relaxed);
        }
    }
}

impl<const N: usize> fmt::Debug for SegmentSource<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GraphForgePartitionSegments")
    }
}

impl<const N: usize> PartitionStream for SegmentSource<N> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let readers = self
            .readers
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
            .unwrap_or_default();
        let batches = SegmentBatches::<N> {
            codec: self.codec,
            schema: Arc::clone(&self.schema),
            batch_records: self.batch_records,
            readers: readers.into_iter(),
            current: None,
            state: Arc::clone(&self.state),
            done: false,
        };
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(batches),
        ))
    }
}

/// A synchronous batch iterator. It runs on the load worker's current-thread
/// runtime, where a blocking read blocks only this partition's sort.
struct SegmentBatches<const N: usize> {
    codec: Option<DetailCodec>,
    schema: SchemaRef,
    batch_records: usize,
    readers: std::vec::IntoIter<SegmentReader>,
    current: Option<SegmentReader>,
    state: Arc<SourceState>,
    done: bool,
}

impl<const N: usize> SegmentBatches<N> {
    fn finish_reader(&self, mut reader: SegmentReader) -> Result<(), GfError> {
        let released = reader.0.get_mut().inner.finish().map_err(storage)?;
        let (read_bytes, read_operations) = reader.1.values();
        let mut counters = self.state.counters.lock().map_err(storage)?;
        merge_cache_release_evidence(&mut counters.cache_release, released)?;
        counters.read_bytes = counters
            .read_bytes
            .checked_add(read_bytes)
            .ok_or_else(|| storage("partition read byte count overflows"))?;
        counters.read_operations = counters
            .read_operations
            .checked_add(read_operations)
            .ok_or_else(|| storage("partition read operation count overflows"))?;
        Ok(())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, GfError> {
        let mut fixed = FixedSizeBinaryBuilder::with_capacity(
            self.batch_records,
            i32::try_from(N).map_err(storage)?,
        );
        let mut details = BinaryBuilder::with_capacity(self.batch_records, self.batch_records * 64);
        let mut records = 0;
        let mut fed = Multiset::default();
        let mut first = None;
        while records < self.batch_records {
            if self.current.is_none() {
                self.current = self.readers.next();
                if self.current.is_none() {
                    break;
                }
            }
            let reader = self.current.as_mut().expect("current segment");
            let Some(record) = read_run_record::<N>(&mut reader.0, self.codec)? else {
                let reader = self.current.take().expect("current segment");
                self.finish_reader(reader)?;
                continue;
            };
            let wire = if self.codec.is_some() {
                &record[..N - 255 + usize::from(record[N - 256])]
            } else {
                &record[..]
            };
            if self.codec.is_some() {
                details.append_value(wire);
            } else {
                fixed.append_value(wire).map_err(storage)?;
            }
            fed.add(wire);
            if first.is_none() {
                first = Some(wire.to_vec());
            }
            records += 1;
        }
        {
            let mut total = self.state.fed.lock().map_err(storage)?;
            total.records += fed.records;
            total.sum = total.sum.wrapping_add(fed.sum);
            total.salted = total.salted.wrapping_add(fed.salted);
            let mut counters = self.state.counters.lock().map_err(storage)?;
            counters.records += fed.records;
        }
        if let Some(first) = first {
            let mut slot = self.state.first_record.lock().map_err(storage)?;
            slot.get_or_insert(first);
        }
        self.state.observe_disk();
        if records == 0 {
            return Ok(None);
        }
        let column: Arc<dyn Array> = if self.codec.is_some() {
            Arc::new(details.finish())
        } else {
            Arc::new(fixed.finish())
        };
        RecordBatch::try_new(Arc::clone(&self.schema), vec![column])
            .map(Some)
            .map_err(storage)
    }

    /// End of input: the sort phase has spilled every run it will spill.
    /// The one place a test fault can observe spilled runs before the merge.
    fn end_of_input(&self) -> Result<(), GfError> {
        // Read the run directories now, not at setup: in the default
        // OS-temporary mode DataFusion creates its directory lazily, on the
        // first spill.
        let directories = self
            .state
            .runtime
            .lock()
            .map_err(storage)?
            .as_ref()
            .map(|runtime| runtime.disk_manager.temp_dir_paths())
            .unwrap_or_default();
        #[cfg(test)]
        if !spill_files(&directories).is_empty() {
            SPILL_OBSERVED.store(true, Ordering::Release);
        }
        let Some(fault) = fault()? else {
            return Ok(());
        };
        if spill_files(&directories).is_empty() || FAULT_FIRED.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let needle = self
            .state
            .first_record
            .lock()
            .map_err(storage)?
            .clone()
            .ok_or_else(|| storage("spill fault has no fed record"))?;
        inject(fault, &directories, &needle)
    }
}

impl<const N: usize> Iterator for SegmentBatches<N> {
    type Item = datafusion::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let next = self.next_batch().and_then(|batch| {
            if batch.is_none() {
                self.end_of_input()?;
            }
            Ok(batch)
        });
        match next {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(datafusion::error::DataFusionError::Execution(
                    error.to_string(),
                )))
            }
        }
    }
}

/// Evidence for one externally sorted partition.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(super) struct ExternalEvidence {
    pub(super) records: u64,
    pub(super) input_wire_bytes: u64,
    pub(super) pool_limit_bytes: u64,
    pub(super) peak_pool_bytes: u64,
    pub(super) spill_count: u64,
    pub(super) spilled_bytes: u64,
    pub(super) spilled_rows: u64,
    pub(super) peak_spill_disk_bytes: u64,
    pub(super) peak_spill_files: u64,
    pub(super) sort_wall_ns: u64,
    pub(super) merge_wall_ns: u64,
}

/// A partition whose sort phase ran on the worker and whose merge streams on
/// the coordinator. Owns its runtime, operator and spill directory: dropping
/// it (consumed, failed or abandoned) deletes every run it wrote.
pub(super) struct ExternalPartition {
    runtime: Option<tokio::runtime::Runtime>,
    stream: Option<SendableRecordBatchStream>,
    pending: Option<RecordBatch>,
    plan: Arc<SortExec>,
    pool: Arc<PeakPool>,
    /// Keeps the disk manager, and with it the run directory, alive until the
    /// merge has read every run.
    _env: Arc<RuntimeEnv>,
    state: Arc<SourceState>,
    evidence: ExternalEvidence,
    guard: bool,
}

impl Drop for ExternalPartition {
    fn drop(&mut self) {
        if let Some(runtime) = &self.runtime {
            let _entered = runtime.enter();
            self.stream.take();
        }
    }
}

impl ExternalPartition {
    /// Stream every record in sorted order into `consume`, then check the
    /// guard. A guard mismatch is an error before the caller can publish.
    pub(super) fn for_each_record(
        mut self,
        mut consume: impl FnMut(&[u8]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let started = Instant::now();
        let mut emitted = Multiset::default();
        let mut batch = self.pending.take();
        loop {
            let Some(current) = batch.take() else {
                break;
            };
            let column = current.column(0);
            if let Some(values) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                for index in 0..values.len() {
                    let record = values.value(index);
                    emitted.add(record);
                    consume(record)?;
                }
            } else if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
                for index in 0..values.len() {
                    let record = values.value(index);
                    emitted.add(record);
                    consume(record)?;
                }
            } else {
                return Err(storage("external sort returned an unexpected column"));
            }
            let runtime = self.runtime.as_ref().expect("external runtime");
            let stream = self.stream.as_mut().expect("external stream");
            batch = runtime
                .block_on(stream.next())
                .transpose()
                .map_err(storage)?;
        }
        self.evidence.merge_wall_ns =
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let fed = *self.state.fed.lock().map_err(storage)?;
        if self.guard && emitted != fed {
            return Err(storage(
                "external sort output differs from its input record multiset",
            ));
        }
        if let Some(metrics) = self.plan.metrics() {
            self.evidence.spill_count = metrics.spill_count().unwrap_or(0) as u64;
            self.evidence.spilled_bytes = metrics.spilled_bytes().unwrap_or(0) as u64;
            self.evidence.spilled_rows = metrics.spilled_rows().unwrap_or(0) as u64;
        }
        self.state.observe_disk();
        self.evidence.peak_pool_bytes = self.pool.peak.load(Ordering::Relaxed) as u64;
        self.evidence.peak_spill_disk_bytes = self.state.peak_disk_bytes.load(Ordering::Relaxed);
        self.evidence.peak_spill_files = self.state.peak_disk_files.load(Ordering::Relaxed) as u64;
        self.stream.take();
        if self.pool.reserved() != 0 {
            return Err(storage("external sort leaked a memory reservation"));
        }
        if std::env::var_os("GF_SHAPE_SPILL_METRICS").is_some() {
            eprintln!(
                "SHAPE_SPILL {}",
                serde_json::to_string(&self.evidence).map_err(storage)?
            );
        }
        #[cfg(test)]
        record_evidence(self.evidence);
        Ok(())
    }
}

#[cfg(test)]
static TOTALS: Mutex<Vec<ExternalEvidence>> = Mutex::new(Vec::new());

#[cfg(test)]
fn record_evidence(evidence: ExternalEvidence) {
    if let Ok(mut totals) = TOTALS.lock() {
        totals.push(evidence);
    }
}

/// Every external partition consumed by this process since the last call.
#[cfg(test)]
pub(crate) fn take_recorded_evidence() -> Vec<serde_json::Value> {
    TOTALS
        .lock()
        .map(|mut totals| {
            totals
                .drain(..)
                .map(|evidence| serde_json::to_value(evidence).expect("serializable"))
                .collect()
        })
        .unwrap_or_default()
}

/// Where DataFusion puts its run directory. A configured directory gets a
/// `datafusion-XXXXXX` child; unset uses the library default, an unprefixed
/// `tempfile` directory (`.tmpXXXXXX`) in the OS temporary directory, which
/// nothing identifies as DataFusion's after a crash.
fn spill_mode() -> DiskManagerMode {
    match std::env::var_os("GF_SHAPE_SPILL_DIR") {
        Some(path) => DiskManagerMode::Directories(vec![Path::new(&path).to_path_buf()]),
        None => DiskManagerMode::OsTmpDirectory,
    }
}

fn guard_enabled() -> Result<bool, GfError> {
    match std::env::var("GF_SHAPE_SPILL_GUARD") {
        Err(std::env::VarError::NotPresent) => Ok(true),
        Ok(value) if value == "on" => Ok(true),
        Ok(value) if value == "off" => Ok(false),
        _ => Err(storage("invalid shape spill guard mode")),
    }
}

/// One partition's pool and runtime environment: a disk manager of its own,
/// so its disk limit is this partition's alone.
fn runtime_env(
    pool_bytes: usize,
    temp_bytes: Option<u64>,
) -> Result<(Arc<PeakPool>, Arc<RuntimeEnv>), GfError> {
    let pool = Arc::new(PeakPool {
        inner: GreedyMemoryPool::new(pool_bytes),
        peak: AtomicUsize::new(0),
    });
    let mut disk = DiskManagerBuilder::default().with_mode(spill_mode());
    if let Some(limit) = temp_bytes {
        disk = disk.with_max_temp_directory_size(limit);
    }
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .with_disk_manager_builder(disk)
        .build_arc()
        .map_err(storage)?;
    Ok((pool, env))
}

/// `SortExec` over the partition's segments, ascending on the whole record.
fn sort_plan<const N: usize>(
    codec: Option<DetailCodec>,
    pool_bytes: usize,
    readers: Vec<SegmentReader>,
    state: &Arc<SourceState>,
) -> Result<Arc<SortExec>, GfError> {
    let data_type = if codec.is_some() {
        DataType::Binary
    } else {
        DataType::FixedSizeBinary(i32::try_from(N).map_err(storage)?)
    };
    let schema = Arc::new(Schema::new(vec![Field::new("record", data_type, false)]));
    let source = StreamingTableExec::try_new(
        Arc::clone(&schema),
        vec![Arc::new(SegmentSource::<N> {
            schema: Arc::clone(&schema),
            codec,
            batch_records: batch_records::<N>(pool_bytes),
            readers: Mutex::new(Some(readers)),
            state: Arc::clone(state),
        })],
        None,
        [],
        false,
        None,
    )
    .map_err(storage)?;
    let ordering = LexOrdering::new([PhysicalSortExpr::new(
        col("record", &schema).map_err(storage)?,
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )])
    .ok_or_else(|| storage("empty external sort ordering"))?;
    let plan = Arc::new(SortExec::new(ordering, Arc::new(source)));
    if plan.properties().output_partitioning().partition_count() != 1 {
        return Err(storage("external sort unexpectedly repartitioned"));
    }
    Ok(plan)
}

/// `GF_SHAPE_MAX_PARTITION_BYTES` replaces the recorded
/// `max_partition_bytes` budget for a new session (#1509). It exists so the
/// hybrid's refused-partition path runs on real Graph500 partitions without a
/// public knob. The value is recorded in the checkpoint like any budget: a
/// resume must present the same value or it fails closed. Unset leaves the
/// caller's budgets untouched; an invalid value is refused.
pub(super) fn recorded_budget_override(
    mut budgets: super::GraphConstructionBudgets,
) -> Result<super::GraphConstructionBudgets, GfError> {
    if let Some(bytes) = bytes_env("GF_SHAPE_MAX_PARTITION_BYTES")? {
        budgets.max_partition_bytes = bytes;
    }
    Ok(budgets)
}

/// Sort one fixed-width partition with DataFusion's external `SortExec`.
///
/// Runs the sort phase to completion on the calling worker: every input
/// record is read, buffered under the pool, sorted, and spilled as needed.
/// The merge phase is returned unstarted beyond its first batch.
pub(super) fn load_external<const N: usize>(
    root: &StableDirectory,
    names: &[String],
    expected_records: Option<u64>,
    codec: Option<DetailCodec>,
    max_partition_bytes: u64,
    stop: &AtomicBool,
) -> Result<(ExternalPartition, PartitionLoadCounters), GfError> {
    if names.is_empty() {
        return Err(storage("partition has no sealed segments"));
    }
    if let Some(codec) = codec {
        codec.validate_size(N, 0, 0).map_err(storage)?;
    }
    let pool_bytes = bytes_env("GF_SHAPE_SPILL_POOL_BYTES")?.unwrap_or(max_partition_bytes);
    let pool_bytes = usize::try_from(pool_bytes).map_err(storage)?;
    let temp_bytes = bytes_env("GF_SHAPE_SPILL_TEMP_BYTES")?;
    let guard = guard_enabled()?;
    let mut spill_bytes = 0_u64;
    let mut readers = Vec::with_capacity(names.len());
    for name in names {
        let (reader, counter, length) = open_fixed_reader(root, name)?;
        spill_bytes = spill_bytes
            .checked_add(length)
            .ok_or_else(|| storage("partition spill byte count overflows"))?;
        readers.push((reader, counter));
    }
    let (pool, env) = runtime_env(pool_bytes, temp_bytes)?;
    let state = Arc::new(SourceState::default());
    *state.runtime.lock().map_err(storage)? = Some(Arc::clone(&env));
    let config = SessionConfig::new()
        .with_batch_size(batch_records::<N>(pool_bytes))
        .with_target_partitions(1)
        .with_sort_spill_reservation_bytes((pool_bytes / 4).min(MAX_MERGE_RESERVATION_BYTES));
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(Arc::clone(&env)),
    );
    let plan = sort_plan::<N>(codec, pool_bytes, readers, &state)?;
    // DataFusion reads spilled runs back through `spawn_blocking`; bound that
    // pool rather than accept tokio's default of 512 threads per partition.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .map_err(storage)?;
    let started = Instant::now();
    let (stream, pending) = runtime.block_on(async {
        let mut stream = plan.execute(0, context).map_err(storage)?;
        // The source is cooperative, so the sort yields to the runtime every
        // tokio budget; each yield re-checks the coordinator's stop flag.
        // Cancelling is DataFusion's own mechanism: the stream is dropped,
        // which deletes every run it spilled.
        let first = tokio::select! {
            biased;
            () = std::future::poll_fn(|_| {
                if stop.load(Ordering::Acquire) {
                    std::task::Poll::Ready(())
                } else {
                    std::task::Poll::Pending
                }
            }) => Err(storage("partition load abandoned after coordinator stop")),
            first = stream.next() => first.transpose().map_err(storage),
        }?;
        Ok::<_, GfError>((stream, first))
    })?;
    let sort_wall_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let mut counters = *state.counters.lock().map_err(storage)?;
    counters.spill_bytes = spill_bytes;
    let fed = *state.fed.lock().map_err(storage)?;
    if expected_records.is_some_and(|expected| fed.records != expected) {
        return Err(storage("partition differs from admitted record count"));
    }
    let input_wire_bytes = if codec.is_some() {
        spill_bytes
    } else {
        fed.records * N as u64
    };
    let partition = ExternalPartition {
        runtime: Some(runtime),
        stream: Some(stream),
        pending,
        plan,
        pool: Arc::clone(&pool),
        _env: env,
        state,
        evidence: ExternalEvidence {
            records: fed.records,
            input_wire_bytes,
            pool_limit_bytes: pool_bytes as u64,
            sort_wall_ns,
            ..ExternalEvidence::default()
        },
        guard,
    };
    Ok((partition, counters))
}

#[cfg(test)]
mod tests;
