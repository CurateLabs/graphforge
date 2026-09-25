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
//!   external `SortExec` under a memory pool of `min(max_partition_bytes/4,
//!   64 MiB)` (see `TESTED_POOL_MAX_BYTES`), spilling to a DataFusion
//!   `DiskManager`;
//! * `datafusion-always` — every fixed-width partition goes through the
//!   external sort, which measures the operator's cost where the baseline
//!   needs no spill;
//! * `native` — hybrid: partitions the baseline admits stay resident, and a
//!   partition the baseline would refuse is sorted by GraphForge's own
//!   bounded external merge (run-sort then k-way merge), with no third-party
//!   memory pool.  Run size uses the same formula as the DataFusion pool
//!   (`default_datafusion_pool_bytes`) so both candidates operate at equal
//!   per-partition memory envelopes.
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
use std::cmp::Reverse;
use std::collections::BinaryHeap;
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
    /// Candidate B (#1585): native bounded external merge (no DataFusion pool).
    NativeOnRefusal,
}

pub(super) fn mode() -> Result<Mode, GfError> {
    match std::env::var("GF_SHAPE_SPILL_SPIKE") {
        Err(std::env::VarError::NotPresent) => Ok(Mode::Baseline),
        Ok(mode) if mode == "baseline" => Ok(Mode::Baseline),
        Ok(mode) if mode == "datafusion" => Ok(Mode::OnRefusal),
        Ok(mode) if mode == "datafusion-always" => Ok(Mode::Always),
        Ok(mode) if mode == "native" => Ok(Mode::NativeOnRefusal),
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
        Mode::OnRefusal | Mode::NativeOnRefusal => admit_materialization(
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
    // #1585: zero records the pre-ADR-0047 refusal, for spike controls.
    if let Some(bytes) = bytes_env("GF_SHAPE_MAX_EXTERNAL_PARTITION_BYTES")? {
        budgets.max_external_partition_bytes = bytes;
    }
    Ok(budgets)
}

/// Upper bound on the default DataFusion pool: `budget / 4` is never allowed
/// to exceed this.  64 MiB is empirically proven safe for a 9M-record hub
/// partition that exceeds the default 256 MiB budget
/// (`partition-refusal-1584.md`: pool=64 MiB → 11 spill runs, publishes).
///
/// Setting pool = budget (the previous default) caused DataFusion to exhaust
/// its own `GreedyMemoryPool` during the in-memory merge phase.  With a 256 MiB
/// pool the inner streaming merge's `push_batch` grows `ExternalSorterMerge[0]`
/// while `ExternalSorter[0]` holds sorted sub-stream splits; their combined
/// peak reaches 255.8 MB, leaving only 244.4 KB headroom and causing
/// "Resources exhausted: Failed to allocate additional 264.0 KB for
/// ExternalSorterMerge[0]".  Dividing by 4 shrinks the per-cycle batch count
/// and keeps the two consumers' combined peak safely below the pool limit.
///
/// ADR 0047 obligation: "Any library pool is a sub-budget whose size is chosen
/// from tests at the partition sizes it will meet, not set equal to
/// `max_partition_bytes`."
const TESTED_POOL_MAX_BYTES: u64 = 64 << 20; // 64 MiB

/// Floor on the default DataFusion pool so that tiny-budget correctness tests
/// (tight budget ≪ 256 MiB) still produce a viable pool.  The floor is never
/// less than the budget itself; `default_datafusion_pool_bytes` clips to min.
const TESTED_POOL_MIN_BYTES: u64 = 16 << 10; // 16 KiB

/// Compute the default DataFusion pool from the recorded budget.
///
/// Returns `clamp(budget / 4, TESTED_POOL_MIN_BYTES, TESTED_POOL_MAX_BYTES)`.
/// This value is overridden by `GF_SHAPE_SPILL_POOL_BYTES`.
///
/// The 9M-star evidence proves that `budget / 4 = 64 MiB` is safe; see
/// `docs/development/evidence/partition-refusal-1584.md`.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn default_datafusion_pool_bytes(max_partition_bytes: u64) -> u64 {
    (max_partition_bytes / 4).clamp(TESTED_POOL_MIN_BYTES, TESTED_POOL_MAX_BYTES)
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
    let pool_bytes = bytes_env("GF_SHAPE_SPILL_POOL_BYTES")?
        .unwrap_or_else(|| default_datafusion_pool_bytes(max_partition_bytes));
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

// ─── Candidate B: GraphForge native bounded external merge (#1585) ────────────

/// Smallest run for the native external merge: each run contains at least this
/// many bytes so very small budgets still produce a few records per run.
const MIN_NATIVE_RUN_BYTES: u64 = 1 << 20; // 1 MiB

/// Process-unique counter for native run file names.
static NATIVE_RUN_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// A sorted run written to a temp file during the native sort phase.
/// Removed from disk when dropped.
struct NativeRun {
    path: PathBuf,
    records: u64,
}

impl Drop for NativeRun {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Sort `buf` in place and write raw `N`-byte records to a new temp file under
/// `dir`.  Returns the run on success (records count = `buf.len()`).
fn write_native_run<const N: usize>(dir: &Path, buf: &mut [[u8; N]]) -> Result<NativeRun, GfError> {
    use std::io::Write as _;
    buf.sort_unstable();
    let idx = NATIVE_RUN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = dir.join(format!("gf-native-run-{}-{idx}", std::process::id()));
    let mut file = std::fs::File::create(&path).map_err(storage)?;
    for record in buf.iter() {
        file.write_all(record).map_err(storage)?;
    }
    Ok(NativeRun {
        path,
        records: buf.len() as u64,
    })
}

/// Read exactly `buf.len()` bytes from `reader` into `buf`.
/// Returns `false` at end-of-file aligned to a record boundary.
/// Returns an error if EOF falls mid-record.
fn read_native_record(
    reader: &mut std::io::BufReader<std::fs::File>,
    buf: &mut [u8],
) -> Result<bool, GfError> {
    use std::io::Read as _;
    let n = buf.len();
    let mut total = 0;
    while total < n {
        match reader.read(&mut buf[total..]).map_err(storage)? {
            0 if total == 0 => return Ok(false),
            0 => return Err(storage("native run file truncated mid-record")),
            read => total += read,
        }
    }
    Ok(true)
}

/// A partition sorted by GraphForge's own bounded external merge.
/// Holds sorted run files until the coordinator streams them; dropping it
/// removes every run regardless of outcome.
pub(super) struct NativePartition {
    record_size: usize,
    runs: Vec<NativeRun>,
    fed: Multiset,
    evidence: ExternalEvidence,
    guard: bool,
}

impl NativePartition {
    /// Stream every record in globally sorted order into `consume`, then
    /// verify the multiset guard.  A mismatch is a hard error before any
    /// caller can publish.
    pub(super) fn for_each_record(
        self,
        mut consume: impl FnMut(&[u8]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let started = Instant::now();
        let n = self.record_size;
        let mut emitted = Multiset::default();

        // Open all run readers.
        let mut readers: Vec<std::io::BufReader<std::fs::File>> = self
            .runs
            .iter()
            .map(|run| {
                std::fs::File::open(&run.path)
                    .map(|f| std::io::BufReader::with_capacity(64 << 10, f))
                    .map_err(storage)
            })
            .collect::<Result<_, _>>()?;

        // Current record for each run (empty vec = not yet loaded / exhausted).
        let mut heads: Vec<Vec<u8>> = readers.iter_mut().map(|_| vec![0u8; n]).collect::<Vec<_>>();

        // Prime: load the first record from each run.
        let mut exhausted = vec![false; readers.len()];
        for (i, reader) in readers.iter_mut().enumerate() {
            if !read_native_record(reader, &mut heads[i])? {
                exhausted[i] = true;
            }
        }

        // k-way merge via a min-heap.
        // Heap entry: (Reverse(record_bytes), run_index).
        let mut heap: BinaryHeap<(Reverse<Box<[u8]>>, usize)> =
            BinaryHeap::with_capacity(readers.len());
        for (i, ex) in exhausted.iter().enumerate() {
            if !*ex {
                heap.push((Reverse(heads[i].clone().into_boxed_slice()), i));
            }
        }
        while let Some((Reverse(record), run_idx)) = heap.pop() {
            emitted.add(&record);
            consume(&record)?;
            if read_native_record(&mut readers[run_idx], &mut heads[run_idx])? {
                heap.push((Reverse(heads[run_idx].clone().into_boxed_slice()), run_idx));
            }
        }
        let merge_wall_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        if self.guard && emitted != self.fed {
            return Err(storage(
                "native external sort output differs from its input record multiset",
            ));
        }
        let mut evidence = self.evidence;
        evidence.merge_wall_ns = merge_wall_ns;
        if std::env::var_os("GF_SHAPE_SPILL_METRICS").is_some() {
            eprintln!(
                "SHAPE_SPILL_NATIVE {}",
                serde_json::to_string(&evidence).map_err(storage)?
            );
        }
        #[cfg(test)]
        record_evidence(evidence);
        Ok(())
    }
}

/// Inject a test fault into native run files (mirrors `inject` for DataFusion).
fn inject_native(fault: Fault, runs: &[NativeRun], record_size: usize) -> Result<(), GfError> {
    if fault == Fault::Abort {
        std::process::abort();
    }
    let Some(first) = runs.first() else {
        return Err(storage("native fault: no run files"));
    };
    match fault {
        Fault::Flip => {
            let mut bytes = std::fs::read(&first.path).map_err(storage)?;
            if bytes.len() < record_size {
                return Err(storage("native fault: run too small to flip"));
            }
            // Flip the last byte of the first record (never the UUID prefix).
            bytes[record_size - 1] ^= 0x5a;
            std::fs::write(&first.path, &bytes).map_err(storage)?;
        }
        Fault::Truncate => {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&first.path)
                .map_err(storage)?;
            f.set_len((record_size / 2) as u64).map_err(storage)?;
        }
        Fault::Abort => unreachable!(),
    }
    Ok(())
}

/// Sort phase for `load_native_external`: reads all records from `readers`,
/// groups them into sorted run files under `spill_dir`, and accumulates load
/// counters.  Returns `(runs, fed_multiset, total_records, scratch_bytes,
/// load_counters, sort_wall_ns)`.
#[allow(clippy::type_complexity)]
fn native_sort_phase<const N: usize>(
    readers: Vec<SegmentReader>,
    spill_dir: &Path,
    run_records: usize,
    temp_bytes_limit: Option<u64>,
    stop: &AtomicBool,
    base_counters: PartitionLoadCounters,
) -> Result<
    (
        Vec<NativeRun>,
        Multiset,
        u64,
        u64,
        PartitionLoadCounters,
        u64,
    ),
    GfError,
> {
    let mut runs: Vec<NativeRun> = Vec::new();
    let mut fed = Multiset::default();
    let mut counters = base_counters;
    let mut run_buf: Vec<[u8; N]> = Vec::with_capacity(run_records);
    let mut total_records: u64 = 0;
    let mut scratch_bytes: u64 = 0;
    let started = Instant::now();
    let mut readers_iter = readers.into_iter();
    let mut current: Option<SegmentReader> = None;

    'read: loop {
        if stop.load(Ordering::Acquire) {
            return Err(storage(
                "native partition sort abandoned after coordinator stop",
            ));
        }
        if current.is_none() {
            match readers_iter.next() {
                Some(seg) => current = Some(seg),
                None => break 'read,
            }
        }
        let seg = current.as_mut().unwrap();
        match read_run_record::<N>(&mut seg.0, None)? {
            None => {
                let seg = current.take().unwrap();
                let released = seg.0.into_inner().inner.finish().map_err(storage)?;
                let (rb, ro) = seg.1.values();
                merge_cache_release_evidence(&mut counters.cache_release, released)?;
                counters.read_bytes = counters
                    .read_bytes
                    .checked_add(rb)
                    .ok_or_else(|| storage("native: read byte count overflows"))?;
                counters.read_operations = counters
                    .read_operations
                    .checked_add(ro)
                    .ok_or_else(|| storage("native: read op count overflows"))?;
            }
            Some(record) => {
                fed.add(&record);
                run_buf.push(record);
                total_records += 1;
                counters.records += 1;
                if run_buf.len() >= run_records {
                    let run_bytes_now = run_buf.len() as u64 * N as u64;
                    if temp_bytes_limit
                        .is_some_and(|lim| scratch_bytes.saturating_add(run_bytes_now) > lim)
                    {
                        return Err(storage("native scratch: exceeded the allowable limit"));
                    }
                    let run = write_native_run::<N>(spill_dir, &mut run_buf)?;
                    scratch_bytes = scratch_bytes
                        .checked_add(run.records * N as u64)
                        .ok_or_else(|| storage("native: scratch byte count overflows"))?;
                    #[cfg(test)]
                    if runs.is_empty() {
                        SPILL_OBSERVED.store(true, Ordering::Release);
                    }
                    runs.push(run);
                    run_buf.clear();
                }
            }
        }
    }
    // Final partial run.
    if !run_buf.is_empty() {
        let run_bytes_now = run_buf.len() as u64 * N as u64;
        if temp_bytes_limit.is_some_and(|lim| scratch_bytes.saturating_add(run_bytes_now) > lim) {
            return Err(storage("native scratch: exceeded the allowable limit"));
        }
        let run = write_native_run::<N>(spill_dir, &mut run_buf)?;
        scratch_bytes = scratch_bytes
            .checked_add(run.records * N as u64)
            .ok_or_else(|| storage("native: scratch byte count overflows"))?;
        #[cfg(test)]
        if runs.is_empty() {
            SPILL_OBSERVED.store(true, Ordering::Release);
        }
        runs.push(run);
    }
    let sort_wall_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    Ok((
        runs,
        fed,
        total_records,
        scratch_bytes,
        counters,
        sort_wall_ns,
    ))
}

/// Sort one fixed-width partition with GraphForge's own bounded external merge.
///
/// Reads input records in run-sized chunks on the calling worker, sorts each
/// chunk in place, and writes it as a raw binary temp file.  Returns
/// `NativePartition` with the sorted runs ready for the coordinator's k-way
/// merge via `for_each_record`.
///
/// Only fixed-width (no `DetailCodec`) partitions are supported; the codec
/// path stays on the DataFusion adapter.
pub(super) fn load_native_external<const N: usize>(
    root: &StableDirectory,
    names: &[String],
    expected_records: Option<u64>,
    codec: Option<DetailCodec>,
    max_partition_bytes: u64,
    stop: &AtomicBool,
) -> Result<(NativePartition, PartitionLoadCounters), GfError> {
    if codec.is_some() {
        return Err(storage(
            "native external sort does not support detail-codec partitions",
        ));
    }
    if names.is_empty() {
        return Err(storage("partition has no sealed segments"));
    }
    let guard = guard_enabled()?;
    let temp_bytes_limit = bytes_env("GF_SHAPE_SPILL_TEMP_BYTES")?;
    // Use the same memory envelope as Candidate A (DataFusion pool) so both
    // candidates are compared at equal per-partition memory budgets.
    let run_bytes = default_datafusion_pool_bytes(max_partition_bytes).max(MIN_NATIVE_RUN_BYTES);
    let run_records = ((run_bytes as usize) / N).max(1);
    let spill_dir = match std::env::var_os("GF_SHAPE_SPILL_DIR") {
        Some(path) => PathBuf::from(&path),
        None => std::env::temp_dir(),
    };

    let mut total_spill_bytes = 0_u64;
    let mut readers: Vec<SegmentReader> = Vec::with_capacity(names.len());
    for name in names {
        let (reader, counter, length) = open_fixed_reader(root, name)?;
        total_spill_bytes = total_spill_bytes
            .checked_add(length)
            .ok_or_else(|| storage("native: spill byte count overflows"))?;
        readers.push((reader, counter));
    }
    let base_counters = PartitionLoadCounters {
        spill_bytes: total_spill_bytes,
        ..Default::default()
    };

    let (runs, fed, total_records, scratch_bytes, load_counters, sort_wall_ns) =
        native_sort_phase::<N>(
            readers,
            &spill_dir,
            run_records,
            temp_bytes_limit,
            stop,
            base_counters,
        )?;

    if let Some(fault) = fault()?
        && !runs.is_empty()
        && !FAULT_FIRED.swap(true, Ordering::AcqRel)
    {
        inject_native(fault, &runs, N)?;
    }
    if expected_records.is_some_and(|expected| total_records != expected) {
        return Err(storage(
            "native: partition differs from admitted record count",
        ));
    }
    let evidence = ExternalEvidence {
        records: total_records,
        input_wire_bytes: total_records * N as u64,
        pool_limit_bytes: 0,
        sort_wall_ns,
        spill_count: runs.len() as u64,
        spilled_bytes: scratch_bytes,
        spilled_rows: total_records,
        peak_spill_disk_bytes: scratch_bytes,
        peak_spill_files: runs.len() as u64,
        ..ExternalEvidence::default()
    };
    Ok((
        NativePartition {
            record_size: N,
            runs,
            fed,
            evidence,
            guard,
        },
        load_counters,
    ))
}

#[cfg(test)]
mod tests;
