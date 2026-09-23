//! Test-support-only DataFusion sort and partition experiments (#1506).
//!
//! Three candidates, each measured against the construction path it would
//! replace; none is reachable from a default build or selected by default:
//!
//! * [`datafusion_sort_indices`]: in-memory `SortExec` producing the partition
//!   permutation, selected in the real loader by `GF_SHAPE_SORT_SPIKE=datafusion`.
//! * [`external_sort_fixed_partition`]: `SortExec` with a bounded
//!   `FairSpillPool` and a caller-owned spill directory, streaming the real
//!   sealed spill segments. It is the bounded-processing alternative to the
//!   fail-closed materialization budget for a partition one hub key makes too
//!   large for any splitter set.
//! * [`hash_repartition_then_merge`]: DataFusion's only data-dependent
//!   repartitioning (`Partitioning::Hash`; 54.1 has no range partitioning)
//!   followed by per-partition `SortExec` and a global
//!   `SortPreservingMergeExec`.
//!
//! Library spill files here are transient scratch in a directory the caller
//! owns. They are not receipts, checkpoints or recovery authority (#1507 owns
//! that evaluation). Protocol:
//! `docs/development/construction-reuse-inventory-protocol-1505.md`.
use super::io_evidence::{open_fixed_reader, read_run_record};
use super::storage;
use crate::construction_directory::ConstructionDirectory as StableDirectory;
use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, RecordBatch, UInt32Array};
use arrow::compute::{SortOptions, concat};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{
    FairSpillPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::{LexOrdering, Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::expressions::col;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, collect_partitioned};
use futures::{StreamExt, stream};
use graphforge_core::GfError;
use sha2::{Digest, Sha256};
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const KEY: &str = "record";
const INDEX: &str = "index";
const ASCENDING: SortOptions = SortOptions {
    descending: false,
    nulls_first: true,
};

/// Run a DataFusion plan on a private current-thread runtime. Construction
/// partition loads run on scoped `std` threads; entering this from inside a
/// Tokio runtime would nest runtimes, so it fails closed instead.
fn block_on<T>(future: impl Future<Output = Result<T, GfError>>) -> Result<T, GfError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(storage(
            "sort experiment refuses to nest a Tokio runtime inside construction",
        ));
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?
        .block_on(future)
}

fn ordering(schema: &Schema) -> Result<LexOrdering, GfError> {
    LexOrdering::new([PhysicalSortExpr::new(
        col(KEY, schema).map_err(storage)?,
        ASCENDING,
    )])
    .ok_or_else(|| storage("sort experiment ordering is empty"))
}

/// A re-executable source over batches already in memory.
#[derive(Debug)]
struct Batches(SchemaRef, Vec<RecordBatch>);

impl PartitionStream for Batches {
    fn schema(&self) -> &SchemaRef {
        &self.0
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.0),
            stream::iter(self.1.clone().into_iter().map(Ok)),
        ))
    }
}

fn source(
    schema: &SchemaRef,
    partitions: Vec<Arc<dyn PartitionStream>>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(StreamingTableExec::try_new(
        Arc::clone(schema),
        partitions,
        None,
        [],
        false,
        None,
    )?))
}

/// In-memory `SortExec` over `(record, index)`; returns the `index` column in
/// sorted order, which is the permutation the caller applies to its own
/// representation. The default pool is unbounded: this mode measures the
/// operator, not admission.
pub(super) fn datafusion_sort_indices(keys: ArrayRef) -> Result<UInt32Array, GfError> {
    let rows = u32::try_from(keys.len()).map_err(storage)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new(KEY, keys.data_type().clone(), false),
        Field::new(INDEX, DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![keys, Arc::new(UInt32Array::from_iter_values(0..rows))],
    )
    .map_err(storage)?;
    let input = source(
        &schema,
        vec![Arc::new(Batches(Arc::clone(&schema), vec![batch]))],
    )
    .map_err(storage)?;
    let plan = SortExec::new(ordering(&schema)?, input);
    let context = Arc::new(TaskContext::default().with_session_config(
        // One output batch per partition: splitting only adds a concat.
        SessionConfig::new().with_batch_size(usize::try_from(rows).map_err(storage)?.max(1)),
    ));
    let sorted = block_on(async {
        let mut output = plan.execute(0, context).map_err(storage)?;
        let mut columns = Vec::new();
        while let Some(batch) = output.next().await {
            columns.push(Arc::clone(batch.map_err(storage)?.column(1)));
        }
        Ok(columns)
    })?;
    let sorted = match sorted.as_slice() {
        [single] => Arc::clone(single),
        many => concat(&many.iter().map(AsRef::as_ref).collect::<Vec<_>>()).map_err(storage)?,
    };
    sorted
        .as_any()
        .downcast_ref::<UInt32Array>()
        .cloned()
        .ok_or_else(|| storage("sort experiment produced a non-u32 permutation"))
}

/// `FairSpillPool` that also records its high-water reservation, which the
/// pool itself does not retain.
#[derive(Debug)]
struct PeakPool {
    inner: FairSpillPool,
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
        write!(f, "PeakPool({})", self.inner)
    }
}

impl MemoryPool for PeakPool {
    fn name(&self) -> &'static str {
        "graphforge-sort-spike-peak"
    }
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.observe();
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DfResult<()> {
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

type SpillReader =
    std::io::BufReader<super::CountingRead<graphforge_filesystem::FileCacheReleasingReader>>;

/// Streams sealed fixed-width spill segments as Arrow batches. Taken once:
/// a second `execute` is refused rather than silently reading nothing.
struct SpillSegments<const N: usize> {
    schema: SchemaRef,
    readers: Mutex<Option<Vec<SpillReader>>>,
    batch_records: usize,
}

impl<const N: usize> fmt::Debug for SpillSegments<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GraphForgeSpillSegments")
    }
}

enum Segments {
    Reading(std::collections::VecDeque<SpillReader>),
    Refused,
    Done,
}

fn execution(error: impl fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

fn next_batch<const N: usize>(
    readers: &mut std::collections::VecDeque<SpillReader>,
    schema: &SchemaRef,
    batch_records: usize,
) -> DfResult<Option<RecordBatch>> {
    let mut values = Vec::with_capacity(batch_records * N);
    let mut records = 0;
    while records < batch_records {
        let Some(reader) = readers.front_mut() else {
            break;
        };
        match read_run_record::<N>(reader, None).map_err(execution)? {
            Some(record) => {
                values.extend_from_slice(&record);
                records += 1;
            }
            None => {
                if let Some(mut finished) = readers.pop_front() {
                    finished.get_mut().inner.finish().map_err(execution)?;
                }
            }
        }
    }
    if records == 0 {
        return Ok(None);
    }
    let width = i32::try_from(N).map_err(execution)?;
    let array = FixedSizeBinaryArray::try_new(width, values.into(), None)?;
    Ok(Some(RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(array)],
    )?))
}

impl<const N: usize> PartitionStream for SpillSegments<N> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        // A poisoned lock is refused like a second execution.
        let readers = self
            .readers
            .lock()
            .ok()
            .and_then(|mut readers| readers.take());
        let schema = Arc::clone(&self.schema);
        let batch_records = self.batch_records;
        let initial = match readers {
            Some(readers) => Segments::Reading(readers.into()),
            None => Segments::Refused,
        };
        let batches = stream::unfold(initial, move |state| {
            let schema = Arc::clone(&schema);
            async move {
                match state {
                    Segments::Refused => Some((
                        Err(execution("spill segments executed twice")),
                        Segments::Done,
                    )),
                    Segments::Done => None,
                    Segments::Reading(mut readers) => {
                        match next_batch::<N>(&mut readers, &schema, batch_records) {
                            Ok(Some(batch)) => Some((Ok(batch), Segments::Reading(readers))),
                            Ok(None) => None,
                            Err(error) => Some((Err(error), Segments::Done)),
                        }
                    }
                }
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            batches,
        ))
    }
}

/// What a bounded external sort of one partition did.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ExternalSortOutcome {
    pub(crate) records: u64,
    pub(crate) output_sha256: String,
    pub(crate) output_batches: u64,
    pub(crate) pool_limit_bytes: u64,
    pub(crate) peak_reserved_bytes: u64,
    pub(crate) spill_count: u64,
    pub(crate) spilled_bytes: u64,
    pub(crate) spilled_rows: u64,
}

/// Sort one fixed-width partition's sealed spill segments with a spilling
/// `SortExec` whose pool is capped at `pool_limit` bytes, streaming the sorted
/// wire records to `sink` in order. No step materializes the partition.
///
/// Fails closed if the output is not in nondecreasing record order or the
/// record count is not `expected_records`.
pub(super) fn external_sort_fixed_partition<const N: usize>(
    root: &StableDirectory,
    names: &[String],
    expected_records: u64,
    pool_limit: usize,
    spill_dir: &Path,
    batch_records: usize,
    mut sink: impl FnMut(&[u8]) -> Result<(), GfError>,
) -> Result<ExternalSortOutcome, GfError> {
    if batch_records == 0 || !spill_dir.is_dir() {
        return Err(storage(
            "external sort experiment needs a batch size and spill directory",
        ));
    }
    let width = i32::try_from(N).map_err(storage)?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        KEY,
        DataType::FixedSizeBinary(width),
        false,
    )]));
    let mut readers = Vec::with_capacity(names.len());
    for name in names {
        readers.push(open_fixed_reader(root, name)?.0);
    }
    let segments: Arc<dyn PartitionStream> = Arc::new(SpillSegments::<N> {
        schema: Arc::clone(&schema),
        readers: Mutex::new(Some(readers)),
        batch_records,
    });
    let pool = Arc::new(PeakPool {
        inner: FairSpillPool::new(pool_limit),
        peak: AtomicUsize::new(0),
    });
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .with_disk_manager_builder(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(vec![spill_dir.to_path_buf()])),
        )
        .build_arc()
        .map_err(storage)?;
    let config = SessionConfig::new()
        .with_batch_size(batch_records)
        // The merge phase's up-front reservation must fit inside the pool.
        .with_sort_spill_reservation_bytes(pool_limit / 4);
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime),
    );
    let plan = SortExec::new(
        ordering(&schema)?,
        source(&schema, vec![segments]).map_err(storage)?,
    );
    let mut digest = Sha256::new();
    let mut records = 0_u64;
    let mut output_batches = 0_u64;
    let mut previous: Option<[u8; N]> = None;
    block_on(async {
        let mut output = plan.execute(0, context).map_err(storage)?;
        while let Some(batch) = output.next().await {
            let batch = batch.map_err(storage)?;
            output_batches += 1;
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| storage("external sort produced a non-fixed column"))?;
            for index in 0..column.len() {
                let value: [u8; N] = column
                    .value(index)
                    .try_into()
                    .map_err(|_| storage("external sort changed the record width"))?;
                if previous.is_some_and(|prior| prior > value) {
                    return Err(storage("external sort output is not ordered"));
                }
                previous = Some(value);
                digest.update(value);
                records += 1;
                sink(&value)?;
            }
        }
        Ok(())
    })?;
    if records != expected_records {
        return Err(storage("external sort changed the partition record count"));
    }
    let metrics = plan
        .metrics()
        .ok_or_else(|| storage("external sort reported no metrics"))?;
    Ok(ExternalSortOutcome {
        records,
        output_sha256: super::hex(&digest.finalize()),
        output_batches,
        pool_limit_bytes: pool_limit as u64,
        peak_reserved_bytes: pool.peak.load(Ordering::Relaxed) as u64,
        spill_count: metrics.spill_count().unwrap_or(0) as u64,
        spilled_bytes: metrics.spilled_bytes().unwrap_or(0) as u64,
        spilled_rows: metrics.spilled_rows().unwrap_or(0) as u64,
    })
}

/// What hash repartitioning did to the concatenation property.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct HashRepartitionOutcome {
    pub(crate) partitions: u64,
    pub(crate) nonempty_partitions: u64,
    /// Whether `concat(sorted hash partition 0, .., P-1)` is globally sorted:
    /// the property recorded range splitters guarantee and shaping relies on.
    pub(crate) concat_is_globally_sorted: bool,
    pub(crate) max_partition_rows: u64,
    pub(crate) records: u64,
}

fn fixed_batches<const N: usize>(
    records: &[[u8; N]],
    batch_records: usize,
) -> Result<(SchemaRef, Vec<RecordBatch>), GfError> {
    let width = i32::try_from(N).map_err(storage)?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        KEY,
        DataType::FixedSizeBinary(width),
        false,
    )]));
    let mut batches = Vec::new();
    for chunk in records.chunks(batch_records.max(1)) {
        let array = FixedSizeBinaryArray::try_from_iter(chunk.iter()).map_err(storage)?;
        batches.push(
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).map_err(storage)?,
        );
    }
    Ok((schema, batches))
}

fn fixed_values<const N: usize>(batches: &[RecordBatch]) -> Result<Vec<[u8; N]>, GfError> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| storage("repartition produced a non-fixed column"))?;
        for index in 0..column.len() {
            values.push(
                column
                    .value(index)
                    .try_into()
                    .map_err(|_| storage("repartition changed the record width"))?,
            );
        }
    }
    Ok(values)
}

/// Hash-repartition `records` into `partitions`, sort each partition, and
/// (when `inspect`) report whether their concatenation is ordered; then
/// produce the global order the only way hash partitions allow, a
/// `SortPreservingMergeExec`. Timed callers pass `inspect = false` so only the
/// merge pass runs.
pub(super) fn hash_repartition_then_merge<const N: usize>(
    records: &[[u8; N]],
    partitions: usize,
    batch_records: usize,
    inspect: bool,
) -> Result<(Vec<[u8; N]>, HashRepartitionOutcome), GfError> {
    let (schema, batches) = fixed_batches(records, batch_records)?;
    // A `RepartitionExec` executes once, so each pass gets its own plan.
    let sorted_partitions = || -> Result<Arc<dyn ExecutionPlan>, GfError> {
        let input = source(
            &schema,
            vec![Arc::new(Batches(Arc::clone(&schema), batches.clone()))],
        )
        .map_err(storage)?;
        let hashed = Arc::new(
            RepartitionExec::try_new(
                input,
                Partitioning::Hash(vec![col(KEY, &schema).map_err(storage)?], partitions),
            )
            .map_err(storage)?,
        );
        Ok(Arc::new(
            SortExec::new(ordering(&schema)?, hashed).with_preserve_partitioning(true),
        ))
    };
    let sorted = if inspect {
        Some(sorted_partitions()?)
    } else {
        None
    };
    let merged = SortPreservingMergeExec::new(ordering(&schema)?, sorted_partitions()?);
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(batch_records.max(1))),
    );
    let (per_partition, global) = block_on(async {
        let per_partition = if let Some(sorted) = sorted {
            collect_partitioned(sorted, Arc::clone(&context))
                .await
                .map_err(storage)?
        } else {
            Vec::new()
        };
        let mut global = Vec::new();
        let mut output = merged.execute(0, context).map_err(storage)?;
        while let Some(batch) = output.next().await {
            global.push(batch.map_err(storage)?);
        }
        Ok((per_partition, global))
    })?;
    let mut concatenated = Vec::new();
    let mut nonempty = 0_u64;
    let mut max_rows = 0_u64;
    for partition in &per_partition {
        let values = fixed_values::<N>(partition)?;
        nonempty += u64::from(!values.is_empty());
        max_rows = max_rows.max(values.len() as u64);
        concatenated.extend(values);
    }
    let global = fixed_values::<N>(&global)?;
    if global.len() != records.len() || (inspect && concatenated.len() != records.len()) {
        return Err(storage("repartition changed the record count"));
    }
    if global.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(storage("sort-preserving merge output is not ordered"));
    }
    Ok((
        global,
        HashRepartitionOutcome {
            partitions: partitions as u64,
            nonempty_partitions: nonempty,
            concat_is_globally_sorted: inspect
                && concatenated.windows(2).all(|pair| pair[0] <= pair[1]),
            max_partition_rows: max_rows,
            records: records.len() as u64,
        },
    ))
}

#[cfg(feature = "test-support")]
pub mod bench;
#[cfg(test)]
mod tests;
