//! Registered import files as bulk-builder sources (#1883).
//!
//! The builder reads each source in place, in parallel tasks. A task is a run
//! of whole construction batches, so batch boundaries, and therefore the
//! operation identity every batch normalizes under, match a sequential read of
//! the file.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_storage::{BulkBatchReader, BulkSource, SourceWorkspace};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use uuid::Uuid;

use super::bounded_ipc::{self, CheckedIpcReader};
use super::external_source::{self, ExternalSource, ObservedFile, SourceDigest};
use super::parquet_admission::SourceScan;
use super::{
    ImportSourceKind, SourceRecord, cancelled, canonicalize_parquet_batch, import_batch_operation,
    normalize_batch, storage, validation,
};
use crate::{BulkInputKind, CancellationToken, GraphForge};

/// Construction batches decoded per task: 16 x 65,536 rows is one row group of
/// the Graph500 generator and of the Parquet writer's default.
const BATCHES_PER_TASK: u64 = 16;

/// Only exact, non-null identity bounds can describe a task's UUID range.
fn exact_uuid_bounds(
    statistics: &parquet::file::statistics::Statistics,
) -> Option<([u8; 16], [u8; 16])> {
    if statistics.null_count_opt() != Some(0)
        || !statistics.min_is_exact()
        || !statistics.max_is_exact()
    {
        return None;
    }
    let low = <[u8; 16]>::try_from(statistics.min_bytes_opt()?).ok()?;
    let high = <[u8; 16]>::try_from(statistics.max_bytes_opt()?).ok()?;
    (low <= high).then_some((low, high))
}

enum Format {
    Parquet {
        metadata: ArrowReaderMetadata,
        rows: u64,
        /// What the source's pages hold and what its batches decode to (#1918).
        scan: SourceScan,
    },
    /// The planned IPC source (`bounded_ipc`): footer version, block
    /// inventory, schema and the rows of every record batch, so a task reads
    /// each block checked against this plan instead of parsing the footer
    /// again or constructing an eagerly decoding file reader.
    Arrow { plan: Arc<bounded_ipc::IpcPlan> },
}

/// The first batch a build refused, in the staged path's processing order (node
/// sources before edge sources, then source sequence, then batch index), with its
/// row count. Tasks run in parallel, so the lowest key is kept, not the first
/// to fail. A refusal that names no batch, such as a duplicate across batches,
/// leaves it empty, as in the staged path.
#[derive(Default)]
pub(super) struct Refusals(std::sync::Mutex<Option<(RefusalKey, u64)>>);

/// Node-or-edge, source sequence, batch index.
type RefusalKey = (u8, u64, u64);

impl Refusals {
    fn record(&self, key: RefusalKey, rows: u64) {
        if let Ok(mut slot) = self.0.lock()
            && slot.as_ref().is_none_or(|(held, _)| key < *held)
        {
            *slot = Some((key, rows));
        }
    }

    /// Rows of the refused batch, once.
    pub(super) fn take_rows(&self) -> Option<u64> {
        self.0.lock().ok()?.take().map(|(_, rows)| rows)
    }
}

/// Whole-file SHA-256 of each in-place source in a bulk build.
///
/// The digest is folded from the bytes the build's own tasks read, so hashing
/// adds no read of the source. SHA-256 is sequential and the tasks run in
/// parallel, but they are claimed in file order (see `claim_in_order` in
/// `graphforge-storage`): the bytes read ahead of the hashed prefix are held,
/// bounded by `external_source::pending_limit`, and whatever the decode never
/// reads, such as the page index, is read once by [`Digests::finish`].
#[derive(Default)]
pub(super) struct Digests {
    sources: Mutex<Vec<(u64, ExternalSource, SourceDigest)>>,
}

impl Digests {
    fn register(&self, sequence: u64, external: &ExternalSource, digest: &SourceDigest) {
        self.sources
            .lock()
            .expect("source digest registry lock poisoned")
            .push((sequence, external.clone(), digest.clone()));
    }

    /// Complete every source's digest, keyed by source sequence, and confirm each
    /// is still the file registration recorded.
    pub(super) fn finish(&self) -> Result<BTreeMap<u64, String>, GfError> {
        let sources = std::mem::take(
            &mut *self
                .sources
                .lock()
                .expect("source digest registry lock poisoned"),
        );
        let mut digests = BTreeMap::new();
        for (sequence, external, digest) in sources {
            let file = external.reopen()?;
            let sha256 = digest.finish(&external, &file)?;
            external.check(&file)?;
            digests.insert(sequence, sha256);
        }
        Ok(digests)
    }
}

/// An in-place Parquet source and the digest its tasks feed.
struct InPlace {
    external: ExternalSource,
    digest: SourceDigest,
}

struct SourceReader<'a> {
    graph: &'a GraphForge,
    path: PathBuf,
    /// `None` for a source an earlier version copied into the session.
    in_place: Option<InPlace>,
    kind: BulkInputKind,
    operation_uuid: Uuid,
    sequence: u64,
    batch_rows: usize,
    format: Format,
    schema_bytes: u64,
    decoding_bytes: u64,
    /// The canonical batch window; a batch that decodes to more than the window
    /// allows is refused before it is decoded.
    window_bytes: u64,
    /// The pool the builder binds before any task runs.
    workspace: Mutex<Option<Arc<SourceWorkspace>>>,
    cancellation: Option<&'a CancellationToken>,
    refusals: &'a Refusals,
}

impl SourceReader<'_> {
    fn emit(
        &self,
        index: u64,
        batch: RecordBatch,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        if self
            .cancellation
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(cancelled());
        }
        let rows = batch.num_rows() as u64;
        let refused = (|| {
            let batch = match self.format {
                Format::Parquet { .. } => canonicalize_parquet_batch(self.kind, &batch)?,
                Format::Arrow { .. } => batch,
            };
            let normalized = normalize_batch(
                self.graph,
                import_batch_operation(self.operation_uuid, self.sequence, index),
                self.kind,
                &batch,
            )?;
            let canonical = crate::resumable_construction::canonical_property_columns(
                match self.kind {
                    BulkInputKind::Node => graphforge_storage::ConstructionChunkKind::Node,
                    BulkInputKind::Edge => graphforge_storage::ConstructionChunkKind::Edge,
                },
                &normalized,
            )?;
            sink(canonical)
        })();
        if refused.is_err()
            && !self
                .cancellation
                .is_some_and(CancellationToken::is_cancelled)
        {
            self.refusals.record(
                (
                    u8::from(self.kind == BulkInputKind::Edge),
                    self.sequence,
                    index,
                ),
                rows,
            );
        }
        refused
    }
}

impl SourceReader<'_> {
    /// The reader over `task`'s rows: whole row groups, trimmed by a row
    /// selection where the task starts or ends inside one.
    fn parquet_reader<T: parquet::file::reader::ChunkReader + 'static>(
        &self,
        input: T,
        metadata: &ArrowReaderMetadata,
        rows: u64,
        first_batch: u64,
    ) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReader, GfError> {
        let batch_rows = self.batch_rows as u64;
        let start = first_batch * batch_rows;
        let end = (start + BATCHES_PER_TASK * batch_rows).min(rows);
        let mut groups = Vec::new();
        let mut group_start = 0_u64;
        let mut first_group_start = 0_u64;
        for (index, group) in metadata.metadata().row_groups().iter().enumerate() {
            let group_end = group_start + u64::try_from(group.num_rows()).unwrap_or(0);
            if group_end > start && group_start < end {
                if groups.is_empty() {
                    first_group_start = group_start;
                }
                groups.push(index);
            }
            group_start = group_end;
        }
        let covered = groups
            .iter()
            .map(|index| {
                u64::try_from(metadata.metadata().row_group(*index).num_rows()).unwrap_or(0)
            })
            .sum::<u64>();
        let mut builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(input, metadata.clone())
                .with_batch_size(self.batch_rows)
                .with_row_groups(groups);
        if first_group_start != start || first_group_start + covered != end {
            let mut selectors = Vec::new();
            let skip_before = start - first_group_start;
            if skip_before > 0 {
                selectors.push(RowSelector::skip(
                    usize::try_from(skip_before).map_err(storage)?,
                ));
            }
            selectors.push(RowSelector::select(
                usize::try_from(end - start).map_err(storage)?,
            ));
            let skip_after = first_group_start + covered - end;
            if skip_after > 0 {
                selectors.push(RowSelector::skip(
                    usize::try_from(skip_after).map_err(storage)?,
                ));
            }
            builder = builder.with_row_selection(RowSelection::from(selectors));
        }
        builder.build().map_err(storage)
    }

    /// Decode an in-place task, confirming before every batch and at the end that
    /// the file is still the one registered.
    #[allow(clippy::too_many_arguments)]
    fn decode_parquet(
        &self,
        input: ObservedFile,
        metadata: &ArrowReaderMetadata,
        rows: u64,
        first_batch: u64,
        in_place: &InPlace,
        guard: &File,
        admission: &TaskAdmission,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let changed = |error: GfError| in_place.external.reclassify(guard, error);
        let mut reader = self
            .parquet_reader(input, metadata, rows, first_batch)
            .map_err(changed)?;
        let mut offset = 0_u64;
        loop {
            // The next batch is sized before the reader decodes it.
            self.admit_batch(admission, rows, first_batch, offset)?;
            let Some(batch) = reader.next() else { break };
            #[cfg(test)]
            super::external_source::pass_hook(
                &in_place.external.path,
                "batch",
                first_batch + offset,
            );
            // The source can change between any two batches.
            in_place.external.check(guard)?;
            let batch = batch.map_err(|error| changed(storage(error)))?;
            self.emit(first_batch + offset, batch, sink)?;
            offset += 1;
        }
        in_place.external.check(guard)
    }

    /// Refuse a schema that normalization would refuse, before the task decodes
    /// anything. The check is the one the first batch's normalization runs, on an
    /// empty batch of the source's schema, so the refusal is the same and an
    /// unsupported column costs no decode (an Arrow file's reader would decode
    /// every dictionary first).
    fn validate_schema(&self, first_batch: u64) -> Result<(), GfError> {
        let (schema, parquet, first_rows) = match &self.format {
            Format::Parquet { metadata, rows, .. } => (
                Arc::clone(metadata.schema()),
                true,
                (self.batch_rows as u64)
                    .min(rows.saturating_sub(first_batch * self.batch_rows as u64)),
            ),
            Format::Arrow { plan } => (
                Arc::clone(&plan.schema),
                false,
                usize::try_from(first_batch)
                    .ok()
                    .and_then(|index| plan.rows.get(index))
                    .copied()
                    .unwrap_or(0),
            ),
        };
        let empty = RecordBatch::new_empty(schema);
        let checked = (|| {
            let empty = if parquet {
                canonicalize_parquet_batch(self.kind, &empty)?
            } else {
                empty
            };
            normalize_batch(
                self.graph,
                import_batch_operation(self.operation_uuid, self.sequence, first_batch),
                self.kind,
                &empty,
            )
            .map(|_| ())
        })();
        if checked.is_err() && self.check_cancelled().is_ok() {
            self.refusals.record(
                (
                    u8::from(self.kind == BulkInputKind::Edge),
                    self.sequence,
                    first_batch,
                ),
                first_rows,
            );
        }
        checked
    }

    fn admission_limit(&self) -> u64 {
        // The window is checked exactly, on the decoded batch, after it is
        // decoded; this limit refuses only what no accepted batch could be,
        // before the decode allocates it.
        self.window_bytes.saturating_add(self.window_bytes / 8)
    }

    /// Size a Parquet task's batches and reserve what its decode will hold.
    fn admit(
        &self,
        scan: &SourceScan,
        rows: u64,
        task: usize,
        first_batch: u64,
    ) -> Result<TaskAdmission, GfError> {
        let batch_rows = self.batch_rows as u64;
        let batches = rows.div_ceil(batch_rows);
        let count = BATCHES_PER_TASK.min(batches.saturating_sub(first_batch));
        let sizes = (0..count)
            .map(|offset| scan.batch_bytes(first_batch + offset))
            .collect::<Vec<_>>();
        let widest = sizes
            .iter()
            .map(|size| (*size).min(self.admission_limit()))
            .max()
            .unwrap_or(0);
        let first_row = first_batch * batch_rows;
        let last_row = ((first_batch + count) * batch_rows).min(rows);
        let pool = self
            .workspace
            .lock()
            .expect("workspace binding poisoned")
            .clone();
        let reservation = pool
            .as_ref()
            .map(|pool| {
                // The decompressed pages every column holds, the batch being
                // decoded and the copy that normalization hands the builder, and
                // the per-row state of normalizing it.
                let need = scan
                    .pages_resident(first_row, last_row)
                    .saturating_add(widest.saturating_mul(2))
                    .saturating_add(batch_rows.saturating_mul(NORMALIZATION_ROW_BYTES));
                pool.reserve(need, &format!("Parquet task {task}"), &|| {
                    self.check_cancelled()
                })
            })
            .transpose()?;
        Ok(TaskAdmission {
            sizes,
            _reservation: reservation,
        })
    }

    fn check_cancelled(&self) -> Result<(), GfError> {
        if self
            .cancellation
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(cancelled());
        }
        Ok(())
    }

    /// Refuse the batch at `offset` of a task if it would decode to more than
    /// the window allows, before it is decoded.
    fn admit_batch(
        &self,
        admission: &TaskAdmission,
        rows: u64,
        first_batch: u64,
        offset: u64,
    ) -> Result<(), GfError> {
        let Some(size) = usize::try_from(offset)
            .ok()
            .and_then(|offset| admission.sizes.get(offset))
        else {
            return Ok(());
        };
        if *size <= self.admission_limit() {
            return Ok(());
        }
        let index = first_batch + offset;
        let batch_rows = self.batch_rows as u64;
        self.refusals.record(
            (
                u8::from(self.kind == BulkInputKind::Edge),
                self.sequence,
                index,
            ),
            batch_rows.min(rows.saturating_sub(index * batch_rows)),
        );
        Err(super::limit(format!(
            "Parquet batch {index} would decode to {size} bytes, above the {}-byte batch window;              refused before it was decoded",
            self.window_bytes
        )))
    }
}

/// Bytes of per-row state normalizing a batch keeps beside the batch: the
/// identity and label it rebuilds for every row.
const NORMALIZATION_ROW_BYTES: u64 = 64;

/// What a task reserved and what its batches will decode to.
struct TaskAdmission {
    /// Arrow bytes of each of the task's batches, in order.
    sizes: Vec<u64>,
    _reservation: Option<graphforge_storage::SourceReservation>,
}

impl BulkBatchReader for SourceReader<'_> {
    fn schema_resident_bytes(&self) -> u64 {
        self.schema_bytes
    }
    fn retained_metadata_bytes(&self) -> u64 {
        self.schema_bytes.saturating_add(match &self.format {
            Format::Parquet { metadata, scan, .. } => {
                (metadata.metadata().memory_size() as u64).saturating_add(scan.resident_bytes())
            }
            Format::Arrow { plan } => plan.inventory_bytes,
        })
    }

    fn decoded_workspace_bytes(&self) -> u64 {
        self.decoding_bytes
    }

    fn bind_workspace(&self, workspace: &Arc<SourceWorkspace>) {
        *self.workspace.lock().expect("workspace binding poisoned") = Some(Arc::clone(workspace));
    }
    fn task_rows(&self, task: usize) -> usize {
        let first_batch = task as u64 * BATCHES_PER_TASK;
        let batch_rows = self.batch_rows as u64;
        let rows = match &self.format {
            Format::Parquet { rows, .. } => {
                (first_batch * batch_rows + BATCHES_PER_TASK * batch_rows).min(*rows)
                    - (first_batch * batch_rows).min(*rows)
            }
            Format::Arrow { plan } => {
                let first = usize::try_from(first_batch).unwrap_or(usize::MAX);
                plan.rows
                    .iter()
                    .skip(first)
                    .take(usize::try_from(BATCHES_PER_TASK).unwrap_or(0))
                    .sum()
            }
        };
        usize::try_from(rows).unwrap_or(0)
    }

    /// The identity column's bounds over the row groups the task reads, when
    /// the footer states them exactly: Parquet only, and only without nulls
    /// (a null identity is derived, so it is not in the column's range).
    fn uuid_bounds(&self, task: usize) -> Option<([u8; 16], [u8; 16])> {
        let Format::Parquet { metadata, rows, .. } = &self.format else {
            return None;
        };
        let task_rows = BATCHES_PER_TASK * self.batch_rows as u64;
        let start = task as u64 * task_rows;
        let end = (start + task_rows).min(*rows);
        let mut bounds: Option<([u8; 16], [u8; 16])> = None;
        let mut group_start = 0_u64;
        for group in metadata.metadata().row_groups() {
            let group_end = group_start + u64::try_from(group.num_rows()).ok()?;
            if group_end > start && group_start < end {
                let statistics = group.column(0).statistics()?;
                let (low, high) = exact_uuid_bounds(statistics)?;
                bounds =
                    Some(bounds.map_or((low, high), |(min, max)| (min.min(low), max.max(high))));
            }
            group_start = group_end;
        }
        bounds
    }

    fn read_task(
        &self,
        task: usize,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let first_batch = task as u64 * BATCHES_PER_TASK;
        self.validate_schema(first_batch)?;
        match &self.format {
            Format::Parquet {
                metadata,
                rows,
                scan,
            } => {
                if let Some(in_place) = &self.in_place {
                    let file = in_place.external.reopen()?;
                    #[cfg(test)]
                    super::external_source::pass_hook(
                        &in_place.external.path,
                        "opened",
                        task as u64,
                    );
                    let admission = self.admit(scan, *rows, task, first_batch)?;
                    let guard = file.try_clone().map_err(storage)?;
                    let input = ObservedFile::new(file, in_place.digest.clone())?;
                    self.decode_parquet(
                        input,
                        metadata,
                        *rows,
                        first_batch,
                        in_place,
                        &guard,
                        &admission,
                        sink,
                    )?;
                } else {
                    // A session an earlier version began holds its own copy.
                    let file = File::open(&self.path).map_err(storage)?;
                    let admission = self.admit(scan, *rows, task, first_batch)?;
                    let mut decoded = self.parquet_reader(file, metadata, *rows, first_batch)?;
                    let mut offset = 0_u64;
                    loop {
                        self.admit_batch(&admission, *rows, first_batch, offset)?;
                        let Some(batch) = decoded.next() else { break };
                        self.emit(first_batch + offset, batch.map_err(storage)?, sink)?;
                        offset += 1;
                    }
                }
            }
            Format::Arrow { plan } => {
                // Everything the checked reader allocates (its dictionaries,
                // then one block, its decoded arrays and its frame workspaces
                // per batch) was sized at plan time; reserve it, then read
                // each block checked against the plan, the file and this
                // reservation before it is decoded (#1918).
                let pool = self
                    .workspace
                    .lock()
                    .expect("workspace binding poisoned")
                    .clone();
                let _reservation = pool
                    .as_ref()
                    .map(|pool| {
                        pool.reserve(plan.decoding_bytes, &format!("Arrow task {task}"), &|| {
                            self.check_cancelled()
                        })
                    })
                    .transpose()?;
                let file = File::open(&self.path).map_err(storage)?;
                let mut reader = CheckedIpcReader::new(plan, file)?;
                reader.read_dictionaries()?;
                let total = plan.rows.len() as u64;
                for index in first_batch..(first_batch + BATCHES_PER_TASK).min(total) {
                    let batch =
                        reader.read_record_batch(usize::try_from(index).map_err(storage)?)?;
                    self.emit(index, batch, sink)?;
                }
            }
        }
        Ok(())
    }
}

/// Resident bytes an initial build may plan to use (see `memory_budget`).
///
/// This chooses between builds of the same bytes, never what is built: an
/// initial build whose estimate exceeds it uses bounded normalized-row scratch
/// workspace (ADR 0058). Registered-source decoding and normalization expansion
/// have a separate admission boundary under #1918.
pub(crate) fn bulk_build_memory_budget() -> Result<u64, GfError> {
    #[cfg(test)]
    if let Some(budget) = TEST_BUDGET.with(std::cell::Cell::get) {
        return Ok(budget);
    }
    super::memory_budget::bulk_build_memory_budget()
}

#[cfg(test)]
thread_local! {
    /// Lets a test force the plan-time routing decision.
    pub(crate) static TEST_BUDGET: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

pub(super) fn schema_owned_bytes(schema: &arrow::datatypes::Schema) -> u64 {
    (std::mem::size_of_val(schema)
        + schema
            .fields()
            .iter()
            .map(|field| field.size())
            .sum::<usize>()
        + schema
            .metadata()
            .iter()
            .map(|(key, value)| key.capacity() + value.capacity() + 64)
            .sum::<usize>()) as u64
}

/// The most compressed bytes any one task reads: the row groups its rows touch.
fn largest_task_bytes(metadata: &ArrowReaderMetadata, batch_rows: usize) -> u64 {
    let task_rows = BATCHES_PER_TASK * batch_rows as u64;
    let mut largest = 0_u64;
    let mut per_task = BTreeMap::<u64, u64>::new();
    let mut group_start = 0_u64;
    for group in metadata.metadata().row_groups() {
        let rows = u64::try_from(group.num_rows()).unwrap_or(0);
        if rows == 0 {
            continue;
        }
        let bytes = u64::try_from(group.compressed_size()).unwrap_or(0);
        for task in group_start / task_rows..=(group_start + rows - 1) / task_rows {
            let total = per_task.entry(task).or_default();
            *total = total.saturating_add(bytes);
            largest = largest.max(*total);
        }
        group_start += rows;
    }
    largest
}

/// Parsed footer metadata is at most this many times the footer's own bytes
/// (measured in `bulk_source::footer_tests`), and the footer is held beside it.
const FOOTER_PARSE_FACTOR: u64 = 12;

/// The budget the plan-time size limits are stated against, at least this much.
///
/// The bulk route's fixed footprint alone is 512 MiB, so below a gigabyte it
/// cannot take the build and `validate` stages it instead, which has its own
/// admission. The limits below describe the bulk route; they must not refuse an
/// input that the staged path takes.
const PLANNING_FLOOR_BYTES: u64 = 1 << 30;

fn require_footer_fits(footer_bytes: u64, budget: u64) -> Result<(), GfError> {
    let budget = budget.max(PLANNING_FLOOR_BYTES);
    let needed = footer_bytes.saturating_mul(FOOTER_PARSE_FACTOR + 1);
    if needed > budget / 4 {
        return Err(super::limit(format!(
            "a Parquet footer of {footer_bytes} bytes needs {needed} bytes to parse, above a quarter \
             of the {budget}-byte construction memory budget"
        )));
    }
    Ok(())
}

/// Plan one registered source from its footer (pass 0).
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the build's shared observers travel with the plan's inputs, and each source kind plans in one place"
)]
pub(super) fn plan<'a>(
    graph: &'a GraphForge,
    root: &Path,
    source: &SourceRecord,
    batch_rows: usize,
    operation_uuid: Uuid,
    cancellation: Option<&'a CancellationToken>,
    refusals: &'a Refusals,
    digests: &'a Digests,
    window_bytes: u64,
) -> Result<BulkSource<'a>, GfError> {
    let path = root.join("sources").join(&source.name);
    let kind = source.kind.input_kind();
    let mut in_place = None;
    let required = match kind {
        BulkInputKind::Node => 2,
        BulkInputKind::Edge => 4,
    };
    let mut pending_bytes = 0_u64;
    let (format, rows, columns, schema_bytes, decoding_bytes) = match source.kind {
        ImportSourceKind::ParquetNodes | ImportSourceKind::ParquetEdges => {
            let budget = bulk_build_memory_budget()?;
            let metadata = if let Some(external) = source.external.as_ref() {
                // The footer is read once through the digest, which keeps it.
                let digest = SourceDigest::new(external.size);
                let file = external.open_observed(&digest)?;
                let guard = file.try_clone().map_err(storage)?;
                let input = ObservedFile::new(file, digest.clone())?;
                // The footer is read whole and parsed into structures many times
                // its size; refuse one the budget cannot hold before reading it.
                require_footer_fits(external.footer_bytes(), budget)?;
                let metadata = ArrowReaderMetadata::load(&input, ArrowReaderOptions::new())
                    .map_err(|error| external.reclassify(&guard, storage(error)))?;
                // Reads ahead of the hashed prefix are held, so the bound on
                // them is resident workspace like any other: it never exceeds
                // a sixty-fourth of the budget, and the digest reads again what
                // it had to drop.
                pending_bytes = u64::try_from(external_source::pending_limit(
                    std::thread::available_parallelism().map_or(1, usize::from),
                    largest_task_bytes(&metadata, batch_rows),
                ))
                .unwrap_or(u64::MAX)
                .min(budget / 64)
                .max(1 << 20);
                digest.set_pending_limit(usize::try_from(pending_bytes).unwrap_or(usize::MAX));
                digests.register(source.sequence, external, &digest);
                in_place = Some(InPlace {
                    external: external.clone(),
                    digest,
                });
                metadata
            } else {
                // A session an earlier version began holds its own copy.
                let file = File::open(&path).map_err(storage)?;
                ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).map_err(storage)?
            };
            let rows =
                u64::try_from(metadata.metadata().file_metadata().num_rows()).map_err(storage)?;
            let columns = metadata.schema().fields().len();
            let schema_bytes = schema_owned_bytes(metadata.schema().as_ref());
            // Page headers and the values whose expansion they do not state.
            let scan_file = match &in_place {
                Some(held) => held.external.reopen()?,
                None => File::open(&path).map_err(storage)?,
            };
            let scan = SourceScan::build(
                scan_file,
                &metadata,
                batch_rows as u64,
                budget.max(PLANNING_FLOOR_BYTES),
                // A batch past the intake window plus an eighth is refused.
                window_bytes.saturating_add(window_bytes / 8),
                cancellation,
            )?;
            let decoding_bytes = scan.pages_resident_max().saturating_add(pending_bytes);
            (
                Format::Parquet {
                    metadata,
                    rows,
                    scan,
                },
                rows,
                columns,
                schema_bytes,
                decoding_bytes,
            )
        }
        ImportSourceKind::ArrowNodes | ImportSourceKind::ArrowEdges => {
            let sizing = Arc::new(bounded_ipc::ipc_plan(&path)?);
            let rows = sizing
                .rows
                .iter()
                .try_fold(0_u64, |total, rows| total.checked_add(*rows))
                .ok_or_else(|| {
                    super::limit("Arrow source row count exceeds construction capacity")
                })?;
            (
                Format::Arrow {
                    plan: Arc::clone(&sizing),
                },
                rows,
                sizing.columns,
                sizing.schema_bytes,
                sizing.decoding_bytes,
            )
        }
    };
    if columns < required {
        return Err(validation("Parquet import schema lacks required columns"));
    }
    let batches = match &format {
        Format::Parquet { .. } => rows.div_ceil(batch_rows as u64),
        Format::Arrow { plan } => plan.rows.len() as u64,
    };
    let decoded_bytes = match &format {
        // What the batches decode to, not what the footer says they are stored as.
        Format::Parquet { scan, .. } => scan.decoded_bytes(),
        Format::Arrow { .. } => fs::metadata(&path).map_err(storage)?.len(),
    };
    let tasks = usize::try_from(batches.div_ceil(BATCHES_PER_TASK)).map_err(storage)?;
    Ok(BulkSource {
        reader: Arc::new(SourceReader {
            graph,
            path,
            in_place,
            kind,
            operation_uuid,
            sequence: source.sequence,
            batch_rows,
            format,
            schema_bytes,
            decoding_bytes,
            window_bytes,
            workspace: Mutex::new(None),
            cancellation,
            refusals,
        }),
        tasks,
        rows,
        property_free: columns == required,
        decoded_bytes,
    })
}

#[cfg(test)]
mod bounds_tests {
    use parquet::data_type::FixedLenByteArray;
    use parquet::file::statistics::{Statistics, ValueStatistics};

    use super::exact_uuid_bounds;

    #[test]
    fn inexact_or_nullable_footer_bounds_require_sampling() {
        let bounds = |min_exact, max_exact, nulls| {
            Statistics::from(
                ValueStatistics::new(
                    Some(FixedLenByteArray::from(vec![1; 16])),
                    Some(FixedLenByteArray::from(vec![2; 16])),
                    None,
                    nulls,
                    false,
                )
                .with_min_is_exact(min_exact)
                .with_max_is_exact(max_exact),
            )
        };
        assert_eq!(
            exact_uuid_bounds(&bounds(true, true, Some(0))),
            Some(([1; 16], [2; 16]))
        );
        for (low, high, nulls) in [
            (false, true, Some(0)),
            (true, false, Some(0)),
            (false, false, Some(0)),
            (true, true, Some(1)),
            (true, true, None),
        ] {
            assert_eq!(exact_uuid_bounds(&bounds(low, high, nulls)), None);
        }
    }
}

#[cfg(test)]
mod footer_tests;

#[cfg(test)]
mod reservation_tests;
