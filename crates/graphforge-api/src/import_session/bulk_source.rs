//! Registered import files as bulk-builder sources (#1883).
//!
//! The builder reads each source in place, in parallel tasks. A task is a run
//! of whole construction batches, so batch boundaries, and therefore the
//! operation identity every batch normalizes under, match a sequential read of
//! the file.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use graphforge_core::GfError;
use graphforge_storage::{BulkBatchReader, BulkSource, SourceReservation, SourceWorkspace};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::schema::parquet_to_arrow_field_levels;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::reader::{ChunkReader, Length};
use uuid::Uuid;

use super::bounded_ipc::{self, CheckedIpcReader};
use super::external_source::{self, ExternalSource, ObservedFile, SourceDigest};
use super::inventory_budget::InventoryBudget;
use super::parquet_admission::SourceScan;
use super::parquet_reader::{OwnedBatchReader, PageFailures, TaskDecodeBudget};
use super::parquet_row_groups::OwnedRowGroups;
use super::parquet_windows::PhysicalBatchMap;
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
        arrow_inference_bytes: u64,
        /// What the source's pages hold and what its batches decode to (#1918).
        scan: Box<SourceScan>,
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
pub(super) struct Digests {
    sources: Mutex<Vec<(u64, ExternalSource, SourceDigest)>>,
    pending_budget: external_source::PendingDigestBudget,
    source_level_reservation: Mutex<Option<SourceReservation>>,
}

impl Default for Digests {
    fn default() -> Self {
        Self::for_build_budget(0, false)
    }
}

impl Digests {
    pub(super) fn for_build_budget(budget: u64, has_external_sources: bool) -> Self {
        let capacity = if has_external_sources {
            budget.min(1 << 30) / 64
        } else {
            0
        };
        Self {
            sources: Mutex::new(Vec::new()),
            pending_budget: external_source::PendingDigestBudget::new(
                usize::try_from(capacity).unwrap_or(usize::MAX),
            ),
            source_level_reservation: Mutex::new(None),
        }
    }

    pub(super) fn pending_budget_bytes(&self) -> u64 {
        u64::try_from(self.pending_budget.capacity_bytes()).unwrap_or(u64::MAX)
    }

    fn hold_source_level_reservation(
        &self,
        workspace: &Arc<SourceWorkspace>,
        cancelled: &dyn Fn() -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let capacity = self.pending_budget_bytes();
        if capacity == 0 {
            return Ok(());
        }
        let mut reservation = self
            .source_level_reservation
            .lock()
            .expect("source digest reservation lock poisoned");
        if reservation.is_none() {
            *reservation = Some(workspace.reserve(
                capacity,
                "shared source-level digest workspace",
                cancelled,
            )?);
        }
        Ok(())
    }

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
    digests: &'a Digests,
    path: PathBuf,
    /// The registered Parquet file this reader decodes where it is. `None` for
    /// an Arrow source, which the session owns.
    in_place: Option<InPlace>,
    kind: BulkInputKind,
    operation_uuid: Uuid,
    sequence: u64,
    batch_rows: usize,
    format: Format,
    schema_bytes: u64,
    decoding_bytes: u64,
    max_task_workspace_bytes: u64,
    source_level_workspace_bytes: u64,
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
        first_ordinal: u64,
        batch: RecordBatch,
        seen: &mut HashSet<Uuid>,
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
                // The decoded piece's buffers may hold slack; the builder is
                // handed exactly its values.
                Format::Parquet { .. } => {
                    canonicalize_parquet_batch(self.kind, &super::piece_buffers::exact(batch)?)?
                }
                Format::Arrow { .. } => batch,
            };
            let operation = import_batch_operation(self.operation_uuid, self.sequence, index);
            let normalized = match (&self.format, self.kind) {
                (Format::Parquet { .. }, BulkInputKind::Node) => self
                    .graph
                    .normalize_import_node_chunk_at_with_seen(
                        operation,
                        &batch,
                        first_ordinal,
                        seen,
                    )
                    .map_err(|error| validation(error.to_string()))?,
                (Format::Parquet { .. }, BulkInputKind::Edge) => self
                    .graph
                    .normalize_import_edge_chunk_at_with_seen(
                        operation,
                        &batch,
                        first_ordinal,
                        seen,
                    )
                    .map_err(|error| validation(error.to_string()))?,
                (Format::Arrow { .. }, _) => {
                    normalize_batch(self.graph, operation, self.kind, &batch)?
                }
            };
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
    fn parquet_reader<T>(
        &self,
        input: T,
        metadata: &ArrowReaderMetadata,
        rows: u64,
        first_batch: u64,
        admission: &TaskAdmission,
    ) -> Result<OwnedBatchReader, GfError>
    where
        T: parquet::file::reader::ChunkReader + 'static,
        T::T: Read + Send + 'static,
    {
        let batch_rows = self.batch_rows as u64;
        let start = first_batch * batch_rows;
        let end = (start + BATCHES_PER_TASK * batch_rows).min(rows);
        let physical_rows = admission.physical_batch_rows;
        let mut groups = Vec::new();
        groups
            .try_reserve_exact(admission.selected_group_count)
            .map_err(|_| super::limit("Cannot allocate admitted Parquet row-group indices"))?;
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
        if groups.len() != admission.selected_group_count {
            return Err(storage(
                "Parquet task row-group selection differs from its admission",
            ));
        }
        let covered = groups
            .iter()
            .map(|index| {
                u64::try_from(metadata.metadata().row_group(*index).num_rows()).unwrap_or(0)
            })
            .sum::<u64>();
        let mut selection = None;
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
            selection = Some(RowSelection::from(selectors));
        }
        let failures = PageFailures::new();
        let decode_budget = Arc::clone(&admission.decode_budget);
        let cancellation = self.cancellation.cloned();
        let parquet_metadata = Arc::clone(metadata.metadata());
        let preflight_metadata = Arc::clone(&parquet_metadata);
        let row_groups = OwnedRowGroups::new_bounded(
            Arc::new(input),
            parquet_metadata,
            Arc::new(groups),
            move |group_index, column_index| {
                let group = preflight_metadata.row_group(group_index);
                let column = group.column(column_index);
                let rows = u64::try_from(group.num_rows()).map_err(storage)?;
                super::parquet_sizing::runtime_preflight(
                    column,
                    rows,
                    Arc::clone(&decode_budget),
                    cancellation.clone(),
                    |_, _| Ok(()),
                )
            },
            self.cancellation.cloned(),
            failures.clone(),
            Arc::clone(&admission.decode_budget),
        )?;
        // The admitted Arrow schema carries the file's type hints (large
        // offsets, dictionaries); the native reader must decode to it, or its
        // fields differ from the schema every batch is checked against.
        let levels = parquet_to_arrow_field_levels(
            metadata.metadata().file_metadata().schema_descr(),
            ProjectionMask::all(),
            Some(metadata.schema().fields()),
        )
        .map_err(storage)?;
        let native =
            parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new_with_row_groups(
                &levels,
                &row_groups,
                usize::try_from(physical_rows).map_err(storage)?,
                selection,
            )
            .map_err(storage)?;
        OwnedBatchReader::new(native, metadata.schema().clone(), failures)
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
            .parquet_reader(input, metadata, rows, first_batch, admission)
            .map_err(changed)?;
        let mut offset = 0_u64;
        let mut seen = HashSet::new();
        let mut seen_batch = None;
        loop {
            // The next batch is sized before the reader decodes it.
            self.admit_batch(admission, rows, first_batch, offset)?;
            let Some(batch) = reader.next() else { break };
            let (logical_batch, ordinal, _) =
                self.physical_identity(rows, first_batch, offset, admission)?;
            if seen_batch != Some(logical_batch) {
                seen.clear();
                seen_batch = Some(logical_batch);
            }
            #[cfg(test)]
            super::external_source::pass_hook(&in_place.external.path, "batch", logical_batch);
            // The source can change between any two batches.
            in_place.external.check(guard)?;
            let batch = batch.map_err(|error| changed(storage(error)))?;
            self.emit(logical_batch, ordinal, batch, &mut seen, sink)?;
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
        self.window_bytes
    }

    /// Size a Parquet task's batches and reserve what its decode will hold.
    fn admit(
        &self,
        scan: &SourceScan,
        metadata: &ArrowReaderMetadata,
        rows: u64,
        task: usize,
        first_batch: u64,
    ) -> Result<TaskAdmission, GfError> {
        let batch_rows = self.batch_rows as u64;
        let batch_workspace = parquet_task_workspace(
            scan,
            metadata,
            rows,
            batch_rows,
            self.kind,
            first_batch,
            self.window_bytes,
        )
        .map_err(|error| {
            error.record(
                self.refusals,
                self.kind,
                self.sequence,
                first_batch,
                batch_rows.min(rows.saturating_sub(first_batch * batch_rows)),
            )
        })?;
        let decode_budget = TaskDecodeBudget::new(batch_workspace.page_workspace);
        let pool = self
            .workspace
            .lock()
            .expect("workspace binding poisoned")
            .clone();
        let reservation = pool
            .as_ref()
            .map(|pool| {
                // The decompressed pages every column holds, the batch being
                // decoded and the copy handed to the builder, plus normalization
                // row vectors, edge endpoint candidates, and the logical-batch
                // duplicate set retained across physical pieces.
                pool.reserve(
                    batch_workspace.reservation_bytes,
                    &format!("Parquet task {task}"),
                    &|| self.check_cancelled(),
                )
            })
            .transpose()?;
        Ok(TaskAdmission {
            decode_budget,
            selected_group_count: batch_workspace.selected_group_count,
            physical_batch_rows: batch_workspace.physical_batch_rows,
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

    fn physical_identity(
        &self,
        rows: u64,
        first_batch: u64,
        offset: u64,
        admission: &TaskAdmission,
    ) -> Result<(u64, u64, u64), GfError> {
        let logical_rows = self.batch_rows as u64;
        let physical_index = first_batch
            .checked_mul(logical_rows)
            .and_then(|row| row.checked_div(admission.physical_batch_rows))
            .and_then(|index| index.checked_add(offset))
            .ok_or_else(|| storage("Parquet physical batch index overflows"))?;
        PhysicalBatchMap::new(rows, logical_rows, admission.physical_batch_rows)?
            .identity(physical_index)
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
        let batch_rows = self.batch_rows as u64;
        let physical_rows = admission.physical_batch_rows;
        let physical_index = first_batch
            .saturating_mul(batch_rows)
            .checked_div(physical_rows)
            .unwrap_or(0)
            .saturating_add(offset);
        let oversized = match &self.format {
            Format::Parquet { scan, .. } => {
                scan.physical_batch_exceeds(physical_index, self.admission_limit())
            }
            Format::Arrow { .. } => return Ok(()),
        };
        if !oversized {
            return Ok(());
        }
        let (index, _, _) =
            PhysicalBatchMap::new(rows, batch_rows, physical_rows)?.identity(physical_index)?;
        self.refusals.record(
            (
                u8::from(self.kind == BulkInputKind::Edge),
                self.sequence,
                index,
            ),
            batch_rows.min(rows.saturating_sub(index * batch_rows)),
        );
        Err(super::limit(format!(
            "Parquet batch {index} exceeds the {}-byte batch window; refused before it was decoded",
            self.window_bytes
        )))
    }
}

/// Import normalization retains duplicate IDs for the whole logical batch,
/// even when Parquet supplies several physical pieces for that batch.
const SEEN_UUID_PEAK_BYTES_PER_ROW: u64 = 64;

#[derive(Clone, Copy)]
struct TaskReservationSizing {
    page_workspace: u64,
    selected_group_count: usize,
    widest_batch: u64,
    physical_rows: u64,
    logical_rows: u64,
    kind: BulkInputKind,
    native_auxiliary: u64,
    property_columns_bytes: u64,
}

fn task_reservation_bytes(sizing: TaskReservationSizing) -> Result<u64, GfError> {
    let TaskReservationSizing {
        page_workspace,
        selected_group_count,
        widest_batch,
        physical_rows,
        logical_rows,
        kind,
        native_auxiliary,
        property_columns_bytes,
    } = sizing;
    let group_indices = u64::try_from(selected_group_count)
        .map_err(storage)?
        .checked_mul(u64::try_from(std::mem::size_of::<usize>()).map_err(storage)?)
        .and_then(|bytes| {
            bytes.checked_add(
                u64::try_from(std::mem::size_of::<Vec<usize>>() + 2 * std::mem::size_of::<usize>())
                    .ok()?,
            )
        })
        .ok_or_else(|| super::limit("Parquet row-group index workspace overflows"))?;
    let physical_row_bytes = match kind {
        BulkInputKind::Node => 3_u64
            .checked_mul(u64::try_from(std::mem::size_of::<crate::BulkNodeRow>()).map_err(storage)?)
            .ok_or_else(|| super::limit("Parquet node normalization workspace overflows"))?,
        BulkInputKind::Edge => 3_u64
            .checked_mul(u64::try_from(std::mem::size_of::<crate::BulkEdgeRow>()).map_err(storage)?)
            .and_then(|bytes| {
                bytes.checked_add(
                    6_u64.checked_mul(
                        u64::try_from(std::mem::size_of::<crate::BulkNodeRow>()).ok()?,
                    )?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    4_u64.checked_mul(u64::try_from(std::mem::size_of::<Uuid>()).ok()?)?,
                )
            })
            .ok_or_else(|| super::limit("Parquet edge normalization workspace overflows"))?,
    };
    let normalization_rows = physical_rows
        .checked_mul(physical_row_bytes)
        .and_then(|bytes| {
            bytes.checked_add(logical_rows.checked_mul(SEEN_UUID_PEAK_BYTES_PER_ROW)?)
        })
        .ok_or_else(|| super::limit("Parquet normalization workspace overflows"))?;
    page_workspace
        .checked_add(native_auxiliary)
        .and_then(|bytes| bytes.checked_add(property_columns_bytes))
        .and_then(|bytes| bytes.checked_add(group_indices))
        // Source Arrow arrays, copied labels/identity arrays, and a one-cell
        // property conversion may coexist while import validation runs.
        .and_then(|bytes| bytes.checked_add(widest_batch.checked_mul(3)?))
        .and_then(|bytes| bytes.checked_add(normalization_rows))
        .ok_or_else(|| super::limit("Parquet task reservation overflows"))
}

/// What a task reserved and what its batches will decode to.
struct TaskAdmission {
    /// Shared bounded credit for owned Parquet pages and their preflight.
    decode_budget: Arc<TaskDecodeBudget>,
    /// Number of selected row groups; their single retained index vector is
    /// included in the source-pool reservation.
    selected_group_count: usize,
    physical_batch_rows: u64,
    _reservation: Option<graphforge_storage::SourceReservation>,
}

/// Why a task's workspace could not be planned: a batch refused before it was
/// decoded, with the rows the refusal rejects, or a failure of the planning
/// itself.
enum TaskWorkspaceError {
    Refused {
        batch: u64,
        rows: u64,
        error: GfError,
    },
    Failed(GfError),
}

impl From<GfError> for TaskWorkspaceError {
    fn from(error: GfError) -> Self {
        Self::Failed(error)
    }
}

impl TaskWorkspaceError {
    /// Record what the error rejects and return it: the refused batch's own rows,
    /// or the first batch of the task (`fallback_batch`, `fallback_rows`) when a
    /// resource limit stopped planning before a batch was named. Other failures
    /// reject nothing.
    fn record(
        self,
        refusals: &Refusals,
        kind: BulkInputKind,
        sequence: u64,
        fallback_batch: u64,
        fallback_rows: u64,
    ) -> GfError {
        let edge = u8::from(kind == BulkInputKind::Edge);
        match self {
            Self::Refused { batch, rows, error } => {
                refusals.record((edge, sequence, batch), rows);
                error
            }
            Self::Failed(error) => {
                if is_resource_limit(&error) {
                    refusals.record((edge, sequence, fallback_batch), fallback_rows);
                }
                error
            }
        }
    }
}

fn is_resource_limit(error: &GfError) -> bool {
    matches!(
        error,
        GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            ..
        }
    )
}

struct ParquetTaskWorkspace {
    selected_group_count: usize,
    physical_batch_rows: u64,
    page_workspace: u64,
    reservation_bytes: u64,
}

fn parquet_task_workspace(
    scan: &SourceScan,
    metadata: &ArrowReaderMetadata,
    rows: u64,
    batch_rows: u64,
    kind: BulkInputKind,
    first_batch: u64,
    window_bytes: u64,
) -> Result<ParquetTaskWorkspace, TaskWorkspaceError> {
    // The builder charges a batch what its buffers hold against this window,
    // so no admitted piece may be bounded above it.
    let admission_limit = window_bytes;
    let logical_batches = rows.div_ceil(batch_rows);
    let logical_count = BATCHES_PER_TASK.min(logical_batches.saturating_sub(first_batch));
    let first_row = first_batch
        .checked_mul(batch_rows)
        .ok_or_else(|| super::limit("Parquet task row start overflows"))?;
    let last_row = first_batch
        .checked_add(logical_count)
        .and_then(|batch| batch.checked_mul(batch_rows))
        .ok_or_else(|| super::limit("Parquet task row end overflows"))?
        .min(rows);
    let physical_batch_rows = scan.physical_batch_rows();
    let map = PhysicalBatchMap::new(rows, batch_rows, physical_batch_rows)?;
    let first_physical = first_row / physical_batch_rows;
    let physical_count = last_row
        .saturating_sub(first_row)
        .div_ceil(physical_batch_rows);
    // A piece past the window, the growth of its decoded buffers included, is
    // refused before the task runs, since no piece size can hold its widest
    // row; the refusal names the logical batch that holds the piece.
    if let Some(piece) = (first_physical..first_physical.saturating_add(physical_count))
        .find(|piece| scan.physical_batch_bytes(*piece) > admission_limit)
    {
        let size = scan.physical_batch_bytes(piece);
        let batch = piece.saturating_mul(physical_batch_rows) / batch_rows;
        return Err(TaskWorkspaceError::Refused {
            batch,
            rows: batch_rows.min(rows.saturating_sub(batch.saturating_mul(batch_rows))),
            error: super::limit(format!(
                "Parquet batch {batch} would decode to {size} bytes, above the \
                 {window_bytes}-byte batch window; refused before it was decoded"
            )),
        });
    }
    // The reservation holds the reader's buffers at their capacity and the
    // exact copy the builder is handed, side by side.
    let widest = scan
        .physical_batch_max_capacity_bytes(first_physical, physical_count)
        .saturating_add(scan.physical_batch_max_bytes(first_physical, physical_count));
    let (selected_group_count, validator_workspace) =
        super::parquet_sizing::runtime_scratch_capacity(
            metadata.metadata(),
            scan,
            first_row,
            last_row,
        )?;
    let page_workspace = scan
        .pages_resident(first_row, last_row)
        .checked_add(validator_workspace)
        .ok_or_else(|| super::limit("Parquet task page workspace overflows"))?;
    let required_fields = match kind {
        BulkInputKind::Node => 2,
        BulkInputKind::Edge => 4,
    };
    let property_fields = metadata
        .schema()
        .fields()
        .len()
        .saturating_sub(required_fields);
    let property_columns_bytes = u64::try_from(property_fields)
        .map_err(storage)?
        .checked_mul(
            u64::try_from(std::mem::size_of::<(
                &arrow::datatypes::Field,
                &arrow::array::ArrayRef,
            )>())
            .map_err(storage)?,
        )
        .ok_or_else(|| super::limit("Parquet property-column workspace overflows"))?;
    let reservation_bytes = task_reservation_bytes(TaskReservationSizing {
        page_workspace,
        selected_group_count,
        widest_batch: widest,
        physical_rows: physical_batch_rows,
        logical_rows: batch_rows,
        kind,
        native_auxiliary: scan.native_decoder_auxiliary(first_row, last_row),
        property_columns_bytes,
    })?;
    Ok(ParquetTaskWorkspace {
        selected_group_count,
        physical_batch_rows: map.physical_rows(),
        page_workspace,
        reservation_bytes,
    })
}

fn max_parquet_task_workspace(
    scan: &SourceScan,
    metadata: &ArrowReaderMetadata,
    rows: u64,
    batch_rows: usize,
    kind: BulkInputKind,
    window_bytes: u64,
) -> Result<u64, TaskWorkspaceError> {
    let batch_rows = u64::try_from(batch_rows).map_err(storage)?.max(1);
    let tasks = rows.div_ceil(batch_rows.saturating_mul(BATCHES_PER_TASK));
    let mut maximum = 0;
    for task in 0..tasks {
        let first_batch = task
            .checked_mul(BATCHES_PER_TASK)
            .ok_or_else(|| super::limit("Parquet task batch start overflows"))?;
        maximum = maximum.max(
            parquet_task_workspace(
                scan,
                metadata,
                rows,
                batch_rows,
                kind,
                first_batch,
                window_bytes,
            )?
            .reservation_bytes,
        );
    }
    Ok(maximum)
}

impl BulkBatchReader for SourceReader<'_> {
    fn schema_resident_bytes(&self) -> u64 {
        self.schema_bytes
    }
    fn retained_metadata_bytes(&self) -> u64 {
        self.schema_bytes.saturating_add(match &self.format {
            Format::Parquet {
                metadata,
                scan,
                arrow_inference_bytes,
                ..
            } => (metadata.metadata().memory_size() as u64)
                .saturating_add(scan.resident_bytes())
                .saturating_add(*arrow_inference_bytes),
            Format::Arrow { plan } => plan.inventory_bytes,
        })
    }

    fn decoded_workspace_bytes(&self) -> u64 {
        self.decoding_bytes
    }

    fn max_task_workspace_bytes(&self) -> u64 {
        self.max_task_workspace_bytes
    }

    fn source_level_workspace_bytes(&self) -> u64 {
        self.source_level_workspace_bytes
    }

    fn bind_workspace(&self, workspace: &Arc<SourceWorkspace>) -> Result<(), GfError> {
        if self.source_level_workspace_bytes > 0 {
            self.digests
                .hold_source_level_reservation(workspace, &|| self.check_cancelled())?;
        }
        *self.workspace.lock().expect("workspace binding poisoned") = Some(Arc::clone(workspace));
        Ok(())
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
                ..
            } => {
                let in_place = self
                    .in_place
                    .as_ref()
                    .ok_or_else(|| storage("a Parquet source is read where it was registered"))?;
                let file = in_place.external.reopen()?;
                #[cfg(test)]
                super::external_source::pass_hook(&in_place.external.path, "opened", task as u64);
                let admission = self.admit(scan, metadata, *rows, task, first_batch)?;
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
                    self.emit(index, 0, batch, &mut HashSet::new(), sink)?;
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

/// A single owned Parquet footer snapshot exposed at its original file offsets
/// to parquet's metadata reader. The compact accounting and native parser both
/// consume these same bytes.
struct FooterWindow {
    file_len: u64,
    start: u64,
    bytes: Bytes,
}

impl Length for FooterWindow {
    fn len(&self) -> u64 {
        self.file_len
    }
}

impl ChunkReader for FooterWindow {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let offset = start.checked_sub(self.start).ok_or_else(|| {
            ParquetError::EOF("Parquet metadata read is outside the admitted footer".into())
        })?;
        let offset = usize::try_from(offset).map_err(|_| {
            ParquetError::EOF("Parquet metadata offset is not representable".into())
        })?;
        if offset > self.bytes.len() {
            return Err(ParquetError::EOF(
                "Parquet metadata read is outside the admitted footer".into(),
            ));
        }
        Ok(Cursor::new(self.bytes.slice(offset..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let offset = start.checked_sub(self.start).ok_or_else(|| {
            ParquetError::EOF("Parquet metadata read is outside the admitted footer".into())
        })?;
        let offset = usize::try_from(offset).map_err(|_| {
            ParquetError::EOF("Parquet metadata offset is not representable".into())
        })?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| ParquetError::EOF("Parquet metadata range overflows".into()))?;
        if end > self.bytes.len() {
            return Err(ParquetError::EOF(
                "Parquet metadata read is outside the admitted footer".into(),
            ));
        }
        Ok(self.bytes.slice(offset..end))
    }
}

fn load_admitted_parquet_metadata<T: ChunkReader>(
    input: &T,
    capacity: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<(ArrowReaderMetadata, u64), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    let file_len = input.len();
    if file_len < 8 {
        return Err(storage("Parquet source is too short for its footer"));
    }

    let mut budget = InventoryBudget::new(capacity);
    budget.admit(8, "the Parquet footer tail")?;
    let tail = input.get_bytes(file_len - 8, 8).map_err(storage)?;
    let magic = &tail[4..];
    if magic != b"PAR1" && magic != b"PARE" {
        return Err(storage("Parquet source has an invalid footer magic"));
    }
    let metadata_len = u64::from(u32::from_le_bytes(
        tail[..4]
            .try_into()
            .map_err(|_| storage("Parquet footer length is malformed"))?,
    ));
    let footer_len = metadata_len
        .checked_add(8)
        .ok_or_else(|| super::limit("Parquet footer size overflows"))?;
    if footer_len > file_len {
        return Err(storage("Parquet footer extends before its source"));
    }
    budget.admit(metadata_len, "the Parquet footer snapshot")?;
    let footer_start = file_len - footer_len;
    let footer_bytes = input
        .get_bytes(footer_start, usize::try_from(footer_len).map_err(storage)?)
        .map_err(storage)?;
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }

    let footer_body_len = usize::try_from(metadata_len).map_err(storage)?;
    let window = FooterWindow {
        file_len,
        start: footer_start,
        bytes: footer_bytes,
    };
    let footer_body = window
        .bytes
        .get(..footer_body_len)
        .ok_or_else(|| storage("Parquet footer snapshot is truncated"))?;
    let footer_facts =
        super::parquet_footer_counts::preflight(footer_body, budget.remaining(), cancellation)?;
    let footer_peak = footer_facts.peak_bytes()?;
    let footer_retained = footer_facts.retained_bytes()?;
    budget.admit(footer_peak, "native Parquet footer metadata")?;

    // The native thrift reader constructs the nested Type tree and schema
    // descriptor while decoding the footer. Validate and reserve that tree
    // before entering the native parser, while the footer bytes and footer
    // metadata envelope are still included in the same peak.
    let schema_facts = super::parquet_schema_envelope::preflight(
        footer_body,
        &footer_facts,
        &mut budget,
        cancellation,
    )?;
    budget.admit(
        schema_facts.native_request_bytes,
        "the native Parquet schema",
    )?;

    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&window)
        .map_err(storage)?;
    budget.release(footer_peak.saturating_sub(footer_retained));
    let arrow_envelope = super::parquet_arrow_admission::preflight(
        &metadata,
        schema_facts,
        budget.remaining(),
        cancellation,
    )?;
    budget.admit(
        arrow_envelope.peak_request_bytes,
        "inferred Arrow schema and Parquet fields",
    )?;
    let metadata = ArrowReaderMetadata::try_new(Arc::new(metadata), ArrowReaderOptions::new())
        .map_err(storage)?;
    budget.release(
        arrow_envelope
            .peak_request_bytes
            .saturating_sub(arrow_envelope.retained_request_bytes),
    );
    Ok((metadata, arrow_envelope.retained_request_bytes))
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
pub(super) const PLANNING_FLOOR_BYTES: u64 = 1 << 30;

fn require_footer_fits(footer_bytes: u64, budget: u64) -> Result<(), GfError> {
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
    planning_budget: u64,
) -> Result<BulkSource<'a>, GfError> {
    let path = root.join("sources").join(&source.name);
    let kind = source.kind.input_kind();
    let mut in_place = None;
    let required = match kind {
        BulkInputKind::Node => 2,
        BulkInputKind::Edge => 4,
    };
    let mut source_level_workspace_bytes = 0_u64;
    let (format, rows, columns, schema_bytes, decoding_bytes, max_task_workspace_bytes) =
        match source.kind {
            ImportSourceKind::ParquetNodes | ImportSourceKind::ParquetEdges => {
                let budget = planning_budget;
                let external = source
                    .external
                    .as_ref()
                    .ok_or_else(|| storage("a Parquet source is registered where it stays"))?;
                require_footer_fits(external.footer_bytes(), budget)?;
                // Footer verification precedes the decoder's workspace
                // reservation. Its read arrives ahead of the hashed prefix,
                // so the digest holds it under the shared pending bound
                // until the decode's own reads reach it: every byte is
                // hashed once, and `finish` re-reads only what the decode
                // never asks for.
                let digest = SourceDigest::new(external.size);
                digest.attach_pending_budget(digests.pending_budget.clone());
                let file = external.open_observed(&digest)?;
                let guard = file.try_clone().map_err(storage)?;
                let input = ObservedFile::new(file, digest.clone())?;
                // The footer is read whole and parsed into structures many times
                // its size; refuse one the budget cannot hold before reading it.
                let (metadata, arrow_inference_bytes) =
                    load_admitted_parquet_metadata(&input, budget, cancellation)
                        .map_err(|error| external.reclassify(&guard, error))?;
                // Reads ahead of the hashed prefix are held, so the bound on
                // them is resident workspace like any other: it never exceeds
                // a sixty-fourth of the budget, and the digest reads again what
                // it had to drop.
                let pending_bytes = u64::try_from(external_source::pending_limit(
                    std::thread::available_parallelism().map_or(1, usize::from),
                    largest_task_bytes(&metadata, batch_rows),
                ))
                .unwrap_or(u64::MAX)
                .min(digests.pending_budget_bytes());
                source_level_workspace_bytes = digests.pending_budget_bytes();
                digest.set_pending_limit(usize::try_from(pending_bytes).unwrap_or(usize::MAX));
                digests.register(source.sequence, external, &digest);
                in_place = Some(InPlace {
                    external: external.clone(),
                    digest,
                });
                let rows = u64::try_from(metadata.metadata().file_metadata().num_rows())
                    .map_err(storage)?;
                let columns = metadata.schema().fields().len();
                let schema_bytes = schema_owned_bytes(metadata.schema().as_ref());
                let retained_metadata_bytes = (metadata.metadata().memory_size() as u64)
                    .saturating_add(schema_bytes)
                    .saturating_add(arrow_inference_bytes);
                let scan_budget = budget.saturating_sub(retained_metadata_bytes);
                if scan_budget == 0 {
                    return Err(super::limit(
                        "Parquet footer and schema leave no admitted workspace for page inventory",
                    ));
                }
                // Page headers and the values whose expansion they do not state.
                let scan_file = external.reopen()?;
                let task_batch_rows = u64::try_from(batch_rows).unwrap_or(1).max(1);
                // A source refused while its pages are inventoried rejects its
                // first batch; a failure of the inventory rejects nothing.
                let scan = SourceScan::build(
                    &scan_file,
                    &metadata,
                    batch_rows as u64,
                    scan_budget,
                    // Pieces fit the intake window; a row past it is refused.
                    window_bytes,
                    cancellation,
                )
                .inspect_err(|error| {
                    if is_resource_limit(error) {
                        refusals.record(
                            (u8::from(kind == BulkInputKind::Edge), source.sequence, 0),
                            task_batch_rows.min(rows),
                        );
                    }
                })?;
                let max_task_workspace_bytes = max_parquet_task_workspace(
                    &scan,
                    &metadata,
                    rows,
                    batch_rows,
                    kind,
                    window_bytes,
                )
                .map_err(|error| {
                    error.record(
                        refusals,
                        kind,
                        source.sequence,
                        0,
                        task_batch_rows.min(rows),
                    )
                })?;
                let decoding_bytes = scan.pages_resident_max();
                (
                    Format::Parquet {
                        metadata,
                        rows,
                        arrow_inference_bytes,
                        scan: Box::new(scan),
                    },
                    rows,
                    columns,
                    schema_bytes,
                    decoding_bytes,
                    max_task_workspace_bytes,
                )
            }
            ImportSourceKind::ArrowNodes | ImportSourceKind::ArrowEdges => {
                let sizing = Arc::new(bounded_ipc::ipc_plan_with_budget(&path, planning_budget)?);
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
                    0,
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
            digests,
            path,
            in_place,
            kind,
            operation_uuid,
            sequence: source.sequence,
            batch_rows,
            format,
            schema_bytes,
            decoding_bytes,
            max_task_workspace_bytes,
            source_level_workspace_bytes,
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

    use super::{BulkInputKind, TaskReservationSizing, exact_uuid_bounds, task_reservation_bytes};

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

    #[test]
    fn many_small_row_groups_are_charged_for_the_retained_index_vector() {
        let groups = 4096;
        let baseline = task_reservation_bytes(TaskReservationSizing {
            page_workspace: 1,
            selected_group_count: 0,
            widest_batch: 0,
            physical_rows: 0,
            logical_rows: 0,
            kind: BulkInputKind::Node,
            native_auxiliary: 0,
            property_columns_bytes: 0,
        })
        .unwrap();
        let admitted = task_reservation_bytes(TaskReservationSizing {
            page_workspace: 1,
            selected_group_count: groups,
            widest_batch: 0,
            physical_rows: 0,
            logical_rows: 0,
            kind: BulkInputKind::Node,
            native_auxiliary: 0,
            property_columns_bytes: 0,
        })
        .unwrap();
        let index_capacity =
            u64::try_from(groups).unwrap() * u64::try_from(std::mem::size_of::<usize>()).unwrap();
        // The fixed Vec/header overhead is present in both totals; their
        // difference isolates the retained group's index capacity.
        assert_eq!(admitted - baseline, index_capacity);
    }

    #[test]
    fn edge_and_logical_duplicate_workspaces_are_charged_across_physical_pieces() {
        let sizing =
            |kind, logical_rows, native_auxiliary, property_columns_bytes| TaskReservationSizing {
                page_workspace: 1,
                selected_group_count: 0,
                widest_batch: 0,
                physical_rows: 2,
                logical_rows,
                kind,
                native_auxiliary,
                property_columns_bytes,
            };
        let node = task_reservation_bytes(sizing(BulkInputKind::Node, 8, 0, 0)).unwrap();
        let edge = task_reservation_bytes(sizing(BulkInputKind::Edge, 8, 0, 0)).unwrap();
        let edge_without_seen =
            task_reservation_bytes(sizing(BulkInputKind::Edge, 0, 0, 0)).unwrap();
        let edge_with_native_aux =
            task_reservation_bytes(sizing(BulkInputKind::Edge, 0, 4_096, 0)).unwrap();
        let edge_with_property_refs =
            task_reservation_bytes(sizing(BulkInputKind::Edge, 0, 0, 16_000)).unwrap();

        assert!(edge > node);
        assert_eq!(
            edge - edge_without_seen,
            8 * super::SEEN_UUID_PEAK_BYTES_PER_ROW
        );
        assert_eq!(edge_with_native_aux - edge_without_seen, 4_096);
        assert_eq!(edge_with_property_refs - edge_without_seen, 16_000);
    }
}

#[cfg(test)]
mod footer_tests;

#[cfg(test)]
mod reservation_tests;
