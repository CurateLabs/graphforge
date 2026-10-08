//! Registered import files as bulk-builder sources (#1883).
//!
//! The builder reads each source in place, in parallel tasks. A task is a run
//! of whole construction batches, so batch boundaries, and therefore the
//! operation identity every batch normalizes under, match a sequential read of
//! the file.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::ipc::reader::FileReader as ArrowFileReader;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_storage::concurrency_attribution::ObservedSha256;
use graphforge_storage::{BulkBatchReader, BulkSource};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use sha2::Digest as _;
use uuid::Uuid;

use super::external_source::{self, ExternalSource, SourceChange, source_changed};
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
    },
    /// Rows of every record batch, from the IPC footer's message headers.
    Arrow { batch_rows: Vec<u64> },
}

/// Footer-only sizing precedes FileReader: that reader eagerly decodes every
/// dictionary, so constructing it is already a payload allocation.
struct IpcPlan {
    rows: Vec<u64>,
    columns: usize,
    schema_bytes: u64,
    decoding_bytes: u64,
}

#[allow(clippy::too_many_lines)]
fn ipc_plan(path: &Path) -> Result<IpcPlan, GfError> {
    use std::io::{Read as _, Seek as _, SeekFrom};
    let budget = bulk_build_memory_budget()?;
    let mut file = File::open(path).map_err(storage)?;
    let length = file.metadata().map_err(storage)?.len();
    if length < 10 {
        return Err(storage("Arrow source is too short for an IPC footer"));
    }
    file.seek(SeekFrom::Start(length - 10)).map_err(storage)?;
    let mut tail = [0; 10];
    file.read_exact(&mut tail).map_err(storage)?;
    let footer_length = u64::try_from(i32::from_le_bytes(tail[..4].try_into().expect("4 bytes")))
        .map_err(|_| storage("Arrow footer length is negative"))?;
    if &tail[4..] != b"ARROW1" || footer_length + 10 > length {
        return Err(storage("Arrow source is not an IPC file"));
    }
    if footer_length > budget {
        return Err(super::limit(
            "Arrow source footer exceeds construction memory budget",
        ));
    }
    let mut footer_bytes = vec![0; usize::try_from(footer_length).map_err(storage)?];
    file.seek(SeekFrom::Start(length - 10 - footer_length))
        .map_err(storage)?;
    file.read_exact(&mut footer_bytes).map_err(storage)?;
    let footer = arrow::ipc::root_as_footer(&footer_bytes).map_err(storage)?;
    let ipc_schema = footer
        .schema()
        .ok_or_else(|| storage("Arrow footer has no schema"))?;
    let mut schema_charge =
        (std::mem::size_of::<arrow::datatypes::Schema>() as u64).saturating_add(256);
    if let Some(fields) = ipc_schema.fields() {
        for field in fields {
            ipc_field_bytes(field, &mut schema_charge, budget, 0)?;
        }
    }
    if let Some(metadata) = ipc_schema.custom_metadata() {
        for entry in metadata {
            schema_charge = schema_charge
                .saturating_add(entry.key().map_or(0, str::len) as u64)
                .saturating_add(entry.value().map_or(0, str::len) as u64)
                .saturating_add(128);
        }
    }
    if footer_length.saturating_add(schema_charge) > budget {
        return Err(super::limit(
            "Arrow source schema exceeds construction memory budget",
        ));
    }
    let schema = arrow::ipc::convert::fb_to_schema(ipc_schema);
    let schema_bytes = schema_owned_bytes(&schema);
    let columns = schema.fields().len();
    let mut dictionaries = 0_u64;
    let mut maximum_batch = 0_u64;
    let batch_count = footer.recordBatches().map_or(0, |blocks| blocks.len());
    let rows_bytes = (batch_count as u64).saturating_mul(8);
    if footer_length
        .saturating_add(schema_bytes)
        .saturating_add(rows_bytes)
        > budget
    {
        return Err(super::limit(
            "Arrow source row inventory exceeds construction memory budget",
        ));
    }
    let mut rows = Vec::with_capacity(batch_count);
    for (dictionary, blocks) in [
        (true, footer.dictionaries()),
        (false, footer.recordBatches()),
    ] {
        for block in blocks.into_iter().flatten() {
            let offset = u64::try_from(block.offset()).map_err(storage)?;
            let metadata_length = u64::try_from(block.metaDataLength()).map_err(storage)?;
            if footer_length
                .saturating_add(schema_bytes)
                .saturating_add(metadata_length)
                .saturating_add((rows.capacity() as u64).saturating_mul(8))
                > budget
                || offset
                    .checked_add(metadata_length)
                    .is_none_or(|end| end > length)
            {
                return Err(super::limit(
                    "Arrow message metadata exceeds construction memory budget",
                ));
            }
            let footer_body_length = u64::try_from(block.bodyLength())
                .map_err(|_| storage("Arrow footer block body length is negative"))?;
            if offset
                .checked_add(metadata_length)
                .and_then(|start| start.checked_add(footer_body_length))
                .is_none_or(|end| end > length)
            {
                return Err(storage("Arrow footer block body extends beyond its source"));
            }
            if metadata_length.saturating_add(footer_body_length) > budget {
                return Err(super::limit(
                    "Arrow footer block allocation exceeds construction memory budget",
                ));
            }
            let mut header = vec![0; usize::try_from(metadata_length).map_err(storage)?];
            file.seek(SeekFrom::Start(offset)).map_err(storage)?;
            file.read_exact(&mut header).map_err(storage)?;
            let skip = if header.starts_with(&[0xff; 4]) { 8 } else { 4 };
            let message = arrow::ipc::root_as_message(header.get(skip..).unwrap_or_default())
                .map_err(storage)?;
            let body_length = u64::try_from(message.bodyLength()).map_err(storage)?;
            if body_length != footer_body_length {
                return Err(storage("Arrow footer and message body lengths disagree"));
            }
            let body_start = offset
                .checked_add(metadata_length)
                .ok_or_else(|| storage("Arrow message offset overflows"))?;
            if body_start
                .checked_add(body_length)
                .is_none_or(|end| end > length)
            {
                return Err(storage("Arrow message body is truncated"));
            }
            let batch = if dictionary {
                message
                    .header_as_dictionary_batch()
                    .and_then(|dictionary| dictionary.data())
            } else {
                message.header_as_record_batch()
            }
            .ok_or_else(|| storage("Arrow footer block has the wrong message kind"))?;
            let decoded = ipc_buffer_bytes(&mut file, batch, body_start, body_length)?;
            let workspace = metadata_length
                .saturating_add(body_length)
                .saturating_add(decoded);
            if dictionary {
                dictionaries = dictionaries.saturating_add(workspace);
            } else {
                maximum_batch = maximum_batch.max(workspace);
                rows.push(u64::try_from(batch.length()).map_err(storage)?);
            }
        }
    }
    let decoding_bytes = dictionaries.saturating_add(maximum_batch);
    if decoding_bytes > budget {
        return Err(super::limit(
            "Arrow source decoding exceeds construction memory budget",
        ));
    }
    Ok(IpcPlan {
        rows,
        columns,
        schema_bytes,
        decoding_bytes,
    })
}

fn ipc_field_bytes(
    field: arrow::ipc::Field<'_>,
    bytes: &mut u64,
    budget: u64,
    depth: usize,
) -> Result<(), GfError> {
    *bytes = bytes
        .saturating_add(field.name().map_or(0, str::len) as u64)
        .saturating_add(256);
    if let Some(timestamp) = field.type_as_timestamp() {
        *bytes = bytes.saturating_add(timestamp.timezone().map_or(0, str::len) as u64);
    }
    if let Some(union) = field.type_as_union() {
        *bytes = bytes.saturating_add(
            union
                .typeIds()
                .map_or(0, |ids| ids.len() as u64)
                .saturating_mul(4),
        );
    }

    if let Some(metadata) = field.custom_metadata() {
        for entry in metadata {
            *bytes = bytes
                .saturating_add(entry.key().map_or(0, str::len) as u64)
                .saturating_add(entry.value().map_or(0, str::len) as u64)
                .saturating_add(128);
        }
    }
    if *bytes > budget || depth > 64 {
        return Err(super::limit(
            "Arrow source schema exceeds construction memory budget",
        ));
    }
    if let Some(children) = field.children() {
        for child in children {
            ipc_field_bytes(child, bytes, budget, depth + 1)?;
        }
    }
    Ok(())
}

fn ipc_buffer_bytes(
    file: &mut File,
    batch: arrow::ipc::RecordBatch<'_>,
    body_start: u64,
    body_length: u64,
) -> Result<u64, GfError> {
    use std::io::{Read as _, Seek as _, SeekFrom};
    let mut decoded = 0_u64;
    for buffer in batch.buffers().into_iter().flatten() {
        let offset = u64::try_from(buffer.offset()).map_err(storage)?;
        let length = u64::try_from(buffer.length()).map_err(storage)?;
        if offset
            .checked_add(length)
            .is_none_or(|end| end > body_length)
        {
            return Err(storage("Arrow buffer extends beyond its message body"));
        }
        let expanded = if batch.compression().is_some() && length != 0 {
            if length < 8 {
                return Err(storage("Arrow compressed buffer lacks expanded length"));
            }
            file.seek(SeekFrom::Start(body_start + offset))
                .map_err(storage)?;
            let mut prefix = [0; 8];
            file.read_exact(&mut prefix).map_err(storage)?;
            let expanded = i64::from_le_bytes(prefix);
            if expanded == -1 {
                length - 8
            } else {
                u64::try_from(expanded).map_err(storage)?
            }
        } else {
            length
        };
        decoded = decoded.saturating_add(expanded);
    }
    decoded = decoded.saturating_add(
        batch
            .nodes()
            .map_or(0, |nodes| nodes.len() as u64)
            .saturating_mul(256),
    );
    Ok(decoded)
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
/// SHA-256 is sequential, and the build decodes row groups in parallel: the
/// fastest workers run a full worker count of row groups ahead of the slowest,
/// so folding their reads into one ordered digest would hold that lead in memory
/// or read most of the file again afterwards (measured: 90% and 96% of the
/// bytes at S20 and S22 with a 64 MiB bound). So each source gets one thread that
/// reads it front to back while the workers decode. The pages it reads are the
/// ones the workers read next, and the digest costs no time after the last task.
#[derive(Default)]
pub(super) struct Digests {
    stop: Arc<AtomicBool>,
    hashers: Mutex<Vec<(u64, std::thread::JoinHandle<Result<String, GfError>>)>>,
}

impl Digests {
    fn start(&self, sequence: u64, external: ExternalSource) {
        let stop = self.stop.clone();
        let hasher = std::thread::Builder::new()
            .name("gf-source-digest".into())
            .spawn(move || hash_source(&external, &stop));
        match hasher {
            Ok(hasher) => self
                .hashers
                .lock()
                .expect("source digest registry lock poisoned")
                .push((sequence, hasher)),
            Err(error) => {
                // Surface it when the digests are collected; never skip the digest.
                let message = error.to_string();
                let failed = std::thread::spawn(move || Err(storage(message)));
                self.hashers
                    .lock()
                    .expect("source digest registry lock poisoned")
                    .push((sequence, failed));
            }
        }
    }

    /// Wait for every source's digest, keyed by source sequence.
    pub(super) fn finish(&self) -> Result<BTreeMap<u64, String>, GfError> {
        let hashers = std::mem::take(
            &mut *self
                .hashers
                .lock()
                .expect("source digest registry lock poisoned"),
        );
        let mut digests = BTreeMap::new();
        let mut failure = None;
        for (sequence, hasher) in hashers {
            match hasher
                .join()
                .map_err(|_| storage("source digest thread panicked"))?
            {
                Ok(sha256) => {
                    digests.insert(sequence, sha256);
                }
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(digests), Err)
    }
}

impl Drop for Digests {
    /// A build that ended early leaves no thread reading a source.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Ok(hashers) = self.hashers.get_mut() {
            for (_, hasher) in std::mem::take(hashers) {
                let _ = hasher.join();
            }
        }
    }
}

/// Read one source front to back, hashing it, and confirm it is still the file
/// registration recorded.
fn hash_source(external: &ExternalSource, stop: &AtomicBool) -> Result<String, GfError> {
    use std::io::Read as _;

    let mut file = external.open()?;
    let mut hasher = ObservedSha256::new();
    let mut buffer = vec![0_u8; 1 << 20];
    let mut total = 0_u64;
    loop {
        if stop.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let read = file.read(&mut buffer).map_err(storage)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
    }
    if total != external.size {
        return Err(source_changed(
            &external.path,
            SourceChange::Resized,
            &format!("read {total} bytes, registered {}", external.size),
        ));
    }
    external.check(&file)?;
    Ok(external_source::hex(&hasher.finalize()))
}

/// An in-place Parquet source and whether its digest reader has started.
struct InPlace<'a> {
    external: ExternalSource,
    digests: &'a Digests,
    started: AtomicBool,
}

impl InPlace<'_> {
    /// Start the digest reader once, when the first task has the source open.
    fn start_digest(&self, sequence: u64) {
        if !self.started.swap(true, Ordering::AcqRel) {
            self.digests.start(sequence, self.external.clone());
        }
    }
}

struct SourceReader<'a> {
    graph: &'a GraphForge,
    path: PathBuf,
    in_place: Option<InPlace<'a>>,
    kind: BulkInputKind,
    operation_uuid: Uuid,
    sequence: u64,
    batch_rows: usize,
    format: Format,
    schema_bytes: u64,
    decoding_bytes: u64,
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

impl BulkBatchReader for SourceReader<'_> {
    fn schema_resident_bytes(&self) -> u64 {
        self.schema_bytes
    }
    fn retained_metadata_bytes(&self) -> u64 {
        self.schema_bytes.saturating_add(match &self.format {
            Format::Parquet { metadata, .. } => metadata.metadata().memory_size() as u64,
            Format::Arrow { batch_rows } => (batch_rows.capacity() as u64).saturating_mul(8),
        })
    }

    fn decoded_workspace_bytes(&self) -> u64 {
        self.decoding_bytes
    }
    fn task_rows(&self, task: usize) -> usize {
        let first_batch = task as u64 * BATCHES_PER_TASK;
        let batch_rows = self.batch_rows as u64;
        let rows = match &self.format {
            Format::Parquet { rows, .. } => {
                (first_batch * batch_rows + BATCHES_PER_TASK * batch_rows).min(*rows)
                    - (first_batch * batch_rows).min(*rows)
            }
            Format::Arrow { batch_rows } => {
                let first = usize::try_from(first_batch).unwrap_or(usize::MAX);
                batch_rows
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
        let Format::Parquet { metadata, rows } = &self.format else {
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
        match &self.format {
            Format::Parquet { metadata, rows } => {
                let in_place = self
                    .in_place
                    .as_ref()
                    .ok_or_else(|| storage("Parquet import source has no registered identity"))?;
                let file = in_place.external.open()?;
                #[cfg(test)]
                super::external_source::pass_hook(&in_place.external.path, "opened", task as u64);
                in_place.start_digest(self.sequence);
                let guard = file.try_clone().map_err(storage)?;
                let batch_rows = self.batch_rows as u64;
                let start = first_batch * batch_rows;
                let end = (start + BATCHES_PER_TASK * batch_rows).min(*rows);
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
                    ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata.clone())
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
                let reader = builder.build().map_err(storage)?;
                for (offset, batch) in reader.enumerate() {
                    #[cfg(test)]
                    super::external_source::pass_hook(
                        &in_place.external.path,
                        "batch",
                        first_batch + offset as u64,
                    );
                    // The source can change between any two batches.
                    in_place.external.check(&guard)?;
                    self.emit(first_batch + offset as u64, batch.map_err(storage)?, sink)?;
                }
                in_place.external.check(&guard)?;
            }
            Format::Arrow { .. } => {
                let file = File::open(&self.path).map_err(storage)?;
                let mut reader = ArrowFileReader::try_new(file, None).map_err(storage)?;
                let total = reader.num_batches() as u64;
                for index in first_batch..(first_batch + BATCHES_PER_TASK).min(total) {
                    reader
                        .set_index(usize::try_from(index).map_err(storage)?)
                        .map_err(storage)?;
                    let batch = reader
                        .next()
                        .ok_or_else(|| storage("Arrow source ended before its footer count"))?
                        .map_err(storage)?;
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
pub(super) fn bulk_build_memory_budget() -> Result<u64, GfError> {
    #[cfg(test)]
    if let Some(budget) = TEST_BUDGET.with(std::cell::Cell::get) {
        return Ok(budget);
    }
    super::memory_budget::bulk_build_memory_budget()
}

#[cfg(test)]
thread_local! {
    /// Lets a test force the plan-time routing decision.
    pub(super) static TEST_BUDGET: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

fn schema_owned_bytes(schema: &arrow::datatypes::Schema) -> u64 {
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

/// Plan one registered source from its footer (pass 0).
#[allow(
    clippy::too_many_arguments,
    reason = "the build's shared observers travel with the plan's inputs"
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
) -> Result<BulkSource<'a>, GfError> {
    let path = root.join("sources").join(&source.name);
    let kind = source.kind.input_kind();
    let required = match kind {
        BulkInputKind::Node => 2,
        BulkInputKind::Edge => 4,
    };
    let (format, rows, columns, schema_bytes, decoding_bytes) = match source.kind {
        ImportSourceKind::ParquetNodes | ImportSourceKind::ParquetEdges => {
            let external = source.external.as_ref().ok_or_else(|| {
                storage("Parquet import source has no registered identity; register it again")
            })?;
            let file = external.open()?;
            let metadata =
                ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).map_err(storage)?;
            let rows =
                u64::try_from(metadata.metadata().file_metadata().num_rows()).map_err(storage)?;
            let columns = metadata.schema().fields().len();
            let schema_bytes = schema_owned_bytes(metadata.schema().as_ref());
            (
                Format::Parquet { metadata, rows },
                rows,
                columns,
                schema_bytes,
                0,
            )
        }
        ImportSourceKind::ArrowNodes | ImportSourceKind::ArrowEdges => {
            let sizing = ipc_plan(&path)?;
            let rows = sizing
                .rows
                .iter()
                .try_fold(0_u64, |total, rows| total.checked_add(*rows))
                .ok_or_else(|| {
                    super::limit("Arrow source row count exceeds construction capacity")
                })?;
            (
                Format::Arrow {
                    batch_rows: sizing.rows,
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
        Format::Arrow { batch_rows } => batch_rows.len() as u64,
    };
    let decoded_bytes = match &format {
        Format::Parquet { metadata, .. } => metadata
            .metadata()
            .row_groups()
            .iter()
            .map(|group| u64::try_from(group.total_byte_size()).unwrap_or(0))
            .sum(),
        Format::Arrow { .. } => fs::metadata(&path).map_err(storage)?.len(),
    };
    let tasks = usize::try_from(batches.div_ceil(BATCHES_PER_TASK)).map_err(storage)?;
    let in_place = source.external.as_ref().map(|external| InPlace {
        external: external.clone(),
        digests,
        started: AtomicBool::new(false),
    });
    if tasks == 0
        && let Some(in_place) = &in_place
    {
        // No task will open it, so nothing else would digest it.
        in_place.start_digest(source.sequence);
    }
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
mod ipc_planning_tests {
    use super::ipc_plan;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
    use arrow::record_batch::RecordBatch;
    use std::fs::File;
    use std::sync::Arc;

    fn write_source(path: &std::path::Path, compression: bool) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(
                (0..1024).map(i64::from).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let options = IpcWriteOptions::default()
            .try_with_compression(compression.then_some(arrow::ipc::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut writer =
            FileWriter::try_new_with_options(File::create(path).unwrap(), &schema, options)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn ipc_footer_planning_counts_expansion_without_decoding_arrays() {
        let root = tempfile::tempdir().unwrap();
        for compressed in [false, true] {
            let path = root.path().join(format!("{compressed}.arrow"));
            write_source(&path, compressed);
            let plan = ipc_plan(&path).unwrap();
            assert_eq!(plan.rows, vec![1024]);
            assert_eq!(plan.columns, 1);
            assert!(plan.decoding_bytes >= 8192);
        }
    }

    #[test]
    fn ipc_timezone_schema_expansion_is_admitted_before_conversion() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("timezone.arrow");
        let zone = std::iter::repeat_n('x', 400_000).collect::<String>();
        let schema = Schema::new(vec![Field::new(
            "when",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some(zone.into())),
            true,
        )]);
        let mut writer = FileWriter::try_new(File::create(&path).unwrap(), &schema).unwrap();
        writer.finish().unwrap();
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(600 << 10)));
        let refused = ipc_plan(&path);
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
        let error = refused
            .err()
            .expect("footer plus cloned timezone must be charged before conversion");
        assert!(error.to_string().contains("schema"), "{error}");
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(Some(2 << 20)));
        let accepted = ipc_plan(&path);
        super::super::bulk_source::TEST_BUDGET.with(|budget| budget.set(None));
        let accepted = accepted.unwrap();
        assert!(accepted.rows.is_empty());
        assert!(accepted.schema_bytes >= 400_000);
    }

    #[test]
    fn ipc_footer_allocation_lengths_are_validated_before_decoder_creation() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("input.arrow");
        write_source(&path, false);
        let original = std::fs::read(&path).unwrap();
        let footer_length = i32::from_le_bytes(
            original[original.len() - 10..original.len() - 6]
                .try_into()
                .unwrap(),
        ) as usize;
        let footer_start = original.len() - 10 - footer_length;
        let footer =
            arrow::ipc::root_as_footer(&original[footer_start..original.len() - 10]).unwrap();
        let slot = footer_start
            + footer._tab.loc()
            + usize::from(
                footer
                    ._tab
                    .vtable()
                    .get(arrow::ipc::Footer::VT_RECORDBATCHES),
            );
        let vector =
            slot + u32::from_le_bytes(original[slot..slot + 4].try_into().unwrap()) as usize;
        // IPC Block is a fixed struct: offset8, metadata length4, padding4,
        // body length8. This mutates the allocation FileReader actually uses.
        for invalid in [-1_i64, i64::MAX] {
            let mut corrupt = original.clone();
            corrupt[vector + 4 + 16..vector + 4 + 24].copy_from_slice(&invalid.to_le_bytes());
            std::fs::write(&path, corrupt).unwrap();
            let error = ipc_plan(&path)
                .err()
                .expect("bad footer must refuse before allocating");
            assert!(error.to_string().contains("footer block body"), "{error}");
        }
    }
}
