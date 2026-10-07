//! Registered import files as bulk-builder sources (#1883).
//!
//! The builder reads each source in place, in parallel tasks. A task is a run
//! of whole construction batches, so batch boundaries, and therefore the
//! operation identity every batch normalizes under, match a sequential read of
//! the file.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::ipc::reader::FileReader as ArrowFileReader;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_storage::{BulkBatchReader, BulkSource};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use uuid::Uuid;

use super::{
    ImportSourceKind, SourceRecord, cancelled, canonicalize_parquet_batch, import_batch_operation,
    normalize_batch, storage, validation,
};
use crate::{BulkInputKind, CancellationToken, GraphForge};

/// Construction batches decoded per task: 16 x 65,536 rows is one row group of
/// the Graph500 generator and of the Parquet writer's default.
const BATCHES_PER_TASK: u64 = 16;

enum Format {
    Parquet {
        metadata: ArrowReaderMetadata,
        rows: u64,
    },
    /// Rows of every record batch, from the IPC footer's message headers.
    Arrow { batch_rows: Vec<u64> },
}

/// Rows of every record batch of an Arrow IPC file, read from the footer and the
/// batch message headers without decoding any column.
fn ipc_batch_rows(path: &Path) -> Result<Vec<u64>, GfError> {
    use std::io::{Read as _, Seek as _, SeekFrom};

    let mut file = File::open(path).map_err(storage)?;
    let length = file.metadata().map_err(storage)?.len();
    let mut tail = [0_u8; 10];
    if length < 10 {
        return Err(storage("Arrow source is too short for an IPC footer"));
    }
    file.seek(SeekFrom::Start(length - 10)).map_err(storage)?;
    file.read_exact(&mut tail).map_err(storage)?;
    let footer_length = u64::try_from(i32::from_le_bytes(tail[..4].try_into().expect("4 bytes")))
        .map_err(|_| storage("Arrow footer length is negative"))?;
    if &tail[4..] != b"ARROW1" || footer_length + 10 > length {
        return Err(storage("Arrow source is not an IPC file"));
    }
    let mut footer_bytes = vec![0_u8; usize::try_from(footer_length).map_err(storage)?];
    file.seek(SeekFrom::Start(length - 10 - footer_length))
        .map_err(storage)?;
    file.read_exact(&mut footer_bytes).map_err(storage)?;
    let footer = arrow::ipc::root_as_footer(&footer_bytes)
        .map_err(|error| storage(format!("Arrow footer is invalid: {error}")))?;
    let blocks = footer
        .recordBatches()
        .map(|blocks| blocks.iter().collect::<Vec<_>>());
    blocks
        .unwrap_or_default()
        .into_iter()
        .map(|block| {
            let mut header = vec![0_u8; usize::try_from(block.metaDataLength()).map_err(storage)?];
            file.seek(SeekFrom::Start(
                u64::try_from(block.offset()).map_err(storage)?,
            ))
            .map_err(storage)?;
            file.read_exact(&mut header).map_err(storage)?;
            // A continuation marker (0xFFFFFFFF) precedes the 4-byte size.
            let skip = if header.starts_with(&[0xff; 4]) { 8 } else { 4 };
            let message = arrow::ipc::root_as_message(header.get(skip..).unwrap_or_default())
                .map_err(|error| storage(format!("Arrow message is invalid: {error}")))?;
            let batch = message
                .header_as_record_batch()
                .ok_or_else(|| storage("Arrow block is not a record batch"))?;
            u64::try_from(batch.length()).map_err(storage)
        })
        .collect()
}

struct SourceReader<'a> {
    graph: &'a GraphForge,
    path: PathBuf,
    kind: BulkInputKind,
    operation_uuid: Uuid,
    sequence: u64,
    batch_rows: usize,
    format: Format,
    cancellation: Option<&'a CancellationToken>,
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
    }
}

impl BulkBatchReader for SourceReader<'_> {
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

    fn read_task(
        &self,
        task: usize,
        sink: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let first_batch = task as u64 * BATCHES_PER_TASK;
        let file = File::open(&self.path).map_err(storage)?;
        match &self.format {
            Format::Parquet { metadata, rows } => {
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
                    self.emit(first_batch + offset as u64, batch.map_err(storage)?, sink)?;
                }
            }
            Format::Arrow { .. } => {
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
/// This chooses between two builds of the same bytes, never what is built: an
/// initial build whose estimate exceeds it takes the staged path, which holds a
/// fixed window of memory (ADR 0058).
pub(super) fn bulk_build_memory_budget() -> u64 {
    #[cfg(test)]
    if let Some(budget) = TEST_BUDGET.with(std::cell::Cell::get) {
        return budget;
    }
    super::memory_budget::bulk_build_memory_budget()
}

#[cfg(test)]
thread_local! {
    /// Lets a test force the plan-time routing decision.
    pub(super) static TEST_BUDGET: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Plan one registered source from its footer (pass 0).
pub(super) fn plan<'a>(
    graph: &'a GraphForge,
    root: &Path,
    source: &SourceRecord,
    batch_rows: usize,
    operation_uuid: Uuid,
    cancellation: Option<&'a CancellationToken>,
) -> Result<BulkSource<'a>, GfError> {
    let path = root.join("sources").join(&source.name);
    let kind = source.kind.input_kind();
    let required = match kind {
        BulkInputKind::Node => 2,
        BulkInputKind::Edge => 4,
    };
    let (format, rows, columns) = match source.kind {
        ImportSourceKind::ParquetNodes | ImportSourceKind::ParquetEdges => {
            let file = File::open(&path).map_err(storage)?;
            let metadata =
                ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).map_err(storage)?;
            let rows =
                u64::try_from(metadata.metadata().file_metadata().num_rows()).map_err(storage)?;
            let columns = metadata.schema().fields().len();
            (Format::Parquet { metadata, rows }, rows, columns)
        }
        ImportSourceKind::ArrowNodes | ImportSourceKind::ArrowEdges => {
            let file = File::open(&path).map_err(storage)?;
            let reader = ArrowFileReader::try_new(file, None).map_err(storage)?;
            let columns = reader.schema().fields().len();
            let counts = ipc_batch_rows(&path)?;
            if counts.len() != reader.num_batches() {
                return Err(storage("Arrow footer and batch headers disagree"));
            }
            let rows = counts.iter().sum();
            (Format::Arrow { batch_rows: counts }, rows, columns)
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
    Ok(BulkSource {
        reader: Arc::new(SourceReader {
            graph,
            path,
            kind,
            operation_uuid,
            sequence: source.sequence,
            batch_rows,
            format,
            cancellation,
        }),
        tasks: usize::try_from(batches.div_ceil(BATCHES_PER_TASK)).map_err(storage)?,
        rows,
        property_free: columns == required,
        decoded_bytes,
    })
}
