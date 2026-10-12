//! Exact row selection for edge files whose `edge_id` column is dense.
//!
//! A canonical edge file holds a contiguous run of surrogate edge ids in
//! ascending order, so an id is its own row ordinal: `ordinal = edge_id -
//! first_id`. The Parquet statistics at open prove that layout (the same proof
//! node topology uses, [`dense_node_layout`]). When the proof holds, a read
//! selects the requested rows by ordinal and decodes only the pages that hold
//! them; when it does not (a file that shares its id window with another
//! route, a file without a page index, a file whose rows contradict its
//! statistics) the caller keeps the conservative membership-predicate read.
//!
//! The decision is made per file from that file's own footer. Two consequences
//! matter at scale:
//!
//! - A file that holds none of the requested ids selects no row group, so a
//!   wide frontier never decodes the files outside its id range. The
//!   predicate path compared the size of the whole request with the size of
//!   one file, and read every file in full once the request passed half a
//!   file.
//! - Only the requested projection is decoded, and only for the selected rows.

use super::FilteredReadKind;
use super::FilteredReadObservation;
use super::dense_node_layout;
use super::dense_node_selection;
use super::io_err;
use super::parquet_err;
use super::record_filtered;
use super::record_scanned;
use super::total_rows;
use crate::catalog::admitted_path_file;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;

/// The edge ids one traversal asked a route for.
///
/// The ascending copy that the per-file ordinal lookup needs is built on first
/// use and shared by every file of the call, so a wide frontier is sorted once
/// rather than once per file, and a route whose files are all non-dense never
/// pays for it.
pub(crate) struct EdgeIdRequest<'a> {
    ids: &'a HashSet<u64>,
    sorted: OnceLock<Vec<u64>>,
}

impl<'a> EdgeIdRequest<'a> {
    pub(crate) fn new(ids: &'a HashSet<u64>) -> Self {
        Self {
            ids,
            sorted: OnceLock::new(),
        }
    }

    pub(crate) fn ids(&self) -> &'a HashSet<u64> {
        self.ids
    }

    fn sorted(&self) -> &[u64] {
        self.sorted.get_or_init(|| {
            let mut sorted: Vec<u64> = self.ids.iter().copied().collect();
            sorted.sort_unstable();
            sorted
        })
    }
}

/// Read the requested rows of one edge file by ordinal.
///
/// `projection` is the canonical column set with the `edge_id` key included,
/// or `None` for every column. Returns `Ok(None)` when the file does not carry
/// the dense layout proof, when the request covers more than half of the file
/// (a plain read is cheaper), or when the rows read back contradict the proof;
/// the caller then takes the conservative read, which also handles the
/// full-read accounting.
pub(super) fn read_dense_edge_file(
    path: &Path,
    schema: &SchemaRef,
    request: &EdgeIdRequest<'_>,
    observer: Option<&Arc<dyn crate::io_stats::FilteredReadObserver>>,
    projection: Option<&[usize]>,
) -> Result<Option<Vec<RecordBatch>>, DataFusionError> {
    use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    use parquet::file::metadata::PageIndexPolicy;

    let file = File::open(path).map_err(|error| io_err(&error))?;
    // Optional, not required: a file without a page index is not dense-proven
    // and takes the conservative path.
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(admitted_path_file(file)?, options)
            .map_err(parquet_err)?;
    let metadata = Arc::clone(builder.metadata());
    let Some(key_leaf) = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|column| column.name() == "edge_id")
    else {
        return Ok(None);
    };
    let Some(layout) = dense_node_layout(&metadata, key_leaf) else {
        crate::io_stats::record_edge_dense_layout_declined();
        return Ok(None);
    };
    let Some(last_id) = u64::try_from(layout.total_rows)
        .ok()
        .and_then(|rows| layout.first_id.checked_add(rows.checked_sub(1)?))
    else {
        return Ok(None);
    };

    let sorted = request.sorted();
    let begin = sorted.partition_point(|id| *id < layout.first_id);
    let end = sorted.partition_point(|id| *id <= last_id);
    let candidates = &sorted[begin..end];
    let candidate_rows = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
    let file_rows = u64::try_from(layout.total_rows).unwrap_or(u64::MAX);
    if candidate_rows.saturating_mul(2) > file_rows {
        crate::io_stats::record_edge_dense_half_file_declined();
        return Ok(None);
    }

    let projected_schema = projected_schema(schema, projection);
    let row_groups_considered = u64::try_from(metadata.num_row_groups()).unwrap_or(u64::MAX);
    let mut observation = FilteredReadObservation::new(observer, FilteredReadKind::Edge);
    if candidates.is_empty() {
        // The file holds none of the requested ids: no row group is selected
        // and nothing but its footer was read.
        record_filtered(FilteredReadKind::Edge, 0);
        let pruning = crate::io_stats::FilteredReadPruning {
            strategy: crate::io_stats::FilteredReadStrategy::DenseRowSelection,
            row_groups_considered,
            row_groups_selected: 0,
            pages_considered: layout.pages_considered,
            pages_selected: 0,
            exact_rows_selected: 0,
            metadata_fallbacks: 0,
            validation_fallbacks: 0,
        };
        crate::io_stats::record_edge_dense_pruning(pruning);
        observation.pruning(pruning);
        observation.complete(0, false);
        return Ok(Some(vec![RecordBatch::new_empty(projected_schema)]));
    }
    let Some(dense) = dense_node_selection(&metadata, key_leaf, candidates) else {
        observation.complete(0, false);
        return Ok(None);
    };
    let mut pruning = crate::io_stats::FilteredReadPruning {
        strategy: crate::io_stats::FilteredReadStrategy::DenseRowSelection,
        row_groups_considered,
        row_groups_selected: u64::try_from(dense.row_groups.len()).unwrap_or(u64::MAX),
        pages_considered: dense.pages_considered,
        pages_selected: dense.pages_selected,
        exact_rows_selected: dense.exact_rows_selected,
        metadata_fallbacks: 0,
        validation_fallbacks: 0,
    };

    let batches = read_selection(builder, dense.row_groups, dense.selection, projection)?;
    let returned = total_rows(&batches);
    record_filtered(FilteredReadKind::Edge, returned);
    record_scanned(FilteredReadKind::Edge, returned);
    observation.scanned(returned);

    // The statistics prove the layout; the rows confirm it. The selected rows
    // come back in file order, so they must carry exactly the candidate ids in
    // ascending order. Anything else means the file contradicts its own
    // metadata, and the conservative read decides what it holds.
    if !keys_equal(&batches, candidates) {
        pruning.validation_fallbacks = 1;
        crate::io_stats::record_edge_dense_pruning(pruning);
        observation.pruning(pruning);
        observation.complete(returned, false);
        return Ok(None);
    }
    crate::io_stats::record_edge_dense_pruning(pruning);
    observation.pruning(pruning);
    observation.complete(returned, false);
    if batches.is_empty() {
        return Ok(Some(vec![RecordBatch::new_empty(projected_schema)]));
    }
    Ok(Some(batches))
}

/// The schema of a read of `projection` (every column when `None`).
fn projected_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> SchemaRef {
    projection.map_or_else(
        || Arc::clone(schema),
        |indices| {
            Arc::new(arrow::datatypes::Schema::new(
                indices
                    .iter()
                    .map(|index| schema.field(*index).clone())
                    .collect::<Vec<_>>(),
            ))
        },
    )
}

/// Decode the selected rows of the retained row groups, in file order.
fn read_selection(
    builder: parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder<
        crate::lifecycle_io::ReadPathFile,
    >,
    row_groups: Vec<usize>,
    selection: parquet::arrow::arrow_reader::RowSelection,
    projection: Option<&[usize]>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    use parquet::arrow::ProjectionMask;

    let builder = builder
        .with_row_groups(row_groups)
        .with_row_selection(selection);
    let builder = if let Some(indices) = projection {
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
        builder.with_projection(mask)
    } else {
        builder
    };
    builder
        .build()
        .map_err(parquet_err)?
        .collect::<Result<_, _>>()
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

/// Whether the `edge_id` column of `batches`, in order, is exactly `expected`.
fn keys_equal(batches: &[RecordBatch], expected: &[u64]) -> bool {
    use arrow::array::Array as _;

    let mut next = expected.iter();
    for batch in batches {
        let Some(ids) = batch
            .column_by_name("edge_id")
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::UInt64Array>())
        else {
            return false;
        };
        for row in 0..ids.len() {
            if ids.is_null(row) || next.next() != Some(&ids.value(row)) {
                return false;
            }
        }
    }
    next.next().is_none()
}
