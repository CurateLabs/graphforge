//! DataFusion [`TableProvider`] and [`CatalogProvider`] implementations.
//!
//! Each GraphForge graph directory (`project/`) maps to a [`GraphCatalog`] which
//! presents its Parquet files as DataFusion tables under the address
//! `graph.graph.<table_name>`:
//!
//! | Table name | File | Schema |
//! |---|---|---|
//! | `topology_nodes` | `topology/nodes.parquet` | `TOPOLOGY_NODES_SCHEMA` |
//! | `edges_TYPENAME` | `topology/edges/TYPENAME.parquet` | `TYPED_EDGE_SCHEMA` |
//! | `edges__exploratory` | `topology/edges/_exploratory.parquet` | `EXPLORATORY_EDGE_SCHEMA` |
//! | `properties_ENTITY` | `properties/ENTITY.parquet` | `property_schema(entity, defs)` |
//!
//! # Scan implementation
//!
//! Scans are implemented via DataFusion's [`MemTable`]: the Parquet file is read
//! into memory at query time and wrapped in a `MemTable` which handles projection
//! and filter application.  This is correct and simple for M12; lower-level
//! pushdown can be added in a later milestone.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_catalog::Session;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use gf_core::OntologyMode;
use gf_ir::RuntimeCatalog;
use gf_ontology::OntologyHandle;

use crate::schemas::{
    EXPLORATORY_EDGE_SCHEMA, TOPOLOGY_NODES_SCHEMA, TYPED_EDGE_SCHEMA, property_schema,
};

// ---------------------------------------------------------------------------
// Parquet I/O helpers
// ---------------------------------------------------------------------------

fn parquet_err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::External(e.to_string().into())
}

fn io_err(e: &std::io::Error) -> DataFusionError {
    DataFusionError::External(e.to_string().into())
}

/// Total rows across `batches`, for the [`io_stats`](crate::io_stats) counters.
fn total_rows(batches: &[RecordBatch]) -> u64 {
    u64::try_from(batches.iter().map(RecordBatch::num_rows).sum::<usize>()).unwrap_or(u64::MAX)
}

/// Read all row groups from a Parquet file into a single [`RecordBatch`].
///
/// Returns an empty batch (correct schema, zero rows) if the file does not exist.
pub(crate) fn read_parquet_or_empty(
    path: &Path,
    schema: SchemaRef,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if !path.exists() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }
    let file = File::open(path).map_err(|e| io_err(&e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(parquet_err)?;
    let file_schema = builder.schema().clone();
    let reader = builder.build().map_err(parquet_err)?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().map_err(parquet_err)?;
    if batches.is_empty() {
        return Ok(vec![RecordBatch::new_empty(file_schema)]);
    }
    let merged = concat_batches(&file_schema, &batches)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(vec![merged])
}

/// Normalize legacy scalar-label topology batches to the current multi-label
/// schema. Existing `type_id` values become singleton `type_ids` lists.
pub(crate) fn normalize_topology_nodes(
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    use arrow::array::{Array, ListArray, UInt32Array};
    use arrow::datatypes::UInt32Type;

    batches
        .into_iter()
        .map(|batch| {
            if batch.schema().field_with_name("type_ids").is_ok() {
                return Ok(batch);
            }
            let type_idx = batch.schema().index_of("type_id").map_err(|e| {
                DataFusionError::Execution(format!("legacy node topology missing type_id: {e}"))
            })?;
            let primary_ids = batch
                .column(type_idx)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| DataFusionError::Execution("type_id is not UInt32".into()))?;
            let nullable_labels = ListArray::from_iter_primitive::<UInt32Type, _, _>(
                (0..batch.num_rows()).map(|row| Some([Some(primary_ids.value(row))])),
            );
            let labels = ListArray::new(
                Arc::new(Field::new("item", DataType::UInt32, false)),
                nullable_labels.offsets().clone(),
                nullable_labels.values().clone(),
                None,
            );
            let mut columns = batch.columns().to_vec();
            columns.insert(type_idx + 1, Arc::new(labels));
            RecordBatch::try_new(TOPOLOGY_NODES_SCHEMA.clone(), columns)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Direct readers (catalog-free)
// ---------------------------------------------------------------------------
//
// Physical execution nodes (e.g. `VarLenExpandExec`, #580) need to read the
// edge / node tables directly from the project directory: the DataFusion
// `TaskContext` they execute with exposes neither the `GraphCatalog` nor the
// project path, so the path is baked into the node at lowering time and the
// node reads the Parquet itself.  These helpers expose the same on-disk layout
// and schemas the `GraphWriter` produces, reusing [`read_parquet_or_empty`]
// (which returns a correctly-typed empty batch when the file is absent).

/// Read all edge rows for relation `rel_name` from the project at `dir`.
///
/// The on-disk layout mirrors [`GraphWriter`](crate::GraphWriter):
/// - **Strict / Advisory** — typed edges in `topology/edges/<rel_name>.parquet`
///   ([`TYPED_EDGE_SCHEMA`]).
/// - **Exploratory** — all edges in `topology/edges/_exploratory.parquet`
///   ([`EXPLORATORY_EDGE_SCHEMA`], carrying a `rel_type_name` column).  The
///   returned batch is **not** filtered by `rel_name`; callers that need a
///   single relation must filter on `rel_type_name` themselves.
///
/// Returns a single (possibly empty) [`RecordBatch`] with the mode-appropriate
/// schema; a missing file yields an empty batch rather than an error.
///
/// # Errors
/// Returns [`DataFusionError::Execution`] if `rel_name` is not a plain file
/// stem (contains path separators or `..`), and propagates Parquet / Arrow
/// errors encountered while reading.
pub fn read_edges(
    dir: &Path,
    rel_name: &str,
    mode: OntologyMode,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    // Untyped wildcard in a typed project (#823): `"*"` means "all relation
    // types", served as a union over every edge file rather than a literal
    // (nonexistent) `*.parquet`. Exploratory already reads the shared file.
    if rel_name == "*" && matches!(mode, OntologyMode::Advisory | OntologyMode::Strict) {
        return read_edges_union(dir, None, None);
    }
    // `rel_name` becomes a path component in Strict/Advisory mode; require a
    // single plain file stem so it can't traverse outside `topology/edges/`
    // (rejects path separators, `..`, absolute prefixes, and empty names).
    // Exploratory mode uses a fixed stem, so the caller-supplied name never
    // reaches the filesystem there.
    if matches!(mode, OntologyMode::Advisory | OntologyMode::Strict) {
        let mut comps = Path::new(rel_name).components();
        let single_normal =
            matches!(comps.next(), Some(std::path::Component::Normal(_))) && comps.next().is_none();
        if !single_normal {
            return Err(DataFusionError::Execution(format!(
                "invalid relation name {rel_name:?}: must be a plain file stem"
            )));
        }
    }
    let (stem, schema) = match mode {
        OntologyMode::Exploratory => ("_exploratory", EXPLORATORY_EDGE_SCHEMA.clone()),
        OntologyMode::Advisory | OntologyMode::Strict => (rel_name, TYPED_EDGE_SCHEMA.clone()),
    };
    let path = dir
        .join("topology")
        .join("edges")
        .join(format!("{stem}.parquet"));
    let batches = read_parquet_or_empty(&path, schema)?;
    crate::io_stats::record_edge_full_read(total_rows(&batches));
    Ok(batches)
}

/// Like [`read_edges`] but returns only rows whose `edge_id` is in
/// `edge_ids` — the traversal's lazy edge-record read (#830): on an adjacency
/// Hit, only the traversed edges' records are needed, not the whole file.
///
/// Two pruning layers before decode:
/// 1. **Row groups** whose `edge_id` min/max statistics cannot contain any
///    requested id are skipped entirely (edge files are globally
///    edge_id-ascending, so groups partition the id range).
/// 2. A Parquet **row filter** on `edge_id` within surviving groups (with the
///    page index enabled when present, this also skips whole pages).
///
/// Short-circuits: an empty `edge_ids` never opens the file (one empty batch);
/// a requested set covering more than half the file falls back to the plain
/// full read (the filter would cost more than it saves). Contract parity with
/// [`read_edges`]: always at least one (possibly empty) batch with the
/// mode-appropriate schema; a missing file yields an empty batch.
///
/// # Errors
/// Same as [`read_edges`], plus Parquet filter construction failures.
#[allow(clippy::implicit_hasher)]
pub fn read_edges_filtered(
    dir: &Path,
    rel_name: &str,
    mode: OntologyMode,
    edge_ids: &std::collections::HashSet<u64>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    read_edges_filtered_observed(dir, rel_name, mode, edge_ids, None)
}

/// [`read_edges_filtered`] with optional aggregate-only operator attribution.
#[allow(clippy::implicit_hasher)]
#[doc(hidden)]
pub fn read_edges_filtered_observed(
    dir: &Path,
    rel_name: &str,
    mode: OntologyMode,
    edge_ids: &std::collections::HashSet<u64>,
    observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    // Untyped wildcard union (#823) — the lazy #709 read over all relations.
    if rel_name == "*" && matches!(mode, OntologyMode::Advisory | OntologyMode::Strict) {
        return read_edges_union(dir, Some(edge_ids), observer);
    }
    if matches!(mode, OntologyMode::Advisory | OntologyMode::Strict) {
        let mut comps = Path::new(rel_name).components();
        let single_normal =
            matches!(comps.next(), Some(std::path::Component::Normal(_))) && comps.next().is_none();
        if !single_normal {
            return Err(DataFusionError::Execution(format!(
                "invalid relation name {rel_name:?}: must be a plain file stem"
            )));
        }
    }
    let (stem, schema) = match mode {
        OntologyMode::Exploratory => ("_exploratory", EXPLORATORY_EDGE_SCHEMA.clone()),
        OntologyMode::Advisory | OntologyMode::Strict => (rel_name, TYPED_EDGE_SCHEMA.clone()),
    };
    let path = dir
        .join("topology")
        .join("edges")
        .join(format!("{stem}.parquet"));
    read_parquet_filtered_u64(
        &path,
        schema,
        "edge_id",
        edge_ids,
        FilteredReadKind::Edge,
        observer,
    )
}

/// Read the union of every relation's edges (#823): the "all relation types"
/// read for an untyped traversal in a typed project. Enumerates every
/// `topology/edges/*.parquet` (stem order, for deterministic adjacency/BFS),
/// reads each (filtered to `edge_ids` when given — the lazy #709 read), and
/// normalizes every batch to [`EXPLORATORY_EDGE_SCHEMA`] by tagging a typed
/// file's rows with `rel_type_name = <file stem>` (a file already carrying the
/// column — a stray `_exploratory.parquet` — passes through). Always returns at
/// least one (possibly empty) `EXPLORATORY_EDGE_SCHEMA` batch.
fn read_edges_union(
    dir: &Path,
    edge_ids: Option<&std::collections::HashSet<u64>>,
    observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut files = crate::mutator::parquet_files_in(dir, "topology/edges")
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    files.sort();
    let mut out = Vec::new();
    for path in files {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_owned();
        let schema = discover_parquet_schema(&path).unwrap_or_else(|| TYPED_EDGE_SCHEMA.clone());
        let batches = if let Some(ids) = edge_ids {
            read_parquet_filtered_u64(
                &path,
                schema,
                "edge_id",
                ids,
                FilteredReadKind::Edge,
                observer,
            )?
        } else {
            let b = read_parquet_or_empty(&path, schema)?;
            crate::io_stats::record_edge_full_read(total_rows(&b));
            b
        };
        for batch in &batches {
            if batch.num_rows() > 0 {
                out.push(tag_rel_type_name(batch, &stem)?);
            }
        }
    }
    if out.is_empty() {
        out.push(RecordBatch::new_empty(EXPLORATORY_EDGE_SCHEMA.clone()));
    }
    Ok(out)
}

/// Normalize a typed-edge batch to [`EXPLORATORY_EDGE_SCHEMA`] by appending a
/// constant `rel_type_name = stem` column. A batch already carrying the column
/// (an `_exploratory` file) is returned unchanged.
fn tag_rel_type_name(batch: &RecordBatch, stem: &str) -> Result<RecordBatch, DataFusionError> {
    if batch.schema().field_with_name("rel_type_name").is_ok() {
        return Ok(batch.clone());
    }
    let names = arrow::array::StringArray::from(vec![stem; batch.num_rows()]);
    let mut cols: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
    cols.push(Arc::new(names));
    RecordBatch::try_new(EXPLORATORY_EDGE_SCHEMA.clone(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Which [`io_stats`](crate::io_stats) counters a filtered read attributes to —
/// `read_parquet_filtered_u64` is keyed generically but the counters are split
/// by table so the benchmark can prove edge *and* node reads are
/// neighborhood-proportional.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FilteredReadKind {
    Edge,
    Node,
}

/// Ensures every observed read has exactly one terminal completion/failure
/// event, including errors from reader construction and batch decoding.
struct FilteredReadObservation {
    observer: Option<std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
    table: crate::io_stats::FilteredReadTable,
    completed: bool,
}

impl FilteredReadObservation {
    fn new(
        observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
        kind: FilteredReadKind,
    ) -> Self {
        let table = kind.into();
        if let Some(observer) = &observer {
            observer.read_started(table);
        }
        Self {
            observer: observer.cloned(),
            table,
            completed: false,
        }
    }

    fn scanned(&self, rows: u64) {
        if let Some(observer) = &self.observer {
            observer.rows_scanned(self.table, rows);
        }
    }

    fn pruning(&self, pruning: crate::io_stats::FilteredReadPruning) {
        if let Some(observer) = &self.observer {
            observer.pruning(self.table, pruning);
        }
    }

    fn complete(&mut self, rows: u64, full: bool) {
        if let Some(observer) = &self.observer {
            observer.read_completed(self.table, rows, full);
        }
        self.completed = true;
    }
}

impl Drop for FilteredReadObservation {
    fn drop(&mut self) {
        if !self.completed
            && let Some(observer) = &self.observer
        {
            observer.read_failed(self.table);
        }
    }
}

impl From<FilteredReadKind> for crate::io_stats::FilteredReadTable {
    fn from(value: FilteredReadKind) -> Self {
        match value {
            FilteredReadKind::Edge => Self::Edge,
            FilteredReadKind::Node => Self::Node,
        }
    }
}

/// Exact row selection for the canonical dense `node_id == row_ordinal + 1`
/// layout. The selection is relative to the concatenation of `row_groups`, as
/// required by Parquet after row-group filtering.
struct DenseNodeSelection {
    row_groups: Vec<usize>,
    selection: parquet::arrow::arrow_reader::RowSelection,
    pages_considered: u64,
    pages_selected: u64,
    exact_rows_selected: u64,
}

struct DenseNodeLayout {
    group_rows: Vec<usize>,
    group_pages: Vec<Vec<usize>>,
    total_rows: usize,
    pages_considered: u64,
}

/// Prove the canonical dense node layout from row-group and page metadata.
fn dense_node_layout(
    metadata: &parquet::file::metadata::ParquetMetaData,
    key_leaf: usize,
) -> Option<DenseNodeLayout> {
    use parquet::basic::BoundaryOrder;
    use parquet::file::page_index::column_index::ColumnIndexMetaData;
    use parquet::file::statistics::Statistics;

    let total_rows = usize::try_from(metadata.file_metadata().num_rows()).ok()?;
    if total_rows == 0 || u64::try_from(total_rows).ok()? > i64::MAX as u64 {
        return None;
    }
    let row_groups = metadata.row_groups();
    let column_indexes = metadata.column_index()?;
    let offset_indexes = metadata.offset_index()?;
    if column_indexes.len() != row_groups.len() || offset_indexes.len() != row_groups.len() {
        return None;
    }

    let mut group_rows = Vec::with_capacity(row_groups.len());
    let mut group_pages = Vec::with_capacity(row_groups.len());
    let mut file_row_offset = 0usize;
    let mut pages_considered = 0u64;

    for (group_idx, row_group) in row_groups.iter().enumerate() {
        let rows = usize::try_from(row_group.num_rows()).ok()?;
        if rows == 0 {
            return None;
        }
        let expected_min = i64::try_from(file_row_offset.checked_add(1)?).ok()?;
        let expected_max = i64::try_from(file_row_offset.checked_add(rows)?).ok()?;
        let Statistics::Int64(group_stats) = row_group.column(key_leaf).statistics()? else {
            return None;
        };
        if group_stats.null_count_opt() != Some(0)
            || group_stats.min_opt() != Some(&expected_min)
            || group_stats.max_opt() != Some(&expected_max)
        {
            return None;
        }

        let page_index = column_indexes.get(group_idx)?.get(key_leaf)?;
        if page_index.get_boundary_order() != Some(BoundaryOrder::ASCENDING) {
            return None;
        }
        let ColumnIndexMetaData::INT64(page_stats) = page_index else {
            return None;
        };
        let locations = offset_indexes
            .get(group_idx)?
            .get(key_leaf)?
            .page_locations();
        if locations.is_empty()
            || usize::try_from(page_stats.num_pages()).ok()? != locations.len()
            || (0..locations.len()).any(|page| page_stats.null_count(page) != Some(0))
        {
            return None;
        }

        let mut first_rows = Vec::with_capacity(locations.len());
        for (page_idx, location) in locations.iter().enumerate() {
            let first = usize::try_from(location.first_row_index).ok()?;
            if (page_idx == 0 && first != 0)
                || first >= rows
                || first_rows.last().is_some_and(|previous| *previous >= first)
            {
                return None;
            }
            first_rows.push(first);
        }
        for (page_idx, &first) in first_rows.iter().enumerate() {
            let end = first_rows.get(page_idx + 1).copied().unwrap_or(rows);
            let page_rows = end.checked_sub(first)?;
            let page_min =
                i64::try_from(file_row_offset.checked_add(first)?.checked_add(1)?).ok()?;
            let page_max =
                i64::try_from(file_row_offset.checked_add(first)?.checked_add(page_rows)?).ok()?;
            if page_stats.min_value(page_idx) != Some(&page_min)
                || page_stats.max_value(page_idx) != Some(&page_max)
            {
                return None;
            }
        }

        pages_considered = pages_considered.checked_add(u64::try_from(locations.len()).ok()?)?;
        group_rows.push(rows);
        group_pages.push(first_rows);
        file_row_offset = file_row_offset.checked_add(rows)?;
    }
    if file_row_offset != total_rows {
        return None;
    }

    Some(DenseNodeLayout {
        group_rows,
        group_pages,
        total_rows,
        pages_considered,
    })
}

/// Map requested ids to exact row ordinals after proving the canonical dense
/// layout. Any incomplete or surprising metadata fails closed to the
/// conservative predicate path.
fn dense_node_selection(
    metadata: &parquet::file::metadata::ParquetMetaData,
    key_leaf: usize,
    sorted_ids: &[u64],
) -> Option<DenseNodeSelection> {
    let DenseNodeLayout {
        group_rows,
        group_pages,
        total_rows,
        pages_considered,
    } = dense_node_layout(metadata, key_leaf)?;

    let max_id = u64::try_from(total_rows).ok()?;
    let ordinals: Vec<usize> = sorted_ids
        .iter()
        .copied()
        .filter(|&id| id != 0 && id <= max_id)
        .map(|id| usize::try_from(id - 1).ok())
        .collect::<Option<_>>()?;
    let mut selected_groups = Vec::new();
    let mut ranges = Vec::with_capacity(ordinals.len());
    let mut selected_pages = 0u64;
    let mut ordinal_cursor = 0usize;
    let mut file_start = 0usize;
    let mut retained_start = 0usize;

    for (group_idx, &rows) in group_rows.iter().enumerate() {
        let file_end = file_start.checked_add(rows)?;
        let first = ordinal_cursor;
        while ordinal_cursor < ordinals.len() && ordinals[ordinal_cursor] < file_end {
            ordinal_cursor += 1;
        }
        if first != ordinal_cursor {
            selected_groups.push(group_idx);
            let mut last_page = None;
            for &ordinal in &ordinals[first..ordinal_cursor] {
                let local = ordinal.checked_sub(file_start)?;
                let selected = retained_start.checked_add(local)?;
                ranges.push(selected..selected.checked_add(1)?);
                let page = group_pages[group_idx].partition_point(|&start| start <= local) - 1;
                if last_page != Some(page) {
                    selected_pages = selected_pages.checked_add(1)?;
                    last_page = Some(page);
                }
            }
            retained_start = retained_start.checked_add(rows)?;
        }
        file_start = file_end;
    }

    Some(DenseNodeSelection {
        row_groups: selected_groups,
        selection: parquet::arrow::arrow_reader::RowSelection::from_consecutive_ranges(
            ranges.into_iter(),
            retained_start,
        ),
        pages_considered,
        pages_selected: selected_pages,
        exact_rows_selected: u64::try_from(ordinals.len()).ok()?,
    })
}

fn filtered_keys_match(
    batches: &[RecordBatch],
    key_column: &str,
    expected: &std::collections::HashSet<u64>,
) -> bool {
    use arrow::array::Array as _;

    let mut actual = std::collections::HashSet::with_capacity(expected.len());
    let mut rows = 0usize;
    for batch in batches {
        let Some(column) = batch.column_by_name(key_column) else {
            return false;
        };
        let Some(ids) = column.as_any().downcast_ref::<arrow::array::UInt64Array>() else {
            return false;
        };
        rows = match rows.checked_add(ids.len()) {
            Some(rows) => rows,
            None => return false,
        };
        for row in 0..ids.len() {
            if ids.is_null(row) || !actual.insert(ids.value(row)) {
                return false;
            }
        }
    }
    rows == expected.len() && actual == *expected
}

/// Read a Parquet file keeping only rows whose `key_column` (UInt64) value is
/// in `ids`, with row-group pruning on the column's min/max statistics. `kind`
/// selects the [`io_stats`](crate::io_stats) counters: the >50% fallback is a
/// full read, the pushdown path a filtered read, of the named table.
#[allow(clippy::too_many_lines)]
fn read_parquet_filtered_u64(
    path: &Path,
    fallback_schema: SchemaRef,
    key_column: &str,
    ids: &std::collections::HashSet<u64>,
    kind: FilteredReadKind,
    observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    // Empty request or missing file: never open / one empty batch (contract
    // parity with `read_parquet_or_empty`).
    if ids.is_empty() || !path.exists() {
        return Ok(vec![RecordBatch::new_empty(fallback_schema)]);
    }
    read_parquet_filtered_u64_attempt(path, fallback_schema, key_column, ids, kind, observer, true)
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
fn read_parquet_filtered_u64_attempt(
    path: &Path,
    fallback_schema: SchemaRef,
    key_column: &str,
    ids: &std::collections::HashSet<u64>,
    kind: FilteredReadKind,
    observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
    allow_dense_node_selection: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    use parquet::arrow::ProjectionMask;
    use parquet::arrow::arrow_reader::{
        ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter,
    };
    use parquet::file::metadata::PageIndexPolicy;
    use parquet::file::statistics::Statistics;

    let mut observation = FilteredReadObservation::new(observer, kind);
    let file = File::open(path).map_err(|e| io_err(&e))?;
    // Optional, NOT required: with_page_index(true) errors on files lacking a
    // page index; Optional enables page-level skipping when one is present.
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .map_err(parquet_err)?;

    // Fallback: a large requested fraction makes the Parquet-level filter
    // overhead a net loss — read plainly, then trim in memory so the public
    // "only the requested ids" contract still holds.
    let total = builder.metadata().file_metadata().num_rows();
    let builder_row_groups = u64::try_from(builder.metadata().num_row_groups()).unwrap_or(u64::MAX);
    if total >= 0 && ids.len() as u64 * 2 > u64::try_from(total).unwrap_or(u64::MAX) {
        drop(builder);
        let batches = read_parquet_or_empty(path, fallback_schema.clone())?;
        // The fallback scanned the whole file before trimming, so record it as
        // a full read (its row count is the full file, not the trimmed result):
        // a fallback must not masquerade as a cheap filtered read.
        let scanned = total_rows(&batches);
        record_full(kind, scanned);
        observation.scanned(scanned);
        // Resolve the key column against the batches' ACTUAL schema (the
        // on-disk file's), not `fallback_schema`: a column-shifted file would
        // otherwise turn the index lookup into an out-of-bounds panic at
        // `RecordBatch::column` instead of a graceful error.
        let file_schema = batches
            .first()
            .map_or_else(|| fallback_schema.clone(), RecordBatch::schema);
        let key_idx = file_schema
            .index_of(key_column)
            .map_err(|e| DataFusionError::Execution(format!("filtered read: {e}")))?;
        let mut filtered = Vec::with_capacity(batches.len());
        for batch in &batches {
            let col = batch
                .column(key_idx)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("filtered read: key column not UInt64".into())
                })?;
            let mask: arrow::array::BooleanArray = {
                use arrow::array::Array as _;
                (0..col.len())
                    .map(|i| Some(!col.is_null(i) && ids.contains(&col.value(i))))
                    .collect()
            };
            filtered.push(
                arrow::compute::filter_record_batch(batch, &mask)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            );
        }
        if filtered.is_empty() {
            filtered.push(RecordBatch::new_empty(fallback_schema));
        }
        record_pruning(
            kind,
            &observation,
            crate::io_stats::FilteredReadPruning {
                strategy: crate::io_stats::FilteredReadStrategy::FullFallback,
                row_groups_considered: builder_row_groups,
                row_groups_selected: builder_row_groups,
                pages_considered: 0,
                pages_selected: 0,
                exact_rows_selected: 0,
                metadata_fallbacks: 0,
                validation_fallbacks: 0,
            },
        );
        observation.complete(total_rows(&filtered), true);
        return Ok(filtered);
    }

    // The key column's leaf index (flat schemas: leaf index == field index).
    let key_leaf = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|c| c.name() == key_column)
        .ok_or_else(|| {
            DataFusionError::Execution(format!("filtered read: no column {key_column}"))
        })?;

    let mut sorted: Vec<u64> = ids.iter().copied().collect();
    sorted.sort_unstable();
    let dense_requested =
        allow_dense_node_selection && kind == FilteredReadKind::Node && key_column == "node_id";
    let dense = dense_requested
        .then(|| dense_node_selection(builder.metadata(), key_leaf, &sorted))
        .flatten();
    let metadata_fallbacks = u64::from(dense_requested && dense.is_none());

    // Exact ordinal selection is node-only. Edges and noncanonical node files
    // retain the conservative row-group min/max behavior.
    let (keep, selection, mut pruning) = if let Some(dense) = dense {
        let selected_groups = u64::try_from(dense.row_groups.len()).unwrap_or(u64::MAX);
        (
            dense.row_groups,
            Some(dense.selection),
            crate::io_stats::FilteredReadPruning {
                strategy: crate::io_stats::FilteredReadStrategy::DenseRowSelection,
                row_groups_considered: builder_row_groups,
                row_groups_selected: selected_groups,
                pages_considered: dense.pages_considered,
                pages_selected: dense.pages_selected,
                exact_rows_selected: dense.exact_rows_selected,
                metadata_fallbacks: 0,
                validation_fallbacks: 0,
            },
        )
    } else {
        // Missing or non-Int64 statistics keep the group (never prune blind).
        let keep: Vec<usize> = builder
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .filter(|(_, rg)| match rg.column(key_leaf).statistics() {
                Some(Statistics::Int64(s)) => match (s.min_opt(), s.max_opt()) {
                    (Some(&min), Some(&max)) => {
                        let lo = u64::try_from(min).unwrap_or(0);
                        let hi = u64::try_from(max).unwrap_or(u64::MAX);
                        sorted.partition_point(|&x| x < lo) < sorted.partition_point(|&x| x <= hi)
                    }
                    _ => true,
                },
                _ => true,
            })
            .map(|(i, _)| i)
            .collect();
        let selected_groups = u64::try_from(keep.len()).unwrap_or(u64::MAX);
        (
            keep,
            None,
            crate::io_stats::FilteredReadPruning {
                strategy: crate::io_stats::FilteredReadStrategy::RowGroupPredicate,
                row_groups_considered: builder_row_groups,
                row_groups_selected: selected_groups,
                pages_considered: 0,
                pages_selected: 0,
                exact_rows_selected: 0,
                metadata_fallbacks,
                validation_fallbacks: 0,
            },
        )
    };
    let used_dense_selection = selection.is_some();

    // 2) Row filter on the key column within surviving groups.
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [key_leaf]);
    let owned: std::sync::Arc<std::collections::HashSet<u64>> = std::sync::Arc::new(ids.clone());
    let scan_observer = observer.cloned();
    let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
        use arrow::array::Array as _;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| {
                arrow::error::ArrowError::CastError("filtered read: key column not UInt64".into())
            })?;
        // The predicate only sees rows in pages the page index did not skip, so
        // this counts the decode-cost footprint (#838): flat for a clustered id
        // set, ~whole-file for a scattered one.
        let rows = total_rows(std::slice::from_ref(&batch));
        record_scanned(kind, rows);
        if let Some(observer) = &scan_observer {
            observer.rows_scanned(kind.into(), rows);
        }
        Ok((0..col.len())
            .map(|i| Some(!col.is_null(i) && owned.contains(&col.value(i))))
            .collect())
    });
    let builder = builder.with_row_groups(keep);
    let builder = if let Some(selection) = selection {
        builder.with_row_selection(selection)
    } else {
        builder
    };
    let reader = builder
        .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        .build()
        .map_err(parquet_err)?;
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<_, _>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    // Predicate-pushdown path: record the rows actually materialized after
    // row-group + row-filter pruning (the neighborhood-proportional cost #767
    // measures), whether or not any survived.
    let returned = total_rows(&batches);
    record_filtered(kind, returned);
    if used_dense_selection {
        let max_id = u64::try_from(total).unwrap_or(0);
        let expected: std::collections::HashSet<u64> = ids
            .iter()
            .copied()
            .filter(|&id| id != 0 && id <= max_id)
            .collect();
        if !filtered_keys_match(&batches, key_column, &expected) {
            pruning.validation_fallbacks = 1;
            record_pruning(kind, &observation, pruning);
            observation.complete(returned, false);
            return read_parquet_filtered_u64_attempt(
                path,
                fallback_schema,
                key_column,
                ids,
                kind,
                observer,
                false,
            );
        }
    }
    record_pruning(kind, &observation, pruning);
    observation.complete(returned, false);
    if batches.is_empty() {
        return Ok(vec![RecordBatch::new_empty(fallback_schema)]);
    }
    Ok(batches)
}

/// Attribute a full read of `rows` to the table named by `kind`.
fn record_full(kind: FilteredReadKind, rows: u64) {
    match kind {
        FilteredReadKind::Edge => crate::io_stats::record_edge_full_read(rows),
        FilteredReadKind::Node => crate::io_stats::record_node_full_read(rows),
    }
}

/// Attribute a filtered (predicate-pushdown) read of `rows` to `kind`'s table.
fn record_filtered(kind: FilteredReadKind, rows: u64) {
    match kind {
        FilteredReadKind::Edge => crate::io_stats::record_edge_filtered_read(rows),
        FilteredReadKind::Node => crate::io_stats::record_node_filtered_read(rows),
    }
}

/// Attribute `rows` evaluated by the pushdown predicate (the decode footprint
/// after page-index skipping) to `kind`'s table.
fn record_scanned(kind: FilteredReadKind, rows: u64) {
    match kind {
        FilteredReadKind::Edge => crate::io_stats::record_edge_scanned(rows),
        FilteredReadKind::Node => crate::io_stats::record_node_scanned(rows),
    }
}

/// Record aggregate pruning work globally and, when installed, against the
/// calling physical hop.
fn record_pruning(
    kind: FilteredReadKind,
    observation: &FilteredReadObservation,
    pruning: crate::io_stats::FilteredReadPruning,
) {
    if kind == FilteredReadKind::Node {
        crate::io_stats::record_node_pruning(pruning);
    }
    observation.pruning(pruning);
}

/// Read all node rows from `topology/nodes.parquet` in the project at `dir`.
///
/// Returns a single (possibly empty) [`RecordBatch`] with
/// [`TOPOLOGY_NODES_SCHEMA`]; a missing file yields an empty batch.
///
/// # Errors
/// Propagates Parquet / Arrow errors encountered while reading.
pub fn read_nodes(dir: &Path) -> Result<Vec<RecordBatch>, DataFusionError> {
    let path = dir.join("topology").join("nodes.parquet");
    let batches =
        normalize_topology_nodes(read_parquet_or_empty(&path, TOPOLOGY_NODES_SCHEMA.clone())?)?;
    crate::io_stats::record_node_full_read(total_rows(&batches));
    Ok(batches)
}

/// Like [`read_nodes`] but returns only rows whose `node_id` is in `node_ids` —
/// the traversal's lazy node-record read (#838): on an adjacency Hit only the
/// reached destination nodes' records are needed to project the destination
/// columns, not the whole node table. Canonical dense files use exact physical
/// row selection; legacy, gapped, or noncanonical files retain conservative
/// row-group pruning plus a membership predicate.
///
/// Contract parity with [`read_nodes`]: always at least one (possibly empty)
/// batch with [`TOPOLOGY_NODES_SCHEMA`]; an empty `node_ids` or a missing file
/// never opens the file.
///
/// # Errors
/// Same as [`read_nodes`], plus Parquet filter construction failures.
#[allow(clippy::implicit_hasher)]
pub fn read_nodes_filtered(
    dir: &Path,
    node_ids: &std::collections::HashSet<u64>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    read_nodes_filtered_observed(dir, node_ids, None)
}

/// [`read_nodes_filtered`] with optional aggregate-only operator attribution.
#[allow(clippy::implicit_hasher)]
#[doc(hidden)]
pub fn read_nodes_filtered_observed(
    dir: &Path,
    node_ids: &std::collections::HashSet<u64>,
    observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let path = dir.join("topology").join("nodes.parquet");
    normalize_topology_nodes(read_parquet_filtered_u64(
        &path,
        TOPOLOGY_NODES_SCHEMA.clone(),
        "node_id",
        node_ids,
        FilteredReadKind::Node,
        observer,
    )?)
}

/// Return the largest `edge_id` surrogate across every edge file under
/// `topology/edges/` (both typed `<rel>.parquet` and `_exploratory.parquet`),
/// or `0` if there are no edge files yet.
///
/// Used by [`GraphWriter`](crate::GraphWriter) to continue surrogate assignment
/// from the on-disk maximum when appending across separate write sessions.
///
/// # Errors
/// Propagates Parquet / Arrow errors encountered while reading an edge file.
pub(crate) fn max_edge_id(dir: &Path) -> Result<u64, DataFusionError> {
    use arrow::array::{Array, UInt64Array};

    let edges_dir = dir.join("topology").join("edges");
    let entries = match std::fs::read_dir(&edges_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(io_err(&e)),
    };

    let mut max = 0u64;
    for entry in entries {
        let path = entry.map_err(|e| io_err(&e))?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            continue;
        }
        // The `edge_id` column exists in both the typed and exploratory edge
        // schemas, so the on-disk file schema (discovered) reads it either way.
        let Some(schema) = discover_parquet_schema(&path) else {
            continue;
        };
        for batch in read_parquet_or_empty(&path, schema)? {
            if let Some(col) = batch.column_by_name("edge_id")
                && let Some(ids) = col.as_any().downcast_ref::<UInt64Array>()
            {
                for i in 0..ids.len() {
                    if !ids.is_null(i) {
                        max = max.max(ids.value(i));
                    }
                }
            }
        }
    }
    Ok(max)
}

/// Read `properties/<stem>.parquet` for the project at `dir`, discovering its
/// (dynamic) schema from the file. Returns an **empty `Vec`** when the file is
/// absent — so a caller decoding rows sees zero pre-existing property rows.
///
/// Production reads go through the staged-batch read-through in `writer`
/// (#792); the node-hydration path (`nodes(p)`, #1024) also reads through it.
///
/// # Errors
/// Propagates Parquet / Arrow errors encountered while reading.
pub fn read_properties(dir: &Path, stem: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
    let path = dir.join("properties").join(format!("{stem}.parquet"));
    match discover_parquet_schema(&path) {
        Some(schema) => read_parquet_or_empty(&path, schema),
        None => Ok(Vec::new()),
    }
}

/// Edge analogue of [`read_properties`]: read `edge_properties/<stem>.parquet`
/// (keyed by `edge_uuid`), discovering its dynamic schema from the file. Returns
/// an **empty `Vec`** when the file is absent.
///
/// # Errors
/// Propagates Parquet / Arrow errors encountered while reading.
pub fn read_edge_properties(dir: &Path, stem: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
    let path = dir.join("edge_properties").join(format!("{stem}.parquet"));
    match discover_parquet_schema(&path) {
        Some(schema) => read_parquet_or_empty(&path, schema),
        None => Ok(Vec::new()),
    }
}

/// Stems (relation names) of every `edge_properties/<stem>.parquet` under
/// `dir`, **sorted** so schema unions built from them are deterministic
/// (#1023). Empty when the directory is absent — a project with no persisted
/// edge properties.
#[must_use]
pub fn list_edge_property_stems(dir: &Path) -> Vec<String> {
    list_parquet_stems(&dir.join("edge_properties"))
}

/// Stems (entity type names, or `_untyped`) of every
/// `properties/<stem>.parquet` under `dir`, **sorted** for deterministic
/// schema unions (#1024). Empty when the directory is absent.
#[must_use]
pub fn list_property_stems(dir: &Path) -> Vec<String> {
    list_parquet_stems(&dir.join("properties"))
}

/// Sorted `<stem>` names of the `<stem>.parquet` files directly under `dir`.
fn list_parquet_stems(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut stems: Vec<String> = entries
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
                return None;
            }
            Some(path.file_stem()?.to_str()?.to_owned())
        })
        .collect();
    stems.sort();
    stems
}

// ---------------------------------------------------------------------------
// TopologyNodeTable
// ---------------------------------------------------------------------------

/// [`TableProvider`] for `topology/nodes.parquet`.
#[derive(Debug, Clone)]
pub struct TopologyNodeTable {
    path: PathBuf,
}

impl TopologyNodeTable {
    /// Create a table backed by the given Parquet file path.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl TableProvider for TopologyNodeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        TOPOLOGY_NODES_SCHEMA.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batches = normalize_topology_nodes(read_parquet_or_empty(
            &self.path,
            TOPOLOGY_NODES_SCHEMA.clone(),
        )?)?;
        let mem = MemTable::try_new(TOPOLOGY_NODES_SCHEMA.clone(), vec![batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}

// ---------------------------------------------------------------------------
// TypedEdgeTable
// ---------------------------------------------------------------------------

/// [`TableProvider`] for `topology/edges/TYPENAME.parquet`.
#[derive(Debug, Clone)]
pub struct TypedEdgeTable {
    path: PathBuf,
    schema: SchemaRef,
}

impl TypedEdgeTable {
    /// Open the edge table for `rel_type_name` inside `dir`.
    ///
    /// - `"_exploratory"` → schema includes `rel_type_name` column
    /// - any other name → [`TYPED_EDGE_SCHEMA`]
    #[must_use]
    pub fn open(dir: &Path, rel_type_name: &str) -> Self {
        let path = dir
            .join("topology")
            .join("edges")
            .join(format!("{rel_type_name}.parquet"));
        let schema = if rel_type_name == "_exploratory" {
            EXPLORATORY_EDGE_SCHEMA.clone()
        } else {
            TYPED_EDGE_SCHEMA.clone()
        };
        Self { path, schema }
    }
}

#[async_trait]
impl TableProvider for TypedEdgeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batches = read_parquet_or_empty(&self.path, self.schema.clone())?;
        let mem = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}

// ---------------------------------------------------------------------------
// UnionEdgeTable
// ---------------------------------------------------------------------------

/// [`TableProvider`] over the union of every relation's edge file (#823) — the
/// scan source for an **untyped** single-hop pattern (`(a)-[]->(b)`) in a typed
/// project, where the `_exploratory` table does not exist. Materializes
/// [`read_edges_union`] into a [`MemTable`]; the schema is always
/// [`EXPLORATORY_EDGE_SCHEMA`] (each row tagged with its source relation), so it
/// is a drop-in for the exploratory edge scan the untyped lowering already uses.
#[derive(Debug, Clone)]
pub struct UnionEdgeTable {
    dir: PathBuf,
}

impl UnionEdgeTable {
    /// Open a union edge table over `dir`'s `topology/edges/`.
    #[must_use]
    pub fn open(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }
}

#[async_trait]
impl TableProvider for UnionEdgeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        EXPLORATORY_EDGE_SCHEMA.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batches = read_edges_union(&self.dir, None, None)?;
        let mem = MemTable::try_new(EXPLORATORY_EDGE_SCHEMA.clone(), vec![batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}

// ---------------------------------------------------------------------------
// PropertyTable
// ---------------------------------------------------------------------------

/// [`TableProvider`] for `properties/ENTITY_TYPE.parquet`.
#[derive(Debug, Clone)]
pub struct PropertyTable {
    path: PathBuf,
    schema: SchemaRef,
}

impl PropertyTable {
    /// Open a property table.
    ///
    /// If the file does not yet exist scans return an empty batch with the
    /// correct schema.
    #[must_use]
    pub fn open(dir: &Path, entity_type: &str, schema: SchemaRef) -> Self {
        let path = dir
            .join("properties")
            .join(format!("{entity_type}.parquet"));
        Self { path, schema }
    }

    /// Open a property table for `stem` (an entity type name, or `"_untyped"`),
    /// discovering the column schema from the Parquet file on disk.
    ///
    /// The exploratory `_untyped.parquet` file's schema is inferred at write
    /// time from the observed property literals, so the read path cannot know it
    /// statically — it must be read back from the file. When the file does not
    /// exist yet, falls back to [`PROPERTY_BASE_SCHEMA`] (just `node_uuid`), so a
    /// join against an as-yet-unwritten property table yields zero property rows
    /// rather than an error.
    #[must_use]
    pub fn open_discovered(dir: &Path, stem: &str) -> Self {
        let path = dir.join("properties").join(format!("{stem}.parquet"));
        let schema = discover_parquet_schema(&path)
            .unwrap_or_else(|| crate::schemas::PROPERTY_BASE_SCHEMA.clone());
        Self { path, schema }
    }

    /// The property column schema (including the `node_uuid` join key).
    #[must_use]
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// EdgePropertyTable
// ---------------------------------------------------------------------------

/// [`TableProvider`] for `edge_properties/REL_TYPE.parquet` (#784).
///
/// The edge analogue of [`PropertyTable`], keyed by `edge_uuid` and read from
/// the dedicated `edge_properties/` directory so a relation type cannot collide
/// with a same-named node label under `properties/`.
#[derive(Debug, Clone)]
pub struct EdgePropertyTable {
    path: PathBuf,
    schema: SchemaRef,
}

impl EdgePropertyTable {
    /// Open an edge-property table for `rel_type`, discovering the column schema
    /// from the Parquet file on disk.
    ///
    /// The per-relation schema is inferred at write time from the observed
    /// property literals, so the read path reads it back from the file. When the
    /// file does not exist yet, falls back to [`EDGE_PROPERTY_BASE_SCHEMA`] (just
    /// `edge_uuid`), so a join against an as-yet-unwritten edge-property table
    /// yields zero property rows rather than an error.
    #[must_use]
    pub fn open_discovered(dir: &Path, rel_type: &str) -> Self {
        let path = dir
            .join("edge_properties")
            .join(format!("{rel_type}.parquet"));
        let schema = discover_parquet_schema(&path)
            .unwrap_or_else(|| crate::schemas::EDGE_PROPERTY_BASE_SCHEMA.clone());
        Self { path, schema }
    }

    /// The property column schema (including the `edge_uuid` join key).
    #[must_use]
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl TableProvider for EdgePropertyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batches = read_parquet_or_empty(&self.path, self.schema.clone())?;
        let mem = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}

/// Read just the Arrow schema of a Parquet file, or `None` if it is absent or
/// unreadable.
pub(crate) fn discover_parquet_schema(path: &Path) -> Option<SchemaRef> {
    let file = File::open(path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
    Some(builder.schema().clone())
}

#[async_trait]
impl TableProvider for PropertyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batches = read_parquet_or_empty(&self.path, self.schema.clone())?;
        let mem = MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}

// ---------------------------------------------------------------------------
// GraphSchema — inner schema provider
// ---------------------------------------------------------------------------

struct GraphSchema {
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl fmt::Debug for GraphSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphSchema")
            .field("table_names", &self.table_names())
            .finish()
    }
}

impl GraphSchema {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    fn register(&mut self, name: impl Into<String>, table: Arc<dyn TableProvider>) {
        self.tables.insert(name.into(), table);
    }
}

#[async_trait]
impl SchemaProvider for GraphSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

// ---------------------------------------------------------------------------
// GraphCatalog
// ---------------------------------------------------------------------------

/// DataFusion [`CatalogProvider`] for a GraphForge project directory.
///
/// Exposes topology, edge, and property tables under the `"graph"` schema.
/// Construct via [`GraphCatalog::open`].
pub struct GraphCatalog {
    schema: Arc<GraphSchema>,
    /// Reverse map `PropId.0` → property name, merged from the ontology and the
    /// runtime catalog at [`open`](Self::open) time. The relational lowering
    /// layer borrows it to resolve numeric `PropertyAccess` IDs to real column
    /// names without re-plumbing the ontology/runtime catalog separately.
    prop_names: HashMap<u32, String>,
    /// Reverse map `TypeId.0` → relation-type name, merged from the ontology and
    /// the runtime catalog. Lets the lowering layer resolve a `TypedEdgeScan`'s
    /// relation name in exploratory mode (where the ontology map is empty).
    rel_names: HashMap<u32, String>,
    /// Reverse map `TypeId.0` → entity-type (node label) name, from the runtime
    /// catalog. Lets the lowering layer render a real label for an unlabelled
    /// `MATCH (n) RETURN n` in exploratory mode (where the ontology map is
    /// empty) by resolving the node's stored `type_id` (#889).
    label_names: HashMap<u32, String>,
}

impl fmt::Debug for GraphCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphCatalog")
            .field("schema_names", &self.schema_names())
            .finish()
    }
}

impl GraphCatalog {
    /// Open a GraphForge project directory as a DataFusion catalog.
    ///
    /// - `dir`: project root (contains `topology/`, `properties/`, etc.)
    /// - `ontology`: compiled ontology, or `None` in exploratory mode
    /// - `runtime_catalog`: runtime type catalog for exploratory mode
    ///
    /// # Errors
    ///
    /// Propagates I/O errors encountered while registering tables.
    pub fn open(
        dir: &Path,
        ontology: Option<&OntologyHandle>,
        runtime_catalog: &RuntimeCatalog,
    ) -> Result<Self, DataFusionError> {
        let mut schema = GraphSchema::new();

        // ---- topology nodes ----
        let nodes_path = dir.join("topology").join("nodes.parquet");
        schema.register(
            "topology_nodes",
            Arc::new(TopologyNodeTable::new(nodes_path)),
        );

        // ---- typed edge tables ----
        if let Some(handle) = ontology {
            for rel_name in handle.relation_type_names() {
                schema.register(
                    format!("edges_{rel_name}"),
                    Arc::new(TypedEdgeTable::open(dir, rel_name)),
                );
            }
        } else {
            // No ontology. Exploratory-written edges all land in the single
            // `_exploratory.parquet` (tagged with `rel_type_name`); register that
            // catch-all. For runtime relation types, register a per-relation
            // `edges_<rel>` table ONLY when its typed file actually exists on disk
            // (e.g. data written in strict/advisory mode then reloaded with just a
            // runtime catalog). Registering `edges_<rel>` for a relation whose
            // data is really in `_exploratory.parquet` would make the read path
            // scan a non-existent typed file and return 0 rows.
            schema.register(
                "edges__exploratory",
                Arc::new(TypedEdgeTable::open(dir, "_exploratory")),
            );
            for rel_name in runtime_catalog.relation_types() {
                let typed_path = dir
                    .join("topology")
                    .join("edges")
                    .join(format!("{rel_name}.parquet"));
                if typed_path.exists() {
                    schema.register(
                        format!("edges_{rel_name}"),
                        Arc::new(TypedEdgeTable::open(dir, rel_name)),
                    );
                }
            }
        }

        // Always register the exploratory fallback (advisory mode uses it too).
        if !schema.table_exist("edges__exploratory") {
            schema.register(
                "edges__exploratory",
                Arc::new(TypedEdgeTable::open(dir, "_exploratory")),
            );
        }

        // ---- property tables ----
        register_property_tables(dir, ontology, &mut schema);

        // ---- name maps (for read-path property + relation resolution) ----
        let prop_names = build_prop_names(ontology, runtime_catalog);
        let rel_names = build_rel_names(runtime_catalog);
        let label_names = build_label_names(runtime_catalog);

        Ok(Self {
            schema: Arc::new(schema),
            prop_names,
            rel_names,
            label_names,
        })
    }

    /// Reverse map `PropId.0` → property name (ontology + runtime catalog),
    /// used by the relational lowering layer to resolve `PropertyAccess`.
    #[must_use]
    pub fn prop_names(&self) -> &HashMap<u32, String> {
        &self.prop_names
    }

    /// Reverse map `RuntimeTypeId.0` → relation-type name from the runtime
    /// catalog. The relational lowerer tags these keys before merging them with
    /// ontology TypeIds so the two zero-based ID spaces cannot collide.
    #[must_use]
    pub fn rel_names(&self) -> &HashMap<u32, String> {
        &self.rel_names
    }

    /// Reverse map `TypeId.0` → entity-type (node label) name, from the runtime
    /// catalog. Used by the relational lowering layer to render a node value's
    /// label for an unlabelled match — including in exploratory mode, where the
    /// ontology map is empty (#889).
    #[must_use]
    pub fn label_names(&self) -> &HashMap<u32, String> {
        &self.label_names
    }
}

/// Build the `PropId.0 → name` map for resolving `PropertyAccess`.
///
/// The binder interns every observed property into the [`RuntimeCatalog`] and
/// emits its runtime `PropId` (it does **not** emit ontology property IDs — they
/// live in a separate ID space). So the runtime catalog is the single
/// authoritative source for `PropId → name`, in all ontology modes.
fn build_prop_names(
    _ontology: Option<&OntologyHandle>,
    runtime_catalog: &RuntimeCatalog,
) -> HashMap<u32, String> {
    runtime_catalog
        .property_names()
        .map(|(id, name)| (id.0, name.to_owned()))
        .collect()
}

/// Build the `TypeId.0 → relation-name` map from the runtime catalog.
///
/// Only the runtime-catalog side is needed here: the binder resolves relation
/// types ontology-first (so an ontology-sourced `TypeId` is already covered by
/// the lowerer's ontology map) and falls back to the `RuntimeCatalog` only in
/// exploratory mode (or advisory misses) — the case this map fills.
fn build_rel_names(runtime_catalog: &RuntimeCatalog) -> HashMap<u32, String> {
    runtime_catalog
        .relation_type_names_with_ids()
        .map(|(id, name)| (id.0, name.to_owned()))
        .collect()
}

/// Build the `TypeId.0 → label-name` map from the runtime catalog.
///
/// As with [`build_rel_names`], only the runtime-catalog side is needed: the
/// binder resolves labels ontology-first, so ontology-sourced label `TypeId`s
/// are already covered by the lowerer's ontology map; this fills the exploratory
/// case (no ontology), letting an unlabelled `MATCH (n) RETURN n` recover the
/// node's label name from its stored `type_id` (#889).
fn build_label_names(runtime_catalog: &RuntimeCatalog) -> HashMap<u32, String> {
    runtime_catalog
        .entity_type_names_with_ids()
        .map(|(id, name)| (id.0, name.to_owned()))
        .collect()
}

impl CatalogProvider for GraphCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["graph".to_owned()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "graph" {
            Some(self.schema.clone())
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Property table registration helper
// ---------------------------------------------------------------------------

fn register_property_tables(
    dir: &Path,
    ontology: Option<&OntologyHandle>,
    schema: &mut GraphSchema,
) {
    if let Some(handle) = ontology {
        for (entity_name, prop_defs) in handle.entity_property_defs() {
            let prop_schema = Arc::new(property_schema(entity_name, &prop_defs));
            schema.register(
                format!("properties_{entity_name}"),
                Arc::new(PropertyTable::open(dir, entity_name, prop_schema)),
            );
        }
    } else {
        // Exploratory: properties are written to a single `_untyped.parquet`
        // whose column schema is inferred at write time, so register it with the
        // schema discovered from disk (just `node_uuid` until it is written).
        schema.register(
            "properties__untyped",
            Arc::new(PropertyTable::open_discovered(dir, "_untyped")),
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, TimestampMicrosecondArray, UInt32Array, UInt64Array};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    fn write_nodes_parquet(path: &Path) {
        let uuid_bytes: Vec<u8> = vec![1u8; 16];
        let uuid_arr =
            FixedSizeBinaryArray::try_from_iter(std::iter::once(uuid_bytes.clone())).unwrap();
        let ts =
            TimestampMicrosecondArray::from(vec![0i64]).with_timezone_opt(Some(Arc::from("UTC")));
        let labels = arrow::array::ListArray::new(
            Arc::new(Field::new("item", DataType::UInt32, false)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(UInt32Array::from(vec![0u32])),
            None,
        );

        let batch = RecordBatch::try_new(
            TOPOLOGY_NODES_SCHEMA.clone(),
            vec![
                Arc::new(uuid_arr),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(labels),
                Arc::new(ts.clone()),
                Arc::new(ts),
            ],
        )
        .unwrap();

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(
            file,
            TOPOLOGY_NODES_SCHEMA.clone(),
            Some(WriterProperties::builder().build()),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn legacy_scalar_node_labels_normalize_to_singleton_sets() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("topology")).unwrap();
        let old_schema = Arc::new(Schema::new(vec![
            crate::schemas::uuid_field("node_uuid"),
            crate::schemas::id_field("node_id"),
            Field::new("type_id", DataType::UInt32, false),
            crate::schemas::ts_field("created_at"),
            crate::schemas::ts_field("updated_at"),
        ]));
        let uuid = FixedSizeBinaryArray::try_from_iter([vec![1u8; 16]].into_iter()).unwrap();
        let ts =
            TimestampMicrosecondArray::from(vec![0i64]).with_timezone_opt(Some(Arc::from("UTC")));
        let legacy = RecordBatch::try_new(
            old_schema,
            vec![
                Arc::new(uuid),
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(UInt32Array::from(vec![7])),
                Arc::new(ts.clone()),
                Arc::new(ts),
            ],
        )
        .unwrap();

        let file = File::create(dir.path().join("topology/nodes.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, legacy.schema(), None).unwrap();
        writer.write(&legacy).unwrap();
        writer.close().unwrap();

        let normalized = read_nodes(dir.path()).unwrap();
        assert_eq!(normalized[0].schema(), TOPOLOGY_NODES_SCHEMA.clone());
        let labels = normalized[0]
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let values = labels.value(0);
        let values = values.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(values.values(), &[7]);
    }

    fn write_edge_parquet(path: &Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let fsb = |v: Vec<u8>| FixedSizeBinaryArray::try_from_iter(std::iter::once(v)).unwrap();
        let ts =
            TimestampMicrosecondArray::from(vec![0i64]).with_timezone_opt(Some(Arc::from("UTC")));

        let batch = RecordBatch::try_new(
            TYPED_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(fsb(vec![2u8; 16])),
                Arc::new(fsb(vec![1u8; 16])),
                Arc::new(fsb(vec![3u8; 16])),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(UInt64Array::from(vec![2u64])),
                Arc::new(ts),
            ],
        )
        .unwrap();

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(
            file,
            TYPED_EDGE_SCHEMA.clone(),
            Some(WriterProperties::builder().build()),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn topology_node_table_scan_returns_rows() {
        let dir = TempDir::new().unwrap();
        let nodes_dir = dir.path().join("topology");
        std::fs::create_dir_all(&nodes_dir).unwrap();
        let path = nodes_dir.join("nodes.parquet");
        write_nodes_parquet(&path);

        let table = TopologyNodeTable::new(path);
        let ctx = SessionContext::new();
        ctx.register_table("nodes", Arc::new(table)).unwrap();
        let df = ctx.sql("SELECT node_id FROM nodes").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn topology_node_table_missing_file_returns_empty() {
        let table = TopologyNodeTable::new(PathBuf::from("/nonexistent/nodes.parquet"));
        let ctx = SessionContext::new();
        ctx.register_table("nodes", Arc::new(table)).unwrap();
        let df = ctx.sql("SELECT node_id FROM nodes").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn typed_edge_table_scan_returns_rows() {
        let dir = TempDir::new().unwrap();
        let edge_path = dir
            .path()
            .join("topology")
            .join("edges")
            .join("KNOWS.parquet");
        write_edge_parquet(&edge_path);

        let table = TypedEdgeTable::open(dir.path(), "KNOWS");
        let ctx = SessionContext::new();
        ctx.register_table("edges", Arc::new(table)).unwrap();
        let df = ctx.sql("SELECT src_id, dst_id FROM edges").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn typed_edge_table_exploratory_has_rel_type_name_column() {
        let table = TypedEdgeTable::open(Path::new("/nonexistent"), "_exploratory");
        let schema = table.schema();
        assert!(
            schema.field_with_name("rel_type_name").is_ok(),
            "exploratory schema must have rel_type_name"
        );
    }

    #[tokio::test]
    async fn union_edge_table_scan_unions_all_relations() {
        let dir = TempDir::new().unwrap();
        let edges = dir.path().join("topology").join("edges");
        write_typed_edge(&edges.join("KNOWS.parquet"), 1, 1, 2);
        write_typed_edge(&edges.join("OWNS.parquet"), 2, 2, 3);

        let table = UnionEdgeTable::open(dir.path());
        assert_eq!(table.schema(), EXPLORATORY_EDGE_SCHEMA.clone());
        let ctx = SessionContext::new();
        ctx.register_table("edges", Arc::new(table)).unwrap();
        let df = ctx
            .sql("SELECT edge_id, rel_type_name FROM edges ORDER BY edge_id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(row_count(&batches), 2, "both relations' edges unioned");
    }

    #[tokio::test]
    async fn property_table_missing_file_returns_empty_with_correct_schema() {
        let schema = Arc::new(property_schema("Person", &[]));
        let table = PropertyTable::open(Path::new("/nonexistent"), "Person", schema.clone());
        let ctx = SessionContext::new();
        ctx.register_table("props", Arc::new(table)).unwrap();
        let df = ctx.sql("SELECT node_uuid FROM props").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn graph_catalog_open_exploratory_registers_tables() {
        let dir = TempDir::new().unwrap();
        let catalog = RuntimeCatalog::new();
        let gc = GraphCatalog::open(dir.path(), None, &catalog).unwrap();
        let schema = gc.schema("graph").unwrap();
        let names = schema.table_names();
        assert!(
            names.contains(&"topology_nodes".to_owned()),
            "got {names:?}"
        );
        assert!(
            names.contains(&"edges__exploratory".to_owned()),
            "got {names:?}"
        );
    }

    #[test]
    fn graph_catalog_schema_names() {
        let dir = TempDir::new().unwrap();
        let catalog = RuntimeCatalog::new();
        let gc = GraphCatalog::open(dir.path(), None, &catalog).unwrap();
        assert_eq!(gc.schema_names(), vec!["graph"]);
    }

    // -----------------------------------------------------------------------
    // Direct readers (read_edges / read_nodes) — #580
    // -----------------------------------------------------------------------

    fn row_count(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    #[test]
    fn read_edges_strict_returns_typed_rows() {
        let dir = TempDir::new().unwrap();
        write_edge_parquet(
            &dir.path()
                .join("topology")
                .join("edges")
                .join("KNOWS.parquet"),
        );

        let batches = read_edges(dir.path(), "KNOWS", OntologyMode::Strict).unwrap();
        assert_eq!(row_count(&batches), 1);
        // Strict mode reads the typed schema (no rel_type_name column).
        assert_eq!(batches[0].schema(), TYPED_EDGE_SCHEMA.clone());
        assert!(
            batches[0]
                .schema()
                .field_with_name("rel_type_name")
                .is_err()
        );
    }

    #[test]
    fn read_edges_rejects_path_traversal_rel_name() {
        let dir = TempDir::new().unwrap();
        for bad in ["../secret", "a/b", "..", "/etc/passwd"] {
            let err = read_edges(dir.path(), bad, OntologyMode::Strict).unwrap_err();
            assert!(
                err.to_string().contains("invalid relation name"),
                "expected rejection for {bad:?}, got: {err}"
            );
        }
        // Exploratory mode uses a fixed stem, so a traversal-looking rel_name is
        // harmless (never reaches the path) — it must NOT error.
        assert!(read_edges(dir.path(), "../secret", OntologyMode::Exploratory).is_ok());
    }

    #[test]
    fn read_edges_missing_file_returns_empty_typed_batch() {
        let dir = TempDir::new().unwrap();
        // No edge file written.
        let batches = read_edges(dir.path(), "KNOWS", OntologyMode::Strict).unwrap();
        assert_eq!(row_count(&batches), 0);
        assert_eq!(batches[0].schema(), TYPED_EDGE_SCHEMA.clone());
    }

    #[test]
    fn read_edges_exploratory_uses_exploratory_file_and_schema() {
        let dir = TempDir::new().unwrap();
        // Exploratory edges live in `_exploratory.parquet`; a typed `KNOWS.parquet`
        // must be ignored in this mode.
        write_edge_parquet(
            &dir.path()
                .join("topology")
                .join("edges")
                .join("KNOWS.parquet"),
        );

        let batches = read_edges(dir.path(), "KNOWS", OntologyMode::Exploratory).unwrap();
        // The exploratory file does not exist → empty batch with the exploratory
        // schema (which carries rel_type_name), NOT the typed KNOWS rows.
        assert_eq!(row_count(&batches), 0);
        assert_eq!(batches[0].schema(), EXPLORATORY_EDGE_SCHEMA.clone());
        assert!(batches[0].schema().field_with_name("rel_type_name").is_ok());
    }

    // -----------------------------------------------------------------------
    // Untyped wildcard union read (#823)
    // -----------------------------------------------------------------------

    /// Write a one-row typed edge file with the given surrogates.
    fn write_typed_edge(path: &Path, edge_id: u64, src_id: u64, dst_id: u64) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let fsb = |v: Vec<u8>| FixedSizeBinaryArray::try_from_iter(std::iter::once(v)).unwrap();
        // A 16-byte uuid from an id, non-panicking for any u64 (the tests only
        // assert on edge_id/rel_type_name, but keep the helper id-range-safe).
        let uuid = |id: u64| {
            let mut b = [0u8; 16];
            b[..8].copy_from_slice(&id.to_le_bytes());
            b.to_vec()
        };
        let ts =
            TimestampMicrosecondArray::from(vec![0i64]).with_timezone_opt(Some(Arc::from("UTC")));
        let batch = RecordBatch::try_new(
            TYPED_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(fsb(uuid(edge_id))),
                Arc::new(fsb(uuid(src_id))),
                Arc::new(fsb(uuid(dst_id))),
                Arc::new(UInt64Array::from(vec![edge_id])),
                Arc::new(UInt64Array::from(vec![src_id])),
                Arc::new(UInt64Array::from(vec![dst_id])),
                Arc::new(ts),
            ],
        )
        .unwrap();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, TYPED_EDGE_SCHEMA.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// `(edge_id, rel_type_name)` pairs from EXPLORATORY-schema batches.
    fn edge_rel_pairs(batches: &[RecordBatch]) -> Vec<(u64, String)> {
        use arrow::array::{StringArray, UInt64Array};
        let mut out = Vec::new();
        for b in batches {
            let eids = b.column(3).as_any().downcast_ref::<UInt64Array>().unwrap();
            let rels = b
                .column_by_name("rel_type_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.push((eids.value(i), rels.value(i).to_owned()));
            }
        }
        out.sort();
        out
    }

    #[test]
    fn read_edges_strict_wildcard_unions_all_relations() {
        let dir = TempDir::new().unwrap();
        let edges = dir.path().join("topology").join("edges");
        write_typed_edge(&edges.join("KNOWS.parquet"), 1, 1, 2);
        write_typed_edge(&edges.join("OWNS.parquet"), 2, 2, 3);

        let batches = read_edges(dir.path(), "*", OntologyMode::Strict).unwrap();
        // Union schema carries rel_type_name, each row tagged with its file stem.
        assert_eq!(batches[0].schema(), EXPLORATORY_EDGE_SCHEMA.clone());
        assert_eq!(
            edge_rel_pairs(&batches),
            vec![(1, "KNOWS".to_owned()), (2, "OWNS".to_owned())]
        );
    }

    #[test]
    fn read_edges_filtered_strict_wildcard_unions_traversed_ids() {
        let dir = TempDir::new().unwrap();
        let edges = dir.path().join("topology").join("edges");
        write_typed_edge(&edges.join("KNOWS.parquet"), 1, 1, 2);
        write_typed_edge(&edges.join("OWNS.parquet"), 2, 2, 3);

        let want: std::collections::HashSet<u64> = [2].into_iter().collect();
        let one = read_edges_filtered(dir.path(), "*", OntologyMode::Strict, &want).unwrap();
        assert_eq!(edge_rel_pairs(&one), vec![(2, "OWNS".to_owned())]);

        let both: std::collections::HashSet<u64> = [1, 2].into_iter().collect();
        let two = read_edges_filtered(dir.path(), "*", OntologyMode::Strict, &both).unwrap();
        assert_eq!(
            edge_rel_pairs(&two),
            vec![(1, "KNOWS".to_owned()), (2, "OWNS".to_owned())]
        );
    }

    #[test]
    fn read_edges_strict_wildcard_empty_dir_is_one_empty_exploratory_batch() {
        let dir = TempDir::new().unwrap();
        let batches = read_edges(dir.path(), "*", OntologyMode::Strict).unwrap();
        assert_eq!(row_count(&batches), 0);
        assert_eq!(batches[0].schema(), EXPLORATORY_EDGE_SCHEMA.clone());
    }

    #[test]
    fn read_nodes_returns_rows_and_empty_when_absent() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("topology")).unwrap();

        // Missing file first → empty batch with the topology schema.
        let empty = read_nodes(dir.path()).unwrap();
        assert_eq!(row_count(&empty), 0);
        assert_eq!(empty[0].schema(), TOPOLOGY_NODES_SCHEMA.clone());

        // Then write one node and read it back.
        write_nodes_parquet(&dir.path().join("topology").join("nodes.parquet"));
        let batches = read_nodes(dir.path()).unwrap();
        assert_eq!(row_count(&batches), 1);
        assert_eq!(batches[0].schema(), TOPOLOGY_NODES_SCHEMA.clone());
    }
}
