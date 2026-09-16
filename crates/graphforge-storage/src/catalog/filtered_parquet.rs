//! Filtered topology reads, Parquet pruning, and read observation.

use super::admitted_parquet;
use super::io_err;
use super::normalize_topology_nodes;
use super::parquet_err;
use super::read_edges_union;
use super::read_edges_union_paths;
use super::read_parquet_or_empty;
use super::read_parquet_required;
use super::total_rows;
use crate::schemas::EXPLORATORY_EDGE_SCHEMA;
use crate::schemas::TOPOLOGY_NODES_SCHEMA;
use crate::schemas::TYPED_EDGE_SCHEMA;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use graphforge_core::OntologyMode;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Like [`read_edges`](super::read_edges) but returns only rows whose `edge_id` is in
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
/// [`read_edges`](super::read_edges): always at least one (possibly empty) batch with the
/// mode-appropriate schema; a missing file yields an empty batch.
///
/// # Errors
/// Same as [`read_edges`](super::read_edges), plus Parquet filter construction failures.
#[allow(clippy::implicit_hasher)]
pub fn read_edges_filtered(
    dir: &Path,
    rel_name: &str,
    mode: OntologyMode,
    edge_ids: &std::collections::HashSet<u64>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    read_edges_filtered_observed(dir, rel_name, mode, edge_ids, None)
}

/// Filter edge rows using an explicitly admitted semantic route inventory.
#[allow(clippy::implicit_hasher)]
pub fn read_edges_filtered_from_inventory(
    inventory: &crate::AuthenticatedPropertyInventory,
    rel_name: &str,
    mode: OntologyMode,
    edge_ids: &std::collections::HashSet<u64>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    read_edges_filtered_observed_from_inventory(inventory, rel_name, mode, edge_ids, None)
}

/// Filter admitted edge rows with aggregate-only operator attribution.
#[allow(clippy::implicit_hasher)]
#[doc(hidden)]
pub fn read_edges_filtered_observed_from_inventory(
    inventory: &crate::AuthenticatedPropertyInventory,
    rel_name: &str,
    mode: OntologyMode,
    edge_ids: &std::collections::HashSet<u64>,
    observer: Option<&Arc<dyn crate::io_stats::FilteredReadObserver>>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if rel_name == "*" && matches!(mode, OntologyMode::Advisory | OntologyMode::Strict) {
        return read_edges_union_paths(inventory.edge_files(None), Some(edge_ids), observer, true);
    }
    let (route, schema) = match mode {
        OntologyMode::Exploratory => ("_exploratory", EXPLORATORY_EDGE_SCHEMA.clone()),
        OntologyMode::Advisory | OntologyMode::Strict => (rel_name, TYPED_EDGE_SCHEMA.clone()),
    };
    let mut batches = Vec::new();
    for (_, path) in inventory.edge_files(Some(route)) {
        batches.extend(read_required_edge_filtered(
            &path,
            Arc::clone(&schema),
            edge_ids,
            observer,
            None,
        )?);
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema));
    }
    Ok(batches)
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
    let mut batches = Vec::new();
    for (_, path) in crate::mutator::edge_parquet_files(dir, Some(stem))
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
    {
        batches.extend(read_parquet_filtered_u64(
            &path,
            schema.clone(),
            "edge_id",
            edge_ids,
            FilteredReadKind::Edge,
            observer,
        )?);
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema));
    }
    Ok(batches)
}

/// Filter edge topology by `edge_id` while physically decoding only the
/// requested canonical columns plus the join key.
#[allow(clippy::implicit_hasher)]
#[doc(hidden)]
pub fn read_edges_filtered_projected_observed(
    dir: &Path,
    rel_name: &str,
    mode: OntologyMode,
    edge_ids: &std::collections::HashSet<u64>,
    projection: &[usize],
    observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if rel_name == "*" && matches!(mode, OntologyMode::Advisory | OntologyMode::Strict) {
        // Union normalization synthesizes rel_type_name. Normalize first, then
        // shape the result; individual typed files still avoid node reads.
        return read_edges_union(dir, Some(edge_ids), observer).and_then(|batches| {
            project_batches_with_key(batches, &EXPLORATORY_EDGE_SCHEMA, projection, "edge_id")
        });
    }
    if matches!(mode, OntologyMode::Advisory | OntologyMode::Strict) {
        let mut components = Path::new(rel_name).components();
        if !matches!(components.next(), Some(std::path::Component::Normal(_)))
            || components.next().is_some()
        {
            return Err(DataFusionError::Execution(format!(
                "invalid relation name {rel_name:?}: must be a plain file stem"
            )));
        }
    }
    let (stem, schema) = match mode {
        OntologyMode::Exploratory => ("_exploratory", EXPLORATORY_EDGE_SCHEMA.clone()),
        OntologyMode::Advisory | OntologyMode::Strict => (rel_name, TYPED_EDGE_SCHEMA.clone()),
    };
    let mut batches = Vec::new();
    for (_, path) in crate::mutator::edge_parquet_files(dir, Some(stem))
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
    {
        let file_schema = admitted_parquet(&path)?.schema().clone();
        if file_schema.fields() != schema.fields() {
            return Err(DataFusionError::Execution(format!(
                "projected edge read requires canonical schema: {}",
                path.display()
            )));
        }
        batches.extend(read_parquet_filtered_u64_projected(
            &path,
            schema.clone(),
            "edge_id",
            edge_ids,
            FilteredReadKind::Edge,
            observer,
            projection,
        )?);
    }
    if batches.is_empty() {
        return project_batches_with_key(Vec::new(), &schema, projection, "edge_id");
    }
    Ok(batches)
}

/// Project and filter admitted edge fragments using their semantic route authority.
#[allow(clippy::implicit_hasher)]
pub fn read_edges_filtered_projected_from_inventory(
    inventory: &crate::AuthenticatedPropertyInventory,
    rel_name: &str,
    mode: OntologyMode,
    edge_ids: &std::collections::HashSet<u64>,
    projection: &[usize],
    observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if rel_name == "*" && matches!(mode, OntologyMode::Advisory | OntologyMode::Strict) {
        // Union normalization synthesizes rel_type_name. Normalize first, then
        // shape the result; individual typed files still avoid node reads.
        return read_edges_union_paths(inventory.edge_files(None), Some(edge_ids), observer, true)
            .and_then(|batches| {
                project_batches_with_key(batches, &EXPLORATORY_EDGE_SCHEMA, projection, "edge_id")
            });
    }
    let (stem, schema) = match mode {
        OntologyMode::Exploratory => ("_exploratory", EXPLORATORY_EDGE_SCHEMA.clone()),
        OntologyMode::Advisory | OntologyMode::Strict => (rel_name, TYPED_EDGE_SCHEMA.clone()),
    };
    if edge_ids.is_empty() {
        return project_batches_with_key(Vec::new(), &schema, projection, "edge_id");
    }
    let mut batches = Vec::new();
    for (_, path) in inventory.edge_files(Some(stem)) {
        let file_schema = admitted_parquet(&path)?.schema().clone();
        if file_schema.fields() != schema.fields() {
            return Err(DataFusionError::Execution(format!(
                "projected edge read requires canonical schema: {}",
                path.display()
            )));
        }
        batches.extend(read_required_edge_filtered(
            &path,
            schema.clone(),
            edge_ids,
            observer,
            Some(projection),
        )?);
    }
    if batches.is_empty() {
        return project_batches_with_key(Vec::new(), &schema, projection, "edge_id");
    }
    Ok(batches)
}

/// Which [`io_stats`](crate::io_stats) counters a filtered read attributes to —
/// `read_parquet_filtered_u64` is keyed generically but the counters are split
/// by table so the benchmark can prove edge *and* node reads are
/// neighborhood-proportional.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum FilteredReadKind {
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

/// Exact row selection for a canonical dense contiguous `node_id` range. The
/// selection is relative to the concatenation of `row_groups`, as required by
/// Parquet after row-group filtering.
struct DenseNodeSelection {
    row_groups: Vec<usize>,
    selection: parquet::arrow::arrow_reader::RowSelection,
    pages_considered: u64,
    pages_selected: u64,
    exact_rows_selected: u64,
    first_id: u64,
    last_id: u64,
}

struct DenseNodeLayout {
    first_id: u64,
    group_rows: Vec<usize>,
    group_pages: Vec<Vec<usize>>,
    total_rows: usize,
    pages_considered: u64,
}

struct DenseNodeGroups {
    group_rows: Vec<usize>,
    group_pages: Vec<Vec<usize>>,
    rows_seen: usize,
    pages_considered: u64,
}

/// Prove the canonical dense node layout from row-group and page metadata.
fn dense_node_layout(
    metadata: &parquet::file::metadata::ParquetMetaData,
    key_leaf: usize,
) -> Option<DenseNodeLayout> {
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

    let Statistics::Int64(first_stats) = row_groups.first()?.column(key_leaf).statistics()? else {
        return None;
    };
    let first_id = u64::try_from(*first_stats.min_opt()?).ok()?;
    if first_id == 0 {
        return None;
    }

    let groups = dense_node_groups(metadata, key_leaf, first_id)?;
    if groups.rows_seen != total_rows {
        return None;
    }

    Some(DenseNodeLayout {
        first_id,
        group_rows: groups.group_rows,
        group_pages: groups.group_pages,
        total_rows,
        pages_considered: groups.pages_considered,
    })
}

/// Validate every row group and page against the dense id sequence.
fn dense_node_groups(
    metadata: &parquet::file::metadata::ParquetMetaData,
    key_leaf: usize,
    first_id: u64,
) -> Option<DenseNodeGroups> {
    use parquet::basic::BoundaryOrder;
    use parquet::file::page_index::column_index::ColumnIndexMetaData;
    use parquet::file::statistics::Statistics;

    let row_groups = metadata.row_groups();
    let column_indexes = metadata.column_index()?;
    let offset_indexes = metadata.offset_index()?;
    let mut group_rows = Vec::with_capacity(row_groups.len());
    let mut group_pages = Vec::with_capacity(row_groups.len());
    let mut file_row_offset = 0usize;
    let mut pages_considered = 0u64;

    for (group_idx, row_group) in row_groups.iter().enumerate() {
        let rows = usize::try_from(row_group.num_rows()).ok()?;
        if rows == 0 {
            return None;
        }
        let expected_min =
            i64::try_from(first_id.checked_add(u64::try_from(file_row_offset).ok()?)?).ok()?;
        let expected_max = i64::try_from(
            first_id.checked_add(u64::try_from(file_row_offset.checked_add(rows - 1)?).ok()?)?,
        )
        .ok()?;
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
            let page_min = i64::try_from(
                first_id.checked_add(u64::try_from(file_row_offset.checked_add(first)?).ok()?)?,
            )
            .ok()?;
            let page_max = i64::try_from(
                first_id.checked_add(
                    u64::try_from(
                        file_row_offset
                            .checked_add(first)?
                            .checked_add(page_rows - 1)?,
                    )
                    .ok()?,
                )?,
            )
            .ok()?;
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
    Some(DenseNodeGroups {
        group_rows,
        group_pages,
        rows_seen: file_row_offset,
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
        first_id,
        group_rows,
        group_pages,
        total_rows,
        pages_considered,
    } = dense_node_layout(metadata, key_leaf)?;

    let max_id = first_id.checked_add(u64::try_from(total_rows.checked_sub(1)?).ok()?)?;
    let ordinals: Vec<usize> = sorted_ids
        .iter()
        .copied()
        .filter(|&id| id >= first_id && id <= max_id)
        .map(|id| usize::try_from(id.checked_sub(first_id)?).ok())
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
        first_id,
        last_id: max_id,
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
pub(super) fn read_parquet_filtered_u64(
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
    read_parquet_filtered_u64_attempt(
        path,
        fallback_schema,
        key_column,
        ids,
        kind,
        observer,
        true,
        None,
        false,
    )
}

pub(super) fn read_required_edge_filtered(
    path: &Path,
    schema: SchemaRef,
    ids: &std::collections::HashSet<u64>,
    observer: Option<&Arc<dyn crate::io_stats::FilteredReadObserver>>,
    projection: Option<&[usize]>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let projection = projection
        .map(|indices| canonical_projection_with_key(&schema, indices, "edge_id"))
        .transpose()?;
    if ids.is_empty() {
        return match projection {
            Some(indices) => project_batches_with_key(Vec::new(), &schema, &indices, "edge_id"),
            None => Ok(vec![RecordBatch::new_empty(schema)]),
        };
    }
    read_parquet_filtered_u64_attempt(
        path,
        schema,
        "edge_id",
        ids,
        FilteredReadKind::Edge,
        observer,
        true,
        projection.as_deref(),
        true,
    )
}

fn canonical_projection_with_key(
    schema: &SchemaRef,
    projection: &[usize],
    key_column: &str,
) -> Result<Vec<usize>, DataFusionError> {
    let mut indices = projection.to_vec();
    indices.push(
        schema
            .index_of(key_column)
            .map_err(|error| DataFusionError::Execution(format!("filtered read: {error}")))?,
    );
    indices.sort_unstable();
    indices.dedup();
    if indices.iter().any(|index| *index >= schema.fields().len()) {
        return Err(DataFusionError::Execution(
            "filtered read projection index is out of range".into(),
        ));
    }
    Ok(indices)
}

pub(super) fn project_batches_with_key(
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
    projection: &[usize],
    key_column: &str,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let indices = canonical_projection_with_key(schema, projection, key_column)?;
    if batches.is_empty() {
        let projected = Arc::new(arrow::datatypes::Schema::new(
            indices
                .iter()
                .map(|index| schema.field(*index).clone())
                .collect::<Vec<_>>(),
        ));
        return Ok(vec![RecordBatch::new_empty(projected)]);
    }
    batches
        .into_iter()
        .map(|batch| batch.project(&indices).map_err(Into::into))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn read_parquet_filtered_u64_projected(
    path: &Path,
    fallback_schema: SchemaRef,
    key_column: &str,
    ids: &std::collections::HashSet<u64>,
    kind: FilteredReadKind,
    observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
    projection: &[usize],
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let projection = canonical_projection_with_key(&fallback_schema, projection, key_column)?;
    if ids.is_empty() || !path.exists() {
        return project_batches_with_key(Vec::new(), &fallback_schema, &projection, key_column);
    }
    read_parquet_filtered_u64_attempt(
        path,
        fallback_schema,
        key_column,
        ids,
        kind,
        observer,
        true,
        Some(&projection),
        false,
    )
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
    projection: Option<&[usize]>,
    required: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    use parquet::arrow::ProjectionMask;
    use parquet::arrow::arrow_reader::{
        ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter,
    };
    use parquet::file::metadata::PageIndexPolicy;
    use parquet::file::statistics::Statistics;

    let projected_schema = projection.map_or_else(
        || fallback_schema.clone(),
        |indices| {
            Arc::new(arrow::datatypes::Schema::new(
                indices
                    .iter()
                    .map(|index| fallback_schema.field(*index).clone())
                    .collect::<Vec<_>>(),
            ))
        },
    );
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
        let batches = if required {
            read_parquet_required(path)?
        } else {
            read_parquet_or_empty(path, fallback_schema.clone())?
        };
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
            let filtered_batch = arrow::compute::filter_record_batch(batch, &mask)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            filtered.push(if let Some(indices) = projection {
                filtered_batch
                    .project(indices)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?
            } else {
                filtered_batch
            });
        }
        if filtered.is_empty() {
            filtered.push(RecordBatch::new_empty(projected_schema));
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
    let dense_id_range = dense.as_ref().map(|dense| (dense.first_id, dense.last_id));

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
    let builder = if let Some(indices) = projection {
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
        builder.with_projection(mask)
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
        let (first_id, max_id) =
            dense_id_range.expect("dense selection has an authenticated range");
        let expected: std::collections::HashSet<u64> = ids
            .iter()
            .copied()
            .filter(|&id| id >= first_id && id <= max_id)
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
                projection,
                required,
            );
        }
    }
    record_pruning(kind, &observation, pruning);
    observation.complete(returned, false);
    if batches.is_empty() {
        return Ok(vec![RecordBatch::new_empty(projected_schema)]);
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

/// Like [`read_nodes`](super::read_nodes) but returns only rows whose `node_id` is in `node_ids` —
/// the traversal's lazy node-record read (#838): on an adjacency Hit only the
/// reached destination nodes' records are needed to project the destination
/// columns, not the whole node table. Canonical dense files use exact physical
/// row selection; legacy, gapped, or noncanonical files retain conservative
/// row-group pruning plus a membership predicate.
///
/// Contract parity with [`read_nodes`](super::read_nodes): always at least one (possibly empty)
/// batch with [`TOPOLOGY_NODES_SCHEMA`]; an empty `node_ids` or a missing file
/// never opens the file.
///
/// # Errors
/// Same as [`read_nodes`](super::read_nodes), plus Parquet filter construction failures.
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
    let paths = crate::mutator::node_parquet_files(dir)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let mut batches = Vec::new();
    for path in paths {
        batches.extend(normalize_topology_nodes(read_parquet_filtered_u64(
            &path,
            TOPOLOGY_NODES_SCHEMA.clone(),
            "node_id",
            node_ids,
            FilteredReadKind::Node,
            observer,
        )?)?);
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(TOPOLOGY_NODES_SCHEMA.clone()));
    }
    Ok(batches)
}

/// Filter destination nodes by `node_id` while physically decoding only the
/// requested canonical columns plus the join key. Legacy layouts retain full
/// normalization before projection.
#[allow(clippy::implicit_hasher)]
#[doc(hidden)]
pub fn read_nodes_filtered_projected_observed(
    dir: &Path,
    node_ids: &std::collections::HashSet<u64>,
    projection: &[usize],
    observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let paths = crate::mutator::node_parquet_files(dir)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let indices = canonical_projection_with_key(&TOPOLOGY_NODES_SCHEMA, projection, "node_id")?;
    let mut batches = Vec::new();
    for path in paths {
        let file_schema = admitted_parquet(&path)?.schema().clone();
        let canonical = file_schema.fields().len() == TOPOLOGY_NODES_SCHEMA.fields().len()
            && file_schema
                .fields()
                .iter()
                .zip(TOPOLOGY_NODES_SCHEMA.fields())
                .all(|(actual, expected)| actual.name() == expected.name());
        if canonical {
            batches.extend(read_parquet_filtered_u64_projected(
                &path,
                TOPOLOGY_NODES_SCHEMA.clone(),
                "node_id",
                node_ids,
                FilteredReadKind::Node,
                observer,
                &indices,
            )?);
        } else {
            let normalized = normalize_topology_nodes(read_parquet_filtered_u64(
                &path,
                TOPOLOGY_NODES_SCHEMA.clone(),
                "node_id",
                node_ids,
                FilteredReadKind::Node,
                observer,
            )?)?;
            batches.extend(
                normalized
                    .into_iter()
                    .map(|batch| batch.project(&indices).map_err(Into::into))
                    .collect::<Result<Vec<_>, DataFusionError>>()?,
            );
        }
    }
    if batches.is_empty() {
        return project_batches_with_key(Vec::new(), &TOPOLOGY_NODES_SCHEMA, &indices, "node_id");
    }
    Ok(batches)
}

#[cfg(test)]
mod tests;
