//! DataFusion [`TableProvider`] and [`CatalogProvider`] implementations.
//!
//! Each GraphForge graph directory (`project/`) maps to a [`GraphCatalog`] which
//! presents its Parquet files as DataFusion tables under the address
//! `graph.graph.<table_name>`:
//!
//! | Table name | Logical source | Schema |
//! |---|---|---|
//! | `topology_nodes` | legacy `topology/nodes.parquet` plus canonical `topology/nodes/*.parquet` shards | `TOPOLOGY_NODES_SCHEMA` |
//! | `edges_TYPENAME` | legacy `topology/edges/TYPENAME.parquet` plus canonical `topology/edges/TYPENAME/*.parquet` shards | `TYPED_EDGE_SCHEMA` |
//! | `edges__exploratory` | legacy and canonical `_exploratory` edge shards | `EXPLORATORY_EDGE_SCHEMA` |
//! | `properties_ENTITY` | authenticated newest-wins overlay of legacy and canonical property fragments | `property_schema(entity, defs)` |
//!
//! # Scan implementation
//!
//! Query-facing scans build a streaming [`GraphForgeParquetExec`](crate::parquet_scan::GraphForgeParquetExec)
//! during `TableProvider::scan` without reading or concatenating Parquet payloads
//! (#339). Decode happens in `ExecutionPlan::execute`, emitting bounded batches
//! sized from the session batch size. Unsupported predicates stay DataFusion-owned
//! (default filter pushdown is unsupported / non-exact).
//!
//! Direct readers (`read_edges`, `read_nodes`, …) used by ExpandExec and writers
//! consume the same canonical shard union; they are outside the query-provider
//! scan path.

use self::filtered_parquet::FilteredReadKind;
use self::filtered_parquet::read_parquet_filtered_u64;
use self::filtered_parquet::read_required_edge_filtered;
use crate::schemas::EXPLORATORY_EDGE_SCHEMA;
use crate::schemas::TOPOLOGY_NODES_SCHEMA;
use crate::schemas::TYPED_EDGE_SCHEMA;
use crate::schemas::property_schema;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use graphforge_core::OntologyMode;
use graphforge_ir::RuntimeCatalog;
use graphforge_ontology::OntologyHandle;
use graphforge_value::EntityTypeId;
use graphforge_value::PropertyId;
use graphforge_value::RelationTypeId;
use graphforge_value::RuntimeEntityId;
use graphforge_value::RuntimeRelationId;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

mod filtered_parquet;
mod property_readers;
mod providers;

pub use filtered_parquet::read_edges_filtered;
pub use filtered_parquet::read_edges_filtered_from_inventory;
pub use filtered_parquet::read_edges_filtered_observed;
pub use filtered_parquet::read_edges_filtered_observed_from_inventory;
pub use filtered_parquet::read_edges_filtered_projected_from_inventory;
pub use filtered_parquet::read_edges_filtered_projected_observed;
pub use filtered_parquet::read_nodes_filtered;
pub use filtered_parquet::read_nodes_filtered_observed;
pub use filtered_parquet::read_nodes_filtered_projected_observed;
pub use property_readers::read_edge_properties;
pub use property_readers::read_edge_properties_from_inventory;
pub use property_readers::read_edge_properties_projected;
pub use property_readers::read_edge_properties_projected_from_inventory;
pub use property_readers::read_properties;
pub use property_readers::read_properties_batched;
pub use property_readers::read_properties_from_inventory;
pub use property_readers::visit_node_property_overlay_admitted;
pub use property_readers::visit_properties_batched;
pub use property_readers::visit_property_fragments_admitted;
pub(crate) use property_readers::visit_property_overlay_batched;
pub(crate) use property_readers::visit_property_overlay_batched_projected;
pub(crate) use property_readers::visit_property_overlay_batched_with_inventory;
pub use providers::EdgePropertyTable;
pub use providers::PropertyTable;
pub use providers::TopologyNodeTable;
pub use providers::TypedEdgeTable;
pub use providers::UnionEdgeTable;

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
    read_parquet_required(path)
}

fn read_parquet_required(path: &Path) -> Result<Vec<RecordBatch>, DataFusionError> {
    let builder = admitted_parquet(path)?;
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
    use graphforge_value::{EntityTypeId, PrimaryEntityTypeId};

    batches
        .into_iter()
        .map(|batch| {
            let invalid = |message: String| {
                DataFusionError::Execution(format!("invalid node type identity: {message}"))
            };
            let type_idx = batch
                .schema()
                .index_of("type_id")
                .map_err(|error| invalid(format!("missing primary route: {error}")))?;
            let primary_ids = batch
                .column(type_idx)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| invalid("type_id is not UInt32".into()))?;
            for (row, raw) in primary_ids.iter().enumerate() {
                let raw = raw.ok_or_else(|| invalid(format!("null primary route at row {row}")))?;
                PrimaryEntityTypeId::decode(raw)
                    .map_err(|error| invalid(format!("primary row {row}: {error}")))?;
            }
            if let Some(column) = batch.column_by_name("type_ids") {
                let memberships = column
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| invalid("type_ids is not List<UInt32>".into()))?;
                for row in 0..memberships.len() {
                    if memberships.is_null(row) {
                        return Err(invalid(format!("null membership list at row {row}")));
                    }
                    let values = memberships.value(row);
                    let values = values
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .ok_or_else(|| invalid("type_ids values are not UInt32".into()))?;
                    for raw in values {
                        let raw =
                            raw.ok_or_else(|| invalid(format!("null membership at row {row}")))?;
                        EntityTypeId::decode(raw)
                            .map_err(|error| invalid(format!("membership row {row}: {error}")))?;
                    }
                }
                // The primary is immutable routing authority: do not infer or
                // require membership from it, even when current labels are present.
                return Ok(batch);
            }
            let nullable_labels = ListArray::from_iter_primitive::<UInt32Type, _, _>(
                primary_ids.values().iter().map(|raw| {
                    Some(
                        PrimaryEntityTypeId::decode(*raw)
                            .expect("primary route checked above")
                            .label()
                            .map(|id| Some(id.encode())),
                    )
                }),
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
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
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
    let mut batches = Vec::new();
    for (_, path) in crate::mutator::edge_parquet_files(dir, Some(stem))
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
    {
        batches.extend(read_parquet_or_empty(&path, schema.clone())?);
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema));
    }
    crate::io_stats::record_edge_full_read(total_rows(&batches));
    Ok(batches)
}

/// Read edge rows using the caller's explicit semantic route authority.
pub fn read_edges_from_inventory(
    inventory: &crate::AuthenticatedPropertyInventory,
    rel_name: &str,
    mode: OntologyMode,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if rel_name == "*" && matches!(mode, OntologyMode::Advisory | OntologyMode::Strict) {
        return read_edges_union_paths(inventory.edge_files(None), None, None, true);
    }
    let (route, schema) = match mode {
        OntologyMode::Exploratory => ("_exploratory", EXPLORATORY_EDGE_SCHEMA.clone()),
        OntologyMode::Advisory | OntologyMode::Strict => (rel_name, TYPED_EDGE_SCHEMA.clone()),
    };
    let mut batches = Vec::new();
    for (_, path) in inventory.edge_files(Some(route)) {
        batches.extend(read_parquet_required(&path)?);
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema));
    }
    crate::io_stats::record_edge_full_read(total_rows(&batches));
    Ok(batches)
}

/// Count topology edge rows without materializing adjacency or the full graph.
///
/// Used by unconstrained `count(r)` so a missing CSR index cannot charge O(E)
/// RSS (#1094). `"*"` unions every edge file; a Strict name counts its stem.
/// Advisory named counts include the typed stem and matching legacy exploratory
/// rows. Shared-file filtering streams only the relation-name column in bounded
/// batches.
///
/// # Errors
/// Returns [`crate::GfError::Storage`] on an invalid typed stem or footer read
/// failure, or a malformed relation-name column.
pub fn count_edge_rows(
    dir: &Path,
    rel_name: &str,
    mode: OntologyMode,
) -> Result<u64, crate::GfError> {
    let relation = match mode {
        OntologyMode::Exploratory => Some("_exploratory"),
        OntologyMode::Advisory | OntologyMode::Strict if rel_name == "*" => None,
        OntologyMode::Advisory | OntologyMode::Strict => {
            let mut comps = Path::new(rel_name).components();
            let single_normal = matches!(comps.next(), Some(std::path::Component::Normal(_)))
                && comps.next().is_none();
            if !single_normal {
                return Err(crate::GfError::Storage(format!(
                    "invalid relation name {rel_name:?}: must be a plain file stem"
                )));
            }
            (mode == OntologyMode::Strict).then_some(rel_name)
        }
    };
    count_edge_paths(
        crate::mutator::edge_parquet_files(dir, relation)?,
        rel_name,
        mode,
    )
}

/// Count admitted edge payload rows without decoding graph or adjacency data.
pub fn count_edge_rows_from_inventory(
    inventory: &crate::AuthenticatedPropertyInventory,
    rel_name: &str,
    mode: OntologyMode,
) -> Result<u64, crate::GfError> {
    let relation = match mode {
        OntologyMode::Exploratory => Some("_exploratory"),
        OntologyMode::Strict if rel_name != "*" => Some(rel_name),
        _ => None,
    };
    count_edge_paths(inventory.edge_files(relation), rel_name, mode)
}

fn count_edge_paths(
    paths: Vec<(String, PathBuf)>,
    rel_name: &str,
    mode: OntologyMode,
) -> Result<u64, crate::GfError> {
    let mut total = 0_u64;
    for (stem, path) in paths {
        if mode == OntologyMode::Advisory
            && rel_name != "*"
            && stem != rel_name
            && stem != "_exploratory"
        {
            continue;
        }
        let file = File::open(&path).map_err(|error| {
            crate::GfError::Storage(format!("open edge footer {}: {error}", path.display()))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            crate::GfError::Storage(format!("read edge footer {}: {error}", path.display()))
        })?;
        if stem == "_exploratory" && rel_name != "*" {
            let relation_column = builder
                .schema()
                .index_of("rel_type_name")
                .map_err(|error| {
                    crate::GfError::Storage(format!(
                        "edge relation column {}: {error}",
                        path.display()
                    ))
                })?;
            let projection =
                parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), [relation_column]);
            let reader = builder
                .with_projection(projection)
                .with_batch_size(8_192)
                .build()
                .map_err(|error| {
                    crate::GfError::Storage(format!(
                        "read edge relations {}: {error}",
                        path.display()
                    ))
                })?;
            for batch in reader {
                let batch = batch.map_err(|error| {
                    crate::GfError::Storage(format!(
                        "decode edge relations {}: {error}",
                        path.display()
                    ))
                })?;
                let names = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        crate::GfError::Storage(format!(
                            "edge rel_type_name is not Utf8: {}",
                            path.display()
                        ))
                    })?;
                let matching =
                    u64::try_from(names.iter().filter(|name| *name == Some(rel_name)).count())
                        .map_err(|_| crate::GfError::Storage("edge count exceeds u64".into()))?;
                total = total
                    .checked_add(matching)
                    .ok_or_else(|| crate::GfError::Storage("edge count exceeds u64".into()))?;
            }
            continue;
        }
        let rows = u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
            crate::GfError::Storage(format!(
                "edge footer row count overflows u64: {}",
                path.display()
            ))
        })?;
        total = total
            .checked_add(rows)
            .ok_or_else(|| crate::GfError::Storage("edge count exceeds u64".into()))?;
    }
    Ok(total)
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
    let paths = crate::mutator::edge_parquet_files(dir, None)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    read_edges_union_paths(paths, edge_ids, observer, false)
}

fn read_edges_union_paths(
    paths: Vec<(String, PathBuf)>,
    edge_ids: Option<&std::collections::HashSet<u64>>,
    observer: Option<&std::sync::Arc<dyn crate::io_stats::FilteredReadObserver>>,
    required: bool,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if required && edge_ids.is_some_and(std::collections::HashSet::is_empty) {
        return Ok(vec![RecordBatch::new_empty(
            EXPLORATORY_EDGE_SCHEMA.clone(),
        )]);
    }
    let mut out = Vec::new();
    for (stem, path) in paths {
        let schema = if required {
            admitted_parquet(&path)?.schema().clone()
        } else {
            discover_parquet_schema(&path).unwrap_or_else(|| TYPED_EDGE_SCHEMA.clone())
        };
        let batches = if let Some(ids) = edge_ids {
            if required {
                read_required_edge_filtered(&path, schema, ids, observer, None)?
            } else {
                read_parquet_filtered_u64(
                    &path,
                    schema,
                    "edge_id",
                    ids,
                    FilteredReadKind::Edge,
                    observer,
                )?
            }
        } else {
            let b = if required {
                read_parquet_required(&path)?
            } else {
                read_parquet_or_empty(&path, schema)?
            };
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
pub(crate) fn tag_rel_type_name(
    batch: &RecordBatch,
    stem: &str,
) -> Result<RecordBatch, DataFusionError> {
    if batch.schema().field_with_name("rel_type_name").is_ok() {
        return Ok(batch.clone());
    }
    let names = arrow::array::StringArray::from(vec![stem; batch.num_rows()]);
    let mut cols: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
    cols.push(Arc::new(names));
    RecordBatch::try_new(EXPLORATORY_EDGE_SCHEMA.clone(), cols)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Enumerate the logical node topology's legacy flat file and canonical shards.
///
/// Paths are returned in canonical topology order. An absent node topology
/// yields an empty vector; malformed canonical shard entries fail closed.
///
/// # Errors
/// Propagates Parquet / Arrow errors encountered while reading.
pub fn topology_node_files(dir: &Path) -> Result<Vec<PathBuf>, graphforge_core::GfError> {
    crate::mutator::node_parquet_files(dir)
}

/// Enumerate every canonical node-property fragment for `stem`.
///
/// This is the storage-owned source-of-truth used by consumers that must charge
/// or fingerprint the exact same legacy and immutable-shard files that
/// [`read_properties`] consumes.
pub fn node_property_files(
    dir: &Path,
    stem: &str,
) -> Result<Vec<PathBuf>, graphforge_core::GfError> {
    crate::mutator::property_parquet_files(dir, "properties", stem)
}

/// Enumerate all canonical node-property source fragments, failing closed on
/// malformed route entries instead of silently omitting them.
pub fn node_property_source_files(dir: &Path) -> Result<Vec<PathBuf>, graphforge_core::GfError> {
    Ok(node_property_source_fragments(dir)?
        .into_iter()
        .map(|(_, path)| path)
        .collect())
}

/// Enumerate the route and exact path of every canonical node-property source.
pub fn node_property_source_fragments(
    dir: &Path,
) -> Result<Vec<(String, PathBuf)>, graphforge_core::GfError> {
    let root = dir.join("properties");
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(graphforge_core::GfError::Storage(error.to_string())),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| graphforge_core::GfError::Storage(error.to_string()))?;
        let file_type = entry
            .file_type()
            .map_err(|error| graphforge_core::GfError::Storage(error.to_string()))?;
        let path = entry.path();
        if file_type.is_symlink() {
            return Err(graphforge_core::GfError::Storage(
                "property source contains a symbolic link".into(),
            ));
        }
        if file_type.is_file() {
            if path.extension().and_then(|value| value.to_str()) == Some("parquet") {
                let stem = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| {
                        graphforge_core::GfError::Storage(
                            "property source route is not canonical UTF-8".into(),
                        )
                    })?;
                paths.push((stem.to_owned(), path));
            }
            continue;
        }
        if file_type.is_dir() {
            let stem = path
                .file_name()
                .and_then(|value| value.to_str())
                .ok_or_else(|| {
                    graphforge_core::GfError::Storage(
                        "property shard route is not canonical UTF-8".into(),
                    )
                })?;
            if stem.ends_with(".parquet") {
                return Err(graphforge_core::GfError::Storage(
                    "property source Parquet path is not a regular file".into(),
                ));
            }
            paths.extend(
                crate::mutator::property_parquet_files(dir, "properties", stem)?
                    .into_iter()
                    .map(|path| (stem.to_owned(), path)),
            );
            continue;
        }
        return Err(graphforge_core::GfError::Storage(
            "property source contains a special file".into(),
        ));
    }
    paths.sort_by(|left, right| left.1.cmp(&right.1));
    paths.dedup_by(|left, right| left.1 == right.1);
    Ok(paths)
}

/// Read all node rows from every canonical topology fragment.
pub fn read_nodes(dir: &Path) -> Result<Vec<RecordBatch>, DataFusionError> {
    let paths = crate::mutator::node_parquet_files(dir)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let mut batches = Vec::new();
    for path in paths {
        batches.extend(normalize_topology_nodes(read_parquet_or_empty(
            &path,
            TOPOLOGY_NODES_SCHEMA.clone(),
        )?)?);
    }
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(TOPOLOGY_NODES_SCHEMA.clone()));
    }
    let mut uuids = std::collections::HashSet::new();
    let mut surrogates = std::collections::HashSet::new();
    for batch in &batches {
        let uuid = batch
            .column_by_name("node_uuid")
            .and_then(|array| {
                array
                    .as_any()
                    .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            })
            .ok_or_else(|| DataFusionError::Execution("node_uuid is not fixed binary".into()))?;
        let surrogate = batch
            .column_by_name("node_id")
            .and_then(|array| array.as_any().downcast_ref::<arrow::array::UInt64Array>())
            .ok_or_else(|| DataFusionError::Execution("node_id is not UInt64".into()))?;
        for row in 0..batch.num_rows() {
            if !uuids.insert(uuid.value(row).to_vec()) || !surrogates.insert(surrogate.value(row)) {
                return Err(DataFusionError::Execution(
                    "canonical node shards contain a duplicate UUID or surrogate".into(),
                ));
            }
        }
    }
    crate::io_stats::record_node_full_read(total_rows(&batches));
    Ok(batches)
}

/// Whether at least one canonical legacy or sharded node file exists.
pub fn node_topology_present(dir: &Path) -> Result<bool, DataFusionError> {
    Ok(!crate::mutator::node_parquet_files(dir)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
        .is_empty())
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
    let mut max = 0u64;
    for (_, path) in crate::mutator::edge_parquet_files(dir, None)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
    {
        // Every enumerated Parquet path is canonical topology. Ignoring a
        // malformed shard here could resume surrogate allocation from an
        // incomplete maximum and authenticate colliding edge identities.
        max = max.max(max_ordered_u64_tail(&path, "edge_id")?);
    }
    Ok(max)
}

/// Return the largest canonical node surrogate without decoding the complete
/// node table. Canonical topology keeps surrogate ids strictly increasing, so
/// only the final bounded Parquet row group is needed.
pub(crate) fn max_node_id(dir: &Path) -> Result<u64, DataFusionError> {
    let mut max = 0;
    for path in crate::mutator::node_parquet_files(dir)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
    {
        max = max.max(max_ordered_u64_tail(&path, "node_id")?);
    }
    Ok(max)
}

fn max_ordered_u64_tail(path: &Path, column: &str) -> Result<u64, DataFusionError> {
    use arrow::array::{Array, UInt64Array};

    let input = match File::open(path) {
        Ok(input) => input,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(io_err(&error)),
    };
    let builder = ParquetRecordBatchReaderBuilder::try_new(input).map_err(parquet_err)?;
    let row_groups = builder.metadata().num_row_groups();
    if row_groups == 0 {
        return Ok(0);
    }
    let reader = builder
        .with_row_groups(vec![row_groups - 1])
        .with_batch_size(8_192)
        .build()
        .map_err(parquet_err)?;
    let mut max = 0_u64;
    for batch in reader {
        let batch = batch.map_err(parquet_err)?;
        if let Some(values) = batch
            .column_by_name(column)
            .and_then(|value| value.as_any().downcast_ref::<UInt64Array>())
        {
            for row in 0..values.len() {
                if !values.is_null(row) {
                    max = max.max(values.value(row));
                }
            }
        }
    }
    Ok(max)
}

const MAX_ADMITTED_PARQUET_COLUMNS: usize = 4_096;
const MAX_ADMITTED_PARQUET_METADATA_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ADMITTED_ROW_GROUP_BYTES: i64 = 256 * 1024 * 1024;
const MAX_ADMITTED_COLUMN_BYTES: i64 = 64 * 1024 * 1024;

/// Open one path through the storage-wide fail-closed Parquet admission policy.
pub(crate) fn admitted_parquet(
    path: &Path,
) -> Result<ParquetRecordBatchReaderBuilder<File>, DataFusionError> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
    }
    let mut file = options.open(path).map_err(|error| io_err(&error))?;
    let metadata = file.metadata().map_err(|error| io_err(&error))?;
    if !metadata.file_type().is_file() {
        return Err(DataFusionError::Execution(format!(
            "Parquet source {} is not a regular file",
            path.display()
        )));
    }
    preflight_parquet_handle(&mut file, metadata.len())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(parquet_err)?;
    admit_decoded_parquet(&builder)?;
    Ok(builder)
}

fn preflight_parquet_handle(file: &mut File, length: u64) -> Result<(), DataFusionError> {
    if length < 12 {
        return Err(DataFusionError::Execution(
            "Parquet footer is truncated".into(),
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(|e| io_err(&e))?;
    let mut leading = [0_u8; 4];
    file.read_exact(&mut leading).map_err(|e| io_err(&e))?;
    if &leading != b"PAR1" {
        return Err(DataFusionError::Execution(
            "Parquet leading magic is invalid".into(),
        ));
    }
    file.seek(SeekFrom::End(-8)).map_err(|e| io_err(&e))?;
    let mut footer = [0_u8; 8];
    file.read_exact(&mut footer).map_err(|e| io_err(&e))?;
    let metadata_len = u64::from(u32::from_le_bytes(footer[..4].try_into().unwrap()));
    if &footer[4..] != b"PAR1"
        || metadata_len > MAX_ADMITTED_PARQUET_METADATA_BYTES
        || metadata_len
            .checked_add(8)
            .is_none_or(|bytes| bytes > length)
    {
        return Err(DataFusionError::ResourcesExhausted(
            "Parquet metadata exceeds admission limit".into(),
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(|e| io_err(&e))?;
    Ok(())
}

fn admit_decoded_parquet(
    builder: &ParquetRecordBatchReaderBuilder<File>,
) -> Result<(), DataFusionError> {
    if builder.schema().fields().len() > MAX_ADMITTED_PARQUET_COLUMNS {
        return Err(DataFusionError::ResourcesExhausted(
            "Parquet schema exceeds column admission limit".into(),
        ));
    }
    for group in builder.metadata().row_groups() {
        if group.total_byte_size() < 0 || group.total_byte_size() > MAX_ADMITTED_ROW_GROUP_BYTES {
            return Err(DataFusionError::ResourcesExhausted(
                "Parquet row group exceeds decoded-byte admission limit".into(),
            ));
        }
        for column in group.columns() {
            if column.uncompressed_size() < 0
                || column.uncompressed_size() > MAX_ADMITTED_COLUMN_BYTES
            {
                return Err(DataFusionError::ResourcesExhausted(
                    "Parquet column chunk exceeds decoded-byte admission limit".into(),
                ));
            }
        }
    }
    Ok(())
}

/// Visit canonical topology fragments with byte/decode admission and decoding
/// performed through the same no-follow file handle.
pub fn visit_node_fragments_admitted<F>(
    dir: &Path,
    batch_size: usize,
    byte_limit: u64,
    evidence: &mut Vec<AdmittedSourceFile>,
    mut visit: F,
) -> Result<u64, DataFusionError>
where
    F: FnMut(&RecordBatch) -> Result<bool, DataFusionError>,
{
    let paths = crate::mutator::node_parquet_files(dir)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let mut total = 0_u64;
    for path in paths {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = options.open(&path).map_err(|e| io_err(&e))?;
        let metadata = file.metadata().map_err(|e| io_err(&e))?;
        if !metadata.file_type().is_file() {
            return Err(DataFusionError::Execution(format!(
                "topology source {} is not a regular file",
                path.display()
            )));
        }
        total = total.checked_add(metadata.len()).ok_or_else(|| {
            DataFusionError::ResourcesExhausted("topology source bytes overflow".into())
        })?;
        if total > byte_limit {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "topology source bytes exceed {byte_limit}"
            )));
        }
        preflight_parquet_handle(&mut file, metadata.len())?;
        evidence.push(hash_admitted_source(
            node_relative_name(&path)?,
            &mut file,
            metadata.len(),
        )?);
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(parquet_err)?;
        admit_decoded_parquet(&builder)?;
        for batch in builder
            .with_batch_size(batch_size.max(1))
            .build()
            .map_err(parquet_err)?
        {
            let batch = batch.map_err(parquet_err)?;
            for normalized in normalize_topology_nodes(vec![batch])? {
                if !visit(&normalized)? {
                    return Ok(total);
                }
            }
        }
    }
    Ok(total)
}

/// Content identity captured from the same stable handle later decoded by a
/// bounded graph-source visitor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedSourceFile {
    /// Canonical graph-root-relative source name.
    pub name: String,
    /// Exact handle length admitted before hashing and decode.
    pub byte_length: u64,
    /// SHA-256 of the complete bytes read from that handle.
    pub sha256: [u8; 32],
}

fn property_relative_name(stem: &str, path: &Path) -> Result<String, DataFusionError> {
    let file = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| DataFusionError::Execution("property source name is not UTF-8".into()))?;
    Ok(
        if path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some(stem)
        {
            format!("properties/{stem}/{file}")
        } else {
            format!("properties/{file}")
        },
    )
}

fn node_relative_name(path: &Path) -> Result<String, DataFusionError> {
    let file = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| DataFusionError::Execution("topology source name is not UTF-8".into()))?;
    Ok(
        if path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some("nodes")
        {
            format!("topology/nodes/{file}")
        } else {
            "topology/nodes.parquet".into()
        },
    )
}

fn hash_admitted_source(
    name: String,
    file: &mut File,
    length: u64,
) -> Result<AdmittedSourceFile, DataFusionError> {
    file.seek(SeekFrom::Start(0)).map_err(|e| io_err(&e))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut read = 0_u64;
    loop {
        let count = file.read(&mut buffer).map_err(|e| io_err(&e))?;
        if count == 0 {
            break;
        }
        read = read.checked_add(count as u64).ok_or_else(|| {
            DataFusionError::ResourcesExhausted("graph source length overflow".into())
        })?;
        if read > length {
            return Err(DataFusionError::Execution(
                "graph source changed while hashing admitted handle".into(),
            ));
        }
        digest.update(&buffer[..count]);
    }
    if read != length {
        return Err(DataFusionError::Execution(
            "graph source changed while hashing admitted handle".into(),
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(|e| io_err(&e))?;
    Ok(AdmittedSourceFile {
        name,
        byte_length: length,
        sha256: digest.finalize().into(),
    })
}

/// Visit the canonical node-topology shard union one bounded batch at a time (#706).
///
/// Applies the same legacy `type_id` → `type_ids` normalization as
/// [`read_nodes`]. `visit` returns `Ok(true)` to continue or `Ok(false)` to
/// stop early. An absent node topology yields a single empty schema-shaped
/// batch (parity with [`read_nodes`]).
///
/// # Errors
/// Propagates Parquet / Arrow / normalization errors, or any error from `visit`.
pub fn visit_nodes_batched<F>(
    dir: &Path,
    batch_size: usize,
    mut visit: F,
) -> Result<(), DataFusionError>
where
    F: FnMut(&RecordBatch) -> Result<bool, DataFusionError>,
{
    let paths = crate::mutator::node_parquet_files(dir)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    if paths.is_empty() {
        let empty = RecordBatch::new_empty(TOPOLOGY_NODES_SCHEMA.clone());
        let _ = visit(&empty)?;
        return Ok(());
    }
    let mut any = false;
    for path in paths {
        let file = File::open(&path).map_err(|e| io_err(&e))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(parquet_err)?
            .with_batch_size(batch_size.max(1))
            .build()
            .map_err(parquet_err)?;
        for batch in reader {
            any = true;
            let normalized = normalize_topology_nodes(vec![batch.map_err(parquet_err)?])?;
            for b in &normalized {
                if !visit(b)? {
                    return Ok(());
                }
            }
        }
    }
    if !any {
        let empty = RecordBatch::new_empty(TOPOLOGY_NODES_SCHEMA.clone());
        let _ = visit(&empty)?;
    }
    Ok(())
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
            let entry = entry.ok()?;
            let path = entry.path();
            if entry.file_type().ok()?.is_dir() {
                return Some(path.file_name()?.to_str()?.to_owned());
            }
            if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
                return None;
            }
            Some(path.file_stem()?.to_str()?.to_owned())
        })
        .collect();
    stems.sort();
    stems.dedup();
    stems
}

/// Read just the Arrow schema of a Parquet file, or `None` if it is absent or
/// unreadable.
pub(crate) fn discover_parquet_schema(path: &Path) -> Option<SchemaRef> {
    discover_parquet_schema_detailed(path).ok()
}

/// Like [`discover_parquet_schema`], but preserves the underlying I/O / Parquet
/// error string for scale-host diagnostics.
pub(crate) fn discover_parquet_schema_detailed(path: &Path) -> Result<SchemaRef, String> {
    let file = File::open(path).map_err(|error| format!("open failed: {error}"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| format!("parquet footer/schema failed: {error}"))?;
    Ok(builder.schema().clone())
}

// ---------------------------------------------------------------------------
// GraphSchema — inner schema provider
// ---------------------------------------------------------------------------

struct GraphSchema {
    authority: std::sync::RwLock<GraphCatalogAuthority>,
}

struct GraphCatalogAuthority {
    tables: HashMap<String, Arc<dyn TableProvider>>,
    property_inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
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
            authority: std::sync::RwLock::new(GraphCatalogAuthority {
                tables: HashMap::new(),
                property_inventory: None,
            }),
        }
    }

    fn register(&mut self, name: impl Into<String>, table: Arc<dyn TableProvider>) {
        self.authority
            .get_mut()
            .expect("new graph schema lock is not poisoned")
            .tables
            .insert(name.into(), table);
    }
}

#[async_trait]
impl SchemaProvider for GraphSchema {
    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .authority
            .read()
            .expect("graph schema lock poisoned")
            .tables
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self
            .authority
            .read()
            .expect("graph schema lock poisoned")
            .tables
            .get(name)
            .cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.authority
            .read()
            .expect("graph schema lock poisoned")
            .tables
            .contains_key(name)
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
    prop_names: HashMap<PropertyId, String>,
    /// Reverse map `TypeId.0` → relation-type name, merged from the ontology and
    /// the runtime catalog. Lets the lowering layer resolve a `TypedEdgeScan`'s
    /// relation name in exploratory mode (where the ontology map is empty).
    rel_names: HashMap<RuntimeRelationId, String>,
    /// Reverse map checked runtime identity → entity-type (node label) name, from the
    /// runtime catalog. The lowerer tags these keys with
    /// [`EntityTypeId::runtime`] before merging them with
    /// ontology TypeIds (#702 / #889).
    label_names: HashMap<RuntimeEntityId, String>,
    semantic_rel_routes: HashMap<RelationTypeId, String>,
    semantic_label_routes: HashMap<EntityTypeId, String>,
    semantic_label_names: HashMap<EntityTypeId, String>,
    semantic_composition_fingerprint: Option<String>,
    semantic_edge_property_tables: HashMap<RelationTypeId, Arc<dyn TableProvider>>,
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
        Self::open_with_semantic_bindings(dir, ontology, runtime_catalog, None)
    }

    /// Open a catalog whose property tables all share one authenticated,
    /// immutable committed-generation inventory.
    pub fn open_authenticated(
        dir: &Path,
        ontology: Option<&OntologyHandle>,
        runtime_catalog: &RuntimeCatalog,
        inventory: Arc<crate::AuthenticatedPropertyInventory>,
    ) -> Result<Self, DataFusionError> {
        Self::open_with_authority(dir, ontology, runtime_catalog, None, Some(inventory))
    }

    /// Open with exact generation-pinned qualified storage bindings.
    #[allow(clippy::too_many_lines)] // registration must build one internally consistent catalog
    pub fn open_with_semantic_bindings(
        dir: &Path,
        ontology: Option<&OntologyHandle>,
        runtime_catalog: &RuntimeCatalog,
        semantic: Option<&crate::SemanticStorageBindings>,
    ) -> Result<Self, DataFusionError> {
        Self::open_with_authority(dir, ontology, runtime_catalog, semantic, None)
    }

    /// Open with semantic bindings and one already-authenticated property authority.
    pub fn open_authenticated_with_semantic_bindings(
        dir: &Path,
        ontology: Option<&OntologyHandle>,
        runtime_catalog: &RuntimeCatalog,
        semantic: Option<&crate::SemanticStorageBindings>,
        inventory: Arc<crate::AuthenticatedPropertyInventory>,
    ) -> Result<Self, DataFusionError> {
        Self::open_with_authority(dir, ontology, runtime_catalog, semantic, Some(inventory))
    }

    #[allow(clippy::too_many_lines)]
    fn open_with_authority(
        dir: &Path,
        ontology: Option<&OntologyHandle>,
        runtime_catalog: &RuntimeCatalog,
        semantic: Option<&crate::SemanticStorageBindings>,
        inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
    ) -> Result<Self, DataFusionError> {
        let inventory = match inventory {
            Some(inventory) => Some(inventory),
            None if !dir.exists() => None,
            None => Some(Arc::new(
                crate::property_overlay::authenticated_property_inventory(dir)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?,
            )),
        };
        let mut schema = GraphSchema::new();

        // ---- topology nodes ----
        schema.register(
            "topology_nodes",
            Arc::new(TopologyNodeTable::open_project(dir)?),
        );

        // ---- typed edge tables ----
        if let Some(bindings) = semantic {
            for binding in &bindings.bindings {
                if binding.route_kind == crate::SemanticRouteKind::Relation {
                    schema.register(
                        format!("edges_{}", binding.route),
                        Arc::new(
                            TypedEdgeTable::open(dir, &binding.route)
                                .with_inventory(inventory.clone()),
                        ),
                    );
                }
            }
        } else if let Some(handle) = ontology {
            for rel_name in handle.relation_type_names() {
                schema.register(
                    format!("edges_{rel_name}"),
                    Arc::new(TypedEdgeTable::open(dir, rel_name).with_inventory(inventory.clone())),
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
                Arc::new(
                    TypedEdgeTable::open(dir, "_exploratory").with_inventory(inventory.clone()),
                ),
            );
            for rel_name in runtime_catalog.relation_types() {
                let has_typed = inventory
                    .as_ref()
                    .is_some_and(|inventory| !inventory.edge_files(Some(rel_name)).is_empty());
                if has_typed {
                    schema.register(
                        format!("edges_{rel_name}"),
                        Arc::new(
                            TypedEdgeTable::open(dir, rel_name).with_inventory(inventory.clone()),
                        ),
                    );
                }
            }
        }

        // Always register the exploratory fallback (advisory mode uses it too).
        if !schema.table_exist("edges__exploratory") {
            schema.register(
                "edges__exploratory",
                Arc::new(
                    TypedEdgeTable::open(dir, "_exploratory").with_inventory(inventory.clone()),
                ),
            );
        }

        // ---- property tables ----
        if let Some(bindings) = semantic {
            let mut node_routes = std::collections::BTreeSet::new();
            for binding in &bindings.bindings {
                match binding.route_kind {
                    crate::SemanticRouteKind::NodeProperty
                        if node_routes.insert(binding.route.clone()) =>
                    {
                        schema.register(
                            format!("properties_{}", binding.route),
                            Arc::new(inventory.as_ref().map_or_else(
                                || PropertyTable::open_discovered(dir, &binding.route),
                                |inventory| {
                                    PropertyTable::open_authenticated(
                                        dir,
                                        &binding.route,
                                        Arc::clone(inventory),
                                    )
                                },
                            )),
                        );
                    }
                    crate::SemanticRouteKind::EdgeProperty => {
                        schema.register(
                            format!("edge_properties_{}", binding.route),
                            Arc::new(inventory.as_ref().map_or_else(
                                || EdgePropertyTable::open_discovered(dir, &binding.route),
                                |inventory| {
                                    EdgePropertyTable::open_authenticated(
                                        dir,
                                        &binding.route,
                                        Arc::clone(inventory),
                                    )
                                },
                            )),
                        );
                    }
                    _ => {}
                }
            }
        } else {
            register_property_tables(dir, ontology, &mut schema, inventory.as_ref());
        }

        // ---- name maps (for read-path property + relation resolution) ----
        let mut prop_names = build_prop_names(ontology, runtime_catalog);
        let rel_names = build_rel_names(runtime_catalog);
        let label_names = build_label_names(runtime_catalog);
        let mut semantic_rel_routes = HashMap::new();
        let mut semantic_label_routes = HashMap::new();
        let mut semantic_label_names = HashMap::new();
        let mut semantic_edge_property_tables: HashMap<RelationTypeId, Arc<dyn TableProvider>> =
            HashMap::new();
        if let Some(bindings) = semantic {
            for binding in &bindings.bindings {
                match binding.route_kind {
                    crate::SemanticRouteKind::Entity => {
                        let id = EntityTypeId::decode(binding.storage_id)
                            .map_err(|e| DataFusionError::Plan(e.to_string()))?;
                        semantic_label_routes.insert(id, binding.route.clone());
                        semantic_label_names.insert(id, binding.symbol.display());
                    }
                    crate::SemanticRouteKind::Relation => {
                        let id = RelationTypeId::decode(binding.storage_id)
                            .map_err(|e| DataFusionError::Plan(e.to_string()))?;
                        semantic_rel_routes.insert(id, binding.route.clone());
                    }
                    crate::SemanticRouteKind::NodeProperty
                    | crate::SemanticRouteKind::EdgeProperty => {
                        let name = binding
                            .symbol
                            .local_id
                            .split_once(':')
                            .map_or(binding.symbol.local_id.as_str(), |(_, name)| name);
                        prop_names.insert(
                            PropertyId::ontology(graphforge_core::PropId(binding.storage_id))
                                .map_err(|error| DataFusionError::Plan(error.to_string()))?,
                            name.to_owned(),
                        );
                    }
                }
            }
            for relation in bindings
                .bindings
                .iter()
                .filter(|binding| binding.route_kind == crate::SemanticRouteKind::Relation)
            {
                if bindings.bindings.iter().any(|binding| {
                    binding.route_kind == crate::SemanticRouteKind::EdgeProperty
                        && binding.owner.as_ref() == Some(&relation.symbol)
                }) {
                    semantic_edge_property_tables.insert(
                        RelationTypeId::decode(relation.storage_id)
                            .map_err(|e| DataFusionError::Plan(e.to_string()))?,
                        Arc::new(inventory.as_ref().map_or_else(
                            || EdgePropertyTable::open_discovered(dir, &relation.route),
                            |inventory| {
                                EdgePropertyTable::open_authenticated(
                                    dir,
                                    &relation.route,
                                    Arc::clone(inventory),
                                )
                            },
                        )),
                    );
                }
            }
        }

        schema
            .authority
            .get_mut()
            .expect("new graph schema lock is not poisoned")
            .property_inventory = inventory;
        Ok(Self {
            schema: Arc::new(schema),
            prop_names,
            rel_names,
            label_names,
            semantic_rel_routes,
            semantic_label_routes,
            semantic_label_names,
            semantic_composition_fingerprint: semantic
                .map(|bindings| bindings.composition_fingerprint.clone()),
            semantic_edge_property_tables,
        })
    }

    /// Retain the explicit generation authority for first-party physical readers.
    #[must_use]
    pub fn admitted_inventory(&self) -> Option<Arc<crate::AuthenticatedPropertyInventory>> {
        self.lowering_property_inventory()
    }

    /// Relation provider pinned to this catalog's admitted route authority.
    #[must_use]
    pub fn edge_table(&self, dir: &Path, route: &str) -> TypedEdgeTable {
        TypedEdgeTable::open(dir, route).with_inventory(self.lowering_property_inventory())
    }

    /// Union provider preserving semantic relation names from admitted authority.
    #[must_use]
    pub fn union_edge_table(&self, dir: &Path) -> UnionEdgeTable {
        UnionEdgeTable {
            dir: dir.to_path_buf(),
            inventory: self.lowering_property_inventory(),
        }
    }

    /// Node-property provider pinned to this catalog's generation authority.
    #[must_use]
    pub fn property_table(&self, dir: &Path, route: &str) -> PropertyTable {
        self.schema
            .authority
            .read()
            .expect("property inventory lock poisoned")
            .property_inventory
            .as_ref()
            .map_or_else(
                || PropertyTable::open_discovered(dir, route),
                |inventory| PropertyTable::open_authenticated(dir, route, Arc::clone(inventory)),
            )
    }

    /// Edge-property provider pinned to this catalog's generation authority.
    #[must_use]
    pub fn edge_property_table(&self, dir: &Path, route: &str) -> EdgePropertyTable {
        self.schema
            .authority
            .read()
            .expect("property inventory lock poisoned")
            .property_inventory
            .as_ref()
            .map_or_else(
                || EdgePropertyTable::open_discovered(dir, route),
                |inventory| {
                    EdgePropertyTable::open_authenticated(dir, route, Arc::clone(inventory))
                },
            )
    }

    /// Retain one inventory while collecting a compilation's schema facts.
    pub(crate) fn lowering_property_inventory(
        &self,
    ) -> Option<Arc<crate::AuthenticatedPropertyInventory>> {
        self.schema
            .authority
            .read()
            .expect("property inventory lock poisoned")
            .property_inventory
            .clone()
    }

    /// Canonical routes in this catalog's authenticated inventory.
    #[must_use]
    pub fn property_routes(&self, kind: crate::PropertyRouteKind) -> Vec<String> {
        self.schema
            .authority
            .read()
            .expect("property inventory lock poisoned")
            .property_inventory
            .as_ref()
            .map_or_else(Vec::new, |inventory| {
                inventory.routes(kind).map(str::to_owned).collect()
            })
    }

    /// Replace the property authority and registered providers after a
    /// successful same-session write commit.
    ///
    /// Plans already executing retain their provider and immutable inventory;
    /// later plans resolve tables from this atomically refreshed catalog view.
    pub fn refresh_property_inventory(&self, dir: &Path) -> Result<(), DataFusionError> {
        let inventory = Arc::new(
            crate::property_overlay::authenticated_property_inventory(dir)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?,
        );
        let replacements = {
            let authority = self
                .schema
                .authority
                .read()
                .expect("graph schema lock poisoned");
            authority
                .tables
                .keys()
                .filter_map(|name| {
                    name.strip_prefix("properties_")
                        .map(|route| (name.clone(), route.to_owned(), false))
                        .or_else(|| {
                            name.strip_prefix("edge_properties_")
                                .map(|route| (name.clone(), route.to_owned(), true))
                        })
                })
                .collect::<Vec<_>>()
        };
        let mut authority = self
            .schema
            .authority
            .write()
            .expect("graph schema lock poisoned");
        for (name, route, edge) in replacements {
            let table: Arc<dyn TableProvider> = if edge {
                Arc::new(EdgePropertyTable::open_authenticated(
                    dir,
                    &route,
                    Arc::clone(&inventory),
                ))
            } else {
                Arc::new(PropertyTable::open_authenticated(
                    dir,
                    &route,
                    Arc::clone(&inventory),
                ))
            };
            authority.tables.insert(name, table);
        }
        let mut edge_routes = authority
            .tables
            .keys()
            .filter_map(|name| name.strip_prefix("edges_").map(str::to_owned))
            .collect::<std::collections::BTreeSet<_>>();
        edge_routes.extend(
            inventory
                .edge_files(None)
                .into_iter()
                .map(|(route, _)| route),
        );
        for route in edge_routes {
            authority.tables.insert(
                format!("edges_{route}"),
                Arc::new(
                    TypedEdgeTable::open(dir, &route).with_inventory(Some(Arc::clone(&inventory))),
                ),
            );
        }
        authority.property_inventory = Some(inventory);
        Ok(())
    }

    /// Reverse map `PropId.0` → property name (ontology + runtime catalog),
    /// used by the relational lowering layer to resolve `PropertyAccess`.
    #[must_use]
    pub fn prop_names(&self) -> &HashMap<PropertyId, String> {
        &self.prop_names
    }

    /// Reverse map checked runtime identity → relation-type name from the runtime
    /// catalog. The relational lowerer tags these keys before merging them with
    /// ontology TypeIds so the two zero-based ID spaces cannot collide.
    #[must_use]
    pub fn rel_names(&self) -> &HashMap<RuntimeRelationId, String> {
        &self.rel_names
    }

    /// Reverse map checked runtime identity → entity-type (node label) name, from the
    /// runtime catalog. The relational lowerer tags these keys before merging
    /// them with ontology TypeIds so the two zero-based ID spaces cannot collide
    /// (#702).
    #[must_use]
    pub fn label_names(&self) -> &HashMap<RuntimeEntityId, String> {
        &self.label_names
    }

    /// Generation-pinned semantic relation ID to opaque physical route.
    #[must_use]
    pub fn semantic_rel_routes(&self) -> &HashMap<RelationTypeId, String> {
        &self.semantic_rel_routes
    }

    /// Generation-pinned semantic entity ID to opaque property route.
    #[must_use]
    pub fn semantic_label_routes(&self) -> &HashMap<EntityTypeId, String> {
        &self.semantic_label_routes
    }

    /// Generation-pinned semantic entity ID to exact qualified display name.
    #[must_use]
    pub fn semantic_label_names(&self) -> &HashMap<EntityTypeId, String> {
        &self.semantic_label_names
    }

    /// Exact composition fingerprint authenticating semantic routes.
    #[must_use]
    pub fn semantic_composition_fingerprint(&self) -> Option<&str> {
        self.semantic_composition_fingerprint.as_deref()
    }

    /// Registered authenticated provider for one semantic relation ID.
    #[must_use]
    pub fn semantic_edge_table(&self, id: RelationTypeId) -> Option<Arc<dyn TableProvider>> {
        let route = self.semantic_rel_routes.get(&id)?;
        self.schema
            .authority
            .read()
            .expect("graph schema lock poisoned")
            .tables
            .get(&format!("edges_{route}"))
            .cloned()
    }

    /// Registered authenticated property provider for one semantic relation ID.
    #[must_use]
    pub fn semantic_edge_property_table(
        &self,
        id: RelationTypeId,
    ) -> Option<Arc<dyn TableProvider>> {
        self.semantic_edge_property_tables.get(&id).cloned()
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
) -> HashMap<PropertyId, String> {
    runtime_catalog
        .property_names()
        .map(|(id, name)| (PropertyId::runtime(id), name.to_owned()))
        .collect()
}

/// Build the `TypeId.0 → relation-name` map from the runtime catalog.
///
/// Only the runtime-catalog side is needed here: the binder resolves relation
/// types ontology-first (so an ontology-sourced `TypeId` is already covered by
/// the lowerer's ontology map) and falls back to the `RuntimeCatalog` only in
/// exploratory mode (or advisory misses) — the case this map fills.
fn build_rel_names(runtime_catalog: &RuntimeCatalog) -> HashMap<RuntimeRelationId, String> {
    runtime_catalog
        .relation_type_names_with_ids()
        .map(|(id, name)| (id, name.to_owned()))
        .collect()
}

/// Build the checked runtime entity → label-name map from the runtime catalog.
///
/// As with [`build_rel_names`], only the runtime-catalog side is needed: the
/// binder resolves labels ontology-first, so ontology-sourced label `TypeId`s
/// are already covered by the lowerer's ontology map; this fills the exploratory
/// / advisory-miss case. Consumers must tag keys with
/// [`EntityTypeId::runtime`] before comparing them to stored
/// plan/storage TypeIds (#702 / #889).
fn build_label_names(runtime_catalog: &RuntimeCatalog) -> HashMap<RuntimeEntityId, String> {
    runtime_catalog
        .entity_type_names_with_ids()
        .map(|(id, name)| (id, name.to_owned()))
        .collect()
}

impl CatalogProvider for GraphCatalog {
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
    inventory: Option<&Arc<crate::AuthenticatedPropertyInventory>>,
) {
    if let Some(handle) = ontology {
        for (entity_name, prop_defs) in handle.entity_property_defs() {
            let prop_schema = Arc::new(property_schema(entity_name, &prop_defs));
            schema.register(
                format!("properties_{entity_name}"),
                Arc::new(inventory.map_or_else(
                    || PropertyTable::open(dir, entity_name, prop_schema),
                    |inventory| {
                        PropertyTable::open_authenticated(dir, entity_name, Arc::clone(inventory))
                    },
                )),
            );
        }
    } else {
        // Exploratory: properties are written to a single `_untyped.parquet`
        // whose column schema is inferred at write time, so register it with the
        // schema discovered from disk (just `node_uuid` until it is written).
        schema.register(
            "properties__untyped",
            Arc::new(inventory.map_or_else(
                || PropertyTable::open_discovered(dir, "_untyped"),
                |inventory| {
                    PropertyTable::open_authenticated(dir, "_untyped", Arc::clone(inventory))
                },
            )),
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
