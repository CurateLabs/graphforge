//! Deterministic graph-only workspace projections.
//!
//! This module deliberately understands only graph-owned workspace files. It
//! never enumerates or copies project-generation participants, provenance,
//! knowledge, epistemic, valid-time, search, or derived-index directories.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, ListBuilder, StringArray, StringBuilder, StructArray, UInt32Array,
    UInt64Array,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use parquet::arrow::ArrowWriter;

type GraphUuid = [u8; 16];
type EdgeEndpoints = BTreeMap<GraphUuid, (GraphUuid, GraphUuid)>;

/// Referential-closure mode for one graph projection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GraphProjectionClosure {
    /// Selected edges plus both endpoint nodes. Nodes never induce edges.
    #[default]
    Referential,
    /// Selected nodes plus every edge whose endpoints are both selected.
    InducedEdges,
}

/// Explicit graph identities requested for one projection.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphProjectionSelection {
    /// Explicit node UUIDs.
    pub node_uuids: BTreeSet<[u8; 16]>,
    /// Explicit edge UUIDs.
    pub edge_uuids: BTreeSet<[u8; 16]>,
    /// Closure semantics applied before materialization.
    pub closure: GraphProjectionClosure,
    /// Property field names excluded from projected property tables.
    pub exclude_properties: BTreeSet<String>,
}

/// Exact identities materialized into the graph-only target workspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphProjectionSummary {
    /// Canonically ordered node UUIDs, including edge endpoint closure.
    pub node_uuids: Vec<[u8; 16]>,
    /// Canonically ordered explicitly selected edge UUIDs.
    pub edge_uuids: Vec<[u8; 16]>,
    /// Endpoint UUIDs added beyond the caller's explicit node set.
    pub endpoint_node_uuids: Vec<[u8; 16]>,
    /// Domain-separated digest of canonical logical graph tables.
    pub graph_content_fingerprint: [u8; 32],
}

/// Materialize one deterministic graph-only workspace projection.
///
/// The target must be absent or empty. Node/edge UUIDs and surrogate IDs are
/// preserved. Closure semantics follow [`GraphProjectionClosure`]. Topology and
/// property rows are rewritten in UUID order. Only core graph files and
/// required ontology/runtime-catalog metadata are copied; derived indexes and
/// every non-graph domain are excluded.
///
/// # Errors
/// Returns validation for missing identities, unsafe/non-empty targets, or
/// malformed graph files, and storage errors for I/O/Arrow/Parquet failures.
pub fn materialize_graph_projection(
    source: &Path,
    target: &Path,
    selection: &GraphProjectionSelection,
) -> Result<GraphProjectionSummary, GfError> {
    materialize_graph_projection_with_options(source, target, selection, true)
}

/// Materialize a portable graph-tree projection without copying ontology files.
///
/// # Errors
/// Same as [`materialize_graph_projection`].
pub fn materialize_portable_graph_tree_projection(
    source: &Path,
    target: &Path,
    selection: &GraphProjectionSelection,
) -> Result<GraphProjectionSummary, GfError> {
    materialize_graph_projection_with_options(source, target, selection, false)
}

fn materialize_graph_projection_with_options(
    source: &Path,
    target: &Path,
    selection: &GraphProjectionSelection,
    copy_ontology_files: bool,
) -> Result<GraphProjectionSummary, GfError> {
    validate_distinct_paths(source, target)?;
    validate_graph_empty_target(target)?;

    let nodes_path = source.join("topology/nodes.parquet");
    let node_ids = uuid_rows(&nodes_path, "node_uuid")?;
    require_present(&selection.node_uuids, &node_ids, "node")?;

    let edge_files = sorted_parquet_files(&source.join("topology/edges"))?;
    let edges = edge_endpoints(&edge_files)?;
    let edge_ids = edges.keys().copied().collect::<BTreeSet<_>>();
    require_present(&selection.edge_uuids, &edge_ids, "edge")?;

    let (selected_nodes, selected_edges) =
        resolve_projection_closure(selection, &node_ids, &edges)?;
    let endpoint_node_uuids = selected_nodes
        .difference(&selection.node_uuids)
        .copied()
        .collect::<Vec<_>>();

    clear_graph_empty_target(target)?;
    fs::create_dir_all(target).map_err(storage)?;
    project_parquet_file(
        &nodes_path,
        &target.join("topology/nodes.parquet"),
        "node_uuid",
        &selected_nodes,
        &selection.exclude_properties,
    )?;
    project_parquet_directory(
        &source.join("topology/edges"),
        &target.join("topology/edges"),
        "edge_uuid",
        &selected_edges,
        &BTreeSet::new(),
    )?;
    project_property_directory(
        source,
        target,
        false,
        "node_uuid",
        &selected_nodes,
        &selection.exclude_properties,
    )?;
    project_property_directory(
        source,
        target,
        true,
        "edge_uuid",
        &selected_edges,
        &selection.exclude_properties,
    )?;
    copy_runtime_catalog(source, target)?;
    if copy_ontology_files {
        for file in [
            graphforge_core::manifest::MANIFEST_FILE,
            graphforge_core::manifest::ONTOLOGY_FILE,
        ] {
            copy_regular_file_if_present(&source.join(file), &target.join(file))?;
        }
    }
    let graph_content_fingerprint = projected_graph_fingerprint(target)?;

    Ok(GraphProjectionSummary {
        node_uuids: selected_nodes.into_iter().collect(),
        edge_uuids: selected_edges.into_iter().collect(),
        endpoint_node_uuids,
        graph_content_fingerprint,
    })
}

fn resolve_projection_closure(
    selection: &GraphProjectionSelection,
    node_ids: &BTreeSet<GraphUuid>,
    edges: &EdgeEndpoints,
) -> Result<(BTreeSet<GraphUuid>, BTreeSet<GraphUuid>), GfError> {
    match selection.closure {
        GraphProjectionClosure::Referential => {
            let mut selected_nodes = selection.node_uuids.clone();
            for edge_uuid in &selection.edge_uuids {
                let (src, dst) = edges
                    .get(edge_uuid)
                    .expect("selected edge presence was validated");
                if !node_ids.contains(src) || !node_ids.contains(dst) {
                    return Err(validation(
                        "selected edge references a missing endpoint node",
                    ));
                }
                selected_nodes.insert(*src);
                selected_nodes.insert(*dst);
            }
            Ok((selected_nodes, selection.edge_uuids.clone()))
        }
        GraphProjectionClosure::InducedEdges => {
            if !selection.edge_uuids.is_empty() {
                return Err(validation(
                    "induced-edges closure rejects explicit edge selectors",
                ));
            }
            let selected_nodes = selection.node_uuids.clone();
            let mut selected_edges = BTreeSet::new();
            for (edge_uuid, (src, dst)) in edges {
                if selected_nodes.contains(src) && selected_nodes.contains(dst) {
                    selected_edges.insert(*edge_uuid);
                }
            }
            Ok((selected_nodes, selected_edges))
        }
    }
}

fn edge_endpoints(files: &[PathBuf]) -> Result<EdgeEndpoints, GfError> {
    let mut edges = BTreeMap::new();
    for path in files {
        for batch in read_parquet(path)? {
            let edge_ids = uuid_column(&batch, "edge_uuid")?;
            let sources = uuid_column(&batch, "src_uuid")?;
            let targets = uuid_column(&batch, "dst_uuid")?;
            for row in 0..batch.num_rows() {
                let edge_uuid = uuid_at(edge_ids, row)?;
                let endpoints = (uuid_at(sources, row)?, uuid_at(targets, row)?);
                if edges.insert(edge_uuid, endpoints).is_some() {
                    return Err(validation("graph contains a duplicate edge UUID"));
                }
            }
        }
    }
    Ok(edges)
}

fn uuid_rows(path: &Path, column: &str) -> Result<BTreeSet<GraphUuid>, GfError> {
    if !path.exists() {
        return Ok(BTreeSet::new());
    }
    let mut rows = BTreeSet::new();
    for batch in read_parquet(path)? {
        let values = uuid_column(&batch, column)?;
        for row in 0..batch.num_rows() {
            if !rows.insert(uuid_at(values, row)?) {
                return Err(validation(format!("graph contains a duplicate {column}")));
            }
        }
    }
    Ok(rows)
}

fn project_parquet_directory(
    source: &Path,
    target: &Path,
    key: &str,
    selected: &BTreeSet<[u8; 16]>,
    exclude_properties: &BTreeSet<String>,
) -> Result<(), GfError> {
    for path in sorted_parquet_files(source)? {
        let name = path
            .file_name()
            .ok_or_else(|| validation("graph parquet path has no file name"))?;
        project_parquet_file(&path, &target.join(name), key, selected, exclude_properties)?;
    }
    Ok(())
}

fn project_parquet_file(
    source: &Path,
    target: &Path,
    key: &str,
    selected: &BTreeSet<[u8; 16]>,
    exclude_properties: &BTreeSet<String>,
) -> Result<(), GfError> {
    if !source.exists() {
        return Ok(());
    }
    let batches = read_parquet(source)?;
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .or_else(|| crate::catalog::discover_parquet_schema(source))
        .ok_or_else(|| validation("graph parquet schema is unavailable"))?;
    let combined = if batches.is_empty() {
        RecordBatch::new_empty(Arc::clone(&schema))
    } else {
        concat_batches(&schema, &batches).map_err(storage)?
    };
    project_record_batch(&combined, target, key, selected, exclude_properties)
}

fn project_property_directory(
    source: &Path,
    target: &Path,
    edge: bool,
    key: &str,
    selected: &BTreeSet<[u8; 16]>,
    exclude_properties: &BTreeSet<String>,
) -> Result<(), GfError> {
    let routes = if edge {
        crate::catalog::list_edge_property_stems(source)
    } else {
        crate::catalog::list_property_stems(source)
    };
    let directory = if edge {
        "edge_properties"
    } else {
        "properties"
    };
    for route in routes {
        let batches = if edge {
            crate::catalog::read_edge_properties(source, &route)
        } else {
            crate::catalog::read_properties(source, &route)
        }
        .map_err(|error| GfError::Storage(error.to_string()))?;
        let Some(schema) = batches.first().map(RecordBatch::schema) else {
            continue;
        };
        let combined = concat_batches(&schema, &batches).map_err(storage)?;
        project_record_batch(
            &combined,
            &target.join(directory).join(format!("{route}.parquet")),
            key,
            selected,
            exclude_properties,
        )?;
    }
    Ok(())
}

fn project_record_batch(
    combined: &RecordBatch,
    target: &Path,
    key: &str,
    selected: &BTreeSet<[u8; 16]>,
    exclude_properties: &BTreeSet<String>,
) -> Result<(), GfError> {
    let keys = uuid_column(combined, key)?;
    let mut rows = Vec::new();
    for row in 0..combined.num_rows() {
        let uuid = uuid_at(keys, row)?;
        if selected.contains(&uuid) {
            rows.push((uuid, row));
        }
    }
    rows.sort_unstable();
    let indices = rows
        .into_iter()
        .map(|(_, row)| {
            u32::try_from(row).map_err(|_| validation("graph projection row index exceeds UInt32"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let indices = UInt32Array::from(indices);
    let keep_columns = combined
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            field.name() == key || !exclude_properties.contains(field.name().as_str())
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let fields = keep_columns
        .iter()
        .map(|index| combined.schema().field(*index).clone())
        .collect::<Vec<_>>();
    let mut metadata = combined.schema().metadata().clone();
    // A subset changes UUID ownership counts, so it cannot inherit the source
    // route's incremental live-schema authority. The projected flat snapshot
    // remains a valid legacy complete snapshot and can be upgraded by a full
    // migration, rather than publishing false counts.
    metadata.remove(crate::property_overlay::PROPERTY_LIVE_SCHEMA_KEY);
    let projected_schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    let columns = keep_columns
        .into_iter()
        .map(|index| take(combined.column(index).as_ref(), &indices, None).map_err(storage))
        .collect::<Result<Vec<_>, _>>()?;
    let projected = RecordBatch::try_new(projected_schema, columns).map_err(storage)?;
    write_parquet(target, &projected)
}

fn copy_runtime_catalog(source: &Path, target: &Path) -> Result<(), GfError> {
    let source = source.join("topology/runtime_catalog.parquet");
    if !source.exists() {
        return Ok(());
    }
    let batches = read_parquet(&source)?;
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| validation("runtime catalog has no schema"))?;
    let batch = concat_batches(&schema, &batches).map_err(storage)?;
    let canonical = graphforge_ir::RuntimeCatalog::from_record_batch(&batch)?.to_record_batch();
    let selected = selected_catalog_rows(target, &canonical)?;
    let indices = UInt32Array::from(
        selected
            .into_iter()
            .map(|row| {
                u32::try_from(row)
                    .map_err(|_| validation("runtime catalog row index exceeds UInt32"))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let columns = canonical
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(storage))
        .collect::<Result<Vec<_>, _>>()?;
    let canonical = RecordBatch::try_new(canonical.schema(), columns).map_err(storage)?;
    write_parquet(&target.join("topology/runtime_catalog.parquet"), &canonical)
}

#[allow(
    clippy::too_many_lines,
    reason = "catalog dependency closure remains one auditable selection pass"
)]
fn selected_catalog_rows(target: &Path, catalog: &RecordBatch) -> Result<Vec<usize>, GfError> {
    let mut type_ids = BTreeSet::new();
    let nodes = target.join("topology/nodes.parquet");
    if nodes.exists() {
        for batch in read_parquet(&nodes)? {
            if let Some(column) = batch.column_by_name("type_id") {
                let values = column
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| validation("node type_id is not UInt32"))?;
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        type_ids.insert(values.value(row));
                    }
                }
            }
            if let Some(column) = batch.column_by_name("type_ids") {
                let lists = column
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| validation("node type_ids is not List"))?;
                for row in 0..lists.len() {
                    if lists.is_null(row) {
                        continue;
                    }
                    let values = lists.value(row);
                    let values = values
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .ok_or_else(|| validation("node type_ids values are not UInt32"))?;
                    type_ids.extend(values.values().iter().copied());
                }
            }
        }
    }

    let mut relation_names = BTreeSet::new();
    for path in sorted_parquet_files(&target.join("topology/edges"))? {
        let stem = parquet_stem(&path)?;
        let batches = read_parquet(&path)?;
        if stem != "_exploratory" && batches.iter().any(|batch| batch.num_rows() != 0) {
            relation_names.insert(stem);
        }
        for batch in batches {
            if let Some(column) = batch.column_by_name("rel_type_name") {
                let values = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| validation("edge rel_type_name is not Utf8"))?;
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        relation_names.insert(values.value(row).to_owned());
                    }
                }
            }
        }
    }

    let mut property_names = BTreeSet::new();
    for directory in ["properties", "edge_properties"] {
        for path in sorted_parquet_files(&target.join(directory))? {
            let batches = read_parquet(&path)?;
            if batches.iter().all(|batch| batch.num_rows() == 0) {
                continue;
            }
            let schema = batches
                .first()
                .map(RecordBatch::schema)
                .ok_or_else(|| validation("projected property table has no schema"))?;
            for field in schema.fields() {
                if !matches!(
                    field.name().as_str(),
                    "node_uuid" | "node_id" | "edge_uuid" | "edge_id"
                ) {
                    property_names.insert(field.name().clone());
                }
            }
        }
    }

    let kinds = string_column(catalog, "entry_kind")?;
    let names = string_column(catalog, "name")?;
    let ids = catalog
        .column_by_name("runtime_id")
        .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
        .ok_or_else(|| validation("runtime catalog runtime_id is not UInt32"))?;
    let owners = string_column(catalog, "owner_label")?;

    let mut active_owners = BTreeSet::new();
    for row in 0..catalog.num_rows() {
        if kinds.value(row) == "entity_type"
            && type_ids.iter().any(|stored| {
                *stored == ids.value(row)
                    || graphforge_ir::runtime_type_id_from_entity_plan_id(graphforge_core::TypeId(
                        *stored,
                    ))
                    .is_some_and(|runtime| runtime.0 == ids.value(row))
            })
        {
            active_owners.insert(names.value(row).to_owned());
        }
    }
    active_owners.extend(relation_names.iter().cloned());

    let mut selected = BTreeSet::new();
    for row in 0..catalog.num_rows() {
        let keep = match kinds.value(row) {
            "entity_type" => type_ids.iter().any(|stored| {
                *stored == ids.value(row)
                    || graphforge_ir::runtime_type_id_from_entity_plan_id(graphforge_core::TypeId(
                        *stored,
                    ))
                    .is_some_and(|runtime| runtime.0 == ids.value(row))
            }),
            "relation_type" => relation_names.contains(names.value(row)),
            "property" => {
                property_names.contains(names.value(row))
                    && (owners.is_null(row) || active_owners.contains(owners.value(row)))
            }
            _ => false,
        };
        if keep {
            selected.insert(row);
            if kinds.value(row) == "property" && !owners.is_null(row) {
                let owner = owners.value(row);
                for owner_row in 0..catalog.num_rows() {
                    if matches!(kinds.value(owner_row), "entity_type" | "relation_type")
                        && names.value(owner_row) == owner
                    {
                        selected.insert(owner_row);
                    }
                }
            }
        }
    }
    Ok(selected.into_iter().collect())
}

fn parquet_stem(path: &Path) -> Result<String, GfError> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .map(str::to_owned)
        .ok_or_else(|| validation("graph parquet path has no UTF-8 stem"))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| validation(format!("runtime catalog {name} is not Utf8")))
}

fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>, GfError> {
    let schema = crate::catalog::discover_parquet_schema(path).ok_or_else(|| {
        validation(format!(
            "cannot discover graph schema for {}",
            path.display()
        ))
    })?;
    crate::catalog::read_parquet_or_empty(path, schema)
        .map_err(|error| GfError::Storage(error.to_string()))
}

pub(crate) fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<(), GfError> {
    let parent = path
        .parent()
        .ok_or_else(|| validation("graph parquet target has no parent"))?;
    fs::create_dir_all(parent).map_err(storage)?;
    let file = File::create(path).map_err(storage)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).map_err(storage)?;
    writer.write(batch).map_err(storage)?;
    writer.close().map_err(storage)?;
    Ok(())
}

pub(crate) fn projected_graph_fingerprint(root: &Path) -> Result<[u8; 32], GfError> {
    let mut paths = Vec::new();
    let nodes = root.join("topology/nodes.parquet");
    if nodes.exists() {
        paths.push(nodes);
    }
    for directory in ["topology/edges", "properties", "edge_properties"] {
        paths.extend(sorted_parquet_files(&root.join(directory))?);
    }
    let runtime_catalog = root.join("topology/runtime_catalog.parquet");
    if runtime_catalog.exists() {
        paths.push(runtime_catalog);
    }
    paths.sort();
    fingerprint_graph_paths(root, paths)
}

/// Portable semantic graph identity excludes runtime catalog IDs while
/// retaining decoded topology, edges, node properties, and edge properties.
pub(crate) fn portable_graph_data_fingerprint(root: &Path) -> Result<[u8; 32], GfError> {
    let runtime_entity_names = portable_runtime_entity_names(root)?;
    let mut tables = Vec::<(String, RecordBatch)>::new();
    let nodes = root.join("topology/nodes.parquet");
    if nodes.exists() {
        let batches = read_parquet(&nodes)?;
        let schema = batches[0].schema();
        tables.push((
            "topology/nodes.parquet".into(),
            concat_batches(&schema, &batches).map_err(storage)?,
        ));
    }
    for path in sorted_parquet_files(&root.join("topology/edges"))? {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| validation("graph projection path escaped target"))?
            .to_string_lossy()
            .replace('\\', "/");
        let batches = read_parquet(&path)?;
        let schema = batches[0].schema();
        tables.push((
            relative,
            concat_batches(&schema, &batches).map_err(storage)?,
        ));
    }
    for (directory, is_edge) in [("properties", false), ("edge_properties", true)] {
        let stems = if is_edge {
            crate::catalog::list_edge_property_stems(root)
        } else {
            crate::catalog::list_property_stems(root)
        };
        for stem in stems {
            let batches = if is_edge {
                crate::catalog::read_edge_properties(root, &stem)
            } else {
                crate::catalog::read_properties(root, &stem)
            }
            .map_err(storage)?;
            if let Some(schema) = batches.first().map(RecordBatch::schema) {
                tables.push((
                    format!("{directory}/{stem}.parquet"),
                    concat_batches(&schema, &batches).map_err(storage)?,
                ));
            }
        }
    }
    tables.sort_by(|left, right| left.0.cmp(&right.0));
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFGP1").map_err(canonical_error)?;
    writer
        .u32(exact_u32(tables.len(), "graph table count")?)
        .map_err(canonical_error)?;
    for (relative, batch) in tables {
        writer.text(&relative).map_err(canonical_error)?;
        let logical = logical_fingerprint_batch(&relative, &batch, Some(&runtime_entity_names))?;
        encode_table(&mut writer, &logical)?;
    }
    fingerprint(
        CanonicalDomain::GraphProjection,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .map_err(canonical_error)
}

fn fingerprint_graph_paths(root: &Path, paths: Vec<PathBuf>) -> Result<[u8; 32], GfError> {
    fingerprint_graph_paths_with_runtime_names(root, paths, None)
}

fn fingerprint_graph_paths_with_runtime_names(
    root: &Path,
    paths: Vec<PathBuf>,
    runtime_entity_names: Option<&BTreeMap<u32, String>>,
) -> Result<[u8; 32], GfError> {
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFGP1").map_err(canonical_error)?;
    writer
        .u32(exact_u32(paths.len(), "graph table count")?)
        .map_err(canonical_error)?;
    for path in paths {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| validation("graph projection path escaped target"))?
            .to_str()
            .ok_or_else(|| validation("graph projection path is not UTF-8"))?;
        writer.text(relative).map_err(canonical_error)?;
        let batches = read_parquet(&path)?;
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| validation("graph projection table has no schema"))?;
        let batch = concat_batches(&schema, &batches).map_err(storage)?;
        let logical = logical_fingerprint_batch(relative, &batch, runtime_entity_names)?;
        encode_table(&mut writer, &logical)?;
    }
    fingerprint(
        CanonicalDomain::GraphProjection,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .map_err(canonical_error)
}

fn logical_fingerprint_batch(
    relative: &str,
    batch: &RecordBatch,
    runtime_entity_names: Option<&BTreeMap<u32, String>>,
) -> Result<RecordBatch, GfError> {
    let source_schema = batch.schema();
    let mut names: Vec<&str> = if relative == "topology/nodes.parquet" {
        vec!["node_uuid", "type_id", "type_ids"]
    } else if relative.starts_with("topology/edges/") {
        let mut names = vec!["edge_uuid", "src_uuid", "dst_uuid"];
        if batch.column_by_name("rel_type_name").is_some() {
            names.push("rel_type_name");
        }
        names
    } else if relative == "topology/runtime_catalog.parquet" {
        vec!["entry_kind", "name", "runtime_id", "owner_label"]
    } else {
        source_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect()
    };
    if !relative.starts_with("topology/") {
        names.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    }
    let mut fields = Vec::with_capacity(names.len());
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
        let index = source_schema
            .index_of(name)
            .map_err(|_| validation(format!("graph fingerprint field {name} is absent")))?;
        if let Some(names) = runtime_entity_names.filter(|_| {
            relative == "topology/nodes.parquet" && matches!(name, "type_id" | "type_ids")
        }) {
            let (field, column) = portable_node_type_column(name, batch.column(index), names)?;
            fields.push(field);
            columns.push(column);
        } else {
            fields.push(Arc::clone(&source_schema.fields()[index]));
            columns.push(Arc::clone(batch.column(index)));
        }
    }
    let mut metadata = source_schema.metadata().clone();
    // Incremental live-owner counts are authenticated operational authority,
    // not graph data. Portable semantic identity is representation-neutral.
    metadata.remove(crate::property_overlay::PROPERTY_LIVE_SCHEMA_KEY);
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    RecordBatch::try_new(schema, columns).map_err(storage)
}

fn portable_runtime_entity_names(root: &Path) -> Result<BTreeMap<u32, String>, GfError> {
    let path = root.join("topology/runtime_catalog.parquet");
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let batches = read_parquet(&path)?;
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| validation("runtime catalog has no schema"))?;
    let batch = concat_batches(&schema, &batches).map_err(storage)?;
    // Parsing through RuntimeCatalog applies the catalog's complete structural
    // and uniqueness validation before IDs are used as portable authority.
    let canonical = graphforge_ir::RuntimeCatalog::from_record_batch(&batch)?.to_record_batch();
    let kinds = string_column(&canonical, "entry_kind")?;
    let names = string_column(&canonical, "name")?;
    let ids = canonical
        .column_by_name("runtime_id")
        .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
        .ok_or_else(|| validation("runtime catalog runtime_id is not UInt32"))?;
    let mut result = BTreeMap::new();
    for row in 0..canonical.num_rows() {
        if kinds.value(row) == "entity_type"
            && result
                .insert(ids.value(row), names.value(row).to_owned())
                .is_some()
        {
            return Err(validation("runtime catalog has a duplicate entity type ID"));
        }
    }
    Ok(result)
}

fn portable_type_name(id: u32, runtime: &BTreeMap<u32, String>) -> Result<String, GfError> {
    let id = graphforge_core::TypeId(id);
    if graphforge_ir::is_runtime_entity_type_id(id) {
        let local = graphforge_ir::runtime_type_id_from_entity_plan_id(id)
            .expect("runtime entity tag checked")
            .0;
        return runtime
            .get(&local)
            .map(|name| format!("runtime-entity:{name}"))
            .ok_or_else(|| validation("node runtime type ID has no catalog name"));
    }
    if id.0 & graphforge_ir::RUNTIME_RELATION_TYPE_TAG != 0 {
        return Err(validation("node type carries a runtime relation tag"));
    }
    // Untagged IDs are generation-bound semantic storage IDs. Their stable
    // namespace is deliberately distinct from runtime names; #872 validates
    // these IDs against the authenticated semantic-binding participant when a
    // generation is admitted.
    Ok(format!("semantic-storage-id:{}", id.0))
}

fn portable_node_type_column(
    name: &str,
    column: &ArrayRef,
    runtime: &BTreeMap<u32, String>,
) -> Result<(Arc<Field>, ArrayRef), GfError> {
    if name == "type_id" {
        let values = column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| validation("node type_id is not UInt32"))?;
        let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 32);
        for row in 0..values.len() {
            if values.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value(portable_type_name(values.value(row), runtime)?);
            }
        }
        return Ok((
            Arc::new(Field::new(name, DataType::Utf8, true)),
            Arc::new(builder.finish()),
        ));
    }
    let lists = column
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| validation("node type_ids is not List"))?;
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in 0..lists.len() {
        if lists.is_null(row) {
            builder.append(false);
            continue;
        }
        let values = lists.value(row);
        let values = values
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| validation("node type_ids values are not UInt32"))?;
        let mut resolved = Vec::with_capacity(values.len());
        for item in 0..values.len() {
            if values.is_null(item) {
                return Err(validation("node type_ids contains null"));
            }
            resolved.push(portable_type_name(values.value(item), runtime)?);
        }
        resolved.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        if resolved.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(validation("node type_ids contains a duplicate assignment"));
        }
        for value in resolved {
            builder.values().append_value(value);
        }
        builder.append(true);
    }
    let values: ArrayRef = Arc::new(builder.finish());
    Ok((
        Arc::new(Field::new(name, values.data_type().clone(), true)),
        values,
    ))
}

fn encode_table(writer: &mut CanonicalWriter, batch: &RecordBatch) -> Result<(), GfError> {
    encode_schema(writer, batch.schema().as_ref())?;
    writer
        .u64(exact_u64(batch.num_rows(), "graph row count")?)
        .map_err(canonical_error)?;
    let schema = batch.schema();
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            let logical = dictionary_value_type(field.data_type());
            if logical == field.data_type() {
                Ok((logical, Arc::clone(column)))
            } else {
                arrow::compute::cast(column, logical)
                    .map(|decoded| (logical, decoded))
                    .map_err(storage)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    for row in 0..batch.num_rows() {
        for (field, (data_type, column)) in schema.fields().iter().zip(&columns) {
            encode_value(writer, data_type, column, row, field.is_nullable())?;
        }
    }
    Ok(())
}

fn encode_schema(writer: &mut CanonicalWriter, schema: &Schema) -> Result<(), GfError> {
    writer.raw(b"GFS1").map_err(canonical_error)?;
    writer
        .u32(exact_u32(schema.fields().len(), "graph field count")?)
        .map_err(canonical_error)?;
    for field in &schema.fields {
        encode_field(writer, field)?;
    }
    let ordered = schema.metadata().iter().collect::<BTreeMap<_, _>>();
    writer
        .u32(exact_u32(ordered.len(), "graph metadata count")?)
        .map_err(canonical_error)?;
    for (key, value) in ordered {
        writer.text(key).map_err(canonical_error)?;
        writer.text(value).map_err(canonical_error)?;
    }
    Ok(())
}

fn encode_field(writer: &mut CanonicalWriter, field: &Field) -> Result<(), GfError> {
    writer.text(field.name()).map_err(canonical_error)?;
    writer
        .u8(u8::from(field.is_nullable()))
        .map_err(canonical_error)?;
    encode_type(writer, field.data_type())
}

fn encode_type(writer: &mut CanonicalWriter, data_type: &DataType) -> Result<(), GfError> {
    match data_type {
        DataType::Boolean => writer.u8(0x02),
        DataType::Int32 => writer.u8(0x12),
        DataType::Int64 => writer.u8(0x13),
        DataType::UInt32 => writer.u8(0x16),
        DataType::UInt64 => writer.u8(0x17),
        DataType::Float32 => writer.u8(0x21),
        DataType::Float64 => writer.u8(0x22),
        DataType::Utf8 | DataType::LargeUtf8 => writer.u8(0x30),
        DataType::Binary | DataType::LargeBinary => writer.u8(0x31),
        DataType::FixedSizeBinary(width) => {
            writer.u8(0x32).map_err(canonical_error)?;
            writer.u32(
                u32::try_from(*width)
                    .map_err(|_| validation("negative fixed-size binary width"))?,
            )
        }
        DataType::Timestamp(unit, timezone) => {
            validate_timezone(timezone.as_deref())?;
            writer.u8(0x52).map_err(canonical_error)?;
            writer.u8(time_unit_tag(*unit))
        }
        DataType::Time64(unit) => {
            writer.u8(0x53).map_err(canonical_error)?;
            writer.u8(time_unit_tag(*unit))
        }
        DataType::List(field) | DataType::LargeList(field) => {
            writer.u8(0x60).map_err(canonical_error)?;
            encode_field(writer, field)?;
            return Ok(());
        }
        DataType::FixedSizeList(field, length) => {
            writer.u8(0x61).map_err(canonical_error)?;
            writer
                .u32(u32::try_from(*length).map_err(|_| validation("negative fixed-list length"))?)
                .map_err(canonical_error)?;
            encode_field(writer, field)?;
            return Ok(());
        }
        DataType::Struct(fields) => {
            writer.u8(0x62).map_err(canonical_error)?;
            writer
                .u32(exact_u32(fields.len(), "struct field count")?)
                .map_err(canonical_error)?;
            for field in fields {
                encode_field(writer, field)?;
            }
            return Ok(());
        }
        DataType::Dictionary(_, value) => return encode_type(writer, value),
        other => return Err(validation(format!("unsupported graph Arrow type {other}"))),
    }
    .map_err(canonical_error)
}

fn encode_value(
    writer: &mut CanonicalWriter,
    data_type: &DataType,
    array: &ArrayRef,
    row: usize,
    nullable: bool,
) -> Result<(), GfError> {
    if array.is_null(row) {
        if !nullable {
            return Err(validation("non-nullable graph field contains null"));
        }
        writer.u8(0).map_err(canonical_error)?;
        return Ok(());
    }
    writer.u8(1).map_err(canonical_error)?;
    encode_present_value(writer, data_type, array, row)
}

#[allow(clippy::too_many_lines)]
fn encode_present_value(
    writer: &mut CanonicalWriter,
    data_type: &DataType,
    array: &ArrayRef,
    row: usize,
) -> Result<(), GfError> {
    macro_rules! write {
        ($value:expr) => {
            $value.map_err(canonical_error)?
        };
    }
    match data_type {
        DataType::Boolean => {
            write!(writer.u8(u8::from(downcast::<BooleanArray>(array)?.value(row))));
        }
        DataType::Int32 => {
            write!(writer.raw(&downcast::<Int32Array>(array)?.value(row).to_be_bytes()));
        }
        DataType::Int64 => write!(writer.i64(downcast::<Int64Array>(array)?.value(row))),
        DataType::UInt32 => write!(writer.u32(downcast::<UInt32Array>(array)?.value(row))),
        DataType::UInt64 => write!(writer.u64(downcast::<UInt64Array>(array)?.value(row))),
        DataType::Float32 => {
            write!(writer.u32(normalize_f32(downcast::<Float32Array>(array)?.value(row))));
        }
        DataType::Float64 => {
            write!(writer.u64(normalize_f64(downcast::<Float64Array>(array)?.value(row))));
        }
        DataType::Utf8 => write!(writer.text(downcast::<StringArray>(array)?.value(row))),
        DataType::LargeUtf8 => {
            write!(writer.text(downcast::<LargeStringArray>(array)?.value(row)));
        }
        DataType::Binary => write!(writer.binary(downcast::<BinaryArray>(array)?.value(row))),
        DataType::LargeBinary => {
            write!(writer.binary(downcast::<LargeBinaryArray>(array)?.value(row)));
        }
        DataType::FixedSizeBinary(_) => {
            write!(writer.raw(downcast::<FixedSizeBinaryArray>(array)?.value(row)));
        }
        DataType::Timestamp(unit, timezone) => {
            validate_timezone(timezone.as_deref())?;
            write!(writer.i64(timestamp_value(array, *unit, row)?));
        }
        DataType::Time64(unit) => write!(writer.i64(time64_value(array, *unit, row)?)),
        DataType::List(field) => {
            encode_list(writer, field, &downcast::<ListArray>(array)?.value(row))?;
        }
        DataType::LargeList(field) => {
            encode_list(
                writer,
                field,
                &downcast::<LargeListArray>(array)?.value(row),
            )?;
        }
        DataType::FixedSizeList(field, _) => {
            encode_list(
                writer,
                field,
                &downcast::<FixedSizeListArray>(array)?.value(row),
            )?;
        }
        DataType::Struct(fields) => {
            let values = downcast::<StructArray>(array)?;
            for (field, child) in fields.iter().zip(values.columns()) {
                encode_value(writer, field.data_type(), child, row, field.is_nullable())?;
            }
        }
        DataType::Dictionary(_, value) => {
            let decoded = arrow::compute::cast(array, value).map_err(storage)?;
            encode_present_value(writer, value, &decoded, row)?;
        }
        other => return Err(validation(format!("unsupported graph Arrow value {other}"))),
    }
    Ok(())
}

fn encode_list(
    writer: &mut CanonicalWriter,
    field: &Field,
    values: &ArrayRef,
) -> Result<(), GfError> {
    writer
        .u64(exact_u64(values.len(), "graph list length")?)
        .map_err(canonical_error)?;
    for index in 0..values.len() {
        encode_value(
            writer,
            field.data_type(),
            values,
            index,
            field.is_nullable(),
        )?;
    }
    Ok(())
}

fn dictionary_value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => value,
        other => other,
    }
}

fn downcast<T: 'static>(array: &ArrayRef) -> Result<&T, GfError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| validation("graph Arrow array/type mismatch"))
}

fn timestamp_value(array: &ArrayRef, unit: TimeUnit, row: usize) -> Result<i64, GfError> {
    Ok(match unit {
        TimeUnit::Second => downcast::<arrow::array::TimestampSecondArray>(array)?.value(row),
        TimeUnit::Millisecond => {
            downcast::<arrow::array::TimestampMillisecondArray>(array)?.value(row)
        }
        TimeUnit::Microsecond => {
            downcast::<arrow::array::TimestampMicrosecondArray>(array)?.value(row)
        }
        TimeUnit::Nanosecond => {
            downcast::<arrow::array::TimestampNanosecondArray>(array)?.value(row)
        }
    })
}

fn time64_value(array: &ArrayRef, unit: TimeUnit, row: usize) -> Result<i64, GfError> {
    match unit {
        TimeUnit::Microsecond => {
            Ok(downcast::<arrow::array::Time64MicrosecondArray>(array)?.value(row))
        }
        TimeUnit::Nanosecond => {
            Ok(downcast::<arrow::array::Time64NanosecondArray>(array)?.value(row))
        }
        _ => Err(validation(
            "Time64 must use microsecond or nanosecond units",
        )),
    }
}

fn validate_timezone(timezone: Option<&str>) -> Result<(), GfError> {
    if timezone.is_none_or(|value| matches!(value, "UTC" | "Etc/UTC" | "Z" | "+00:00")) {
        Ok(())
    } else {
        Err(validation("graph timestamp timezone is not canonical UTC"))
    }
}

const fn time_unit_tag(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

fn normalize_f32(value: f32) -> u32 {
    if value.is_nan() {
        0x7fc0_0000
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

fn normalize_f64(value: f64) -> u64 {
    if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

fn exact_u32(value: usize, field: &str) -> Result<u32, GfError> {
    u32::try_from(value).map_err(|_| validation(format!("{field} exceeds UInt32")))
}

fn exact_u64(value: usize, field: &str) -> Result<u64, GfError> {
    u64::try_from(value).map_err(|_| validation(format!("{field} exceeds UInt64")))
}

fn canonical_error(error: impl std::fmt::Display) -> GfError {
    validation(error.to_string())
}

fn sorted_parquet_files(directory: &Path) -> Result<Vec<PathBuf>, GfError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(storage(error)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(storage)?;
        let file_type = entry.file_type().map_err(storage)?;
        if file_type.is_symlink() {
            return Err(validation("graph directory contains a symbolic link"));
        }
        let path = entry.path();
        if file_type.is_file()
            && path.extension().and_then(|value| value.to_str()) == Some("parquet")
        {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .filter(|column| column.value_length() == 16)
        .ok_or_else(|| validation(format!("graph column {name} must be FixedSizeBinary(16)")))
}

fn uuid_at(column: &FixedSizeBinaryArray, row: usize) -> Result<[u8; 16], GfError> {
    if column.is_null(row) {
        return Err(validation("graph UUID column contains null"));
    }
    column
        .value(row)
        .try_into()
        .map_err(|_| validation("graph UUID has invalid width"))
}

fn require_present(
    requested: &BTreeSet<[u8; 16]>,
    available: &BTreeSet<[u8; 16]>,
    kind: &str,
) -> Result<(), GfError> {
    if requested.is_subset(available) {
        Ok(())
    } else {
        Err(validation(format!(
            "graph projection references a missing {kind} UUID"
        )))
    }
}

fn validate_distinct_paths(source: &Path, target: &Path) -> Result<(), GfError> {
    let source = source.canonicalize().map_err(storage)?;
    let target = target
        .canonicalize()
        .or_else(|_| {
            target
                .parent()
                .ok_or_else(|| std::io::Error::other("target has no parent"))?
                .canonicalize()
                .map(|parent| parent.join(target.file_name().unwrap_or_default()))
        })
        .map_err(storage)?;
    if source == target || target.starts_with(&source) || source.starts_with(&target) {
        return Err(validation(
            "graph projection source and target must be disjoint",
        ));
    }
    Ok(())
}

fn validate_graph_empty_target(target: &Path) -> Result<(), GfError> {
    match fs::symlink_metadata(target) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(validation("graph projection target must be a directory"))
        }
        Ok(_) => {
            for entry in fs::read_dir(target).map_err(storage)? {
                let entry = entry.map_err(storage)?;
                let name = entry.file_name();
                let name = name
                    .to_str()
                    .ok_or_else(|| validation("graph projection target name is not UTF-8"))?;
                match name {
                    "topology" => validate_empty_topology(&entry.path())?,
                    "properties" | "edge_properties" => {
                        validate_empty_parquet_directory(&entry.path())?;
                    }
                    value
                        if value == graphforge_core::manifest::MANIFEST_FILE
                            || value == graphforge_core::manifest::ONTOLOGY_FILE =>
                    {
                        if !entry.file_type().map_err(storage)?.is_file() {
                            return Err(validation("graph target metadata is not a regular file"));
                        }
                    }
                    _ => {
                        return Err(validation(
                            "graph projection target contains non-graph or non-empty state",
                        ));
                    }
                }
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage(error)),
    }
}

fn validate_empty_topology(directory: &Path) -> Result<(), GfError> {
    for entry in fs::read_dir(directory).map_err(storage)? {
        let entry = entry.map_err(storage)?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| validation("topology target name is not UTF-8"))?;
        match name {
            "edges" => validate_empty_parquet_directory(&entry.path())?,
            "nodes.parquet" => require_empty_parquet(&entry.path())?,
            "runtime_catalog.parquet" | "generation.json" => {
                if !entry.file_type().map_err(storage)?.is_file() {
                    return Err(validation("graph target metadata is not a regular file"));
                }
            }
            _ => return Err(validation("graph projection target topology is not empty")),
        }
    }
    Ok(())
}

fn validate_empty_parquet_directory(directory: &Path) -> Result<(), GfError> {
    for entry in fs::read_dir(directory).map_err(storage)? {
        let entry = entry.map_err(storage)?;
        let path = entry.path();
        if !entry.file_type().map_err(storage)?.is_file()
            || path.extension().and_then(|value| value.to_str()) != Some("parquet")
        {
            return Err(validation(
                "graph projection target graph directory is not empty",
            ));
        }
        require_empty_parquet(&path)?;
    }
    Ok(())
}

fn require_empty_parquet(path: &Path) -> Result<(), GfError> {
    let rows = read_parquet(path)?
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    if rows == 0 {
        Ok(())
    } else {
        Err(validation(
            "graph projection target already contains graph rows",
        ))
    }
}

fn clear_graph_empty_target(target: &Path) -> Result<(), GfError> {
    if !target.exists() {
        return Ok(());
    }
    for name in ["topology", "properties", "edge_properties"] {
        let path = target.join(name);
        if path.exists() {
            fs::remove_dir_all(path).map_err(storage)?;
        }
    }
    for name in [
        graphforge_core::manifest::MANIFEST_FILE,
        graphforge_core::manifest::ONTOLOGY_FILE,
    ] {
        let path = target.join(name);
        if path.exists() {
            fs::remove_file(path).map_err(storage)?;
        }
    }
    Ok(())
}

fn copy_regular_file_if_present(source: &Path, target: &Path) -> Result<(), GfError> {
    let metadata = match fs::symlink_metadata(source) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    if !metadata.file_type().is_file() {
        return Err(validation("graph metadata must be a regular file"));
    }
    let parent = target
        .parent()
        .ok_or_else(|| validation("graph metadata target has no parent"))?;
    fs::create_dir_all(parent).map_err(storage)?;
    fs::copy(source, target).map_err(storage)?;
    Ok(())
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryBuilder, Float32Array, Float64Array,
        Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, ListArray, StringArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
        UInt64Array,
    };
    use graphforge_core::uuid::Uuid;
    use graphforge_core::{OntologyMode, TypeId};
    use graphforge_ir::{IrLiteral, RuntimeCatalog};
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::{GraphWriter, read_edge_properties, read_nodes, read_properties};

    const TS: i64 = 1_700_000_000_000_000;

    fn uuid(marker: u8) -> Uuid {
        let mut bytes = [0_u8; 16];
        bytes[15] = marker;
        Uuid::from_bytes(bytes)
    }

    fn fixture() -> (TempDir, [Uuid; 3], [Uuid; 2]) {
        let source = TempDir::new().unwrap();
        let nodes = [uuid(3), uuid(1), uuid(2)];
        let edges = [uuid(11), uuid(12)];
        let mut writer =
            GraphWriter::open_at(source.path(), OntologyMode::Exploratory, TS).unwrap();
        writer
            .create_node_with_labels(nodes[0], &[TypeId(7), TypeId(9)])
            .unwrap();
        writer.create_node(nodes[1], TypeId(8)).unwrap();
        writer.create_node(nodes[2], TypeId(10)).unwrap();
        for (index, node) in nodes.iter().enumerate() {
            writer
                .set_properties(
                    node,
                    None,
                    HashMap::from([("value".into(), IrLiteral::Int(index as i64))]),
                )
                .unwrap();
        }
        writer
            .create_edge(edges[0], "KNOWS", &nodes[0], &nodes[1])
            .unwrap();
        writer
            .create_edge(edges[1], "KNOWS", &nodes[1], &nodes[2])
            .unwrap();
        writer
            .set_edge_properties(
                &edges[0],
                Some("KNOWS"),
                HashMap::from([("weight".into(), IrLiteral::Float(0.75))]),
            )
            .unwrap();
        writer.flush().unwrap();

        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label("Person");
        catalog.intern_relation_type("KNOWS");
        catalog.intern_property("value", Some("Person"));
        write_parquet(
            &source.path().join("topology/runtime_catalog.parquet"),
            &catalog.to_record_batch(),
        )
        .unwrap();
        fs::write(
            source.path().join(graphforge_core::manifest::MANIFEST_FILE),
            b"ontology: ontology.yaml\n",
        )
        .unwrap();
        fs::write(
            source.path().join(graphforge_core::manifest::ONTOLOGY_FILE),
            b"version: 1\n",
        )
        .unwrap();
        for excluded in [
            "knowledge",
            "epistemic",
            "provenance",
            "valid_time",
            "indexes",
        ] {
            fs::create_dir_all(source.path().join(excluded)).unwrap();
            fs::write(
                source.path().join(excluded).join("must-not-copy"),
                b"secret",
            )
            .unwrap();
        }
        (source, nodes, edges)
    }

    #[test]
    fn projection_preserves_graph_rows_closes_endpoints_and_never_induces_edges() {
        let (source, nodes, edges) = fixture();
        let target = TempDir::new().unwrap();
        let summary = materialize_graph_projection(
            source.path(),
            target.path(),
            &GraphProjectionSelection {
                node_uuids: BTreeSet::from([*nodes[2].as_bytes()]),
                edge_uuids: BTreeSet::from([*edges[0].as_bytes()]),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(
            summary.node_uuids,
            vec![
                *nodes[1].as_bytes(),
                *nodes[2].as_bytes(),
                *nodes[0].as_bytes(),
            ]
        );
        assert_eq!(
            summary.endpoint_node_uuids,
            vec![*nodes[1].as_bytes(), *nodes[0].as_bytes()]
        );
        assert_eq!(summary.edge_uuids, vec![*edges[0].as_bytes()]);

        let source_nodes = read_nodes(source.path()).unwrap();
        let projected_nodes = read_nodes(target.path()).unwrap();
        let source_ids = id_map(&source_nodes[0], "node_uuid", "node_id");
        let projected_ids = id_map(&projected_nodes[0], "node_uuid", "node_id");
        assert_eq!(projected_ids.len(), 3);
        for uuid in &summary.node_uuids {
            assert_eq!(projected_ids.get(uuid), source_ids.get(uuid));
        }
        let labels = projected_nodes[0]
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let selected_row = summary
            .node_uuids
            .iter()
            .position(|uuid| uuid == nodes[0].as_bytes())
            .unwrap();
        let values = labels.value(selected_row);
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values(),
            &[7, 9]
        );

        let projected_edges =
            read_parquet(&target.path().join("topology/edges/_exploratory.parquet")).unwrap();
        assert_eq!(projected_edges[0].num_rows(), 1);
        assert_eq!(
            uuid_at(uuid_column(&projected_edges[0], "edge_uuid").unwrap(), 0).unwrap(),
            *edges[0].as_bytes()
        );
        let source_edge_ids = id_map(
            &read_parquet(&source.path().join("topology/edges/_exploratory.parquet")).unwrap()[0],
            "edge_uuid",
            "edge_id",
        );
        let projected_edge_ids = id_map(&projected_edges[0], "edge_uuid", "edge_id");
        assert_eq!(
            projected_edge_ids.get(edges[0].as_bytes()),
            source_edge_ids.get(edges[0].as_bytes())
        );

        assert_eq!(
            read_properties(target.path(), "_untyped")
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            3
        );
        assert_eq!(
            read_edge_properties(target.path(), "KNOWS")
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        assert!(
            target
                .path()
                .join("topology/runtime_catalog.parquet")
                .exists()
        );
        assert!(
            target
                .path()
                .join(graphforge_core::manifest::MANIFEST_FILE)
                .exists()
        );
        assert!(
            target
                .path()
                .join(graphforge_core::manifest::ONTOLOGY_FILE)
                .exists()
        );
        for excluded in [
            "knowledge",
            "epistemic",
            "provenance",
            "valid_time",
            "indexes",
        ] {
            assert!(!target.path().join(excluded).exists());
        }
    }

    #[test]
    fn projection_is_canonically_ordered_and_reproducible() {
        let (source, nodes, edges) = fixture();
        let first = TempDir::new().unwrap();
        let second = TempDir::new().unwrap();
        let selection = GraphProjectionSelection {
            node_uuids: BTreeSet::from([*nodes[2].as_bytes()]),
            edge_uuids: BTreeSet::from([*edges[0].as_bytes()]),
            ..Default::default()
        };
        let left = materialize_graph_projection(source.path(), first.path(), &selection).unwrap();
        let right = materialize_graph_projection(source.path(), second.path(), &selection).unwrap();
        assert_eq!(left, right);
        assert_ne!(left.graph_content_fingerprint, [0; 32]);
        for relative in [
            "topology/nodes.parquet",
            "topology/edges/_exploratory.parquet",
            "properties/_untyped.parquet",
            "edge_properties/KNOWS.parquet",
            "topology/runtime_catalog.parquet",
        ] {
            assert_eq!(
                fs::read(first.path().join(relative)).unwrap(),
                fs::read(second.path().join(relative)).unwrap(),
                "non-deterministic output for {relative}"
            );
        }
    }

    #[test]
    fn projection_fingerprint_ignores_parquet_chunking_and_dictionary_layout() {
        let (source, nodes, edges) = fixture();
        let baseline_target = TempDir::new().unwrap();
        let rewritten_target = TempDir::new().unwrap();
        let selection = GraphProjectionSelection {
            node_uuids: BTreeSet::from([*nodes[2].as_bytes()]),
            edge_uuids: BTreeSet::from([*edges[0].as_bytes()]),
            ..Default::default()
        };
        let baseline =
            materialize_graph_projection(source.path(), baseline_target.path(), &selection)
                .unwrap();

        let mut paths = vec![
            source.path().join("topology/nodes.parquet"),
            source.path().join("topology/edges/_exploratory.parquet"),
            source.path().join("topology/runtime_catalog.parquet"),
        ];
        for (kind, route) in [
            (crate::property_overlay::PropertyRouteKind::Node, "_untyped"),
            (crate::property_overlay::PropertyRouteKind::Edge, "KNOWS"),
        ] {
            paths.extend(
                crate::property_overlay::enumerate_property_fragments(source.path(), kind, route)
                    .unwrap()
                    .into_iter()
                    .map(|fragment| fragment.path),
            );
        }
        for path in paths {
            let batches = read_parquet(&path).unwrap();
            let schema = batches[0].schema();
            let replacement = path.with_extension("rewritten");
            let file = fs::File::create(&replacement).unwrap();
            let properties = WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_max_row_group_row_count(Some(1))
                .build();
            let mut writer = ArrowWriter::try_new(file, schema, Some(properties)).unwrap();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    writer.write(&batch.slice(row, 1)).unwrap();
                }
            }
            writer.close().unwrap();
            fs::rename(replacement, path).unwrap();
        }

        let rewritten =
            materialize_graph_projection(source.path(), rewritten_target.path(), &selection)
                .unwrap();
        assert_eq!(
            baseline.graph_content_fingerprint,
            rewritten.graph_content_fingerprint
        );
    }

    #[test]
    fn unrelated_runtime_catalog_entries_do_not_change_projection_identity() {
        let (source, nodes, edges) = fixture();
        let first = TempDir::new().unwrap();
        let second = TempDir::new().unwrap();
        let selection = GraphProjectionSelection {
            node_uuids: BTreeSet::from([*nodes[0].as_bytes()]),
            edge_uuids: BTreeSet::from([*edges[0].as_bytes()]),
            ..Default::default()
        };
        let baseline =
            materialize_graph_projection(source.path(), first.path(), &selection).unwrap();

        let catalog_path = source.path().join("topology/runtime_catalog.parquet");
        let batch = read_parquet(&catalog_path).unwrap().remove(0);
        let mut catalog = RuntimeCatalog::from_record_batch(&batch).unwrap();
        catalog.intern_label("Unrelated");
        catalog.intern_relation_type("IGNORES");
        catalog.intern_property("noise", Some("Unrelated"));
        write_parquet(&catalog_path, &catalog.to_record_batch()).unwrap();

        let with_noise =
            materialize_graph_projection(source.path(), second.path(), &selection).unwrap();
        assert_eq!(
            baseline.graph_content_fingerprint,
            with_noise.graph_content_fingerprint
        );
        assert_eq!(
            fs::read(first.path().join("topology/runtime_catalog.parquet")).unwrap(),
            fs::read(second.path().join("topology/runtime_catalog.parquet")).unwrap()
        );
        let projected = read_parquet(&second.path().join("topology/runtime_catalog.parquet"))
            .unwrap()
            .remove(0);
        let names = string_column(&projected, "name").unwrap();
        assert!(!(0..names.len()).any(|row| names.value(row) == "Unrelated"));
        assert!(!(0..names.len()).any(|row| names.value(row) == "IGNORES"));
        assert!(!(0..names.len()).any(|row| names.value(row) == "noise"));
    }

    #[test]
    fn typed_projection_keeps_exact_owned_catalog_and_reopens_graph_rows() {
        let source = TempDir::new().unwrap();
        let (alice, bob, excluded) = (uuid(31), uuid(32), uuid(33));
        let (knows, ignores) = (uuid(41), uuid(42));
        let mut catalog = RuntimeCatalog::new();
        let person = catalog.intern_label("Person");
        let company = catalog.intern_label("Company");
        catalog.intern_relation_type("KNOWS");
        catalog.intern_relation_type("IGNORES");
        catalog.intern_property("name", Some("Person"));
        catalog.intern_property("global", None);
        catalog.intern_property("since", Some("KNOWS"));
        catalog.intern_property("noise", Some("Company"));

        let mut writer = GraphWriter::open_at(source.path(), OntologyMode::Strict, TS).unwrap();
        writer.create_node(alice, TypeId(person.0)).unwrap();
        writer.create_node(bob, TypeId(person.0)).unwrap();
        writer.create_node(excluded, TypeId(company.0)).unwrap();
        writer.create_edge(knows, "KNOWS", &alice, &bob).unwrap();
        writer
            .create_edge(ignores, "IGNORES", &alice, &excluded)
            .unwrap();
        writer
            .set_properties(
                &alice,
                Some("Person"),
                HashMap::from([
                    ("name".into(), IrLiteral::Str("Alice".into())),
                    ("global".into(), IrLiteral::Bool(true)),
                ]),
            )
            .unwrap();
        writer
            .set_properties(
                &excluded,
                Some("Company"),
                HashMap::from([("noise".into(), IrLiteral::Str("exclude".into()))]),
            )
            .unwrap();
        writer
            .set_edge_properties(
                &knows,
                Some("KNOWS"),
                HashMap::from([("since".into(), IrLiteral::Int(2020))]),
            )
            .unwrap();
        writer.flush().unwrap();
        write_parquet(
            &source.path().join("topology/runtime_catalog.parquet"),
            &catalog.to_record_batch(),
        )
        .unwrap();

        let target = TempDir::new().unwrap();
        let summary = materialize_graph_projection(
            source.path(),
            target.path(),
            &GraphProjectionSelection {
                node_uuids: BTreeSet::from([*alice.as_bytes()]),
                edge_uuids: BTreeSet::from([*knows.as_bytes()]),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(summary.node_uuids, vec![*alice.as_bytes(), *bob.as_bytes()]);
        assert_eq!(summary.edge_uuids, vec![*knows.as_bytes()]);
        assert_eq!(summary.endpoint_node_uuids, vec![*bob.as_bytes()]);

        let projected = read_parquet(&target.path().join("topology/runtime_catalog.parquet"))
            .unwrap()
            .remove(0);
        let kinds = string_column(&projected, "entry_kind").unwrap();
        let names = string_column(&projected, "name").unwrap();
        let owners = string_column(&projected, "owner_label").unwrap();
        let inventory = (0..projected.num_rows())
            .map(|row| {
                (
                    kinds.value(row).to_owned(),
                    names.value(row).to_owned(),
                    (!owners.is_null(row)).then(|| owners.value(row).to_owned()),
                )
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            inventory,
            BTreeSet::from([
                ("entity_type".into(), "Person".into(), None),
                ("relation_type".into(), "KNOWS".into(), None),
                ("property".into(), "global".into(), None),
                ("property".into(), "name".into(), Some("Person".into())),
                ("property".into(), "since".into(), Some("KNOWS".into())),
            ])
        );

        let reopened_nodes = read_nodes(target.path()).unwrap();
        assert_eq!(
            reopened_nodes
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        assert_eq!(
            read_properties(target.path(), "Person").unwrap()[0].num_rows(),
            1
        );
        assert_eq!(
            read_edge_properties(target.path(), "KNOWS").unwrap()[0].num_rows(),
            1
        );
        let projected_again = projected_graph_fingerprint(target.path()).unwrap();
        assert_eq!(projected_again, summary.graph_content_fingerprint);
    }

    #[test]
    fn existing_graph_empty_hydrated_workspace_is_a_valid_target() {
        let (source, nodes, _) = fixture();
        let target = TempDir::new().unwrap();
        write_parquet(
            &target.path().join("topology/nodes.parquet"),
            &RecordBatch::new_empty(Arc::clone(&crate::TOPOLOGY_NODES_SCHEMA)),
        )
        .unwrap();
        write_parquet(
            &target.path().join("topology/runtime_catalog.parquet"),
            &RuntimeCatalog::new().to_record_batch(),
        )
        .unwrap();
        fs::write(
            target.path().join("topology/generation.json"),
            b"{\"topology_generation\":0,\"search_generation\":0}\n",
        )
        .unwrap();

        let summary = materialize_graph_projection(
            source.path(),
            target.path(),
            &GraphProjectionSelection {
                node_uuids: BTreeSet::from([*nodes[0].as_bytes()]),
                edge_uuids: BTreeSet::new(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(summary.node_uuids, vec![*nodes[0].as_bytes()]);
        assert_eq!(read_nodes(target.path()).unwrap()[0].num_rows(), 1);
        assert!(!target.path().join("topology/generation.json").exists());
    }

    #[test]
    fn empty_projection_target_validation_rejects_nonregular_metadata_and_graph_entries() {
        let target = TempDir::new().unwrap();
        fs::create_dir(target.path().join(graphforge_core::manifest::MANIFEST_FILE)).unwrap();
        assert_eq!(
            validate_graph_empty_target(target.path())
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );

        let target = TempDir::new().unwrap();
        let properties = target.path().join("properties");
        fs::create_dir(&properties).unwrap();
        fs::write(properties.join("not-parquet.txt"), b"preserve").unwrap();
        assert_eq!(
            validate_graph_empty_target(target.path())
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        assert_eq!(
            fs::read(properties.join("not-parquet.txt")).unwrap(),
            b"preserve"
        );

        let target = TempDir::new().unwrap();
        let edges = target.path().join("topology/edges");
        fs::create_dir_all(&edges).unwrap();
        fs::create_dir(edges.join("nested.parquet")).unwrap();
        assert_eq!(
            validate_graph_empty_target(target.path())
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
    }

    #[test]
    fn missing_identity_and_nonempty_target_fail_before_writing() {
        let (source, _, _) = fixture();
        let target = TempDir::new().unwrap();
        let missing = uuid(99);
        let error = materialize_graph_projection(
            source.path(),
            target.path(),
            &GraphProjectionSelection {
                node_uuids: BTreeSet::from([*missing.as_bytes()]),
                edge_uuids: BTreeSet::new(),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(matches!(error, GfError::Validation(_)));
        assert!(fs::read_dir(target.path()).unwrap().next().is_none());

        fs::write(target.path().join("owned"), b"keep").unwrap();
        let error = materialize_graph_projection(
            source.path(),
            target.path(),
            &GraphProjectionSelection::default(),
        )
        .unwrap_err();
        assert!(matches!(error, GfError::Validation(_)));
        assert_eq!(fs::read(target.path().join("owned")).unwrap(), b"keep");
    }

    #[test]
    fn projection_rejects_overlapping_and_nonempty_targets_without_mutation() {
        let (source, _, _) = fixture();
        let empty = GraphProjectionSelection::default();
        let same_error =
            materialize_graph_projection(source.path(), source.path(), &empty).unwrap_err();
        assert_eq!(same_error.code(), "GF_VALIDATION");
        assert!(same_error.to_string().contains("must be disjoint"));

        let child = source.path().join("projection-child");
        fs::create_dir(&child).unwrap();
        fs::write(child.join("sentinel"), b"child").unwrap();
        let child_error = materialize_graph_projection(source.path(), &child, &empty).unwrap_err();
        assert_eq!(child_error.code(), "GF_VALIDATION");
        assert_eq!(fs::read(child.join("sentinel")).unwrap(), b"child");

        let ancestor = TempDir::new().unwrap();
        let nested_source = ancestor.path().join("source");
        fs::create_dir(&nested_source).unwrap();
        let ancestor_error =
            materialize_graph_projection(&nested_source, ancestor.path(), &empty).unwrap_err();
        assert_eq!(ancestor_error.code(), "GF_VALIDATION");
        assert!(nested_source.exists());

        let regular_root = TempDir::new().unwrap();
        let regular_target = regular_root.path().join("target");
        fs::write(&regular_target, b"regular").unwrap();
        assert_eq!(
            materialize_graph_projection(source.path(), &regular_target, &empty)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        assert_eq!(fs::read(&regular_target).unwrap(), b"regular");

        let unexpected = TempDir::new().unwrap();
        fs::write(unexpected.path().join("knowledge.parquet"), b"owned").unwrap();
        assert_eq!(
            materialize_graph_projection(source.path(), unexpected.path(), &empty)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        assert_eq!(
            fs::read(unexpected.path().join("knowledge.parquet")).unwrap(),
            b"owned"
        );

        let bad_topology = TempDir::new().unwrap();
        fs::create_dir(bad_topology.path().join("topology")).unwrap();
        fs::write(bad_topology.path().join("topology/unknown"), b"keep").unwrap();
        assert_eq!(
            materialize_graph_projection(source.path(), bad_topology.path(), &empty)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        assert_eq!(
            fs::read(bad_topology.path().join("topology/unknown")).unwrap(),
            b"keep"
        );

        let bad_properties = TempDir::new().unwrap();
        fs::create_dir(bad_properties.path().join("properties")).unwrap();
        fs::write(
            bad_properties.path().join("properties/not-parquet"),
            b"keep",
        )
        .unwrap();
        assert_eq!(
            materialize_graph_projection(source.path(), bad_properties.path(), &empty)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        assert_eq!(
            fs::read(bad_properties.path().join("properties/not-parquet")).unwrap(),
            b"keep"
        );

        let nonempty = TempDir::new().unwrap();
        let mut node_uuid = FixedSizeBinaryBuilder::new(16);
        node_uuid.append_value(uuid(90).as_bytes()).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("node_uuid", Arc::new(node_uuid.finish()) as ArrayRef),
            ("value", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        let nonempty_path = nonempty.path().join("properties/Person.parquet");
        write_parquet(&nonempty_path, &batch).unwrap();
        let before = fs::read(&nonempty_path).unwrap();
        assert_eq!(
            materialize_graph_projection(source.path(), nonempty.path(), &empty)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        assert_eq!(fs::read(&nonempty_path).unwrap(), before);
    }

    #[test]
    fn corrupt_property_uuid_is_rejected_instead_of_silently_dropped() {
        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();
        let mut uuids = FixedSizeBinaryBuilder::new(16);
        uuids.append_null();
        let batch = RecordBatch::try_from_iter([
            ("node_uuid", Arc::new(uuids.finish()) as ArrayRef),
            ("value", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        let source_path = source.path().join("properties/Person.parquet");
        write_parquet(&source_path, &batch).unwrap();

        let error = project_parquet_file(
            &source_path,
            &target.path().join("properties/Person.parquet"),
            "node_uuid",
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .unwrap_err();
        assert!(matches!(error, GfError::Validation(_)));
        assert!(error.to_string().contains("UUID column contains null"));
        assert!(!target.path().join("properties/Person.parquet").exists());
    }

    #[test]
    fn canonical_graph_encoding_normalizes_floats_timezones_and_nested_types() {
        assert_eq!(normalize_f32(f32::NAN), 0x7fc0_0000);
        assert_eq!(normalize_f32(-0.0), 0);
        assert_eq!(normalize_f64(f64::NAN), 0x7ff8_0000_0000_0000);
        assert_eq!(normalize_f64(-0.0), 0);
        assert_eq!(time_unit_tag(TimeUnit::Second), 0);
        assert_eq!(time_unit_tag(TimeUnit::Millisecond), 1);
        assert_eq!(time_unit_tag(TimeUnit::Microsecond), 2);
        assert_eq!(time_unit_tag(TimeUnit::Nanosecond), 3);
        for timezone in [
            None,
            Some("UTC"),
            Some("Etc/UTC"),
            Some("Z"),
            Some("+00:00"),
        ] {
            assert!(validate_timezone(timezone).is_ok());
        }
        assert_eq!(
            validate_timezone(Some("America/Denver"))
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );

        let supported = [
            DataType::Boolean,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(16),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::UInt64, false)), 2),
            DataType::Struct(vec![Field::new("name", DataType::Utf8, false)].into()),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ];
        for data_type in supported {
            let mut writer = CanonicalWriter::new();
            encode_type(&mut writer, &data_type).unwrap();
            assert!(!writer.finish().is_empty());
        }
        let mut writer = CanonicalWriter::new();
        assert_eq!(
            encode_type(&mut writer, &DataType::Date32)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );

        let micros: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![TS]));
        assert_eq!(
            timestamp_value(&micros, TimeUnit::Microsecond, 0).unwrap(),
            TS
        );
        let time: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![123_i64]));
        assert_eq!(time64_value(&time, TimeUnit::Microsecond, 0).unwrap(), 123);
        assert_eq!(
            time64_value(&time, TimeUnit::Second, 0).unwrap_err().code(),
            "GF_VALIDATION"
        );
    }

    #[test]
    fn canonical_value_encoding_traverses_every_supported_nested_arrow_shape() {
        use arrow::array::{
            FixedSizeListArray, Int32Array, LargeListArray, StringDictionaryBuilder, StructArray,
        };
        use arrow::datatypes::Int32Type;

        let large: ArrayRef = Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None, Some(2)]),
        ]));
        let fixed: ArrayRef = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            [Some(vec![Some(3), Some(4)])],
            2,
        ));
        let struct_fields: arrow::datatypes::Fields =
            vec![Field::new("value", DataType::Int32, false)].into();
        let structure: ArrayRef = Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(Int32Array::from(vec![5]))],
            None,
        ));
        let mut dictionary_builder = StringDictionaryBuilder::<Int32Type>::new();
        dictionary_builder.append("six").unwrap();
        let dictionary: ArrayRef = Arc::new(dictionary_builder.finish());

        for (data_type, array) in [
            (large.data_type().clone(), large),
            (fixed.data_type().clone(), fixed),
            (DataType::Struct(struct_fields), structure),
            (dictionary.data_type().clone(), dictionary),
        ] {
            let mut writer = CanonicalWriter::new();
            encode_present_value(&mut writer, &data_type, &array, 0).unwrap();
            assert!(!writer.finish().is_empty());
        }
    }

    #[test]
    fn canonical_value_encoding_rejects_type_mismatch_and_nonnullable_null() {
        let floats: ArrayRef = Arc::new(Float32Array::from(vec![Some(-0.0), Some(f32::NAN)]));
        let doubles: ArrayRef = Arc::new(Float64Array::from(vec![Some(-0.0), Some(f64::NAN)]));
        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("value"), None]));
        let mut writer = CanonicalWriter::new();
        encode_present_value(&mut writer, &DataType::Float32, &floats, 0).unwrap();
        encode_present_value(&mut writer, &DataType::Float32, &floats, 1).unwrap();
        encode_present_value(&mut writer, &DataType::Float64, &doubles, 0).unwrap();
        encode_present_value(&mut writer, &DataType::Float64, &doubles, 1).unwrap();
        encode_value(&mut writer, &DataType::Utf8, &strings, 0, false).unwrap();
        encode_value(&mut writer, &DataType::Utf8, &strings, 1, true).unwrap();
        assert!(!writer.finish().is_empty());

        let mut writer = CanonicalWriter::new();
        assert_eq!(
            encode_value(&mut writer, &DataType::Utf8, &strings, 1, false)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        let mut writer = CanonicalWriter::new();
        assert_eq!(
            encode_present_value(&mut writer, &DataType::UInt64, &strings, 0)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
    }

    #[test]
    fn canonical_value_encoding_covers_every_scalar_and_time_representation() {
        let values: Vec<(DataType, ArrayRef)> = vec![
            (DataType::Boolean, Arc::new(BooleanArray::from(vec![true]))),
            (DataType::Int32, Arc::new(Int32Array::from(vec![-7]))),
            (DataType::Int64, Arc::new(Int64Array::from(vec![-9]))),
            (DataType::UInt32, Arc::new(UInt32Array::from(vec![7]))),
            (DataType::UInt64, Arc::new(UInt64Array::from(vec![9]))),
            (DataType::Utf8, Arc::new(StringArray::from(vec!["small"]))),
            (
                DataType::LargeUtf8,
                Arc::new(LargeStringArray::from(vec!["large"])),
            ),
            (
                DataType::Binary,
                Arc::new(BinaryArray::from_vec(vec![b"small".as_slice()])),
            ),
            (
                DataType::LargeBinary,
                Arc::new(LargeBinaryArray::from_vec(vec![b"large".as_slice()])),
            ),
            (
                DataType::Timestamp(TimeUnit::Second, None),
                Arc::new(TimestampSecondArray::from(vec![1_i64])),
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some("Z".into())),
                Arc::new(TimestampMillisecondArray::from(vec![2_i64]).with_timezone("Z")),
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                Arc::new(TimestampMicrosecondArray::from(vec![3_i64]).with_timezone("UTC")),
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("Etc/UTC".into())),
                Arc::new(TimestampNanosecondArray::from(vec![4_i64]).with_timezone("Etc/UTC")),
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                Arc::new(Time64MicrosecondArray::from(vec![5_i64])),
            ),
            (
                DataType::Time64(TimeUnit::Nanosecond),
                Arc::new(Time64NanosecondArray::from(vec![6_i64])),
            ),
        ];
        let mut encodings = Vec::new();
        for (data_type, array) in values {
            let mut writer = CanonicalWriter::new();
            encode_present_value(&mut writer, &data_type, &array, 0).unwrap();
            let encoded = writer.finish();
            assert!(!encoded.is_empty(), "{data_type} must emit canonical bytes");
            encodings.push(encoded);
        }
        assert_eq!(encodings.len(), 15);

        let seconds: ArrayRef = Arc::new(TimestampSecondArray::from(vec![11_i64]));
        let millis: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![12_i64]));
        let nanos: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![13_i64]));
        assert_eq!(timestamp_value(&seconds, TimeUnit::Second, 0).unwrap(), 11);
        assert_eq!(
            timestamp_value(&millis, TimeUnit::Millisecond, 0).unwrap(),
            12
        );
        assert_eq!(
            timestamp_value(&nanos, TimeUnit::Nanosecond, 0).unwrap(),
            13
        );
        let time_nanos: ArrayRef = Arc::new(Time64NanosecondArray::from(vec![14_i64]));
        assert_eq!(
            time64_value(&time_nanos, TimeUnit::Nanosecond, 0).unwrap(),
            14
        );
    }

    #[test]
    fn projection_identity_and_path_validation_matrix_fails_before_mutation() {
        let one = [1_u8; 16];
        let two = [2_u8; 16];
        assert!(require_present(&BTreeSet::new(), &BTreeSet::new(), "node").is_ok());
        assert!(
            require_present(&BTreeSet::from([one]), &BTreeSet::from([one, two]), "node").is_ok()
        );
        assert!(
            require_present(&BTreeSet::from([two]), &BTreeSet::from([one]), "edge")
                .unwrap_err()
                .to_string()
                .contains("missing edge UUID")
        );

        let root = TempDir::new().unwrap();
        let source = root.path().join("source");
        let sibling = root.path().join("sibling");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(&sibling).unwrap();
        assert!(validate_distinct_paths(&source, &sibling).is_ok());
        assert!(validate_distinct_paths(&source, &source).is_err());
        assert!(validate_distinct_paths(&source, &source.join("child")).is_err());
        assert!(validate_distinct_paths(&source.join("child"), &source).is_err());

        assert!(
            uuid_rows(&source.join("missing.parquet"), "node_uuid")
                .unwrap()
                .is_empty()
        );
        let wrong = RecordBatch::try_from_iter([(
            "node_uuid",
            Arc::new(UInt64Array::from(vec![1_u64])) as ArrayRef,
        )])
        .unwrap();
        assert!(uuid_column(&wrong, "node_uuid").is_err());
        assert!(uuid_column(&wrong, "missing").is_err());

        let mut nullable = FixedSizeBinaryBuilder::new(16);
        nullable.append_null();
        let nullable = nullable.finish();
        assert!(uuid_at(&nullable, 0).is_err());
    }

    #[test]
    fn wave10_projection_private_bounds_and_missing_inventory_are_exact() {
        assert!(
            sorted_parquet_files(Path::new("definitely-absent"))
                .unwrap()
                .is_empty()
        );
        assert!(exact_u32(usize::MAX, "field count").is_err());
        assert!(validate_timezone(Some("America/Denver")).is_err());

        let values: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1]));
        assert!(time64_value(&values, TimeUnit::Second, 0).is_err());
        assert!(downcast::<arrow::array::StringArray>(&values).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn wave10_projection_inventory_rejects_symbolic_links() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target.parquet");
        fs::write(&target, b"caller").unwrap();
        symlink(&target, root.path().join("linked.parquet")).unwrap();
        assert!(sorted_parquet_files(root.path()).is_err());
        assert_eq!(fs::read(target).unwrap(), b"caller");
    }

    #[test]
    fn wave13_projection_rejects_duplicate_graph_identities() {
        let root = TempDir::new().unwrap();
        let duplicate = uuid(41);
        let mut node_uuids = FixedSizeBinaryBuilder::new(16);
        node_uuids.append_value(duplicate.as_bytes()).unwrap();
        node_uuids.append_value(duplicate.as_bytes()).unwrap();
        let nodes =
            RecordBatch::try_from_iter([("node_uuid", Arc::new(node_uuids.finish()) as ArrayRef)])
                .unwrap();
        let nodes_path = root.path().join("nodes.parquet");
        write_parquet(&nodes_path, &nodes).unwrap();
        assert!(uuid_rows(&nodes_path, "node_uuid").is_err());

        let mut edge_uuids = FixedSizeBinaryBuilder::new(16);
        let mut sources = FixedSizeBinaryBuilder::new(16);
        let mut targets = FixedSizeBinaryBuilder::new(16);
        for _ in 0..2 {
            edge_uuids.append_value(duplicate.as_bytes()).unwrap();
            sources.append_value(uuid(42).as_bytes()).unwrap();
            targets.append_value(uuid(43).as_bytes()).unwrap();
        }
        let edges = RecordBatch::try_from_iter([
            ("edge_uuid", Arc::new(edge_uuids.finish()) as ArrayRef),
            ("src_uuid", Arc::new(sources.finish()) as ArrayRef),
            ("dst_uuid", Arc::new(targets.finish()) as ArrayRef),
        ])
        .unwrap();
        let edges_path = root.path().join("edges.parquet");
        write_parquet(&edges_path, &edges).unwrap();
        assert!(edge_endpoints(&[edges_path]).is_err());
    }

    #[test]
    fn wave13_projection_path_shape_and_cleanup_guards_are_structured() {
        let root = TempDir::new().unwrap();
        let missing = root.path().join("missing.parquet");
        assert!(
            project_parquet_file(
                &missing,
                &root.path().join("unused.parquet"),
                "node_uuid",
                &BTreeSet::new(),
                &BTreeSet::new(),
            )
            .is_ok()
        );
        assert!(copy_regular_file_if_present(&missing, &root.path().join("copy")).is_ok());
        assert!(clear_graph_empty_target(&root.path().join("absent-target")).is_ok());

        let metadata_directory = root.path().join("metadata-directory");
        fs::create_dir(&metadata_directory).unwrap();
        assert!(
            copy_regular_file_if_present(&metadata_directory, &root.path().join("metadata-copy"))
                .is_err()
        );

        let source_file = root.path().join("manifest-source");
        let target_file = root.path().join("nested/manifest-copy");
        fs::write(&source_file, b"manifest").unwrap();
        copy_regular_file_if_present(&source_file, &target_file).unwrap();
        assert_eq!(fs::read(&target_file).unwrap(), b"manifest");

        let target = root.path().join("clear-target");
        for directory in ["topology", "properties", "edge_properties"] {
            fs::create_dir_all(target.join(directory)).unwrap();
        }
        for name in [
            graphforge_core::manifest::MANIFEST_FILE,
            graphforge_core::manifest::ONTOLOGY_FILE,
        ] {
            fs::write(target.join(name), b"metadata").unwrap();
        }
        clear_graph_empty_target(&target).unwrap();
        assert!(fs::read_dir(&target).unwrap().next().is_none());
    }

    #[test]
    fn wave13_projection_target_metadata_must_be_regular_files() {
        let target = TempDir::new().unwrap();
        fs::create_dir(target.path().join(graphforge_core::manifest::MANIFEST_FILE)).unwrap();
        assert!(validate_graph_empty_target(target.path()).is_err());

        let topology = TempDir::new().unwrap();
        fs::create_dir(topology.path().join("generation.json")).unwrap();
        assert!(validate_empty_topology(topology.path()).is_err());

        let graph_directory = TempDir::new().unwrap();
        fs::create_dir(graph_directory.path().join("nested.parquet")).unwrap();
        assert!(validate_empty_parquet_directory(graph_directory.path()).is_err());

        assert_ne!(normalize_f32(1.25), 0);
        assert_ne!(normalize_f64(1.25), 0);
        assert_eq!(
            dictionary_value_type(&DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8),
            )),
            &DataType::Utf8
        );
    }

    fn portable_typed_graph(label: &str, catalog_prefix: Option<&str>) -> TempDir {
        let root = TempDir::new().unwrap();
        let mut catalog = RuntimeCatalog::new();
        if let Some(prefix) = catalog_prefix {
            catalog.intern_label(prefix);
        }
        let runtime_id = catalog.intern_label(label);
        let storage_id = graphforge_ir::runtime_entity_type_id(runtime_id);
        let mut writer = GraphWriter::open_at(root.path(), OntologyMode::Exploratory, TS).unwrap();
        writer.create_node(uuid(91), storage_id).unwrap();
        writer.flush().unwrap();
        write_parquet(
            &root.path().join("topology/runtime_catalog.parquet"),
            &catalog.to_record_batch(),
        )
        .unwrap();
        root
    }

    #[test]
    fn portable_type_fingerprint_is_name_stable_parallel_and_semantic() {
        let first = portable_typed_graph("Person", None);
        let shifted = portable_typed_graph("Person", Some("EarlierInsertion"));
        let changed = portable_typed_graph("Company", None);
        let expected = portable_graph_data_fingerprint(first.path()).unwrap();
        assert_eq!(
            expected,
            portable_graph_data_fingerprint(shifted.path()).unwrap(),
            "runtime catalog allocation order must not leak into portable identity"
        );
        assert_ne!(
            expected,
            portable_graph_data_fingerprint(changed.path()).unwrap(),
            "changing only the semantic type assignment must change identity"
        );
        std::thread::scope(|scope| {
            let handles = (0..8)
                .map(|_| scope.spawn(|| portable_graph_data_fingerprint(first.path()).unwrap()))
                .collect::<Vec<_>>();
            for handle in handles {
                assert_eq!(handle.join().unwrap(), expected);
            }
        });
    }

    #[test]
    fn portable_fingerprint_is_stable_across_immutable_overlay_projection() {
        let source = TempDir::new().unwrap();
        let node = uuid(93);
        let mut writer = GraphWriter::open_at(source.path(), OntologyMode::Strict, TS).unwrap();
        writer
            .create_node(node, graphforge_core::TypeId(1))
            .unwrap();
        writer
            .set_properties(
                &node,
                Some("Person"),
                HashMap::from([("name".into(), graphforge_ir::IrLiteral::Str("Ada".into()))]),
            )
            .unwrap();
        writer.flush().unwrap();
        let parent = TempDir::new().unwrap();
        let projected = parent.path().join("projected");
        materialize_portable_graph_tree_projection(
            source.path(),
            &projected,
            &GraphProjectionSelection {
                node_uuids: BTreeSet::from([*node.as_bytes()]),
                ..GraphProjectionSelection::default()
            },
        )
        .unwrap();
        assert_eq!(
            portable_graph_data_fingerprint(source.path()).unwrap(),
            portable_graph_data_fingerprint(&projected).unwrap(),
        );
    }

    #[test]
    fn portable_type_fingerprint_fails_closed_for_unresolved_runtime_id() {
        let root = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(root.path(), OntologyMode::Exploratory, TS).unwrap();
        writer
            .create_node(
                uuid(92),
                graphforge_ir::runtime_entity_type_id(graphforge_ir::RuntimeTypeId(41)),
            )
            .unwrap();
        writer.flush().unwrap();
        let error = portable_graph_data_fingerprint(root.path()).unwrap_err();
        assert!(error.to_string().contains("no catalog name"));
    }

    fn id_map(batch: &RecordBatch, uuid_name: &str, id_name: &str) -> BTreeMap<[u8; 16], u64> {
        let uuids = uuid_column(batch, uuid_name).unwrap();
        let ids = batch
            .column_by_name(id_name)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| (uuid_at(uuids, row).unwrap(), ids.value(row)))
            .collect()
    }
}
