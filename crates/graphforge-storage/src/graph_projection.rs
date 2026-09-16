//! Deterministic graph-only workspace projections.
//!
//! This module deliberately understands only graph-owned workspace files. It
//! never enumerates or copies project-generation participants, provenance,
//! knowledge, epistemic, valid-time, search, or derived-index directories.

use graphforge_value::{EntityTypeId, PrimaryEntityTypeId, RuntimeEntityId};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, ListArray, StringArray, UInt32Array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use parquet::arrow::ArrowWriter;

mod logical_fingerprint;
mod runtime_remap;
pub(crate) use logical_fingerprint::{
    portable_graph_data_fingerprint, projected_graph_fingerprint,
};

/// Route authority captured once for a private graph transform. Never migrates the source.
pub(crate) struct TransformRoutes {
    table: Option<crate::route_component::RouteTable>,
    pub(crate) properties: crate::AuthenticatedPropertyInventory,
}

impl TransformRoutes {
    pub(crate) fn capture(root: &Path) -> Result<Self, GfError> {
        let (inventory, _) = crate::capture_graph_files(root)?;
        Self::from_inventory(root, inventory)
    }

    pub(crate) fn from_inventory(
        root: &Path,
        inventory: crate::GraphFilesInventory,
    ) -> Result<Self, GfError> {
        let table = match inventory.format_version {
            crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION => Some(
                crate::graph_files::authenticate_route_table(root, &inventory)?,
            ),
            crate::graph_files::GRAPH_FILES_RECORD_VERSION => None,
            _ => {
                return Err(validation(
                    "graph transform requires an expanded graph inventory",
                ));
            }
        };
        let properties =
            crate::AuthenticatedPropertyInventory::from_inventory_at_root(root, inventory, None)?;
        Ok(Self { table, properties })
    }

    pub(crate) fn semantic_path(&self, relative: &str) -> Result<String, GfError> {
        match &self.table {
            Some(table) => table.semantic_relative_path(relative),
            None => crate::graph_files::legacy_inventory_logical_text(relative),
        }
    }

    pub(crate) fn property_batches(
        &self,
        root: &Path,
        route: &str,
        edge: bool,
    ) -> Result<Vec<RecordBatch>, GfError> {
        let mut batches = Vec::new();
        crate::catalog::visit_property_overlay_batched_with_inventory(
            root,
            Some(&self.properties),
            route,
            edge,
            8192,
            |batch| {
                batches.push(batch.clone());
                Ok(true)
            },
        )
        .map_err(storage)?;
        Ok(batches)
    }
}

pub(crate) fn encode_transform_path(
    relative: &str,
    table: &mut crate::route_component::RouteTable,
) -> Result<String, GfError> {
    crate::route_component::encode_relative_route(relative, table, 64 * 1024 * 1024, 100_000)
}

/// The caller owns this unpublished graph and has finished emitting every route.
pub(crate) fn install_transform_table(
    root: &Path,
    table: &crate::route_component::RouteTable,
) -> Result<(), GfError> {
    let bytes = table.encode(64 * 1024 * 1024)?;
    let mut file = File::options()
        .write(true)
        .create_new(true)
        .open(root.join(crate::route_component::TABLE_FILE))
        .map_err(storage)?;
    file.write_all(&bytes).map_err(storage)?;
    file.sync_all().map_err(storage)
}

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

    let routes = TransformRoutes::capture(source)?;
    let mut output_table = crate::route_component::RouteTable::default();
    let node_paths = crate::mutator::node_parquet_files(source).map_err(storage)?;
    let node_ids = uuid_rows_files(&node_paths, "node_uuid")?;
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
    for path in node_paths {
        let relative = path.strip_prefix(source).map_err(storage)?;
        project_parquet_file(
            &path,
            &target.join(relative),
            "node_uuid",
            &selected_nodes,
            &selection.exclude_properties,
        )?;
    }
    for path in edge_files {
        let relative = path
            .strip_prefix(source)
            .map_err(storage)?
            .to_str()
            .ok_or_else(|| validation("graph path is not UTF-8"))?
            .replace('\\', "/");
        let semantic = routes.semantic_path(&relative)?;
        let destination = encode_transform_path(&semantic, &mut output_table)?;
        project_parquet_file(
            &path,
            &target.join(destination),
            "edge_uuid",
            &selected_edges,
            &BTreeSet::new(),
        )?;
    }
    project_property_directory(
        source,
        target,
        &routes,
        &mut output_table,
        false,
        "node_uuid",
        &selected_nodes,
        &selection.exclude_properties,
    )?;
    project_property_directory(
        source,
        target,
        &routes,
        &mut output_table,
        true,
        "edge_uuid",
        &selected_edges,
        &selection.exclude_properties,
    )?;
    install_transform_table(target, &output_table)?;
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

fn uuid_rows_files(paths: &[PathBuf], column: &str) -> Result<BTreeSet<GraphUuid>, GfError> {
    let mut rows = BTreeSet::new();
    for path in paths {
        for value in uuid_rows(path, column)? {
            if !rows.insert(value) {
                return Err(validation(format!("graph contains a duplicate {column}")));
            }
        }
    }
    Ok(rows)
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

#[allow(clippy::too_many_arguments)] // explicit input authority and output table belong to one transform
fn project_property_directory(
    source: &Path,
    target: &Path,
    authority: &TransformRoutes,
    table: &mut crate::route_component::RouteTable,
    edge: bool,
    key: &str,
    selected: &BTreeSet<[u8; 16]>,
    exclude_properties: &BTreeSet<String>,
) -> Result<(), GfError> {
    let kind = if edge {
        crate::PropertyRouteKind::Edge
    } else {
        crate::PropertyRouteKind::Node
    };
    let routes = authority.properties.routes(kind);
    let directory = if edge {
        "edge_properties"
    } else {
        "properties"
    };
    for route in routes {
        let batches = authority.property_batches(source, route, edge)?;
        let Some(schema) = batches.first().map(RecordBatch::schema) else {
            continue;
        };
        let combined = concat_batches(&schema, &batches).map_err(storage)?;
        project_record_batch(
            &combined,
            &target.join(encode_transform_path(
                &format!("{directory}/{route}.parquet"),
                table,
            )?),
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
    let canonical = runtime_remap::compact(&canonical, target)?;
    write_parquet(&target.join("topology/runtime_catalog.parquet"), &canonical)
}

#[allow(
    clippy::too_many_lines,
    reason = "catalog dependency closure remains one auditable selection pass"
)]
fn selected_catalog_rows(target: &Path, catalog: &RecordBatch) -> Result<Vec<usize>, GfError> {
    let authority = TransformRoutes::capture(target)?;
    let mut type_ids = HashSet::new();
    for nodes in crate::mutator::node_parquet_files(target).map_err(storage)? {
        for batch in read_parquet(&nodes)? {
            if let Some(column) = batch.column_by_name("type_id") {
                let values = column
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| validation("node type_id is not UInt32"))?;
                for row in 0..values.len() {
                    if values.is_null(row) {
                        return Err(validation("node type_id contains null"));
                    }
                    if let Some(id) = PrimaryEntityTypeId::decode(values.value(row))
                        .map_err(|e| validation(e.to_string()))?
                        .label()
                    {
                        type_ids.insert(id);
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
                        return Err(validation("node type_ids contains null list"));
                    }
                    let values = lists.value(row);
                    let values = values
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .ok_or_else(|| validation("node type_ids values are not UInt32"))?;
                    for index in 0..values.len() {
                        if values.is_null(index) {
                            return Err(validation("node type_ids contains null"));
                        }
                        type_ids.insert(
                            EntityTypeId::decode(values.value(index))
                                .map_err(|e| validation(e.to_string()))?,
                        );
                    }
                }
            }
        }
    }

    let mut relation_names = BTreeSet::new();
    let edge_root = target.join("topology/edges");
    for path in sorted_parquet_files(&edge_root)? {
        let logical = authority.semantic_path(
            &path
                .strip_prefix(target)
                .map_err(storage)?
                .to_str()
                .ok_or_else(|| validation("edge path is not UTF-8"))?
                .replace('\\', "/"),
        )?;
        let parts = logical.split('/').collect::<Vec<_>>();
        let stem = match parts.as_slice() {
            ["topology", "edges", file] => file
                .strip_suffix(".parquet")
                .ok_or_else(|| validation("edge route suffix is invalid"))?,
            ["topology", "edges", route, _] => route,
            _ => return Err(validation("edge route shape is invalid")),
        };
        let batches = read_parquet(&path)?;
        if stem != "_exploratory" && batches.iter().any(|batch| batch.num_rows() != 0) {
            relation_names.insert(stem.to_owned());
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
            && type_ids.contains(&EntityTypeId::runtime(
                RuntimeEntityId::new(ids.value(row)).map_err(|e| validation(e.to_string()))?,
            ))
        {
            active_owners.insert(names.value(row).to_owned());
        }
    }
    active_owners.extend(relation_names.iter().cloned());

    let mut selected = BTreeSet::new();
    for row in 0..catalog.num_rows() {
        let keep = match kinds.value(row) {
            "entity_type" => type_ids.contains(&EntityTypeId::runtime(
                RuntimeEntityId::new(ids.value(row)).map_err(|e| validation(e.to_string()))?,
            )),
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
    let mut writer = ArrowWriter::try_new(
        file,
        batch.schema(),
        Some(crate::permanent_parquet::writer_properties().build()),
    )
    .map_err(storage)?;
    writer.write(batch).map_err(storage)?;
    writer.close().map_err(storage)?;
    Ok(())
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
        if file_type.is_dir() {
            paths.extend(sorted_parquet_files(&path)?);
        } else if file_type.is_file()
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
            "edges" | "nodes" => validate_empty_parquet_directory(&entry.path())?,
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
    fn admitted_test_path(root: &std::path::Path, semantic: &str) -> std::path::PathBuf {
        let (inventory, _) = crate::capture_graph_files(root).unwrap();
        let authority = super::TransformRoutes::from_inventory(root, inventory.clone()).unwrap();
        let entries = inventory
            .files
            .iter()
            .filter(|entry| authority.semantic_path(&entry.relative_path).unwrap() == semantic)
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), 1, "one exact fixture route {semantic}");
        crate::graph_files::resolve_v1_inventory_entry(root, entries[0]).unwrap()
    }

    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, FixedSizeBinaryBuilder, Float64Array, Int64Array, ListArray, UInt32Array,
        UInt64Array,
    };
    use graphforge_core::uuid::Uuid;
    use graphforge_core::{OntologyMode, TypeId};
    use graphforge_ir::{IrLiteral, RuntimeCatalog};
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::{GraphWriter, read_edge_properties, read_nodes, read_properties};

    pub(super) const TS: i64 = 1_700_000_000_000_000;

    fn uuid(marker: u8) -> Uuid {
        let mut bytes = [0_u8; 16];
        bytes[15] = marker;
        Uuid::from_bytes(bytes)
    }

    #[test]
    fn mapped_projection_keeps_exact_routes_and_source_inventory() {
        let source = TempDir::new().unwrap();
        let target = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(source.path(), OntologyMode::Strict, TS).unwrap();
        let a = uuid(120);
        let b = uuid(121);
        let edge = uuid(122);
        for node in [a, b] {
            writer
                .create_node(node, EntityTypeId::ontology(TypeId(1)).unwrap())
                .unwrap();
        }
        writer.create_edge(edge, "CON\\route", &a, &b).unwrap();
        writer
            .set_properties(
                &a,
                Some("AUX\\label"),
                HashMap::from([("name".into(), IrLiteral::Str("Ada".into()))]),
            )
            .unwrap();
        writer
            .set_edge_properties(
                &edge,
                Some("CON\\route"),
                HashMap::from([("cost".into(), IrLiteral::Float(2.5))]),
            )
            .unwrap();
        writer.flush().unwrap();
        drop(writer);
        let before = crate::capture_graph_files(source.path()).unwrap().0;
        let summary = materialize_portable_graph_tree_projection(
            source.path(),
            target.path(),
            &GraphProjectionSelection {
                node_uuids: BTreeSet::from([*a.as_bytes()]),
                edge_uuids: BTreeSet::from([*edge.as_bytes()]),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(summary.node_uuids, vec![*a.as_bytes(), *b.as_bytes()]);
        assert_eq!(summary.edge_uuids, vec![*edge.as_bytes()]);
        let output = crate::capture_graph_files(target.path()).unwrap().0;
        let table = crate::graph_files::authenticate_route_table(target.path(), &output).unwrap();
        table
            .validate_paths(
                output
                    .files
                    .iter()
                    .map(|entry| entry.relative_path.as_str()),
            )
            .unwrap();
        let authority = TransformRoutes::from_inventory(target.path(), output).unwrap();
        assert_eq!(
            authority
                .properties
                .routes(crate::PropertyRouteKind::Node)
                .collect::<Vec<_>>(),
            vec!["AUX\\label"]
        );
        assert_eq!(
            authority
                .properties
                .routes(crate::PropertyRouteKind::Edge)
                .collect::<Vec<_>>(),
            vec!["CON\\route"]
        );
        let rows = authority
            .property_batches(target.path(), "CON\\route", true)
            .unwrap();
        assert_eq!(
            rows[0]
                .column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.5
        );
        assert_eq!(crate::capture_graph_files(source.path()).unwrap().0, before);
        assert_eq!(
            portable_graph_data_fingerprint(source.path()).unwrap(),
            portable_graph_data_fingerprint(target.path()).unwrap()
        );
    }

    #[test]
    fn projected_semantic_id_does_not_select_equal_runtime_local_id() {
        let target = TempDir::new().unwrap();
        let mut catalog = RuntimeCatalog::new();
        let runtime = catalog.intern_label("UnrelatedRuntime").unwrap();
        assert_eq!(runtime.get(), 0);
        let mut writer = GraphWriter::open_at(target.path(), OntologyMode::Strict, TS).unwrap();
        writer
            .create_node(uuid(90), EntityTypeId::ontology(TypeId(0)).unwrap())
            .unwrap();
        writer.flush().unwrap();
        assert!(
            selected_catalog_rows(target.path(), &catalog.to_record_batch())
                .unwrap()
                .is_empty()
        );
    }

    fn fixture() -> (TempDir, [Uuid; 3], [Uuid; 2]) {
        let source = TempDir::new().unwrap();
        let nodes = [uuid(3), uuid(1), uuid(2)];
        let edges = [uuid(11), uuid(12)];
        let mut writer =
            GraphWriter::open_at(source.path(), OntologyMode::Exploratory, TS).unwrap();
        writer
            .create_node_with_labels(
                nodes[0],
                &[
                    EntityTypeId::ontology(TypeId(7)).unwrap(),
                    EntityTypeId::ontology(TypeId(9)).unwrap(),
                ],
            )
            .unwrap();
        writer
            .create_node(nodes[1], EntityTypeId::ontology(TypeId(8)).unwrap())
            .unwrap();
        writer
            .create_node(nodes[2], EntityTypeId::ontology(TypeId(10)).unwrap())
            .unwrap();
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
        catalog.intern_label("Person").unwrap();
        catalog.intern_relation_type("KNOWS").unwrap();
        catalog.intern_property("value", Some("Person")).unwrap();
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

        let projected_edges = read_parquet(&admitted_test_path(
            target.path(),
            "topology/edges/_exploratory.parquet",
        ))
        .unwrap();
        assert_eq!(projected_edges[0].num_rows(), 1);
        assert_eq!(
            uuid_at(uuid_column(&projected_edges[0], "edge_uuid").unwrap(), 0).unwrap(),
            *edges[0].as_bytes()
        );
        let source_edge_ids = id_map(
            &read_parquet(&admitted_test_path(
                source.path(),
                "topology/edges/_exploratory.parquet",
            ))
            .unwrap()[0],
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
    fn projection_exports_and_reopens_mixed_node_shards() {
        let (source, nodes, _) = fixture();
        let appended = uuid(4);
        let mut writer =
            GraphWriter::open_at(source.path(), OntologyMode::Exploratory, TS + 1).unwrap();
        writer
            .create_node(appended, EntityTypeId::ontology(TypeId(10)).unwrap())
            .unwrap();
        writer.flush().unwrap();
        let target = TempDir::new().unwrap();
        let mut selected = nodes
            .iter()
            .map(|uuid| *uuid.as_bytes())
            .collect::<BTreeSet<_>>();
        selected.insert(*appended.as_bytes());
        materialize_graph_projection(
            source.path(),
            target.path(),
            &GraphProjectionSelection {
                node_uuids: selected,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            read_nodes(target.path())
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );
        assert!(target.path().join("topology/nodes").is_dir());
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
                fs::read(admitted_test_path(first.path(), relative)).unwrap(),
                fs::read(admitted_test_path(second.path(), relative)).unwrap(),
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
            admitted_test_path(source.path(), "topology/edges/_exploratory.parquet"),
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
        catalog.intern_label("Unrelated").unwrap();
        catalog.intern_relation_type("IGNORES").unwrap();
        catalog.intern_property("noise", Some("Unrelated")).unwrap();
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
        let company = catalog.intern_label("Company").unwrap();
        catalog.intern_relation_type("IGNORES").unwrap();
        catalog.intern_property("noise", Some("Company")).unwrap();
        let person = catalog.intern_label("Person").unwrap();
        catalog.intern_relation_type("KNOWS").unwrap();
        catalog.intern_property("name", Some("Person")).unwrap();
        catalog.intern_property("global", None).unwrap();
        catalog.intern_property("since", Some("KNOWS")).unwrap();

        let mut writer = GraphWriter::open_at(source.path(), OntologyMode::Strict, TS).unwrap();
        writer
            .create_node(alice, EntityTypeId::runtime(person))
            .unwrap();
        writer
            .create_node(bob, EntityTypeId::runtime(person))
            .unwrap();
        writer
            .create_node(excluded, EntityTypeId::runtime(company))
            .unwrap();
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

        let reopened_catalog = RuntimeCatalog::from_record_batch(&projected).unwrap();
        let (projected_person, name) = reopened_catalog
            .entity_type_names_with_ids()
            .next()
            .unwrap();
        assert_eq!(name, "Person");
        assert_ne!(projected_person, person);
        let reopened_nodes = read_nodes(target.path()).unwrap();
        let mut actual_nodes = Vec::new();
        for batch in &reopened_nodes {
            let ids = batch
                .column_by_name("node_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let primary = batch
                .column_by_name("type_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(
                    primary.value(row),
                    EntityTypeId::runtime(projected_person).encode()
                );
                actual_nodes.push(ids.value(row).to_vec());
            }
        }
        actual_nodes.sort();
        assert_eq!(
            actual_nodes,
            vec![alice.as_bytes().to_vec(), bob.as_bytes().to_vec()]
        );
        let admitted = TransformRoutes::capture(target.path()).unwrap();
        let edge_paths = admitted.properties.edge_files(Some("KNOWS"));
        assert_eq!(edge_paths.len(), 1);
        let edge = read_parquet(&edge_paths[0].1).unwrap().remove(0);
        for (column, expected) in [("edge_uuid", knows), ("src_uuid", alice), ("dst_uuid", bob)] {
            let ids = edge
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(ids.len(), 1);
            assert_eq!(ids.value(0), expected.as_bytes());
        }
        let properties = crate::read_node_property_rows(target.path(), "Person").unwrap();
        assert_eq!(
            properties[alice.as_bytes()]["name"],
            IrLiteral::Str("Alice".into())
        );
        let edge_properties = read_edge_properties(target.path(), "KNOWS").unwrap();
        let since = edge_properties[0]
            .column_by_name("since")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(since.value(0), 2020);
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
            &target
                .path()
                .join("topology/nodes/00000000000000000000-00000000000000000000.parquet"),
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
    fn wave10_projection_missing_inventory_is_empty() {
        assert!(
            sorted_parquet_files(Path::new("definitely-absent"))
                .unwrap()
                .is_empty()
        );
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
    }

    fn portable_typed_graph(label: &str, catalog_prefix: Option<&str>) -> TempDir {
        let root = TempDir::new().unwrap();
        let mut catalog = RuntimeCatalog::new();
        if let Some(prefix) = catalog_prefix {
            catalog.intern_label(prefix).unwrap();
        }
        let runtime_id = catalog.intern_label(label).unwrap();
        let storage_id = graphforge_value::EntityTypeId::runtime(runtime_id);
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
            .create_node(
                node,
                EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
            )
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
    fn portable_fingerprint_unions_legacy_and_immutable_node_fragments() {
        fn graph(split: bool) -> TempDir {
            let root = TempDir::new().unwrap();
            let mut catalog = RuntimeCatalog::new();
            let storage_id =
                graphforge_value::EntityTypeId::runtime(catalog.intern_label("Person").unwrap());
            let mut writer =
                GraphWriter::open_at(root.path(), OntologyMode::Exploratory, TS).unwrap();
            writer.create_node(uuid(91), storage_id).unwrap();
            writer.create_node(uuid(92), storage_id).unwrap();
            writer.flush().unwrap();
            write_parquet(
                &root.path().join("topology/runtime_catalog.parquet"),
                &catalog.to_record_batch(),
            )
            .unwrap();
            if split {
                let legacy = root.path().join("topology/nodes.parquet");
                let batches = read_parquet(&legacy).unwrap();
                let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
                let legacy_schema = Arc::new(arrow::datatypes::Schema::new(
                    [0, 1, 2, 4, 5]
                        .into_iter()
                        .map(|index| batch.schema().field(index).clone())
                        .collect::<Vec<_>>(),
                ));
                let legacy_batch = RecordBatch::try_new(
                    legacy_schema,
                    [0, 1, 2, 4, 5]
                        .into_iter()
                        .map(|index| Arc::clone(batch.column(index)))
                        .collect(),
                )
                .unwrap();
                write_parquet(&legacy, &legacy_batch.slice(0, 1)).unwrap();
                let shards = root.path().join("topology/nodes");
                fs::create_dir_all(&shards).unwrap();
                write_parquet(
                    &shards.join("00000000000000000002-00000000000000000002.parquet"),
                    &batch.slice(1, 1),
                )
                .unwrap();
            }
            root
        }

        let legacy = graph(false);
        let sharded = graph(true);
        assert_eq!(
            portable_graph_data_fingerprint(legacy.path()).unwrap(),
            portable_graph_data_fingerprint(sharded.path()).unwrap()
        );
    }

    #[test]
    fn portable_type_fingerprint_fails_closed_for_unresolved_runtime_id() {
        let root = TempDir::new().unwrap();
        let mut writer = GraphWriter::open_at(root.path(), OntologyMode::Exploratory, TS).unwrap();
        writer
            .create_node(
                uuid(92),
                graphforge_value::EntityTypeId::runtime(
                    graphforge_value::RuntimeEntityId::new(41).unwrap(),
                ),
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
