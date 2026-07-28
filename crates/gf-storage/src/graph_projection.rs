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
    LargeStringArray, ListArray, StringArray, StructArray, UInt32Array, UInt64Array,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use gf_core::GfError;
use gf_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use parquet::arrow::ArrowWriter;

type GraphUuid = [u8; 16];
type EdgeEndpoints = BTreeMap<GraphUuid, (GraphUuid, GraphUuid)>;

/// Explicit graph identities requested for one projection.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphProjectionSelection {
    /// Explicit node UUIDs. Selected edges add their endpoints automatically.
    pub node_uuids: BTreeSet<[u8; 16]>,
    /// Explicit edge UUIDs. No other edge is induced from selected nodes.
    pub edge_uuids: BTreeSet<[u8; 16]>,
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
/// preserved. Every selected edge adds both endpoint nodes; node selection
/// never induces an edge. Topology and property rows are rewritten in UUID
/// order. Only core graph files and required ontology/runtime-catalog metadata
/// are copied; derived indexes and every non-graph domain are excluded.
///
/// # Errors
/// Returns validation for missing identities, unsafe/non-empty targets, or
/// malformed graph files, and storage errors for I/O/Arrow/Parquet failures.
pub fn materialize_graph_projection(
    source: &Path,
    target: &Path,
    selection: &GraphProjectionSelection,
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
    )?;
    project_parquet_directory(
        &source.join("topology/edges"),
        &target.join("topology/edges"),
        "edge_uuid",
        &selection.edge_uuids,
    )?;
    project_parquet_directory(
        &source.join("properties"),
        &target.join("properties"),
        "node_uuid",
        &selected_nodes,
    )?;
    project_parquet_directory(
        &source.join("edge_properties"),
        &target.join("edge_properties"),
        "edge_uuid",
        &selection.edge_uuids,
    )?;
    copy_runtime_catalog(source, target)?;
    for file in [
        gf_core::manifest::MANIFEST_FILE,
        gf_core::manifest::ONTOLOGY_FILE,
    ] {
        copy_regular_file_if_present(&source.join(file), &target.join(file))?;
    }
    let graph_content_fingerprint = projected_graph_fingerprint(target)?;

    Ok(GraphProjectionSummary {
        node_uuids: selected_nodes.into_iter().collect(),
        edge_uuids: selection.edge_uuids.iter().copied().collect(),
        endpoint_node_uuids,
        graph_content_fingerprint,
    })
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
) -> Result<(), GfError> {
    for path in sorted_parquet_files(source)? {
        let name = path
            .file_name()
            .ok_or_else(|| validation("graph parquet path has no file name"))?;
        project_parquet_file(&path, &target.join(name), key, selected)?;
    }
    Ok(())
}

fn project_parquet_file(
    source: &Path,
    target: &Path,
    key: &str,
    selected: &BTreeSet<[u8; 16]>,
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
    let keys = uuid_column(&combined, key)?;
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
    let columns = combined
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(storage))
        .collect::<Result<Vec<_>, _>>()?;
    let projected = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(storage)?;
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
    let canonical = gf_ir::RuntimeCatalog::from_record_batch(&batch)?.to_record_batch();
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
        if stem != "_exploratory" {
            relation_names.insert(stem);
        }
        for batch in read_parquet(&path)? {
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
        if kinds.value(row) == "entity_type" && type_ids.contains(&ids.value(row)) {
            active_owners.insert(names.value(row).to_owned());
        }
    }
    active_owners.extend(relation_names.iter().cloned());

    let mut selected = BTreeSet::new();
    for row in 0..catalog.num_rows() {
        let keep = match kinds.value(row) {
            "entity_type" => type_ids.contains(&ids.value(row)),
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

fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<(), GfError> {
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

fn projected_graph_fingerprint(root: &Path) -> Result<[u8; 32], GfError> {
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
        let logical = logical_fingerprint_batch(relative, &batch)?;
        encode_table(&mut writer, &logical)?;
    }
    fingerprint(
        CanonicalDomain::GraphProjection,
        CANONICAL_CONTRACT_VERSION,
        &writer.finish(),
    )
    .map_err(canonical_error)
}

fn logical_fingerprint_batch(relative: &str, batch: &RecordBatch) -> Result<RecordBatch, GfError> {
    let source_schema = batch.schema();
    let names: Vec<&str> = if relative == "topology/nodes.parquet" {
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
    let mut fields = Vec::with_capacity(names.len());
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
        let index = source_schema
            .index_of(name)
            .map_err(|_| validation(format!("graph fingerprint field {name} is absent")))?;
        fields.push(Arc::clone(&source_schema.fields()[index]));
        columns.push(Arc::clone(batch.column(index)));
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(storage)
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
                        if value == gf_core::manifest::MANIFEST_FILE
                            || value == gf_core::manifest::ONTOLOGY_FILE =>
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
        gf_core::manifest::MANIFEST_FILE,
        gf_core::manifest::ONTOLOGY_FILE,
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

    use arrow::array::{FixedSizeBinaryBuilder, Int64Array, ListArray, UInt32Array, UInt64Array};
    use gf_core::uuid::Uuid;
    use gf_core::{OntologyMode, TypeId};
    use gf_ir::{IrLiteral, RuntimeCatalog};
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
            source.path().join(gf_core::manifest::MANIFEST_FILE),
            b"ontology: ontology.yaml\n",
        )
        .unwrap();
        fs::write(
            source.path().join(gf_core::manifest::ONTOLOGY_FILE),
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
                .join(gf_core::manifest::MANIFEST_FILE)
                .exists()
        );
        assert!(
            target
                .path()
                .join(gf_core::manifest::ONTOLOGY_FILE)
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
        };
        let baseline =
            materialize_graph_projection(source.path(), baseline_target.path(), &selection)
                .unwrap();

        for relative in [
            "topology/nodes.parquet",
            "topology/edges/_exploratory.parquet",
            "properties/_untyped.parquet",
            "edge_properties/KNOWS.parquet",
            "topology/runtime_catalog.parquet",
        ] {
            let path = source.path().join(relative);
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
            },
        )
        .unwrap();
        assert_eq!(summary.node_uuids, vec![*nodes[0].as_bytes()]);
        assert_eq!(read_nodes(target.path()).unwrap()[0].num_rows(), 1);
        assert!(!target.path().join("topology/generation.json").exists());
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
        )
        .unwrap_err();
        assert!(matches!(error, GfError::Validation(_)));
        assert!(error.to_string().contains("UUID column contains null"));
        assert!(!target.path().join("properties/Person.parquet").exists());
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
