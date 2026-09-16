//! Canonical logical fingerprints for projected and portable graph data.

use super::{TransformRoutes, read_parquet, sorted_parquet_files, storage, validation};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, ListBuilder, NullArray, StringArray, StringBuilder, StructArray,
    UInt32Array, UInt64Array,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use graphforge_value::RuntimeEntityId;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) fn projected_graph_fingerprint(root: &Path) -> Result<[u8; 32], GfError> {
    let mut paths = Vec::new();
    paths.extend(crate::mutator::node_parquet_files(root).map_err(storage)?);
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
    let authority = TransformRoutes::capture(root)?;
    let runtime_entity_names = portable_runtime_entity_names(root)?;
    let mut tables = Vec::<(String, RecordBatch)>::new();
    let node_paths = crate::mutator::node_parquet_files(root).map_err(storage)?;
    if !node_paths.is_empty() {
        let mut batches = Vec::new();
        for path in node_paths {
            batches.extend(
                crate::catalog::normalize_topology_nodes(read_parquet(&path)?).map_err(storage)?,
            );
        }
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| validation("graph projection node table has no schema"))?;
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
            authority.semantic_path(&relative)?,
            concat_batches(&schema, &batches).map_err(storage)?,
        ));
    }
    for (directory, is_edge) in [("properties", false), ("edge_properties", true)] {
        let kind = if is_edge {
            crate::PropertyRouteKind::Edge
        } else {
            crate::PropertyRouteKind::Node
        };
        for stem in authority.properties.routes(kind) {
            let batches = authority.property_batches(root, stem, is_edge)?;
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
    runtime_entity_names: Option<&HashMap<RuntimeEntityId, String>>,
) -> Result<[u8; 32], GfError> {
    let authority = TransformRoutes::capture(root)?;
    let mut logical_tables = BTreeMap::<String, Vec<PathBuf>>::new();
    for path in paths {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| validation("graph projection path escaped target"))?;
        let semantic = authority.semantic_path(
            &relative
                .to_str()
                .ok_or_else(|| validation("graph path is not UTF-8"))?
                .replace('\\', "/"),
        )?;
        let components = semantic.split('/').collect::<Vec<_>>();
        let logical = match components.as_slice() {
            ["topology", "nodes.parquet"] | ["topology", "nodes", _] => {
                "topology/nodes.parquet".to_owned()
            }
            ["topology", "edges", file] => format!("topology/edges/{file}"),
            ["topology", "edges", stem, _] => format!("topology/edges/{stem}.parquet"),
            [domain @ ("properties" | "edge_properties"), file] => {
                format!("{domain}/{file}")
            }
            [domain @ ("properties" | "edge_properties"), stem, _] => {
                format!("{domain}/{stem}.parquet")
            }
            ["topology", "runtime_catalog.parquet"] => {
                "topology/runtime_catalog.parquet".to_owned()
            }
            _ => return Err(validation("graph projection path has no logical table")),
        };
        logical_tables.entry(logical).or_default().push(path);
    }
    let mut writer = CanonicalWriter::new();
    writer.raw(b"GFGP1").map_err(canonical_error)?;
    writer
        .u32(exact_u32(logical_tables.len(), "graph table count")?)
        .map_err(canonical_error)?;
    for (relative, mut table_paths) in logical_tables {
        table_paths.sort();
        writer.text(&relative).map_err(canonical_error)?;
        let mut batches = Vec::new();
        for path in table_paths {
            let fragments = read_parquet(&path)?;
            if relative == "topology/nodes.parquet" {
                batches
                    .extend(crate::catalog::normalize_topology_nodes(fragments).map_err(storage)?);
            } else {
                batches.extend(fragments);
            }
        }
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| validation("graph projection table has no schema"))?;
        let batch = concat_batches(&schema, &batches).map_err(storage)?;
        let logical = logical_fingerprint_batch(&relative, &batch, runtime_entity_names)?;
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
    runtime_entity_names: Option<&HashMap<RuntimeEntityId, String>>,
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

fn portable_runtime_entity_names(root: &Path) -> Result<HashMap<RuntimeEntityId, String>, GfError> {
    let path = root.join("topology/runtime_catalog.parquet");
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let batches = read_parquet(&path)?;
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| validation("runtime catalog has no schema"))?;
    let batch = concat_batches(&schema, &batches).map_err(storage)?;
    let catalog = graphforge_ir::RuntimeCatalog::from_record_batch(&batch)?;
    Ok(catalog
        .entity_type_names_with_ids()
        .map(|(id, name)| (id, name.to_owned()))
        .collect())
}

fn portable_type_name(
    id: u32,
    runtime: &HashMap<RuntimeEntityId, String>,
) -> Result<String, GfError> {
    let id = graphforge_value::EntityTypeId::decode(id)
        .map_err(|error| validation(format!("invalid node type: {error}")))?;
    if let Some(local) = id.tagged().runtime_entity_id() {
        return runtime
            .get(&local)
            .map(|name| format!("runtime-entity:{name}"))
            .ok_or_else(|| validation("node runtime type ID has no catalog name"));
    }
    // Admission authenticates this semantic identity against its generation's
    // composition. Encoding a checked ID does not reinterpret a runtime domain.
    Ok(format!("semantic-storage-id:{}", id.encode()))
}

fn portable_node_type_column(
    name: &str,
    column: &ArrayRef,
    runtime: &HashMap<RuntimeEntityId, String>,
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
                let primary = graphforge_value::PrimaryEntityTypeId::decode(values.value(row))
                    .map_err(|error| validation(format!("invalid primary node route: {error}")))?;
                match primary.label() {
                    Some(id) => builder.append_value(portable_type_name(id.encode(), runtime)?),
                    None => builder.append_null(),
                }
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
        DataType::Null => writer.u8(0x01),
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
    if data_type == &DataType::Null {
        let _ = downcast::<NullArray>(array)?;
        if !nullable {
            return Err(validation("non-nullable graph field contains null"));
        }
        writer.u8(0).map_err(canonical_error)?;
        return Ok(());
    }
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

#[cfg(test)]
mod tests;
