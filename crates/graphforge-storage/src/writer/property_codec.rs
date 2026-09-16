//! Arrow property encoding, schema inference, and decoding.

use super::Arc;
use super::Array;
use super::ArrayRef;
use super::BooleanBuilder;
use super::DataType;
use super::EDGE_PROPERTY_UUID_FIELD;
use super::EdgePropRow;
use super::Field;
use super::FixedSizeBinaryArray;
use super::Float64Array;
use super::Float64Builder;
use super::GfError;
use super::HashMap;
use super::HashSet;
use super::Int64Builder;
use super::IrLiteral;
use super::NODE_PROPERTY_UUID_FIELD;
use super::PropRow;
use super::PropRowLike;
use super::RecordBatch;
use super::Schema;
use super::SpatialCoordinates;
use super::SpatialCrs;
use super::SpatialGeometryType;
use super::SpatialType;
use super::SpatialValue;
use super::StringArray;
use super::StringBuilder;
use super::TimeUnit;
use super::TimestampMicrosecondArray;
use super::TimestampMicrosecondBuilder;
use super::pq_err;
use super::uuid_field;

fn col_type_from_data_type(data_type: &DataType) -> Option<ColType> {
    match data_type {
        DataType::Int64 => Some(ColType::Int),
        DataType::Float64 => Some(ColType::Float),
        DataType::Boolean => Some(ColType::Bool),
        DataType::Utf8 => Some(ColType::Str),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(ColType::DateTime),
        DataType::Time64(TimeUnit::Nanosecond) => Some(ColType::Time),
        DataType::List(field) => {
            col_type_from_data_type(field.data_type()).map(|inner| ColType::List(Box::new(inner)))
        }
        DataType::Struct(fields) if *fields == heterogeneous_scalar_fields() => {
            Some(ColType::HetScalar)
        }
        DataType::Struct(fields) if *fields == crate::schemas::duration_struct_fields() => {
            Some(ColType::Duration)
        }
        DataType::Struct(fields) if *fields == crate::schemas::date_struct_fields() => {
            Some(ColType::Date)
        }
        DataType::Struct(fields) if *fields == crate::schemas::localdatetime_struct_fields() => {
            Some(ColType::LocalDateTime)
        }
        DataType::Struct(fields) if *fields == crate::schemas::time_struct_fields() => {
            Some(ColType::ZonedTime)
        }
        DataType::Struct(fields) if *fields == crate::schemas::datetime_struct_fields() => {
            Some(ColType::ZonedDateTime)
        }
        _ => None,
    }
}

pub(super) fn col_type_from_field(field: &Field) -> Option<ColType> {
    let Some(extension_name) = field.metadata().get("ARROW:extension:name") else {
        return col_type_from_data_type(field.data_type());
    };
    let geometry = match extension_name.as_str() {
        "geoarrow.point" => SpatialGeometryType::Point,
        "geoarrow.linestring" => SpatialGeometryType::LineString,
        "geoarrow.polygon" => SpatialGeometryType::Polygon,
        "geoarrow.multipoint" => SpatialGeometryType::MultiPoint,
        "geoarrow.multilinestring" => SpatialGeometryType::MultiLineString,
        "geoarrow.multipolygon" => SpatialGeometryType::MultiPolygon,
        _ => [
            SpatialGeometryType::Point,
            SpatialGeometryType::LineString,
            SpatialGeometryType::Polygon,
            SpatialGeometryType::MultiPoint,
            SpatialGeometryType::MultiLineString,
            SpatialGeometryType::MultiPolygon,
        ]
        .into_iter()
        .find(|geometry| {
            spatial_data_type(&SpatialType {
                geometry: *geometry,
                crs: SpatialCrs::Epsg4326,
            }) == *field.data_type()
        })?,
    };
    let extension_metadata = field.metadata().get("ARROW:extension:metadata")?;
    let crs_name = serde_json::from_str::<serde_json::Value>(extension_metadata)
        .ok()?
        .get("crs")?
        .as_str()?
        .to_owned();
    let crs = match crs_name.as_str() {
        "EPSG:4326" => SpatialCrs::Epsg4326,
        "EPSG:3857" => SpatialCrs::Epsg3857,
        _ => SpatialCrs::Preserved(crs_name),
    };
    let spatial_type = SpatialType { geometry, crs };
    (spatial_data_type(&spatial_type) == *field.data_type()).then(|| {
        ColType::Spatial(
            spatial_type,
            Some(extension_name.clone()),
            Some(extension_metadata.clone()),
        )
    })
}

pub(super) fn property_rows_batch_with_schema<R: PropRowLike>(
    schema: &Schema,
    uuid_field_name: &str,
    rows: &[R],
) -> Result<RecordBatch, GfError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(
        FixedSizeBinaryArray::try_from_iter(rows.iter().map(|row| row.uuid_bytes().to_vec()))
            .map_err(pq_err)?,
    ));
    for field in schema.fields().iter().skip(1) {
        let column_type = col_type_from_field(field).ok_or_else(|| {
            pq_err(format!(
                "unsupported canonical property type for {}",
                field.name()
            ))
        })?;
        columns.push(build_property_array(field.name(), column_type, rows));
    }
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns).map_err(pq_err)?;
    if batch.schema().field(0).name() != uuid_field_name {
        return Err(pq_err("canonical property UUID field mismatch"));
    }
    Ok(batch)
}
/// Arrow type a property column is built as, inferred from the literals seen.
// Not `Copy`: `List` boxes its inner type (a homogeneous `List<inner>` column).
#[derive(Clone, PartialEq, Eq)]
pub(super) enum ColType {
    Int,
    Float,
    Bool,
    Str,
    HetScalar,
    Duration,
    DateTime,
    Date,
    LocalDateTime,
    Time,
    ZonedTime,
    ZonedDateTime,
    Spatial(SpatialType, Option<String>, Option<String>),
    /// A homogeneous `List<inner>` column (#1006).
    List(Box<ColType>),
}

impl ColType {
    pub(super) fn of(lit: &IrLiteral) -> Option<Self> {
        match lit {
            IrLiteral::Null
            // Query-parameter maps are not a storage property type.
            | IrLiteral::Map(_)
            // Typed UUID parameters are identity predicates, not properties.
            | IrLiteral::Uuid(_) => None,
            IrLiteral::Int(_) => Some(Self::Int),
            IrLiteral::Float(_) => Some(Self::Float),
            IrLiteral::Bool(_) => Some(Self::Bool),
            IrLiteral::Str(_) => Some(Self::Str),
            IrLiteral::Duration { .. } => Some(Self::Duration),
            IrLiteral::DateTime(_) => Some(Self::DateTime),
            IrLiteral::Date(_) => Some(Self::Date),
            IrLiteral::LocalDateTime { .. } => Some(Self::LocalDateTime),
            IrLiteral::Time(_) => Some(Self::Time),
            IrLiteral::ZonedTime { .. } => Some(Self::ZonedTime),
            IrLiteral::ZonedDateTime { .. } => Some(Self::ZonedDateTime),
            IrLiteral::Spatial(value) => Some(Self::Spatial(
                value.spatial_type.clone(),
                value.extension_name.clone(),
                value.extension_metadata.clone(),
            )),
            // A homogeneous list: infer the inner type from the first non-null
            // element. A list whose elements are all null (or an empty list)
            // yields no type and the column falls back to `Str`. (#1006)
            IrLiteral::List(items) => items
                .iter()
                .find_map(Self::of)
                .map(|inner| Self::List(Box::new(inner))),
        }
    }

    pub(super) fn data_type(&self) -> DataType {
        match self {
            Self::Int => DataType::Int64,
            Self::Float => DataType::Float64,
            Self::Bool => DataType::Boolean,
            // `Str` is also the coercion target for mixed-type columns.
            Self::Str => DataType::Utf8,
            Self::HetScalar => DataType::Struct(heterogeneous_scalar_fields()),
            Self::Duration => DataType::Struct(crate::schemas::duration_struct_fields()),
            Self::DateTime => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Date => DataType::Struct(crate::schemas::date_struct_fields()),
            Self::LocalDateTime => DataType::Struct(crate::schemas::localdatetime_struct_fields()),
            Self::Time => DataType::Time64(TimeUnit::Nanosecond),
            Self::ZonedTime => DataType::Struct(crate::schemas::time_struct_fields()),
            Self::ZonedDateTime => DataType::Struct(crate::schemas::datetime_struct_fields()),
            Self::Spatial(spatial_type, _, _) => spatial_data_type(spatial_type),
            Self::List(inner) => {
                DataType::List(Arc::new(Field::new("item", inner.data_type(), true)))
            }
        }
    }

    pub(super) fn is_scalar(&self) -> bool {
        matches!(
            self,
            Self::Int | Self::Float | Self::Bool | Self::Str | Self::HetScalar
        )
    }
}

fn spatial_data_type(spatial_type: &SpatialType) -> DataType {
    let coordinate = DataType::Struct(arrow::datatypes::Fields::from(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
    ]));
    let list = |name: &str, child| DataType::List(Arc::new(Field::new(name, child, false)));
    match spatial_type.geometry {
        SpatialGeometryType::Point => coordinate,
        SpatialGeometryType::LineString => list("vertices", coordinate),
        SpatialGeometryType::Polygon => list("rings", list("vertices", coordinate)),
        SpatialGeometryType::MultiPoint => list("points", coordinate),
        SpatialGeometryType::MultiLineString => list("linestrings", list("vertices", coordinate)),
        SpatialGeometryType::MultiPolygon => {
            list("polygons", list("rings", list("vertices", coordinate)))
        }
    }
}

fn spatial_field(
    name: &str,
    spatial_type: &SpatialType,
    preserved_name: Option<&str>,
    preserved_metadata: Option<&str>,
    nullable: bool,
) -> Field {
    let canonical_name = match spatial_type.geometry {
        SpatialGeometryType::Point => "geoarrow.point",
        SpatialGeometryType::LineString => "geoarrow.linestring",
        SpatialGeometryType::Polygon => "geoarrow.polygon",
        SpatialGeometryType::MultiPoint => "geoarrow.multipoint",
        SpatialGeometryType::MultiLineString => "geoarrow.multilinestring",
        SpatialGeometryType::MultiPolygon => "geoarrow.multipolygon",
    };
    let crs = match &spatial_type.crs {
        SpatialCrs::Epsg4326 => "EPSG:4326",
        SpatialCrs::Epsg3857 => "EPSG:3857",
        SpatialCrs::Preserved(value) => value,
    };
    let extension_name = preserved_name.unwrap_or(canonical_name);
    let extension_metadata = preserved_metadata.map_or_else(
        || format!("{{\"crs\":\"{crs}\",\"crs_type\":\"authority_code\"}}"),
        ToOwned::to_owned,
    );
    Field::new(name, spatial_data_type(spatial_type), nullable).with_metadata(HashMap::from([
        ("ARROW:extension:name".to_owned(), extension_name.to_owned()),
        ("ARROW:extension:metadata".to_owned(), extension_metadata),
    ]))
}

pub(crate) fn heterogeneous_scalar_fields() -> arrow::datatypes::Fields {
    graphforge_value::heterogeneous::scalar_fields()
}

/// Build the dynamic schema and column arrays for a **node**-property file
/// (join key `node_uuid`, metadata key `graphforge.entity_type`).
pub(super) fn build_property_columns(
    entity_type: &str,
    rows: &[PropRow],
) -> Result<(Schema, Vec<ArrayRef>), GfError> {
    build_property_columns_keyed(
        NODE_PROPERTY_UUID_FIELD,
        "graphforge.entity_type",
        entity_type,
        rows,
    )
}

/// Build the dynamic schema and column arrays for a property file, keyed by an
/// arbitrary uuid join column.
///
/// Shared by node properties (`node_uuid`) and edge properties (`edge_uuid`).
/// Column order is the first-seen order of property names across `rows`
/// (deterministic).  Each column's type is inferred from the first non-null
/// value; conflicting scalar types use a tagged struct that preserves each value.
///
/// `uuid_field_name` is the leading join-key column; `meta_key`/`meta_value`
/// is the schema-level metadata identifying the file's entity or relation type.
pub(super) fn build_property_columns_keyed<R: PropRowLike>(
    uuid_field_name: &str,
    meta_key: &str,
    meta_value: &str,
    rows: &[R],
) -> Result<(Schema, Vec<ArrayRef>), GfError> {
    // First-seen-ordered list of property names + inferred column type.
    // `seen` tracks column order independently of `col_types`: a column may be
    // seen (ordered) before any concrete value fixes its type, so order-dedup
    // must not key on `col_types` membership.
    let mut order: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut col_types: HashMap<String, ColType> = HashMap::new();
    let mut explicit_nulls: HashSet<String> = HashSet::new();
    let mut untyped_non_nulls: HashSet<String> = HashSet::new();
    for row in rows {
        for (name, lit) in row.props() {
            reject_map_property_value(name, lit)?;
            if seen.insert(name.clone()) {
                order.push(name.clone());
            }
            if matches!(lit, IrLiteral::Null) {
                explicit_nulls.insert(name.clone());
            } else if ColType::of(lit).is_none() {
                untyped_non_nulls.insert(name.clone());
            }
            // Only concrete literals contribute a type; `Null` contributes
            // none, so the first *non-null* value determines the column type.
            // A column that never sees a concrete value defaults to `Str` via
            // `unwrap_or(ColType::Str)` below.
            if let Some(t) = ColType::of(lit) {
                col_types
                    .entry(name.clone())
                    .and_modify(|existing| {
                        if *existing != t {
                            *existing = if existing.is_scalar() && t.is_scalar() {
                                ColType::HetScalar
                            } else {
                                ColType::Str
                            };
                        }
                    })
                    .or_insert(t);
            }
        }
    }
    for name in explicit_nulls {
        if untyped_non_nulls.contains(&name) {
            continue;
        }
        match col_types.get(&name) {
            None | Some(ColType::Int | ColType::Float | ColType::Bool | ColType::Str) => {
                col_types.insert(name, ColType::HetScalar);
            }
            Some(_) => {}
        }
    }

    let mut fields: Vec<Field> = vec![uuid_field(uuid_field_name)];
    for name in &order {
        let ct = col_types.get(name).cloned().unwrap_or(ColType::Str);
        fields.push(match ct {
            ColType::Spatial(spatial_type, extension_name, extension_metadata) => spatial_field(
                name,
                &spatial_type,
                extension_name.as_deref(),
                extension_metadata.as_deref(),
                true,
            ),
            _ => Field::new(name, ct.data_type(), true),
        });
    }
    let meta: HashMap<String, String> = [(meta_key.to_owned(), meta_value.to_owned())]
        .into_iter()
        .collect();
    let schema = Schema::new(fields).with_metadata(meta);

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(order.len() + 1);
    let uuids = FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| r.uuid_bytes().to_vec()))
        .map_err(pq_err)?;
    cols.push(Arc::new(uuids));

    for name in &order {
        let ct = col_types.get(name).cloned().unwrap_or(ColType::Str);
        cols.push(build_property_array(name, ct, rows));
    }
    Ok((schema, cols))
}

pub(crate) fn property_snapshots_to_batch(
    route: &str,
    is_edge: bool,
    rows: Vec<crate::property_overlay::PropertySnapshotRow>,
) -> Result<Option<RecordBatch>, GfError> {
    if rows.is_empty() {
        return Ok(None);
    }
    if is_edge {
        let rows = rows
            .into_iter()
            .map(|row| EdgePropRow {
                edge_uuid: row.uuid,
                props: row.values.into_iter().collect(),
            })
            .collect::<Vec<_>>();
        let (schema, columns) = build_property_columns_keyed(
            EDGE_PROPERTY_UUID_FIELD,
            "graphforge.rel_type",
            route,
            &rows,
        )?;
        RecordBatch::try_new(Arc::new(schema), columns)
            .map(Some)
            .map_err(pq_err)
    } else {
        let rows = rows
            .into_iter()
            .map(|row| PropRow {
                node_uuid: row.uuid,
                props: row.values.into_iter().collect(),
            })
            .collect::<Vec<_>>();
        let (schema, columns) = build_property_columns(route, &rows)?;
        RecordBatch::try_new(Arc::new(schema), columns)
            .map(Some)
            .map_err(pq_err)
    }
}

pub(super) fn reject_map_property_value(name: &str, lit: &IrLiteral) -> Result<(), GfError> {
    if contains_uuid_literal(lit) {
        return Err(GfError::Validation(format!(
            "property `{name}` cannot store typed UUID query parameters"
        )));
    }
    if contains_map_literal(lit) {
        return Err(GfError::Storage(format!(
            "property `{name}` cannot store map values"
        )));
    }
    Ok(())
}

fn contains_map_literal(lit: &IrLiteral) -> bool {
    match lit {
        IrLiteral::Map(_) => true,
        IrLiteral::List(items) => items.iter().any(contains_map_literal),
        _ => false,
    }
}

fn contains_uuid_literal(lit: &IrLiteral) -> bool {
    match lit {
        IrLiteral::Uuid(_) => true,
        IrLiteral::List(items) => items.iter().any(contains_uuid_literal),
        IrLiteral::Map(entries) => entries
            .iter()
            .any(|(_, value)| contains_uuid_literal(value)),
        _ => false,
    }
}

/// Build one nullable property column, appending nulls for rows that omit the
/// property (or whose value does not match the column's inferred type — those
/// are stringified when the column is `Str`, else null).
#[allow(
    clippy::too_many_lines,
    reason = "one builder arm per ColType; the per-type append loops read clearest inline"
)]
pub(super) fn build_property_array<R: PropRowLike>(
    name: &str,
    ct: ColType,
    rows: &[R],
) -> ArrayRef {
    match ct {
        ColType::Int => {
            let mut b = Int64Builder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Int(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ColType::Float => {
            let mut b = Float64Builder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Float(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ColType::Bool => {
            let mut b = BooleanBuilder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Bool(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ColType::Duration => {
            // A typed duration is a `Struct{months, days, seconds, nanos}` (all
            // Int64; Parquet cannot persist Arrow `Interval`); shared field defs
            // via `duration_struct_fields`.
            use arrow::array::{Int64Builder, StructArray};
            use arrow::buffer::NullBuffer;
            let (mut mb, mut db, mut sb, mut nb) = (
                Int64Builder::new(),
                Int64Builder::new(),
                Int64Builder::new(),
                Int64Builder::new(),
            );
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::Duration {
                    months,
                    days,
                    seconds,
                    nanos,
                }) = row.props().get(name)
                {
                    mb.append_value(*months);
                    db.append_value(*days);
                    sb.append_value(*seconds);
                    nb.append_value(*nanos);
                    valid.push(true);
                } else {
                    mb.append_null();
                    db.append_null();
                    sb.append_null();
                    nb.append_null();
                    valid.push(false);
                }
            }
            Arc::new(StructArray::new(
                crate::schemas::duration_struct_fields(),
                vec![
                    Arc::new(mb.finish()),
                    Arc::new(db.finish()),
                    Arc::new(sb.finish()),
                    Arc::new(nb.finish()),
                ],
                Some(NullBuffer::from(valid)),
            ))
        }
        ColType::DateTime => {
            let mut b = TimestampMicrosecondBuilder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::DateTime(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish().with_timezone_opt(Some(Arc::from("UTC"))))
        }
        ColType::Date => {
            // A typed `date` is a self-describing `Struct{epoch_day: Int64}` —
            // i64 days, full year range (#1011).
            use arrow::array::{Int64Builder, StructArray};
            use arrow::buffer::NullBuffer;
            let mut b = Int64Builder::new();
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::Date(v)) = row.props().get(name) {
                    b.append_value(*v);
                    valid.push(true);
                } else {
                    b.append_null();
                    valid.push(false);
                }
            }
            Arc::new(StructArray::new(
                crate::schemas::date_struct_fields(),
                vec![Arc::new(b.finish())],
                Some(NullBuffer::from(valid)),
            ))
        }
        ColType::LocalDateTime => {
            // A typed `localdatetime` is a `Struct{date: Int64, time: Time64(ns)}`
            // (shared field defs via `localdatetime_struct_fields`).
            use arrow::array::{Int64Builder, StructArray, Time64NanosecondBuilder};
            use arrow::buffer::NullBuffer;
            let (mut date_b, mut time_b) = (Int64Builder::new(), Time64NanosecondBuilder::new());
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::LocalDateTime { days, nanos }) = row.props().get(name) {
                    date_b.append_value(*days);
                    time_b.append_value(*nanos);
                    valid.push(true);
                } else {
                    date_b.append_null();
                    time_b.append_null();
                    valid.push(false);
                }
            }
            Arc::new(StructArray::new(
                crate::schemas::localdatetime_struct_fields(),
                vec![Arc::new(date_b.finish()), Arc::new(time_b.finish())],
                Some(NullBuffer::from(valid)),
            ))
        }
        ColType::Time => {
            use arrow::array::Time64NanosecondBuilder;
            let mut b = Time64NanosecondBuilder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Time(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ColType::ZonedTime => {
            // A typed `time` is a `Struct{time: Time64(ns), offset: Int32}`.
            use arrow::array::{Int32Builder, StructArray, Time64NanosecondBuilder};
            use arrow::buffer::NullBuffer;
            let (mut time_b, mut off_b) = (Time64NanosecondBuilder::new(), Int32Builder::new());
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::ZonedTime { nanos, offset }) = row.props().get(name) {
                    time_b.append_value(*nanos);
                    off_b.append_value(*offset);
                    valid.push(true);
                } else {
                    time_b.append_null();
                    off_b.append_null();
                    valid.push(false);
                }
            }
            Arc::new(StructArray::new(
                crate::schemas::time_struct_fields(),
                vec![Arc::new(time_b.finish()), Arc::new(off_b.finish())],
                Some(NullBuffer::from(valid)),
            ))
        }
        ColType::ZonedDateTime => {
            // A typed `datetime` is a
            // `Struct{date: Int64, time: Time64(ns), offset: Int32, zone: Utf8}`.
            use arrow::array::{
                Int32Builder, Int64Builder, StringBuilder, StructArray, Time64NanosecondBuilder,
            };
            use arrow::buffer::NullBuffer;
            let (mut date_b, mut time_b, mut off_b, mut zone_b) = (
                Int64Builder::new(),
                Time64NanosecondBuilder::new(),
                Int32Builder::new(),
                StringBuilder::new(),
            );
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::ZonedDateTime {
                    days,
                    nanos,
                    offset,
                    zone,
                }) = row.props().get(name)
                {
                    date_b.append_value(*days);
                    time_b.append_value(*nanos);
                    off_b.append_value(*offset);
                    // None (offset-only) is stored as a NULL zone, not "".
                    zone_b.append_option(zone.as_deref());
                    valid.push(true);
                } else {
                    date_b.append_null();
                    time_b.append_null();
                    off_b.append_null();
                    zone_b.append_null();
                    valid.push(false);
                }
            }
            Arc::new(StructArray::new(
                crate::schemas::datetime_struct_fields(),
                vec![
                    Arc::new(date_b.finish()),
                    Arc::new(time_b.finish()),
                    Arc::new(off_b.finish()),
                    Arc::new(zone_b.finish()),
                ],
                Some(NullBuffer::from(valid)),
            ))
        }
        ColType::Spatial(spatial_type, _, _) => build_spatial_array(name, &spatial_type, rows),
        ColType::Str => {
            let mut b = StringBuilder::new();
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Null) | None => b.append_null(),
                    Some(other) => b.append_value(literal_to_string(other)),
                }
            }
            Arc::new(b.finish())
        }
        ColType::HetScalar => build_heterogeneous_scalar_array(name, rows),
        ColType::List(inner) => {
            // Flatten every list's elements into one-property synthetic rows, then
            // build the child array with the SAME per-type machinery (so temporal
            // element types reuse their struct builders); `offsets` delimit each
            // row's slice, and a row that is not a list is a null list slot. (#1006)
            use arrow::array::ListArray;
            use arrow::buffer::{NullBuffer, OffsetBuffer};
            let mut elem_rows: Vec<PropRow> = Vec::new();
            let mut offsets: Vec<i32> = vec![0];
            let mut valid = Vec::with_capacity(rows.len());
            for row in rows {
                if let Some(IrLiteral::List(items)) = row.props().get(name) {
                    for it in items {
                        let mut props = HashMap::with_capacity(1);
                        props.insert("item".to_string(), it.clone());
                        elem_rows.push(PropRow {
                            node_uuid: [0u8; 16],
                            props,
                        });
                    }
                    valid.push(true);
                } else {
                    valid.push(false);
                }
                offsets.push(i32::try_from(elem_rows.len()).unwrap_or(i32::MAX));
            }
            let child = build_property_array("item", (*inner).clone(), &elem_rows);
            let field = Arc::new(Field::new("item", inner.data_type(), true));
            Arc::new(ListArray::new(
                field,
                OffsetBuffer::new(offsets.into()),
                child,
                Some(NullBuffer::from(valid)),
            ))
        }
    }
}

fn coordinate_builder(capacity: usize) -> arrow::array::StructBuilder {
    use arrow::array::{ArrayBuilder, Float64Builder, StructBuilder};
    let DataType::Struct(fields) = spatial_data_type(&SpatialType {
        geometry: SpatialGeometryType::Point,
        crs: SpatialCrs::Epsg4326,
    }) else {
        unreachable!()
    };
    StructBuilder::new(
        fields,
        vec![
            Box::new(Float64Builder::with_capacity(capacity)) as Box<dyn ArrayBuilder>,
            Box::new(Float64Builder::with_capacity(capacity)),
        ],
    )
}

fn append_coordinate(builder: &mut arrow::array::StructBuilder, coordinate: [f64; 2]) {
    builder
        .field_builder::<Float64Builder>(0)
        .expect("canonical x builder")
        .append_value(coordinate[0]);
    builder
        .field_builder::<Float64Builder>(1)
        .expect("canonical y builder")
        .append_value(coordinate[1]);
    builder.append(true);
}

fn append_null_coordinate(builder: &mut arrow::array::StructBuilder) {
    builder
        .field_builder::<Float64Builder>(0)
        .expect("canonical x builder")
        .append_null();
    builder
        .field_builder::<Float64Builder>(1)
        .expect("canonical y builder")
        .append_null();
    builder.append(false);
}

#[allow(
    clippy::too_many_lines,
    reason = "six canonical GeoArrow nesting shapes share one exhaustive writer"
)]
fn build_spatial_array<R: PropRowLike>(
    name: &str,
    spatial_type: &SpatialType,
    rows: &[R],
) -> ArrayRef {
    use arrow::array::ListBuilder;

    match spatial_type.geometry {
        SpatialGeometryType::Point => {
            let mut builder = coordinate_builder(rows.len());
            for row in rows {
                match row.props().get(name) {
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::Point(coordinate),
                        ..
                    })) if observed == spatial_type => {
                        append_coordinate(&mut builder, *coordinate);
                    }
                    _ => append_null_coordinate(&mut builder),
                }
            }
            Arc::new(builder.finish())
        }
        SpatialGeometryType::LineString | SpatialGeometryType::MultiPoint => {
            let child_name = if spatial_type.geometry == SpatialGeometryType::LineString {
                "vertices"
            } else {
                "points"
            };
            let mut builder =
                ListBuilder::new(coordinate_builder(0)).with_field(Arc::new(Field::new(
                    child_name,
                    spatial_data_type(&SpatialType {
                        geometry: SpatialGeometryType::Point,
                        crs: spatial_type.crs.clone(),
                    }),
                    false,
                )));
            for row in rows {
                let coordinates = match row.props().get(name) {
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::LineString(values),
                        ..
                    })) if observed == spatial_type => Some(values.as_slice()),
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::MultiPoint(values),
                        ..
                    })) if observed == spatial_type => Some(values.as_slice()),
                    _ => None,
                };
                if let Some(coordinates) = coordinates {
                    for coordinate in coordinates {
                        append_coordinate(builder.values(), *coordinate);
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Arc::new(builder.finish())
        }
        SpatialGeometryType::Polygon | SpatialGeometryType::MultiLineString => {
            let coordinate_type = spatial_data_type(&SpatialType {
                geometry: SpatialGeometryType::Point,
                crs: spatial_type.crs.clone(),
            });
            let inner = ListBuilder::new(coordinate_builder(0)).with_field(Arc::new(Field::new(
                "vertices",
                coordinate_type.clone(),
                false,
            )));
            let outer_name = if spatial_type.geometry == SpatialGeometryType::Polygon {
                "rings"
            } else {
                "linestrings"
            };
            let mut builder = ListBuilder::new(inner).with_field(Arc::new(Field::new(
                outer_name,
                DataType::List(Arc::new(Field::new("vertices", coordinate_type, false))),
                false,
            )));
            for row in rows {
                let parts = match row.props().get(name) {
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::Polygon(values),
                        ..
                    })) if observed == spatial_type => Some(values.as_slice()),
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::MultiLineString(values),
                        ..
                    })) if observed == spatial_type => Some(values.as_slice()),
                    _ => None,
                };
                if let Some(parts) = parts {
                    for coordinates in parts {
                        for coordinate in coordinates {
                            append_coordinate(builder.values().values(), *coordinate);
                        }
                        builder.values().append(true);
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Arc::new(builder.finish())
        }
        SpatialGeometryType::MultiPolygon => {
            let coordinate_type = spatial_data_type(&SpatialType {
                geometry: SpatialGeometryType::Point,
                crs: spatial_type.crs.clone(),
            });
            let vertices = ListBuilder::new(coordinate_builder(0)).with_field(Arc::new(
                Field::new("vertices", coordinate_type.clone(), false),
            ));
            let rings_type = DataType::List(Arc::new(Field::new(
                "vertices",
                coordinate_type.clone(),
                false,
            )));
            let rings = ListBuilder::new(vertices).with_field(Arc::new(Field::new(
                "rings",
                rings_type.clone(),
                false,
            )));
            let mut builder = ListBuilder::new(rings).with_field(Arc::new(Field::new(
                "polygons",
                DataType::List(Arc::new(Field::new("rings", rings_type, false))),
                false,
            )));
            for row in rows {
                let polygons = match row.props().get(name) {
                    Some(IrLiteral::Spatial(SpatialValue {
                        spatial_type: observed,
                        coordinates: SpatialCoordinates::MultiPolygon(values),
                        ..
                    })) if observed == spatial_type => Some(values.as_slice()),
                    _ => None,
                };
                if let Some(polygons) = polygons {
                    for rings in polygons {
                        for coordinates in rings {
                            for coordinate in coordinates {
                                append_coordinate(builder.values().values().values(), *coordinate);
                            }
                            builder.values().values().append(true);
                        }
                        builder.values().append(true);
                    }
                    builder.append(true);
                } else {
                    builder.append(false);
                }
            }
            Arc::new(builder.finish())
        }
    }
}

fn build_heterogeneous_scalar_array<R: PropRowLike>(name: &str, rows: &[R]) -> ArrayRef {
    use graphforge_value::heterogeneous::{Scalar, encode_scalar};
    Arc::new(encode_scalar(rows.iter().map(
        |row| match row.props().get(name) {
            Some(IrLiteral::Null) => Some(Scalar::Null),
            Some(IrLiteral::Int(v)) => Some(Scalar::Int(*v)),
            Some(IrLiteral::Float(v)) => Some(Scalar::Float(*v)),
            Some(IrLiteral::Str(v)) => Some(Scalar::Str(v)),
            Some(IrLiteral::Bool(v)) => Some(Scalar::Bool(*v)),
            _ => None,
        },
    )))
}

/// Stringify a literal for a `Utf8`-coerced (mixed-type) property column.
fn literal_to_string(lit: &IrLiteral) -> String {
    match lit {
        IrLiteral::Null => String::new(),
        IrLiteral::Bool(b) => b.to_string(),
        IrLiteral::Int(i) => i.to_string(),
        IrLiteral::Float(f) => f.to_string(),
        IrLiteral::Str(s) => s.clone(),
        IrLiteral::Uuid(bytes) => {
            let mut encoded = String::with_capacity(32);
            for byte in bytes {
                std::fmt::Write::write_fmt(&mut encoded, format_args!("{byte:02x}"))
                    .expect("writing to a String cannot fail");
            }
            encoded
        }
        // A duration in a mixed (stringified) column: a deterministic
        // months/days/seconds/nanos form (the canonical `P…` render lives in graphforge-rel).
        IrLiteral::Duration {
            months,
            days,
            seconds,
            nanos,
        } => format!("{months}mo{days}d{seconds}s{nanos}ns"),
        IrLiteral::DateTime(t) => t.to_string(),
        IrLiteral::Date(d) => d.to_string(),
        // Temporal values in a mixed (stringified) column: deterministic forms
        // (the canonical renders live in graphforge-rel).
        IrLiteral::LocalDateTime { days, nanos } => format!("{days}d{nanos}ns"),
        IrLiteral::Time(nanos) => format!("{nanos}ns"),
        IrLiteral::ZonedTime { nanos, offset } => format!("{nanos}ns{offset:+}s"),
        IrLiteral::ZonedDateTime {
            days,
            nanos,
            offset,
            zone,
        } => format!(
            "{days}d{nanos}ns{offset:+}s{}",
            zone.as_deref().unwrap_or("")
        ),
        IrLiteral::Spatial(value) => {
            serde_json::to_string(value).expect("canonical spatial values are serializable")
        }
        // A list in a mixed (stringified) column: a deterministic bracketed form.
        IrLiteral::List(items) => {
            let parts: Vec<String> = items.iter().map(literal_to_string).collect();
            format!("[{}]", parts.join(","))
        }
        IrLiteral::Map(entries) => {
            let parts: Vec<String> = entries
                .iter()
                .map(|(key, value)| format!("{key}:{}", literal_to_string(value)))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
    }
}

/// Decode a previously-written property Parquet (the dynamic per-stem schema
/// `build_property_columns` produces) back into [`PropRow`]s, so a later flush
/// can re-run inference over `[decoded ++ new]` and merge (#733).
///
/// `node_uuid` (the join key, `FixedSizeBinary(16)`) populates `PropRow.node_uuid`;
/// every other column maps by its Arrow type to an [`IrLiteral`]. A **null** slot
/// omits the key for that row — it must never become a concrete value, or it
/// would wrongly pin a previously-all-null column's type on re-inference.
///
/// `IrLiteral` is a closed 7-variant set, so the writer only ever produces these
/// column types; an unexpected type is a defensive error.
pub(super) fn decode_property_rows(batches: &[RecordBatch]) -> Result<Vec<PropRow>, GfError> {
    let mut out = Vec::new();
    for batch in batches {
        decode_property_batch(batch, NODE_PROPERTY_UUID_FIELD, |node_uuid, props| {
            out.push(PropRow { node_uuid, props });
        })?;
    }
    Ok(out)
}

/// Edge analogue of [`decode_property_rows`]: decode `edge_properties/*.parquet`
/// back into [`EdgePropRow`]s (join key `edge_uuid`) for the read-merge-rewrite
/// flush cycle.
pub(super) fn decode_edge_property_rows(
    batches: &[RecordBatch],
) -> Result<Vec<EdgePropRow>, GfError> {
    let mut out = Vec::new();
    for batch in batches {
        decode_property_batch(batch, EDGE_PROPERTY_UUID_FIELD, |edge_uuid, props| {
            out.push(EdgePropRow { edge_uuid, props });
        })?;
    }
    Ok(out)
}
/// Validate heterogeneous data before returning any property row from a batch.
pub(crate) fn validate_property_values(
    batch: &RecordBatch,
) -> Result<(), graphforge_value::heterogeneous::ValueError> {
    for column in batch.columns() {
        graphforge_value::heterogeneous::validate_array(column.as_ref())?;
    }
    Ok(())
}

/// Decode one property batch row-by-row, invoking `emit(uuid, props)` per row.
///
/// `uuid_field_name` is the join-key column (`node_uuid` / `edge_uuid`); every
/// other column maps by its Arrow type to an [`IrLiteral`]. A **null** slot
/// omits the key for that row — it must never become a concrete value, or it
/// would wrongly pin a previously-all-null column's type on re-inference.
#[allow(
    clippy::too_many_lines,
    reason = "one decode arm per Arrow type, plus per-shape struct dispatch; clearest inline"
)]
pub(crate) fn decode_property_batch(
    batch: &RecordBatch,
    uuid_field_name: &str,
    mut emit: impl FnMut([u8; 16], HashMap<String, IrLiteral>),
) -> Result<(), GfError> {
    use arrow::array::Array;

    validate_property_values(batch).map_err(|error| GfError::Project {
        code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
        message: error.to_string(),
    })?;
    let schema = batch.schema();
    let uuid_col = batch
        .column_by_name(uuid_field_name)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| {
            GfError::Storage(format!("property file missing {uuid_field_name} column"))
        })?;
    for r in 0..batch.num_rows() {
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(uuid_col.value(r));
        let mut props: HashMap<String, IrLiteral> = HashMap::new();
        for (c, field) in schema.fields().iter().enumerate() {
            if field.name() == uuid_field_name {
                continue;
            }
            if field.data_type() == &DataType::Null {
                continue;
            }
            let col = batch.column(c);
            if col.is_null(r) {
                continue; // null slot: omit the key (never fabricate a value)
            }
            let lit = decode_value(col, field, r)?;
            props.insert(field.name().clone(), lit);
        }
        emit(uuid, props);
    }
    Ok(())
}

/// Decode one property value at row `r` from its column, dispatched by Arrow
/// type (structs by field NAMES). A `List` recurses element-wise on the inner
/// field's type, so list-of-temporals reuse the struct dispatch (#1006). Shared
/// by [`decode_property_batch`] and its own list arm.
#[allow(
    clippy::too_many_lines,
    reason = "one decode arm per Arrow type, plus per-shape struct dispatch; clearest inline"
)]
fn decode_value(
    col: &arrow::array::ArrayRef,
    field: &arrow::datatypes::Field,
    r: usize,
) -> Result<IrLiteral, GfError> {
    use arrow::array::{
        Array, BooleanArray, Int32Array, Int64Array, ListArray, StructArray, Time64NanosecondArray,
    };
    if field.metadata().contains_key("ARROW:extension:name") {
        return decode_spatial_value(col, field, r);
    }
    Ok(match field.data_type() {
        DataType::Int64 => IrLiteral::Int(downcast::<Int64Array>(col, field)?.value(r)),
        DataType::Float64 => IrLiteral::Float(downcast::<Float64Array>(col, field)?.value(r)),
        DataType::Boolean => IrLiteral::Bool(downcast::<BooleanArray>(col, field)?.value(r)),
        DataType::Utf8 => IrLiteral::Str(downcast::<StringArray>(col, field)?.value(r).to_owned()),
        // A typed temporal struct (#920). Dispatch by the struct's field
        // NAMES — every persisted Struct used to be assumed a duration,
        // but `localdatetime` (and later `time`/`datetime`) are also
        // structs, so the shape must select the decode.
        DataType::Struct(fields) => {
            let s = downcast::<StructArray>(col, field)?;
            if graphforge_value::heterogeneous::recognize(field.data_type())
                .map_err(|error| GfError::Storage(error.to_string()))?
                .is_some()
            {
                return graphforge_value::heterogeneous::decode_scalar(s, r)
                    .map_err(|error| GfError::Storage(error.to_string()));
            }
            let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
            match names.as_slice() {
                ["months", "days", "seconds", "nanos"] => {
                    let i64_at = |idx: usize| -> Result<i64, GfError> {
                        Ok(s.column(idx)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                GfError::Storage("duration struct child not Int64".into())
                            })?
                            .value(r))
                    };
                    IrLiteral::Duration {
                        months: i64_at(0)?,
                        days: i64_at(1)?,
                        seconds: i64_at(2)?,
                        nanos: i64_at(3)?,
                    }
                }
                ["epoch_day"] => IrLiteral::Date(
                    s.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| GfError::Storage("date epoch_day not Int64".into()))?
                        .value(r),
                ),
                ["date", "time"] => {
                    let days = s
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| GfError::Storage("localdatetime date not Int64".into()))?
                        .value(r);
                    let nanos = s
                        .column(1)
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .ok_or_else(|| {
                            GfError::Storage("localdatetime time not Time64(ns)".into())
                        })?
                        .value(r);
                    IrLiteral::LocalDateTime { days, nanos }
                }
                ["time", "offset"] => {
                    let nanos = s
                        .column(0)
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .ok_or_else(|| GfError::Storage("time not Time64(ns)".into()))?
                        .value(r);
                    let offset = s
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| GfError::Storage("time offset not Int32".into()))?
                        .value(r);
                    IrLiteral::ZonedTime { nanos, offset }
                }
                ["date", "time", "offset", "zone"] => {
                    let days = s
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| GfError::Storage("datetime date not Int64".into()))?
                        .value(r);
                    let nanos = s
                        .column(1)
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .ok_or_else(|| GfError::Storage("datetime time not Time64(ns)".into()))?
                        .value(r);
                    let offset = s
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| GfError::Storage("datetime offset not Int32".into()))?
                        .value(r);
                    let zone_col = s
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| GfError::Storage("datetime zone not Utf8".into()))?;
                    // A NULL zone child means offset-only (no named zone).
                    let zone = (!zone_col.is_null(r)).then(|| zone_col.value(r).to_owned());
                    IrLiteral::ZonedDateTime {
                        days,
                        nanos,
                        offset,
                        zone,
                    }
                }
                _ => {
                    return Err(GfError::Storage(format!(
                        "property column {} has unsupported struct shape {names:?}",
                        field.name()
                    )));
                }
            }
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            IrLiteral::Time(downcast::<Time64NanosecondArray>(col, field)?.value(r))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            IrLiteral::DateTime(downcast::<TimestampMicrosecondArray>(col, field)?.value(r))
        }
        // A homogeneous list (#1006): decode each element by recursing on
        // the inner field's type (so list-of-temporals reuse the struct
        // dispatch). Nested lists recurse naturally.
        DataType::List(inner) => {
            let larr = downcast::<ListArray>(col, field)?;
            let elems = larr.value(r);
            let mut items = Vec::with_capacity(elems.len());
            for j in 0..elems.len() {
                if elems.is_null(j) {
                    items.push(IrLiteral::Null);
                } else {
                    items.push(decode_value(&elems, inner, j)?);
                }
            }
            IrLiteral::List(items)
        }
        other => {
            return Err(GfError::Storage(format!(
                "property column {} has unsupported type {other:?}",
                field.name()
            )));
        }
    })
}

fn decode_spatial_value(col: &ArrayRef, field: &Field, row: usize) -> Result<IrLiteral, GfError> {
    decode_spatial_property_value(col.as_ref(), field, row).map(IrLiteral::Spatial)
}

/// Decode one canonical GeoArrow property row without reconstructing geometry
/// in a language binding.
#[allow(
    clippy::too_many_lines,
    reason = "six canonical GeoArrow nesting shapes share one exhaustive decoder"
)]
pub fn decode_spatial_property_value(
    col: &dyn arrow::array::Array,
    field: &Field,
    row: usize,
) -> Result<SpatialValue, GfError> {
    use arrow::array::{Array, ListArray, StructArray};
    let extension_name = field
        .metadata()
        .get("ARROW:extension:name")
        .ok_or_else(|| GfError::Storage("spatial field missing extension name".into()))?;
    let geometry = match extension_name.as_str() {
        "geoarrow.point" => SpatialGeometryType::Point,
        "geoarrow.linestring" => SpatialGeometryType::LineString,
        "geoarrow.polygon" => SpatialGeometryType::Polygon,
        "geoarrow.multipoint" => SpatialGeometryType::MultiPoint,
        "geoarrow.multilinestring" => SpatialGeometryType::MultiLineString,
        "geoarrow.multipolygon" => SpatialGeometryType::MultiPolygon,
        _ => [
            SpatialGeometryType::Point,
            SpatialGeometryType::LineString,
            SpatialGeometryType::Polygon,
            SpatialGeometryType::MultiPoint,
            SpatialGeometryType::MultiLineString,
            SpatialGeometryType::MultiPolygon,
        ]
        .into_iter()
        .find(|geometry| {
            spatial_data_type(&SpatialType {
                geometry: *geometry,
                crs: SpatialCrs::Epsg4326,
            }) == *field.data_type()
        })
        .ok_or_else(|| GfError::Storage("unsupported spatial extension storage type".into()))?,
    };
    let metadata = field
        .metadata()
        .get("ARROW:extension:metadata")
        .ok_or_else(|| GfError::Storage("spatial field missing extension metadata".into()))?;
    let crs_name = serde_json::from_str::<serde_json::Value>(metadata)
        .ok()
        .and_then(|value| value.get("crs")?.as_str().map(ToOwned::to_owned))
        .ok_or_else(|| GfError::Storage("spatial CRS metadata is malformed".into()))?;
    let crs = match crs_name.as_str() {
        "EPSG:4326" => SpatialCrs::Epsg4326,
        "EPSG:3857" => SpatialCrs::Epsg3857,
        _ => SpatialCrs::Preserved(crs_name),
    };
    let coordinates = match geometry {
        SpatialGeometryType::Point => SpatialCoordinates::Point(read_coordinate(
            col.as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| GfError::Storage("spatial point is not a struct".into()))?,
            row,
        )?),
        SpatialGeometryType::LineString | SpatialGeometryType::MultiPoint => {
            let value = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Storage("spatial geometry is not a list".into()))?
                .value(row);
            let values =
                read_coordinates(value.as_any().downcast_ref::<StructArray>().ok_or_else(
                    || GfError::Storage("spatial coordinate payload is not a struct".into()),
                )?)?;
            if geometry == SpatialGeometryType::LineString {
                SpatialCoordinates::LineString(values)
            } else {
                SpatialCoordinates::MultiPoint(values)
            }
        }
        SpatialGeometryType::Polygon | SpatialGeometryType::MultiLineString => {
            let value = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Storage("spatial geometry is not a nested list".into()))?
                .value(row);
            let lists = value
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Storage("spatial parts are not lists".into()))?;
            let mut parts = Vec::with_capacity(lists.len());
            for index in 0..lists.len() {
                let coordinates = lists.value(index);
                parts.push(read_coordinates(
                    coordinates
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            GfError::Storage("spatial coordinates are not structs".into())
                        })?,
                )?);
            }
            if geometry == SpatialGeometryType::Polygon {
                SpatialCoordinates::Polygon(parts)
            } else {
                SpatialCoordinates::MultiLineString(parts)
            }
        }
        SpatialGeometryType::MultiPolygon => {
            let value = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Storage("multipolygon is not a nested list".into()))?
                .value(row);
            let polygons = value
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Storage("multipolygon polygons are not lists".into()))?;
            let mut decoded = Vec::with_capacity(polygons.len());
            for polygon_index in 0..polygons.len() {
                let polygon = polygons.value(polygon_index);
                let rings = polygon
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| GfError::Storage("multipolygon rings are not lists".into()))?;
                let mut decoded_rings = Vec::with_capacity(rings.len());
                for ring_index in 0..rings.len() {
                    let coordinates = rings.value(ring_index);
                    decoded_rings.push(read_coordinates(
                        coordinates
                            .as_any()
                            .downcast_ref::<StructArray>()
                            .ok_or_else(|| {
                                GfError::Storage("multipolygon coordinates are not structs".into())
                            })?,
                    )?);
                }
                decoded.push(decoded_rings);
            }
            SpatialCoordinates::MultiPolygon(decoded)
        }
    };
    let preserved_crs = matches!(crs, SpatialCrs::Preserved(_));
    Ok(SpatialValue {
        spatial_type: SpatialType { geometry, crs },
        coordinates,
        extension_name: (!matches!(
            extension_name.as_str(),
            "geoarrow.point"
                | "geoarrow.linestring"
                | "geoarrow.polygon"
                | "geoarrow.multipoint"
                | "geoarrow.multilinestring"
                | "geoarrow.multipolygon"
        ))
        .then(|| extension_name.clone()),
        extension_metadata: preserved_crs.then(|| metadata.clone()),
    })
}

fn read_coordinates(array: &arrow::array::StructArray) -> Result<Vec<[f64; 2]>, GfError> {
    use arrow::array::Array;
    (0..array.len())
        .map(|index| read_coordinate(array, index))
        .collect()
}

fn read_coordinate(array: &arrow::array::StructArray, row: usize) -> Result<[f64; 2], GfError> {
    let x = array
        .column_by_name("x")
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| GfError::Storage("spatial x coordinate is not Float64".into()))?;
    let y = array
        .column_by_name("y")
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| GfError::Storage("spatial y coordinate is not Float64".into()))?;
    Ok([x.value(row), y.value(row)])
}

/// Downcast a column to a concrete Arrow array type, erroring with the column
/// name if the dynamic type does not match its declared field type.
fn downcast<'a, A: 'static>(
    col: &'a arrow::array::ArrayRef,
    field: &arrow::datatypes::Field,
) -> Result<&'a A, GfError> {
    col.as_any().downcast_ref::<A>().ok_or_else(|| {
        GfError::Storage(format!(
            "property column {} could not be read as its declared type",
            field.name()
        ))
    })
}

#[cfg(test)]
mod tests;
