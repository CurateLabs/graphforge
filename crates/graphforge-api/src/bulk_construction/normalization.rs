//! Bulk normalization ownership.

use super::{
    Arc, Array, ArrayRef, BTreeMap, BTreeSet, BULK_CONSTRUCTION_CONTRACT_VERSION, BooleanArray,
    BulkEdgeRow, BulkInputKind, BulkNodeRow, BulkValidationError, BulkValidationReason, DataType,
    Digest, Field, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float32Array, Float64Array,
    GraphForge, HashMap, HashSet, Int8Array, Int16Array, Int32Array, Int64Array, LargeListArray,
    LargeStringArray, ListArray, OntologyMode, OperationId, PropValue, PropertyValueType,
    RecordBatch, Schema, SchemaRef, Sha256, StringArray, StructArray, Time64NanosecondArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, Uuid, ValidatedBulkEdges,
    ValidatedBulkNodes, batch_error, candidate_endpoint_uuids, candidate_uuids, contract_error,
    existing_edge_context, field_error, indexed_existing, open_membership_index, row_error,
};

const NODE_REQUIRED: [(&str, DataType, bool); 2] = [
    ("node_uuid", DataType::FixedSizeBinary(16), true),
    ("label", DataType::Utf8, false),
];
const EDGE_REQUIRED: [(&str, DataType, bool); 4] = [
    ("edge_uuid", DataType::FixedSizeBinary(16), true),
    ("rel_type", DataType::Utf8, false),
    ("source_uuid", DataType::FixedSizeBinary(16), false),
    ("target_uuid", DataType::FixedSizeBinary(16), false),
];

fn input_schema(
    kind: BulkInputKind,
    required: &[(&str, DataType, bool)],
    mut properties: Vec<Field>,
) -> Result<SchemaRef, BulkValidationError> {
    properties.sort_unstable_by(|left, right| left.name().cmp(right.name()));
    let mut prior = None;
    for field in &properties {
        if required.iter().any(|(name, _, _)| *name == field.name()) {
            return Err(field_error(
                kind,
                BulkValidationReason::ReservedField,
                field.name(),
                "property name is reserved by the topology contract",
            ));
        }
        validate_property_name(kind, field.name())?;
        if field.metadata().contains_key("ARROW:extension:name") {
            (spatial_type_from_field(field).is_some() || is_preserved_spatial_field(field))
                .then_some(())
                .ok_or_else(|| {
                    field_error(
                        kind,
                        BulkValidationReason::UnsupportedPropertyType,
                        field.name(),
                        "unsupported Arrow extension property",
                    )
                })?;
        } else {
            validate_property_type(kind, field.name(), field.data_type())?;
        }
        if prior == Some(field.name()) {
            return Err(field_error(
                kind,
                BulkValidationReason::DuplicateField,
                field.name(),
                "property field is duplicated",
            ));
        }
        prior = Some(field.name());
    }
    let fields = required
        .iter()
        .map(|(name, data_type, nullable)| Field::new(*name, data_type.clone(), *nullable))
        .chain(properties)
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        contract_metadata(kind.as_str()),
    )))
}

pub(super) fn contract_metadata(kind: &str) -> HashMap<String, String> {
    HashMap::from([
        (
            "graphforge.bulk_contract_version".to_owned(),
            BULK_CONSTRUCTION_CONTRACT_VERSION.to_string(),
        ),
        ("graphforge.bulk_kind".to_owned(), kind.to_owned()),
        (
            "graphforge.row_order".to_owned(),
            "logical_input_order".to_owned(),
        ),
    ])
}

fn validate_partition_schemas(
    kind: BulkInputKind,
    required: &[(&str, DataType, bool)],
    batches: &[RecordBatch],
) -> Result<(), BulkValidationError> {
    let Some(first) = batches.first() else {
        return Ok(());
    };
    validate_batch_schema(kind, 0, required, first.schema().as_ref())?;
    for (index, batch) in batches.iter().enumerate().skip(1) {
        validate_batch_schema(kind, index, required, batch.schema().as_ref())?;
        if batch.schema().as_ref() != first.schema().as_ref() {
            return Err(batch_error(
                kind,
                index,
                BulkValidationReason::SchemaMismatch,
                None,
                "schema differs from batch 0",
            ));
        }
    }
    Ok(())
}

fn validate_batch_schema(
    kind: BulkInputKind,
    batch_index: usize,
    required: &[(&str, DataType, bool)],
    schema: &Schema,
) -> Result<(), BulkValidationError> {
    if schema.metadata() != &contract_metadata(kind.as_str()) {
        return Err(batch_error(
            kind,
            batch_index,
            BulkValidationReason::SchemaMismatch,
            None,
            "contract version/kind/order metadata is not canonical",
        ));
    }
    if schema.fields().len() < required.len() {
        return Err(batch_error(
            kind,
            batch_index,
            BulkValidationReason::SchemaMismatch,
            None,
            "required topology fields are missing",
        ));
    }
    for (index, (name, expected, nullable)) in required.iter().enumerate() {
        let field = &schema.fields()[index];
        if field.name() != name || field.data_type() != expected || field.is_nullable() != *nullable
        {
            return Err(batch_error(
                kind,
                batch_index,
                BulkValidationReason::SchemaMismatch,
                Some(name),
                &format!("field {index} must be {name:?}: {expected} nullable={nullable}"),
            ));
        }
    }
    let properties = schema
        .fields()
        .iter()
        .skip(required.len())
        .collect::<Vec<_>>();
    if !properties
        .windows(2)
        .all(|pair| pair[0].name() < pair[1].name())
    {
        return Err(batch_error(
            kind,
            batch_index,
            BulkValidationReason::SchemaMismatch,
            None,
            "property fields must be unique and lexicographically ordered",
        ));
    }
    for field in properties {
        validate_property_name(kind, field.name())?;
        if field.metadata().contains_key("ARROW:extension:name") {
            (spatial_type_from_field(field).is_some() || is_preserved_spatial_field(field))
                .then_some(())
                .ok_or_else(|| {
                    field_error(
                        kind,
                        BulkValidationReason::UnsupportedPropertyType,
                        field.name(),
                        "unsupported Arrow extension property",
                    )
                })?;
        } else {
            validate_property_type(kind, field.name(), field.data_type())?;
        }
    }
    Ok(())
}

fn property_columns<'a>(
    batch: &'a RecordBatch,
    required: &[(&str, DataType, bool)],
) -> Vec<(&'a Field, &'a ArrayRef)> {
    batch
        .schema_ref()
        .fields()
        .iter()
        .enumerate()
        .skip(required.len())
        .map(|(index, field)| (field.as_ref(), batch.column(index)))
        .collect()
}

fn validate_edge_identity(
    edge_uuid: Uuid,
    known_nodes: &HashSet<Uuid>,
    existing_edges: &HashSet<Uuid>,
    observed: &mut HashSet<Uuid>,
    ordinal: u64,
) -> Result<(), BulkValidationError> {
    if known_nodes.contains(&edge_uuid)
        || existing_edges.contains(&edge_uuid)
        || !observed.insert(edge_uuid)
    {
        return Err(row_error(
            BulkInputKind::Edge,
            BulkValidationReason::IdentityConflict,
            ordinal,
            "edge_uuid",
            "duplicate or existing UUID",
        ));
    }
    Ok(())
}

fn validate_edge_endpoint(
    endpoint_uuid: Uuid,
    known_nodes: &HashSet<Uuid>,
    ordinal: u64,
    field: &str,
) -> Result<(), BulkValidationError> {
    if !known_nodes.contains(&endpoint_uuid) {
        return Err(row_error(
            BulkInputKind::Edge,
            BulkValidationReason::MissingEndpoint,
            ordinal,
            field,
            "endpoint does not exist",
        ));
    }
    Ok(())
}

fn normalize_properties<F>(
    kind: BulkInputKind,
    ordinal: u64,
    row: usize,
    columns: &[(&Field, &ArrayRef)],
    mut owner_validation: F,
) -> Result<BTreeMap<String, PropValue>, BulkValidationError>
where
    F: FnMut(&str, &Field) -> Result<(), BulkValidationError>,
{
    let mut values = BTreeMap::new();
    for (field, array) in columns {
        owner_validation(field.name(), field)?;
        let value = if field.metadata().contains_key("ARROW:extension:name") {
            if array.is_null(row) {
                PropValue::Null
            } else {
                PropValue::Spatial(
                    graphforge_storage::decode_spatial_property_value(array.as_ref(), field, row)
                        .map_err(|error| {
                        row_error(
                            kind,
                            BulkValidationReason::UnsupportedPropertyType,
                            ordinal,
                            field.name(),
                            &error.to_string(),
                        )
                    })?,
                )
            }
        } else {
            property_value_at(array.as_ref(), row).map_err(|message| {
                row_error(
                    kind,
                    BulkValidationReason::UnsupportedPropertyType,
                    ordinal,
                    field.name(),
                    &message,
                )
            })?
        };
        values.insert(field.name().to_owned(), value);
    }
    Ok(values)
}

fn preflight_spatial_columns(
    kind: BulkInputKind,
    first_ordinal: u64,
    columns: &[(&Field, &ArrayRef)],
) -> Result<(), BulkValidationError> {
    for (field, array) in columns {
        let Some(_) = field.metadata().get("ARROW:extension:name") else {
            continue;
        };
        if let Some(spatial) = spatial_type_from_field(field) {
            spatial
                .validate_array(
                    field,
                    array.as_ref(),
                    graphforge_ontology::SpatialValidationLimits::default(),
                )
                .map_err(|error| {
                    row_error(
                        kind,
                        BulkValidationReason::PropertyTypeMismatch,
                        first_ordinal,
                        field.name(),
                        error.code(),
                    )
                })?;
        } else {
            let limits = graphforge_ontology::SpatialValidationLimits::default();
            let metadata = field
                .metadata()
                .get("ARROW:extension:metadata")
                .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                .filter(|value| value.get("crs").is_some());
            if metadata.is_none()
                || array.len() > limits.max_geometries
                || array.get_array_memory_size() > limits.max_bytes
            {
                return Err(row_error(
                    kind,
                    BulkValidationReason::PropertyTypeMismatch,
                    first_ordinal,
                    field.name(),
                    "preserved spatial field has malformed metadata or exceeds its resource limit",
                ));
            }
        }
    }
    Ok(())
}

fn spatial_type_from_field(field: &Field) -> Option<graphforge_ontology::SpatialType> {
    use graphforge_ontology::{SpatialCrs, SpatialGeometryType, SpatialType};
    let geometry = match field.metadata().get("ARROW:extension:name")?.as_str() {
        "geoarrow.point" => SpatialGeometryType::Point,
        "geoarrow.linestring" => SpatialGeometryType::LineString,
        "geoarrow.polygon" => SpatialGeometryType::Polygon,
        "geoarrow.multipoint" => SpatialGeometryType::MultiPoint,
        "geoarrow.multilinestring" => SpatialGeometryType::MultiLineString,
        "geoarrow.multipolygon" => SpatialGeometryType::MultiPolygon,
        _ => return None,
    };
    let metadata = field.metadata().get("ARROW:extension:metadata")?;
    let crs = if metadata == &SpatialCrs::Epsg4326.extension_metadata() {
        SpatialCrs::Epsg4326
    } else if metadata == &SpatialCrs::Epsg3857.extension_metadata() {
        SpatialCrs::Epsg3857
    } else {
        return None;
    };
    Some(SpatialType { geometry, crs })
}

fn is_preserved_spatial_field(field: &Field) -> bool {
    use graphforge_ontology::{SpatialCrs, SpatialGeometryType, SpatialType};

    let Some(metadata) = field.metadata().get("ARROW:extension:metadata") else {
        return false;
    };
    if serde_json::from_str::<serde_json::Value>(metadata)
        .ok()
        .and_then(|value| value.get("crs").cloned())
        .is_none()
    {
        return false;
    }
    [
        SpatialGeometryType::Point,
        SpatialGeometryType::LineString,
        SpatialGeometryType::Polygon,
        SpatialGeometryType::MultiPoint,
        SpatialGeometryType::MultiLineString,
        SpatialGeometryType::MultiPolygon,
    ]
    .into_iter()
    .any(|geometry| {
        SpatialType {
            geometry,
            crs: SpatialCrs::Epsg4326,
        }
        .field("spatial", field.is_nullable())
        .data_type()
            == field.data_type()
    })
}

fn property_value_at(array: &dyn Array, row: usize) -> Result<PropValue, String> {
    if array.is_null(row) {
        return Ok(PropValue::Null);
    }
    macro_rules! scalar {
        ($ty:ty, $value:expr) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .map(|values| $value(values.value(row)))
                .ok_or_else(|| "Arrow array does not match its schema".to_owned())
        };
    }
    match array.data_type() {
        DataType::Boolean => scalar!(BooleanArray, PropValue::Bool),
        DataType::Int8 => scalar!(Int8Array, |value| PropValue::Int(i64::from(value))),
        DataType::Int16 => scalar!(Int16Array, |value| PropValue::Int(i64::from(value))),
        DataType::Int32 => scalar!(Int32Array, |value| PropValue::Int(i64::from(value))),
        DataType::Int64 => scalar!(Int64Array, PropValue::Int),
        DataType::UInt8 => scalar!(UInt8Array, |value| PropValue::Int(i64::from(value))),
        DataType::UInt16 => scalar!(UInt16Array, |value| PropValue::Int(i64::from(value))),
        DataType::UInt32 => scalar!(UInt32Array, |value| PropValue::Int(i64::from(value))),
        DataType::Float32 => scalar!(Float32Array, |value| PropValue::Float(f64::from(value))),
        DataType::Float64 => scalar!(Float64Array, PropValue::Float),
        DataType::Utf8 => scalar!(StringArray, |value: &str| PropValue::Str(value.to_owned())),
        DataType::LargeUtf8 => {
            scalar!(LargeStringArray, |value: &str| PropValue::Str(
                value.to_owned()
            ))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, timezone)
            if timezone.as_deref() == Some("UTC") =>
        {
            scalar!(TimestampMicrosecondArray, |epoch_micros| {
                PropValue::Temporal(graphforge_core::TemporalValue::UtcDateTime { epoch_micros })
            })
        }
        DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => {
            let nanos = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .ok_or_else(|| "Arrow array does not match its schema".to_owned())?
                .value(row);
            validate_wall_clock_nanos(nanos)?;
            Ok(PropValue::Temporal(
                graphforge_core::TemporalValue::LocalTime { nanos },
            ))
        }
        DataType::Struct(fields) => temporal_struct_value(array, row, fields),
        DataType::List(_) => {
            let values = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "Arrow array does not match its schema".to_owned())?
                .value(row);
            (0..values.len())
                .map(|index| property_value_at(values.as_ref(), index))
                .collect::<Result<Vec<_>, _>>()
                .map(PropValue::List)
        }
        DataType::LargeList(_) => {
            let values = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| "Arrow array does not match its schema".to_owned())?
                .value(row);
            (0..values.len())
                .map(|index| property_value_at(values.as_ref(), index))
                .collect::<Result<Vec<_>, _>>()
                .map(PropValue::List)
        }
        other => Err(format!("unsupported property type {other}")),
    }
}

fn temporal_struct_value(
    array: &dyn Array,
    row: usize,
    fields: &arrow::datatypes::Fields,
) -> Result<PropValue, String> {
    use graphforge_core::TemporalValue;
    let values = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "Arrow array does not match its schema".to_owned())?;
    let names = fields
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    let i64_at = |name: &str| -> Result<i64, String> {
        let column = values
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| format!("temporal field {name} is not Int64"))?;
        (!column.is_null(row))
            .then(|| column.value(row))
            .ok_or_else(|| format!("temporal field {name} is null for a present value"))
    };
    let nanos_at = |name: &str| -> Result<i64, String> {
        let column = values
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Time64NanosecondArray>())
            .ok_or_else(|| format!("temporal field {name} is not Time64(Nanosecond)"))?;
        let nanos = (!column.is_null(row))
            .then(|| column.value(row))
            .ok_or_else(|| format!("temporal field {name} is null for a present value"))?;
        validate_wall_clock_nanos(nanos)?;
        Ok(nanos)
    };
    let offset = || -> Result<i32, String> {
        let column = values
            .column_by_name("offset")
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| "temporal field offset is not Int32".to_owned())?;
        let value = (!column.is_null(row))
            .then(|| column.value(row))
            .ok_or_else(|| "temporal field offset is null for a present value".to_owned())?;
        if !(-64_800..=64_800).contains(&value) {
            return Err("temporal offset exceeds the certified +/-18 hour range".to_owned());
        }
        Ok(value)
    };
    let temporal = match names.as_slice() {
        ["months", "days", "seconds", "nanos"] => TemporalValue::Duration {
            months: i64_at("months")?,
            days: i64_at("days")?,
            seconds: i64_at("seconds")?,
            nanos: i64_at("nanos")?,
        },
        ["epoch_day"] => TemporalValue::Date {
            epoch_days: i64_at("epoch_day")?,
        },
        ["date", "time"] => TemporalValue::LocalDateTime {
            epoch_days: i64_at("date")?,
            nanos: nanos_at("time")?,
        },
        ["time", "offset"] => TemporalValue::OffsetTime {
            nanos: nanos_at("time")?,
            offset_seconds: offset()?,
        },
        ["date", "time", "offset", "zone"] => {
            let zones = values
                .column_by_name("zone")
                .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| "temporal field zone is not Utf8".to_owned())?;
            let zone = (!zones.is_null(row)).then(|| zones.value(row).to_owned());
            if zone.as_ref().is_some_and(|zone| {
                zone.is_empty() || zone.len() > 255 || zone.chars().any(char::is_control)
            }) {
                return Err("temporal zone identity is malformed".to_owned());
            }
            TemporalValue::ZonedDateTime {
                epoch_days: i64_at("date")?,
                nanos: nanos_at("time")?,
                offset_seconds: offset()?,
                zone,
            }
        }
        _ => return Err("unsupported temporal struct contract".to_owned()),
    };
    temporal.validate().map_err(|error| error.to_string())?;
    Ok(PropValue::Temporal(temporal))
}

fn validate_wall_clock_nanos(nanos: i64) -> Result<(), String> {
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    if (0..NANOS_PER_DAY).contains(&nanos) {
        Ok(())
    } else {
        Err("temporal wall-clock nanoseconds are outside one day".to_owned())
    }
}

fn validate_node_owner(
    graph: &GraphForge,
    ordinal: u64,
    label: &str,
) -> Result<(), BulkValidationError> {
    if graph.ontology_mode == OntologyMode::Strict
        && graph
            .ontology
            .as_ref()
            .and_then(|ontology| ontology.entity_type_id(label))
            .is_none()
    {
        return Err(row_error(
            BulkInputKind::Node,
            BulkValidationReason::UnknownOntologyType,
            ordinal,
            "label",
            "unknown strict ontology entity type",
        ));
    }
    Ok(())
}

fn validate_edge_owner(
    graph: &GraphForge,
    ordinal: u64,
    rel_type: &str,
) -> Result<(), BulkValidationError> {
    if graph.ontology_mode == OntologyMode::Strict
        && graph
            .ontology
            .as_ref()
            .and_then(|ontology| ontology.relation_type_id(rel_type))
            .is_none()
    {
        return Err(row_error(
            BulkInputKind::Edge,
            BulkValidationReason::UnknownOntologyType,
            ordinal,
            "rel_type",
            "unknown strict ontology relationship type",
        ));
    }
    Ok(())
}

fn validate_node_property(
    graph: &GraphForge,
    ordinal: u64,
    label: &str,
    name: &str,
    field: &Field,
) -> Result<(), BulkValidationError> {
    if graph.ontology_mode != OntologyMode::Strict {
        return Ok(());
    }
    let ontology = graph.ontology.as_ref().ok_or_else(|| {
        contract_error(
            BulkInputKind::Node,
            BulkValidationReason::ProjectState,
            "strict project has no ontology",
        )
    })?;
    let owner = ontology.entity_type_id(label).ok_or_else(|| {
        row_error(
            BulkInputKind::Node,
            BulkValidationReason::UnknownOntologyType,
            ordinal,
            "label",
            "unknown strict ontology entity type",
        )
    })?;
    let definition = ontology.entity_property_def(owner, name).ok_or_else(|| {
        row_error(
            BulkInputKind::Node,
            BulkValidationReason::UnknownOntologyProperty,
            ordinal,
            name,
            "property is not declared for strict entity type",
        )
    })?;
    validate_ontology_field(
        BulkInputKind::Node,
        ordinal,
        name,
        field,
        &definition.value_type,
        definition.nullable,
    )
}

fn validate_edge_property(
    graph: &GraphForge,
    ordinal: u64,
    rel_type: &str,
    name: &str,
    field: &Field,
) -> Result<(), BulkValidationError> {
    if graph.ontology_mode != OntologyMode::Strict {
        return Ok(());
    }
    let ontology = graph.ontology.as_ref().ok_or_else(|| {
        contract_error(
            BulkInputKind::Edge,
            BulkValidationReason::ProjectState,
            "strict project has no ontology",
        )
    })?;
    let owner = ontology.relation_type_id(rel_type).ok_or_else(|| {
        row_error(
            BulkInputKind::Edge,
            BulkValidationReason::UnknownOntologyType,
            ordinal,
            "rel_type",
            "unknown strict ontology relationship type",
        )
    })?;
    let definition = ontology.relation_property_def(owner, name).ok_or_else(|| {
        row_error(
            BulkInputKind::Edge,
            BulkValidationReason::UnknownOntologyProperty,
            ordinal,
            name,
            "property is not declared for strict relationship type",
        )
    })?;
    validate_ontology_field(
        BulkInputKind::Edge,
        ordinal,
        name,
        field,
        &definition.value_type,
        definition.nullable,
    )
}

fn validate_ontology_field(
    kind: BulkInputKind,
    ordinal: u64,
    name: &str,
    field: &Field,
    expected: &PropertyValueType,
    nullable: bool,
) -> Result<(), BulkValidationError> {
    let expected_arrow = graphforge_storage::property_type_to_arrow(expected);
    let compatible = match expected {
        PropertyValueType::Utf8 => {
            matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8)
        }
        PropertyValueType::Int64 => matches!(
            field.data_type(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
        ),
        PropertyValueType::Float64 => {
            matches!(field.data_type(), DataType::Float32 | DataType::Float64)
        }
        PropertyValueType::Bool => matches!(field.data_type(), DataType::Boolean),
        PropertyValueType::List => {
            matches!(
                field.data_type(),
                DataType::List(_) | DataType::LargeList(_)
            )
        }
        PropertyValueType::Duration => field.data_type() == &expected_arrow,
        PropertyValueType::DateTime => temporal_datetime_data_type(field.data_type()),
        PropertyValueType::Spatial(spatial) => {
            field.data_type() == &spatial.data_type()
                && field.metadata() == &spatial.field_metadata()
        }
        PropertyValueType::Map => false,
    };
    if !compatible {
        return Err(row_error(
            kind,
            BulkValidationReason::PropertyTypeMismatch,
            ordinal,
            name,
            &format!(
                "property type {} does not match strict ontology type {expected_arrow}",
                field.data_type()
            ),
        ));
    }
    if field.is_nullable() && !nullable {
        return Err(row_error(
            kind,
            BulkValidationReason::NullabilityMismatch,
            ordinal,
            name,
            "nullable field violates non-null strict ontology property",
        ));
    }
    Ok(())
}

fn temporal_datetime_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, zone)
            if zone.as_deref() == Some("UTC")
    ) || matches!(
        data_type,
        DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond)
    ) || matches!(data_type, DataType::Struct(fields)
            if fields == &graphforge_storage::schemas::date_struct_fields()
                || fields == &graphforge_storage::schemas::localdatetime_struct_fields()
                || fields == &graphforge_storage::schemas::time_struct_fields()
                || fields == &graphforge_storage::schemas::datetime_struct_fields())
}

fn validate_property_name(kind: BulkInputKind, name: &str) -> Result<(), BulkValidationError> {
    validate_identifier_parts(name)
        .then_some(())
        .ok_or_else(|| {
            field_error(
                kind,
                BulkValidationReason::InvalidIdentifier,
                name,
                "property name is not a valid identifier",
            )
        })
}

fn validate_property_type(
    kind: BulkInputKind,
    name: &str,
    data_type: &DataType,
) -> Result<(), BulkValidationError> {
    let supported = match data_type {
        DataType::List(field) | DataType::LargeList(field) => {
            property_data_type_supported(field.data_type())
        }
        other => property_data_type_supported(other),
    };
    if supported {
        Ok(())
    } else {
        Err(field_error(
            kind,
            BulkValidationReason::UnsupportedPropertyType,
            name,
            &format!("unsupported Arrow property type {data_type}"),
        ))
    }
}

fn property_data_type_supported(data_type: &DataType) -> bool {
    graphforge_storage::schemas::property_data_type_supported(data_type)
}

fn validate_identifier(
    kind: BulkInputKind,
    ordinal: u64,
    field: &str,
    value: &str,
) -> Result<(), BulkValidationError> {
    if validate_identifier_parts(value) {
        Ok(())
    } else {
        Err(row_error(
            kind,
            BulkValidationReason::InvalidIdentifier,
            ordinal,
            field,
            "invalid identifier",
        ))
    }
}

fn validate_identifier_parts(value: &str) -> bool {
    graphforge_core::identifier::is_graph_identifier(value)
}

pub(super) fn uuid_column<'a>(
    batch: &'a RecordBatch,
    kind: BulkInputKind,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, BulkValidationError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| {
            field_error(
                kind,
                BulkValidationReason::SchemaMismatch,
                name,
                "field is not FixedSizeBinary(16)",
            )
        })
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    kind: BulkInputKind,
    name: &str,
) -> Result<&'a StringArray, BulkValidationError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            field_error(
                kind,
                BulkValidationReason::SchemaMismatch,
                name,
                "field is not Utf8",
            )
        })
}

fn uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    kind: BulkInputKind,
    ordinal: u64,
    field: &str,
    operation_uuid: OperationId,
) -> Result<Uuid, BulkValidationError> {
    validated_uuid_at(values, row, kind, ordinal, field, || {
        Ok(generated_uuid(operation_uuid, kind, ordinal))
    })
}

fn explicit_uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    kind: BulkInputKind,
    ordinal: u64,
    field: &str,
) -> Result<Uuid, BulkValidationError> {
    validated_uuid_at(values, row, kind, ordinal, field, || {
        Err(uuid_row_error(
            kind,
            ordinal,
            field,
            "endpoint UUID cannot be null",
        ))
    })
}

fn validated_uuid_at(
    values: &FixedSizeBinaryArray,
    row: usize,
    kind: BulkInputKind,
    ordinal: u64,
    field: &str,
    on_null: impl FnOnce() -> Result<Uuid, BulkValidationError>,
) -> Result<Uuid, BulkValidationError> {
    if values.is_null(row) {
        return on_null();
    }
    let uuid = Uuid::from_slice(values.value(row))
        .map_err(|_| uuid_row_error(kind, ordinal, field, "invalid UUID bytes"))?;
    if uuid.get_version_num() != 7 {
        return Err(uuid_row_error(kind, ordinal, field, "value must be UUIDv7"));
    }
    Ok(uuid)
}

fn uuid_row_error(
    kind: BulkInputKind,
    ordinal: u64,
    field: &str,
    message: &str,
) -> BulkValidationError {
    row_error(
        kind,
        BulkValidationReason::InvalidUuid,
        ordinal,
        field,
        message,
    )
}

fn validate_operation_uuid(
    kind: BulkInputKind,
    operation_uuid: OperationId,
) -> Result<(), BulkValidationError> {
    if operation_uuid.0.get_version_num() == 7 {
        Ok(())
    } else {
        Err(field_error(
            kind,
            BulkValidationReason::InvalidUuid,
            "operation_uuid",
            "operation identity must be UUIDv7",
        ))
    }
}

fn generated_uuid(operation_uuid: OperationId, kind: BulkInputKind, ordinal: u64) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge.bulk.generated-uuid.v1\0");
    hasher.update(operation_uuid.0.as_bytes());
    hasher.update(kind.as_str().as_bytes());
    hasher.update(ordinal.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes[..6].copy_from_slice(&operation_uuid.0.as_bytes()[..6]);
    bytes[6..].copy_from_slice(&digest[..10]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Build the canonical node input schema from caller property columns.
///
/// Required fields are nullable `node_uuid: FixedSizeBinary(16)` (null means
/// deterministic generation) and non-null `label: Utf8`. Property columns are
/// sorted by name so schema identity is independent of map iteration.
pub fn bulk_node_input_schema(properties: Vec<Field>) -> Result<SchemaRef, BulkValidationError> {
    input_schema(BulkInputKind::Node, &NODE_REQUIRED, properties)
}

/// Build the canonical edge input schema from caller property columns.
///
/// Required fields are nullable `edge_uuid` (null means deterministic
/// generation), explicit non-null `source_uuid` and `target_uuid`, plus
/// non-null `rel_type: Utf8`.
pub fn bulk_edge_input_schema(properties: Vec<Field>) -> Result<SchemaRef, BulkValidationError> {
    input_schema(BulkInputKind::Edge, &EDGE_REQUIRED, properties)
}

fn canonical_import_chunk(
    kind: BulkInputKind,
    source: &RecordBatch,
    mut required_columns: Vec<ArrayRef>,
) -> Result<RecordBatch, BulkValidationError> {
    let required = match kind {
        BulkInputKind::Node => &NODE_REQUIRED[..],
        BulkInputKind::Edge => &EDGE_REQUIRED[..],
    };
    let property_fields = source.schema().fields()[required.len()..]
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut fields = match kind {
        BulkInputKind::Node => graphforge_storage::CONSTRUCTION_NODE_SCHEMA
            .fields()
            .to_vec(),
        BulkInputKind::Edge => graphforge_storage::CONSTRUCTION_EDGE_SCHEMA
            .fields()
            .to_vec(),
    };
    fields.extend(property_fields.into_iter().map(Arc::new));
    let schema = Arc::new(Schema::new(fields));
    required_columns.extend(source.columns()[required.len()..].iter().cloned());
    RecordBatch::try_new(schema, required_columns).map_err(|error| {
        contract_error(kind, BulkValidationReason::ProjectState, &error.to_string())
    })
}

fn canonical_import_uuid_array(
    kind: BulkInputKind,
    values: impl ExactSizeIterator<Item = Uuid>,
) -> Result<ArrayRef, BulkValidationError> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        builder.append_value(value.as_bytes()).map_err(|error| {
            contract_error(kind, BulkValidationReason::ProjectState, &error.to_string())
        })?;
    }
    Ok(Arc::new(builder.finish()))
}

impl GraphForge {
    pub(crate) fn normalize_import_nodes(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
    ) -> Result<ValidatedBulkNodes, BulkValidationError> {
        self.normalize_bulk_nodes(operation_uuid, batches, false)
    }

    pub(crate) fn normalize_import_node_chunk(
        &self,
        operation_uuid: OperationId,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, BulkValidationError> {
        let normalized =
            self.normalize_import_nodes(operation_uuid, std::slice::from_ref(batch))?;
        let identities = canonical_import_uuid_array(
            BulkInputKind::Node,
            normalized.rows().iter().map(|row| row.node_uuid),
        )?;
        let labels =
            StringArray::from_iter_values(normalized.rows().iter().map(|row| row.label.as_str()));
        canonical_import_chunk(
            BulkInputKind::Node,
            batch,
            vec![identities, Arc::new(labels)],
        )
    }

    /// Normalize and validate every Arrow node row without writing storage,
    /// the runtime catalog, ontology state, or project generations.
    pub fn validate_bulk_nodes(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
    ) -> Result<ValidatedBulkNodes, BulkValidationError> {
        let _visibility = self.graph_visibility.read().map_err(|error| {
            contract_error(
                BulkInputKind::Node,
                BulkValidationReason::ProjectState,
                &error.to_string(),
            )
        })?;
        self.normalize_bulk_nodes(operation_uuid, batches, true)
    }

    pub(super) fn normalize_bulk_nodes(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
        reject_existing: bool,
    ) -> Result<ValidatedBulkNodes, BulkValidationError> {
        let source_generation_uuid = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        validate_operation_uuid(BulkInputKind::Node, operation_uuid)?;
        validate_partition_schemas(BulkInputKind::Node, &NODE_REQUIRED, batches)?;
        let mut existing = HashSet::new();
        if reject_existing {
            let candidates = candidate_uuids(batches, BulkInputKind::Node, "node_uuid")?;
            let mut index = open_membership_index(self, BulkInputKind::Node)?;
            existing = indexed_existing(
                index.as_mut(),
                &candidates,
                graphforge_storage::UuidIndexKind::Node,
                BulkInputKind::Node,
            )?;
            existing.extend(indexed_existing(
                index.as_mut(),
                &candidates,
                graphforge_storage::UuidIndexKind::Edge,
                BulkInputKind::Node,
            )?);
        }
        let mut observed = HashSet::new();
        let mut rows = Vec::new();
        let mut ordinal = 0_u64;

        for batch in batches {
            let uuids = uuid_column(batch, BulkInputKind::Node, "node_uuid")?;
            let labels = string_column(batch, BulkInputKind::Node, "label")?;
            let properties = property_columns(batch, &NODE_REQUIRED);
            preflight_spatial_columns(BulkInputKind::Node, ordinal, &properties)?;
            for row in 0..batch.num_rows() {
                let node_uuid = uuid_at(
                    uuids,
                    row,
                    BulkInputKind::Node,
                    ordinal,
                    "node_uuid",
                    operation_uuid,
                )?;
                if existing.contains(&node_uuid) || !observed.insert(node_uuid) {
                    return Err(row_error(
                        BulkInputKind::Node,
                        BulkValidationReason::IdentityConflict,
                        ordinal,
                        "node_uuid",
                        "duplicate or existing UUID",
                    ));
                }
                if labels.is_null(row) {
                    return Err(row_error(
                        BulkInputKind::Node,
                        BulkValidationReason::SchemaMismatch,
                        ordinal,
                        "label",
                        "value is null",
                    ));
                }
                let label = labels.value(row);
                validate_identifier(BulkInputKind::Node, ordinal, "label", label)?;
                validate_node_owner(self, ordinal, label)?;
                let values = normalize_properties(
                    BulkInputKind::Node,
                    ordinal,
                    row,
                    &properties,
                    |name, field| validate_node_property(self, ordinal, label, name, field),
                )?;
                rows.push(BulkNodeRow {
                    row_ordinal: ordinal,
                    node_uuid,
                    label: label.to_owned(),
                    properties: values,
                });
                ordinal = ordinal.checked_add(1).ok_or_else(|| {
                    contract_error(
                        BulkInputKind::Node,
                        BulkValidationReason::OrdinalOverflow,
                        "logical row ordinal overflow",
                    )
                })?;
            }
        }
        Ok(ValidatedBulkNodes {
            rows,
            operation_uuid,
            source_generation_uuid,
        })
    }

    /// Normalize and validate every Arrow edge row without writing storage.
    /// Endpoints may reference existing nodes or nodes in `same_request_nodes`.
    pub fn validate_bulk_edges(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
        same_request_nodes: &ValidatedBulkNodes,
    ) -> Result<ValidatedBulkEdges, BulkValidationError> {
        let _visibility = self.graph_visibility.read().map_err(|error| {
            contract_error(
                BulkInputKind::Edge,
                BulkValidationReason::ProjectState,
                &error.to_string(),
            )
        })?;
        self.normalize_bulk_edges(operation_uuid, batches, same_request_nodes, true, None)
    }

    pub(super) fn normalize_bulk_edges(
        &self,
        operation_uuid: OperationId,
        batches: &[RecordBatch],
        same_request_nodes: &ValidatedBulkNodes,
        reject_existing: bool,
        additional_known_nodes: Option<&BTreeSet<Uuid>>,
    ) -> Result<ValidatedBulkEdges, BulkValidationError> {
        let source_generation_uuid = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        validate_operation_uuid(BulkInputKind::Edge, operation_uuid)?;
        if same_request_nodes.source_generation_uuid != source_generation_uuid {
            return Err(contract_error(
                BulkInputKind::Edge,
                BulkValidationReason::GenerationMismatch,
                "same-request nodes were validated against a different graph generation",
            ));
        }
        validate_partition_schemas(BulkInputKind::Edge, &EDGE_REQUIRED, batches)?;
        let endpoint_candidates = candidate_endpoint_uuids(batches)?;
        let edge_candidates = reject_existing
            .then(|| candidate_uuids(batches, BulkInputKind::Edge, "edge_uuid"))
            .transpose()?;
        let (mut known_nodes, existing_edges) =
            existing_edge_context(self, &endpoint_candidates, edge_candidates.as_deref())?;
        if let Some(additional) = additional_known_nodes {
            known_nodes.extend(additional.iter().copied());
        }
        known_nodes.extend(same_request_nodes.identities());
        let mut observed = HashSet::new();
        let mut rows = Vec::new();
        let mut ordinal = 0_u64;

        for batch in batches {
            let uuids = uuid_column(batch, BulkInputKind::Edge, "edge_uuid")?;
            let rel_types = string_column(batch, BulkInputKind::Edge, "rel_type")?;
            let sources = uuid_column(batch, BulkInputKind::Edge, "source_uuid")?;
            let targets = uuid_column(batch, BulkInputKind::Edge, "target_uuid")?;
            let properties = property_columns(batch, &EDGE_REQUIRED);
            preflight_spatial_columns(BulkInputKind::Edge, ordinal, &properties)?;
            for row in 0..batch.num_rows() {
                let edge_uuid = uuid_at(
                    uuids,
                    row,
                    BulkInputKind::Edge,
                    ordinal,
                    "edge_uuid",
                    operation_uuid,
                )?;
                validate_edge_identity(
                    edge_uuid,
                    &known_nodes,
                    &existing_edges,
                    &mut observed,
                    ordinal,
                )?;
                let source_uuid =
                    explicit_uuid_at(sources, row, BulkInputKind::Edge, ordinal, "source_uuid")?;
                validate_edge_endpoint(source_uuid, &known_nodes, ordinal, "source_uuid")?;
                let target_uuid =
                    explicit_uuid_at(targets, row, BulkInputKind::Edge, ordinal, "target_uuid")?;
                validate_edge_endpoint(target_uuid, &known_nodes, ordinal, "target_uuid")?;
                if rel_types.is_null(row) {
                    return Err(row_error(
                        BulkInputKind::Edge,
                        BulkValidationReason::SchemaMismatch,
                        ordinal,
                        "rel_type",
                        "value is null",
                    ));
                }
                let rel_type = rel_types.value(row);
                validate_identifier(BulkInputKind::Edge, ordinal, "rel_type", rel_type)?;
                validate_edge_owner(self, ordinal, rel_type)?;
                let values = normalize_properties(
                    BulkInputKind::Edge,
                    ordinal,
                    row,
                    &properties,
                    |name, field| validate_edge_property(self, ordinal, rel_type, name, field),
                )?;
                rows.push(BulkEdgeRow {
                    row_ordinal: ordinal,
                    edge_uuid,
                    rel_type: rel_type.to_owned(),
                    source_uuid,
                    target_uuid,
                    properties: values,
                });
                ordinal = ordinal.checked_add(1).ok_or_else(|| {
                    contract_error(
                        BulkInputKind::Edge,
                        BulkValidationReason::OrdinalOverflow,
                        "logical row ordinal overflow",
                    )
                })?;
            }
        }
        Ok(ValidatedBulkEdges {
            rows,
            operation_uuid,
            source_generation_uuid,
        })
    }

    pub(crate) fn normalize_import_edge_chunk(
        &self,
        operation_uuid: OperationId,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, BulkValidationError> {
        // Construction sealing owns the global endpoint proof across all node
        // chunks. This prevalidation still checks the complete edge schema,
        // identities, properties, and within-chunk duplicates without retaining
        // every imported node UUID in memory.
        let assumed_endpoints = ValidatedBulkNodes {
            rows: candidate_endpoint_uuids(std::slice::from_ref(batch))?
                .into_iter()
                .enumerate()
                .map(|(ordinal, node_uuid)| BulkNodeRow {
                    row_ordinal: ordinal as u64,
                    node_uuid,
                    label: String::new(),
                    properties: BTreeMap::new(),
                })
                .collect(),
            operation_uuid,
            source_generation_uuid: *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
        };
        let normalized = self.normalize_bulk_edges(
            operation_uuid,
            std::slice::from_ref(batch),
            &assumed_endpoints,
            true,
            None,
        )?;
        canonical_import_chunk(
            BulkInputKind::Edge,
            batch,
            vec![
                canonical_import_uuid_array(
                    BulkInputKind::Edge,
                    normalized.rows().iter().map(|row| row.edge_uuid),
                )?,
                Arc::new(StringArray::from_iter_values(
                    normalized.rows().iter().map(|row| row.rel_type.as_str()),
                )),
                canonical_import_uuid_array(
                    BulkInputKind::Edge,
                    normalized.rows().iter().map(|row| row.source_uuid),
                )?,
                canonical_import_uuid_array(
                    BulkInputKind::Edge,
                    normalized.rows().iter().map(|row| row.target_uuid),
                )?,
            ],
        )
    }
}

#[cfg(test)]
mod tests;
