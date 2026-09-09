//! IR and DataFusion scalar representation adapters.

#[cfg(doc)]
use crate::expr::lower_literal;
use crate::expr::{
    const_map_scalar, date_scalar, date_struct_value, datetime_scalar, datetime_struct_parts,
    decode_het_scalar, dur_secs_nanos, duration_scalar, duration_struct_parts,
    duration_value_to_ir, is_date_struct, is_datetime_struct, is_duration_struct,
    is_localdatetime_struct, is_time_struct, localdatetime_scalar, localdatetime_struct_parts,
    time_scalar, time_struct_parts,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;
use graphforge_core::LoweringError;
use graphforge_ir::expr::IrLiteral;
use std::sync::Arc;

/// Convert an [`IrLiteral`] to a DataFusion [`ScalarValue`].
///
/// The single source of truth for the IR-literal → Arrow-scalar mapping, used
/// both for literal expression lowering ([`lower_literal`]) and for binding
/// query parameters to placeholder values (`$param` injection, #584).
#[must_use]
pub fn ir_literal_to_scalar(lit_val: &IrLiteral) -> ScalarValue {
    match lit_val {
        IrLiteral::Null => ScalarValue::Null,
        IrLiteral::Bool(b) => ScalarValue::Boolean(Some(*b)),
        IrLiteral::Int(n) => ScalarValue::Int64(Some(*n)),
        IrLiteral::Float(f) => ScalarValue::Float64(Some(*f)),
        IrLiteral::Str(s) => ScalarValue::Utf8(Some(s.clone())),
        IrLiteral::Uuid(uuid) => ScalarValue::FixedSizeBinary(16, Some(uuid.to_vec())),
        IrLiteral::Duration {
            months,
            days,
            seconds,
            nanos,
        } => duration_scalar(Some(crate::temporal::DurationValue {
            months: *months,
            days: *days,
            seconds: *seconds,
            nanos: *nanos,
        })),
        IrLiteral::DateTime(us) => ScalarValue::TimestampMicrosecond(Some(*us), Some("UTC".into())),
        IrLiteral::Date(days) => date_scalar(Some(*days)),
        IrLiteral::LocalDateTime { days, nanos } => localdatetime_scalar(Some((*days, *nanos))),
        IrLiteral::Time(nanos) => ScalarValue::Time64Nanosecond(Some(*nanos)),
        IrLiteral::ZonedTime { nanos, offset } => time_scalar(Some((*nanos, *offset))),
        IrLiteral::ZonedDateTime {
            days,
            nanos,
            offset,
            zone,
        } => datetime_scalar(Some((*days, *nanos, *offset, zone.clone()))),
        IrLiteral::Spatial(value) => spatial_property_scalar(value),
        // A homogeneous list → a `ScalarValue::List` of the element scalars; the
        // inner type is the first element's (re-typing untyped nulls to it so the
        // array stays homogeneous, as the list-literal lowering does). (#1006)
        IrLiteral::List(items) => {
            let scalars: Vec<ScalarValue> = items.iter().map(ir_literal_to_scalar).collect();
            let elem_type = scalars
                .iter()
                .find(|s| !s.is_null())
                .map_or(DataType::Null, ScalarValue::data_type);
            let typed: Vec<ScalarValue> = scalars
                .iter()
                .map(|s| {
                    if matches!(s, ScalarValue::Null) {
                        ScalarValue::try_from(&elem_type).unwrap_or(ScalarValue::Null)
                    } else {
                        s.clone()
                    }
                })
                .collect();
            ScalarValue::List(ScalarValue::new_list(&typed, &elem_type, true))
        }
        IrLiteral::Map(entries) => {
            let scalars: Vec<(String, ScalarValue)> = entries
                .iter()
                .map(|(key, value)| (key.clone(), ir_literal_to_scalar(value)))
                .collect();
            const_map_scalar(&scalars).expect("IR map literal should lower to an Arrow struct")
        }
    }
}

pub(in crate::expr) fn spatial_scalar(value: &graphforge_core::SpatialValue) -> ScalarValue {
    use datafusion::arrow::array::{Float64Array, StructArray};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{Field, Fields};
    use graphforge_core::SpatialCoordinates;

    fn coordinate(value: [f64; 2]) -> ScalarValue {
        let fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);
        ScalarValue::Struct(Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(Float64Array::from(vec![value[0]])),
                Arc::new(Float64Array::from(vec![value[1]])),
            ],
            Some(NullBuffer::from(vec![true])),
        )))
    }

    fn list(values: &[ScalarValue]) -> ScalarValue {
        let data_type = values
            .first()
            .map_or(DataType::Null, ScalarValue::data_type);
        ScalarValue::List(ScalarValue::new_list(values, &data_type, false))
    }

    fn coordinates(values: &[[f64; 2]]) -> ScalarValue {
        list(&values.iter().copied().map(coordinate).collect::<Vec<_>>())
    }

    match &value.coordinates {
        SpatialCoordinates::Point(value) => coordinate(*value),
        SpatialCoordinates::LineString(values) | SpatialCoordinates::MultiPoint(values) => {
            coordinates(values)
        }
        SpatialCoordinates::Polygon(parts) | SpatialCoordinates::MultiLineString(parts) => list(
            &parts
                .iter()
                .map(|part| coordinates(part))
                .collect::<Vec<_>>(),
        ),
        SpatialCoordinates::MultiPolygon(polygons) => list(
            &polygons
                .iter()
                .map(|polygon| {
                    list(
                        &polygon
                            .iter()
                            .map(|ring| coordinates(ring))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
        ),
    }
}

pub(in crate::expr) fn spatial_property_scalar(
    value: &graphforge_core::SpatialValue,
) -> ScalarValue {
    use datafusion::arrow::array::{StringArray, StructArray};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{Field, Fields};
    let fields = Fields::from(vec![Field::new(
        "__graphforge_spatial_v1",
        DataType::Utf8,
        false,
    )]);
    ScalarValue::Struct(Arc::new(StructArray::new(
        fields,
        vec![Arc::new(StringArray::from(vec![
            serde_json::to_string(value).expect("spatial value serialization is infallible"),
        ]))],
        Some(NullBuffer::from(vec![true])),
    )))
}

/// Convert a DataFusion [`ScalarValue`] back to an [`IrLiteral`] for storage as
/// a property value (#791 SET).
///
/// The inverse of [`ir_literal_to_scalar`], used by the SET execution node:
/// after evaluating a value expression per row it gets a `ScalarValue` that must
/// be stored as an `IrLiteral`. Numeric and temporal widths the writer does not
/// have a dedicated literal for are normalised to the nearest `IrLiteral`
/// (smaller ints → `Int`, `Float32` → `Float`, dates/times → `DateTime`). A
/// `null` scalar (typed or untyped) → [`IrLiteral::Null`].
///
/// Lists and temporal structs are converted recursively. Maps, graph values,
/// and other non-property shapes return an invalid-property-type error.
///
/// # Errors
/// Returns [`LoweringError::InvalidType`] for a value that openCypher does not
/// permit as a stored property.
#[allow(
    clippy::too_many_lines,
    reason = "closed exhaustive property scalar conversion table"
)]
pub fn scalar_to_ir_literal(value: &ScalarValue) -> Result<IrLiteral, LoweringError> {
    if let Some(decoded) =
        decode_het_scalar(value).map_err(|error| LoweringError::InvalidType(error.to_string()))?
    {
        return scalar_to_ir_literal(&decoded);
    }
    // Any null (typed `Int64(None)` or untyped `Null`) stores as Cypher null.
    if value.is_null() {
        return Ok(IrLiteral::Null);
    }
    let lit = match value {
        ScalarValue::Boolean(Some(b)) => IrLiteral::Bool(*b),
        ScalarValue::Int8(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::Int16(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::Int32(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::Int64(Some(n)) => IrLiteral::Int(*n),
        ScalarValue::UInt8(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::UInt16(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::UInt32(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::UInt64(Some(n)) => i64::try_from(*n).map(IrLiteral::Int).map_err(|_| {
            LoweringError::UnsupportedExpr(format!("SET value {n} exceeds the i64 range"))
        })?,
        ScalarValue::Float32(Some(f)) => IrLiteral::Float(f64::from(*f)),
        ScalarValue::Float64(Some(f)) => IrLiteral::Float(*f),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => IrLiteral::Str(s.clone()),
        ScalarValue::FixedSizeBinary(16, Some(_)) => {
            return Err(LoweringError::InvalidType(
                "UUID values cannot be stored as graph properties".into(),
            ));
        }
        // Flat native-Arrow duration widths carry no month/day part. Split each
        // unit into whole seconds + non-negative nanos-of-second WITHOUT forming a
        // `*1e9` total (which would overflow i64 for large native durations — the
        // seconds field stores them directly). (#1011)
        ScalarValue::DurationSecond(Some(s)) => duration_value_to_ir(dur_secs_nanos(*s, 0)),
        ScalarValue::DurationMillisecond(Some(ms)) => duration_value_to_ir(dur_secs_nanos(
            ms.div_euclid(1_000),
            ms.rem_euclid(1_000) * 1_000_000,
        )),
        ScalarValue::DurationMicrosecond(Some(us)) => duration_value_to_ir(dur_secs_nanos(
            us.div_euclid(1_000_000),
            us.rem_euclid(1_000_000) * 1_000,
        )),
        ScalarValue::DurationNanosecond(Some(ns)) => {
            duration_value_to_ir(crate::temporal::DurationValue::from_total_nanos(0, 0, *ns))
        }
        ScalarValue::TimestampMicrosecond(Some(us), _) => IrLiteral::DateTime(*us),
        ScalarValue::TimestampSecond(Some(s), _) => IrLiteral::DateTime(s * 1_000_000),
        ScalarValue::TimestampMillisecond(Some(ms), _) => IrLiteral::DateTime(ms * 1_000),
        ScalarValue::TimestampNanosecond(Some(ns), _) => IrLiteral::DateTime(ns / 1_000),
        // A date keeps its date identity (ADR 0009/0012): a `Struct{epoch_day}` of
        // i64 days, not coerced to a `DateTime` — so it reads back and renders as a
        // date.
        ScalarValue::Struct(arr)
            if arr.fields().len() == 1 && arr.fields()[0].name() == "__graphforge_spatial_v1" =>
        {
            let json = arr
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or_else(|| {
                    LoweringError::InvalidType("malformed internal spatial value".into())
                })?
                .value(0);
            IrLiteral::Spatial(serde_json::from_str(json).map_err(|_| {
                LoweringError::InvalidType("malformed internal spatial value".into())
            })?)
        }
        ScalarValue::Struct(arr) if is_date_struct(&DataType::Struct(arr.fields().clone())) => {
            match date_struct_value(arr, 0) {
                Some(days) => IrLiteral::Date(days),
                None => IrLiteral::Null,
            }
        }
        // A typed `duration` struct (#920) keeps its months/days/seconds/nanos model.
        ScalarValue::Struct(arr) if is_duration_struct(&DataType::Struct(arr.fields().clone())) => {
            match duration_struct_parts(arr, 0) {
                Some(d) => duration_value_to_ir(d),
                None => IrLiteral::Null,
            }
        }
        // A typed `localdatetime` struct (#920): date-days + nanos-of-day.
        ScalarValue::Struct(arr)
            if is_localdatetime_struct(&DataType::Struct(arr.fields().clone())) =>
        {
            match localdatetime_struct_parts(arr, 0) {
                Some((days, nanos)) => IrLiteral::LocalDateTime { days, nanos },
                None => IrLiteral::Null,
            }
        }
        // A typed `localtime` (#920): nanoseconds-of-day, no zone.
        ScalarValue::Time64Nanosecond(Some(n)) => IrLiteral::Time(*n),
        // A typed `time` struct (#920): time-of-day + UTC offset.
        ScalarValue::Struct(arr) if is_time_struct(&DataType::Struct(arr.fields().clone())) => {
            match time_struct_parts(arr, 0) {
                Some((nanos, offset)) => IrLiteral::ZonedTime { nanos, offset },
                None => IrLiteral::Null,
            }
        }
        // A typed `datetime` struct (#920): date + time + offset + named zone.
        ScalarValue::Struct(arr) if is_datetime_struct(&DataType::Struct(arr.fields().clone())) => {
            match datetime_struct_parts(arr, 0) {
                Some((days, nanos, offset, zone)) => IrLiteral::ZonedDateTime {
                    days,
                    nanos,
                    offset,
                    zone,
                },
                None => IrLiteral::Null,
            }
        }
        // A homogeneous list (#1006): recurse element-wise (a null element →
        // `IrLiteral::Null`; an element type the gate rejects propagates its
        // error). Single-row `List`/`LargeList` scalars carry the elements at
        // row 0.
        ScalarValue::List(arr) => list_scalar_to_ir_literal(&arr.value(0))?,
        ScalarValue::LargeList(arr) => list_scalar_to_ir_literal(&arr.value(0))?,
        other => {
            // Plain maps, graph structs, and any width not handled above are
            // invalid openCypher property values.
            return Err(LoweringError::InvalidType(format!(
                "invalid property type for SET value: {:?}",
                other.data_type()
            )));
        }
    };
    Ok(lit)
}

/// Build an [`IrLiteral::List`] from a list scalar's element array, recursing
/// through [`scalar_to_ir_literal`] per element. (#1006)
pub(in crate::expr) fn list_scalar_to_ir_literal(
    elems: &datafusion::arrow::array::ArrayRef,
) -> Result<IrLiteral, LoweringError> {
    let mut items = Vec::with_capacity(elems.len());
    for j in 0..elems.len() {
        let ev = ScalarValue::try_from_array(elems, j).map_err(|e| {
            LoweringError::UnsupportedExpr(format!("list element is not a scalar value: {e}"))
        })?;
        items.push(scalar_to_ir_literal(&ev)?);
    }
    Ok(IrLiteral::List(items))
}

/// Canonicalise a literal-string temporal-constructor argument. Returns `None`
/// when the constructor doesn't take a string, or the string isn't a form the
/// `temporal` module recognises (the caller then falls back to the runtime
/// path). (#599)
pub(in crate::expr) fn render_temporal(name: &str, s: &str) -> Option<String> {
    use crate::temporal;
    match name {
        "date" => temporal::render_date(s),
        "localtime" => temporal::render_local_time(s),
        "time" => temporal::render_time(s),
        "localdatetime" => temporal::render_local_date_time(s),
        "datetime" => temporal::render_date_time(s),
        "duration" => temporal::render_duration(s),
        _ => None,
    }
}
