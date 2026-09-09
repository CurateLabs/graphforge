//! Arrow representations and DataFusion adapters for Cypher temporal values.

mod accessors;
mod duration;
mod project;
mod truncate;

#[cfg(test)]
mod tests;

pub(super) use accessors::{
    CYPHER_DATE_COMPONENT, CYPHER_DURATION_COMPONENT, CYPHER_TEMPORAL_COMPONENT,
    CYPHER_TEMPORAL_ZONE_STR, temporal_accessor_valid,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::scalar::ScalarValue;
pub(super) use duration::{
    CYPHER_DURATION_ADD, CYPHER_DURATION_BETWEEN, CYPHER_DURATION_PARSE, CYPHER_DURATION_SCALE,
    CYPHER_TEMPORAL_ARITH,
};
#[cfg(test)]
use duration::{
    CypherDurationAdd, CypherDurationBetween, CypherDurationParse, CypherDurationScale,
    CypherTemporalArith,
};
use graphforge_ir::expr::IrLiteral;
pub(super) use project::{
    CYPHER_DATE_PROJECT, CYPHER_DATETIME_PROJECT, CYPHER_LOCALDATETIME_PROJECT,
    CYPHER_LOCALTIME_PROJECT, CYPHER_TIME_PROJECT,
};
#[cfg(test)]
use project::{
    CypherDateProject, CypherDateTimeProject, CypherLocalDateTimeProject, CypherLocalTimeProject,
    CypherTimeProject,
};
pub(super) use truncate::{
    CYPHER_DATE_TRUNCATE, CYPHER_DATETIME_TRUNCATE, CYPHER_LOCALDATETIME_TRUNCATE,
    CYPHER_LOCALTIME_TRUNCATE, CYPHER_TIME_TRUNCATE,
};
#[cfg(test)]
use truncate::{
    CypherDateTimeTruncate, CypherDateTruncate, CypherLocalDateTimeTruncate,
    CypherLocalTimeTruncate, CypherTimeTruncate,
};

fn udf_argument_arrays(
    args: &ScalarFunctionArgs,
) -> datafusion::error::Result<Vec<datafusion::arrow::array::ArrayRef>> {
    args.args
        .iter()
        .map(|value| value.to_array(args.number_rows))
        .collect()
}

fn cast_argument_arrays(
    arrays: &[datafusion::arrow::array::ArrayRef],
    data_type: &DataType,
) -> datafusion::error::Result<Vec<datafusion::arrow::array::ArrayRef>> {
    arrays
        .iter()
        .map(|array| datafusion::arrow::compute::cast(array, data_type).map_err(Into::into))
        .collect()
}

/// The Arrow fields of a standalone `date` value — `Struct{epoch_day: Int64}`
/// (ADR 0012). A one-field struct (not a bare `Int64`) so a `date` is
/// self-describing on storage decode — a plain integer property would be
/// indistinguishable — while spanning the full openCypher year range (#1011).
fn date_fields() -> datafusion::arrow::datatypes::Fields {
    graphforge_ir::arrow_schema::date_struct_fields()
}

/// True if `dt` is the standalone `date` struct type (`Struct{epoch_day: Int64}`),
/// distinguished from the other temporal structs by its single field name.
pub(super) fn is_date_struct(dt: &DataType) -> bool {
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 1
            && fields[0].name() == "epoch_day"
            && *fields[0].data_type() == DataType::Int64)
}

/// Build a standalone `date` struct array from per-row i64 epoch-days (`None` ⇒ a
/// null row).
fn build_date_struct(rows: &[Option<i64>]) -> datafusion::arrow::array::StructArray {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::buffer::NullBuffer;
    let days: Int64Array = rows.iter().copied().collect();
    let nulls = rows.iter().map(Option::is_some).collect::<NullBuffer>();
    datafusion::arrow::array::StructArray::new(
        date_fields(),
        vec![std::sync::Arc::new(days)],
        Some(nulls),
    )
}

/// A standalone `date` scalar (`None` ⇒ a null value).
pub(super) fn date_scalar(days: Option<i64>) -> ScalarValue {
    ScalarValue::Struct(std::sync::Arc::new(build_date_struct(&[days])))
}

/// The i64 epoch-day of a standalone `date` struct row (`None` for a null row).
pub(super) fn date_struct_value(
    arr: &datafusion::arrow::array::StructArray,
    i: usize,
) -> Option<i64> {
    use datafusion::arrow::array::{Array, Int64Array};
    if arr.is_null(i) {
        return None;
    }
    let days = arr.column(0).as_any().downcast_ref::<Int64Array>()?;
    days.is_valid(i).then(|| days.value(i))
}

/// Read one nullable Int64 override from an already-normalized Arrow column.
/// Temporal projectors cast override columns once before their row loop, so a
/// failed physical downcast or a null row has the same absent-override meaning.
fn optional_i64_at(array: &datafusion::arrow::array::ArrayRef, row: usize) -> Option<i64> {
    use datafusion::arrow::array::{Array, Int64Array};

    let values = array.as_any().downcast_ref::<Int64Array>()?;
    (!values.is_null(row)).then(|| values.value(row))
}

/// The Arrow fields of a `localdatetime` value — `Struct{date: Int64, time:
/// Time64(Nanosecond)}`. `date` is first so DataFusion's row-format sort orders
/// chronologically (date, then time-of-day); `date` is i64 days (#1011).
fn localdatetime_fields() -> datafusion::arrow::datatypes::Fields {
    graphforge_ir::arrow_schema::localdatetime_struct_fields()
}

/// True if `dt` is the `localdatetime` struct type (used to dispatch base
/// extraction and rendering without colliding with user maps, whose `date`/
/// `time` fields would not have these exact `Int64`/`Time64` types).
pub(super) fn is_localdatetime_struct(dt: &DataType) -> bool {
    use datafusion::arrow::datatypes::TimeUnit;
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 2
            && fields[0].name() == "date"
            && *fields[0].data_type() == DataType::Int64
            && fields[1].name() == "time"
            && *fields[1].data_type() == DataType::Time64(TimeUnit::Nanosecond))
}

/// Build a `localdatetime` struct array from per-row `(date_days, nanos_of_day)`
/// (`None` ⇒ a null row).
fn build_localdatetime_struct(
    rows: &[Option<(i64, i64)>],
) -> datafusion::arrow::array::StructArray {
    use datafusion::arrow::array::{Int64Array, Time64NanosecondArray};
    use datafusion::arrow::buffer::NullBuffer;
    let days: Int64Array = rows.iter().map(|r| r.map(|(d, _)| d)).collect();
    let nanos: Time64NanosecondArray = rows.iter().map(|r| r.map(|(_, n)| n)).collect();
    let nulls = rows.iter().map(Option::is_some).collect::<NullBuffer>();
    datafusion::arrow::array::StructArray::new(
        localdatetime_fields(),
        vec![std::sync::Arc::new(days), std::sync::Arc::new(nanos)],
        Some(nulls),
    )
}

/// A `localdatetime` scalar (`None` ⇒ a null value).
pub(super) fn localdatetime_scalar(parts: Option<(i64, i64)>) -> ScalarValue {
    ScalarValue::Struct(std::sync::Arc::new(build_localdatetime_struct(&[parts])))
}

/// True if `dt` is the typed `duration` struct (`Struct{months, days, seconds, nanos}`,
/// ADR 0009) — distinguished from the other temporal structs by its field names.
pub(super) fn is_duration_struct(dt: &DataType) -> bool {
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 4
            && fields[0].name() == "months"
            && fields[1].name() == "days"
            && fields[2].name() == "seconds"
            && fields[3].name() == "nanos")
}

/// Whether a function name is a temporal clock accessor —
/// `<type>.transaction` / `.statement` / `.realtime` for an instant type
/// (`date`/`localtime`/`time`/`localdatetime`/`datetime`). (#920)
pub(super) fn is_temporal_clock_fn(name: &str) -> bool {
    // Cypher function names are case-insensitive (`Date.Realtime` ≡ `date.realtime`).
    matches!(
        name.to_ascii_lowercase().split_once('.'),
        Some((
            "date" | "localtime" | "time" | "localdatetime" | "datetime",
            "transaction" | "statement" | "realtime",
        ))
    )
}

/// The typed null for a temporal constructor / clock function — `date` →
/// `Date32(None)`, `localtime` → `Time64(None)`, etc. — so null propagation
/// preserves the Arrow temporal type rather than erasing it to `Null` (which
/// would defeat downstream `is_temporal_typed` checks). The base name is the
/// part before any `.clock` suffix, matched case-insensitively. (#920)
pub(super) fn temporal_null_scalar(name: &str) -> ScalarValue {
    let lower = name.to_ascii_lowercase();
    let base = lower.split('.').next().unwrap_or(&lower);
    match base {
        "date" => date_scalar(None),
        "localtime" => ScalarValue::Time64Nanosecond(None),
        "time" => time_scalar(None),
        "localdatetime" => localdatetime_scalar(None),
        "datetime" => datetime_scalar(None),
        "duration" => duration_scalar(None),
        _ => ScalarValue::Null,
    }
}

/// Build a `duration` struct array from per-row [`DurationValue`]s (`None` ⇒ a
/// null row). The on-disk + query representation of a Cypher duration —
/// `Struct{months,days,seconds,nanos}` all Int64 (Parquet cannot persist Arrow
/// `Interval(MonthDayNano)`). (#920/#1011)
fn build_duration_struct(
    rows: &[Option<crate::temporal::DurationValue>],
) -> datafusion::arrow::array::StructArray {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::buffer::NullBuffer;
    let months: Int64Array = rows.iter().map(|r| r.map(|d| d.months)).collect();
    let days: Int64Array = rows.iter().map(|r| r.map(|d| d.days)).collect();
    let seconds: Int64Array = rows.iter().map(|r| r.map(|d| d.seconds)).collect();
    let nanos: Int64Array = rows.iter().map(|r| r.map(|d| d.nanos)).collect();
    let nulls = rows.iter().map(Option::is_some).collect::<NullBuffer>();
    datafusion::arrow::array::StructArray::new(
        graphforge_ir::arrow_schema::duration_struct_fields(),
        vec![
            std::sync::Arc::new(months),
            std::sync::Arc::new(days),
            std::sync::Arc::new(seconds),
            std::sync::Arc::new(nanos),
        ],
        Some(nulls),
    )
}

/// Extract a [`DurationValue`] from a `duration` struct array at row `i`
/// (`None` for a null row). (#920/#1011)
pub(super) fn duration_struct_parts(
    arr: &datafusion::arrow::array::StructArray,
    i: usize,
) -> Option<crate::temporal::DurationValue> {
    use datafusion::arrow::array::{Array, Int64Array};
    if arr.is_null(i) {
        return None;
    }
    let col = |idx: usize| arr.column(idx).as_any().downcast_ref::<Int64Array>();
    Some(crate::temporal::DurationValue {
        months: col(0)?.value(i),
        days: col(1)?.value(i),
        seconds: col(2)?.value(i),
        nanos: col(3)?.value(i),
    })
}

/// Build a typed `duration` scalar from a [`DurationValue`] (`None` ⇒ null). (#920)
pub(super) fn duration_scalar(parts: Option<crate::temporal::DurationValue>) -> ScalarValue {
    ScalarValue::Struct(std::sync::Arc::new(build_duration_struct(&[parts])))
}

/// A sub-day-only [`DurationValue`] from whole `seconds` + non-negative
/// `nanos`-of-second (no month/day part) — for the native-Arrow duration arms. (#1011)
pub(super) fn dur_secs_nanos(seconds: i64, nanos: i64) -> crate::temporal::DurationValue {
    crate::temporal::DurationValue {
        months: 0,
        days: 0,
        seconds,
        nanos,
    }
}

/// Convert a [`DurationValue`] to an `IrLiteral::Duration` (storage form). (#1011)
pub(super) fn duration_value_to_ir(d: crate::temporal::DurationValue) -> IrLiteral {
    IrLiteral::Duration {
        months: d.months,
        days: d.days,
        seconds: d.seconds,
        nanos: d.nanos,
    }
}

/// Extract `(date_days, nanos_of_day)` from a `localdatetime` struct array at
/// row `i` (`None` for a null row). Also used to read the local date+time of a
/// `datetime` struct (whose leading two fields are the same `Int64`+`Time64`),
/// dropping its zone — the correct semantics for `date`/`localtime`/
/// `localdatetime` projections from a `datetime`.
pub(super) fn localdatetime_struct_parts(
    arr: &datafusion::arrow::array::StructArray,
    i: usize,
) -> Option<(i64, i64)> {
    use datafusion::arrow::array::{Array, Int64Array, Time64NanosecondArray};
    if arr.is_null(i) {
        return None;
    }
    let d = arr.column(0).as_any().downcast_ref::<Int64Array>()?;
    let t = arr
        .column(1)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()?;
    (!d.is_null(i) && !t.is_null(i)).then(|| (d.value(i), t.value(i)))
}

/// The Arrow fields of a `time` value — `Struct{time: Time64(Nanosecond),
/// offset: Int32}` (nanoseconds-of-day + zone offset in seconds).
fn time_fields() -> datafusion::arrow::datatypes::Fields {
    graphforge_ir::arrow_schema::time_struct_fields()
}

/// True if `dt` is the `time` struct type (dispatches base extraction/rendering
/// without colliding with the `localdatetime` struct or user maps).
pub(super) fn is_time_struct(dt: &DataType) -> bool {
    use datafusion::arrow::datatypes::TimeUnit;
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 2
            && fields[0].name() == "time"
            && *fields[0].data_type() == DataType::Time64(TimeUnit::Nanosecond)
            && fields[1].name() == "offset"
            && *fields[1].data_type() == DataType::Int32)
}

/// Build a `time` struct array from per-row `(nanos_of_day, offset_seconds)`
/// (`None` ⇒ a null row).
fn build_time_struct(rows: &[Option<(i64, i32)>]) -> datafusion::arrow::array::StructArray {
    use datafusion::arrow::array::{Int32Array, Time64NanosecondArray};
    use datafusion::arrow::buffer::NullBuffer;
    let nanos: Time64NanosecondArray = rows.iter().map(|r| r.map(|(n, _)| n)).collect();
    let offset: Int32Array = rows.iter().map(|r| r.map(|(_, o)| o)).collect();
    let nulls = rows.iter().map(Option::is_some).collect::<NullBuffer>();
    datafusion::arrow::array::StructArray::new(
        time_fields(),
        vec![std::sync::Arc::new(nanos), std::sync::Arc::new(offset)],
        Some(nulls),
    )
}

/// A `time` scalar (`None` ⇒ a null value).
pub(super) fn time_scalar(parts: Option<(i64, i32)>) -> ScalarValue {
    ScalarValue::Struct(std::sync::Arc::new(build_time_struct(&[parts])))
}

/// Extract `(nanos_of_day, offset_seconds)` from a `time` struct array at row
/// `i` (`None` for a null row).
pub(super) fn time_struct_parts(
    arr: &datafusion::arrow::array::StructArray,
    i: usize,
) -> Option<(i64, i32)> {
    use datafusion::arrow::array::{Array, Int32Array, Time64NanosecondArray};
    if arr.is_null(i) {
        return None;
    }
    let t = arr
        .column(0)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()?;
    let o = arr.column(1).as_any().downcast_ref::<Int32Array>()?;
    (!t.is_null(i) && !o.is_null(i)).then(|| (t.value(i), o.value(i)))
}

/// One `datetime` row: `(date_days, nanos_of_day, offset_seconds, zone_label)`,
/// or `None` for a null value.
type DateTimeRow = Option<(i64, i64, i32, Option<String>)>;

/// The Arrow fields of a `datetime` value — `Struct{date: Int64, time:
/// Time64(Nanosecond), offset: Int32, zone: Utf8}` (the date = i64 days,
/// time-of-day, resolved zone offset in seconds, and an optional named-IANA-zone
/// label). (#1011)
fn datetime_fields() -> datafusion::arrow::datatypes::Fields {
    graphforge_ir::arrow_schema::datetime_struct_fields()
}

/// True if `dt` is the `datetime` struct type.
pub(super) fn is_datetime_struct(dt: &DataType) -> bool {
    use datafusion::arrow::datatypes::TimeUnit;
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 4
            && fields[0].name() == "date" && *fields[0].data_type() == DataType::Int64
            && fields[1].name() == "time"
            && *fields[1].data_type() == DataType::Time64(TimeUnit::Nanosecond)
            && fields[2].name() == "offset" && *fields[2].data_type() == DataType::Int32
            && fields[3].name() == "zone" && *fields[3].data_type() == DataType::Utf8)
}

/// Build a `datetime` struct array from per-row `(date_days, nanos_of_day,
/// offset_seconds, zone_label)` (`None` ⇒ a null row).
fn build_datetime_struct(rows: &[DateTimeRow]) -> datafusion::arrow::array::StructArray {
    use datafusion::arrow::array::{Int32Array, Int64Array, StringArray, Time64NanosecondArray};
    use datafusion::arrow::buffer::NullBuffer;
    let days: Int64Array = rows.iter().map(|r| r.as_ref().map(|t| t.0)).collect();
    let nanos: Time64NanosecondArray = rows.iter().map(|r| r.as_ref().map(|t| t.1)).collect();
    let offset: Int32Array = rows.iter().map(|r| r.as_ref().map(|t| t.2)).collect();
    // The zone field is empty (NOT null) when there is no named zone, so two
    // offset-only datetimes compare equal — `cypher_struct_eq` propagates null,
    // and a null=null field would make the whole equality null (Temporal7 [5]).
    let zone: StringArray = rows
        .iter()
        .map(|r| r.as_ref().map(|t| t.3.clone().unwrap_or_default()))
        .collect();
    let nulls = rows.iter().map(Option::is_some).collect::<NullBuffer>();
    datafusion::arrow::array::StructArray::new(
        datetime_fields(),
        vec![
            std::sync::Arc::new(days),
            std::sync::Arc::new(nanos),
            std::sync::Arc::new(offset),
            std::sync::Arc::new(zone),
        ],
        Some(nulls),
    )
}

/// A `datetime` scalar (`None` ⇒ a null value).
pub(super) fn datetime_scalar(parts: DateTimeRow) -> ScalarValue {
    ScalarValue::Struct(std::sync::Arc::new(build_datetime_struct(&[parts])))
}

/// Extract `(date_days, nanos_of_day, offset_seconds, zone_label)` from a
/// `datetime` struct array at row `i` (`None` for a null row).
pub(super) fn datetime_struct_parts(
    arr: &datafusion::arrow::array::StructArray,
    i: usize,
) -> DateTimeRow {
    use datafusion::arrow::array::{
        Array, Int32Array, Int64Array, StringArray, Time64NanosecondArray,
    };
    if arr.is_null(i) {
        return None;
    }
    let days = arr.column(0).as_any().downcast_ref::<Int64Array>()?;
    let nanos = arr
        .column(1)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()?;
    let offset = arr.column(2).as_any().downcast_ref::<Int32Array>()?;
    let zone = arr.column(3).as_any().downcast_ref::<StringArray>()?;
    if days.is_null(i) || nanos.is_null(i) || offset.is_null(i) {
        return None;
    }
    // An empty zone label means "no named zone" (offset-only datetime).
    let zone_label =
        (!zone.is_null(i) && !zone.value(i).is_empty()).then(|| zone.value(i).to_string());
    Some((days.value(i), nanos.value(i), offset.value(i), zone_label))
}
