//! Cypher temporal truncate adapters.

use super::{
    DateTimeRow, build_date_struct, build_datetime_struct, build_localdatetime_struct,
    build_time_struct, cast_argument_arrays, date_struct_value, datetime_fields,
    datetime_struct_parts, is_date_struct, is_datetime_struct, is_localdatetime_struct,
    is_time_struct, localdatetime_fields, localdatetime_struct_parts, optional_i64_at, time_fields,
    time_struct_parts, udf_argument_arrays,
};
use crate::expr::validate_heterogeneous_arguments;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::ScalarUDFImpl;
use datafusion::logical_expr::Signature;
use datafusion::logical_expr::Volatility;
use std::sync::LazyLock;

/// `localtime.truncate(unit, value, map)` (`Temporal9`): truncate `value`'s
/// time-of-day to `unit`, then apply the override `map`. Args are `[value, unit,
/// hour, minute, second, millisecond, microsecond, nanosecond]` — `value` is a
/// `Time64(ns)` / `localdatetime`/`time`/`datetime` struct / ISO string, `unit` a
/// string, the six overrides nullable integers. Returns `Time64(ns)`. (#920)
pub(in crate::expr) static CYPHER_LOCALTIME_TRUNCATE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherLocalTimeTruncate::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherLocalTimeTruncate {
    signature: Signature,
}

impl CypherLocalTimeTruncate {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(8, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherLocalTimeTruncate {
    fn name(&self) -> &'static str {
        "cypher_localtime_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Time64(
            datafusion::arrow::datatypes::TimeUnit::Nanosecond,
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            LocalTimeOverrides, project_localtime, time_of_day_nanos_any, truncate_time_nanos,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let base: ArrayRef =
            if matches!(cols[0].data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_localdatetime_struct(cols[0].data_type())
                || is_time_struct(cols[0].data_type())
                || is_datetime_struct(cols[0].data_type())
            {
                std::sync::Arc::clone(&cols[0])
            } else {
                cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
            };
        let units_arr = cast(&cols[1], &DataType::Utf8).map_err(DataFusionError::from)?;
        let units = units_arr.as_any().downcast_ref::<StringArray>();
        let ov = cast_argument_arrays(&cols[2..8], &DataType::Int64)?;

        let base_nanos = |i: usize| -> Option<i64> {
            if base.is_null(i) {
                return None;
            }
            match base.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some(
                    base.as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                ),
                DataType::Struct(_) => {
                    let s = base.as_any().downcast_ref::<StructArray>()?;
                    if is_time_struct(base.data_type()) {
                        Some(time_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.1)
                    }
                }
                DataType::Utf8 => {
                    time_of_day_nanos_any(base.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let out: Time64NanosecondArray = (0..rows)
            .map(|i| {
                let u = units?;
                if u.is_null(i) {
                    return None;
                }
                let truncated = truncate_time_nanos(base_nanos(i)?, u.value(i))?;
                let overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[0], i),
                    minute: optional_i64_at(&ov[1], i),
                    second: optional_i64_at(&ov[2], i),
                    millisecond: optional_i64_at(&ov[3], i),
                    microsecond: optional_i64_at(&ov[4], i),
                    nanosecond: optional_i64_at(&ov[5], i),
                };
                project_localtime(truncated, &overrides)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

/// `localdatetime.truncate(unit, value, map)` (`Temporal9`): truncate `value` to
/// `unit` — for `day`-and-coarser units the date is truncated and the time zeroed
/// to midnight; for finer units (`hour`…`microsecond`) the date is kept and the
/// time-of-day floored — then the override `map` is applied. Args are `[value,
/// unit, year, month, day, week, dayOfWeek, ordinalDay, quarter, dayOfQuarter,
/// hour, minute, second, millisecond, microsecond, nanosecond]`. Returns the
/// `localdatetime` struct. (#920)
pub(in crate::expr) static CYPHER_LOCALDATETIME_TRUNCATE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherLocalDateTimeTruncate::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherLocalDateTimeTruncate {
    signature: Signature,
}

impl CypherLocalDateTimeTruncate {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(16, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherLocalDateTimeTruncate {
    fn name(&self) -> &'static str {
        "cypher_localdatetime_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(localdatetime_fields()))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive per-row truncation: typed source extraction, the \
                  date/time granularity split, and 14 component overrides"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            DateOverrides, LocalTimeOverrides, parse_date_or_datetime_prefix, project_date,
            project_localtime, time_of_day_nanos_any, truncate_date, truncate_time_nanos,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // One `value` source feeds both the date and time components.
        let value: ArrayRef =
            if matches!(cols[0].data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_date_struct(cols[0].data_type())
                || is_localdatetime_struct(cols[0].data_type())
                || is_time_struct(cols[0].data_type())
                || is_datetime_struct(cols[0].data_type())
            {
                std::sync::Arc::clone(&cols[0])
            } else {
                cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
            };
        let units_arr = cast(&cols[1], &DataType::Utf8).map_err(DataFusionError::from)?;
        let units = units_arr.as_any().downcast_ref::<StringArray>();
        let ov = cast_argument_arrays(&cols[2..16], &DataType::Int64)?;

        let base_date = |i: usize| -> Option<i64> {
            if value.is_null(i) {
                return None;
            }
            match value.data_type() {
                DataType::Struct(_) => {
                    let s = value.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(value.data_type()) {
                        date_struct_value(s, i)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    value.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };
        let base_time = |i: usize| -> Option<i64> {
            if value.is_null(i) {
                return None;
            }
            match value.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some(
                    value
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                ),
                DataType::Struct(_) => {
                    let s = value.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(value.data_type()) {
                        Some(0) // date-only → midnight
                    } else if is_time_struct(value.data_type()) {
                        Some(time_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.1)
                    }
                }
                DataType::Utf8 => {
                    time_of_day_nanos_any(value.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<Option<(i64, i64)>> = (0..rows)
            .map(|i| {
                let u = units?;
                if u.is_null(i) {
                    return None;
                }
                // A `day`-and-coarser unit truncates the date and zeroes the time;
                // a finer unit keeps the date and floors the time-of-day.
                let (date, time) = match truncate_date(base_date(i)?, u.value(i)) {
                    Some(d) => (d, 0i64),
                    None => (
                        base_date(i)?,
                        truncate_time_nanos(base_time(i)?, u.value(i))?,
                    ),
                };
                let date_overrides = DateOverrides {
                    year: optional_i64_at(&ov[0], i),
                    month: optional_i64_at(&ov[1], i),
                    day: optional_i64_at(&ov[2], i),
                    week: optional_i64_at(&ov[3], i),
                    day_of_week: optional_i64_at(&ov[4], i),
                    ordinal_day: optional_i64_at(&ov[5], i),
                    quarter: optional_i64_at(&ov[6], i),
                    day_of_quarter: optional_i64_at(&ov[7], i),
                };
                let time_overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[8], i),
                    minute: optional_i64_at(&ov[9], i),
                    second: optional_i64_at(&ov[10], i),
                    millisecond: optional_i64_at(&ov[11], i),
                    microsecond: optional_i64_at(&ov[12], i),
                    nanosecond: optional_i64_at(&ov[13], i),
                };
                let date = project_date(date, &date_overrides)?;
                let time = project_localtime(time, &time_overrides)?;
                Some((date, time))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_localdatetime_struct(&parts),
        )))
    }
}

/// `time.truncate(unit, value, map)` (`Temporal9`): truncate `value`'s
/// time-of-day to `unit` (keeping its zone offset), then apply the override
/// `map`. Args are `[value, unit, hour, minute, second, millisecond,
/// microsecond, nanosecond, timezone]`. Returns the `time` struct. (#920)
pub(in crate::expr) static CYPHER_TIME_TRUNCATE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherTimeTruncate::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherTimeTruncate {
    signature: Signature,
}

impl CypherTimeTruncate {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(9, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherTimeTruncate {
    fn name(&self) -> &'static str {
        "cypher_time_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(time_fields()))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            LocalTimeOverrides, parse_offset_seconds, project_localtime, project_time,
            time_of_day_with_offset, truncate_time_nanos,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let base: ArrayRef =
            if matches!(cols[0].data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_time_struct(cols[0].data_type())
                || is_localdatetime_struct(cols[0].data_type())
                || is_datetime_struct(cols[0].data_type())
            {
                std::sync::Arc::clone(&cols[0])
            } else {
                cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
            };
        let units_arr = cast(&cols[1], &DataType::Utf8).map_err(DataFusionError::from)?;
        let units = units_arr.as_any().downcast_ref::<StringArray>();
        let ov = cast_argument_arrays(&cols[2..8], &DataType::Int64)?;
        let tz_arr = cast(&cols[8], &DataType::Utf8).map_err(DataFusionError::from)?;
        let tz = tz_arr.as_any().downcast_ref::<StringArray>();

        let base_parts = |i: usize| -> Option<(i64, Option<i32>)> {
            if base.is_null(i) {
                return None;
            }
            match base.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some((
                    base.as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                    None,
                )),
                DataType::Struct(_) => {
                    let s = base.as_any().downcast_ref::<StructArray>()?;
                    if is_time_struct(base.data_type()) {
                        let (n, o) = time_struct_parts(s, i)?;
                        Some((n, Some(o)))
                    } else if is_datetime_struct(base.data_type()) {
                        let (_, n, o, _) = datetime_struct_parts(s, i)?;
                        Some((n, Some(o)))
                    } else {
                        Some((localdatetime_struct_parts(s, i)?.1, None))
                    }
                }
                DataType::Utf8 => {
                    time_of_day_with_offset(base.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<Option<(i64, i32)>> = (0..rows)
            .map(|i| {
                let u = units?;
                if u.is_null(i) {
                    return None;
                }
                let (base_nanos, base_offset) = base_parts(i)?;
                let truncated = truncate_time_nanos(base_nanos, u.value(i))?;
                let overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[0], i),
                    minute: optional_i64_at(&ov[1], i),
                    second: optional_i64_at(&ov[2], i),
                    millisecond: optional_i64_at(&ov[3], i),
                    microsecond: optional_i64_at(&ov[4], i),
                    nanosecond: optional_i64_at(&ov[5], i),
                };
                let nanos = project_localtime(truncated, &overrides)?;
                let new_offset = match tz {
                    Some(a) if !a.is_null(i) => Some(parse_offset_seconds(a.value(i))?),
                    _ => None,
                };
                // A `timezone` override ATTACHES to the truncated wall-clock (the
                // instant is not shifted) — pass `None` as the base offset so
                // `project_time` attaches, mirroring datetime.truncate (#990).
                // (#1008, Temporal9 [5])
                let eff_offset = if new_offset.is_some() {
                    None
                } else {
                    base_offset
                };
                Some(project_time(nanos, eff_offset, new_offset))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_time_struct(&parts),
        )))
    }
}

/// `datetime.truncate(unit, value, map)` (`Temporal9`): truncate `value` to
/// `unit` — date for `day`-and-coarser units (time zeroed), time-of-day for finer
/// units — keeping the source zone, then apply the override `map` and optional
/// `timezone`. Args are `[value, unit, year, month, day, week, dayOfWeek,
/// ordinalDay, quarter, dayOfQuarter, hour, minute, second, millisecond,
/// microsecond, nanosecond, timezone]`. Returns the `datetime` struct. (#920)
pub(in crate::expr) static CYPHER_DATETIME_TRUNCATE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDateTimeTruncate::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherDateTimeTruncate {
    signature: Signature,
}

impl CypherDateTimeTruncate {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(17, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDateTimeTruncate {
    fn name(&self) -> &'static str {
        "cypher_datetime_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(datetime_fields()))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive per-row truncation: typed source extraction, the \
                  date/time granularity split, 14 overrides, and zone re-resolution"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            DateOverrides, LocalTimeOverrides, parse_date_or_datetime_prefix, project_date,
            project_datetime, project_localtime, time_offset_zone, truncate_date,
            truncate_time_nanos,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // One `value` source feeds both the date and time components.
        let value: ArrayRef =
            if matches!(cols[0].data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_date_struct(cols[0].data_type())
                || is_localdatetime_struct(cols[0].data_type())
                || is_time_struct(cols[0].data_type())
                || is_datetime_struct(cols[0].data_type())
            {
                std::sync::Arc::clone(&cols[0])
            } else {
                cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
            };
        let units_arr = cast(&cols[1], &DataType::Utf8).map_err(DataFusionError::from)?;
        let units = units_arr.as_any().downcast_ref::<StringArray>();
        let ov = cast_argument_arrays(&cols[2..16], &DataType::Int64)?;
        let tz_arr = cast(&cols[16], &DataType::Utf8).map_err(DataFusionError::from)?;
        let tz = tz_arr.as_any().downcast_ref::<StringArray>();

        let base_date = |i: usize| -> Option<i64> {
            if value.is_null(i) {
                return None;
            }
            match value.data_type() {
                DataType::Struct(_) => {
                    let s = value.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(value.data_type()) {
                        date_struct_value(s, i)
                    } else if is_datetime_struct(value.data_type()) {
                        Some(datetime_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    value.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };
        // Base time-of-day plus the source's offset and named zone (if any).
        let base_time = |i: usize| -> Option<(i64, Option<i32>, Option<String>)> {
            if value.is_null(i) {
                return None;
            }
            match value.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some((
                    value
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                    None,
                    None,
                )),
                DataType::Struct(_) => {
                    let s = value.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(value.data_type()) {
                        Some((0, None, None)) // date-only → midnight
                    } else if is_datetime_struct(value.data_type()) {
                        let (_, n, o, z) = datetime_struct_parts(s, i)?;
                        Some((n, Some(o), z))
                    } else if is_time_struct(value.data_type()) {
                        let (n, o) = time_struct_parts(s, i)?;
                        Some((n, Some(o), None))
                    } else {
                        Some((localdatetime_struct_parts(s, i)?.1, None, None))
                    }
                }
                DataType::Utf8 => {
                    time_offset_zone(value.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<DateTimeRow> = (0..rows)
            .map(|i| {
                let u = units?;
                if u.is_null(i) {
                    return None;
                }
                let (bt_nanos, src_offset, src_zone) = base_time(i)?;
                // A `day`-and-coarser unit truncates the date and zeroes the time;
                // a finer unit keeps the date and floors the time-of-day.
                let (date0, nanos0) = match truncate_date(base_date(i)?, u.value(i)) {
                    Some(d) => (d, 0i64),
                    None => (base_date(i)?, truncate_time_nanos(bt_nanos, u.value(i))?),
                };
                let date_overrides = DateOverrides {
                    year: optional_i64_at(&ov[0], i),
                    month: optional_i64_at(&ov[1], i),
                    day: optional_i64_at(&ov[2], i),
                    week: optional_i64_at(&ov[3], i),
                    day_of_week: optional_i64_at(&ov[4], i),
                    ordinal_day: optional_i64_at(&ov[5], i),
                    quarter: optional_i64_at(&ov[6], i),
                    day_of_quarter: optional_i64_at(&ov[7], i),
                };
                let time_overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[8], i),
                    minute: optional_i64_at(&ov[9], i),
                    second: optional_i64_at(&ov[10], i),
                    millisecond: optional_i64_at(&ov[11], i),
                    microsecond: optional_i64_at(&ov[12], i),
                    nanosecond: optional_i64_at(&ov[13], i),
                };
                let date = project_date(date0, &date_overrides)?;
                let nanos = project_localtime(nanos0, &time_overrides)?;
                let new_tz = tz.and_then(|a| (!a.is_null(i)).then(|| a.value(i)));
                // Truncation is WALL-CLOCK preserving: a `{timezone: …}` override
                // ATTACHES the zone to the truncated local time (midnight stays
                // midnight), it does not re-express the source instant. So drop
                // the source offset when a new zone is given, forcing
                // `project_datetime`'s attach path instead of an instant shift
                // (#920 — otherwise `truncate(…, {timezone: 'Europe/Stockholm'})`
                // shifted midnight by the source offset).
                let src_offset = if new_tz.is_some() { None } else { src_offset };
                let (date, nanos, offset, zone) =
                    project_datetime(date, nanos, src_offset, src_zone.as_deref(), new_tz)?;
                Some((date, nanos, offset, zone))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_datetime_struct(&parts),
        )))
    }
}

/// `date.truncate(unit, value, map)` (`Temporal9`): truncate `value`'s date to
/// `unit`, then apply the override `map` (same fields as projection). Args are
/// `[value, unit, year, month, day, week, dayOfWeek, ordinalDay, quarter,
/// dayOfQuarter]` — `value` is a `Date32` or ISO date/datetime string, `unit` a
/// string, the eight overrides nullable integers. Returns `Date32`. (#920)
pub(in crate::expr) static CYPHER_DATE_TRUNCATE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDateTruncate::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherDateTruncate {
    signature: Signature,
}

impl CypherDateTruncate {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(10, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDateTruncate {
    fn name(&self) -> &'static str {
        "cypher_date_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_ir::arrow_schema::date_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            DateOverrides, parse_date_or_datetime_prefix, project_date, truncate_date,
        };
        use datafusion::arrow::array::{Array, ArrayRef, StringArray, StructArray};
        use datafusion::arrow::compute::cast;
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // DataFusion emits `Utf8View` for string *columns* by default (string
        // literals stay `Utf8`), and our downcasts target `StringArray` — cast
        // string inputs to `Utf8` so a column-typed value/unit isn't silently
        // nulled. A `date`/`localdatetime`/`time`/`datetime` struct is taken directly.
        let value: ArrayRef = if is_date_struct(cols[0].data_type())
            || is_localdatetime_struct(cols[0].data_type())
            || is_time_struct(cols[0].data_type())
            || is_datetime_struct(cols[0].data_type())
        {
            std::sync::Arc::clone(&cols[0])
        } else {
            cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
        };
        let units_arr = cast(&cols[1], &DataType::Utf8).map_err(DataFusionError::from)?;
        let units = units_arr.as_any().downcast_ref::<StringArray>();
        let ov = cast_argument_arrays(&cols[2..10], &DataType::Int64)?;

        let base_date = |i: usize| -> Option<i64> {
            if value.is_null(i) {
                return None;
            }
            match value.data_type() {
                DataType::Struct(_) => {
                    let s = value.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(value.data_type()) {
                        date_struct_value(s, i)
                    } else {
                        // A `localdatetime`/`datetime` value — truncate its date.
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    value.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };

        let out: Vec<Option<i64>> = (0..rows)
            .map(|i| {
                let u = units?;
                if u.is_null(i) {
                    return None;
                }
                let truncated = truncate_date(base_date(i)?, u.value(i))?;
                let overrides = DateOverrides {
                    year: optional_i64_at(&ov[0], i),
                    month: optional_i64_at(&ov[1], i),
                    day: optional_i64_at(&ov[2], i),
                    week: optional_i64_at(&ov[3], i),
                    day_of_week: optional_i64_at(&ov[4], i),
                    ordinal_day: optional_i64_at(&ov[5], i),
                    quarter: optional_i64_at(&ov[6], i),
                    day_of_quarter: optional_i64_at(&ov[7], i),
                };
                project_date(truncated, &overrides)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_date_struct(&out),
        )))
    }
}
