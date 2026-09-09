//! Cypher temporal project adapters.

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

/// `date`-from-value projection (`Temporal3`): `date(base)` / `date({date: base,
/// …overrides})`. Args are `[base, year, month, day, week, dayOfWeek,
/// ordinalDay, quarter, dayOfQuarter]` — `base` is a `Date32` or an ISO date/
/// datetime string, the eight overrides are nullable integers (null ⇒ keep the
/// base's component). Returns `Date32`. (ADR 0009 / #920)
pub(in crate::expr) static CYPHER_DATE_PROJECT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDateProject::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherDateProject {
    signature: Signature,
}

impl CypherDateProject {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(9, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDateProject {
    fn name(&self) -> &'static str {
        "cypher_date_project"
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
        use crate::temporal::{DateOverrides, parse_date_or_datetime_prefix};
        use datafusion::arrow::array::{Array, StringArray, StructArray};
        use datafusion::arrow::compute::cast;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // A typed temporal-struct base (date / localdatetime / time / datetime) is
        // read directly; a string base is cast to `Utf8` (handling `Utf8View`).
        let base = if is_date_struct(cols[0].data_type())
            || is_localdatetime_struct(cols[0].data_type())
            || is_time_struct(cols[0].data_type())
            || is_datetime_struct(cols[0].data_type())
        {
            std::sync::Arc::clone(&cols[0])
        } else {
            cast(&cols[0], &DataType::Utf8).map_err(datafusion::error::DataFusionError::from)?
        };
        // Overrides: cast each to Int64 once (a null/absent override stays null).
        let ov: Vec<_> = cols[1..9]
            .iter()
            .map(|c| cast(c, &DataType::Int64))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(datafusion::error::DataFusionError::from)?;

        let base_date = |i: usize| -> Option<i64> {
            if base.is_null(i) {
                return None;
            }
            match base.data_type() {
                DataType::Struct(_) => {
                    let s = base.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(base.data_type()) {
                        date_struct_value(s, i)
                    } else {
                        // A `localdatetime`/`datetime` value — take its date component.
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    base.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };

        let out: Vec<Option<i64>> = (0..rows)
            .map(|i| {
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
                crate::temporal::project_date(base_date(i)?, &overrides)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_date_struct(&out),
        )))
    }
}

/// `localtime`-from-value projection (`Temporal3`): `localtime(base)` /
/// `localtime({time: base, …overrides})`. Args are `[base, hour, minute, second,
/// millisecond, microsecond, nanosecond]` — `base` is a `Time64(Nanosecond)` or
/// any ISO temporal string (its time-of-day is extracted), the six overrides are
/// nullable integers (null ⇒ keep the base's component). Returns
/// `Time64(Nanosecond)`. (ADR 0009)
pub(in crate::expr) static CYPHER_LOCALTIME_PROJECT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherLocalTimeProject::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherLocalTimeProject {
    signature: Signature,
}

impl CypherLocalTimeProject {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(7, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherLocalTimeProject {
    fn name(&self) -> &'static str {
        "cypher_localtime_project"
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
        use crate::temporal::{LocalTimeOverrides, project_localtime, time_of_day_nanos_any};
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // A `Time64(Nanosecond)` or `localdatetime`-struct base is read directly;
        // any other (string) base is cast to `Utf8` (handling `Utf8View`).
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
        let ov = cast_argument_arrays(&cols[1..7], &DataType::Int64)?;

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
                // A `localdatetime` or `time` value — take its time-of-day.
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
                let overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[0], i),
                    minute: optional_i64_at(&ov[1], i),
                    second: optional_i64_at(&ov[2], i),
                    millisecond: optional_i64_at(&ov[3], i),
                    microsecond: optional_i64_at(&ov[4], i),
                    nanosecond: optional_i64_at(&ov[5], i),
                };
                project_localtime(base_nanos(i)?, &overrides)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

/// `localdatetime`-from-value projection (`Temporal3`). Args are `[date_source,
/// time_source, year, month, day, week, dayOfWeek, ordinalDay, quarter,
/// dayOfQuarter, hour, minute, second, millisecond, microsecond, nanosecond]`.
/// `date_source`/`time_source` are the lowered `datetime`/`date`/`time` anchors
/// (a `Date32`/`Time64`/`localdatetime`-struct/temporal-string, or null),
/// interpreted as a date and a time-of-day respectively (a null date ⇒ epoch, a
/// null time ⇒ midnight). The 14 overrides are nullable integers (null ⇒ keep
/// the base's component). Returns the `localdatetime` struct. (ADR 0009)
pub(in crate::expr) static CYPHER_LOCALDATETIME_PROJECT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherLocalDateTimeProject::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherLocalDateTimeProject {
    signature: Signature,
}

impl CypherLocalDateTimeProject {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(16, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherLocalDateTimeProject {
    fn name(&self) -> &'static str {
        "cypher_localdatetime_project"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(localdatetime_fields()))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive per-row projection: two typed base extractions \
                  (date + time) plus 14 component overrides — splitting it would \
                  scatter the row logic across helpers without aiding clarity"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            DateOverrides, LocalTimeOverrides, parse_date_or_datetime_prefix, project_date,
            project_localtime, time_of_day_nanos_any,
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
        // Typed sources (`date`/`localdatetime`/`time`/`datetime` struct or
        // `Time64`) are read directly; a string source is cast to `Utf8`
        // (handling `Utf8View`).
        let typed_or_utf8 = |a: &ArrayRef| -> datafusion::error::Result<ArrayRef> {
            if matches!(a.data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_date_struct(a.data_type())
                || is_localdatetime_struct(a.data_type())
                || is_time_struct(a.data_type())
                || is_datetime_struct(a.data_type())
            {
                Ok(std::sync::Arc::clone(a))
            } else {
                cast(a, &DataType::Utf8).map_err(DataFusionError::from)
            }
        };
        let date_src = typed_or_utf8(&cols[0])?;
        let time_src = typed_or_utf8(&cols[1])?;
        let ov = cast_argument_arrays(&cols[2..16], &DataType::Int64)?;

        let base_date = |i: usize| -> Option<i64> {
            if date_src.is_null(i) {
                return Some(0); // a missing date defaults to the epoch (day 0)
            }
            match date_src.data_type() {
                DataType::Struct(_) => {
                    let s = date_src.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(date_src.data_type()) {
                        date_struct_value(s, i)
                    } else {
                        // A `localdatetime`/`datetime` value — take its date.
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    date_src.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };
        let base_time = |i: usize| -> Option<i64> {
            if time_src.is_null(i) {
                return Some(0); // a missing time defaults to midnight
            }
            match time_src.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some(
                    time_src
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                ),
                DataType::Struct(_) => {
                    let s = time_src.as_any().downcast_ref::<StructArray>()?;
                    // A date-only source (bare `localdatetime(date(…))`, where the
                    // same value feeds both slots) has no time-of-day → midnight,
                    // matching `localdatetime({date: d})`. (A time-only source in
                    // the *date* slot correctly stays null — no date to fabricate.)
                    if is_date_struct(time_src.data_type()) {
                        Some(0)
                    } else if is_time_struct(time_src.data_type()) {
                        Some(time_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.1)
                    }
                }
                DataType::Utf8 => {
                    time_of_day_nanos_any(time_src.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<Option<(i64, i64)>> = (0..rows)
            .map(|i| {
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
                let date = project_date(base_date(i)?, &date_overrides)?;
                let time = project_localtime(base_time(i)?, &time_overrides)?;
                Some((date, time))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_localdatetime_struct(&parts),
        )))
    }
}

/// `time`-from-value projection (`Temporal3`). Args are `[base, hour, minute,
/// second, millisecond, microsecond, nanosecond, timezone]`. `base` is a
/// `Time64`/`time`-struct/`localdatetime`-struct/temporal-string (its time-of-day
/// and, for `time`/`datetime` bases, its offset are read); the six integer
/// overrides adjust the time-of-day; `timezone` (a `+HH:MM`/`Z` string, or null)
/// attaches a zone — shifting the wall-clock time if the base already had one.
/// Returns the `time` struct. (ADR 0009)
pub(in crate::expr) static CYPHER_TIME_PROJECT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherTimeProject::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherTimeProject {
    signature: Signature,
}

impl CypherTimeProject {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(8, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherTimeProject {
    fn name(&self) -> &'static str {
        "cypher_time_project"
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
            time_of_day_with_offset,
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
        let ov = cast_argument_arrays(&cols[1..7], &DataType::Int64)?;
        let tz_arr = cast(&cols[7], &DataType::Utf8).map_err(DataFusionError::from)?;
        let tz = tz_arr.as_any().downcast_ref::<StringArray>();

        // Base time-of-day + whether it carried an offset (`None` ⇒ attach a new
        // zone; `Some` ⇒ shift to preserve the instant).
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
                        // A `datetime` carries its zone offset — keep it so a new
                        // zone shifts the instant (`time(datetime)`).
                        let (_, n, o, _) = datetime_struct_parts(s, i)?;
                        Some((n, Some(o)))
                    } else {
                        // localdatetime struct — its time-of-day, no offset.
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
                let (base_nanos, base_offset) = base_parts(i)?;
                let overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[0], i),
                    minute: optional_i64_at(&ov[1], i),
                    second: optional_i64_at(&ov[2], i),
                    millisecond: optional_i64_at(&ov[3], i),
                    microsecond: optional_i64_at(&ov[4], i),
                    nanosecond: optional_i64_at(&ov[5], i),
                };
                let nanos = project_localtime(base_nanos, &overrides)?;
                // A `timezone` override (offset string) re-zones the value.
                let new_offset = match tz {
                    Some(a) if !a.is_null(i) => Some(parse_offset_seconds(a.value(i))?),
                    _ => None,
                };
                Some(project_time(nanos, base_offset, new_offset))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_time_struct(&parts),
        )))
    }
}

/// `datetime`-from-value projection (`Temporal3` [8]-[11]). Args are `[date_src,
/// time_src, year, month, day, week, dayOfWeek, ordinalDay, quarter,
/// dayOfQuarter, hour, minute, second, millisecond, microsecond, nanosecond,
/// timezone]`. The date/time sources are any temporal value/string (the time
/// source also carries the source offset + named zone); the 14 integer overrides
/// adjust the local date/time; `timezone` re-zones. Returns the `datetime`
/// struct. (ADR 0009)
pub(in crate::expr) static CYPHER_DATETIME_PROJECT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDateTimeProject::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherDateTimeProject {
    signature: Signature,
}

impl CypherDateTimeProject {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(17, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDateTimeProject {
    fn name(&self) -> &'static str {
        "cypher_datetime_project"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(datetime_fields()))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive per-row projection: typed date/time/zone source \
                  extraction, 14 component overrides, and zone re-resolution"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            DateOverrides, LocalTimeOverrides, parse_date_or_datetime_prefix, project_date,
            project_datetime, project_localtime, time_offset_zone,
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
        let typed_or_utf8 = |a: &ArrayRef| -> datafusion::error::Result<ArrayRef> {
            if matches!(a.data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_date_struct(a.data_type())
                || is_localdatetime_struct(a.data_type())
                || is_time_struct(a.data_type())
                || is_datetime_struct(a.data_type())
            {
                Ok(std::sync::Arc::clone(a))
            } else {
                cast(a, &DataType::Utf8).map_err(DataFusionError::from)
            }
        };
        let date_src = typed_or_utf8(&cols[0])?;
        let time_src = typed_or_utf8(&cols[1])?;
        let ov = cast_argument_arrays(&cols[2..16], &DataType::Int64)?;
        let tz_arr = cast(&cols[16], &DataType::Utf8).map_err(DataFusionError::from)?;
        let tz = tz_arr.as_any().downcast_ref::<StringArray>();

        let base_date = |i: usize| -> Option<i64> {
            if date_src.is_null(i) {
                return Some(0); // a missing date defaults to the epoch (day 0)
            }
            match date_src.data_type() {
                DataType::Struct(_) => {
                    let s = date_src.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(date_src.data_type()) {
                        date_struct_value(s, i)
                    } else if is_datetime_struct(date_src.data_type()) {
                        Some(datetime_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    date_src.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };
        // Base time-of-day plus the source's offset and named zone (if any).
        let base_time = |i: usize| -> Option<(i64, Option<i32>, Option<String>)> {
            if time_src.is_null(i) {
                return Some((0, None, None));
            }
            match time_src.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some((
                    time_src
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                    None,
                    None,
                )),
                DataType::Struct(_) => {
                    let s = time_src.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(time_src.data_type()) {
                        // A bare date carries no time-of-day (matches the pre-#1011
                        // `Date32` fall-through: `datetime(date(…))` → null).
                        None
                    } else if is_datetime_struct(time_src.data_type()) {
                        let (_, n, o, z) = datetime_struct_parts(s, i)?;
                        Some((n, Some(o), z))
                    } else if is_time_struct(time_src.data_type()) {
                        let (n, o) = time_struct_parts(s, i)?;
                        Some((n, Some(o), None))
                    } else {
                        Some((localdatetime_struct_parts(s, i)?.1, None, None))
                    }
                }
                DataType::Utf8 => {
                    time_offset_zone(time_src.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<DateTimeRow> = (0..rows)
            .map(|i| {
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
                let (base_nanos, src_offset, src_zone) = base_time(i)?;
                let date = project_date(base_date(i)?, &date_overrides)?;
                let nanos = project_localtime(base_nanos, &time_overrides)?;
                let new_tz = tz.and_then(|a| (!a.is_null(i)).then(|| a.value(i)));
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
