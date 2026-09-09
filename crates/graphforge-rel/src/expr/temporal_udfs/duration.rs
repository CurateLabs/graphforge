//! Cypher temporal duration adapters.

use super::{
    DateTimeRow, build_date_struct, build_datetime_struct, build_duration_struct,
    build_localdatetime_struct, build_time_struct, date_struct_value, datetime_struct_parts,
    duration_struct_parts, is_date_struct, is_datetime_struct, is_localdatetime_struct,
    is_time_struct, localdatetime_struct_parts, time_struct_parts, udf_argument_arrays,
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

/// `duration.between(a, b)` / `inMonths` / `inDays` / `inSeconds` (`Temporal10`):
/// `[a, b, mode]` → `Interval(MonthDayNano)`. `a`/`b` are typed temporals
/// (`Date32`/`Time64`/`localdatetime`/`time`/`datetime` struct); `mode` selects
/// the family member. (#920)
pub(in crate::expr) static CYPHER_DURATION_BETWEEN: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDurationBetween::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherDurationBetween {
    signature: Signature,
}

impl CypherDurationBetween {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

/// Extract a [`BetweenOperand`](crate::temporal::BetweenOperand) — `(date, nanos,
/// offset)` — from a typed temporal array at row `i`.
fn between_operand(
    arr: &datafusion::arrow::array::ArrayRef,
    i: usize,
) -> Option<crate::temporal::BetweenOperand> {
    use datafusion::arrow::array::{Array, StructArray, Time64NanosecondArray};
    use datafusion::arrow::datatypes::TimeUnit;
    if arr.is_null(i) {
        return None;
    }
    match arr.data_type() {
        DataType::Time64(TimeUnit::Nanosecond) => Some((
            None,
            arr.as_any()
                .downcast_ref::<Time64NanosecondArray>()?
                .value(i),
            None,
            None,
        )),
        DataType::Struct(_) => {
            let s = arr.as_any().downcast_ref::<StructArray>()?;
            if is_date_struct(arr.data_type()) {
                Some((Some(date_struct_value(s, i)?), 0, None, None))
            } else if is_datetime_struct(arr.data_type()) {
                // Keep the named zone (if any) so DST is re-resolvable across a
                // span (#1007).
                let (days, nanos, offset, zone) = datetime_struct_parts(s, i)?;
                Some((Some(days), nanos, Some(offset), zone))
            } else if is_time_struct(arr.data_type()) {
                let (nanos, offset) = time_struct_parts(s, i)?;
                Some((None, nanos, Some(offset), None))
            } else {
                let (days, nanos) = localdatetime_struct_parts(s, i)?;
                Some((Some(days), nanos, None, None))
            }
        }
        _ => None,
    }
}

impl ScalarUDFImpl for CypherDurationBetween {
    fn name(&self) -> &'static str {
        "cypher_duration_between"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_ir::arrow_schema::duration_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{BetweenMode, duration_between};
        use datafusion::arrow::array::{Array, StringArray};
        use datafusion::arrow::compute::cast;
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let mode_arr = cast(&cols[2], &DataType::Utf8).map_err(DataFusionError::from)?;
        let modes = mode_arr.as_any().downcast_ref::<StringArray>();

        let parts: Vec<Option<crate::temporal::DurationValue>> = (0..rows)
            .map(|i| {
                let m = modes?;
                if m.is_null(i) {
                    return None;
                }
                let mode = match m.value(i) {
                    "duration.between" => BetweenMode::Between,
                    "duration.inmonths" => BetweenMode::Months,
                    "duration.indays" => BetweenMode::Days,
                    "duration.inseconds" => BetweenMode::Seconds,
                    _ => return None,
                };
                let a = between_operand(&cols[0], i)?;
                let b = between_operand(&cols[1], i)?;
                duration_between(&a, &b, mode)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_duration_struct(&parts),
        )))
    }
}

/// `temporal ± duration` (`Temporal8`): `[temporal, duration_struct, sign]` →
/// the SAME type as the temporal operand (`return_type` echoes `arg_types[0]`).
/// `sign` is `+1` (add) or `-1` (subtract). Dispatches on the temporal type:
/// date adds months+days (sub-day time → whole days); localtime/time wrap the
/// time-of-day mod 24h; localdatetime/datetime add months+days+time carrying
/// overflow (zone offset / named zone preserved). (#920)
pub(in crate::expr) static CYPHER_TEMPORAL_ARITH: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherTemporalArith::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherTemporalArith {
    signature: Signature,
}

impl CypherTemporalArith {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherTemporalArith {
    fn name(&self) -> &'static str {
        "cypher_temporal_arith"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        // The result is the same temporal type as the first (temporal) operand.
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive per-temporal-type dispatch (date/localtime/time/\
                  localdatetime/datetime) applying a signed duration"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            date_plus_duration, datetime_plus_duration, localtime_plus_duration,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, Int64Array, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::datatypes::TimeUnit;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let temporal = &cols[0];
        let dur = cols[1].as_any().downcast_ref::<StructArray>();
        let signs = cols[2].as_any().downcast_ref::<Int64Array>();

        // The signed [`DurationValue`] for row `i` (None if either operand is null).
        let signed = |i: usize| -> Option<crate::temporal::DurationValue> {
            let (d, sg) = (dur?, signs?);
            if d.is_null(i) || sg.is_null(i) {
                return None;
            }
            let dv = duration_struct_parts(d, i)?;
            Some(if sg.value(i) < 0 {
                crate::temporal::DurationValue {
                    months: -dv.months,
                    days: -dv.days,
                    seconds: -dv.seconds,
                    nanos: -dv.nanos,
                }
            } else {
                dv
            })
        };
        // Total signed sub-day nanoseconds of a duration (time-of-day arithmetic).
        // Sub-day nanoseconds of a duration for time-of-day (mod-24h) arithmetic.
        // Reduce `seconds` mod a day FIRST so a huge duration can't overflow the
        // `* 1e9` (the result is used mod a day anyway, so this is exact). (#1011)
        let sub_day_nanos = |d: &crate::temporal::DurationValue| {
            d.seconds.rem_euclid(86_400) * 1_000_000_000 + d.nanos
        };

        let out: ArrayRef = match temporal.data_type() {
            DataType::Struct(_) if is_date_struct(temporal.data_type()) => {
                let t = temporal.as_any().downcast_ref::<StructArray>();
                let days: Vec<Option<i64>> = (0..rows)
                    .map(|i| {
                        let t = t?;
                        let d = date_struct_value(t, i)?;
                        let dv = signed(i)?;
                        Some(date_plus_duration(d, &dv))
                    })
                    .collect();
                std::sync::Arc::new(build_date_struct(&days))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let t = temporal.as_any().downcast_ref::<Time64NanosecondArray>();
                let a: Time64NanosecondArray = (0..rows)
                    .map(|i| {
                        let t = t?;
                        if t.is_null(i) {
                            return None;
                        }
                        let dv = signed(i)?;
                        Some(localtime_plus_duration(t.value(i), sub_day_nanos(&dv)))
                    })
                    .collect();
                std::sync::Arc::new(a)
            }
            DataType::Struct(_) if is_time_struct(temporal.data_type()) => {
                let s = temporal.as_any().downcast_ref::<StructArray>();
                let parts: Vec<Option<(i64, i32)>> = (0..rows)
                    .map(|i| {
                        let s = s?;
                        let (nanos, offset) = time_struct_parts(s, i)?;
                        let dv = signed(i)?;
                        Some((localtime_plus_duration(nanos, sub_day_nanos(&dv)), offset))
                    })
                    .collect();
                std::sync::Arc::new(build_time_struct(&parts))
            }
            DataType::Struct(_) if is_datetime_struct(temporal.data_type()) => {
                let s = temporal.as_any().downcast_ref::<StructArray>();
                let parts: Vec<DateTimeRow> = (0..rows)
                    .map(|i| {
                        let s = s?;
                        let (days, nanos, offset, zone) = datetime_struct_parts(s, i)?;
                        let dv = signed(i)?;
                        let (date, no) = datetime_plus_duration(days, nanos, &dv);
                        Some((date, no, offset, zone))
                    })
                    .collect();
                std::sync::Arc::new(build_datetime_struct(&parts))
            }
            // localdatetime struct (date + time, no zone).
            DataType::Struct(_) if is_localdatetime_struct(temporal.data_type()) => {
                let s = temporal.as_any().downcast_ref::<StructArray>();
                let parts: Vec<Option<(i64, i64)>> = (0..rows)
                    .map(|i| {
                        let s = s?;
                        let (days, nanos) = localdatetime_struct_parts(s, i)?;
                        let dv = signed(i)?;
                        let (date, no) = datetime_plus_duration(days, nanos, &dv);
                        Some((date, no))
                    })
                    .collect();
                std::sync::Arc::new(build_localdatetime_struct(&parts))
            }
            other => {
                return Err(datafusion::error::DataFusionError::Internal(format!(
                    "cypher_temporal_arith: left operand is not a temporal value ({other:?})"
                )));
            }
        };
        Ok(ColumnarValue::Array(out))
    }
}

/// Runtime `duration(<string-expr>)` (`Temporal6`): parse an ISO-8601 duration
/// string per row into a `Struct{months, days, seconds, nanos}` (null on unparseable or
/// null input), the inverse of the `toString` render. Used when the argument is
/// not a constant (e.g. `duration(toString(d))`). (#920)
pub(in crate::expr) static CYPHER_DURATION_PARSE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDurationParse::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherDurationParse {
    signature: Signature,
}

impl CypherDurationParse {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDurationParse {
    fn name(&self) -> &'static str {
        "cypher_duration_parse"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_ir::arrow_schema::duration_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, StringArray};
        use datafusion::arrow::compute::cast;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let arr = cast(&args.args[0].to_array(rows)?, &DataType::Utf8)?;
        let s = arr.as_any().downcast_ref::<StringArray>();
        let parts: Vec<Option<crate::temporal::DurationValue>> = (0..rows)
            .map(|i| {
                let s = s?;
                if s.is_null(i) {
                    return Option::None;
                }
                crate::temporal::duration_value_from_str(s.value(i))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_duration_struct(&parts),
        )))
    }
}

/// `duration ± duration` (`Temporal8`): `[a, b, sign]` → component-wise
/// `(a.months + sign·b.months, …days, …nanos)` as a duration struct. (#920)
pub(in crate::expr) static CYPHER_DURATION_ADD: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDurationAdd::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherDurationAdd {
    signature: Signature,
}

impl CypherDurationAdd {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDurationAdd {
    fn name(&self) -> &'static str {
        "cypher_duration_add"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_ir::arrow_schema::duration_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, Int64Array, StructArray};

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let a = cols[0].as_any().downcast_ref::<StructArray>();
        let b = cols[1].as_any().downcast_ref::<StructArray>();
        let signs = cols[2].as_any().downcast_ref::<Int64Array>();

        let parts: Vec<Option<crate::temporal::DurationValue>> = (0..rows)
            .map(|i| {
                let (a, b, sg) = (a?, b?, signs?);
                let av = duration_struct_parts(a, i)?;
                let bv = duration_struct_parts(b, i)?;
                let s: i64 = if sg.is_null(i) || sg.value(i) >= 0 {
                    1
                } else {
                    -1
                };
                // Add componentwise and normalise the nanos carry into seconds —
                // WITHOUT forming a `seconds * 1e9` total (which would overflow
                // i64 for combined sub-day spans > ~292 years, defeating the
                // widened `seconds` field). `nanos` sums into (-1e9, 2e9), so
                // div/rem_euclid re-canonicalise to a non-negative `[0, 1e9)`. (#1011)
                let nanos_sum = av.nanos + s * bv.nanos;
                let seconds = av.seconds + s * bv.seconds + nanos_sum.div_euclid(1_000_000_000);
                Some(crate::temporal::DurationValue {
                    months: av.months + s * bv.months,
                    days: av.days + s * bv.days,
                    seconds,
                    nanos: nanos_sum.rem_euclid(1_000_000_000),
                })
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_duration_struct(&parts),
        )))
    }
}

/// `duration * number` / `duration / number` (#920 Temporal8 [7]). Args are
/// `[duration_struct, number, is_div]`; scales each component and re-normalises
/// via [`crate::temporal::scale_duration`] (fractional months → days → time).
pub(in crate::expr) static CYPHER_DURATION_SCALE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDurationScale::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherDurationScale {
    signature: Signature,
}

impl CypherDurationScale {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDurationScale {
    fn name(&self) -> &'static str {
        "cypher_duration_scale"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_ir::arrow_schema::duration_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, BooleanArray, Float64Array, StructArray};
        use datafusion::arrow::compute::cast;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let dur = cols[0].as_any().downcast_ref::<StructArray>();
        // The numeric factor may arrive as any int/float width — cast to f64.
        let num = cast(&cols[1], &DataType::Float64)?;
        let num = num.as_any().downcast_ref::<Float64Array>();
        let is_div = cols[2].as_any().downcast_ref::<BooleanArray>();

        let parts: Vec<Option<crate::temporal::DurationValue>> = (0..rows)
            .map(|i| {
                let (dur, num, is_div) = (dur?, num?, is_div?);
                if num.is_null(i) {
                    return Option::None; // duration ∘ null = null
                }
                let dv = duration_struct_parts(dur, i)?;
                let divide = !is_div.is_null(i) && is_div.value(i);
                Some(crate::temporal::scale_duration(&dv, num.value(i), divide))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_duration_struct(&parts),
        )))
    }
}
