//! Cypher temporal accessors adapters.

use super::{
    date_struct_value, datetime_struct_parts, duration_struct_parts, is_datetime_struct,
    is_localdatetime_struct, is_time_struct, localdatetime_struct_parts, time_struct_parts,
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

/// `date` component accessor (`Temporal5`): `cypher_date_component(date, name)`
/// where `date` is a `Date32` and `name` is the accessor (`year`/`quarter`/
/// `month`/`week`/`weekYear`/`day`/`ordinalDay`/`weekDay`/`dayOfQuarter`).
/// Returns the component as `Int64`. (ADR 0009 / #920)
pub(in crate::expr) static CYPHER_DATE_COMPONENT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDateComponent::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherDateComponent {
    signature: Signature,
}

impl CypherDateComponent {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDateComponent {
    fn name(&self) -> &'static str {
        "cypher_date_component"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::date_component;
        use datafusion::arrow::array::{Array, Int64Array, StringArray, StructArray};

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let dates = args.args[0].to_array(rows)?;
        let names = args.args[1].to_array(rows)?;
        let d = dates.as_any().downcast_ref::<StructArray>();
        let n = names.as_any().downcast_ref::<StringArray>();
        let out: Int64Array = (0..rows)
            .map(|i| {
                let (d, n) = (d?, n?);
                if n.is_null(i) {
                    return None;
                }
                date_component(date_struct_value(d, i)?, n.value(i))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

/// `duration` component accessor (`d.days`/`d.seconds`/`d.monthsOfQuarter`/…):
/// `[interval_value, component_name]` → `Int64`. (#920)
pub(in crate::expr) static CYPHER_DURATION_COMPONENT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDurationComponent::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherDurationComponent {
    signature: Signature,
}

impl CypherDurationComponent {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDurationComponent {
    fn name(&self) -> &'static str {
        "cypher_duration_component"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::duration_component;
        use datafusion::arrow::array::{Array, Int64Array, StringArray, StructArray};

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let durs = args.args[0].to_array(rows)?;
        let names = args.args[1].to_array(rows)?;
        let d = durs.as_any().downcast_ref::<StructArray>();
        let n = names.as_any().downcast_ref::<StringArray>();
        let out: Int64Array = (0..rows)
            .map(|i| {
                let (d, n) = (d?, n?);
                if d.is_null(i) || n.is_null(i) {
                    return None;
                }
                duration_component(&duration_struct_parts(d, i)?, n.value(i))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

/// Whether `name` is a valid component accessor for a typed-temporal COLUMN type
/// other than `Date32`/duration (which dispatch separately): `localtime`
/// (`Time64`), or a `time`/`localdatetime`/`datetime` struct. Gates the accessor
/// dispatch so a non-temporal property access still falls through to a column
/// lookup. (#1008)
pub(in crate::expr) fn temporal_accessor_valid(dt: &DataType, name: &str) -> bool {
    use crate::temporal::{
        is_date_accessor, is_epoch_accessor, is_time_accessor, is_zone_int_accessor,
        is_zone_str_accessor,
    };
    match dt {
        DataType::Time64(_) => is_time_accessor(name),
        DataType::Struct(_) if is_time_struct(dt) => {
            is_time_accessor(name) || is_zone_int_accessor(name) || is_zone_str_accessor(name)
        }
        DataType::Struct(_) if is_localdatetime_struct(dt) => {
            is_date_accessor(name) || is_time_accessor(name)
        }
        DataType::Struct(_) if is_datetime_struct(dt) => {
            is_date_accessor(name)
                || is_time_accessor(name)
                || is_zone_int_accessor(name)
                || is_zone_str_accessor(name)
                || is_epoch_accessor(name)
        }
        _ => false,
    }
}

/// `Temporal5` INT component accessor (`d.hour`/`d.year`/`d.offsetSeconds`/
/// `d.epochMillis`/…): `[value, name]` → `Int64`. `value` is a typed `localtime`
/// (`Time64`) or `time`/`localdatetime`/`datetime` struct; the UDF inspects the
/// Arrow type to extract the relevant field. (#1008)
pub(in crate::expr) static CYPHER_TEMPORAL_COMPONENT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherTemporalComponent::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherTemporalComponent {
    signature: Signature,
}

impl CypherTemporalComponent {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherTemporalComponent {
    fn name(&self) -> &'static str {
        "cypher_temporal_component"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            date_component, epoch_component, is_date_accessor, is_time_accessor,
            is_zone_int_accessor, time_component, zone_int_component,
        };
        use datafusion::arrow::array::{
            Array, Int64Array, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::datatypes::TimeUnit;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let vals = args.args[0].to_array(rows)?;
        let names = args.args[1].to_array(rows)?;
        let n = names.as_any().downcast_ref::<StringArray>();
        let out: Int64Array = (0..rows)
            .map(|i| {
                let n = n?;
                if vals.is_null(i) || n.is_null(i) {
                    return None;
                }
                let name = n.value(i);
                match vals.data_type() {
                    DataType::Time64(TimeUnit::Nanosecond) => {
                        let v = vals.as_any().downcast_ref::<Time64NanosecondArray>()?;
                        time_component(v.value(i), name)
                    }
                    DataType::Struct(_) => {
                        let s = vals.as_any().downcast_ref::<StructArray>()?;
                        if is_time_struct(vals.data_type()) {
                            let (nanos, offset) = time_struct_parts(s, i)?;
                            if is_zone_int_accessor(name) {
                                zone_int_component(offset, name)
                            } else {
                                time_component(nanos, name)
                            }
                        } else if is_localdatetime_struct(vals.data_type()) {
                            let (days, nanos) = localdatetime_struct_parts(s, i)?;
                            if is_date_accessor(name) {
                                date_component(days, name)
                            } else {
                                time_component(nanos, name)
                            }
                        } else if is_datetime_struct(vals.data_type()) {
                            let (days, nanos, offset, _) = datetime_struct_parts(s, i)?;
                            if is_date_accessor(name) {
                                date_component(days, name)
                            } else if is_time_accessor(name) {
                                time_component(nanos, name)
                            } else if is_zone_int_accessor(name) {
                                zone_int_component(offset, name)
                            } else {
                                epoch_component(days, nanos, offset, name)
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

/// `Temporal5` STRING component accessor (`d.timezone`/`d.offset`): `[value,
/// name]` → `Utf8`. `value` is a typed `time` or `datetime` struct. (#1008)
pub(in crate::expr) static CYPHER_TEMPORAL_ZONE_STR: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherTemporalZoneStr::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct CypherTemporalZoneStr {
    signature: Signature,
}

impl CypherTemporalZoneStr {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherTemporalZoneStr {
    fn name(&self) -> &'static str {
        "cypher_temporal_zone_str"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::zone_str_component;
        use datafusion::arrow::array::{Array, StringArray, StructArray};

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let vals = args.args[0].to_array(rows)?;
        let names = args.args[1].to_array(rows)?;
        let n = names.as_any().downcast_ref::<StringArray>();
        let out: StringArray = (0..rows)
            .map(|i| {
                let n = n?;
                if vals.is_null(i) || n.is_null(i) {
                    return None;
                }
                let name = n.value(i);
                let s = vals.as_any().downcast_ref::<StructArray>()?;
                if is_time_struct(vals.data_type()) {
                    let (_, offset) = time_struct_parts(s, i)?;
                    zone_str_component(offset, None, name)
                } else if is_datetime_struct(vals.data_type()) {
                    let (_, _, offset, zone) = datetime_struct_parts(s, i)?;
                    zone_str_component(offset, zone.as_deref(), name)
                } else {
                    None
                }
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}
