//! Cypher scalar conversion UDF adapters.

use crate::expr::{
    date_struct_value, datetime_struct_parts, decoded_scalar_at, duration_struct_parts,
    is_date_struct, is_datetime_struct, is_duration_struct, is_het_struct_type,
    is_localdatetime_struct, is_time_struct, localdatetime_struct_parts, scalar_as_f64,
    scalar_as_i128, time_struct_parts, validate_heterogeneous_arguments,
};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::ScalarUDFImpl;
use datafusion::logical_expr::Signature;
use datafusion::logical_expr::Volatility;
use datafusion::scalar::ScalarValue;
use std::sync::LazyLock;

pub(in crate::expr) static CYPHER_TO_INTEGER: LazyLock<ScalarUDF> = LazyLock::new(|| {
    ScalarUDF::new_from_impl(CypherConversion::new(CypherConversionKind::Integer))
});

pub(in crate::expr) static CYPHER_TO_FLOAT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherConversion::new(CypherConversionKind::Float)));

pub(in crate::expr) static CYPHER_TO_BOOLEAN: LazyLock<ScalarUDF> = LazyLock::new(|| {
    ScalarUDF::new_from_impl(CypherConversion::new(CypherConversionKind::Boolean))
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::expr) enum CypherConversionKind {
    Integer,
    Float,
    Boolean,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(in crate::expr) struct CypherConversion {
    kind: CypherConversionKind,
    signature: Signature,
}

impl CypherConversion {
    pub(in crate::expr) fn new(kind: CypherConversionKind) -> Self {
        Self {
            kind,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherConversion {
    fn name(&self) -> &'static str {
        match self.kind {
            CypherConversionKind::Integer => "cypher_to_integer",
            CypherConversionKind::Float => "cypher_to_float",
            CypherConversionKind::Boolean => "cypher_to_boolean",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(match self.kind {
            CypherConversionKind::Integer => DataType::Int64,
            CypherConversionKind::Float => DataType::Float64,
            CypherConversionKind::Boolean => DataType::Boolean,
        })
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{BooleanArray, Float64Array, Int64Array};

        validate_heterogeneous_arguments(&args.args)?;
        let array = args.args[0].to_array(args.number_rows)?;
        match self.kind {
            CypherConversionKind::Integer => {
                let out: datafusion::error::Result<Int64Array> = (0..array.len())
                    .map(|i| {
                        let value = decoded_scalar_at(&array, i)?;
                        to_cypher_integer(&value)
                    })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(out?)))
            }
            CypherConversionKind::Float => {
                let out: datafusion::error::Result<Float64Array> = (0..array.len())
                    .map(|i| {
                        let value = decoded_scalar_at(&array, i)?;
                        to_cypher_float(&value)
                    })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(out?)))
            }
            CypherConversionKind::Boolean => {
                let out: datafusion::error::Result<BooleanArray> = (0..array.len())
                    .map(|i| {
                        let value = decoded_scalar_at(&array, i)?;
                        to_cypher_boolean(&value)
                    })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(out?)))
            }
        }
    }
}

pub(in crate::expr) fn conversion_type_error(
    fn_name: &str,
    value: &ScalarValue,
) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(format!(
        "{fn_name}() cannot convert value of type {:?}",
        value.data_type()
    ))
}

pub(in crate::expr) fn to_cypher_integer(
    value: &ScalarValue,
) -> datafusion::error::Result<Option<i64>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(i) = scalar_as_i128(value) {
        return i64::try_from(i)
            .map(Some)
            .map_err(|_| conversion_type_error("toInteger", value));
    }
    match value {
        ScalarValue::Float32(Some(f)) => Ok(trunc_float_to_i64(f64::from(*f))),
        ScalarValue::Float64(Some(f)) => Ok(trunc_float_to_i64(*f)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(s
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .and_then(trunc_float_to_i64)),
        _ => Err(conversion_type_error("toInteger", value)),
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "openCypher toInteger truncates finite floating values toward zero"
)]
pub(in crate::expr) fn trunc_float_to_i64(f: f64) -> Option<i64> {
    if !f.is_finite() {
        return None;
    }
    let truncated = f.trunc();
    if truncated < i64::MIN as f64 || truncated > i64::MAX as f64 {
        return None;
    }
    Some(truncated as i64)
}

pub(in crate::expr) fn to_cypher_float(
    value: &ScalarValue,
) -> datafusion::error::Result<Option<f64>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(f) = scalar_as_f64(value) {
        return Ok(Some(f));
    }
    match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            Ok(s.parse::<f64>().ok().filter(|f| f.is_finite()))
        }
        _ => Err(conversion_type_error("toFloat", value)),
    }
}

pub(in crate::expr) fn to_cypher_boolean(
    value: &ScalarValue,
) -> datafusion::error::Result<Option<bool>> {
    if value.is_null() {
        return Ok(None);
    }
    match value {
        ScalarValue::Boolean(Some(b)) => Ok(Some(*b)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => match s.as_str() {
            "true" => Ok(Some(true)),
            "false" => Ok(Some(false)),
            _ => Ok(None),
        },
        _ => Err(conversion_type_error("toBoolean", value)),
    }
}

pub(in crate::expr) fn cypher_float_string(f: f64) -> String {
    if f == 0.0 {
        "0.0".to_owned()
    } else if f.is_nan() {
        "NaN".to_owned()
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            "Infinity".to_owned()
        } else {
            "-Infinity".to_owned()
        }
    } else {
        let mut s = f.to_string();
        if !s.contains('.') && !s.contains('e') && !s.contains('E') {
            s.push_str(".0");
        }
        s
    }
}

pub(in crate::expr) fn to_cypher_string(
    value: &ScalarValue,
) -> datafusion::error::Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    match value {
        ScalarValue::Int8(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::Int16(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::Int32(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::Int64(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::UInt8(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::UInt16(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::UInt32(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::UInt64(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::Float32(Some(f)) => Ok(Some(cypher_float_string(f64::from(*f)))),
        ScalarValue::Float64(Some(f)) => Ok(Some(cypher_float_string(*f))),
        ScalarValue::Boolean(Some(b)) => Ok(Some(b.to_string())),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(Some(s.clone())),
        _ => Err(conversion_type_error("toString", value)),
    }
}

/// `toString(x)`: a typed temporal value renders to its canonical openCypher
/// string (a plain `cast` to `Utf8` would emit a fixed, untrimmed form for
/// `Time64`, and outright fail for the temporal structs). Handles `Date32`/
/// `Time64`/`localdatetime`/`time`; every other type — including `datetime`,
/// which is still a `Utf8` value until its migration — falls back to the same
/// `Utf8` cast as before, so non-temporal `toString` behaviour is unchanged.
/// (ADR 0009)
pub(in crate::expr) static CYPHER_TO_STRING: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherToString::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
pub(in crate::expr) struct CypherToString {
    signature: Signature,
}

impl CypherToString {
    pub(in crate::expr) fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherToString {
    fn name(&self) -> &'static str {
        "cypher_to_string"
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
        use crate::temporal::{
            format_date, render_localdatetime, render_localtime_nanos, render_time_value,
        };
        use datafusion::arrow::array::{Array, StringArray, StructArray, Time64NanosecondArray};
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;

        validate_heterogeneous_arguments(&args.args)?;
        let arr = args.args[0].to_array(args.number_rows)?;
        let rows = arr.len();
        // Render one canonical string per row from a closure, preserving nulls.
        let render = |f: &dyn Fn(usize) -> Option<String>| -> ColumnarValue {
            let out: StringArray = (0..rows)
                .map(|i| if arr.is_null(i) { None } else { f(i) })
                .collect();
            ColumnarValue::Array(std::sync::Arc::new(out))
        };

        let result = match arr.data_type() {
            DataType::Struct(_) if is_date_struct(arr.data_type()) => {
                let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
                render(&|i| date_struct_value(s, i).map(format_date))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let a = arr
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap();
                render(&|i| Some(render_localtime_nanos(a.value(i))))
            }
            DataType::Struct(_) if is_localdatetime_struct(arr.data_type()) => {
                let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
                render(&|i| {
                    localdatetime_struct_parts(s, i).map(|(d, n)| render_localdatetime(d, n))
                })
            }
            DataType::Struct(_) if is_time_struct(arr.data_type()) => {
                let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
                render(&|i| time_struct_parts(s, i).map(|(n, o)| render_time_value(n, o)))
            }
            DataType::Struct(_) if is_datetime_struct(arr.data_type()) => {
                let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
                render(&|i| {
                    datetime_struct_parts(s, i).map(|(d, n, o, z)| {
                        crate::temporal::render_datetime_value(d, n, o, z.as_deref())
                    })
                })
            }
            DataType::Struct(_) if is_duration_struct(arr.data_type()) => {
                let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
                render(&|i| {
                    duration_struct_parts(s, i).map(|d| crate::temporal::render_duration_value(&d))
                })
            }
            DataType::Struct(_) if is_het_struct_type(Some(arr.data_type())) => {
                let out: datafusion::error::Result<StringArray> = (0..rows)
                    .map(|i| {
                        let value = decoded_scalar_at(&arr, i)?;
                        to_cypher_string(&value)
                    })
                    .collect();
                ColumnarValue::Array(std::sync::Arc::new(out?))
            }
            // Non-temporal (or non-temporal struct): the original `Utf8` cast.
            _ => ColumnarValue::Array(
                cast(&arr, &DataType::Utf8).map_err(datafusion::error::DataFusionError::from)?,
            ),
        };
        Ok(result)
    }
}
