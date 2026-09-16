//! Conversions bindings and native task ownership.

use crate::Arc;
use crate::Buffer;
use crate::ClassInstance;
use crate::Cursor;
use crate::Env;
use crate::ExecutionResult;
use crate::FromNapiValue;
use crate::GfError;
use crate::HashMap;
use crate::IrLiteral;
use crate::JsValue;
use crate::NodeHandle;
use crate::Object;
use crate::PropValue;
use crate::Result;
use crate::StreamReader;
use crate::StreamWriter;
use crate::Unknown;
use crate::ValueType;
use crate::concat_batches;
use crate::to_napi_err;
use crate::type_error;

/// Serialize an execution result to an Arrow IPC **stream** Buffer. The stream
/// preamble carries the schema (incl. the `graphforge.*` metadata). Internal
/// execution/storage batches are coalesced at this non-streaming binding
/// boundary so JavaScript observes one logical result batch; a zero-row result
/// emits one typed empty batch. JS decodes it with apache-arrow `tableFromIPC`.
pub(super) fn result_to_ipc(result: &ExecutionResult) -> std::result::Result<Vec<u8>, GfError> {
    let logical = if result.batches.is_empty() {
        arrow::record_batch::RecordBatch::new_empty(Arc::clone(&result.schema))
    } else {
        concat_batches(&result.schema, &result.batches)
            .map_err(|error| GfError::Execution(error.to_string()))?
    };
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, result.schema.as_ref())
            .map_err(|e| GfError::Execution(e.to_string()))?;
        writer
            .write(&logical)
            .map_err(|e| GfError::Execution(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| GfError::Execution(e.to_string()))?;
    }
    Ok(buf)
}

/// Serialize one native analyst-verb batch without changing its schema.
pub(crate) fn record_batch_to_ipc(
    batch: &arrow::record_batch::RecordBatch,
) -> std::result::Result<Vec<u8>, GfError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|error| GfError::Execution(error.to_string()))?;
        writer
            .write(batch)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| GfError::Execution(error.to_string()))?;
    }
    Ok(buf)
}

/// Decode and coalesce one Arrow IPC stream without changing its schema metadata.
pub(super) fn ipc_to_record_batch(data: &Buffer) -> Result<arrow::record_batch::RecordBatch> {
    let mut reader = StreamReader::try_new(Cursor::new(data.as_ref()), None).map_err(|error| {
        to_napi_err(&GfError::Validation(format!(
            "invalid Arrow IPC stream: {error}"
        )))
    })?;
    let schema = reader.schema();
    let batches = reader
        .by_ref()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            to_napi_err(&GfError::Validation(format!(
                "invalid Arrow IPC record batch: {error}"
            )))
        })?;
    concat_batches(&schema, &batches).map_err(|error| {
        to_napi_err(&GfError::Validation(format!(
            "cannot coalesce Arrow IPC batches: {error}"
        )))
    })
}

/// Convert a JSON query-parameter value to the matching [`IrLiteral`].
/// `serde_json::Number` doesn't distinguish int/float, so check `is_i64` first
/// (mirrors the Python binding's bool-before-int, int-before-float ordering).
fn json_to_ir_literal(value: &serde_json::Value) -> Result<IrLiteral> {
    use serde_json::Value;
    Ok(match value {
        Value::Null => IrLiteral::Null,
        Value::Bool(b) => IrLiteral::Bool(*b),
        Value::Number(n) if n.is_i64() => IrLiteral::Int(n.as_i64().unwrap()),
        Value::Number(n) => {
            IrLiteral::Float(n.as_f64().ok_or_else(|| {
                to_napi_err(&GfError::Validation("non-finite numeric param".into()))
            })?)
        }
        Value::String(s) => IrLiteral::Str(s.clone()),
        Value::Array(items) => IrLiteral::List(
            items
                .iter()
                .map(json_to_ir_literal)
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Object(entries) if entries.contains_key("$uuid") => {
            if entries.len() != 1 {
                return Err(to_napi_err(&GfError::Validation(
                    "UUID parameter tag must contain only $uuid".into(),
                )));
            }
            let text = entries["$uuid"].as_str().ok_or_else(|| {
                to_napi_err(&GfError::Validation(
                    "UUID parameter $uuid value must be a string".into(),
                ))
            })?;
            let uuid = uuid::Uuid::parse_str(text).map_err(|_| {
                to_napi_err(&GfError::Validation(
                    "UUID parameter must be canonical hyphenated UUID text".into(),
                ))
            })?;
            if uuid.hyphenated().to_string() != text {
                return Err(to_napi_err(&GfError::Validation(
                    "UUID parameter must be canonical hyphenated UUID text".into(),
                )));
            }
            IrLiteral::Uuid(*uuid.as_bytes())
        }
        Value::Object(entries) => IrLiteral::Map(
            entries
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_to_ir_literal(value)?)))
                .collect::<Result<Vec<_>>>()?,
        ),
    })
}

/// Build the `$param` map from an optional JS object (empty when omitted).
pub(super) fn params_from_map(
    params: Option<HashMap<String, serde_json::Value>>,
) -> Result<HashMap<String, IrLiteral>> {
    let mut out = HashMap::new();
    if let Some(map) = params {
        for (k, v) in &map {
            out.insert(k.clone(), json_to_ir_literal(v)?);
        }
    }
    Ok(out)
}

/// Convert one JSON construction value into the shared Rust property model.
pub(super) fn json_to_prop_value(value: &serde_json::Value) -> Result<PropValue> {
    use serde_json::Value;
    Ok(match value {
        Value::Null => PropValue::Null,
        Value::Bool(value) => PropValue::Bool(*value),
        Value::Number(value) if value.is_i64() => PropValue::Int(value.as_i64().unwrap()),
        Value::Number(value) if value.is_u64() => {
            return Err(to_napi_err(&GfError::Validation(
                "integer node property exceeds signed 64-bit range".into(),
            )));
        }
        Value::Number(value) => {
            PropValue::Float(value.as_f64().ok_or_else(|| {
                to_napi_err(&GfError::Validation("non-finite node property".into()))
            })?)
        }
        Value::String(value) => PropValue::Str(value.clone()),
        Value::Array(values) => PropValue::List(
            values
                .iter()
                .map(json_to_prop_value)
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Object(_) => {
            if looks_like_spatial_json(value) {
                let spatial: graphforge_api::SpatialValue = serde_json::from_value(value.clone())
                    .map_err(|error| {
                    to_napi_err(&GfError::Validation(format!(
                        "invalid canonical spatial property: {error}"
                    )))
                })?;
                spatial.validate_interchange_profile().map_err(|error| {
                    to_napi_err(&GfError::Validation(format!(
                        "invalid canonical spatial property: {error}"
                    )))
                })?;
                PropValue::Spatial(spatial)
            } else if looks_like_temporal_json(value) {
                let normalized = normalize_temporal_json_numbers(value.clone())?;
                let temporal: graphforge_api::TemporalValue = serde_json::from_value(normalized)
                    .map_err(|error| {
                        to_napi_err(&GfError::Validation(format!(
                            "invalid temporal node property: {error}"
                        )))
                    })?;
                temporal.validate().map_err(|error| to_napi_err(&error))?;
                PropValue::Temporal(temporal)
            } else {
                return Err(to_napi_err(&GfError::Validation(
                    UNSUPPORTED_PROP_TYPE_MSG.into(),
                )));
            }
        }
    })
}

fn looks_like_spatial_json(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(|object| {
        object.contains_key("spatial_type") || object.contains_key("coordinates")
    })
}

fn looks_like_temporal_json(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(|object| {
        object
            .get("type")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|kind| {
                matches!(
                    kind,
                    "date"
                        | "utc_date_time"
                        | "local_time"
                        | "offset_time"
                        | "local_date_time"
                        | "zoned_date_time"
                        | "duration"
                )
            })
    })
}

fn normalize_temporal_json_numbers(value: serde_json::Value) -> Result<serde_json::Value> {
    const MAX_SAFE_JS_INTEGER: f64 = 9_007_199_254_740_991.0;
    match value {
        serde_json::Value::Number(number) if number.is_f64() => {
            let value = number.as_f64().ok_or_else(|| {
                to_napi_err(&GfError::Validation("invalid temporal number".into()))
            })?;
            if !value.is_finite()
                || value.fract() != 0.0
                || !(-MAX_SAFE_JS_INTEGER..=MAX_SAFE_JS_INTEGER).contains(&value)
            {
                return Err(to_napi_err(&GfError::Validation(
                    "temporal numeric fields must be finite signed integers".into(),
                )));
            }
            #[allow(
                clippy::cast_possible_truncation,
                reason = "finite integral f64 range is checked above"
            )]
            Ok(serde_json::Value::Number((value as i64).into()))
        }
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(normalize_temporal_json_numbers)
            .collect::<Result<Vec<_>>>()
            .map(serde_json::Value::Array),
        serde_json::Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, normalize_temporal_json_numbers(value)?)))
            .collect::<Result<serde_json::Map<_, _>>>()
            .map(serde_json::Value::Object),
        value => Ok(value),
    }
}

pub(crate) fn props_from_map(
    props: Option<HashMap<String, serde_json::Value>>,
) -> Result<HashMap<String, PropValue>> {
    props
        .unwrap_or_default()
        .into_iter()
        .map(|(name, value)| Ok((name, json_to_prop_value(&value)?)))
        .collect()
}

const UNSUPPORTED_PROP_TYPE_MSG: &str = "unsupported node property type (expected null/boolean/number/string/array/temporal or canonical spatial object)";

/// Convert a JS property bag, raising a real `TypeError` for unsupported values
/// (functions, symbols, nested plain objects).
pub(super) fn props_from_js_object(
    env: Env,
    props: Option<Object>,
) -> Result<HashMap<String, PropValue>> {
    let Some(obj) = props else {
        return Ok(HashMap::new());
    };
    let keys = Object::keys(&obj).map_err(|error| {
        type_error(
            env,
            format!("failed to read node property keys: {}", error.reason),
        )
    })?;
    let mut out = HashMap::with_capacity(keys.len());
    for key in keys {
        let Some(value) = obj.get::<Unknown>(&key).map_err(|error| {
            type_error(
                env,
                format!("failed to read property `{key}`: {}", error.reason),
            )
        })?
        else {
            continue;
        };
        out.insert(key, js_unknown_to_prop_value(env, value)?);
    }
    Ok(out)
}

// SAFETY: each `cast` / `from_napi_value` follows an explicit `ValueType`
// check (or ClassInstance fallible coerce) so the napi value kind matches.
#[allow(unsafe_code)]
fn js_unknown_to_prop_value(env: Env, value: Unknown<'_>) -> Result<PropValue> {
    match value.get_type().map_err(|error| {
        type_error(
            env,
            format!("failed to inspect property value type: {}", error.reason),
        )
    })? {
        ValueType::Undefined | ValueType::Null => Ok(PropValue::Null),
        ValueType::Boolean => {
            let flag = unsafe { value.cast::<bool>() }.map_err(|error| {
                type_error(env, format!("expected boolean property: {}", error.reason))
            })?;
            Ok(PropValue::Bool(flag))
        }
        ValueType::Number => {
            let number = unsafe { value.cast::<f64>() }.map_err(|error| {
                type_error(env, format!("expected number property: {}", error.reason))
            })?;
            if number.is_finite() && number.fract() == 0.0 {
                // JS numbers are IEEE-754; whole values in the exact integer
                // range are stored as PropValue::Int.
                #[allow(
                    clippy::cast_precision_loss,
                    clippy::cast_possible_truncation,
                    reason = "JS Number is f64; whole values in i64 range are intentional"
                )]
                {
                    if number >= i64::MIN as f64 && number <= i64::MAX as f64 {
                        return Ok(PropValue::Int(number as i64));
                    }
                }
                return Err(to_napi_err(&GfError::Validation(
                    "integer node property exceeds signed 64-bit range".into(),
                )));
            }
            if !number.is_finite() {
                return Err(to_napi_err(&GfError::Validation(
                    "non-finite node property".into(),
                )));
            }
            Ok(PropValue::Float(number))
        }
        ValueType::String => {
            let text = unsafe { value.cast::<String>() }.map_err(|error| {
                type_error(env, format!("expected string property: {}", error.reason))
            })?;
            Ok(PropValue::Str(text))
        }
        ValueType::Object => {
            let object = unsafe { value.cast::<Object>() }.map_err(|error| {
                type_error(env, format!("expected object property: {}", error.reason))
            })?;
            let is_array = object.is_array().map_err(|error| {
                type_error(
                    env,
                    format!("failed to inspect array property: {}", error.reason),
                )
            })?;
            let json = unsafe {
                <serde_json::Value as FromNapiValue>::from_napi_value(env.raw(), object.raw())
            }
            .map_err(|error| {
                type_error(
                    env,
                    format!("failed to read object property: {}", error.reason),
                )
            })?;
            if !is_array && !looks_like_spatial_json(&json) && !looks_like_temporal_json(&json) {
                return Err(type_error(env, UNSUPPORTED_PROP_TYPE_MSG));
            }
            json_to_prop_value(&json)
        }
        ValueType::Function
        | ValueType::Symbol
        | ValueType::External
        | ValueType::Unknown
        | ValueType::BigInt => Err(type_error(env, UNSUPPORTED_PROP_TYPE_MSG)),
    }
}

// SAFETY: `from_napi_value` fallibly coerces a ClassInstance; failures become
// TypeError rather than a generic napi Error.
#[allow(unsafe_code)]
pub(super) fn node_handle_from_unknown<'env>(
    env: Env,
    value: Unknown<'env>,
    role: &str,
) -> Result<ClassInstance<'env, NodeHandle>> {
    unsafe { ClassInstance::<NodeHandle>::from_napi_value(env.raw(), value.raw()) }
        .map_err(|_| type_error(env, format!("expected NodeHandle for addEdge {role}")))
}

#[cfg(test)]
mod tests;
