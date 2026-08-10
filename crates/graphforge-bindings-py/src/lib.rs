//! GraphForge Python bindings via PyO3.
// PyO3 macros expand to unsafe FFI code; unsafe is permitted here but audited.
#![warn(unsafe_code)]

mod composite;

use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::pyarrow::{FromPyArrow, IntoPyArrow, Table, ToPyArrow};
use arrow::record_batch::RecordBatchReader;
use futures::StreamExt;
use graphforge_api::{
    AlgorithmEmbeddingDistance, AlgorithmEmbeddingNormalization,
    AlgorithmEmbeddingPublicationRequest, BulkEdgePublicationError, BulkNodePublicationError,
    CallerEmbeddingBatchRequest, CallerEmbeddingBatchRow, CallerEmbeddingDistance,
    CallerEmbeddingNormalization, CapabilityId, EmbeddingAnalyzeOptions, EmbeddingOptions,
    EmbeddingRefreshFailureClass, EmbeddingRefreshInspection, EmbeddingRefreshOutcomeStatus,
    EmbeddingRefreshProjectPolicy, EmbeddingRefreshSpacePolicy, EmbeddingRefreshWorkerState,
    EmbeddingSpaceFreshnessInspection, EmbeddingSpaceFreshnessState, EmbeddingSpaceInfo,
    EmbeddingSpaceProducer, EmbeddingSpaceReadDecision, EmbeddingTokenCountClass, ExecutionResult,
    FastRpOptions, FindDiagnostic, FindExecutionOptions, FindRerankOptions, GfError,
    GraphForgeOptions, GraphSageAggregator, GraphSageOptions, HashGnnOptions, InvocationDescriptor,
    InvocationError, IrLiteral, Node2VecOptions, NodeSelector, OpenRouterProviderSession,
    OpenRouterProviderSessionConfig, OpenRouterWireLimits, OperationId, ProjectWriteMode,
    PropValue, ProviderBatchLimits, ProviderCapabilities, ProviderCapability,
    ProviderEmbeddingDistance, ProviderEmbeddingNormalization, ProviderEmbeddingPlanInspection,
    ProviderEmbeddingPlanRequest, ProviderExecutionLimits, ProviderRequestLimits,
    RerankAdvisoryPolicy, RerankFailurePolicy, RuntimeGuard, SearchIndexOptions,
    SendableRecordBatchStream, TextIndexInspection, TokenCountClass, WriteContext,
    validate_embedding_options,
};
use pyo3::create_exception;
use pyo3::exceptions::{
    PyException, PyImportError, PyModuleNotFoundError, PyNotImplementedError, PyRuntimeWarning,
    PyTypeError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyInt, PyList};

// Python exception hierarchy (re-exported as `graphforge.exceptions`): a
// `GraphForgeError` base so callers can catch broadly, plus one subclass per
// `GfError` fault domain. `GfError::NotImplemented` maps to the builtin
// `NotImplementedError` (the idiomatic "pending" signal).
create_exception!(
    graphforge,
    GraphForgeError,
    PyException,
    "Base class for all GraphForge exceptions."
);
create_exception!(
    graphforge,
    ParseError,
    GraphForgeError,
    "The Cypher parser rejected the input; carries a `span` (offset, length)."
);
create_exception!(
    graphforge,
    PlanError,
    GraphForgeError,
    "The binder or query planner could not produce a valid plan."
);
create_exception!(
    graphforge,
    ExecutionError,
    GraphForgeError,
    "A runtime fault occurred during query execution."
);
create_exception!(
    graphforge,
    StorageError,
    GraphForgeError,
    "A storage I/O operation failed."
);
create_exception!(
    graphforge,
    LifecycleError,
    GraphForgeError,
    "An operation was invalid for the current instance lifecycle state."
);
create_exception!(
    graphforge,
    ValidationError,
    GraphForgeError,
    "Input failed validation at the API boundary."
);
create_exception!(
    graphforge,
    OntologyError,
    GraphForgeError,
    "An ontology file could not be loaded or applied."
);

/// Convert a [`GfError`] into the matching Python exception. `ParseError` and
/// binder failures ([`GfError::Bind`]) carry a `span` attribute — the
/// `(offset, length)` of the offending token, per the shim's `ParseError`
/// contract. Binder failures share the public `ParseError` / `GF_PARSE` domain.
pub(crate) fn to_pyerr(py: Python<'_>, err: &GfError) -> PyErr {
    let error = match err {
        GfError::Parse { msg, span } | GfError::Bind { msg, span } => {
            let e = PyErr::new::<ParseError, _>(msg.clone());
            let _ = e
                .value(py)
                .setattr("span", (span.start, span.end.saturating_sub(span.start)));
            e
        }
        GfError::Plan(m) => PyErr::new::<PlanError, _>(m.clone()),
        GfError::Execution(m) => PyErr::new::<ExecutionError, _>(m.clone()),
        GfError::Provider {
            class,
            provider,
            model,
        } => {
            let error = PyErr::new::<ExecutionError, _>(format!(
                "provider invocation failed: class={class} provider={provider} model={model}"
            ));
            let value = error.value(py);
            let _ = value.setattr("provider_class", class);
            let _ = value.setattr("provider", provider);
            let _ = value.setattr("model", model);
            error
        }
        GfError::Storage(m) => PyErr::new::<StorageError, _>(m.clone()),
        GfError::Project { message, .. } => PyErr::new::<StorageError, _>(message.clone()),
        GfError::Api { message, .. } => PyErr::new::<ValidationError, _>(message.clone()),
        GfError::Lifecycle(m) => PyErr::new::<LifecycleError, _>(m.clone()),
        GfError::Validation(m) => PyErr::new::<ValidationError, _>(m.clone()),
        GfError::Ontology(m) => PyErr::new::<OntologyError, _>(m.clone()),
        GfError::NotImplemented(name) => PyErr::new::<PyNotImplementedError, _>((*name).to_owned()),
    };
    let _ = error.value(py).setattr("code", err.code());
    error
}

fn to_py_invocation_error(py: Python<'_>, err: &InvocationError) -> PyErr {
    if let InvocationError::Graph(error) = err {
        return to_pyerr(py, error);
    }
    let error = PyErr::new::<ValidationError, _>(err.to_string());
    let _ = error.value(py).setattr("code", err.code());
    error
}

/// Convert a Python value to the matching [`IrLiteral`] for a query parameter.
/// `bool` is checked before `int` (Python `bool` is an `int` subclass).
fn py_to_ir_literal(v: &Bound<'_, PyAny>) -> PyResult<IrLiteral> {
    // bool is checked before int (Python `bool` is an `int` subclass). For an
    // `int`, extract `i64` directly so a value outside the i64 range surfaces as
    // an error rather than silently degrading to a lossy float.
    if v.is_none() {
        Ok(IrLiteral::Null)
    } else if v.is_instance(&v.py().import("uuid")?.getattr("UUID")?)? {
        let bytes = v.getattr("bytes")?.extract::<Vec<u8>>()?;
        Ok(IrLiteral::Uuid(bytes.try_into().map_err(|_| {
            PyTypeError::new_err("uuid.UUID bytes must contain exactly 16 bytes")
        })?))
    } else if let Ok(b) = v.extract::<bool>() {
        Ok(IrLiteral::Bool(b))
    } else if v.is_instance_of::<PyInt>() {
        Ok(IrLiteral::Int(v.extract::<i64>()?))
    } else if let Ok(f) = v.extract::<f64>() {
        Ok(IrLiteral::Float(f))
    } else if let Ok(s) = v.extract::<String>() {
        Ok(IrLiteral::Str(s))
    } else if let Ok(dict) = v.cast::<PyDict>() {
        let mut entries = Vec::with_capacity(dict.len());
        for (key, value) in dict {
            entries.push((key.extract::<String>()?, py_to_ir_literal(&value)?));
        }
        Ok(IrLiteral::Map(entries))
    } else if let Ok(list) = v.cast::<PyList>() {
        let mut items = Vec::with_capacity(list.len());
        for value in list {
            items.push(py_to_ir_literal(&value)?);
        }
        Ok(IrLiteral::List(items))
    } else {
        Err(PyTypeError::new_err(
            "unsupported query parameter type (expected None/bool/int/float/str/uuid.UUID/list/dict)",
        ))
    }
}

/// Convert one Python construction value into the shared Rust property model.
pub(crate) fn py_to_prop_value(value: &Bound<'_, PyAny>) -> PyResult<PropValue> {
    if value.is_none() {
        Ok(PropValue::Null)
    } else if let Ok(boolean) = value.extract::<bool>() {
        Ok(PropValue::Bool(boolean))
    } else if value.is_instance_of::<PyInt>() {
        Ok(PropValue::Int(value.extract::<i64>()?))
    } else if let Ok(float) = value.extract::<f64>() {
        Ok(PropValue::Float(float))
    } else if let Ok(string) = value.extract::<String>() {
        Ok(PropValue::Str(string))
    } else if let Ok(list) = value.cast::<PyList>() {
        list.iter()
            .map(|item| py_to_prop_value(&item))
            .collect::<PyResult<Vec<_>>>()
            .map(PropValue::List)
    } else {
        Err(PyTypeError::new_err(
            "unsupported node property type (expected None/bool/int/float/str/list)",
        ))
    }
}

pub(crate) fn props_from_dict(
    props: Option<&Bound<'_, PyDict>>,
) -> PyResult<HashMap<String, PropValue>> {
    let mut values = HashMap::new();
    if let Some(props) = props {
        for (name, value) in props {
            values.insert(name.extract::<String>()?, py_to_prop_value(&value)?);
        }
    }
    Ok(values)
}

/// Coerce public Python selector shapes without resolving graph state.
fn py_to_node_selector(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<NodeSelector> {
    if let Ok(handle) = value.extract::<PyRef<'_, PyNodeHandle>>() {
        return Ok(NodeSelector::Handle(handle.inner.clone()));
    }
    if let Ok(uuid) = value.extract::<String>() {
        return NodeSelector::uuid(&uuid).map_err(|error| to_pyerr(py, &error));
    }
    if let Ok(selector) = value.cast::<PyDict>() {
        if selector.len() != 3 {
            return Err(PyTypeError::new_err(
                "property selector must contain exactly label, property, and value",
            ));
        }
        let label = selector
            .get_item("label")?
            .ok_or_else(|| PyTypeError::new_err("property selector requires label"))?
            .extract::<String>()?;
        let property = selector
            .get_item("property")?
            .ok_or_else(|| PyTypeError::new_err("property selector requires property"))?
            .extract::<String>()?;
        let value = selector
            .get_item("value")?
            .ok_or_else(|| PyTypeError::new_err("property selector requires value"))?;
        return Ok(NodeSelector::Match {
            label,
            property,
            value: py_to_prop_value(&value)?,
        });
    }
    Err(PyTypeError::new_err(
        "node selector must be a UUID string, NodeHandle, or label/property/value dict",
    ))
}

/// Coerce Python keyword representations, then delegate all variant semantics
/// to the shared Rust search-index option boundary.
fn search_index_options_from_kwargs(
    py: Python<'_>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<SearchIndexOptions> {
    let mut properties = None;
    let mut rebuild = None;
    let mut node = None;
    let mut vector = None;
    let mut space = None;

    if let Some(kwargs) = kwargs {
        for (name, value) in kwargs {
            match name.extract::<String>()?.as_str() {
                "properties" => {
                    properties = Some(if value.is_none() {
                        None
                    } else {
                        Some(value.extract::<Vec<String>>()?)
                    });
                }
                "rebuild" => rebuild = Some(value.extract::<bool>()?),
                "node" => {
                    node =
                        Some(py_to_node_selector(py, &value).map_err(|error| {
                            to_pyerr(py, &GfError::Validation(error.to_string()))
                        })?);
                }
                "vector" => vector = Some(value.extract::<Vec<f32>>()?),
                "space" => space = Some(value.extract::<String>()?),
                unknown => {
                    return Err(to_pyerr(
                        py,
                        &GfError::Validation(format!("unknown search index keyword {unknown:?}")),
                    ));
                }
            }
        }
    }

    SearchIndexOptions::from_binding_fields(properties, rebuild, node, vector, space)
        .map_err(|error| to_pyerr(py, &error))
}

fn caller_embedding_rows(
    py: Python<'_>,
    rows: &Bound<'_, PyList>,
) -> PyResult<Vec<CallerEmbeddingBatchRow>> {
    rows.iter()
        .map(|row| {
            let row = row
                .cast::<PyDict>()
                .map_err(|_| PyTypeError::new_err("caller embedding rows must be dictionaries"))?;
            if row.len() != 2 {
                return Err(PyTypeError::new_err(
                    "caller embedding row must contain exactly node and vector",
                ));
            }
            let node = row
                .get_item("node")?
                .ok_or_else(|| PyTypeError::new_err("caller embedding row requires node"))?;
            let vector = row
                .get_item("vector")?
                .ok_or_else(|| PyTypeError::new_err("caller embedding row requires vector"))?
                .extract::<Vec<f32>>()?;
            Ok(CallerEmbeddingBatchRow {
                node: py_to_node_selector(py, &node)?,
                vector,
            })
        })
        .collect()
}

fn string_map(values: &Bound<'_, PyDict>) -> PyResult<BTreeMap<String, String>> {
    values
        .iter()
        .map(|(key, value)| Ok((key.extract::<String>()?, value.extract::<String>()?)))
        .collect()
}

fn py_to_json_value(value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if value.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Ok(boolean) = value.extract::<bool>() {
        Ok(serde_json::Value::Bool(boolean))
    } else if value.is_instance_of::<PyInt>() {
        Ok(serde_json::Value::Number(value.extract::<i64>()?.into()))
    } else if let Ok(number) = value.extract::<f64>() {
        serde_json::Number::from_f64(number)
            .map(serde_json::Value::Number)
            .ok_or_else(|| PyTypeError::new_err("JSON numbers must be finite"))
    } else if let Ok(string) = value.extract::<String>() {
        Ok(serde_json::Value::String(string))
    } else if let Ok(list) = value.cast::<PyList>() {
        list.iter()
            .map(|item| py_to_json_value(&item))
            .collect::<PyResult<Vec<_>>>()
            .map(serde_json::Value::Array)
    } else if let Ok(dict) = value.cast::<PyDict>() {
        dict.iter()
            .map(|(key, value)| Ok((key.extract::<String>()?, py_to_json_value(&value)?)))
            .collect::<PyResult<serde_json::Map<_, _>>>()
            .map(serde_json::Value::Object)
    } else {
        Err(PyTypeError::new_err(
            "unsupported JSON value (expected None/bool/int/float/str/list/dict)",
        ))
    }
}

fn json_value_to_python(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    Ok(match value {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(value) => value.into_pyobject(py)?.to_owned().unbind().into_any(),
        serde_json::Value::Number(value) if value.is_i64() => value
            .as_i64()
            .expect("checked")
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        serde_json::Value::Number(value) if value.is_u64() => value
            .as_u64()
            .expect("checked")
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        serde_json::Value::Number(value) => value
            .as_f64()
            .expect("JSON number")
            .into_pyobject(py)?
            .into_any()
            .unbind(),
        serde_json::Value::String(value) => value.into_pyobject(py)?.into_any().unbind(),
        serde_json::Value::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(json_value_to_python(py, value)?)?;
            }
            list.into_any().unbind()
        }
        serde_json::Value::Object(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, json_value_to_python(py, value)?)?;
            }
            dict.into_any().unbind()
        }
    })
}

fn ontology_mode(value: &str) -> Result<graphforge_api::OntologyMode, GfError> {
    match value {
        "advisory" => Ok(graphforge_api::OntologyMode::Advisory),
        "strict" => Ok(graphforge_api::OntologyMode::Strict),
        _ => Err(GfError::Validation(
            "ontology mode must be advisory or strict".into(),
        )),
    }
}

fn ontology_export_format(value: &str) -> Result<graphforge_api::OntologyExportFormat, GfError> {
    match value {
        "yaml" | "yml" => Ok(graphforge_api::OntologyExportFormat::Yaml),
        "json" => Ok(graphforge_api::OntologyExportFormat::Json),
        _ => Err(GfError::Validation(
            "ontology export format must be yaml or json".into(),
        )),
    }
}

fn json_map(values: Option<&Bound<'_, PyDict>>) -> PyResult<BTreeMap<String, serde_json::Value>> {
    let mut mapped = BTreeMap::new();
    if let Some(values) = values {
        for (key, value) in values {
            mapped.insert(key.extract::<String>()?, py_to_json_value(&value)?);
        }
    }
    Ok(mapped)
}

fn pyarrow_table_to_batch(value: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let (batches, schema) = Table::from_pyarrow_bound(value)?.into_inner();
    if batches.is_empty() {
        Ok(RecordBatch::new_empty(schema))
    } else {
        arrow::compute::concat_batches(&schema, &batches)
            .map_err(|error| PyTypeError::new_err(error.to_string()))
    }
}

/// Normalize supported Python bulk containers into one Arrow record batch.
///
/// Accepted forms: `pyarrow.Table`, Arrow-compatible DataFrame (`to_arrow` /
/// pandas via `pyarrow.Table.from_pandas`), and `list[dict]` via
/// `pyarrow.Table.from_pylist`. Ontology, identity, and publication stay in Rust.
fn py_bulk_input_to_batch(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    if let Ok(batch) = pyarrow_table_to_batch(value) {
        return Ok(batch);
    }

    let pa = py.import("pyarrow")?;
    let table_cls = pa.getattr("Table")?;

    if let Ok(to_arrow) = value.getattr("to_arrow")
        && to_arrow.is_callable()
    {
        let table = to_arrow.call0()?;
        return pyarrow_table_to_batch(&table);
    }

    if (value.hasattr("__dataframe__")?
        || value
            .get_type()
            .name()
            .is_ok_and(|name| name == "DataFrame"))
        && let Ok(table) = table_cls.call_method1("from_pandas", (value,))
    {
        return pyarrow_table_to_batch(&table);
    }

    if value.is_instance_of::<pyo3::types::PyList>() {
        let table = table_cls.call_method1("from_pylist", (value,))?;
        return pyarrow_table_to_batch(&table);
    }

    Err(PyTypeError::new_err(
        "bulk construction data must be a pyarrow.Table, Arrow-compatible DataFrame, or list[dict]",
    ))
}

fn bulk_contract_metadata<'py>(
    py: Python<'py>,
    kind: &str,
) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
    let metadata = pyo3::types::PyDict::new(py);
    metadata.set_item("graphforge.bulk_contract_version", "1")?;
    metadata.set_item("graphforge.bulk_kind", kind)?;
    metadata.set_item("graphforge.row_order", "logical_input_order")?;
    Ok(metadata)
}

fn null_uuid_array(py: Python<'_>, n: usize) -> PyResult<Bound<'_, PyAny>> {
    let pa = py.import("pyarrow")?;
    let nulls = pa.getattr("nulls")?.call1((n,))?;
    nulls.call_method1("cast", (pa.getattr("binary")?.call1((16,))?,))
}

fn cast_uuid_column<'py>(
    py: Python<'py>,
    table: &Bound<'py, PyAny>,
    name: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let pa = py.import("pyarrow")?;
    let binary16 = pa.getattr("binary")?.call1((16,))?;
    let column = table.call_method1("column", (name,))?;
    if column.getattr("type")?.eq(&binary16)? {
        return Ok(column);
    }
    // Accept null / binary / utf8 UUID text by going through pylist of bytes|None.
    let values = column.call_method0("to_pylist")?;
    let normalized = pyo3::types::PyList::empty(py);
    for item in values.try_iter()? {
        let item = item?;
        if item.is_none() {
            normalized.append(py.None())?;
            continue;
        }
        if let Ok(bytes) = item.extract::<&[u8]>() {
            if bytes.len() != 16 {
                return Err(PyTypeError::new_err(format!(
                    "bulk {name} values must be 16-byte UUIDs"
                )));
            }
            normalized.append(bytes)?;
            continue;
        }
        let text = item.extract::<&str>()?;
        let parsed = py
            .import("uuid")?
            .getattr("UUID")?
            .call1((text,))
            .map_err(|error| {
                PyTypeError::new_err(format!("bulk {name} value is not a UUID: {error}"))
            })?;
        normalized.append(parsed.getattr("bytes")?)?;
    }
    pa.getattr("array")?
        .call1((normalized,))?
        .call_method1("cast", (binary16,))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "PyO3 Bound receivers are conventionally passed by value at call sites"
)]
fn select_canonical_table(
    py: Python<'_>,
    table: Bound<'_, PyAny>,
    required: &[(&str, bool)],
    kind: &str,
) -> PyResult<RecordBatch> {
    let pa = py.import("pyarrow")?;
    let names: Vec<String> = table.getattr("column_names")?.extract()?;
    for (name, _) in required {
        if !names.iter().any(|existing| existing == name) {
            return Err(PyTypeError::new_err(format!(
                "bulk {kind} data is missing required column {name:?}"
            )));
        }
    }
    let mut property_names: Vec<String> = names
        .into_iter()
        .filter(|name| !required.iter().any(|(required, _)| required == name))
        .collect();
    property_names.sort();

    let arrays = pyo3::types::PyList::empty(py);
    let fields = pyo3::types::PyList::empty(py);
    for (name, nullable) in required {
        let column = if name.ends_with("_uuid") {
            cast_uuid_column(py, &table, name)?
        } else {
            table.call_method1("column", (name,))?
        };
        arrays.append(&column)?;
        fields.append(pa.getattr("field")?.call1((
            *name,
            column.getattr("type")?,
            *nullable,
        ))?)?;
    }
    for name in &property_names {
        let column = table.call_method1("column", (name,))?;
        arrays.append(&column)?;
        fields.append(pa.getattr("field")?.call1((
            name.as_str(),
            column.getattr("type")?,
            true,
        ))?)?;
    }
    let metadata = bulk_contract_metadata(py, kind)?;
    let schema = pa
        .getattr("schema")?
        .call1((fields,))?
        .call_method1("with_metadata", (metadata,))?;
    let kwargs = pyo3::types::PyDict::new(py);
    kwargs.set_item("schema", schema)?;
    let rebuilt = pa
        .getattr("Table")?
        .call_method("from_arrays", (arrays,), Some(&kwargs))?;
    pyarrow_table_to_batch(&rebuilt)
}

fn ensure_bulk_node_batch(
    py: Python<'_>,
    label: &str,
    data: &Bound<'_, PyAny>,
) -> PyResult<RecordBatch> {
    let batch = py_bulk_input_to_batch(py, data)?;
    let pa = py.import("pyarrow")?;
    let mut table = record_batch_to_pyarrow_table(py, &batch)?;
    let bound = table.bind(py);
    let names: Vec<String> = bound.getattr("column_names")?.extract()?;
    let n = batch.num_rows();

    if !names.iter().any(|name| name == "node_uuid") {
        table = bound
            .call_method1("append_column", ("node_uuid", null_uuid_array(py, n)?))?
            .unbind();
    }
    let bound = table.bind(py);
    let names: Vec<String> = bound.getattr("column_names")?.extract()?;

    if names.iter().any(|name| name == "label") {
        let labels = bound
            .call_method1("column", ("label",))?
            .call_method0("to_pylist")?;
        for item in labels.try_iter()? {
            let item = item?;
            if item.is_none() {
                continue;
            }
            let text = item.extract::<&str>()?;
            if text != label {
                return Err(PyTypeError::new_err(format!(
                    "bulk node label column value {text:?} does not match add_nodes label {label:?}"
                )));
            }
        }
    } else {
        let labels = pa.getattr("array")?.call1((vec![label.to_owned(); n],))?;
        table = bound
            .call_method1("append_column", ("label", labels))?
            .unbind();
    }

    select_canonical_table(
        py,
        table.bind(py).clone(),
        &[("node_uuid", true), ("label", false)],
        "node",
    )
}

fn ensure_bulk_edge_batch(
    py: Python<'_>,
    rel_type: &str,
    data: &Bound<'_, PyAny>,
    src: &str,
    dst: &str,
) -> PyResult<RecordBatch> {
    let batch = py_bulk_input_to_batch(py, data)?;
    let pa = py.import("pyarrow")?;
    let mut table = record_batch_to_pyarrow_table(py, &batch)?;
    let bound = table.bind(py);
    let names: Vec<String> = bound.getattr("column_names")?.extract()?;
    let n = batch.num_rows();

    if !names.iter().any(|name| name == "edge_uuid") {
        table = bound
            .call_method1("append_column", ("edge_uuid", null_uuid_array(py, n)?))?
            .unbind();
    }
    let bound = table.bind(py);
    let names: Vec<String> = bound.getattr("column_names")?.extract()?;

    if names.iter().any(|name| name == "rel_type") {
        let rels = bound
            .call_method1("column", ("rel_type",))?
            .call_method0("to_pylist")?;
        for item in rels.try_iter()? {
            let item = item?;
            if item.is_none() {
                continue;
            }
            let text = item.extract::<&str>()?;
            if text != rel_type {
                return Err(PyTypeError::new_err(format!(
                    "bulk edge rel_type column value {text:?} does not match add_edges rel_type {rel_type:?}"
                )));
            }
        }
    } else {
        let rels = pa
            .getattr("array")?
            .call1((vec![rel_type.to_owned(); n],))?;
        table = bound
            .call_method1("append_column", ("rel_type", rels))?
            .unbind();
    }

    let bound = table.bind(py);
    let names: Vec<String> = bound.getattr("column_names")?.extract()?;
    if !names.iter().any(|name| name == "source_uuid") {
        if !names.iter().any(|name| name == src) {
            return Err(PyTypeError::new_err(format!(
                "bulk edge data must include source_uuid or {src:?} endpoint column"
            )));
        }
        table = bound
            .call_method1(
                "rename_columns",
                (rename_map(py, &names, src, "source_uuid")?,),
            )?
            .unbind();
    }
    let bound = table.bind(py);
    let names: Vec<String> = bound.getattr("column_names")?.extract()?;
    if !names.iter().any(|name| name == "target_uuid") {
        if !names.iter().any(|name| name == dst) {
            return Err(PyTypeError::new_err(format!(
                "bulk edge data must include target_uuid or {dst:?} endpoint column"
            )));
        }
        table = bound
            .call_method1(
                "rename_columns",
                (rename_map(py, &names, dst, "target_uuid")?,),
            )?
            .unbind();
    }

    select_canonical_table(
        py,
        table.bind(py).clone(),
        &[
            ("edge_uuid", true),
            ("rel_type", false),
            ("source_uuid", false),
            ("target_uuid", false),
        ],
        "edge",
    )
}

fn rename_map<'py>(
    py: Python<'py>,
    names: &[String],
    from: &str,
    to: &str,
) -> PyResult<Bound<'py, pyo3::types::PyList>> {
    let renamed = pyo3::types::PyList::empty(py);
    for name in names {
        if name == from {
            renamed.append(to)?;
        } else {
            renamed.append(name.as_str())?;
        }
    }
    Ok(renamed)
}

fn embedding_space_to_python(py: Python<'_>, space: EmbeddingSpaceInfo) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("compatibility_id", space.compatibility_id)?;
    value.set_item("aliases", space.aliases)?;
    value.set_item("default_alias", space.default_alias)?;
    value.set_item("dimensions", space.dimensions)?;

    let producer = PyDict::new(py);
    match space.producer {
        EmbeddingSpaceProducer::Algorithm {
            algorithm,
            algorithm_version,
        } => {
            producer.set_item("kind", "algorithm")?;
            producer.set_item("algorithm", algorithm)?;
            producer.set_item("algorithm_version", algorithm_version)?;
        }
        EmbeddingSpaceProducer::Local {
            implementation,
            model,
            revision,
            contract_version,
        } => {
            producer.set_item("kind", "local")?;
            producer.set_item("implementation", implementation)?;
            producer.set_item("model", model)?;
            producer.set_item("revision", revision)?;
            producer.set_item("contract_version", contract_version)?;
        }
        EmbeddingSpaceProducer::Callback {
            callback_contract,
            contract_version,
        } => {
            producer.set_item("kind", "callback")?;
            producer.set_item("callback_contract", callback_contract)?;
            producer.set_item("contract_version", contract_version)?;
        }
        EmbeddingSpaceProducer::Remote {
            provider,
            model,
            revision,
            response_contract_version,
        } => {
            producer.set_item("kind", "remote")?;
            producer.set_item("provider", provider)?;
            producer.set_item("model", model)?;
            producer.set_item("revision", revision)?;
            producer.set_item("response_contract_version", response_contract_version)?;
        }
        EmbeddingSpaceProducer::CallerSupplied { contract_version } => {
            producer.set_item("kind", "caller_supplied")?;
            producer.set_item("contract_version", contract_version)?;
        }
    }
    value.set_item("producer", producer)?;

    let tokenizer = space.tokenizer.map(|tokenizer| {
        let value = PyDict::new(py);
        value.set_item("identifier", tokenizer.identifier)?;
        value.set_item("version", tokenizer.version)?;
        value.set_item(
            "count_class",
            match tokenizer.count_class {
                EmbeddingTokenCountClass::ExactLocal => "exact_local",
                EmbeddingTokenCountClass::ProviderReported => "provider_reported",
                EmbeddingTokenCountClass::Approximate => "approximate",
            },
        )?;
        value.set_item("max_input_tokens", tokenizer.max_input_tokens)?;
        value.set_item("normalization", tokenizer.normalization)?;
        Ok::<_, PyErr>(value)
    });
    value.set_item("tokenizer", tokenizer.transpose()?)?;

    let chunking = space.chunking.map(|chunking| {
        let value = PyDict::new(py);
        value.set_item("chunk_size_tokens", chunking.chunk_size_tokens)?;
        value.set_item("overlap_tokens", chunking.overlap_tokens)?;
        value.set_item("aggregation", chunking.aggregation)?;
        value.set_item("truncation_policy", chunking.truncation_policy)?;
        Ok::<_, PyErr>(value)
    });
    value.set_item("chunking", chunking.transpose()?)?;

    let active = space.active.map(|active| {
        let value = PyDict::new(py);
        value.set_item("generation_id", active.generation_id)?;
        value.set_item("vector_count", active.vector_count)?;
        value.set_item("source_graph_generation", active.source_graph_generation)?;
        value.set_item("source_fingerprint", active.source_fingerprint)?;
        value.set_item("generated_at_micros", active.generated_at_micros)?;
        value.set_item("committed_at_micros", active.committed_at_micros)?;
        Ok::<_, PyErr>(value)
    });
    value.set_item("active", active.transpose()?)?;
    Ok(value.into_any().unbind())
}

fn refresh_project_policy_to_python(
    py: Python<'_>,
    policy: EmbeddingRefreshProjectPolicy,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("proactive", policy.proactive)?;
    value.set_item("debounce_millis", policy.debounce.as_millis())?;
    value.set_item("max_concurrent_jobs", policy.max_concurrent_jobs)?;
    Ok(value.into_any().unbind())
}

fn refresh_space_policy_to_python(
    py: Python<'_>,
    policy: EmbeddingRefreshSpacePolicy,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("proactive", policy.proactive)?;
    value.set_item(
        "debounce_millis",
        policy.debounce.map(|duration| duration.as_millis()),
    )?;
    Ok(value.into_any().unbind())
}

fn refresh_freshness_to_python(
    py: Python<'_>,
    freshness: EmbeddingSpaceFreshnessInspection,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("compatibility_id", freshness.compatibility_id)?;
    value.set_item("generation_id", freshness.generation_id)?;
    value.set_item(
        "state",
        match freshness.state {
            EmbeddingSpaceFreshnessState::Fresh => "fresh",
            EmbeddingSpaceFreshnessState::Stale => "stale",
            EmbeddingSpaceFreshnessState::SubstantiallyStale => "substantially_stale",
        },
    )?;
    value.set_item("reason", freshness.reason)?;
    let decision = PyDict::new(py);
    match freshness.decision {
        EmbeddingSpaceReadDecision::ServeFresh => {
            decision.set_item("kind", "serve_fresh")?;
        }
        EmbeddingSpaceReadDecision::ServeStale { reason } => {
            decision.set_item("kind", "serve_stale")?;
            decision.set_item("reason", reason)?;
        }
        EmbeddingSpaceReadDecision::RefreshRequired { reason } => {
            decision.set_item("kind", "refresh_required")?;
            decision.set_item("reason", reason)?;
        }
        EmbeddingSpaceReadDecision::ServeForcedStale { diagnostic } => {
            decision.set_item("kind", "serve_forced_stale")?;
            decision.set_item("diagnostic", diagnostic)?;
        }
    }
    value.set_item("decision", decision)?;
    Ok(value.into_any().unbind())
}

fn refresh_failure_token(failure: EmbeddingRefreshFailureClass) -> &'static str {
    match failure {
        EmbeddingRefreshFailureClass::Provider => "provider",
        EmbeddingRefreshFailureClass::Validation => "validation",
        EmbeddingRefreshFailureClass::ResourceExhausted => "resource_exhausted",
        EmbeddingRefreshFailureClass::Storage => "storage",
        EmbeddingRefreshFailureClass::ConcurrentMutation => "concurrent_mutation",
        EmbeddingRefreshFailureClass::Incompatible => "incompatible",
        EmbeddingRefreshFailureClass::Corrupt => "corrupt",
        EmbeddingRefreshFailureClass::Unavailable => "unavailable",
    }
}

fn refresh_inspection_to_python(
    py: Python<'_>,
    inspection: EmbeddingRefreshInspection,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("compatibility_id", inspection.compatibility_id)?;
    value.set_item(
        "project_policy",
        refresh_project_policy_to_python(py, inspection.project_policy)?,
    )?;
    value.set_item(
        "space_policy",
        inspection
            .space_policy
            .map(|policy| refresh_space_policy_to_python(py, policy))
            .transpose()?,
    )?;
    let resolved = PyDict::new(py);
    resolved.set_item("proactive", inspection.resolved_policy.proactive)?;
    resolved.set_item(
        "debounce_millis",
        inspection.resolved_policy.debounce.as_millis(),
    )?;
    resolved.set_item(
        "max_concurrent_jobs",
        inspection.resolved_policy.max_concurrent_jobs,
    )?;
    value.set_item("resolved_policy", resolved)?;
    let outcome = inspection.last_outcome.map(|outcome| {
        let value = PyDict::new(py);
        match outcome.status {
            EmbeddingRefreshOutcomeStatus::Succeeded => value.set_item("status", "succeeded")?,
            EmbeddingRefreshOutcomeStatus::Cancelled => value.set_item("status", "cancelled")?,
            EmbeddingRefreshOutcomeStatus::Failed(failure) => {
                value.set_item("status", "failed")?;
                value.set_item("failure_class", refresh_failure_token(failure))?;
            }
        }
        value.set_item("graph_generation", outcome.graph_generation)?;
        value.set_item("source_fingerprint", outcome.source_fingerprint.to_hex())?;
        value.set_item("completed_at_micros", outcome.completed_at_micros)?;
        Ok::<_, PyErr>(value)
    });
    value.set_item("last_outcome", outcome.transpose()?)?;
    value.set_item(
        "freshness",
        inspection
            .freshness
            .map(|freshness| refresh_freshness_to_python(py, freshness))
            .transpose()?,
    )?;
    let worker = PyDict::new(py);
    worker.set_item(
        "state",
        match inspection.worker.state {
            EmbeddingRefreshWorkerState::Running => "running",
            EmbeddingRefreshWorkerState::Shutdown => "shutdown",
        },
    )?;
    worker.set_item("queued_lineages", inspection.worker.queued_lineages)?;
    worker.set_item("in_flight_lineages", inspection.worker.in_flight_lineages)?;
    worker.set_item(
        "selected_lineage_queued",
        inspection.worker.selected_lineage_queued,
    )?;
    worker.set_item(
        "selected_lineage_in_flight",
        inspection.worker.selected_lineage_in_flight,
    )?;
    worker.set_item("coalesced_notices", inspection.worker.coalesced_notices)?;
    worker.set_item("succeeded", inspection.worker.succeeded)?;
    worker.set_item("failed", inspection.worker.failed)?;
    worker.set_item("cancelled", inspection.worker.cancelled)?;
    value.set_item("worker", worker)?;
    Ok(value.into_any().unbind())
}

fn text_index_inspection_to_python(
    py: Python<'_>,
    inspection: TextIndexInspection,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item(
        "project_generation_uuid",
        inspection.project_generation_uuid.to_string(),
    )?;
    value.set_item("properties", inspection.properties)?;
    value.set_item("source_generation", inspection.source_generation)?;
    value.set_item("source_fingerprint", inspection.source_fingerprint)?;
    value.set_item("artifact_generation", inspection.artifact_generation)?;
    value.set_item(
        "artifact_source_generation",
        inspection.artifact_source_generation,
    )?;
    value.set_item(
        "artifact_source_fingerprint",
        inspection.artifact_source_fingerprint,
    )?;
    value.set_item("state", inspection.state.as_str())?;
    value.set_item(
        "reason",
        inspection
            .reason
            .map(graphforge_api::TextIndexFreshnessReason::as_str),
    )?;
    Ok(value.into_any().unbind())
}

fn adjacency_inspection_to_python(
    py: Python<'_>,
    inspection: graphforge_api::AdjacencyInspection,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item(
        "artifact_effective_generation",
        inspection.artifact_effective_generation,
    )?;
    value.set_item("artifact_fingerprint", inspection.artifact_fingerprint)?;
    value.set_item(
        "artifact_source_generation",
        inspection.artifact_source_generation,
    )?;
    value.set_item(
        "project_generation_uuid",
        inspection.project_generation_uuid.to_string(),
    )?;
    value.set_item(
        "reason",
        inspection
            .reason
            .map(graphforge_api::AdjacencyFreshnessReason::as_str),
    )?;
    value.set_item(
        "source_topology_fingerprint",
        inspection.source_topology_fingerprint,
    )?;
    value.set_item(
        "source_topology_generation",
        inspection.source_topology_generation,
    )?;
    value.set_item("state", inspection.state.as_str())?;
    Ok(value.into_any().unbind())
}

fn parse_terminal_uuids(values: &[String]) -> Result<Vec<[u8; 16]>, GfError> {
    let mut terminals = Vec::new();
    terminals.try_reserve_exact(values.len()).map_err(|_| {
        GfError::Execution("Steiner terminal allocation exceeds available memory".into())
    })?;
    for value in values {
        if value.len() != 36 {
            return Err(GfError::Validation(format!(
                "invalid Steiner terminal UUID {value:?}"
            )));
        }
        let NodeSelector::Uuid(uuid) = NodeSelector::uuid(value)? else {
            unreachable!("UUID parser always constructs a UUID selector")
        };
        if uuid.hyphenated().to_string() != *value {
            return Err(GfError::Validation(format!(
                "invalid Steiner terminal UUID {value:?}"
            )));
        }
        terminals.push(*uuid.as_bytes());
    }
    Ok(terminals)
}

pub(crate) fn canonical_operation_id(value: &str) -> Result<OperationId, GfError> {
    if value.len() != 36 {
        return Err(GfError::Validation(format!("invalid UUID {value:?}")));
    }
    let NodeSelector::Uuid(uuid) = NodeSelector::uuid(value)? else {
        unreachable!("UUID parser always constructs a UUID selector")
    };
    if uuid.hyphenated().to_string() != value {
        return Err(GfError::Validation(format!("invalid UUID {value:?}")));
    }
    Ok(OperationId(uuid))
}

fn py_operation_id(value: &Bound<'_, PyAny>) -> Result<OperationId, GfError> {
    let value = value
        .str()
        .map_err(|error| GfError::Validation(error.to_string()))?;
    canonical_operation_id(
        value
            .to_str()
            .map_err(|error| GfError::Validation(error.to_string()))?,
    )
}

fn assertion_status(value: &str) -> Result<graphforge_api::AssertionStatus, GfError> {
    match value {
        "hypothesis" => Ok(graphforge_api::AssertionStatus::Hypothesis),
        "supported" => Ok(graphforge_api::AssertionStatus::Supported),
        "refuted" => Ok(graphforge_api::AssertionStatus::Refuted),
        "disputed" => Ok(graphforge_api::AssertionStatus::Disputed),
        "retracted" => Ok(graphforge_api::AssertionStatus::Retracted),
        "superseded" => Ok(graphforge_api::AssertionStatus::Superseded),
        _ => Err(GfError::Validation("unknown assertion status".into())),
    }
}

fn parse_capability_id(value: &str) -> Result<CapabilityId, GfError> {
    match value {
        "graph" => Ok(CapabilityId::Graph),
        "provenance" => Ok(CapabilityId::Provenance),
        "knowledge" => Ok(CapabilityId::Knowledge),
        "epistemic" => Ok(CapabilityId::Epistemic),
        "valid_time" => Ok(CapabilityId::ValidTime),
        _ => Err(GfError::Validation(format!("unknown capability {value:?}"))),
    }
}

fn py_assertion_graph_ref(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<graphforge_api::AssertionGraphRefInput> {
    let value = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("graph_refs entries must be dictionaries"))?;
    let field = |name: &str| {
        value
            .get_item(name)?
            .ok_or_else(|| PyTypeError::new_err(format!("graph_refs entry requires {name}")))
    };
    let graph_uuid = canonical_operation_id(&field("graph_uuid")?.extract::<String>()?)
        .map_err(|error| to_pyerr(py, &error))?
        .0;
    let graph_kind = match field("graph_kind")?.extract::<String>()?.as_str() {
        "node" => graphforge_api::GraphObjectKind::Node,
        "edge" => graphforge_api::GraphObjectKind::Edge,
        _ => {
            return Err(PyTypeError::new_err("graph_kind must be 'node' or 'edge'"));
        }
    };
    let role = match field("role")?.extract::<String>()?.as_str() {
        "subject" => graphforge_api::AssertionGraphRole::Subject,
        "object" => graphforge_api::AssertionGraphRole::Object,
        "context" => graphforge_api::AssertionGraphRole::Context,
        _ => {
            return Err(PyTypeError::new_err(
                "role must be 'subject', 'object', or 'context'",
            ));
        }
    };
    let ordinal = field("ordinal")?.extract::<u32>()?;
    Ok(graphforge_api::AssertionGraphRefInput {
        graph_uuid,
        graph_kind,
        role,
        ordinal,
    })
}

fn py_evidence_input(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<graphforge_api::EvidenceInput> {
    let value = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("evidence entries must be dictionaries"))?;
    let field = |name: &str| {
        value
            .get_item(name)?
            .ok_or_else(|| PyTypeError::new_err(format!("evidence entry requires {name}")))
    };
    let evidence_uuid = canonical_operation_id(&field("evidence_uuid")?.extract::<String>()?)
        .map_err(|error| to_pyerr(py, &error))?
        .0;
    let source_uuid = canonical_operation_id(&field("source_uuid")?.extract::<String>()?)
        .map_err(|error| to_pyerr(py, &error))?
        .0;
    let source_kind = match field("source_kind")?.extract::<String>()?.as_str() {
        "document" => graphforge_api::EvidenceSourceKind::Document,
        "observation" => graphforge_api::EvidenceSourceKind::Observation,
        "graph_node" => graphforge_api::EvidenceSourceKind::GraphNode,
        "graph_edge" => graphforge_api::EvidenceSourceKind::GraphEdge,
        _ => return Err(PyTypeError::new_err("unknown evidence source kind")),
    };
    let role = match field("role")?.extract::<String>()?.as_str() {
        "supports" => graphforge_api::EvidenceRole::Supports,
        "contradicts" => graphforge_api::EvidenceRole::Contradicts,
        "context" => graphforge_api::EvidenceRole::Context,
        _ => return Err(PyTypeError::new_err("unknown evidence role")),
    };
    let weight = value
        .get_item("weight")?
        .filter(|value| !value.is_none())
        .map(|value| value.extract::<f64>())
        .transpose()?;
    Ok(graphforge_api::EvidenceInput {
        evidence_uuid,
        source_uuid,
        source_kind,
        role,
        weight,
    })
}

fn embedding_validation(py: Python<'_>, message: impl Into<String>) -> PyErr {
    to_pyerr(py, &GfError::Validation(message.into()))
}

fn embedding_count(
    py: Python<'_>,
    algorithm: &str,
    name: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<usize> {
    let value = value
        .extract::<i128>()
        .map_err(|_| embedding_validation(py, format!("{algorithm} {name} must be an integer")))?;
    usize::try_from(value)
        .map_err(|_| embedding_validation(py, format!("{algorithm} {name} must be nonnegative")))
}

fn embedding_counts(
    py: Python<'_>,
    algorithm: &str,
    name: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<Vec<usize>> {
    value
        .extract::<Vec<i128>>()
        .map_err(|_| {
            embedding_validation(py, format!("{algorithm} {name} must be integer values"))
        })?
        .into_iter()
        .map(|value| {
            usize::try_from(value).map_err(|_| {
                embedding_validation(py, format!("{algorithm} {name} values must be nonnegative"))
            })
        })
        .collect()
}

fn embedding_seed(py: Python<'_>, algorithm: &str, value: &Bound<'_, PyAny>) -> PyResult<u64> {
    let value = value
        .extract::<i128>()
        .map_err(|_| embedding_validation(py, format!("{algorithm} seed must be an integer")))?;
    u64::try_from(value)
        .map_err(|_| embedding_validation(py, format!("{algorithm} seed must fit unsigned 64-bit")))
}

fn embedding_strings(
    py: Python<'_>,
    algorithm: &str,
    name: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<Vec<String>> {
    value.extract::<Vec<String>>().map_err(|_| {
        embedding_validation(
            py,
            format!("{algorithm} {name} must be an ordered list of property names"),
        )
    })
}

fn node2vec_options(
    py: Python<'_>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Node2VecOptions> {
    let mut options = Node2VecOptions::default();
    if let Some(kwargs) = kwargs {
        for (name, value) in kwargs {
            let name = name.extract::<String>()?;
            match name.as_str() {
                "dimensions" => {
                    options.dimensions = embedding_count(py, "node2vec", &name, &value)?;
                }
                "walk_length" => {
                    options.walk_length = embedding_count(py, "node2vec", &name, &value)?;
                }
                "walks_per_node" => {
                    options.walks_per_node = embedding_count(py, "node2vec", &name, &value)?;
                }
                "p" => options.p = value.extract()?,
                "q" => options.q = value.extract()?,
                "window_size" => {
                    options.window_size = embedding_count(py, "node2vec", &name, &value)?;
                }
                "negative_samples" => {
                    options.negative_samples = embedding_count(py, "node2vec", &name, &value)?;
                }
                "epochs" => options.epochs = embedding_count(py, "node2vec", &name, &value)?,
                "learning_rate" => options.learning_rate = value.extract()?,
                "seed" => options.seed = embedding_seed(py, "node2vec", &value)?,
                _ => {
                    return Err(embedding_validation(
                        py,
                        format!("unknown node2vec option {name:?}"),
                    ));
                }
            }
        }
    }
    Ok(options)
}

fn graphsage_options(
    py: Python<'_>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<GraphSageOptions> {
    let mut options = GraphSageOptions::default();
    if let Some(kwargs) = kwargs {
        for (name, value) in kwargs {
            let name = name.extract::<String>()?;
            match name.as_str() {
                "dimensions" => {
                    options.dimensions = embedding_count(py, "graphsage", &name, &value)?;
                }
                "hidden_dimensions" => {
                    options.hidden_dimensions = embedding_count(py, "graphsage", &name, &value)?;
                }
                "layers" => options.layers = embedding_count(py, "graphsage", &name, &value)?,
                "sample_sizes" => {
                    options.sample_sizes = embedding_counts(py, "graphsage", &name, &value)?;
                }
                "aggregator" => {
                    let aggregator = value.extract::<String>()?;
                    if aggregator != "mean" {
                        return Err(embedding_validation(
                            py,
                            "graphsage aggregator must be \"mean\"",
                        ));
                    }
                    options.aggregator = GraphSageAggregator::Mean;
                }
                "epochs" => options.epochs = embedding_count(py, "graphsage", &name, &value)?,
                "negative_samples" => {
                    options.negative_samples = embedding_count(py, "graphsage", &name, &value)?;
                }
                "learning_rate" => options.learning_rate = value.extract()?,
                "feature_properties" => {
                    options.feature_properties = embedding_strings(py, "graphsage", &name, &value)?;
                }
                "seed" => options.seed = embedding_seed(py, "graphsage", &value)?,
                _ => {
                    return Err(embedding_validation(
                        py,
                        format!("unknown graphsage option {name:?}"),
                    ));
                }
            }
        }
    }
    Ok(options)
}

fn fastrp_options(py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<FastRpOptions> {
    let mut options = FastRpOptions::default();
    if let Some(kwargs) = kwargs {
        for (name, value) in kwargs {
            let name = name.extract::<String>()?;
            match name.as_str() {
                "dimensions" => {
                    options.dimensions =
                        embedding_count(py, "fast_random_projection", &name, &value)?;
                }
                "iteration_weights" => options.iteration_weights = value.extract()?,
                "normalization_strength" => options.normalization_strength = value.extract()?,
                "feature_weight" => options.feature_weight = value.extract()?,
                "feature_properties" => {
                    options.feature_properties =
                        embedding_strings(py, "fast_random_projection", &name, &value)?;
                }
                "seed" => {
                    options.seed = embedding_seed(py, "fast_random_projection", &value)?;
                }
                _ => {
                    return Err(embedding_validation(
                        py,
                        format!("unknown fast_random_projection option {name:?}"),
                    ));
                }
            }
        }
    }
    Ok(options)
}

fn hashgnn_options(py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<HashGnnOptions> {
    let mut options = HashGnnOptions::default();
    if let Some(kwargs) = kwargs {
        for (name, value) in kwargs {
            let name = name.extract::<String>()?;
            match name.as_str() {
                "dimensions" => {
                    options.dimensions = embedding_count(py, "hashgnn", &name, &value)?;
                }
                "iterations" => {
                    options.iterations = embedding_count(py, "hashgnn", &name, &value)?;
                }
                "embedding_density" => options.embedding_density = value.extract()?,
                "heterogeneous" => options.heterogeneous = value.extract()?,
                "node_type_property" => options.node_type_property = value.extract()?,
                "relationship_type_property" => {
                    options.relationship_type_property = value.extract()?;
                }
                "seed" => options.seed = embedding_seed(py, "hashgnn", &value)?,
                _ => {
                    return Err(embedding_validation(
                        py,
                        format!("unknown hashgnn option {name:?}"),
                    ));
                }
            }
        }
    }
    Ok(options)
}

fn embedding_options_from_kwargs(
    py: Python<'_>,
    by: graphforge_api::AnalyzeAlgorithm,
    via: Option<&str>,
    directed: bool,
    weight: Option<&str>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<EmbeddingAnalyzeOptions> {
    let options = match by {
        graphforge_api::AnalyzeAlgorithm::Node2Vec => {
            EmbeddingOptions::Node2Vec(node2vec_options(py, kwargs)?)
        }
        graphforge_api::AnalyzeAlgorithm::GraphSage => {
            EmbeddingOptions::GraphSage(graphsage_options(py, kwargs)?)
        }
        graphforge_api::AnalyzeAlgorithm::FastRandomProjection => {
            EmbeddingOptions::FastRandomProjection(fastrp_options(py, kwargs)?)
        }
        graphforge_api::AnalyzeAlgorithm::HashGnn => {
            EmbeddingOptions::HashGnn(hashgnn_options(py, kwargs)?)
        }
        _ => {
            return Err(embedding_validation(
                py,
                format!("{by} is not an embedding algorithm"),
            ));
        }
    };
    Ok(EmbeddingAnalyzeOptions {
        by,
        via: via.map(str::to_owned),
        directed,
        weight: weight.map(str::to_owned),
        options,
    })
}

/// Build the `$param` map from a Python `dict` (or empty when `None`).
fn params_from_dict(params: Option<&Bound<'_, PyDict>>) -> PyResult<HashMap<String, IrLiteral>> {
    let mut out = HashMap::new();
    if let Some(dict) = params {
        for (k, v) in dict.iter() {
            out.insert(k.extract::<String>()?, py_to_ir_literal(&v)?);
        }
    }
    Ok(out)
}

/// Transfer an [`ExecutionResult`] to a `pyarrow.Table` via the Arrow C Data
/// Interface, preserving the schema (and its `graphforge.*` metadata). An empty
/// result yields a zero-row table with the correct schema.
fn result_to_pyarrow(py: Python<'_>, result: &ExecutionResult) -> PyResult<Py<PyAny>> {
    let batches = result
        .batches
        .iter()
        .map(|b| b.to_pyarrow(py))
        .collect::<PyResult<Vec<_>>>()?;
    let schema = result.schema.to_pyarrow(py)?;
    let table = py
        .import("pyarrow")?
        .getattr("Table")?
        .call_method1("from_batches", (batches, schema))?;
    Ok(table.unbind())
}

/// Transfer a native analyst-algorithm batch to a `pyarrow.Table` without reshaping it.
fn algorithm_result(py: Python<'_>, r: Result<RecordBatch, GfError>) -> PyResult<Py<PyAny>> {
    let batch = r.map_err(|error| to_pyerr(py, &error))?;
    record_batch_to_pyarrow_table(py, &batch)
}

fn record_batch_to_pyarrow_table(py: Python<'_>, batch: &RecordBatch) -> PyResult<Py<PyAny>> {
    let schema = batch.schema().to_pyarrow(py)?;
    let batch = batch.to_pyarrow(py)?;
    let table = py
        .import("pyarrow")?
        .getattr("Table")?
        .call_method1("from_batches", ([batch], schema))?;
    Ok(table.unbind())
}

fn bulk_node_publication_error(py: Python<'_>, error: BulkNodePublicationError) -> PyErr {
    match error {
        BulkNodePublicationError::Validation(error) => {
            to_pyerr(py, &GfError::Validation(error.to_string()))
        }
        BulkNodePublicationError::Publication(error) => to_pyerr(py, &error),
    }
}

fn bulk_edge_publication_error(py: Python<'_>, error: BulkEdgePublicationError) -> PyErr {
    match error {
        BulkEdgePublicationError::Validation(error) => {
            to_pyerr(py, &GfError::Validation(error.to_string()))
        }
        BulkEdgePublicationError::Publication(error) => to_pyerr(py, &error),
    }
}

/// A synchronous [`RecordBatchReader`] that drives the facade's async streaming
/// query one batch per pull. Handed to PyArrow (via the Arrow C Stream
/// Interface) so `execute_stream` returns a genuine `pyarrow.RecordBatchReader`.
///
/// Owns a [`RuntimeGuard`] so the Tokio runtime and on-disk graph workspace
/// outlive the parent `GraphForge`: the reader is lazy and `'static`, and a bare
/// runtime handle would not keep the runtime (and the stream's worker threads /
/// Parquet fragment paths) alive.
struct StreamReader {
    schema: SchemaRef,
    stream: SendableRecordBatchStream,
    guard: RuntimeGuard,
}

impl Iterator for StreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Pulled from Python through the Arrow C stream with the GIL held;
        // release it while blocking on the next batch so other Python threads
        // run. A panic here would unwind into the C `get_next` callback (UB), so
        // catch it and convert to an Arrow error. DataFusion stream errors map
        // into the Arrow error domain (the typed GfError cannot survive the C
        // boundary — only build-time errors keep their Python exception class).
        let pulled = Python::attach(|py| {
            py.detach(|| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.guard.block_on(self.stream.next())
                }))
            })
        });
        match pulled {
            Ok(item) => item.map(|r| r.map_err(|e| ArrowError::ExternalError(Box::new(e)))),
            Err(_) => Some(Err(ArrowError::ExternalError(
                "panic while polling the GraphForge stream".into(),
            ))),
        }
    }
}

impl RecordBatchReader for StreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// UUID-backed node handle returned by [`GraphForge::add_node`].
#[pyclass(name = "NodeHandle", module = "graphforge")]
pub struct PyNodeHandle {
    inner: graphforge_api::NodeHandle,
}

#[pymethods]
impl PyNodeHandle {
    /// Stable public UUID identity.
    #[getter]
    fn uuid(&self) -> String {
        self.inner.uuid.to_string()
    }

    /// Primary label metadata (not an identity surrogate).
    #[getter]
    fn label(&self) -> &str {
        &self.inner.label
    }

    fn __repr__(&self) -> String {
        self.inner.to_string()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }
}

/// UUID-backed edge handle returned by [`GraphForge::add_edge`].
#[pyclass(name = "EdgeHandle", module = "graphforge")]
pub struct PyEdgeHandle {
    inner: graphforge_api::EdgeHandle,
}

/// Opaque Rust-owned neutral algorithm invocation descriptor.
#[pyclass(name = "InvocationDescriptor", module = "graphforge")]
pub struct PyInvocationDescriptor {
    inner: InvocationDescriptor,
}

/// Result of one first-time recorded algorithm dispatch.
#[pyclass(name = "RecordedAlgorithmResult", module = "graphforge")]
pub struct PyRecordedAlgorithmResult {
    run_uuid: String,
    result: Py<PyAny>,
}

/// Opaque Rust-owned graph-only resolved-belief projection.
#[pyclass(name = "ResolvedBeliefProjection", module = "graphforge")]
pub struct PyResolvedBeliefProjection {
    inner: Arc<graphforge_api::ResolvedBeliefProjection>,
}

/// Result of a resolved recorded dispatch and its separate attachment outcome.
#[pyclass(name = "ResolvedRecordedAlgorithmResult", module = "graphforge")]
pub struct PyResolvedRecordedAlgorithmResult {
    run_uuid: String,
    result: Py<PyAny>,
    attachment_state: &'static str,
    attachment: Option<Py<PyAny>>,
    attachment_uuid: Option<String>,
    attachment_error_code: Option<String>,
}

#[pymethods]
impl PyRecordedAlgorithmResult {
    /// Durable run UUID.
    #[getter]
    fn run_uuid(&self) -> &str {
        &self.run_uuid
    }

    /// Canonical Arrow result table.
    #[getter]
    fn result(&self, py: Python<'_>) -> Py<PyAny> {
        self.result.clone_ref(py)
    }
}

#[pymethods]
impl PyResolvedRecordedAlgorithmResult {
    #[getter]
    fn run_uuid(&self) -> &str {
        &self.run_uuid
    }

    #[getter]
    fn result(&self, py: Python<'_>) -> Py<PyAny> {
        self.result.clone_ref(py)
    }

    #[getter]
    fn attachment_state(&self) -> &'static str {
        self.attachment_state
    }

    #[getter]
    fn attachment(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.attachment.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter]
    fn attachment_uuid(&self) -> Option<&str> {
        self.attachment_uuid.as_deref()
    }

    #[getter]
    fn attachment_error_code(&self) -> Option<&str> {
        self.attachment_error_code.as_deref()
    }
}

#[pymethods]
impl PyResolvedBeliefProjection {
    #[getter]
    fn source_generation_uuid(&self) -> String {
        self.inner.source_generation_uuid().to_string()
    }

    #[getter]
    fn graph_content_fingerprint(&self) -> String {
        hex_bytes(&self.inner.graph_content_fingerprint())
    }

    #[getter]
    fn policy_fingerprint(&self) -> String {
        hex_bytes(&self.inner.policy_fingerprint())
    }

    #[getter]
    fn policy_bytes(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, self.inner.policy_bytes()).unbind()
    }

    #[getter]
    fn snapshot_fingerprint(&self) -> String {
        hex_bytes(&self.inner.snapshot_fingerprint())
    }

    #[getter]
    fn valid_time_fingerprint(&self) -> Option<String> {
        self.inner
            .valid_time_fingerprint()
            .map(|value| hex_bytes(&value))
    }

    #[getter]
    fn source_record_uuids(&self) -> Vec<String> {
        self.inner
            .source_record_uuids()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[getter]
    fn transaction_cutoff(&self) -> i64 {
        self.inner.transaction_cutoff_micros()
    }

    #[getter]
    fn valid_time(&self) -> Option<i64> {
        self.inner.valid_time_micros()
    }

    #[pyo3(signature = (label, *, by, via=None, directed=true))]
    fn prepare_rank_invocation(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        via: Option<&str>,
        directed: bool,
    ) -> PyResult<PyInvocationDescriptor> {
        let options = graphforge_api::RankOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            via: via.map(str::to_owned),
            directed,
            write_property: None,
        };
        let label = label.to_owned();
        py.detach(|| self.inner.prepare_rank_invocation(&label, &options))
            .map(|inner| PyInvocationDescriptor { inner })
            .map_err(|error| to_py_invocation_error(py, &error))
    }

    #[pyo3(signature = (label, *, by, vector_property=None, via=None, directed=false))]
    fn prepare_cluster_invocation(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        vector_property: Option<&str>,
        via: Option<&str>,
        directed: bool,
    ) -> PyResult<PyInvocationDescriptor> {
        let options = graphforge_api::ClusterOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            vector_property: vector_property.map(str::to_owned),
            via: via.map(str::to_owned),
            directed,
            write_property: None,
        };
        let label = label.to_owned();
        py.detach(|| self.inner.prepare_cluster_invocation(&label, &options))
            .map(|inner| PyInvocationDescriptor { inner })
            .map_err(|error| to_py_invocation_error(py, &error))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (source=None, target=None, *, by, via=None, directed=true, k=1, weight=None, capacity_property=None, cost_property=None, heuristic=None, walk_length=None, seed=None, terminal_uuids=None, prize_property=None))]
    fn prepare_paths_invocation(
        &self,
        py: Python<'_>,
        source: Option<&Bound<'_, PyAny>>,
        target: Option<&Bound<'_, PyAny>>,
        by: &str,
        via: Option<&str>,
        directed: bool,
        k: usize,
        weight: Option<&str>,
        capacity_property: Option<&str>,
        cost_property: Option<&str>,
        heuristic: Option<&str>,
        walk_length: Option<usize>,
        seed: Option<u64>,
        terminal_uuids: Option<Vec<String>>,
        prize_property: Option<&str>,
    ) -> PyResult<PyInvocationDescriptor> {
        let terminal_uuids = parse_terminal_uuids(&terminal_uuids.unwrap_or_default())
            .map_err(|error| to_pyerr(py, &error))?;
        let options = graphforge_api::PathsOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            via: via.map(str::to_owned),
            directed,
            k,
            weight: weight.map(str::to_owned),
            capacity_property: capacity_property.map(str::to_owned),
            cost_property: cost_property.map(str::to_owned),
            heuristic: heuristic.map(str::to_owned),
            walk_length,
            seed,
            terminal_uuids,
            prize_property: prize_property.map(str::to_owned),
        };
        let source = source
            .map(|value| py_to_node_selector(py, value))
            .transpose()?;
        let target = target
            .map(|value| py_to_node_selector(py, value))
            .transpose()?;
        py.detach(|| {
            self.inner
                .prepare_paths_invocation(source.as_ref(), target.as_ref(), &options)
        })
        .map(|inner| PyInvocationDescriptor { inner })
        .map_err(|error| to_py_invocation_error(py, &error))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (label=None, *, by, via=None, directed=true, weight=None, partition_property=None, k=None))]
    fn prepare_analyze_invocation(
        &self,
        py: Python<'_>,
        label: Option<&str>,
        by: &str,
        via: Option<&str>,
        directed: bool,
        weight: Option<&str>,
        partition_property: Option<&str>,
        k: Option<usize>,
    ) -> PyResult<PyInvocationDescriptor> {
        let options = graphforge_api::AnalyzeOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            via: via.map(str::to_owned),
            directed,
            weight: weight.map(str::to_owned),
            k,
            partition_property: partition_property.map(str::to_owned),
        };
        let label = label.map(str::to_owned);
        py.detach(|| {
            self.inner
                .prepare_analyze_invocation(label.as_deref(), &options)
        })
        .map(|inner| PyInvocationDescriptor { inner })
        .map_err(|error| to_py_invocation_error(py, &error))
    }

    #[pyo3(signature = (label, *, by, k=10, vector_property=None, via=None))]
    fn prepare_similar_invocation(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        k: usize,
        vector_property: Option<&str>,
        via: Option<&str>,
    ) -> PyResult<PyInvocationDescriptor> {
        let options = graphforge_api::SimilarOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            k,
            vector_property: vector_property.map(str::to_owned),
            via: via.map(str::to_owned),
        };
        let label = label.to_owned();
        py.detach(|| self.inner.prepare_similar_invocation(&label, &options))
            .map(|inner| PyInvocationDescriptor { inner })
            .map_err(|error| to_py_invocation_error(py, &error))
    }
}

#[pymethods]
impl PyInvocationDescriptor {
    /// Canonical language-neutral descriptor bytes.
    #[getter]
    fn canonical_bytes(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, self.inner.canonical_bytes()).unbind()
    }

    /// Full descriptor fingerprint as lowercase hex.
    #[getter]
    fn fingerprint(&self) -> String {
        hex_bytes(self.inner.fingerprint())
    }

    /// Exact logical projection fingerprint as lowercase hex.
    #[getter]
    fn projection_fingerprint(&self) -> String {
        hex_bytes(self.inner.projection_fingerprint())
    }

    /// Owning analyst verb.
    #[getter]
    fn verb(&self) -> &'static str {
        self.inner.algorithm().verb().as_str()
    }

    /// Canonical algorithm catalog value.
    #[getter]
    fn algorithm(&self) -> &'static str {
        self.inner.algorithm().as_str()
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}

fn parse_algorithm_id(value: &str) -> Result<graphforge_api::Algorithm, GfError> {
    let (verb, name) = value
        .split_once('.')
        .ok_or_else(|| GfError::Validation("algorithm must be verb.name".into()))?;
    let verb = match verb {
        "rank" => graphforge_api::AlgorithmVerb::Rank,
        "cluster" => graphforge_api::AlgorithmVerb::Cluster,
        "paths" => graphforge_api::AlgorithmVerb::Paths,
        "analyze" => graphforge_api::AlgorithmVerb::Analyze,
        "similar" => graphforge_api::AlgorithmVerb::Similar,
        _ => return Err(GfError::Validation("unknown algorithm verb".into())),
    };
    graphforge_api::Algorithm::parse(verb, name)
        .map_err(|_| GfError::Validation("unknown algorithm ID".into()))
}

#[pymethods]
impl PyEdgeHandle {
    /// Stable public UUID identity.
    #[getter]
    fn uuid(&self) -> String {
        self.inner.uuid.to_string()
    }

    /// Relationship-type metadata (not an identity surrogate).
    #[getter]
    fn rel_type(&self) -> &str {
        &self.inner.rel_type
    }

    fn __repr__(&self) -> String {
        self.inner.to_string()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }
}

struct ConfiguredProviderBinding {
    session: OpenRouterProviderSession,
    request_limits: ProviderRequestLimits,
    execution_limits: ProviderExecutionLimits,
}

fn provider_capabilities(values: Option<Vec<String>>) -> Result<ProviderCapabilities, GfError> {
    let values = values.unwrap_or_else(|| {
        vec![
            "document_embeddings".to_owned(),
            "query_embeddings".to_owned(),
            "candidate_reranking".to_owned(),
        ]
    });
    let values = values
        .into_iter()
        .map(|value| match value.as_str() {
            "document_embeddings" => Ok(ProviderCapability::DocumentEmbeddings),
            "query_embeddings" => Ok(ProviderCapability::QueryEmbeddings),
            "candidate_reranking" => Ok(ProviderCapability::CandidateReranking),
            _ => Err(GfError::Validation(format!(
                "unknown provider capability {value:?}"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    ProviderCapabilities::new(values).map_err(Into::into)
}

fn provider_plan_request(
    configured: &ConfiguredProviderBinding,
    name: &str,
    label: &str,
    properties: Vec<String>,
    dimensions: u32,
    normalization: &str,
    replace: bool,
) -> Result<ProviderEmbeddingPlanRequest, GfError> {
    let normalization = match normalization {
        "none" => ProviderEmbeddingNormalization::None,
        "l2" => ProviderEmbeddingNormalization::L2,
        other => {
            return Err(GfError::Validation(format!(
                "unknown provider embedding normalization {other:?}"
            )));
        }
    };
    Ok(ProviderEmbeddingPlanRequest {
        display_name: name.to_owned(),
        label: label.to_owned(),
        properties,
        contract: configured.session.contract().clone(),
        dimensions,
        normalization,
        distance: ProviderEmbeddingDistance::Cosine,
        request_limits: configured.request_limits,
        batch_limits: ProviderBatchLimits::default(),
        execution_limits: configured.execution_limits,
        replace_alias: replace,
    })
}

fn provider_execution_limits_to_python(
    py: Python<'_>,
    limits: ProviderExecutionLimits,
) -> PyResult<Bound<'_, PyDict>> {
    let value = PyDict::new(py);
    value.set_item("provider_calls", limits.provider_calls)?;
    value.set_item("retries", limits.retries)?;
    value.set_item("input_token_exposure", limits.input_token_exposure)?;
    value.set_item(
        "estimated_cost_microunits",
        limits.estimated_cost_microunits,
    )?;
    value.set_item("timeout_millis", limits.timeout.as_millis())?;
    value.set_item(
        "minimum_call_interval_millis",
        limits.minimum_call_interval.as_millis(),
    )?;
    value.set_item("retry_backoff_millis", limits.retry_backoff.as_millis())?;
    value.set_item(
        "maximum_retry_backoff_millis",
        limits.maximum_retry_backoff.as_millis(),
    )?;
    Ok(value)
}

fn provider_plan_to_python(
    py: Python<'_>,
    inspection: ProviderEmbeddingPlanInspection,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("display_name", inspection.display_name)?;
    value.set_item("compatibility_id", inspection.compatibility_id)?;
    value.set_item("source_fingerprint", inspection.source_fingerprint)?;
    value.set_item("graph_generation", inspection.graph_generation)?;
    value.set_item("label", inspection.label)?;
    value.set_item("properties", inspection.properties)?;
    value.set_item("provider", inspection.provider)?;
    value.set_item("model", inspection.model)?;
    value.set_item("revision", inspection.revision)?;
    value.set_item(
        "response_contract_version",
        inspection.response_contract_version,
    )?;
    value.set_item("tokenizer_identifier", inspection.tokenizer_identifier)?;
    value.set_item("tokenizer_version", inspection.tokenizer_version)?;
    value.set_item(
        "token_count_class",
        match inspection.token_count_class {
            TokenCountClass::ExactLocal => "exact_local",
            TokenCountClass::ProviderReported => "provider_reported",
            TokenCountClass::Approximate => "approximate",
        },
    )?;
    value.set_item("model_input_tokens", inspection.model_input_tokens)?;
    value.set_item(
        "tokenizer_normalization",
        inspection.tokenizer_normalization,
    )?;
    let chunking = inspection.chunking.map(|chunking| {
        let value = PyDict::new(py);
        value.set_item("chunk_size_tokens", chunking.chunk_size_tokens)?;
        value.set_item("overlap_tokens", chunking.overlap_tokens)?;
        value.set_item("aggregation", chunking.aggregation)?;
        value.set_item("truncation_policy", chunking.truncation_policy)?;
        Ok::<_, PyErr>(value)
    });
    value.set_item("chunking", chunking.transpose()?)?;
    value.set_item("dimensions", inspection.dimensions)?;
    value.set_item(
        "normalization",
        match inspection.normalization {
            ProviderEmbeddingNormalization::None => "none",
            ProviderEmbeddingNormalization::L2 => "l2",
        },
    )?;
    value.set_item("distance", "cosine")?;
    value.set_item("selected_nodes", inspection.selected_nodes)?;
    value.set_item("input_bytes", inspection.input_bytes)?;
    value.set_item("input_tokens", inspection.input_tokens)?;
    let batches = inspection
        .batches
        .into_iter()
        .map(|batch| {
            let value = PyDict::new(py);
            value.set_item("items", batch.items)?;
            value.set_item("input_bytes", batch.input_bytes)?;
            value.set_item("input_tokens", batch.input_tokens)?;
            Ok::<_, PyErr>(value)
        })
        .collect::<PyResult<Vec<_>>>()?;
    value.set_item("batches", batches)?;
    let request_limits = PyDict::new(py);
    request_limits.set_item("items", inspection.request_limits.items)?;
    request_limits.set_item("input_bytes", inspection.request_limits.input_bytes)?;
    request_limits.set_item("input_tokens", inspection.request_limits.input_tokens)?;
    request_limits.set_item("output_values", inspection.request_limits.output_values)?;
    request_limits.set_item("provider_calls", inspection.request_limits.provider_calls)?;
    value.set_item("request_limits", request_limits)?;
    let batch_limits = PyDict::new(py);
    batch_limits.set_item("items", inspection.batch_limits.items)?;
    batch_limits.set_item("input_bytes", inspection.batch_limits.input_bytes)?;
    batch_limits.set_item("input_tokens", inspection.batch_limits.input_tokens)?;
    value.set_item("batch_limits", batch_limits)?;
    value.set_item(
        "execution_limits",
        provider_execution_limits_to_python(py, inspection.execution_limits)?,
    )?;
    Ok(value.into_any().unbind())
}

fn emit_find_warnings(py: Python<'_>, diagnostics: &[FindDiagnostic]) -> PyResult<()> {
    for diagnostic in diagnostics {
        let message = match diagnostic {
            FindDiagnostic::ForcedStale { diagnostic } => diagnostic.clone(),
            FindDiagnostic::RerankSuggested { provider, model } => format!(
                "configured reranker {provider}/{model} was omitted; explicit reranking may improve top-result quality"
            ),
        };
        let message = CString::new(message)
            .map_err(|_| PyTypeError::new_err("warning text contained a NUL byte"))?;
        PyErr::warn(py, &py.get_type::<PyRuntimeWarning>(), &message, 1)?;
    }
    Ok(())
}

fn py_rerank_options(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
    configured: &ConfiguredProviderBinding,
) -> PyResult<FindRerankOptions> {
    const ALLOWED: &[&str] = &["query", "properties", "candidate_depth", "failure_policy"];
    for (key, _) in value.iter() {
        let key = key.extract::<String>()?;
        if !ALLOWED.contains(&key.as_str()) {
            return Err(to_pyerr(
                py,
                &GfError::Validation(format!("unknown rerank option {key:?}")),
            ));
        }
    }
    let required = |key: &str| -> PyResult<Bound<'_, PyAny>> {
        value.get_item(key)?.ok_or_else(|| {
            to_pyerr(
                py,
                &GfError::Validation(format!("rerank option {key:?} is required")),
            )
        })
    };
    let failure_policy = match value
        .get_item("failure_policy")?
        .map(|value| value.extract::<String>())
        .transpose()?
        .as_deref()
        .unwrap_or("error")
    {
        "error" => RerankFailurePolicy::Error,
        "canonical_unreranked" => RerankFailurePolicy::CanonicalUnreranked,
        other => {
            return Err(to_pyerr(
                py,
                &GfError::Validation(format!("unknown rerank failure policy {other:?}")),
            ));
        }
    };
    Ok(FindRerankOptions {
        query: required("query")?.extract()?,
        properties: required("properties")?.extract()?,
        candidate_depth: required("candidate_depth")?.extract()?,
        contract: configured.session.contract().clone(),
        request_limits: configured.request_limits,
        execution_limits: configured.execution_limits,
        failure_policy,
    })
}

/// The GraphForge engine — a Python handle over the native Rust core
/// ([`graphforge_api::GraphForge`]).
///
/// Construct in-memory (`GraphForge()`) or over a Parquet project directory
/// (`GraphForge(path)`), then query with [`execute`](Self::execute). The
/// analyst verbs (`rank`/`cluster`/…) are exposed in a follow-up binding-surface PR.
#[pyclass(module = "graphforge")]
pub struct GraphForge {
    inner: graphforge_api::GraphForge,
    provider: Option<ConfiguredProviderBinding>,
    closed: bool,
}

/// Read-only native facade pinned to one checkpoint generation.
#[pyclass(name = "CheckpointView", module = "graphforge")]
pub struct PyCheckpointView {
    inner: graphforge_api::CheckpointView,
}

#[pymethods]
impl PyCheckpointView {
    /// Stable UUID of the named checkpoint.
    #[getter]
    fn checkpoint_uuid(&self) -> String {
        self.inner.checkpoint_uuid().to_string()
    }

    /// UUID of the immutable generation pinned by this view.
    #[getter]
    fn generation_uuid(&self) -> String {
        self.inner.generation_uuid().to_string()
    }

    /// Execute one read-only Cypher query and return a `pyarrow.Table`.
    fn execute(&self, py: Python<'_>, query: &str) -> PyResult<Py<PyAny>> {
        let query = query.to_owned();
        let result = py
            .detach(|| self.inner.execute(&query))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return pinned project capabilities as a `pyarrow.Table`.
    fn project_capabilities(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let result = py
            .detach(|| self.inner.project_capabilities())
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Inspect adjacency freshness from the pinned generation.
    fn inspect_adjacency(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inspection = py
            .detach(|| self.inner.inspect_adjacency())
            .map_err(|error| to_pyerr(py, &error))?;
        adjacency_inspection_to_python(py, inspection)
    }
}

/// Native cloneable cooperative cancellation token.
#[pyclass(name = "CancellationToken", module = "graphforge")]
pub struct PyCancellationToken {
    inner: graphforge_api::CancellationToken,
}

#[pymethods]
impl PyCancellationToken {
    #[new]
    fn new() -> Self {
        Self {
            inner: graphforge_api::CancellationToken::new(),
        }
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    #[getter]
    fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

impl GraphForge {
    /// Guard mirroring the v0.5 lifecycle contract: operations after `close()`
    /// raise `LifecycleError`.
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(Python::attach(|py| {
                to_pyerr(
                    py,
                    &GfError::Lifecycle("operation on a closed GraphForge instance".into()),
                )
            }));
        }
        Ok(())
    }
}

fn project_write_mode(value: &str) -> Result<ProjectWriteMode, GfError> {
    match value {
        "single_writer" => Ok(ProjectWriteMode::SingleWriter),
        "queued_writer" => Ok(ProjectWriteMode::QueuedWriter),
        "optimistic_multi_writer" => Ok(ProjectWriteMode::OptimisticMultiWriter),
        _ => Err(GfError::Validation(
            "write_mode must be single_writer, queued_writer, or optimistic_multi_writer".into(),
        )),
    }
}

#[pymethods]
impl GraphForge {
    /// Open an in-memory (`path=None`) or Parquet-backed (`path=<dir>`) instance.
    #[new]
    #[pyo3(signature = (path=None, *, write_mode="single_writer", write_queue_capacity=64, max_rebase_attempts=3))]
    fn new(
        py: Python<'_>,
        path: Option<&str>,
        write_mode: &str,
        write_queue_capacity: i64,
        max_rebase_attempts: i64,
    ) -> PyResult<Self> {
        let path = path.map(str::to_owned);
        let options = GraphForgeOptions {
            write_mode: project_write_mode(write_mode).map_err(|error| to_pyerr(py, &error))?,
            write_queue_capacity: usize::try_from(write_queue_capacity).map_err(|_| {
                to_pyerr(
                    py,
                    &GfError::Validation("write_queue_capacity must be between 1 and 65536".into()),
                )
            })?,
            max_rebase_attempts: u32::try_from(max_rebase_attempts).map_err(|_| {
                to_pyerr(
                    py,
                    &GfError::Validation("max_rebase_attempts must not exceed 32".into()),
                )
            })?,
            ..GraphForgeOptions::default()
        };
        let inner = py
            .detach(|| graphforge_api::GraphForge::new_with_options(path.as_deref(), options))
            .map_err(|e| to_pyerr(py, &e))?;
        Ok(Self {
            inner,
            provider: None,
            closed: false,
        })
    }

    /// Inspect the committed project capability manifest as a `pyarrow.Table`.
    fn project_capabilities(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let result = py
            .detach(|| self.inner.project_capabilities())
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Create a durable named checkpoint and return its native Arrow receipt.
    #[pyo3(signature = (*, name, idempotency_key, description=None, actor_uuid=None))]
    fn checkpoint(
        &self,
        py: Python<'_>,
        name: String,
        idempotency_key: &Bound<'_, PyAny>,
        description: Option<String>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request = graphforge_api::CheckpointRequest {
            name,
            description,
            idempotency_key: py_operation_id(idempotency_key)
                .map_err(|error| to_pyerr(py, &error))?,
            actor_uuid: actor_uuid
                .map(canonical_operation_id)
                .transpose()
                .map_err(|error| to_pyerr(py, &error))?
                .map(|id| id.0),
        };
        let result = py
            .detach(|| self.inner.checkpoint(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// List active checkpoints with native pagination and cancellation.
    #[pyo3(signature = (*, limit=100, after=None, cancellation=None))]
    fn list_checkpoints(
        &self,
        py: Python<'_>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_checkpoints(graphforge_api::ListCheckpointsRequest {
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Open an immutable view pinned to one named checkpoint.
    fn open_checkpoint(&self, py: Python<'_>, name: &str) -> PyResult<PyCheckpointView> {
        self.ensure_open()?;
        let name = name.to_owned();
        py.detach(|| self.inner.open_checkpoint(&name))
            .map(|inner| PyCheckpointView { inner })
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Delete an active checkpoint reference and return its Arrow receipt.
    #[pyo3(signature = (*, name, idempotency_key, actor_uuid=None))]
    fn delete_checkpoint(
        &self,
        py: Python<'_>,
        name: String,
        idempotency_key: &Bound<'_, PyAny>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request = graphforge_api::DeleteCheckpointRequest {
            name,
            idempotency_key: py_operation_id(idempotency_key)
                .map_err(|error| to_pyerr(py, &error))?,
            actor_uuid: actor_uuid
                .map(canonical_operation_id)
                .transpose()
                .map_err(|error| to_pyerr(py, &error))?
                .map(|id| id.0),
        };
        let result = py
            .detach(|| self.inner.delete_checkpoint(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Restore a complete workspace from a checkpoint using an audited reason.
    #[pyo3(signature = (*, name, reason, idempotency_key, actor_uuid=None))]
    fn revert_to_checkpoint(
        &mut self,
        py: Python<'_>,
        name: String,
        reason: String,
        idempotency_key: &Bound<'_, PyAny>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request = graphforge_api::RevertCheckpointRequest {
            name,
            reason,
            idempotency_key: py_operation_id(idempotency_key)
                .map_err(|error| to_pyerr(py, &error))?,
            actor_uuid: actor_uuid
                .map(canonical_operation_id)
                .transpose()
                .map_err(|error| to_pyerr(py, &error))?
                .map(|id| id.0),
        };
        let result = py
            .detach(|| self.inner.revert_to_checkpoint(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Diff two checkpoint/current endpoints through the Rust-owned engine.
    #[pyo3(signature = (*, from_checkpoint=None, to_checkpoint=None, scope="summary", detail="summary", limit=100, after=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn diff_checkpoints(
        &self,
        py: Python<'_>,
        from_checkpoint: Option<String>,
        to_checkpoint: Option<String>,
        scope: &str,
        detail: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let selector = |name: Option<String>| {
            name.map_or(
                graphforge_api::CheckpointSelector::Current,
                graphforge_api::CheckpointSelector::Named,
            )
        };
        let scope = match scope {
            "summary" => graphforge_api::CheckpointDiffScope::Summary,
            "graph" => graphforge_api::CheckpointDiffScope::Graph,
            "ontology" => graphforge_api::CheckpointDiffScope::Ontology,
            "configuration" => graphforge_api::CheckpointDiffScope::Configuration,
            "capabilities" => graphforge_api::CheckpointDiffScope::Capabilities,
            "provenance" => graphforge_api::CheckpointDiffScope::Provenance,
            "knowledge" => graphforge_api::CheckpointDiffScope::Knowledge,
            "epistemic" => graphforge_api::CheckpointDiffScope::Epistemic,
            "all" => graphforge_api::CheckpointDiffScope::All,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("invalid checkpoint diff scope".into()),
                ));
            }
        };
        let detail = match detail {
            "summary" => graphforge_api::CheckpointDiffDetail::Summary,
            "records" => graphforge_api::CheckpointDiffDetail::Records,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("invalid checkpoint diff detail".into()),
                ));
            }
        };
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .diff_checkpoints(graphforge_api::DiffCheckpointsRequest {
                        from: selector(from_checkpoint),
                        to: selector(to_checkpoint),
                        scope,
                        detail,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically enable one registered project capability.
    #[pyo3(signature = (*, operation_uuid, capability_id, capability_version, actor_uuid=None))]
    fn enable_capability(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        capability_id: &str,
        capability_version: u32,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let capability_id =
            parse_capability_id(capability_id).map_err(|error| to_pyerr(py, &error))?;
        let result = py
            .detach(|| {
                self.inner
                    .enable_capability(graphforge_api::EnableCapabilityRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        capability_id,
                        capability_version,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact provenance event as a `pyarrow.Table`.
    #[pyo3(signature = (provenance_uuid, *, cancellation=None))]
    fn provenance_event(
        &self,
        py: Python<'_>,
        provenance_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let provenance_uuid = canonical_operation_id(provenance_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .provenance_event(provenance_uuid, cancellation.clone())
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound provenance history page.
    #[pyo3(signature = (*, subject_uuid=None, operation_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_provenance_history(
        &self,
        py: Python<'_>,
        subject_uuid: Option<&str>,
        operation_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let subject_uuid = subject_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let operation_uuid = operation_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_provenance_history(graphforge_api::ProvenanceHistoryRequest {
                        subject_uuid,
                        operation_uuid,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically create one immutable analytical assertion.
    #[pyo3(signature = (*, operation_uuid, assertion_uuid, claim, graph_refs, actor_uuid=None))]
    fn create_assertion(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        assertion_uuid: &str,
        claim: String,
        graph_refs: &Bound<'_, PyList>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let graph_refs = graph_refs
            .iter()
            .map(|value| py_assertion_graph_ref(py, &value))
            .collect::<PyResult<Vec<_>>>()?;
        let result = py
            .detach(|| {
                self.inner
                    .create_assertion(graphforge_api::CreateAssertionRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        assertion_uuid,
                        claim,
                        graph_refs,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically create one assertion and a non-empty evidence bundle.
    #[pyo3(signature = (*, operation_uuid, assertion_uuid, claim, graph_refs, evidence, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn create_assertion_with_evidence(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        assertion_uuid: &str,
        claim: String,
        graph_refs: &Bound<'_, PyList>,
        evidence: &Bound<'_, PyList>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let graph_refs = graph_refs
            .iter()
            .map(|value| py_assertion_graph_ref(py, &value))
            .collect::<PyResult<Vec<_>>>()?;
        let evidence = evidence
            .iter()
            .map(|value| py_evidence_input(py, &value))
            .collect::<PyResult<Vec<_>>>()?;
        let result = py
            .detach(|| {
                self.inner.create_assertion_with_evidence(
                    graphforge_api::CreateAssertionWithEvidenceRequest {
                        assertion: graphforge_api::CreateAssertionRequest {
                            context: WriteContext {
                                operation_uuid,
                                actor_uuid,
                            },
                            assertion_uuid,
                            claim,
                            graph_refs,
                        },
                        evidence,
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically create one assertion and its first explicit status.
    #[pyo3(signature = (*, operation_uuid, assertion_uuid, claim, graph_refs, status_event_uuid, status, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn create_assertion_with_status(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        assertion_uuid: &str,
        claim: String,
        graph_refs: &Bound<'_, PyList>,
        status_event_uuid: &str,
        status: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let status_event_uuid = canonical_operation_id(status_event_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let status = assertion_status(status).map_err(|error| to_pyerr(py, &error))?;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let graph_refs = graph_refs
            .iter()
            .map(|value| py_assertion_graph_ref(py, &value))
            .collect::<PyResult<Vec<_>>>()?;
        let result = py
            .detach(|| {
                self.inner.create_assertion_with_status(
                    graphforge_api::CreateAssertionWithStatusRequest {
                        assertion: graphforge_api::CreateAssertionRequest {
                            context: WriteContext {
                                operation_uuid,
                                actor_uuid,
                            },
                            assertion_uuid,
                            claim,
                            graph_refs,
                        },
                        first_status: graphforge_api::FirstAssertionStatusInput {
                            status_event_uuid,
                            status,
                        },
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact immutable assertion.
    #[pyo3(signature = (assertion_uuid, *, cancellation=None))]
    fn assertion(
        &self,
        py: Python<'_>,
        assertion_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| self.inner.assertion(assertion_uuid, cancellation.clone()))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound assertion page.
    #[pyo3(signature = (*, graph_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_assertions(
        &self,
        py: Python<'_>,
        graph_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let graph_uuid = graph_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_assertions(graphforge_api::ListAssertionsRequest {
                        graph_uuid,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one assertion's graph references in canonical order.
    #[pyo3(signature = (assertion_uuid, *, limit=100, after=None, cancellation=None))]
    fn assertion_graph_refs(
        &self,
        py: Python<'_>,
        assertion_uuid: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner.assertion_graph_refs(
                    assertion_uuid,
                    graphforge_api::PageRequest {
                        limit,
                        after,
                        cancellation: cancellation.clone(),
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically record one immutable confidence assessment.
    #[pyo3(signature = (*, operation_uuid, confidence_uuid, assertion_uuid, policy, value=None, input_confidence_uuids=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn assess_confidence(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        confidence_uuid: &str,
        assertion_uuid: &str,
        policy: &str,
        value: Option<f64>,
        input_confidence_uuids: Option<Vec<String>>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let confidence_uuid = canonical_operation_id(confidence_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let policy = match policy {
            "explicit" => graphforge_api::ConfidencePolicyRequest::Explicit {
                value: value.ok_or_else(|| {
                    to_pyerr(py, &GfError::Validation("explicit requires value".into()))
                })?,
            },
            "conservative_min" => {
                if value.is_some() {
                    return Err(to_pyerr(
                        py,
                        &GfError::Validation(
                            "conservative_min does not accept explicit value".into(),
                        ),
                    ));
                }
                let input_confidence_uuids = input_confidence_uuids
                    .unwrap_or_default()
                    .iter()
                    .map(|value| {
                        canonical_operation_id(value)
                            .map(|id| id.0)
                            .map_err(|error| to_pyerr(py, &error))
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                graphforge_api::ConfidencePolicyRequest::ConservativeMin {
                    input_confidence_uuids,
                }
            }
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("unknown confidence policy".into()),
                ));
            }
        };
        let result = py
            .detach(|| {
                self.inner
                    .assess_confidence(graphforge_api::AssessConfidenceRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        confidence_uuid,
                        assertion_uuid,
                        policy,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact immutable confidence assessment.
    #[pyo3(signature = (confidence_uuid, *, cancellation=None))]
    fn confidence_assessment(
        &self,
        py: Python<'_>,
        confidence_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let confidence_uuid = canonical_operation_id(confidence_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .confidence_assessment(confidence_uuid, cancellation.clone())
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound confidence page.
    #[pyo3(signature = (*, assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_confidence_assessments(
        &self,
        py: Python<'_>,
        assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = assertion_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner.list_confidence_assessments(
                    graphforge_api::ListConfidenceAssessmentsRequest {
                        assertion_uuid,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one assessment's immutable normalized input snapshot.
    #[pyo3(signature = (confidence_uuid, *, limit=100, after=None, cancellation=None))]
    fn confidence_inputs(
        &self,
        py: Python<'_>,
        confidence_uuid: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let confidence_uuid = canonical_operation_id(confidence_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner.confidence_inputs(
                    confidence_uuid,
                    graphforge_api::PageRequest {
                        limit,
                        after,
                        cancellation: cancellation.clone(),
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically attach one immutable evidence link.
    #[pyo3(signature = (*, operation_uuid, evidence_uuid, assertion_uuid, source_uuid, source_kind, role, weight=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn attach_evidence(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        evidence_uuid: &str,
        assertion_uuid: &str,
        source_uuid: &str,
        source_kind: &str,
        role: &str,
        weight: Option<f64>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let evidence_uuid = canonical_operation_id(evidence_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let source_uuid = canonical_operation_id(source_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let source_kind = match source_kind {
            "document" => graphforge_api::EvidenceSourceKind::Document,
            "observation" => graphforge_api::EvidenceSourceKind::Observation,
            "graph_node" => graphforge_api::EvidenceSourceKind::GraphNode,
            "graph_edge" => graphforge_api::EvidenceSourceKind::GraphEdge,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("unknown evidence source kind".into()),
                ));
            }
        };
        let role = match role {
            "supports" => graphforge_api::EvidenceRole::Supports,
            "contradicts" => graphforge_api::EvidenceRole::Contradicts,
            "context" => graphforge_api::EvidenceRole::Context,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("unknown evidence role".into()),
                ));
            }
        };
        let result = py
            .detach(|| {
                self.inner
                    .attach_evidence(graphforge_api::AttachEvidenceRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        evidence_uuid,
                        assertion_uuid,
                        source_uuid,
                        source_kind,
                        role,
                        weight,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact immutable evidence link.
    #[pyo3(signature = (evidence_uuid, *, cancellation=None))]
    fn evidence_link(
        &self,
        py: Python<'_>,
        evidence_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let evidence_uuid = canonical_operation_id(evidence_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .evidence_link(evidence_uuid, cancellation.clone())
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound evidence page.
    #[pyo3(signature = (*, assertion_uuid=None, source_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_evidence_links(
        &self,
        py: Python<'_>,
        assertion_uuid: Option<&str>,
        source_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = assertion_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let source_uuid = source_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_evidence_links(graphforge_api::ListEvidenceLinksRequest {
                        assertion_uuid,
                        source_uuid,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically append one immutable epistemic reasoning record.
    #[pyo3(signature = (*, operation_uuid, reasoning_uuid, assertion_uuid, kind, content_format, content, provenance_uuid, supersedes_reasoning_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_reasoning(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        reasoning_uuid: &str,
        assertion_uuid: &str,
        kind: &str,
        content_format: &str,
        content: Vec<u8>,
        provenance_uuid: &str,
        supersedes_reasoning_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let reasoning_uuid = canonical_operation_id(reasoning_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let provenance_uuid = canonical_operation_id(provenance_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let supersedes_reasoning_uuid = supersedes_reasoning_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|value| value.0);
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|value| value.0);
        let kind = match kind {
            "evidence_interpretation" => graphforge_api::ReasoningKind::EvidenceInterpretation,
            "logical_inference" => graphforge_api::ReasoningKind::LogicalInference,
            "methodological_note" => graphforge_api::ReasoningKind::MethodologicalNote,
            "decision_rationale" => graphforge_api::ReasoningKind::DecisionRationale,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("unknown reasoning kind".into()),
                ));
            }
        };
        let content_format = match content_format {
            "text/plain" => graphforge_api::ReasoningContentFormat::TextPlain,
            "text/markdown" => graphforge_api::ReasoningContentFormat::TextMarkdown,
            "application/json" => graphforge_api::ReasoningContentFormat::ApplicationJson,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("unknown reasoning content format".into()),
                ));
            }
        };
        let result = py
            .detach(|| {
                self.inner
                    .record_reasoning(graphforge_api::RecordReasoningRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        reasoning_uuid,
                        assertion_uuid,
                        kind,
                        content_format,
                        content,
                        supersedes_reasoning_uuid,
                        provenance_uuid,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact immutable reasoning record.
    #[pyo3(signature = (reasoning_uuid, *, cancellation=None))]
    fn reasoning(
        &self,
        py: Python<'_>,
        reasoning_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let reasoning_uuid = canonical_operation_id(reasoning_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| self.inner.reasoning(reasoning_uuid, cancellation.clone()))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic immutable reasoning history.
    #[pyo3(signature = (*, assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_reasoning(
        &self,
        py: Python<'_>,
        assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = assertion_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|value| value.0);
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_reasoning(graphforge_api::ListReasoningRequest {
                        assertion_uuid,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Append one explicit assertion-status event.
    #[pyo3(signature = (*, operation_uuid, status_event_uuid, assertion_uuid, status, provenance_uuid, confidence_uuid=None, reasoning_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_assertion_status(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        status_event_uuid: &str,
        assertion_uuid: &str,
        status: &str,
        provenance_uuid: &str,
        confidence_uuid: Option<&str>,
        reasoning_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse = |value: &str| {
            canonical_operation_id(value)
                .map(|id| id.0)
                .map_err(|error| to_pyerr(py, &error))
        };
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let request = graphforge_api::RecordAssertionStatusRequest {
            context: WriteContext {
                operation_uuid,
                actor_uuid,
            },
            status_event_uuid: parse(status_event_uuid)?,
            assertion_uuid: parse(assertion_uuid)?,
            status: assertion_status(status).map_err(|error| to_pyerr(py, &error))?,
            confidence_uuid: confidence_uuid.map(parse).transpose()?,
            reasoning_uuid: reasoning_uuid.map(parse).transpose()?,
            provenance_uuid: parse(provenance_uuid)?,
        };
        let result = py
            .detach(|| self.inner.record_assertion_status(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return the current explicit status or an empty Arrow table when statusless.
    fn assertion_status(&self, py: Python<'_>, assertion_uuid: &str) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let result = py
            .detach(|| self.inner.assertion_status(assertion_uuid))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic append-only assertion-status history.
    #[pyo3(signature = (*, assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_assertion_status(
        &self,
        py: Python<'_>,
        assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = assertion_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_assertion_status(graphforge_api::ListAssertionStatusRequest {
                        assertion_uuid,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically append one assertion supersession and paired terminal status.
    #[pyo3(signature = (*, operation_uuid, supersession_uuid, prior_assertion_uuid, replacement_assertion_uuid, status_event_uuid, reasoning_uuid, provenance_uuid, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn supersede_assertion(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        supersession_uuid: &str,
        prior_assertion_uuid: &str,
        replacement_assertion_uuid: &str,
        status_event_uuid: &str,
        reasoning_uuid: &str,
        provenance_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse = |value: &str| {
            canonical_operation_id(value)
                .map(|id| id.0)
                .map_err(|error| to_pyerr(py, &error))
        };
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let request = graphforge_api::SupersedeAssertionRequest {
            context: WriteContext {
                operation_uuid,
                actor_uuid,
            },
            supersession_uuid: parse(supersession_uuid)?,
            prior_assertion_uuid: parse(prior_assertion_uuid)?,
            replacement_assertion_uuid: parse(replacement_assertion_uuid)?,
            status_event_uuid: parse(status_event_uuid)?,
            reasoning_uuid: parse(reasoning_uuid)?,
            provenance_uuid: parse(provenance_uuid)?,
        };
        let result = py
            .detach(|| self.inner.supersede_assertion(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic branch-preserving assertion-supersession history.
    #[pyo3(signature = (*, prior_assertion_uuid=None, replacement_assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_assertion_supersessions(
        &self,
        py: Python<'_>,
        prior_assertion_uuid: Option<&str>,
        replacement_assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse_optional = |value: Option<&str>| {
            value
                .map(canonical_operation_id)
                .transpose()
                .map(|value| value.map(|id| id.0))
                .map_err(|error| to_pyerr(py, &error))
        };
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let request = graphforge_api::ListAssertionSupersessionsRequest {
            prior_assertion_uuid: parse_optional(prior_assertion_uuid)?,
            replacement_assertion_uuid: parse_optional(replacement_assertion_uuid)?,
            page: graphforge_api::PageRequest {
                limit,
                after,
                cancellation,
            },
        };
        let result = py
            .detach(|| self.inner.list_assertion_supersessions(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Create one immutable hypothesis group.
    #[pyo3(signature = (*, operation_uuid, group_uuid, question_key, provenance_uuid, actor_uuid=None))]
    fn create_hypothesis_group(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        group_uuid: &str,
        question_key: String,
        provenance_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let request = graphforge_api::CreateHypothesisGroupRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            group_uuid: parse(group_uuid)?.0,
            question_key,
            provenance_uuid: parse(provenance_uuid)?.0,
        };
        let result = py
            .detach(|| self.inner.create_hypothesis_group(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Append one explicit hypothesis-membership event.
    #[pyo3(signature = (*, operation_uuid, membership_event_uuid, group_uuid, assertion_uuid, action, reasoning_uuid, provenance_uuid, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_hypothesis_membership(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        membership_event_uuid: &str,
        group_uuid: &str,
        assertion_uuid: &str,
        action: &str,
        reasoning_uuid: &str,
        provenance_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let action = match action {
            "added" => graphforge_api::HypothesisMembershipAction::Added,
            "removed" => graphforge_api::HypothesisMembershipAction::Removed,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("action must be 'added' or 'removed'".into()),
                ));
            }
        };
        let request = graphforge_api::RecordHypothesisMembershipRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            membership_event_uuid: parse(membership_event_uuid)?.0,
            group_uuid: parse(group_uuid)?.0,
            assertion_uuid: parse(assertion_uuid)?.0,
            action,
            reasoning_uuid: parse(reasoning_uuid)?.0,
            provenance_uuid: parse(provenance_uuid)?.0,
        };
        let result = py
            .detach(|| self.inner.record_hypothesis_membership(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Append one explicit hypothesis selection or clear event.
    #[pyo3(signature = (*, operation_uuid, selection_event_uuid, group_uuid, reasoning_uuid, provenance_uuid, selected_assertion_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_hypothesis_selection(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        selection_event_uuid: &str,
        group_uuid: &str,
        reasoning_uuid: &str,
        provenance_uuid: &str,
        selected_assertion_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let request = graphforge_api::RecordHypothesisSelectionRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            selection_event_uuid: parse(selection_event_uuid)?.0,
            group_uuid: parse(group_uuid)?.0,
            selected_assertion_uuid: selected_assertion_uuid
                .map(parse)
                .transpose()?
                .map(|id| id.0),
            reasoning_uuid: parse(reasoning_uuid)?.0,
            provenance_uuid: parse(provenance_uuid)?.0,
        };
        let result = py
            .detach(|| self.inner.record_hypothesis_selection(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically remove one member and explicitly change or clear selection.
    #[pyo3(signature = (*, operation_uuid, membership_event_uuid, selection_event_uuid, group_uuid, assertion_uuid, reasoning_uuid, provenance_uuid, selected_assertion_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn remove_hypothesis_member(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        membership_event_uuid: &str,
        selection_event_uuid: &str,
        group_uuid: &str,
        assertion_uuid: &str,
        reasoning_uuid: &str,
        provenance_uuid: &str,
        selected_assertion_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let request = graphforge_api::RemoveHypothesisMemberRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            membership_event_uuid: parse(membership_event_uuid)?.0,
            selection_event_uuid: parse(selection_event_uuid)?.0,
            group_uuid: parse(group_uuid)?.0,
            assertion_uuid: parse(assertion_uuid)?.0,
            selected_assertion_uuid: selected_assertion_uuid
                .map(parse)
                .transpose()?
                .map(|id| id.0),
            reasoning_uuid: parse(reasoning_uuid)?.0,
            provenance_uuid: parse(provenance_uuid)?.0,
        };
        let result = py
            .detach(|| self.inner.remove_hypothesis_member(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic hypothesis-group history.
    #[pyo3(signature = (*, question_key=None, limit=100, after=None, cancellation=None))]
    fn list_hypothesis_groups(
        &self,
        py: Python<'_>,
        question_key: Option<String>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request = graphforge_api::ListHypothesisGroupsRequest {
            question_key,
            page: graphforge_api::PageRequest {
                limit,
                after: after
                    .map(graphforge_api::PageToken::parse)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?,
                cancellation: cancellation.map(|token| token.inner.clone()),
            },
        };
        let result = py
            .detach(|| self.inner.list_hypothesis_groups(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic hypothesis-membership history.
    #[pyo3(signature = (*, group_uuid=None, assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_hypothesis_membership(
        &self,
        py: Python<'_>,
        group_uuid: Option<&str>,
        assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse = |value: &str| {
            canonical_operation_id(value)
                .map(|id| id.0)
                .map_err(|error| to_pyerr(py, &error))
        };
        let request = graphforge_api::ListHypothesisMembershipRequest {
            group_uuid: group_uuid.map(parse).transpose()?,
            assertion_uuid: assertion_uuid.map(parse).transpose()?,
            page: graphforge_api::PageRequest {
                limit,
                after: after
                    .map(graphforge_api::PageToken::parse)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?,
                cancellation: cancellation.map(|token| token.inner.clone()),
            },
        };
        let result = py
            .detach(|| self.inner.list_hypothesis_membership(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic hypothesis-selection history.
    #[pyo3(signature = (*, group_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_hypothesis_selection(
        &self,
        py: Python<'_>,
        group_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let group_uuid = group_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let request = graphforge_api::ListHypothesisSelectionRequest {
            group_uuid,
            page: graphforge_api::PageRequest {
                limit,
                after: after
                    .map(graphforge_api::PageToken::parse)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?,
                cancellation: cancellation.map(|token| token.inner.clone()),
            },
        };
        let result = py
            .detach(|| self.inner.list_hypothesis_selection(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return current hypothesis members.
    fn hypothesis_members(&self, py: Python<'_>, group_uuid: &str) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let group_uuid = canonical_operation_id(group_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let result = py
            .detach(|| self.inner.hypothesis_members(group_uuid))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return the current explicit hypothesis selection.
    fn hypothesis_selection(&self, py: Python<'_>, group_uuid: &str) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let group_uuid = canonical_operation_id(group_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let result = py
            .detach(|| self.inner.hypothesis_selection(group_uuid))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Reconstruct one deterministic epistemic transaction-time snapshot.
    #[pyo3(signature = (*, transaction_cutoff))]
    fn epistemic_snapshot(&self, py: Python<'_>, transaction_cutoff: i64) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let result = py
            .detach(|| self.inner.epistemic_snapshot(transaction_cutoff))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Append one immutable assertion valid-time event.
    #[pyo3(signature = (*, operation_uuid, validity_event_uuid, assertion_uuid, provenance_uuid, valid_from=None, valid_to=None, reasoning_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_assertion_validity(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        validity_event_uuid: &str,
        assertion_uuid: &str,
        provenance_uuid: &str,
        valid_from: Option<i64>,
        valid_to: Option<i64>,
        reasoning_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let request = graphforge_api::RecordAssertionValidityRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            validity_event_uuid: parse(validity_event_uuid)?.0,
            assertion_uuid: parse(assertion_uuid)?.0,
            valid_from_micros: valid_from,
            valid_to_micros: valid_to,
            reasoning_uuid: reasoning_uuid.map(parse).transpose()?.map(|id| id.0),
            provenance_uuid: parse(provenance_uuid)?.0,
        };
        let result = py
            .detach(|| self.inner.record_assertion_validity(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic append-only assertion validity history.
    #[pyo3(signature = (*, assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_assertion_validity(
        &self,
        py: Python<'_>,
        assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = assertion_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_assertion_validity(graphforge_api::ListAssertionValidityRequest {
                        assertion_uuid,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Apply valid time after resolving the mandatory transaction-time cutoff.
    #[pyo3(signature = (*, transaction_cutoff, valid_time))]
    fn apply_valid_time(
        &self,
        py: Python<'_>,
        transaction_cutoff: i64,
        valid_time: i64,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let result = py
            .detach(|| {
                self.inner
                    .apply_valid_time(graphforge_api::ApplyValidTimeRequest {
                        transaction_cutoff_micros: transaction_cutoff,
                        valid_time_micros: valid_time,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Run a Cypher query and return the result as a `pyarrow.Table`.
    ///
    /// `params` binds `$name` placeholders (values: `None`/`bool`/`int`/`float`/
    /// `str`/`uuid.UUID`/`list`/`dict`). Writes (`CREATE`/`SET`/`DELETE`/…) execute
    /// and return a summary.
    #[pyo3(signature = (query, params=None))]
    fn execute(
        &self,
        py: Python<'_>,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let query = query.to_owned();
        let result = match params {
            Some(_) => {
                let p = params_from_dict(params)?;
                py.detach(|| self.inner.execute_with_params(&query, &p))
            }
            None => py.detach(|| self.inner.execute(&query)),
        }
        .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }

    /// Run a Cypher query and return the result as a `polars.DataFrame`.
    ///
    /// Thin convenience wrapper over [`execute`](Self::execute) (Polars consumes
    /// Arrow zero-copy). Requires the optional `polars` dependency; if it is not
    /// installed this raises `ImportError` with install guidance. `params` binds
    /// `$name` placeholders exactly as [`execute`](Self::execute).
    #[pyo3(signature = (query, params=None))]
    fn execute_polars(
        &self,
        py: Python<'_>,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let table = self.execute(py, query, params)?;
        let polars = py.import("polars").map_err(|err| {
            // Only rewrite the message when polars is genuinely absent; surface
            // any other import failure (e.g. a broken polars install) unchanged.
            if err.is_instance_of::<PyModuleNotFoundError>(py) {
                PyImportError::new_err(
                    "execute_polars() requires the optional 'polars' dependency; \
                     install it with `pip install \"graphforge[polars]\"`",
                )
            } else {
                err
            }
        })?;
        let df = polars.call_method1("from_arrow", (table,))?;
        Ok(df.unbind())
    }

    /// Run a read-only Cypher query and return a lazy `pyarrow.RecordBatchReader`.
    ///
    /// The reader's `schema` is available immediately (before iterating); batches
    /// are produced on demand, so a large result is never fully materialised.
    /// Writes (`CREATE`/`MERGE`/`DELETE`/`SET`/`REMOVE`) raise `ValidationError` —
    /// use [`execute`](Self::execute) for those. `params` binds `$name`
    /// placeholders exactly as [`execute`](Self::execute). Errors raised before
    /// the reader is returned (parse/bind/validation) keep their typed exception
    /// class; an error encountered *mid-stream* surfaces as a `pyarrow` Arrow
    /// error (the typed `GfError` cannot cross the Arrow C stream boundary).
    #[pyo3(signature = (query, params=None))]
    fn execute_stream(
        &self,
        py: Python<'_>,
        query: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let query = query.to_owned();
        let p = params_from_dict(params)?;
        let (stream, schema, guard) = py
            .detach(|| self.inner.execute_stream_owned(&query, &p))
            .map_err(|e| to_pyerr(py, &e))?;
        let reader = StreamReader {
            schema,
            stream,
            guard,
        };
        let boxed: Box<dyn RecordBatchReader + Send> = Box::new(reader);
        Ok(boxed.into_pyarrow(py)?.unbind())
    }

    /// Return a human-readable explanation of the compiler pipeline for `query`
    /// (`AST` → `GraphIR` → `LogicalPlan` → `PhysicalPlan`).
    fn explain(&self, py: Python<'_>, query: &str) -> PyResult<String> {
        self.ensure_open()?;
        let query = query.to_owned();
        py.detach(|| self.inner.explain(&query))
            .map_err(|e| to_pyerr(py, &e))
    }

    /// Load and apply an ontology from `path` (YAML/JSON by extension).
    fn load_ontology(&mut self, py: Python<'_>, path: &str) -> PyResult<()> {
        self.ensure_open()?;
        let path = path.to_owned();
        py.detach(|| self.inner.load_ontology(&path))
            .map_err(|e| to_pyerr(py, &e))
    }

    /// Return the stable, deterministically ordered runtime-catalog contract.
    fn inspect_runtime_catalog(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let snapshot = py
            .detach(|| self.inner.inspect_runtime_catalog())
            .map_err(|error| to_pyerr(py, &error))?;
        let value = serde_json::to_value(snapshot)
            .map_err(|error| to_pyerr(py, &GfError::Validation(error.to_string())))?;
        json_value_to_python(py, &value)
    }

    /// Suggest a conservative, explicitly non-authoritative ontology draft.
    fn suggest_ontology(
        &self,
        py: Python<'_>,
        ontology_id: &str,
        version: &str,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let ontology_id = ontology_id.to_owned();
        let version = version.to_owned();
        let suggestion = py
            .detach(|| {
                self.inner
                    .suggest_ontology(graphforge_api::OntologySuggestionOptions {
                        ontology_id,
                        version,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        let value = serde_json::json!({
            "draft": suggestion.draft,
            "document": suggestion.document,
            "fingerprint_sha256": suggestion.fingerprint_sha256,
            "omitted_relation_types": suggestion.omitted_relation_types,
        });
        json_value_to_python(py, &value)
    }

    /// Validate an ontology document without changing live or durable state.
    fn validate_ontology(
        &self,
        py: Python<'_>,
        document: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let document: graphforge_api::OntologyDoc =
            serde_json::from_value(py_to_json_value(document)?)
                .map_err(|error| to_pyerr(py, &GfError::Validation(error.to_string())))?;
        let report = py.detach(|| self.inner.validate_ontology(&document));
        let diagnostics = report
            .diagnostics
            .into_iter()
            .map(|diagnostic| {
                serde_json::json!({
                    "kind": diagnostic.kind.to_string(),
                    "location": diagnostic.location,
                    "message": diagnostic.message,
                })
            })
            .collect::<Vec<_>>();
        json_value_to_python(
            py,
            &serde_json::json!({ "valid": report.valid, "diagnostics": diagnostics }),
        )
    }

    /// Atomically export an explicit ontology source as YAML or JSON.
    #[pyo3(signature = (source, destination, format, *, document=None))]
    fn export_ontology(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        format: &str,
        document: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let source = match source {
            "loaded" => graphforge_api::OntologyExportSource::Loaded,
            "adopted" => graphforge_api::OntologyExportSource::Adopted,
            "suggested" => {
                let document = document.ok_or_else(|| {
                    to_pyerr(
                        py,
                        &GfError::Validation(
                            "document is required for suggested ontology export".into(),
                        ),
                    )
                })?;
                let document = serde_json::from_value(py_to_json_value(document)?)
                    .map_err(|error| to_pyerr(py, &GfError::Validation(error.to_string())))?;
                graphforge_api::OntologyExportSource::Suggested(document)
            }
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(
                        "ontology export source must be suggested, loaded, or adopted".into(),
                    ),
                ));
            }
        };
        let format = ontology_export_format(format).map_err(|error| to_pyerr(py, &error))?;
        let destination = std::path::PathBuf::from(destination);
        py.detach(|| self.inner.export_ontology(source, &destination, format))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Inspect the generation-managed authoritative ontology record.
    fn workspace_ontology(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let record = py
            .detach(|| self.inner.workspace_ontology())
            .map_err(|error| to_pyerr(py, &error))?;
        let value = serde_json::to_value(record)
            .map_err(|error| to_pyerr(py, &GfError::Validation(error.to_string())))?;
        json_value_to_python(py, &value)
    }

    /// Adopt an ontology as durable project authority.
    #[pyo3(signature = (path, mode, *, operation_uuid, actor_uuid=None))]
    fn adopt_ontology(
        &mut self,
        py: Python<'_>,
        path: &str,
        mode: &str,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let request = graphforge_api::AdoptOntologyRequest {
            context: WriteContext {
                operation_uuid: canonical_operation_id(operation_uuid)
                    .map_err(|error| to_pyerr(py, &error))?,
                actor_uuid: actor_uuid
                    .map(canonical_operation_id)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?
                    .map(|operation| operation.0),
            },
            path: path.into(),
            mode: ontology_mode(mode).map_err(|error| to_pyerr(py, &error))?,
        };
        py.detach(|| self.inner.adopt_ontology(request))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Publish explicit durable ontology absence.
    #[pyo3(signature = (*, operation_uuid, actor_uuid=None))]
    fn clear_ontology(
        &mut self,
        py: Python<'_>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let request = graphforge_api::ClearOntologyRequest {
            context: WriteContext {
                operation_uuid: canonical_operation_id(operation_uuid)
                    .map_err(|error| to_pyerr(py, &error))?,
                actor_uuid: actor_uuid
                    .map(canonical_operation_id)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?
                    .map(|operation| operation.0),
            },
        };
        py.detach(|| self.inner.clear_ontology(request))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Rank nodes by a centrality/structural algorithm (`by=`). Returns a
    /// `pyarrow.Table`.
    #[pyo3(signature = (label, *, by, via=None, directed=true, write_property=None))]
    fn rank(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        via: Option<&str>,
        directed: bool,
        write_property: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let opts = graphforge_api::RankOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        };
        let label = label.to_owned();
        algorithm_result(py, py.detach(|| self.inner.rank(&label, opts)))
    }

    /// Prepare a Rust-owned neutral rank invocation without executing it.
    #[pyo3(signature = (label, *, by, via=None, directed=true))]
    fn prepare_rank_invocation(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        via: Option<&str>,
        directed: bool,
    ) -> PyResult<PyInvocationDescriptor> {
        self.ensure_open()?;
        let options = graphforge_api::RankOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            via: via.map(str::to_owned),
            directed,
            write_property: None,
        };
        let label = label.to_owned();
        py.detach(|| self.inner.prepare_rank_invocation(&label, &options))
            .map(|inner| PyInvocationDescriptor { inner })
            .map_err(|error| to_py_invocation_error(py, &error))
    }

    /// Dispatch an opaque descriptor through its Rust-owned analyst verb.
    fn invoke_descriptor(
        &self,
        py: Python<'_>,
        descriptor: &PyInvocationDescriptor,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let descriptor = descriptor.inner.clone();
        let batch = py
            .detach(|| self.inner.invoke_descriptor(&descriptor))
            .map_err(|error| to_py_invocation_error(py, &error))?;
        algorithm_result(py, Ok(batch))
    }

    /// Decode canonical descriptor bytes in Rust and dispatch them.
    fn invoke_descriptor_bytes(&self, py: Python<'_>, descriptor: &[u8]) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let descriptor = descriptor.to_owned();
        let batch = py
            .detach(|| self.inner.invoke_descriptor_bytes(&descriptor))
            .map_err(|error| to_py_invocation_error(py, &error))?;
        algorithm_result(py, Ok(batch))
    }

    /// Durably record a run lifecycle around the unchanged descriptor dispatch.
    #[pyo3(signature = (*, operation_uuid, run_uuid, descriptor, actor_uuid=None, cancellation=None))]
    fn invoke_recorded(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        run_uuid: &str,
        descriptor: &PyInvocationDescriptor,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<PyRecordedAlgorithmResult> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let run_uuid = canonical_operation_id(run_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|value| value.0);
        let cancellation = cancellation.map(|token| token.inner.clone());
        let descriptor = descriptor.inner.clone();
        let recorded = py
            .detach(|| {
                self.inner
                    .invoke_recorded(graphforge_api::RecordedAlgorithmRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        run_uuid,
                        descriptor,
                        cancellation: cancellation.clone(),
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        Ok(PyRecordedAlgorithmResult {
            run_uuid: recorded.run_uuid.to_string(),
            result: result_to_pyarrow(py, &recorded.result)?,
        })
    }

    /// Resolve an explicit epistemic policy into an opaque graph-only projection.
    #[pyo3(signature = (*, transaction_cutoff, included_statuses, statusless, supersession_branches, hypotheses, valid_time=None))]
    #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
    fn resolve_belief_projection(
        &self,
        py: Python<'_>,
        transaction_cutoff: i64,
        included_statuses: Vec<String>,
        statusless: &str,
        supersession_branches: &str,
        hypotheses: &str,
        valid_time: Option<i64>,
    ) -> PyResult<PyResolvedBeliefProjection> {
        self.ensure_open()?;
        let policy = parse_belief_projection_policy(
            &included_statuses,
            statusless,
            supersession_branches,
            hypotheses,
        )
        .map_err(|error| to_pyerr(py, &error))?;
        py.detach(|| {
            self.inner
                .resolve_belief_projection(graphforge_api::ResolveBeliefProjectionRequest {
                    transaction_cutoff_micros: transaction_cutoff,
                    valid_time_micros: valid_time,
                    policy,
                })
        })
        .map(|inner| PyResolvedBeliefProjection {
            inner: Arc::new(inner),
        })
        .map_err(|error| to_pyerr(py, &error))
    }

    /// Execute one neutral descriptor on a resolved projection and record its attachment.
    #[pyo3(signature = (*, projection, operation_uuid, run_uuid, attachment_uuid, descriptor, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn invoke_resolved_recorded(
        &self,
        py: Python<'_>,
        projection: &PyResolvedBeliefProjection,
        operation_uuid: &str,
        run_uuid: &str,
        attachment_uuid: &str,
        descriptor: &PyInvocationDescriptor,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<PyResolvedRecordedAlgorithmResult> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let attachment_uuid = parse(attachment_uuid)?.0;
        let requested_attachment_uuid = attachment_uuid.to_string();
        let cancellation = cancellation.map(|token| token.inner.clone());
        let projection = Arc::clone(&projection.inner);
        let descriptor = descriptor.inner.clone();
        let operation_uuid = parse(operation_uuid)?;
        let actor_uuid = actor_uuid.map(parse).transpose()?.map(|id| id.0);
        let run_uuid = parse(run_uuid)?.0;
        let result = py
            .detach(|| {
                self.inner.invoke_resolved_recorded(
                    &projection,
                    graphforge_api::ResolvedRecordedAlgorithmRequest {
                        recorded: graphforge_api::RecordedAlgorithmRequest {
                            context: WriteContext {
                                operation_uuid,
                                actor_uuid,
                            },
                            run_uuid,
                            descriptor,
                            cancellation: cancellation.clone(),
                        },
                        attachment_uuid,
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        resolved_recorded_result_to_python(py, result, requested_attachment_uuid)
    }

    /// Retry only the epistemic attachment for an already-completed knowledge run.
    #[pyo3(signature = (*, projection, operation_uuid, attachment_uuid, run_uuid, descriptor, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn attach_resolved_run(
        &self,
        py: Python<'_>,
        projection: &PyResolvedBeliefProjection,
        operation_uuid: &str,
        attachment_uuid: &str,
        run_uuid: &str,
        descriptor: &PyInvocationDescriptor,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let projection = Arc::clone(&projection.inner);
        let request = graphforge_api::AttachResolvedRunRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            attachment_uuid: parse(attachment_uuid)?.0,
            run_uuid: parse(run_uuid)?.0,
            descriptor: descriptor.inner.clone(),
        };
        let result = py
            .detach(|| self.inner.attach_resolved_run(&projection, request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one immutable algorithm-run identity.
    #[pyo3(signature = (run_uuid, *, cancellation=None))]
    fn algorithm_run(
        &self,
        py: Python<'_>,
        run_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let run_uuid = canonical_operation_id(run_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| self.inner.algorithm_run(run_uuid, cancellation.clone()))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound run page.
    #[pyo3(signature = (*, algorithm=None, limit=100, after=None, cancellation=None))]
    fn list_algorithm_runs(
        &self,
        py: Python<'_>,
        algorithm: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let algorithm = algorithm
            .map(parse_algorithm_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_algorithm_runs(graphforge_api::ListAlgorithmRunsRequest {
                        algorithm,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound lifecycle page.
    #[pyo3(signature = (run_uuid, *, limit=100, after=None, cancellation=None))]
    fn algorithm_run_events(
        &self,
        py: Python<'_>,
        run_uuid: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let run_uuid = canonical_operation_id(run_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner.algorithm_run_events(
                    run_uuid,
                    graphforge_api::PageRequest {
                        limit,
                        after,
                        cancellation: cancellation.clone(),
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Detect communities/components (`by=`). Returns a `pyarrow.Table`.
    #[allow(clippy::too_many_arguments)] // kwarg-rich v0.5 cluster() signature
    #[pyo3(signature = (label, *, by, vector_property=None, via=None, directed=false, write_property=None))]
    fn cluster(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        vector_property: Option<&str>,
        via: Option<&str>,
        directed: bool,
        write_property: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let opts = graphforge_api::ClusterOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            vector_property: vector_property.map(str::to_owned),
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        };
        let label = label.to_owned();
        algorithm_result(py, py.detach(|| self.inner.cluster(&label, opts)))
    }

    /// Path-finding / flow between nodes (`by=`). Returns a `pyarrow.Table`.
    #[allow(clippy::too_many_arguments)] // kwarg-rich v0.5 paths() signature
    #[pyo3(signature = (source=None, target=None, *, by, via=None, directed=true, k=1, weight=None, capacity_property=None, cost_property=None, heuristic=None, walk_length=None, seed=None, terminal_uuids=None, prize_property=None))]
    fn paths(
        &self,
        py: Python<'_>,
        source: Option<&Bound<'_, PyAny>>,
        target: Option<&Bound<'_, PyAny>>,
        by: &str,
        via: Option<&str>,
        directed: bool,
        k: usize,
        weight: Option<&str>,
        capacity_property: Option<&str>,
        cost_property: Option<&str>,
        heuristic: Option<&str>,
        walk_length: Option<usize>,
        seed: Option<u64>,
        terminal_uuids: Option<Vec<String>>,
        prize_property: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let terminal_uuids = terminal_uuids.unwrap_or_default();
        let terminal_uuids =
            parse_terminal_uuids(&terminal_uuids).map_err(|error| to_pyerr(py, &error))?;
        let opts = graphforge_api::PathsOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            via: via.map(str::to_owned),
            directed,
            k,
            weight: weight.map(str::to_owned),
            capacity_property: capacity_property.map(str::to_owned),
            cost_property: cost_property.map(str::to_owned),
            heuristic: heuristic.map(str::to_owned),
            walk_length,
            seed,
            terminal_uuids,
            prize_property: prize_property.map(str::to_owned),
        };
        let source = source
            .map(|value| py_to_node_selector(py, value))
            .transpose()?;
        let target = target
            .map(|value| py_to_node_selector(py, value))
            .transpose()?;
        algorithm_result(
            py,
            py.detach(|| self.inner.paths(source.as_ref(), target.as_ref(), opts)),
        )
    }

    /// Graph-level structural metric (`by=`). Returns a `pyarrow.Table`.
    #[allow(clippy::too_many_arguments)] // kwarg-rich v0.5 analyze() signature
    #[pyo3(signature = (label=None, *, by, via=None, directed=None, weight=None, partition_property=None, k=None, embedding_options=None))]
    fn analyze(
        &self,
        py: Python<'_>,
        label: Option<&str>,
        by: &str,
        via: Option<&str>,
        directed: Option<bool>,
        weight: Option<&str>,
        partition_property: Option<&str>,
        k: Option<usize>,
        embedding_options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let algorithm = by.parse().map_err(|error| to_pyerr(py, &error))?;
        let label = label.map(str::to_owned);
        if matches!(
            algorithm,
            graphforge_api::AnalyzeAlgorithm::Node2Vec
                | graphforge_api::AnalyzeAlgorithm::GraphSage
                | graphforge_api::AnalyzeAlgorithm::FastRandomProjection
                | graphforge_api::AnalyzeAlgorithm::HashGnn
        ) {
            if partition_property.is_some() || k.is_some() {
                return Err(embedding_validation(
                    py,
                    "embedding algorithms do not accept partition_property or k",
                ));
            }
            let directed = directed.unwrap_or(!matches!(
                algorithm,
                graphforge_api::AnalyzeAlgorithm::GraphSage
            ));
            let options = embedding_options_from_kwargs(
                py,
                algorithm,
                via,
                directed,
                weight,
                embedding_options,
            )?;
            validate_embedding_options(&options).map_err(|error| to_pyerr(py, &error))?;
            return algorithm_result(
                py,
                py.detach(|| self.inner.analyze_embedding(label.as_deref(), &options)),
            );
        }
        if embedding_options.is_some() {
            return Err(embedding_validation(
                py,
                format!("{by} does not accept embedding_options"),
            ));
        }
        // Keep binding construction extension-safe as AnalyzeOptions gains fields.
        #[allow(clippy::needless_update)]
        let opts = graphforge_api::AnalyzeOptions {
            by: algorithm,
            via: via.map(str::to_owned),
            directed: directed.unwrap_or(true),
            weight: weight.map(str::to_owned),
            k,
            partition_property: partition_property.map(str::to_owned),
            ..graphforge_api::AnalyzeOptions::default()
        };
        algorithm_result(py, py.detach(|| self.inner.analyze(label.as_deref(), opts)))
    }

    /// Pairwise node similarity (`by=`). Returns a `pyarrow.Table`.
    #[pyo3(signature = (label, *, by, k=10, vector_property=None, via=None))]
    fn similar(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        k: usize,
        vector_property: Option<&str>,
        via: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let opts = graphforge_api::SimilarOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            k,
            vector_property: vector_property.map(str::to_owned),
            via: via.map(str::to_owned),
        };
        let label = label.to_owned();
        algorithm_result(py, py.detach(|| self.inner.similar(&label, opts)))
    }

    /// Configure one opt-in OpenRouter session shared by provider indexing and find.
    #[pyo3(signature = (credential, *, origin, model, revision="unavailable", response_contract_version="v1", capabilities=None, max_input_tokens=1_000_000, transport_timeout_millis=30_000, estimated_cost_microunits_per_token=1))]
    #[allow(clippy::too_many_arguments)]
    fn configure_openrouter(
        &mut self,
        py: Python<'_>,
        credential: String,
        origin: String,
        model: String,
        revision: &str,
        response_contract_version: &str,
        capabilities: Option<Vec<String>>,
        max_input_tokens: u64,
        transport_timeout_millis: u64,
        estimated_cost_microunits_per_token: u64,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let request_limits = ProviderRequestLimits::default();
        let execution_limits = ProviderExecutionLimits::default();
        let config = OpenRouterProviderSessionConfig {
            origin,
            model,
            revision: revision.to_owned(),
            response_contract_version: response_contract_version.to_owned(),
            capabilities: provider_capabilities(capabilities)
                .map_err(|error| to_pyerr(py, &error))?,
            max_input_tokens,
            chunking: None,
            wire_limits: OpenRouterWireLimits::default(),
            request_limits,
            execution_limits,
            transport_timeout: Duration::from_millis(transport_timeout_millis),
            estimated_cost_microunits_per_token,
        };
        let session = py
            .detach(|| OpenRouterProviderSession::new(config, credential))
            .map_err(|error| to_pyerr(py, &error))?;
        self.provider = Some(ConfiguredProviderBinding {
            session,
            request_limits,
            execution_limits,
        });
        Ok(())
    }

    /// Inspect one content-free provider property-embedding plan without network work.
    #[pyo3(signature = (name, label, properties, *, dimensions, normalization="none", replace=false))]
    #[allow(clippy::too_many_arguments)]
    fn inspect_provider_embedding_plan(
        &self,
        py: Python<'_>,
        name: &str,
        label: &str,
        properties: Vec<String>,
        dimensions: u32,
        normalization: &str,
        replace: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let configured = self.provider.as_ref().ok_or_else(|| {
            to_pyerr(
                py,
                &GfError::Validation("OpenRouter is not configured".to_owned()),
            )
        })?;
        let request = provider_plan_request(
            configured,
            name,
            label,
            properties,
            dimensions,
            normalization,
            replace,
        )
        .map_err(|error| to_pyerr(py, &error))?;
        let inspection = py
            .detach(|| {
                configured
                    .session
                    .inspect_embedding_plan(&self.inner, &request)
            })
            .map_err(|error| to_pyerr(py, &GfError::Execution(error.to_string())))?;
        provider_plan_to_python(py, inspection)
    }

    /// Confirm, execute, and atomically publish one provider embedding generation.
    #[pyo3(signature = (name, label, properties, *, dimensions, normalization="none", replace=false))]
    #[allow(clippy::too_many_arguments)]
    fn publish_provider_embeddings(
        &self,
        py: Python<'_>,
        name: &str,
        label: &str,
        properties: Vec<String>,
        dimensions: u32,
        normalization: &str,
        replace: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let configured = self.provider.as_ref().ok_or_else(|| {
            to_pyerr(
                py,
                &GfError::Validation("OpenRouter is not configured".to_owned()),
            )
        })?;
        let request = provider_plan_request(
            configured,
            name,
            label,
            properties,
            dimensions,
            normalization,
            replace,
        )
        .map_err(|error| to_pyerr(py, &error))?;
        let space = py
            .detach(|| configured.session.publish_embeddings(&self.inner, &request))
            .map_err(|error| to_pyerr(py, &GfError::Execution(error.to_string())))?;
        embedding_space_to_python(py, space)
    }

    /// Text + vector hybrid search. Returns a `pyarrow.Table`.
    #[pyo3(signature = (query=None, *, label=None, vector=None, similar_to=None, semantic_query=None, limit=10, space=None, force_stale=false, rerank=None, suppress_rerank_advisory=false))]
    #[allow(clippy::too_many_arguments)]
    fn find(
        &self,
        py: Python<'_>,
        query: Option<&str>,
        label: Option<&str>,
        vector: Option<Vec<f32>>,
        similar_to: Option<&Bound<'_, PyAny>>,
        semantic_query: Option<&str>,
        limit: usize,
        space: Option<&str>,
        force_stale: bool,
        rerank: Option<&Bound<'_, PyDict>>,
        suppress_rerank_advisory: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let similar_to = similar_to
            .map(|value| py_to_node_selector(py, value))
            .transpose()?;
        let opts = graphforge_api::FindOptions {
            query: query.map(str::to_owned),
            label: label.map(str::to_owned),
            vector,
            similar_to,
            semantic_query: semantic_query.map(str::to_owned),
            limit,
            space: space.map(str::to_owned),
            force_stale,
        };
        let rerank = match (rerank, self.provider.as_ref()) {
            (Some(value), Some(configured)) => Some(py_rerank_options(py, value, configured)?),
            (Some(_), None) => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(
                        "rerank requires a configured OpenRouter session".to_owned(),
                    ),
                ));
            }
            (None, _) => None,
        };
        let omitted_reranker = self.provider.as_ref().and_then(|configured| {
            (rerank.is_none()
                && configured
                    .session
                    .contract()
                    .capabilities()
                    .supports(ProviderCapability::CandidateReranking))
            .then(|| configured.session.contract().clone())
        });
        let execution = FindExecutionOptions {
            find: opts,
            rerank,
            omitted_reranker,
            advisory_policy: if suppress_rerank_advisory {
                RerankAdvisoryPolicy::Suppress
            } else {
                RerankAdvisoryPolicy::Emit
            },
        };
        let result = match self.provider.as_ref() {
            Some(configured) => py.detach(|| configured.session.find(&self.inner, execution)),
            None => py.detach(|| self.inner.find_with_diagnostics(execution, None)),
        }
        .map_err(|error| to_pyerr(py, &error))?;
        let (batch, diagnostics, _) = result.into_parts();
        emit_find_warnings(py, &diagnostics)?;
        algorithm_result(py, Ok(batch))
    }

    /// Atomically publish one complete caller-supplied UUID/vector generation.
    #[pyo3(signature = (name, rows, *, dimensions, source_projection, contract_version="graphforge_binding_caller_v1", normalization="none", replace=false))]
    #[allow(clippy::too_many_arguments)]
    fn publish_caller_embeddings(
        &self,
        py: Python<'_>,
        name: &str,
        rows: &Bound<'_, PyList>,
        dimensions: u32,
        source_projection: &Bound<'_, PyDict>,
        contract_version: &str,
        normalization: &str,
        replace: bool,
    ) -> PyResult<String> {
        self.ensure_open()?;
        let normalization = match normalization {
            "none" => CallerEmbeddingNormalization::None,
            "l2" => CallerEmbeddingNormalization::L2,
            other => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(format!(
                        "unknown caller embedding normalization {other:?}"
                    )),
                ));
            }
        };
        let request = CallerEmbeddingBatchRequest {
            display_name: name.to_owned(),
            contract_version: contract_version.to_owned(),
            dimensions,
            normalization,
            distance: CallerEmbeddingDistance::Cosine,
            source_projection_recipe: string_map(source_projection)?,
            rows: caller_embedding_rows(py, rows)?,
            replace_alias: replace,
        };
        let published = py
            .detach(|| self.inner.publish_caller_embeddings(request))
            .map_err(|error| to_pyerr(py, &error))?;
        Ok(published.compatibility_id)
    }

    /// Atomically publish one complete canonical algorithm embedding Arrow result.
    #[pyo3(signature = (name, result, *, algorithm, algorithm_version, dimensions, input_recipe, source_projection, hyperparameters=None, normalization="none", replace=false))]
    #[allow(clippy::too_many_arguments)]
    fn publish_algorithm_embeddings(
        &self,
        py: Python<'_>,
        name: &str,
        result: &Bound<'_, PyAny>,
        algorithm: &str,
        algorithm_version: &str,
        dimensions: u32,
        input_recipe: &Bound<'_, PyDict>,
        source_projection: &Bound<'_, PyDict>,
        hyperparameters: Option<&Bound<'_, PyDict>>,
        normalization: &str,
        replace: bool,
    ) -> PyResult<String> {
        self.ensure_open()?;
        let normalization = match normalization {
            "none" => AlgorithmEmbeddingNormalization::None,
            "l2" => AlgorithmEmbeddingNormalization::L2,
            other => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(format!(
                        "unknown algorithm embedding normalization {other:?}"
                    )),
                ));
            }
        };
        let algorithm = algorithm.parse().map_err(|error| to_pyerr(py, &error))?;
        let request = AlgorithmEmbeddingPublicationRequest {
            display_name: name.to_owned(),
            algorithm,
            algorithm_version: algorithm_version.to_owned(),
            dimensions,
            normalization,
            distance: AlgorithmEmbeddingDistance::Cosine,
            hyperparameters: json_map(hyperparameters)?,
            input_recipe: json_map(Some(input_recipe))?,
            source_projection_recipe: json_map(Some(source_projection))?,
            result: pyarrow_table_to_batch(result)?,
            replace_alias: replace,
        };
        let published = py
            .detach(|| self.inner.publish_algorithm_embeddings(request))
            .map_err(|error| to_pyerr(py, &error))?;
        Ok(published.compatibility_id)
    }

    /// List verified embedding-space lineages in deterministic Rust order.
    fn embedding_spaces(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.ensure_open()?;
        py.detach(|| self.inner.embedding_spaces())
            .map_err(|error| to_pyerr(py, &error))?
            .into_iter()
            .map(|space| embedding_space_to_python(py, space))
            .collect()
    }

    /// Inspect one embedding-space alias, or the configured default.
    #[pyo3(signature = (name=None))]
    fn embedding_space(&self, py: Python<'_>, name: Option<&str>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let name = name.map(str::to_owned);
        let space = py
            .detach(|| self.inner.embedding_space(name.as_deref()))
            .map_err(|error| to_pyerr(py, &error))?;
        embedding_space_to_python(py, space)
    }

    /// Bind one alias to an existing verified compatibility lineage.
    #[pyo3(signature = (name, compatibility_id, *, replace=false))]
    fn bind_embedding_space_alias(
        &self,
        py: Python<'_>,
        name: &str,
        compatibility_id: &str,
        replace: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let name = name.to_owned();
        let compatibility_id = compatibility_id.to_owned();
        let space = py
            .detach(|| {
                self.inner
                    .bind_embedding_space_alias(&name, &compatibility_id, replace)
            })
            .map_err(|error| to_pyerr(py, &error))?;
        embedding_space_to_python(py, space)
    }

    /// Remove one alias without deleting primary vector data.
    fn remove_embedding_space_alias(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        self.ensure_open()?;
        let name = name.to_owned();
        py.detach(|| self.inner.remove_embedding_space_alias(&name))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Delete one named/default compatibility lineage and every targeting alias.
    #[pyo3(signature = (name=None))]
    fn delete_embedding_space(&self, py: Python<'_>, name: Option<&str>) -> PyResult<bool> {
        self.ensure_open()?;
        let name = name.map(str::to_owned);
        py.detach(|| self.inner.delete_embedding_space(name.as_deref()))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Select one existing alias as default, or clear the default with `None`.
    #[pyo3(signature = (name=None))]
    fn set_default_embedding_space(
        &self,
        py: Python<'_>,
        name: Option<&str>,
    ) -> PyResult<Option<Py<PyAny>>> {
        self.ensure_open()?;
        let name = name.map(str::to_owned);
        py.detach(|| self.inner.set_default_embedding_space(name.as_deref()))
            .map_err(|error| to_pyerr(py, &error))?
            .map(|space| embedding_space_to_python(py, space))
            .transpose()
    }

    /// Inspect one active embedding generation's Rust-owned freshness decision.
    #[pyo3(signature = (name=None, *, force_stale=false))]
    fn inspect_embedding_space_freshness(
        &self,
        py: Python<'_>,
        name: Option<&str>,
        force_stale: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let name = name.map(str::to_owned);
        let freshness = py
            .detach(|| {
                self.inner
                    .inspect_embedding_space_freshness(name.as_deref(), force_stale)
            })
            .map_err(|error| to_pyerr(py, &error))?;
        refresh_freshness_to_python(py, freshness)
    }

    /// Read the durable project-wide embedding refresh defaults.
    fn embedding_refresh_project_policy(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let policy = py
            .detach(|| self.inner.embedding_refresh_project_policy())
            .map_err(|error| to_pyerr(py, &error))?;
        refresh_project_policy_to_python(py, policy)
    }

    /// Replace the durable project-wide embedding refresh defaults.
    #[pyo3(signature = (*, proactive, debounce_millis, max_concurrent_jobs))]
    fn set_embedding_refresh_project_policy(
        &self,
        py: Python<'_>,
        proactive: bool,
        debounce_millis: u64,
        max_concurrent_jobs: usize,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let policy = py
            .detach(|| {
                self.inner
                    .set_embedding_refresh_project_policy(EmbeddingRefreshProjectPolicy {
                        proactive,
                        debounce: Duration::from_millis(debounce_millis),
                        max_concurrent_jobs,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        refresh_project_policy_to_python(py, policy)
    }

    /// Set or explicitly clear one lineage's durable refresh override.
    #[pyo3(signature = (name=None, *, proactive=None, debounce_millis=None, clear=false))]
    fn set_embedding_refresh_space_policy(
        &self,
        py: Python<'_>,
        name: Option<&str>,
        proactive: Option<bool>,
        debounce_millis: Option<u64>,
        clear: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let policy = if clear {
            if proactive.is_some() || debounce_millis.is_some() {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(
                        "clearing an embedding refresh space policy cannot include overrides"
                            .to_owned(),
                    ),
                ));
            }
            None
        } else {
            if proactive.is_none() && debounce_millis.is_none() {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(
                        "embedding refresh space policy requires an override or clear=True"
                            .to_owned(),
                    ),
                ));
            }
            Some(EmbeddingRefreshSpacePolicy {
                proactive,
                debounce: debounce_millis.map(Duration::from_millis),
            })
        };
        let name = name.map(str::to_owned);
        let inspection = py
            .detach(|| {
                self.inner
                    .set_embedding_refresh_space_policy(name.as_deref(), policy)
            })
            .map_err(|error| to_pyerr(py, &error))?;
        refresh_inspection_to_python(py, inspection)
    }

    /// Inspect durable refresh state and this process's worker counters.
    #[pyo3(signature = (name=None))]
    fn inspect_embedding_refresh(&self, py: Python<'_>, name: Option<&str>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let name = name.map(str::to_owned);
        let inspection = py
            .detach(|| self.inner.inspect_embedding_refresh(name.as_deref()))
            .map_err(|error| to_pyerr(py, &error))?;
        refresh_inspection_to_python(py, inspection)
    }

    /// Build, reuse, or replace one graph-native text/vector search index.
    ///
    /// The legacy no-keyword `index("adjacency")` call remains compatible;
    /// new code should use `index_adjacency()` for the unambiguous operation.
    #[pyo3(signature = (label, **kwargs))]
    fn index(
        &self,
        py: Python<'_>,
        label: &str,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let label = label.to_owned();
        if kwargs.is_none_or(pyo3::types::PyDictMethods::is_empty) && label == "adjacency" {
            py.detach(|| self.inner.index(&label))
                .map_err(|error| to_pyerr(py, &error))?;
            return Ok(py.None());
        }
        let options = search_index_options_from_kwargs(py, kwargs)?;
        let receipt = py
            .detach(|| self.inner.index_search(&label, options))
            .map_err(|error| to_pyerr(py, &error))?;
        receipt.map_or_else(
            || Ok(py.None()),
            |value| text_index_inspection_to_python(py, value),
        )
    }

    /// Inspect a graph-native text index without building it.
    #[pyo3(signature = (label, *, properties=None))]
    #[allow(clippy::needless_pass_by_value)]
    fn inspect_text_index(
        &self,
        py: Python<'_>,
        label: &str,
        properties: Option<Vec<String>>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let label = label.to_owned();
        let inspection = py
            .detach(|| self.inner.inspect_text_index(&label, properties.as_deref()))
            .map_err(|error| to_pyerr(py, &error))?;
        text_index_inspection_to_python(py, inspection)
    }

    /// Explicitly build the derived CSR adjacency index.
    fn index_adjacency(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let inspection = py
            .detach(|| self.inner.index_adjacency())
            .map_err(|error| to_pyerr(py, &error))?;
        adjacency_inspection_to_python(py, inspection)
    }

    /// Inspect the derived adjacency index without rebuilding it.
    fn inspect_adjacency(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let inspection = py
            .detach(|| self.inner.inspect_adjacency())
            .map_err(|error| to_pyerr(py, &error))?;
        adjacency_inspection_to_python(py, inspection)
    }

    /// Rebuild adjacency with an optional shared cancellation token.
    #[pyo3(signature = (*, cancellation=None))]
    fn rebuild_adjacency(
        &self,
        py: Python<'_>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let inspection = py
            .detach(|| self.inner.rebuild_adjacency(cancellation))
            .map_err(|error| to_pyerr(py, &error))?;
        adjacency_inspection_to_python(py, inspection)
    }

    // ----- Construction (write API).

    /// Add one node through the Rust facade and return its UUID handle.
    #[pyo3(signature = (label, **props))]
    fn add_node(
        &self,
        py: Python<'_>,
        label: &str,
        props: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyNodeHandle> {
        self.ensure_open()?;
        let props = props_from_dict(props)?;
        let label = label.to_owned();
        py.detach(|| self.inner.add_node(&label, &props))
            .map(|inner| PyNodeHandle { inner })
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Add a directed edge and return its graph UUID handle.
    #[pyo3(signature = (src, rel_type, dst, **props))]
    fn add_edge(
        &self,
        py: Python<'_>,
        src: &PyNodeHandle,
        rel_type: &str,
        dst: &PyNodeHandle,
        props: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyEdgeHandle> {
        self.ensure_open()?;
        let props = props_from_dict(props)?;
        let src = src.inner.clone();
        let dst = dst.inner.clone();
        let rel_type = rel_type.to_owned();
        py.detach(|| self.inner.add_edge(&src, &rel_type, &dst, &props))
            .map(|inner| PyEdgeHandle { inner })
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Publish one composite graph + knowledge/epistemic transaction through Rust.
    ///
    /// Python only converts the request; validation, staging, publication,
    /// recovery, and idempotency stay in Rust. Returns the canonical Arrow
    /// receipt without a bespoke wrapper.
    #[pyo3(signature = (*, operation_uuid, graph_mutations, knowledge=None, actor_uuid=None, contract_version=1))]
    fn publish_composite_transaction(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        graph_mutations: &Bound<'_, PyList>,
        knowledge: Option<&Bound<'_, PyDict>>,
        actor_uuid: Option<&str>,
        contract_version: u32,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request = composite::py_composite_request(
            py,
            operation_uuid,
            graph_mutations,
            knowledge,
            actor_uuid,
            contract_version,
        )?;
        let receipt = py
            .detach(|| self.inner.publish_composite_transaction(request))
            .map_err(|error| to_pyerr(py, &error))?;
        record_batch_to_pyarrow_table(py, &receipt)
    }

    /// Publish one atomic bulk node batch through the Rust-owned Arrow contract.
    ///
    /// `data` may be a `pyarrow.Table`, Arrow-compatible DataFrame, or
    /// `list[dict]` of canonical node rows.
    #[pyo3(signature = (operation_uuid, data))]
    fn publish_bulk_nodes(
        &self,
        py: Python<'_>,
        operation_uuid: &Bound<'_, PyAny>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            py_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let batch = py_bulk_input_to_batch(py, data)?;
        let receipt = py
            .detach(|| self.inner.publish_bulk_nodes(operation_uuid, &[batch]))
            .map_err(|error| bulk_node_publication_error(py, error))?;
        record_batch_to_pyarrow_table(py, &receipt)
    }

    /// Publish one atomic bulk edge batch through the Rust-owned Arrow contract.
    ///
    /// `data` may be a `pyarrow.Table`, Arrow-compatible DataFrame, or
    /// `list[dict]` of canonical edge rows.
    #[pyo3(signature = (operation_uuid, data))]
    fn publish_bulk_edges(
        &self,
        py: Python<'_>,
        operation_uuid: &Bound<'_, PyAny>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            py_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let batch = py_bulk_input_to_batch(py, data)?;
        let receipt = py
            .detach(|| self.inner.publish_bulk_edges(operation_uuid, &[batch]))
            .map_err(|error| bulk_edge_publication_error(py, error))?;
        record_batch_to_pyarrow_table(py, &receipt)
    }

    /// Bulk-add nodes by normalizing convenience containers onto
    /// [`Self::publish_bulk_nodes`].
    ///
    /// `data` may omit `label` / `node_uuid`; the binding injects the provided
    /// label and nullable UUID column, then forwards the canonical Arrow batch.
    #[pyo3(signature = (label, data, *, operation_uuid))]
    fn add_nodes(
        &self,
        py: Python<'_>,
        label: &str,
        data: &Bound<'_, PyAny>,
        operation_uuid: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            py_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let batch = ensure_bulk_node_batch(py, label, data)?;
        let receipt = py
            .detach(|| self.inner.publish_bulk_nodes(operation_uuid, &[batch]))
            .map_err(|error| bulk_node_publication_error(py, error))?;
        record_batch_to_pyarrow_table(py, &receipt)
    }

    /// Bulk-add edges by normalizing convenience containers onto
    /// [`Self::publish_bulk_edges`].
    ///
    /// Endpoint columns default to `src_id` / `dst_id` and are renamed to the
    /// canonical `source_uuid` / `target_uuid` fields before Rust publication.
    #[pyo3(signature = (rel_type, data, *, operation_uuid, src="src_id", dst="dst_id"))]
    fn add_edges(
        &self,
        py: Python<'_>,
        rel_type: &str,
        data: &Bound<'_, PyAny>,
        operation_uuid: &Bound<'_, PyAny>,
        src: &str,
        dst: &str,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            py_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let batch = ensure_bulk_edge_batch(py, rel_type, data, src, dst)?;
        let receipt = py
            .detach(|| self.inner.publish_bulk_edges(operation_uuid, &[batch]))
            .map_err(|error| bulk_edge_publication_error(py, error))?;
        record_batch_to_pyarrow_table(py, &receipt)
    }

    /// Remove all nodes and edges (in-memory instances only).
    fn clear(&self, py: Python<'_>) -> PyResult<()> {
        self.ensure_open()?;
        py.detach(|| self.inner.clear())
            .map_err(|e| to_pyerr(py, &e))
    }

    // ----- Introspection.

    /// Sorted label and relationship counts as a `pyarrow.Table`.
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        algorithm_result(py, py.detach(|| self.inner.schema()))
    }

    /// The node labels present in the graph.
    fn labels(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.ensure_open()?;
        py.detach(|| self.inner.labels())
            .map_err(|e| to_pyerr(py, &e))
    }

    /// The relationship types present in the graph.
    fn relationship_types(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        self.ensure_open()?;
        py.detach(|| self.inner.relationship_types())
            .map_err(|e| to_pyerr(py, &e))
    }

    /// Count nodes (optionally for one `label`).
    #[pyo3(signature = (label=None))]
    fn node_count(&self, py: Python<'_>, label: Option<&str>) -> PyResult<u64> {
        self.ensure_open()?;
        let label = label.map(str::to_owned);
        py.detach(|| self.inner.node_count(label.as_deref().unwrap_or("")))
            .map_err(|e| to_pyerr(py, &e))
    }

    /// Close the instance; subsequent operations raise `LifecycleError`.
    /// Idempotent. Storage is flushed/released when the handle is dropped.
    fn close(&mut self) {
        self.closed = true;
    }

    /// The storage path, or `None` for an in-memory instance.
    #[getter]
    fn path(&self) -> Option<String> {
        self.inner.path().map(|p| p.display().to_string())
    }

    /// The effective ontology mode: `"exploratory"` | `"advisory"` | `"strict"`.
    #[getter]
    fn ontology_mode(&self) -> String {
        format!("{:?}", self.inner.ontology_mode()).to_lowercase()
    }

    fn __repr__(&self) -> String {
        self.inner.path().map_or_else(
            || "GraphForge(in-memory)".to_owned(),
            |p| format!("GraphForge(path={})", p.display()),
        )
    }
}

fn parse_belief_projection_policy(
    included_statuses: &[String],
    statusless: &str,
    supersession_branches: &str,
    hypotheses: &str,
) -> Result<graphforge_api::BeliefProjectionPolicyV1, GfError> {
    let included_statuses = included_statuses
        .iter()
        .map(|status| match status.as_str() {
            "hypothesis" => Ok(graphforge_api::AssertionStatus::Hypothesis),
            "supported" => Ok(graphforge_api::AssertionStatus::Supported),
            "refuted" => Ok(graphforge_api::AssertionStatus::Refuted),
            "disputed" => Ok(graphforge_api::AssertionStatus::Disputed),
            "retracted" => Ok(graphforge_api::AssertionStatus::Retracted),
            "superseded" => Ok(graphforge_api::AssertionStatus::Superseded),
            _ => Err(GfError::Validation(
                "included_statuses contains an unknown status".into(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let statusless = match statusless {
        "reject" => graphforge_api::StatuslessPolicyV1::Reject,
        "exclude" => graphforge_api::StatuslessPolicyV1::Exclude,
        "include" => graphforge_api::StatuslessPolicyV1::Include,
        _ => {
            return Err(GfError::Validation(
                "statusless must be reject, exclude, or include".into(),
            ));
        }
    };
    let supersession_branches = match supersession_branches {
        "reject" => graphforge_api::SupersessionBranchPolicyV1::Reject,
        "include_all_leaves" => graphforge_api::SupersessionBranchPolicyV1::IncludeAllLeaves,
        _ => {
            return Err(GfError::Validation(
                "supersession_branches must be reject or include_all_leaves".into(),
            ));
        }
    };
    let hypotheses = match hypotheses {
        "require_selected" => graphforge_api::HypothesisSelectionPolicyV1::RequireSelected,
        "exclude_unselected_group" => {
            graphforge_api::HypothesisSelectionPolicyV1::ExcludeUnselectedGroup
        }
        "include_all_current_members" => {
            graphforge_api::HypothesisSelectionPolicyV1::IncludeAllCurrentMembers
        }
        _ => {
            return Err(GfError::Validation(
                "hypotheses must be require_selected, exclude_unselected_group, or include_all_current_members".into(),
            ));
        }
    };
    Ok(graphforge_api::BeliefProjectionPolicyV1 {
        included_statuses,
        statusless,
        supersession_branches,
        hypotheses,
    })
}

fn resolved_recorded_result_to_python(
    py: Python<'_>,
    value: graphforge_api::ResolvedRecordedAlgorithmResult,
    requested_attachment_uuid: String,
) -> PyResult<PyResolvedRecordedAlgorithmResult> {
    let run_uuid = value.recorded.run_uuid.to_string();
    let result = result_to_pyarrow(py, &value.recorded.result)?;
    match value.attachment {
        graphforge_api::ResolvedAttachmentOutcome::Attached(attachment) => {
            Ok(PyResolvedRecordedAlgorithmResult {
                run_uuid,
                result,
                attachment_state: "attached",
                attachment: Some(result_to_pyarrow(py, &attachment)?),
                attachment_uuid: Some(requested_attachment_uuid),
                attachment_error_code: None,
            })
        }
        graphforge_api::ResolvedAttachmentOutcome::Failed {
            attachment_uuid,
            run_uuid: _,
            error_code,
        } => Ok(PyResolvedRecordedAlgorithmResult {
            run_uuid,
            result,
            attachment_state: "attachment_failed",
            attachment: None,
            attachment_uuid: Some(attachment_uuid.to_string()),
            attachment_error_code: Some(error_code),
        }),
    }
}

#[derive(Default)]
struct GilReleaseProbeState {
    entered: bool,
    released: bool,
}

static GIL_RELEASE_PROBE: LazyLock<(Mutex<GilReleaseProbeState>, Condvar)> =
    LazyLock::new(|| (Mutex::new(GilReleaseProbeState::default()), Condvar::new()));

/// Deterministic native blocking probe used by wheel acceptance tests.
#[pyfunction]
fn _test_gil_release_probe(py: Python<'_>) {
    py.detach(|| {
        let (lock, condition) = &*GIL_RELEASE_PROBE;
        let mut state = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.released = false;
        state.entered = true;
        condition.notify_all();
        while !state.released {
            state = condition
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        state.entered = false;
    });
}

/// Wait until the blocking probe has entered native code without holding the GIL.
#[pyfunction]
fn _test_gil_release_probe_wait(py: Python<'_>) {
    py.detach(|| {
        let (lock, condition) = &*GIL_RELEASE_PROBE;
        let mut state = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.entered {
            state = condition
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    });
}

/// Release the deterministic native blocking probe.
#[pyfunction]
fn _test_gil_release_probe_signal() {
    let (lock, condition) = &*GIL_RELEASE_PROBE;
    let mut state = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.released = true;
    condition.notify_all();
}

#[derive(Default)]
struct WriterHoldProbeState {
    held: Option<graphforge_api::concurrency_test_support::HeldWriter>,
}

static WRITER_HOLD_PROBE: LazyLock<Mutex<WriterHoldProbeState>> =
    LazyLock::new(|| Mutex::new(WriterHoldProbeState::default()));

/// Stage and retain the project writer lock for concurrency acceptance tests.
#[pyfunction]
fn _test_acquire_writer_hold(py: Python<'_>, path: &str) -> PyResult<()> {
    {
        let state = WRITER_HOLD_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.held.is_some() {
            return Err(to_pyerr(
                py,
                &GfError::Validation("writer-hold probe already active".into()),
            ));
        }
    }
    let owned = path.to_owned();
    let held = py
        .detach(|| graphforge_api::concurrency_test_support::hold_writer(&owned))
        .map_err(|error| to_pyerr(py, &error))?;
    let mut state = WRITER_HOLD_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.held.is_some() {
        return Err(to_pyerr(
            py,
            &GfError::Validation("writer-hold probe already active".into()),
        ));
    }
    state.held = Some(held);
    Ok(())
}

/// Drop the staged writer hold created by [`_test_acquire_writer_hold`].
#[pyfunction]
fn _test_release_writer_hold(py: Python<'_>) -> PyResult<()> {
    let mut state = WRITER_HOLD_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.held.take().is_none() {
        return Err(to_pyerr(
            py,
            &GfError::Validation("writer-hold probe is not active".into()),
        ));
    }
    Ok(())
}

/// Execute the Rust-owned CLI without reimplementing parsing or behavior in Python.
#[pyfunction]
fn _cli_execute(
    py: Python<'_>,
    args: Vec<String>,
) -> (i32, Bound<'_, PyBytes>, Bound<'_, PyBytes>) {
    let execution = graphforge_cli::execute(args);
    (
        execution.exit_code,
        PyBytes::new(py, &execution.stdout),
        PyBytes::new(py, &execution.stderr),
    )
}

/// GraphForge native extension module (`graphforge._graphforge_rs`).
#[pymodule]
fn _graphforge_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(composite::composite_provenance_uuid, m)?)?;
    m.add_function(wrap_pyfunction!(_test_gil_release_probe, m)?)?;
    m.add_function(wrap_pyfunction!(_test_gil_release_probe_wait, m)?)?;
    m.add_function(wrap_pyfunction!(_test_gil_release_probe_signal, m)?)?;
    m.add_function(wrap_pyfunction!(_test_acquire_writer_hold, m)?)?;
    m.add_function(wrap_pyfunction!(_test_release_writer_hold, m)?)?;
    m.add_function(wrap_pyfunction!(_cli_execute, m)?)?;
    m.add_class::<GraphForge>()?;
    m.add_class::<PyCheckpointView>()?;
    m.add_class::<PyCancellationToken>()?;
    m.add_class::<PyNodeHandle>()?;
    m.add_class::<PyEdgeHandle>()?;
    m.add_class::<PyInvocationDescriptor>()?;
    m.add_class::<PyRecordedAlgorithmResult>()?;
    m.add_class::<PyResolvedBeliefProjection>()?;
    m.add_class::<PyResolvedRecordedAlgorithmResult>()?;
    m.add("GraphForgeError", py.get_type::<GraphForgeError>())?;
    m.add("ParseError", py.get_type::<ParseError>())?;
    m.add("PlanError", py.get_type::<PlanError>())?;
    m.add("ExecutionError", py.get_type::<ExecutionError>())?;
    m.add("StorageError", py.get_type::<StorageError>())?;
    m.add("LifecycleError", py.get_type::<LifecycleError>())?;
    m.add("ValidationError", py.get_type::<ValidationError>())?;
    m.add("OntologyError", py.get_type::<OntologyError>())?;
    Ok(())
}

/// Returns the crate version.
#[pyfunction]
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steiner_terminals_are_checked_and_preserve_input_order() {
        let first = "018f0f4e-7b8c-7000-8000-000000000002".to_owned();
        let second = "018f0f4e-7b8c-7000-8000-000000000001".to_owned();
        let terminals = parse_terminal_uuids(&[first.clone(), second.clone()]).unwrap();

        let NodeSelector::Uuid(first_uuid) = NodeSelector::uuid(&first).unwrap() else {
            unreachable!()
        };
        let NodeSelector::Uuid(second_uuid) = NodeSelector::uuid(&second).unwrap() else {
            unreachable!()
        };
        assert_eq!(
            terminals,
            vec![*first_uuid.as_bytes(), *second_uuid.as_bytes()]
        );

        assert!(matches!(
            parse_terminal_uuids(&["not-a-uuid".to_owned()]),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            parse_terminal_uuids(&[first.to_uppercase()]),
            Err(GfError::Validation(_))
        ));
    }
}
