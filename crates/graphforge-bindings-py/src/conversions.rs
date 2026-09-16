//! Conversions Python binding methods and conversions.

use super::{rename_map, to_pyerr};
use arrow::array::RecordBatch;
use arrow::pyarrow::FromPyArrow;
use arrow::pyarrow::Table;
use arrow::pyarrow::ToPyArrow;
use graphforge_api::BulkEdgePublicationError;
use graphforge_api::BulkNodePublicationError;
use graphforge_api::ExecutionResult;
use graphforge_api::GfError;
use graphforge_api::IrLiteral;
use graphforge_api::PropValue;
use graphforge_api::TemporalValue;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyInt;
use pyo3::types::PyList;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

/// Convert a Python value to the matching [`IrLiteral`] for a query parameter.
/// `bool` is checked before `int` (Python `bool` is an `int` subclass).
pub(super) fn py_to_ir_literal(v: &Bound<'_, PyAny>) -> PyResult<IrLiteral> {
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
    } else if let Ok(dict) = value.cast::<PyDict>() {
        let is_spatial = dict.contains("spatial_type")? || dict.contains("coordinates")?;
        let is_temporal = dict.contains("type")?;
        if !is_spatial && !is_temporal {
            return Err(PyTypeError::new_err(
                "unsupported node property type (plain nested dictionaries are not properties)",
            ));
        }
        let json = py_property_json(value).map_err(|error| {
            to_pyerr(
                value.py(),
                &GfError::Validation(format!("invalid canonical spatial property: {error}")),
            )
        })?;
        if is_spatial {
            let spatial: graphforge_api::SpatialValue =
                serde_json::from_value(json).map_err(|error| {
                    to_pyerr(
                        value.py(),
                        &GfError::Validation(format!(
                            "invalid canonical spatial property: {error}"
                        )),
                    )
                })?;
            spatial.validate_interchange_profile().map_err(|error| {
                to_pyerr(
                    value.py(),
                    &GfError::Validation(format!("invalid canonical spatial property: {error}")),
                )
            })?;
            Ok(PropValue::Spatial(spatial))
        } else {
            let temporal = serde_json::from_value::<TemporalValue>(json).map_err(|error| {
                PyTypeError::new_err(format!("invalid temporal property: {error}"))
            })?;
            temporal
                .validate()
                .map_err(|error| to_pyerr(value.py(), &error))?;
            Ok(PropValue::Temporal(temporal))
        }
    } else {
        Err(PyTypeError::new_err(
            "unsupported node property type (expected None/bool/int/float/str/list/temporal or canonical spatial dict)",
        ))
    }
}

fn py_property_json(value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if value.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Ok(value) = value.extract::<bool>() {
        Ok(serde_json::Value::Bool(value))
    } else if value.is_instance_of::<PyInt>() {
        Ok(serde_json::Value::Number(value.extract::<i64>()?.into()))
    } else if let Ok(value) = value.extract::<f64>() {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| PyTypeError::new_err("spatial coordinates must be finite"))
    } else if let Ok(value) = value.extract::<String>() {
        Ok(serde_json::Value::String(value))
    } else if let Ok(values) = value.cast::<PyList>() {
        values
            .iter()
            .map(|item| py_property_json(&item))
            .collect::<PyResult<Vec<_>>>()
            .map(serde_json::Value::Array)
    } else if let Ok(values) = value.cast::<PyDict>() {
        let mut object = serde_json::Map::with_capacity(values.len());
        for (key, value) in values {
            object.insert(key.extract::<String>()?, py_property_json(&value)?);
        }
        Ok(serde_json::Value::Object(object))
    } else {
        Err(PyTypeError::new_err(
            "structured properties contain only dict/list/string/number/bool/None values",
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

pub(super) fn string_map(values: &Bound<'_, PyDict>) -> PyResult<BTreeMap<String, String>> {
    values
        .iter()
        .map(|(key, value)| Ok((key.extract::<String>()?, value.extract::<String>()?)))
        .collect()
}

pub(super) fn py_to_json_value(value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
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

pub(crate) fn json_value_to_python(
    py: Python<'_>,
    value: &serde_json::Value,
) -> PyResult<Py<PyAny>> {
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

pub(super) fn json_map(
    values: Option<&Bound<'_, PyDict>>,
) -> PyResult<BTreeMap<String, serde_json::Value>> {
    let mut mapped = BTreeMap::new();
    if let Some(values) = values {
        for (key, value) in values {
            mapped.insert(key.extract::<String>()?, py_to_json_value(&value)?);
        }
    }
    Ok(mapped)
}

pub(super) fn pyarrow_table_to_batch(value: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
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
pub(crate) fn py_bulk_input_to_batch(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<RecordBatch> {
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

pub(super) fn ensure_bulk_node_batch(
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

pub(super) fn ensure_bulk_edge_batch(
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

/// Build the `$param` map from a Python `dict` (or empty when `None`).
pub(crate) fn params_from_dict(
    params: Option<&Bound<'_, PyDict>>,
) -> PyResult<HashMap<String, IrLiteral>> {
    let mut out = HashMap::new();
    if let Some(dict) = params {
        for (k, v) in dict.iter() {
            out.insert(k.extract::<String>()?, py_to_ir_literal(&v)?);
        }
    }
    Ok(out)
}

/// Transfer an [`ExecutionResult`] to a `pyarrow.Table` via the Arrow C Data
/// Interface, preserving the schema (and its `graphforge.*` metadata). Internal
/// execution/storage batches are coalesced at this non-streaming binding
/// boundary so Python observes one logical result batch. `execute_stream`
/// retains the engine's genuine streaming batches. An empty result yields one
/// typed zero-row batch.
pub(super) fn result_to_pyarrow(py: Python<'_>, result: &ExecutionResult) -> PyResult<Py<PyAny>> {
    let logical = if result.batches.is_empty() {
        RecordBatch::new_empty(Arc::clone(&result.schema))
    } else {
        arrow::compute::concat_batches(&result.schema, &result.batches)
            .map_err(|error| to_pyerr(py, &GfError::Execution(error.to_string())))?
    };
    let batch = logical.to_pyarrow(py)?;
    let schema = result.schema.to_pyarrow(py)?;
    let table = py
        .import("pyarrow")?
        .getattr("Table")?
        .call_method1("from_batches", ([batch], schema))?;
    Ok(table.unbind())
}

/// Transfer a native analyst-algorithm batch to a `pyarrow.Table` without reshaping it.
pub(super) fn algorithm_result(
    py: Python<'_>,
    r: Result<RecordBatch, GfError>,
) -> PyResult<Py<PyAny>> {
    let batch = r.map_err(|error| to_pyerr(py, &error))?;
    record_batch_to_pyarrow_table(py, &batch)
}

pub(super) fn record_batch_to_pyarrow_table(
    py: Python<'_>,
    batch: &RecordBatch,
) -> PyResult<Py<PyAny>> {
    let schema = batch.schema().to_pyarrow(py)?;
    let batch = batch.to_pyarrow(py)?;
    let table = py
        .import("pyarrow")?
        .getattr("Table")?
        .call_method1("from_batches", ([batch], schema))?;
    Ok(table.unbind())
}

pub(super) fn bulk_node_publication_error(
    py: Python<'_>,
    error: BulkNodePublicationError,
) -> PyErr {
    match error {
        BulkNodePublicationError::Validation(error) => {
            to_pyerr(py, &GfError::Validation(error.to_string()))
        }
        BulkNodePublicationError::Publication(error) => to_pyerr(py, &error),
    }
}

pub(super) fn bulk_edge_publication_error(
    py: Python<'_>,
    error: BulkEdgePublicationError,
) -> PyErr {
    match error {
        BulkEdgePublicationError::Validation(error) => {
            to_pyerr(py, &GfError::Validation(error.to_string()))
        }
        BulkEdgePublicationError::Publication(error) => to_pyerr(py, &error),
    }
}
