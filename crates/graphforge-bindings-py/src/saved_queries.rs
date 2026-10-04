//! Thin projections of project-owned saved query definitions and execution.
use super::{
    GraphForge, PyCancellationToken, canonical_operation_id, json_value_to_python,
    params_from_dict, py_to_json_value, result_to_pyarrow, to_pyerr,
};
use graphforge_api::{SavedQuery, SavedQuerySource};
use pyo3::{prelude::*, types::PyDict};

fn contract<T: serde::de::DeserializeOwned>(
    py: Python<'_>,
    value: serde_json::Value,
) -> PyResult<T> {
    serde_json::from_value(value).map_err(|_| {
        to_pyerr(
            py,
            &graphforge_api::GfError::Validation("invalid saved query JSON contract".into()),
        )
    })
}
fn source(py: Python<'_>, value: Option<&Bound<'_, PyAny>>) -> PyResult<SavedQuerySource> {
    value
        .map(|value| contract(py, py_to_json_value(value)?))
        .unwrap_or(Ok(SavedQuerySource::Current))
}
fn output(py: Python<'_>, value: impl serde::Serialize) -> PyResult<Py<PyAny>> {
    json_value_to_python(
        py,
        &serde_json::to_value(value).expect("saved query metadata serializes"),
    )
}

#[pymethods]
impl GraphForge {
    fn create_saved_query(
        &mut self,
        py: Python<'_>,
        definition: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let definition: SavedQuery = contract(py, py_to_json_value(definition)?)?;
        let graph = self.ensure_open_mut()?;
        let result = py
            .detach(|| graph.create_saved_query(definition))
            .map_err(|e| to_pyerr(py, &e))?;
        output(py, result)
    }
    fn update_saved_query(
        &mut self,
        py: Python<'_>,
        definition: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let definition: SavedQuery = contract(py, py_to_json_value(definition)?)?;
        let graph = self.ensure_open_mut()?;
        let result = py
            .detach(|| graph.update_saved_query(definition))
            .map_err(|e| to_pyerr(py, &e))?;
        output(py, result)
    }
    fn delete_saved_query(&mut self, py: Python<'_>, query_uuid: &str) -> PyResult<()> {
        let id = canonical_operation_id(query_uuid)
            .map_err(|e| to_pyerr(py, &e))?
            .0;
        let graph = self.ensure_open_mut()?;
        py.detach(|| graph.delete_saved_query(id))
            .map_err(|e| to_pyerr(py, &e))
    }
    #[pyo3(signature = (query_uuid, *, source=None))]
    fn saved_query(
        &self,
        py: Python<'_>,
        query_uuid: &str,
        source: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(query_uuid)
            .map_err(|e| to_pyerr(py, &e))?
            .0;
        let source = self::source(py, source)?;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.saved_query_at(id, &source))
            .map_err(|e| to_pyerr(py, &e))?;
        output(py, result)
    }
    #[pyo3(signature = (*, source=None))]
    fn saved_queries(
        &self,
        py: Python<'_>,
        source: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let source = self::source(py, source)?;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.saved_queries_at(&source))
            .map_err(|e| to_pyerr(py, &e))?;
        output(py, result)
    }
    #[pyo3(signature = (query_uuid, params=None, *, source=None, cancellation=None))]
    fn execute_saved_query(
        &self,
        py: Python<'_>,
        query_uuid: &str,
        params: Option<&Bound<'_, PyDict>>,
        source: Option<&Bound<'_, PyAny>>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(query_uuid)
            .map_err(|e| to_pyerr(py, &e))?
            .0;
        let params = params_from_dict(params)?;
        let source = self::source(py, source)?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.execute_saved_query(id, &params, &source, cancellation.as_ref()))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
}
