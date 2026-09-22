//! Thin native reference, export, and independent Fork transport.
use super::{GraphForge, PyCancellationToken, json_value_to_python, py_to_json_value, to_pyerr};
use pyo3::prelude::*;
#[pymethods]
impl GraphForge {
    /// Execute the native ResearchReferenceTarget contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn research_reference(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ResearchReferenceTarget =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid research interchange JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.research_reference(&request, &token))
            .map_err(|error| to_pyerr(py, &error))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("native research metadata serializes"),
        )
    }
    /// Execute the native ExportResearchRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn export_research(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ExportResearchRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid research interchange JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.export_research(&request, &token))
            .map_err(|error| crate::portable::to_portable_pyerr(py, error))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("native research metadata serializes"),
        )
    }
    /// Execute the native ForkResearchRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn fork_research(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ForkResearchRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid research interchange JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.fork_research(&request, &token))
            .map_err(|error| crate::portable::to_portable_pyerr(py, error))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("native research metadata serializes"),
        )
    }
}
