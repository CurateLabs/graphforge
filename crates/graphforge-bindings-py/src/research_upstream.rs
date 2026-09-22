//! Thin transport for native upstream review, publication and immutable history.
use super::{
    GraphForge, PyCancellationToken, json_value_to_python, py_to_json_value, result_to_pyarrow,
    to_pyerr,
};
use pyo3::prelude::*;
#[pymethods]
impl GraphForge {
    /// Execute the native PreviewResearchUpstreamRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn preview_research_upstream(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::PreviewResearchUpstreamRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid upstream research JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation
            .map(|token| token.inner.clone())
            .unwrap_or_default();
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.preview_research_upstream(&request, &token))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }
    /// Execute the native UpdateResearchBranchRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn update_research_branch(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::UpdateResearchBranchRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid upstream research JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation
            .map(|token| token.inner.clone())
            .unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let result = py
            .detach(|| graph.update_research_branch(&request, &token))
            .map_err(|error| to_pyerr(py, &error))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("native receipt serializes"),
        )
    }
    /// Execute the native ResearchUpstreamHistoryRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn research_upstream_history(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ResearchUpstreamHistoryRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid upstream research JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation
            .map(|token| token.inner.clone())
            .unwrap_or_default();
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.research_upstream_history(&request, &token))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }
}
