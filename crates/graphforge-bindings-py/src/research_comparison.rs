//! Thin native semantic comparison transport.
use super::{GraphForge, PyCancellationToken, py_to_json_value, result_to_pyarrow, to_pyerr};
use pyo3::prelude::*;
#[pymethods]
impl GraphForge {
    /// Execute the native ResearchComparisonRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn compare_research(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ResearchComparisonRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid research comparison JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.compare_research(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
}
