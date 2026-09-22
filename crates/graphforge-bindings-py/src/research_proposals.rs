//! Thin native proposal JSON and Arrow transport.
use super::{
    GraphForge, PyCancellationToken, json_value_to_python, py_to_json_value, result_to_pyarrow,
    to_pyerr,
};
use pyo3::prelude::*;
#[pymethods]
impl GraphForge {
    /// Execute the native SubmitResearchProposalRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn submit_research_proposal(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::SubmitResearchProposalRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Proposal JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let result = py
            .detach(|| graph.submit_research_proposal(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("native receipt serializes"),
        )
    }
    /// Execute the native PreviewResearchProposalRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn preview_research_proposal(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::PreviewResearchProposalRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Proposal JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.preview_research_proposal(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Execute the native ReviewResearchProposalRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn review_research_proposal(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ReviewResearchProposalRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Proposal JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let result = py
            .detach(|| graph.review_research_proposal(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("native receipt serializes"),
        )
    }
    /// Execute the native ReleaseResearchProposalRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn release_research_proposal(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ReleaseResearchProposalRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Proposal JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let result = py
            .detach(|| graph.release_research_proposal(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("native receipt serializes"),
        )
    }
    /// Execute the native ResearchProposalHistoryRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn research_proposal_history(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ResearchProposalHistoryRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Proposal JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.research_proposal_history(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
}
