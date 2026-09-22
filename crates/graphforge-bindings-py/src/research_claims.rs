//! Thin native contextual claim transports; all behavior stays in Rust.
use super::{
    GraphForge, PyCancellationToken, json_value_to_python, py_to_json_value, result_to_pyarrow,
    to_pyerr,
};
use pyo3::prelude::*;
#[pymethods]
impl GraphForge {
    /// Execute the native CreateResearchClaimRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn create_research_claim(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::CreateResearchClaimRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid contextual research JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let result = py
            .detach(|| graph.create_research_claim(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Execute the native RelateResearchClaimsRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn relate_research_claims(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::RelateResearchClaimsRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid contextual research JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let result = py
            .detach(|| graph.relate_research_claims(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Execute the native RecordResearchDecisionsRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn record_research_decisions(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::RecordResearchDecisionsRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid contextual research JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let result = py
            .detach(|| graph.record_research_decisions(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Execute the native InspectResearchClaimsRequest contract.
    fn inspect_research_claims(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::InspectResearchClaimsRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid contextual research JSON contract".into(),
                    ),
                )
            })?;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.inspect_research_claims(&request))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Execute the native ResearchClaimHistoryRequest contract.
    fn research_claim_history(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ResearchClaimHistoryRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid contextual research JSON contract".into(),
                    ),
                )
            })?;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.research_claim_history(&request))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Execute the native ResearchAuthorityQuery contract.
    fn research_decision_history(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ResearchAuthorityQuery =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid contextual research JSON contract".into(),
                    ),
                )
            })?;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.research_decision_history(&request.context, request.community_uuid))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Execute the native ResearchAuthorityQuery contract.
    fn research_canonical_choices(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ResearchAuthorityQuery =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid contextual research JSON contract".into(),
                    ),
                )
            })?;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.research_canonical_choices(&request.context, request.community_uuid))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Execute the native ChangeResearchBranchClaimRequest contract.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn change_research_branch_claim(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ChangeResearchBranchClaimRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation(
                        "invalid contextual research JSON contract".into(),
                    ),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let result = py
            .detach(|| graph.change_research_branch_claim(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("native research receipt serializes"),
        )
    }
}
