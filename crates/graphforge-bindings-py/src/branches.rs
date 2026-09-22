//! Thin projections of native Branch publication and immutable reads.
use super::{
    GraphForge, PyCancellationToken, canonical_operation_id, json_value_to_python,
    py_to_json_value, result_to_pyarrow, to_pyerr,
};
use pyo3::prelude::*;
#[pymethods]
impl GraphForge {
    /// Inspect the permanent exact-membership creation selector as Arrow.
    fn research_branch_selection(&self, py: Python<'_>, branch_uuid: &str) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(branch_uuid)
            .map_err(|e| to_pyerr(py, &e))?
            .0;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.research_branch_selection(id))
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }

    /// Execute the native CreateResearchBranchRequest contract with exact replay semantics.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn create_research_branch(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::CreateResearchBranchRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Branch JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let receipt = py
            .detach(|| graph.create_research_branch(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(receipt).expect("Branch receipt serializes"),
        )
    }
    /// Execute the native ExecuteResearchBranchRequest contract with exact replay semantics.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn execute_research_branch(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ExecuteResearchBranchRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Branch JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let receipt = py
            .detach(|| graph.execute_research_branch(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(receipt).expect("Branch receipt serializes"),
        )
    }
    /// Execute the native RestoreResearchBranchRequest contract with exact replay semantics.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn restore_research_branch(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::RestoreResearchBranchRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Branch JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let receipt = py
            .detach(|| graph.restore_research_branch(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(receipt).expect("Branch receipt serializes"),
        )
    }
    /// Execute the native ChangeResearchBranchOntologyRequest contract with exact replay semantics.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn change_research_branch_ontology(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ChangeResearchBranchOntologyRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Branch JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let receipt = py
            .detach(|| graph.change_research_branch_ontology(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(receipt).expect("Branch receipt serializes"),
        )
    }
    /// Execute the native ReferenceResearchBranchRequest contract with exact replay semantics.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn reference_research_branch(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::ReferenceResearchBranchRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Branch JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let receipt = py
            .detach(|| graph.reference_research_branch(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(receipt).expect("Branch receipt serializes"),
        )
    }
    /// Execute the native BringResearchBranchRequest contract with exact replay semantics.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn bring_research_branch(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::BringResearchBranchRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Branch JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let receipt = py
            .detach(|| graph.bring_research_branch(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(receipt).expect("Branch receipt serializes"),
        )
    }
    /// Execute the native SuppressResearchBranchAssertionRequest contract with exact replay semantics.
    #[pyo3(signature = (request, *, cancellation=None))]
    fn suppress_research_branch_assertion(
        &mut self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let request: graphforge_api::SuppressResearchBranchAssertionRequest =
            serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
                to_pyerr(
                    py,
                    &graphforge_api::GfError::Validation("invalid Branch JSON contract".into()),
                )
            })?;
        let token = cancellation.map(|t| t.inner.clone()).unwrap_or_default();
        let graph = self.ensure_open_mut()?;
        let receipt = py
            .detach(|| graph.suppress_research_branch_assertion(&request, &token))
            .map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(
            py,
            &serde_json::to_value(receipt).expect("Branch receipt serializes"),
        )
    }
    /// Inspect immutable genealogy and the exact opened current Version.
    fn research_branch(&self, py: Python<'_>, branch_uuid: &str) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(branch_uuid)
            .map_err(|e| to_pyerr(py, &e))?
            .0;
        let graph = self.ensure_open()?;
        let value = py.detach(|| graph.open_research_branch(id).map(|view| serde_json::json!({"record": view.record(), "version_uuid": view.version_uuid()}))).map_err(|e| to_pyerr(py, &e))?;
        json_value_to_python(py, &value)
    }
    /// Read the exact native effective Branch view as Arrow.
    fn research_branch_fields(&self, py: Python<'_>, branch_uuid: &str) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(branch_uuid)
            .map_err(|e| to_pyerr(py, &e))?
            .0;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| {
                graph
                    .open_research_branch(id)
                    .and_then(|view| view.fields())
            })
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Read the exact native effective Branch view as Arrow.
    fn research_branch_references(&self, py: Python<'_>, branch_uuid: &str) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(branch_uuid)
            .map_err(|e| to_pyerr(py, &e))?
            .0;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| {
                graph
                    .open_research_branch(id)
                    .and_then(|view| view.references())
            })
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
    /// Read the exact native effective Branch view as Arrow.
    fn query_research_branch(
        &self,
        py: Python<'_>,
        branch_uuid: &str,
        query: &str,
    ) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(branch_uuid)
            .map_err(|e| to_pyerr(py, &e))?
            .0;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| {
                graph
                    .open_research_branch(id)
                    .and_then(|view| view.graph().execute(query))
            })
            .map_err(|e| to_pyerr(py, &e))?;
        result_to_pyarrow(py, &result)
    }
}
