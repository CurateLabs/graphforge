//! Thin projections of Rust-owned immutable research Version contracts.
use super::{
    GraphForge, canonical_operation_id, json_value_to_python, py_to_json_value, result_to_pyarrow,
    to_pyerr,
};
use pyo3::prelude::*;

#[pymethods]
impl GraphForge {
    /// Freeze a complete-Project capture with owner-derived evidence.
    fn prepare_research_version(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let request = serde_json::from_value(py_to_json_value(request)?).map_err(|_| {
            to_pyerr(
                py,
                &graphforge_api::GfError::Validation(
                    "invalid research Version JSON contract".into(),
                ),
            )
        })?;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.prepare_research_version(request))
            .map_err(|error| to_pyerr(py, &error))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("research request serializes"),
        )
    }

    /// Commit an exact prepared request; preserve it unchanged for retries.
    #[pyo3(signature = (operation, *, cancellation=None))]
    fn commit_research_version_operation(
        &mut self,
        py: Python<'_>,
        operation: &Bound<'_, PyAny>,
        cancellation: Option<&super::PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let operation = serde_json::from_value(py_to_json_value(operation)?).map_err(|_| {
            to_pyerr(
                py,
                &graphforge_api::GfError::Validation(
                    "invalid research Version JSON contract".into(),
                ),
            )
        })?;
        let graph = self.ensure_open_mut()?;
        let cancellation = cancellation
            .map(|token| token.inner.clone())
            .unwrap_or_default();
        let result = py
            .detach(|| graph.commit_research_version_operation(operation, &cancellation))
            .map_err(|error| to_pyerr(py, &error))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("research receipt serializes"),
        )
    }

    /// Inspect frozen citation and content identity.
    fn research_version(&self, py: Python<'_>, version_uuid: &str) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(version_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.research_version(id))
            .map_err(|error| to_pyerr(py, &error))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("research Version serializes"),
        )
    }

    /// List immutable Version identities and labels as Arrow.
    fn list_research_versions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.list_research_versions())
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Inspect current retention dependencies and durable operation receipts.
    fn research_version_retention(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.research_version_retention())
            .map_err(|error| to_pyerr(py, &error))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("research retention serializes"),
        )
    }

    /// Run read-only native Cypher against exact historical research.
    fn query_research_version(
        &self,
        py: Python<'_>,
        version_uuid: &str,
        query: &str,
    ) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(version_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.open_research_version(id)?.execute(query))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Read exact historical Artifact metadata and availability as Arrow.
    fn research_version_artifact(
        &self,
        py: Python<'_>,
        version_uuid: &str,
        artifact_uuid: &str,
    ) -> PyResult<Py<PyAny>> {
        let version = canonical_operation_id(version_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let artifact = canonical_operation_id(artifact_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.open_research_version(version)?.artifact(artifact))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }
    /// Read exact historical local Artifact bytes as Arrow.
    fn research_version_artifact_payload(
        &self,
        py: Python<'_>,
        version_uuid: &str,
        artifact_uuid: &str,
    ) -> PyResult<Py<PyAny>> {
        let version = canonical_operation_id(version_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let artifact = canonical_operation_id(artifact_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| {
                graph
                    .open_research_version(version)?
                    .artifact_payload(artifact)
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }
    /// Read frozen ontology metadata, independent of current composition.
    fn research_version_ontology(&self, py: Python<'_>, version_uuid: &str) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(version_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.open_research_version(id)?.workspace_ontology())
            .map_err(|error| to_pyerr(py, &error))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("ontology serializes"),
        )
    }

    /// Read frozen Project research metadata.
    fn research_version_metadata(&self, py: Python<'_>, version_uuid: &str) -> PyResult<Py<PyAny>> {
        let id = canonical_operation_id(version_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let graph = self.ensure_open()?;
        let result = py
            .detach(|| graph.open_research_version(id)?.research_project_metadata())
            .map_err(|error| to_pyerr(py, &error))?;
        json_value_to_python(
            py,
            &serde_json::to_value(result).expect("research metadata serializes"),
        )
    }
}
