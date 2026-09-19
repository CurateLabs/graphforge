//! Research Project metadata and bounded local discovery bindings.

use super::{
    GraphForge, canonical_operation_id, json_value_to_python, py_to_json_value, result_to_pyarrow,
    to_pyerr,
};
use graphforge_api::{
    DiscoverResearchProjectsRequest, ResearchProjectDiscoveryLimits, ResearchProjectDiscoveryQuery,
    UpdateResearchMetadataRequest, WorkspaceResearchMetadata, WriteContext,
};
use pyo3::prelude::*;
use pyo3::types::PyList;

#[pymethods]
impl GraphForge {
    /// Inspect authoritative research metadata for the open Project.
    fn research_project_metadata(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let metadata = py
            .detach(|| native.research_project_metadata())
            .map_err(|error| to_pyerr(py, &error))?;
        let value = serde_json::to_value(metadata).map_err(|error| {
            to_pyerr(py, &graphforge_api::GfError::Validation(error.to_string()))
        })?;
        json_value_to_python(py, &value)
    }

    /// Return one metadata-only summary for the open Project.
    fn research_project_summary(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let summary = py
            .detach(|| native.research_project_summary())
            .map_err(|error| to_pyerr(py, &error))?;
        let value = serde_json::json!({
            "project_path": summary.project_path.display().to_string(),
            "identity": {
                "volume_serial": summary.identity.volume_serial,
                "file_id_hex": summary.identity.file_id_hex,
                "generation_uuid": summary.identity.generation_uuid.hyphenated().to_string(),
            },
            "metadata": summary.metadata,
        });
        json_value_to_python(py, &value)
    }

    /// Replace authoritative research metadata atomically.
    #[pyo3(signature = (metadata, *, operation_uuid, actor_uuid=None))]
    fn update_research_metadata(
        &mut self,
        py: Python<'_>,
        metadata: &Bound<'_, PyAny>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<()> {
        let native = self.ensure_open_mut()?;
        let metadata: WorkspaceResearchMetadata =
            serde_json::from_value(py_to_json_value(metadata)?).map_err(|error| {
                to_pyerr(py, &graphforge_api::GfError::Validation(error.to_string()))
            })?;
        let request = UpdateResearchMetadataRequest {
            context: WriteContext {
                operation_uuid: canonical_operation_id(operation_uuid)
                    .map_err(|error| to_pyerr(py, &error))?,
                actor_uuid: actor_uuid
                    .map(canonical_operation_id)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?
                    .map(|id| id.0),
            },
            metadata,
        };
        py.detach(|| native.update_research_metadata(request))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Discover caller-supplied local Projects without opening graph payloads.
    #[staticmethod]
    #[pyo3(signature = (project_roots, *, free_text=None, languages=None, subjects=None, ontologies=None, source_types=None, temporal_label=None, max_projects=1024, max_candidates=1024))]
    #[allow(clippy::too_many_arguments)]
    fn discover_research_projects(
        py: Python<'_>,
        project_roots: &Bound<'_, PyList>,
        free_text: Option<String>,
        languages: Option<&Bound<'_, PyList>>,
        subjects: Option<&Bound<'_, PyList>>,
        ontologies: Option<&Bound<'_, PyList>>,
        source_types: Option<&Bound<'_, PyList>>,
        temporal_label: Option<String>,
        max_projects: usize,
        max_candidates: usize,
    ) -> PyResult<Py<PyAny>> {
        let roots = project_roots
            .iter()
            .map(|entry| {
                entry
                    .extract::<String>()
                    .map(std::path::PathBuf::from)
                    .map_err(|error| {
                        to_pyerr(py, &graphforge_api::GfError::Validation(error.to_string()))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let request = DiscoverResearchProjectsRequest {
            project_roots: roots,
            query: ResearchProjectDiscoveryQuery {
                free_text,
                languages: string_list(py, languages)?,
                subjects: string_list(py, subjects)?,
                ontologies: string_list(py, ontologies)?,
                source_types: string_list(py, source_types)?,
                temporal_label,
            },
            limits: ResearchProjectDiscoveryLimits {
                max_projects,
                max_candidates,
            },
        };
        let result = py
            .detach(|| graphforge_api::GraphForge::discover_research_projects(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }
}

fn string_list(py: Python<'_>, values: Option<&Bound<'_, PyList>>) -> PyResult<Vec<String>> {
    match values {
        None => Ok(Vec::new()),
        Some(values) => values
            .iter()
            .map(|entry| {
                entry.extract::<String>().map_err(|error| {
                    to_pyerr(py, &graphforge_api::GfError::Validation(error.to_string()))
                })
            })
            .collect(),
    }
}
