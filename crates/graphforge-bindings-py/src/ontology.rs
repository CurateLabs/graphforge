//! Ontology Python binding methods and conversions.

use super::{
    GraphForge, PyCancellationToken, canonical_operation_id, json_value_to_python,
    py_to_json_value, to_pyerr,
};
use crate::multi_ontology;
use graphforge_api::GfError;
use graphforge_api::WriteContext;
use pyo3::prelude::*;

pub(super) fn ontology_mode(value: &str) -> Result<graphforge_api::OntologyMode, GfError> {
    match value {
        "advisory" => Ok(graphforge_api::OntologyMode::Advisory),
        "strict" => Ok(graphforge_api::OntologyMode::Strict),
        _ => Err(GfError::Validation(
            "ontology mode must be advisory or strict".into(),
        )),
    }
}

fn ontology_export_format(value: &str) -> Result<graphforge_api::OntologyExportFormat, GfError> {
    match value {
        "yaml" | "yml" => Ok(graphforge_api::OntologyExportFormat::Yaml),
        "json" => Ok(graphforge_api::OntologyExportFormat::Json),
        _ => Err(GfError::Validation(
            "ontology export format must be yaml or json".into(),
        )),
    }
}

pub(super) fn rename_map<'py>(
    py: Python<'py>,
    names: &[String],
    from: &str,
    to: &str,
) -> PyResult<Bound<'py, pyo3::types::PyList>> {
    let renamed = pyo3::types::PyList::empty(py);
    for name in names {
        if name == from {
            renamed.append(to)?;
        } else {
            renamed.append(name.as_str())?;
        }
    }
    Ok(renamed)
}

#[pymethods]
impl GraphForge {
    /// Load and apply an ontology from `path` (YAML/JSON by extension).
    fn load_ontology(&mut self, py: Python<'_>, path: &str) -> PyResult<()> {
        let native = self.ensure_open_mut()?;
        let path = path.to_owned();
        py.detach(|| native.load_ontology(&path))
            .map_err(|e| to_pyerr(py, &e))
    }

    /// Return the stable, deterministically ordered runtime-catalog contract.
    fn inspect_runtime_catalog(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let snapshot = py
            .detach(|| native.inspect_runtime_catalog())
            .map_err(|error| to_pyerr(py, &error))?;
        let value = serde_json::to_value(snapshot)
            .map_err(|error| to_pyerr(py, &GfError::Validation(error.to_string())))?;
        json_value_to_python(py, &value)
    }

    /// Suggest a conservative, explicitly non-authoritative ontology draft.
    fn suggest_ontology(
        &self,
        py: Python<'_>,
        ontology_id: &str,
        version: &str,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let ontology_id = ontology_id.to_owned();
        let version = version.to_owned();
        let suggestion = py
            .detach(|| {
                native.suggest_ontology(graphforge_api::OntologySuggestionOptions {
                    ontology_id,
                    version,
                })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        let value = serde_json::json!({
            "draft": suggestion.draft,
            "document": suggestion.document,
            "fingerprint_sha256": suggestion.fingerprint_sha256,
            "omitted_relation_types": suggestion.omitted_relation_types,
        });
        json_value_to_python(py, &value)
    }

    /// Validate an ontology document without changing live or durable state.
    fn validate_ontology(
        &self,
        py: Python<'_>,
        document: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let document: graphforge_api::OntologyDoc =
            serde_json::from_value(py_to_json_value(document)?)
                .map_err(|error| to_pyerr(py, &GfError::Validation(error.to_string())))?;
        let report = py.detach(|| native.validate_ontology(&document));
        let diagnostics = report
            .diagnostics
            .into_iter()
            .map(|diagnostic| {
                serde_json::json!({
                    "kind": diagnostic.kind.to_string(),
                    "location": diagnostic.location,
                    "message": diagnostic.message,
                })
            })
            .collect::<Vec<_>>();
        json_value_to_python(
            py,
            &serde_json::json!({ "valid": report.valid, "diagnostics": diagnostics }),
        )
    }

    /// Atomically export an explicit ontology source as YAML or JSON.
    #[pyo3(signature = (source, destination, format, *, document=None))]
    fn export_ontology(
        &self,
        py: Python<'_>,
        source: &str,
        destination: &str,
        format: &str,
        document: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let native = self.ensure_open()?;
        let source = match source {
            "loaded" => graphforge_api::OntologyExportSource::Loaded,
            "adopted" => graphforge_api::OntologyExportSource::Adopted,
            "suggested" => {
                let document = document.ok_or_else(|| {
                    to_pyerr(
                        py,
                        &GfError::Validation(
                            "document is required for suggested ontology export".into(),
                        ),
                    )
                })?;
                let document = serde_json::from_value(py_to_json_value(document)?)
                    .map_err(|error| to_pyerr(py, &GfError::Validation(error.to_string())))?;
                graphforge_api::OntologyExportSource::Suggested(document)
            }
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(
                        "ontology export source must be suggested, loaded, or adopted".into(),
                    ),
                ));
            }
        };
        let format = ontology_export_format(format).map_err(|error| to_pyerr(py, &error))?;
        let destination = std::path::PathBuf::from(destination);
        py.detach(|| native.export_ontology(source, &destination, format))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Inspect the generation-managed authoritative ontology record.
    fn workspace_ontology(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let record = py
            .detach(|| native.workspace_ontology())
            .map_err(|error| to_pyerr(py, &error))?;
        let value = serde_json::to_value(record)
            .map_err(|error| to_pyerr(py, &GfError::Validation(error.to_string())))?;
        json_value_to_python(py, &value)
    }

    /// Adopt an ontology as durable project authority.
    #[pyo3(signature = (path, mode, *, operation_uuid, actor_uuid=None))]
    fn adopt_ontology(
        &mut self,
        py: Python<'_>,
        path: &str,
        mode: &str,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<()> {
        let native = self.ensure_open_mut()?;
        let request = graphforge_api::AdoptOntologyRequest {
            context: WriteContext {
                operation_uuid: canonical_operation_id(operation_uuid)
                    .map_err(|error| to_pyerr(py, &error))?,
                actor_uuid: actor_uuid
                    .map(canonical_operation_id)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?
                    .map(|operation| operation.0),
            },
            path: path.into(),
            mode: ontology_mode(mode).map_err(|error| to_pyerr(py, &error))?,
        };
        py.detach(|| native.adopt_ontology(request))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Publish explicit durable ontology absence.
    #[pyo3(signature = (*, operation_uuid, actor_uuid=None))]
    fn clear_ontology(
        &mut self,
        py: Python<'_>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<()> {
        let native = self.ensure_open_mut()?;
        let request = graphforge_api::ClearOntologyRequest {
            context: WriteContext {
                operation_uuid: canonical_operation_id(operation_uuid)
                    .map_err(|error| to_pyerr(py, &error))?,
                actor_uuid: actor_uuid
                    .map(canonical_operation_id)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?
                    .map(|operation| operation.0),
            },
        };
        py.detach(|| native.clear_ontology(request))
            .map_err(|error| to_pyerr(py, &error))
    }

    fn ontology_modules(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        multi_ontology::ontology_modules(self, py)
    }

    fn ontology_authority_state(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        multi_ontology::authority_state(self, py)
    }

    #[pyo3(signature = (ontology_id, *, authored_version=None, canonical_digest=None))]
    fn inspect_ontology_module(
        &self,
        py: Python<'_>,
        ontology_id: &str,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::inspect_module(self, py, ontology_id, authored_version, canonical_digest)
    }

    fn validate_ontology_module(
        &self,
        py: Python<'_>,
        document: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::validate_module(self, py, document)
    }

    #[pyo3(signature = (document, dependencies, *, enforcement=None))]
    fn create_ontology_module(
        &self,
        py: Python<'_>,
        document: &Bound<'_, PyAny>,
        dependencies: &Bound<'_, PyAny>,
        enforcement: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::create_module(self, py, document, dependencies, enforcement)
    }

    #[pyo3(signature = (text, dependencies, *, format="auto"))]
    fn import_ontology_module(
        &self,
        py: Python<'_>,
        text: &str,
        dependencies: &Bound<'_, PyAny>,
        format: &str,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::import_module(self, py, text, format, dependencies)
    }

    #[pyo3(signature = (candidate, *, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn adopt_ontology_module(
        &mut self,
        py: Python<'_>,
        candidate: &Bound<'_, PyAny>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::adopt_module(
            self,
            py,
            candidate,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
            cancellation,
        )
    }

    #[pyo3(signature = (ontology_id, document, dependencies, *, authored_version=None, canonical_digest=None))]
    fn preview_update_ontology_module(
        &self,
        py: Python<'_>,
        ontology_id: &str,
        document: &Bound<'_, PyAny>,
        dependencies: &Bound<'_, PyAny>,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::preview_update_module(
            self,
            py,
            ontology_id,
            authored_version,
            canonical_digest,
            document,
            dependencies,
        )
    }

    #[pyo3(signature = (ontology_id, document, dependencies, *, authored_version=None, canonical_digest=None, enforcement=None, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn update_ontology_module(
        &mut self,
        py: Python<'_>,
        ontology_id: &str,
        document: &Bound<'_, PyAny>,
        dependencies: &Bound<'_, PyAny>,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
        enforcement: Option<&str>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::update_module(
            self,
            py,
            ontology_id,
            authored_version,
            canonical_digest,
            document,
            dependencies,
            enforcement,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
            cancellation,
        )
    }

    #[pyo3(signature = (ontology_id, document, dependencies, *, authored_version=None, canonical_digest=None, enforcement=None, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn preview_migrate_ontology_module(
        &self,
        py: Python<'_>,
        ontology_id: &str,
        document: &Bound<'_, PyAny>,
        dependencies: &Bound<'_, PyAny>,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
        enforcement: Option<&str>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::preview_migrate_module(
            self,
            py,
            ontology_id,
            authored_version,
            canonical_digest,
            document,
            dependencies,
            enforcement,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
        )
    }

    #[pyo3(signature = (ontology_id, document, dependencies, preview, *, authored_version=None, canonical_digest=None, enforcement=None, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn migrate_ontology_module(
        &mut self,
        py: Python<'_>,
        ontology_id: &str,
        document: &Bound<'_, PyAny>,
        dependencies: &Bound<'_, PyAny>,
        preview: &Bound<'_, PyAny>,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
        enforcement: Option<&str>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::migrate_module(
            self,
            py,
            ontology_id,
            authored_version,
            canonical_digest,
            document,
            dependencies,
            enforcement,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
            preview,
            cancellation,
        )
    }

    fn multi_ontology_certification_report(
        &self,
        py: Python<'_>,
        composition_before: &str,
        migration_plan_digest: &str,
        rows_scanned: u64,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::certification_report(
            self,
            py,
            composition_before,
            migration_plan_digest,
            rows_scanned,
        )
    }

    #[pyo3(signature = (ontology_id, *, authored_version=None, canonical_digest=None))]
    fn preview_delete_ontology_module(
        &self,
        py: Python<'_>,
        ontology_id: &str,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::preview_delete_module(
            self,
            py,
            ontology_id,
            authored_version,
            canonical_digest,
        )
    }

    #[pyo3(signature = (ontology_id, *, authored_version=None, canonical_digest=None, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn delete_ontology_module(
        &mut self,
        py: Python<'_>,
        ontology_id: &str,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::delete_module(
            self,
            py,
            ontology_id,
            authored_version,
            canonical_digest,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
            cancellation,
        )
    }

    #[pyo3(signature = (ontology_id, *, format, authored_version=None, canonical_digest=None))]
    fn export_ontology_module(
        &self,
        py: Python<'_>,
        ontology_id: &str,
        format: &str,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
    ) -> PyResult<String> {
        multi_ontology::export_module(
            self,
            py,
            ontology_id,
            authored_version,
            canonical_digest,
            format,
        )
    }

    fn ontology_bridges(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        multi_ontology::ontology_bridges(self, py)
    }

    #[pyo3(signature = (bridge_id, *, authored_version=None, canonical_digest=None))]
    fn inspect_ontology_bridge(
        &self,
        py: Python<'_>,
        bridge_id: &str,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::inspect_bridge(self, py, bridge_id, authored_version, canonical_digest)
    }

    fn validate_ontology_bridge(
        &self,
        py: Python<'_>,
        document: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::validate_bridge(self, py, document)
    }

    fn create_ontology_bridge(
        &self,
        py: Python<'_>,
        document: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::create_bridge(self, py, document)
    }

    #[pyo3(signature = (text, *, format="auto"))]
    fn import_ontology_bridge(
        &self,
        py: Python<'_>,
        text: &str,
        format: &str,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::import_bridge(self, py, text, format)
    }

    #[pyo3(signature = (candidate, *, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn adopt_ontology_bridge(
        &mut self,
        py: Python<'_>,
        candidate: &Bound<'_, PyAny>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::adopt_bridge(
            self,
            py,
            candidate,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
            cancellation,
        )
    }

    #[pyo3(signature = (bridge_id, document, *, authored_version=None, canonical_digest=None))]
    fn preview_update_ontology_bridge(
        &self,
        py: Python<'_>,
        bridge_id: &str,
        document: &Bound<'_, PyAny>,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::preview_update_bridge(
            self,
            py,
            bridge_id,
            authored_version,
            canonical_digest,
            document,
        )
    }

    #[pyo3(signature = (bridge_id, document, *, authored_version=None, canonical_digest=None, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn update_ontology_bridge(
        &mut self,
        py: Python<'_>,
        bridge_id: &str,
        document: &Bound<'_, PyAny>,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::update_bridge(
            self,
            py,
            bridge_id,
            authored_version,
            canonical_digest,
            document,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
            cancellation,
        )
    }

    #[pyo3(signature = (bridge_id, *, authored_version=None, canonical_digest=None))]
    fn preview_delete_ontology_bridge(
        &self,
        py: Python<'_>,
        bridge_id: &str,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::preview_delete_bridge(
            self,
            py,
            bridge_id,
            authored_version,
            canonical_digest,
        )
    }

    #[pyo3(signature = (bridge_id, *, authored_version=None, canonical_digest=None, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn delete_ontology_bridge(
        &mut self,
        py: Python<'_>,
        bridge_id: &str,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::delete_bridge(
            self,
            py,
            bridge_id,
            authored_version,
            canonical_digest,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
            cancellation,
        )
    }

    #[pyo3(signature = (bridge_id, *, format, authored_version=None, canonical_digest=None))]
    fn export_ontology_bridge(
        &self,
        py: Python<'_>,
        bridge_id: &str,
        format: &str,
        authored_version: Option<&str>,
        canonical_digest: Option<&str>,
    ) -> PyResult<String> {
        multi_ontology::export_bridge(
            self,
            py,
            bridge_id,
            authored_version,
            canonical_digest,
            format,
        )
    }

    fn ontology_activation_profile(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        multi_ontology::activation_profile(self, py)
    }

    #[pyo3(signature = (profile_default, activation, *, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn change_ontology_activation_profile(
        &mut self,
        py: Python<'_>,
        profile_default: &str,
        activation: &Bound<'_, PyAny>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::change_activation_profile(
            self,
            py,
            profile_default,
            activation,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
            cancellation,
        )
    }

    fn validate_ontology_composition(
        &self,
        py: Python<'_>,
        candidate: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::validate_composition(self, py, candidate)
    }

    #[pyo3(signature = (candidate, *, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn preflight_ontology_composition(
        &self,
        py: Python<'_>,
        candidate: &Bound<'_, PyAny>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::preflight_composition(
            self,
            py,
            candidate,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
            cancellation,
        )
    }

    #[pyo3(signature = (kind, local_id, *, module=None, max_candidates=16))]
    fn explain_ontology_resolution(
        &self,
        py: Python<'_>,
        kind: &str,
        local_id: &str,
        module: Option<&Bound<'_, PyAny>>,
        max_candidates: usize,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::explain_resolution(self, py, module, kind, local_id, max_candidates)
    }

    fn portable_ontology_staging(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        multi_ontology::portable_staging(self, py)
    }

    #[pyo3(signature = (*, expected_project_generation_uuid, expected_composition_fingerprint, operation_uuid, actor_uuid=None, cancellation=None))]
    fn adopt_portable_ontology_staging(
        &mut self,
        py: Python<'_>,
        expected_project_generation_uuid: &str,
        expected_composition_fingerprint: Option<&str>,
        operation_uuid: &str,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        multi_ontology::adopt_portable_staging(
            self,
            py,
            expected_project_generation_uuid,
            expected_composition_fingerprint,
            operation_uuid,
            actor_uuid,
            cancellation,
        )
    }
}
