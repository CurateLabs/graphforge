//! Thin Python projection of the Rust-owned multi-ontology lifecycle (#842).
#![expect(
    clippy::too_many_arguments,
    reason = "thin helpers preserve the explicit keyword-rich native lifecycle contract"
)]

use graphforge_api::{
    ActivationMode, ActivationProfileChangeRequest, ActivationRecord, BridgeAdoptionRequest,
    BridgeCandidate, BridgeDeleteRequest, BridgeDocument, BridgeExportFormat,
    BridgeImportFormatHint, BridgeSelector, BridgeUpdateRequest, CompositionChangeRequest,
    CompositionDataDisposition, GfError, ImportFormatHint, ModuleAdoptionRequest, ModuleCandidate,
    ModuleDeleteRequest, ModuleSelector, ModuleUpdateRequest, OntologyAuthorityExpectation,
    OntologyDoc, OntologyModuleId, ResolutionExplainRequest, SymbolKind,
    WorkspaceOntologyComposition, WriteContext,
};
use pyo3::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{
    GraphForge, PyCancellationToken, canonical_operation_id, json_value_to_python,
    py_to_json_value, to_pyerr,
};

fn from_python<T: DeserializeOwned>(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<T> {
    serde_json::from_value(py_to_json_value(value)?)
        .map_err(|error| to_pyerr(py, &GfError::Validation(error.to_string())))
}

fn to_python<T: Serialize>(py: Python<'_>, value: &T) -> PyResult<Py<PyAny>> {
    let value = serde_json::to_value(value)
        .map_err(|error| to_pyerr(py, &GfError::Validation(error.to_string())))?;
    json_value_to_python(py, &value)
}

pub(crate) fn to_multi_ontology_pyerr(
    py: Python<'_>,
    error: &graphforge_api::MultiOntologyError,
) -> PyErr {
    let exception = PyErr::new::<crate::ValidationError, _>(error.message.clone());
    let value = exception.value(py);
    let _ = value.setattr("code", error.code());
    if let Ok(diagnostics) = serde_json::to_value(&error.diagnostics)
        && let Ok(projected) = json_value_to_python(py, &diagnostics)
    {
        let _ = value.setattr("diagnostics", projected);
    }
    exception
}

fn mode(py: Python<'_>, value: &str) -> PyResult<ActivationMode> {
    match value {
        "exploratory" => Ok(ActivationMode::Exploratory),
        "advisory" => Ok(ActivationMode::Advisory),
        "strict" => Ok(ActivationMode::Strict),
        _ => Err(to_pyerr(
            py,
            &GfError::Validation("mode must be exploratory, advisory, or strict".into()),
        )),
    }
}

fn module_selector(
    py: Python<'_>,
    ontology_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
) -> PyResult<ModuleSelector> {
    match (authored_version, canonical_digest) {
        (None, None) => Ok(ModuleSelector::OntologyId(ontology_id.to_owned())),
        (Some(version), Some(digest)) => Ok(ModuleSelector::Exact(OntologyModuleId {
            ontology_id: ontology_id.to_owned(),
            authored_version: version.to_owned(),
            canonical_digest: digest.to_owned(),
        })),
        _ => Err(to_pyerr(
            py,
            &GfError::Validation(
                "exact module selection requires authored_version and canonical_digest".into(),
            ),
        )),
    }
}

fn bridge_selector(
    py: Python<'_>,
    bridge_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
) -> PyResult<BridgeSelector> {
    match (authored_version, canonical_digest) {
        (None, None) => Ok(BridgeSelector::BridgeId(bridge_id.to_owned())),
        (Some(version), Some(digest)) => Ok(BridgeSelector::Exact(graphforge_api::BridgeSetId {
            bridge_id: bridge_id.to_owned(),
            authored_version: version.to_owned(),
            canonical_digest: digest.to_owned(),
        })),
        _ => Err(to_pyerr(
            py,
            &GfError::Validation(
                "exact bridge selection requires authored_version and canonical_digest".into(),
            ),
        )),
    }
}

fn authority(
    py: Python<'_>,
    expected_project_generation_uuid: &str,
    expected_composition_fingerprint: Option<&str>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
) -> PyResult<OntologyAuthorityExpectation> {
    Ok(OntologyAuthorityExpectation {
        context: WriteContext {
            operation_uuid: canonical_operation_id(operation_uuid)
                .map_err(|error| to_pyerr(py, &error))?,
            actor_uuid: actor_uuid
                .map(canonical_operation_id)
                .transpose()
                .map_err(|error| to_pyerr(py, &error))?
                .map(|value| value.0),
        },
        expected_project_generation_uuid: expected_project_generation_uuid.parse().map_err(
            |_| {
                to_pyerr(
                    py,
                    &GfError::Validation("invalid project generation UUID".into()),
                )
            },
        )?,
        expected_composition_fingerprint: expected_composition_fingerprint.map(str::to_owned),
    })
}

fn cancellation(value: Option<&PyCancellationToken>) -> Option<graphforge_api::CancellationToken> {
    value.map(|token| token.inner.clone())
}

pub(crate) fn ontology_modules(forge: &GraphForge, py: Python<'_>) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let result = py
        .detach(|| forge.inner.ontology_modules())
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn authority_state(forge: &GraphForge, py: Python<'_>) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let result = py
        .detach(|| forge.inner.ontology_authority_state())
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn inspect_module(
    forge: &GraphForge,
    py: Python<'_>,
    ontology_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let selector = module_selector(py, ontology_id, authored_version, canonical_digest)?;
    let result = py
        .detach(|| forge.inner.inspect_ontology_module(&selector))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn validate_module(
    forge: &GraphForge,
    py: Python<'_>,
    document: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let document: OntologyDoc = from_python(py, document)?;
    let result = py
        .detach(|| forge.inner.validate_ontology_module(&document))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn create_module(
    forge: &GraphForge,
    py: Python<'_>,
    document: &Bound<'_, PyAny>,
    dependencies: &Bound<'_, PyAny>,
    enforcement: Option<&str>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let document: OntologyDoc = from_python(py, document)?;
    let dependencies: Vec<OntologyModuleId> = from_python(py, dependencies)?;
    let enforcement = enforcement.map(|value| mode(py, value)).transpose()?;
    let result = py
        .detach(|| {
            forge
                .inner
                .create_ontology_module(document, dependencies, enforcement)
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn import_module(
    forge: &GraphForge,
    py: Python<'_>,
    text: &str,
    format: &str,
    dependencies: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let text = text.to_owned();
    let dependencies: Vec<OntologyModuleId> = from_python(py, dependencies)?;
    let format = match format {
        "auto" => ImportFormatHint::Auto,
        "json" => ImportFormatHint::Json,
        "yaml" | "yml" => ImportFormatHint::Yaml,
        _ => {
            return Err(to_pyerr(
                py,
                &GfError::Validation("format must be auto, json, or yaml".into()),
            ));
        }
    };
    let result = py
        .detach(|| {
            forge
                .inner
                .import_ontology_module(&text, format, dependencies)
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn adopt_module(
    forge: &mut GraphForge,
    py: Python<'_>,
    candidate: &Bound<'_, PyAny>,
    expected_generation: &str,
    expected_fingerprint: Option<&str>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
    cancel: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let candidate: ModuleCandidate = from_python(py, candidate)?;
    let authority = authority(
        py,
        expected_generation,
        expected_fingerprint,
        operation_uuid,
        actor_uuid,
    )?;
    let request = ModuleAdoptionRequest {
        authority,
        candidate,
    };
    let cancel = cancellation(cancel);
    let result = py
        .detach(|| forge.inner.adopt_ontology_module(&request, cancel.as_ref()))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn preview_update_module(
    forge: &GraphForge,
    py: Python<'_>,
    ontology_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
    document: &Bound<'_, PyAny>,
    dependencies: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let selector = module_selector(py, ontology_id, authored_version, canonical_digest)?;
    let document: OntologyDoc = from_python(py, document)?;
    let dependencies: Vec<OntologyModuleId> = from_python(py, dependencies)?;
    let result = py
        .detach(|| {
            forge
                .inner
                .preview_update_ontology_module(&selector, &document, &dependencies)
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn update_module(
    forge: &mut GraphForge,
    py: Python<'_>,
    ontology_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
    document: &Bound<'_, PyAny>,
    dependencies: &Bound<'_, PyAny>,
    enforcement: Option<&str>,
    expected_generation: &str,
    expected_fingerprint: Option<&str>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
    cancel: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let selector = module_selector(py, ontology_id, authored_version, canonical_digest)?;
    let document: OntologyDoc = from_python(py, document)?;
    let dependencies: Vec<OntologyModuleId> = from_python(py, dependencies)?;
    let enforcement = enforcement.map(|value| mode(py, value)).transpose()?;
    let authority = authority(
        py,
        expected_generation,
        expected_fingerprint,
        operation_uuid,
        actor_uuid,
    )?;
    let request = ModuleUpdateRequest {
        authority,
        selector,
        document,
        dependencies,
        enforcement,
    };
    let cancel = cancellation(cancel);
    let result = py
        .detach(|| {
            forge
                .inner
                .update_ontology_module(&request, cancel.as_ref())
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn preview_delete_module(
    forge: &GraphForge,
    py: Python<'_>,
    ontology_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let selector = module_selector(py, ontology_id, authored_version, canonical_digest)?;
    let result = py
        .detach(|| forge.inner.preview_delete_ontology_module(&selector))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn delete_module(
    forge: &mut GraphForge,
    py: Python<'_>,
    ontology_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
    expected_generation: &str,
    expected_fingerprint: Option<&str>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
    cancel: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let selector = module_selector(py, ontology_id, authored_version, canonical_digest)?;
    let authority = authority(
        py,
        expected_generation,
        expected_fingerprint,
        operation_uuid,
        actor_uuid,
    )?;
    let request = ModuleDeleteRequest {
        authority,
        selector,
    };
    let cancel = cancellation(cancel);
    let result = py
        .detach(|| {
            forge
                .inner
                .delete_ontology_module(&request, cancel.as_ref())
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn export_module(
    forge: &GraphForge,
    py: Python<'_>,
    ontology_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
    format: &str,
) -> PyResult<String> {
    forge.ensure_open()?;
    let selector = module_selector(py, ontology_id, authored_version, canonical_digest)?;
    let format = match format {
        "json" => graphforge_api::ExportFormat::Json,
        "yaml" | "yml" => graphforge_api::ExportFormat::Yaml,
        _ => {
            return Err(to_pyerr(
                py,
                &GfError::Validation("format must be json or yaml".into()),
            ));
        }
    };
    py.detach(|| forge.inner.export_ontology_module(&selector, format))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))
}

pub(crate) fn ontology_bridges(forge: &GraphForge, py: Python<'_>) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let result = py
        .detach(|| forge.inner.ontology_bridges())
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn inspect_bridge(
    forge: &GraphForge,
    py: Python<'_>,
    bridge_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let selector = bridge_selector(py, bridge_id, authored_version, canonical_digest)?;
    let result = py
        .detach(|| forge.inner.inspect_ontology_bridge(&selector))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn validate_bridge(
    forge: &GraphForge,
    py: Python<'_>,
    document: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let document: BridgeDocument = from_python(py, document)?;
    let result = py
        .detach(|| forge.inner.validate_ontology_bridge(&document))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn create_bridge(
    forge: &GraphForge,
    py: Python<'_>,
    document: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let document: BridgeDocument = from_python(py, document)?;
    let result = py
        .detach(|| forge.inner.create_ontology_bridge(document))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn import_bridge(
    forge: &GraphForge,
    py: Python<'_>,
    text: &str,
    format: &str,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let text = text.to_owned();
    let format = match format {
        "auto" => BridgeImportFormatHint::Auto,
        "json" => BridgeImportFormatHint::Json,
        "yaml" | "yml" => BridgeImportFormatHint::Yaml,
        _ => {
            return Err(to_pyerr(
                py,
                &GfError::Validation("format must be auto, json, or yaml".into()),
            ));
        }
    };
    let result = py
        .detach(|| forge.inner.import_ontology_bridge(&text, format))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn adopt_bridge(
    forge: &mut GraphForge,
    py: Python<'_>,
    candidate: &Bound<'_, PyAny>,
    expected_generation: &str,
    expected_fingerprint: Option<&str>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
    cancel: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let candidate: BridgeCandidate = from_python(py, candidate)?;
    let authority = authority(
        py,
        expected_generation,
        expected_fingerprint,
        operation_uuid,
        actor_uuid,
    )?;
    let request = BridgeAdoptionRequest {
        authority,
        candidate,
    };
    let cancel = cancellation(cancel);
    let result = py
        .detach(|| forge.inner.adopt_ontology_bridge(&request, cancel.as_ref()))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn preview_update_bridge(
    forge: &GraphForge,
    py: Python<'_>,
    bridge_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
    document: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let selector = bridge_selector(py, bridge_id, authored_version, canonical_digest)?;
    let document: BridgeDocument = from_python(py, document)?;
    let result = py
        .detach(|| {
            forge
                .inner
                .preview_update_ontology_bridge(&selector, &document)
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn update_bridge(
    forge: &mut GraphForge,
    py: Python<'_>,
    bridge_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
    document: &Bound<'_, PyAny>,
    expected_generation: &str,
    expected_fingerprint: Option<&str>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
    cancel: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let selector = bridge_selector(py, bridge_id, authored_version, canonical_digest)?;
    let document: BridgeDocument = from_python(py, document)?;
    let authority = authority(
        py,
        expected_generation,
        expected_fingerprint,
        operation_uuid,
        actor_uuid,
    )?;
    let request = BridgeUpdateRequest {
        authority,
        selector,
        document,
    };
    let cancel = cancellation(cancel);
    let result = py
        .detach(|| {
            forge
                .inner
                .update_ontology_bridge(&request, cancel.as_ref())
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn preview_delete_bridge(
    forge: &GraphForge,
    py: Python<'_>,
    bridge_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let selector = bridge_selector(py, bridge_id, authored_version, canonical_digest)?;
    let result = py
        .detach(|| forge.inner.preview_delete_ontology_bridge(&selector))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn delete_bridge(
    forge: &mut GraphForge,
    py: Python<'_>,
    bridge_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
    expected_generation: &str,
    expected_fingerprint: Option<&str>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
    cancel: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let selector = bridge_selector(py, bridge_id, authored_version, canonical_digest)?;
    let authority = authority(
        py,
        expected_generation,
        expected_fingerprint,
        operation_uuid,
        actor_uuid,
    )?;
    let request = BridgeDeleteRequest {
        authority,
        selector,
    };
    let cancel = cancellation(cancel);
    let result = py
        .detach(|| {
            forge
                .inner
                .delete_ontology_bridge(&request, cancel.as_ref())
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn export_bridge(
    forge: &GraphForge,
    py: Python<'_>,
    bridge_id: &str,
    authored_version: Option<&str>,
    canonical_digest: Option<&str>,
    format: &str,
) -> PyResult<String> {
    forge.ensure_open()?;
    let selector = bridge_selector(py, bridge_id, authored_version, canonical_digest)?;
    let format = match format {
        "json" => BridgeExportFormat::Json,
        "yaml" | "yml" => BridgeExportFormat::Yaml,
        _ => {
            return Err(to_pyerr(
                py,
                &GfError::Validation("format must be json or yaml".into()),
            ));
        }
    };
    py.detach(|| forge.inner.export_ontology_bridge(&selector, format))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))
}

pub(crate) fn activation_profile(forge: &GraphForge, py: Python<'_>) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let (profile_default, activation) = py
        .detach(|| forge.inner.ontology_activation_profile())
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(
        py,
        &serde_json::json!({"profile_default": profile_default, "activation": activation}),
    )
}

pub(crate) fn change_activation_profile(
    forge: &mut GraphForge,
    py: Python<'_>,
    profile_default: &str,
    activation: &Bound<'_, PyAny>,
    expected_generation: &str,
    expected_fingerprint: Option<&str>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
    cancel: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let profile_default = mode(py, profile_default)?;
    let activation: Vec<ActivationRecord> = from_python(py, activation)?;
    let authority = authority(
        py,
        expected_generation,
        expected_fingerprint,
        operation_uuid,
        actor_uuid,
    )?;
    let request = ActivationProfileChangeRequest {
        authority,
        profile_default,
        activation,
    };
    let cancel = cancellation(cancel);
    let result = py
        .detach(|| {
            forge
                .inner
                .change_ontology_activation_profile(&request, cancel.as_ref())
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn validate_composition(
    forge: &GraphForge,
    py: Python<'_>,
    candidate: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let candidate: WorkspaceOntologyComposition = from_python(py, candidate)?;
    let result = py
        .detach(|| forge.inner.validate_ontology_composition(&candidate))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn preflight_composition(
    forge: &GraphForge,
    py: Python<'_>,
    candidate: &Bound<'_, PyAny>,
    expected_generation: &str,
    expected_fingerprint: Option<&str>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
    cancel: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let candidate: WorkspaceOntologyComposition = from_python(py, candidate)?;
    let authority = authority(
        py,
        expected_generation,
        expected_fingerprint,
        operation_uuid,
        actor_uuid,
    )?;
    let request = CompositionChangeRequest {
        context: authority.context,
        expected_project_generation_uuid: authority.expected_project_generation_uuid,
        expected_composition_fingerprint: authority.expected_composition_fingerprint,
        candidate,
        data_disposition: CompositionDataDisposition::RequireConforming,
    };
    let cancel = cancellation(cancel);
    let result = py
        .detach(|| {
            forge
                .inner
                .preflight_ontology_composition(&request, cancel.as_ref())
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn explain_resolution(
    forge: &GraphForge,
    py: Python<'_>,
    module: Option<&Bound<'_, PyAny>>,
    kind: &str,
    local_id: &str,
    max_candidates: usize,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let module = module.map(|value| from_python(py, value)).transpose()?;
    let kind = match kind {
        "entity" => SymbolKind::Entity,
        "relation" => SymbolKind::Relation,
        "property" => SymbolKind::Property,
        _ => {
            return Err(to_pyerr(
                py,
                &GfError::Validation("kind must be entity, relation, or property".into()),
            ));
        }
    };
    let request = ResolutionExplainRequest {
        module,
        kind,
        local_id: local_id.to_owned(),
        max_candidates,
    };
    let result = py
        .detach(|| forge.inner.explain_ontology_resolution(&request))
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn portable_staging(forge: &GraphForge, py: Python<'_>) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let result = py
        .detach(|| {
            forge
                .inner
                .portable_ontology_staging(graphforge_api::PortableV2Limits::default())
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}

pub(crate) fn adopt_portable_staging(
    forge: &mut GraphForge,
    py: Python<'_>,
    expected_generation: &str,
    expected_fingerprint: Option<&str>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
    cancel: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    forge.ensure_open()?;
    let authority = authority(
        py,
        expected_generation,
        expected_fingerprint,
        operation_uuid,
        actor_uuid,
    )?;
    let cancel = cancellation(cancel);
    let result = py
        .detach(|| {
            forge.inner.adopt_portable_ontology_staging(
                &authority,
                graphforge_api::PortableV2Limits::default(),
                cancel.as_ref(),
            )
        })
        .map_err(|e| to_multi_ontology_pyerr(py, &e))?;
    to_python(py, &result)
}
