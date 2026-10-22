//! Thin Python bindings for portable preview/export/verify/import/OCI (#744).

use std::path::PathBuf;

use graphforge_api::{
    GfError, PortableError, PortableExportRequest, PortableGraphSelector, PortableImportRequest,
    PortableLimits, PortableMode, PortableOciAuthenticityPolicy, PortableOciPublishFacadeRequest,
    PortableOciPullFacadeRequest, PortableOciSignatureMaterial, PortableOutput,
    PortableParticipantId, PortablePropertyProjection, PortableSelection,
    PortableSelectionPreviewRequest, PortableSelectionProfile, PortableSelectionRequest,
    PortableSubsetClosure, PortableSubsetPlan, PortableSubsetPreviewRequest, PortableSubsetRequest,
    PortableVerifyRequest, ResultSinkOptions, ResultSinkReceipt,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::{
    GraphForge, PyCancellationToken, canonical_operation_id, json_value_to_python,
    params_from_dict, to_pyerr,
};

/// Map a sanitized portable failure without credentials, headers, or host paths.
pub(crate) fn to_portable_pyerr(py: Python<'_>, error: PortableError) -> PyErr {
    let entry = error.entry.clone();
    let committed = error.committed_import.clone();
    let projected = graphforge_api::MultiOntologyError::from(error);
    let err = crate::multi_ontology::to_multi_ontology_pyerr(py, &projected);
    let value = err.value(py);
    if let Some(entry) = &entry {
        let _ = value.setattr("entry", entry.as_str());
    }
    if let Some(receipt) = committed
        && let Ok(json) = serde_json::to_value(receipt)
        && let Ok(receipt) = crate::json_value_to_python(py, &json)
    {
        let _ = value.setattr("committed_import", receipt);
    }
    err
}

fn selection_from_checkpoint(checkpoint: Option<String>) -> PortableSelection {
    match checkpoint {
        Some(name) => PortableSelection::Checkpoint(name),
        None => PortableSelection::Current,
    }
}

fn parse_limits(py: Python<'_>, value: Option<&Bound<'_, PyDict>>) -> PyResult<PortableLimits> {
    let mut limits = PortableLimits::default();
    let Some(dict) = value else {
        return Ok(limits);
    };
    if let Some(item) = dict.get_item("max_components")? {
        limits.max_components = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_entries")? {
        limits.max_entries = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_entry_bytes")? {
        limits.max_entry_bytes = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_total_bytes")? {
        limits.max_total_bytes = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_manifest_bytes")? {
        limits.max_manifest_bytes = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_semantic_manifest_bytes")? {
        limits.max_semantic_manifest_bytes = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_tag_manifest_bytes")? {
        limits.max_tag_manifest_bytes = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_retained_control_bytes")? {
        limits.max_retained_control_bytes = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_parquet_walk_bytes")? {
        limits.max_parquet_walk_bytes = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_parquet_footer_bytes")? {
        limits.max_parquet_footer_bytes = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_research_version_bytes")? {
        limits.max_research_version_bytes = item.extract()?;
    }
    if let Some(item) = dict.get_item("max_path_bytes")? {
        limits.max_path_bytes = item.extract()?;
    }
    if let Some(item) = dict.get_item("copy_buffer_bytes")? {
        limits.copy_buffer_bytes = item.extract()?;
    }
    let _ = py;
    Ok(limits)
}

fn parse_profile(
    py: Python<'_>,
    profile: &str,
    identities: Option<&Bound<'_, PyList>>,
) -> PyResult<PortableSelectionProfile> {
    match profile {
        "complete" => Ok(PortableSelectionProfile::Complete),
        "ontology_only" => Ok(PortableSelectionProfile::OntologyOnly),
        "data_components" => Ok(PortableSelectionProfile::DataComponents),
        "artifacts" => Ok(PortableSelectionProfile::Artifacts),
        "settings" => Ok(PortableSelectionProfile::Settings),
        "custom" => {
            let identities = identities.ok_or_else(|| {
                to_pyerr(
                    py,
                    &GfError::Validation("custom profile requires identities".into()),
                )
            })?;
            let mut parsed = Vec::with_capacity(identities.len());
            for item in identities {
                let dict = item.cast::<PyDict>().map_err(|_| {
                    to_pyerr(
                        py,
                        &GfError::Validation("identity entries must be dictionaries".into()),
                    )
                })?;
                let capability_id = dict
                    .get_item("capability_id")?
                    .ok_or_else(|| {
                        to_pyerr(
                            py,
                            &GfError::Validation("identity requires capability_id".into()),
                        )
                    })?
                    .extract::<String>()?;
                let record_family_id = dict
                    .get_item("record_family_id")?
                    .ok_or_else(|| {
                        to_pyerr(
                            py,
                            &GfError::Validation("identity requires record_family_id".into()),
                        )
                    })?
                    .extract::<String>()?;
                parsed.push(PortableParticipantId {
                    capability_id,
                    record_family_id,
                });
            }
            Ok(PortableSelectionProfile::Custom(parsed))
        }
        _ => Err(to_pyerr(
            py,
            &GfError::Validation(
                "profile must be complete, ontology_only, data_components, artifacts, settings, or custom"
                    .into(),
            ),
        )),
    }
}

fn parse_subset(
    py: Python<'_>,
    value: Option<&Bound<'_, PyDict>>,
) -> PyResult<Option<PortableSubsetRequest>> {
    let Some(dict) = value else {
        return Ok(None);
    };
    let selector_value = dict
        .get_item("selector")?
        .ok_or_else(|| to_pyerr(py, &GfError::Validation("subset requires selector".into())))?;
    let selector = selector_value.cast::<PyDict>().map_err(|_| {
        to_pyerr(
            py,
            &GfError::Validation("selector must be a dictionary".into()),
        )
    })?;
    let node_uuids = selector
        .get_item("node_uuids")?
        .map(|item| item.extract::<Vec<String>>())
        .transpose()?
        .unwrap_or_default();
    let edge_uuids = selector
        .get_item("edge_uuids")?
        .map(|item| item.extract::<Vec<String>>())
        .transpose()?
        .unwrap_or_default();
    let closure = match dict
        .get_item("closure")?
        .map(|item| item.extract::<String>())
        .transpose()?
        .as_deref()
        .unwrap_or("induced_edges")
    {
        "induced_edges" => PortableSubsetClosure::InducedEdges,
        "referential" => PortableSubsetClosure::Referential,
        _ => {
            return Err(to_pyerr(
                py,
                &GfError::Validation("closure must be induced_edges or referential".into()),
            ));
        }
    };
    let exclude = dict
        .get_item("projection")?
        .and_then(|item| {
            item.cast::<PyDict>().ok().and_then(|projection| {
                projection
                    .get_item("exclude")
                    .ok()
                    .flatten()
                    .and_then(|exclude| exclude.extract::<Vec<String>>().ok())
            })
        })
        .unwrap_or_default();
    Ok(Some(PortableSubsetRequest {
        selector: PortableGraphSelector {
            node_uuids,
            edge_uuids,
        },
        closure,
        projection: PortablePropertyProjection { exclude },
    }))
}

fn parse_output(py: Python<'_>, representation: &str) -> PyResult<PortableOutput> {
    match representation {
        "expanded" => Ok(PortableOutput::Expanded),
        "bundle" => Ok(PortableOutput::Bundle),
        _ => Err(to_pyerr(
            py,
            &GfError::Validation("representation must be expanded or bundle".into()),
        )),
    }
}

fn parse_mode(py: Python<'_>, mode: &str) -> PyResult<PortableMode> {
    match mode {
        "structure_only" => Ok(PortableMode::StructureOnly),
        "full" => Ok(PortableMode::Full),
        _ => Err(to_pyerr(
            py,
            &GfError::Validation("mode must be structure_only or full".into()),
        )),
    }
}

fn selection_plan_json(plan: &graphforge_api::PortableSelectionPlan) -> serde_json::Value {
    serde_json::json!({
        "source_generation_uuid": plan.source_generation_uuid,
        "source_manifest_sha256": plan.source_manifest_sha256,
        "package_class": plan.package_class,
        "included": plan.included,
        "excluded": plan.excluded,
        "projected": plan.projected,
        "include_graph_tree": plan.includes_graph_tree(),
        "redactions": plan.redactions,
        "required_capabilities": plan.required_capabilities,
        "estimated_payload_bytes": plan.estimated_payload_bytes,
        "selection_fingerprint": plan.selection_fingerprint,
    })
}

fn subset_plan_json(plan: &PortableSubsetPlan) -> serde_json::Value {
    serde_json::json!({
        "selection": selection_plan_json(&plan.selection),
        "graph_subset": plan.graph_subset,
        "selected_node_count": plan.selected_node_count,
        "selected_edge_count": plan.selected_edge_count,
        "endpoint_node_count": plan.endpoint_node_count,
        "result_fingerprint": plan.result_fingerprint,
        "subset_fingerprint": plan.subset_fingerprint,
    })
}

fn export_result_json(result: &graphforge_api::PortableExportFacadeResult) -> serde_json::Value {
    serde_json::json!({
        "contract": result.contract,
        "source": result.source,
        "checkpoint": result.checkpoint,
        "generation_uuid": result.generation_uuid.to_string(),
        "package_digest": result.package_digest,
        "transport_digest": result.transport_digest,
        "entry_count": result.entry_count,
        "payload_bytes": result.payload_bytes,
        "representation": result.representation,
        "selection_fingerprint": result.selection_fingerprint,
        "output": result.output.display().to_string(),
    })
}

fn verify_result_json(
    py: Python<'_>,
    report: &graphforge_api::PortableVerifyResult,
) -> PyResult<serde_json::Value> {
    serde_json::to_value(report)
        .map_err(|error| to_pyerr(py, &GfError::Execution(error.to_string())))
}

fn import_result_json(result: &graphforge_api::PortableImportResult) -> serde_json::Value {
    serde_json::json!({
        "package_digest": result.package_digest,
        "transport_digest": result.transport_digest,
        "generation_uuid": result.generation_uuid.to_string(),
        "idempotent_replay": result.idempotent_replay,
        "warnings": result.warnings,
    })
}

fn oci_reference_json(
    py: Python<'_>,
    reference: &graphforge_api::PortableOciReference,
) -> PyResult<serde_json::Value> {
    serde_json::to_value(reference)
        .map_err(|error| to_pyerr(py, &GfError::Execution(error.to_string())))
}

fn oci_pull_json(
    py: Python<'_>,
    receipt: &graphforge_api::PortableOciPullReceipt,
) -> PyResult<serde_json::Value> {
    let reference = oci_reference_json(py, &receipt.reference)?;
    let report = verify_result_json(py, &receipt.report)?;
    Ok(serde_json::json!({
        "reference": reference,
        "destination": receipt.destination.display().to_string(),
        "report": report,
        "signature_state": receipt.signature_state,
    }))
}

fn sink_receipt_dict(py: Python<'_>, receipt: &ResultSinkReceipt) -> PyResult<Py<PyAny>> {
    let out = PyDict::new(py);
    out.set_item("destination", receipt.destination.display().to_string())?;
    out.set_item(
        "format",
        match receipt.format {
            graphforge_api::ResultSinkFormat::Parquet => "parquet",
            graphforge_api::ResultSinkFormat::ArrowIpc => "arrow_ipc",
        },
    )?;
    let progress = PyDict::new(py);
    progress.set_item("phase", receipt.progress.phase)?;
    progress.set_item("rows", receipt.progress.rows)?;
    progress.set_item("batches", receipt.progress.batches)?;
    progress.set_item("bytes", receipt.progress.bytes)?;
    progress.set_item(
        "elapsed_ms",
        u64::try_from(receipt.progress.elapsed.as_millis()).unwrap_or(u64::MAX),
    )?;
    progress.set_item("complete", receipt.progress.complete)?;
    out.set_item("progress", progress)?;
    Ok(out.into_any().unbind())
}

fn authenticity_policy(
    py: Python<'_>,
    value: Option<&Bound<'_, PyDict>>,
) -> PyResult<PortableOciAuthenticityPolicy> {
    let Some(dict) = value else {
        return Ok(PortableOciAuthenticityPolicy::default());
    };
    let require_named_signer = dict
        .get_item("require_named_signer")?
        .map(|item| item.extract::<String>())
        .transpose()?;
    let verification_key = dict
        .get_item("verification_key")?
        .map(|item| item.extract::<Vec<u8>>())
        .transpose()?;
    let _ = py;
    Ok(PortableOciAuthenticityPolicy {
        require_named_signer,
        verification_key,
    })
}

fn signature_material(
    py: Python<'_>,
    value: Option<&Bound<'_, PyDict>>,
) -> PyResult<Option<PortableOciSignatureMaterial>> {
    let Some(dict) = value else {
        return Ok(None);
    };
    let signer = dict
        .get_item("signer")?
        .ok_or_else(|| to_pyerr(py, &GfError::Validation("signature requires signer".into())))?
        .extract::<String>()?;
    let key_id = dict
        .get_item("key_id")?
        .ok_or_else(|| to_pyerr(py, &GfError::Validation("signature requires key_id".into())))?
        .extract::<String>()?;
    let secret = dict
        .get_item("secret")?
        .ok_or_else(|| to_pyerr(py, &GfError::Validation("signature requires secret".into())))?
        .extract::<Vec<u8>>()?;
    Ok(Some(PortableOciSignatureMaterial {
        signer,
        key_id,
        secret,
    }))
}

/// Preview one content-free portable component selection.
#[allow(clippy::too_many_arguments)]
pub(crate) fn preview_portable_selection(
    forge: &GraphForge,
    py: Python<'_>,
    checkpoint: Option<String>,
    profile: &str,
    identities: Option<&Bound<'_, pyo3::types::PyList>>,
    strict: bool,
    limits: Option<&Bound<'_, PyDict>>,
) -> PyResult<Py<PyAny>> {
    let native = forge.ensure_open()?;
    let request = PortableSelectionPreviewRequest {
        selection: selection_from_checkpoint(checkpoint),
        request: PortableSelectionRequest {
            profile: parse_profile(py, profile, identities)?,
            strict,
        },
        limits: parse_limits(py, limits)?,
    };
    let plan = py
        .detach(|| native.preview_portable_selection(&request))
        .map_err(|error| to_portable_pyerr(py, error))?;
    json_value_to_python(py, &selection_plan_json(&plan))
}

/// Preview one content-free portable graph-data subset.
pub(crate) fn preview_portable_graph_subset(
    forge: &GraphForge,
    py: Python<'_>,
    checkpoint: Option<String>,
    subset: &Bound<'_, PyDict>,
    limits: Option<&Bound<'_, PyDict>>,
) -> PyResult<Py<PyAny>> {
    let native = forge.ensure_open()?;
    let subset = parse_subset(py, Some(subset))?.ok_or_else(|| {
        to_pyerr(
            py,
            &GfError::Validation("subset request is required".into()),
        )
    })?;
    let request = PortableSubsetPreviewRequest {
        selection: selection_from_checkpoint(checkpoint),
        request: subset,
        limits: parse_limits(py, limits)?,
    };
    let plan = py
        .detach(|| native.preview_portable_graph_subset(&request))
        .map_err(|error| to_portable_pyerr(py, error))?;
    json_value_to_python(py, &subset_plan_json(&plan))
}

/// Export one pinned generation as an expanded or bundled portable package.
#[allow(clippy::too_many_arguments)]
pub(crate) fn export_portable(
    forge: &GraphForge,
    py: Python<'_>,
    output_path: &str,
    representation: &str,
    profile: &str,
    identities: Option<&Bound<'_, pyo3::types::PyList>>,
    checkpoint: Option<String>,
    subset: Option<&Bound<'_, PyDict>>,
    limits: Option<&Bound<'_, PyDict>>,
    cancellation: Option<&PyCancellationToken>,
    progress: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let native = forge.ensure_open()?;
    let request = PortableExportRequest {
        selection: selection_from_checkpoint(checkpoint),
        output_path: PathBuf::from(output_path),
        representation: parse_output(py, representation)?,
        profile: parse_profile(py, profile, identities)?,
        subset: parse_subset(py, subset)?,
        limits: parse_limits(py, limits)?,
    };
    let cancelled = cancellation.map(|token| token.inner.flag());
    let progress_cb = match progress {
        Some(callback) => {
            if !callback.is_callable() {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("progress callback must be callable".into()),
                ));
            }
            Some(callback.clone().unbind())
        }
        None => None,
    };
    let progress_error = std::sync::Mutex::new(None::<PyErr>);
    let result = py
        .detach(|| {
            native.export_portable(&request, cancelled, |event| {
                if let Some(callback) = progress_cb.as_ref() {
                    let callback_result = Python::attach(|py| {
                        let payload = PyDict::new(py);
                        payload.set_item("entries_completed", event.entries_completed)?;
                        payload.set_item("bytes_completed", event.bytes_completed)?;
                        payload.set_item("entries_total", event.entries_total)?;
                        payload.set_item("bytes_total", event.bytes_total)?;
                        callback.bind(py).call1((payload,))?;
                        Ok::<(), PyErr>(())
                    });
                    if let Err(error) = callback_result {
                        let mut slot = progress_error
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if slot.is_none() {
                            *slot = Some(error);
                        }
                    }
                }
            })
        })
        .map_err(|error| to_portable_pyerr(py, error))?;
    if let Some(error) = progress_error
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
    {
        return Err(error);
    }
    json_value_to_python(py, &export_result_json(&result))
}

/// Verify portable content without opening a project.
pub(crate) fn verify_portable(
    py: Python<'_>,
    input: &str,
    mode: &str,
    limits: Option<&Bound<'_, PyDict>>,
    cancellation: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    let request = PortableVerifyRequest {
        input: PathBuf::from(input),
        mode: parse_mode(py, mode)?,
        limits: parse_limits(py, limits)?,
    };
    let cancelled = cancellation.map(|token| token.inner.flag());
    let report = py
        .detach(|| graphforge_api::verify_portable(&request, cancelled))
        .map_err(|error| to_portable_pyerr(py, error))?;
    json_value_to_python(py, &verify_result_json(py, &report)?)
}

/// Verify and atomically import a complete portable package.
pub(crate) fn import_portable(
    py: Python<'_>,
    project_root: &str,
    input: &str,
    operation_id: &str,
    limits: Option<&Bound<'_, PyDict>>,
    cancellation: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    let request = PortableImportRequest {
        input: PathBuf::from(input),
        operation_id: canonical_operation_id(operation_id).map_err(|error| to_pyerr(py, &error))?,
        limits: parse_limits(py, limits)?,
    };
    let cancelled = cancellation.map(|token| token.inner.flag());
    let root = PathBuf::from(project_root);
    let result = py
        .detach(|| graphforge_api::GraphForge::import_portable(&root, &request, cancelled))
        .map_err(|error| to_portable_pyerr(py, error))?;
    json_value_to_python(py, &import_result_json(&result))
}

/// Publish a verified portable package to an OCI registry.
#[allow(clippy::too_many_arguments)]
pub(crate) fn publish_portable_oci(
    py: Python<'_>,
    package_path: &str,
    registry: &str,
    repository: &str,
    tag: Option<String>,
    limits: Option<&Bound<'_, PyDict>>,
    authenticity: Option<&Bound<'_, PyDict>>,
    signature: Option<&Bound<'_, PyDict>>,
    insecure_http: bool,
    credential: Option<String>,
    cancellation: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    let request = PortableOciPublishFacadeRequest {
        package_path: PathBuf::from(package_path),
        registry: registry.to_owned(),
        repository: repository.to_owned(),
        tag,
        limits: parse_limits(py, limits)?,
        authenticity: authenticity_policy(py, authenticity)?,
        signature: signature_material(py, signature)?,
        insecure_http,
        credential,
    };
    let cancelled = cancellation.map(|token| token.inner.flag());
    let reference = py
        .detach(|| graphforge_api::publish_portable_oci(&request, cancelled))
        .map_err(|error| to_portable_pyerr(py, error))?;
    json_value_to_python(py, &oci_reference_json(py, &reference)?)
}

/// Pull and verify a portable package from an OCI registry.
#[allow(clippy::too_many_arguments)]
pub(crate) fn pull_portable_oci(
    py: Python<'_>,
    registry: &str,
    repository: &str,
    reference: &str,
    destination: &str,
    expected_oci_digest: Option<String>,
    limits: Option<&Bound<'_, PyDict>>,
    authenticity: Option<&Bound<'_, PyDict>>,
    insecure_http: bool,
    credential: Option<String>,
    cancellation: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    let request = PortableOciPullFacadeRequest {
        registry: registry.to_owned(),
        repository: repository.to_owned(),
        reference: reference.to_owned(),
        expected_oci_digest,
        destination: PathBuf::from(destination),
        limits: parse_limits(py, limits)?,
        authenticity: authenticity_policy(py, authenticity)?,
        insecure_http,
        credential,
    };
    let cancelled = cancellation.map(|token| token.inner.flag());
    let receipt = py
        .detach(|| graphforge_api::pull_portable_oci(&request, cancelled))
        .map_err(|error| to_portable_pyerr(py, error))?;
    json_value_to_python(py, &oci_pull_json(py, &receipt)?)
}

/// Stream a query into an atomic Parquet result with explicit limits.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_to_parquet_stream(
    forge: &GraphForge,
    py: Python<'_>,
    query: &str,
    path: &str,
    params: Option<&Bound<'_, PyDict>>,
    max_row_group_rows: usize,
    max_batch_rows: usize,
    cancellation: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    let native = forge.ensure_open()?;
    let params = params_from_dict(params)?;
    let options = ResultSinkOptions {
        max_row_group_rows,
        max_batch_rows,
    };
    let cancellation = cancellation.map(|token| token.inner.clone());
    let query = query.to_owned();
    let path = path.to_owned();
    let receipt = py
        .detach(|| {
            native.execute_to_parquet_stream_with_params(
                &query,
                &params,
                &path,
                &options,
                cancellation.as_ref(),
            )
        })
        .map_err(|error| to_pyerr(py, &error))?;
    sink_receipt_dict(py, &receipt)
}

/// Stream a query into an atomic Arrow IPC stream file with explicit limits.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_to_arrow_ipc_stream(
    forge: &GraphForge,
    py: Python<'_>,
    query: &str,
    path: &str,
    params: Option<&Bound<'_, PyDict>>,
    max_row_group_rows: usize,
    max_batch_rows: usize,
    cancellation: Option<&PyCancellationToken>,
) -> PyResult<Py<PyAny>> {
    let native = forge.ensure_open()?;
    let params = params_from_dict(params)?;
    let options = ResultSinkOptions {
        max_row_group_rows,
        max_batch_rows,
    };
    let cancellation = cancellation.map(|token| token.inner.clone());
    let query = query.to_owned();
    let path = path.to_owned();
    let receipt = py
        .detach(|| {
            native.execute_to_arrow_ipc_stream_with_params(
                &query,
                &params,
                &path,
                &options,
                cancellation.as_ref(),
            )
        })
        .map_err(|error| to_pyerr(py, &error))?;
    sink_receipt_dict(py, &receipt)
}
