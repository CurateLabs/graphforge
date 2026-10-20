//! Thin Node bindings for portable and streaming result sinks (#744).

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use graphforge_api::{
    CancellationToken, GfError, MultiOntologyError, PortableAuthenticity, PortableCompatibility,
    PortableError, PortableExportRequest, PortableGraphSelector, PortableImportRequest,
    PortableIntegrity, PortableLimits, PortableMode, PortableOciAuthenticityPolicy,
    PortableOciPublishFacadeRequest, PortableOciPullFacadeRequest, PortableOciSignatureMaterial,
    PortableOciSignatureState, PortableOutput, PortableParticipantId, PortablePropertyProjection,
    PortableSelection, PortableSelectionPreviewRequest, PortableSelectionProfile,
    PortableSelectionRequest, PortableSubsetClosure, PortableSubsetPlan,
    PortableSubsetPreviewRequest, PortableSubsetRequest, PortableVerifyRequest, ResultSinkFormat,
    ResultSinkOptions, ResultSinkReceipt,
};
use napi::bindgen_prelude::{AbortSignal, AsyncTask, BigInt, Buffer};
use napi::{Env, Task};
use napi_derive::napi;

use crate::error::to_napi_err;
use crate::{Result, napi_validation};

pub(crate) fn to_portable_napi_err(error: PortableError) -> crate::NodeError {
    let committed = error.committed_import.clone();
    let envelope = MultiOntologyError::from(error);
    let mut value = serde_json::to_value(&envelope).expect("native error serializes");
    if let Some(receipt) = committed {
        value["committed_import"] =
            serde_json::to_value(receipt).expect("native receipt serializes");
    }
    let reason = serde_json::to_string(&value)
        .unwrap_or_else(|_| format!("{{\"code\":\"{}\"}}", envelope.code()));
    napi::Error::new(envelope.code().to_owned(), reason)
}

pub(crate) fn to_portable_deferred_err(env: Env, error: PortableError) -> napi::Error {
    let value = napi::JsError::from(to_portable_napi_err(error)).into_unknown(env);
    napi::Error::from(value)
}

fn selection_from_checkpoint(checkpoint: Option<String>) -> PortableSelection {
    match checkpoint {
        Some(name) => PortableSelection::Checkpoint(name),
        None => PortableSelection::Current,
    }
}

fn parse_limits(input: Option<PortableLimitsInput>) -> Result<PortableLimits> {
    let defaults = PortableLimits::default();
    let Some(input) = input else {
        return Ok(defaults);
    };
    Ok(PortableLimits {
        max_components: match input.max_components {
            Some(value) => crate::node_u64(Some(value), "maxComponents")?,
            None => defaults.max_components,
        },
        max_entries: match input.max_entries {
            Some(value) => crate::node_u64(Some(value), "maxEntries")?,
            None => defaults.max_entries,
        },
        max_entry_bytes: match input.max_entry_bytes {
            Some(value) => crate::node_u64(Some(value), "maxEntryBytes")?,
            None => defaults.max_entry_bytes,
        },
        max_total_bytes: match input.max_total_bytes {
            Some(value) => crate::node_u64(Some(value), "maxTotalBytes")?,
            None => defaults.max_total_bytes,
        },
        max_manifest_bytes: match input.max_manifest_bytes {
            Some(value) => crate::node_u64(Some(value), "maxManifestBytes")?,
            None => defaults.max_manifest_bytes,
        },
        max_semantic_manifest_bytes: match input.max_semantic_manifest_bytes {
            Some(value) => crate::node_u64(Some(value), "maxSemanticManifestBytes")?,
            None => defaults.max_semantic_manifest_bytes,
        },
        max_tag_manifest_bytes: match input.max_tag_manifest_bytes {
            Some(value) => crate::node_u64(Some(value), "maxTagManifestBytes")?,
            None => defaults.max_tag_manifest_bytes,
        },
        max_path_bytes: match input.max_path_bytes {
            Some(value) => crate::node_usize(Some(value), defaults.max_path_bytes, "maxPathBytes")?,
            None => defaults.max_path_bytes,
        },
        copy_buffer_bytes: match input.copy_buffer_bytes {
            Some(value) => {
                crate::node_usize(Some(value), defaults.copy_buffer_bytes, "copyBufferBytes")?
            }
            None => defaults.copy_buffer_bytes,
        },
    })
}

fn parse_profile(
    profile: Option<String>,
    identities: Option<Vec<PortableParticipantIdInput>>,
) -> Result<PortableSelectionProfile> {
    match profile.as_deref().unwrap_or("complete") {
        "complete" => Ok(PortableSelectionProfile::Complete),
        "ontology_only" => Ok(PortableSelectionProfile::OntologyOnly),
        "data_components" => Ok(PortableSelectionProfile::DataComponents),
        "artifacts" => Ok(PortableSelectionProfile::Artifacts),
        "settings" => Ok(PortableSelectionProfile::Settings),
        "custom" => {
            let identities = identities.ok_or_else(|| {
                to_napi_err(&GfError::Validation(
                    "custom profile requires identities".into(),
                ))
            })?;
            Ok(PortableSelectionProfile::Custom(
                identities
                    .into_iter()
                    .map(|identity| PortableParticipantId {
                        capability_id: identity.capability_id,
                        record_family_id: identity.record_family_id,
                    })
                    .collect(),
            ))
        }
        _ => Err(napi_validation(
            "profile must be complete, ontology_only, data_components, artifacts, settings, or custom",
        )),
    }
}

fn parse_subset(input: Option<PortableSubsetInput>) -> Result<Option<PortableSubsetRequest>> {
    let Some(input) = input else {
        return Ok(None);
    };
    let selector = input.selector.unwrap_or_default();
    let closure = match input.closure.as_deref().unwrap_or("induced_edges") {
        "induced_edges" => PortableSubsetClosure::InducedEdges,
        "referential" => PortableSubsetClosure::Referential,
        _ => {
            return Err(napi_validation(
                "closure must be induced_edges or referential",
            ));
        }
    };
    Ok(Some(PortableSubsetRequest {
        selector: PortableGraphSelector {
            node_uuids: selector.node_uuids.unwrap_or_default(),
            edge_uuids: selector.edge_uuids.unwrap_or_default(),
        },
        closure,
        projection: PortablePropertyProjection {
            exclude: input
                .projection
                .and_then(|projection| projection.exclude)
                .unwrap_or_default(),
        },
    }))
}

fn parse_output(representation: Option<&str>) -> Result<PortableOutput> {
    match representation.unwrap_or("bundle") {
        "expanded" => Ok(PortableOutput::Expanded),
        "bundle" => Ok(PortableOutput::Bundle),
        _ => Err(napi_validation("representation must be expanded or bundle")),
    }
}

fn parse_mode(mode: Option<&str>) -> Result<PortableMode> {
    match mode.unwrap_or("full") {
        "structure_only" => Ok(PortableMode::StructureOnly),
        "full" => Ok(PortableMode::Full),
        _ => Err(napi_validation("mode must be structure_only or full")),
    }
}

fn bind_signal(signal: Option<AbortSignal>) -> CancellationToken {
    let cancellation = CancellationToken::new();
    if let Some(signal) = signal {
        let cancellation = cancellation.clone();
        signal.on_abort(move || cancellation.cancel());
    }
    cancellation
}

fn selection_plan_json(plan: &graphforge_api::PortableSelectionPlan) -> serde_json::Value {
    serde_json::json!({
        "sourceGenerationUuid": plan.source_generation_uuid,
        "sourceManifestSha256": plan.source_manifest_sha256,
        "packageClass": plan.package_class,
        "included": plan.included,
        "excluded": plan.excluded,
        "projected": plan.projected,
        "includeGraphTree": plan.includes_graph_tree(),
        "redactions": plan.redactions,
        "requiredCapabilities": plan.required_capabilities,
        "estimatedPayloadBytes": plan.estimated_payload_bytes,
        "selectionFingerprint": plan.selection_fingerprint,
    })
}

fn subset_plan_json(plan: &PortableSubsetPlan) -> serde_json::Value {
    serde_json::json!({
        "selection": selection_plan_json(&plan.selection),
        "graphSubset": plan.graph_subset,
        "selectedNodeCount": plan.selected_node_count,
        "selectedEdgeCount": plan.selected_edge_count,
        "endpointNodeCount": plan.endpoint_node_count,
        "resultFingerprint": plan.result_fingerprint,
        "subsetFingerprint": plan.subset_fingerprint,
    })
}

fn export_result_output(
    result: graphforge_api::PortableExportFacadeResult,
) -> PortableExportOutput {
    PortableExportOutput {
        contract: result.contract.to_owned(),
        source: result.source.to_owned(),
        checkpoint: result.checkpoint,
        generation_uuid: result.generation_uuid.to_string(),
        package_digest: result.package_digest,
        transport_digest: result.transport_digest,
        entry_count: BigInt::from(u64::try_from(result.entry_count).unwrap_or(u64::MAX)),
        payload_bytes: BigInt::from(result.payload_bytes),
        representation: result.representation.to_owned(),
        selection_fingerprint: result.selection_fingerprint,
        output: result.output.display().to_string(),
    }
}

fn integrity_token(value: PortableIntegrity) -> String {
    match value {
        PortableIntegrity::NotChecked => "not_checked".into(),
        PortableIntegrity::Verified => "verified".into(),
        PortableIntegrity::Failed => "failed".into(),
    }
}

fn compatibility_token(value: PortableCompatibility) -> String {
    match value {
        PortableCompatibility::Supported => "supported".into(),
        PortableCompatibility::UnsupportedFuture => "unsupported_future".into(),
        PortableCompatibility::Failed => "failed".into(),
    }
}

fn authenticity_token(value: PortableAuthenticity) -> String {
    match value {
        PortableAuthenticity::NotEvaluated => "not_evaluated".into(),
        PortableAuthenticity::Unsigned => "unsigned".into(),
        PortableAuthenticity::Verified => "verified".into(),
        PortableAuthenticity::Failed => "failed".into(),
    }
}

fn signature_state_token(value: PortableOciSignatureState) -> String {
    match value {
        PortableOciSignatureState::Valid => "valid".into(),
        PortableOciSignatureState::Invalid => "invalid".into(),
        PortableOciSignatureState::Absent => "absent".into(),
        PortableOciSignatureState::PolicyMismatched => "policy_mismatched".into(),
    }
}

fn verify_report_output(report: graphforge_api::PortableVerifyResult) -> PortableVerifyOutput {
    PortableVerifyOutput {
        research_interchange: report.research_interchange,
        research_entries: report
            .research_entries
            .into_iter()
            .map(|entry| PortableResearchEntryOutput {
                component_id: entry.component_id,
                path: entry.path,
                length: BigInt::from(entry.length),
                sha256: entry.sha256,
            })
            .collect(),
        contract: report.contract.to_owned(),
        representation: match report.representation {
            graphforge_api::PortableRepresentation::Expanded => "expanded".into(),
            graphforge_api::PortableRepresentation::Bundle => "bundle".into(),
        },
        package_digest: report.package_digest,
        producing_core_version: report.producing_core_version,
        package_class: match report.package_class {
            graphforge_api::PortablePackageClass::Complete => "complete".into(),
            graphforge_api::PortablePackageClass::OntologyOnly => "ontology_only".into(),
            graphforge_api::PortablePackageClass::ComponentSelective => {
                "component_selective".into()
            }
            graphforge_api::PortablePackageClass::GraphDataSubset => "graph_data_subset".into(),
        },
        component_count: BigInt::from(report.component_count),
        entry_count: BigInt::from(report.entry_count),
        payload_bytes: BigInt::from(report.payload_bytes),
        integrity: integrity_token(report.integrity),
        compatibility: compatibility_token(report.compatibility),
        authenticity: authenticity_token(report.authenticity),
        transport_digest: report.transport_digest,
        ontology_composition: serde_json::to_value(report.ontology_composition)
            .expect("portable composition contains only serializable contract fields"),
        ontology_composition_entries: report
            .ontology_composition_entries
            .into_iter()
            .map(|entry| PortableCompositionEntryOutput {
                kind: entry.kind,
                identity: serde_json::to_value(entry.identity)
                    .expect("portable identity contains only strings"),
                path: entry.path,
                media_type: entry.media_type,
                length: BigInt::from(entry.length),
                sha256: entry.sha256,
                required_dependencies: entry.required_dependencies,
            })
            .collect(),
    }
}

fn sink_receipt_output(receipt: ResultSinkReceipt) -> ResultSinkReceiptOutput {
    ResultSinkReceiptOutput {
        destination: receipt.destination.display().to_string(),
        format: match receipt.format {
            ResultSinkFormat::Parquet => "parquet".into(),
            ResultSinkFormat::ArrowIpc => "arrow_ipc".into(),
        },
        progress: ResultSinkProgressOutput {
            phase: receipt.progress.phase.to_owned(),
            rows: BigInt::from(receipt.progress.rows),
            batches: BigInt::from(receipt.progress.batches),
            bytes: BigInt::from(receipt.progress.bytes),
            elapsed_ms: BigInt::from(
                u64::try_from(receipt.progress.elapsed.as_millis()).unwrap_or(u64::MAX),
            ),
            complete: receipt.progress.complete,
        },
    }
}

#[napi(object)]
pub struct PortableLimitsInput {
    pub max_components: Option<BigInt>,
    pub max_entries: Option<BigInt>,
    pub max_entry_bytes: Option<BigInt>,
    pub max_total_bytes: Option<BigInt>,
    pub max_manifest_bytes: Option<BigInt>,
    pub max_semantic_manifest_bytes: Option<BigInt>,
    pub max_tag_manifest_bytes: Option<BigInt>,
    pub max_path_bytes: Option<BigInt>,
    pub copy_buffer_bytes: Option<BigInt>,
}

#[napi(object)]
pub struct PortableParticipantIdInput {
    pub capability_id: String,
    pub record_family_id: String,
}

#[napi(object)]
#[derive(Default)]
pub struct PortableGraphSelectorInput {
    pub node_uuids: Option<Vec<String>>,
    pub edge_uuids: Option<Vec<String>>,
}

#[napi(object)]
pub struct PortablePropertyProjectionInput {
    pub exclude: Option<Vec<String>>,
}

#[napi(object)]
pub struct PortableSubsetInput {
    pub selector: Option<PortableGraphSelectorInput>,
    pub closure: Option<String>,
    pub projection: Option<PortablePropertyProjectionInput>,
}

#[napi(object)]
pub struct PortableSelectionPreviewInput {
    pub checkpoint: Option<String>,
    pub profile: Option<String>,
    pub identities: Option<Vec<PortableParticipantIdInput>>,
    pub strict: Option<bool>,
    pub limits: Option<PortableLimitsInput>,
}

#[napi(object)]
pub struct PortableSubsetPreviewInput {
    pub checkpoint: Option<String>,
    pub subset: PortableSubsetInput,
    pub limits: Option<PortableLimitsInput>,
}

#[napi(object, object_to_js = false)]
pub struct PortableExportInput {
    pub output_path: String,
    pub representation: Option<String>,
    pub profile: Option<String>,
    pub identities: Option<Vec<PortableParticipantIdInput>>,
    pub checkpoint: Option<String>,
    pub subset: Option<PortableSubsetInput>,
    pub limits: Option<PortableLimitsInput>,
    pub signal: Option<AbortSignal>,
}

#[napi(object, object_to_js = false)]
pub struct PortableVerifyInput {
    pub input: String,
    pub mode: Option<String>,
    pub limits: Option<PortableLimitsInput>,
    pub signal: Option<AbortSignal>,
}

#[napi(object, object_to_js = false)]
pub struct PortableImportInput {
    pub project_root: String,
    pub input: String,
    pub operation_id: String,
    pub limits: Option<PortableLimitsInput>,
    pub signal: Option<AbortSignal>,
}

#[napi(object)]
pub struct PortableOciAuthenticityInput {
    pub require_named_signer: Option<String>,
    pub verification_key: Option<Buffer>,
}

#[napi(object)]
pub struct PortableOciSignatureInput {
    pub signer: String,
    pub key_id: String,
    pub secret: Buffer,
}

#[napi(object, object_to_js = false)]
pub struct PortableOciPublishInput {
    pub package_path: String,
    pub registry: String,
    pub repository: String,
    pub tag: Option<String>,
    pub limits: Option<PortableLimitsInput>,
    pub authenticity: Option<PortableOciAuthenticityInput>,
    pub signature: Option<PortableOciSignatureInput>,
    pub insecure_http: Option<bool>,
    pub credential: Option<String>,
    pub signal: Option<AbortSignal>,
}

#[napi(object, object_to_js = false)]
pub struct PortableOciPullInput {
    pub registry: String,
    pub repository: String,
    pub reference: String,
    pub destination: String,
    pub expected_oci_digest: Option<String>,
    pub limits: Option<PortableLimitsInput>,
    pub authenticity: Option<PortableOciAuthenticityInput>,
    pub insecure_http: Option<bool>,
    pub credential: Option<String>,
    pub signal: Option<AbortSignal>,
}

#[napi(object)]
pub struct PortableExportOutput {
    pub contract: String,
    pub source: String,
    pub checkpoint: Option<String>,
    pub generation_uuid: String,
    pub package_digest: String,
    pub transport_digest: String,
    pub entry_count: BigInt,
    pub payload_bytes: BigInt,
    pub representation: String,
    pub selection_fingerprint: String,
    pub output: String,
}

#[napi(object)]
pub struct PortableVerifyOutput {
    /// Whether the registered native research interchange component was found.
    pub research_interchange: bool,
    /// Manifest-authenticated research files, preserving exact integer lengths.
    pub research_entries: Vec<PortableResearchEntryOutput>,
    pub contract: String,
    pub representation: String,
    pub package_digest: String,
    pub package_class: String,
    /// Exact GraphForge Core version the package manifest declares as producer.
    pub producing_core_version: String,
    pub component_count: BigInt,
    pub entry_count: BigInt,
    pub payload_bytes: BigInt,
    pub integrity: String,
    pub compatibility: String,
    pub authenticity: String,
    pub transport_digest: Option<String>,
    /// Exact authenticated composition, with canonical contract field names.
    pub ontology_composition: serde_json::Value,
    /// Authenticated entries; lengths preserve the binding's bigint contract.
    pub ontology_composition_entries: Vec<PortableCompositionEntryOutput>,
}

#[napi(object)]
pub struct PortableResearchEntryOutput {
    pub component_id: String,
    pub path: String,
    pub length: BigInt,
    pub sha256: String,
}

#[napi(object)]
pub struct PortableImportOutput {
    pub package_digest: String,
    pub transport_digest: Option<String>,
    pub generation_uuid: String,
    pub idempotent_replay: bool,
}

#[napi(object)]
pub struct PortableOciReferenceOutput {
    pub registry: String,
    pub repository: String,
    pub oci_manifest_digest: String,
    pub package_digest: String,
    pub package_class: String,
    pub tag: Option<String>,
    pub bytes_transferred: BigInt,
    pub blob_count: BigInt,
}

#[napi(object)]
pub struct PortableOciPullOutput {
    pub reference: PortableOciReferenceOutput,
    pub destination: String,
    pub report: PortableVerifyOutput,
    pub signature_state: String,
}

#[napi(object, object_to_js = false)]
pub struct ResultSinkOptionsInput {
    pub max_row_group_rows: Option<BigInt>,
    pub max_batch_rows: Option<BigInt>,
    pub signal: Option<AbortSignal>,
}

#[napi(object)]
pub struct ResultSinkProgressOutput {
    pub phase: String,
    pub rows: BigInt,
    pub batches: BigInt,
    pub bytes: BigInt,
    pub elapsed_ms: BigInt,
    pub complete: bool,
}

#[napi(object)]
pub struct ResultSinkReceiptOutput {
    pub destination: String,
    pub format: String,
    pub progress: ResultSinkProgressOutput,
}

pub(crate) fn preview_selection(
    graph: &graphforge_api::GraphForge,
    input: PortableSelectionPreviewInput,
) -> Result<serde_json::Value> {
    let request = PortableSelectionPreviewRequest {
        selection: selection_from_checkpoint(input.checkpoint),
        request: PortableSelectionRequest {
            profile: parse_profile(input.profile, input.identities)?,
            strict: input.strict.unwrap_or(false),
        },
        limits: parse_limits(input.limits)?,
    };
    let plan = graph
        .preview_portable_selection(&request)
        .map_err(to_portable_napi_err)?;
    Ok(selection_plan_json(&plan))
}

pub(crate) fn preview_subset(
    graph: &graphforge_api::GraphForge,
    input: PortableSubsetPreviewInput,
) -> Result<serde_json::Value> {
    let subset = parse_subset(Some(input.subset))?
        .ok_or_else(|| to_napi_err(&GfError::Validation("subset is required".into())))?;
    let request = PortableSubsetPreviewRequest {
        selection: selection_from_checkpoint(input.checkpoint),
        request: subset,
        limits: parse_limits(input.limits)?,
    };
    let plan = graph
        .preview_portable_graph_subset(&request)
        .map_err(to_portable_napi_err)?;
    Ok(subset_plan_json(&plan))
}

pub struct ExportPortableTask {
    pub engine: Arc<RwLock<graphforge_api::GraphForge>>,
    pub request: PortableExportRequest,
    pub cancellation: CancellationToken,
}

impl Task for ExportPortableTask {
    type Output = std::result::Result<graphforge_api::PortableExportFacadeResult, PortableError>;
    type JsValue = PortableExportOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let graph = self
            .engine
            .read()
            .map_err(|_| napi::Error::from_reason("GraphForge lock poisoned"))?;
        Ok(graph.export_portable(&self.request, Some(self.cancellation.flag()), |_| {}))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(export_result_output)
            .map_err(|error| to_portable_deferred_err(env, error))
    }
}

pub struct VerifyPortableTask {
    pub request: PortableVerifyRequest,
    pub cancellation: CancellationToken,
}

impl Task for VerifyPortableTask {
    type Output = std::result::Result<graphforge_api::PortableVerifyResult, PortableError>;
    type JsValue = PortableVerifyOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok(graphforge_api::verify_portable(
            &self.request,
            Some(self.cancellation.flag()),
        ))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(verify_report_output)
            .map_err(|error| to_portable_deferred_err(env, error))
    }
}

pub struct ImportPortableTask {
    pub project_root: PathBuf,
    pub request: PortableImportRequest,
    pub cancellation: CancellationToken,
}

impl Task for ImportPortableTask {
    type Output = std::result::Result<graphforge_api::PortableImportResult, PortableError>;
    type JsValue = PortableImportOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok(graphforge_api::GraphForge::import_portable(
            &self.project_root,
            &self.request,
            Some(self.cancellation.flag()),
        ))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(|result| PortableImportOutput {
                package_digest: result.package_digest,
                transport_digest: result.transport_digest,
                generation_uuid: result.generation_uuid.to_string(),
                idempotent_replay: result.idempotent_replay,
            })
            .map_err(|error| to_portable_deferred_err(env, error))
    }
}

fn authenticity_policy(
    input: Option<PortableOciAuthenticityInput>,
) -> PortableOciAuthenticityPolicy {
    let Some(input) = input else {
        return PortableOciAuthenticityPolicy::default();
    };
    PortableOciAuthenticityPolicy {
        require_named_signer: input.require_named_signer,
        verification_key: input.verification_key.map(|key| key.to_vec()),
    }
}

fn signature_material(
    input: Option<PortableOciSignatureInput>,
) -> Option<PortableOciSignatureMaterial> {
    input.map(|input| PortableOciSignatureMaterial {
        signer: input.signer,
        key_id: input.key_id,
        secret: input.secret.to_vec(),
    })
}

fn oci_reference_output(
    reference: graphforge_api::PortableOciReference,
) -> PortableOciReferenceOutput {
    PortableOciReferenceOutput {
        registry: reference.registry,
        repository: reference.repository,
        oci_manifest_digest: reference.oci_manifest_digest,
        package_digest: reference.package_digest,
        package_class: match reference.package_class {
            graphforge_api::PortablePackageClass::Complete => "complete".into(),
            graphforge_api::PortablePackageClass::OntologyOnly => "ontology_only".into(),
            graphforge_api::PortablePackageClass::ComponentSelective => {
                "component_selective".into()
            }
            graphforge_api::PortablePackageClass::GraphDataSubset => "graph_data_subset".into(),
        },
        tag: reference.tag,
        bytes_transferred: BigInt::from(reference.bytes_transferred),
        blob_count: BigInt::from(reference.blob_count),
    }
}

pub struct PublishOciTask {
    pub request: PortableOciPublishFacadeRequest,
    pub cancellation: CancellationToken,
}

impl Task for PublishOciTask {
    type Output = std::result::Result<graphforge_api::PortableOciReference, PortableError>;
    type JsValue = PortableOciReferenceOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok(graphforge_api::publish_portable_oci(
            &self.request,
            Some(self.cancellation.flag()),
        ))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(oci_reference_output)
            .map_err(|error| to_portable_deferred_err(env, error))
    }
}

pub struct PullOciTask {
    pub request: PortableOciPullFacadeRequest,
    pub cancellation: CancellationToken,
}

impl Task for PullOciTask {
    type Output = std::result::Result<graphforge_api::PortableOciPullReceipt, PortableError>;
    type JsValue = PortableOciPullOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok(graphforge_api::pull_portable_oci(
            &self.request,
            Some(self.cancellation.flag()),
        ))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(|receipt| PortableOciPullOutput {
                reference: oci_reference_output(receipt.reference),
                destination: receipt.destination.display().to_string(),
                report: verify_report_output(receipt.report),
                signature_state: signature_state_token(receipt.signature_state),
            })
            .map_err(|error| to_portable_deferred_err(env, error))
    }
}

pub fn build_export_task(
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    input: PortableExportInput,
) -> Result<AsyncTask<ExportPortableTask>> {
    let cancellation = bind_signal(input.signal);
    Ok(AsyncTask::new(ExportPortableTask {
        engine,
        request: PortableExportRequest {
            selection: selection_from_checkpoint(input.checkpoint),
            output_path: PathBuf::from(input.output_path),
            representation: parse_output(input.representation.as_deref())?,
            profile: parse_profile(input.profile, input.identities)?,
            subset: parse_subset(input.subset)?,
            limits: parse_limits(input.limits)?,
        },
        cancellation,
    }))
}

pub fn build_verify_task(input: PortableVerifyInput) -> Result<AsyncTask<VerifyPortableTask>> {
    let cancellation = bind_signal(input.signal);
    Ok(AsyncTask::new(VerifyPortableTask {
        request: PortableVerifyRequest {
            input: PathBuf::from(input.input),
            mode: parse_mode(input.mode.as_deref())?,
            limits: parse_limits(input.limits)?,
        },
        cancellation,
    }))
}

pub fn build_import_task(input: PortableImportInput) -> Result<AsyncTask<ImportPortableTask>> {
    let cancellation = bind_signal(input.signal);
    Ok(AsyncTask::new(ImportPortableTask {
        project_root: PathBuf::from(input.project_root),
        request: PortableImportRequest {
            input: PathBuf::from(input.input),
            operation_id: crate::canonical_operation_id(&input.operation_id)?,
            limits: parse_limits(input.limits)?,
        },
        cancellation,
    }))
}

pub fn build_publish_task(input: PortableOciPublishInput) -> Result<AsyncTask<PublishOciTask>> {
    let cancellation = bind_signal(input.signal);
    Ok(AsyncTask::new(PublishOciTask {
        request: PortableOciPublishFacadeRequest {
            package_path: PathBuf::from(input.package_path),
            registry: input.registry,
            repository: input.repository,
            tag: input.tag,
            limits: parse_limits(input.limits)?,
            authenticity: authenticity_policy(input.authenticity),
            signature: signature_material(input.signature),
            insecure_http: input.insecure_http.unwrap_or(false),
            credential: input.credential,
        },
        cancellation,
    }))
}

pub fn build_pull_task(input: PortableOciPullInput) -> Result<AsyncTask<PullOciTask>> {
    let cancellation = bind_signal(input.signal);
    Ok(AsyncTask::new(PullOciTask {
        request: PortableOciPullFacadeRequest {
            registry: input.registry,
            repository: input.repository,
            reference: input.reference,
            expected_oci_digest: input.expected_oci_digest,
            destination: PathBuf::from(input.destination),
            limits: parse_limits(input.limits)?,
            authenticity: authenticity_policy(input.authenticity),
            insecure_http: input.insecure_http.unwrap_or(false),
            credential: input.credential,
        },
        cancellation,
    }))
}

pub fn parse_sink_options(
    input: Option<ResultSinkOptionsInput>,
) -> Result<(ResultSinkOptions, CancellationToken)> {
    let defaults = ResultSinkOptions::default();
    let Some(input) = input else {
        return Ok((defaults, CancellationToken::new()));
    };
    let options = ResultSinkOptions {
        max_row_group_rows: crate::node_usize(
            input.max_row_group_rows,
            defaults.max_row_group_rows,
            "maxRowGroupRows",
        )?,
        max_batch_rows: crate::node_usize(
            input.max_batch_rows,
            defaults.max_batch_rows,
            "maxBatchRows",
        )?,
    };
    Ok((options, bind_signal(input.signal)))
}

pub struct SinkStreamTask {
    pub engine: Arc<RwLock<graphforge_api::GraphForge>>,
    pub closed: Arc<std::sync::atomic::AtomicBool>,
    pub cypher: String,
    pub params: std::collections::HashMap<String, graphforge_api::IrLiteral>,
    pub path: String,
    pub format: ResultSinkFormat,
    pub options: ResultSinkOptions,
    pub cancellation: CancellationToken,
}

impl Task for SinkStreamTask {
    type Output = std::result::Result<ResultSinkReceipt, GfError>;
    type JsValue = ResultSinkReceiptOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            if self.closed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(GfError::Lifecycle(
                    "operation on a closed GraphForge instance".into(),
                ));
            }
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            match self.format {
                ResultSinkFormat::Parquet => graph.execute_to_parquet_stream_with_params(
                    &self.cypher,
                    &self.params,
                    &self.path,
                    &self.options,
                    Some(&self.cancellation),
                ),
                ResultSinkFormat::ArrowIpc => graph.execute_to_arrow_ipc_stream_with_params(
                    &self.cypher,
                    &self.params,
                    &self.path,
                    &self.options,
                    Some(&self.cancellation),
                ),
            }
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(sink_receipt_output)
            .map_err(|error| crate::to_napi_deferred_err(env, &error))
    }
}

#[napi(object)]
pub struct PortableCompositionEntryOutput {
    pub kind: String,
    pub identity: serde_json::Value,
    pub path: String,
    pub media_type: String,
    pub length: BigInt,
    pub sha256: String,
    pub required_dependencies: Vec<String>,
}
