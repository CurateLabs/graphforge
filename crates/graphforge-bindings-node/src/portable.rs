//! Thin Node bindings for portable-v2 and streaming result sinks (#744).

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use graphforge_api::{
    CancellationToken, GfError, MultiOntologyError, PortableSelection, PortableV2Authenticity,
    PortableV2Compatibility, PortableV2Error, PortableV2ExportRequest, PortableV2GraphSelector,
    PortableV2ImportRequest, PortableV2Integrity, PortableV2Limits, PortableV2Mode,
    PortableV2OciAuthenticityPolicy, PortableV2OciPublishFacadeRequest,
    PortableV2OciPullFacadeRequest, PortableV2OciSignatureMaterial, PortableV2OciSignatureState,
    PortableV2Output, PortableV2ParticipantId, PortableV2PropertyProjection,
    PortableV2SelectionPreviewRequest, PortableV2SelectionProfile, PortableV2SelectionRequest,
    PortableV2SubsetClosure, PortableV2SubsetPlan, PortableV2SubsetPreviewRequest,
    PortableV2SubsetRequest, PortableVerifyRequest, ResultSinkFormat, ResultSinkOptions,
    ResultSinkReceipt,
};
use napi::bindgen_prelude::{AbortSignal, AsyncTask, BigInt, Buffer};
use napi::{Env, Task};
use napi_derive::napi;

use crate::error::to_napi_err;
use crate::{Result, napi_validation};

pub(crate) fn to_portable_napi_err(error: PortableV2Error) -> crate::NodeError {
    let envelope = MultiOntologyError::from(error);
    let reason = serde_json::to_string(&envelope)
        .unwrap_or_else(|_| format!("{{\"code\":\"{}\"}}", envelope.code()));
    napi::Error::new(envelope.code().to_owned(), reason)
}

fn to_portable_deferred_err(env: Env, error: PortableV2Error) -> napi::Error {
    let value = napi::JsError::from(to_portable_napi_err(error)).into_unknown(env);
    napi::Error::from(value)
}

fn selection_from_checkpoint(checkpoint: Option<String>) -> PortableSelection {
    match checkpoint {
        Some(name) => PortableSelection::Checkpoint(name),
        None => PortableSelection::Current,
    }
}

fn parse_limits(input: Option<PortableV2LimitsInput>) -> Result<PortableV2Limits> {
    let defaults = PortableV2Limits::default();
    let Some(input) = input else {
        return Ok(defaults);
    };
    Ok(PortableV2Limits {
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
) -> Result<PortableV2SelectionProfile> {
    match profile.as_deref().unwrap_or("complete") {
        "complete" => Ok(PortableV2SelectionProfile::Complete),
        "ontology_only" => Ok(PortableV2SelectionProfile::OntologyOnly),
        "data_components" => Ok(PortableV2SelectionProfile::DataComponents),
        "artifacts" => Ok(PortableV2SelectionProfile::Artifacts),
        "settings" => Ok(PortableV2SelectionProfile::Settings),
        "custom" => {
            let identities = identities.ok_or_else(|| {
                to_napi_err(&GfError::Validation(
                    "custom profile requires identities".into(),
                ))
            })?;
            Ok(PortableV2SelectionProfile::Custom(
                identities
                    .into_iter()
                    .map(|identity| PortableV2ParticipantId {
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

fn parse_subset(input: Option<PortableSubsetInput>) -> Result<Option<PortableV2SubsetRequest>> {
    let Some(input) = input else {
        return Ok(None);
    };
    let selector = input.selector.unwrap_or_default();
    let closure = match input.closure.as_deref().unwrap_or("induced_edges") {
        "induced_edges" => PortableV2SubsetClosure::InducedEdges,
        "referential" => PortableV2SubsetClosure::Referential,
        _ => {
            return Err(napi_validation(
                "closure must be induced_edges or referential",
            ));
        }
    };
    Ok(Some(PortableV2SubsetRequest {
        selector: PortableV2GraphSelector {
            node_uuids: selector.node_uuids.unwrap_or_default(),
            edge_uuids: selector.edge_uuids.unwrap_or_default(),
        },
        closure,
        projection: PortableV2PropertyProjection {
            exclude: input
                .projection
                .and_then(|projection| projection.exclude)
                .unwrap_or_default(),
        },
    }))
}

fn parse_output(representation: Option<&str>) -> Result<PortableV2Output> {
    match representation.unwrap_or("bundle") {
        "expanded" => Ok(PortableV2Output::Expanded),
        "bundle" => Ok(PortableV2Output::Bundle),
        _ => Err(napi_validation("representation must be expanded or bundle")),
    }
}

fn parse_mode(mode: Option<&str>) -> Result<PortableV2Mode> {
    match mode.unwrap_or("full") {
        "structure_only" => Ok(PortableV2Mode::StructureOnly),
        "full" => Ok(PortableV2Mode::Full),
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

fn selection_plan_json(plan: &graphforge_api::PortableV2SelectionPlan) -> serde_json::Value {
    serde_json::json!({
        "sourceGenerationUuid": plan.source_generation_uuid,
        "sourceManifestSha256": plan.source_manifest_sha256,
        "packageClass": plan.package_class,
        "included": plan.included,
        "excluded": plan.excluded,
        "redactions": plan.redactions,
        "requiredCapabilities": plan.required_capabilities,
        "estimatedPayloadBytes": plan.estimated_payload_bytes,
        "selectionFingerprint": plan.selection_fingerprint,
    })
}

fn subset_plan_json(plan: &PortableV2SubsetPlan) -> serde_json::Value {
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
    result: graphforge_api::PortableV2ExportFacadeResult,
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

fn integrity_token(value: PortableV2Integrity) -> String {
    match value {
        PortableV2Integrity::NotChecked => "not_checked".into(),
        PortableV2Integrity::Verified => "verified".into(),
        PortableV2Integrity::Failed => "failed".into(),
    }
}

fn compatibility_token(value: PortableV2Compatibility) -> String {
    match value {
        PortableV2Compatibility::Supported => "supported".into(),
        PortableV2Compatibility::UnsupportedFuture => "unsupported_future".into(),
        PortableV2Compatibility::Failed => "failed".into(),
    }
}

fn authenticity_token(value: PortableV2Authenticity) -> String {
    match value {
        PortableV2Authenticity::NotEvaluated => "not_evaluated".into(),
        PortableV2Authenticity::Unsigned => "unsigned".into(),
        PortableV2Authenticity::Verified => "verified".into(),
        PortableV2Authenticity::Failed => "failed".into(),
    }
}

fn signature_state_token(value: PortableV2OciSignatureState) -> String {
    match value {
        PortableV2OciSignatureState::Valid => "valid".into(),
        PortableV2OciSignatureState::Invalid => "invalid".into(),
        PortableV2OciSignatureState::Absent => "absent".into(),
        PortableV2OciSignatureState::PolicyMismatched => "policy_mismatched".into(),
    }
}

fn verify_report_output(report: graphforge_api::PortableVerifyResult) -> PortableVerifyOutput {
    PortableVerifyOutput {
        contract: report.contract.to_owned(),
        representation: match report.representation {
            graphforge_api::PortableV2Representation::Expanded => "expanded".into(),
            graphforge_api::PortableV2Representation::Bundle => "bundle".into(),
        },
        package_digest: report.package_digest,
        package_class: match report.package_class {
            graphforge_api::PortableV2PackageClass::Complete => "complete".into(),
            graphforge_api::PortableV2PackageClass::OntologyOnly => "ontology_only".into(),
            graphforge_api::PortableV2PackageClass::ComponentSelective => {
                "component_selective".into()
            }
            graphforge_api::PortableV2PackageClass::GraphDataSubset => "graph_data_subset".into(),
        },
        component_count: BigInt::from(report.component_count),
        entry_count: BigInt::from(report.entry_count),
        payload_bytes: BigInt::from(report.payload_bytes),
        integrity: integrity_token(report.integrity),
        compatibility: compatibility_token(report.compatibility),
        authenticity: authenticity_token(report.authenticity),
        transport_digest: report.transport_digest,
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
pub struct PortableV2LimitsInput {
    pub max_components: Option<BigInt>,
    pub max_entries: Option<BigInt>,
    pub max_entry_bytes: Option<BigInt>,
    pub max_total_bytes: Option<BigInt>,
    pub max_manifest_bytes: Option<BigInt>,
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
    pub limits: Option<PortableV2LimitsInput>,
}

#[napi(object)]
pub struct PortableSubsetPreviewInput {
    pub checkpoint: Option<String>,
    pub subset: PortableSubsetInput,
    pub limits: Option<PortableV2LimitsInput>,
}

#[napi(object, object_to_js = false)]
pub struct PortableExportInput {
    pub output_path: String,
    pub representation: Option<String>,
    pub profile: Option<String>,
    pub identities: Option<Vec<PortableParticipantIdInput>>,
    pub checkpoint: Option<String>,
    pub subset: Option<PortableSubsetInput>,
    pub limits: Option<PortableV2LimitsInput>,
    pub signal: Option<AbortSignal>,
}

#[napi(object, object_to_js = false)]
pub struct PortableVerifyInput {
    pub input: String,
    pub mode: Option<String>,
    pub limits: Option<PortableV2LimitsInput>,
    pub signal: Option<AbortSignal>,
}

#[napi(object, object_to_js = false)]
pub struct PortableImportInput {
    pub project_root: String,
    pub input: String,
    pub operation_id: String,
    pub limits: Option<PortableV2LimitsInput>,
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
    pub limits: Option<PortableV2LimitsInput>,
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
    pub limits: Option<PortableV2LimitsInput>,
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
    pub contract: String,
    pub representation: String,
    pub package_digest: String,
    pub package_class: String,
    pub component_count: BigInt,
    pub entry_count: BigInt,
    pub payload_bytes: BigInt,
    pub integrity: String,
    pub compatibility: String,
    pub authenticity: String,
    pub transport_digest: Option<String>,
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
    let request = PortableV2SelectionPreviewRequest {
        selection: selection_from_checkpoint(input.checkpoint),
        request: PortableV2SelectionRequest {
            profile: parse_profile(input.profile, input.identities)?,
            strict: input.strict.unwrap_or(false),
        },
        limits: parse_limits(input.limits)?,
    };
    let plan = graph
        .preview_portable_v2_selection(&request)
        .map_err(to_portable_napi_err)?;
    Ok(selection_plan_json(&plan))
}

pub(crate) fn preview_subset(
    graph: &graphforge_api::GraphForge,
    input: PortableSubsetPreviewInput,
) -> Result<serde_json::Value> {
    let subset = parse_subset(Some(input.subset))?
        .ok_or_else(|| to_napi_err(&GfError::Validation("subset is required".into())))?;
    let request = PortableV2SubsetPreviewRequest {
        selection: selection_from_checkpoint(input.checkpoint),
        request: subset,
        limits: parse_limits(input.limits)?,
    };
    let plan = graph
        .preview_portable_v2_graph_subset(&request)
        .map_err(to_portable_napi_err)?;
    Ok(subset_plan_json(&plan))
}

pub struct ExportPortableTask {
    pub engine: Arc<RwLock<graphforge_api::GraphForge>>,
    pub request: PortableV2ExportRequest,
    pub cancellation: CancellationToken,
}

impl Task for ExportPortableTask {
    type Output =
        std::result::Result<graphforge_api::PortableV2ExportFacadeResult, PortableV2Error>;
    type JsValue = PortableExportOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let graph = self
            .engine
            .read()
            .map_err(|_| napi::Error::from_reason("GraphForge lock poisoned"))?;
        Ok(graph.export_portable_v2(&self.request, Some(self.cancellation.flag()), |_| {}))
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
    type Output = std::result::Result<graphforge_api::PortableVerifyResult, PortableV2Error>;
    type JsValue = PortableVerifyOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok(graphforge_api::verify_portable_v2(
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
    pub request: PortableV2ImportRequest,
    pub cancellation: CancellationToken,
}

impl Task for ImportPortableTask {
    type Output = std::result::Result<graphforge_api::PortableV2ImportResult, PortableV2Error>;
    type JsValue = PortableImportOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok(graphforge_api::GraphForge::import_portable_v2(
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
) -> PortableV2OciAuthenticityPolicy {
    let Some(input) = input else {
        return PortableV2OciAuthenticityPolicy::default();
    };
    PortableV2OciAuthenticityPolicy {
        require_named_signer: input.require_named_signer,
        verification_key: input.verification_key.map(|key| key.to_vec()),
    }
}

fn signature_material(
    input: Option<PortableOciSignatureInput>,
) -> Option<PortableV2OciSignatureMaterial> {
    input.map(|input| PortableV2OciSignatureMaterial {
        signer: input.signer,
        key_id: input.key_id,
        secret: input.secret.to_vec(),
    })
}

fn oci_reference_output(
    reference: graphforge_api::PortableV2OciReference,
) -> PortableOciReferenceOutput {
    PortableOciReferenceOutput {
        registry: reference.registry,
        repository: reference.repository,
        oci_manifest_digest: reference.oci_manifest_digest,
        package_digest: reference.package_digest,
        package_class: match reference.package_class {
            graphforge_api::PortableV2PackageClass::Complete => "complete".into(),
            graphforge_api::PortableV2PackageClass::OntologyOnly => "ontology_only".into(),
            graphforge_api::PortableV2PackageClass::ComponentSelective => {
                "component_selective".into()
            }
            graphforge_api::PortableV2PackageClass::GraphDataSubset => "graph_data_subset".into(),
        },
        tag: reference.tag,
        bytes_transferred: BigInt::from(reference.bytes_transferred),
        blob_count: BigInt::from(reference.blob_count),
    }
}

pub struct PublishOciTask {
    pub request: PortableV2OciPublishFacadeRequest,
    pub cancellation: CancellationToken,
}

impl Task for PublishOciTask {
    type Output = std::result::Result<graphforge_api::PortableV2OciReference, PortableV2Error>;
    type JsValue = PortableOciReferenceOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok(graphforge_api::publish_portable_v2_oci(
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
    pub request: PortableV2OciPullFacadeRequest,
    pub cancellation: CancellationToken,
}

impl Task for PullOciTask {
    type Output = std::result::Result<graphforge_api::PortableV2OciPullReceipt, PortableV2Error>;
    type JsValue = PortableOciPullOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok(graphforge_api::pull_portable_v2_oci(
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
        request: PortableV2ExportRequest {
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
        request: PortableV2ImportRequest {
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
        request: PortableV2OciPublishFacadeRequest {
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
        request: PortableV2OciPullFacadeRequest {
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
