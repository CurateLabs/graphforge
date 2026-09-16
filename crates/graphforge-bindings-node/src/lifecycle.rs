//! Lifecycle bindings and native task ownership.

use crate::AbortSignal;
use crate::Arc;
use crate::AsyncTask;
use crate::BTreeMap;
use crate::BigInt;
use crate::Buffer;
use crate::CommittedGenerationIdentity;
use crate::Env;
use crate::GenerationDiffDisposition;
use crate::GenerationDiffLimits;
use crate::GenerationDiffRequest;
use crate::GenerationGraphDiff;
use crate::GfError;
use crate::GraphChangeStream;
use crate::GraphForge;
use crate::OperationId;
use crate::ReloadRequiredReason;
use crate::Result;
use crate::RwLock;
use crate::Task;
use crate::WriteContext;
use crate::adjacency_inspection_to_json;
use crate::canonical_operation_id;
use crate::import_session;
use crate::napi;
use crate::napi_validation;
use crate::node_usize;
use crate::optional_uuid;
use crate::parse_capability_id;
use crate::portable;
use crate::result_to_ipc;
use crate::to_napi_deferred_err;
use crate::to_napi_err;
use crate::transaction;

/// Thin Node request for atomic capability initialization.
#[napi(object)]
pub struct EnableCapabilityInput {
    /// Required idempotency UUID.
    pub operation_uuid: String,
    /// Registered lowercase capability ID.
    pub capability_id: String,
    /// Requested capability contract version.
    pub capability_version: u32,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for durable checkpoint creation.
#[napi(object)]
pub struct CheckpointInput {
    /// Canonical checkpoint name.
    pub name: String,
    /// Optional bounded description.
    pub description: Option<String>,
    /// Required idempotency UUID.
    pub idempotency_key: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for deterministic checkpoint listing.
#[napi(object, object_to_js = false)]
pub struct ListCheckpointsInput {
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque cursor returned in Arrow schema metadata.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for durable checkpoint deletion.
#[napi(object)]
pub struct DeleteCheckpointInput {
    /// Exact active checkpoint name.
    pub name: String,
    /// Required idempotency UUID.
    pub idempotency_key: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for complete-workspace checkpoint restoration.
#[napi(object)]
pub struct RevertCheckpointInput {
    /// Exact active checkpoint name.
    pub name: String,
    /// Required non-empty restoration reason.
    pub reason: String,
    /// Required idempotency UUID.
    pub idempotency_key: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for deterministic checkpoint diffing.
#[napi(object, object_to_js = false)]
pub struct DiffCheckpointsInput {
    /// Checkpoint name or the reserved selector `current`.
    pub from: String,
    /// Checkpoint name or the reserved selector `current`.
    pub to: String,
    /// `summary`, `graph`, `ontology`, `configuration`, `capabilities`,
    /// `provenance`, `knowledge`, `epistemic`, or `all`.
    pub scope: String,
    /// `summary` or `records`.
    pub detail: String,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque cursor returned in Arrow schema metadata.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Exact binary identity of one immutable committed generation.
#[napi(object)]
pub struct CommittedGenerationIdentityOutput {
    /// Raw 16-byte UUID, never a JavaScript number or reconstructed string.
    pub generation_uuid: Buffer,
    /// Raw 32-byte SHA-256 of the canonical generation manifest.
    pub manifest_sha256: Buffer,
}

/// Thin input carrying one exact binary committed-generation identity.
#[napi(object)]
pub struct CommittedGenerationIdentityInput {
    /// Raw 16-byte UUID.
    pub generation_uuid: Buffer,
    /// Raw 32-byte manifest SHA-256.
    pub manifest_sha256: Buffer,
}

/// Bounded Rust semantic-generation diff request.
#[napi(object, object_to_js = false)]
pub struct GenerationDiffInput {
    /// Exact earlier generation.
    pub source: CommittedGenerationIdentityInput,
    /// Exact later generation.
    pub target: CommittedGenerationIdentityInput,
    /// Unsigned 64-bit record budget; defaults to 1,000,000.
    pub max_records_per_generation: Option<BigInt>,
    /// Unsigned 64-bit combined IPC byte budget; defaults to 256 MiB.
    pub max_output_bytes: Option<BigInt>,
    /// Optional standard AbortSignal mapped to Rust cancellation.
    pub signal: Option<AbortSignal>,
}

/// One unchanged Rust-owned Arrow IPC change stream.
#[napi(object)]
pub struct GraphChangeStreamOutput {
    /// Exact unsigned row count.
    pub row_count: BigInt,
    /// Complete Arrow IPC stream bytes.
    pub ipc: Buffer,
}

/// Changed-property names keyed by an exact binary record UUID.
#[napi(object)]
pub struct ModifiedPropertiesOutput {
    /// Raw 16-byte node or edge UUID.
    pub record_uuid: Buffer,
    /// Canonically ordered changed-property names.
    pub names: Vec<String>,
}

/// Ready all-stream result or typed reload-required disposition.
#[napi(object)]
pub struct GenerationDiffOutput {
    /// `ready` or `reload_required`.
    pub kind: String,
    /// Stable reload reason; absent for a ready result.
    pub reason: Option<String>,
    /// Exact source identity; absent for reload-required.
    pub source: Option<CommittedGenerationIdentityOutput>,
    /// Exact target identity; absent for reload-required.
    pub target: Option<CommittedGenerationIdentityOutput>,
    /// Complete target-state added-node IPC stream.
    pub added_nodes: Option<GraphChangeStreamOutput>,
    /// Removed-node identity IPC stream.
    pub removed_nodes: Option<GraphChangeStreamOutput>,
    /// Complete target-state modified-node IPC stream.
    pub modified_nodes: Option<GraphChangeStreamOutput>,
    /// Complete target-state added-edge IPC stream.
    pub added_edges: Option<GraphChangeStreamOutput>,
    /// Removed-edge identity IPC stream.
    pub removed_edges: Option<GraphChangeStreamOutput>,
    /// Complete target-state modified-edge IPC stream.
    pub modified_edges: Option<GraphChangeStreamOutput>,
    /// Canonical changed-property names for modified nodes.
    pub modified_node_properties: Option<Vec<ModifiedPropertiesOutput>>,
    /// Canonical changed-property names for modified edges.
    pub modified_edge_properties: Option<Vec<ModifiedPropertiesOutput>>,
    /// Raw 32-byte checkpoint binding; absent for reload-required.
    pub checkpoint_binding: Option<Buffer>,
}

/// Thin Node provenance-history page and filter request.
#[napi(object, object_to_js = false)]
pub struct ProvenanceHistoryInput {
    /// Optional referenced graph/knowledge UUID.
    pub subject_uuid: Option<String>,
    /// Optional operation/idempotency UUID.
    pub operation_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque cursor returned in Arrow schema metadata.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

pub(super) fn node_page(
    limit: Option<u32>,
    after: Option<&str>,
    signal: Option<AbortSignal>,
) -> Result<graphforge_api::PageRequest> {
    let after = after
        .map(graphforge_api::PageToken::parse)
        .transpose()
        .map_err(|error| to_napi_err(&error))?;
    let cancellation = graphforge_api::CancellationToken::new();
    if let Some(signal) = &signal {
        let cancellation = cancellation.clone();
        signal.on_abort(move || cancellation.cancel());
    }
    Ok(graphforge_api::PageRequest {
        limit: limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
        after,
        cancellation: Some(cancellation),
    })
}

fn node_generation_identity(
    input: CommittedGenerationIdentityInput,
) -> Result<CommittedGenerationIdentity> {
    let generation_uuid = uuid::Uuid::from_slice(input.generation_uuid.as_ref()).map_err(|_| {
        to_napi_err(&GfError::Validation(
            "generationUuid must contain exactly 16 bytes".into(),
        ))
    })?;
    let manifest_sha256 = input.manifest_sha256.as_ref().try_into().map_err(|_| {
        to_napi_err(&GfError::Validation(
            "manifestSha256 must contain exactly 32 bytes".into(),
        ))
    })?;
    Ok(CommittedGenerationIdentity {
        generation_uuid,
        manifest_sha256,
    })
}

fn node_generation_identity_output(
    identity: CommittedGenerationIdentity,
) -> CommittedGenerationIdentityOutput {
    CommittedGenerationIdentityOutput {
        generation_uuid: Buffer::from(identity.generation_uuid.as_bytes().to_vec()),
        manifest_sha256: Buffer::from(identity.manifest_sha256.to_vec()),
    }
}

fn node_change_stream(stream: GraphChangeStream) -> GraphChangeStreamOutput {
    GraphChangeStreamOutput {
        row_count: BigInt::from(u64::try_from(stream.row_count).unwrap_or(u64::MAX)),
        ipc: Buffer::from(stream.ipc),
    }
}

fn node_modified_properties(
    properties: BTreeMap<uuid::Uuid, Vec<String>>,
) -> Vec<ModifiedPropertiesOutput> {
    properties
        .into_iter()
        .map(|(uuid, names)| ModifiedPropertiesOutput {
            record_uuid: Buffer::from(uuid.as_bytes().to_vec()),
            names,
        })
        .collect()
}

fn node_reload_reason(reason: ReloadRequiredReason) -> &'static str {
    match reason {
        ReloadRequiredReason::GenerationUnavailable => "generation_unavailable",
        ReloadRequiredReason::IdentityMismatch => "identity_mismatch",
        ReloadRequiredReason::CorruptGeneration => "corrupt_generation",
        ReloadRequiredReason::IncompatibleGraph => "incompatible_graph",
        ReloadRequiredReason::ResourceLimit => "resource_limit",
    }
}

fn node_generation_diff_output(disposition: GenerationDiffDisposition) -> GenerationDiffOutput {
    let GenerationDiffDisposition::Ready(diff) = disposition else {
        let GenerationDiffDisposition::ReloadRequired(reason) = disposition else {
            unreachable!()
        };
        return GenerationDiffOutput {
            kind: "reload_required".into(),
            reason: Some(node_reload_reason(reason).into()),
            source: None,
            target: None,
            added_nodes: None,
            removed_nodes: None,
            modified_nodes: None,
            added_edges: None,
            removed_edges: None,
            modified_edges: None,
            modified_node_properties: None,
            modified_edge_properties: None,
            checkpoint_binding: None,
        };
    };
    let GenerationGraphDiff {
        source,
        target,
        added_nodes,
        removed_nodes,
        modified_nodes,
        added_edges,
        removed_edges,
        modified_edges,
        modified_node_properties,
        modified_edge_properties,
        checkpoint_binding,
    } = *diff;
    GenerationDiffOutput {
        kind: "ready".into(),
        reason: None,
        source: Some(node_generation_identity_output(source)),
        target: Some(node_generation_identity_output(target)),
        added_nodes: Some(node_change_stream(added_nodes)),
        removed_nodes: Some(node_change_stream(removed_nodes)),
        modified_nodes: Some(node_change_stream(modified_nodes)),
        added_edges: Some(node_change_stream(added_edges)),
        removed_edges: Some(node_change_stream(removed_edges)),
        modified_edges: Some(node_change_stream(modified_edges)),
        modified_node_properties: Some(node_modified_properties(modified_node_properties)),
        modified_edge_properties: Some(node_modified_properties(modified_edge_properties)),
        checkpoint_binding: Some(Buffer::from(checkpoint_binding.to_vec())),
    }
}

fn checkpoint_selector(value: String) -> graphforge_api::CheckpointSelector {
    if value == "current" {
        graphforge_api::CheckpointSelector::Current
    } else {
        graphforge_api::CheckpointSelector::Named(value)
    }
}

fn checkpoint_diff_scope(value: &str) -> Result<graphforge_api::CheckpointDiffScope> {
    match value {
        "summary" => Ok(graphforge_api::CheckpointDiffScope::Summary),
        "graph" => Ok(graphforge_api::CheckpointDiffScope::Graph),
        "ontology" => Ok(graphforge_api::CheckpointDiffScope::Ontology),
        "configuration" => Ok(graphforge_api::CheckpointDiffScope::Configuration),
        "capabilities" => Ok(graphforge_api::CheckpointDiffScope::Capabilities),
        "provenance" => Ok(graphforge_api::CheckpointDiffScope::Provenance),
        "knowledge" => Ok(graphforge_api::CheckpointDiffScope::Knowledge),
        "epistemic" => Ok(graphforge_api::CheckpointDiffScope::Epistemic),
        "all" => Ok(graphforge_api::CheckpointDiffScope::All),
        _ => Err(napi_validation("unknown checkpoint diff scope")),
    }
}

fn checkpoint_diff_detail(value: &str) -> Result<graphforge_api::CheckpointDiffDetail> {
    match value {
        "summary" => Ok(graphforge_api::CheckpointDiffDetail::Summary),
        "records" => Ok(graphforge_api::CheckpointDiffDetail::Records),
        _ => Err(napi_validation("unknown checkpoint diff detail")),
    }
}

/// Immutable, lease-pinned view of one named checkpoint.
///
/// The class deliberately exposes only Rust-owned read operations; it has no
/// binding-side mutation or history implementation.
#[napi]
pub struct CheckpointView {
    inner: Arc<RwLock<graphforge_api::CheckpointView>>,
}

#[napi]
impl CheckpointView {
    /// Stable checkpoint UUID.
    #[napi(getter)]
    pub fn checkpoint_uuid(&self) -> Result<String> {
        let view = self
            .inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("CheckpointView lock poisoned".into())))?;
        Ok(view.checkpoint_uuid().to_string())
    }

    /// Pinned generation UUID.
    #[napi(getter)]
    pub fn generation_uuid(&self) -> Result<String> {
        let view = self
            .inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("CheckpointView lock poisoned".into())))?;
        Ok(view.generation_uuid().to_string())
    }

    /// Execute read-only Cypher against the pinned generation as Arrow IPC.
    #[napi]
    pub fn execute(&self, cypher: String) -> Result<Buffer> {
        let view = self
            .inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("CheckpointView lock poisoned".into())))?;
        let result = view.execute(&cypher).map_err(|error| to_napi_err(&error))?;
        result_to_ipc(&result)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect the pinned capability manifest as Arrow IPC.
    #[napi]
    pub fn project_capabilities(&self) -> Result<Buffer> {
        let view = self
            .inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("CheckpointView lock poisoned".into())))?;
        let result = view
            .project_capabilities()
            .map_err(|error| to_napi_err(&error))?;
        result_to_ipc(&result)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect adjacency freshness from the pinned generation.
    #[napi]
    pub fn inspect_adjacency(&self) -> Result<serde_json::Value> {
        let view = self
            .inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("CheckpointView lock poisoned".into())))?;
        view.inspect_adjacency()
            .map(adjacency_inspection_to_json)
            .map_err(|error| to_napi_err(&error))
    }
}

enum CheckpointOperation {
    Create(graphforge_api::CheckpointRequest),
    List(graphforge_api::ListCheckpointsRequest),
    Delete(graphforge_api::DeleteCheckpointRequest),
    Diff(graphforge_api::DiffCheckpointsRequest),
    Revert(graphforge_api::RevertCheckpointRequest),
}

/// Worker task for the bounded semantic committed-generation diff.
pub struct GenerationDiffTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: GenerationDiffRequest,
}

impl Task for GenerationDiffTask {
    type Output = std::result::Result<GenerationDiffDisposition, GfError>;
    type JsValue = GenerationDiffOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            self.engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                .diff_committed_generations(&self.request)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        output
            .map(node_generation_diff_output)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for Rust-owned checkpoint operations.
pub struct CheckpointTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    operation: CheckpointOperation,
}

impl Task for CheckpointTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let result = match &self.operation {
                CheckpointOperation::Create(request) => self
                    .engine
                    .read()
                    .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                    .checkpoint(request.clone())?,
                CheckpointOperation::List(request) => self
                    .engine
                    .read()
                    .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                    .list_checkpoints(request.clone())?,
                CheckpointOperation::Delete(request) => self
                    .engine
                    .read()
                    .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                    .delete_checkpoint(request.clone())?,
                CheckpointOperation::Diff(request) => self
                    .engine
                    .read()
                    .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                    .diff_checkpoints(request.clone())?,
                CheckpointOperation::Revert(request) => self
                    .engine
                    .write()
                    .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                    .revert_to_checkpoint(request.clone())?,
            };
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for manifest-only capability inspection.
pub struct ProjectCapabilitiesTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
}

impl Task for ProjectCapabilitiesTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result = graph.project_capabilities()?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for atomic capability initialization.
pub struct EnableCapabilityTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::EnableCapabilityRequest,
}

impl Task for EnableCapabilityTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result = graph.enable_capability(self.request.clone())?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact provenance event.
pub struct ProvenanceEventTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    provenance_uuid: OperationId,
    cancellation: graphforge_api::CancellationToken,
}

impl Task for ProvenanceEventTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result =
                graph.provenance_event(self.provenance_uuid.0, Some(self.cancellation.clone()))?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one deterministic provenance-history page.
pub struct ProvenanceHistoryTask {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    request: graphforge_api::ProvenanceHistoryRequest,
}

impl Task for ProvenanceHistoryTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result = graph.list_provenance_history(self.request.clone())?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

#[napi]
impl GraphForge {
    /// Inspect the committed project capability manifest as Arrow IPC.
    #[napi]
    pub fn project_capabilities(&self) -> Result<AsyncTask<ProjectCapabilitiesTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(ProjectCapabilitiesTask {
            engine: Arc::clone(&self.inner),
        }))
    }

    /// Create a durable named checkpoint and return its Arrow receipt.
    #[napi]
    pub fn checkpoint(&self, request: CheckpointInput) -> Result<AsyncTask<CheckpointTask>> {
        self.ensure_open()?;
        let actor_uuid = optional_uuid(request.actor_uuid.as_deref())?;
        Ok(AsyncTask::new(CheckpointTask {
            engine: Arc::clone(&self.inner),
            operation: CheckpointOperation::Create(graphforge_api::CheckpointRequest {
                name: request.name,
                description: request.description,
                idempotency_key: canonical_operation_id(&request.idempotency_key)?,
                actor_uuid,
            }),
        }))
    }

    /// List active checkpoints in canonical order as Arrow IPC.
    #[napi]
    pub fn list_checkpoints(
        &self,
        request: Option<ListCheckpointsInput>,
    ) -> Result<AsyncTask<CheckpointTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListCheckpointsInput {
            limit: None,
            after: None,
            signal: None,
        });
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(CheckpointTask {
            engine: Arc::clone(&self.inner),
            operation: CheckpointOperation::List(graphforge_api::ListCheckpointsRequest { page }),
        }))
    }

    /// Open an immutable view pinned to one named checkpoint.
    #[napi]
    pub fn open_checkpoint(&self, name: String) -> Result<CheckpointView> {
        let graph = self.open_guard()?;
        graph
            .open_checkpoint(&name)
            .map(|inner| CheckpointView {
                inner: Arc::new(RwLock::new(inner)),
            })
            .map_err(|error| to_napi_err(&error))
    }

    /// Delete an active checkpoint reference and return its Arrow receipt.
    #[napi]
    pub fn delete_checkpoint(
        &self,
        request: DeleteCheckpointInput,
    ) -> Result<AsyncTask<CheckpointTask>> {
        self.ensure_open()?;
        let actor_uuid = optional_uuid(request.actor_uuid.as_deref())?;
        Ok(AsyncTask::new(CheckpointTask {
            engine: Arc::clone(&self.inner),
            operation: CheckpointOperation::Delete(graphforge_api::DeleteCheckpointRequest {
                name: request.name,
                idempotency_key: canonical_operation_id(&request.idempotency_key)?,
                actor_uuid,
            }),
        }))
    }

    /// Return the exact binary identity of the selected committed generation.
    #[napi]
    pub fn committed_generation_identity(&self) -> Result<CommittedGenerationIdentityOutput> {
        self.open_guard()?
            .committed_generation_identity()
            .map(node_generation_identity_output)
            .map_err(|error| to_napi_err(&error))
    }

    /// Return Rust-owned semantic Arrow IPC changes between two generations.
    #[napi]
    pub fn diff_committed_generations(
        &self,
        request: GenerationDiffInput,
    ) -> Result<AsyncTask<GenerationDiffTask>> {
        self.ensure_open()?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &request.signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        let request = GenerationDiffRequest {
            source: node_generation_identity(request.source)?,
            target: node_generation_identity(request.target)?,
            limits: GenerationDiffLimits {
                max_records_per_generation: node_usize(
                    request.max_records_per_generation,
                    GenerationDiffLimits::default().max_records_per_generation,
                    "maxRecordsPerGeneration",
                )?,
                max_output_bytes: node_usize(
                    request.max_output_bytes,
                    GenerationDiffLimits::default().max_output_bytes,
                    "maxOutputBytes",
                )?,
            },
            cancellation: Some(cancellation),
        };
        Ok(AsyncTask::new(GenerationDiffTask {
            engine: Arc::clone(&self.inner),
            request,
        }))
    }

    /// Preview one content-free portable-v2 component selection.
    #[napi]
    pub fn preview_portable_v2_selection(
        &self,
        request: portable::PortableSelectionPreviewInput,
    ) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        portable::preview_selection(&graph, request)
    }

    /// Preview one content-free portable-v2 graph-data subset.
    #[napi]
    pub fn preview_portable_v2_graph_subset(
        &self,
        request: portable::PortableSubsetPreviewInput,
    ) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        portable::preview_subset(&graph, request)
    }

    /// Export one pinned generation as an expanded or bundled portable-v2 package.
    #[napi]
    pub fn export_portable_v2(
        &self,
        request: portable::PortableExportInput,
    ) -> Result<AsyncTask<portable::ExportPortableTask>> {
        self.ensure_open()?;
        portable::build_export_task(Arc::clone(&self.inner), request)
    }

    /// Verify portable-v2 content without opening or mutating a project.
    #[napi]
    pub fn verify_portable_v2(
        request: portable::PortableVerifyInput,
    ) -> Result<AsyncTask<portable::VerifyPortableTask>> {
        portable::build_verify_task(request)
    }

    /// Verify and atomically import a complete portable-v2 package.
    #[napi]
    pub fn import_portable_v2(
        request: portable::PortableImportInput,
    ) -> Result<AsyncTask<portable::ImportPortableTask>> {
        portable::build_import_task(request)
    }

    /// Publish a verified portable-v2 package to an OCI Distribution registry.
    #[napi]
    pub fn publish_portable_v2_oci(
        request: portable::PortableOciPublishInput,
    ) -> Result<AsyncTask<portable::PublishOciTask>> {
        portable::build_publish_task(request)
    }

    /// Pull and verify a portable-v2 package from an OCI Distribution registry.
    #[napi]
    pub fn pull_portable_v2_oci(
        request: portable::PortableOciPullInput,
    ) -> Result<AsyncTask<portable::PullOciTask>> {
        portable::build_pull_task(request)
    }

    /// Begin a durable staged import session.
    #[napi]
    pub fn begin_import_session(
        &self,
        operation_uuid: String,
        limits: Option<import_session::ImportSessionLimitsInput>,
    ) -> Result<import_session::GraphImportSession> {
        import_session::begin_import_session(
            Arc::clone(&self.inner),
            Arc::clone(&self.closed),
            operation_uuid,
            limits,
        )
    }

    /// Resume one durable, non-terminal import session.
    #[napi]
    pub fn resume_import_session(
        &self,
        session_uuid: String,
    ) -> Result<import_session::GraphImportSession> {
        import_session::resume_import_session(
            Arc::clone(&self.inner),
            Arc::clone(&self.closed),
            session_uuid,
        )
    }

    /// Abort and remove non-terminal sessions older than `maxAgeSecs`.
    #[napi]
    pub fn cleanup_stale_import_sessions(&self, max_age_secs: BigInt) -> Result<BigInt> {
        let graph = self.open_guard()?;
        import_session::cleanup_stale_import_sessions(&graph, max_age_secs)
    }

    /// Compare two checkpoint/current endpoints through the Rust diff engine.
    #[napi]
    pub fn diff_checkpoints(
        &self,
        request: DiffCheckpointsInput,
    ) -> Result<AsyncTask<CheckpointTask>> {
        self.ensure_open()?;
        let scope = checkpoint_diff_scope(&request.scope)?;
        let detail = checkpoint_diff_detail(&request.detail)?;
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(CheckpointTask {
            engine: Arc::clone(&self.inner),
            operation: CheckpointOperation::Diff(graphforge_api::DiffCheckpointsRequest {
                from: checkpoint_selector(request.from),
                to: checkpoint_selector(request.to),
                scope,
                detail,
                page,
            }),
        }))
    }

    /// Restore a checkpoint as a new committed generation and return its receipt.
    #[napi]
    pub fn revert_to_checkpoint(
        &self,
        request: RevertCheckpointInput,
    ) -> Result<AsyncTask<CheckpointTask>> {
        self.ensure_open()?;
        let actor_uuid = optional_uuid(request.actor_uuid.as_deref())?;
        Ok(AsyncTask::new(CheckpointTask {
            engine: Arc::clone(&self.inner),
            operation: CheckpointOperation::Revert(graphforge_api::RevertCheckpointRequest {
                name: request.name,
                reason: request.reason,
                idempotency_key: canonical_operation_id(&request.idempotency_key)?,
                actor_uuid,
            }),
        }))
    }

    /// Atomically enable one registered project capability.
    #[napi]
    pub fn enable_capability(
        &self,
        request: EnableCapabilityInput,
    ) -> Result<AsyncTask<EnableCapabilityTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let capability_id = parse_capability_id(&request.capability_id)?;
        Ok(AsyncTask::new(EnableCapabilityTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                capability_id,
                capability_version: request.capability_version,
            },
        }))
    }

    /// Return one exact provenance event as an Arrow IPC stream.
    #[napi]
    pub fn provenance_event(
        &self,
        provenance_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<ProvenanceEventTask>> {
        self.ensure_open()?;
        let provenance_uuid = canonical_operation_id(&provenance_uuid)?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ProvenanceEventTask {
            engine: Arc::clone(&self.inner),
            provenance_uuid,
            cancellation,
        }))
    }

    /// Return one deterministic provenance-history page as Arrow IPC.
    #[napi]
    pub fn list_provenance_history(
        &self,
        request: Option<ProvenanceHistoryInput>,
    ) -> Result<AsyncTask<ProvenanceHistoryTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ProvenanceHistoryInput {
            subject_uuid: None,
            operation_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let subject_uuid = request
            .subject_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let operation_uuid = request
            .operation_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?;
        let after = request
            .after
            .as_deref()
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = graphforge_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ProvenanceHistoryTask {
            engine: Arc::clone(&self.inner),
            request: graphforge_api::ProvenanceHistoryRequest {
                subject_uuid,
                operation_uuid,
                page: graphforge_api::PageRequest {
                    limit: request.limit.unwrap_or(graphforge_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Begin an explicit multi-mutation transaction (Rust-owned lifecycle).
    #[napi]
    pub fn begin_transaction(
        &self,
        operation_uuid: String,
        actor_uuid: Option<String>,
    ) -> Result<transaction::GraphTransaction> {
        self.ensure_open()?;
        transaction::begin_transaction(Arc::clone(&self.inner), operation_uuid, actor_uuid)
    }

    /// Safe recovery-on-open evidence for this instance.
    #[napi]
    pub fn project_open_recovery(&self) -> Result<transaction::ProjectOpenRecoveryOutput> {
        let graph = self.open_guard()?;
        Ok(transaction::recovery_output(graph.project_open_recovery()))
    }

    /// Inspect verified generation reachability for retention/GC planning.
    #[napi]
    pub fn inspect_project_reachability(
        &self,
        request: Option<transaction::ProjectRetentionInput>,
    ) -> Result<transaction::ProjectReachabilityReportOutput> {
        let graph = self.open_guard()?;
        transaction::inspect_reachability(&graph, request)
    }

    /// Preview retention/GC candidates without removing anything.
    #[napi]
    pub fn preview_project_cleanup(
        &self,
        request: Option<transaction::ProjectRetentionInput>,
    ) -> Result<transaction::ProjectCleanupReportOutput> {
        let graph = self.open_guard()?;
        transaction::preview_cleanup(&graph, request)
    }

    /// Execute retention/GC for unreachable generations.
    #[napi]
    pub fn execute_project_cleanup(
        &self,
        request: Option<transaction::ProjectRetentionInput>,
    ) -> Result<transaction::ProjectCleanupReportOutput> {
        let graph = self.open_write_guard()?;
        transaction::execute_cleanup(&graph, request)
    }

    /// Report whether CURRENT's verified delta chain should compact.
    #[napi]
    pub fn graph_delta_compaction_status(
        &self,
        request: Option<transaction::GraphDeltaCompactionStatusInput>,
    ) -> Result<transaction::GraphDeltaCompactionStatusOutput> {
        let graph = self.open_guard()?;
        transaction::compaction_status(&graph, request)
    }

    /// Preview delta compaction without publishing CURRENT.
    #[napi]
    pub fn preview_graph_delta_compaction(
        &self,
        request: transaction::GraphDeltaCompactionInput,
    ) -> Result<transaction::GraphDeltaCompactionReportOutput> {
        let graph = self.open_guard()?;
        transaction::preview_compaction(&graph, request, None)
    }

    /// Compact a contiguous verified delta prefix into a new Parquet generation.
    #[napi]
    pub fn compact_graph_delta(
        &self,
        request: transaction::GraphDeltaCompactionInput,
    ) -> Result<transaction::GraphDeltaCompactionReportOutput> {
        let mut graph = self.open_write_guard()?;
        transaction::compact(&mut graph, request, None)
    }

    /// Remove all nodes and edges (in-memory instances only).
    #[napi]
    pub fn clear(&self) -> Result<()> {
        let g = self.open_guard()?;
        g.clear().map_err(|e| to_napi_err(&e))
    }
}

#[cfg(test)]
mod tests;
