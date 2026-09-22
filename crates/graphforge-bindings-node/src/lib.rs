//! GraphForge Node.js bindings via napi-rs.
//!
//! Domain modules own conversions, annotated methods, and native handles/tasks.
//! The root retains facade identity, runtime ownership, and registration.
// napi-derive macros expand to unsafe FFI code; unsafe is permitted here but audited.
#![warn(unsafe_code)]
// napi-derive deserializes JS arguments into owned Rust values, so `#[napi]`
// methods take their args by value even when only borrowed in the body.
#![allow(clippy::needless_pass_by_value)]

use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use arrow::compute::concat_batches;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use graphforge_api::{
    AlgorithmEmbeddingDistance, AlgorithmEmbeddingNormalization,
    AlgorithmEmbeddingPublicationRequest, AnalyzeAlgorithm, AnalyzeOptions, AssertionGraphRefInput,
    AssertionGraphRole, AttachResolvedRunRequest, BeliefProjectionPolicyV1, BeliefSubjectV1,
    BulkEdgePublicationError, BulkNodePublicationError, CallerEmbeddingBatchRequest,
    CallerEmbeddingBatchRow, CallerEmbeddingDistance, CallerEmbeddingNormalization, CapabilityId,
    CommittedGenerationIdentity, EmbeddingAnalyzeOptions, EmbeddingOptions,
    EmbeddingRefreshFailureClass, EmbeddingRefreshInspection, EmbeddingRefreshOutcomeStatus,
    EmbeddingRefreshProjectPolicy, EmbeddingRefreshSpacePolicy, EmbeddingRefreshWorkerState,
    EmbeddingSpaceFreshnessInspection, EmbeddingSpaceFreshnessState, EmbeddingSpaceInfo,
    EmbeddingSpaceProducer, EmbeddingSpaceReadDecision, EmbeddingTokenCountClass, ExecutionResult,
    FastRpOptions, FindDiagnostic, FindExecutionOptions, FindOptions, FindRerankOptions,
    GenerationDiffDisposition, GenerationDiffLimits, GenerationDiffRequest, GenerationGraphDiff,
    GfError, GraphChangeStream, GraphForgeOptions, GraphObjectKind, GraphSageAggregator,
    GraphSageOptions, HashGnnOptions, InvocationDescriptor, InvocationError, IrLiteral,
    Node2VecOptions, NodeSelector, OpenRouterProviderSession, OpenRouterProviderSessionConfig,
    OpenRouterWireLimits, OperationId, PathsOptions, ProjectWriteMode, PropValue,
    ProviderBatchLimits, ProviderCapabilities, ProviderCapability, ProviderEmbeddingDistance,
    ProviderEmbeddingNormalization, ProviderEmbeddingPlanInspection, ProviderEmbeddingPlanRequest,
    ProviderExecutionLimits, ProviderRequestLimits, ReloadRequiredReason, RerankAdvisoryPolicy,
    RerankFailurePolicy, ResolveBeliefProjectionRequest, ResolveBeliefSubjectRequest,
    ResolvedAttachmentOutcome, ResolvedBeliefProjection, ResolvedBeliefSubject,
    ResolvedRecordedAlgorithmRequest, SearchIndexOptions, SimilarOptions, StatuslessPolicyV1,
    SupersessionBranchPolicyV1, TextIndexInspection, TokenCountClass, WriteContext,
    algorithm_descriptor_contracts, validate_embedding_options,
};
use napi::bindgen_prelude::{
    AbortSignal, AsyncTask, BigInt, Buffer, ClassInstance, Either3, FromNapiValue, Function,
    JsObjectValue, Object, Unknown,
};
use napi::{Env, JsValue, Task, ValueType};
use napi_derive::napi;

mod composite;
mod error;
mod import_session;
mod multi_ontology;
mod portable;
mod telemetry;
mod transaction;
use composite::CompositeTransactionInput;
use error::{NodeError, to_napi_err, type_error};

/// Result alias whose error surfaces a typed JS `error.code` (see [`error`]).
pub(crate) type Result<T> = std::result::Result<T, NodeError>;

pub(crate) fn napi_validation(message: &'static str) -> NodeError {
    to_napi_err(&GfError::Validation(message.into()))
}

fn project_write_mode(value: &str) -> Result<ProjectWriteMode> {
    match value {
        "single_writer" => Ok(ProjectWriteMode::SingleWriter),
        "queued_writer" => Ok(ProjectWriteMode::QueuedWriter),
        "optimistic_multi_writer" => Ok(ProjectWriteMode::OptimisticMultiWriter),
        _ => Err(napi_validation(
            "writeMode must be single_writer, queued_writer, or optimistic_multi_writer",
        )),
    }
}

#[napi(object)]
/// Embedded project-write construction options.
pub struct GraphForgeOptionsInput {
    /// Write coordination policy name.
    pub write_mode: Option<String>,
    /// Maximum number of queued same-instance writers.
    pub write_queue_capacity: Option<i32>,
    /// Maximum optimistic rebase attempts after initial staging.
    pub max_rebase_attempts: Option<i32>,
}

fn to_napi_invocation_err(error: &InvocationError) -> NodeError {
    match error {
        InvocationError::Graph(error) => to_napi_err(error),
        _ => napi::Error::new(error.code().to_owned(), error.to_string()),
    }
}

fn bulk_node_publication_error(error: BulkNodePublicationError) -> NodeError {
    match error {
        BulkNodePublicationError::Validation(error) => {
            to_napi_err(&GfError::Validation(error.to_string()))
        }
        BulkNodePublicationError::Publication(error) => to_napi_err(&error),
    }
}

fn bulk_edge_publication_error(error: BulkEdgePublicationError) -> NodeError {
    match error {
        BulkEdgePublicationError::Validation(error) => {
            to_napi_err(&GfError::Validation(error.to_string()))
        }
        BulkEdgePublicationError::Publication(error) => to_napi_err(&error),
    }
}

pub(crate) fn canonical_operation_id(value: &str) -> Result<OperationId> {
    if value.len() != 36 {
        return Err(to_napi_err(&GfError::Validation(format!(
            "invalid UUID {value:?}"
        ))));
    }
    let NodeSelector::Uuid(uuid) =
        NodeSelector::uuid(value).map_err(|error| to_napi_err(&error))?
    else {
        unreachable!("UUID parser always constructs a UUID selector")
    };
    if uuid.hyphenated().to_string() != value {
        return Err(to_napi_err(&GfError::Validation(format!(
            "invalid UUID {value:?}"
        ))));
    }
    Ok(OperationId(uuid))
}

pub(crate) fn optional_uuid(value: Option<&str>) -> Result<Option<uuid::Uuid>> {
    value
        .map(canonical_operation_id)
        .transpose()
        .map(|value| value.map(|id| id.0))
}

pub(crate) fn node_u64(value: Option<BigInt>, name: &str) -> Result<u64> {
    let Some(value) = value else {
        return Err(to_napi_err(&GfError::Validation(format!(
            "{name} is required"
        ))));
    };
    let (negative, value, lossless) = value.get_u64();
    if negative || !lossless {
        return Err(to_napi_err(&GfError::Validation(format!(
            "{name} must be a lossless unsigned 64-bit integer"
        ))));
    }
    Ok(value)
}

pub(crate) fn node_usize(value: Option<BigInt>, default: usize, name: &str) -> Result<usize> {
    let Some(value) = value else {
        return Ok(default);
    };
    let (negative, value, lossless) = value.get_u64();
    if negative || !lossless {
        return Err(to_napi_err(&GfError::Validation(format!(
            "{name} must be a lossless unsigned 64-bit integer"
        ))));
    }
    usize::try_from(value).map_err(|_| {
        to_napi_err(&GfError::Validation(format!(
            "{name} exceeds this runtime's addressable range"
        )))
    })
}

/// Returns the crate version.
#[napi]
#[must_use]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Captured output from one Rust-owned CLI invocation.
#[napi(object)]
pub struct CliExecutionOutput {
    /// Process-compatible exit status.
    pub exit_code: i32,
    /// Exact standard-output bytes, including binary Arrow IPC results.
    pub stdout: Buffer,
    /// Exact standard-error bytes.
    pub stderr: Buffer,
}

/// Parse and execute the native GraphForge CLI without terminating Node.js.
#[napi(js_name = "runCli")]
#[must_use]
pub fn run_cli(args: Vec<String>) -> CliExecutionOutput {
    let execution = graphforge_cli::execute(std::iter::once("gf".to_owned()).chain(args));
    CliExecutionOutput {
        exit_code: execution.exit_code,
        stdout: execution.stdout.into(),
        stderr: execution.stderr.into(),
    }
}

static WRITER_HOLD_PROBE: LazyLock<
    Mutex<Option<graphforge_api::concurrency_test_support::HeldWriter>>,
> = LazyLock::new(|| Mutex::new(None));

/// Stage and retain the project writer lock for concurrency acceptance tests.
#[napi(js_name = "testAcquireWriterHold")]
pub fn test_acquire_writer_hold(path: String) -> Result<()> {
    {
        let state = WRITER_HOLD_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.is_some() {
            return Err(to_napi_err(&GfError::Validation(
                "writer-hold probe already active".into(),
            )));
        }
    }
    let held = graphforge_api::concurrency_test_support::hold_writer(&path)
        .map_err(|error| to_napi_err(&error))?;
    let mut state = WRITER_HOLD_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.is_some() {
        return Err(to_napi_err(&GfError::Validation(
            "writer-hold probe already active".into(),
        )));
    }
    *state = Some(held);
    Ok(())
}

/// Drop the staged writer hold created by [`test_acquire_writer_hold`].
#[napi(js_name = "testReleaseWriterHold")]
pub fn test_release_writer_hold() -> Result<()> {
    let mut state = WRITER_HOLD_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.take().is_none() {
        return Err(to_napi_err(&GfError::Validation(
            "writer-hold probe is not active".into(),
        )));
    }
    Ok(())
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}

/// The GraphForge engine — a Node handle over the native Rust core
/// ([`graphforge_api::GraphForge`]).
///
/// Construct in-memory (`new GraphForge()`) or over a Parquet project directory
/// (`new GraphForge(path)`); the constructor returns a `GraphForge` instance.
/// Cypher execution, analyst verbs, explicit indexing, and text/vector/hybrid
/// search delegate to Rust and return their tabular results as Arrow IPC.
#[napi]
pub struct GraphForge {
    inner: OwnedEngine,
    provider: Option<ConfiguredProviderBinding>,
    closed: Arc<AtomicBool>,
}

/// The binding's owned reference to the native engine.
///
/// Worker tasks and deferred plans clone the inner `Arc`, but the JavaScript
/// wrapper must relinquish its own reference synchronously on `close()`. This
/// matters on Windows, where retaining the native engine also retains open
/// project-directory handles until JavaScript garbage collection runs.
struct OwnedEngine(Option<Arc<RwLock<graphforge_api::GraphForge>>>);

impl OwnedEngine {
    fn new(engine: graphforge_api::GraphForge) -> Self {
        Self(Some(Arc::new(RwLock::new(engine))))
    }

    fn close(&mut self) {
        self.0.take();
    }
}

impl Deref for OwnedEngine {
    type Target = Arc<RwLock<graphforge_api::GraphForge>>;

    fn deref(&self) -> &Self::Target {
        self.0
            .as_ref()
            .expect("lifecycle gate must reject access after close")
    }
}

impl GraphForge {
    /// Lifecycle gate mirroring the v0.5 contract: operations after `close()`
    /// raise `LifecycleError`.
    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(to_napi_err(&GfError::Lifecycle(
                "operation on a closed GraphForge instance".into(),
            )));
        }
        Ok(())
    }

    /// Check the lifecycle gate, then acquire shared engine access.
    fn open_guard(&self) -> Result<RwLockReadGuard<'_, graphforge_api::GraphForge>> {
        self.ensure_open()?;
        self.inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))
    }

    /// Check the lifecycle gate, then acquire exclusive engine access.
    fn open_write_guard(&self) -> Result<RwLockWriteGuard<'_, graphforge_api::GraphForge>> {
        self.ensure_open()?;
        self.inner
            .write()
            .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))
    }
}

#[napi]
impl GraphForge {
    /// Open an in-memory (`path` omitted) or Parquet-backed (`path` = dir) instance.
    #[napi(constructor)]
    pub fn new(path: Option<String>, options: Option<GraphForgeOptionsInput>) -> Result<Self> {
        let defaults = GraphForgeOptions::default();
        let options = options.unwrap_or(GraphForgeOptionsInput {
            write_mode: None,
            write_queue_capacity: None,
            max_rebase_attempts: None,
        });
        let options = GraphForgeOptions {
            write_mode: match options.write_mode {
                Some(value) => project_write_mode(&value)?,
                None => defaults.write_mode,
            },
            write_queue_capacity: match options.write_queue_capacity {
                Some(value) if (1..=65_536).contains(&value) => {
                    usize::try_from(value).map_err(|_| {
                        napi_validation("writeQueueCapacity must be between 1 and 65536")
                    })?
                }
                Some(_) => {
                    return Err(napi_validation(
                        "writeQueueCapacity must be between 1 and 65536",
                    ));
                }
                None => defaults.write_queue_capacity,
            },
            max_rebase_attempts: match options.max_rebase_attempts {
                Some(value) if (0..=32).contains(&value) => u32::try_from(value)
                    .map_err(|_| napi_validation("maxRebaseAttempts must be between 0 and 32"))?,
                Some(_) => {
                    return Err(napi_validation(
                        "maxRebaseAttempts must be between 0 and 32",
                    ));
                }
                None => defaults.max_rebase_attempts,
            },
            resource: defaults.resource,
        };
        let inner = graphforge_api::GraphForge::new_with_options(path.as_deref(), options)
            .map_err(|e| to_napi_err(&e))?;
        Ok(Self {
            inner: OwnedEngine::new(inner),
            provider: None,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    // ----- Analyst verbs.

    // ----- Construction (write API) — not yet implemented (raise NotImplementedError).

    // ----- Introspection.

    /// Sorted label and relationship counts as an Arrow IPC `Buffer`.
    #[napi]
    pub fn schema(&self) -> Result<Buffer> {
        let graph = self.open_guard()?;
        let batch = graph.schema().map_err(|error| to_napi_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// The node labels present in the graph.
    #[napi]
    pub fn labels(&self) -> Result<Vec<String>> {
        let g = self.open_guard()?;
        g.labels().map_err(|e| to_napi_err(&e))
    }

    /// The relationship types present in the graph.
    #[napi]
    pub fn relationship_types(&self) -> Result<Vec<String>> {
        let g = self.open_guard()?;
        g.relationship_types().map_err(|e| to_napi_err(&e))
    }

    /// Count nodes (optionally for one `label`).
    #[napi]
    pub fn node_count(&self, label: Option<String>) -> Result<i64> {
        let g = self.open_guard()?;
        let n = g
            .node_count(label.as_deref().unwrap_or(""))
            .map_err(|e| to_napi_err(&e))?;
        Ok(i64::try_from(n).unwrap_or(i64::MAX))
    }

    /// Read optional project-level graph directedness (`directed` / `undirected`).
    #[napi]
    pub fn graph_directedness(&self) -> Result<Option<String>> {
        let graph = self.open_guard()?;
        Ok(graph
            .graph_directedness()
            .map_err(|error| to_napi_err(&error))?
            .map(|value| value.as_str().to_owned()))
    }

    /// Set or clear project-level graph directedness for GSI grading.
    #[napi]
    pub fn set_graph_directedness(
        &self,
        operation_uuid: String,
        directedness: Option<String>,
        actor_uuid: Option<String>,
    ) -> Result<()> {
        let directedness = match directedness.as_deref() {
            None => None,
            Some(value) => Some(
                graphforge_api::GraphDirectedness::parse(value)
                    .map_err(|error| to_napi_err(&error))?,
            ),
        };
        let context = WriteContext {
            operation_uuid: canonical_operation_id(&operation_uuid)?,
            actor_uuid: optional_uuid(actor_uuid.as_deref())?,
        };
        let mut graph = self.open_write_guard()?;
        graph
            .set_graph_directedness(&context, directedness)
            .map_err(|error| to_napi_err(&error))
    }

    /// Grade the live graph to a Graph Scale Index profile.
    #[napi]
    pub fn profile_gsi(&self) -> Result<GraphScaleIndexProfileOutput> {
        let graph = self.open_guard()?;
        let profile = graph.profile_gsi().map_err(|error| to_napi_err(&error))?;
        Ok(GraphScaleIndexProfileOutput {
            gsi: profile.gsi,
            directedness: profile.directedness.as_str().to_owned(),
            node_count: BigInt::from(profile.node_count),
            edge_count: BigInt::from(profile.edge_count),
            density: profile.density,
            scale_code: profile.scale_code,
            size_tag: profile.size_tag,
            density_integer: profile.density_integer,
        })
    }

    /// Close the instance; subsequent operations raise `LifecycleError`. Idempotent.
    #[napi]
    pub fn close(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.inner.close();
    }

    /// The storage path, or `null` for an in-memory instance.
    #[napi(getter)]
    pub fn path(&self) -> Result<Option<String>> {
        let g = self.open_guard()?;
        Ok(g.path().map(|p| p.display().to_string()))
    }

    /// The effective ontology mode: `"exploratory"` | `"advisory"` | `"strict"`.
    #[napi(getter)]
    pub fn ontology_mode(&self) -> Result<String> {
        let g = self.open_guard()?;
        Ok(format!("{:?}", g.ontology_mode()).to_lowercase())
    }
}

pub(crate) fn to_napi_deferred_err(env: Env, error: &GfError) -> napi::Error {
    let value = napi::JsError::from(to_napi_err(error)).into_unknown(env);
    napi::Error::from(value)
}

fn cancelled_error() -> GfError {
    GfError::Api {
        code: graphforge_api::ApiErrorCode::Cancelled,
        message: "operation was cancelled".into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;

    use super::*;

    #[test]
    fn node_engine_lock_allows_shared_reads_and_blocks_exclusive_replacement() {
        let graph = Arc::new(GraphForge::new(None, None).unwrap());

        let read_guard = graph.open_guard().unwrap();
        let reader = Arc::clone(&graph);
        let (read_tx, read_rx) = mpsc::channel();
        let read_thread = thread::spawn(move || {
            read_tx.send(reader.path().is_ok()).unwrap();
        });
        assert!(read_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        drop(read_guard);
        read_thread.join().unwrap();

        let write_guard = graph.open_write_guard().unwrap();
        let blocked_reader = Arc::clone(&graph);
        let (blocked_tx, blocked_rx) = mpsc::channel();
        let blocked_thread = thread::spawn(move || {
            blocked_tx.send(blocked_reader.path().is_ok()).unwrap();
        });
        assert_eq!(
            blocked_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        );
        drop(write_guard);
        assert!(blocked_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        blocked_thread.join().unwrap();
    }
}

mod analyst;
mod assertions;
mod construction;
mod conversions;
mod epistemic;
mod lifecycle;
mod ontology;
mod providers;
mod query;
mod recorded;
mod research_project;
mod research_versions;
mod slices;
mod source_artifact;

pub use analyst::AlgorithmDescriptorContractJs;
pub use analyst::GraphScaleIndexProfileOutput;
pub use analyst::InvocationDescriptorHandle;
use analyst::parse_algorithm_id;
use analyst::parse_seed;
use analyst::parse_terminal_uuids;
pub use assertions::AssertionGraphRefInputJs;
pub use assertions::AssertionGraphRefsInput;
pub use assertions::AssertionGraphRefsTask;
pub use assertions::AssertionStatusTask;
pub use assertions::AssertionTask;
pub use assertions::AssessConfidenceInput;
pub use assertions::AssessConfidenceTask;
pub use assertions::AttachEvidenceInput;
pub use assertions::AttachEvidenceTask;
pub use assertions::ConfidenceAssessmentTask;
pub use assertions::ConfidenceInputsInput;
pub use assertions::ConfidenceInputsTask;
pub use assertions::CreateAssertionInput;
pub use assertions::CreateAssertionTask;
pub use assertions::CreateAssertionWithEvidenceInput;
pub use assertions::CreateAssertionWithEvidenceTask;
pub use assertions::CreateAssertionWithStatusInput;
pub use assertions::CreateAssertionWithStatusTask;
pub use assertions::EvidenceInputJs;
pub use assertions::EvidenceLinkTask;
pub use assertions::ListAssertionStatusInput;
pub use assertions::ListAssertionStatusTask;
pub use assertions::ListAssertionsInput;
pub use assertions::ListAssertionsTask;
pub use assertions::ListConfidenceAssessmentsInput;
pub use assertions::ListConfidenceAssessmentsTask;
pub use assertions::ListEvidenceLinksInput;
pub use assertions::ListEvidenceLinksTask;
pub use assertions::ListReasoningInput;
pub use assertions::ListReasoningTask;
pub use assertions::ReasoningTask;
pub use assertions::RecordAssertionStatusInput;
pub use assertions::RecordAssertionStatusTask;
pub use assertions::RecordReasoningInput;
pub use assertions::RecordReasoningTask;
pub(crate) use assertions::assertion_status;
use assertions::parse_capability_id;
pub use construction::EdgeHandle;
pub use construction::NodeHandle;
use conversions::ipc_to_record_batch;
use conversions::json_to_prop_value;
use conversions::node_handle_from_unknown;
use conversions::params_from_map;
use conversions::props_from_js_object;
pub(crate) use conversions::props_from_map;
pub(crate) use conversions::record_batch_to_ipc;
use conversions::result_to_ipc;
pub use epistemic::ApplyValidTimeInput;
pub use epistemic::ApplyValidTimeTask;
pub use epistemic::BeliefProjectionPolicyInput;
pub use epistemic::BeliefSubjectPolicyInput;
pub use epistemic::CreateHypothesisGroupInput;
pub use epistemic::EpistemicSnapshotTask;
pub use epistemic::HypothesisTask;
pub use epistemic::ListAssertionSupersessionsInput;
pub use epistemic::ListAssertionSupersessionsTask;
pub use epistemic::ListAssertionValidityInput;
pub use epistemic::ListAssertionValidityTask;
pub use epistemic::ListHypothesisGroupsInput;
pub use epistemic::ListHypothesisMembershipInput;
pub use epistemic::ListHypothesisSelectionInput;
pub use epistemic::RecordAssertionValidityInput;
pub use epistemic::RecordAssertionValidityTask;
pub use epistemic::RecordHypothesisMembershipInput;
pub use epistemic::RecordHypothesisSelectionInput;
pub use epistemic::RemoveHypothesisMemberInput;
pub use epistemic::ResolveBeliefProjectionInput;
pub use epistemic::ResolveBeliefProjectionTask;
pub use epistemic::ResolveBeliefSubjectInput;
pub use epistemic::ResolveBeliefSubjectTask;
pub use epistemic::ResolvedBeliefProjectionHandle;
pub use epistemic::ResolvedBeliefSubjectOutput;
pub use epistemic::SupersedeAssertionInput;
pub use epistemic::SupersedeAssertionTask;
pub use lifecycle::CheckpointInput;
pub use lifecycle::CheckpointTask;
pub use lifecycle::CheckpointView;
pub use lifecycle::CommittedGenerationIdentityInput;
pub use lifecycle::CommittedGenerationIdentityOutput;
pub use lifecycle::DeleteCheckpointInput;
pub use lifecycle::DiffCheckpointsInput;
pub use lifecycle::EnableCapabilityInput;
pub use lifecycle::EnableCapabilityTask;
pub use lifecycle::GenerationDiffInput;
pub use lifecycle::GenerationDiffOutput;
pub use lifecycle::GenerationDiffTask;
pub use lifecycle::GraphChangeStreamOutput;
pub use lifecycle::ListCheckpointsInput;
pub use lifecycle::ModifiedPropertiesOutput;
pub use lifecycle::ProjectCapabilitiesTask;
pub use lifecycle::ProvenanceEventTask;
pub use lifecycle::ProvenanceHistoryInput;
pub use lifecycle::ProvenanceHistoryTask;
pub use lifecycle::RevertCheckpointInput;
use lifecycle::node_page;
pub use ontology::OntologySuggestionOutput;
pub use ontology::OntologyValidationDiagnosticOutput;
pub use ontology::OntologyValidationReportOutput;
pub use ontology::RuntimeCatalogEntryOutput;
pub use ontology::RuntimeCatalogSnapshotOutput;
pub use ontology::WorkspaceOntologyOutput;
pub use providers::AlgorithmEmbeddingPublicationInput;
pub use providers::CallerEmbeddingPublicationInput;
pub use providers::CallerEmbeddingRowInput;
use providers::ConfiguredProviderBinding;
use providers::EmbeddingInput;
use providers::NodeSelectorInput;
pub use providers::OpenRouterProviderConfigInput;
pub use providers::ProviderEmbeddingPlanInput;
pub use providers::ProviderRerankInput;
pub use providers::SearchIndexInput;
use providers::adjacency_inspection_to_json;
use providers::embedding_error;
use providers::embedding_options_from_input;
use providers::node_selector_from_input;
pub use query::CollectIpcTask;
pub use query::PlanHandle;
pub use recorded::AlgorithmRunEventsInput;
pub use recorded::AlgorithmRunEventsTask;
pub use recorded::AlgorithmRunTask;
pub use recorded::AttachResolvedRunInput;
pub use recorded::AttachResolvedRunTask;
pub use recorded::ListAlgorithmRunsInput;
pub use recorded::ListAlgorithmRunsTask;
pub use recorded::RecordedAlgorithmInput;
pub use recorded::RecordedAlgorithmOutput;
pub use recorded::RecordedAlgorithmTask;
pub use recorded::ResolvedRecordedAlgorithmInput;
pub use recorded::ResolvedRecordedAlgorithmOutput;
pub use recorded::ResolvedRecordedAlgorithmTask;
pub use recorded::ResolvedRecordedOutputData;
