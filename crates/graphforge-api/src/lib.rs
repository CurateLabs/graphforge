//! GraphForge public Rust API — the engine facade.
//!
//! `graphforge-api` sits at the top of the crate stack: it depends on the full pipeline
//! (`graphforge-cypher` parse → `graphforge-ir` bind → `graphforge-rel` lower → `graphforge-exec` execute) plus
//! `graphforge-storage` and `graphforge-ontology`. The [`GraphForge`] facade ties these together
//! into the single public surface callers (CLI, language bindings) use.
//!
//! This crate exists because the facade **cannot** live in `graphforge-core`: `graphforge-core`
//! is the foundation crate every other crate depends on, so depending on
//! `graphforge-exec`/`graphforge-cypher` from there would be a dependency cycle (see #583 /
//! #716). `graphforge-core` keeps the shared value types ([`GfError`], [`OntologyMode`],
//! handles, …); `graphforge-api` orchestrates them.
//!
//! # Milestone status
//!
//! - #716 — crate scaffold: [`GraphForge`] relocated here from `graphforge-core`; the
//!   pipeline-backed methods are still `NotYetImplemented` stubs.
//! - #717/#718 — read scans wired to the catalog, fixed-hop joins, property
//!   JOINs.
//! - #719 — [`GraphForge::new`]/[`GraphForge::execute`] wired into the real
//!   parse → bind → lower → execute pipeline, returning Arrow results.
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::datatypes::SchemaRef;
use graphforge_core::{GraphIdentity, TypeId};
use graphforge_ir::{
    BindError, Binder, GraphOp, GraphPlan, IrExpr, ProcedureRegistry, RuntimeCatalog,
};
use graphforge_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};
use graphforge_storage::GraphCatalog;
use graphforge_storage::ResolvedProjectGeneration;
use sha2::{Digest, Sha256};

#[cfg(test)]
mod adjacency_rebuild_barrier;
mod algorithm_embedding_publication;
mod algorithm_runs;
mod algorithm_writeback;
mod belief_projection;
mod bulk_construction;
mod canonical_arrow;
mod capabilities;
mod checkpoint_graph_diff;
mod checkpoints;
mod composite_publish;
mod composite_receipt;
#[cfg(test)]
mod composite_recovery_tests;
mod composite_transaction;
mod composite_validation;
/// Hidden writer-hold helpers for native binding concurrency probes.
#[doc(hidden)]
pub mod concurrency_test_support;
mod construction;
#[cfg(test)]
mod construction_concurrency_tests;
#[cfg(test)]
mod durability_certification_tests;
mod embedding_freshness;
mod embedding_publication;
mod embedding_refresh;
mod embedding_spaces;
mod epistemic_snapshot;
mod find_execution;
mod graph_inspection;
mod graph_snapshot;
mod gsi_profiler;
mod hypotheses;
mod invocation_descriptor;
mod knowledge;
mod maintenance;
#[cfg(test)]
mod multi_process_publication_tests;
mod node_selector;
mod ontology_lifecycle;
mod paging;
mod portable;
mod provenance;
mod provider_embedding;
mod provider_embedding_execution;
mod provider_find;
mod provider_rerank;
mod provider_session;
mod repository;
mod resource_policy;
#[cfg(test)]
mod same_process_concurrency_tests;
#[cfg(test)]
mod schema_inventory;
mod search_find;
mod search_index;
mod search_output;
#[cfg(test)]
mod shared_directory_semantics_tests;
#[cfg(test)]
mod stream_cancellation_isolation_tests;
mod transaction;
mod valid_time;
mod workspace_ontology;
mod write_modes;

pub use portable::{
    PortableExportRequest, PortableExportResult, PortableImportRequest, PortableImportResult,
    PortableSelection,
};
pub use repository::{
    GitProvenance, InfraCapabilityCompatibility, InfraNotChecked, InfraPlan, InfraStaticValidity,
    InfraValidationResult, ProjectConfig, RepositoryContext, RepositoryDefinitionDigest,
    RepositoryInitReceipt, RepositoryRemoveReceipt, RepositorySourceDigest, RepositorySyncRequest,
    RepositorySyncResult, RepositorySyncStatus, SkillBundle, SkillBundleFile, SkillMutationReceipt,
    SkillStatus, SkillStatusReceipt,
};

// Re-export the foundational types callers need alongside the facade, so a
// single `use graphforge_api::...` reaches the common surface.
pub use bulk_construction::{
    BULK_CONSTRUCTION_CONTRACT_VERSION, BulkEdgePublicationError, BulkEdgeRow, BulkInputKind,
    BulkNodePublicationError, BulkNodeRow, BulkValidationError, BulkValidationReason,
    ValidatedBulkEdges, ValidatedBulkNodes, bulk_edge_input_schema, bulk_node_input_schema,
    bulk_receipt_schema,
};
pub use epistemic_snapshot::EPISTEMIC_SNAPSHOT_POLICY_VERSION;
pub use graphforge_core::algorithms::{
    Algorithm, AlgorithmField, AlgorithmFieldType, AlgorithmResultSchema, AlgorithmVerb,
    AnalyzeAlgorithm, ClusterAlgorithm, PathAlgorithm, RankAlgorithm, SimilarAlgorithm,
};
pub use graphforge_core::embedding_options::{
    EmbeddingAnalyzeOptions, EmbeddingOptions, FastRpOptions, GraphSageAggregator,
    GraphSageOptions, HashGnnOptions, Node2VecOptions,
};
pub use graphforge_core::manifest::{MANIFEST_FILE, ONTOLOGY_FILE, ProjectManifest};
pub use graphforge_core::{
    AnalyzeOptions, ApiErrorCode, ClusterOptions, EdgeHandle, FindOptions, GfError, NodeHandle,
    NodeSelector, OntologyFormat, OntologyMode, PathsOptions, ProjectErrorCode, PropValue,
    RankOptions, SimilarOptions, Span,
};
// The Arrow-backed result of [`GraphForge::execute`].
pub use graphforge_exec::validate_embedding_options;
pub use graphforge_exec::{ExecutionResult, ExecutionStats, SendableRecordBatchStream};
pub use graphforge_storage::{
    GraphDirectedness, WorkspaceConfiguration, WorkspaceOntology, WorkspaceOntologyMode,
    WorkspaceOntologySourceFormat,
};
// Query parameter literal type (for `execute_with_params`), re-exported so the
// language bindings can build params without depending on `graphforge-ir` directly.
pub use algorithm_runs::{
    AlgorithmId, ListAlgorithmRunsRequest, RecordedAlgorithmRequest, RecordedAlgorithmResult,
};
pub use belief_projection::{
    AttachResolvedRunRequest, BELIEF_PROJECTION_POLICY_VERSION, BeliefProjectionPolicyV1,
    BeliefSubjectV1, HypothesisSelectionPolicyV1, ResolveBeliefProjectionRequest,
    ResolveBeliefSubjectRequest, ResolvedAttachmentOutcome, ResolvedBeliefProjection,
    ResolvedBeliefSubject, ResolvedRecordedAlgorithmRequest, ResolvedRecordedAlgorithmResult,
    StatuslessPolicyV1, SupersessionBranchPolicyV1,
};
pub use capabilities::{
    CapabilityId, EnableCapabilityRequest, KNOWLEDGE_API_VERSION, OperationId, WriteContext,
};
pub use checkpoints::{
    CheckpointDiffDetail, CheckpointDiffScope, CheckpointRequest, CheckpointSelector,
    CheckpointView, DeleteCheckpointRequest, DiffCheckpointsRequest, ListCheckpointsRequest,
    PreviewRevertCheckpointRequest, RevertCheckpointPreview, RevertCheckpointRequest,
    ShowCheckpointRequest,
};
pub use composite_receipt::{
    authorize_composite_transaction, composite_generation_uuid, composite_receipt_schema,
};
pub use composite_transaction::{
    COMPOSITE_KNOWLEDGE_PARTICIPANT_KINDS, COMPOSITE_TRANSACTION_CONTRACT_VERSION,
    CompositeGraphMutation, CompositeKnowledgeParticipants, CompositeTransactionRequest,
    MAX_COMPOSITE_TRANSACTION_ENTRIES,
};
pub use composite_validation::{CompositeOntologySnapshot, CompositeValidationSnapshot};
pub use embedding_freshness::{
    EmbeddingSpaceFreshnessInspection, EmbeddingSpaceFreshnessState, EmbeddingSpaceReadDecision,
};
pub use embedding_publication::{
    CallerEmbeddingBatchRequest, CallerEmbeddingBatchRow, CallerEmbeddingDistance,
    CallerEmbeddingNormalization,
};
pub use embedding_refresh::{
    EmbeddingRefreshInspection, EmbeddingRefreshWorkerInspection, EmbeddingRefreshWorkerState,
};
pub use embedding_spaces::{
    ActiveEmbeddingGenerationInfo, EmbeddingChunkingInfo, EmbeddingSpaceInfo,
    EmbeddingSpaceProducer, EmbeddingTokenCountClass, EmbeddingTokenizerInfo,
};
pub use find_execution::{
    FindDiagnostic, FindExecutionOptions, FindExecutionResult, FindRerankOptions,
};
pub use graphforge_ir::{IrLiteral, ProcedureDefinition, ProcedureField};
pub use graphforge_knowledge::{
    Assertion, AssertionGraphRef, AssertionGraphRole, AssertionStatus, AssertionStatusEvent,
    AssertionSupersession, AssertionValidityEvent, ConfidenceAssessment, ConfidenceInput,
    ConfidencePolicy, EvidenceLink, EvidenceRole, EvidenceSourceKind, GraphObjectKind,
    HypothesisGroup, HypothesisMembershipAction, HypothesisMembershipEvent,
    HypothesisSelectionEvent, KnowledgeError, ReasoningContentFormat, ReasoningKind,
    ReasoningRecord,
};
pub use graphforge_ontology::OntologyDoc;
pub use graphforge_provenance::{
    EventKind, LineageRecord, LineageRole, ProvenanceError, ProvenanceEvent, SubjectKind,
};
pub use graphforge_search::{
    CandidateReranker, DocumentEmbeddingOutput, DocumentEmbeddingProvider,
    DocumentEmbeddingRequest, OpenRouterWireLimits, ProviderBatchLimits, ProviderBatchShape,
    ProviderCapabilities, ProviderCapability, ProviderError, ProviderExecutionLimits,
    ProviderExecutionRuntime, ProviderFailureClass, ProviderModelContract,
    ProviderPublicationError, ProviderRequestLimits, ProviderResult, QueryEmbeddingProvider,
    QueryEmbeddingRequest, RerankAdvisoryPolicy, RerankFailurePolicy, RerankOmissionAdvisory,
    RerankOutput, RerankStatus, RerankWorkShape, StandardProviderExecutionRuntime,
};
pub use graphforge_search::{TextIndexFreshnessReason, TextIndexFreshnessState};
pub use graphforge_storage::adjacency::{AdjacencyFreshnessReason, AdjacencyFreshnessState};
pub use graphforge_storage::{
    ChunkingIdentity, EmbeddingRefreshFailureClass, EmbeddingRefreshOutcomeRecord,
    EmbeddingRefreshOutcomeStatus, EmbeddingRefreshProjectPolicy, EmbeddingRefreshSpacePolicy,
    ResolvedEmbeddingRefreshPolicy, SearchArtifactError, TokenCountClass, TokenizerIdentity,
};
pub use graphforge_storage::{
    GraphDeltaCompactionLimits, GraphDeltaCompactionPolicy, GraphDeltaCompactionReport,
    GraphDeltaCompactionRequest, GraphDeltaCompactionStatus, GraphDeltaJournalLimits,
    ProjectCleanupDisposition, ProjectCleanupEntry, ProjectCleanupLocation, ProjectCleanupReport,
    ProjectOpenRecoveryEvidence, ProjectOpenRecoveryKind, ProjectReachabilityReport,
    ProjectRecoveryDeferral, ProjectRecoveryGenerationClass, ProjectRetentionLimits,
    ProjectRetentionPolicy,
};
pub use gsi_profiler::{GraphScaleIndexProfile, GsiDirectedness, grade_gsi};
pub use hypotheses::{
    CreateHypothesisGroupRequest, ListHypothesisGroupsRequest, ListHypothesisMembershipRequest,
    ListHypothesisSelectionRequest, RecordHypothesisMembershipRequest,
    RecordHypothesisSelectionRequest, RemoveHypothesisMemberRequest,
};
pub use invocation_descriptor::{
    AlgorithmDescriptorContract, DESCRIPTOR_CONTRACT_VERSION, InvocationDescriptor,
    InvocationDescriptorError, InvocationError, InvocationParameter,
    algorithm_descriptor_contracts,
};
pub use knowledge::{
    AssertionGraphRefInput, AssessConfidenceRequest, AttachEvidenceRequest,
    ConfidencePolicyRequest, CreateAssertionRequest, CreateAssertionWithEvidenceRequest,
    CreateAssertionWithStatusRequest, EvidenceInput, FirstAssertionStatusInput,
    ListAssertionStatusRequest, ListAssertionSupersessionsRequest, ListAssertionsRequest,
    ListConfidenceAssessmentsRequest, ListEvidenceLinksRequest, ListReasoningRequest,
    RecordAssertionStatusRequest, RecordReasoningRequest, SupersedeAssertionRequest,
};
pub use ontology_lifecycle::{
    CatalogEntryKind, OntologyExportFormat, OntologyExportSource, OntologySuggestion,
    OntologySuggestionOptions, OntologyValidationReport, RuntimeCatalogEntry,
    RuntimeCatalogSnapshot,
};
pub use paging::{CancellationToken, DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, PageRequest, PageToken};
pub use provenance::ProvenanceHistoryRequest;
pub use resource_policy::{
    ExecutionResourcePolicy, NormalizedResourcePolicy, ResourcePolicyDiagnostics,
    ResourcePolicyMode, SpillPolicy,
};
pub use search_index::{AdjacencyInspection, TextIndexInspection};
pub use transaction::{
    GraphTransaction, MutationFamily, TransactionCommitReceipt, TransactionPhase,
    TransactionStatus, TransactionSupport, transaction_support,
};
pub use valid_time::{
    ApplyValidTimeRequest, ListAssertionValidityRequest, RecordAssertionValidityRequest,
    VALID_TIME_POLICY_VERSION,
};
pub use workspace_ontology::{AdoptOntologyRequest, ClearOntologyRequest};
pub use write_modes::{GraphForgeOptions, ProjectWriteMode};

fn insert_usize(
    parameters: &mut std::collections::BTreeMap<String, InvocationParameter>,
    name: &str,
    value: usize,
) -> Result<(), InvocationDescriptorError> {
    parameters.insert(
        name.to_owned(),
        InvocationParameter::U64(
            u64::try_from(value).map_err(|_| {
                InvocationDescriptorError::Invalid(format!("{name} exceeds UInt64"))
            })?,
        ),
    );
    Ok(())
}
pub use algorithm_embedding_publication::{
    AlgorithmEmbeddingDistance, AlgorithmEmbeddingNormalization,
    AlgorithmEmbeddingPublicationRequest,
};
pub use provider_embedding::{
    ProviderEmbeddingDistance, ProviderEmbeddingNormalization, ProviderEmbeddingPlanError,
    ProviderEmbeddingPlanInspection, ProviderEmbeddingPlanRequest, ProviderEmbeddingPlannedBatch,
};
pub use provider_embedding_execution::{
    ProviderArtifactCheckpoint, ProviderEmbeddingExecution, ProviderEmbeddingExecutionError,
    ProviderTokenCounter,
};
pub use provider_find::{
    ConfiguredProviderFindRuntime, ProviderFindError, ProviderFindExecution,
    ProviderQueryCostEstimator, ProviderQueryWorkShape,
};
pub use provider_rerank::{
    ProviderRerankError, ProviderRerankExecution, ProviderRerankPlanInspection,
    ProviderRerankRequest, ProviderRerankedFindResult,
};
pub use provider_session::{OpenRouterProviderSession, OpenRouterProviderSessionConfig};
pub use search_index::SearchIndexOptions;

// ---------------------------------------------------------------------------
// RecordBatch (interim tabular result)
// ---------------------------------------------------------------------------

/// Minimal tabular result.  Replaced by Arrow `RecordBatch` when the read path
/// lands (#717/#719).
#[derive(Debug, Clone, Default)]
pub struct RecordBatch {
    /// Column names.
    pub schema: Vec<String>,
    /// One entry per column; each entry is the column's values as strings.
    pub columns: Vec<Vec<String>>,
}

impl RecordBatch {
    /// Return an empty batch with the given schema.
    #[must_use]
    pub fn empty(schema: Vec<String>) -> Self {
        let ncols = schema.len();
        Self {
            schema,
            columns: vec![vec![]; ncols],
        }
    }
}

// ---------------------------------------------------------------------------
// GraphForge
// ---------------------------------------------------------------------------

/// The GraphForge engine — the public entry point for openCypher execution.
///
/// Built with [`new`](Self::new) (in-memory or Parquet-backed), then queried via
/// [`execute`](Self::execute). The facade owns the project directory, the
/// resolved [`OntologyMode`], an optional compiled ontology, and a shared
/// [`RuntimeCatalog`] that the binder grows as it observes new labels/properties.
pub struct GraphForge {
    /// Opaque ownership token for public handles created by this instance.
    identity: GraphIdentity,
    /// The configured path, if the instance is Parquet-backed; `None` for an
    /// in-memory instance (whose data lives in `dir`).
    path: Option<PathBuf>,
    /// One immutable committed generation selected exactly once at open.
    resolved_generation: ResolvedProjectGeneration,
    /// Whether this facade is an immutable historical checkpoint view.
    read_only: bool,
    /// Generation UUID whose graph snapshot was hydrated into `dir`.
    current_generation_uuid: Arc<Mutex<uuid::Uuid>>,
    /// Injected durable-write UTC microsecond clock.
    clock: Mutex<Arc<dyn Fn() -> Result<i64, GfError> + Send + Sync>>,
    /// Project directory backing topology/properties Parquet files. For an
    /// instance this is a private mutable workspace materialized from the pinned
    /// graph generation (file-backed tree or legacy snapshot).
    dir: PathBuf,
    /// Keeps the private mutable graph workspace alive for the engine's life.
    workspace_guard: Arc<tempfile::TempDir>,
    /// Structural evidence for how the graph workspace was opened.
    graph_open_evidence: graphforge_storage::GraphFilesOpenEvidence,
    /// Safe recovery-on-open summary (cleanup, deferral, or checkpoint skip).
    project_open_recovery: graphforge_storage::ProjectOpenRecoveryEvidence,
    /// Keeps an in-memory instance's temp directory alive for the engine's life.
    tempdir: Option<Arc<tempfile::TempDir>>,
    /// Compiled ontology, present in advisory/strict mode.
    ontology: Option<OntologyHandle>,
    /// Source document backing the live, session-scoped compiled ontology.
    ontology_document: Option<OntologyDoc>,
    /// Shared runtime catalog (grown by the binder during `execute`).
    runtime_catalog: Arc<Mutex<RuntimeCatalog>>,
    /// Procedures available to `CALL` clauses on this engine instance.
    procedures: Arc<Mutex<ProcedureRegistry>>,
    /// Effective ontology enforcement mode.
    ontology_mode: OntologyMode,
    /// Long-lived adjacency provider (#832): one per instance so loaded CSR
    /// views amortize across queries. Each session revalidates it (one
    /// generation read) at construction, and write paths invalidate it.
    adjacency_provider: Arc<graphforge_exec::PersistentAdjacencyProvider>,
    /// Prevent adjacency readers from observing a staged directory swap.
    adjacency_visibility: Arc<std::sync::RwLock<()>>,
    /// Bounded worker state owned only by this exact embedded process.
    embedding_refresh_scheduler: Arc<Mutex<graphforge_search::EmbeddingRefreshScheduler>>,
    /// Monotonic origin shared by every process-local refresh notice and lease.
    embedding_refresh_epoch: Instant,
    /// Prevent freshness readers from observing publication/journal relinking mid-transition.
    embedding_refresh_visibility: Arc<Mutex<()>>,
    /// Pins the in-process graph view across descriptor comparison and
    /// execution, and serializes same-instance graph mutations against replay.
    ///
    /// Cross-publication stability comes from `resolved_generation`; this lock
    /// closes the remaining window for mutation APIs that still operate through
    /// this exact facade instance.
    /// Same-instance write admission and visibility coordinator.
    pub(crate) graph_visibility: Arc<write_modes::WriteCoordinator>,
    /// Validated embedded write behavior for this facade.
    write_options: GraphForgeOptions,
    /// Normalized execution resource policy applied to runtime and sessions (#337).
    resource_policy: resource_policy::NormalizedResourcePolicy,
    /// Instance-owned private CPU pool for parallel algorithm kernels (#337 / #342 / #343).
    compute_pool: graphforge_exec::SharedComputePool,
    /// Instance-owned heavy-query admission gate (#337).
    heavy_query_admission: Arc<resource_policy::HeavyQueryAdmission>,
    /// Ensures mutation bursts share one bounded process-local driver thread.
    provider_refresh_driver_active: Arc<AtomicBool>,
    /// Runtime-only provider recipes capable of refreshing exact lineages.
    provider_refresh_runtimes:
        Arc<Mutex<Vec<Arc<provider_session::ConfiguredProviderRefreshRuntime>>>>,
    /// Runtime-only query providers keyed by exact persisted model identity.
    provider_find_runtimes:
        Arc<Mutex<Vec<Arc<Mutex<provider_find::ConfiguredProviderFindRuntime>>>>>,
    /// Long-lived Tokio runtime that drives the async DataFusion pipeline. Held
    /// for the instance's life so background tasks a streaming query spawns
    /// (repartition/coalesce) are not orphaned when a query returns — a
    /// per-call runtime would drop them mid-stream ("task cancelled"). Shared
    /// so a returned `execute_stream` handle keeps it alive. Wrapped in
    /// [`OwnedRuntime`] so dropping a `GraphForge` inside someone else's async
    /// context shuts the runtime down in the background instead of panicking.
    runtime: Arc<OwnedRuntime>,
}

impl std::fmt::Debug for GraphForge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Custom impl: the ontology handle / runtime catalog are large and not
        // usefully printable, so summarise rather than deriving Debug.
        f.debug_struct("GraphForge")
            .field("identity", &self.identity)
            .field("path", &self.path)
            .field(
                "generation_uuid",
                &self.resolved_generation.generation_uuid(),
            )
            .field("dir", &self.dir)
            .field("ontology_mode", &self.ontology_mode)
            .field("write_options", &self.write_options)
            .field("has_ontology", &self.ontology.is_some())
            .finish_non_exhaustive()
    }
}

impl GraphForge {
    /// Create a new in-memory (`None`) or Parquet-backed (`Some(path)`) instance.
    ///
    /// For a persistent instance, the directory may be absent when its parent
    /// exists; storage admission owns creation of the final project directory.
    /// Ontology authority and enforcement mode are resolved from the committed
    /// workspace ontology and configuration participants in the selected
    /// project generation.
    /// Loose `graphforge.yaml` or `ontology.yaml` files are not authority and
    /// are not loaded implicitly. An existing runtime-catalog participant seeds
    /// the runtime catalog.
    ///
    /// An in-memory instance is exploratory and backed by a temp directory.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] if the persistent path's parent does not
    /// exist or the temp dir cannot be created, [`GfError::Validation`] for
    /// malformed committed workspace records, and [`GfError::Ontology`] if the
    /// adopted ontology cannot be decoded or compiled.
    /// Opening a persistent project can also return structured knowledge,
    /// provenance, or publication errors while reconciling an interrupted
    /// recorded algorithm run.
    pub fn new(path: Option<&str>) -> Result<Self, GfError> {
        Self::new_with_options(path, GraphForgeOptions::default())
    }

    /// Create a facade with an explicit embedded project-write policy.
    ///
    /// # Errors
    /// Returns the same open errors as [`Self::new`] and rejects unbounded or
    /// otherwise invalid write-coordination limits.
    pub fn new_with_options(
        path: Option<&str>,
        options: GraphForgeOptions,
    ) -> Result<Self, GfError> {
        let (options, resource_policy) = options.validate()?;
        if let Some(p) = path {
            return Self::open_dir_with_options(PathBuf::from(p), options, resource_policy);
        }
        // In-memory: exploratory, backed by a temp directory kept alive for the
        // engine's lifetime.
        let tmp = tempfile::TempDir::new()
            .map_err(|e| GfError::Storage(format!("failed to create temp dir: {e}")))?;
        let (resolved_generation, project_open_recovery) =
            graphforge_storage::open_or_initialize_ephemeral_project_with_recovery(tmp.path())?;
        let generation_uuid = resolved_generation.generation_uuid();
        let (ontology_mode, ontology, ontology_document) =
            load_workspace_ontology(&resolved_generation)?;
        let (dir, workspace, graph_open_evidence) =
            hydrate_graph_workspace(&resolved_generation, false)?;
        Ok(Self {
            identity: GraphIdentity::new(),
            path: None,
            resolved_generation,
            read_only: false,
            current_generation_uuid: Arc::new(Mutex::new(generation_uuid)),
            clock: Mutex::new(Arc::new(system_time_micros)),
            adjacency_provider: Arc::new(graphforge_exec::PersistentAdjacencyProvider::new(
                dir.clone(),
                ontology_mode,
            )),
            adjacency_visibility: Arc::new(std::sync::RwLock::new(())),
            embedding_refresh_scheduler: Arc::new(Mutex::new(
                embedding_refresh::initialize_embedding_refresh_scheduler(&dir)?,
            )),
            embedding_refresh_epoch: Instant::now(),
            embedding_refresh_visibility: Arc::new(Mutex::new(())),
            graph_visibility: Arc::new(write_modes::WriteCoordinator::new(&options)),
            write_options: options,
            heavy_query_admission: Arc::new(resource_policy::HeavyQueryAdmission::new(
                resource_policy.max_concurrent_heavy_queries,
            )),
            compute_pool: Arc::new(graphforge_exec::ComputePool::new(
                resource_policy.compute_threads,
            )?),
            provider_refresh_driver_active: Arc::new(AtomicBool::new(false)),
            provider_refresh_runtimes: Arc::new(Mutex::new(Vec::new())),
            provider_find_runtimes: Arc::new(Mutex::new(Vec::new())),
            dir,
            workspace_guard: workspace,
            graph_open_evidence,
            project_open_recovery,
            tempdir: Some(Arc::new(tmp)),
            ontology,
            ontology_document,
            runtime_catalog: Arc::new(Mutex::new(RuntimeCatalog::new())),
            procedures: Arc::new(Mutex::new(ProcedureRegistry::new())),
            ontology_mode,
            runtime: build_runtime(&resource_policy)?,
            resource_policy,
        })
    }

    #[cfg(test)]
    fn set_clock_for_test(&self, clock: impl Fn() -> Result<i64, GfError> + Send + Sync + 'static) {
        *self.clock.lock().expect("clock lock poisoned") = Arc::new(clock);
    }

    /// Structural evidence for the graph open/materialization strategy.
    #[must_use]
    pub fn graph_open_evidence(&self) -> &graphforge_storage::GraphFilesOpenEvidence {
        &self.graph_open_evidence
    }

    /// Safe recovery-on-open summary for this facade instance.
    #[must_use]
    pub fn project_open_recovery(&self) -> &graphforge_storage::ProjectOpenRecoveryEvidence {
        &self.project_open_recovery
    }

    fn open_dir_with_options(
        dir: PathBuf,
        options: GraphForgeOptions,
        resource_policy: resource_policy::NormalizedResourcePolicy,
    ) -> Result<Self, GfError> {
        let parent = dir
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if !parent.is_dir() {
            return Err(GfError::Storage(format!(
                "project parent does not exist or is not a directory: {}",
                parent.display()
            )));
        }

        let (resolved_generation, project_open_recovery) =
            graphforge_storage::open_or_initialize_project_with_recovery(&dir)?;
        Self::open_resolved_with_options(
            dir,
            resolved_generation,
            false,
            options,
            resource_policy,
            project_open_recovery,
        )
    }

    fn open_resolved_with_mode(
        container_dir: PathBuf,
        resolved_generation: ResolvedProjectGeneration,
        read_only: bool,
    ) -> Result<Self, GfError> {
        let options = GraphForgeOptions::default();
        let (_, resource_policy) = options.clone().validate()?;
        let project_open_recovery = if read_only {
            graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
                resolved_generation.generation_uuid(),
            )
        } else {
            graphforge_storage::ProjectOpenRecoveryEvidence::initialization(
                resolved_generation.generation_uuid(),
            )
        };
        Self::open_resolved_with_options(
            container_dir,
            resolved_generation,
            read_only,
            options,
            resource_policy,
            project_open_recovery,
        )
    }

    fn open_resolved_with_options(
        container_dir: PathBuf,
        resolved_generation: ResolvedProjectGeneration,
        read_only: bool,
        write_options: GraphForgeOptions,
        resource_policy: resource_policy::NormalizedResourcePolicy,
        project_open_recovery: graphforge_storage::ProjectOpenRecoveryEvidence,
    ) -> Result<Self, GfError> {
        let generation_uuid = resolved_generation.generation_uuid();
        let (ontology_mode, ontology, ontology_document) =
            load_workspace_ontology(&resolved_generation)?;
        let (dir, workspace, graph_open_evidence) =
            hydrate_graph_workspace(&resolved_generation, read_only)?;

        let runtime_catalog = load_runtime_catalog(&dir)?;
        if read_only {
            graphforge_storage::validate_runtime_entity_label_ids(
                &dir,
                ontology.as_ref(),
                &runtime_catalog,
            )?;
        } else {
            graphforge_storage::reconcile_runtime_entity_label_ids(
                &dir,
                ontology.as_ref(),
                &runtime_catalog,
            )?;
        }
        let heavy_query_admission = Arc::new(resource_policy::HeavyQueryAdmission::new(
            resource_policy.max_concurrent_heavy_queries,
        ));
        let compute_pool = Arc::new(graphforge_exec::ComputePool::new(
            resource_policy.compute_threads,
        )?);
        let runtime = build_runtime(&resource_policy)?;

        let graph = Self {
            identity: GraphIdentity::new(),
            path: Some(container_dir),
            resolved_generation,
            read_only,
            current_generation_uuid: Arc::new(Mutex::new(generation_uuid)),
            clock: Mutex::new(Arc::new(system_time_micros)),
            adjacency_provider: Arc::new(graphforge_exec::PersistentAdjacencyProvider::new(
                dir.clone(),
                ontology_mode,
            )),
            adjacency_visibility: Arc::new(std::sync::RwLock::new(())),
            embedding_refresh_scheduler: Arc::new(Mutex::new(
                embedding_refresh::initialize_embedding_refresh_scheduler(&dir)?,
            )),
            embedding_refresh_epoch: Instant::now(),
            embedding_refresh_visibility: Arc::new(Mutex::new(())),
            graph_visibility: Arc::new(write_modes::WriteCoordinator::new(&write_options)),
            write_options,
            resource_policy,
            compute_pool,
            heavy_query_admission,
            provider_refresh_driver_active: Arc::new(AtomicBool::new(false)),
            provider_refresh_runtimes: Arc::new(Mutex::new(Vec::new())),
            provider_find_runtimes: Arc::new(Mutex::new(Vec::new())),
            dir,
            workspace_guard: workspace,
            graph_open_evidence,
            project_open_recovery,
            tempdir: None,
            ontology,
            ontology_document,
            runtime_catalog: Arc::new(Mutex::new(runtime_catalog)),
            procedures: Arc::new(Mutex::new(ProcedureRegistry::new())),
            ontology_mode,
            runtime,
        };
        if !read_only {
            graph.reconcile_algorithm_runs()?;
        }
        Ok(graph)
    }

    /// Normalized execution resource policy for this instance (#337).
    #[must_use]
    pub fn resource_policy(&self) -> &NormalizedResourcePolicy {
        &self.resource_policy
    }

    /// Safe aggregate diagnostics for the instance resource policy (#337).
    #[must_use]
    pub fn resource_diagnostics(&self) -> resource_policy::ResourcePolicyDiagnostics {
        resource_policy::ResourcePolicyDiagnostics {
            mode: self.resource_policy.mode,
            tokio_worker_threads: self.resource_policy.tokio_worker_threads,
            target_partitions: self.resource_policy.target_partitions,
            batch_size: self.resource_policy.batch_size,
            memory_budget_bytes: self.resource_policy.memory_budget_bytes,
            spill_enabled: self.resource_policy.spill_enabled,
            io_concurrency: self.resource_policy.io_concurrency,
            compute_threads: self.resource_policy.compute_threads,
            max_concurrent_heavy_queries: self.resource_policy.max_concurrent_heavy_queries,
            heavy_query_available: self.heavy_query_admission.available_permits(),
            observed_logical_cpus: self.resource_policy.observed_logical_cpus,
        }
    }

    fn session_resource_config(&self) -> graphforge_exec::SessionResourceConfig {
        graphforge_exec::SessionResourceConfig {
            target_partitions: self.resource_policy.target_partitions,
            batch_size: self.resource_policy.batch_size,
            memory_budget_bytes: self.resource_policy.memory_budget_bytes,
            spill_enabled: self.resource_policy.spill_enabled,
            spill_directory: self.resource_policy.spill_directory.clone(),
            spill_max_bytes: self.resource_policy.spill_max_bytes,
            io_concurrency: self.resource_policy.io_concurrency,
        }
    }

    fn admit_heavy_query(&self) -> Result<tokio::sync::SemaphorePermit<'_>, GfError> {
        self.heavy_query_admission.try_acquire()
    }

    fn admit_heavy_query_owned(&self) -> Result<tokio::sync::OwnedSemaphorePermit, GfError> {
        self.heavy_query_admission.try_acquire_owned()
    }

    fn generation_for_read(&self) -> Result<ResolvedProjectGeneration, GfError> {
        if self.read_only {
            Ok(self.resolved_generation.clone())
        } else {
            graphforge_storage::resolve_project_generation(
                self.resolved_generation.container_root(),
            )
        }
    }

    fn execute_read_only(&self, cypher: &str) -> Result<ExecutionResult, GfError> {
        if cypher.trim().is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        let ast = graphforge_cypher::parse(cypher).map_err(|e| GfError::Parse {
            msg: e.message,
            span: e.span,
        })?;
        if ast.clauses.is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        let plan = Binder::new(
            self.ontology.clone(),
            self.runtime_catalog.clone(),
            self.ontology_mode,
        )
        .with_procedures(self.procedure_snapshot())
        .bind(&ast)
        .map_err(|errs| bind_errors_to_gferror(&errs))?;
        if plan.ops.iter().any(|op| {
            matches!(
                op,
                GraphOp::Create { .. }
                    | GraphOp::Merge { .. }
                    | GraphOp::Delete { .. }
                    | GraphOp::Set { .. }
                    | GraphOp::Remove { .. }
            )
        }) {
            return Err(GfError::Project {
                code: ProjectErrorCode::ReadOnlyView,
                message: "checkpoint views are read-only".into(),
            });
        }
        shape_result(
            self.run_plan(&plan, &HashMap::new())?,
            self.ontology_mode,
            self.ontology.as_ref(),
        )
    }

    /// Execute an openCypher query and return its Arrow-backed result.
    ///
    /// Runs the full pipeline: `parse → bind → lower → execute`. A query
    /// containing `CREATE` writes through [`graphforge_exec::ExecutionSession::execute_create`];
    /// a read query runs through `execute_plan`. The result exposes UUID
    /// identity columns (`node_uuid`/`edge_uuid`) — never internal surrogate
    /// scan keys — while preserving legal user aliases such as
    /// `RETURN id(n) AS node_id` (#703). The schema carries query metadata.
    ///
    /// # Errors
    /// Returns [`GfError::Parse`] on a parse failure, [`GfError::Plan`] on a bind
    /// failure (e.g. a strict-mode unknown label), and [`GfError::Plan`] /
    /// [`GfError::Execution`] on lowering / execution failures.
    pub fn execute(&self, cypher: &str) -> Result<ExecutionResult, GfError> {
        self.execute_with_params(cypher, &HashMap::new())
    }

    /// Register or replace a deterministic procedure available to `CALL`.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] when a fixture row does not match the
    /// declared input and output width.
    pub fn register_procedure(&self, procedure: ProcedureDefinition) -> Result<(), GfError> {
        let width = procedure.inputs.len() + procedure.outputs.len();
        if let Some(row) = procedure.rows.iter().find(|row| row.len() != width) {
            return Err(GfError::Validation(format!(
                "procedure {} expects {width} fixture columns, found {}",
                procedure.name,
                row.len()
            )));
        }
        self.procedures
            .lock()
            .expect("procedure registry poisoned")
            .insert(procedure.name.clone(), procedure);
        Ok(())
    }

    fn procedure_snapshot(&self) -> Arc<ProcedureRegistry> {
        Arc::new(
            self.procedures
                .lock()
                .expect("procedure registry poisoned")
                .clone(),
        )
    }

    /// Execute an openCypher query with bind-time parameters.
    ///
    /// See [`execute`](Self::execute); `params` supplies values for `$name`
    /// placeholders in the query.
    ///
    /// # Errors
    /// As [`execute`](Self::execute).
    pub fn execute_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query(cypher, params)
    }

    fn run_query(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query_with_publish(cypher, params, true)
    }

    /// Execute a write Cypher statement against the private workspace without
    /// moving `CURRENT`. Used by the uniform transaction lifecycle so multiple
    /// staged writers share one later publication.
    pub(crate) fn execute_write_without_publish(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query_with_publish(cypher, params, false)
    }

    fn run_query_with_publish(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        publish: bool,
    ) -> Result<ExecutionResult, GfError> {
        let _admission = self.admit_heavy_query()?;
        if cypher.trim().is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }

        let ast = graphforge_cypher::parse(cypher).map_err(|e| GfError::Parse {
            msg: e.message,
            span: e.span,
        })?;
        // A query that strips to zero clauses (e.g. comment-only or block-comment
        // -only) is empty even though its raw text is not blank, so the
        // `trim().is_empty()` guard above misses it. Reject it here rather than
        // letting the empty plan panic the result shaper (#603 — found by fuzz_exec).
        if ast.clauses.is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        validate_typed_parameter_binding(
            &ast,
            params,
            self.ontology.clone(),
            &self.runtime_catalog,
            self.ontology_mode,
            self.procedure_snapshot(),
        )?;

        // Bind against the shared runtime catalog so newly observed types/props
        // persist across queries in this instance.
        let plan = {
            let binder = Binder::new(
                self.ontology.clone(),
                self.runtime_catalog.clone(),
                self.ontology_mode,
            )
            .with_procedures(self.procedure_snapshot());
            binder
                .bind(&ast)
                .map_err(|errs| bind_errors_to_gferror(&errs))?
        };

        validate_call_params(&plan, params)?;

        let result = self
            .run_plan_with_publish(&plan, params, publish)
            .map_err(publicize_query_error)?;
        shape_result(result, self.ontology_mode, self.ontology.as_ref())
            .map_err(publicize_query_error)
    }

    /// Build a session reflecting the current runtime catalog and run `plan`,
    /// routing CREATE to the write path and reads to `execute_plan` with `$name`
    /// parameters substituted where the lowered logical plan still carries them.
    fn run_plan(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_plan_with_publish(plan, params, true)
    }

    fn run_plan_with_publish(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, IrLiteral>,
        publish: bool,
    ) -> Result<ExecutionResult, GfError> {
        use graphforge_exec::ExecutionSession;

        let plan = materialize_row_count_params(plan, params)?;

        // Route every supported write shape through the clause-ordered statement driver.
        let write_ops = plan
            .ops
            .iter()
            .filter(|op| {
                matches!(
                    op,
                    GraphOp::Create { .. }
                        | GraphOp::Merge { .. }
                        | GraphOp::Delete { .. }
                        | GraphOp::Set { .. }
                        | GraphOp::Remove { .. }
                )
            })
            .count();
        let is_write = write_ops > 0;
        // Transaction commit already holds write admission when publish is false.
        let _write_visibility = (is_write && publish)
            .then(|| self.graph_visibility.lock())
            .transpose()?;
        let _read_visibility = (!is_write)
            .then(|| self.graph_visibility.read())
            .transpose()?;
        let expected_generation_before_write = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        // File-backed generations restore from the still-authoritative parent
        // generation on publish failure instead of capturing a whole-workspace
        // Arrow snapshot envelope.
        let rollback_generation = (is_write && publish)
            .then(|| {
                graphforge_storage::resolve_project_generation(
                    self.resolved_generation.container_root(),
                )
            })
            .transpose()?;

        // Open a catalog snapshot reflecting the freshly-bound runtime catalog so
        // read scans resolve property names interned during bind.
        let catalog = {
            let rc = self
                .runtime_catalog
                .lock()
                .expect("runtime catalog poisoned");
            GraphCatalog::open(&self.dir, self.ontology.as_ref(), &rc)
                .map_err(|e| GfError::Storage(e.to_string()))?
        };
        let session = ExecutionSession::new_with_target_provider_and_resources(
            catalog,
            self.ontology.clone(),
            self.dir.clone(),
            self.ontology_mode,
            Arc::clone(&self.adjacency_provider),
            &self.session_resource_config(),
        )?;

        let result = self.block_on(async {
            if is_write {
                session
                    .execute_write_statement_with_params(&plan, params)
                    .await
            } else {
                session.execute_plan_with_params(&plan, params).await
            }
        })?;

        // Persist the runtime catalog after a write so a later `GraphForge::new`
        // on this directory reloads the types/properties the binder observed
        // (the read side already loads it on open). In-memory instances skip
        // this — their temp dir is discarded on drop. (#725)
        if is_write && self.path.is_some() {
            let rc = self
                .runtime_catalog
                .lock()
                .expect("runtime catalog poisoned");
            persist_runtime_catalog(&self.dir, &rc)?;
        }
        if publish
            && let Some(receipt) = result
                .mutation_receipt
                .as_ref()
                .filter(|receipt| !receipt.is_empty())
        {
            let rollback_generation = rollback_generation
                .as_ref()
                .expect("write path resolved a rollback generation");
            if let Err(error) = self.publish_graph_mutation(receipt) {
                let still_prior = *self
                    .current_generation_uuid
                    .lock()
                    .expect("generation UUID lock poisoned")
                    == expected_generation_before_write;
                if still_prior {
                    rematerialize_graph_workspace(rollback_generation, &self.dir)?;
                    self.adjacency_provider.invalidate();
                }
                return Err(error);
            }
        }
        if is_write
            && result
                .side_effects
                .as_ref()
                .is_some_and(|effects| effects != &graphforge_exec::SideEffects::default())
        {
            self.notice_provider_embedding_mutation();
        }
        Ok(result)
    }

    fn publish_graph_mutation(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
    ) -> Result<(), GfError> {
        let operation_uuid = uuid::Uuid::now_v7();
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        self.publish_graph_mutation_with_context(receipt, operation_uuid, None, recorded_at_micros)
    }

    pub(crate) fn publish_graph_mutation_with_context(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
        operation_uuid: uuid::Uuid,
        actor_uuid: Option<uuid::Uuid>,
        recorded_at_micros: i64,
    ) -> Result<(), GfError> {
        use graphforge_storage::{
            ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
        };

        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(GfError::Validation(
                "project generation changed before graph publication".into(),
            ));
        }

        let graph = graphforge_storage::capture_graph_files(&self.dir)?.1;
        let provenance_enabled = parent.capability("provenance")?.is_some();
        let participants = graph_publication_participants(
            &parent,
            graph,
            provenance_enabled,
            receipt,
            operation_uuid,
            actor_uuid,
            recorded_at_micros,
        )?;
        let capabilities = parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect::<Vec<_>>();
        let generation_uuid = mutation_generation_uuid(operation_uuid, &participants);
        let request = ProjectGenerationRequest {
            transaction_uuid: operation_uuid,
            generation_uuid,
            capabilities,
            participants,
        };
        let publication = match graphforge_storage::stage_project_generation_with_graph_tree(
            root,
            &request,
            Some(self.dir.as_path()),
        )? {
            ProjectStageOutcome::AlreadyPublished(receipt) => Ok(receipt),
            ProjectStageOutcome::Staged(staged) => staged
                .validate(
                    |_| Ok(()),
                    |actual_parent, _| {
                        if actual_parent.generation_uuid() != expected_parent {
                            return Err(GfError::Validation(
                                "project generation changed before graph publication".into(),
                            ));
                        }
                        Ok(())
                    },
                )?
                .publish(),
        };
        let published = match publication {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Ok(current) = graphforge_storage::resolve_project_generation(root)
                    && current.generation_uuid() == generation_uuid
                {
                    *self
                        .current_generation_uuid
                        .lock()
                        .expect("generation UUID lock poisoned") = generation_uuid;
                }
                return Err(error);
            }
        };
        *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") = published.generation_uuid;
        Ok(())
    }

    fn publish_graph_mutation_with_generation(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
        operation_uuid: uuid::Uuid,
        generation_uuid: uuid::Uuid,
        expected_parent: uuid::Uuid,
        recorded_at_micros: i64,
    ) -> Result<(), GfError> {
        use graphforge_storage::{
            ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
        };

        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        if parent.generation_uuid() != expected_parent {
            return Err(GfError::Validation(
                "project generation changed before graph publication".into(),
            ));
        }
        let graph = graphforge_storage::capture_graph_files(&self.dir)?.1;
        let provenance_enabled = parent.capability("provenance")?.is_some();
        let participants = graph_publication_participants(
            &parent,
            graph,
            provenance_enabled,
            receipt,
            operation_uuid,
            None,
            recorded_at_micros,
        )?;
        let capabilities = parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect();
        let request = ProjectGenerationRequest {
            transaction_uuid: operation_uuid,
            generation_uuid,
            capabilities,
            participants,
        };
        let publication = match graphforge_storage::stage_project_generation_with_graph_tree(
            root,
            &request,
            Some(self.dir.as_path()),
        )? {
            ProjectStageOutcome::AlreadyPublished(receipt) => Ok(receipt),
            ProjectStageOutcome::Staged(staged) => staged
                .validate(
                    |_| Ok(()),
                    |actual_parent, _| {
                        if actual_parent.generation_uuid() != expected_parent {
                            return Err(GfError::Validation(
                                "project generation changed before graph publication".into(),
                            ));
                        }
                        Ok(())
                    },
                )?
                .publish(),
        };
        let published = match publication {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Ok(current) = graphforge_storage::resolve_project_generation(root)
                    && current.generation_uuid() == generation_uuid
                {
                    *self
                        .current_generation_uuid
                        .lock()
                        .expect("generation UUID lock poisoned") = generation_uuid;
                }
                return Err(error);
            }
        };
        *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") = published.generation_uuid;
        Ok(())
    }

    fn publish_workspace_update(&self) -> Result<(), GfError> {
        self.publish_graph_mutation(&graphforge_exec::MutationReceipt::default())
    }

    /// Execute a read-only openCypher query and return a lazy stream of its
    /// result batches (the streaming counterpart of [`execute`](Self::execute)).
    ///
    /// Like `execute`, each batch exposes UUID identity (never internal surrogate
    /// scan keys) while preserving legal user aliases named `node_id`/`edge_id`
    /// (#703), and the stream's schema carries query metadata. `CREATE`/`MERGE`
    /// are not supported on the streaming path — use
    /// [`execute`](Self::execute) for writes.
    ///
    /// # Errors
    /// As [`execute`](Self::execute); additionally [`GfError::Validation`] if the
    /// query is a write.
    pub fn execute_stream(
        &self,
        cypher: &str,
    ) -> Result<graphforge_exec::SendableRecordBatchStream, GfError> {
        self.execute_stream_with_params(cypher, &HashMap::new())
    }

    /// Streaming variant of [`execute_with_params`](Self::execute_with_params).
    ///
    /// # Errors
    /// As [`execute_stream`](Self::execute_stream).
    pub fn execute_stream_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<graphforge_exec::SendableRecordBatchStream, GfError> {
        use graphforge_exec::ExecutionSession;

        let admission = self.admit_heavy_query_owned()?;
        if cypher.trim().is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        let ast = graphforge_cypher::parse(cypher).map_err(|e| GfError::Parse {
            msg: e.message,
            span: e.span,
        })?;
        // See `execute_with_params`: a comment-only query strips to zero clauses.
        if ast.clauses.is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        validate_typed_parameter_binding(
            &ast,
            params,
            self.ontology.clone(),
            &self.runtime_catalog,
            self.ontology_mode,
            self.procedure_snapshot(),
        )?;
        let plan = {
            let binder = Binder::new(
                self.ontology.clone(),
                self.runtime_catalog.clone(),
                self.ontology_mode,
            )
            .with_procedures(self.procedure_snapshot());
            binder
                .bind(&ast)
                .map_err(|errs| bind_errors_to_gferror(&errs))?
        };
        validate_call_params(&plan, params)?;
        if plan.ops.iter().any(|op| {
            matches!(
                op,
                GraphOp::Create { .. }
                    | GraphOp::Merge { .. }
                    | GraphOp::Delete { .. }
                    | GraphOp::Set { .. }
                    | GraphOp::Remove { .. }
            )
        }) {
            return Err(GfError::Validation(
                "execute_stream does not support writes; \
                 use execute for CREATE/MERGE/DELETE/SET/REMOVE"
                    .into(),
            ));
        }

        let catalog = {
            let rc = self
                .runtime_catalog
                .lock()
                .expect("runtime catalog poisoned");
            GraphCatalog::open(&self.dir, self.ontology.as_ref(), &rc)
                .map_err(|e| GfError::Storage(e.to_string()))?
        };
        let session = ExecutionSession::new_with_target_provider_and_resources(
            catalog,
            self.ontology.clone(),
            self.dir.clone(),
            self.ontology_mode,
            Arc::clone(&self.adjacency_provider),
            &self.session_resource_config(),
        )?;

        // Build the stream on the instance's long-lived runtime so the tasks it
        // spawns (repartition/coalesce) outlive this call — they are dropped
        // only when the `GraphForge` is. `block_on` drives the construction
        // inside that runtime's context.
        let stream = self.block_on(async { session.execute_plan_stream(&plan, params).await })?;
        // Admission is intentionally released after stream construction: the
        // stream is demand-driven and may outlive this call; holding the slot
        // for the full consumer lifetime would serialize all streaming clients.
        drop(admission);
        Ok(shape_stream(
            stream,
            self.ontology_mode,
            self.ontology.as_ref(),
        ))
    }

    /// Streaming query plus a [`RuntimeGuard`] that keeps the instance's
    /// runtime **and** on-disk graph workspace alive for as long as the returned
    /// stream is held — for bindings that detach the stream into a foreign,
    /// lazily-consumed reader (e.g. a `pyarrow.RecordBatchReader`, #587).
    ///
    /// A bare runtime `Handle` does not keep the runtime alive, and streaming
    /// Parquet scans (#339) read fragment paths during consumer pull — so the
    /// guard also pins the private workspace (and in-memory project tempdir)
    /// that back those paths. Resources are released only once both this
    /// `GraphForge` and every outstanding guard drop.
    ///
    /// Returns the (shaped) stream, its schema (advertised up front so a reader
    /// can expose `schema` before the first batch), and the guard.
    ///
    /// # Errors
    /// As [`execute_stream_with_params`](Self::execute_stream_with_params).
    pub fn execute_stream_owned(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<
        (
            graphforge_exec::SendableRecordBatchStream,
            SchemaRef,
            RuntimeGuard,
        ),
        GfError,
    > {
        let stream = self.execute_stream_with_params(cypher, params)?;
        let schema = stream.schema();
        Ok((
            stream,
            schema,
            RuntimeGuard {
                runtime: Arc::clone(&self.runtime),
                workspace: Arc::clone(&self.workspace_guard),
                tempdir: self.tempdir.clone(),
            },
        ))
    }

    /// Drive a future on the instance's runtime from a synchronous caller.
    ///
    /// `Handle::block_on` panics if the calling thread is already inside a Tokio
    /// runtime (e.g. an async test/harness like the cucumber BDD runner), so in
    /// that case run on a scoped thread — outside any ambient runtime — that
    /// blocks on our (multi-thread) runtime's handle instead.
    fn block_on<T, F>(&self, fut: F) -> Result<T, GfError>
    where
        T: Send,
        F: std::future::Future<Output = Result<T, GfError>> + Send,
    {
        let handle = self.runtime.handle().clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::scope(|s| {
                s.spawn(|| handle.block_on(fut))
                    .join()
                    .map_err(|_| GfError::Execution("execution thread panicked".into()))?
            })
        } else {
            handle.block_on(fut)
        }
    }

    /// Remove all nodes and edges (in-memory instances only).
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] for persistent projects or if the in-memory
    /// project cannot be reset.
    pub fn clear(&self) -> Result<(), GfError> {
        let _graph_visibility = self.graph_visibility.lock()?;
        if self.path.is_some() {
            return Err(GfError::Storage(
                "clear is supported only for in-memory GraphForge instances".to_owned(),
            ));
        }

        let cleanup_result = (|| -> Result<(), GfError> {
            let entries = std::fs::read_dir(&self.dir)
                .map_err(|e| GfError::Storage(format!("failed to read in-memory project: {e}")))?;
            let mut first_error = None;

            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) => {
                        first_error.get_or_insert_with(|| {
                            GfError::Storage(format!(
                                "failed to inspect in-memory project entry: {error}"
                            ))
                        });
                        continue;
                    }
                };
                let path = entry.path();
                let file_type = match entry.file_type() {
                    Ok(file_type) => file_type,
                    Err(error) => {
                        first_error.get_or_insert_with(|| {
                            GfError::Storage(format!(
                                "failed to inspect in-memory project entry {}: {error}",
                                path.display()
                            ))
                        });
                        continue;
                    }
                };
                let result = if file_type.is_dir() && !file_type.is_symlink() {
                    std::fs::remove_dir_all(&path)
                } else {
                    std::fs::remove_file(&path)
                };
                if let Err(error) = result {
                    first_error.get_or_insert_with(|| {
                        GfError::Storage(format!(
                            "failed to remove in-memory project entry {}: {error}",
                            path.display()
                        ))
                    });
                }
            }

            first_error.map_or(Ok(()), Err)
        })();

        // These registries describe the fixture, not only its remaining files.
        // Reset them even when filesystem cleanup is partial so callers never
        // observe a stale catalog or procedure registry after `clear()` returns.
        *self.runtime_catalog.lock().expect("runtime catalog lock") = RuntimeCatalog::new();
        self.procedures
            .lock()
            .expect("procedure registry lock")
            .clear();
        self.adjacency_provider.invalidate();
        cleanup_result
    }

    fn algorithm_label(&self, label: &str, verb: &str) -> Result<(TypeId, String), GfError> {
        if label.is_empty() || label.trim() != label || label.chars().any(char::is_control) {
            return Err(GfError::Validation(format!(
                "invalid {verb} label {label:?}"
            )));
        }
        let label_id = self
            .ontology
            .as_ref()
            .and_then(|ontology| ontology.entity_type_id(label))
            .or_else(|| {
                self.runtime_catalog
                    .lock()
                    .expect("runtime catalog poisoned")
                    .entity_type_names_with_ids()
                    .find_map(|(id, name)| {
                        (name == label).then_some(graphforge_ir::runtime_entity_type_id(id))
                    })
            })
            .unwrap_or(TypeId(u32::MAX));
        let stem = if matches!(self.ontology_mode, OntologyMode::Exploratory) {
            "_untyped".to_owned()
        } else {
            label.to_owned()
        };
        Ok((label_id, stem))
    }

    /// Prepare a canonical, knowledge-neutral rank descriptor without running it.
    ///
    /// # Errors
    /// Returns a structured graph or descriptor failure. Write-back is rejected
    /// because it is a separate mutation, not part of neutral invocation state.
    pub fn prepare_rank_invocation(
        &self,
        label: &str,
        options: &RankOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        if options.write_property.is_some() {
            return Err(InvocationDescriptorError::Invalid(
                "rank write_property is not part of a neutral invocation".into(),
            )
            .into());
        }
        let (label_id, _) = self.algorithm_label(label, "rank")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        let projection = graphforge_exec::rank_projection_fingerprint(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            label_id,
            options,
        )?;
        InvocationDescriptor::new(
            Algorithm::Rank(options.by),
            projection,
            std::collections::BTreeMap::from([
                (
                    "directed".into(),
                    InvocationParameter::Bool(options.directed),
                ),
                ("label".into(), InvocationParameter::Utf8(label.to_owned())),
                (
                    "via".into(),
                    InvocationParameter::Utf8(options.via.clone().unwrap_or_else(|| "*".into())),
                ),
            ]),
        )
        .map_err(Into::into)
    }

    /// Dispatch a prepared rank descriptor through the same executor as [`Self::rank`].
    ///
    /// # Errors
    /// Returns `GF_PROJECTION_CHANGED` if graph input changed after preparation,
    /// or the structured descriptor/algorithm failure.
    pub fn invoke_rank_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Rank(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "rank dispatch requires a rank descriptor".into(),
            )
            .into());
        };
        let parameters = descriptor.parameters();
        let label = invocation_descriptor::required_utf8(parameters, "label")?;
        let via = invocation_descriptor::required_utf8(parameters, "via")?;
        let options = RankOptions {
            by,
            via: (via != "*").then(|| via.to_owned()),
            directed: invocation_descriptor::required_bool(parameters, "directed")?,
            write_property: None,
        };
        let current = self.prepare_rank_invocation(label, &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(descriptor, self.rank(label, options)?)
    }

    /// Prepare a canonical, knowledge-neutral clustering descriptor.
    ///
    /// # Errors
    /// Returns a structured graph or descriptor failure; write-back is rejected.
    pub fn prepare_cluster_invocation(
        &self,
        label: &str,
        options: &ClusterOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        if options.write_property.is_some() {
            return Err(InvocationDescriptorError::Invalid(
                "cluster write_property is not part of a neutral invocation".into(),
            )
            .into());
        }
        let (label_id, stem) = self.algorithm_label(label, "cluster")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        let projection = graphforge_exec::cluster_projection_fingerprint(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            label_id,
            std::slice::from_ref(&stem),
            options,
        )?;
        let mut parameters = std::collections::BTreeMap::from([
            (
                "directed".into(),
                InvocationParameter::Bool(options.directed && options.by.respects_direction()),
            ),
            ("label".into(), InvocationParameter::Utf8(label.to_owned())),
        ]);
        if matches!(
            options.by,
            ClusterAlgorithm::Hdbscan | ClusterAlgorithm::KMeans
        ) {
            let property = options.vector_property.as_ref().ok_or_else(|| {
                InvocationDescriptorError::Invalid(format!(
                    "cluster.{} requires vector_property",
                    options.by
                ))
            })?;
            parameters.insert(
                "vector_property".into(),
                InvocationParameter::Utf8(property.clone()),
            );
        } else {
            parameters.insert(
                "via".into(),
                InvocationParameter::Utf8(options.via.clone().unwrap_or_else(|| "*".into())),
            );
        }
        InvocationDescriptor::new(Algorithm::Cluster(options.by), projection, parameters)
            .map_err(Into::into)
    }

    /// Dispatch a prepared clustering descriptor through [`Self::cluster`].
    ///
    /// # Errors
    /// Returns a structured descriptor, projection, graph, or execution failure.
    pub fn invoke_cluster_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Cluster(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "cluster dispatch requires a cluster descriptor".into(),
            )
            .into());
        };
        let parameters = descriptor.parameters();
        let label = invocation_descriptor::required_utf8(parameters, "label")?;
        let via = invocation_descriptor::optional_utf8(parameters, "via")?;
        let options = ClusterOptions {
            by,
            vector_property: invocation_descriptor::optional_utf8(parameters, "vector_property")?,
            via: via.filter(|value| value != "*"),
            directed: invocation_descriptor::required_bool(parameters, "directed")?,
            write_property: None,
        };
        let current = self.prepare_cluster_invocation(label, &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(descriptor, self.cluster(label, options)?)
    }

    /// Prepare a canonical, knowledge-neutral similarity descriptor.
    ///
    /// # Errors
    /// Returns a structured graph or descriptor failure.
    pub fn prepare_similar_invocation(
        &self,
        label: &str,
        options: &SimilarOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        let (label_id, stem) = self.algorithm_label(label, "similar")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        let projection = graphforge_exec::similar_projection_fingerprint(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            label_id,
            std::slice::from_ref(&stem),
            options,
        )?;
        let mut parameters = std::collections::BTreeMap::from([
            (
                "k".into(),
                InvocationParameter::U64(u64::try_from(options.k).map_err(|_| {
                    InvocationDescriptorError::Invalid("similar k exceeds UInt64".into())
                })?),
            ),
            ("label".into(), InvocationParameter::Utf8(label.to_owned())),
        ]);
        if matches!(
            options.by,
            SimilarAlgorithm::Knn | SimilarAlgorithm::FilteredKnn | SimilarAlgorithm::Cosine
        ) {
            let property = options.vector_property.as_ref().ok_or_else(|| {
                InvocationDescriptorError::Invalid(format!(
                    "similar.{} requires vector_property",
                    options.by
                ))
            })?;
            parameters.insert(
                "vector_property".into(),
                InvocationParameter::Utf8(property.clone()),
            );
        }
        if !matches!(options.by, SimilarAlgorithm::Knn | SimilarAlgorithm::Cosine) {
            parameters.insert(
                "via".into(),
                InvocationParameter::Utf8(options.via.clone().unwrap_or_else(|| "*".into())),
            );
        }
        InvocationDescriptor::new(Algorithm::Similar(options.by), projection, parameters)
            .map_err(Into::into)
    }

    /// Dispatch a prepared similarity descriptor through [`Self::similar`].
    ///
    /// # Errors
    /// Returns a structured descriptor, projection, graph, or execution failure.
    pub fn invoke_similar_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Similar(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "similar dispatch requires a similarity descriptor".into(),
            )
            .into());
        };
        let parameters = descriptor.parameters();
        let label = invocation_descriptor::required_utf8(parameters, "label")?;
        let via = invocation_descriptor::optional_utf8(parameters, "via")?;
        let options = SimilarOptions {
            by,
            k: usize::try_from(invocation_descriptor::required_u64(parameters, "k")?).map_err(
                |_| InvocationDescriptorError::Invalid("similar k exceeds usize".into()),
            )?,
            vector_property: invocation_descriptor::optional_utf8(parameters, "vector_property")?,
            via: via.filter(|value| value != "*"),
        };
        let current = self.prepare_similar_invocation(label, &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(descriptor, self.similar(label, options)?)
    }

    /// Prepare a canonical, knowledge-neutral embedding descriptor.
    ///
    /// # Errors
    /// Returns the same normalization, projection, property, and resource
    /// failures as embedding execution, without starting the kernel.
    #[allow(
        clippy::too_many_lines,
        reason = "the closed four-variant embedding registry is encoded in one exhaustive match"
    )]
    pub fn prepare_embedding_invocation(
        &self,
        label: Option<&str>,
        options: &EmbeddingAnalyzeOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        let label_id = label
            .map(|value| self.algorithm_label(value, "analyze").map(|(id, _)| id))
            .transpose()?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        let prepared = graphforge_exec::prepare_embedding_invocation_descriptor_with_compute(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            label_id,
            label,
            options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )?;
        let mut parameters = std::collections::BTreeMap::from([
            (
                "directed".into(),
                InvocationParameter::Bool(prepared.selector.directed),
            ),
            (
                "label".into(),
                InvocationParameter::Utf8(prepared.selector.label.unwrap_or_default()),
            ),
            ("seed".into(), InvocationParameter::U64(prepared.rng.seed)),
            (
                "via".into(),
                InvocationParameter::Utf8(prepared.selector.via.unwrap_or_else(|| "*".into())),
            ),
            (
                "weight".into(),
                InvocationParameter::Utf8(prepared.selector.weight.unwrap_or_default()),
            ),
        ]);
        match prepared.options {
            EmbeddingOptions::Node2Vec(value) => {
                insert_usize(&mut parameters, "dimensions", value.dimensions)?;
                insert_usize(&mut parameters, "walk_length", value.walk_length)?;
                insert_usize(&mut parameters, "walks_per_node", value.walks_per_node)?;
                parameters.insert("p".into(), InvocationParameter::F64(value.p));
                parameters.insert("q".into(), InvocationParameter::F64(value.q));
                insert_usize(&mut parameters, "window_size", value.window_size)?;
                insert_usize(&mut parameters, "negative_samples", value.negative_samples)?;
                insert_usize(&mut parameters, "epochs", value.epochs)?;
                parameters.insert(
                    "learning_rate".into(),
                    InvocationParameter::F64(value.learning_rate),
                );
            }
            EmbeddingOptions::GraphSage(value) => {
                insert_usize(&mut parameters, "dimensions", value.dimensions)?;
                insert_usize(
                    &mut parameters,
                    "hidden_dimensions",
                    value.hidden_dimensions,
                )?;
                insert_usize(&mut parameters, "layers", value.layers)?;
                parameters.insert(
                    "sample_sizes".into(),
                    InvocationParameter::U64List(
                        value
                            .sample_sizes
                            .into_iter()
                            .map(|item| {
                                u64::try_from(item).map_err(|_| {
                                    InvocationDescriptorError::Invalid(
                                        "GraphSAGE sample size exceeds UInt64".into(),
                                    )
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                );
                parameters.insert(
                    "aggregator".into(),
                    InvocationParameter::Utf8(match value.aggregator {
                        GraphSageAggregator::Mean => "mean".into(),
                    }),
                );
                insert_usize(&mut parameters, "epochs", value.epochs)?;
                insert_usize(&mut parameters, "negative_samples", value.negative_samples)?;
                parameters.insert(
                    "learning_rate".into(),
                    InvocationParameter::F64(value.learning_rate),
                );
                parameters.insert(
                    "feature_properties".into(),
                    InvocationParameter::Utf8List(value.feature_properties),
                );
            }
            EmbeddingOptions::FastRandomProjection(value) => {
                insert_usize(&mut parameters, "dimensions", value.dimensions)?;
                parameters.insert(
                    "iteration_weights".into(),
                    InvocationParameter::F64List(value.iteration_weights),
                );
                parameters.insert(
                    "normalization_strength".into(),
                    InvocationParameter::F64(value.normalization_strength),
                );
                parameters.insert(
                    "feature_weight".into(),
                    InvocationParameter::F64(value.feature_weight),
                );
                parameters.insert(
                    "feature_properties".into(),
                    InvocationParameter::Utf8List(value.feature_properties),
                );
            }
            EmbeddingOptions::HashGnn(value) => {
                insert_usize(&mut parameters, "dimensions", value.dimensions)?;
                insert_usize(&mut parameters, "iterations", value.iterations)?;
                parameters.insert(
                    "embedding_density".into(),
                    InvocationParameter::F64(value.embedding_density),
                );
                parameters.insert(
                    "heterogeneous".into(),
                    InvocationParameter::Bool(value.heterogeneous),
                );
                parameters.insert(
                    "node_type_property".into(),
                    InvocationParameter::Utf8(value.node_type_property.unwrap_or_default()),
                );
                parameters.insert(
                    "relationship_type_property".into(),
                    InvocationParameter::Utf8(value.relationship_type_property.unwrap_or_default()),
                );
            }
        }
        InvocationDescriptor::new(
            Algorithm::Analyze(options.by),
            prepared.projection_fingerprint,
            parameters,
        )
        .map_err(Into::into)
    }

    /// Dispatch a prepared embedding descriptor through [`Self::analyze_embedding`].
    ///
    /// # Errors
    /// Returns a structured descriptor, projection, graph, or execution failure.
    #[allow(
        clippy::too_many_lines,
        reason = "the closed four-variant embedding registry is decoded in one exhaustive match"
    )]
    pub fn invoke_embedding_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Analyze(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "embedding dispatch requires an analyze descriptor".into(),
            )
            .into());
        };
        if !matches!(
            by,
            AnalyzeAlgorithm::Node2Vec
                | AnalyzeAlgorithm::GraphSage
                | AnalyzeAlgorithm::FastRandomProjection
                | AnalyzeAlgorithm::HashGnn
        ) {
            return Err(InvocationDescriptorError::Invalid(
                "descriptor is not an embedding algorithm".into(),
            )
            .into());
        }
        let parameters = descriptor.parameters();
        let usize_value = |name| {
            usize::try_from(invocation_descriptor::required_u64(parameters, name)?)
                .map_err(|_| InvocationDescriptorError::Invalid(format!("{name} exceeds usize")))
        };
        let seed = invocation_descriptor::required_u64(parameters, "seed")?;
        let embedding_options = match by {
            AnalyzeAlgorithm::Node2Vec => EmbeddingOptions::Node2Vec(Node2VecOptions {
                dimensions: usize_value("dimensions")?,
                walk_length: usize_value("walk_length")?,
                walks_per_node: usize_value("walks_per_node")?,
                p: invocation_descriptor::required_f64(parameters, "p")?,
                q: invocation_descriptor::required_f64(parameters, "q")?,
                window_size: usize_value("window_size")?,
                negative_samples: usize_value("negative_samples")?,
                epochs: usize_value("epochs")?,
                learning_rate: invocation_descriptor::required_f64(parameters, "learning_rate")?,
                seed,
            }),
            AnalyzeAlgorithm::GraphSage => {
                let aggregator =
                    match invocation_descriptor::required_utf8(parameters, "aggregator")? {
                        "mean" => GraphSageAggregator::Mean,
                        value => {
                            return Err(InvocationDescriptorError::Invalid(format!(
                                "unsupported GraphSAGE aggregator {value:?}"
                            ))
                            .into());
                        }
                    };
                EmbeddingOptions::GraphSage(GraphSageOptions {
                    dimensions: usize_value("dimensions")?,
                    hidden_dimensions: usize_value("hidden_dimensions")?,
                    layers: usize_value("layers")?,
                    sample_sizes: invocation_descriptor::required_u64_list(
                        parameters,
                        "sample_sizes",
                    )?
                    .into_iter()
                    .map(|value| {
                        usize::try_from(value).map_err(|_| {
                            InvocationDescriptorError::Invalid(
                                "GraphSAGE sample size exceeds usize".into(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                    aggregator,
                    epochs: usize_value("epochs")?,
                    negative_samples: usize_value("negative_samples")?,
                    learning_rate: invocation_descriptor::required_f64(
                        parameters,
                        "learning_rate",
                    )?,
                    feature_properties: invocation_descriptor::required_utf8_list(
                        parameters,
                        "feature_properties",
                    )?,
                    seed,
                })
            }
            AnalyzeAlgorithm::FastRandomProjection => {
                EmbeddingOptions::FastRandomProjection(FastRpOptions {
                    dimensions: usize_value("dimensions")?,
                    iteration_weights: invocation_descriptor::required_f64_list(
                        parameters,
                        "iteration_weights",
                    )?,
                    normalization_strength: invocation_descriptor::required_f64(
                        parameters,
                        "normalization_strength",
                    )?,
                    feature_weight: invocation_descriptor::required_f64(
                        parameters,
                        "feature_weight",
                    )?,
                    feature_properties: invocation_descriptor::required_utf8_list(
                        parameters,
                        "feature_properties",
                    )?,
                    seed,
                })
            }
            AnalyzeAlgorithm::HashGnn => EmbeddingOptions::HashGnn(HashGnnOptions {
                dimensions: usize_value("dimensions")?,
                iterations: usize_value("iterations")?,
                embedding_density: invocation_descriptor::required_f64(
                    parameters,
                    "embedding_density",
                )?,
                heterogeneous: invocation_descriptor::required_bool(parameters, "heterogeneous")?,
                node_type_property: {
                    let value =
                        invocation_descriptor::required_utf8(parameters, "node_type_property")?;
                    (!value.is_empty()).then(|| value.to_owned())
                },
                relationship_type_property: {
                    let value = invocation_descriptor::required_utf8(
                        parameters,
                        "relationship_type_property",
                    )?;
                    (!value.is_empty()).then(|| value.to_owned())
                },
                seed,
            }),
            _ => unreachable!("non-embedding analyze algorithms were rejected above"),
        };
        let empty_to_none = |value: &str| (!value.is_empty()).then(|| value.to_owned());
        let label = invocation_descriptor::required_utf8(parameters, "label")?;
        let via = invocation_descriptor::required_utf8(parameters, "via")?;
        let weight = invocation_descriptor::required_utf8(parameters, "weight")?;
        let options = EmbeddingAnalyzeOptions {
            by,
            via: (via != "*").then(|| via.to_owned()),
            directed: invocation_descriptor::required_bool(parameters, "directed")?,
            weight: empty_to_none(weight),
            options: embedding_options,
        };
        let current =
            self.prepare_embedding_invocation(empty_to_none(label).as_deref(), &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(
            descriptor,
            self.analyze_embedding(empty_to_none(label).as_deref(), &options)?,
        )
    }

    /// Prepare a canonical, knowledge-neutral structural-analysis descriptor.
    ///
    /// # Errors
    /// Returns a structured graph or descriptor failure.
    pub fn prepare_analyze_invocation(
        &self,
        label: Option<&str>,
        options: &AnalyzeOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        let label_id = label
            .map(|value| self.algorithm_label(value, "analyze").map(|(id, _)| id))
            .transpose()?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        let projection = graphforge_exec::analyze_projection_fingerprint(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            label_id,
            options,
        )?;
        let mut parameters = std::collections::BTreeMap::from([
            (
                "directed".into(),
                InvocationParameter::Bool(options.directed),
            ),
            (
                "via".into(),
                InvocationParameter::Utf8(options.via.clone().unwrap_or_else(|| "*".into())),
            ),
        ]);
        if let Some(value) = label {
            parameters.insert("label".into(), InvocationParameter::Utf8(value.to_owned()));
        }
        if let Some(value) = &options.weight {
            parameters.insert("weight".into(), InvocationParameter::Utf8(value.clone()));
        }
        if let Some(value) = options.k {
            parameters.insert(
                "k".into(),
                InvocationParameter::U64(u64::try_from(value).map_err(|_| {
                    InvocationDescriptorError::Invalid("analyze k exceeds UInt64".into())
                })?),
            );
        }
        if let Some(value) = &options.partition_property {
            parameters.insert(
                "partition_property".into(),
                InvocationParameter::Utf8(value.clone()),
            );
        }
        InvocationDescriptor::new(Algorithm::Analyze(options.by), projection, parameters)
            .map_err(Into::into)
    }

    /// Dispatch a prepared analysis descriptor through [`Self::analyze`].
    ///
    /// # Errors
    /// Returns a structured descriptor, projection, graph, or execution failure.
    pub fn invoke_analyze_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Analyze(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "analyze dispatch requires an analyze descriptor".into(),
            )
            .into());
        };
        let parameters = descriptor.parameters();
        let label = invocation_descriptor::optional_utf8(parameters, "label")?;
        let via = invocation_descriptor::required_utf8(parameters, "via")?;
        let options = AnalyzeOptions {
            by,
            via: (via != "*").then(|| via.to_owned()),
            directed: invocation_descriptor::required_bool(parameters, "directed")?,
            weight: invocation_descriptor::optional_utf8(parameters, "weight")?,
            k: invocation_descriptor::optional_u64(parameters, "k")?
                .map(|value| {
                    usize::try_from(value).map_err(|_| {
                        InvocationDescriptorError::Invalid("analyze k exceeds usize".into())
                    })
                })
                .transpose()?,
            partition_property: invocation_descriptor::optional_utf8(
                parameters,
                "partition_property",
            )?,
        };
        let current = self.prepare_analyze_invocation(label.as_deref(), &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(descriptor, self.analyze(label.as_deref(), options)?)
    }

    /// Prepare a canonical, knowledge-neutral paths descriptor.
    ///
    /// Node selectors are resolved to stable UUIDs before canonicalization.
    ///
    /// # Errors
    /// Returns a structured selector, projection, option, or descriptor failure.
    pub fn prepare_paths_invocation(
        &self,
        source: Option<&NodeSelector>,
        target: Option<&NodeSelector>,
        options: &PathsOptions,
    ) -> Result<InvocationDescriptor, InvocationError> {
        if matches!(
            options.by,
            PathAlgorithm::MinSteinerTree
                | PathAlgorithm::PrizeCollectingSteinerTree
                | PathAlgorithm::GomoryHuTree
        ) && (source.is_some() || target.is_some())
        {
            return Err(GfError::Validation(format!(
                "{} does not accept positional source or target selectors",
                options.by
            ))
            .into());
        }
        let source = source
            .map(|selector| self.resolve_node_selector(selector))
            .transpose()?
            .map(|uuid| *uuid.as_bytes());
        let target = target
            .map(|selector| self.resolve_node_selector(selector))
            .transpose()?
            .map(|uuid| *uuid.as_bytes());
        let mut normalized = options.clone();
        normalized.terminal_uuids.sort_unstable();
        normalized.terminal_uuids.dedup();
        if normalized.by == PathAlgorithm::RandomWalk {
            normalized.walk_length = Some(normalized.walk_length.unwrap_or(10));
            normalized.seed = Some(normalized.seed.unwrap_or(0));
        }
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        let projection = graphforge_exec::paths_projection_fingerprint(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            source,
            target,
            &normalized,
        )?;
        let mut parameters = std::collections::BTreeMap::from([
            (
                "directed".into(),
                InvocationParameter::Bool(normalized.directed),
            ),
            (
                "k".into(),
                InvocationParameter::U64(u64::try_from(normalized.k).map_err(|_| {
                    InvocationDescriptorError::Invalid("paths k exceeds UInt64".into())
                })?),
            ),
            (
                "via".into(),
                InvocationParameter::Utf8(normalized.via.clone().unwrap_or_else(|| "*".into())),
            ),
        ]);
        if let Some(value) = source {
            parameters.insert("source_uuid".into(), InvocationParameter::Uuid(value));
        }
        if let Some(value) = target {
            parameters.insert("target_uuid".into(), InvocationParameter::Uuid(value));
        }
        for (name, value) in [
            ("weight", normalized.weight.as_ref()),
            ("capacity_property", normalized.capacity_property.as_ref()),
            ("cost_property", normalized.cost_property.as_ref()),
            ("heuristic", normalized.heuristic.as_ref()),
            ("prize_property", normalized.prize_property.as_ref()),
        ] {
            if let Some(value) = value {
                parameters.insert(name.into(), InvocationParameter::Utf8(value.clone()));
            }
        }
        if let Some(value) = normalized.walk_length {
            parameters.insert(
                "walk_length".into(),
                InvocationParameter::U64(u64::try_from(value).map_err(|_| {
                    InvocationDescriptorError::Invalid("walk length exceeds UInt64".into())
                })?),
            );
        }
        if let Some(value) = normalized.seed {
            parameters.insert("seed".into(), InvocationParameter::U64(value));
        }
        if !normalized.terminal_uuids.is_empty() {
            parameters.insert(
                "terminal_uuids".into(),
                InvocationParameter::UuidList(normalized.terminal_uuids),
            );
        }
        InvocationDescriptor::new(Algorithm::Paths(normalized.by), projection, parameters)
            .map_err(Into::into)
    }

    /// Dispatch a prepared paths descriptor through [`Self::paths`].
    ///
    /// # Errors
    /// Returns a structured descriptor, projection, selector, graph, or execution failure.
    pub fn invoke_paths_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        let _graph_visibility = self.graph_visibility.lock()?;
        let Algorithm::Paths(by) = descriptor.algorithm() else {
            return Err(InvocationDescriptorError::Invalid(
                "paths dispatch requires a paths descriptor".into(),
            )
            .into());
        };
        let parameters = descriptor.parameters();
        let source = invocation_descriptor::optional_uuid(parameters, "source_uuid")?
            .map(|value| NodeSelector::Uuid(uuid::Uuid::from_bytes(value)));
        let target = invocation_descriptor::optional_uuid(parameters, "target_uuid")?
            .map(|value| NodeSelector::Uuid(uuid::Uuid::from_bytes(value)));
        let via = invocation_descriptor::required_utf8(parameters, "via")?;
        let options = PathsOptions {
            by,
            via: (via != "*").then(|| via.to_owned()),
            directed: invocation_descriptor::required_bool(parameters, "directed")?,
            k: usize::try_from(invocation_descriptor::required_u64(parameters, "k")?)
                .map_err(|_| InvocationDescriptorError::Invalid("paths k exceeds usize".into()))?,
            weight: invocation_descriptor::optional_utf8(parameters, "weight")?,
            capacity_property: invocation_descriptor::optional_utf8(
                parameters,
                "capacity_property",
            )?,
            cost_property: invocation_descriptor::optional_utf8(parameters, "cost_property")?,
            heuristic: invocation_descriptor::optional_utf8(parameters, "heuristic")?,
            walk_length: invocation_descriptor::optional_u64(parameters, "walk_length")?
                .map(|value| {
                    usize::try_from(value).map_err(|_| {
                        InvocationDescriptorError::Invalid("walk length exceeds usize".into())
                    })
                })
                .transpose()?,
            seed: invocation_descriptor::optional_u64(parameters, "seed")?,
            terminal_uuids: invocation_descriptor::optional_uuid_list(
                parameters,
                "terminal_uuids",
            )?
            .unwrap_or_default(),
            prize_property: invocation_descriptor::optional_utf8(parameters, "prize_property")?,
        };
        let current = self.prepare_paths_invocation(source.as_ref(), target.as_ref(), &options)?;
        if current.projection_fingerprint() != descriptor.projection_fingerprint() {
            return Err(InvocationError::ProjectionChanged);
        }
        invocation_descriptor::validate_result(
            descriptor,
            self.paths(source.as_ref(), target.as_ref(), options)?,
        )
    }

    /// Dispatch any prepared neutral algorithm descriptor through its owning verb.
    ///
    /// # Errors
    /// Returns the same structured descriptor, projection, graph, or execution
    /// failure as the typed verb-specific dispatch method.
    pub fn invoke_descriptor(
        &self,
        descriptor: &InvocationDescriptor,
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        match descriptor.algorithm() {
            Algorithm::Rank(_) => self.invoke_rank_descriptor(descriptor),
            Algorithm::Cluster(_) => self.invoke_cluster_descriptor(descriptor),
            Algorithm::Paths(_) => self.invoke_paths_descriptor(descriptor),
            Algorithm::Analyze(
                AnalyzeAlgorithm::Node2Vec
                | AnalyzeAlgorithm::GraphSage
                | AnalyzeAlgorithm::FastRandomProjection
                | AnalyzeAlgorithm::HashGnn,
            ) => self.invoke_embedding_descriptor(descriptor),
            Algorithm::Analyze(_) => self.invoke_analyze_descriptor(descriptor),
            Algorithm::Similar(_) => self.invoke_similar_descriptor(descriptor),
        }
    }

    /// Decode canonical bytes and dispatch the resulting neutral descriptor.
    ///
    /// # Errors
    /// Rejects malformed, non-canonical, unknown-version, or changed-projection
    /// descriptors before kernel execution.
    pub fn invoke_descriptor_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<arrow::record_batch::RecordBatch, InvocationError> {
        let descriptor = InvocationDescriptor::from_canonical_bytes(bytes)?;
        self.invoke_descriptor(&descriptor)
    }

    /// Rank nodes by a structural algorithm through the Rust-only registry.
    ///
    /// `degree` normalizes adjacency-entry counts by `max(selected_nodes - 1,
    /// 1)`. It counts outgoing entries when `directed` is true (the default)
    /// and both endpoints when false. Parallel edges count separately; an
    /// undirected self-loop contributes two. Rows follow stable topology order
    /// and expose only UUID identity. `via=None` selects every relation.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for malformed selectors or a rank
    /// algorithm without a registered Rust implementation, and structured
    /// execution/storage failures from adjacency, limits, shaping, or atomic
    /// opt-in write-back.
    pub fn rank(
        &self,
        label: &str,
        options: RankOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let _admission = self.admit_heavy_query()?;
        let RankOptions {
            by,
            via,
            directed,
            write_property,
        } = options;
        let dispatch_options = RankOptions {
            by,
            via,
            directed,
            write_property: None,
        };
        let _graph_visibility = write_property
            .as_ref()
            .map(|_| self.graph_visibility.lock())
            .transpose()?;
        let (label_id, stem) = self.algorithm_label(label, "rank")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        let batch = graphforge_exec::rank_algorithm_with_compute(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            label_id,
            std::slice::from_ref(&stem),
            &dispatch_options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )?;
        self.write_algorithm_property(
            label,
            &stem,
            Algorithm::Rank(by),
            write_property.as_deref(),
            &batch,
        )?;
        Ok(batch)
    }

    /// Cluster nodes through the Rust-only algorithm registry.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for malformed labels/selectors or a
    /// cluster algorithm without a registered Rust implementation, and
    /// structured execution/storage failures from adjacency, limits, shaping,
    /// or atomic opt-in write-back.
    pub fn cluster(
        &self,
        label: &str,
        options: ClusterOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let ClusterOptions {
            by,
            vector_property,
            via,
            directed,
            write_property,
        } = options;
        let dispatch_options = ClusterOptions {
            by,
            vector_property,
            via,
            directed,
            write_property: None,
        };
        let _graph_visibility = write_property
            .as_ref()
            .map(|_| self.graph_visibility.lock())
            .transpose()?;
        let (label_id, stem) = self.algorithm_label(label, "cluster")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        let batch = graphforge_exec::cluster_algorithm_with_compute(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            label_id,
            std::slice::from_ref(&stem),
            &dispatch_options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )?;
        self.write_algorithm_property(
            label,
            &stem,
            Algorithm::Cluster(by),
            write_property.as_deref(),
            &batch,
        )?;
        Ok(batch)
    }

    /// Find paths / flows between nodes selected by UUID, handle, or property.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for a missing, ambiguous, malformed, or
    /// cross-graph selector, then [`GfError::NotImplemented`] until the selected
    /// path algorithm ships.
    pub fn paths<'a>(
        &self,
        source: impl Into<Option<&'a NodeSelector>>,
        target: Option<&NodeSelector>,
        options: PathsOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let source = source.into();
        if matches!(
            options.by,
            PathAlgorithm::MinSteinerTree
                | PathAlgorithm::PrizeCollectingSteinerTree
                | PathAlgorithm::GomoryHuTree
        ) && (source.is_some() || target.is_some())
        {
            return Err(GfError::Validation(format!(
                "{} does not accept positional source or target selectors",
                options.by
            )));
        }
        let source = source
            .map(|selector| self.resolve_node_selector(selector))
            .transpose()?;
        let target = target
            .map(|selector| self.resolve_node_selector(selector))
            .transpose()?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        graphforge_exec::paths_algorithm_with_compute(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            source.map(|uuid| *uuid.as_bytes()),
            target.map(|uuid| *uuid.as_bytes()),
            options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )
    }

    /// Compute a graph-level structural metric (spanning trees, DAG checks,
    /// coloring, embeddings, …).
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for malformed labels/options or an
    /// analysis algorithm without a registered Rust implementation, and
    /// structured execution/storage failures from adjacency, limits, or shaping.
    pub fn analyze(
        &self,
        label: Option<&str>,
        options: AnalyzeOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let dispatch_options = options;
        let label_id = label
            .map(|value| self.algorithm_label(value, "analyze").map(|(id, _)| id))
            .transpose()?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        graphforge_exec::analyze_algorithm_with_compute(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            label_id,
            &dispatch_options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )
    }

    /// Compute one graph-native node embedding through an activated Rust kernel.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for malformed labels or typed options,
    /// [`GfError::NotImplemented`] for embedding values whose native kernel has
    /// not shipped, and structured projection, resource, execution, or shaping
    /// failures.
    pub fn analyze_embedding(
        &self,
        label: Option<&str>,
        options: &EmbeddingAnalyzeOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let _admission = self.admit_heavy_query()?;
        let label_id = label
            .map(|value| self.algorithm_label(value, "analyze").map(|(id, _)| id))
            .transpose()?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        graphforge_exec::embedding_algorithm_execution_with_compute(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            label_id,
            label,
            options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )
        .map(|execution| execution.result)
    }

    /// Compute pairwise node similarity through the Rust-only algorithm registry.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] for malformed options or a similarity
    /// algorithm without a registered Rust implementation, and structured
    /// execution/storage failures from adjacency, limits, or result shaping.
    pub fn similar(
        &self,
        label: &str,
        options: SimilarOptions,
    ) -> Result<arrow::record_batch::RecordBatch, GfError> {
        let _admission = self.admit_heavy_query()?;
        let (label_id, stem) = self.algorithm_label(label, "similar")?;
        let _adjacency_visibility = self
            .adjacency_visibility
            .read()
            .expect("adjacency visibility lock poisoned");
        self.adjacency_provider.revalidate();
        graphforge_exec::similar_algorithm_with_compute(
            self.adjacency_provider.as_ref(),
            &self.dir,
            self.ontology_mode,
            label_id,
            std::slice::from_ref(&stem),
            options,
            graphforge_exec::AlgorithmLimits::default()
                .with_batch_size(self.resource_policy.batch_size)
                .with_compute_threads(self.resource_policy.compute_threads),
            Some(self.compute_pool.clone()),
        )
    }

    /// Build the legacy adjacency index compatibility entry point.
    ///
    /// New Rust callers should use [`Self::index_adjacency`] for adjacency or
    /// [`Self::index_search`] with typed [`SearchIndexOptions`] for search.
    /// Search labels never route through this string-only compatibility API.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] if the adjacency build fails, or
    /// [`GfError::NotImplemented`] for every other string.
    pub fn index(&self, label: &str) -> Result<(), GfError> {
        if label == "adjacency" {
            return self.index_adjacency().map(|_| ());
        }
        Err(GfError::NotImplemented("index"))
    }

    /// Return deterministic label and relationship counts as an Arrow batch.
    ///
    /// Label rows precede relationship rows. The unrelated column pair is null,
    /// and both sections are ordered lexically.
    ///
    /// # Errors
    /// Returns a structured project, execution, or schema error if the committed
    /// graph generation cannot be inspected.
    pub fn schema(&self) -> Result<arrow::record_batch::RecordBatch, GfError> {
        self.inspect_graph()?.into_record_batch()
    }

    /// Return all node label strings.
    ///
    /// # Errors
    /// Returns a structured project, execution, or schema error if the committed
    /// graph generation cannot be inspected.
    pub fn labels(&self) -> Result<Vec<String>, GfError> {
        Ok(self.inspect_graph()?.labels())
    }

    /// Return all relationship type strings.
    ///
    /// # Errors
    /// Returns a structured project, execution, or schema error if the committed
    /// graph generation cannot be inspected.
    pub fn relationship_types(&self) -> Result<Vec<String>, GfError> {
        Ok(self.inspect_graph()?.relationship_types())
    }

    /// Return the total node count for an empty label, or the exact count for a label.
    ///
    /// # Errors
    /// Returns a structured project, execution, or schema error if the committed
    /// graph generation cannot be inspected.
    pub fn node_count(&self, label: &str) -> Result<u64, GfError> {
        Ok(self.inspect_graph()?.node_count(label))
    }

    /// Return a human-readable explanation of every compiler stage for `cypher`:
    /// `AST` → `GraphIR` → `LogicalPlan` → `PhysicalPlan`.
    ///
    /// The query is bound once with this instance's ontology and mode, against a
    /// **snapshot** of the runtime catalog — so `explain` is side-effect-free
    /// (unlike `execute`, it does not grow the shared catalog) while still
    /// reflecting the types this query would intern.
    ///
    /// # Errors
    /// Returns [`GfError::Parse`] for a syntax error, [`GfError::Plan`] for a
    /// bind/serialisation failure, or a storage/execution error if the physical
    /// plan cannot be built.
    pub fn explain(&self, cypher: &str) -> Result<String, GfError> {
        let ast = graphforge_cypher::parse(cypher).map_err(|e| GfError::Parse {
            msg: e.to_string(),
            span: e.span,
        })?;

        // Bind against a clone of the runtime catalog so EXPLAIN never mutates
        // shared state; the snapshot still backs the catalog the physical plan
        // is built over, so types interned during this bind resolve.
        let snapshot = Arc::new(Mutex::new(
            self.runtime_catalog
                .lock()
                .expect("runtime catalog poisoned")
                .clone(),
        ));
        let plan = {
            let binder = Binder::new(
                self.ontology.clone(),
                Arc::clone(&snapshot),
                self.ontology_mode,
            )
            .with_procedures(self.procedure_snapshot());
            binder
                .bind(&ast)
                .map_err(|errors| bind_errors_to_gferror(&errors))?
        };

        let ast_json =
            serde_json::to_string_pretty(&ast).map_err(|e| GfError::Plan(e.to_string()))?;
        let graph_ir =
            serde_json::to_string_pretty(&plan).map_err(|e| GfError::Plan(e.to_string()))?;

        // Open the catalog from the same snapshot so the logical and physical
        // stages resolve property names interned during this bind (a None
        // catalog would render them as `prop_<id>` and fail to lower).
        let catalog = {
            let rc = snapshot.lock().expect("runtime catalog poisoned");
            GraphCatalog::open(&self.dir, self.ontology.as_ref(), &rc)
                .map_err(|e| GfError::Storage(e.to_string()))?
        };
        // Best-effort: some operators (notably variable-length expand) only
        // lower in the dir-backed physical stage, so the dir-less logical
        // renderer errors on them. Don't fail the whole EXPLAIN — the physical
        // section still shows the plan, including the inference rule_id (#605).
        // Write terminals need `new_for_writes` so CREATE/MERGE/DELETE/SET/
        // REMOVE render instead of failing as "requires a write target".
        let logical = {
            let needs_writes = plan.ops.iter().any(|op| {
                matches!(
                    op,
                    GraphOp::Create { .. }
                        | GraphOp::Merge { .. }
                        | GraphOp::Delete { .. }
                        | GraphOp::Set { .. }
                        | GraphOp::Remove { .. }
                )
            });
            let explained = if needs_writes {
                graphforge_rel::explain_logical_for_writes(
                    &plan,
                    Some(&catalog),
                    self.ontology.as_ref(),
                    &self.dir,
                    self.ontology_mode,
                )
            } else {
                graphforge_rel::explain_logical_with_catalog(
                    &plan,
                    Some(&catalog),
                    self.ontology.as_ref(),
                )
            };
            explained.unwrap_or_else(|e| format!("(logical plan unavailable: {e})"))
        };

        let session = graphforge_exec::ExecutionSession::new_with_target_provider_and_resources(
            catalog,
            self.ontology.clone(),
            self.dir.clone(),
            self.ontology_mode,
            Arc::clone(&self.adjacency_provider),
            &self.session_resource_config(),
        )?;
        let physical = self.block_on(async move { session.explain_physical(&plan).await })?;

        Ok(format!(
            "AST\n---\n{ast_json}\n\n\
             GraphIR\n-------\n{graph_ir}\n\n\
             LogicalPlan\n-----------\n{logical}\n\n\
             PhysicalPlan\n------------\n{physical}"
        ))
    }

    /// Load and compile an ontology from `path` (YAML or JSON, dispatched by
    /// file extension) and apply it to this instance: subsequent queries bind
    /// against the declared types. An instance in [`OntologyMode::Exploratory`]
    /// is promoted to [`OntologyMode::Advisory`] so the loaded types take effect
    /// (mirroring the open-time "ontology present ⇒ advisory" rule), and the
    /// adjacency provider is rebuilt at the new mode (the on-disk read layout is
    /// mode-dependent, so a stale provider would scan the wrong edge files).
    ///
    /// **Session-scoped**: the ontology is applied to this live instance only —
    /// it is **not** published to the committed workspace ontology/configuration
    /// records, so reopening a persistent project does not see it. Because the on-disk
    /// layout differs by mode, this is intended for a fresh instance (or before
    /// writing data). Durable authority changes only through
    /// [`adopt_ontology`](Self::adopt_ontology) and
    /// [`clear_ontology`](Self::clear_ontology).
    ///
    /// # Errors
    /// Returns [`GfError::Ontology`] if the file cannot be loaded or compiled.
    pub fn load_ontology(&mut self, path: &str) -> Result<(), GfError> {
        let doc = OntologyLoader::load_file(std::path::Path::new(path))
            .map_err(|e| GfError::Ontology(format!("failed to load ontology: {e}")))?;
        let runtime = OntologyCompiler::compile(&doc)
            .map_err(|e| GfError::Ontology(format!("failed to compile ontology: {e}")))?;
        self.ontology = Some(OntologyHandle::new(runtime));
        self.ontology_document = Some(doc);
        if matches!(self.ontology_mode, OntologyMode::Exploratory) {
            self.ontology_mode = OntologyMode::Advisory;
            // The provider caches the construction-time mode and drives edge
            // reads by it (exploratory `_exploratory.parquet` vs typed
            // `topology/edges/<REL>.parquet`); rebuild it so the adjacency path
            // matches the new mode.
            self.adjacency_provider = Arc::new(graphforge_exec::PersistentAdjacencyProvider::new(
                self.dir.clone(),
                self.ontology_mode,
            ));
        }
        Ok(())
    }

    /// Execute `cypher` and write the result to a Parquet file at `path`.
    ///
    /// A thin sink over [`execute`](Self::execute): the result batches are
    /// written with a single Arrow Parquet writer (the schema — including its
    /// `graphforge.*` metadata — is preserved; a zero-row result writes a valid
    /// schema-only file). The write is **atomic**: batches are written to a
    /// sibling temp file that is renamed onto `path` only after a clean close, so
    /// a mid-write failure never leaves a torn file at the destination.
    ///
    /// # Errors
    /// Propagates any [`execute`](Self::execute) error, or [`GfError::Storage`]
    /// if the file cannot be created, written, or persisted.
    pub fn execute_to_parquet(&self, cypher: &str, path: &str) -> Result<(), GfError> {
        self.execute_to_parquet_with_params(cypher, &HashMap::new(), path)
    }

    /// Params-aware variant of [`execute_to_parquet`](Self::execute_to_parquet):
    /// run `cypher` with `$name` bindings and write the result to `path`.
    ///
    /// # Errors
    /// As [`execute_to_parquet`](Self::execute_to_parquet).
    pub fn execute_to_parquet_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        path: &str,
    ) -> Result<(), GfError> {
        let result = self.execute_with_params(cypher, params)?;
        let dest = std::path::Path::new(path);
        let parent = dest
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map_or_else(
                || std::path::PathBuf::from("."),
                std::path::Path::to_path_buf,
            );
        let tmp = tempfile::NamedTempFile::new_in(&parent)
            .map_err(|e| GfError::Storage(format!("create temp for {path}: {e}")))?;
        {
            // `&File: Write`, so the writer borrows the temp file and `tmp` stays
            // owned for the atomic persist below.
            let mut parquet_metadata = result
                .schema
                .metadata()
                .iter()
                .map(|(key, value)| {
                    parquet::file::metadata::KeyValue::new(key.clone(), Some(value.clone()))
                })
                .collect::<Vec<_>>();
            parquet_metadata.sort_unstable_by(|left, right| left.key.cmp(&right.key));
            let writer_properties = parquet::file::properties::WriterProperties::builder()
                .set_key_value_metadata(Some(parquet_metadata))
                .build();
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                tmp.as_file(),
                Arc::clone(&result.schema),
                Some(writer_properties),
            )
            .map_err(|e| GfError::Storage(e.to_string()))?;
            for batch in &result.batches {
                writer
                    .write(batch)
                    .map_err(|e| GfError::Storage(e.to_string()))?;
            }
            writer
                .close()
                .map_err(|e| GfError::Storage(e.to_string()))?;
        }
        tmp.persist(dest)
            .map_err(|e| GfError::Storage(format!("persist {path}: {e}")))?;
        Ok(())
    }

    /// Return the storage path, if any (`None` for an in-memory instance).
    #[must_use]
    pub fn path(&self) -> Option<&std::path::Path> {
        self.path.as_deref()
    }

    /// The effective [`OntologyMode`] this instance enforces.
    #[must_use]
    pub fn ontology_mode(&self) -> OntologyMode {
        self.ontology_mode
    }

    /// The shared runtime catalog, grown by the binder as queries observe new
    /// labels, relation types, and properties.
    ///
    /// Returned as a cloned `Arc` handle; lock it to inspect interned types
    /// (e.g. `forge.runtime_catalog().lock().unwrap().contains_entity_type("Person")`).
    #[must_use]
    pub fn runtime_catalog(&self) -> Arc<Mutex<RuntimeCatalog>> {
        self.runtime_catalog.clone()
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn materialize_row_count_params(
    plan: &GraphPlan,
    params: &HashMap<String, IrLiteral>,
) -> Result<GraphPlan, GfError> {
    let mut plan = plan.clone();
    if plan_contains_aggregate(&plan) {
        plan.exprs.substitute_parameters(params);
    }
    materialize_row_count_ops(&mut plan.ops, params)?;
    Ok(plan)
}

fn plan_contains_aggregate(plan: &GraphPlan) -> bool {
    plan.ops.iter().any(|op| match op {
        GraphOp::Aggregate { .. } => true,
        GraphOp::Optional { child }
        | GraphOp::Exists { child, .. }
        | GraphOp::PatternComprehension { child, .. }
        | GraphOp::ListElementPatternComprehension { child, .. } => plan_contains_aggregate(child),
        GraphOp::Union { inputs, .. } => inputs.iter().any(plan_contains_aggregate),
        _ => false,
    })
}

fn materialize_row_count_ops(
    ops: &mut [GraphOp],
    params: &HashMap<String, IrLiteral>,
) -> Result<(), GfError> {
    for op in ops {
        match op {
            GraphOp::SkipParam { name } => {
                let count = row_count_param_value("SKIP", name, params)?;
                *op = GraphOp::Skip { count };
            }
            GraphOp::LimitParam { name } => {
                let count = row_count_param_value("LIMIT", name, params)?;
                *op = GraphOp::Limit { count };
            }
            GraphOp::Optional { child }
            | GraphOp::Exists { child, .. }
            | GraphOp::PatternComprehension { child, .. }
            | GraphOp::ListElementPatternComprehension { child, .. } => {
                materialize_row_count_ops(&mut child.ops, params)?;
            }
            GraphOp::Union { inputs, .. } => {
                for input in inputs {
                    materialize_row_count_ops(&mut input.ops, params)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn row_count_param_value(
    keyword: &str,
    name: &str,
    params: &HashMap<String, IrLiteral>,
) -> Result<u64, GfError> {
    match params.get(name) {
        Some(IrLiteral::Int(n)) => u64::try_from(*n).map_err(|_| {
            GfError::Execution(format!(
                "{keyword} parameter `${name}` must be a non-negative integer"
            ))
        }),
        Some(_) => Err(GfError::Execution(format!(
            "{keyword} parameter `${name}` must be an integer"
        ))),
        None => Err(GfError::Execution(format!(
            "missing query parameter `${name}` for {keyword}"
        ))),
    }
}

/// Remap planner-surface failures that the public API classifies as execution
/// errors: unbound query parameters and arithmetic/type coercion mismatches.
fn publicize_query_error(err: GfError) -> GfError {
    match err {
        GfError::Plan(msg) if is_public_execution_plan_failure(&msg) => {
            let msg = msg
                .strip_prefix("Execution error: ")
                .unwrap_or(&msg)
                .to_owned();
            GfError::Execution(msg)
        }
        other => other,
    }
}

fn is_public_execution_plan_failure(msg: &str) -> bool {
    msg.contains("Placeholder '")
        || msg.contains("Placeholder \"$")
        || msg.contains("placeholder with name $")
        || msg.contains("No value found for placeholder")
        || msg.contains("was not provided a value for execution")
        || msg.contains("Cannot coerce")
}

/// Collapse a binder's `Vec<BindError>` into a span-rich [`GfError::Bind`]
/// (#606). The binder collects every problem before returning, so `msg` lists
/// them all; `span` carries the *first* error's location so callers (and the
/// Python/Node bindings) can point at the offending token.
fn bind_errors_to_gferror(errs: &[BindError]) -> GfError {
    if let Some(error) = errs
        .iter()
        .filter(|error| {
            error.kind == graphforge_ir::BindErrorKind::InvalidArgument
                && error.message.starts_with("typed UUID parameter `$")
        })
        .min_by_key(|error| (error.span.start, error.message.as_str()))
    {
        return GfError::Validation(error.message.clone());
    }
    let span = errs.first().map_or(Span::default(), |e| e.span);
    let msg = errs
        .iter()
        .map(|e| e.message.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    // `GfError::Bind`'s Display already prefixes "bind error at <span>: ", so
    // `msg` carries only the joined binder messages.
    GfError::Bind { msg, span }
}

fn validate_typed_parameter_binding(
    query: &graphforge_cypher::AstQuery,
    params: &HashMap<String, IrLiteral>,
    ontology: Option<OntologyHandle>,
    runtime_catalog: &Arc<Mutex<RuntimeCatalog>>,
    mode: OntologyMode,
    procedures: Arc<ProcedureRegistry>,
) -> Result<(), GfError> {
    if !params.values().any(ir_literal_contains_uuid) {
        return Ok(());
    }
    let catalog = Arc::new(Mutex::new(
        runtime_catalog
            .lock()
            .expect("runtime catalog poisoned")
            .clone(),
    ));
    Binder::new(ontology, catalog, mode)
        .with_procedures(procedures)
        .with_parameter_literals(params)
        .bind(query)
        .map(|_| ())
        .map_err(|errors| bind_errors_to_gferror(&errors))
}

fn ir_literal_contains_uuid(value: &IrLiteral) -> bool {
    match value {
        IrLiteral::Uuid(_) => true,
        IrLiteral::List(items) => items.iter().any(ir_literal_contains_uuid),
        IrLiteral::Map(entries) => entries
            .iter()
            .any(|(_, value)| ir_literal_contains_uuid(value)),
        _ => false,
    }
}

fn validate_call_params(
    plan: &GraphPlan,
    params: &HashMap<String, IrLiteral>,
) -> Result<(), GfError> {
    for op in &plan.ops {
        if let GraphOp::Call { args, .. } = op {
            for arg in args {
                if let IrExpr::Parameter(name) = plan.exprs.get(*arg)
                    && !params.contains_key(name)
                {
                    return Err(GfError::Bind {
                        msg: format!("MissingParameter: no value supplied for `${name}`"),
                        span: Span::default(),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Seed a [`RuntimeCatalog`] from `topology/runtime_catalog.parquet` if present,
/// else return a fresh one. Missing or empty files yield an empty catalog; a
/// present but malformed / undecodable catalog fails closed so reconciliation
/// cannot write the encoding marker against an incomplete identity map (#702/#725).
fn load_runtime_catalog(dir: &std::path::Path) -> Result<RuntimeCatalog, GfError> {
    let path = dir.join("topology").join("runtime_catalog.parquet");
    if !path.exists() {
        return Ok(RuntimeCatalog::new());
    }
    read_runtime_catalog(&path)
}

/// Read and decode every batch of `runtime_catalog.parquet` into a [`RuntimeCatalog`].
fn read_runtime_catalog(path: &std::path::Path) -> Result<RuntimeCatalog, GfError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path).map_err(|e| {
        GfError::Storage(format!(
            "failed to open runtime catalog {}: {e}",
            path.display()
        ))
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| {
            GfError::Storage(format!("malformed runtime catalog {}: {e}", path.display()))
        })?
        .build()
        .map_err(|e| {
            GfError::Storage(format!("malformed runtime catalog {}: {e}", path.display()))
        })?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| {
            GfError::Storage(format!(
                "failed reading runtime catalog {}: {e}",
                path.display()
            ))
        })?);
    }
    // Zero-row / zero-batch parquet is equivalent to a missing catalog. Fail
    // closed only on malformed or undecodable content.
    if batches.is_empty() {
        return Ok(RuntimeCatalog::new());
    }
    let schema = batches[0].schema();
    let merged = arrow::compute::concat_batches(&schema, &batches).map_err(|e| {
        GfError::Storage(format!(
            "failed to merge runtime catalog batches in {}: {e}",
            path.display()
        ))
    })?;
    RuntimeCatalog::from_record_batch(&merged)
        .map_err(|e| GfError::Storage(format!("invalid runtime catalog {}: {e}", path.display())))
}

/// A long-lived multi-thread Tokio runtime that shuts down **without blocking**
/// on drop.
///
/// `GraphForge` owns this for its lifetime so a streaming query's background
/// tasks (repartition/coalesce) run on worker threads that outlive the call
/// that created the stream — a per-call runtime would cancel them mid-stream.
///
/// Dropping a bare `tokio::runtime::Runtime` from inside an async context panics
/// ("Cannot drop a runtime in a context where blocking is not allowed"), and a
/// `GraphForge` may well be dropped inside someone else's async task (e.g. the
/// cucumber harness). The `Drop` here calls `shutdown_background`, which returns
/// immediately and never blocks, so dropping is safe from any context.
#[derive(Debug)]
struct OwnedRuntime(Option<tokio::runtime::Runtime>);

impl OwnedRuntime {
    fn handle(&self) -> &tokio::runtime::Handle {
        self.0
            .as_ref()
            .expect("runtime present until drop")
            .handle()
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

/// An opaque guard that keeps a [`GraphForge`]'s Tokio runtime and on-disk graph
/// workspace alive after the instance is dropped, so a detached
/// [`execute_stream_owned`](GraphForge::execute_stream_owned) stream can still
/// be driven to completion (e.g. a lazy `pyarrow.RecordBatchReader`, #587).
///
/// Cheap to clone (`Arc` bumps). The runtime shuts down and temp workspaces are
/// removed only once the `GraphForge` and all guards have dropped. Streaming
/// Parquet scans (#339) open fragment paths at pull time, so pinning the
/// workspace is required for the same lifetime contract MemTable planning had.
#[derive(Clone, Debug)]
pub struct RuntimeGuard {
    runtime: Arc<OwnedRuntime>,
    /// Private mutable graph workspace hydrated for this facade (`dir`).
    /// Held solely so `TempDir` cleanup waits until stream consumers finish.
    #[allow(dead_code)]
    workspace: Arc<tempfile::TempDir>,
    /// In-memory project root, when the facade is not path-backed.
    #[allow(dead_code)]
    tempdir: Option<Arc<tempfile::TempDir>>,
}

impl RuntimeGuard {
    /// Drive a future to completion on the guarded runtime from a synchronous
    /// caller.
    ///
    /// `Handle::block_on` panics if the calling thread is already inside a Tokio
    /// runtime, so — mirroring [`GraphForge::block_on`] — detect that and run on
    /// a scoped thread outside any ambient runtime. A panic inside `fut` resumes
    /// on the caller (callers across an FFI boundary must guard with
    /// `catch_unwind`).
    pub fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        let handle = self.runtime.handle().clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::scope(|s| {
                s.spawn(|| handle.block_on(fut))
                    .join()
                    .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
            })
        } else {
            handle.block_on(fut)
        }
    }
}

/// Build the instance's long-lived multi-thread runtime.
fn build_runtime(
    policy: &resource_policy::NormalizedResourcePolicy,
) -> Result<Arc<OwnedRuntime>, GfError> {
    policy
        .build_tokio_runtime()
        .map(|rt| Arc::new(OwnedRuntime(Some(rt))))
}

fn hydrate_graph_workspace(
    generation: &ResolvedProjectGeneration,
    read_only: bool,
) -> Result<
    (
        PathBuf,
        Arc<tempfile::TempDir>,
        graphforge_storage::GraphFilesOpenEvidence,
    ),
    GfError,
> {
    let files = generation.participant_snapshot(
        graphforge_storage::GRAPH_CAPABILITY_ID,
        graphforge_storage::GRAPH_FILES_FAMILY,
    )?;
    let snapshot = generation.participant_snapshot("graph", "snapshot")?;
    if files.is_some() && snapshot.is_some() {
        return Err(GfError::Validation(
            "graph generation cannot declare both snapshot and files participants".into(),
        ));
    }

    if let Some(files) = files {
        if files.capability_version != graphforge_storage::GRAPH_CAPABILITY_VERSION
            || files.record_version != graphforge_storage::GRAPH_FILES_RECORD_VERSION
            || files.encoding != "json"
        {
            return Err(GfError::Validation(
                "unsupported graph files participant contract".into(),
            ));
        }
        let inventory = graphforge_storage::decode_inventory(&files.bytes)?;
        let tree = generation.graph_tree_root();
        graphforge_storage::verify_graph_tree(&tree, &inventory)?;
        if read_only {
            let guard = Arc::new(
                tempfile::Builder::new()
                    .prefix("graphforge-graph-pinned-")
                    .tempdir()
                    .map_err(|error| {
                        GfError::Storage(format!("failed to create graph workspace guard: {error}"))
                    })?,
            );
            return Ok((
                tree,
                guard,
                graphforge_storage::pinned_open_evidence(&inventory),
            ));
        }
        let workspace = Arc::new(
            tempfile::Builder::new()
                .prefix("graphforge-graph-workspace-")
                .tempdir()
                .map_err(|error| {
                    GfError::Storage(format!("failed to create graph workspace: {error}"))
                })?,
        );
        let evidence =
            graphforge_storage::materialize_graph_tree(&tree, &inventory, workspace.path())?;
        return Ok((workspace.path().to_path_buf(), workspace, evidence));
    }

    let workspace = Arc::new(
        tempfile::Builder::new()
            .prefix("graphforge-graph-workspace-")
            .tempdir()
            .map_err(|error| {
                GfError::Storage(format!("failed to create graph workspace: {error}"))
            })?,
    );
    let mut evidence = graphforge_storage::GraphFilesOpenEvidence {
        strategy: graphforge_storage::GraphFilesOpenStrategy::Empty,
        ..graphforge_storage::GraphFilesOpenEvidence::default()
    };
    if let Some(snapshot) = snapshot {
        if snapshot.capability_version != 1
            || snapshot.record_version != 1
            || snapshot.encoding != "arrow"
        {
            return Err(GfError::Validation(
                "unsupported graph snapshot participant contract".into(),
            ));
        }
        graph_snapshot::hydrate(&snapshot.bytes, workspace.path())?;
        evidence.strategy = graphforge_storage::GraphFilesOpenStrategy::LegacySnapshotHydrate;
        evidence.bytes_copied = u64::try_from(snapshot.bytes.len()).unwrap_or(u64::MAX);
        evidence.files_copied = 1;
    }
    Ok((workspace.path().to_path_buf(), workspace, evidence))
}

pub(crate) fn rematerialize_graph_workspace(
    generation: &ResolvedProjectGeneration,
    target: &std::path::Path,
) -> Result<(), GfError> {
    if target.exists() {
        for entry in std::fs::read_dir(target).map_err(|error| {
            GfError::Storage(format!(
                "failed to read graph workspace for restore: {error}"
            ))
        })? {
            let entry = entry.map_err(|error| {
                GfError::Storage(format!("failed to read graph workspace entry: {error}"))
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|error| {
                GfError::Storage(format!("failed to inspect graph workspace entry: {error}"))
            })?;
            if file_type.is_dir() {
                std::fs::remove_dir_all(&path).map_err(|error| {
                    GfError::Storage(format!(
                        "failed to clear graph workspace directory: {error}"
                    ))
                })?;
            } else {
                std::fs::remove_file(&path).map_err(|error| {
                    GfError::Storage(format!("failed to clear graph workspace file: {error}"))
                })?;
            }
        }
    }
    if let Some(inventory) = generation.graph_files_inventory()? {
        graphforge_storage::materialize_graph_tree(
            &generation.graph_tree_root(),
            &inventory,
            target,
        )?;
        return Ok(());
    }
    if let Some(snapshot) = generation.participant_snapshot("graph", "snapshot")? {
        if snapshot.capability_version != 1
            || snapshot.record_version != 1
            || snapshot.encoding != "arrow"
        {
            return Err(GfError::Validation(
                "unsupported graph snapshot participant contract".into(),
            ));
        }
        graph_snapshot::hydrate(&snapshot.bytes, target)?;
    }
    Ok(())
}

fn load_workspace_ontology(
    generation: &ResolvedProjectGeneration,
) -> Result<(OntologyMode, Option<OntologyHandle>, Option<OntologyDoc>), GfError> {
    generation.require_capability(
        graphforge_storage::WORKSPACE_CAPABILITY_ID,
        graphforge_storage::WORKSPACE_CAPABILITY_VERSION,
    )?;
    let ontology_snapshot = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_ONTOLOGY_FAMILY,
        )?
        .ok_or_else(|| {
            GfError::Validation("committed generation is missing workspace ontology".into())
        })?;
    let configuration_snapshot = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_CONFIGURATION_FAMILY,
        )?
        .ok_or_else(|| {
            GfError::Validation("committed generation is missing workspace configuration".into())
        })?;
    if ontology_snapshot.capability_version != 1
        || ontology_snapshot.record_version != 1
        || ontology_snapshot.encoding != "json"
        || configuration_snapshot.capability_version != 1
        || configuration_snapshot.record_version != 1
        || configuration_snapshot.encoding != "json"
    {
        return Err(GfError::Validation(
            "unsupported workspace participant contract".into(),
        ));
    }
    let ontology_record =
        graphforge_storage::WorkspaceOntology::from_canonical_json(&ontology_snapshot.bytes)?;
    let configuration = graphforge_storage::WorkspaceConfiguration::from_canonical_json(
        &configuration_snapshot.bytes,
    )?;
    if ontology_record.mode != configuration.ontology_mode {
        return Err(GfError::Validation(
            "workspace ontology and configuration modes disagree".into(),
        ));
    }
    let mode = ontology_record.mode.execution_mode();
    let document = ontology_record
        .canonical_ontology
        .map(|document| {
            let document: graphforge_ontology::OntologyDoc = serde_json::from_value(document)
                .map_err(|error| GfError::Ontology(format!("invalid adopted ontology: {error}")))?;
            Ok::<OntologyDoc, GfError>(document)
        })
        .transpose()?;
    let ontology = document
        .as_ref()
        .map(|document| {
            let runtime = OntologyCompiler::compile(document).map_err(|error| {
                GfError::Ontology(format!("failed to compile ontology: {error}"))
            })?;
            Ok::<OntologyHandle, GfError>(OntologyHandle::new(runtime))
        })
        .transpose()?;
    Ok((mode, ontology, document))
}

fn participant_encoding(
    value: &str,
) -> Result<graphforge_storage::ProjectParticipantEncoding, GfError> {
    match value {
        "parquet" => Ok(graphforge_storage::ProjectParticipantEncoding::Parquet),
        "arrow" => Ok(graphforge_storage::ProjectParticipantEncoding::Arrow),
        "json" => Ok(graphforge_storage::ProjectParticipantEncoding::Json),
        _ => Err(GfError::Validation(
            "committed participant has unsupported encoding".into(),
        )),
    }
}

fn graph_publication_participants(
    parent: &graphforge_storage::ResolvedProjectGeneration,
    graph: graphforge_storage::ProjectParticipant,
    provenance_enabled: bool,
    receipt: &graphforge_exec::MutationReceipt,
    operation_uuid: uuid::Uuid,
    actor_uuid: Option<uuid::Uuid>,
    recorded_at_micros: i64,
) -> Result<Vec<graphforge_storage::ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "graph"
                && matches!(snapshot.record_family_id.as_str(), "snapshot" | "files")
                || provenance_enabled
                    && snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(|snapshot| {
            Ok(graphforge_storage::ProjectParticipant {
                capability_id: snapshot.capability_id,
                capability_version: snapshot.capability_version,
                record_family_id: snapshot.record_family_id,
                record_version: snapshot.record_version,
                encoding: participant_encoding(&snapshot.encoding)?,
                schema_fingerprint: snapshot.schema_fingerprint,
                row_count: snapshot.row_count,
                bytes: snapshot.bytes,
            })
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    participants.push(graph);
    if provenance_enabled {
        participants.extend(provenance::merged_participants(
            parent,
            receipt,
            operation_uuid,
            actor_uuid,
            recorded_at_micros,
        )?);
    }
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

fn mutation_generation_uuid(
    operation_uuid: uuid::Uuid,
    participants: &[graphforge_storage::ProjectParticipant],
) -> uuid::Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-graph-mutation-generation/1");
    hasher.update(operation_uuid.as_bytes());
    for participant in participants {
        hasher.update(participant.capability_id.as_bytes());
        hasher.update([0]);
        hasher.update(participant.record_family_id.as_bytes());
        hasher.update([0]);
        hasher.update(Sha256::digest(&participant.bytes));
    }
    let digest: [u8; 32] = hasher.finalize().into();
    graphforge_core::canonical::uuid_v8(digest)
}

fn system_time_micros() -> Result<i64, GfError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GfError::Execution("system clock is before Unix epoch".into()))?;
    i64::try_from(duration.as_micros())
        .map_err(|_| GfError::Execution("system clock exceeds microsecond range".into()))
}

/// Shape an [`ExecutionResult`] for the public API:
/// - drop internal surrogate identity columns (provenance-marked / UInt64 scan
///   keys — never by final field name alone; see #703 / #719),
/// - attach query metadata (`graphforge.query_id`, `ontology_version`,
///   `ir_version`, `ontology_mode`) to the schema.
fn shape_result(
    result: ExecutionResult,
    mode: OntologyMode,
    ontology: Option<&OntologyHandle>,
) -> Result<ExecutionResult, GfError> {
    let ExecutionResult {
        schema,
        batches,
        stats,
        side_effects,
        mutation_receipt,
    } = result;
    let shaper = Shaper::new(&schema, mode, ontology);
    let new_batches = batches
        .iter()
        .map(|batch| {
            shaper
                .apply(batch)
                .map_err(|error| GfError::Execution(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ExecutionResult {
        schema: shaper.schema,
        batches: new_batches,
        stats,
        side_effects,
        mutation_receipt,
    })
}

/// Per-batch output shaper: prunes internal surrogate identity columns and
/// re-stamps the public schema (kept fields + query metadata). Built once from
/// the raw result schema, then applied to each batch — shared by the collected
/// ([`shape_result`]) and streaming ([`shape_stream`]) paths.
struct Shaper {
    /// Source-batch column indices to keep, in output order.
    keep: Vec<usize>,
    /// True when at least one internal surrogate column was dropped from the
    /// source schema. Distinguishes surrogate-only projections (#703: preserve
    /// row count) from already-empty schemas such as void `CALL` unit rows
    /// (public result must stay empty for TCK Call1).
    dropped_internal_surrogates: bool,
    /// The pruned, metadata-stamped public schema.
    schema: SchemaRef,
}

impl Shaper {
    fn new(schema: &SchemaRef, mode: OntologyMode, ontology: Option<&OntologyHandle>) -> Self {
        let dropped_internal_surrogates = schema
            .fields()
            .iter()
            .any(|f| graphforge_storage::is_internal_surrogate_field(f));
        let keep: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !graphforge_storage::is_internal_surrogate_field(f))
            .map(|(i, _)| i)
            .collect();
        let kept_fields: Vec<_> = keep.iter().map(|&i| schema.field(i).clone()).collect();
        let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            kept_fields,
            result_metadata(mode, ontology),
        ));
        Self {
            keep,
            dropped_internal_surrogates,
            schema: new_schema,
        }
    }

    fn apply(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError> {
        if self.keep.iter().any(|index| *index >= batch.num_columns()) {
            return Err(arrow::error::ArrowError::SchemaError(format!(
                "result batch has {} columns but shaper requires source indices {:?}",
                batch.num_columns(),
                self.keep
            )));
        }
        let cols: Vec<_> = self.keep.iter().map(|&i| batch.column(i).clone()).collect();
        // Surrogate-only projections must keep their logical row count (#703).
        // Already-empty schemas (void CALL unit rows) must stay publicly empty
        // so TCK Call1 "yields no results" scenarios do not regress.
        let row_count = if self.keep.is_empty() && !self.dropped_internal_surrogates {
            0
        } else {
            batch.num_rows()
        };
        arrow::record_batch::RecordBatch::try_new_with_options(
            self.schema.clone(),
            cols,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
    }
}

/// Apply the public output shaping (UUID-only columns + schema metadata) to a
/// streaming result, mapping each batch as it flows. The returned stream
/// advertises the shaped schema up front (the `RecordBatchReader` contract that
/// the bindings rely on — #587).
fn shape_stream(
    stream: graphforge_exec::SendableRecordBatchStream,
    mode: OntologyMode,
    ontology: Option<&OntologyHandle>,
) -> graphforge_exec::SendableRecordBatchStream {
    use futures::StreamExt;

    let shaper = Shaper::new(&stream.schema(), mode, ontology);
    let out_schema = shaper.schema.clone();
    let mapped = stream.map(move |item| {
        item.and_then(|batch| {
            shaper.apply(&batch).map_err(|error| {
                datafusion::error::DataFusionError::ArrowError(Box::new(error), None)
            })
        })
    });
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(out_schema, mapped))
}

/// Write the runtime catalog to `topology/runtime_catalog.parquet` so a later
/// `GraphForge::new(path)` reloads the types/properties observed this session
/// (#725). Best-effort directory creation; surfaces I/O / Parquet errors.
fn persist_runtime_catalog(dir: &std::path::Path, rc: &RuntimeCatalog) -> Result<(), GfError> {
    use parquet::arrow::ArrowWriter;

    let topology = dir.join("topology");
    std::fs::create_dir_all(&topology)
        .map_err(|e| GfError::Storage(format!("failed to create {}: {e}", topology.display())))?;
    let batch = rc.to_record_batch();
    let path = topology.join("runtime_catalog.parquet");
    let file = std::fs::File::create(&path)
        .map_err(|e| GfError::Storage(format!("failed to write {}: {e}", path.display())))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)
        .map_err(|e| GfError::Storage(e.to_string()))?;
    writer
        .write(&batch)
        .map_err(|e| GfError::Storage(e.to_string()))?;
    writer
        .close()
        .map_err(|e| GfError::Storage(e.to_string()))?;
    // Persisting observed runtime entity labels implies the tagged plan/storage
    // encoding (#702). Mark the project so reopen does not treat ontology type
    // zero as an unmarked legacy collision with the first advisory label.
    graphforge_storage::write_runtime_entity_label_encoding_marker(dir)?;
    Ok(())
}

/// Build the schema-level metadata attached to every public result.
fn result_metadata(
    mode: OntologyMode,
    ontology: Option<&OntologyHandle>,
) -> std::collections::HashMap<String, String> {
    let mut meta = std::collections::HashMap::new();
    meta.insert(
        "graphforge.query_id".to_owned(),
        graphforge_core::uuid::to_string(&graphforge_core::uuid::new_v7()),
    );
    meta.insert(
        "graphforge.ir_version".to_owned(),
        graphforge_ir::IrVersion::CURRENT.to_string(),
    );
    meta.insert(
        "graphforge.ontology_mode".to_owned(),
        format!("{mode:?}").to_lowercase(),
    );
    if let Some(handle) = ontology {
        meta.insert(
            "graphforge.ontology_version".to_owned(),
            format!("{}:{}", handle.version(), handle.checksum()),
        );
    }
    meta
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array,
        Int64Array, ListArray, StringArray, StructArray, UInt64Array,
    };
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    #[test]
    fn facade_debug_empty_batch_and_procedure_width_contracts_are_exact() {
        let empty = RecordBatch::empty(vec!["node_uuid".into(), "name".into()]);
        assert_eq!(empty.schema, ["node_uuid", "name"]);
        assert_eq!(empty.columns, [Vec::<String>::new(), Vec::new()]);

        let graph = GraphForge::new(None).unwrap();
        let debug = format!("{graph:?}");
        for field in [
            "GraphForge",
            "identity",
            "path",
            "generation_uuid",
            "dir",
            "ontology_mode",
            "write_options",
            "has_ontology",
        ] {
            assert!(debug.contains(field), "missing {field:?} in {debug}");
        }
        assert!(debug.contains("has_ontology: false"));

        let error = graph
            .register_procedure(ProcedureDefinition {
                name: "test.bad_width".into(),
                inputs: vec![ProcedureField {
                    name: "input".into(),
                    type_name: "STRING".into(),
                    nullable: false,
                }],
                outputs: vec![],
                rows: vec![vec![]],
            })
            .unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert_eq!(
            error.to_string(),
            "validation error: procedure test.bad_width expects 1 fixture columns, found 0"
        );
    }

    #[test]
    fn facade_label_and_clear_boundaries_are_exact_and_non_mutating() {
        let graph = GraphForge::new(None).unwrap();
        for invalid in ["", " Person", "Person ", "Per\nson", "\0Person"] {
            assert!(matches!(
                graph.algorithm_label(invalid, "rank"),
                Err(GfError::Validation(_))
            ));
        }
        let (unknown, stem) = graph.algorithm_label("Unknown", "rank").unwrap();
        assert_eq!(unknown, TypeId(u32::MAX));
        assert_eq!(stem, "_untyped");

        graph
            .register_procedure(ProcedureDefinition {
                name: "test.clear".into(),
                inputs: vec![],
                outputs: vec![ProcedureField {
                    name: "value".into(),
                    type_name: "INTEGER".into(),
                    nullable: false,
                }],
                rows: vec![vec![IrLiteral::Int(1)]],
            })
            .unwrap();
        graph.add_node("Person", &HashMap::new()).unwrap();
        assert_eq!(graph.node_count("Person").unwrap(), 1);
        graph.clear().unwrap();
        assert_eq!(graph.node_count("Person").unwrap(), 0);
        assert!(graph.labels().unwrap().is_empty());
        assert!(graph.execute("CALL test.clear()").is_err());

        let directory = tempfile::tempdir().unwrap();
        let persistent = GraphForge::new(directory.path().to_str()).unwrap();
        persistent.add_node("Person", &HashMap::new()).unwrap();
        let error = persistent.clear().unwrap_err();
        assert!(matches!(error, GfError::Storage(_)));
        assert_eq!(persistent.node_count("Person").unwrap(), 1);
        drop(persistent);
        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        assert_eq!(reopened.node_count("Person").unwrap(), 1);
    }

    fn degree_options(directed: bool, via: Option<&str>) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::Degree,
            via: via.map(str::to_owned),
            directed,
            write_property: None,
        }
    }

    fn degree_scores(batch: &arrow::record_batch::RecordBatch) -> Vec<f64> {
        batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn betweenness_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::Betweenness,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn closeness_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::Closeness,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn harmonic_closeness_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::HarmonicCloseness,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn eigenvector_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::Eigenvector,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn article_rank_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::ArticleRank,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn hits_hub_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::HitsHub,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn hits_authority_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::HitsAuthority,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn celf_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::Celf,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn clustering_coefficient_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::ClusteringCoefficient,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn triangles_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::Triangles,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn k_core_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::KCore,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn preferential_attachment_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::PreferentialAttachment,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn adamic_adar_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::AdamicAdar,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn common_neighbors_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::CommonNeighbors,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn resource_allocation_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::ResourceAllocation,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn total_neighbors_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> RankOptions {
        RankOptions {
            by: RankAlgorithm::TotalNeighbors,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn assert_rank_scores_close(batch: &arrow::record_batch::RecordBatch, expected: &[f64]) {
        let actual = degree_scores(batch);
        assert_eq!(actual.len(), expected.len());
        assert!(
            actual
                .iter()
                .zip(expected)
                .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-12)
        );
    }

    fn components_options(directed: bool, via: Option<&str>) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::Components,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: None,
        }
    }

    fn strongly_connected_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::StronglyConnected,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn biconnected_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::Biconnected,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn k_core_decomposition_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::KCoreDecomposition,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn approximate_max_cut_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::ApproximateMaxKCut,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn louvain_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::Louvain,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn leiden_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::Leiden,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn label_propagation_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::LabelPropagation,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn speaker_listener_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::SpeakerListener,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn girvan_newman_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::GirvanNewman,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn modularity_optimization_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::ModularityOptimization,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn fastgreedy_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::FastGreedy,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn infomap_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::InfoMap,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn leading_eigenvector_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::LeadingEigenvector,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn walktrap_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::Walktrap,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn spinglass_options(
        directed: bool,
        via: Option<&str>,
        write_property: Option<&str>,
    ) -> ClusterOptions {
        ClusterOptions {
            by: ClusterAlgorithm::Spinglass,
            vector_property: None,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        }
    }

    fn community_ids(batch: &arrow::record_batch::RecordBatch) -> Vec<i64> {
        batch
            .column_by_name("community_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn node_similarity_options(k: usize, via: Option<&str>) -> SimilarOptions {
        SimilarOptions {
            by: SimilarAlgorithm::NodeSimilarity,
            k,
            vector_property: None,
            via: via.map(str::to_owned),
        }
    }

    fn filtered_node_similarity_options(k: usize, via: Option<&str>) -> SimilarOptions {
        SimilarOptions {
            by: SimilarAlgorithm::FilteredNodeSimilarity,
            k,
            vector_property: None,
            via: via.map(str::to_owned),
        }
    }

    fn knn_options(k: usize, vector_property: Option<&str>) -> SimilarOptions {
        SimilarOptions {
            by: SimilarAlgorithm::Knn,
            k,
            vector_property: vector_property.map(str::to_owned),
            via: None,
        }
    }

    fn cosine_options(k: usize, vector_property: Option<&str>) -> SimilarOptions {
        SimilarOptions {
            by: SimilarAlgorithm::Cosine,
            k,
            vector_property: vector_property.map(str::to_owned),
            via: None,
        }
    }

    fn filtered_knn_options(
        k: usize,
        vector_property: Option<&str>,
        via: Option<&str>,
    ) -> SimilarOptions {
        SimilarOptions {
            by: SimilarAlgorithm::FilteredKnn,
            k,
            vector_property: vector_property.map(str::to_owned),
            via: via.map(str::to_owned),
        }
    }

    fn bfs_options(directed: bool, via: Option<&str>) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::Bfs,
            directed,
            k: 1,
            via: via.map(str::to_owned),
            weight: None,
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn random_walk_options(k: usize, walk_length: usize, seed: u64) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::RandomWalk,
            directed: true,
            k,
            via: Some("KNOWS".into()),
            weight: None,
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: Some(walk_length),
            seed: Some(seed),
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn max_flow_options(by: PathAlgorithm, weight: Option<&str>) -> PathsOptions {
        PathsOptions {
            by,
            directed: true,
            k: 1,
            via: Some("PIPE".into()),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn min_cut_options(by: PathAlgorithm, directed: bool, weight: Option<&str>) -> PathsOptions {
        PathsOptions {
            by,
            directed,
            k: 1,
            via: Some("PIPE".into()),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn min_cost_flow_options(by: PathAlgorithm, directed: bool) -> PathsOptions {
        PathsOptions {
            by,
            directed,
            k: 1,
            via: Some("PIPE".into()),
            weight: None,
            capacity_property: Some("capacity".into()),
            cost_property: Some("cost".into()),
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn gomory_hu_options(weight: Option<&str>) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::GomoryHuTree,
            directed: false,
            k: 1,
            via: Some("PIPE".into()),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn min_steiner_options(terminals: &[&NodeHandle], weight: Option<&str>) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::MinSteinerTree,
            directed: false,
            k: 1,
            via: Some("ROAD".into()),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: terminals.iter().map(|node| *node.uuid.as_bytes()).collect(),
            prize_property: None,
        }
    }

    fn prize_steiner_options(
        terminals: &[&NodeHandle],
        weight: Option<&str>,
        prize_property: &str,
    ) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::PrizeCollectingSteinerTree,
            directed: false,
            k: 1,
            via: Some("ROAD".into()),
            weight: weight.map(str::to_owned),
            terminal_uuids: terminals.iter().map(|node| *node.uuid.as_bytes()).collect(),
            prize_property: Some(prize_property.into()),
            ..PathsOptions::default()
        }
    }

    fn uuid_column<'a>(
        batch: &'a arrow::record_batch::RecordBatch,
        name: &str,
    ) -> &'a FixedSizeBinaryArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap()
    }

    fn float_column<'a>(
        batch: &'a arrow::record_batch::RecordBatch,
        name: &str,
    ) -> &'a Float64Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap()
    }

    fn ordered_uuid_pair(left: [u8; 16], right: [u8; 16]) -> ([u8; 16], [u8; 16]) {
        if left <= right {
            (left, right)
        } else {
            (right, left)
        }
    }

    fn dfs_options(directed: bool, via: Option<&str>) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::Dfs,
            directed,
            k: 1,
            via: via.map(str::to_owned),
            weight: None,
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn dijkstra_options(directed: bool, via: Option<&str>, weight: Option<&str>) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::Dijkstra,
            directed,
            k: 1,
            via: via.map(str::to_owned),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn dijkstra_all_pairs_options(
        directed: bool,
        via: Option<&str>,
        weight: Option<&str>,
    ) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::DijkstraAllPairs,
            directed,
            k: 1,
            via: via.map(str::to_owned),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn astar_options(
        directed: bool,
        via: Option<&str>,
        weight: Option<&str>,
        heuristic: Option<&str>,
    ) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::AStar,
            directed,
            k: 1,
            via: via.map(str::to_owned),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: heuristic.map(str::to_owned),
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn bellman_ford_options(
        directed: bool,
        via: Option<&str>,
        weight: Option<&str>,
    ) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::BellmanFord,
            directed,
            k: 1,
            via: via.map(str::to_owned),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn delta_stepping_options(
        directed: bool,
        via: Option<&str>,
        weight: Option<&str>,
    ) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::DeltaStepping,
            directed,
            k: 1,
            via: via.map(str::to_owned),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn yens_options(
        directed: bool,
        k: usize,
        via: Option<&str>,
        weight: Option<&str>,
    ) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::Yens,
            directed,
            k,
            via: via.map(str::to_owned),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn floyd_warshall_options(
        directed: bool,
        via: Option<&str>,
        weight: Option<&str>,
    ) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::FloydWarshall,
            directed,
            k: 1,
            via: via.map(str::to_owned),
            weight: weight.map(str::to_owned),
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    fn transitive_closure_options(directed: bool, via: Option<&str>) -> PathsOptions {
        PathsOptions {
            by: PathAlgorithm::TransitiveClosure,
            directed,
            k: 1,
            via: via.map(str::to_owned),
            weight: None,
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        }
    }

    #[test]
    fn steiner_algorithms_reject_positional_endpoints() {
        let graph = GraphForge::new(None).unwrap();
        let alice = add_person(&graph, "Alice");
        let bob = add_person(&graph, "Bob");
        assert!(matches!(
            graph.paths(
                &NodeSelector::Handle(alice.clone()),
                None,
                PathsOptions {
                    by: PathAlgorithm::PrizeCollectingSteinerTree,
                    directed: false,
                    terminal_uuids: vec![*alice.uuid.as_bytes(), *bob.uuid.as_bytes()],
                    prize_property: Some("prize".into()),
                    ..PathsOptions::default()
                },
            ),
                Err(GfError::Validation(message))
                    if message == "prize_collecting_steiner_tree does not accept positional source or target selectors"
        ));

        assert!(matches!(
            graph.paths(None, None, bfs_options(true, None)),
            Err(GfError::Validation(message)) if message == "bfs requires a source selector"
        ));
        let missing = NodeSelector::Match {
            label: "Person".into(),
            property: "name".into(),
            value: PropValue::Str("Missing".into()),
        };
        assert!(matches!(
            graph.paths(
                &missing,
                None,
                PathsOptions {
                    by: PathAlgorithm::MinSteinerTree,
                    directed: false,
                    terminal_uuids: vec![*alice.uuid.as_bytes(), *bob.uuid.as_bytes()],
                    ..PathsOptions::default()
                },
            ),
            Err(GfError::Validation(message))
                if message == "min_steiner_tree does not accept positional source or target selectors"
        ));
        assert!(matches!(
            graph.paths(
                None,
                Some(&NodeSelector::Handle(bob.clone())),
                min_steiner_options(&[&alice, &bob], None),
            ),
            Err(GfError::Validation(message))
                if message == "min_steiner_tree does not accept positional source or target selectors"
        ));
    }

    #[test]
    fn gomory_hu_persists_canonical_weighted_forest_and_closed_contract() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let nodes = ["A", "B", "C", "Isolated"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}) \
                 CREATE (a)-[:PIPE {capacity:3.0, bad:3.0}]->(b), \
                 (a)-[:PIPE {capacity:1.0, bad:1.0}]->(b), \
                 (a)-[:PIPE {capacity:2.0, bad:-1.0}]->(c), \
                 (c)-[:PIPE {capacity:1.0, bad:1.0}]->(a), \
                 (b)-[:PIPE {capacity:4.0, bad:4.0}]->(c), \
                 (a)-[:PIPE {capacity:99.0, bad:0.0}]->(a), \
                 (a)-[:OTHER {capacity:99.0}]->(c)",
            )
            .unwrap();
        drop(graph);

        let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let weighted_options = gomory_hu_options(Some("capacity"));
        let weighted = reopened
            .paths(None, None, weighted_options.clone())
            .unwrap();
        assert_eq!(
            weighted,
            reopened
                .paths(None, None, weighted_options.clone())
                .unwrap()
        );
        assert_eq!(weighted.num_rows(), 2);
        assert_eq!(
            weighted
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("target_uuid", &DataType::FixedSizeBinary(16), false),
                ("cut_value", &DataType::Float64, false),
            ]
        );
        assert_eq!(
            weighted.schema().metadata()["graphforge.algorithm"],
            "gomory_hu_tree"
        );
        assert_eq!(weighted.schema().metadata()["graphforge.verb"], "paths");
        assert_eq!(
            weighted.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        let sources = uuid_column(&weighted, "source_uuid");
        let targets = uuid_column(&weighted, "target_uuid");
        assert!((0..weighted.num_rows()).all(|row| sources.value(row) < targets.value(row)));
        assert!((0..weighted.num_rows() - 1).all(|row| {
            (sources.value(row), targets.value(row))
                < (sources.value(row + 1), targets.value(row + 1))
        }));
        let mut cuts = float_column(&weighted, "cut_value").values().to_vec();
        cuts.sort_by(f64::total_cmp);
        assert_eq!(cuts, vec![7.0, 7.0]);

        let unit = reopened.paths(None, None, gomory_hu_options(None)).unwrap();
        assert_eq!(float_column(&unit, "cut_value").values(), &[3.0, 3.0]);
        assert!(matches!(
            reopened.paths(
                &NodeSelector::Handle(nodes[0].clone()),
                None,
                weighted_options.clone(),
            ),
            Err(GfError::Validation(message))
                if message
                    == "gomory_hu_tree does not accept positional source or target selectors"
        ));
        for invalid in [
            PathsOptions {
                directed: true,
                ..weighted_options.clone()
            },
            PathsOptions {
                k: 2,
                ..weighted_options.clone()
            },
            PathsOptions {
                heuristic: Some("capacity".into()),
                ..weighted_options.clone()
            },
            PathsOptions {
                capacity_property: Some("capacity".into()),
                ..weighted_options.clone()
            },
            PathsOptions {
                terminal_uuids: vec![[0; 16]],
                ..weighted_options.clone()
            },
            PathsOptions {
                prize_property: Some("capacity".into()),
                ..weighted_options.clone()
            },
            gomory_hu_options(Some("bad")),
            gomory_hu_options(Some("missing")),
        ] {
            assert!(reopened.paths(None, None, invalid).is_err());
        }
    }

    #[test]
    fn minimum_steiner_persists_exact_uuid_only_weighted_tree() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let nodes = ["A", "B", "Center", "Unused"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'Center'}), (u:Person {name:'Unused'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(c), \
                 (b)-[:ROAD {cost:1.0}]->(c), \
                 (a)-[:ROAD {cost:5.0}]->(b), \
                 (a)-[:ROAD {cost:1.0}]->(c), \
                 (c)-[:ROAD {cost:0.0}]->(c), \
                 (a)-[:OTHER {cost:0.0}]->(b), \
                 (u)-[:ROAD {cost:9.0}]->(u)",
            )
            .unwrap();
        let terminal_ids = [nodes[1].uuid, nodes[0].uuid, nodes[1].uuid];
        drop(graph);

        let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let options = PathsOptions {
            by: PathAlgorithm::MinSteinerTree,
            directed: false,
            k: 1,
            via: Some("ROAD".into()),
            weight: Some("cost".into()),
            terminal_uuids: terminal_ids.iter().map(|uuid| *uuid.as_bytes()).collect(),
            ..PathsOptions::default()
        };
        let result = reopened.paths(None, None, options.clone()).unwrap();
        assert_eq!(result, reopened.paths(None, None, options.clone()).unwrap());
        assert_eq!(result.num_rows(), 2);
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", &DataType::FixedSizeBinary(16), false),
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("target_uuid", &DataType::FixedSizeBinary(16), false),
                ("weight", &DataType::Float64, false),
            ]
        );
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm"],
            "min_steiner_tree"
        );
        assert_eq!(result.schema().metadata()["graphforge.verb"], "paths");
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_eq!(result.schema().metadata().len(), 3);
        assert!(
            result
                .columns()
                .iter()
                .all(|column| column.null_count() == 0)
        );
        for forbidden in [
            "node_id",
            "edge_id",
            "provenance_id",
            "confidence",
            "assertion_uuid",
            "belief_status",
            "valid_time",
        ] {
            assert!(result.column_by_name(forbidden).is_none());
        }
        let edge_ids = uuid_column(&result, "edge_uuid");
        assert!(edge_ids.value(0) < edge_ids.value(1));
        let mut expected_edge_ids = relationship_rows(&reopened, "ROAD")
            .into_iter()
            .filter_map(|(edge, source, target)| {
                let endpoints = ordered_uuid_pair(source, target);
                let a_center =
                    ordered_uuid_pair(*nodes[0].uuid.as_bytes(), *nodes[2].uuid.as_bytes());
                let b_center =
                    ordered_uuid_pair(*nodes[1].uuid.as_bytes(), *nodes[2].uuid.as_bytes());
                (endpoints == a_center || endpoints == b_center).then_some((endpoints, edge))
            })
            .collect::<Vec<_>>();
        expected_edge_ids.sort_unstable();
        let a_center = ordered_uuid_pair(*nodes[0].uuid.as_bytes(), *nodes[2].uuid.as_bytes());
        let b_center = ordered_uuid_pair(*nodes[1].uuid.as_bytes(), *nodes[2].uuid.as_bytes());
        let mut exact_expected = vec![
            expected_edge_ids
                .iter()
                .filter(|(endpoints, _)| *endpoints == a_center)
                .map(|(_, edge)| *edge)
                .min()
                .unwrap(),
            expected_edge_ids
                .iter()
                .find(|(endpoints, _)| *endpoints == b_center)
                .unwrap()
                .1,
        ];
        exact_expected.sort_unstable();
        assert_eq!(
            (0..result.num_rows())
                .map(|row| edge_ids.value(row).try_into().unwrap())
                .collect::<Vec<[u8; 16]>>(),
            exact_expected
        );
        let weights = float_column(&result, "weight");
        assert_eq!([weights.value(0), weights.value(1)], [1.0, 1.0]);
        let sources = uuid_column(&result, "source_uuid");
        let targets = uuid_column(&result, "target_uuid");
        let endpoints = (0..result.num_rows())
            .map(|row| {
                ordered_uuid_pair(
                    sources.value(row).try_into().unwrap(),
                    targets.value(row).try_into().unwrap(),
                )
            })
            .collect::<HashSet<_>>();
        assert_eq!(
            endpoints,
            HashSet::from([
                ordered_uuid_pair(*nodes[0].uuid.as_bytes(), *nodes[2].uuid.as_bytes()),
                ordered_uuid_pair(*nodes[1].uuid.as_bytes(), *nodes[2].uuid.as_bytes()),
            ])
        );
    }

    #[test]
    fn minimum_steiner_validates_closed_contract_and_errors_atomically() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["A", "B", "C"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) \
                 CREATE (a)-[:ROAD {cost:2.0, bad:-1.0}]->(b)",
            )
            .unwrap();
        let valid = min_steiner_options(&[&nodes[0], &nodes[1]], Some("cost"));
        let baseline = graph.paths(None, None, valid.clone()).unwrap();
        assert_eq!(baseline.num_rows(), 1);

        for invalid in [
            PathsOptions {
                directed: true,
                ..valid.clone()
            },
            PathsOptions {
                k: 2,
                ..valid.clone()
            },
            min_steiner_options(&[&nodes[0]], Some("cost")),
            min_steiner_options(&[&nodes[0], &nodes[2]], Some("cost")),
            min_steiner_options(&[&nodes[0], &nodes[1]], Some("bad")),
            min_steiner_options(&[&nodes[0], &nodes[1]], Some("missing")),
            PathsOptions {
                prize_property: Some("prize".into()),
                ..valid.clone()
            },
        ] {
            assert!(graph.paths(None, None, invalid).is_err());
            assert_eq!(graph.paths(None, None, valid.clone()).unwrap(), baseline);
        }

        let unit = graph
            .paths(
                None,
                None,
                min_steiner_options(&[&nodes[1], &nodes[0]], None),
            )
            .unwrap();
        assert_eq!(float_column(&unit, "weight").value(0), 1.0);
    }

    #[test]
    fn prize_steiner_persists_exact_objective_schema_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let terminal = graph
            .add_node(
                "Person",
                &HashMap::from([
                    ("name".into(), PropValue::Str("Terminal".into())),
                    ("prize".into(), PropValue::Float(0.0)),
                ]),
            )
            .unwrap();
        let winner = graph
            .add_node(
                "Person",
                &HashMap::from([
                    ("name".into(), PropValue::Str("Winner".into())),
                    ("prize".into(), PropValue::Float(10.0)),
                ]),
            )
            .unwrap();
        let excluded = graph
            .add_node(
                "Person",
                &HashMap::from([
                    ("name".into(), PropValue::Str("Excluded".into())),
                    ("prize".into(), PropValue::Float(2.0)),
                ]),
            )
            .unwrap();
        graph
            .execute(
                "MATCH (t:Person {name:'Terminal'}), (w:Person {name:'Winner'}), \
                 (x:Person {name:'Excluded'}) \
                 CREATE (t)-[:ROAD {cost:3.0}]->(w), \
                 (t)-[:ROAD {cost:3.0}]->(w), \
                 (t)-[:ROAD {cost:5.0}]->(x), \
                 (w)-[:ROAD {cost:0.0}]->(w), \
                 (t)-[:OTHER {cost:0.0}]->(x)",
            )
            .unwrap();
        let expected_edge = relationship_rows(&graph, "ROAD")
            .into_iter()
            .filter(|(_, source, target)| {
                ordered_uuid_pair(*source, *target)
                    == ordered_uuid_pair(*terminal.uuid.as_bytes(), *winner.uuid.as_bytes())
            })
            .map(|(edge, _, _)| edge)
            .min()
            .unwrap();
        drop(graph);

        let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let options = prize_steiner_options(&[&terminal], Some("cost"), "prize");
        let result = reopened.paths(None, None, options.clone()).unwrap();
        assert_eq!(result, reopened.paths(None, None, options).unwrap());
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", &DataType::FixedSizeBinary(16), false),
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("target_uuid", &DataType::FixedSizeBinary(16), false),
                ("weight", &DataType::Float64, false),
            ]
        );
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm"],
            "prize_collecting_steiner_tree"
        );
        assert_eq!(result.schema().metadata()["graphforge.verb"], "paths");
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_eq!(result.schema().metadata().len(), 3);
        assert!(
            result
                .columns()
                .iter()
                .all(|column| column.null_count() == 0)
        );
        for forbidden in [
            "node_id",
            "edge_id",
            "provenance_id",
            "confidence",
            "assertion_uuid",
            "belief_status",
            "valid_time",
        ] {
            assert!(result.column_by_name(forbidden).is_none());
        }
        assert_eq!(uuid_column(&result, "edge_uuid").value(0), expected_edge);
        assert_eq!(float_column(&result, "weight").value(0), 3.0);
        let endpoints = ordered_uuid_pair(
            uuid_column(&result, "source_uuid")
                .value(0)
                .try_into()
                .unwrap(),
            uuid_column(&result, "target_uuid")
                .value(0)
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            endpoints,
            ordered_uuid_pair(*terminal.uuid.as_bytes(), *winner.uuid.as_bytes())
        );
        assert_ne!(endpoints.0, *excluded.uuid.as_bytes());
        assert_ne!(endpoints.1, *excluded.uuid.as_bytes());

        let unit_options = prize_steiner_options(&[&winner, &terminal, &winner], None, "prize");
        let unit = reopened.paths(None, None, unit_options.clone()).unwrap();
        assert_eq!(unit, reopened.paths(None, None, unit_options).unwrap());
        assert_eq!(unit.num_rows(), 2);
        let unit_edges = uuid_column(&unit, "edge_uuid");
        assert!(unit_edges.value(0) < unit_edges.value(1));
        assert!((0..2).any(|row| unit_edges.value(row) == expected_edge));
        assert_eq!(
            [
                float_column(&unit, "weight").value(0),
                float_column(&unit, "weight").value(1),
            ],
            [1.0, 1.0]
        );
    }

    #[test]
    fn prize_steiner_one_terminal_and_closed_errors_are_atomic() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = [("A", 0.0), ("B", 0.0), ("C", 0.0)].map(|(name, prize)| {
            graph
                .add_node(
                    "Person",
                    &HashMap::from([
                        ("name".into(), PropValue::Str(name.into())),
                        ("prize".into(), PropValue::Float(prize)),
                    ]),
                )
                .unwrap()
        });
        graph
            .execute(
                "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) \
                 CREATE (a)-[:ROAD {cost:2.0, bad:-1.0}]->(b)",
            )
            .unwrap();
        let valid = prize_steiner_options(&[&nodes[0]], Some("cost"), "prize");
        let baseline = graph.paths(None, None, valid.clone()).unwrap();
        assert_eq!(baseline.num_rows(), 0);

        for invalid in [
            PathsOptions {
                directed: true,
                ..valid.clone()
            },
            PathsOptions {
                k: 2,
                ..valid.clone()
            },
            PathsOptions {
                terminal_uuids: Vec::new(),
                ..valid.clone()
            },
            PathsOptions {
                terminal_uuids: vec![[0xff; 16]],
                ..valid.clone()
            },
            prize_steiner_options(&[&nodes[0], &nodes[2]], Some("cost"), "prize"),
            prize_steiner_options(&[&nodes[0]], Some("bad"), "prize"),
            prize_steiner_options(&[&nodes[0]], Some("missing"), "prize"),
            prize_steiner_options(&[&nodes[0]], Some("cost"), "missing"),
            PathsOptions {
                prize_property: None,
                ..valid.clone()
            },
        ] {
            assert!(graph.paths(None, None, invalid).is_err());
            assert_eq!(graph.paths(None, None, valid.clone()).unwrap(), baseline);
        }

        let missing_prize = add_person(&graph, "NoPrize");
        assert!(
            graph
                .paths(
                    None,
                    None,
                    prize_steiner_options(&[&missing_prize], Some("cost"), "prize"),
                )
                .is_err()
        );
    }

    fn is_dag_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::IsDag,
            via: via.map(str::to_owned),
            directed,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn topological_sort_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::TopologicalSort,
            via: via.map(str::to_owned),
            directed,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn minimum_spanning_tree_options(via: Option<&str>, weight: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::MinimumSpanningTree,
            via: via.map(str::to_owned),
            directed: false,
            weight: weight.map(str::to_owned),
            k: None,
            partition_property: None,
        }
    }

    fn maximum_spanning_tree_options(via: Option<&str>, weight: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::MaximumSpanningTree,
            via: via.map(str::to_owned),
            directed: false,
            weight: weight.map(str::to_owned),
            k: None,
            partition_property: None,
        }
    }

    fn articulation_points_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::ArticulationPoints,
            via: via.map(str::to_owned),
            directed: false,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn bridges_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::Bridges,
            via: via.map(str::to_owned),
            directed: false,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn triangle_count_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::TriangleCount,
            via: via.map(str::to_owned),
            directed: false,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn count_automorphisms_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::CountAutomorphisms,
            via: via.map(str::to_owned),
            directed,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn transitivity_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::Transitivity,
            via: via.map(str::to_owned),
            directed: false,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn is_planar_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::IsPlanar,
            via: via.map(str::to_owned),
            directed: false,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn triad_census_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::TriadCensus,
            via: via.map(str::to_owned),
            directed: true,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn dyad_census_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::DyadCensus,
            via: via.map(str::to_owned),
            directed: true,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn node_coloring_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::NodeColoring,
            via: via.map(str::to_owned),
            directed: false,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn k1_coloring_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::K1Coloring,
            via: via.map(str::to_owned),
            directed: false,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn chromatic_number_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::ChromaticNumber,
            via: via.map(str::to_owned),
            directed: false,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn find_cycles_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::FindCycles,
            via: via.map(str::to_owned),
            directed,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn dag_longest_path_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::DagLongestPath,
            via: via.map(str::to_owned),
            directed,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn weighted_dag_longest_path_options(
        directed: bool,
        via: Option<&str>,
        weight: Option<&str>,
    ) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::DagLongestPathWeighted,
            via: via.map(str::to_owned),
            directed,
            weight: weight.map(str::to_owned),
            k: None,
            partition_property: None,
        }
    }

    fn edge_coloring_options(via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::EdgeColoring,
            via: via.map(str::to_owned),
            directed: false,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn has_euler_circuit_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::HasEulerCircuit,
            via: via.map(str::to_owned),
            directed,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn has_euler_path_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by: AnalyzeAlgorithm::HasEulerPath,
            via: via.map(str::to_owned),
            directed,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn euler_options(by: AnalyzeAlgorithm, directed: bool, via: Option<&str>) -> AnalyzeOptions {
        AnalyzeOptions {
            by,
            via: via.map(str::to_owned),
            directed,
            weight: None,
            k: None,
            partition_property: None,
        }
    }

    fn euler_uuid_list(batch: &arrow::record_batch::RecordBatch, column: &str) -> Vec<[u8; 16]> {
        let lists = batch
            .column_by_name(column)
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(lists.null_count(), 0);
        let values = lists.value(0);
        let values = values
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(values.null_count(), 0);
        (0..values.len())
            .map(|index| values.value(index).try_into().unwrap())
            .collect()
    }

    fn relationship_rows(
        graph: &GraphForge,
        rel_type: &str,
    ) -> Vec<([u8; 16], [u8; 16], [u8; 16])> {
        let result = graph
            .execute(&format!(
                "MATCH (source)-[edge:{rel_type}]->(target) RETURN source, edge, target"
            ))
            .unwrap();
        let batch = &result.batches[0];
        let uuid_column = |column: &str, field: &str| {
            batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column_by_name(field)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
        };
        let edges = uuid_column("edge", "edge_uuid");
        let sources = uuid_column("source", "node_uuid");
        let targets = uuid_column("target", "node_uuid");
        (0..batch.num_rows())
            .map(|row| {
                (
                    edges.value(row).try_into().unwrap(),
                    sources.value(row).try_into().unwrap(),
                    targets.value(row).try_into().unwrap(),
                )
            })
            .collect()
    }

    fn assert_euler_edge_alignment(
        node_path: &[[u8; 16]],
        edge_path: &[[u8; 16]],
        relationship_rows: &[([u8; 16], [u8; 16], [u8; 16])],
        directed: bool,
    ) {
        assert_eq!(node_path.len(), edge_path.len() + 1);
        let mut remaining = relationship_rows
            .iter()
            .copied()
            .map(|(edge, source, target)| (edge, (source, target)))
            .collect::<HashMap<_, _>>();
        for (edge, nodes) in edge_path.iter().zip(node_path.windows(2)) {
            let (source, target) = remaining.remove(edge).expect("edge UUID occurs once");
            assert!(
                (nodes[0] == source && nodes[1] == target)
                    || (!directed && nodes[0] == target && nodes[1] == source),
                "edge UUID must align with its adjacent node UUIDs"
            );
        }
        assert!(
            remaining.is_empty(),
            "every selected edge UUID is preserved"
        );
    }

    fn assert_euler_schema(batch: &arrow::record_batch::RecordBatch, algorithm: &str) {
        let fields = batch.schema().fields().clone();
        assert_eq!(fields.len(), 2);
        for (field, name) in fields.iter().zip(["node_path", "edge_path"]) {
            assert_eq!(field.name(), name);
            assert!(!field.is_nullable());
            let DataType::List(item) = field.data_type() else {
                panic!("{name} must be a List");
            };
            assert_eq!(item.data_type(), &DataType::FixedSizeBinary(16));
            assert!(!item.is_nullable());
        }
        assert_eq!(
            batch.schema().metadata(),
            &HashMap::from([
                ("graphforge.algorithm".to_owned(), algorithm.to_owned()),
                (
                    "graphforge.algorithm_schema_version".to_owned(),
                    "1".to_owned()
                ),
                ("graphforge.verb".to_owned(), "analyze".to_owned()),
            ])
        );
    }

    fn is_dag_value(batch: &arrow::record_batch::RecordBatch) -> bool {
        batch
            .column_by_name("is_dag")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    }

    fn has_euler_circuit_value(batch: &arrow::record_batch::RecordBatch) -> bool {
        let values = batch
            .column_by_name("has_euler_circuit")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.null_count(), 0);
        values.value(0)
    }

    fn has_euler_path_value(batch: &arrow::record_batch::RecordBatch) -> bool {
        let values = batch
            .column_by_name("has_euler_path")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.null_count(), 0);
        values.value(0)
    }

    fn topological_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<([u8; 16], u64)> {
        let nodes = batch
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let orders = batch
            .column_by_name("order")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| (nodes.value(row).try_into().unwrap(), orders.value(row)))
            .collect()
    }

    fn node_coloring_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<([u8; 16], u64)> {
        let nodes = batch
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let colors = batch
            .column_by_name("color")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| (nodes.value(row).try_into().unwrap(), colors.value(row)))
            .collect()
    }

    fn edge_color_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<([u8; 16], u64)> {
        let edges = batch
            .column_by_name("edge_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let colors = batch
            .column_by_name("color")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(edges.null_count(), 0);
        assert_eq!(colors.null_count(), 0);
        (0..batch.num_rows())
            .map(|row| (edges.value(row).try_into().unwrap(), colors.value(row)))
            .collect()
    }

    fn uuid_path(batch: &arrow::record_batch::RecordBatch, row: usize) -> Vec<[u8; 16]> {
        let paths = batch
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = paths.value(row);
        let values = values
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        (0..values.len())
            .map(|index| values.value(index).try_into().unwrap())
            .collect()
    }

    fn uuid_walk(batch: &arrow::record_batch::RecordBatch, row: usize) -> Vec<[u8; 16]> {
        let walks = batch
            .column_by_name("walk")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = walks.value(row);
        let values = values
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        (0..values.len())
            .map(|index| values.value(index).try_into().unwrap())
            .collect()
    }

    fn uuid_pairs(batch: &arrow::record_batch::RecordBatch) -> Vec<([u8; 16], [u8; 16])> {
        let sources = batch
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = batch
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| {
                (
                    sources.value(row).try_into().unwrap(),
                    targets.value(row).try_into().unwrap(),
                )
            })
            .collect()
    }

    fn arrow_batch_fingerprint(batch: &arrow::record_batch::RecordBatch) -> String {
        let mut hasher = sha2::Sha256::new();
        for field in batch.schema().fields() {
            hasher.update(field.name().as_bytes());
            hasher.update([0]);
            hasher.update(format!("{:?}", field.data_type()).as_bytes());
            hasher.update([0]);
            hasher.update([u8::from(field.is_nullable())]);
        }
        hasher.update(batch.num_rows().to_le_bytes());
        for column in batch.columns() {
            hasher.update(column.len().to_le_bytes());
            hasher.update(column.null_count().to_le_bytes());
            hasher.update(format!("{column:?}").as_bytes());
        }
        let digest: [u8; 32] = hasher.finalize().into();
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(64);
        for byte in digest {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0xf) as usize] as char);
        }
        out
    }

    fn add_person(graph: &GraphForge, name: &str) -> NodeHandle {
        graph
            .add_node(
                "Person",
                &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
            )
            .unwrap()
    }

    fn add_person_with_heuristic(graph: &GraphForge, name: &str, heuristic: f64) -> NodeHandle {
        add_person_with_heuristic_value(graph, name, PropValue::Float(heuristic))
    }

    fn add_person_with_heuristic_value(
        graph: &GraphForge,
        name: &str,
        heuristic: PropValue,
    ) -> NodeHandle {
        graph
            .add_node(
                "Person",
                &HashMap::from([
                    ("name".to_owned(), PropValue::Str(name.to_owned())),
                    ("heuristic".to_owned(), heuristic),
                ]),
            )
            .unwrap()
    }

    fn sorted_utf8_list_values(
        batch: &arrow::record_batch::RecordBatch,
        column: &str,
        row: usize,
    ) -> Vec<String> {
        let list = batch
            .column_by_name(column)
            .expect(column)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("column is a list");
        let values = list.value(row);
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("list values are Utf8");
        let mut actual = (0..values.len())
            .map(|i| values.value(i).to_owned())
            .collect::<Vec<_>>();
        actual.sort();
        actual
    }

    #[test]
    fn graphforge_new_inmemory() {
        let gf = GraphForge::new(None).expect("in-memory instance");
        assert!(gf.path().is_none());
        assert_eq!(gf.ontology_mode(), OntologyMode::Exploratory);
        assert!(gf.dir.is_dir());
        assert!(gf.dir.file_name().is_some_and(|name| {
            name.to_string_lossy()
                .starts_with("graphforge-graph-workspace-")
        }));
    }

    #[test]
    fn persistent_open_resolves_and_reuses_one_committed_generation() {
        let root = tempfile::tempdir().unwrap();
        let first = GraphForge::new(root.path().to_str()).expect("create v1 project");
        let generation_uuid = first.resolved_generation.generation_uuid();
        assert_eq!(first.path(), Some(root.path()));
        assert_eq!(
            std::fs::read(root.path().join(graphforge_storage::FORMAT_FILE)).unwrap(),
            graphforge_storage::PROJECT_FORMAT_BYTES
        );
        drop(first);

        let reopened = GraphForge::new(root.path().to_str()).expect("reopen v1 project");
        assert_eq!(
            reopened.resolved_generation.generation_uuid(),
            generation_uuid
        );
    }

    #[test]
    fn persistent_open_creates_an_absent_final_target_through_storage() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().canonicalize().unwrap().join("project");
        assert!(!root.exists());

        let first = GraphForge::new(root.to_str()).expect("admit and create v1 project");
        let generation_uuid = first.resolved_generation.generation_uuid();
        assert_eq!(first.path(), Some(root.as_path()));
        assert!(root.is_dir());
        drop(first);

        let reopened = GraphForge::new(root.to_str()).expect("reopen admitted project");
        assert_eq!(
            reopened.resolved_generation.generation_uuid(),
            generation_uuid
        );
    }

    #[test]
    fn persistent_open_rejects_pre_v1_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let legacy = root.path().join("topology/nodes.parquet");
        std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        std::fs::write(&legacy, b"legacy bytes").unwrap();

        let error = GraphForge::new(root.path().to_str()).unwrap_err();

        assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
        assert_eq!(std::fs::read(&legacy).unwrap(), b"legacy bytes");
        assert!(!root.path().join(graphforge_storage::FORMAT_FILE).exists());
    }

    #[test]
    fn graphforge_new_bad_path() {
        let parent = tempfile::tempdir().unwrap();
        let result = GraphForge::new(parent.path().join("missing/project").to_str());
        assert!(matches!(result, Err(GfError::Storage(_))));
    }

    #[cfg(unix)]
    #[test]
    fn clear_resets_in_memory_state_after_partial_filesystem_failure() {
        use std::os::unix::fs::PermissionsExt;

        struct PermissionGuard {
            path: PathBuf,
            original: Option<std::fs::Permissions>,
        }

        impl PermissionGuard {
            fn restore(&mut self) {
                if let Some(original) = self.original.take() {
                    std::fs::set_permissions(&self.path, original)
                        .expect("restore fixture directory permissions");
                }
            }
        }

        impl Drop for PermissionGuard {
            fn drop(&mut self) {
                self.restore();
            }
        }

        let gf = GraphForge::new(None).expect("in-memory instance");
        gf.execute("CREATE (:Person {name: 'Alice'})")
            .expect("seed fixture files and runtime catalog");
        gf.register_procedure(ProcedureDefinition {
            name: "test.fixture".into(),
            inputs: vec![],
            outputs: vec![],
            rows: vec![vec![]],
        })
        .expect("register fixture procedure");

        let original = std::fs::metadata(&gf.dir)
            .expect("fixture directory metadata")
            .permissions();
        let mut restricted = original.clone();
        restricted.set_mode(0o500);
        std::fs::set_permissions(&gf.dir, restricted)
            .expect("restrict fixture directory permissions");
        let mut guard = PermissionGuard {
            path: gf.dir.clone(),
            original: Some(original),
        };

        let error = gf
            .clear()
            .expect_err("filesystem cleanup must report the permission failure");
        guard.restore();

        assert!(matches!(error, GfError::Storage(_)));
        let catalog = gf.runtime_catalog.lock().expect("runtime catalog lock");
        assert!(catalog.entity_types().is_empty());
        assert!(catalog.relation_types().is_empty());
        assert_eq!(catalog.property_names().count(), 0);
        drop(catalog);
        assert!(
            gf.execute("CALL test.fixture()").is_err(),
            "procedure registry must reset even when filesystem cleanup fails"
        );

        gf.clear()
            .expect("cleanup succeeds after permissions are restored");
    }

    #[test]
    fn create_then_match_returns_uuid_and_property() {
        // The #583 acceptance test (exploratory): CREATE a Person, then read its
        // node_uuid + name back. node_uuid is FixedSizeBinary(16); no surrogate
        // node_id leaks; the runtime catalog records the Person label.
        let gf = GraphForge::new(None).expect("in-memory instance");
        gf.execute("CREATE (:Person {name: 'Alice'})")
            .expect("create");

        let result = gf
            .execute("MATCH (n:Person) RETURN n.node_uuid AS node_uuid, n.name AS name")
            .expect("read");

        assert_eq!(result.stats.rows_produced, 1, "one Person row");

        // No surrogate identity column in the public result.
        assert!(
            result.schema.column_with_name("node_id").is_none(),
            "node_id surrogate must not appear in results"
        );

        // node_uuid is FixedSizeBinary(16).
        let batch = &result.batches[0];
        let uuids = batch
            .column_by_name("node_uuid")
            .expect("node_uuid column")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("node_uuid is FixedSizeBinary");
        assert_eq!(uuids.value_length(), 16);

        // The runtime catalog observed the Person label during bind.
        assert!(
            gf.runtime_catalog()
                .lock()
                .unwrap()
                .contains_entity_type("Person")
        );

        // Schema metadata is attached.
        let meta = result.schema.metadata();
        assert!(meta.contains_key("graphforge.query_id"));
        assert_eq!(
            meta.get("graphforge.ontology_mode").map(String::as_str),
            Some("exploratory")
        );
    }

    #[test]
    fn parse_error_surfaces_as_parse() {
        let gf = GraphForge::new(None).unwrap();
        assert!(matches!(
            gf.execute("MATCH (n RETURN"),
            Err(GfError::Parse { .. })
        ));
    }

    #[test]
    fn create_multi_type_rel_surfaces_as_parse_error() {
        // #724: a CREATE relationship with a type disjunction is invalid syntax.
        let gf = GraphForge::new(None).unwrap();
        assert!(matches!(
            gf.execute("CREATE (a:Person)-[:KNOWS|LIKES]->(b:Person)"),
            Err(GfError::Parse { .. })
        ));
    }

    #[test]
    fn delete_clause_executes() {
        // #740: DELETE now executes (it was rejected at bind under #724). On an
        // empty graph the MATCH yields no rows, so the delete is a no-op that
        // succeeds rather than erroring.
        let gf = GraphForge::new(None).unwrap();
        gf.execute("MATCH (p:Person) DELETE p")
            .expect("DELETE executes (no-op on an empty graph)");
    }

    #[test]
    fn unwind_list_literal_explodes_to_rows() {
        // #714: a list literal lowers and UNWIND explodes it end-to-end.
        let gf = GraphForge::new(None).unwrap();
        let result = gf
            .execute("UNWIND [1, 2, 3] AS x RETURN x")
            .expect("unwind");
        assert_eq!(result.stats.rows_produced, 3);
    }

    #[test]
    fn unwind_empty_list_yields_no_rows() {
        let gf = GraphForge::new(None).unwrap();
        let result = gf.execute("UNWIND [] AS x RETURN x").expect("unwind empty");
        assert_eq!(result.stats.rows_produced, 0);
    }

    #[test]
    fn strict_mode_unknown_label_is_a_bind_error() {
        // The #583 acceptance test (strict): a query naming a label absent from
        // the ontology fails to bind. Build a strict project dir with a minimal
        // ontology that declares only `Person`.
        let dir = tempfile::TempDir::new().unwrap();
        let mut bootstrap = GraphForge::new(dir.path().to_str()).unwrap();
        let ontology_path = dir.path().join(ONTOLOGY_FILE);
        std::fs::write(
            &ontology_path,
            "ontology_id: t\nversion: \"v1\"\nentity_types:\n  - name: Person\n    abstract: false\n",
        )
        .unwrap();
        bootstrap
            .adopt_ontology(AdoptOntologyRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid::Uuid::from_u128(606)),
                    actor_uuid: None,
                },
                path: ontology_path,
                mode: OntologyMode::Strict,
            })
            .unwrap();
        drop(bootstrap);

        let gf = GraphForge::new(dir.path().to_str()).expect("open strict project");
        assert_eq!(gf.ontology_mode(), OntologyMode::Strict);

        let err = gf
            .execute("MATCH (n:NoSuchLabel) RETURN n.node_uuid AS u")
            .expect_err("unknown label in strict mode must error");
        assert!(
            matches!(err, GfError::Bind { .. }),
            "expected a bind error (#606), got: {err:?}"
        );
    }

    /// A small fixture (5 Person + 4 KNOWS + 1 LIKES) created in one statement
    /// (one CREATE keeps it compact; separate CREATEs also accumulate — #733).
    fn fixture() -> GraphForge {
        let gf = GraphForge::new(None).unwrap();
        gf.execute(
            "CREATE (alice:Person {name:'Alice', age:30}), (bob:Person {name:'Bob', age:25}), \
             (carol:Person {name:'Carol', age:35}), (dave:Person {name:'Dave', age:28}), \
             (eve:Person {name:'Eve', age:22}), \
             (alice)-[:KNOWS]->(bob), (bob)-[:KNOWS]->(carol), (carol)-[:KNOWS]->(dave), \
             (alice)-[:KNOWS]->(carol), (dave)-[:LIKES]->(eve)",
        )
        .expect("create fixture");
        gf
    }

    #[test]
    fn exploratory_single_hop_traversal_connects_endpoints() {
        // #728: a fixed `(a)-[:KNOWS]->(b)` join works in exploratory mode (the
        // relation name resolves from the runtime catalog and the edge was
        // written to `_exploratory.parquet` with the correct `rel_type_name`).
        let gf = fixture();
        let result = gf
            .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.node_uuid, b.node_uuid")
            .expect("traversal");
        assert_eq!(result.stats.rows_produced, 4, "4 KNOWS edges");
    }

    #[test]
    fn exploratory_two_hop_traversal() {
        let gf = fixture();
        let result = gf
            .execute("MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.node_uuid")
            .expect("two-hop");
        // Alice→Bob→Carol, Bob→Carol→Dave, Alice→Carol→Dave.
        assert_eq!(result.stats.rows_produced, 3);
    }

    #[test]
    fn optional_match_projects_optional_side_variable() {
        // #730: projecting the optional-side `m` resolves; every `n` is kept and
        // `m.node_uuid` is null where there is no LIKES edge.
        let gf = fixture();
        let result = gf
            .execute(
                "MATCH (n:Person) OPTIONAL MATCH (n)-[:LIKES]->(m) RETURN n.node_uuid, m.node_uuid",
            )
            .expect("optional");
        assert_eq!(result.stats.rows_produced, 5, "one row per Person");
    }

    #[test]
    fn count_aggregate_returns_node_total() {
        // #729: `RETURN count(n)` / `count(*)` produce one row with the total.
        let gf = fixture();
        for q in [
            "MATCH (n:Person) RETURN count(n) AS total",
            "MATCH (n:Person) RETURN count(*) AS total",
        ] {
            let result = gf.execute(q).expect("count");
            assert_eq!(result.stats.rows_produced, 1, "aggregate → one row | {q}");
            let total = result.batches[0]
                .column_by_name("total")
                .expect("total column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 count")
                .value(0);
            assert_eq!(total, 5, "5 Person nodes | {q}");
        }
    }

    #[test]
    fn parameter_binding_filters_by_value() {
        // #584: `WHERE n.age > $min` with `{min: 28}` substitutes the placeholder
        // and filters — Alice (30) and Carol (35) pass `> 28`.
        let gf = fixture();
        let params = HashMap::from([("min".to_owned(), IrLiteral::Int(28))]);
        let result = gf
            .execute_with_params(
                "MATCH (n:Person) WHERE n.age > $min RETURN n.node_uuid",
                &params,
            )
            .expect("parameterized query");
        assert_eq!(result.stats.rows_produced, 2);
    }

    #[test]
    fn parameter_binding_applies_to_single_set_write() {
        let gf = fixture();
        let params = HashMap::from([
            ("old".to_owned(), IrLiteral::Str("Eve".into())),
            ("new".to_owned(), IrLiteral::Str("Zed".into())),
        ]);
        gf.execute_with_params(
            "MATCH (n:Person) WHERE n.name = $old SET n.name = $new",
            &params,
        )
        .expect("parameterized SET");

        let result = gf
            .execute("MATCH (n:Person) WHERE n.name = 'Zed' RETURN count(n) AS total")
            .expect("read after SET");
        let total = result.batches[0]
            .column_by_name("total")
            .expect("total column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 count")
            .value(0);
        assert_eq!(total, 1);
    }

    #[test]
    fn parameter_binding_applies_to_single_delete_write() {
        let gf = GraphForge::new(None).unwrap();
        gf.execute("CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'})")
            .expect("create fixture");

        let params = HashMap::from([("name".to_owned(), IrLiteral::Str("Bob".into()))]);
        gf.execute_with_params("MATCH (n:Person) WHERE n.name = $name DELETE n", &params)
            .expect("parameterized DELETE");

        let result = gf
            .execute("MATCH (n:Person) RETURN count(n) AS total")
            .expect("read after DELETE");
        let total = result.batches[0]
            .column_by_name("total")
            .expect("total column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 count")
            .value(0);
        assert_eq!(total, 1);
    }

    #[test]
    fn parameterized_skip_limit_applies_row_counts() {
        let gf = fixture();
        let params = HashMap::from([
            ("s".to_owned(), IrLiteral::Int(1)),
            ("l".to_owned(), IrLiteral::Int(2)),
        ]);
        let result = gf
            .execute_with_params(
                "MATCH (n:Person) RETURN n.name AS name ORDER BY name ASC SKIP $s LIMIT $l",
                &params,
            )
            .expect("parameterized SKIP/LIMIT query");
        assert_eq!(result.stats.rows_produced, 2);
        let names = result.batches[0]
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 names");
        assert_eq!(names.value(0), "Bob");
        assert_eq!(names.value(1), "Carol");
    }

    #[test]
    fn parameterized_skip_limit_validate_runtime_values() {
        let gf = fixture();

        let negative = HashMap::from([("s".to_owned(), IrLiteral::Int(-1))]);
        let err = gf
            .execute_with_params("MATCH (n:Person) RETURN n.name SKIP $s", &negative)
            .expect_err("negative SKIP parameter must error at runtime");
        assert!(
            matches!(err, GfError::Execution(_)),
            "expected runtime execution error, got: {err:?}"
        );

        let float = HashMap::from([("l".to_owned(), IrLiteral::Float(1.5))]);
        let err = gf
            .execute_with_params("MATCH (n:Person) RETURN n.name LIMIT $l", &float)
            .expect_err("float LIMIT parameter must error at runtime");
        assert!(
            matches!(err, GfError::Execution(_)),
            "expected runtime execution error, got: {err:?}"
        );
    }

    #[test]
    fn keys_and_properties_work_for_maps_and_nulls() {
        let gf = GraphForge::new(None).unwrap();
        let result = gf
            .execute(
                "WITH null AS m \
                 RETURN keys({name: 'Alice', age: null}) AS k, \
                        keys(m) AS null_keys, \
                        properties({name: 'Popeye', level: 9001}) AS props, \
                        properties(m) AS null_props",
            )
            .expect("map keys/properties");

        let batch = &result.batches[0];
        let actual = sorted_utf8_list_values(batch, "k", 0);
        assert_eq!(actual, vec!["age".to_owned(), "name".to_owned()]);

        assert!(
            batch
                .column_by_name("null_keys")
                .expect("null_keys column")
                .is_null(0),
            "keys(null) must return null"
        );
        assert_eq!(
            batch
                .column_by_name("null_props")
                .expect("null_props column")
                .data_type(),
            &arrow::datatypes::DataType::Null,
            "properties(null) must return null"
        );

        let props = batch
            .column_by_name("props")
            .expect("props column")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("properties(map) returns a struct map");
        let name = props
            .column_by_name("name")
            .expect("name field")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name is Utf8");
        let level = props
            .column_by_name("level")
            .expect("level field")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("level is Int64");
        assert_eq!(name.value(0), "Popeye");
        assert_eq!(level.value(0), 9001);
    }

    #[test]
    fn keys_accepts_parameter_maps() {
        let gf = GraphForge::new(None).unwrap();
        let params = HashMap::from([(
            "param".to_owned(),
            IrLiteral::Map(vec![
                ("name".to_owned(), IrLiteral::Str("Alice".to_owned())),
                ("age".to_owned(), IrLiteral::Int(38)),
                ("missing".to_owned(), IrLiteral::Null),
            ]),
        )]);
        let result = gf
            .execute_with_params("RETURN keys($param) AS k", &params)
            .expect("keys(parameter map)");
        let actual = sorted_utf8_list_values(&result.batches[0], "k", 0);
        assert_eq!(
            actual,
            vec!["age".to_owned(), "missing".to_owned(), "name".to_owned()]
        );
    }

    #[test]
    fn properties_work_for_nodes_and_relationships() {
        let gf = GraphForge::new(None).unwrap();
        gf.execute("CREATE (:Person {name: 'Popeye', level: 9001})-[:R {name: 'Olive', level: 7}]->(:Person {name: 'Bluto'})")
            .expect("create graph");
        let result = gf
            .execute(
                "MATCH (n:Person {name: 'Popeye'})-[r:R]->() \
                 RETURN keys(properties(n)) AS node_keys, \
                        keys(properties(r)) AS rel_keys, \
                        toString(properties(n)['name']) AS node_name, \
                        toString(properties(r)['name']) AS rel_name",
            )
            .expect("entity properties");
        let batch = &result.batches[0];
        assert_eq!(
            sorted_utf8_list_values(batch, "node_keys", 0),
            vec!["level".to_owned(), "name".to_owned()]
        );
        assert_eq!(
            sorted_utf8_list_values(batch, "rel_keys", 0),
            vec!["level".to_owned(), "name".to_owned()]
        );
        let node_name = batch
            .column_by_name("node_name")
            .expect("node_name")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("node name is Utf8");
        let rel_name = batch
            .column_by_name("rel_name")
            .expect("rel_name")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("rel name is Utf8");
        assert_eq!(node_name.value(0), "Popeye");
        assert_eq!(rel_name.value(0), "Olive");
    }

    #[test]
    fn map_literals_preserve_nested_graph_values() {
        let gf = GraphForge::new(None).unwrap();
        gf.execute("CREATE (a:A), (b:B) CREATE (a)-[:T]->(b)")
            .expect("create graph");

        let result = gf
            .execute("MATCH (n)-[r]->(m) RETURN {node1: n, rel: r, node2: m} AS m")
            .expect("map of graph values");
        let batch = &result.batches[0];
        let map = batch
            .column_by_name("m")
            .expect("m")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("m is a map struct");
        let node1 = map
            .column_by_name("node1")
            .expect("node1")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("node1 is a node struct");
        let node2 = map
            .column_by_name("node2")
            .expect("node2")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("node2 is a node struct");
        let rel = map
            .column_by_name("rel")
            .expect("rel")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("rel is a relationship struct");

        assert!(node1.column_by_name("node_uuid").is_some());
        assert!(node2.column_by_name("node_uuid").is_some());
        assert!(rel.column_by_name("edge_uuid").is_some());

        let first_label = |node: &StructArray| {
            let labels = node
                .column_by_name("labels")
                .expect("labels")
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("labels is a list");
            let values = labels.value(0);
            values
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("label values are Utf8")
                .value(0)
                .to_owned()
        };
        assert_eq!(first_label(node1), "A");
        assert_eq!(first_label(node2), "B");

        let rel_type = rel
            .column_by_name("rel_type")
            .expect("rel_type")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("rel_type is Utf8");
        assert_eq!(rel_type.value(0), "T");
    }

    #[test]
    fn properties_omit_absent_sparse_entity_properties() {
        let gf = GraphForge::new(None).unwrap();
        gf.execute(
            "CREATE (:Person {name: 'A', keep: 1}), \
                    (:Person {name: 'B', keep: 2, sparse: 'x'}), \
                    ()-[:R {keep: 1}]->(), \
                    ()-[:R {keep: 2, sparse: 'x'}]->()",
        )
        .expect("create sparse graph");

        let result = gf
            .execute(
                "MATCH (n:Person {name: 'A'}) \
                 MATCH ()-[r:R]->() \
                 WHERE r.keep = 1 \
                 RETURN keys(properties(n)) AS node_keys, \
                        keys(properties(r)) AS rel_keys, \
                        properties(n)['sparse'] AS node_missing, \
                        properties(r)['sparse'] AS rel_missing",
            )
            .expect("sparse entity properties");
        let batch = &result.batches[0];
        assert_eq!(
            sorted_utf8_list_values(batch, "node_keys", 0),
            vec!["keep".to_owned(), "name".to_owned()]
        );
        assert_eq!(
            sorted_utf8_list_values(batch, "rel_keys", 0),
            vec!["keep".to_owned()]
        );
        assert!(
            batch
                .column_by_name("node_missing")
                .expect("node_missing")
                .is_null(0),
            "absent sparse node property must read as null"
        );
        assert!(
            batch
                .column_by_name("rel_missing")
                .expect("rel_missing")
                .is_null(0),
            "absent sparse relationship property must read as null"
        );
    }

    #[test]
    fn properties_return_null_for_absent_optional_entities() {
        let gf = GraphForge::new(None).unwrap();
        let result = gf
            .execute(
                "OPTIONAL MATCH (n:DoesNotExist) \
                 OPTIONAL MATCH (n)-[r:NOT_THERE]->() \
                 RETURN properties(n) AS node_props, \
                        properties(r) AS rel_props, \
                        properties(null) AS null_props",
            )
            .expect("optional entity properties");
        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1);
        for name in ["node_props", "rel_props", "null_props"] {
            let col = batch.column_by_name(name).expect(name);
            assert!(
                matches!(col.data_type(), arrow::datatypes::DataType::Null) || col.is_null(0),
                "{name} should be null for an absent optional entity"
            );
        }
    }

    #[test]
    fn properties_rejects_invalid_literal_inputs() {
        let gf = GraphForge::new(None).unwrap();
        for query in [
            "RETURN properties(1)",
            "RETURN properties('Cypher')",
            "RETURN properties([true, false])",
        ] {
            let err = gf.execute(query).expect_err(query);
            assert!(
                matches!(err, GfError::Plan(_)),
                "expected InvalidArgumentType-style plan error for {query}, got {err:?}"
            );
        }
    }

    #[test]
    fn missing_parameter_value_errors() {
        // A `$param` with no provided value is a clear error, not a silent wrong
        // result.
        let gf = fixture();
        let err = gf
            .execute("MATCH (n:Person) WHERE n.age > $min RETURN n.node_uuid")
            .expect_err("missing param must error");
        assert!(
            matches!(
                err,
                GfError::Bind { .. } | GfError::Plan(_) | GfError::Execution(_)
            ),
            "expected a bind/plan/execution error, got: {err:?}"
        );
    }

    #[test]
    fn registered_procedure_executes_and_yields_alias() {
        use arrow::array::Int64Array;

        let gf = GraphForge::new(None).unwrap();
        gf.register_procedure(ProcedureDefinition {
            name: "test.double".into(),
            inputs: vec![ProcedureField {
                name: "in".into(),
                type_name: "INTEGER".into(),
                nullable: true,
            }],
            outputs: vec![ProcedureField {
                name: "out".into(),
                type_name: "INTEGER".into(),
                nullable: true,
            }],
            rows: vec![
                vec![IrLiteral::Int(1), IrLiteral::Int(2)],
                vec![IrLiteral::Int(2), IrLiteral::Int(4)],
            ],
        })
        .unwrap();

        let result = gf
            .execute("CALL test.double(2) YIELD out AS value RETURN value")
            .expect("registered procedure executes");
        let values = result.batches[0]
            .column_by_name("value")
            .expect("yield alias")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("integer output");
        assert_eq!(values.values(), &[4]);
    }

    #[test]
    fn registered_procedure_missing_parameter_matches_streaming_path() {
        let gf = GraphForge::new(None).unwrap();
        gf.register_procedure(ProcedureDefinition {
            name: "test.echo".into(),
            inputs: vec![ProcedureField {
                name: "value".into(),
                type_name: "INTEGER".into(),
                nullable: true,
            }],
            outputs: vec![],
            rows: vec![vec![IrLiteral::Int(1)]],
        })
        .unwrap();

        let err = match gf.execute_stream("CALL test.echo") {
            Ok(_) => panic!("implicit parameter must be supplied"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("MissingParameter"));
    }

    #[test]
    fn union_all_preserves_duplicates_and_union_deduplicates() {
        use arrow::array::Int64Array;

        let gf = GraphForge::new(None).unwrap();
        for (keyword, expected) in [("UNION ALL", vec![1, 1]), ("UNION", vec![1])] {
            let result = gf
                .execute(&format!("RETURN 1 AS x {keyword} RETURN 1 AS x"))
                .expect("UNION executes");
            let values: Vec<i64> = result
                .batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column_by_name("x")
                        .expect("x")
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("integer x")
                        .values()
                        .iter()
                        .copied()
                })
                .collect();
            assert_eq!(values, expected);
        }
    }

    #[test]
    fn empty_scope_return_wildcard_errors_but_with_preserves_the_row() {
        let gf = GraphForge::new(None).unwrap();
        let error = gf.execute("RETURN *").expect_err("RETURN *");
        assert!(matches!(error, GfError::Bind { .. }));
        assert!(error.to_string().contains("wildcard requires"));

        let result = gf
            .execute("WITH * RETURN 1 AS value")
            .expect("WITH * should preserve the implicit row");
        assert_eq!(result.stats.rows_produced, 1);
        assert_eq!(
            result.batches[0]
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[test]
    fn shaper_schema_mismatch_is_an_error_not_a_panic() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let source_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(Int64Array::from(vec![1])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let shaper = Shaper::new(&source_schema, OntologyMode::Exploratory, None);

        let error = shaper.apply(&batch).expect_err("schema mismatch must fail");
        assert!(matches!(error, arrow::error::ArrowError::SchemaError(_)));
    }

    #[test]
    fn shaper_preserves_user_aliases_named_like_surrogates() {
        use arrow::array::{Int64Array, StringArray, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use graphforge_storage::{INTERNAL_SURROGATE_META_KEY, is_internal_surrogate_field};

        let marked_node = Field::new("node_id", DataType::UInt64, false).with_metadata(
            [(INTERNAL_SURROGATE_META_KEY.to_owned(), "true".to_owned())]
                .into_iter()
                .collect(),
        );
        let marked_edge = Field::new("edge_id", DataType::UInt64, false).with_metadata(
            [(INTERNAL_SURROGATE_META_KEY.to_owned(), "true".to_owned())]
                .into_iter()
                .collect(),
        );
        assert!(is_internal_surrogate_field(&marked_node));
        assert!(is_internal_surrogate_field(&marked_edge));

        let source_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("node_id", DataType::Int64, false),
            marked_node,
            Field::new("edge_id", DataType::FixedSizeBinary(16), false),
            marked_edge,
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            source_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("Alice")])) as arrow::array::ArrayRef,
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(UInt64Array::from(vec![7])),
                Arc::new(
                    arrow::array::FixedSizeBinaryArray::try_from_iter(std::iter::once(
                        [0u8; 16].as_slice(),
                    ))
                    .unwrap(),
                ),
                Arc::new(UInt64Array::from(vec![9])),
            ],
        )
        .unwrap();
        let shaper = Shaper::new(&source_schema, OntologyMode::Exploratory, None);
        let shaped = shaper.apply(&batch).expect("shape user aliases");
        assert_eq!(
            shaped
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["name", "node_id", "edge_id"]
        );
        assert_eq!(shaped.num_rows(), 1);
        assert_eq!(
            shaped
                .column_by_name("node_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
    }

    #[test]
    fn shaper_preserves_row_count_when_only_surrogates_remain() {
        use arrow::array::UInt64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use graphforge_storage::INTERNAL_SURROGATE_META_KEY;

        let marked = Field::new("node_id", DataType::UInt64, false).with_metadata(
            [(INTERNAL_SURROGATE_META_KEY.to_owned(), "true".to_owned())]
                .into_iter()
                .collect(),
        );
        let source_schema = Arc::new(Schema::new(vec![marked]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            source_schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef],
        )
        .unwrap();
        let shaper = Shaper::new(&source_schema, OntologyMode::Exploratory, None);
        let shaped = shaper.apply(&batch).expect("zero-column shape");
        assert_eq!(shaped.num_columns(), 0);
        assert_eq!(shaped.num_rows(), 3);
    }

    #[test]
    fn shaper_collapses_void_unit_row_without_surrogate_drops() {
        use arrow::datatypes::Schema;

        // Empty-plan / void CALL execution yields a zero-column unit row. Public
        // shaping must report an empty result (TCK Call1), not preserve the
        // internal unit row when no surrogate columns were dropped.
        let source_schema = Arc::new(Schema::empty());
        let batch = arrow::record_batch::RecordBatch::try_new_with_options(
            source_schema.clone(),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
        let shaper = Shaper::new(&source_schema, OntologyMode::Exploratory, None);
        let shaped = shaper.apply(&batch).expect("void shape");
        assert_eq!(shaped.num_columns(), 0);
        assert_eq!(shaped.num_rows(), 0);
    }

    #[test]
    fn projected_node_id_and_edge_id_aliases_survive_execute() {
        let gf = GraphForge::new(None).unwrap();

        let node = gf
            .execute("RETURN 42 AS node_id")
            .expect("literal node_id alias");
        assert_eq!(node.stats.rows_produced, 1);
        assert_eq!(
            node.schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["node_id"]
        );
        assert_eq!(
            node.batches[0]
                .column_by_name("node_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );

        let edge = gf
            .execute("RETURN 42 AS edge_id")
            .expect("literal edge_id alias");
        assert_eq!(edge.stats.rows_produced, 1);
        assert_eq!(
            edge.schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["edge_id"]
        );
        assert_eq!(
            edge.batches[0]
                .column_by_name("edge_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );

        gf.execute("CREATE (p:Person {name: 'Alice', hub: 107})")
            .expect("seed person");
        let mixed = gf
            .execute(
                "MATCH (p:Person {name: 'Alice'}) \
                 RETURN p.name AS name, p.node_uuid AS node_id, p.hub AS edge_id, 1 AS keep",
            )
            .expect("mixed reserved-looking aliases");
        assert_eq!(mixed.stats.rows_produced, 1);
        assert_eq!(
            mixed
                .schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["name", "node_id", "edge_id", "keep"]
        );
        assert_eq!(
            mixed.schema.field_with_name("node_id").unwrap().data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            mixed.batches[0]
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Alice"
        );
        assert_eq!(
            mixed.batches[0]
                .column_by_name("edge_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            107
        );
        assert_eq!(
            mixed.batches[0]
                .column_by_name("keep")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );

        // Documented network-analysis style: property projected AS node_id.
        let hubs = gf
            .execute(
                "MATCH (p:Person {name: 'Alice'}) \
                 RETURN p.hub AS node_id, 1 AS degree",
            )
            .expect("property AS node_id");
        assert_eq!(hubs.stats.rows_produced, 1);
        assert_eq!(
            hubs.schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["node_id", "degree"]
        );

        // Internal scan surrogates still stay private on ordinary projections.
        let uuid_only = gf
            .execute("MATCH (p:Person) RETURN p.node_uuid AS node_uuid")
            .expect("uuid projection");
        assert!(uuid_only.schema.column_with_name("node_id").is_none());
        assert!(uuid_only.schema.column_with_name("edge_id").is_none());
    }

    #[test]
    fn persistent_open_does_not_cleanup_generation_files() {
        let dir = tempfile::TempDir::new().unwrap();
        let first = GraphForge::new(dir.path().to_str()).unwrap();
        let topology = first.dir.join("topology");
        std::fs::create_dir_all(&topology).unwrap();
        let stale = topology.join("nodes.parquet.Abc123.tmp");
        let unrelated = topology.join("notes.tmp");
        std::fs::write(&stale, b"stale").unwrap();
        std::fs::write(&unrelated, b"keep").unwrap();
        first.publish_workspace_update().unwrap();
        drop(first);

        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();

        assert!(graph.dir.join("topology/nodes.parquet.Abc123.tmp").exists());
        assert!(graph.dir.join("topology/notes.tmp").exists());
        assert_eq!(graph.path(), Some(dir.path()));
    }

    #[test]
    fn degree_obeys_uuid_schema_direction_via_and_multigraph_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(a), (a)-[:OTHER]->(c)",
            )
            .unwrap();

        let directed = graph
            .rank("Person", degree_options(true, Some("KNOWS")))
            .unwrap();
        assert_eq!(degree_scores(&directed), [1.5, 0.0, 0.0]);
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("Alice"), Some("Bob"), None]
        );
        let uuids = directed
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(uuids.value_length(), 16);
        assert_eq!(uuids.null_count(), 0);
        assert_eq!(
            directed,
            graph
                .rank("Person", degree_options(true, Some("KNOWS")))
                .unwrap()
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank("Person", degree_options(false, Some("KNOWS")))
                    .unwrap()
            ),
            [2.0, 1.0, 0.0]
        );
        assert_eq!(
            degree_scores(&graph.rank("Person", degree_options(true, None)).unwrap()),
            [2.0, 0.0, 0.0]
        );
    }

    #[test]
    fn degree_empty_and_invalid_inputs_are_structured() {
        let graph = GraphForge::new(None).unwrap();
        let empty = graph.rank("Person", degree_options(true, None)).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(
            empty.schema().field_with_name("score").unwrap().data_type(),
            &DataType::Float64
        );
        for result in [
            graph.rank("", degree_options(true, None)),
            graph.rank("Person", degree_options(true, Some(" "))),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn degree_public_writeback_is_opt_in_exact_and_persistent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (a)-[:KNOWS]->(b)",
            )
            .unwrap();

        let expected = graph
            .rank("Person", degree_options(false, Some("KNOWS")))
            .unwrap();
        assert_eq!(
            graph
                .execute(
                    "MATCH (n:Person) WHERE n.degree_score IS NOT NULL \
                     RETURN n.degree_score"
                )
                .unwrap()
                .stats
                .rows_produced,
            0
        );

        let mut options = degree_options(false, Some("KNOWS"));
        options.write_property = Some("degree_score".into());
        assert_eq!(graph.rank("Person", options).unwrap(), expected);
        let immediate = graph
            .execute(
                "MATCH (n:Person) RETURN n.name AS name, \
                 n.degree_score AS degree_score ORDER BY name",
            )
            .unwrap();
        assert_eq!(
            immediate.batches[0]
                .column_by_name("degree_score")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[0.5, 0.5, 0.0]
        );
        drop(graph);

        let reopened = GraphForge::new(Some(path)).unwrap();
        let persisted = reopened
            .execute("MATCH (n:Person) RETURN n.degree_score AS degree_score ORDER BY n.name")
            .unwrap();
        assert_eq!(
            persisted.batches[0]
                .column_by_name("degree_score")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[0.5, 0.5, 0.0]
        );
    }

    #[test]
    fn betweenness_obeys_public_topology_arrow_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (a)-[:KNOWS]->(d), (d)-[:KNOWS]->(c), (b)-[:KNOWS]->(b), \
                 (a)-[:OTHER]->(c)",
            )
            .unwrap();

        let options = betweenness_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_eq!(degree_scores(&directed), [0.0, 1.0 / 9.0, 0.0, 1.0 / 18.0]);
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "betweenness"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert_eq!(
            directed
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "score", "name"]
        );
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
        );
        assert_ne!(
            degree_scores(&directed),
            degree_scores(
                &graph
                    .rank("Person", betweenness_options(false, Some("KNOWS"), None))
                    .unwrap()
            )
        );
        assert_ne!(
            degree_scores(&directed),
            degree_scores(
                &graph
                    .rank("Person", betweenness_options(true, None, None))
                    .unwrap()
            )
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.between IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                betweenness_options(true, Some("KNOWS"), Some("between")),
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.between IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );

        let disconnected = GraphForge::new(None).unwrap();
        disconnected
            .execute("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person), (d:Person)")
            .unwrap();
        assert_eq!(
            degree_scores(
                &disconnected
                    .rank("Person", betweenness_options(true, None, None))
                    .unwrap()
            ),
            [0.0, 1.0 / 6.0, 0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", betweenness_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn closeness_obeys_public_topology_arrow_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (b)-[:KNOWS]->(b), (a)-[:OTHER]->(c)",
            )
            .unwrap();

        let options = closeness_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_eq!(degree_scores(&directed), [4.0 / 9.0, 1.0 / 3.0, 0.0, 0.0]);
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "closeness"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank("Person", closeness_options(false, Some("KNOWS"), None))
                    .unwrap()
            ),
            [4.0 / 9.0, 2.0 / 3.0, 4.0 / 9.0, 0.0]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank("Person", closeness_options(true, None, None))
                    .unwrap()
            ),
            [2.0 / 3.0, 1.0 / 3.0, 0.0, 0.0]
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.close_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                closeness_options(true, Some("KNOWS"), Some("close_score")),
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.close_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", closeness_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn harmonic_closeness_obeys_public_topology_arrow_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (b)-[:KNOWS]->(b), (a)-[:OTHER]->(c)",
            )
            .unwrap();

        let options = harmonic_closeness_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_eq!(degree_scores(&directed), [0.5, 1.0 / 3.0, 0.0, 0.0]);
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "harmonic_closeness"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank(
                        "Person",
                        harmonic_closeness_options(false, Some("KNOWS"), None),
                    )
                    .unwrap()
            ),
            [0.5, 2.0 / 3.0, 0.5, 0.0]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank("Person", harmonic_closeness_options(true, None, None),)
                    .unwrap()
            ),
            [2.0 / 3.0, 1.0 / 3.0, 0.0, 0.0]
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.harmonic IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                harmonic_closeness_options(true, Some("KNOWS"), Some("harmonic")),
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.harmonic IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", harmonic_closeness_options(true, None, None),)
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn eigenvector_obeys_public_topology_arrow_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(b), \
                 (a)-[:OTHER]->(c)",
            )
            .unwrap();

        let options = eigenvector_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        let ratio = 3.0 * 2.0_f64.powi(20) - 2.0;
        let denominator = (ratio * ratio + 3.0).sqrt();
        let expected = [
            1.0 / denominator,
            ratio / denominator,
            1.0 / denominator,
            1.0 / denominator,
        ];
        assert!(
            degree_scores(&directed)
                .iter()
                .zip(expected)
                .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-15)
        );
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "eigenvector"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
        );

        let undirected = degree_scores(
            &graph
                .rank("Person", eigenvector_options(false, Some("KNOWS"), None))
                .unwrap(),
        );
        let phi = (1.0 + 5.0_f64.sqrt()) / 2.0;
        let norm = (1.0 + phi * phi).sqrt();
        assert!((undirected[0] - 1.0 / norm).abs() <= 1.0e-7);
        assert!((undirected[1] - phi / norm).abs() <= 1.0e-7);
        assert!(undirected[2] <= 1.0e-7 && undirected[3] <= 1.0e-7);
        let all_edges = degree_scores(
            &graph
                .rank("Person", eigenvector_options(true, None, None))
                .unwrap(),
        );
        assert!(all_edges[2] > all_edges[0]);

        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.eigen_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                eigenvector_options(true, Some("KNOWS"), Some("eigen_score")),
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.eigen_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert!(
            degree_scores(
                &edgeless
                    .rank("Person", eigenvector_options(true, None, None))
                    .unwrap()
            )
            .iter()
            .all(|score| (score - 1.0 / 3.0_f64.sqrt()).abs() <= 1.0e-15)
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", eigenvector_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn article_rank_obeys_public_topology_arrow_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (a)-[:OTHER]->(c), \
                 (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
            )
            .unwrap();

        let options = article_rank_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert!(
            degree_scores(&directed)
                .iter()
                .zip([0.15, 0.252, 0.15, 0.15])
                .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-15)
        );
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "article_rank"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
        );

        let undirected = degree_scores(
            &graph
                .rank("Person", article_rank_options(false, Some("KNOWS"), None))
                .unwrap(),
        );
        assert_ne!(undirected, degree_scores(&directed));
        let all_edges = degree_scores(
            &graph
                .rank("Person", article_rank_options(true, None, None))
                .unwrap(),
        );
        assert!(all_edges[2] > all_edges[1]);

        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.article_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                article_rank_options(true, Some("KNOWS"), Some("article_score")),
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.article_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert!(
            degree_scores(
                &edgeless
                    .rank("Person", article_rank_options(true, None, None))
                    .unwrap(),
            )
            .iter()
            .all(|score| (score - 0.15).abs() <= 1.0e-15)
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", article_rank_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn hits_hub_obeys_public_topology_arrow_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
            )
            .unwrap();

        let options = hits_hub_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        let expected = [1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0, 0.0];
        assert!(
            degree_scores(&directed)
                .iter()
                .zip(expected)
                .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-15)
        );
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "hits_hub"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
        );

        let undirected = degree_scores(
            &graph
                .rank("Person", hits_hub_options(false, Some("KNOWS"), None))
                .unwrap(),
        );
        assert!(
            undirected[..3]
                .iter()
                .all(|score| (score - 1.0 / 3.0_f64.sqrt()).abs() <= 1.0e-12)
        );
        assert_eq!(undirected[3], 0.0);
        let all_edges = degree_scores(
            &graph
                .rank("Person", hits_hub_options(true, None, None))
                .unwrap(),
        );
        assert!(all_edges[2] > 0.0);

        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.hub_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                hits_hub_options(true, Some("KNOWS"), Some("hub_score")),
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.hub_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", hits_hub_options(true, None, None))
                    .unwrap()
            ),
            [0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", hits_hub_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn hits_authority_obeys_public_topology_arrow_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
            )
            .unwrap();

        let options = hits_authority_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        let expected = [0.0, 1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0];
        assert!(
            degree_scores(&directed)
                .iter()
                .zip(expected)
                .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-15)
        );
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "hits_authority"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
        );

        let undirected = degree_scores(
            &graph
                .rank("Person", hits_authority_options(false, Some("KNOWS"), None))
                .unwrap(),
        );
        let root_six = 6.0_f64.sqrt();
        assert!(
            undirected
                .iter()
                .zip([1.0 / root_six, 2.0 / root_six, 1.0 / root_six, 0.0])
                .all(|(actual, expected)| (actual - expected).abs() <= 1.0e-12)
        );
        assert_ne!(
            degree_scores(
                &graph
                    .rank("Person", hits_authority_options(true, None, None))
                    .unwrap()
            ),
            degree_scores(&directed)
        );

        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.authority_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                hits_authority_options(true, Some("KNOWS"), Some("authority_score")),
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.authority_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", hits_authority_options(true, None, None))
                    .unwrap()
            ),
            [0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", hits_authority_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn celf_obeys_public_topology_arrow_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
            )
            .unwrap();

        let options = celf_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        let assert_scores = |actual: &[f64]| {
            assert!(
                actual
                    .iter()
                    .all(|score| score.is_finite() && *score >= 0.0)
            );
            assert!((actual.iter().sum::<f64>() - 4.0).abs() <= 1.0e-12);
            assert!((actual[3] - 1.0).abs() <= 1.0e-12);
        };
        let directed_scores = degree_scores(&directed);
        assert_scores(&directed_scores);
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(directed.schema().metadata()["graphforge.algorithm"], "celf");
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan")]
        );
        let undirected_scores = degree_scores(
            &graph
                .rank("Person", celf_options(false, Some("KNOWS"), None))
                .unwrap(),
        );
        assert_scores(&undirected_scores);
        assert_ne!(undirected_scores, directed_scores);
        let all_scores = degree_scores(
            &graph
                .rank("Person", celf_options(true, None, None))
                .unwrap(),
        );
        assert_scores(&all_scores);
        assert_ne!(all_scores, directed_scores);

        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.celf_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                celf_options(true, Some("KNOWS"), Some("celf_score")),
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.celf_score IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", celf_options(true, None, None))
                    .unwrap()
            ),
            [1.0, 1.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", celf_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn clustering_coefficient_obeys_public_alias_topology_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), \
                 (c)-[:KNOWS]->(c), (d)-[:KNOWS]->(e), (a)-[:OTHER]->(d)",
            )
            .unwrap();

        let options = clustering_coefficient_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_eq!(degree_scores(&directed), [0.5, 0.5, 0.5, 0.0, 0.0]);
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "clustering_coefficient"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("Alice"),
                Some("Bob"),
                Some("Carol"),
                Some("Dan"),
                Some("Eve")
            ]
        );

        let alias: RankAlgorithm = "local_clustering_coefficient".parse().unwrap();
        assert_eq!(alias, RankAlgorithm::ClusteringCoefficient);
        assert_eq!(
            directed,
            graph
                .rank(
                    "Person",
                    RankOptions {
                        by: alias,
                        via: Some("KNOWS".into()),
                        ..RankOptions::default()
                    },
                )
                .unwrap()
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank(
                        "Person",
                        clustering_coefficient_options(false, Some("KNOWS"), None),
                    )
                    .unwrap(),
            ),
            [1.0, 1.0, 1.0, 0.0, 0.0],
        );
        assert_ne!(
            degree_scores(&directed),
            degree_scores(
                &graph
                    .rank("Person", clustering_coefficient_options(true, None, None),)
                    .unwrap()
            )
        );

        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.clustering IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                clustering_coefficient_options(true, Some("KNOWS"), Some("clustering")),
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.clustering IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            5
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", clustering_coefficient_options(true, None, None),)
                    .unwrap()
            ),
            [0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", clustering_coefficient_options(true, None, None),)
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn triangles_obey_uuid_topology_order_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Finn'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(a), (c)-[:KNOWS]->(c), (e)-[:KNOWS]->(f), \
                 (b)-[:OTHER]->(d)",
            )
            .unwrap();

        let options = triangles_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_eq!(degree_scores(&directed), [2.0, 1.0, 2.0, 1.0, 0.0, 0.0]);
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "triangles"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("Alice"),
                Some("Bob"),
                Some("Carol"),
                Some("Dan"),
                Some("Eve"),
                Some("Finn")
            ]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank("Person", triangles_options(false, Some("KNOWS"), None))
                    .unwrap()
            ),
            [2.0, 1.0, 2.0, 1.0, 0.0, 0.0]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank("Person", triangles_options(true, None, None))
                    .unwrap()
            ),
            [3.0, 3.0, 3.0, 3.0, 0.0, 0.0]
        );

        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.triangle_count IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                triangles_options(true, Some("KNOWS"), Some("triangle_count")),
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.triangle_count IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            6
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", triangles_options(true, None, None))
                    .unwrap()
            ),
            [0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", triangles_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn k_core_obeys_uuid_topology_order_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (h:Person {name:'H'}), \
                 (i:Person {name:'I'}), (j:Person {name:'J'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), (b)-[:KNOWS]->(c), \
                 (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), (c)-[:KNOWS]->(c), \
                 (a)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), \
                 (h)-[:KNOWS]->(i), (i)-[:KNOWS]->(j), (j)-[:KNOWS]->(h), \
                 (f)-[:OTHER]->(a)",
            )
            .unwrap();

        let options = k_core_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_eq!(
            degree_scores(&directed),
            [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 0.0, 2.0, 2.0, 2.0]
        );
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "k_core"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F"),
                Some("G"),
                Some("H"),
                Some("I"),
                Some("J")
            ]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank("Person", k_core_options(false, Some("KNOWS"), None))
                    .unwrap()
            ),
            degree_scores(&directed)
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank("Person", k_core_options(true, None, None))
                    .unwrap()
            ),
            [3.0, 3.0, 3.0, 3.0, 2.0, 2.0, 0.0, 2.0, 2.0, 2.0]
        );

        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.core IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank("Person", k_core_options(true, Some("KNOWS"), Some("core")))
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.core IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            10
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", k_core_options(true, None, None))
                    .unwrap()
            ),
            [0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", k_core_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn preferential_attachment_obeys_aggregate_schema_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), \
                 (a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), \
                 (d)-[:KNOWS]->(c), (e)-[:OTHER]->(f)",
            )
            .unwrap();

        let options = preferential_attachment_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_eq!(degree_scores(&directed), [2.0, 3.0, 2.0, 3.0, 0.0, 0.0]);
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "preferential_attachment"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F")
            ]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank(
                        "Person",
                        preferential_attachment_options(false, Some("KNOWS"), None),
                    )
                    .unwrap()
            ),
            [2.0, 2.0, 0.0, 4.0, 0.0, 0.0]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank("Person", preferential_attachment_options(true, None, None),)
                    .unwrap()
            ),
            [4.0, 4.0, 3.0, 4.0, 5.0, 0.0]
        );

        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.pa IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        graph
            .rank(
                "Person",
                preferential_attachment_options(true, Some("KNOWS"), Some("pa")),
            )
            .unwrap();
        let persisted = graph
            .execute(
                "MATCH (n:Person) WHERE n.pa IS NOT NULL \
                 RETURN n.name AS name, n.pa AS pa ORDER BY name",
            )
            .unwrap();
        assert_eq!(persisted.batches.len(), 1);
        let persisted = &persisted.batches[0];
        assert_eq!(
            persisted
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F")
            ]
        );
        assert_eq!(
            persisted
                .column_by_name("pa")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[2.0, 3.0, 2.0, 3.0, 0.0, 0.0]
        );

        let disconnected = GraphForge::new(None).unwrap();
        disconnected
            .execute(
                "CREATE (a:Person)-[:KNOWS]->(b:Person), (b)-[:KNOWS]->(a), \
                 (c:Person)-[:KNOWS]->(d:Person), (d)-[:KNOWS]->(c)",
            )
            .unwrap();
        assert_eq!(
            degree_scores(
                &disconnected
                    .rank(
                        "Person",
                        preferential_attachment_options(true, Some("KNOWS"), None),
                    )
                    .unwrap()
            ),
            [2.0, 2.0, 2.0, 2.0]
        );

        let complete = GraphForge::new(None).unwrap();
        complete
            .execute(
                "CREATE (a:Person), (b:Person), (c:Person), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), \
                 (a)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(b)",
            )
            .unwrap();
        assert_eq!(
            degree_scores(
                &complete
                    .rank(
                        "Person",
                        preferential_attachment_options(true, Some("KNOWS"), None),
                    )
                    .unwrap()
            ),
            [0.0, 0.0, 0.0]
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", preferential_attachment_options(true, None, None),)
                    .unwrap()
            ),
            [0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", preferential_attachment_options(true, None, None),)
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn adamic_adar_obeys_aggregate_schema_via_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), \
                 (a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), \
                 (a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
            )
            .unwrap();

        let inverse_log_two = 1.0 / 2.0_f64.ln();
        let options = adamic_adar_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_rank_scores_close(
            &directed,
            &[
                2.0 * inverse_log_two,
                2.0 * inverse_log_two,
                inverse_log_two,
                inverse_log_two,
                0.0,
                0.0,
            ],
        );
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "adamic_adar"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F")
            ]
        );

        let inverse_log_three = 1.0 / 3.0_f64.ln();
        assert_rank_scores_close(
            &graph
                .rank("Person", adamic_adar_options(false, Some("KNOWS"), None))
                .unwrap(),
            &[
                4.0 * inverse_log_three,
                4.0 * inverse_log_three,
                3.0 * inverse_log_two,
                3.0 * inverse_log_two,
                4.0 * inverse_log_three,
                0.0,
            ],
        );
        assert_rank_scores_close(
            &graph
                .rank("Person", adamic_adar_options(true, None, None))
                .unwrap(),
            &[
                3.0 * inverse_log_two,
                3.0 * inverse_log_two,
                inverse_log_two,
                inverse_log_two,
                0.0,
                0.0,
            ],
        );

        graph
            .rank(
                "Person",
                adamic_adar_options(true, Some("KNOWS"), Some("adamic")),
            )
            .unwrap();
        let persisted = graph
            .execute(
                "MATCH (n:Person) WHERE n.adamic IS NOT NULL \
                 RETURN n.name AS name, n.adamic AS score ORDER BY name",
            )
            .unwrap();
        assert_eq!(persisted.batches.len(), 1);
        let persisted = &persisted.batches[0];
        assert_eq!(
            persisted
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F")
            ]
        );
        assert_rank_scores_close(
            persisted,
            &[
                2.0 * inverse_log_two,
                2.0 * inverse_log_two,
                inverse_log_two,
                inverse_log_two,
                0.0,
                0.0,
            ],
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", adamic_adar_options(true, None, None))
                    .unwrap()
            ),
            [0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", adamic_adar_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn common_neighbors_obeys_aggregate_schema_via_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), \
                 (a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), \
                 (a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
            )
            .unwrap();

        let options = common_neighbors_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_eq!(degree_scores(&directed), [2.0, 2.0, 1.0, 1.0, 0.0, 0.0]);
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "common_neighbors"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F")
            ]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank(
                        "Person",
                        common_neighbors_options(false, Some("KNOWS"), None),
                    )
                    .unwrap()
            ),
            [4.0, 4.0, 3.0, 3.0, 4.0, 0.0]
        );
        assert_eq!(
            degree_scores(
                &graph
                    .rank("Person", common_neighbors_options(true, None, None))
                    .unwrap()
            ),
            [3.0, 3.0, 1.0, 1.0, 0.0, 0.0]
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.common IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );

        graph
            .rank(
                "Person",
                common_neighbors_options(true, Some("KNOWS"), Some("common")),
            )
            .unwrap();
        let persisted = graph
            .execute(
                "MATCH (n:Person) WHERE n.common IS NOT NULL \
                 RETURN n.name AS name, n.common AS score ORDER BY name",
            )
            .unwrap();
        assert_eq!(persisted.batches.len(), 1);
        assert_eq!(
            degree_scores(&persisted.batches[0]),
            [2.0, 2.0, 1.0, 1.0, 0.0, 0.0]
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", common_neighbors_options(true, None, None))
                    .unwrap()
            ),
            [0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", common_neighbors_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn resource_allocation_obeys_aggregate_schema_via_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), \
                 (a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), \
                 (a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
            )
            .unwrap();

        let options = resource_allocation_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_rank_scores_close(&directed, &[1.0, 1.0, 0.5, 0.5, 0.0, 0.0]);
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "resource_allocation"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert_eq!(
            directed
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "score", "name"]
        );
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F")
            ]
        );
        assert_rank_scores_close(
            &graph
                .rank(
                    "Person",
                    resource_allocation_options(false, Some("KNOWS"), None),
                )
                .unwrap(),
            &[4.0 / 3.0, 4.0 / 3.0, 1.5, 1.5, 4.0 / 3.0, 0.0],
        );
        assert_rank_scores_close(
            &graph
                .rank("Person", resource_allocation_options(true, None, None))
                .unwrap(),
            &[1.5, 1.5, 0.5, 0.5, 0.0, 0.0],
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.resource IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );

        graph
            .rank(
                "Person",
                resource_allocation_options(true, Some("KNOWS"), Some("resource")),
            )
            .unwrap();
        let persisted = graph
            .execute(
                "MATCH (n:Person) WHERE n.resource IS NOT NULL \
                 RETURN n.name AS name, n.resource AS score ORDER BY name",
            )
            .unwrap();
        assert_eq!(persisted.batches.len(), 1);
        assert_rank_scores_close(&persisted.batches[0], &[1.0, 1.0, 0.5, 0.5, 0.0, 0.0]);

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", resource_allocation_options(true, None, None))
                    .unwrap()
            ),
            [0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", resource_allocation_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn total_neighbors_obeys_aggregate_schema_via_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), \
                 (a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), \
                 (a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
            )
            .unwrap();

        let options = total_neighbors_options(true, Some("KNOWS"), None);
        let directed = graph.rank("Person", options.clone()).unwrap();
        assert_rank_scores_close(&directed, &[6.0, 6.0, 8.0, 9.0, 7.0, 7.0]);
        assert_eq!(directed, graph.rank("Person", options).unwrap());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "total_neighbors"
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("node_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            directed
                .schema()
                .field_with_name("score")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert_eq!(
            directed
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "score", "name"]
        );
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F")
            ]
        );
        assert_rank_scores_close(
            &graph
                .rank(
                    "Person",
                    total_neighbors_options(false, Some("KNOWS"), None),
                )
                .unwrap(),
            &[6.0, 6.0, 6.0, 6.0, 6.0, 12.0],
        );
        assert_rank_scores_close(
            &graph
                .rank("Person", total_neighbors_options(true, None, None))
                .unwrap(),
            &[6.0, 6.0, 9.0, 11.0, 9.0, 9.0],
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.total IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );

        graph
            .rank(
                "Person",
                total_neighbors_options(true, Some("KNOWS"), Some("total")),
            )
            .unwrap();
        let persisted = graph
            .execute(
                "MATCH (n:Person) WHERE n.total IS NOT NULL \
                 RETURN n.name AS name, n.total AS score ORDER BY name",
            )
            .unwrap();
        assert_eq!(persisted.batches.len(), 1);
        assert_rank_scores_close(&persisted.batches[0], &[6.0, 6.0, 8.0, 9.0, 7.0, 7.0]);

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            degree_scores(
                &edgeless
                    .rank("Person", total_neighbors_options(true, None, None))
                    .unwrap()
            ),
            [0.0, 0.0]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .rank("Person", total_neighbors_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn components_obeys_uuid_schema_direction_via_and_multigraph_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(a), \
                 (c)-[:OTHER]->(d)",
            )
            .unwrap();

        let directed = graph
            .cluster("Person", components_options(true, Some("KNOWS")))
            .unwrap();
        assert_eq!(community_ids(&directed), [0, 0, 1, 2, 3]);
        assert_eq!(
            directed
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            directed.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(directed.schema().field(1).data_type(), &DataType::Int64);
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan"), None]
        );
        assert_eq!(
            directed,
            graph
                .cluster("Person", components_options(true, Some("KNOWS")))
                .unwrap()
        );
        assert_eq!(
            community_ids(
                &graph
                    .cluster("Person", components_options(false, Some("KNOWS")))
                    .unwrap()
            ),
            [0, 0, 1, 2, 3]
        );
        assert_eq!(
            community_ids(
                &graph
                    .cluster("Person", components_options(false, None))
                    .unwrap()
            ),
            [0, 0, 1, 1, 2]
        );
    }

    #[test]
    fn components_writeback_empty_and_invalid_inputs_are_structured() {
        let graph = GraphForge::new(None).unwrap();
        let empty = graph
            .cluster("Person", components_options(false, None))
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema().field(1).data_type(), &DataType::Int64);

        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (a)-[:KNOWS]->(b)",
            )
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (p:Person) WHERE p.component IS NOT NULL RETURN p.name AS name")
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        let mut options = components_options(false, Some("KNOWS"));
        options.write_property = Some("component".into());
        assert_eq!(
            community_ids(&graph.cluster("Person", options).unwrap()),
            [0, 0, 1]
        );
        let readback = graph
            .execute("MATCH (p:Person) RETURN p.component AS component ORDER BY p.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("component")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 0, 1]
        );

        for result in [
            graph.cluster("", components_options(false, None)),
            graph.cluster("Person", components_options(false, Some(" "))),
            graph.cluster(
                "Person",
                ClusterOptions {
                    by: ClusterAlgorithm::Hdbscan,
                    ..ClusterOptions::default()
                },
            ),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn components_public_writeback_is_atomic_empty_and_persistent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (a)-[:KNOWS]->(b)",
            )
            .unwrap();

        let _read_only = graph
            .cluster("Person", components_options(false, Some("KNOWS")))
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.component IS NOT NULL RETURN n.component")
                .unwrap()
                .stats
                .rows_produced,
            0
        );

        graph
            .execute("MATCH (n:Person {name:'Alice'}) SET n.atomic_component = 'old'")
            .unwrap();
        for property in ["", "atomic_component"] {
            let mut options = components_options(false, Some("KNOWS"));
            options.write_property = Some(property.into());
            assert!(matches!(
                graph.cluster("Person", options),
                Err(GfError::Validation(_))
            ));
        }
        let unchanged = graph
            .execute(
                "MATCH (n:Person) WHERE n.atomic_component IS NOT NULL \
                 RETURN n.name AS name, n.atomic_component AS value ORDER BY name",
            )
            .unwrap();
        assert_eq!(unchanged.stats.rows_produced, 1);
        assert_eq!(
            unchanged.batches[0]
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "old"
        );

        let mut empty_options = components_options(false, Some("KNOWS"));
        empty_options.write_property = Some("empty_component".into());
        assert_eq!(
            graph.cluster("Missing", empty_options).unwrap().num_rows(),
            0
        );
        assert_eq!(
            graph
                .execute(
                    "MATCH (n:Person) WHERE n.empty_component IS NOT NULL \
                     RETURN n.empty_component"
                )
                .unwrap()
                .stats
                .rows_produced,
            0
        );

        let expected = graph
            .cluster("Person", components_options(false, Some("KNOWS")))
            .unwrap();
        let mut options = components_options(false, Some("KNOWS"));
        options.write_property = Some("component".into());
        let written = graph.cluster("Person", options).unwrap();
        assert_eq!(written, expected);
        drop(graph);

        let reopened = GraphForge::new(Some(path)).unwrap();
        let persisted = reopened
            .execute("MATCH (n:Person) RETURN n.component AS component ORDER BY n.name")
            .unwrap();
        assert_eq!(
            persisted.batches[0]
                .column_by_name("component")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 0, 1]
        );
    }

    #[test]
    fn read_only_verb_options_have_no_write_property_surface() {
        let PathsOptions {
            by: _,
            via: _,
            directed: _,
            k: _,
            weight: _,
            capacity_property: _,
            cost_property: _,
            heuristic: _,
            walk_length: _,
            seed: _,
            terminal_uuids: _,
            prize_property: _,
        } = PathsOptions::default();
        let AnalyzeOptions {
            by: _,
            via: _,
            directed: _,
            weight: _,
            k: _,
            partition_property: _,
        } = AnalyzeOptions::default();
        let SimilarOptions {
            by: _,
            k: _,
            vector_property: _,
            via: _,
        } = SimilarOptions::default();
    }

    #[test]
    fn public_embedding_option_validation_stays_validation_only() {
        let valid = EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::Node2Vec,
            via: Some("KNOWS".to_owned()),
            directed: true,
            weight: None,
            options: EmbeddingOptions::Node2Vec(Node2VecOptions::default()),
        };
        validate_embedding_options(&valid).unwrap();

        let invalid = EmbeddingAnalyzeOptions {
            options: EmbeddingOptions::Node2Vec(Node2VecOptions {
                dimensions: 0,
                ..Node2VecOptions::default()
            }),
            ..valid
        };
        assert!(matches!(
            validate_embedding_options(&invalid),
            Err(GfError::Validation(_))
        ));
    }

    #[test]
    fn node2vec_executes_through_typed_api_with_canonical_arrow_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute(
                "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'}), \
                 (:Person {name:'Carol'})",
            )
            .unwrap();
        let options = EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::Node2Vec,
            via: Some("KNOWS".to_owned()),
            directed: true,
            weight: None,
            options: EmbeddingOptions::Node2Vec(Node2VecOptions {
                dimensions: 2,
                walk_length: 2,
                walks_per_node: 1,
                window_size: 1,
                negative_samples: 1,
                epochs: 1,
                seed: 7,
                ..Node2VecOptions::default()
            }),
        };
        let first = graph.analyze_embedding(Some("Person"), &options).unwrap();
        assert_eq!(
            first,
            graph.analyze_embedding(Some("Person"), &options).unwrap()
        );
        assert_eq!(first.num_rows(), 3);
        assert_eq!(
            first
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("node_uuid", &DataType::FixedSizeBinary(16), false),
                (
                    "embedding",
                    &DataType::FixedSizeList(
                        Arc::new(arrow::datatypes::Field::new(
                            "item",
                            DataType::Float32,
                            false
                        )),
                        2
                    ),
                    false
                )
            ]
        );
        assert_eq!(
            first.schema().metadata()["graphforge.algorithm"],
            "node2vec"
        );
        assert_eq!(
            first.schema().metadata()["graphforge.algorithm_version"],
            "node2vec-v1"
        );
        assert_eq!(first.schema().metadata()["graphforge.dimensions"], "2");
        assert_eq!(first.schema().metadata()["graphforge.seed"], "7");
        let uuids = first
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!((1..uuids.len()).all(|row| uuids.value(row - 1) < uuids.value(row)));
        let embeddings = first
            .column_by_name("embedding")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(embeddings.value_length(), 2);
        assert_eq!(embeddings.null_count(), 0);
        assert!(
            embeddings
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .all(|value| value.is_some_and(f32::is_finite))
        );

        assert!(matches!(
            graph.analyze_embedding(
                Some(""),
                &EmbeddingAnalyzeOptions {
                    by: AnalyzeAlgorithm::Node2Vec,
                    via: None,
                    directed: false,
                    weight: None,
                    options: EmbeddingOptions::Node2Vec(Node2VecOptions::default()),
                }
            ),
            Err(GfError::Validation(_))
        ));
        drop(graph);
        assert_eq!(
            first,
            GraphForge::new(Some(path))
                .unwrap()
                .analyze_embedding(Some("Person"), &options)
                .unwrap()
        );
    }

    #[test]
    fn fastrp_executes_through_typed_api_with_features_and_canonical_arrow_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute(
                "CREATE (:Person {name:'Alice', score:1.0})\
                 -[:KNOWS {strength:2.0}]->(:Person {name:'Bob', score:2.0}), \
                 (:Person {name:'Carol', score:3.0})",
            )
            .unwrap();
        let options = EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::FastRandomProjection,
            via: Some("KNOWS".to_owned()),
            directed: true,
            weight: Some("strength".to_owned()),
            options: EmbeddingOptions::FastRandomProjection(FastRpOptions {
                dimensions: 4,
                iteration_weights: vec![1.0, 1.0],
                feature_weight: 1.0,
                feature_properties: vec!["score".to_owned()],
                seed: 11,
                ..FastRpOptions::default()
            }),
        };
        let first = graph.analyze_embedding(Some("Person"), &options).unwrap();
        assert_eq!(
            first,
            graph.analyze_embedding(Some("Person"), &options).unwrap()
        );
        assert_eq!(first.num_rows(), 3);
        assert_eq!(
            first
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("node_uuid", &DataType::FixedSizeBinary(16), false),
                (
                    "embedding",
                    &DataType::FixedSizeList(
                        Arc::new(arrow::datatypes::Field::new(
                            "item",
                            DataType::Float32,
                            false
                        )),
                        4
                    ),
                    false
                )
            ]
        );
        assert_eq!(
            first.schema().metadata()["graphforge.algorithm"],
            "fast_random_projection"
        );
        assert_eq!(
            first.schema().metadata()["graphforge.algorithm_version"],
            "fastrp-v1"
        );
        assert_eq!(first.schema().metadata()["graphforge.dimensions"], "4");
        assert_eq!(first.schema().metadata()["graphforge.seed"], "11");
        let uuids = first
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!((1..uuids.len()).all(|row| uuids.value(row - 1) < uuids.value(row)));
        let embeddings = first
            .column_by_name("embedding")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(embeddings.value_length(), 4);
        assert_eq!(embeddings.null_count(), 0);
        assert!(
            embeddings
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .all(|value| value.is_some_and(f32::is_finite))
        );

        let mut invalid = options.clone();
        let EmbeddingOptions::FastRandomProjection(invalid_options) = &mut invalid.options else {
            unreachable!()
        };
        invalid_options.feature_properties = vec!["missing".to_owned()];
        assert!(matches!(
            graph.analyze_embedding(Some("Person"), &invalid),
            Err(GfError::Validation(message)) if message.contains("missing property")
        ));

        drop(graph);
        let reopened = GraphForge::new(Some(path)).unwrap();
        assert_eq!(
            first,
            reopened
                .analyze_embedding(Some("Person"), &options)
                .unwrap()
        );
    }

    #[test]
    fn prepared_embedding_descriptors_round_trip_all_non_node2vec_variants() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (:Person {score:1.0, features:[1.0,0.0], kind:'human'})\
                 -[:KNOWS {kind:'friend'}]->\
                 (:Person {score:2.0, features:[0.0,1.0], kind:'human'})",
            )
            .unwrap();
        let cases = [
            EmbeddingAnalyzeOptions {
                by: AnalyzeAlgorithm::GraphSage,
                via: Some("KNOWS".into()),
                directed: false,
                weight: None,
                options: EmbeddingOptions::GraphSage(GraphSageOptions {
                    dimensions: 2,
                    hidden_dimensions: 3,
                    layers: 1,
                    sample_sizes: vec![2],
                    epochs: 1,
                    negative_samples: 1,
                    learning_rate: 0.001,
                    feature_properties: vec!["score".into(), "features".into()],
                    seed: 41,
                    ..GraphSageOptions::default()
                }),
            },
            EmbeddingAnalyzeOptions {
                by: AnalyzeAlgorithm::FastRandomProjection,
                via: Some("KNOWS".into()),
                directed: false,
                weight: None,
                options: EmbeddingOptions::FastRandomProjection(FastRpOptions {
                    dimensions: 3,
                    iteration_weights: vec![0.5, 1.0],
                    normalization_strength: -0.25,
                    feature_weight: 0.75,
                    feature_properties: vec!["score".into()],
                    seed: 42,
                }),
            },
            EmbeddingAnalyzeOptions {
                by: AnalyzeAlgorithm::HashGnn,
                via: Some("KNOWS".into()),
                directed: true,
                weight: None,
                options: EmbeddingOptions::HashGnn(HashGnnOptions {
                    dimensions: 8,
                    iterations: 2,
                    embedding_density: 0.25,
                    heterogeneous: true,
                    node_type_property: Some("kind".into()),
                    relationship_type_property: Some("kind".into()),
                    seed: 43,
                }),
            },
        ];

        for options in cases {
            let direct = graph.analyze_embedding(Some("Person"), &options).unwrap();
            let descriptor = graph
                .prepare_embedding_invocation(Some("Person"), &options)
                .unwrap();
            let decoded =
                InvocationDescriptor::from_canonical_bytes(descriptor.canonical_bytes()).unwrap();
            assert_eq!(decoded, descriptor);
            assert_eq!(descriptor.algorithm(), Algorithm::Analyze(options.by));
            assert_eq!(
                graph.invoke_embedding_descriptor(&descriptor).unwrap(),
                direct
            );
            assert_eq!(graph.invoke_descriptor(&descriptor).unwrap(), direct);
            assert_eq!(
                direct.schema().metadata()["graphforge.algorithm"],
                options.by.as_str()
            );
        }

        let rank_descriptor = graph
            .prepare_rank_invocation("Person", &degree_options(false, None))
            .unwrap();
        let error = graph
            .invoke_embedding_descriptor(&rank_descriptor)
            .unwrap_err();
        assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
        assert!(error.to_string().contains("requires an analyze descriptor"));
    }

    #[test]
    fn descriptor_dispatch_rejects_every_cross_verb_and_invalid_graphsage_aggregator() {
        let graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Person {score: 1.0})").unwrap();
        let rank = graph
            .prepare_rank_invocation("Person", &degree_options(false, None))
            .unwrap();
        for error in [
            graph.invoke_cluster_descriptor(&rank).unwrap_err(),
            graph.invoke_similar_descriptor(&rank).unwrap_err(),
            graph.invoke_embedding_descriptor(&rank).unwrap_err(),
            graph.invoke_analyze_descriptor(&rank).unwrap_err(),
            graph.invoke_paths_descriptor(&rank).unwrap_err(),
        ] {
            assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
        }

        let options = EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::GraphSage,
            via: None,
            directed: false,
            weight: None,
            options: EmbeddingOptions::GraphSage(GraphSageOptions {
                dimensions: 2,
                hidden_dimensions: 2,
                layers: 1,
                sample_sizes: vec![1],
                epochs: 1,
                negative_samples: 1,
                learning_rate: 0.01,
                feature_properties: vec!["score".into()],
                seed: 7,
                ..GraphSageOptions::default()
            }),
        };
        let descriptor = graph
            .prepare_embedding_invocation(Some("Person"), &options)
            .unwrap();
        let mut parameters = descriptor.parameters().clone();
        parameters.insert(
            "aggregator".into(),
            InvocationParameter::Utf8("unsupported".into()),
        );
        let malformed = InvocationDescriptor::new(
            descriptor.algorithm(),
            *descriptor.projection_fingerprint(),
            parameters,
        )
        .unwrap();
        let error = graph.invoke_embedding_descriptor(&malformed).unwrap_err();
        assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
        assert!(
            error
                .to_string()
                .contains("unsupported GraphSAGE aggregator")
        );
    }

    #[test]
    fn neutral_descriptors_reject_writeback_and_detect_projection_changes() {
        let graph = GraphForge::new(None).unwrap();
        let alice = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("Alice".into()))]),
            )
            .unwrap();
        let bob = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("Bob".into()))]),
            )
            .unwrap();
        graph
            .add_edge(&alice, "KNOWS", &bob, &HashMap::new())
            .unwrap();

        let mut rank_options = degree_options(false, Some("KNOWS"));
        rank_options.write_property = Some("rank".into());
        let error = graph
            .prepare_rank_invocation("Person", &rank_options)
            .unwrap_err();
        assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
        assert_eq!(
            error.to_string(),
            "invalid invocation descriptor: rank write_property is not part of a neutral invocation"
        );

        let mut cluster_options = components_options(false, Some("KNOWS"));
        cluster_options.write_property = Some("community".into());
        let error = graph
            .prepare_cluster_invocation("Person", &cluster_options)
            .unwrap_err();
        assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
        assert_eq!(
            error.to_string(),
            "invalid invocation descriptor: cluster write_property is not part of a neutral invocation"
        );

        let rank = graph
            .prepare_rank_invocation("Person", &degree_options(false, Some("KNOWS")))
            .unwrap();
        let cluster = graph
            .prepare_cluster_invocation("Person", &components_options(false, Some("KNOWS")))
            .unwrap();
        let similar = graph
            .prepare_similar_invocation("Person", &node_similarity_options(2, Some("KNOWS")))
            .unwrap();
        let analyze = graph
            .prepare_analyze_invocation(None, &is_dag_options(true, Some("KNOWS")))
            .unwrap();
        let source = NodeSelector::Handle(alice);
        let target = NodeSelector::Handle(bob);
        let paths = graph
            .prepare_paths_invocation(
                Some(&source),
                Some(&target),
                &bfs_options(true, Some("KNOWS")),
            )
            .unwrap();

        let wrong_rank = graph.invoke_rank_descriptor(&cluster).unwrap_err();
        assert_eq!(wrong_rank.code(), "GF_DESCRIPTOR_INVALID");
        assert!(
            wrong_rank
                .to_string()
                .contains("rank dispatch requires a rank descriptor")
        );
        let wrong_embedding = graph.invoke_embedding_descriptor(&analyze).unwrap_err();
        assert_eq!(wrong_embedding.code(), "GF_DESCRIPTOR_INVALID");
        assert!(
            wrong_embedding
                .to_string()
                .contains("descriptor is not an embedding algorithm")
        );

        graph.add_node("Person", &HashMap::new()).unwrap();
        for error in [
            graph.invoke_rank_descriptor(&rank).unwrap_err(),
            graph.invoke_cluster_descriptor(&cluster).unwrap_err(),
            graph.invoke_similar_descriptor(&similar).unwrap_err(),
            graph.invoke_analyze_descriptor(&analyze).unwrap_err(),
            graph.invoke_paths_descriptor(&paths).unwrap_err(),
        ] {
            assert_eq!(error.code(), "GF_PROJECTION_CHANGED");
            assert_eq!(
                error.to_string(),
                "the graph projection changed after descriptor preparation"
            );
        }
    }

    #[test]
    fn runtime_guard_blocks_inside_and_outside_an_ambient_runtime() {
        let graph = GraphForge::new(None).unwrap();
        let (_, _, guard) = graph
            .execute_stream_owned("RETURN 1 AS value", &HashMap::new())
            .unwrap();
        assert_eq!(guard.block_on(async { 41 + 1 }), 42);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            assert_eq!(guard.block_on(async { 20 + 22 }), 42);
        });
    }

    #[test]
    fn descriptor_preparation_materializes_optional_analysis_and_path_parameters() {
        let graph = GraphForge::new(None).unwrap();
        let analyze = AnalyzeOptions {
            by: AnalyzeAlgorithm::IsDag,
            via: Some("KNOWS".into()),
            directed: true,
            weight: Some("weight".into()),
            k: Some(3),
            partition_property: Some("partition".into()),
        };
        assert_eq!(
            graph
                .prepare_analyze_invocation(Some("Person"), &analyze)
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );

        let source = graph
            .add_node(
                "Person",
                &HashMap::from([
                    ("weight".into(), PropValue::Float(1.0)),
                    ("partition".into(), PropValue::Int(0)),
                    ("heuristic".into(), PropValue::Float(1.0)),
                ]),
            )
            .unwrap();
        let analyze_descriptor = graph
            .prepare_analyze_invocation(
                Some("Person"),
                &AnalyzeOptions {
                    by: AnalyzeAlgorithm::MinimumKSpanningTree,
                    via: Some("KNOWS".into()),
                    directed: false,
                    weight: Some("weight".into()),
                    k: Some(3),
                    partition_property: None,
                },
            )
            .unwrap();
        assert_eq!(
            analyze_descriptor.parameters()["weight"],
            InvocationParameter::Utf8("weight".into())
        );
        assert_eq!(
            analyze_descriptor.parameters()["k"],
            InvocationParameter::U64(3)
        );
        let modularity_descriptor = graph
            .prepare_analyze_invocation(
                Some("Person"),
                &AnalyzeOptions {
                    by: AnalyzeAlgorithm::Modularity,
                    via: Some("KNOWS".into()),
                    directed: false,
                    weight: Some("weight".into()),
                    k: None,
                    partition_property: Some("partition".into()),
                },
            )
            .unwrap();
        assert_eq!(
            modularity_descriptor.parameters()["partition_property"],
            InvocationParameter::Utf8("partition".into())
        );
        let target = graph
            .add_node(
                "Person",
                &HashMap::from([("heuristic".into(), PropValue::Float(0.0))]),
            )
            .unwrap();
        graph
            .add_edge(
                &source,
                "KNOWS",
                &target,
                &HashMap::from([
                    ("weight".into(), PropValue::Float(1.0)),
                    ("capacity".into(), PropValue::Float(2.0)),
                    ("cost".into(), PropValue::Float(3.0)),
                ]),
            )
            .unwrap();
        let weighted_paths = PathsOptions {
            by: PathAlgorithm::AStar,
            directed: true,
            k: 1,
            via: Some("KNOWS".into()),
            weight: Some("weight".into()),
            capacity_property: None,
            cost_property: None,
            heuristic: Some("heuristic".into()),
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        };
        let weighted_descriptor = graph
            .prepare_paths_invocation(
                Some(&NodeSelector::Handle(source.clone())),
                Some(&NodeSelector::Handle(target)),
                &weighted_paths,
            )
            .unwrap();
        assert_eq!(
            weighted_descriptor.parameters()["weight"],
            InvocationParameter::Utf8("weight".into())
        );
        assert_eq!(
            weighted_descriptor.parameters()["heuristic"],
            InvocationParameter::Utf8("heuristic".into())
        );
        let source_uuid = source.uuid;
        let source = NodeSelector::Handle(source);

        let random_walk = PathsOptions {
            by: PathAlgorithm::RandomWalk,
            directed: true,
            k: 2,
            via: Some("KNOWS".into()),
            weight: None,
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        };
        let descriptor = graph
            .prepare_paths_invocation(Some(&source), None, &random_walk)
            .unwrap();
        assert_eq!(
            descriptor.parameters()["walk_length"],
            InvocationParameter::U64(10)
        );
        assert_eq!(descriptor.parameters()["seed"], InvocationParameter::U64(0));
        assert_eq!(
            descriptor.parameters()["via"],
            InvocationParameter::Utf8("KNOWS".into())
        );

        let mut explicit = random_walk;
        explicit.walk_length = Some(7);
        explicit.seed = Some(9);
        let descriptor = graph
            .prepare_paths_invocation(Some(&source), None, &explicit)
            .unwrap();
        assert_eq!(
            descriptor.parameters()["walk_length"],
            InvocationParameter::U64(7)
        );
        assert_eq!(descriptor.parameters()["seed"], InvocationParameter::U64(9));

        let terminals = PathsOptions {
            by: PathAlgorithm::MinSteinerTree,
            directed: false,
            k: 1,
            via: None,
            weight: None,
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: vec![source_uuid.into_bytes()],
            prize_property: None,
        };
        let descriptor = graph
            .prepare_paths_invocation(None, None, &terminals)
            .unwrap();
        assert_eq!(
            descriptor.parameters()["terminal_uuids"],
            InvocationParameter::UuidList(vec![source_uuid.into_bytes()])
        );

        for by in [
            PathAlgorithm::MinSteinerTree,
            PathAlgorithm::PrizeCollectingSteinerTree,
            PathAlgorithm::GomoryHuTree,
        ] {
            let options = PathsOptions {
                by,
                directed: false,
                k: 1,
                via: None,
                weight: None,
                capacity_property: None,
                cost_property: None,
                heuristic: None,
                walk_length: None,
                seed: None,
                terminal_uuids: Vec::new(),
                prize_property: None,
            };
            let selector = NodeSelector::Uuid(uuid::Uuid::nil());
            let error = graph
                .prepare_paths_invocation(Some(&selector), None, &options)
                .unwrap_err();
            assert_eq!(error.code(), "GF_VALIDATION");
            assert!(error.to_string().contains("does not accept positional"));
        }
    }

    #[test]
    fn vector_descriptor_preparation_requires_and_routes_declared_properties() {
        let graph = GraphForge::new(None).unwrap();
        for by in [ClusterAlgorithm::Hdbscan, ClusterAlgorithm::KMeans] {
            let options = ClusterOptions {
                by,
                vector_property: None,
                via: Some("IGNORED".into()),
                directed: true,
                write_property: None,
            };
            let error = graph
                .prepare_cluster_invocation("Person", &options)
                .unwrap_err();
            assert_eq!(error.code(), "GF_VALIDATION");
            assert_eq!(
                error.to_string(),
                format!("validation error: cluster.{by} requires vector_property")
            );
        }

        for by in [
            SimilarAlgorithm::Knn,
            SimilarAlgorithm::FilteredKnn,
            SimilarAlgorithm::Cosine,
        ] {
            let options = SimilarOptions {
                by,
                k: 3,
                vector_property: None,
                via: Some("KNOWS".into()),
            };
            let error = graph
                .prepare_similar_invocation("Person", &options)
                .unwrap_err();
            assert_eq!(error.code(), "GF_VALIDATION");
            assert_eq!(
                error.to_string(),
                format!("validation error: similar.{by} requires vector_property")
            );
        }

        let cluster = graph
            .prepare_cluster_invocation(
                "Person",
                &ClusterOptions {
                    by: ClusterAlgorithm::KMeans,
                    vector_property: Some("embedding".into()),
                    via: None,
                    directed: true,
                    write_property: None,
                },
            )
            .unwrap();
        assert_eq!(
            cluster.parameters()["vector_property"],
            InvocationParameter::Utf8("embedding".into())
        );
        assert!(!cluster.parameters().contains_key("via"));

        let similar = graph
            .prepare_similar_invocation(
                "Person",
                &SimilarOptions {
                    by: SimilarAlgorithm::FilteredKnn,
                    k: 3,
                    vector_property: Some("embedding".into()),
                    via: Some("KNOWS".into()),
                },
            )
            .unwrap();
        assert_eq!(
            similar.parameters()["vector_property"],
            InvocationParameter::Utf8("embedding".into())
        );
        assert_eq!(
            similar.parameters()["via"],
            InvocationParameter::Utf8("KNOWS".into())
        );
    }

    #[test]
    fn graphsage_executes_through_typed_api_with_scalar_and_list_features() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute(
                "CREATE (:Person {name:'Alice', score:1.0, features:[1.0,0.0]})\
                 -[:KNOWS]->(:Person {name:'Bob', score:2.0, features:[0.0,1.0]}), \
                 (:Person {name:'Carol', score:3.0, features:[0.5,0.5]})",
            )
            .unwrap();
        let options = EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::GraphSage,
            via: Some("KNOWS".to_owned()),
            directed: false,
            weight: None,
            options: EmbeddingOptions::GraphSage(GraphSageOptions {
                dimensions: 2,
                hidden_dimensions: 2,
                layers: 1,
                sample_sizes: vec![1],
                epochs: 1,
                negative_samples: 1,
                learning_rate: 0.001,
                feature_properties: vec!["score".to_owned(), "features".to_owned()],
                seed: 13,
                ..GraphSageOptions::default()
            }),
        };
        let empty = GraphForge::new(None)
            .unwrap()
            .analyze_embedding(Some("Person"), &options)
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(
            empty.schema().metadata()["graphforge.algorithm"],
            "graphsage"
        );
        assert_eq!(empty.schema().metadata()["graphforge.dimensions"], "2");

        let first = graph.analyze_embedding(Some("Person"), &options).unwrap();
        assert_eq!(
            first,
            graph.analyze_embedding(Some("Person"), &options).unwrap()
        );
        assert_eq!(first.num_rows(), 3);
        assert_eq!(
            first
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("node_uuid", &DataType::FixedSizeBinary(16), false),
                (
                    "embedding",
                    &DataType::FixedSizeList(
                        Arc::new(arrow::datatypes::Field::new(
                            "item",
                            DataType::Float32,
                            false
                        )),
                        2
                    ),
                    false
                )
            ]
        );
        assert_eq!(
            first.schema().metadata()["graphforge.algorithm"],
            "graphsage"
        );
        assert_eq!(
            first.schema().metadata()["graphforge.algorithm_version"],
            "graphsage-unsupervised-v1"
        );
        assert_eq!(first.schema().metadata()["graphforge.dimensions"], "2");
        assert_eq!(first.schema().metadata()["graphforge.seed"], "13");
        let uuids = first
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!((1..uuids.len()).all(|row| uuids.value(row - 1) < uuids.value(row)));
        let embeddings = first
            .column_by_name("embedding")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(embeddings.value_length(), 2);
        assert_eq!(embeddings.null_count(), 0);
        assert!(
            embeddings
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .all(|value| value.is_some_and(f32::is_finite))
        );

        let mut invalid = options.clone();
        let EmbeddingOptions::GraphSage(invalid_options) = &mut invalid.options else {
            unreachable!()
        };
        invalid_options.feature_properties = vec!["missing".to_owned()];
        assert!(matches!(
            graph.analyze_embedding(Some("Person"), &invalid),
            Err(GfError::Validation(message)) if message.contains("missing feature property")
        ));

        drop(graph);
        assert_eq!(
            first,
            GraphForge::new(Some(path))
                .unwrap()
                .analyze_embedding(Some("Person"), &options)
                .unwrap()
        );
    }

    #[test]
    fn hashgnn_executes_through_typed_api_with_canonical_arrow_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute(
                "CREATE (:Person {name:'Alice', kind:'human'})\
                 -[:KNOWS {kind:'friend'}]->(:Person {name:'Bob', kind:'human'}), \
                 (:Person {name:'Carol', kind:'human'})",
            )
            .unwrap();
        let options = EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::HashGnn,
            via: Some("KNOWS".to_owned()),
            directed: true,
            weight: None,
            options: EmbeddingOptions::HashGnn(HashGnnOptions {
                dimensions: 8,
                iterations: 2,
                embedding_density: 0.25,
                seed: 19,
                ..HashGnnOptions::default()
            }),
        };
        let empty = GraphForge::new(None)
            .unwrap()
            .analyze_embedding(Some("Person"), &options)
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema().metadata()["graphforge.algorithm"], "hashgnn");
        assert_eq!(empty.schema().metadata()["graphforge.dimensions"], "8");

        let first = graph.analyze_embedding(Some("Person"), &options).unwrap();
        assert_eq!(
            first,
            graph.analyze_embedding(Some("Person"), &options).unwrap()
        );
        assert_eq!(first.num_rows(), 3);
        assert_eq!(
            first
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("node_uuid", &DataType::FixedSizeBinary(16), false),
                (
                    "embedding",
                    &DataType::FixedSizeList(
                        Arc::new(arrow::datatypes::Field::new(
                            "item",
                            DataType::Float32,
                            false
                        )),
                        8
                    ),
                    false
                )
            ]
        );
        assert_eq!(first.schema().metadata()["graphforge.algorithm"], "hashgnn");
        assert_eq!(
            first.schema().metadata()["graphforge.algorithm_version"],
            "hashgnn-v1"
        );
        assert_eq!(first.schema().metadata()["graphforge.dimensions"], "8");
        assert_eq!(first.schema().metadata()["graphforge.seed"], "19");
        let uuids = first
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!((1..uuids.len()).all(|row| uuids.value(row - 1) < uuids.value(row)));
        let embeddings = first
            .column_by_name("embedding")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(embeddings.value_length(), 8);
        assert_eq!(embeddings.null_count(), 0);
        assert!(
            embeddings
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .all(|value| value.is_some_and(|value| value == 0.0 || value == 1.0))
        );

        let heterogeneous = EmbeddingAnalyzeOptions {
            options: EmbeddingOptions::HashGnn(HashGnnOptions {
                heterogeneous: true,
                node_type_property: Some("kind".to_owned()),
                relationship_type_property: Some("kind".to_owned()),
                ..match &options.options {
                    EmbeddingOptions::HashGnn(options) => options.clone(),
                    _ => unreachable!(),
                }
            }),
            ..options.clone()
        };
        let typed = graph
            .analyze_embedding(Some("Person"), &heterogeneous)
            .unwrap();
        assert_ne!(first, typed);
        assert_eq!(
            typed,
            graph
                .analyze_embedding(Some("Person"), &heterogeneous)
                .unwrap()
        );
        let mut missing = heterogeneous.clone();
        let EmbeddingOptions::HashGnn(missing_options) = &mut missing.options else {
            unreachable!()
        };
        missing_options.relationship_type_property = Some("missing".to_owned());
        assert!(matches!(
            graph.analyze_embedding(Some("Person"), &missing),
            Err(GfError::Validation(message))
                if message.contains("missing HashGNN type property")
        ));

        let integer_graph = GraphForge::new(None).unwrap();
        integer_graph
            .execute(
                "CREATE (:Person {kind:1})-[:KNOWS {kind:7}]->(:Person {kind:2}), \
                 (:Person {kind:3})",
            )
            .unwrap();
        let integer_result = integer_graph
            .analyze_embedding(Some("Person"), &heterogeneous)
            .unwrap();
        assert_eq!(integer_result.num_rows(), 3);
        assert_eq!(
            integer_result,
            integer_graph
                .analyze_embedding(Some("Person"), &heterogeneous)
                .unwrap()
        );

        drop(graph);
        let reopened = GraphForge::new(Some(path)).unwrap();
        assert_eq!(
            first,
            reopened
                .analyze_embedding(Some("Person"), &options)
                .unwrap()
        );
        assert_eq!(
            typed,
            reopened
                .analyze_embedding(Some("Person"), &heterogeneous)
                .unwrap()
        );
    }

    #[test]
    fn cluster_vector_property_is_typed_and_owned_by_vector_algorithms() {
        let graph = GraphForge::new(None).unwrap();
        let validation = |options| match graph.cluster("Person", options) {
            Err(GfError::Validation(message)) => message,
            other => panic!("expected validation error, got {other:?}"),
        };

        for by in [ClusterAlgorithm::Hdbscan, ClusterAlgorithm::KMeans] {
            assert_eq!(
                validation(ClusterOptions {
                    by,
                    ..ClusterOptions::default()
                }),
                format!("cluster.{} requires vector_property", by.as_str())
            );
        }

        for by in [ClusterAlgorithm::Hdbscan, ClusterAlgorithm::KMeans] {
            assert_eq!(
                validation(ClusterOptions {
                    by,
                    vector_property: Some("features".into()),
                    via: Some("KNOWS".into()),
                    ..ClusterOptions::default()
                }),
                format!("cluster.{} does not accept via", by.as_str())
            );
        }

        for by in [
            ClusterAlgorithm::Components,
            ClusterAlgorithm::ApproximateMaxKCut,
            ClusterAlgorithm::StronglyConnected,
            ClusterAlgorithm::Biconnected,
            ClusterAlgorithm::KCoreDecomposition,
        ] {
            assert_eq!(
                validation(ClusterOptions {
                    by,
                    vector_property: Some("features".into()),
                    ..ClusterOptions::default()
                }),
                format!("cluster.{} does not accept vector_property", by.as_str())
            );
        }
        for property in ["", " features", "features ", "fea\ntures"] {
            assert_eq!(
                validation(ClusterOptions {
                    by: ClusterAlgorithm::Hdbscan,
                    vector_property: Some(property.into()),
                    ..ClusterOptions::default()
                }),
                format!("invalid cluster vector property {property:?}")
            );
        }
    }

    #[test]
    fn strongly_connected_obeys_direction_via_uuid_schema_and_atomic_writeback() {
        let empty = GraphForge::new(None)
            .unwrap()
            .cluster(
                "Person",
                strongly_connected_options(true, Some("KNOWS"), None),
            )
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(
            empty.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(empty.schema().field(1).data_type(), &DataType::Int64);

        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), \
                 (c:Person {name:'c'}), (d:Person {name:'d'}), \
                 (e:Person {name:'e'}), (f:Person {name:'f'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(d), \
                 (e)-[:KNOWS]->(f), (f)-[:OTHER]->(a)",
            )
            .unwrap();

        let directed = graph
            .cluster(
                "Person",
                strongly_connected_options(true, Some("KNOWS"), None),
            )
            .unwrap();
        assert_eq!(community_ids(&directed), [0, 0, 0, 1, 1, 2]);
        assert_eq!(
            directed
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert!(!directed.schema().field(0).is_nullable());
        assert!(!directed.schema().field(1).is_nullable());
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "strongly_connected"
        );
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            ["a", "b", "c", "d", "e", "f"].map(Some)
        );
        let expected = graph
            .execute("MATCH (p:Person) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
            .unwrap();
        assert_eq!(
            directed.column_by_name("node_uuid").unwrap(),
            expected.batches[0].column_by_name("node_uuid").unwrap()
        );
        assert_eq!(
            community_ids(
                &graph
                    .cluster(
                        "Person",
                        strongly_connected_options(false, Some("KNOWS"), None),
                    )
                    .unwrap()
            ),
            [0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            community_ids(
                &graph
                    .cluster("Person", strongly_connected_options(true, None, None))
                    .unwrap()
            ),
            [0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            graph
                .execute("MATCH (p:Person) WHERE p.scc IS NOT NULL RETURN p")
                .unwrap()
                .stats
                .rows_produced,
            0
        );

        graph
            .execute("MATCH (p:Person {name:'a'}) SET p.scc_atomic = 'old'")
            .unwrap();
        assert!(matches!(
            graph.cluster(
                "Person",
                strongly_connected_options(true, Some("KNOWS"), Some("scc_atomic")),
            ),
            Err(GfError::Validation(_))
        ));
        let unchanged = graph
            .execute(
                "MATCH (p:Person) WHERE p.scc_atomic IS NOT NULL \
                 RETURN p.scc_atomic AS value",
            )
            .unwrap();
        assert_eq!(unchanged.stats.rows_produced, 1);
        assert_eq!(
            unchanged.batches[0]
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "old"
        );
        graph
            .cluster(
                "Person",
                strongly_connected_options(true, Some("KNOWS"), Some("scc")),
            )
            .unwrap();
        let written = graph
            .execute("MATCH (p:Person) RETURN p.scc AS scc ORDER BY p.name")
            .unwrap();
        assert_eq!(
            written.batches[0]
                .column_by_name("scc")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 0, 0, 1, 1, 2]
        );
    }

    #[test]
    fn biconnected_projects_overlap_direction_neutrally_and_writes_atomically() {
        let graph = GraphForge::new(None).unwrap();
        assert_eq!(
            graph
                .cluster("Person", biconnected_options(true, Some("KNOWS"), None))
                .unwrap()
                .num_rows(),
            0
        );
        graph
            .execute(
                "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), \
                 (c:Person {name:'c'}), (d:Person {name:'d'}), \
                 (e:Person {name:'e'}), (f:Person {name:'f'}), \
                 (g:Person {name:'g'}), (a)-[:KNOWS {weight:99}]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), \
                 (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(c), \
                 (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(f), (g)-[:OTHER]->(a)",
            )
            .unwrap();

        let options = biconnected_options(true, Some("KNOWS"), None);
        let directed = graph.cluster("Person", options.clone()).unwrap();
        assert_eq!(community_ids(&directed), [0, 0, 0, 1, 1, 2, 3]);
        assert_eq!(
            community_ids(
                &graph
                    .cluster("Person", biconnected_options(false, Some("KNOWS"), None))
                    .unwrap()
            ),
            community_ids(&directed)
        );
        assert_eq!(
            community_ids(
                &graph
                    .cluster("Person", biconnected_options(true, Some("OTHER"), None))
                    .unwrap()
            ),
            [0, 1, 2, 3, 4, 5, 0]
        );
        assert_eq!(
            directed
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "biconnected"
        );
        assert!(!directed.schema().field(0).is_nullable());
        assert!(!directed.schema().field(1).is_nullable());
        assert!(directed.column_by_name("node_id").is_none());
        let expected = graph
            .execute("MATCH (p:Person) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
            .unwrap();
        assert_eq!(
            directed.column_by_name("node_uuid").unwrap(),
            expected.batches[0].column_by_name("node_uuid").unwrap()
        );
        assert_eq!(
            graph
                .execute("MATCH (p:Person) WHERE p.block IS NOT NULL RETURN p")
                .unwrap()
                .stats
                .rows_produced,
            0
        );

        graph
            .execute("MATCH (p:Person {name:'a'}) SET p.atomic_block = 'old'")
            .unwrap();
        assert!(matches!(
            graph.cluster(
                "Person",
                biconnected_options(true, Some("KNOWS"), Some("atomic_block"))
            ),
            Err(GfError::Validation(_))
        ));
        let unchanged = graph
            .execute(
                "MATCH (p:Person) WHERE p.atomic_block IS NOT NULL \
                 RETURN p.atomic_block AS value",
            )
            .unwrap();
        assert_eq!(unchanged.stats.rows_produced, 1);
        assert_eq!(
            unchanged.batches[0]
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "old"
        );
        graph
            .cluster(
                "Person",
                biconnected_options(true, Some("KNOWS"), Some("block")),
            )
            .unwrap();
        let written = graph
            .execute("MATCH (p:Person) RETURN p.block AS block ORDER BY p.name")
            .unwrap();
        assert_eq!(
            written.batches[0]
                .column_by_name("block")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 0, 0, 1, 1, 2, 3]
        );
    }

    #[test]
    fn k_core_decomposition_shapes_exact_numbers_and_writes_atomically() {
        let graph = GraphForge::new(None).unwrap();
        assert_eq!(
            graph
                .cluster(
                    "Person",
                    k_core_decomposition_options(true, Some("KNOWS"), None)
                )
                .unwrap()
                .num_rows(),
            0
        );
        graph
            .execute(
                "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), \
                 (c:Person {name:'c'}), (d:Person {name:'d'}), \
                 (e:Person {name:'e'}), (f:Person {name:'f'}), \
                 (g:Person {name:'g'}), (h:Person {name:'h'}), \
                 (i:Person {name:'i'}), (j:Person {name:'j'}), \
                 (a)-[:KNOWS {weight:99}]->(b), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(a), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), \
                 (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(c), (a)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), \
                 (h)-[:KNOWS]->(i), (i)-[:KNOWS]->(j), (j)-[:KNOWS]->(h), \
                 (f)-[:OTHER]->(a)",
            )
            .unwrap();

        let options = k_core_decomposition_options(true, Some("KNOWS"), None);
        let directed = graph.cluster("Person", options).unwrap();
        assert_eq!(community_ids(&directed), [3, 3, 3, 3, 1, 1, 0, 2, 2, 2]);
        assert_eq!(
            community_ids(
                &graph
                    .cluster(
                        "Person",
                        k_core_decomposition_options(false, Some("KNOWS"), None)
                    )
                    .unwrap()
            ),
            community_ids(&directed)
        );
        assert_eq!(
            community_ids(
                &graph
                    .cluster(
                        "Person",
                        k_core_decomposition_options(true, Some("OTHER"), None)
                    )
                    .unwrap()
            ),
            [1, 0, 0, 0, 0, 1, 0, 0, 0, 0]
        );
        assert_eq!(
            directed
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            directed.schema().metadata()["graphforge.algorithm"],
            "k_core_decomposition"
        );
        assert!(!directed.schema().field(0).is_nullable());
        assert!(!directed.schema().field(1).is_nullable());
        assert!(directed.column_by_name("node_id").is_none());
        let expected = graph
            .execute("MATCH (p:Person) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
            .unwrap();
        assert_eq!(
            directed.column_by_name("node_uuid").unwrap(),
            expected.batches[0].column_by_name("node_uuid").unwrap()
        );
        let read_only = graph
            .execute("MATCH (p:Person) WHERE p.core IS NOT NULL RETURN p")
            .unwrap();
        assert_eq!(read_only.stats.rows_produced, 0);

        graph
            .execute("MATCH (p:Person {name:'a'}) SET p.atomic_core = 'old'")
            .unwrap();
        assert!(matches!(
            graph.cluster(
                "Person",
                k_core_decomposition_options(true, Some("KNOWS"), Some("atomic_core"))
            ),
            Err(GfError::Validation(_))
        ));
        let unchanged = graph
            .execute(
                "MATCH (p:Person) WHERE p.atomic_core IS NOT NULL \
                 RETURN p.atomic_core AS value",
            )
            .unwrap();
        assert_eq!(unchanged.stats.rows_produced, 1);
        assert_eq!(
            unchanged.batches[0]
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "old"
        );
        graph
            .cluster(
                "Person",
                k_core_decomposition_options(true, Some("KNOWS"), Some("core")),
            )
            .unwrap();
        let written = graph
            .execute("MATCH (p:Person) RETURN p.core AS core ORDER BY p.name")
            .unwrap();
        assert_eq!(
            written.batches[0]
                .column_by_name("core")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[3, 3, 3, 3, 1, 1, 0, 2, 2, 2]
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless.execute("CREATE (:Person), (:Person)").unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", k_core_decomposition_options(true, None, None))
                    .unwrap()
            ),
            [0, 0]
        );
    }

    #[test]
    fn approximate_max_cut_dispatches_uuid_partition_and_atomic_writeback() {
        let empty = GraphForge::new(None)
            .unwrap()
            .cluster(
                "Person",
                approximate_max_cut_options(false, Some("KNOWS"), None),
            )
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(
            empty.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(empty.schema().field(1).data_type(), &DataType::Int64);

        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), \
                 (c:Person {name:'c'}), (d:Person {name:'d'}), \
                 (e:Person {name:'e'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(a), (a)-[:OTHER]->(e)",
            )
            .unwrap();
        let result = graph
            .cluster(
                "Person",
                approximate_max_cut_options(false, Some("KNOWS"), None),
            )
            .unwrap();
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert!(!result.schema().field(0).is_nullable());
        assert!(!result.schema().field(1).is_nullable());
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm"],
            "approximate_max_k_cut"
        );
        assert_eq!(community_ids(&result), [0, 1, 0, 1, 0]);
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]
        );
        let expected = graph
            .execute("MATCH (p:Person) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
            .unwrap();
        assert_eq!(
            result.column_by_name("node_uuid").unwrap(),
            expected.batches[0].column_by_name("node_uuid").unwrap()
        );
        assert_eq!(
            result,
            graph
                .cluster(
                    "Person",
                    approximate_max_cut_options(true, Some("KNOWS"), None),
                )
                .unwrap()
        );
        assert_eq!(
            community_ids(
                &graph
                    .cluster("Person", approximate_max_cut_options(false, None, None))
                    .unwrap()
            ),
            [0, 1, 0, 1, 1]
        );
        assert_eq!(
            graph
                .execute("MATCH (p:Person) WHERE p.cluster IS NOT NULL RETURN p")
                .unwrap()
                .stats
                .rows_produced,
            0
        );

        graph
            .execute("MATCH (p:Person {name:'a'}) SET p.maxcut_atomic = 'old'")
            .unwrap();
        assert!(matches!(
            graph.cluster(
                "Person",
                approximate_max_cut_options(false, Some("KNOWS"), Some("maxcut_atomic")),
            ),
            Err(GfError::Validation(_))
        ));
        let unchanged = graph
            .execute(
                "MATCH (p:Person) WHERE p.maxcut_atomic IS NOT NULL \
                 RETURN p.maxcut_atomic AS value",
            )
            .unwrap();
        assert_eq!(unchanged.stats.rows_produced, 1);
        assert_eq!(
            unchanged.batches[0]
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "old"
        );
        graph
            .cluster(
                "Person",
                approximate_max_cut_options(false, Some("KNOWS"), Some("cluster")),
            )
            .unwrap();
        let written = graph
            .execute("MATCH (p:Person) RETURN p.cluster AS cluster ORDER BY p.name")
            .unwrap();
        assert_eq!(
            written.batches[0]
                .column_by_name("cluster")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 1, 0, 1, 0]
        );
    }

    #[test]
    fn kmeans_dispatches_exact_uuid_clusters_and_atomic_writeback() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = (0..20)
            .map(|point| {
                let value = f64::from(point / 2 * 10) + f64::from(point % 2) * 0.25;
                format!("(:Point {{name:'p{point:02}', features:[{value:.2}]}})")
            })
            .collect::<Vec<_>>()
            .join(",");
        graph.execute(&format!("CREATE {nodes}")).unwrap();
        let options = |directed, write_property| ClusterOptions {
            by: ClusterAlgorithm::KMeans,
            vector_property: Some("features".into()),
            directed,
            write_property,
            ..ClusterOptions::default()
        };
        let result = graph.cluster("Point", options(false, None)).unwrap();
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "features", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(!result.schema().field(0).is_nullable());
        assert!(!result.schema().field(1).is_nullable());
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm"],
            "k_means"
        );
        assert_eq!(
            community_ids(&result),
            (0..10).flat_map(|group| [group, group]).collect::<Vec<_>>()
        );
        let expected = graph
            .execute("MATCH (p:Point) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
            .unwrap();
        assert_eq!(
            result.column_by_name("node_uuid").unwrap(),
            expected.batches[0].column_by_name("node_uuid").unwrap()
        );
        assert_eq!(result, graph.cluster("Point", options(true, None)).unwrap());
        assert_eq!(
            graph
                .execute("MATCH (p:Point) WHERE p.cluster IS NOT NULL RETURN p")
                .unwrap()
                .stats
                .rows_produced,
            0
        );

        graph
            .execute("MATCH (p:Point {name:'p00'}) SET p.kmeans_atomic = 'old'")
            .unwrap();
        assert!(matches!(
            graph.cluster("Point", options(false, Some("kmeans_atomic".into()))),
            Err(GfError::Validation(_))
        ));
        let unchanged = graph
            .execute(
                "MATCH (p:Point) WHERE p.kmeans_atomic IS NOT NULL \
                 RETURN p.name AS name, p.kmeans_atomic AS value",
            )
            .unwrap();
        assert_eq!(unchanged.stats.rows_produced, 1);
        assert_eq!(
            unchanged.batches[0]
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "old"
        );
        graph
            .cluster("Point", options(false, Some("cluster".into())))
            .unwrap();
        let written = graph
            .execute("MATCH (p:Point) RETURN p.cluster AS cluster ORDER BY p.name")
            .unwrap();
        assert_eq!(
            written.batches[0]
                .column_by_name("cluster")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9]
        );
    }

    #[test]
    fn kmeans_empty_small_and_invalid_vectors_are_structured() {
        let options = || ClusterOptions {
            by: ClusterAlgorithm::KMeans,
            vector_property: Some("features".into()),
            ..ClusterOptions::default()
        };
        let empty = GraphForge::new(None)
            .unwrap()
            .cluster("Point", options())
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(
            empty.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(empty.schema().field(1).data_type(), &DataType::Int64);

        for query in [
            "CREATE (:Point {features:[0.0]}), (:Point {features:[1.0]})",
            "CREATE (:Point {features:[0.0]}), (:Point {name:'missing'})",
        ] {
            let graph = GraphForge::new(None).unwrap();
            graph.execute(query).unwrap();
            assert!(matches!(
                graph.cluster("Point", options()),
                Err(GfError::Validation(_) | GfError::Execution(_))
            ));
        }
    }

    #[test]
    fn hdbscan_dispatches_stable_uuid_clusters_and_opt_in_writeback() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (:Person {name:'a0', features:[0.0]}), \
                 (:Person {name:'a1', features:[0.1]}), \
                 (:Person {name:'a2', features:[0.2]}), \
                 (:Person {name:'a3', features:[0.3]}), \
                 (:Person {name:'a4', features:[0.4]}), \
                 (:Person {name:'b0', features:[10.0]}), \
                 (:Person {name:'b1', features:[10.1]}), \
                 (:Person {name:'b2', features:[10.2]}), \
                 (:Person {name:'b3', features:[10.3]}), \
                 (:Person {name:'b4', features:[10.4]}), \
                 (:Person {name:'noise', features:[100.0]})",
            )
            .unwrap();
        let options = |directed, write_property| ClusterOptions {
            by: ClusterAlgorithm::Hdbscan,
            vector_property: Some("features".into()),
            via: None,
            directed,
            write_property,
        };

        let undirected = graph.cluster("Person", options(false, None)).unwrap();
        assert_eq!(
            undirected
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "features", "name"]
        );
        assert!(undirected.column_by_name("node_id").is_none());
        assert_eq!(
            undirected.schema().metadata()["graphforge.algorithm"],
            "hdbscan"
        );
        assert_eq!(
            undirected
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(Option::unwrap)
                .collect::<Vec<_>>(),
            [
                "a0", "a1", "a2", "a3", "a4", "b0", "b1", "b2", "b3", "b4", "noise"
            ]
        );
        assert_eq!(
            community_ids(&undirected),
            [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, -1]
        );
        assert_eq!(
            community_ids(&graph.cluster("Person", options(true, None)).unwrap()),
            community_ids(&undirected)
        );
        assert_eq!(
            graph
                .execute("MATCH (p:Person) WHERE p.cluster IS NOT NULL RETURN p")
                .unwrap()
                .stats
                .rows_produced,
            0
        );

        graph
            .execute("MATCH (p:Person {name:'a0'}) SET p.hdbscan_atomic = 'old'")
            .unwrap();
        assert!(matches!(
            graph.cluster("Person", options(false, Some("hdbscan_atomic".into()))),
            Err(GfError::Validation(_))
        ));
        let unchanged = graph
            .execute(
                "MATCH (p:Person) WHERE p.hdbscan_atomic IS NOT NULL \
                 RETURN p.name AS name, p.hdbscan_atomic AS value",
            )
            .unwrap();
        assert_eq!(unchanged.stats.rows_produced, 1);
        for (column, expected) in [("name", "a0"), ("value", "old")] {
            assert_eq!(
                unchanged.batches[0]
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }

        graph
            .cluster("Person", options(false, Some("cluster".into())))
            .unwrap();
        let readback = graph
            .execute("MATCH (p:Person) RETURN p.cluster AS cluster ORDER BY p.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("cluster")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 0, 0, 0, 0, 1, 1, 1, 1, 1, -1]
        );
    }

    #[test]
    fn hdbscan_handles_empty_small_duplicate_and_all_noise_boundaries() {
        let empty = GraphForge::new(None).unwrap();
        let options = || ClusterOptions {
            by: ClusterAlgorithm::Hdbscan,
            vector_property: Some("features".into()),
            ..ClusterOptions::default()
        };
        let result = empty.cluster("Point", options()).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.schema().field(0).name(), "node_uuid");
        assert_eq!(result.schema().field(1).name(), "community_id");

        for (query, expected) in [
            (
                "CREATE (:Point {features:[0.0]}), (:Point {features:[1.0]}), \
                 (:Point {features:[2.0]}), (:Point {features:[3.0]})",
                vec![-1; 4],
            ),
            (
                "CREATE (:Point {features:[1.0]}), (:Point {features:[1.0]}), \
                 (:Point {features:[1.0]}), (:Point {features:[1.0]}), \
                 (:Point {features:[1.0]})",
                vec![-1; 5],
            ),
            (
                "CREATE (:Point {features:[0.0]}), (:Point {features:[10.0]}), \
                 (:Point {features:[20.0]}), (:Point {features:[30.0]}), \
                 (:Point {features:[40.0]})",
                vec![-1; 5],
            ),
        ] {
            let graph = GraphForge::new(None).unwrap();
            graph.execute(query).unwrap();
            assert_eq!(
                community_ids(&graph.cluster("Point", options()).unwrap()),
                expected
            );
        }
    }

    #[test]
    fn hdbscan_vector_failures_are_structured_before_dispatch() {
        for query in [
            "CREATE (:Point {name:'missing'})",
            "CREATE (:Point {features:null})",
            "CREATE (:Point {features:[]})",
            "CREATE (:Point {features:[1.0]}), (:Point {features:[1.0,2.0]})",
        ] {
            let graph = GraphForge::new(None).unwrap();
            graph.execute(query).unwrap();
            assert!(matches!(
                graph.cluster(
                    "Point",
                    ClusterOptions {
                        by: ClusterAlgorithm::Hdbscan,
                        vector_property: Some("features".into()),
                        ..ClusterOptions::default()
                    }
                ),
                Err(GfError::Validation(_))
            ));
        }
    }

    #[test]
    fn louvain_obeys_uuid_partition_selection_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Frank'}), (g:Person), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), \
                 (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), \
                 (f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), (a)-[:OTHER]->(g)",
            )
            .unwrap();

        let directed = graph
            .cluster("Person", louvain_options(true, Some("KNOWS"), None))
            .unwrap();
        assert_eq!(community_ids(&directed), [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            directed
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            directed.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(directed.schema().field(1).data_type(), &DataType::Int64);
        assert!(directed.column_by_name("node_id").is_none());
        assert_eq!(
            directed
                .schema()
                .metadata()
                .get("graphforge.algorithm")
                .map(String::as_str),
            Some("louvain")
        );
        assert_eq!(
            directed
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("Alice"),
                Some("Bob"),
                Some("Carol"),
                Some("Dan"),
                Some("Eve"),
                Some("Frank"),
                None,
            ]
        );
        assert_eq!(
            directed,
            graph
                .cluster("Person", louvain_options(false, Some("KNOWS"), None))
                .unwrap()
        );
        assert_eq!(
            community_ids(
                &graph
                    .cluster("Person", louvain_options(true, None, None))
                    .unwrap()
            ),
            [0, 0, 0, 1, 1, 1, 0]
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        let written = graph
            .cluster(
                "Person",
                louvain_options(true, Some("KNOWS"), Some("group_id")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), [0, 0, 0, 1, 1, 1, 2]);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY id, n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 0, 0, 1, 1, 1, 2]
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", louvain_options(true, None, None))
                    .unwrap()
            ),
            [0, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", louvain_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn leiden_obeys_uuid_refinement_selection_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (h:Person {name:'H'}), \
                 (a)-[:KNOWS]->(e), (a)-[:KNOWS]->(e), (e)-[:KNOWS]->(a), \
                 (a)-[:KNOWS]->(g), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(f), \
                 (b)-[:KNOWS]->(g), (c)-[:KNOWS]->(g), (d)-[:KNOWS]->(g), \
                 (e)-[:KNOWS]->(g), (f)-[:KNOWS]->(g), (a)-[:KNOWS]->(a), \
                 (a)-[:OTHER]->(h)",
            )
            .unwrap();

        let result = graph
            .cluster("Person", leiden_options(true, Some("KNOWS"), None))
            .unwrap();
        assert_eq!(community_ids(&result), [0, 1, 1, 0, 0, 1, 0, 2]);
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result
                .schema()
                .metadata()
                .get("graphforge.algorithm")
                .map(String::as_str),
            Some("leiden")
        );
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F"),
                Some("G"),
                Some("H"),
            ]
        );
        assert_eq!(
            result,
            graph
                .cluster("Person", leiden_options(false, Some("KNOWS"), None))
                .unwrap()
        );
        assert_eq!(
            result,
            graph
                .cluster("Person", leiden_options(true, Some("KNOWS"), None))
                .unwrap()
        );
        assert_ne!(
            community_ids(&result),
            community_ids(
                &graph
                    .cluster("Person", louvain_options(true, Some("KNOWS"), None))
                    .unwrap()
            )
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        let written = graph
            .cluster(
                "Person",
                leiden_options(true, Some("KNOWS"), Some("group_id")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), [0, 1, 1, 0, 0, 1, 0, 2]);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 1, 1, 0, 0, 1, 0, 2]
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", leiden_options(true, None, None))
                    .unwrap()
            ),
            [0, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", leiden_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn label_propagation_obeys_uuid_selection_and_writeback_contracts() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), \
                 (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), \
                 (c)-[:OTHER]->(d)",
            )
            .unwrap();

        let result = graph
            .cluster(
                "Person",
                label_propagation_options(true, Some("KNOWS"), None),
            )
            .unwrap();
        assert_eq!(community_ids(&result), [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result
                .schema()
                .metadata()
                .get("graphforge.algorithm")
                .map(String::as_str),
            Some("label_propagation")
        );
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F"),
                Some("G"),
            ]
        );
        assert_eq!(
            result,
            graph
                .cluster(
                    "Person",
                    label_propagation_options(false, Some("KNOWS"), None),
                )
                .unwrap()
        );
        assert_eq!(
            result,
            graph
                .cluster(
                    "Person",
                    label_propagation_options(true, Some("KNOWS"), None),
                )
                .unwrap()
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        let written = graph
            .cluster(
                "Person",
                label_propagation_options(true, Some("KNOWS"), Some("group_id")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), [0, 0, 0, 1, 1, 1, 2]);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 0, 0, 1, 1, 1, 2]
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", label_propagation_options(true, None, None))
                    .unwrap()
            ),
            [0, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", label_propagation_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn speaker_listener_obeys_uuid_selection_and_writeback_contracts() {
        // Exploratory mode has no ontology/knowledge layer; graph-native output must not depend on it.
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), \
                 (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), \
                 (c)-[:OTHER]->(d)",
            )
            .unwrap();

        let result = graph
            .cluster(
                "Person",
                speaker_listener_options(true, Some("KNOWS"), None),
            )
            .unwrap();
        let expected = [0, 0, 0, 1, 1, 1, 2];
        assert_eq!(community_ids(&result), expected);
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result
                .schema()
                .metadata()
                .get("graphforge.algorithm")
                .map(String::as_str),
            Some("speaker_listener")
        );
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F"),
                Some("G"),
            ]
        );
        assert_eq!(
            result,
            graph
                .cluster(
                    "Person",
                    speaker_listener_options(false, Some("KNOWS"), None),
                )
                .unwrap()
        );
        assert_eq!(
            result,
            graph
                .cluster(
                    "Person",
                    speaker_listener_options(true, Some("KNOWS"), None),
                )
                .unwrap()
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.slpa_group IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        let written = graph
            .cluster(
                "Person",
                speaker_listener_options(true, Some("KNOWS"), Some("slpa_group")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), expected);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.slpa_group AS id ORDER BY n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &expected
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", speaker_listener_options(true, None, None))
                    .unwrap()
            ),
            [0, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", speaker_listener_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn girvan_newman_obeys_uuid_selection_and_writeback_contracts() {
        // Exploratory mode has no ontology/knowledge layer; graph-native output must not depend on it.
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (f)-[:KNOWS]->(f), (a)-[:OTHER]->(e)",
            )
            .unwrap();

        let result = graph
            .cluster("Person", girvan_newman_options(true, Some("KNOWS"), None))
            .unwrap();
        let expected = [0, 0, 0, 1, 1, 1, 2];
        assert_eq!(community_ids(&result), expected);
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result
                .schema()
                .metadata()
                .get("graphforge.algorithm")
                .map(String::as_str),
            Some("girvan_newman")
        );
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F"),
                Some("G")
            ]
        );
        assert_eq!(
            result,
            graph
                .cluster("Person", girvan_newman_options(false, Some("KNOWS"), None))
                .unwrap()
        );
        assert_eq!(
            result,
            graph
                .cluster("Person", girvan_newman_options(true, Some("KNOWS"), None))
                .unwrap()
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.gn_group IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        let written = graph
            .cluster(
                "Person",
                girvan_newman_options(true, Some("KNOWS"), Some("gn_group")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), expected);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.gn_group AS id ORDER BY n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &expected
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", girvan_newman_options(true, None, None))
                    .unwrap()
            ),
            [0, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", girvan_newman_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn modularity_optimization_obeys_uuid_selection_and_writeback_contracts() {
        // Exploratory mode has no ontology/knowledge layer; graph-native output must not depend on it.
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (f)-[:KNOWS]->(f), (a)-[:OTHER]->(e)",
            )
            .unwrap();

        let options = modularity_optimization_options(true, Some("KNOWS"), None);
        let result = graph.cluster("Person", options.clone()).unwrap();
        let expected = [0, 0, 0, 1, 1, 1, 2];
        assert_eq!(community_ids(&result), expected);
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result
                .schema()
                .metadata()
                .get("graphforge.algorithm")
                .map(String::as_str),
            Some("modularity_optimization")
        );
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F"),
                Some("G")
            ]
        );
        assert_eq!(result, graph.cluster("Person", options).unwrap());
        assert_eq!(
            result,
            graph
                .cluster(
                    "Person",
                    modularity_optimization_options(false, Some("KNOWS"), None),
                )
                .unwrap()
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.mod_group IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        let written = graph
            .cluster(
                "Person",
                modularity_optimization_options(true, Some("KNOWS"), Some("mod_group")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), expected);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.mod_group AS id ORDER BY n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &expected
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", modularity_optimization_options(true, None, None),)
                    .unwrap()
            ),
            [0, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", modularity_optimization_options(true, None, None),)
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn fastgreedy_obeys_uuid_selection_and_writeback_contracts() {
        // Exploratory mode has no ontology/knowledge layer; graph-native output must not depend on it.
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (f)-[:KNOWS]->(f), (a)-[:OTHER]->(e)",
            )
            .unwrap();

        let options = fastgreedy_options(true, Some("KNOWS"), None);
        let result = graph.cluster("Person", options.clone()).unwrap();
        let expected = [0, 0, 0, 1, 1, 1, 2];
        assert_eq!(community_ids(&result), expected);
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result
                .schema()
                .metadata()
                .get("graphforge.algorithm")
                .map(String::as_str),
            Some("fastgreedy")
        );
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F"),
                Some("G")
            ]
        );
        assert_eq!(result, graph.cluster("Person", options).unwrap());
        assert_eq!(
            result,
            graph
                .cluster("Person", fastgreedy_options(false, Some("KNOWS"), None),)
                .unwrap()
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.fast_group IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        let written = graph
            .cluster(
                "Person",
                fastgreedy_options(true, Some("KNOWS"), Some("fast_group")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), expected);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.fast_group AS id ORDER BY n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &expected
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", fastgreedy_options(true, None, None))
                    .unwrap()
            ),
            [0, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", fastgreedy_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn infomap_obeys_uuid_selection_flow_and_writeback_contracts() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(c), (b)-[:OTHER]->(c)",
            )
            .unwrap();

        let options = infomap_options(true, Some("KNOWS"), None);
        let result = graph.cluster("Person", options.clone()).unwrap();
        let expected = [0, 0, 1, 1, 2];
        assert_eq!(community_ids(&result), expected);
        assert_eq!(result, graph.cluster("Person", options).unwrap());
        assert_eq!(
            result,
            graph
                .cluster("Person", infomap_options(false, Some("KNOWS"), None))
                .unwrap()
        );
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm"],
            "infomap"
        );
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [Some("A"), Some("B"), Some("C"), Some("D"), Some("E")]
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.flow_group IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        let written = graph
            .cluster(
                "Person",
                infomap_options(true, Some("KNOWS"), Some("flow_group")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), expected);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.flow_group AS id ORDER BY n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &expected
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", infomap_options(true, None, None))
                    .unwrap()
            ),
            [0, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", infomap_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn leading_eigenvector_obeys_uuid_spectral_and_writeback_contracts() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (a)-[:OTHER]->(e)",
            )
            .unwrap();

        let options = leading_eigenvector_options(true, Some("KNOWS"), None);
        let result = graph.cluster("Person", options.clone()).unwrap();
        let expected = [0, 0, 0, 1, 1, 1, 2];
        assert_eq!(community_ids(&result), expected);
        assert_eq!(result, graph.cluster("Person", options).unwrap());
        assert_eq!(
            result,
            graph
                .cluster(
                    "Person",
                    leading_eigenvector_options(false, Some("KNOWS"), None),
                )
                .unwrap()
        );
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm"],
            "leading_eigenvector"
        );
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F"),
                Some("G")
            ]
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.spectral_group IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );
        graph
            .execute("MATCH (n:Person {name:'A'}) SET n.spectral_atomic = 'old'")
            .unwrap();
        assert!(matches!(
            graph.cluster(
                "Person",
                leading_eigenvector_options(true, Some("KNOWS"), Some("spectral_atomic"),),
            ),
            Err(GfError::Validation(_))
        ));
        let unchanged = graph
            .execute(
                "MATCH (n:Person) WHERE n.spectral_atomic IS NOT NULL \
                 RETURN n.name AS name, n.spectral_atomic AS value",
            )
            .unwrap();
        assert_eq!(unchanged.stats.rows_produced, 1);
        for (column, expected) in [("name", "A"), ("value", "old")] {
            assert_eq!(
                unchanged.batches[0]
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
        let written = graph
            .cluster(
                "Person",
                leading_eigenvector_options(true, Some("KNOWS"), Some("spectral_group")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), expected);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.spectral_group AS id ORDER BY n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &expected
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", leading_eigenvector_options(true, None, None))
                    .unwrap()
            ),
            [0, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", leading_eigenvector_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn walktrap_obeys_uuid_partition_and_atomic_writeback_contracts() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (a)-[:OTHER]->(e)",
            )
            .unwrap();

        let options = walktrap_options(true, Some("KNOWS"), None);
        let result = graph.cluster("Person", options.clone()).unwrap();
        let expected = [0, 0, 0, 1, 1, 1, 2];
        assert_eq!(community_ids(&result), expected);
        assert_eq!(result, graph.cluster("Person", options).unwrap());
        assert_eq!(
            result,
            graph
                .cluster("Person", walktrap_options(false, Some("KNOWS"), None))
                .unwrap()
        );
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm"],
            "walktrap"
        );
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F"),
                Some("G")
            ]
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.walktrap_group IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );

        graph
            .execute("MATCH (n:Person {name:'A'}) SET n.walktrap_atomic = 'old'")
            .unwrap();
        assert!(matches!(
            graph.cluster(
                "Person",
                walktrap_options(true, Some("KNOWS"), Some("walktrap_atomic")),
            ),
            Err(GfError::Validation(_))
        ));
        let unchanged = graph
            .execute(
                "MATCH (n:Person) WHERE n.walktrap_atomic IS NOT NULL \
                 RETURN n.name AS name, n.walktrap_atomic AS value",
            )
            .unwrap();
        assert_eq!(unchanged.stats.rows_produced, 1);
        for (column, expected) in [("name", "A"), ("value", "old")] {
            assert_eq!(
                unchanged.batches[0]
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }

        let written = graph
            .cluster(
                "Person",
                walktrap_options(true, Some("KNOWS"), Some("walktrap_group")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), expected);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.walktrap_group AS id ORDER BY n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &expected
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", walktrap_options(true, None, None))
                    .unwrap()
            ),
            [0, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", walktrap_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn spinglass_obeys_uuid_partition_and_atomic_writeback_contracts() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (a)-[:OTHER]->(g), (b)-[:OTHER]->(g), (c)-[:OTHER]->(g)",
            )
            .unwrap();

        let options = spinglass_options(true, Some("KNOWS"), None);
        let result = graph.cluster("Person", options.clone()).unwrap();
        let expected = [0, 0, 0, 1, 1, 1, 2];
        assert_eq!(community_ids(&result), expected);
        assert_eq!(result, graph.cluster("Person", options).unwrap());
        assert_ne!(
            community_ids(&result),
            community_ids(
                &graph
                    .cluster("Person", spinglass_options(true, None, None))
                    .unwrap()
            )
        );
        assert_eq!(
            result,
            graph
                .cluster("Person", spinglass_options(false, Some("KNOWS"), None))
                .unwrap()
        );
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["node_uuid", "community_id", "name"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
        assert!(result.column_by_name("node_id").is_none());
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm"],
            "spinglass"
        );
        assert_eq!(
            result
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
                Some("E"),
                Some("F"),
                Some("G")
            ]
        );
        assert_eq!(
            graph
                .execute("MATCH (n:Person) WHERE n.spin_group IS NOT NULL RETURN n.node_uuid")
                .unwrap()
                .stats
                .rows_produced,
            0
        );

        graph
            .execute("MATCH (n:Person {name:'A'}) SET n.spin_atomic = 'old'")
            .unwrap();
        assert!(matches!(
            graph.cluster(
                "Person",
                spinglass_options(true, Some("KNOWS"), Some("spin_atomic")),
            ),
            Err(GfError::Validation(_))
        ));
        let unchanged = graph
            .execute(
                "MATCH (n:Person) WHERE n.spin_atomic IS NOT NULL \
                 RETURN n.name AS name, n.spin_atomic AS value",
            )
            .unwrap();
        assert_eq!(unchanged.stats.rows_produced, 1);
        for (column, expected) in [("name", "A"), ("value", "old")] {
            assert_eq!(
                unchanged.batches[0]
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }

        let written = graph
            .cluster(
                "Person",
                spinglass_options(true, Some("KNOWS"), Some("spin_group")),
            )
            .unwrap();
        assert_eq!(community_ids(&written), expected);
        let readback = graph
            .execute("MATCH (n:Person) RETURN n.spin_group AS id ORDER BY n.name")
            .unwrap();
        assert_eq!(
            readback.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &expected
        );

        let edgeless = GraphForge::new(None).unwrap();
        edgeless
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        assert_eq!(
            community_ids(
                &edgeless
                    .cluster("Person", spinglass_options(true, None, None))
                    .unwrap()
            ),
            [0, 1, 2]
        );
        let disconnected = GraphForge::new(None).unwrap();
        disconnected
            .execute(
                "CREATE (a:Person), (b:Person), (c:Person), (d:Person), \
                 (e:Person), (f:Person), (:Person), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), \
                 (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)",
            )
            .unwrap();
        assert_eq!(
            community_ids(
                &disconnected
                    .cluster("Person", spinglass_options(true, None, None))
                    .unwrap()
            ),
            [0, 0, 0, 1, 1, 1, 2]
        );
        assert_eq!(
            GraphForge::new(None)
                .unwrap()
                .cluster("Person", spinglass_options(true, None, None))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn bfs_obeys_uuid_schema_target_via_direction_and_order_contracts() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), (d)-[:OTHER]->(e)",
            )
            .unwrap();

        let source = NodeSelector::Handle(nodes[0].clone());
        let all = graph
            .paths(&source, None, bfs_options(true, Some("KNOWS")))
            .unwrap();
        assert_eq!(
            all.schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["source_uuid", "target_uuid", "cost", "path"]
        );
        assert_eq!(
            all.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            all.schema().field(1).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(all.schema().field(2).data_type(), &DataType::Float64);
        assert!(matches!(
            all.schema().field(3).data_type(),
            DataType::List(field) if field.data_type() == &DataType::FixedSizeBinary(16)
        ));
        assert_eq!(all.schema().metadata()["graphforge.algorithm"], "bfs");
        assert!(all.column_by_name("source_id").is_none());
        let sources = all
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = all
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for row in 0..all.num_rows() {
            assert_eq!(sources.value(row), nodes[0].uuid.as_bytes());
        }
        assert_eq!(
            (0..all.num_rows())
                .map(|row| targets.value(row))
                .collect::<Vec<_>>(),
            nodes[..4]
                .iter()
                .map(|node| node.uuid.as_bytes())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            all.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[0.0, 1.0, 1.0, 2.0]
        );
        assert_eq!(
            uuid_path(&all, 3),
            [nodes[0].uuid, nodes[1].uuid, nodes[3].uuid].map(|uuid| *uuid.as_bytes())
        );
        assert_eq!(
            all,
            graph
                .paths(&source, None, bfs_options(true, Some("KNOWS")))
                .unwrap()
        );

        let dan = NodeSelector::Handle(nodes[3].clone());
        let targeted = graph
            .paths(&source, Some(&dan), bfs_options(true, Some("KNOWS")))
            .unwrap();
        assert_eq!(targeted.num_rows(), 1);
        assert_eq!(uuid_path(&targeted, 0), uuid_path(&all, 3));
        let reverse = graph
            .paths(&dan, Some(&source), bfs_options(false, Some("KNOWS")))
            .unwrap();
        assert_eq!(
            uuid_path(&reverse, 0),
            [nodes[3].uuid, nodes[1].uuid, nodes[0].uuid].map(|uuid| *uuid.as_bytes())
        );
        let eve = NodeSelector::Handle(nodes[4].clone());
        assert_eq!(
            graph
                .paths(&dan, Some(&eve), bfs_options(true, Some("OTHER")))
                .unwrap()
                .num_rows(),
            1
        );
        assert_eq!(
            graph
                .paths(&source, Some(&eve), bfs_options(true, Some("KNOWS")))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn bfs_singleton_and_invalid_inputs_are_structured() {
        let graph = GraphForge::new(None).unwrap();
        let alice = add_person(&graph, "Alice");
        let source = NodeSelector::Handle(alice);
        let singleton = graph.paths(&source, None, bfs_options(true, None)).unwrap();
        assert_eq!(singleton.num_rows(), 1);
        assert_eq!(uuid_path(&singleton, 0).len(), 1);

        let invalid = [
            PathsOptions {
                k: 2,
                ..bfs_options(true, None)
            },
            PathsOptions {
                weight: Some("distance".into()),
                ..bfs_options(true, None)
            },
            PathsOptions {
                via: Some(" ".into()),
                ..bfs_options(true, None)
            },
            PathsOptions {
                by: PathAlgorithm::AStar,
                ..bfs_options(true, None)
            },
        ];
        for options in invalid {
            assert!(matches!(
                graph.paths(&source, None, options),
                Err(GfError::Validation(_))
            ));
        }
        assert!(matches!(
            graph.paths(
                &NodeSelector::Uuid(graphforge_core::uuid::new_v7()),
                None,
                bfs_options(true, None),
            ),
            Err(GfError::Validation(_))
        ));
    }

    #[test]
    fn random_walk_is_seeded_uuid_only_and_resource_bounded_through_public_api() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[0].clone());
        let options = random_walk_options(2, 3, 42);
        let result = graph.paths(&source, None, options.clone()).unwrap();

        assert_eq!(result, graph.paths(&source, None, options).unwrap());
        assert_eq!(result.num_rows(), 2);
        assert_eq!(
            result
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["start_uuid", "walk"]
        );
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert!(matches!(
            result.schema().field(1).data_type(),
            DataType::List(field)
                if field.data_type() == &DataType::FixedSizeBinary(16) && !field.is_nullable()
        ));
        assert_eq!(
            result.schema().metadata()["graphforge.algorithm"],
            "random_walk"
        );
        assert!(result.column_by_name("node_id").is_none());
        let starts = result
            .column_by_name("start_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!((0..2).all(|row| starts.value(row) == nodes[0].uuid.as_bytes()));

        let mut middle = [*nodes[1].uuid.as_bytes(), *nodes[2].uuid.as_bytes()];
        middle.sort_unstable();
        assert_eq!(
            uuid_walk(&result, 0),
            [
                *nodes[0].uuid.as_bytes(),
                middle[1],
                *nodes[3].uuid.as_bytes()
            ]
        );
        assert_eq!(
            uuid_walk(&result, 1),
            [
                *nodes[0].uuid.as_bytes(),
                middle[0],
                *nodes[3].uuid.as_bytes()
            ]
        );

        let zero = graph
            .paths(&source, None, random_walk_options(1, 0, 42))
            .unwrap();
        assert_eq!(uuid_walk(&zero, 0), [*nodes[0].uuid.as_bytes()]);

        assert!(matches!(
            graph.paths(&source, None, random_walk_options(0, 3, 42)),
            Err(GfError::Validation(message)) if message.contains("at least 1")
        ));
        assert!(matches!(
            graph.paths(
                &source,
                Some(&NodeSelector::Handle(nodes[3].clone())),
                random_walk_options(1, 3, 42),
            ),
            Err(GfError::Validation(message)) if message.contains("target selector")
        ));
        assert!(matches!(
            graph.paths(&source, None, random_walk_options(101, 100, 42)),
            Err(GfError::Execution(message)) if message.contains("iteration limit")
        ));
    }

    #[test]
    fn maximum_flow_views_are_consistent_uuid_only_and_deterministic_through_public_api() {
        let graph = GraphForge::new(None).unwrap();
        let nodes =
            ["Source", "A", "B", "Sink", "Unreachable"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), \
                 (b:Person {name:'B'}), (t:Person {name:'Sink'}) \
                 CREATE (s)-[:PIPE {capacity:3.0}]->(a), \
                 (s)-[:PIPE {capacity:2.0}]->(b), \
                 (a)-[:PIPE {capacity:1.0}]->(b), \
                 (a)-[:PIPE {capacity:2.0}]->(t), \
                 (b)-[:PIPE {capacity:3.0}]->(t), \
                 (a)-[:PIPE {capacity:7.0}]->(a), \
                 (b)-[:PIPE {capacity:0.0}]->(a), \
                 (s)-[:OTHER {capacity:100.0}]->(t)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[0].clone());
        let sink = NodeSelector::Handle(nodes[3].clone());
        let scalar_options = max_flow_options(PathAlgorithm::MaxFlow, Some("capacity"));
        let edge_options = max_flow_options(PathAlgorithm::MaxFlowEdges, Some("capacity"));
        let scalar = graph
            .paths(&source, Some(&sink), scalar_options.clone())
            .unwrap();
        let edges = graph
            .paths(&source, Some(&sink), edge_options.clone())
            .unwrap();

        assert_eq!(
            scalar,
            graph.paths(&source, Some(&sink), scalar_options).unwrap()
        );
        assert_eq!(
            edges,
            graph.paths(&source, Some(&sink), edge_options).unwrap()
        );
        assert_eq!(
            scalar
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("sink_uuid", &DataType::FixedSizeBinary(16), false),
                ("flow", &DataType::Float64, false),
            ]
        );
        assert_eq!(
            edges
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", &DataType::FixedSizeBinary(16), false),
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("target_uuid", &DataType::FixedSizeBinary(16), false),
                ("flow", &DataType::Float64, false),
            ]
        );
        assert_eq!(
            scalar.schema().metadata()["graphforge.algorithm"],
            "max_flow"
        );
        assert_eq!(
            edges.schema().metadata()["graphforge.algorithm"],
            "max_flow_edges"
        );
        assert_eq!(
            scalar.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_eq!(
            edges.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_eq!(scalar.schema().metadata()["graphforge.verb"], "paths");
        assert_eq!(edges.schema().metadata()["graphforge.verb"], "paths");

        let scalar_sources = scalar
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let scalar_sinks = scalar
            .column_by_name("sink_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(scalar_sources.value(0), nodes[0].uuid.as_bytes());
        assert_eq!(scalar_sinks.value(0), nodes[3].uuid.as_bytes());
        let scalar_flow = scalar
            .column_by_name("flow")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(scalar_flow, 5.0);
        let edge_uuids = edges
            .column_by_name("edge_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let sources = edges
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = edges
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let flows = edges
            .column_by_name("flow")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(edges.num_rows(), 7);
        assert!((1..edges.num_rows()).all(|row| edge_uuids.value(row - 1) < edge_uuids.value(row)));
        let assignments = (0..edges.num_rows())
            .map(|row| {
                (
                    (
                        sources.value(row).try_into().unwrap(),
                        targets.value(row).try_into().unwrap(),
                    ),
                    flows.value(row),
                )
            })
            .collect::<HashMap<([u8; 16], [u8; 16]), f64>>();
        assert_eq!(
            assignments,
            HashMap::from([
                ((*nodes[0].uuid.as_bytes(), *nodes[1].uuid.as_bytes()), 3.0),
                ((*nodes[0].uuid.as_bytes(), *nodes[2].uuid.as_bytes()), 2.0),
                ((*nodes[1].uuid.as_bytes(), *nodes[2].uuid.as_bytes()), 1.0),
                ((*nodes[1].uuid.as_bytes(), *nodes[3].uuid.as_bytes()), 2.0),
                ((*nodes[2].uuid.as_bytes(), *nodes[3].uuid.as_bytes()), 3.0),
                ((*nodes[1].uuid.as_bytes(), *nodes[1].uuid.as_bytes()), 0.0),
                ((*nodes[2].uuid.as_bytes(), *nodes[1].uuid.as_bytes()), 0.0),
            ])
        );

        let flow_at = |node: &[u8; 16], outgoing: bool| {
            (0..edges.num_rows())
                .filter(|&row| {
                    if outgoing {
                        sources.value(row) == node
                    } else {
                        targets.value(row) == node
                    }
                })
                .map(|row| flows.value(row))
                .sum::<f64>()
        };
        assert_eq!(flow_at(nodes[0].uuid.as_bytes(), true), scalar_flow);
        assert_eq!(flow_at(nodes[3].uuid.as_bytes(), false), scalar_flow);
        for node in [&nodes[1], &nodes[2]] {
            assert_eq!(
                flow_at(node.uuid.as_bytes(), false),
                flow_at(node.uuid.as_bytes(), true)
            );
        }
        assert!((0..edges.num_rows()).all(|row| flows.value(row) >= 0.0));
        assert!((0..edges.num_rows()).any(|row| flows.value(row) == 0.0));

        let unreachable = NodeSelector::Handle(nodes[4].clone());
        let zero = graph
            .paths(
                &source,
                Some(&unreachable),
                max_flow_options(PathAlgorithm::MaxFlow, Some("capacity")),
            )
            .unwrap();
        assert_eq!(
            zero.column_by_name("flow")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            0.0
        );
        assert!(matches!(
            graph.paths(
                &source,
                None,
                max_flow_options(PathAlgorithm::MaxFlow, Some("capacity")),
            ),
            Err(GfError::Validation(message)) if message.contains("target selector")
        ));
        assert!(matches!(
            graph.paths(
                &source,
                Some(&source),
                max_flow_options(PathAlgorithm::MaxFlowEdges, Some("capacity")),
            ),
            Err(GfError::Execution(message)) if message.contains("distinct endpoints")
        ));

        let invalid = GraphForge::new(None).unwrap();
        let invalid_nodes = ["Source", "Sink"].map(|name| add_person(&invalid, name));
        invalid
            .execute(
                "MATCH (s:Person {name:'Source'}), (t:Person {name:'Sink'}) \
                 CREATE (s)-[:PIPE {capacity:-1.0}]->(t)",
            )
            .unwrap();
        assert!(matches!(
            invalid.paths(
                &NodeSelector::Handle(invalid_nodes[0].clone()),
                Some(&NodeSelector::Handle(invalid_nodes[1].clone())),
                max_flow_options(PathAlgorithm::MaxFlow, Some("capacity")),
            ),
            Err(GfError::Execution(message)) if message.contains("nonnegative")
        ));
    }

    #[test]
    fn min_cost_flow_persists_and_shares_scalar_and_edge_solution() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let nodes = ["Source", "A", "Sink"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), \
                 (t:Person {name:'Sink'}) \
                 CREATE (s)-[:PIPE {capacity:2.0, cost:-1.0}]->(a), \
                 (a)-[:PIPE {capacity:2.0, cost:3.0}]->(t), \
                 (s)-[:PIPE {capacity:1.0, cost:5.0}]->(t), \
                 (a)-[:PIPE {capacity:9.0, cost:-8.0}]->(a)",
            )
            .unwrap();
        let source_uuid = nodes[0].uuid;
        let sink_uuid = nodes[2].uuid;
        drop(graph);

        let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let source = NodeSelector::Uuid(source_uuid);
        let sink = NodeSelector::Uuid(sink_uuid);
        let scalar_options = min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true);
        let edge_options = min_cost_flow_options(PathAlgorithm::MinCostMaxFlowEdges, true);
        let scalar = reopened
            .paths(&source, Some(&sink), scalar_options.clone())
            .unwrap();
        let edges = reopened
            .paths(&source, Some(&sink), edge_options.clone())
            .unwrap();
        assert_eq!(
            scalar,
            reopened
                .paths(&source, Some(&sink), scalar_options)
                .unwrap()
        );
        assert_eq!(
            edges,
            reopened.paths(&source, Some(&sink), edge_options).unwrap()
        );
        assert_eq!(
            scalar.schema().metadata()["graphforge.algorithm"],
            "min_cost_max_flow"
        );
        assert_eq!(
            edges.schema().metadata()["graphforge.algorithm"],
            "min_cost_max_flow_edges"
        );
        assert_eq!(
            scalar
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("sink_uuid", &DataType::FixedSizeBinary(16), false),
                ("flow", &DataType::Float64, false),
                ("cost", &DataType::Float64, false),
            ]
        );
        assert_eq!(
            edges
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", &DataType::FixedSizeBinary(16), false),
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("target_uuid", &DataType::FixedSizeBinary(16), false),
                ("flow", &DataType::Float64, false),
                ("unit_cost", &DataType::Float64, false),
                ("flow_cost", &DataType::Float64, false),
            ]
        );
        let flow = scalar
            .column_by_name("flow")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let cost = scalar
            .column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!((flow, cost), (3.0, 9.0));
        for batch in [&scalar, &edges] {
            assert_eq!(batch.schema().metadata()["graphforge.verb"], "paths");
            assert_eq!(
                batch.schema().metadata()["graphforge.algorithm_schema_version"],
                "1"
            );
        }
        let edge_uuids = uuid_column(&edges, "edge_uuid");
        assert!((1..edges.num_rows()).all(|row| edge_uuids.value(row - 1) < edge_uuids.value(row)));
        let flows = edges
            .column_by_name("flow")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sources = uuid_column(&edges, "source_uuid");
        let targets = uuid_column(&edges, "target_uuid");
        let unit_costs = float_column(&edges, "unit_cost");
        let flow_costs = edges
            .column_by_name("flow_cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            (0..edges.num_rows())
                .map(|row| flows.value(row))
                .sum::<f64>(),
            5.0
        );
        assert_eq!(
            (0..edges.num_rows())
                .map(|row| flow_costs.value(row))
                .sum::<f64>(),
            cost
        );
        assert!((0..edges.num_rows()).any(|row| flows.value(row) == 0.0));
        for row in 0..edges.num_rows() {
            assert!(flows.value(row) >= 0.0);
            assert!(
                flows.value(row)
                    <= if unit_costs.value(row) == -8.0 {
                        9.0
                    } else if unit_costs.value(row) == 5.0 {
                        1.0
                    } else {
                        2.0
                    }
            );
            assert_eq!(
                flow_costs.value(row),
                flows.value(row) * unit_costs.value(row)
            );
        }
        let balance = |node: &[u8]| {
            (0..edges.num_rows())
                .map(|row| {
                    (if targets.value(row) == node {
                        flows.value(row)
                    } else {
                        0.0
                    }) - (if sources.value(row) == node {
                        flows.value(row)
                    } else {
                        0.0
                    })
                })
                .sum::<f64>()
        };
        assert_eq!(balance(nodes[1].uuid.as_bytes()), 0.0);

        let mut unit = min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true);
        unit.capacity_property = None;
        assert_eq!(
            reopened
                .paths(&source, Some(&sink), unit)
                .unwrap()
                .column_by_name("flow")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
        let mut missing_cost = min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true);
        missing_cost.cost_property = None;
        assert!(matches!(
            reopened.paths(&source, Some(&sink), missing_cost),
            Err(GfError::Validation(message)) if message.contains("cost_property")
        ));
        assert!(matches!(
            reopened.paths(
                &source,
                Some(&source),
                min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true),
            ),
            Err(GfError::Execution(message)) if message.contains("distinct endpoints")
        ));
    }

    #[test]
    fn min_cost_flow_persisted_undirected_signed_parallel_and_failures() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let nodes = ["Source", "Sink"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (s:Person {name:'Source'}), (t:Person {name:'Sink'}) \
             CREATE (s)-[:PIPE {capacity:1.0, cost:2.0}]->(t), \
             (s)-[:PIPE {capacity:1.0, cost:2.0}]->(t)",
            )
            .unwrap();
        let result = graph
            .paths(
                &NodeSelector::Handle(nodes[1].clone()),
                Some(&NodeSelector::Handle(nodes[0].clone())),
                min_cost_flow_options(PathAlgorithm::MinCostMaxFlowEdges, false),
            )
            .unwrap();
        let flows = float_column(&result, "flow");
        let ids = uuid_column(&result, "edge_uuid");
        assert_eq!(result.num_rows(), 2);
        assert!(ids.value(0) < ids.value(1));
        assert_eq!([flows.value(0), flows.value(1)], [-1.0, -1.0]);

        let invalid = GraphForge::new(None).unwrap();
        let bad = ["Source", "Sink"].map(|name| add_person(&invalid, name));
        invalid
            .execute(
                "MATCH (s:Person {name:'Source'}), (t:Person {name:'Sink'}) \
             CREATE (s)-[:PIPE {capacity:-1.0, cost:1.0}]->(t)",
            )
            .unwrap();
        assert!(matches!(
            invalid.paths(
                &NodeSelector::Handle(bad[0].clone()),
                Some(&NodeSelector::Handle(bad[1].clone())),
                min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true),
            ),
            Err(GfError::Execution(message)) if message.contains("nonnegative")
        ));

        let cycle = GraphForge::new(None).unwrap();
        let cycle_nodes = ["Source", "A", "B", "Sink"].map(|name| add_person(&cycle, name));
        cycle
            .execute(
                "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), \
             (b:Person {name:'B'}), (t:Person {name:'Sink'}) \
             CREATE (s)-[:PIPE {capacity:1.0, cost:0.0}]->(a), \
             (a)-[:PIPE {capacity:1.0, cost:-2.0}]->(b), \
             (b)-[:PIPE {capacity:1.0, cost:1.0}]->(a), \
             (b)-[:PIPE {capacity:1.0, cost:0.0}]->(t)",
            )
            .unwrap();
        assert!(matches!(
            cycle.paths(
                &NodeSelector::Handle(cycle_nodes[0].clone()),
                Some(&NodeSelector::Handle(cycle_nodes[3].clone())),
                min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true),
            ),
            Err(GfError::Execution(message)) if message.contains("negative-cost residual cycle")
        ));
    }

    #[test]
    fn minimum_cut_views_agree_on_exact_uuid_results_through_public_api() {
        let graph = GraphForge::new(None).unwrap();
        let nodes =
            ["Source", "A", "B", "Sink", "Unreachable"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), \
                 (b:Person {name:'B'}), (t:Person {name:'Sink'}) \
                 CREATE (s)-[:PIPE {capacity:3.0}]->(a), \
                 (s)-[:PIPE {capacity:2.0}]->(b), \
                 (a)-[:PIPE {capacity:1.0}]->(b), \
                 (a)-[:PIPE {capacity:2.0}]->(t), \
                 (b)-[:PIPE {capacity:4.0}]->(t), \
                 (a)-[:PIPE {capacity:7.0}]->(a), \
                 (b)-[:PIPE {capacity:0.0}]->(a), \
                 (s)-[:OTHER {capacity:100.0}]->(t)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[0].clone());
        let sink = NodeSelector::Handle(nodes[3].clone());
        let scalar_options = min_cut_options(PathAlgorithm::MinCut, true, Some("capacity"));
        let edge_options = min_cut_options(PathAlgorithm::MinCutEdges, true, Some("capacity"));
        let scalar = graph
            .paths(&source, Some(&sink), scalar_options.clone())
            .unwrap();
        let edges = graph
            .paths(&source, Some(&sink), edge_options.clone())
            .unwrap();

        assert_eq!(
            scalar,
            graph.paths(&source, Some(&sink), scalar_options).unwrap()
        );
        assert_eq!(
            edges,
            graph.paths(&source, Some(&sink), edge_options).unwrap()
        );
        assert_eq!(
            scalar
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("sink_uuid", &DataType::FixedSizeBinary(16), false),
                ("cut_value", &DataType::Float64, false),
            ]
        );
        assert_eq!(
            edges
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", &DataType::FixedSizeBinary(16), false),
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("target_uuid", &DataType::FixedSizeBinary(16), false),
                ("capacity", &DataType::Float64, false),
            ]
        );
        assert_eq!(
            scalar.schema().metadata()["graphforge.algorithm"],
            "min_cut"
        );
        assert_eq!(
            edges.schema().metadata()["graphforge.algorithm"],
            "min_cut_edges"
        );
        for batch in [&scalar, &edges] {
            assert_eq!(batch.schema().metadata()["graphforge.verb"], "paths");
            assert_eq!(
                batch.schema().metadata()["graphforge.algorithm_schema_version"],
                "1"
            );
            assert!(
                batch
                    .columns()
                    .iter()
                    .all(|column| column.null_count() == 0)
            );
            for forbidden in [
                "node_id",
                "edge_id",
                "provenance_id",
                "confidence",
                "assertion_uuid",
                "belief_status",
                "valid_time",
            ] {
                assert!(batch.column_by_name(forbidden).is_none());
            }
        }

        let scalar_sources = uuid_column(&scalar, "source_uuid");
        let scalar_sinks = uuid_column(&scalar, "sink_uuid");
        let cut_value = float_column(&scalar, "cut_value").value(0);
        assert_eq!(scalar_sources.value(0), nodes[0].uuid.as_bytes());
        assert_eq!(scalar_sinks.value(0), nodes[3].uuid.as_bytes());
        assert_eq!(cut_value, 5.0);

        let edge_uuids = uuid_column(&edges, "edge_uuid");
        let sources = uuid_column(&edges, "source_uuid");
        let targets = uuid_column(&edges, "target_uuid");
        let capacities = float_column(&edges, "capacity");
        assert_eq!(edges.num_rows(), 2);
        assert!(edge_uuids.value(0) < edge_uuids.value(1));
        let cut = (0..edges.num_rows())
            .map(|row| {
                (
                    (
                        sources.value(row).try_into().unwrap(),
                        targets.value(row).try_into().unwrap(),
                    ),
                    capacities.value(row),
                )
            })
            .collect::<HashMap<([u8; 16], [u8; 16]), f64>>();
        assert_eq!(
            cut,
            HashMap::from([
                ((*nodes[0].uuid.as_bytes(), *nodes[1].uuid.as_bytes()), 3.0),
                ((*nodes[0].uuid.as_bytes(), *nodes[2].uuid.as_bytes()), 2.0),
            ])
        );
        assert_eq!(capacities.values().iter().copied().sum::<f64>(), cut_value);

        let unit = graph
            .paths(
                &source,
                Some(&sink),
                min_cut_options(PathAlgorithm::MinCut, true, None),
            )
            .unwrap();
        assert_eq!(float_column(&unit, "cut_value").value(0), 2.0);

        let unreachable = NodeSelector::Handle(nodes[4].clone());
        let zero = graph
            .paths(
                &source,
                Some(&unreachable),
                min_cut_options(PathAlgorithm::MinCut, true, Some("capacity")),
            )
            .unwrap();
        let no_edges = graph
            .paths(
                &source,
                Some(&unreachable),
                min_cut_options(PathAlgorithm::MinCutEdges, true, Some("capacity")),
            )
            .unwrap();
        assert_eq!(float_column(&zero, "cut_value").value(0), 0.0);
        assert_eq!(no_edges.num_rows(), 0);
        assert_eq!(no_edges.schema(), edges.schema());
    }

    #[test]
    fn minimum_cut_public_facade_preserves_undirected_orientation_and_structured_errors() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Left", "Middle", "Right"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (l:Person {name:'Left'}), (m:Person {name:'Middle'}), \
                 (r:Person {name:'Right'}) \
                 CREATE (l)-[:PIPE {capacity:2.0}]->(m), \
                 (m)-[:PIPE {capacity:2.0}]->(r)",
            )
            .unwrap();
        let left = NodeSelector::Handle(nodes[0].clone());
        let right = NodeSelector::Handle(nodes[2].clone());
        let edges = graph
            .paths(
                &right,
                Some(&left),
                min_cut_options(PathAlgorithm::MinCutEdges, false, Some("capacity")),
            )
            .unwrap();
        assert_eq!(edges.num_rows(), 1);
        let sources = uuid_column(&edges, "source_uuid");
        let targets = uuid_column(&edges, "target_uuid");
        let capacities = float_column(&edges, "capacity");
        assert_eq!(sources.value(0), nodes[0].uuid.as_bytes());
        assert_eq!(targets.value(0), nodes[1].uuid.as_bytes());
        assert_eq!(capacities.value(0), 2.0);

        assert!(matches!(
            graph.paths(
                &left,
                None,
                min_cut_options(PathAlgorithm::MinCut, true, Some("capacity")),
            ),
            Err(GfError::Validation(message)) if message.contains("target selector")
        ));
        assert!(matches!(
            graph.paths(
                &left,
                Some(&left),
                min_cut_options(PathAlgorithm::MinCutEdges, true, Some("capacity")),
            ),
            Err(GfError::Execution(message)) if message.contains("distinct endpoints")
        ));

        let invalid = GraphForge::new(None).unwrap();
        let invalid_nodes = ["Source", "Sink"].map(|name| add_person(&invalid, name));
        invalid
            .execute(
                "MATCH (s:Person {name:'Source'}), (t:Person {name:'Sink'}) \
                 CREATE (s)-[:PIPE {capacity:-1.0}]->(t)",
            )
            .unwrap();
        assert!(matches!(
            invalid.paths(
                &NodeSelector::Handle(invalid_nodes[0].clone()),
                Some(&NodeSelector::Handle(invalid_nodes[1].clone())),
                min_cut_options(PathAlgorithm::MinCut, true, Some("capacity")),
            ),
            Err(GfError::Execution(message)) if message.contains("nonnegative")
        ));
    }

    #[test]
    fn dfs_is_uuid_only_deterministic_and_obeys_direction_and_relation_filters() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), (a)-[:OTHER]->(e)",
            )
            .unwrap();

        let source = NodeSelector::Handle(nodes[0].clone());
        let traversal = graph
            .paths(&source, None, dfs_options(true, Some("KNOWS")))
            .unwrap();
        assert_eq!(
            traversal
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.data_type()))
                .collect::<Vec<_>>(),
            [
                ("node_uuid", &DataType::FixedSizeBinary(16)),
                ("depth", &DataType::UInt64),
                ("order", &DataType::UInt64),
            ]
        );
        assert_eq!(traversal.schema().metadata()["graphforge.algorithm"], "dfs");
        assert!(traversal.column_by_name("node_id").is_none());
        let uuids = traversal
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(
            (0..traversal.num_rows())
                .map(|row| uuids.value(row))
                .collect::<Vec<_>>(),
            [0, 1, 3, 2]
                .map(|index| nodes[index].uuid.as_bytes())
                .to_vec()
        );
        assert_eq!(
            traversal
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[0, 1, 2, 1]
        );
        assert_eq!(
            traversal
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[0, 1, 2, 3]
        );
        assert_eq!(
            traversal,
            graph
                .paths(&source, None, dfs_options(true, Some("KNOWS")))
                .unwrap()
        );

        let dan = NodeSelector::Handle(nodes[3].clone());
        assert_eq!(
            graph
                .paths(&dan, None, dfs_options(true, Some("KNOWS")))
                .unwrap()
                .num_rows(),
            1
        );
        assert_eq!(
            graph
                .paths(&dan, None, dfs_options(false, Some("KNOWS")))
                .unwrap()
                .num_rows(),
            4
        );
        let other = graph
            .paths(&source, None, dfs_options(true, Some("OTHER")))
            .unwrap();
        assert_eq!(other.num_rows(), 2);
        assert_eq!(
            other
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(1),
            nodes[4].uuid.as_bytes()
        );
    }

    #[test]
    fn dfs_rejects_target_weight_k_and_malformed_relation_options() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob"].map(|name| add_person(&graph, name));
        let source = NodeSelector::Handle(nodes[0].clone());
        let target = NodeSelector::Handle(nodes[1].clone());
        assert!(matches!(
            graph.paths(&source, Some(&target), dfs_options(true, None),),
            Err(GfError::Validation(_))
        ));
        for options in [
            PathsOptions {
                k: 2,
                ..dfs_options(true, None)
            },
            PathsOptions {
                weight: Some("distance".into()),
                ..dfs_options(true, None)
            },
            PathsOptions {
                via: Some(" ".into()),
                ..dfs_options(true, None)
            },
        ] {
            assert!(matches!(
                graph.paths(&source, None, options),
                Err(GfError::Validation(_))
            ));
        }
    }

    #[test]
    fn dijkstra_is_uuid_only_weighted_deterministic_and_knowledge_independent() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(c), \
                 (a)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:2.0}]->(d), \
                 (c)-[:ROAD {cost:2.0}]->(d), (a)-[:ROAD {cost:9.0}]->(d), \
                 (d)-[:OTHER {cost:0.5}]->(e)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[0].clone());
        let options = dijkstra_options(true, Some("ROAD"), Some("cost"));
        let all = graph.paths(&source, None, options.clone()).unwrap();

        assert_eq!(all.schema().metadata()["graphforge.algorithm"], "dijkstra");
        assert_eq!(
            all.schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", false),
                ("target_uuid", false),
                ("cost", false),
                ("path", false),
            ]
        );
        assert!(all.column_by_name("source_id").is_none());
        assert_eq!(
            all.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[0.0, 1.0, 1.0, 3.0]
        );
        assert_eq!(
            uuid_path(&all, 3),
            [nodes[0].uuid, nodes[1].uuid, nodes[3].uuid].map(|uuid| *uuid.as_bytes())
        );
        assert_eq!(all, graph.paths(&source, None, options.clone()).unwrap());

        let dan = NodeSelector::Handle(nodes[3].clone());
        let target = graph.paths(&source, Some(&dan), options).unwrap();
        assert_eq!(target.num_rows(), 1);
        assert_eq!(uuid_path(&target, 0), uuid_path(&all, 3));
        assert_eq!(
            graph
                .paths(
                    &dan,
                    Some(&source),
                    dijkstra_options(true, Some("ROAD"), Some("cost")),
                )
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            graph
                .paths(
                    &dan,
                    Some(&source),
                    dijkstra_options(false, Some("ROAD"), Some("cost")),
                )
                .unwrap()
                .num_rows(),
            1
        );
    }

    #[test]
    fn dijkstra_all_pairs_is_uuid_only_ordered_and_source_validated_only() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}),                  (c:Person {name:'Carol'}), (d:Person {name:'Dan'}),                  (e:Person {name:'Eve'})                  CREATE (a)-[:ROAD {cost:1.0}]->(c),                  (a)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:2.0}]->(d),                  (c)-[:ROAD {cost:2.0}]->(d), (a)-[:ROAD {cost:9.0}]->(d),                  (d)-[:ROAD {cost:0.5}]->(e)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[4].clone());
        let options = dijkstra_all_pairs_options(true, Some("ROAD"), Some("cost"));
        let batch = graph.paths(&source, None, options.clone()).unwrap();

        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "dijkstra_all_pairs"
        );
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", false),
                ("target_uuid", false),
                ("cost", false),
                ("path", false),
            ]
        );
        assert!(batch.column_by_name("source_id").is_none());
        assert_eq!(batch.num_rows(), 9);
        let expected = [
            (0, 1, 1.0, vec![0, 1]),
            (0, 2, 1.0, vec![0, 2]),
            (0, 3, 3.0, vec![0, 1, 3]),
            (0, 4, 3.5, vec![0, 1, 3, 4]),
            (1, 3, 2.0, vec![1, 3]),
            (1, 4, 2.5, vec![1, 3, 4]),
            (2, 3, 2.0, vec![2, 3]),
            (2, 4, 2.5, vec![2, 3, 4]),
            (3, 4, 0.5, vec![3, 4]),
        ];
        let sources = batch
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = batch
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let costs = batch
            .column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for (row, (source, target, cost, path)) in expected.iter().enumerate() {
            assert_eq!(sources.value(row), nodes[*source].uuid.as_bytes());
            assert_eq!(targets.value(row), nodes[*target].uuid.as_bytes());
            assert_eq!(costs.value(row), *cost);
            assert_eq!(
                uuid_path(&batch, row),
                path.iter()
                    .map(|index| *nodes[*index].uuid.as_bytes())
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(batch, graph.paths(&source, None, options).unwrap());

        let target = NodeSelector::Handle(nodes[0].clone());
        assert!(matches!(
            graph.paths(&source, Some(&target), dijkstra_all_pairs_options(true, Some("ROAD"), Some("cost"))),
            Err(GfError::Validation(message)) if message.contains("target")
        ));
        assert!(matches!(
            graph.paths(
                &source,
                None,
                PathsOptions { k: 2, ..dijkstra_all_pairs_options(true, None, None) }
            ),
            Err(GfError::Validation(message)) if message.contains("k must be 1")
        ));
    }

    #[test]
    fn dijkstra_all_pairs_public_fingerprint_matches_thread_configs() {
        const NODE_COUNT: usize = 48;
        const OFFSETS: [usize; 4] = [1, 5, 17, 31];

        fn policy(workers: usize) -> ExecutionResourcePolicy {
            ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                tokio_worker_threads: Some(workers),
                target_partitions: Some(workers),
                batch_size: Some(8_192),
                memory_budget_bytes: Some(512 * 1024 * 1024),
                spill: SpillPolicy::default(),
                io_concurrency: Some(workers),
                max_concurrent_heavy_queries: Some(1),
                compute_threads: Some(workers),
            }
        }

        fn automatic_policy() -> ExecutionResourcePolicy {
            ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Automatic,
                tokio_worker_threads: None,
                target_partitions: None,
                batch_size: None,
                memory_budget_bytes: None,
                spill: SpillPolicy::default(),
                io_concurrency: None,
                max_concurrent_heavy_queries: None,
                compute_threads: None,
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let source_uuid = {
            let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
            let nodes = (0..NODE_COUNT)
                .map(|index| add_person(&graph, &format!("n{index}")))
                .collect::<Vec<_>>();
            let match_clause = (0..NODE_COUNT)
                .map(|index| format!("(n{index}:Person {{name:'n{index}'}})"))
                .collect::<Vec<_>>()
                .join(", ");
            let create_clause = (0..NODE_COUNT)
                .flat_map(|source| {
                    OFFSETS.into_iter().map(move |offset| {
                        let target = (source + offset) % NODE_COUNT;
                        let cost = 1.0 + ((source + target) % 7) as f64 / 10.0;
                        format!("(n{source})-[:ROAD {{cost:{cost:.1}}}]->(n{target})")
                    })
                })
                .collect::<Vec<_>>()
                .join(", ");
            graph
                .execute(&format!("MATCH {match_clause} CREATE {create_clause}"))
                .unwrap();
            nodes[0].uuid
        };

        let configs = [
            ("threads-1", policy(1)),
            ("threads-2", policy(2)),
            ("threads-4", policy(4)),
            ("threads-8", policy(8)),
            ("threads-automatic", automatic_policy()),
        ];
        let mut baseline: Option<(Vec<(String, String, bool)>, usize, String)> = None;
        let mut executed = 0_usize;
        for (id, resource) in configs {
            let graph = match GraphForge::new_with_options(
                Some(dir.path().to_str().unwrap()),
                GraphForgeOptions {
                    resource,
                    ..GraphForgeOptions::default()
                },
            ) {
                Ok(graph) => graph,
                Err(error) => {
                    eprintln!("{id}: unavailable resource policy: {error}");
                    continue;
                }
            };
            let batch = graph
                .paths(
                    &NodeSelector::Uuid(source_uuid),
                    None,
                    dijkstra_all_pairs_options(true, Some("ROAD"), Some("cost")),
                )
                .unwrap_or_else(|error| panic!("{id}: {error}"));
            let schema = batch
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.name().clone(),
                        format!("{:?}", field.data_type()),
                        field.is_nullable(),
                    )
                })
                .collect::<Vec<_>>();
            let fingerprint = arrow_batch_fingerprint(&batch);
            eprintln!("{id}: rows={} fingerprint={fingerprint}", batch.num_rows());
            let observed = (schema, batch.num_rows(), fingerprint);
            if let Some(expected) = &baseline {
                assert_eq!(&observed, expected, "{id}: dijkstra_all_pairs parity");
            } else {
                assert_eq!(batch.num_rows(), NODE_COUNT * (NODE_COUNT - 1));
                baseline = Some(observed);
            }
            executed += 1;
        }
        assert!(
            executed >= 2,
            "expected at least threads-1 and one multi-thread/automatic dijkstra_all_pairs cell"
        );
    }

    #[test]
    fn dijkstra_all_pairs_covers_empty_disconnected_undirected_and_weight_errors() {
        let empty = GraphForge::new(None).unwrap();
        let missing = NodeSelector::Uuid(graphforge_core::uuid::new_v7());
        assert!(matches!(
            empty.paths(&missing, None, dijkstra_all_pairs_options(true, None, None)),
            Err(GfError::Validation(_))
        ));

        let graph = GraphForge::new(None).unwrap();
        let alice = add_person(&graph, "Alice");
        let bob = add_person(&graph, "Bob");
        let carol = add_person(&graph, "Carol");
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'})                  CREATE (a)-[:ROAD {cost:1.0, bad:-1.0}]->(b)",
            )
            .unwrap();
        let source = NodeSelector::Handle(carol);
        assert_eq!(
            graph
                .paths(
                    &source,
                    None,
                    dijkstra_all_pairs_options(true, Some("ROAD"), Some("cost"))
                )
                .unwrap()
                .num_rows(),
            1
        );
        assert_eq!(
            graph
                .paths(
                    &source,
                    None,
                    dijkstra_all_pairs_options(false, Some("ROAD"), Some("cost"))
                )
                .unwrap()
                .num_rows(),
            2
        );
        assert!(matches!(
            graph.paths(
                &NodeSelector::Handle(alice),
                None,
                dijkstra_all_pairs_options(true, Some("ROAD"), Some("bad"))
            ),
            Err(GfError::Validation(_)) | Err(GfError::Execution(_))
        ));
        assert!(matches!(
            graph.paths(
                &NodeSelector::Handle(bob),
                None,
                dijkstra_all_pairs_options(true, Some("ROAD"), Some("missing"))
            ),
            Err(GfError::Validation(_)) | Err(GfError::Execution(_))
        ));
    }
    #[test]
    fn dijkstra_defaults_to_unit_cost_and_rejects_invalid_weight_contracts() {
        let graph = GraphForge::new(None).unwrap();
        let alice = add_person(&graph, "Alice");
        let bob = add_person(&graph, "Bob");
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {negative:-1.0}]->(b)",
            )
            .unwrap();
        let source = NodeSelector::Handle(alice);
        let target = NodeSelector::Handle(bob);
        let unit = graph
            .paths(
                &source,
                Some(&target),
                dijkstra_options(true, Some("ROAD"), None),
            )
            .unwrap();
        assert_eq!(
            unit.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.0
        );
        for options in [
            PathsOptions {
                k: 2,
                ..dijkstra_options(true, None, None)
            },
            dijkstra_options(true, Some("ROAD"), Some(" ")),
            dijkstra_options(true, Some("ROAD"), Some("missing")),
            dijkstra_options(true, Some("ROAD"), Some("negative")),
        ] {
            assert!(matches!(
                graph.paths(&source, Some(&target), options),
                Err(GfError::Validation(_)) | Err(GfError::Execution(_))
            ));
        }
    }

    #[test]
    fn bellman_ford_is_exact_uuid_only_deterministic_and_knowledge_independent() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:ROAD {cost:5.0}]->(c), \
                 (a)-[:ROAD {cost:4.0}]->(b), (b)-[:ROAD {cost:-2.0}]->(c), \
                 (b)-[:ROAD {cost:6.0}]->(d), (c)-[:ROAD {cost:3.0}]->(d), \
                 (d)-[:ROAD {cost:-1.0}]->(e), \
                 (a)-[:UNIT]->(b), (b)-[:UNIT]->(e), (d)-[:BACK]->(a)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[0].clone());
        let options = bellman_ford_options(true, Some("ROAD"), Some("cost"));
        let all = graph.paths(&source, None, options.clone()).unwrap();

        assert_eq!(
            all.schema().metadata()["graphforge.algorithm"],
            "bellman_ford"
        );
        assert_eq!(
            all.schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", false),
                ("target_uuid", false),
                ("cost", false),
                ("path", false),
            ]
        );
        assert!(all.column_by_name("source_id").is_none());
        let sources = all
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = all
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!((0..all.num_rows()).all(|row| sources.value(row) == nodes[0].uuid.as_bytes()));
        assert_eq!(
            (0..all.num_rows())
                .map(|row| targets.value(row))
                .collect::<Vec<_>>(),
            nodes
                .iter()
                .map(|node| node.uuid.as_bytes())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            all.column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[0.0, 4.0, 2.0, 5.0, 4.0]
        );
        assert_eq!(
            uuid_path(&all, 4),
            [
                nodes[0].uuid,
                nodes[1].uuid,
                nodes[2].uuid,
                nodes[3].uuid,
                nodes[4].uuid,
            ]
            .map(|uuid| *uuid.as_bytes())
        );
        assert_eq!(all, graph.paths(&source, None, options.clone()).unwrap());

        let eve = NodeSelector::Handle(nodes[4].clone());
        let target = graph.paths(&source, Some(&eve), options).unwrap();
        assert_eq!(target.num_rows(), 1);
        assert_eq!(uuid_path(&target, 0), uuid_path(&all, 4));

        let unit = graph
            .paths(
                &source,
                Some(&eve),
                bellman_ford_options(true, Some("UNIT"), None),
            )
            .unwrap();
        assert_eq!(
            unit.column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
        let dan = NodeSelector::Handle(nodes[3].clone());
        assert_eq!(
            graph
                .paths(
                    &source,
                    Some(&dan),
                    bellman_ford_options(true, Some("BACK"), None),
                )
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            graph
                .paths(
                    &source,
                    Some(&dan),
                    bellman_ford_options(false, Some("BACK"), None),
                )
                .unwrap()
                .num_rows(),
            1
        );
        let singleton = graph
            .paths(
                &source,
                Some(&source),
                bellman_ford_options(true, Some("ROAD"), Some("cost")),
            )
            .unwrap();
        assert_eq!(uuid_path(&singleton, 0), [*nodes[0].uuid.as_bytes()]);
    }

    #[test]
    fn bellman_ford_negative_cycle_scope_is_structured() {
        let reachable = GraphForge::new(None).unwrap();
        let source = add_person(&reachable, "Alice");
        let target = add_person(&reachable, "Dan");
        add_person(&reachable, "Bob");
        reachable
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(b), \
                 (b)-[:ROAD {cost:-2.0}]->(a), (a)-[:ROAD {cost:5.0}]->(d)",
            )
            .unwrap();
        assert!(matches!(
            reachable.paths(
                &NodeSelector::Handle(source),
                Some(&NodeSelector::Handle(target)),
                bellman_ford_options(true, Some("ROAD"), Some("cost")),
            ),
            Err(GfError::Execution(message)) if message.contains("negative cycle")
        ));

        let unreachable = GraphForge::new(None).unwrap();
        let source = add_person(&unreachable, "Alice");
        add_person(&unreachable, "Bob");
        add_person(&unreachable, "Carol");
        add_person(&unreachable, "Dan");
        unreachable
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:2.0}]->(b), \
                 (c)-[:ROAD {cost:-2.0}]->(d), (d)-[:ROAD {cost:1.0}]->(c)",
            )
            .unwrap();
        assert_eq!(
            unreachable
                .paths(
                    &NodeSelector::Handle(source),
                    None,
                    bellman_ford_options(true, Some("ROAD"), Some("cost")),
                )
                .unwrap()
                .num_rows(),
            2
        );
    }

    #[test]
    fn bellman_ford_rejects_invalid_options_and_strict_weight_values() {
        let graph = GraphForge::new(None).unwrap();
        let source = add_person(&graph, "Alice");
        let target = add_person(&graph, "Bob");
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {null_cost:null, text_cost:'heavy', \
                 infinite_cost:1e308 * 2.0}]->(b)",
            )
            .unwrap();
        let source = NodeSelector::Handle(source);
        let target = NodeSelector::Handle(target);
        for options in [
            PathsOptions {
                k: 2,
                ..bellman_ford_options(true, Some("ROAD"), None)
            },
            PathsOptions {
                heuristic: Some("estimate".into()),
                ..bellman_ford_options(true, Some("ROAD"), None)
            },
            bellman_ford_options(true, Some(" "), None),
            bellman_ford_options(true, Some("ROAD"), Some(" ")),
            bellman_ford_options(true, Some("ROAD"), Some("missing")),
            bellman_ford_options(true, Some("ROAD"), Some("null_cost")),
            bellman_ford_options(true, Some("ROAD"), Some("text_cost")),
            bellman_ford_options(true, Some("ROAD"), Some("infinite_cost")),
        ] {
            assert!(matches!(
                graph.paths(&source, Some(&target), options),
                Err(GfError::Validation(_))
            ));
        }
        assert!(matches!(
            graph.paths(
                &NodeSelector::Uuid(graphforge_core::uuid::new_v7()),
                Some(&target),
                bellman_ford_options(true, None, None),
            ),
            Err(GfError::Validation(_))
        ));
    }

    #[test]
    fn floyd_warshall_is_exact_uuid_only_deterministic_and_knowledge_independent() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:ROAD {cost:5.0}]->(c), \
                 (a)-[:ROAD {cost:4.0}]->(b), (b)-[:ROAD {cost:-2.0}]->(c), \
                 (b)-[:ROAD {cost:6.0}]->(d), (c)-[:ROAD {cost:3.0}]->(d), \
                 (d)-[:ROAD {cost:-1.0}]->(e), \
                 (a)-[:UNIT]->(b), (b)-[:UNIT]->(e), (d)-[:BACK]->(a)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[4].clone());
        let options = floyd_warshall_options(true, Some("ROAD"), Some("cost"));
        let batch = graph.paths(&source, None, options.clone()).unwrap();

        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "floyd_warshall"
        );
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", false),
                ("target_uuid", false),
                ("cost", false),
                ("path", false),
            ]
        );
        assert!(batch.column_by_name("source_id").is_none());
        assert_eq!(
            batch
                .column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[4.0, 2.0, 5.0, 4.0, -2.0, 1.0, 0.0, 3.0, 2.0, -1.0]
        );
        assert_eq!(
            uuid_path(&batch, 3),
            nodes
                .iter()
                .map(|node| *node.uuid.as_bytes())
                .collect::<Vec<_>>()
        );
        let sources = batch
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = batch
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let pairs = (0..batch.num_rows())
            .map(|row| (sources.value(row), targets.value(row)))
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            vec![
                (
                    nodes[0].uuid.as_bytes().as_slice(),
                    nodes[1].uuid.as_bytes().as_slice()
                ),
                (
                    nodes[0].uuid.as_bytes().as_slice(),
                    nodes[2].uuid.as_bytes().as_slice()
                ),
                (
                    nodes[0].uuid.as_bytes().as_slice(),
                    nodes[3].uuid.as_bytes().as_slice()
                ),
                (
                    nodes[0].uuid.as_bytes().as_slice(),
                    nodes[4].uuid.as_bytes().as_slice()
                ),
                (
                    nodes[1].uuid.as_bytes().as_slice(),
                    nodes[2].uuid.as_bytes().as_slice()
                ),
                (
                    nodes[1].uuid.as_bytes().as_slice(),
                    nodes[3].uuid.as_bytes().as_slice()
                ),
                (
                    nodes[1].uuid.as_bytes().as_slice(),
                    nodes[4].uuid.as_bytes().as_slice()
                ),
                (
                    nodes[2].uuid.as_bytes().as_slice(),
                    nodes[3].uuid.as_bytes().as_slice()
                ),
                (
                    nodes[2].uuid.as_bytes().as_slice(),
                    nodes[4].uuid.as_bytes().as_slice()
                ),
                (
                    nodes[3].uuid.as_bytes().as_slice(),
                    nodes[4].uuid.as_bytes().as_slice()
                ),
            ]
        );
        assert_eq!(batch, graph.paths(&source, None, options.clone()).unwrap());
        assert!(matches!(
            graph.paths(
                &source,
                Some(&NodeSelector::Handle(nodes[0].clone())),
                options
            ),
            Err(GfError::Validation(message))
                if message == "floyd_warshall does not accept a target selector"
        ));
    }

    #[test]
    fn floyd_warshall_covers_projection_cycles_and_strict_errors() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:2.0, null_cost:null, text_cost:'heavy', \
                 infinite_cost:1e308 * 2.0}]->(b), \
                 (c)-[:CYCLE {cost:-2.0}]->(d), (d)-[:CYCLE {cost:1.0}]->(c), \
                 (a)-[:UNIT]->(b), (d)-[:BACK]->(a)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[0].clone());
        assert_eq!(
            graph
                .paths(
                    &source,
                    None,
                    floyd_warshall_options(true, Some("UNIT"), None)
                )
                .unwrap()
                .num_rows(),
            1
        );
        assert_eq!(
            graph
                .paths(
                    &source,
                    None,
                    floyd_warshall_options(false, Some("BACK"), None)
                )
                .unwrap()
                .num_rows(),
            2
        );
        assert!(matches!(
            graph.paths(
                &source,
                None,
                floyd_warshall_options(true, Some("CYCLE"), Some("cost"))
            ),
            Err(GfError::Execution(message)) if message.contains("negative cycle")
        ));
        for options in [
            PathsOptions {
                k: 2,
                ..floyd_warshall_options(true, Some("ROAD"), None)
            },
            PathsOptions {
                heuristic: Some("estimate".into()),
                ..floyd_warshall_options(true, Some("ROAD"), None)
            },
            floyd_warshall_options(true, Some(" "), None),
            floyd_warshall_options(true, Some("ROAD"), Some(" ")),
            floyd_warshall_options(true, Some("ROAD"), Some("missing")),
            floyd_warshall_options(true, Some("ROAD"), Some("null_cost")),
            floyd_warshall_options(true, Some("ROAD"), Some("text_cost")),
            floyd_warshall_options(true, Some("ROAD"), Some("infinite_cost")),
        ] {
            assert!(matches!(
                graph.paths(&source, None, options),
                Err(GfError::Validation(_))
            ));
        }
        assert!(matches!(
            graph.paths(
                &NodeSelector::Uuid(graphforge_core::uuid::new_v7()),
                None,
                floyd_warshall_options(true, None, None),
            ),
            Err(GfError::Validation(_))
        ));
    }

    #[test]
    fn delta_stepping_is_exact_uuid_only_deterministic_and_knowledge_independent() {
        // Exploratory mode proves graph algorithms do not require a knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(c), \
                 (a)-[:ROAD {cost:0.5}]->(b), (b)-[:ROAD {cost:0.5}]->(c), \
                 (a)-[:ROAD {cost:5.0}]->(d), (c)-[:ROAD {cost:2.0}]->(d), \
                 (a)-[:UNIT]->(b), (b)-[:UNIT]->(d), (d)-[:BACK]->(a)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[0].clone());
        let options = delta_stepping_options(true, Some("ROAD"), Some("cost"));
        let all = graph.paths(&source, None, options.clone()).unwrap();

        assert_eq!(
            all.schema().metadata()["graphforge.algorithm"],
            "delta_stepping"
        );
        assert_eq!(
            all.schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", false),
                ("target_uuid", false),
                ("cost", false),
                ("path", false),
            ]
        );
        assert!(all.column_by_name("source_id").is_none());
        let sources = all
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = all
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!((0..all.num_rows()).all(|row| sources.value(row) == nodes[0].uuid.as_bytes()));
        assert_eq!(
            (0..all.num_rows())
                .map(|row| targets.value(row))
                .collect::<Vec<_>>(),
            nodes[..4]
                .iter()
                .map(|node| node.uuid.as_bytes())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            all.column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[0.0, 0.5, 1.0, 3.0]
        );
        assert_eq!(
            uuid_path(&all, 2),
            [nodes[0].uuid, nodes[1].uuid, nodes[2].uuid].map(|uuid| *uuid.as_bytes())
        );
        assert_eq!(all, graph.paths(&source, None, options.clone()).unwrap());

        let dan = NodeSelector::Handle(nodes[3].clone());
        let target = graph.paths(&source, Some(&dan), options).unwrap();
        assert_eq!(target.num_rows(), 1);
        assert_eq!(uuid_path(&target, 0), uuid_path(&all, 3));
        let eve = NodeSelector::Handle(nodes[4].clone());
        assert_eq!(
            graph
                .paths(
                    &source,
                    Some(&eve),
                    delta_stepping_options(true, Some("ROAD"), Some("cost")),
                )
                .unwrap()
                .num_rows(),
            0
        );
        let unit = graph
            .paths(
                &source,
                Some(&dan),
                delta_stepping_options(true, Some("UNIT"), None),
            )
            .unwrap();
        assert_eq!(
            unit.column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
        assert_eq!(
            graph
                .paths(
                    &source,
                    Some(&dan),
                    delta_stepping_options(true, Some("BACK"), None),
                )
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            graph
                .paths(
                    &source,
                    Some(&dan),
                    delta_stepping_options(false, Some("BACK"), None),
                )
                .unwrap()
                .num_rows(),
            1
        );
        let singleton = graph
            .paths(
                &source,
                Some(&source),
                delta_stepping_options(true, Some("ROAD"), Some("cost")),
            )
            .unwrap();
        assert_eq!(uuid_path(&singleton, 0), [*nodes[0].uuid.as_bytes()]);
    }

    #[test]
    fn delta_stepping_rejects_invalid_options_and_strict_weight_values() {
        let graph = GraphForge::new(None).unwrap();
        let source = add_person(&graph, "Alice");
        let target = add_person(&graph, "Bob");
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {negative:-1.0, null_cost:null, \
                 text_cost:'heavy', infinite_cost:1e308 * 2.0}]->(b)",
            )
            .unwrap();
        let source = NodeSelector::Handle(source);
        let target = NodeSelector::Handle(target);
        for options in [
            PathsOptions {
                k: 2,
                ..delta_stepping_options(true, Some("ROAD"), None)
            },
            PathsOptions {
                heuristic: Some("estimate".into()),
                ..delta_stepping_options(true, Some("ROAD"), None)
            },
            delta_stepping_options(true, Some(" "), None),
            delta_stepping_options(true, Some("ROAD"), Some(" ")),
            delta_stepping_options(true, Some("ROAD"), Some("missing")),
            delta_stepping_options(true, Some("ROAD"), Some("null_cost")),
            delta_stepping_options(true, Some("ROAD"), Some("text_cost")),
            delta_stepping_options(true, Some("ROAD"), Some("infinite_cost")),
        ] {
            assert!(matches!(
                graph.paths(&source, Some(&target), options),
                Err(GfError::Validation(_))
            ));
        }
        assert!(matches!(
            graph.paths(
                &source,
                Some(&target),
                delta_stepping_options(true, Some("ROAD"), Some("negative")),
            ),
            Err(GfError::Execution(message))
                if message.contains("requires finite non-negative edge weights")
        ));
        assert!(matches!(
            graph.paths(
                &NodeSelector::Uuid(graphforge_core::uuid::new_v7()),
                Some(&target),
                delta_stepping_options(true, None, None),
            ),
            Err(GfError::Validation(_))
        ));
    }

    #[test]
    fn yens_is_ranked_uuid_only_deterministic_and_knowledge_independent() {
        // Exploratory mode proves graph algorithms do not require a knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:4.0}]->(b), \
                 (a)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:2.0}]->(d), \
                 (a)-[:ROAD {cost:1.0}]->(c), (c)-[:ROAD {cost:2.0}]->(d), \
                 (b)-[:ROAD {cost:0.5}]->(c), (a)-[:ROAD {cost:4.0}]->(d), \
                 (a)-[:ROAD {cost:0.0}]->(a), (c)-[:ROAD {cost:0.0}]->(a), \
                 (a)-[:UNIT]->(d), (a)-[:UNIT]->(b), (b)-[:UNIT]->(d)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[0].clone());
        let target = NodeSelector::Handle(nodes[3].clone());
        let options = yens_options(true, 10, Some("ROAD"), Some("cost"));
        let batch = graph
            .paths(&source, Some(&target), options.clone())
            .unwrap();

        assert_eq!(batch.schema().metadata()["graphforge.algorithm"], "yens");
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "paths");
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", false),
                ("target_uuid", false),
                ("rank", false),
                ("cost", false),
                ("path", false),
            ]
        );
        assert_eq!(batch.schema().field(2).data_type(), &DataType::UInt64);
        assert!(matches!(
            batch.schema().field(4).data_type(),
            DataType::List(field)
                if field.data_type() == &DataType::FixedSizeBinary(16) && !field.is_nullable()
        ));
        assert!(batch.column_by_name("source_id").is_none());
        assert_eq!(
            batch
                .column_by_name("rank")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[1, 2, 3, 4]
        );
        assert_eq!(
            batch
                .column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[3.0, 3.0, 3.5, 4.0]
        );
        let expected = [vec![0, 1, 3], vec![0, 2, 3], vec![0, 1, 2, 3], vec![0, 3]];
        for (row, path) in expected.iter().enumerate() {
            assert_eq!(
                uuid_path(&batch, row),
                path.iter()
                    .map(|index| *nodes[*index].uuid.as_bytes())
                    .collect::<Vec<_>>()
            );
        }
        let sources = batch
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = batch
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!((0..4).all(|row| sources.value(row) == nodes[0].uuid.as_bytes()));
        assert!((0..4).all(|row| targets.value(row) == nodes[3].uuid.as_bytes()));
        assert_eq!(batch, graph.paths(&source, Some(&target), options).unwrap());

        let unit = graph
            .paths(
                &source,
                Some(&target),
                yens_options(true, 2, Some("UNIT"), None),
            )
            .unwrap();
        assert_eq!(
            unit.column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[1.0, 2.0]
        );
        assert_eq!(
            graph
                .paths(
                    &target,
                    Some(&source),
                    yens_options(true, 2, Some("ROAD"), Some("cost")),
                )
                .unwrap()
                .num_rows(),
            0
        );
        assert!(
            graph
                .paths(
                    &target,
                    Some(&source),
                    yens_options(false, 2, Some("ROAD"), Some("cost")),
                )
                .unwrap()
                .num_rows()
                > 0
        );
        assert_eq!(
            graph
                .paths(
                    &source,
                    Some(&NodeSelector::Handle(nodes[4].clone())),
                    yens_options(true, 2, Some("ROAD"), Some("cost")),
                )
                .unwrap()
                .num_rows(),
            0
        );
        let singleton = graph
            .paths(
                &source,
                Some(&source),
                yens_options(true, 4, Some("ROAD"), Some("cost")),
            )
            .unwrap();
        assert_eq!(
            (
                singleton.num_rows(),
                singleton
                    .column_by_name("rank")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                uuid_path(&singleton, 0),
            ),
            (1, 1, vec![*nodes[0].uuid.as_bytes()])
        );
    }

    #[test]
    fn yens_rejects_missing_target_invalid_options_and_strict_weights() {
        let graph = GraphForge::new(None).unwrap();
        let source = add_person(&graph, "Alice");
        let target = add_person(&graph, "Bob");
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {cost:-1.0, null_cost:null, text_cost:'heavy', \
                 infinite_cost:1e308 * 2.0}]->(b)",
            )
            .unwrap();
        let source = NodeSelector::Handle(source);
        let target = NodeSelector::Handle(target);
        assert!(matches!(
            graph.paths(&source, None, yens_options(true, 2, None, None)),
            Err(GfError::Validation(message)) if message == "yens requires a target selector"
        ));
        assert!(matches!(
            graph.paths(
                &source,
                Some(&target),
                yens_options(true, 0, None, None)
            ),
            Err(GfError::Validation(message)) if message == "yens k must be at least 1"
        ));
        assert!(matches!(
            graph.paths(
                &source,
                Some(&target),
                PathsOptions {
                    heuristic: Some("estimate".into()),
                    ..yens_options(true, 2, None, None)
                }
            ),
            Err(GfError::Validation(message))
                if message == "yens does not accept a heuristic property"
        ));
        for options in [
            yens_options(true, 2, Some(" "), None),
            yens_options(true, 2, Some("ROAD"), Some(" ")),
            yens_options(true, 2, Some("ROAD"), Some("missing")),
            yens_options(true, 2, Some("ROAD"), Some("null_cost")),
            yens_options(true, 2, Some("ROAD"), Some("text_cost")),
            yens_options(true, 2, Some("ROAD"), Some("infinite_cost")),
        ] {
            assert!(matches!(
                graph.paths(&source, Some(&target), options),
                Err(GfError::Validation(_))
            ));
        }
        assert!(matches!(
            graph.paths(
                &source,
                Some(&target),
                yens_options(true, 2, Some("ROAD"), Some("cost"))
            ),
            Err(GfError::Execution(message))
                if message.contains("finite non-negative edge weights")
        ));
    }

    #[test]
    fn astar_is_uuid_only_exact_deterministic_and_knowledge_independent() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = [
            add_person_with_heuristic(&graph, "Alice", 3.0),
            add_person_with_heuristic(&graph, "Bob", 2.0),
            add_person_with_heuristic(&graph, "Carol", 2.0),
            add_person_with_heuristic(&graph, "Dan", 0.0),
            add_person_with_heuristic(&graph, "Eve", 8.0),
        ];
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(c), \
                 (a)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:2.0}]->(d), \
                 (c)-[:ROAD {cost:2.0}]->(d), (a)-[:ROAD {cost:9.0}]->(d)",
            )
            .unwrap();
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:UNIT]->(b), (b)-[:UNIT]->(e)",
            )
            .unwrap();
        let source = NodeSelector::Handle(nodes[0].clone());
        let target = NodeSelector::Handle(nodes[3].clone());
        let options = astar_options(true, Some("ROAD"), Some("cost"), Some("heuristic"));
        let batch = graph
            .paths(&source, Some(&target), options.clone())
            .unwrap();

        assert_eq!(batch.schema().metadata()["graphforge.algorithm"], "astar");
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", false),
                ("target_uuid", false),
                ("cost", false),
                ("path", false),
            ]
        );
        assert!(batch.column_by_name("source_id").is_none());
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            3.0
        );
        assert_eq!(
            uuid_path(&batch, 0),
            [nodes[0].uuid, nodes[1].uuid, nodes[3].uuid].map(|uuid| *uuid.as_bytes())
        );
        assert_eq!(batch, graph.paths(&source, Some(&target), options).unwrap());

        let zero = graph
            .paths(
                &source,
                Some(&target),
                astar_options(true, Some("ROAD"), Some("cost"), None),
            )
            .unwrap();
        assert_eq!(uuid_path(&zero, 0), uuid_path(&batch, 0));
        assert_eq!(
            graph
                .paths(
                    &target,
                    Some(&source),
                    astar_options(true, Some("ROAD"), Some("cost"), None),
                )
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            graph
                .paths(
                    &target,
                    Some(&source),
                    astar_options(false, Some("ROAD"), Some("cost"), None),
                )
                .unwrap()
                .num_rows(),
            1
        );
        let eve = NodeSelector::Handle(nodes[4].clone());
        let unit = graph
            .paths(
                &source,
                Some(&eve),
                astar_options(true, Some("UNIT"), None, None),
            )
            .unwrap();
        assert_eq!(
            unit.column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
        assert_eq!(
            uuid_path(&unit, 0),
            [nodes[0].uuid, nodes[1].uuid, nodes[4].uuid].map(|uuid| *uuid.as_bytes())
        );
        let singleton = graph
            .paths(
                &target,
                Some(&target),
                astar_options(true, None, None, Some("heuristic")),
            )
            .unwrap();
        assert_eq!(uuid_path(&singleton, 0), [*nodes[3].uuid.as_bytes()]);
    }

    #[test]
    fn astar_rejects_missing_target_and_invalid_options_or_properties() {
        let graph = GraphForge::new(None).unwrap();
        let source = add_person_with_heuristic(&graph, "Alice", 1.0);
        let target = add_person_with_heuristic(&graph, "Bob", 0.0);
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {cost:1.0, negative:-1.0}]->(b)",
            )
            .unwrap();
        let source = NodeSelector::Handle(source);
        let target = NodeSelector::Handle(target);

        assert!(matches!(
            graph.paths(
                &source,
                None,
                astar_options(true, Some("ROAD"), Some("cost"), None)
            ),
            Err(GfError::Validation(message)) if message.contains("target")
        ));
        for options in [
            PathsOptions {
                k: 2,
                ..astar_options(true, Some("ROAD"), Some("cost"), None)
            },
            astar_options(true, Some(" "), Some("cost"), None),
            astar_options(true, Some("ROAD"), Some(" "), None),
            astar_options(true, Some("ROAD"), Some("negative"), None),
            astar_options(true, Some("ROAD"), Some("cost"), Some(" ")),
            astar_options(true, Some("ROAD"), Some("cost"), Some("missing")),
        ] {
            assert!(matches!(
                graph.paths(&source, Some(&target), options),
                Err(GfError::Validation(_)) | Err(GfError::Execution(_))
            ));
        }
        assert!(matches!(
            graph.paths(
                &source,
                Some(&target),
                PathsOptions {
                    heuristic: Some("heuristic".into()),
                    ..dijkstra_options(true, Some("ROAD"), Some("cost"))
                },
            ),
            Err(GfError::Validation(message)) if message.contains("does not accept")
        ));

        let invalid_target = GraphForge::new(None).unwrap();
        let source = add_person_with_heuristic(&invalid_target, "Alice", 1.0);
        let target = add_person_with_heuristic(&invalid_target, "Bob", 1.0);
        assert!(matches!(
            invalid_target.paths(
                &NodeSelector::Handle(source),
                Some(&NodeSelector::Handle(target)),
                astar_options(true, None, None, Some("heuristic")),
            ),
            Err(GfError::Execution(message)) if message.contains("target heuristic")
        ));

        for invalid in [
            PropValue::Float(-1.0),
            PropValue::Float(f64::NAN),
            PropValue::Float(f64::INFINITY),
            PropValue::Float(f64::NEG_INFINITY),
            PropValue::Str("near".into()),
        ] {
            let invalid_graph = GraphForge::new(None).unwrap();
            let source = add_person_with_heuristic_value(&invalid_graph, "Alice", invalid);
            let target = add_person_with_heuristic(&invalid_graph, "Bob", 0.0);
            assert!(matches!(
                invalid_graph.paths(
                    &NodeSelector::Handle(source),
                    Some(&NodeSelector::Handle(target)),
                    astar_options(true, None, None, Some("heuristic")),
                ),
                Err(GfError::Validation(_)) | Err(GfError::Execution(_))
            ));
        }

        let overflow = GraphForge::new(None).unwrap();
        let source = add_person_with_heuristic(&overflow, "Alice", f64::MAX);
        add_person_with_heuristic(&overflow, "Bob", f64::MAX);
        let target = add_person_with_heuristic(&overflow, "Dan", 0.0);
        overflow
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:1.7976931348623157e308}]->(b), \
                 (b)-[:ROAD {cost:1.0}]->(d)",
            )
            .unwrap();
        assert!(matches!(
            overflow.paths(
                &NodeSelector::Handle(source),
                Some(&NodeSelector::Handle(target)),
                astar_options(true, Some("ROAD"), Some("cost"), Some("heuristic")),
            ),
            Err(GfError::Validation(_)) | Err(GfError::Execution(_))
        ));
    }

    #[test]
    fn transitive_closure_is_global_uuid_ordered_and_knowledge_independent() {
        // Exploratory mode has no ontology or knowledge sidecars.
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(c), (d)-[:OTHER]->(e)",
            )
            .unwrap();

        let source = NodeSelector::Match {
            label: "Person".into(),
            property: "name".into(),
            value: PropValue::Str("Eve".into()),
        };
        let options = transitive_closure_options(true, Some("KNOWS"));
        let batch = graph.paths(&source, None, options.clone()).unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("target_uuid", &DataType::FixedSizeBinary(16), false),
            ]
        );
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "transitive_closure"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "paths");
        assert!(batch.column_by_name("source_id").is_none());
        assert!(batch.column_by_name("target_id").is_none());

        let uuids = nodes.map(|node| *node.uuid.as_bytes());
        let mut expected = vec![
            (uuids[0], uuids[0]),
            (uuids[0], uuids[1]),
            (uuids[0], uuids[2]),
            (uuids[1], uuids[0]),
            (uuids[1], uuids[1]),
            (uuids[1], uuids[2]),
            (uuids[2], uuids[2]),
        ];
        expected.sort_unstable();
        assert_eq!(uuid_pairs(&batch), expected);
        assert_eq!(batch, graph.paths(&source, None, options).unwrap());

        let undirected = graph
            .paths(
                &source,
                None,
                transitive_closure_options(false, Some("KNOWS")),
            )
            .unwrap();
        let mut expected_undirected = Vec::new();
        for source in &uuids[..3] {
            for target in &uuids[..3] {
                expected_undirected.push((*source, *target));
            }
        }
        expected_undirected.sort_unstable();
        assert_eq!(uuid_pairs(&undirected), expected_undirected);

        assert_eq!(
            uuid_pairs(
                &graph
                    .paths(
                        &source,
                        None,
                        transitive_closure_options(true, Some("OTHER")),
                    )
                    .unwrap()
            ),
            vec![(uuids[3], uuids[4])]
        );

        let edgeless = GraphForge::new(None).unwrap();
        let isolated = add_person(&edgeless, "Isolated");
        assert_eq!(
            edgeless
                .paths(
                    &NodeSelector::Handle(isolated),
                    None,
                    transitive_closure_options(true, None),
                )
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn transitive_closure_rejects_target_and_path_only_options() {
        let graph = GraphForge::new(None).unwrap();
        let source = add_person(&graph, "Alice");
        let target = add_person(&graph, "Bob");
        let source = NodeSelector::Handle(source);
        let target = NodeSelector::Handle(target);

        assert!(matches!(
            graph.paths(
                &source,
                Some(&target),
                transitive_closure_options(true, None),
            ),
            Err(GfError::Validation(message)) if message.contains("does not accept a target")
        ));
        for options in [
            PathsOptions {
                k: 2,
                ..transitive_closure_options(true, None)
            },
            PathsOptions {
                weight: Some("cost".into()),
                ..transitive_closure_options(true, None)
            },
            PathsOptions {
                heuristic: Some("estimate".into()),
                ..transitive_closure_options(true, None)
            },
            transitive_closure_options(true, Some(" ")),
        ] {
            assert!(matches!(
                graph.paths(&source, None, options),
                Err(GfError::Validation(_))
            ));
        }
        assert!(matches!(
            graph.paths(
                &NodeSelector::Uuid(graphforge_core::uuid::new_v7()),
                None,
                transitive_closure_options(true, None),
            ),
            Err(GfError::Validation(message)) if message.contains("matched no nodes")
        ));
    }

    #[test]
    fn minimum_spanning_tree_is_uuid_only_weighted_and_knowledge_independent() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        let nodes =
            ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Fox'}) \
                 CREATE (a)-[:ROAD {cost:4.0}]->(b), \
                 (a)-[:ROAD {cost:3.0}]->(c), (b)-[:ROAD {cost:1.0}]->(c), \
                 (b)-[:ROAD {cost:2.0}]->(d), (c)-[:ROAD {cost:4.0}]->(d), \
                 (e)-[:ROAD {cost:-2.0}]->(f), (e)-[:ROAD {cost:3.0}]->(f), \
                 (d)-[:ROAD {cost:-10.0}]->(d), \
                 (a)-[:OTHER {cost:-100.0}]->(d)",
            )
            .unwrap();
        let options = minimum_spanning_tree_options(Some("ROAD"), Some("cost"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();

        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "minimum_spanning_tree"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.data_type()))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", &DataType::FixedSizeBinary(16)),
                ("source_uuid", &DataType::FixedSizeBinary(16)),
                ("target_uuid", &DataType::FixedSizeBinary(16)),
                ("weight", &DataType::Float64),
            ]
        );
        assert!(batch.column_by_name("edge_id").is_none());
        assert_eq!(
            batch
                .column_by_name("weight")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[-2.0, 1.0, 2.0, 3.0]
        );
        assert_eq!(batch.column_by_name("weight").unwrap().null_count(), 0);

        let sources = batch
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = batch
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (row, (left, right)) in [(4, 5), (1, 2), (1, 3), (0, 2)].into_iter().enumerate() {
            let mut expected = [*nodes[left].uuid.as_bytes(), *nodes[right].uuid.as_bytes()];
            expected.sort_unstable();
            assert_eq!(sources.value(row), expected[0]);
            assert_eq!(targets.value(row), expected[1]);
        }
        let edge_uuids = batch
            .column_by_name("edge_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let unique = (0..edge_uuids.len())
            .map(|row| edge_uuids.value(row))
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(unique.len(), 4);
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
        assert_eq!(
            graph
                .analyze(
                    Some("Missing"),
                    minimum_spanning_tree_options(Some("ROAD"), Some("cost")),
                )
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn minimum_spanning_tree_defaults_to_unit_weight_with_stable_ties() {
        let graph = GraphForge::new(None).unwrap();
        let mut nodes = ["Alice", "Bob", "Carol"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}) \
                 CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), \
                 (a)-[:ROAD]->(c), (b)-[:ROAD]->(c)",
            )
            .unwrap();
        nodes.sort_unstable_by_key(|node| *node.uuid.as_bytes());
        let batch = graph
            .analyze(None, minimum_spanning_tree_options(Some("ROAD"), None))
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .column_by_name("weight")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[1.0, 1.0]
        );
        let sources = batch
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = batch
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (row, target) in [1, 2].into_iter().enumerate() {
            assert_eq!(sources.value(row), nodes[0].uuid.as_bytes());
            assert_eq!(targets.value(row), nodes[target].uuid.as_bytes());
        }

        let empty = GraphForge::new(None).unwrap();
        let empty_batch = empty
            .analyze(None, minimum_spanning_tree_options(None, None))
            .unwrap();
        assert_eq!(empty_batch.num_rows(), 0);
        assert_eq!(empty_batch.schema(), batch.schema());
    }

    #[test]
    fn minimum_spanning_tree_rejects_directed_and_strict_weight_errors() {
        let graph = GraphForge::new(None).unwrap();
        add_person(&graph, "Alice");
        add_person(&graph, "Bob");
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {null_cost:null, text_cost:'heavy', \
                 infinite_cost:1e308 * 2.0}]->(b)",
            )
            .unwrap();

        let mut directed = minimum_spanning_tree_options(Some("ROAD"), None);
        directed.directed = true;
        assert!(matches!(
            graph.analyze(None, directed),
            Err(GfError::Validation(message)) if message.contains("directed=false")
        ));
        for options in [
            minimum_spanning_tree_options(Some(" "), None),
            minimum_spanning_tree_options(Some("ROAD"), Some(" ")),
            minimum_spanning_tree_options(Some("ROAD"), Some("missing")),
            minimum_spanning_tree_options(Some("ROAD"), Some("null_cost")),
            minimum_spanning_tree_options(Some("ROAD"), Some("text_cost")),
            minimum_spanning_tree_options(Some("ROAD"), Some("infinite_cost")),
            AnalyzeOptions {
                weight: Some("cost".into()),
                ..AnalyzeOptions::default()
            },
        ] {
            assert!(matches!(
                graph.analyze(None, options),
                Err(GfError::Validation(_))
            ));
        }
    }

    #[test]
    fn maximum_spanning_tree_is_uuid_only_weighted_and_knowledge_independent() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        let nodes =
            ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Fox'}) \
                 CREATE (a)-[:ROAD {cost:4.0}]->(b), \
                 (a)-[:ROAD {cost:9.0}]->(b), (b)-[:ROAD {cost:8.0}]->(a), \
                 (a)-[:ROAD {cost:7.0}]->(c), (b)-[:ROAD {cost:6.0}]->(c), \
                 (b)-[:ROAD {cost:-3.0}]->(d), (c)-[:ROAD {cost:-1.0}]->(d), \
                 (e)-[:ROAD {cost:-5.0}]->(f), (e)-[:ROAD {cost:-2.0}]->(f), \
                 (d)-[:ROAD {cost:1e308}]->(d), \
                 (a)-[:OTHER {cost:100.0}]->(d)",
            )
            .unwrap();
        let options = maximum_spanning_tree_options(Some("ROAD"), Some("cost"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();

        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "maximum_spanning_tree"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", &DataType::FixedSizeBinary(16), false),
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("target_uuid", &DataType::FixedSizeBinary(16), false),
                ("weight", &DataType::Float64, true),
            ]
        );
        assert!(batch.column_by_name("edge_id").is_none());
        assert_eq!(
            batch
                .column_by_name("weight")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[9.0, 7.0, -1.0, -2.0]
        );
        assert_eq!(batch.column_by_name("weight").unwrap().null_count(), 0);

        let sources = batch
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = batch
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (row, (left, right)) in [(0, 1), (0, 2), (2, 3), (4, 5)].into_iter().enumerate() {
            let mut expected = [*nodes[left].uuid.as_bytes(), *nodes[right].uuid.as_bytes()];
            expected.sort_unstable();
            assert_eq!(sources.value(row), expected[0]);
            assert_eq!(targets.value(row), expected[1]);
        }
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
        assert_eq!(
            graph
                .analyze(
                    Some("Missing"),
                    maximum_spanning_tree_options(Some("ROAD"), Some("cost")),
                )
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn maximum_spanning_tree_defaults_to_unit_weight_and_rejects_invalid_options() {
        let graph = GraphForge::new(None).unwrap();
        add_person(&graph, "Alice");
        add_person(&graph, "Bob");
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {null_cost:null, text_cost:'heavy', \
                 infinite_cost:1e308 * 2.0}]->(b)",
            )
            .unwrap();

        let unit = graph
            .analyze(None, maximum_spanning_tree_options(Some("ROAD"), None))
            .unwrap();
        assert_eq!(unit.num_rows(), 1);
        assert_eq!(
            unit.column_by_name("weight")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.0
        );

        let mut directed = maximum_spanning_tree_options(Some("ROAD"), None);
        directed.directed = true;
        assert!(matches!(
            graph.analyze(None, directed),
            Err(GfError::Validation(message)) if message.contains("directed=false")
        ));
        for options in [
            maximum_spanning_tree_options(Some(" "), None),
            maximum_spanning_tree_options(Some("ROAD"), Some(" ")),
            maximum_spanning_tree_options(Some("ROAD"), Some("missing")),
            maximum_spanning_tree_options(Some("ROAD"), Some("null_cost")),
            maximum_spanning_tree_options(Some("ROAD"), Some("text_cost")),
            maximum_spanning_tree_options(Some("ROAD"), Some("infinite_cost")),
        ] {
            assert!(matches!(
                graph.analyze(None, options),
                Err(GfError::Validation(_))
            ));
        }

        let empty = GraphForge::new(None).unwrap();
        let empty_batch = empty
            .analyze(None, maximum_spanning_tree_options(None, None))
            .unwrap();
        assert_eq!(empty_batch.num_rows(), 0);
        assert_eq!(empty_batch.schema(), unit.schema());
    }

    fn automorphism_count(batch: &arrow::record_batch::RecordBatch) -> u64 {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [("count", &DataType::UInt64, false)]
        );
        assert_eq!(batch.column(0).null_count(), 0);
        assert_eq!(
            batch.schema().metadata(),
            &HashMap::from([
                ("graphforge.algorithm".into(), "count_automorphisms".into()),
                ("graphforge.algorithm_schema_version".into(), "1".into()),
                ("graphforge.verb".into(), "analyze".into()),
            ])
        );
        for forbidden in [
            "node_uuid",
            "provenance",
            "confidence",
            "assertion",
            "evidence",
            "belief",
            "hypothesis",
            "valid_time",
            "algorithm_run_uuid",
            "run_uuid",
        ] {
            assert!(batch.column_by_name(forbidden).is_none());
        }
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0)
    }

    fn build_automorphism_multigraph(graph: &GraphForge, property_prefix: &str) {
        for name in ["A", "B", "C", "D"] {
            graph
                .add_node(
                    "Person",
                    &HashMap::from([
                        ("name".into(), PropValue::Str(name.into())),
                        (
                            "payload".into(),
                            PropValue::Str(format!("{property_prefix}-{name}")),
                        ),
                    ]),
                )
                .unwrap();
        }
        graph
            .execute(
                "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}) \
                 CREATE (a)-[:ROAD]->(a), (b)-[:ROAD]->(b), \
                 (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), \
                 (c)-[:ROAD]->(d), (d)-[:ROAD]->(c)",
            )
            .unwrap();
    }

    #[test]
    fn count_automorphisms_is_exact_persisted_and_uuid_rename_invariant() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        build_automorphism_multigraph(&graph, "persisted");

        let directed = count_automorphisms_options(true, Some("ROAD"));
        let undirected = count_automorphisms_options(false, Some("ROAD"));
        let directed_batch = graph.analyze(Some("Person"), directed.clone()).unwrap();
        let undirected_batch = graph.analyze(Some("Person"), undirected.clone()).unwrap();
        assert_eq!(automorphism_count(&directed_batch), 2);
        assert_eq!(automorphism_count(&undirected_batch), 4);
        assert_eq!(
            directed_batch,
            graph.analyze(Some("Person"), directed.clone()).unwrap()
        );
        assert_eq!(
            undirected_batch,
            graph.analyze(Some("Person"), undirected.clone()).unwrap()
        );
        drop(graph);

        let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        assert_eq!(
            directed_batch,
            reopened.analyze(Some("Person"), directed.clone()).unwrap()
        );
        assert_eq!(
            undirected_batch,
            reopened
                .analyze(Some("Person"), undirected.clone())
                .unwrap()
        );

        let renamed = GraphForge::new(None).unwrap();
        build_automorphism_multigraph(&renamed, "renamed-and-property-distinct");
        assert_eq!(
            automorphism_count(&renamed.analyze(Some("Person"), directed).unwrap()),
            2
        );
        assert_eq!(
            automorphism_count(&renamed.analyze(Some("Person"), undirected).unwrap()),
            4
        );
    }

    #[test]
    fn count_automorphisms_reports_closed_options_and_overflow_structurally() {
        let graph = GraphForge::new(None).unwrap();
        build_automorphism_multigraph(&graph, "baseline");
        let baseline = graph
            .analyze(
                Some("Person"),
                count_automorphisms_options(true, Some("ROAD")),
            )
            .unwrap();
        for options in [
            AnalyzeOptions {
                weight: Some("weight".into()),
                ..count_automorphisms_options(true, Some("ROAD"))
            },
            AnalyzeOptions {
                k: Some(2),
                ..count_automorphisms_options(true, Some("ROAD"))
            },
            AnalyzeOptions {
                partition_property: Some("partition".into()),
                ..count_automorphisms_options(true, Some("ROAD"))
            },
            count_automorphisms_options(true, Some(" ")),
        ] {
            assert!(matches!(
                graph.analyze(Some("Person"), options),
                Err(GfError::Validation(_))
            ));
            assert_eq!(
                graph
                    .analyze(
                        Some("Person"),
                        count_automorphisms_options(true, Some("ROAD"))
                    )
                    .unwrap(),
                baseline
            );
        }

        let overflow = GraphForge::new(None).unwrap();
        for index in 0..21 {
            add_person(&overflow, &format!("isolated-{index}"));
        }
        assert!(matches!(
            overflow.analyze(
                Some("Person"),
                count_automorphisms_options(false, None)
            ),
            Err(GfError::Execution(message))
                if message.contains("automorphism count exceeds UInt64 range")
        ));
    }

    #[test]
    fn triangle_count_is_exact_deterministic_and_projection_scoped() {
        let graph = GraphForge::new(None).unwrap();
        for (label, name) in [
            ("Person", "Alice"),
            ("Person", "Bob"),
            ("Person", "Carol"),
            ("Person", "Dan"),
            ("Person", "Eve"),
            ("Animal", "Fox"),
        ] {
            graph
                .add_node(
                    label,
                    &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
                )
                .unwrap();
        }
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Animal {name:'Fox'}) \
                 CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(d), (c)-[:ROAD]->(d), \
                 (d)-[:ROAD]->(d), (a)-[:OTHER]->(e), \
                 (e)-[:OTHER]->(c), (f)-[:ROAD]->(a)",
            )
            .unwrap();

        let options = triangle_count_options(Some("ROAD"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "triangle_count");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt64);
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(batch.column(0).null_count(), 0);
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "triangle_count"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert!(batch.column_by_name("node_id").is_none());
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

        let other = graph
            .analyze(Some("Person"), triangle_count_options(Some("OTHER")))
            .unwrap();
        assert_eq!(
            other
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            0
        );
    }

    #[test]
    fn triangle_count_returns_zero_for_empty_and_rejects_invalid_options() {
        let graph = GraphForge::new(None).unwrap();
        let empty = graph
            .analyze(Some("Missing"), triangle_count_options(None))
            .unwrap();
        assert_eq!(empty.num_rows(), 1);
        assert_eq!(empty.num_columns(), 1);
        assert_eq!(empty.schema().field(0).name(), "triangle_count");
        assert_eq!(empty.schema().field(0).data_type(), &DataType::UInt64);
        assert!(!empty.schema().field(0).is_nullable());
        assert_eq!(empty.column(0).null_count(), 0);
        assert_eq!(
            empty
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            0
        );

        let mut directed = triangle_count_options(None);
        directed.directed = true;
        let mut weighted = triangle_count_options(None);
        weighted.weight = Some("cost".into());
        for result in [
            graph.analyze(None, directed),
            graph.analyze(None, weighted),
            graph.analyze(None, triangle_count_options(Some(" "))),
            graph.analyze(Some(""), triangle_count_options(None)),
            graph.analyze(Some(" Person"), triangle_count_options(None)),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn transitivity_is_exact_deterministic_and_projection_scoped() {
        let graph = GraphForge::new(None).unwrap();
        for (label, name) in [
            ("Person", "Alice"),
            ("Person", "Bob"),
            ("Person", "Carol"),
            ("Person", "Dan"),
            ("Person", "Eve"),
            ("Animal", "Fox"),
        ] {
            graph
                .add_node(
                    label,
                    &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
                )
                .unwrap();
        }
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Animal {name:'Fox'}) \
                 CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(d), (c)-[:ROAD]->(d), \
                 (d)-[:ROAD]->(d), (a)-[:OTHER]->(e), \
                 (e)-[:OTHER]->(c), (f)-[:ROAD]->(a)",
            )
            .unwrap();

        let options = transitivity_options(Some("ROAD"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "transitivity");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(batch.column(0).null_count(), 0);
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "transitivity"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            0.75
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

        let other = graph
            .analyze(Some("Person"), transitivity_options(Some("OTHER")))
            .unwrap();
        assert_eq!(
            other
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            0.0
        );
    }

    #[test]
    fn transitivity_returns_zero_for_empty_and_rejects_invalid_options() {
        let graph = GraphForge::new(None).unwrap();
        for label in [None, Some("Missing")] {
            let empty = graph.analyze(label, transitivity_options(None)).unwrap();
            assert_eq!(empty.num_rows(), 1);
            assert_eq!(empty.num_columns(), 1);
            assert_eq!(empty.schema().field(0).name(), "transitivity");
            assert_eq!(empty.schema().field(0).data_type(), &DataType::Float64);
            assert!(!empty.schema().field(0).is_nullable());
            assert_eq!(empty.column(0).null_count(), 0);
            assert_eq!(
                empty.schema().metadata()["graphforge.algorithm"],
                "transitivity"
            );
            assert_eq!(empty.schema().metadata()["graphforge.verb"], "analyze");
            assert_eq!(
                empty.schema().metadata()["graphforge.algorithm_schema_version"],
                "1"
            );
            assert_eq!(
                empty
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                0.0
            );
        }

        let mut directed = transitivity_options(None);
        directed.directed = true;
        let mut weighted = transitivity_options(None);
        weighted.weight = Some("cost".into());
        for result in [
            graph.analyze(None, directed),
            graph.analyze(None, weighted),
            graph.analyze(None, transitivity_options(Some(" "))),
            graph.analyze(Some(""), transitivity_options(None)),
            graph.analyze(Some(" Person"), transitivity_options(None)),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn is_planar_is_exact_deterministic_and_projection_scoped() {
        let graph = GraphForge::new(None).unwrap();
        for (label, name) in [
            ("Person", "A"),
            ("Person", "B"),
            ("Person", "C"),
            ("Person", "D"),
            ("Person", "E"),
            ("Person", "F"),
            ("Animal", "Fox"),
        ] {
            graph
                .add_node(
                    label,
                    &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
                )
                .unwrap();
        }
        graph
            .execute(
                "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (fox:Animal {name:'Fox'}) \
                 CREATE (a)-[:ROAD]->(d), (a)-[:ROAD]->(e), (a)-[:ROAD]->(f), \
                 (b)-[:ROAD]->(d), (b)-[:ROAD]->(e), (b)-[:ROAD]->(f), \
                 (c)-[:ROAD]->(d), (c)-[:ROAD]->(e), (c)-[:ROAD]->(f), \
                 (a)-[:ROAD]->(d), (d)-[:ROAD]->(a), (a)-[:ROAD]->(a), \
                 (a)-[:OTHER]->(b), (fox)-[:ROAD]->(a)",
            )
            .unwrap();

        let options = is_planar_options(Some("ROAD"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "is_planar");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Boolean);
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(batch.column(0).null_count(), 0);
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "is_planar"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert!(
            !batch
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

        let other = graph
            .analyze(Some("Person"), is_planar_options(Some("OTHER")))
            .unwrap();
        assert!(
            other
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }

    #[test]
    fn is_planar_accepts_empty_and_forests_and_rejects_invalid_options() {
        let graph = GraphForge::new(None).unwrap();
        for label in [None, Some("Missing")] {
            let empty = graph.analyze(label, is_planar_options(None)).unwrap();
            assert_eq!(empty.num_rows(), 1);
            assert_eq!(empty.num_columns(), 1);
            assert_eq!(empty.schema().field(0).name(), "is_planar");
            assert_eq!(empty.schema().field(0).data_type(), &DataType::Boolean);
            assert!(!empty.schema().field(0).is_nullable());
            assert_eq!(empty.column(0).null_count(), 0);
            assert!(
                empty
                    .column(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(0)
            );
        }

        graph
            .execute(
                "CREATE (:Person {name:'A'})-[:ROAD]->(:Person {name:'B'}), \
                 (:Person {name:'C'}), (:Person {name:'D'})-[:ROAD]->(:Person {name:'E'})",
            )
            .unwrap();
        let forest = graph
            .analyze(Some("Person"), is_planar_options(Some("ROAD")))
            .unwrap();
        assert!(
            forest
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );

        let mut directed = is_planar_options(None);
        directed.directed = true;
        let mut weighted = is_planar_options(None);
        weighted.weight = Some("cost".into());
        for result in [
            graph.analyze(None, directed),
            graph.analyze(None, weighted),
            graph.analyze(None, is_planar_options(Some(" "))),
            graph.analyze(Some(""), is_planar_options(None)),
            graph.analyze(Some(" Person"), is_planar_options(None)),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn triad_census_returns_canonical_sixteen_row_arrow_result() {
        let graph = GraphForge::new(None).unwrap();
        for name in ["Alice", "Bob", "Carol", "Isolate"] {
            graph
                .add_node(
                    "Person",
                    &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
                )
                .unwrap();
        }
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (a)-[:ROAD]->(a), (a)-[:ROAD]->(b), (a)-[:OTHER]->(c)",
            )
            .unwrap();

        let options = triad_census_options(Some("ROAD"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(batch.num_rows(), 16);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "triad_type");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(batch.schema().field(1).name(), "count");
        assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
        assert!(!batch.schema().field(1).is_nullable());
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "triad_census"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let expected_names = [
            "003", "012", "102", "021D", "021U", "021C", "111D", "111U", "030T", "030C", "201",
            "120D", "120U", "120C", "210", "300",
        ];
        assert_eq!(
            names.iter().map(Option::unwrap).collect::<Vec<_>>(),
            expected_names
        );
        assert_eq!(
            counts.values().to_vec(),
            [0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
    }

    #[test]
    fn triad_census_preserves_empty_rows_and_rejects_invalid_options() {
        let graph = GraphForge::new(None).unwrap();
        let empty = graph
            .analyze(Some("Missing"), triad_census_options(None))
            .unwrap();
        assert_eq!(empty.num_rows(), 16);
        assert_eq!(
            empty
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .iter()
                .sum::<u64>(),
            0
        );

        let mut undirected = triad_census_options(None);
        undirected.directed = false;
        let mut weighted = triad_census_options(None);
        weighted.weight = Some("cost".into());
        for result in [
            graph.analyze(None, undirected),
            graph.analyze(None, weighted),
            graph.analyze(None, triad_census_options(Some(" "))),
            graph.analyze(Some(""), triad_census_options(None)),
            graph.analyze(Some(" Person"), triad_census_options(None)),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn dyad_census_returns_canonical_three_row_arrow_result() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        for name in ["Alice", "Bob", "Carol", "Dan", "Isolate"] {
            graph
                .add_node(
                    "Person",
                    &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
                )
                .unwrap();
        }
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), \
                 (a)-[:ROAD]->(b), (a)-[:ROAD]->(c), (d)-[:ROAD]->(c), \
                 (a)-[:ROAD]->(a), (c)-[:OTHER]->(a)",
            )
            .unwrap();

        let options = dyad_census_options(Some("ROAD"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "dyad_type");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(batch.schema().field(1).name(), "count");
        assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
        assert!(!batch.schema().field(1).is_nullable());
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "dyad_census"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(
            names.iter().map(Option::unwrap).collect::<Vec<_>>(),
            ["mutual", "asymmetric", "null"]
        );
        assert_eq!(counts.values(), &[1, 2, 7]);
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

        let all_relationships = graph
            .analyze(Some("Person"), dyad_census_options(None))
            .unwrap();
        assert_eq!(
            all_relationships
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[2, 1, 7]
        );
    }

    #[test]
    fn dyad_census_preserves_zero_rows_and_rejects_invalid_options() {
        let graph = GraphForge::new(None).unwrap();
        for name in ["Fox", "Owl"] {
            graph
                .add_node(
                    "Animal",
                    &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
                )
                .unwrap();
        }
        for (label, expected) in [(Some("Missing"), [0, 0, 0]), (Some("Animal"), [0, 0, 1])] {
            let batch = graph.analyze(label, dyad_census_options(None)).unwrap();
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values(),
                &expected
            );
        }

        let singleton = GraphForge::new(None).unwrap();
        singleton
            .add_node(
                "Person",
                &HashMap::from([("name".to_owned(), PropValue::Str("Solo".to_owned()))]),
            )
            .unwrap();
        assert_eq!(
            singleton
                .analyze(Some("Person"), dyad_census_options(None))
                .unwrap()
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[0, 0, 0]
        );

        let mut undirected = dyad_census_options(None);
        undirected.directed = false;
        let mut weighted = dyad_census_options(None);
        weighted.weight = Some("cost".into());
        for result in [
            graph.analyze(None, undirected),
            graph.analyze(None, weighted),
            graph.analyze(None, dyad_census_options(Some(" "))),
            graph.analyze(Some(""), dyad_census_options(None)),
            graph.analyze(Some(" Person"), dyad_census_options(None)),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn node_coloring_is_exact_deterministic_and_projection_scoped() {
        let graph = GraphForge::new(None).unwrap();
        let mut nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"]
            .into_iter()
            .map(|name| {
                let node = add_person(&graph, name);
                (name, *node.uuid.as_bytes())
            })
            .collect::<Vec<_>>();
        graph
            .add_node(
                "Animal",
                &HashMap::from([("name".to_owned(), PropValue::Str("Fox".to_owned()))]),
            )
            .unwrap();
        nodes.sort_unstable_by_key(|(_, uuid)| *uuid);
        graph
            .execute(&format!(
                "MATCH (a:Person {{name:'{}'}}), (b:Person {{name:'{}'}}), \
                 (c:Person {{name:'{}'}}), (d:Person {{name:'{}'}}), \
                 (e:Person {{name:'{}'}}), (f:Animal {{name:'Fox'}}) \
                 CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(c), \
                 (b)-[:ROAD]->(c), (c)-[:ROAD]->(d), \
                 (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), \
                 (d)-[:OTHER]->(e), (f)-[:ROAD]->(a)",
                nodes[0].0, nodes[1].0, nodes[2].0, nodes[3].0, nodes[4].0,
            ))
            .unwrap();

        let options = node_coloring_options(Some("ROAD"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "node_uuid");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(batch.schema().field(1).name(), "color");
        assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
        assert!(!batch.schema().field(0).is_nullable());
        assert!(!batch.schema().field(1).is_nullable());
        assert_eq!(batch.column(0).null_count(), 0);
        assert_eq!(batch.column(1).null_count(), 0);
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "node_coloring"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_eq!(
            node_coloring_rows(&batch),
            nodes
                .iter()
                .zip([0, 1, 2, 0, 0])
                .map(|((_, uuid), color)| (*uuid, color))
                .collect::<Vec<_>>()
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
    }

    #[test]
    fn node_coloring_keeps_typed_empty_schema_and_rejects_invalid_options() {
        let graph = GraphForge::new(None).unwrap();
        for label in [None, Some("Missing")] {
            let empty = graph
                .analyze(label, node_coloring_options(Some("ROAD")))
                .unwrap();
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(empty.num_columns(), 2);
            assert_eq!(empty.schema().field(0).name(), "node_uuid");
            assert_eq!(
                empty.schema().field(0).data_type(),
                &DataType::FixedSizeBinary(16)
            );
            assert_eq!(empty.schema().field(1).name(), "color");
            assert_eq!(empty.schema().field(1).data_type(), &DataType::UInt64);
            assert!(!empty.schema().field(0).is_nullable());
            assert!(!empty.schema().field(1).is_nullable());
            assert_eq!(empty.column(0).null_count(), 0);
            assert_eq!(empty.column(1).null_count(), 0);
            assert_eq!(
                empty.schema().metadata()["graphforge.algorithm"],
                "node_coloring"
            );
            assert_eq!(empty.schema().metadata()["graphforge.verb"], "analyze");
            assert_eq!(
                empty.schema().metadata()["graphforge.algorithm_schema_version"],
                "1"
            );
        }

        let mut directed = node_coloring_options(None);
        directed.directed = true;
        let mut weighted = node_coloring_options(None);
        weighted.weight = Some("cost".into());
        for result in [
            graph.analyze(None, directed),
            graph.analyze(None, weighted),
            graph.analyze(None, node_coloring_options(Some(" "))),
            graph.analyze(Some(""), node_coloring_options(None)),
            graph.analyze(Some(" Person"), node_coloring_options(None)),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn k1_coloring_is_deterministic_uuid_only_and_distinct_from_chromatic_number() {
        // Exploratory mode reaches the Rust graph path without an ontology or knowledge layer.
        let graph = GraphForge::new(None).unwrap();
        let mut nodes = ["A", "B", "C", "D", "E", "F", "Isolate"]
            .into_iter()
            .map(|name| {
                let node = add_person(&graph, name);
                (name, *node.uuid.as_bytes())
            })
            .collect::<Vec<_>>();
        nodes.sort_unstable_by_key(|(_, uuid)| *uuid);

        // Crown graph K3,3 minus a perfect matching. Its UUID-interleaved greedy order
        // uses three colors even though its exact chromatic number is two.
        for left in [0, 2, 4] {
            for right in [1, 3, 5] {
                if left / 2 == right / 2 {
                    continue;
                }
                graph
                    .execute(&format!(
                        "MATCH (a:Person {{name:'{}'}}), (b:Person {{name:'{}'}}) \
                         CREATE (a)-[:ROAD]->(b)",
                        nodes[left].0, nodes[right].0
                    ))
                    .unwrap();
            }
        }
        graph
            .execute(&format!(
                "MATCH (a:Person {{name:'{}'}}), (b:Person {{name:'{}'}}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(a)",
                nodes[0].0, nodes[3].0
            ))
            .unwrap();

        let options = k1_coloring_options(Some("ROAD"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        let expected = nodes
            .iter()
            .zip([0, 0, 1, 1, 2, 2, 0])
            .map(|((_, uuid), color)| (*uuid, color))
            .collect::<Vec<_>>();

        assert_eq!(batch.num_rows(), 7);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "node_uuid");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(batch.schema().field(1).name(), "color");
        assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
        assert!(!batch.schema().field(0).is_nullable());
        assert!(!batch.schema().field(1).is_nullable());
        assert_eq!(batch.column(0).null_count(), 0);
        assert_eq!(batch.column(1).null_count(), 0);
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "k1_coloring"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_eq!(batch.schema().metadata().len(), 3);
        assert_eq!(node_coloring_rows(&batch), expected);
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

        let chromatic = graph
            .analyze(Some("Person"), chromatic_number_options(Some("ROAD")))
            .unwrap();
        assert_eq!(
            chromatic
                .column_by_name("chromatic_number")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            2
        );
    }

    #[test]
    fn k1_coloring_rejects_self_loops_direction_and_weight_structurally() {
        let looped = GraphForge::new(None).unwrap();
        add_person(&looped, "Looped");
        looped
            .execute("MATCH (n:Person {name:'Looped'}) CREATE (n)-[:ROAD]->(n)")
            .unwrap();
        assert!(matches!(
            looped.analyze(Some("Person"), k1_coloring_options(Some("ROAD"))),
            Err(GfError::Execution(message))
                if message.contains("k1_coloring cannot color a graph containing a self-loop")
        ));

        let graph = GraphForge::new(None).unwrap();
        add_person(&graph, "Solo");
        for options in [
            AnalyzeOptions {
                directed: true,
                ..k1_coloring_options(None)
            },
            AnalyzeOptions {
                weight: Some("cost".into()),
                ..k1_coloring_options(None)
            },
        ] {
            assert!(matches!(
                graph.analyze(Some("Person"), options),
                Err(GfError::Validation(_))
            ));
        }
    }

    #[test]
    fn chromatic_number_is_exact_deterministic_and_projection_scoped() {
        // Exploratory mode exercises the Rust graph path without an ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        for name in ["Alice", "Bob", "Carol", "Dan", "Eve"] {
            add_person(&graph, name);
        }
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), \
                 (c)-[:ROAD]->(a), (a)-[:ROAD]->(b), \
                 (b)-[:ROAD]->(a), (d)-[:OTHER]->(e)",
            )
            .unwrap();

        let options = chromatic_number_options(Some("ROAD"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "chromatic_number");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt64);
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(batch.column(0).null_count(), 0);
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "chromatic_number"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
        assert_eq!(
            graph
                .analyze(Some("Person"), chromatic_number_options(Some("MISSING")))
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[test]
    fn chromatic_number_keeps_typed_scalar_metadata_and_rejects_invalid_input() {
        let empty = GraphForge::new(None).unwrap();
        for label in [None, Some("Missing")] {
            let batch = empty
                .analyze(label, chromatic_number_options(None))
                .unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "chromatic_number");
            assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt64);
            assert!(!batch.schema().field(0).is_nullable());
            assert_eq!(batch.column(0).null_count(), 0);
            assert_eq!(
                batch.schema().metadata()["graphforge.algorithm"],
                "chromatic_number"
            );
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                0
            );
        }

        let populated = GraphForge::new(None).unwrap();
        add_person(&populated, "Alice");
        let missing = populated
            .analyze(Some("Missing"), chromatic_number_options(None))
            .unwrap();
        assert_eq!(missing.num_rows(), 1);
        assert_eq!(missing.schema().field(0).data_type(), &DataType::UInt64);
        assert!(!missing.schema().field(0).is_nullable());
        assert_eq!(missing.column(0).null_count(), 0);
        assert_eq!(
            missing.schema().metadata()["graphforge.algorithm"],
            "chromatic_number"
        );
        assert_eq!(
            missing
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            0
        );

        let looped = GraphForge::new(None).unwrap();
        add_person(&looped, "Alice");
        looped
            .execute("MATCH (a:Person {name:'Alice'}) CREATE (a)-[:ROAD]->(a)")
            .unwrap();
        assert!(matches!(
            looped.analyze(None, chromatic_number_options(Some("ROAD"))),
            Err(GfError::Execution(message))
                if message.contains("undefined for a graph containing a self-loop")
        ));

        for options in [
            AnalyzeOptions {
                directed: true,
                ..chromatic_number_options(None)
            },
            AnalyzeOptions {
                weight: Some("cost".into()),
                ..chromatic_number_options(None)
            },
            chromatic_number_options(Some(" ")),
        ] {
            assert!(matches!(
                empty.analyze(None, options),
                Err(GfError::Validation(_))
            ));
        }
    }

    #[test]
    fn find_cycles_is_uuid_only_canonical_and_direction_aware() {
        let graph = GraphForge::new(None).unwrap();
        let nodes =
            ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Fox'}) \
                 CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), \
                 (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(d), \
                 (e)-[:ROAD]->(f), (a)-[:OTHER]->(d), (d)-[:OTHER]->(a)",
            )
            .unwrap();

        let cycle_rows = |batch: &arrow::record_batch::RecordBatch| {
            let lists = batch
                .column_by_name("cycle")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(lists.null_count(), 0);
            (0..lists.len())
                .map(|row| {
                    let values = lists.value(row);
                    let values = values
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap();
                    assert_eq!(values.null_count(), 0);
                    (0..values.len())
                        .map(|index| values.value(index).try_into().unwrap())
                        .collect::<Vec<[u8; 16]>>()
                })
                .collect::<Vec<_>>()
        };
        let canonical = |cycle: Vec<[u8; 16]>, directed: bool| {
            let rotations = |values: &[[u8; 16]]| {
                (0..values.len())
                    .map(|offset| {
                        values[offset..]
                            .iter()
                            .chain(&values[..offset])
                            .copied()
                            .collect::<Vec<_>>()
                    })
                    .min()
                    .unwrap()
            };
            let forward = rotations(&cycle);
            if directed || cycle.len() < 2 {
                forward
            } else {
                let reversed = cycle.into_iter().rev().collect::<Vec<_>>();
                forward.min(rotations(&reversed))
            }
        };
        let uuid = |index: usize| *nodes[index].uuid.as_bytes();

        let directed_options = find_cycles_options(true, Some("ROAD"));
        let directed = graph
            .analyze(Some("Person"), directed_options.clone())
            .unwrap();
        assert_eq!(directed.num_columns(), 1);
        let directed_schema = directed.schema();
        let field = directed_schema.field(0);
        assert_eq!(field.name(), "cycle");
        assert!(!field.is_nullable());
        let DataType::List(item) = field.data_type() else {
            panic!("cycle must be a List");
        };
        assert_eq!(item.data_type(), &DataType::FixedSizeBinary(16));
        assert!(!item.is_nullable());
        assert_eq!(
            directed_schema.metadata()["graphforge.algorithm"],
            "find_cycles"
        );
        assert_eq!(directed_schema.metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            directed_schema.metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        let mut expected_directed = vec![
            canonical(vec![uuid(0), uuid(1), uuid(2)], true),
            canonical(vec![uuid(1), uuid(3)], true),
            vec![uuid(3)],
        ];
        expected_directed.sort();
        assert_eq!(cycle_rows(&directed), expected_directed);
        assert_eq!(
            directed,
            graph.analyze(Some("Person"), directed_options).unwrap()
        );

        let undirected = graph
            .analyze(Some("Person"), find_cycles_options(false, Some("ROAD")))
            .unwrap();
        let mut expected_undirected = vec![
            canonical(vec![uuid(0), uuid(1), uuid(2)], false),
            vec![uuid(3)],
        ];
        expected_undirected.sort();
        assert_eq!(cycle_rows(&undirected), expected_undirected);
    }

    #[test]
    fn find_cycles_empty_schema_and_option_validation_are_stable() {
        let graph = GraphForge::new(None).unwrap();
        let empty = graph
            .analyze(Some("Missing"), find_cycles_options(true, None))
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.num_columns(), 1);
        let empty_schema = empty.schema();
        assert_eq!(empty_schema.field(0).name(), "cycle");
        assert!(!empty_schema.field(0).is_nullable());
        let DataType::List(item) = empty_schema.field(0).data_type() else {
            panic!("cycle must be a List");
        };
        assert_eq!(item.data_type(), &DataType::FixedSizeBinary(16));
        assert!(!item.is_nullable());
        assert_eq!(
            empty_schema.metadata()["graphforge.algorithm"],
            "find_cycles"
        );
        assert_eq!(empty_schema.metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            empty_schema.metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_eq!(
            empty
                .column_by_name("cycle")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .null_count(),
            0
        );

        let mut weighted = find_cycles_options(true, None);
        weighted.weight = Some("cost".into());
        for result in [
            graph.analyze(None, weighted),
            graph.analyze(None, find_cycles_options(true, Some(" "))),
            graph.analyze(Some(""), find_cycles_options(true, None)),
            graph.analyze(Some(" Person"), find_cycles_options(false, None)),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn articulation_points_is_uuid_only_multigraph_safe_and_deterministic() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus", "Hal"]
            .map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Fox'}), \
                 (g:Person {name:'Gus'}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(e), \
                 (d)-[:ROAD]->(d), (f)-[:ROAD]->(g), (a)-[:OTHER]->(e)",
            )
            .unwrap();
        let options = articulation_points_options(Some("ROAD"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "node_uuid");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "articulation_points"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        let values = batch
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(values.null_count(), 0);
        let mut expected = [*nodes[1].uuid.as_bytes(), *nodes[3].uuid.as_bytes()];
        expected.sort_unstable();
        assert_eq!(
            (0..values.len())
                .map(|row| <[u8; 16]>::try_from(values.value(row)).unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

        let no_articulation = graph
            .analyze(Some("Person"), articulation_points_options(Some("OTHER")))
            .unwrap();
        assert_eq!(no_articulation.num_rows(), 0);
        assert_eq!(no_articulation.schema(), batch.schema());
        assert_eq!(
            graph
                .analyze(Some("Missing"), articulation_points_options(Some("ROAD")))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn articulation_points_rejects_directed_weight_and_invalid_selectors() {
        let graph = GraphForge::new(None).unwrap();
        let mut directed = articulation_points_options(None);
        directed.directed = true;
        let mut weighted = articulation_points_options(None);
        weighted.weight = Some("cost".into());
        for result in [
            graph.analyze(None, directed),
            graph.analyze(None, weighted),
            graph.analyze(None, articulation_points_options(Some(" "))),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn bridges_is_uuid_only_multigraph_safe_and_deterministic() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus", "Hal"]
            .map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Fox'}), \
                 (g:Person {name:'Gus'}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(e), \
                 (d)-[:ROAD]->(d), (f)-[:ROAD]->(g), (a)-[:OTHER]->(e)",
            )
            .unwrap();
        let options = bridges_options(Some("ROAD"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();

        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", &DataType::FixedSizeBinary(16), false),
                ("source_uuid", &DataType::FixedSizeBinary(16), false),
                ("target_uuid", &DataType::FixedSizeBinary(16), false),
            ]
        );
        assert_eq!(batch.schema().metadata()["graphforge.algorithm"], "bridges");
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(batch.num_rows(), 2);
        let edge_uuids = batch
            .column_by_name("edge_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let sources = batch
            .column_by_name("source_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let targets = batch
            .column_by_name("target_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(edge_uuids.null_count(), 0);
        for row in 0..batch.num_rows() {
            assert!(sources.value(row) < targets.value(row));
            if row > 0 {
                assert!(
                    (
                        sources.value(row - 1),
                        targets.value(row - 1),
                        edge_uuids.value(row - 1)
                    ) < (
                        sources.value(row),
                        targets.value(row),
                        edge_uuids.value(row)
                    )
                );
            }
        }
        let expected_endpoints = [(3, 4), (5, 6)]
            .map(|(left, right)| {
                let mut pair = [*nodes[left].uuid.as_bytes(), *nodes[right].uuid.as_bytes()];
                pair.sort_unstable();
                pair
            })
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let actual_endpoints = (0..batch.num_rows())
            .map(|row| {
                [
                    <[u8; 16]>::try_from(sources.value(row)).unwrap(),
                    <[u8; 16]>::try_from(targets.value(row)).unwrap(),
                ]
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(actual_endpoints, expected_endpoints);
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

        for selection in [
            graph.analyze(Some("Person"), bridges_options(Some("MISSING"))),
            graph.analyze(Some("Missing"), bridges_options(Some("ROAD"))),
            GraphForge::new(None)
                .unwrap()
                .analyze(None, bridges_options(None)),
        ] {
            let empty = selection.unwrap();
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(empty.schema(), batch.schema());
        }
    }

    #[test]
    fn bridges_rejects_directed_weight_and_invalid_selectors() {
        let graph = GraphForge::new(None).unwrap();
        let mut directed = bridges_options(None);
        directed.directed = true;
        let mut weighted = bridges_options(None);
        weighted.weight = Some("cost".into());
        for result in [
            graph.analyze(None, directed),
            graph.analyze(None, weighted),
            graph.analyze(None, bridges_options(Some(" "))),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn is_dag_obeys_schema_label_via_direction_and_edge_contracts() {
        assert!(AnalyzeOptions::default().directed);
        let graph = GraphForge::new(None).unwrap();
        for (label, name) in [
            ("Person", "Alice"),
            ("Person", "Bob"),
            ("Person", "Carol"),
            ("Animal", "Fox"),
            ("Animal", "Wolf"),
        ] {
            graph
                .add_node(
                    label,
                    &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
                )
                .unwrap();
        }
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (f:Animal {name:'Fox'}), \
                 (w:Animal {name:'Wolf'}) \
                 CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(c), (f)-[:OTHER]->(w), (w)-[:OTHER]->(f)",
            )
            .unwrap();

        let global = graph.analyze(None, AnalyzeOptions::default()).unwrap();
        assert_eq!(global.num_rows(), 1);
        assert_eq!(global.num_columns(), 1);
        assert_eq!(global.schema().field(0).name(), "is_dag");
        assert_eq!(global.schema().field(0).data_type(), &DataType::Boolean);
        assert!(!global.schema().field(0).is_nullable());
        assert_eq!(global.schema().metadata()["graphforge.algorithm"], "is_dag");
        assert_eq!(global.schema().metadata()["graphforge.verb"], "analyze");
        assert!(!is_dag_value(&global));
        assert_eq!(
            global,
            graph.analyze(None, AnalyzeOptions::default()).unwrap()
        );

        assert!(is_dag_value(
            &graph
                .analyze(Some("Person"), AnalyzeOptions::default())
                .unwrap()
        ));
        assert!(is_dag_value(
            &graph
                .analyze(None, is_dag_options(true, Some("KNOWS")))
                .unwrap()
        ));
        assert!(!is_dag_value(
            &graph
                .analyze(Some("Person"), is_dag_options(false, None))
                .unwrap()
        ));
    }

    #[test]
    fn is_dag_handles_empty_self_loop_and_invalid_inputs() {
        let empty = GraphForge::new(None).unwrap();
        assert!(is_dag_value(
            &empty.analyze(None, AnalyzeOptions::default()).unwrap()
        ));
        assert!(is_dag_value(
            &empty
                .analyze(Some("Missing"), AnalyzeOptions::default())
                .unwrap()
        ));

        let looped = GraphForge::new(None).unwrap();
        add_person(&looped, "Alice");
        looped
            .execute("MATCH (a:Person {name:'Alice'}) CREATE (a)-[:KNOWS]->(a)")
            .unwrap();
        assert!(!is_dag_value(
            &looped.analyze(None, AnalyzeOptions::default()).unwrap()
        ));

        for result in [
            empty.analyze(Some(""), AnalyzeOptions::default()),
            empty.analyze(Some(" Person"), AnalyzeOptions::default()),
            empty.analyze(None, is_dag_options(true, Some(" "))),
            empty.analyze(
                None,
                AnalyzeOptions {
                    by: AnalyzeAlgorithm::MinimumSpanningTree,
                    ..AnalyzeOptions::default()
                },
            ),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn euler_path_persisted_e2e_is_uuid_exact_directed_and_undirected() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let nodes = ["Alice", "Bob", "Carol"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}) \
                 CREATE (a)-[:TRAIL]->(b), (b)-[:TRAIL]->(b), (b)-[:TRAIL]->(c)",
            )
            .unwrap();
        drop(graph);

        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let relationships = relationship_rows(&graph, "TRAIL");
        let mut expected_edges = relationships.iter().map(|row| row.0).collect::<Vec<_>>();
        expected_edges.sort_unstable();
        for directed in [true, false] {
            let options = euler_options(AnalyzeAlgorithm::EulerPath, directed, Some("TRAIL"));
            let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
            assert_euler_schema(&batch, "euler_path");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

            let node_path = euler_uuid_list(&batch, "node_path");
            let edge_path = euler_uuid_list(&batch, "edge_path");
            assert_eq!(
                node_path,
                vec![
                    *nodes[0].uuid.as_bytes(),
                    *nodes[1].uuid.as_bytes(),
                    *nodes[1].uuid.as_bytes(),
                    *nodes[2].uuid.as_bytes(),
                ]
            );
            assert_eq!(edge_path, expected_edges);
            assert_euler_edge_alignment(&node_path, &edge_path, &relationships, directed);
        }
    }

    #[test]
    fn euler_circuit_persisted_e2e_preserves_loops_parallel_edges_and_start() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let nodes = ["Alice", "Bob"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:TRAIL]->(b), (a)-[:TRAIL]->(b), (a)-[:TRAIL]->(a)",
            )
            .unwrap();
        drop(graph);

        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let options = euler_options(AnalyzeAlgorithm::EulerCircuit, false, Some("TRAIL"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_euler_schema(&batch, "euler_circuit");
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
        let node_path = euler_uuid_list(&batch, "node_path");
        let edge_path = euler_uuid_list(&batch, "edge_path");
        let relationships = relationship_rows(&graph, "TRAIL");
        let lowest = nodes
            .iter()
            .map(|node| *node.uuid.as_bytes())
            .min()
            .unwrap();
        assert_eq!(node_path.first(), Some(&lowest));
        assert_eq!(node_path.last(), Some(&lowest));
        assert_euler_edge_alignment(&node_path, &edge_path, &relationships, false);
        let canonical_first = relationships
            .iter()
            .filter(|(_, source, target)| *source == lowest || *target == lowest)
            .map(|(edge, _, _)| *edge)
            .min()
            .unwrap();
        assert_eq!(edge_path.first(), Some(&canonical_first));

        let directed = GraphForge::new(None).unwrap();
        let directed_nodes = ["A", "B"].map(|name| add_person(&directed, name));
        directed
            .execute(
                "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) \
                 CREATE (a)-[:ARC]->(b), (b)-[:ARC]->(a), (a)-[:ARC]->(a)",
            )
            .unwrap();
        let directed_options = euler_options(AnalyzeAlgorithm::EulerCircuit, true, Some("ARC"));
        let directed_batch = directed
            .analyze(Some("Person"), directed_options.clone())
            .unwrap();
        assert_euler_schema(&directed_batch, "euler_circuit");
        assert_eq!(
            directed_batch,
            directed.analyze(Some("Person"), directed_options).unwrap()
        );
        let directed_path = euler_uuid_list(&directed_batch, "node_path");
        let directed_edges = euler_uuid_list(&directed_batch, "edge_path");
        let directed_relationships = relationship_rows(&directed, "ARC");
        let directed_lowest = directed_nodes
            .iter()
            .map(|node| *node.uuid.as_bytes())
            .min()
            .unwrap();
        assert_eq!(directed_path.first(), Some(&directed_lowest));
        assert_eq!(directed_path.last(), Some(&directed_lowest));
        assert_euler_edge_alignment(
            &directed_path,
            &directed_edges,
            &directed_relationships,
            true,
        );
        let canonical_first = directed_relationships
            .iter()
            .filter(|(_, source, _)| *source == directed_lowest)
            .map(|(edge, _, _)| *edge)
            .min()
            .unwrap();
        assert_eq!(directed_edges.first(), Some(&canonical_first));
    }

    #[test]
    fn euler_construction_boundaries_and_undefined_results_are_typed() {
        let empty = GraphForge::new(None).unwrap();
        for by in [AnalyzeAlgorithm::EulerCircuit, AnalyzeAlgorithm::EulerPath] {
            assert_eq!(
                empty
                    .analyze(None, euler_options(by, false, None))
                    .unwrap()
                    .num_rows(),
                0
            );
            let edgeless = GraphForge::new(None).unwrap();
            let isolated = add_person(&edgeless, "Isolated");
            let batch = edgeless
                .analyze(Some("Person"), euler_options(by, false, None))
                .unwrap();
            assert_eq!(
                euler_uuid_list(&batch, "node_path"),
                vec![*isolated.uuid.as_bytes()]
            );
            assert!(euler_uuid_list(&batch, "edge_path").is_empty());
        }

        let circuit_undefined = GraphForge::new(None).unwrap();
        circuit_undefined
            .execute("CREATE (:Person)-[:TRAIL]->(:Person)")
            .unwrap();
        assert!(matches!(
            circuit_undefined.analyze(
                Some("Person"),
                euler_options(AnalyzeAlgorithm::EulerCircuit, false, Some("TRAIL")),
            ),
            Err(GfError::Execution(message))
                if message == "Euler circuit is undefined for the selected graph"
        ));

        let path_undefined = GraphForge::new(None).unwrap();
        path_undefined
            .execute(
                "CREATE (a:Person), (b:Person), (c:Person), (d:Person) \
                 CREATE (a)-[:TRAIL]->(b), (a)-[:TRAIL]->(c), (a)-[:TRAIL]->(d)",
            )
            .unwrap();
        assert!(matches!(
            path_undefined.analyze(
                Some("Person"),
                euler_options(AnalyzeAlgorithm::EulerPath, false, Some("TRAIL")),
            ),
            Err(GfError::Execution(message))
                if message == "Euler path is undefined for the selected graph"
        ));
    }

    #[test]
    fn has_euler_circuit_returns_typed_boolean_for_undirected_and_directed_graphs() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        for name in ["Alice", "Bob", "Carol", "Isolate"] {
            add_person(&graph, name);
        }
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}) \
                 CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a)",
            )
            .unwrap();

        let undirected = graph
            .analyze(
                Some("Person"),
                has_euler_circuit_options(false, Some("KNOWS")),
            )
            .unwrap();
        assert_eq!(undirected.num_rows(), 1);
        assert_eq!(undirected.num_columns(), 1);
        assert_eq!(undirected.schema().field(0).data_type(), &DataType::Boolean);
        assert!(!undirected.schema().field(0).is_nullable());
        assert_eq!(
            undirected.schema().metadata()["graphforge.algorithm"],
            "has_euler_circuit"
        );
        assert!(has_euler_circuit_value(&undirected));

        assert!(has_euler_circuit_value(
            &graph
                .analyze(
                    Some("Person"),
                    has_euler_circuit_options(true, Some("KNOWS")),
                )
                .unwrap()
        ));

        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:PATH]->(b)",
            )
            .unwrap();
        assert!(!has_euler_circuit_value(
            &graph
                .analyze(None, has_euler_circuit_options(false, Some("PATH")))
                .unwrap()
        ));
        assert!(!has_euler_circuit_value(
            &graph
                .analyze(None, has_euler_circuit_options(true, Some("PATH")))
                .unwrap()
        ));
    }

    #[test]
    fn has_euler_circuit_empty_and_missing_selection_are_non_null_true() {
        let graph = GraphForge::new(None).unwrap();
        for batch in [
            graph
                .analyze(None, has_euler_circuit_options(false, None))
                .unwrap(),
            graph
                .analyze(
                    Some("Missing"),
                    has_euler_circuit_options(true, Some("MISSING")),
                )
                .unwrap(),
        ] {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.schema().field(0).data_type(), &DataType::Boolean);
            assert!(!batch.schema().field(0).is_nullable());
            assert!(has_euler_circuit_value(&batch));
        }
    }

    #[test]
    fn has_euler_path_returns_typed_boolean_for_undirected_and_directed_graphs() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        for name in ["Alice", "Bob", "Carol", "Dan", "Isolate"] {
            add_person(&graph, name);
        }
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}) \
                 CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)",
            )
            .unwrap();

        let undirected = graph
            .analyze(Some("Person"), has_euler_path_options(false, Some("KNOWS")))
            .unwrap();
        assert_eq!(undirected.num_rows(), 1);
        assert_eq!(undirected.num_columns(), 1);
        assert_eq!(undirected.schema().field(0).name(), "has_euler_path");
        assert_eq!(undirected.schema().field(0).data_type(), &DataType::Boolean);
        assert!(!undirected.schema().field(0).is_nullable());
        assert_eq!(
            undirected.schema().metadata()["graphforge.algorithm"],
            "has_euler_path"
        );
        assert_eq!(undirected.schema().metadata()["graphforge.verb"], "analyze");
        assert!(has_euler_path_value(&undirected));
        assert_eq!(
            undirected,
            graph
                .analyze(Some("Person"), has_euler_path_options(false, Some("KNOWS")),)
                .unwrap()
        );

        assert!(has_euler_path_value(
            &graph
                .analyze(Some("Person"), has_euler_path_options(true, Some("KNOWS")),)
                .unwrap()
        ));

        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:STAR]->(b), (a)-[:STAR]->(c), (a)-[:STAR]->(d)",
            )
            .unwrap();
        assert!(!has_euler_path_value(
            &graph
                .analyze(None, has_euler_path_options(false, Some("STAR")))
                .unwrap()
        ));
        assert!(!has_euler_path_value(
            &graph
                .analyze(None, has_euler_path_options(true, Some("STAR")))
                .unwrap()
        ));
    }

    #[test]
    fn has_euler_path_empty_and_missing_selection_are_non_null_true() {
        let graph = GraphForge::new(None).unwrap();
        for batch in [
            graph
                .analyze(None, has_euler_path_options(false, None))
                .unwrap(),
            graph
                .analyze(
                    Some("Missing"),
                    has_euler_path_options(true, Some("MISSING")),
                )
                .unwrap(),
        ] {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).data_type(), &DataType::Boolean);
            assert!(!batch.schema().field(0).is_nullable());
            assert!(has_euler_path_value(&batch));
        }
    }

    #[test]
    fn topological_sort_returns_exact_uuid_order_schema_and_projection() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        let mut people = ["Alice", "Bob", "Carol", "Dan"]
            .map(|name| (name, add_person(&graph, name)))
            .to_vec();
        people.sort_unstable_by_key(|(_, node)| *node.uuid.as_bytes());
        for name in ["Fox", "Wolf"] {
            graph
                .add_node(
                    "Animal",
                    &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
                )
                .unwrap();
        }
        graph
            .execute(&format!(
                "MATCH (a:Person {{name:'{}'}}), (b:Person {{name:'{}'}}), \
                 (c:Person {{name:'{}'}}), (f:Animal {{name:'Fox'}}), \
                 (w:Animal {{name:'Wolf'}}) \
                 CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), \
                 (b)-[:KNOWS]->(c), (f)-[:OTHER]->(w), (w)-[:OTHER]->(f)",
                people[0].0, people[1].0, people[2].0
            ))
            .unwrap();

        let options = topological_sort_options(true, None);
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("node_uuid", &DataType::FixedSizeBinary(16), false),
                ("order", &DataType::UInt64, false),
            ]
        );
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "topological_sort"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert!(batch.column_by_name("node_id").is_none());
        assert_eq!(
            topological_rows(&batch),
            people
                .iter()
                .enumerate()
                .map(|(order, (_, node))| (*node.uuid.as_bytes(), u64::try_from(order).unwrap()))
                .collect::<Vec<_>>()
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

        let via = graph
            .analyze(None, topological_sort_options(true, Some("KNOWS")))
            .unwrap();
        assert_eq!(via.num_rows(), 6);
        assert_eq!(
            via.column_by_name("order")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[0, 1, 2, 3, 4, 5]
        );
        assert!(matches!(
            graph.analyze(None, topological_sort_options(true, None)),
            Err(GfError::Execution(message))
                if message == "Rust algorithm execution failed: selected graph contains a cycle"
        ));
    }

    #[test]
    fn topological_sort_handles_empty_self_loop_and_invalid_options() {
        let empty = GraphForge::new(None).unwrap();
        let empty_batch = empty
            .analyze(None, topological_sort_options(true, None))
            .unwrap();
        assert_eq!(empty_batch.num_rows(), 0);
        assert_eq!(
            empty_batch.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(empty_batch.schema().field(1).data_type(), &DataType::UInt64);
        assert_eq!(
            empty
                .analyze(Some("Missing"), topological_sort_options(true, None))
                .unwrap()
                .schema(),
            empty_batch.schema()
        );

        let looped = GraphForge::new(None).unwrap();
        add_person(&looped, "Alice");
        looped
            .execute("MATCH (a:Person {name:'Alice'}) CREATE (a)-[:KNOWS]->(a)")
            .unwrap();
        assert!(matches!(
            looped.analyze(None, topological_sort_options(true, None)),
            Err(GfError::Execution(message))
                if message == "Rust algorithm execution failed: selected graph contains a cycle"
        ));

        assert!(matches!(
            empty.analyze(None, topological_sort_options(false, None)),
            Err(GfError::Validation(message))
                if message == "topological_sort requires directed=true"
        ));
        assert!(matches!(
            empty.analyze(
                None,
                AnalyzeOptions {
                    weight: Some("cost".into()),
                    ..topological_sort_options(true, None)
                }
            ),
            Err(GfError::Validation(message))
                if message == "topological_sort does not accept an edge weight property"
        ));
        for result in [
            empty.analyze(Some(""), topological_sort_options(true, None)),
            empty.analyze(None, topological_sort_options(true, Some(" "))),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn dag_longest_path_returns_exact_uuid_path_schema_and_isolation() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        let nodes =
            ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| (name, add_person(&graph, name)));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), \
                 (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), \
                 (a)-[:OTHER]->(e)",
            )
            .unwrap();

        let options = dag_longest_path_options(true, Some("KNOWS"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("cost", &DataType::Float64, false),
                (
                    "path",
                    &DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::FixedSizeBinary(16),
                        false,
                    ))),
                    false,
                ),
            ]
        );
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "dag_longest_path"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        let costs = batch
            .column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(costs.null_count(), 0);
        assert_eq!(costs.values(), &[2.0]);
        let alice = &nodes[0].1;
        let bob = &nodes[1].1;
        let carol = &nodes[2].1;
        let dan = &nodes[3].1;
        let expected_middle = if bob.uuid < carol.uuid { bob } else { carol };
        assert_eq!(
            uuid_path(&batch, 0),
            [
                *alice.uuid.as_bytes(),
                *expected_middle.uuid.as_bytes(),
                *dan.uuid.as_bytes(),
            ]
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
    }

    #[test]
    fn dag_longest_path_handles_typed_empty_cycle_and_invalid_options() {
        let empty = GraphForge::new(None).unwrap();
        let batch = empty
            .analyze(None, dag_longest_path_options(true, None))
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        let costs = batch
            .column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(costs.null_count(), 0);
        assert_eq!(costs.value(0), 0.0);
        let paths = batch
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(paths.null_count(), 0);
        assert!(uuid_path(&batch, 0).is_empty());
        assert_eq!(
            empty
                .analyze(Some("Missing"), dag_longest_path_options(true, None))
                .unwrap()
                .schema(),
            batch.schema()
        );

        let cyclic = GraphForge::new(None).unwrap();
        for name in ["Alice", "Bob"] {
            add_person(&cyclic, name);
        }
        cyclic
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a)",
            )
            .unwrap();
        assert!(matches!(
            cyclic.analyze(None, dag_longest_path_options(true, None)),
            Err(GfError::Execution(message))
                if message.contains("dag_longest_path requires a directed acyclic graph")
        ));
        assert!(matches!(
            empty.analyze(None, dag_longest_path_options(false, None)),
            Err(GfError::Validation(message))
                if message == "dag_longest_path requires directed=true"
        ));
        assert!(matches!(
            empty.analyze(
                None,
                AnalyzeOptions {
                    weight: Some("cost".into()),
                    ..dag_longest_path_options(true, None)
                }
            ),
            Err(GfError::Validation(message))
                if message == "dag_longest_path does not accept an edge weight property"
        ));
    }

    #[test]
    fn weighted_dag_longest_path_is_exact_typed_and_knowledge_independent() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        let nodes =
            ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| (name, add_person(&graph, name)));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:ROAD {cost:2.0}]->(b), \
                 (b)-[:ROAD {cost:3.0}]->(d), \
                 (a)-[:ROAD {cost:2.0}]->(c), \
                 (c)-[:ROAD {cost:3.0}]->(d), \
                 (e)-[:ROAD {cost:-8.0}]->(d), \
                 (a)-[:OTHER {cost:100.0}]->(d)",
            )
            .unwrap();

        let options = weighted_dag_longest_path_options(true, Some("ROAD"), Some("cost"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("cost", &DataType::Float64, false),
                (
                    "path",
                    &DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::FixedSizeBinary(16),
                        false,
                    ))),
                    false,
                ),
            ]
        );
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "dag_longest_path_weighted"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        let costs = batch
            .column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(costs.null_count(), 0);
        assert_eq!(costs.value(0), 5.0);
        let alice = &nodes[0].1;
        let bob = &nodes[1].1;
        let carol = &nodes[2].1;
        let dan = &nodes[3].1;
        let expected_middle = if bob.uuid < carol.uuid { bob } else { carol };
        assert_eq!(
            uuid_path(&batch, 0),
            [
                *alice.uuid.as_bytes(),
                *expected_middle.uuid.as_bytes(),
                *dan.uuid.as_bytes(),
            ]
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
    }

    #[test]
    fn weighted_dag_longest_path_handles_empty_cycle_and_strict_weights() {
        let empty = GraphForge::new(None).unwrap();
        let options = weighted_dag_longest_path_options(true, None, Some("cost"));
        let batch = empty.analyze(None, options).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let costs = batch
            .column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(costs.null_count(), 0);
        assert_eq!(costs.value(0), 0.0);
        let paths = batch
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(paths.null_count(), 0);
        assert!(uuid_path(&batch, 0).is_empty());

        let cyclic = GraphForge::new(None).unwrap();
        for name in ["Alice", "Bob"] {
            add_person(&cyclic, name);
        }
        cyclic
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(b), \
                 (b)-[:ROAD {cost:1.0}]->(a)",
            )
            .unwrap();
        assert!(matches!(
            cyclic.analyze(
                None,
                weighted_dag_longest_path_options(true, Some("ROAD"), Some("cost"))
            ),
            Err(GfError::Execution(message))
                if message.contains("requires a directed acyclic graph")
        ));
        for options in [
            weighted_dag_longest_path_options(false, None, Some("cost")),
            weighted_dag_longest_path_options(true, None, None),
            weighted_dag_longest_path_options(true, None, Some(" ")),
            weighted_dag_longest_path_options(true, Some("ROAD"), Some("missing")),
        ] {
            assert!(matches!(
                cyclic.analyze(None, options),
                Err(GfError::Validation(_))
            ));
        }
    }

    #[test]
    fn edge_coloring_is_uuid_only_deterministic_and_knowledge_independent() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        for name in ["Alice", "Bob", "Fox"] {
            add_person(&graph, name);
        }
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (f:Person {name:'Fox'}) \
                 CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(a), (a)-[:OTHER]->(f)",
            )
            .unwrap();

        let options = edge_coloring_options(Some("KNOWS"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("edge_uuid", &DataType::FixedSizeBinary(16), false),
                ("color", &DataType::UInt64, false),
            ]
        );
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "edge_coloring"
        );
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
        assert!(batch.column_by_name("edge_id").is_none());

        let rows = edge_color_rows(&batch);
        assert!(rows.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert_eq!(
            rows.iter().map(|(_, color)| *color).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
        assert_eq!(
            graph
                .analyze(Some("Person"), edge_coloring_options(Some("MISSING")))
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn edge_coloring_handles_typed_empty_loops_and_invalid_options() {
        let empty = GraphForge::new(None).unwrap();
        let batch = empty.analyze(None, edge_coloring_options(None)).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(
            empty
                .analyze(Some("Missing"), edge_coloring_options(None))
                .unwrap()
                .schema(),
            batch.schema()
        );
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
        assert!(!batch.schema().field(0).is_nullable());
        assert!(!batch.schema().field(1).is_nullable());
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "edge_coloring"
        );

        let looped = GraphForge::new(None).unwrap();
        add_person(&looped, "Alice");
        looped
            .execute("MATCH (a:Person {name:'Alice'}) CREATE (a)-[:KNOWS]->(a)")
            .unwrap();
        assert!(matches!(
            looped.analyze(None, edge_coloring_options(Some("KNOWS"))),
            Err(GfError::Execution(message))
                if message.contains("edge_coloring cannot color a graph containing a self-loop")
        ));

        for options in [
            AnalyzeOptions {
                directed: true,
                ..edge_coloring_options(None)
            },
            AnalyzeOptions {
                weight: Some("cost".into()),
                ..edge_coloring_options(None)
            },
            edge_coloring_options(Some(" ")),
        ] {
            assert!(matches!(
                empty.analyze(None, options),
                Err(GfError::Validation(_))
            ));
        }
    }

    #[test]
    fn node_similarity_obeys_uuid_jaccard_top_k_via_and_order_contracts() {
        assert_eq!(SimilarOptions::default().k, 10);
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:KNOWS]->(d), (a)-[:KNOWS]->(e), \
                 (a)-[:KNOWS]->(e), (b)-[:KNOWS]->(d), \
                 (b)-[:KNOWS]->(e), (c)-[:KNOWS]->(d), \
                 (a)-[:OTHER]->(d), (c)-[:OTHER]->(d)",
            )
            .unwrap();

        let batch = graph
            .similar("Person", node_similarity_options(2, Some("KNOWS")))
            .unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.data_type()))
                .collect::<Vec<_>>(),
            [
                ("node1_uuid", &DataType::FixedSizeBinary(16)),
                ("node2_uuid", &DataType::FixedSizeBinary(16)),
                ("similarity", &DataType::Float64),
            ]
        );
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "node_similarity"
        );
        assert!(batch.column_by_name("node1_id").is_none());
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let right = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let expected_pairs = [(0, 1), (0, 2), (1, 0), (1, 2), (2, 0), (2, 1)];
        for (row, (source, target)) in expected_pairs.into_iter().enumerate() {
            assert_eq!(left.value(row), nodes[source].uuid.as_bytes());
            assert_eq!(right.value(row), nodes[target].uuid.as_bytes());
        }
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[1.0, 0.5, 1.0, 0.5, 0.5, 0.5]
        );
        assert_eq!(
            batch,
            graph
                .similar("Person", node_similarity_options(2, Some("KNOWS")))
                .unwrap()
        );
        assert_eq!(
            graph
                .similar("Person", node_similarity_options(1, Some("KNOWS")))
                .unwrap()
                .num_rows(),
            3
        );
        let other = graph
            .similar("Person", node_similarity_options(10, Some("OTHER")))
            .unwrap();
        assert_eq!(other.num_rows(), 2);
        assert_eq!(
            other
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[1.0, 1.0]
        );
    }

    #[test]
    fn node_similarity_empty_and_invalid_inputs_are_structured() {
        let graph = GraphForge::new(None).unwrap();
        let empty = graph
            .similar("Person", node_similarity_options(10, None))
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema().field(2).data_type(), &DataType::Float64);

        let mut vector = node_similarity_options(10, None);
        vector.vector_property = Some("embedding".into());
        for result in [
            graph.similar("", node_similarity_options(10, None)),
            graph.similar("Person", node_similarity_options(10, Some(" "))),
            graph.similar("Person", node_similarity_options(0, None)),
            graph.similar("Person", vector),
            graph.similar(
                "Person",
                SimilarOptions {
                    by: SimilarAlgorithm::Knn,
                    ..SimilarOptions::default()
                },
            ),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn filtered_node_similarity_filters_candidates_and_shapes_uuid_jaccard() {
        let graph = GraphForge::new(None).unwrap();
        let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
        graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:KNOWS]->(a), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), \
                 (a)-[:KNOWS]->(d), (b)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(c)",
            )
            .unwrap();

        let batch = graph
            .similar("Person", filtered_node_similarity_options(2, Some("KNOWS")))
            .unwrap();
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "filtered_node_similarity"
        );
        assert!(
            batch
                .schema()
                .fields()
                .iter()
                .all(|field| !field.is_nullable())
        );
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.data_type())
                .collect::<Vec<_>>(),
            [
                &DataType::FixedSizeBinary(16),
                &DataType::FixedSizeBinary(16),
                &DataType::Float64
            ]
        );
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let right = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (row, (source, target)) in [(0, 1), (0, 2), (1, 0), (1, 2)].into_iter().enumerate() {
            assert_eq!(left.value(row), nodes[source].uuid.as_bytes());
            assert_eq!(right.value(row), nodes[target].uuid.as_bytes());
        }
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values(),
            &[0.75, 0.25, 0.75, 1.0 / 3.0]
        );
        assert_eq!(
            batch,
            graph
                .similar("Person", filtered_node_similarity_options(2, Some("KNOWS")))
                .unwrap()
        );
        assert_eq!(
            graph
                .similar("Person", filtered_node_similarity_options(1, Some("KNOWS")))
                .unwrap()
                .num_rows(),
            2
        );
        assert_eq!(
            graph
                .similar("Person", filtered_node_similarity_options(10, None))
                .unwrap()
                .num_rows(),
            6
        );
        assert_eq!(
            graph
                .similar(
                    "Person",
                    filtered_node_similarity_options(10, Some("MISSING"))
                )
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[test]
    fn filtered_node_similarity_boundaries_and_validation_are_structured() {
        let graph = GraphForge::new(None).unwrap();
        assert_eq!(
            graph
                .similar("Person", filtered_node_similarity_options(10, None))
                .unwrap()
                .num_rows(),
            0
        );
        add_person(&graph, "Alice");
        assert_eq!(
            graph
                .similar("Person", filtered_node_similarity_options(10, None))
                .unwrap()
                .num_rows(),
            0
        );

        let mut vector = filtered_node_similarity_options(10, None);
        vector.vector_property = Some("embedding".into());
        for result in [
            graph.similar("", filtered_node_similarity_options(10, None)),
            graph.similar("Person", filtered_node_similarity_options(10, Some(" "))),
            graph.similar("Person", filtered_node_similarity_options(0, None)),
            graph.similar("Person", vector),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn knn_obeys_uuid_cosine_top_k_schema_and_topology_independence() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), \
                 (:Person {name:'b', embedding:[1.0, 0.0]}), \
                 (:Person {name:'c', embedding:[1.0, 1.0]}), \
                 (:Person {name:'d', embedding:[0.0, 1.0]}), \
                 (:Person {name:'e', embedding:[-1.0, 0.0]})",
            )
            .unwrap();
        let batch = graph
            .similar("Person", knn_options(2, Some("embedding")))
            .unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| (
                    field.name().as_str(),
                    field.data_type(),
                    field.is_nullable()
                ))
                .collect::<Vec<_>>(),
            [
                ("node1_uuid", &DataType::FixedSizeBinary(16), false),
                ("node2_uuid", &DataType::FixedSizeBinary(16), false),
                ("similarity", &DataType::Float64, false),
            ]
        );
        assert_eq!(batch.schema().metadata()["graphforge.algorithm"], "knn");
        let identities = graph
            .execute("MATCH (n:Person) RETURN n.node_uuid AS uuid ORDER BY n.name")
            .unwrap();
        let identities = identities.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let right = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let expected = [
            (0, 1),
            (0, 2),
            (1, 0),
            (1, 2),
            (2, 0),
            (2, 1),
            (3, 2),
            (3, 0),
            (4, 3),
        ];
        for (row, (source, target)) in expected.into_iter().enumerate() {
            assert_eq!(left.value(row), identities.value(source));
            assert_eq!(right.value(row), identities.value(target));
        }
        let scores = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(scores.value(0), 1.0);
        assert!((scores.value(1) - 2.0_f64.sqrt().recip()).abs() < 1e-12);
        assert_eq!(scores.value(8), 0.0);

        graph
            .execute(
                "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) \
                 CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a)",
            )
            .unwrap();
        assert_eq!(
            batch,
            graph
                .similar("Person", knn_options(2, Some("embedding")))
                .unwrap()
        );
    }

    #[test]
    fn knn_empty_and_invalid_vectors_are_structured() {
        let empty = GraphForge::new(None).unwrap();
        assert_eq!(
            empty
                .similar("Person", knn_options(10, Some("embedding")))
                .unwrap()
                .num_rows(),
            0
        );
        let zero = GraphForge::new(None).unwrap();
        zero.execute("CREATE (:Person {embedding:[0.0, 0.0]})")
            .unwrap();
        let ragged = GraphForge::new(None).unwrap();
        ragged
            .execute(
                "CREATE (:Person {embedding:[1.0]}), \
                 (:Person {embedding:[1.0, 2.0]})",
            )
            .unwrap();
        let mut via = knn_options(1, Some("embedding"));
        via.via = Some("KNOWS".into());
        for result in [
            empty.similar("Person", knn_options(1, None)),
            empty.similar("Person", via),
            zero.similar("Person", knn_options(1, Some("embedding"))),
            ragged.similar("Person", knn_options(1, Some("embedding"))),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn cosine_keeps_all_scores_with_uuid_schema_and_ignores_topology() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), \
                 (:Person {name:'b', embedding:[0.0, 1.0]}), \
                 (:Person {name:'c', embedding:[-1.0, 0.0]}), \
                 (:Person {name:'d', embedding:[-1.0, -1.0]})",
            )
            .unwrap();
        let options = cosine_options(3, Some("embedding"));
        let batch = graph.similar("Person", options.clone()).unwrap();
        assert_eq!(batch, graph.similar("Person", options).unwrap());
        assert_eq!(batch.num_rows(), 12);
        assert_eq!(batch.schema().metadata()["graphforge.algorithm"], "cosine");
        for (field, (name, data_type)) in batch.schema().fields().iter().zip([
            ("node1_uuid", DataType::FixedSizeBinary(16)),
            ("node2_uuid", DataType::FixedSizeBinary(16)),
            ("similarity", DataType::Float64),
        ]) {
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), &data_type);
            assert!(!field.is_nullable());
        }

        let scores = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let root_half = 2.0_f64.sqrt().recip();
        for (actual, expected) in scores.values().iter().zip([
            0.0, -root_half, -1.0, 0.0, 0.0, -root_half, root_half, 0.0, -1.0, root_half,
            -root_half, -root_half,
        ]) {
            assert!((actual - expected).abs() < 1e-12);
        }
        let identities = graph
            .execute("MATCH (n:Person) RETURN n.node_uuid AS uuid ORDER BY n.name")
            .unwrap();
        let identities = identities.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let right = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (row, (source, target)) in [
            (0, 1),
            (0, 3),
            (0, 2),
            (1, 0),
            (1, 2),
            (1, 3),
            (2, 3),
            (2, 1),
            (2, 0),
            (3, 2),
            (3, 0),
            (3, 1),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(left.value(row), identities.value(source));
            assert_eq!(right.value(row), identities.value(target));
        }

        graph
            .execute(
                "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) \
                 CREATE (a)-[:KNOWS]->(b)",
            )
            .unwrap();
        assert_eq!(
            batch,
            graph
                .similar("Person", cosine_options(3, Some("embedding")))
                .unwrap()
        );
    }

    #[test]
    fn cosine_defaults_top_k_and_rejects_invalid_inputs() {
        let empty = GraphForge::new(None).unwrap();
        let defaults = SimilarOptions {
            by: SimilarAlgorithm::Cosine,
            vector_property: Some("embedding".into()),
            ..SimilarOptions::default()
        };
        assert_eq!(defaults.k, 10);
        assert_eq!(empty.similar("Person", defaults).unwrap().num_rows(), 0);

        let singleton = GraphForge::new(None).unwrap();
        singleton
            .execute("CREATE (:Person {embedding:[1.0]})")
            .unwrap();
        assert_eq!(
            singleton
                .similar("Person", cosine_options(1, Some("embedding")))
                .unwrap()
                .num_rows(),
            0
        );
        let zero = GraphForge::new(None).unwrap();
        zero.execute("CREATE (:Person {embedding:[0.0, 0.0]})")
            .unwrap();
        let ragged = GraphForge::new(None).unwrap();
        ragged
            .execute(
                "CREATE (:Person {embedding:[1.0]}), \
                 (:Person {embedding:[1.0, 2.0]})",
            )
            .unwrap();
        let missing = GraphForge::new(None).unwrap();
        missing.execute("CREATE (:Person {name:'a'})").unwrap();
        let non_finite = GraphForge::new(None).unwrap();
        non_finite
            .add_node(
                "Person",
                &HashMap::from([(
                    "embedding".into(),
                    PropValue::List(vec![PropValue::Float(f64::NAN)]),
                )]),
            )
            .unwrap();
        let mut via = cosine_options(1, Some("embedding"));
        via.via = Some("KNOWS".into());
        for result in [
            empty.similar("", cosine_options(1, Some("embedding"))),
            empty.similar("Person", cosine_options(1, None)),
            empty.similar("Person", cosine_options(0, Some("embedding"))),
            empty.similar("Person", cosine_options(1, Some(" embedding"))),
            empty.similar("Person", via),
            missing.similar("Person", cosine_options(1, Some("embedding"))),
            non_finite.similar("Person", cosine_options(1, Some("embedding"))),
            zero.similar("Person", cosine_options(1, Some("embedding"))),
            ragged.similar("Person", cosine_options(1, Some("embedding"))),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn filtered_knn_obeys_outgoing_via_uuid_schema_and_stable_top_k() {
        // Exploratory mode has no ontology/knowledge layer (#772).
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute(
                "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), \
                 (:Person {name:'b', embedding:[1.0, 0.0]}), \
                 (:Person {name:'c', embedding:[1.0, 1.0]}), \
                 (:Person {name:'d', embedding:[0.0, 1.0]}), \
                 (:Person {name:'e', embedding:[-1.0, 0.0]})",
            )
            .unwrap();
        graph
            .execute(
                "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}), \
                 (c:Person {name:'c'}), (d:Person {name:'d'}), \
                 (e:Person {name:'e'}) \
                 CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(a), (a)-[:OTHER]->(e), \
                 (b)-[:OTHER]->(a), (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(b), \
                 (d)-[:KNOWS]->(c), (d)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), \
                 (e)-[:KNOWS]->(d)",
            )
            .unwrap();

        let options = filtered_knn_options(2, Some("embedding"), Some("KNOWS"));
        let batch = graph.similar("Person", options.clone()).unwrap();
        assert_eq!(batch, graph.similar("Person", options).unwrap());
        let schema = batch.schema();
        assert_eq!(schema.metadata()["graphforge.algorithm"], "filtered_knn");
        assert_eq!(schema.metadata()["graphforge.verb"], "similar");
        for (field, (name, data_type)) in schema.fields().iter().zip([
            ("node1_uuid", DataType::FixedSizeBinary(16)),
            ("node2_uuid", DataType::FixedSizeBinary(16)),
            ("similarity", DataType::Float64),
        ]) {
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), &data_type);
            assert!(!field.is_nullable());
        }

        let identities = graph
            .execute("MATCH (n:Person) RETURN n.node_uuid AS uuid ORDER BY n.name")
            .unwrap();
        let identities = identities.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let right = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (row, (source, target)) in [(0, 1), (0, 2), (2, 0), (2, 1), (3, 2), (3, 0), (4, 3)]
            .into_iter()
            .enumerate()
        {
            assert_eq!(left.value(row), identities.value(source));
            assert_eq!(right.value(row), identities.value(target));
        }
        let scores = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(scores.value(0), 1.0);
        assert!((scores.value(1) - 2.0_f64.sqrt().recip()).abs() < 1e-12);
        assert_eq!(scores.value(6), 0.0);

        for (k, via, expected_rows) in [
            (2, None, 8),
            (10, Some("KNOWS"), 8),
            (2, Some("MISSING"), 0),
        ] {
            assert_eq!(
                graph
                    .similar("Person", filtered_knn_options(k, Some("embedding"), via))
                    .unwrap()
                    .num_rows(),
                expected_rows
            );
        }
    }

    #[test]
    fn filtered_knn_empty_singleton_and_invalid_inputs_are_structured() {
        let empty = GraphForge::new(None).unwrap();
        assert_eq!(
            empty
                .similar("Person", filtered_knn_options(1, Some("embedding"), None))
                .unwrap()
                .num_rows(),
            0
        );
        let singleton = GraphForge::new(None).unwrap();
        singleton
            .execute("CREATE (a:Person {embedding:[1.0]})-[:KNOWS]->(a)")
            .unwrap();
        assert_eq!(
            singleton
                .similar(
                    "Person",
                    filtered_knn_options(1, Some("embedding"), Some("KNOWS")),
                )
                .unwrap()
                .num_rows(),
            0
        );
        let zero = GraphForge::new(None).unwrap();
        zero.execute("CREATE (:Person {embedding:[0.0, 0.0]})")
            .unwrap();
        let ragged = GraphForge::new(None).unwrap();
        ragged
            .execute(
                "CREATE (:Person {embedding:[1.0]}), \
                 (:Person {embedding:[1.0, 2.0]})",
            )
            .unwrap();
        for result in [
            empty.similar("Person", filtered_knn_options(1, None, None)),
            empty.similar("Person", filtered_knn_options(0, Some("embedding"), None)),
            empty.similar(
                "Person",
                filtered_knn_options(1, Some("embedding"), Some(" ")),
            ),
            zero.similar("Person", filtered_knn_options(1, Some("embedding"), None)),
            ragged.similar("Person", filtered_knn_options(1, Some("embedding"), None)),
        ] {
            assert!(matches!(result, Err(GfError::Validation(_))));
        }
    }

    #[test]
    fn streaming_query_preflight_errors_are_exact_and_side_effect_free() {
        let graph = GraphForge::new(None).unwrap();
        let error =
            |result: Result<graphforge_exec::SendableRecordBatchStream, GfError>| match result {
                Ok(_) => panic!("expected streaming query to fail"),
                Err(error) => error,
            };

        let empty = error(graph.execute_stream("   "));
        assert_eq!(empty.code(), "GF_VALIDATION");
        assert!(empty.to_string().contains("empty query"));

        let comment = error(graph.execute_stream("// comment only"));
        assert_eq!(comment.code(), "GF_VALIDATION");
        assert!(comment.to_string().contains("empty query"));

        let parse = error(graph.execute_stream("MATCH ("));
        assert_eq!(parse.code(), "GF_PARSE");

        let missing = error(
            graph.execute_stream_with_params("MATCH (n) RETURN n SKIP $missing", &HashMap::new()),
        );
        assert_eq!(missing.code(), "GF_PLAN");
        assert_eq!(
            missing.to_string(),
            "plan error: unsupported expression: operator not yet lowered (deferred to #577+): \
             SkipParam { name: \"missing\" }"
        );
    }

    #[test]
    fn private_wire_and_row_count_boundaries_match_public_error_domains() {
        for encoding in ["parquet", "arrow", "json"] {
            assert!(participant_encoding(encoding).is_ok());
        }
        let encoding = participant_encoding("PARQUET").unwrap_err();
        assert_eq!(encoding.code(), "GF_VALIDATION");
        assert_eq!(
            encoding.to_string(),
            "validation error: committed participant has unsupported encoding"
        );

        let mut params = HashMap::new();
        params.insert("count".to_owned(), IrLiteral::Int(7));
        assert_eq!(row_count_param_value("LIMIT", "count", &params).unwrap(), 7);

        params.insert("count".to_owned(), IrLiteral::Int(-1));
        let negative = row_count_param_value("LIMIT", "count", &params).unwrap_err();
        assert_eq!(negative.code(), "GF_EXECUTION");
        assert_eq!(
            negative.to_string(),
            "execution error: LIMIT parameter `$count` must be a non-negative integer"
        );

        params.insert("count".to_owned(), IrLiteral::Str("7".to_owned()));
        let wrong_type = row_count_param_value("SKIP", "count", &params).unwrap_err();
        assert_eq!(wrong_type.code(), "GF_EXECUTION");
        assert_eq!(
            wrong_type.to_string(),
            "execution error: SKIP parameter `$count` must be an integer"
        );

        let missing = row_count_param_value("LIMIT", "absent", &params).unwrap_err();
        assert_eq!(missing.code(), "GF_EXECUTION");
        assert_eq!(
            missing.to_string(),
            "execution error: missing query parameter `$absent` for LIMIT"
        );
    }
}
