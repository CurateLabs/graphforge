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
//!
//! Telemetry configuration and lifecycle remain Rust-owned and are projected
//! through [`telemetry`].
#![forbid(unsafe_code)]

/// Rust-owned OpenTelemetry-compatible configuration and lifecycle contract.
pub use graphforge_observability as telemetry;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::datatypes::SchemaRef;
use graphforge_core::GraphIdentity;
pub use graphforge_io::{
    ResultSinkFormat, ResultSinkOptions, ResultSinkProgress, ResultSinkReceipt,
};
use graphforge_ir::{
    BindError, Binder, CompositionBindingContext, CompositionBindingLimits, GraphOp, GraphPlan,
    IrExpr, ProcedureRegistry, RuntimeCatalog,
};
pub use graphforge_ontology::{
    ActivationMode, ActivationRecord, ActivationScope, BridgeDocument, BridgeExportFormat,
    BridgeImportFormatHint, BridgeSelector, BridgeSetId, ExportFormat, ImportFormatHint,
    ModuleSelector, OntologyModuleId, SymbolKind,
};
use graphforge_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};
use graphforge_storage::GraphCatalog;
use graphforge_storage::ResolvedProjectGeneration;
pub use graphforge_storage::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ConstructionChunkReceipt,
    GraphConstructionBudgets, GraphConstructionEvidence, GraphConstructionState,
};
use sha2::{Digest, Sha256};

#[cfg(test)]
mod adjacency_rebuild_barrier;
mod algorithm_embedding_publication;
mod algorithm_runs;
mod algorithm_writeback;
mod analyst;
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
#[cfg(test)]
mod composition_binding_tests;
/// Hidden writer-hold helpers for native binding concurrency probes.
#[doc(hidden)]
pub mod concurrency_test_support;
mod construction;
#[cfg(test)]
mod construction_concurrency_tests;
#[cfg(test)]
mod construction_ordinal_tests;
#[cfg(test)]
mod durability_certification_tests;
mod embedding_freshness;
mod embedding_publication;
mod embedding_refresh;
mod embedding_spaces;
mod epistemic_snapshot;
mod explanation;
mod find_execution;
mod generation_diff;
mod graph_inspection;
mod graph_snapshot;
mod gsi_profiler;
mod hypotheses;
mod import_session;
mod invocation_descriptor;
mod knowledge;
mod maintenance;
#[cfg(test)]
mod mapped_portable_tests;
#[cfg(test)]
mod mapped_stream_tests;
mod multi_ontology;
mod mutation_transaction;
#[cfg(test)]
mod mutation_transaction_fault_tests;
pub use multi_ontology::{
    ActivationProfileChangeRequest, BridgeAdoptionRequest, BridgeCandidate, BridgeDeleteRequest,
    BridgeUpdateRequest, CompositionValidationReceipt, ModuleAdoptionRequest, ModuleCandidate,
    ModuleDeleteRequest, ModuleMigrationPreview, ModuleMigrationReceipt, ModuleMigrationRequest,
    ModuleUpdateRequest, MultiOntologyCaseResult, MultiOntologyCertificationReport,
    MultiOntologyDiagnostic, MultiOntologyError, MultiOntologyMutationReceipt,
    MultiOntologyParityReport, MultiOntologyRetainedDataReport, MultiOntologyValidationReceipt,
    OntologyAuthorityExpectation, OntologyAuthorityState, ResolutionExplainRequest,
    ResolutionExplanation,
};
#[cfg(test)]
mod multi_process_publication_tests;
mod node_selector;
mod ontology_composition_lifecycle;
mod ontology_lifecycle;
pub use ontology_composition_lifecycle::{
    CompositionChangeDiagnostic, CompositionChangePreview, CompositionChangeReceipt,
    CompositionChangeRequest, CompositionDataDisposition, CompositionPortableCompatibility,
    CompositionPortableReceipt,
};
mod discovery_portable_v2;
mod paging;
pub use discovery_portable_v2::{
    DiscoveredPortableV2, DiscoveryPortableV2Error, DiscoveryPortableV2Mismatch,
    DiscoveryPortableV2Request, verify_discovered_portable_v2,
};
mod portable;
mod provenance;
mod provider_embedding;
mod provider_embedding_execution;
mod provider_find;
mod provider_rerank;
mod provider_session;
mod query_evidence;
mod repository;
mod resource_policy;
mod resumable_construction;
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

pub use graphforge_core::portable::{
    PortableV2Authenticity, PortableV2Compatibility, PortableV2Error, PortableV2ErrorCode,
    PortableV2ExportProgress, PortableV2GraphSelector, PortableV2GraphSubsetMeta,
    PortableV2Integrity, PortableV2Limits, PortableV2Mode, PortableV2Output,
    PortableV2PackageClass, PortableV2ParticipantId, PortableV2PropertyProjection,
    PortableV2Representation, PortableV2SelectionEntry, PortableV2SelectionPlan,
    PortableV2SelectionProfile, PortableV2SelectionReason, PortableV2SelectionRequest,
    PortableV2SubsetClosure, PortableV2SubsetPreview as PortableV2SubsetPlan,
    PortableV2SubsetRequest,
};
pub use graphforge_core::storage_receipt::{
    ArtifactCategory, ArtifactStorageTotals, StorageAttributionReceipt,
};
/// Finite portable export budgets.
pub type PortableV2ExportLimits = PortableV2Limits;
pub use graphforge_storage::{
    SemanticMigrationOperation, WorkspaceOntologyComposition, WorkspacePortableOntologyStaging,
};
pub use portable::{
    PortableExportRequest, PortableExportResult, PortableImportRequest, PortableImportResult,
    PortableSelection, PortableV2ExportFacadeResult, PortableV2ExportReceiptView,
    PortableV2ExportRequest, PortableV2ImportRequest, PortableV2ImportResult,
    PortableV2OciPublishFacadeRequest, PortableV2OciPullFacadeRequest, PortableV2RepackRequest,
    PortableV2RepackResult, PortableV2SelectionPreviewRequest, PortableV2SubsetPreviewRequest,
    PortableVerifyRequest, PortableVerifyResult, publish_portable_v2_oci,
    publish_portable_v2_oci_with_registry, pull_portable_v2_oci,
    pull_portable_v2_oci_with_registry, repack_verified_expanded_portable_v2, verify_portable_v2,
};
pub use repository::{
    GitProvenance, InfraCapabilityCompatibility, InfraNotChecked, InfraPlan, InfraStaticValidity,
    InfraValidationResult, ProjectConfig, RepositoryContext, RepositoryDefinitionDigest,
    RepositoryInitReceipt, RepositoryRemoveReceipt, RepositorySourceDigest, RepositorySyncRequest,
    RepositorySyncResult, RepositorySyncStatus, SkillBundle, SkillBundleFile, SkillMutationReceipt,
    SkillStatus, SkillStatusReceipt,
};
pub use resumable_construction::{
    GraphConstructionProgress, GraphConstructionPublicationReceipt, GraphConstructionSession,
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
pub use graphforge_core::uuid::hub_clone_operation;
pub use graphforge_core::{
    AlgorithmError, AnalyzeOptions, ApiErrorCode, ClusterOptions, EdgeHandle, ExplainStage,
    FindOptions, GfError, LoweringError, NodeHandle, NodeSelector, OntologyFormat, OntologyMode,
    ParseErrorKind, PathsOptions, ProjectErrorCode, PropValue, RankOptions, SimilarOptions, Span,
    SpatialCoordinates, SpatialCrs, SpatialGeometryType, SpatialType, SpatialValue, TemporalValue,
};
pub use import_session::{
    GraphImportSession, ImportConstructionEvidence, ImportPhase, ImportProgress,
    ImportSessionLimits, ImportSourceKind, PublicationWorkComponents,
};
pub use query_evidence::{
    QueryExecutionEvidence, QueryHopEvidence, QueryOperatorRssEvidence, QuerySinkEvidenceReceipt,
    QuerySortEvidence,
};
// The Arrow-backed result of [`GraphForge::execute`].
pub use generation_diff::{
    CommittedGenerationIdentity, GenerationDiffDisposition, GenerationDiffLimits,
    GenerationDiffRequest, GenerationGraphDiff, GraphChangeStream, ReloadRequiredReason,
};
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
    ProjectGraphObjectSweepDisposition, ProjectGraphObjectSweepReport, ProjectOpenRecoveryEvidence,
    ProjectOpenRecoveryKind, ProjectReachabilityReport, ProjectRecoveryDeferral,
    ProjectRecoveryGenerationClass, ProjectRetentionLimits, ProjectRetentionPolicy,
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
// GraphForge
// ---------------------------------------------------------------------------

struct GenerationPropertyAuthority {
    generation_uuid: uuid::Uuid,
    inventory: Arc<graphforge_storage::AuthenticatedPropertyInventory>,
}

/// The GraphForge engine — the public entry point for openCypher execution.
///
/// Built with [`new`](Self::new) (in-memory or Parquet-backed), then queried via
/// [`execute`](Self::execute). The facade owns the project directory, the
/// resolved [`OntologyMode`], an optional compiled ontology, and a shared
/// [`RuntimeCatalog`] that the binder grows as it observes new labels/properties.
pub struct GraphForge {
    allocation_operation: Option<graphforge_storage::StorageAllocationOperation>,
    /// Opaque ownership token for public handles created by this instance.
    identity: GraphIdentity,
    /// The configured path, if the instance is Parquet-backed; `None` for an
    /// in-memory instance (whose data lives in `dir`).
    path: Option<PathBuf>,
    /// Filesystem lifecycle contract selected when this facade opened.
    lifecycle_mode: graphforge_storage::filesystem_admission::ProjectLifecycleMode,
    /// One immutable committed generation selected exactly once at open.
    resolved_generation: ResolvedProjectGeneration,
    /// Canonical property generation identity and authenticated inventory,
    /// replaced together under one lock.
    property_authority: Arc<Mutex<GenerationPropertyAuthority>>,
    /// Whether this facade is an immutable historical checkpoint view.
    read_only: bool,
    /// Generation UUID whose graph snapshot was hydrated into `dir`.
    current_generation_uuid: Arc<Mutex<uuid::Uuid>>,
    /// Authenticated UUID index handle cached for one topology generation.
    uuid_membership_index: Mutex<Option<graphforge_storage::UuidMembershipIndex>>,
    /// Exact generation-pinned ordinal destination identity authority shared
    /// by every fixed-hop session.
    ordinal_identities: Arc<graphforge_exec::V4OrdinalIdentityResolver>,
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
    /// Exact generation-bound qualified storage authority, when adopted.
    semantic_storage_bindings: Arc<Mutex<Option<graphforge_storage::SemanticStorageBindings>>>,
    /// Generation-hydrated compiled composition used by ordinary query/write
    /// entry points. The #840 composition participant loader installs this
    /// only after compiling and authenticating the exact persisted closure.
    default_composition_context: Arc<Mutex<Option<Arc<CompositionBindingContext>>>>,
    /// Procedures available to `CALL` clauses on this engine instance.
    procedures: Arc<Mutex<ProcedureRegistry>>,
    /// Effective ontology enforcement mode.
    ontology_mode: OntologyMode,
    /// Long-lived adjacency provider (#832): one per instance so loaded CSR
    /// views amortize across queries. Each session revalidates it (one
    /// generation read) at construction, and write paths invalidate it.
    adjacency_provider: Arc<std::sync::RwLock<Arc<graphforge_exec::PersistentAdjacencyProvider>>>,
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
    #[cfg(test)]
    last_mutation_outcome: Mutex<Option<graphforge_exec::mutation::MutationOutcome>>,
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

type BoundGenerationStorage = (
    Arc<CompositionBindingContext>,
    graphforge_storage::SemanticStorageBindings,
    Vec<(std::path::PathBuf, std::path::PathBuf)>,
);

impl std::fmt::Debug for GraphForge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Custom impl: the ontology handle / runtime catalog are large and not
        // usefully printable, so summarise rather than deriving Debug.
        f.debug_struct("GraphForge")
            .field("identity", &self.identity)
            .field("path", &self.path)
            .field("lifecycle_mode", &self.lifecycle_mode)
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
    /// Capture authenticated logical and physical storage attribution for the
    /// generation visible to this facade.
    ///
    /// The storage layer walks only the generation's authenticated inventories
    /// and opens retained file capabilities. It never recursively scans the
    /// project directory, and qualification fails closed on an unclassified
    /// graph artifact.
    pub fn storage_attribution(
        &self,
    ) -> Result<graphforge_storage::StorageAttributionSnapshot, GfError> {
        let generation = self.generation_for_read()?;
        let snapshot = graphforge_storage::capture_storage_attribution(&generation)?;
        snapshot.validate_for_qualification()?;
        Ok(snapshot)
    }

    /// Capture closed, identity-free storage evidence for ordinary consumers.
    pub fn storage_attribution_receipt(&self) -> Result<StorageAttributionReceipt, GfError> {
        graphforge_storage::storage_attribution_receipt_from_snapshot(&self.storage_attribution()?)
    }

    pub(crate) fn stage_project_generation(
        &self,
        request: &graphforge_storage::ProjectGenerationRequest,
    ) -> Result<graphforge_storage::ProjectStageOutcome, GfError> {
        graphforge_storage::stage_project_generation_with_graph_tree_mode(
            self.resolved_generation.container_root(),
            request,
            None,
            self.lifecycle_mode,
        )
    }

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

    /// Open a project for first-party allocation qualification using an explicit
    /// pre-operation owner context. Ordinary facade options remain unchanged.
    ///
    /// # Errors
    /// Returns ordinary project-open errors and allocation evidence errors.
    #[doc(hidden)]
    pub(crate) fn open_with_allocation_diagnostics(
        path: &std::path::Path,
        allocation: graphforge_storage::StorageAllocationOperation,
    ) -> Result<Self, GfError> {
        let path = graphforge_storage::StorageAllocationOperation::resolve_project_path(path)?;
        let (options, policy) = GraphForgeOptions::default().validate()?;
        let (resolved, recovery) =
            graphforge_storage::open_or_initialize_project_with_allocation(&path, &allocation)?;
        let mut graph = Self::open_resolved_with_options(
            path.clone(),
            resolved,
            false,
            options,
            policy,
            recovery,
        )?;
        graph.allocation_operation = Some(allocation);
        Ok(graph)
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
        let property_inventory =
            property_inventory_for_hydrated_generation(&resolved_generation, &dir)?;
        let ordinal_identities = ordinal_identity_resolver(&resolved_generation, &dir)?;
        Ok(Self {
            identity: GraphIdentity::new(),
            path: None,
            lifecycle_mode:
                graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral,
            resolved_generation,
            property_authority: Arc::new(Mutex::new(GenerationPropertyAuthority {
                generation_uuid,
                inventory: Arc::clone(&property_inventory),
            })),
            read_only: false,
            current_generation_uuid: Arc::new(Mutex::new(generation_uuid)),
            uuid_membership_index: Mutex::new(None),
            ordinal_identities,
            clock: Mutex::new(Arc::new(system_time_micros)),
            adjacency_provider: Arc::new(std::sync::RwLock::new(Arc::new(
                adjacency_provider_for_graph(&dir, ontology_mode, Arc::clone(&property_inventory))?,
            ))),
            adjacency_visibility: Arc::new(std::sync::RwLock::new(())),
            embedding_refresh_scheduler: Arc::new(Mutex::new(
                embedding_refresh::initialize_embedding_refresh_scheduler(&dir)?,
            )),
            embedding_refresh_epoch: Instant::now(),
            embedding_refresh_visibility: Arc::new(Mutex::new(())),
            #[cfg(test)]
            last_mutation_outcome: Mutex::new(None),
            graph_visibility: Arc::new(write_modes::WriteCoordinator::new(&options)),
            write_options: options,
            allocation_operation: None,
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
            semantic_storage_bindings: Arc::new(Mutex::new(None)),
            default_composition_context: Arc::new(Mutex::new(None)),
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

    fn open_resolved_with_lifecycle_mode(
        container_dir: PathBuf,
        resolved_generation: ResolvedProjectGeneration,
        read_only: bool,
        lifecycle_mode: graphforge_storage::filesystem_admission::ProjectLifecycleMode,
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
        let mut graph = Self::open_resolved_with_options(
            container_dir,
            resolved_generation,
            read_only,
            options,
            resource_policy,
            project_open_recovery,
        )?;
        graph.lifecycle_mode = lifecycle_mode;
        Ok(graph)
    }

    #[allow(clippy::too_many_lines)] // open authenticates every coupled generation participant
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
        let property_inventory =
            property_inventory_for_hydrated_generation(&resolved_generation, &dir)?;
        let ordinal_identities = ordinal_identity_resolver(&resolved_generation, &dir)?;

        let runtime_catalog = load_runtime_catalog(&dir)?;
        let semantic_storage_bindings =
            graphforge_storage::semantic_storage_bindings(&resolved_generation)?;
        let default_composition_context = load_composition_binding(&resolved_generation)?;
        if semantic_storage_bindings.is_none()
            && let Some(context) = &default_composition_context
        {
            if context.composition().modules.len() == 1 && ontology_document.is_none() {
                graphforge_storage::SemanticStorageBindings::project_legacy_unambiguous(
                    context.composition(),
                    &dir,
                )?;
            } else if context.composition().modules.len() != 1 {
                graphforge_storage::require_atomic_legacy_migration(&dir)?;
            }
        }
        if let Some(bindings) = &semantic_storage_bindings {
            let context = default_composition_context.as_ref().ok_or_else(|| {
                GfError::Validation(
                    "semantic bindings have no compiled persisted composition authority".into(),
                )
            })?;
            bindings.validate_against(context.composition())?;
            let inventory = resolved_generation.graph_files_inventory()?;
            bindings.validate_physical_routes_with_inventory(&dir, inventory.as_ref())?;
        }
        let default_composition_context =
            match (default_composition_context, &semantic_storage_bindings) {
                (Some(context), Some(bindings)) => Some(Arc::new(
                    context.with_generation_storage_ids(
                        bindings
                            .bindings
                            .iter()
                            .map(|binding| (binding.symbol.clone(), binding.storage_id)),
                    )?,
                )),
                (None, Some(_)) => unreachable!("semantic composition validated above"),
                (Some(context), None)
                    if context.composition().modules.len() == 1 && ontology_document.is_some() =>
                {
                    None
                }
                (context, None) => context,
            };
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

        let adjacency_provider =
            adjacency_provider_for_graph(&dir, ontology_mode, Arc::clone(&property_inventory))?;
        let graph = Self {
            identity: GraphIdentity::new(),
            path: Some(container_dir),
            lifecycle_mode: graphforge_storage::filesystem_admission::ProjectLifecycleMode::Durable,
            resolved_generation,
            property_authority: Arc::new(Mutex::new(GenerationPropertyAuthority {
                generation_uuid,
                inventory: Arc::clone(&property_inventory),
            })),
            read_only,
            current_generation_uuid: Arc::new(Mutex::new(generation_uuid)),
            uuid_membership_index: Mutex::new(None),
            ordinal_identities,
            clock: Mutex::new(Arc::new(system_time_micros)),
            adjacency_provider: Arc::new(std::sync::RwLock::new(Arc::new(adjacency_provider))),
            adjacency_visibility: Arc::new(std::sync::RwLock::new(())),
            embedding_refresh_scheduler: Arc::new(Mutex::new(
                embedding_refresh::initialize_embedding_refresh_scheduler(&dir)?,
            )),
            embedding_refresh_epoch: Instant::now(),
            embedding_refresh_visibility: Arc::new(Mutex::new(())),
            #[cfg(test)]
            last_mutation_outcome: Mutex::new(None),
            graph_visibility: Arc::new(write_modes::WriteCoordinator::new(&write_options)),
            write_options,
            allocation_operation: None,
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
            semantic_storage_bindings: Arc::new(Mutex::new(semantic_storage_bindings)),
            default_composition_context: Arc::new(Mutex::new(default_composition_context)),
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
        self.graph_visibility.health.check()?;
        self.heavy_query_admission.try_acquire()
    }

    fn admit_heavy_query_owned(&self) -> Result<tokio::sync::OwnedSemaphorePermit, GfError> {
        self.graph_visibility.health.check()?;
        self.heavy_query_admission.try_acquire_owned()
    }

    fn generation_for_read(&self) -> Result<ResolvedProjectGeneration, GfError> {
        self.graph_visibility.health.check()?;
        if self.read_only {
            Ok(self.resolved_generation.clone())
        } else {
            graphforge_storage::resolve_project_generation(
                self.resolved_generation.container_root(),
            )
        }
    }

    fn adjacency_provider_for_session(&self) -> Arc<graphforge_exec::PersistentAdjacencyProvider> {
        Arc::clone(
            &self
                .adjacency_provider
                .read()
                .expect("adjacency provider lock poisoned"),
        )
    }

    fn property_inventory_for_session(
        &self,
    ) -> Arc<graphforge_storage::AuthenticatedPropertyInventory> {
        let authority = self
            .property_authority
            .lock()
            .expect("property authority lock poisoned");
        debug_assert_eq!(
            authority.inventory.generation_uuid(),
            Some(authority.generation_uuid)
        );
        Arc::clone(&authority.inventory)
    }

    fn install_property_generation(
        &self,
        generation: &ResolvedProjectGeneration,
    ) -> Result<(), GfError> {
        let replacement = Arc::new(
            graphforge_storage::AuthenticatedPropertyInventory::from_resolved_generation(
                generation,
            )?,
        );
        let ordinal_replacement = ordinal_identity_handle(generation, &self.dir)?;
        let adjacency_replacement = Arc::new(adjacency_provider_for_graph(
            &self.dir,
            self.ontology_mode,
            Arc::clone(&replacement),
        )?);
        *self
            .property_authority
            .lock()
            .expect("property authority lock poisoned") = GenerationPropertyAuthority {
            generation_uuid: generation.generation_uuid(),
            inventory: Arc::clone(&replacement),
        };
        *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") = generation.generation_uuid();
        *self
            .adjacency_provider
            .write()
            .expect("adjacency provider lock poisoned") = adjacency_replacement;
        self.ordinal_identities.replace(ordinal_replacement);
        Ok(())
    }

    fn execute_read_only(&self, cypher: &str) -> Result<ExecutionResult, GfError> {
        if cypher.trim().is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        let ast = graphforge_cypher::parse(cypher).map_err(GfError::from)?;
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

    /// Execute a query using exact compiled multi-ontology binding authority.
    ///
    /// The composition is consumed by the same parse, bind, lower, and execute
    /// path as [`execute`](Self::execute). Its fingerprint and deterministic
    /// binding receipts are retained in the plan; runtime-catalog observations
    /// are published only after the complete bind succeeds.
    ///
    /// # Errors
    /// As [`execute`](Self::execute), including scoped composition diagnostics.
    pub fn execute_with_composition(
        &self,
        cypher: &str,
        composition: Arc<CompositionBindingContext>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query_with_composition(cypher, &HashMap::new(), composition, true)
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
        self.run_query_with_optional_composition(
            cypher,
            params,
            self.default_composition_snapshot(),
            publish,
        )
    }

    fn default_composition_snapshot(&self) -> Option<Arc<CompositionBindingContext>> {
        self.default_composition_context
            .lock()
            .expect("default composition context lock poisoned")
            .clone()
    }

    fn composition_execution_mode(context: &CompositionBindingContext) -> OntologyMode {
        // The binder owns fallback policy; resolved generation symbols retain
        // their typed storage route even under an exploratory profile.
        match context.composition().profile_default {
            graphforge_ontology::ActivationMode::Exploratory
            | graphforge_ontology::ActivationMode::Advisory => OntologyMode::Advisory,
            graphforge_ontology::ActivationMode::Strict => OntologyMode::Strict,
        }
    }

    fn open_query_catalog(
        &self,
        runtime_catalog: &Arc<Mutex<RuntimeCatalog>>,
        candidate: Option<&graphforge_storage::SemanticStorageBindings>,
    ) -> Result<GraphCatalog, GfError> {
        let runtime = runtime_catalog.lock().expect("runtime catalog poisoned");
        let installed = self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned");
        GraphCatalog::open_authenticated_with_semantic_bindings(
            &self.dir,
            self.ontology.as_ref(),
            &runtime,
            candidate.or(installed.as_ref()),
            self.property_inventory_for_session(),
        )
        .map_err(|error| GfError::Storage(error.to_string()))
    }

    /// Install the exact compiled context reconstructed from the persisted
    /// composition participant. This is the narrow atomic hydration seam used
    /// by the composition lifecycle; ordinary execution consumes it by default.
    #[allow(dead_code)] // consumed by the generation composition publisher added by issue #840
    pub(crate) fn install_generation_composition_context(
        &self,
        context: &Arc<CompositionBindingContext>,
    ) -> Result<(), GfError> {
        let bindings = self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned")
            .clone()
            .ok_or_else(|| {
                GfError::Validation(
                    "persisted composition has no generation storage binding authority".into(),
                )
            })?;
        bindings.validate_against(context.composition())?;
        let context = context.with_generation_storage_ids(
            bindings
                .bindings
                .iter()
                .map(|binding| (binding.symbol.clone(), binding.storage_id)),
        )?;
        *self
            .default_composition_context
            .lock()
            .expect("default composition context lock poisoned") = Some(Arc::new(context));
        Ok(())
    }

    fn run_query_with_composition(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        composition: Arc<CompositionBindingContext>,
        publish: bool,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query_with_optional_composition(cypher, params, Some(composition), publish)
    }

    fn run_query_with_optional_composition(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        composition: Option<Arc<CompositionBindingContext>>,
        publish: bool,
    ) -> Result<ExecutionResult, GfError> {
        let _admission = self.admit_heavy_query()?;
        let composition = composition
            .map(|context| self.bind_generation_storage(&context))
            .transpose()?;
        if cypher.trim().is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }

        let ast = graphforge_cypher::parse(cypher).map_err(GfError::from)?;
        // A query that strips to zero clauses (e.g. comment-only or block-comment
        // -only) is empty even though its raw text is not blank, so the
        // `trim().is_empty()` guard above misses it. Reject it here rather than
        // letting the empty plan panic the result shaper (#603 — found by fuzz_exec).
        if ast.clauses.is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        let is_mutation = ast.has_mutation_clauses();
        if is_mutation && self.read_only {
            return Err(GfError::Execution(
                "GF_WRITE_RESOURCE_READ_ONLY: session does not authorize writes".into(),
            ));
        }
        let _mutation_admission = (is_mutation && publish)
            .then(|| self.graph_visibility.lock())
            .transpose()?;
        let transaction = is_mutation.then(|| {
            graphforge_exec::mutation::MutationTransaction::new(
                &self
                    .runtime_catalog
                    .lock()
                    .expect("runtime catalog poisoned"),
            )
        });
        let binding_catalog = transaction.as_ref().map_or_else(
            || Arc::clone(&self.runtime_catalog),
            graphforge_exec::mutation::MutationTransaction::catalog,
        );
        validate_typed_parameter_binding(
            &ast,
            params,
            self.ontology.clone(),
            &binding_catalog,
            self.ontology_mode,
            self.procedure_snapshot(),
        )?;

        // Bind against the shared runtime catalog so newly observed types/props
        // persist across queries in this instance.
        let plan = {
            let mut binder = Binder::new(
                self.ontology.clone(),
                binding_catalog.clone(),
                self.ontology_mode,
            )
            .with_procedures(self.procedure_snapshot());
            if let Some((composition, _, _)) = &composition {
                binder = binder.with_composition(Arc::clone(composition));
            }
            binder
                .bind(&ast)
                .map_err(|errs| bind_errors_to_gferror(&errs))?
        };

        validate_call_params(&plan, params)?;

        let candidate = composition.as_ref().map(|(_, candidate, _)| candidate);
        let legacy_route_moves = composition.as_ref().map(|(_, _, moves)| moves.as_slice());
        let composition_mode = composition
            .as_ref()
            .map(|(context, _, _)| Self::composition_execution_mode(context));
        let result = self.run_plan_with_publish_and_bindings(
            &plan,
            params,
            publish,
            candidate,
            composition_mode,
            legacy_route_moves,
            transaction,
            is_mutation && publish,
        );
        let result = result.map_err(publicize_query_error)?;
        shape_result(result, self.ontology_mode, self.ontology.as_ref())
            .map_err(publicize_query_error)
    }

    fn bind_generation_storage(
        &self,
        context: &Arc<CompositionBindingContext>,
    ) -> Result<BoundGenerationStorage, GfError> {
        let current = self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned");
        let (projected, route_moves) =
            if current.is_none() && context.composition().modules.len() == 1 {
                let projection =
                    graphforge_storage::SemanticStorageBindings::project_legacy_unambiguous(
                        context.composition(),
                        &self.dir,
                    )?;
                (projection.bindings, projection.route_moves)
            } else {
                if current.is_none() {
                    graphforge_storage::require_atomic_legacy_migration(&self.dir)?;
                }
                (
                    graphforge_storage::SemanticStorageBindings::project(
                        context.composition(),
                        current.as_ref(),
                    )?,
                    Vec::new(),
                )
            };
        projected.validate_against(context.composition())?;
        let context = context.with_generation_storage_ids(
            projected
                .bindings
                .iter()
                .map(|binding| (binding.symbol.clone(), binding.storage_id)),
        )?;
        drop(current);
        Ok((Arc::new(context), projected, route_moves))
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
        self.run_plan_with_publish_and_bindings(
            plan, params, publish, None, None, None, None, false,
        )
    }

    #[allow(clippy::too_many_lines)] // one visibility lock spans execution and publication
    #[allow(clippy::too_many_arguments)] // keep publication, binding and pre-admitted write context explicit
    fn run_plan_with_publish_and_bindings(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, IrLiteral>,
        publish: bool,
        candidate_bindings: Option<&graphforge_storage::SemanticStorageBindings>,
        composition_mode: Option<OntologyMode>,
        legacy_route_moves: Option<&[(std::path::PathBuf, std::path::PathBuf)]>,
        transaction: Option<graphforge_exec::mutation::MutationTransaction>,
        write_admission_held: bool,
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
        if is_write && self.read_only {
            return Err(GfError::Execution(
                "GF_WRITE_RESOURCE_READ_ONLY: session does not authorize writes".into(),
            ));
        }
        // Transaction commit already holds write admission when publish is false.
        let _write_visibility = (is_write && publish && !write_admission_held)
            .then(|| self.graph_visibility.lock())
            .transpose()?;
        let _read_visibility = (!is_write)
            .then(|| self.graph_visibility.read())
            .transpose()?;
        let prior_catalog = self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned")
            .clone();
        let mut transaction = if is_write {
            Some(transaction.unwrap_or_else(|| {
                graphforge_exec::mutation::MutationTransaction::new(&prior_catalog)
            }))
        } else {
            None
        };
        let working_catalog = transaction.as_ref().map_or_else(
            || Arc::clone(&self.runtime_catalog),
            graphforge_exec::mutation::MutationTransaction::catalog,
        );
        let mut lifecycle = if is_write {
            Some(mutation_transaction::FacadeMutationLifecycle::new(
                self,
                prior_catalog,
                publish,
                candidate_bindings,
            )?)
        } else {
            None
        };
        let mut legacy_migration = None;
        if legacy_route_moves.is_some_and(|moves| !moves.is_empty()) {
            if !is_write || !publish {
                return Err(GfError::Validation(
                    "GF_SEMANTIC_LEGACY_MIGRATION_REQUIRED: run a publishing write to migrate the unambiguous legacy generation".into(),
                ));
            }
            legacy_migration = Some(graphforge_storage::apply_legacy_route_moves(
                &self.dir,
                legacy_route_moves.expect("checked"),
                candidate_bindings.expect("legacy migration has candidate bindings"),
            )?);
        }

        // Open a catalog snapshot reflecting the freshly-bound runtime catalog so
        // read scans resolve property names interned during bind.
        let catalog = self.open_query_catalog(&working_catalog, candidate_bindings)?;
        // A compiled composition is explicit typed authority even when the
        // legacy workspace ontology profile remains exploratory. Its writes
        // must never fall back to `_untyped` host routing.
        let execution_mode = composition_mode.unwrap_or(self.ontology_mode);
        let adjacency_provider = if execution_mode == self.ontology_mode {
            self.adjacency_provider_for_session()
        } else {
            Arc::new(adjacency_provider_for_graph(
                &self.dir,
                execution_mode,
                self.property_inventory_for_session(),
            )?)
        };
        let session = ExecutionSession::new_with_target_provider_resources_and_identity(
            catalog,
            self.ontology.clone(),
            self.dir.clone(),
            execution_mode,
            adjacency_provider,
            Some(Arc::clone(&self.ordinal_identities)),
            &self.session_resource_config(),
        )?;
        let session = if self.read_only {
            session.restrict_to_reads()
        } else {
            session
        };

        let execution = self.block_on(async {
            if is_write {
                session
                    .prepare_write_statement_with_params(
                        &plan,
                        params,
                        transaction.as_mut().expect("write transaction"),
                    )
                    .await
            } else {
                session.execute_plan_with_params(&plan, params).await
            }
        });
        let result = match execution {
            Ok(result) => result,
            Err(error) => {
                if let Some(transaction) = transaction.take() {
                    return transaction.abort(lifecycle.as_mut().expect("write lifecycle"), error);
                }
                return Err(error);
            }
        };
        if let Some(transaction) = transaction.take() {
            let resource = session.write_resource()?;
            transaction.commit(
                &resource,
                true,
                lifecycle.as_mut().expect("write lifecycle"),
            )?;
        }
        if publish
            && let Some(_receipt) = result
                .mutation_receipt
                .as_ref()
                .filter(|receipt| !receipt.is_empty())
        {
            if let Some(candidate) = candidate_bindings {
                // The write visibility lock still covers this swap, so an
                // older request can never overwrite a newer publication.
                *self
                    .semantic_storage_bindings
                    .lock()
                    .expect("semantic storage binding lock poisoned") = Some(candidate.clone());
            }
            if let Some(migration) = &mut legacy_migration {
                migration.commit();
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
        self.publish_graph_mutation_with_bindings(receipt, None)
    }

    fn publish_graph_mutation_with_bindings(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
        candidate_bindings: Option<&graphforge_storage::SemanticStorageBindings>,
    ) -> Result<(), GfError> {
        let operation_uuid = uuid::Uuid::now_v7();
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        self.publish_graph_mutation_with_context_and_bindings(
            receipt,
            operation_uuid,
            None,
            recorded_at_micros,
            candidate_bindings,
        )
    }

    pub(crate) fn publish_graph_mutation_with_context(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
        operation_uuid: uuid::Uuid,
        actor_uuid: Option<uuid::Uuid>,
        recorded_at_micros: i64,
    ) -> Result<(), GfError> {
        self.publish_graph_mutation_with_context_and_bindings(
            receipt,
            operation_uuid,
            actor_uuid,
            recorded_at_micros,
            None,
        )
    }

    fn publish_graph_mutation_with_context_and_bindings(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
        operation_uuid: uuid::Uuid,
        actor_uuid: Option<uuid::Uuid>,
        recorded_at_micros: i64,
        candidate_bindings: Option<&graphforge_storage::SemanticStorageBindings>,
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

        if !graphforge_storage::uuid_membership_index_is_fresh(&self.dir)? {
            graphforge_storage::rebuild_uuid_membership_indexes(
                &self.dir,
                graphforge_storage::UuidIndexBuildLimits::default(),
            )?;
        }
        let graph = graphforge_storage::capture_graph_files(&self.dir)?.1;
        let provenance_enabled = parent.capability("provenance")?.is_some();
        let installed_bindings = self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned");
        let participants = graph_publication_participants(
            &parent,
            graph,
            candidate_bindings.or(installed_bindings.as_ref()),
            provenance_enabled,
            receipt,
            operation_uuid,
            actor_uuid,
            recorded_at_micros,
        )?;
        drop(installed_bindings);
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
        let publication = match graphforge_storage::stage_project_generation_with_graph_tree_mode(
            root,
            &request,
            Some(self.dir.as_path()),
            self.lifecycle_mode,
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
                    self.install_property_generation(&current)?;
                }
                return Err(error);
            }
        };
        let committed = graphforge_storage::resolve_project_generation(root)?;
        if committed.generation_uuid() != published.generation_uuid {
            return Err(GfError::Storage(
                "published property authority did not resolve exact generation".into(),
            ));
        }
        self.install_property_generation(&committed)?;
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
        if !graphforge_storage::uuid_membership_index_is_fresh(&self.dir)? {
            graphforge_storage::rebuild_uuid_membership_indexes(
                &self.dir,
                graphforge_storage::UuidIndexBuildLimits::default(),
            )?;
        }
        let graph = graphforge_storage::capture_graph_files(&self.dir)?.1;
        let provenance_enabled = parent.capability("provenance")?.is_some();
        let participants = graph_publication_participants(
            &parent,
            graph,
            self.semantic_storage_bindings
                .lock()
                .expect("semantic storage binding lock poisoned")
                .as_ref(),
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
        let publication = match graphforge_storage::stage_project_generation_with_graph_tree_mode(
            root,
            &request,
            Some(self.dir.as_path()),
            self.lifecycle_mode,
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
                    self.install_property_generation(&current)?;
                }
                return Err(error);
            }
        };
        let committed = graphforge_storage::resolve_project_generation(root)?;
        if committed.generation_uuid() != published.generation_uuid {
            return Err(GfError::Storage(
                "published property authority did not resolve exact generation".into(),
            ));
        }
        self.install_property_generation(&committed)?;
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
        let _read_visibility = self.graph_visibility.read()?;
        let composition = self
            .default_composition_snapshot()
            .map(|context| self.bind_generation_storage(&context))
            .transpose()?;
        if cypher.trim().is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        let ast = graphforge_cypher::parse(cypher).map_err(GfError::from)?;
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
            let mut binder = Binder::new(
                self.ontology.clone(),
                self.runtime_catalog.clone(),
                self.ontology_mode,
            )
            .with_procedures(self.procedure_snapshot());
            if let Some((context, _, _)) = &composition {
                binder = binder.with_composition(Arc::clone(context));
            }
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

        // Pin every generation-coupled session participant while publication is
        // excluded. `install_property_generation` replaces the authenticated
        // property inventory and ordinal identity authority as one publication
        // transition; opening them without this guard could otherwise combine
        // participants from adjacent generations.
        if composition
            .as_ref()
            .is_some_and(|(_, _, moves)| !moves.is_empty())
        {
            return Err(GfError::Validation(
                "GF_SEMANTIC_LEGACY_MIGRATION_REQUIRED: run a publishing write to migrate the unambiguous legacy generation".into(),
            ));
        }
        let catalog = self.open_query_catalog(
            &self.runtime_catalog,
            composition.as_ref().map(|(_, candidate, _)| candidate),
        )?;
        let execution_mode = composition
            .as_ref()
            .map_or(self.ontology_mode, |(context, _, _)| {
                Self::composition_execution_mode(context)
            });
        let adjacency_provider = if execution_mode == self.ontology_mode {
            self.adjacency_provider_for_session()
        } else {
            Arc::new(adjacency_provider_for_graph(
                &self.dir,
                execution_mode,
                self.property_inventory_for_session(),
            )?)
        };
        let session = ExecutionSession::new_with_target_provider_resources_and_identity(
            catalog,
            self.ontology.clone(),
            self.dir.clone(),
            execution_mode,
            adjacency_provider,
            Some(Arc::clone(&self.ordinal_identities)),
            &self.session_resource_config(),
        )?;
        let session = if self.read_only {
            session.restrict_to_reads()
        } else {
            session
        };

        // Build the stream on the instance's long-lived runtime so the tasks it
        // spawns (repartition/coalesce) outlive this call — they are dropped
        // only when the `GraphForge` is. `block_on` drives the construction
        // inside that runtime's context.
        let stream = self.block_on(async { session.execute_plan_stream(&plan, params).await })?;
        // Admission is intentionally released after stream construction: the
        // stream is demand-driven and may outlive this call; holding the slot
        // for the full consumer lifetime would serialize all streaming clients.
        drop(admission);
        Ok(self.graph_visibility.health.guard_stream(shape_stream(
            stream,
            self.ontology_mode,
            self.ontology.as_ref(),
        )))
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
            let capture_session = graphforge_exec::demand::bound_capture_session();
            std::thread::scope(|s| {
                s.spawn(|| {
                    graphforge_exec::demand::set_bound_capture_session(capture_session);
                    handle.block_on(fut)
                })
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

        let cleanup_result =
            self.adjacency_provider_for_session()
                .reset_graph(|| -> Result<(), GfError> {
                    let entries = std::fs::read_dir(&self.dir).map_err(|e| {
                        GfError::Storage(format!("failed to read in-memory project: {e}"))
                    })?;
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
                });

        // These registries describe the fixture, not only its remaining files.
        // Reset them even when filesystem cleanup is partial so callers never
        // observe a stale catalog or procedure registry after `clear()` returns.
        *self.runtime_catalog.lock().expect("runtime catalog lock") = RuntimeCatalog::new();
        self.procedures
            .lock()
            .expect("procedure registry lock")
            .clear();
        self.adjacency_provider_for_session().invalidate();
        cleanup_result
    }

    fn algorithm_label(
        &self,
        label: &str,
        verb: &str,
    ) -> Result<(graphforge_value::EntityTypeSelection, String), GfError> {
        self.graph_visibility.health.check()?;
        if label.is_empty() || label.trim() != label || label.chars().any(char::is_control) {
            return Err(GfError::Validation(format!(
                "invalid {verb} label {label:?}"
            )));
        }
        let label_id = match self
            .ontology
            .as_ref()
            .and_then(|ontology| ontology.entity_type_id(label))
        {
            Some(id) => graphforge_value::EntityTypeSelection::Known(
                graphforge_value::EntityTypeId::ontology(id)
                    .map_err(|error| GfError::Validation(error.to_string()))?,
            ),
            None => self
                .runtime_catalog
                .lock()
                .expect("runtime catalog poisoned")
                .entity_type_names_with_ids()
                .find_map(|(id, name)| {
                    (name == label).then_some(graphforge_value::EntityTypeSelection::Known(
                        graphforge_value::EntityTypeId::runtime(id),
                    ))
                })
                .unwrap_or(graphforge_value::EntityTypeSelection::Missing),
        };
        let stem = if matches!(self.ontology_mode, OntologyMode::Exploratory) {
            "_untyped".to_owned()
        } else {
            label.to_owned()
        };
        Ok((label_id, stem))
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
            *self
                .adjacency_provider
                .write()
                .expect("adjacency provider lock poisoned") =
                Arc::new(adjacency_provider_for_graph(
                    &self.dir,
                    self.ontology_mode,
                    self.property_inventory_for_session(),
                )?);
        }
        Ok(())
    }

    /// Execute `cypher` and write the result to a Parquet file at `path`.
    ///
    /// This compatibility wrapper uses the bounded streaming sink with default
    /// limits. The write is atomic: a sibling temporary file is published only
    /// after execution, writer finalization, and file sync all succeed.
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
        self.execute_to_result_sink_with_params(
            cypher,
            params,
            path,
            ResultSinkFormat::Parquet,
            &ResultSinkOptions::default(),
            None,
        )
        .map(|_| ())
    }

    /// Stream a query into an atomic Parquet result with explicit limits and
    /// optional cooperative cancellation.
    pub fn execute_to_parquet_stream_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        path: &str,
        options: &ResultSinkOptions,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ResultSinkReceipt, GfError> {
        self.execute_to_result_sink_with_params(
            cypher,
            params,
            path,
            ResultSinkFormat::Parquet,
            options,
            cancellation,
        )
    }

    /// Stream a query into an atomic Arrow IPC stream file with explicit limits
    /// and optional cooperative cancellation.
    pub fn execute_to_arrow_ipc_stream_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        path: &str,
        options: &ResultSinkOptions,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ResultSinkReceipt, GfError> {
        self.execute_to_result_sink_with_params(
            cypher,
            params,
            path,
            ResultSinkFormat::ArrowIpc,
            options,
            cancellation,
        )
    }

    fn execute_to_result_sink_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        path: &str,
        format: ResultSinkFormat,
        options: &ResultSinkOptions,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ResultSinkReceipt, GfError> {
        cancellation.map_or(Ok(()), CancellationToken::checkpoint)?;
        let stream = self.execute_stream_with_params(cypher, params)?;
        let schema = stream.schema();
        let result = self.block_on(async {
            graphforge_io::sink_record_batch_stream_observed(
                stream,
                schema,
                std::path::Path::new(path),
                format,
                options,
                || cancellation.is_some_and(CancellationToken::is_cancelled),
                |path, file| {
                    let Some(allocation) = &self.allocation_operation else {
                        return Ok(());
                    };
                    let path = if path.is_absolute() {
                        path.to_path_buf()
                    } else {
                        std::env::current_dir()
                            .map_err(|error| error.to_string())?
                            .join(path)
                    };
                    match file {
                        Some(file) => allocation.replace_file_at(&path, file),
                        None => allocation.remove_file_at(&path),
                    }
                    .map_err(|error| error.to_string())
                },
            )
            .await
            .map_err(|error| {
                if error.phase == "cancelled" {
                    GfError::Api {
                        code: ApiErrorCode::Cancelled,
                        message: error.to_string(),
                    }
                } else {
                    GfError::Storage(error.to_string())
                }
            })
        })?;
        Ok(result)
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
        // Preserve the established foreign-DataFusion coercion/placeholder
        // classification without discarding the lowering diagnostic (#1018).
        GfError::Lowering(error @ LoweringError::UnsupportedExpr(_))
            if is_public_execution_plan_failure(&error.to_string()) =>
        {
            GfError::LoweringExecution(error)
        }
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
    GfError::from_bind_errors(errs)
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
                        diagnostics: Vec::new(),
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
            let capture_session = graphforge_exec::demand::bound_capture_session();
            std::thread::scope(|s| {
                s.spawn(|| {
                    graphforge_exec::demand::set_bound_capture_session(capture_session);
                    handle.block_on(fut)
                })
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

fn property_inventory_for_hydrated_generation(
    generation: &ResolvedProjectGeneration,
    hydrated_root: &Path,
) -> Result<Arc<graphforge_storage::AuthenticatedPropertyInventory>, GfError> {
    let inventory = generation.graph_files_inventory()?;
    let has_deltas = match inventory.as_ref() {
        Some(inventory) => !graphforge_storage::list_delta_runs(
            inventory,
            graphforge_storage::GraphDeltaJournalLimits::default(),
        )?
        .is_empty(),
        None => false,
    };
    // Snapshot-only generations have no graph-files inventory; hydration has
    // authenticated their snapshot payload into this private workspace.
    let admitted = if has_deltas || inventory.is_none() {
        let (materialized, _) = graphforge_storage::capture_graph_files(hydrated_root)?;
        graphforge_storage::AuthenticatedPropertyInventory::from_materialized_inventory(
            generation,
            hydrated_root,
            materialized,
        )?
    } else {
        graphforge_storage::AuthenticatedPropertyInventory::from_resolved_generation(generation)?
    };
    Ok(Arc::new(admitted))
}

fn ordinal_identity_handle(
    generation: &ResolvedProjectGeneration,
    graph_root: &Path,
) -> Result<Option<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>, GfError> {
    let Some(authority) = generation.authenticated_v4_ordinal_authority()? else {
        return Ok(None);
    };
    match authority
        .open(
            graph_root,
            graphforge_storage::V4OrdinalIdentityLimits::default(),
        )
        .map_err(|error| GfError::Storage(error.to_string()))?
    {
        graphforge_storage::V4OrdinalIdentityOpen::Ready(handle) => Ok(Some(*handle)),
        graphforge_storage::V4OrdinalIdentityOpen::RebuildRequired { found_version } => {
            Err(GfError::Validation(format!(
                "selected graph generation requires ordinal identity rebuild from version {found_version}"
            )))
        }
    }
}

fn ordinal_identity_resolver(
    generation: &ResolvedProjectGeneration,
    graph_root: &Path,
) -> Result<Arc<graphforge_exec::V4OrdinalIdentityResolver>, GfError> {
    Ok(Arc::new(graphforge_exec::V4OrdinalIdentityResolver::new(
        ordinal_identity_handle(generation, graph_root)?,
    )))
}

fn adjacency_provider_for_graph(
    dir: &Path,
    mode: OntologyMode,
    inventory: Arc<graphforge_storage::AuthenticatedPropertyInventory>,
) -> Result<graphforge_exec::PersistentAdjacencyProvider, GfError> {
    let provider = graphforge_exec::PersistentAdjacencyProvider::new(dir.to_path_buf(), mode)
        .with_inventory(inventory);
    // A pinned view must not mutate its generation. Writable workspaces also
    // have concurrent readers that enumerate graph files, so lazy build temps
    // must stay outside those trees. Each provider owns a unique cache root.
    let artifacts = tempfile::Builder::new()
        .prefix("graphforge-adjacency-cache-")
        .tempdir()
        .map_err(|error| GfError::Storage(format!("cannot create adjacency cache: {error}")))?;
    Ok(provider.with_rebuild_root(artifacts))
}

fn create_graph_workspace() -> Result<Arc<tempfile::TempDir>, GfError> {
    tempfile::Builder::new()
        .prefix("graphforge-graph-workspace-")
        .tempdir()
        .map(Arc::new)
        .map_err(|error| GfError::Storage(format!("failed to create graph workspace: {error}")))
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
        validate_graph_files_snapshot(&files)?;
        if matches!(
            files.record_version,
            graphforge_storage::GRAPH_FILES_V2_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
        ) {
            let inventory = generation
                .graph_files_inventory()?
                .ok_or_else(|| GfError::Validation("compact graph root disappeared".into()))?;
            return hydrate_compact_graph_workspace(generation, &inventory);
        }
        let inventory = graphforge_storage::decode_inventory(&files.bytes)?;
        let tree = generation.graph_tree_root();
        graphforge_storage::verify_graph_tree(&tree, &inventory)?;
        let has_authoritative_deltas = !graphforge_storage::list_delta_runs(
            &inventory,
            graphforge_storage::GraphDeltaJournalLimits::default(),
        )?
        .is_empty();
        if has_authoritative_deltas {
            let workspace = Arc::new(
                tempfile::Builder::new()
                    .prefix("graphforge-graph-replay-")
                    .tempdir()
                    .map_err(|error| {
                        GfError::Storage(format!(
                            "failed to create graph replay workspace: {error}"
                        ))
                    })?,
            );
            let (evidence, _replay) = graphforge_storage::materialize_replayed_graph_tree(
                &tree,
                &inventory,
                workspace.path(),
                graphforge_storage::GraphDeltaJournalLimits::default(),
            )?;
            return Ok((workspace.path().to_path_buf(), workspace, evidence));
        }
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
        let workspace = create_graph_workspace()?;
        let evidence =
            graphforge_storage::materialize_graph_tree(&tree, &inventory, workspace.path())?;
        return Ok((workspace.path().to_path_buf(), workspace, evidence));
    }

    let workspace = create_graph_workspace()?;
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

fn validate_graph_files_snapshot(
    files: &graphforge_storage::ProjectParticipantSnapshot,
) -> Result<(), GfError> {
    if files.capability_version != graphforge_storage::GRAPH_CAPABILITY_VERSION
        || !matches!(
            files.record_version,
            graphforge_storage::GRAPH_FILES_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_V2_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_MAPPED_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
        )
        || files.encoding != "json"
    {
        return Err(GfError::Validation(
            "unsupported graph files participant contract".into(),
        ));
    }
    Ok(())
}

fn hydrate_compact_graph_workspace(
    generation: &ResolvedProjectGeneration,
    inventory: &graphforge_storage::GraphFilesInventory,
) -> Result<
    (
        PathBuf,
        Arc<tempfile::TempDir>,
        graphforge_storage::GraphFilesOpenEvidence,
    ),
    GfError,
> {
    let workspace = Arc::new(
        tempfile::Builder::new()
            .prefix("graphforge-graph-workspace-")
            .tempdir()
            .map_err(|error| {
                GfError::Storage(format!("failed to create graph workspace: {error}"))
            })?,
    );
    let evidence = materialize_compact_graph_target(generation, inventory, workspace.path())?;
    Ok((workspace.path().to_path_buf(), workspace, evidence))
}

fn materialize_compact_graph_target(
    generation: &ResolvedProjectGeneration,
    inventory: &graphforge_storage::GraphFilesInventory,
    target: &std::path::Path,
) -> Result<graphforge_storage::GraphFilesOpenEvidence, GfError> {
    let has_authoritative_deltas = !graphforge_storage::list_delta_runs(
        inventory,
        graphforge_storage::GraphDeltaJournalLimits::default(),
    )?
    .is_empty();
    if !has_authoritative_deltas {
        return graphforge_storage::materialize_graph_objects(
            generation.container_root(),
            inventory,
            target,
        );
    }
    let source = tempfile::Builder::new()
        .prefix("graphforge-graph-cas-source-")
        .tempdir()
        .map_err(|error| {
            GfError::Storage(format!(
                "failed to create graph CAS source workspace: {error}"
            ))
        })?;
    let reused = graphforge_storage::materialize_graph_objects(
        generation.container_root(),
        inventory,
        source.path(),
    )?;
    let (copied, _replay) = graphforge_storage::materialize_replayed_graph_tree(
        source.path(),
        inventory,
        target,
        graphforge_storage::GraphDeltaJournalLimits::default(),
    )?;
    let evidence = graphforge_storage::GraphFilesOpenEvidence {
        strategy: graphforge_storage::GraphFilesOpenStrategy::PrivateMaterialize,
        files_validated: reused
            .files_validated
            .checked_add(copied.files_validated)
            .ok_or_else(|| GfError::Storage("hydration validated-file count overflows".into()))?,
        bytes_validated: reused
            .bytes_validated
            .checked_add(copied.bytes_validated)
            .ok_or_else(|| GfError::Storage("hydration validated-byte count overflows".into()))?,
        files_copied: copied.files_copied,
        bytes_copied: copied.bytes_copied,
        files_opened_in_place: 0,
        files_reused: reused.files_reused,
        bytes_reused: reused.bytes_reused,
        application_read_bytes: reused
            .application_read_bytes
            .checked_add(copied.application_read_bytes)
            .ok_or_else(|| GfError::Storage("hydration read byte count overflows".into()))?,
        application_read_calls: reused
            .application_read_calls
            .checked_add(copied.application_read_calls)
            .ok_or_else(|| GfError::Storage("hydration read call count overflows".into()))?,
        application_write_bytes: reused
            .application_write_bytes
            .checked_add(copied.application_write_bytes)
            .ok_or_else(|| GfError::Storage("hydration write byte count overflows".into()))?,
        application_write_calls: reused
            .application_write_calls
            .checked_add(copied.application_write_calls)
            .ok_or_else(|| GfError::Storage("hydration write call count overflows".into()))?,
        fsync_calls: reused
            .fsync_calls
            .checked_add(copied.fsync_calls)
            .ok_or_else(|| GfError::Storage("hydration fsync count overflows".into()))?,
        file_fsync_calls: reused
            .file_fsync_calls
            .checked_add(copied.file_fsync_calls)
            .ok_or_else(|| GfError::Storage("hydration file barrier count overflows".into()))?,
        directory_fsync_calls: reused
            .directory_fsync_calls
            .checked_add(copied.directory_fsync_calls)
            .ok_or_else(|| {
                GfError::Storage("hydration directory barrier count overflows".into())
            })?,
    };
    Ok(evidence)
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
    if let Some(files) = generation.participant_snapshot(
        graphforge_storage::GRAPH_CAPABILITY_ID,
        graphforge_storage::GRAPH_FILES_FAMILY,
    )? {
        validate_graph_files_snapshot(&files)?;
        if matches!(
            files.record_version,
            graphforge_storage::GRAPH_FILES_V2_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
        ) {
            let inventory = generation
                .graph_files_inventory()?
                .ok_or_else(|| GfError::Validation("compact graph root disappeared".into()))?;
            materialize_compact_graph_target(generation, &inventory, target)?;
        } else {
            let inventory = graphforge_storage::decode_inventory(&files.bytes)?;
            graphforge_storage::materialize_graph_tree(
                &generation.graph_tree_root(),
                &inventory,
                target,
            )?;
        }
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
    let composition = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
        )?
        .map(|snapshot| {
            graphforge_storage::WorkspaceOntologyComposition::from_canonical_json(&snapshot.bytes)
        })
        .transpose()?;
    if let Some(composition) = &composition {
        let composition_mode = match composition.profile_default {
            graphforge_ontology::ActivationMode::Exploratory => {
                graphforge_storage::WorkspaceOntologyMode::None
            }
            graphforge_ontology::ActivationMode::Advisory => {
                graphforge_storage::WorkspaceOntologyMode::Advisory
            }
            graphforge_ontology::ActivationMode::Strict => {
                graphforge_storage::WorkspaceOntologyMode::Strict
            }
        };
        if composition_mode != configuration.ontology_mode {
            return Err(GfError::Validation(
                "workspace composition and configuration modes disagree".into(),
            ));
        }
    } else if ontology_record.mode != configuration.ontology_mode {
        return Err(GfError::Validation(
            "workspace ontology and configuration modes disagree".into(),
        ));
    }
    let mode = configuration.ontology_mode.execution_mode();
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

fn load_composition_binding(
    generation: &ResolvedProjectGeneration,
) -> Result<Option<Arc<CompositionBindingContext>>, GfError> {
    let Some(snapshot) = generation.participant_snapshot(
        graphforge_storage::WORKSPACE_CAPABILITY_ID,
        graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
    )?
    else {
        return Ok(None);
    };
    let expected_schema: [u8; 32] = Sha256::digest(
        format!(
            "workspace/{}@1",
            graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
        )
        .as_bytes(),
    )
    .into();
    if snapshot.capability_version != graphforge_storage::WORKSPACE_CAPABILITY_VERSION
        || snapshot.record_version != graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_VERSION
        || snapshot.encoding != "json"
        || snapshot.schema_fingerprint != expected_schema
        || snapshot.row_count != 1
    {
        return Err(GfError::Validation(
            "unsupported workspace ontology composition participant contract".into(),
        ));
    }
    let authority =
        graphforge_storage::WorkspaceOntologyComposition::from_canonical_json(&snapshot.bytes)?;
    let compiled = authority.compile()?;
    Ok(Some(Arc::new(CompositionBindingContext::new(
        Arc::new(compiled),
        authority.bridges,
        CompositionBindingLimits::default(),
    ))))
}

#[allow(clippy::too_many_arguments)] // participant assembly carries authenticated audit context
fn graph_publication_participants(
    parent: &graphforge_storage::ResolvedProjectGeneration,
    graph: graphforge_storage::ProjectParticipant,
    semantic_bindings: Option<&graphforge_storage::SemanticStorageBindings>,
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
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "snapshot" | "files" | graphforge_storage::GRAPH_SEMANTIC_BINDINGS_FAMILY
                )
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
    if let Some(bindings) = semantic_bindings {
        participants.push(bindings.to_project_participant()?);
        let composition = participants
            .iter()
            .find(|participant| {
                participant.capability_id == "workspace"
                    && participant.record_family_id == "ontology_composition"
            })
            .ok_or_else(|| {
                GfError::Validation(
                    "semantic graph publication requires persisted composition authority".into(),
                )
            })?;
        let value: serde_json::Value = serde_json::from_slice(&composition.bytes)
            .map_err(|_| GfError::Validation("persisted composition is malformed".into()))?;
        if value
            .get("composition_fingerprint")
            .and_then(serde_json::Value::as_str)
            != Some(bindings.composition_fingerprint.as_str())
        {
            return Err(GfError::Validation(
                "semantic bindings and composition must publish at one fingerprint".into(),
            ));
        }
    }
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
        Array, FixedSizeBinaryArray, Int64Array, ListArray, StringArray, StructArray,
    };
    use arrow::datatypes::DataType;
    use std::io::Read as _;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    const ABSENT_TARGET_CHILD: &str = "tests::absent_target_open_child";
    const ABSENT_TARGET_COOKIE: &str = "graphforge-absent-target-open-v1";
    const ABSENT_TARGET_DEADLINE: Duration = Duration::from_secs(10);

    fn publish_compact_graph_workspace(project: &Path, workspace: &Path) {
        use graphforge_core::canonical::{
            CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint,
        };
        use graphforge_storage::{
            ProjectCapability, ProjectGenerationRequest, ProjectParticipant,
            ProjectParticipantEncoding, ProjectStageOutcome,
        };

        let lease = graphforge_storage::begin_graph_object_publication(project).unwrap();
        let mut state = graphforge_storage::GraphManifestState::empty();
        let (inventory, _) = graphforge_storage::capture_graph_files(workspace).unwrap();
        let mapped =
            inventory.format_version == graphforge_storage::GRAPH_FILES_MAPPED_RECORD_VERSION;
        let paths = inventory
            .files
            .into_iter()
            .map(|entry| PathBuf::from(entry.relative_path))
            .collect::<Vec<_>>();
        let (mut root, _) =
            graphforge_storage::append_graph_files_v2(&lease, workspace, &mut state, &paths, &[])
                .unwrap();
        if mapped {
            root.format_version = graphforge_storage::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION;
        }
        let participant = ProjectParticipant {
            capability_id: graphforge_storage::GRAPH_CAPABILITY_ID.into(),
            capability_version: graphforge_storage::GRAPH_CAPABILITY_VERSION,
            record_family_id: graphforge_storage::GRAPH_FILES_FAMILY.into(),
            record_version: root.format_version,
            encoding: ProjectParticipantEncoding::Json,
            schema_fingerprint: fingerprint(
                CanonicalDomain::Schema,
                CANONICAL_CONTRACT_VERSION,
                if mapped { b"graphforge-graph-files-root/4|root_node_sha256|logical_file_count|logical_byte_length|semantic-routes/1" } else { b"graphforge-graph-files-root/2|root_node_sha256|logical_file_count|logical_byte_length" },
            )
            .unwrap(),
            row_count: root.logical_file_count,
            bytes: graphforge_storage::encode_graph_files_root_v2(&root).unwrap(),
        };
        let current = graphforge_storage::resolve_project_generation(project).unwrap();
        let capabilities = current
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect();
        let mut participants = current
            .participant_snapshots()
            .unwrap()
            .into_iter()
            .filter(|snapshot| {
                snapshot.capability_id != graphforge_storage::GRAPH_CAPABILITY_ID
                    || snapshot.record_family_id != graphforge_storage::GRAPH_FILES_FAMILY
            })
            .map(|snapshot| ProjectParticipant {
                capability_id: snapshot.capability_id,
                capability_version: snapshot.capability_version,
                record_family_id: snapshot.record_family_id,
                record_version: snapshot.record_version,
                encoding: match snapshot.encoding.as_str() {
                    "parquet" => ProjectParticipantEncoding::Parquet,
                    "arrow" => ProjectParticipantEncoding::Arrow,
                    "json" => ProjectParticipantEncoding::Json,
                    other => panic!("unsupported fixture participant encoding {other}"),
                },
                schema_fingerprint: snapshot.schema_fingerprint,
                row_count: snapshot.row_count,
                bytes: snapshot.bytes,
            })
            .collect::<Vec<_>>();
        participants.push(participant);
        let request = ProjectGenerationRequest {
            transaction_uuid: uuid::Uuid::new_v4(),
            generation_uuid: uuid::Uuid::new_v4(),
            capabilities,
            participants,
        };
        let ProjectStageOutcome::Staged(staged) =
            graphforge_storage::stage_project_generation(project, &request).unwrap()
        else {
            panic!("compact graph publication unexpectedly replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish_with_graph_objects(&lease)
            .unwrap();
    }

    fn spawn_absent_target_child(parent: &Path, child_id: &str) -> Child {
        Command::new(std::env::current_exe().expect("absent-target current test executable"))
            .args(["--exact", ABSENT_TARGET_CHILD, "--nocapture"])
            .env("GF_ABSENT_TARGET_COOKIE", ABSENT_TARGET_COOKIE)
            .env("GF_ABSENT_TARGET_PARENT", parent)
            .env("GF_ABSENT_TARGET_CHILD_ID", child_id)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|error| panic!("absent-target child={child_id} spawn error={error}"))
    }

    fn wait_for_paths(paths: &[PathBuf], phase: &str) {
        let deadline = Instant::now() + ABSENT_TARGET_DEADLINE;
        while paths.iter().any(|path| !path.is_file()) {
            assert!(
                Instant::now() < deadline,
                "phase={phase} timed out waiting for subprocess barrier"
            );
            std::thread::yield_now();
        }
    }

    fn wait_for_absent_target_child(mut child: Child, child_id: &str) -> uuid::Uuid {
        let deadline = Instant::now() + ABSENT_TARGET_DEADLINE;
        let status = loop {
            if let Some(status) = child.try_wait().expect("absent-target child try_wait") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("absent-target child={child_id} timed out");
            }
            std::thread::yield_now();
        };
        let mut stdout = String::new();
        child
            .stdout
            .take()
            .expect("absent-target child stdout")
            .read_to_string(&mut stdout)
            .expect("read absent-target child stdout");
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .expect("absent-target child stderr")
            .read_to_string(&mut stderr)
            .expect("read absent-target child stderr");
        assert!(
            status.success(),
            "absent-target child={child_id} failed: status={status} stdout={stdout:?} stderr={stderr:?}"
        );
        let generation = stdout
            .lines()
            .find_map(|line| line.strip_prefix("GF_ABSENT_TARGET_UUID "))
            .unwrap_or_else(|| {
                panic!(
                    "absent-target child={child_id} omitted generation marker: stdout={stdout:?}"
                )
            });
        uuid::Uuid::parse_str(generation).unwrap_or_else(|error| {
            panic!("absent-target child={child_id} invalid generation={generation:?}: {error}")
        })
    }

    #[test]
    fn read_only_write_explanations_preserve_catalog_and_generation() {
        fn files(root: &std::path::Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
            fn visit(
                root: &std::path::Path,
                dir: &std::path::Path,
                out: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>,
            ) {
                for entry in std::fs::read_dir(dir).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        visit(root, &path, out);
                    } else {
                        out.insert(
                            path.strip_prefix(root).unwrap().to_path_buf(),
                            std::fs::read(path).unwrap(),
                        );
                    }
                }
            }
            let mut out = std::collections::BTreeMap::new();
            visit(root, root, &mut out);
            out
        }
        let project = tempfile::TempDir::new().unwrap();
        let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
        graph.execute("CREATE (:Person {name:'original'})").unwrap();
        let resolved = graph.generation_for_read().unwrap();
        let view = GraphForge::open_resolved_with_lifecycle_mode(
            resolved.container_root().to_path_buf(),
            resolved,
            true,
            graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral,
        )
        .unwrap();
        let before_catalog = view.runtime_catalog.lock().unwrap().to_record_batch();
        let before_generation = view.generation_for_read().unwrap().generation_uuid();
        let before_files = files(&view.dir);
        assert!(
            view.explain("CREATE (:NewLabel {fresh:1})")
                .unwrap()
                .contains("GraphCreateExec")
        );
        assert_eq!(
            view.runtime_catalog.lock().unwrap().to_record_batch(),
            before_catalog
        );
        for (query, operator) in [
            ("CREATE (:Person {name:'new'})", "GraphCreateExec"),
            ("MATCH (n:Person) SET n.name = 'changed'", "GraphSetExec"),
            ("MATCH (n:Person) REMOVE n.name", "GraphRemoveExec"),
            ("MATCH (n:Person) DELETE n", "GraphDeleteExec"),
        ] {
            assert!(view.explain(query).unwrap().contains(operator));
            for stage in [
                ExplainStage::Ast,
                ExplainStage::GraphIr,
                ExplainStage::LogicalPlan,
            ] {
                assert!(!view.explain_stage(query, stage).unwrap().is_empty());
            }
            assert!(
                view.explain_stage(query, ExplainStage::PhysicalPlan)
                    .unwrap()
                    .contains(operator)
            );
        }
        for stage in [
            ExplainStage::GraphIr,
            ExplainStage::LogicalPlan,
            ExplainStage::PhysicalPlan,
        ] {
            view.explain_stage("CREATE (:NewStageLabel {stageFresh:1})", stage)
                .unwrap();
        }
        assert_eq!(
            view.runtime_catalog.lock().unwrap().to_record_batch(),
            before_catalog
        );
        assert_eq!(
            view.generation_for_read().unwrap().generation_uuid(),
            before_generation
        );
        assert_eq!(files(&view.dir), before_files);
        // Execution binding observes runtime names, unlike snapshot-only EXPLAIN.
        // Rejection must nevertheless preserve canonical data and authority.
        for query in [
            "CREATE (:Person {name:'new'})",
            "MATCH (n:Person) SET n.name='changed'",
            "MATCH (n:Person) REMOVE n.name",
            "MATCH (n:Person) DELETE n",
        ] {
            assert!(view.execute(query).is_err());
        }
        assert_eq!(files(&view.dir), before_files);
        assert_eq!(
            view.generation_for_read().unwrap().generation_uuid(),
            before_generation
        );
        assert_eq!(
            view.execute_read_only("MATCH (n:Person) RETURN n.name")
                .unwrap()
                .batches[0]
                .num_rows(),
            1
        );
    }

    #[test]
    fn facade_debug_and_procedure_width_contracts_are_exact() {
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
    fn compact_graph_root_reopens_through_ordinary_api_and_rematerializes() {
        let project = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
        graph
            .execute("CREATE (:Person {name: 'Ada'})")
            .expect("create compact-root fixture");
        publish_compact_graph_workspace(project.path(), &graph.dir);
        drop(graph);

        let reopened = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
        let result = reopened
            .execute("MATCH (n:Person) RETURN n.name AS name")
            .expect("ordinary query over compact-root generation");
        assert_eq!(result.stats.rows_produced, 1);
        // Mutable route authority owns a private copy; immutable payloads stay shared.
        assert_eq!(reopened.graph_open_evidence().files_copied, 1);
        assert_eq!(
            reopened.graph_open_evidence().bytes_copied,
            std::fs::metadata(reopened.dir.join("semantic-routes.json"))
                .unwrap()
                .len()
        );
        assert!(reopened.graph_open_evidence().files_reused > 0);
        assert!(!reopened.dir.join("files").exists());
        assert!(reopened.dir.join("topology").is_dir());

        let resolved = graphforge_storage::resolve_project_generation(project.path()).unwrap();
        let (read_only_dir, read_only_guard, read_only_evidence) =
            hydrate_graph_workspace(&resolved, true).unwrap();
        assert!(read_only_dir.join("topology").is_dir());
        assert!(read_only_evidence.files_reused > 0);
        assert_eq!(read_only_evidence.files_opened_in_place, 0);

        let rematerialized_owner = tempfile::tempdir().unwrap();
        let rematerialized = rematerialized_owner.path().join("workspace");
        std::fs::create_dir(&rematerialized).unwrap();
        rematerialize_graph_workspace(&resolved, &rematerialized).unwrap();
        let (expected, _) = graphforge_storage::capture_graph_files(&reopened.dir).unwrap();
        let (actual, _) = graphforge_storage::capture_graph_files(&rematerialized).unwrap();
        assert_eq!(actual, expected);

        let inventory = resolved.graph_files_inventory().unwrap().unwrap();
        let victim_entry = inventory
            .files
            .iter()
            .find(|entry| entry.byte_length > 0)
            .expect("compact fixture contains a nonempty graph object");
        let victim =
            graphforge_storage::graph_object_path(project.path(), &victim_entry.content_sha256)
                .unwrap();
        drop(read_only_guard);
        drop(reopened);
        let mut permissions = std::fs::metadata(&victim).unwrap().permissions();
        permissions.set_readonly(false);
        std::fs::set_permissions(&victim, permissions).unwrap();
        std::fs::write(&victim, vec![0_u8; victim_entry.byte_length as usize]).unwrap();
        assert!(GraphForge::new(Some(project.path().to_str().unwrap())).is_err());
    }

    #[test]
    fn compact_graph_root_replays_authoritative_deltas_into_distinct_workspace() {
        use graphforge_storage::{
            GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind, GraphDeltaPayload,
            GraphDeltaPublishRequest,
        };

        let project = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
        graph.execute("CREATE (:Person {name: 'Ada'})").unwrap();
        drop(graph);
        graphforge_storage::publish_graph_delta(
            project.path(),
            &GraphDeltaPublishRequest {
                transaction_uuid: uuid::Uuid::new_v4(),
                generation_uuid: uuid::Uuid::new_v4(),
                run_uuid: uuid::Uuid::new_v4(),
                operations: vec![GraphDeltaOp {
                    operation_uuid: uuid::Uuid::new_v4(),
                    kind: GraphDeltaOpKind::UpsertNode,
                    payload: GraphDeltaPayload::UpsertNodeV2 {
                        node_uuid: uuid::Uuid::new_v4().hyphenated().to_string(),
                        node_id: 2,
                        type_ids: vec![graphforge_value::EntityTypeId::decode(1).unwrap()],
                        created_at_micros: 2,
                        updated_at_micros: 2,
                    },
                }],
                limits: GraphDeltaJournalLimits::default(),
            },
        )
        .unwrap();
        let delta_generation =
            graphforge_storage::resolve_project_generation(project.path()).unwrap();
        assert!(delta_generation.graph_tree_root().join("deltas").is_dir());
        publish_compact_graph_workspace(project.path(), &delta_generation.graph_tree_root());
        drop(delta_generation);

        let reopened = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
        let result = reopened
            .execute("MATCH (n) RETURN count(n) AS total")
            .unwrap();
        let total = result.batches[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 2);
        assert!(!reopened.dir.join("deltas").exists());
        assert!(reopened.graph_open_evidence().files_reused > 0);
        assert!(reopened.graph_open_evidence().files_copied > 0);
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
    fn property_sessions_pin_old_inventory_and_publication_installs_new_snapshot() {
        let graph = GraphForge::new(None).expect("open ephemeral project");
        graph
            .execute("CREATE (:Person {name: 'old'})")
            .expect("publish initial property generation");
        let old = graph.property_inventory_for_session();
        let old_generation = old.generation_uuid().expect("generation-backed inventory");

        graph
            .execute("MATCH (n:Person) SET n.name = 'new' RETURN n.name")
            .expect("publish replacement property generation");
        let new = graph.property_inventory_for_session();
        let new_generation = new.generation_uuid().expect("generation-backed inventory");

        assert_ne!(old_generation, new_generation);
        assert_eq!(old.generation_uuid(), Some(old_generation));
        assert_eq!(
            *graph.current_generation_uuid.lock().unwrap(),
            new_generation
        );
        assert!(!Arc::ptr_eq(&old, &new));
    }

    #[test]
    fn concurrent_property_authority_read_never_observes_a_split_generation_pair() {
        let graph = GraphForge::new(None).expect("open ephemeral project");
        graph.execute("CREATE (:Person {name: 'old'})").unwrap();
        let old = graph.property_inventory_for_session();
        let old_uuid = old.generation_uuid().unwrap();
        graph
            .execute("MATCH (n:Person) SET n.name = 'new'")
            .unwrap();
        let new = graph.property_inventory_for_session();
        let new_uuid = new.generation_uuid().unwrap();

        for _ in 0..64 {
            *graph.property_authority.lock().unwrap() = GenerationPropertyAuthority {
                generation_uuid: old_uuid,
                inventory: Arc::clone(&old),
            };
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let authority_for_install = Arc::clone(&graph.property_authority);
            let install_barrier = Arc::clone(&barrier);
            let new_inventory = Arc::clone(&new);
            let installer = std::thread::spawn(move || {
                install_barrier.wait();
                *authority_for_install.lock().unwrap() = GenerationPropertyAuthority {
                    generation_uuid: new_uuid,
                    inventory: new_inventory,
                };
            });
            let authority_for_read = Arc::clone(&graph.property_authority);
            let read_barrier = Arc::clone(&barrier);
            let reader = std::thread::spawn(move || {
                read_barrier.wait();
                let authority = authority_for_read.lock().unwrap();
                (
                    authority.generation_uuid,
                    authority.inventory.generation_uuid(),
                )
            });
            barrier.wait();
            installer.join().unwrap();
            let observed = reader.join().unwrap();
            assert!(
                observed == (old_uuid, Some(old_uuid)) || observed == (new_uuid, Some(new_uuid)),
                "authority snapshot was split: {observed:?}"
            );
        }
    }

    #[test]
    fn streaming_and_explain_session_pins_wait_for_generation_publication() {
        use std::sync::mpsc::{self, RecvTimeoutError};
        use std::time::Duration;

        let graph = GraphForge::new(None).expect("open ephemeral project");
        graph.execute("CREATE (:Person)").expect("seed graph");

        std::thread::scope(|scope| {
            let publication = graph.graph_visibility.lock().expect("publication lock");
            let ready = std::sync::Arc::new(std::sync::Barrier::new(2));
            let (sent, received) = mpsc::channel();
            let graph = &graph;
            let child_ready = std::sync::Arc::clone(&ready);
            scope.spawn(move || {
                child_ready.wait();
                sent.send(graph.explain("MATCH (n:Person) RETURN n.node_uuid"))
                    .expect("send explain result");
            });
            ready.wait();
            assert!(matches!(
                received.recv_timeout(Duration::from_millis(100)),
                Err(RecvTimeoutError::Timeout)
            ));
            drop(publication);
            received
                .recv_timeout(Duration::from_secs(5))
                .expect("explain completes after publication")
                .expect("explain session");
        });

        std::thread::scope(|scope| {
            let publication = graph.graph_visibility.lock().expect("publication lock");
            let ready = std::sync::Arc::new(std::sync::Barrier::new(2));
            let (sent, received) = mpsc::channel();
            let graph = &graph;
            let child_ready = std::sync::Arc::clone(&ready);
            scope.spawn(move || {
                child_ready.wait();
                let result = graph
                    .execute_stream("MATCH (n:Person) RETURN n.node_uuid")
                    .map(drop);
                sent.send(result).expect("send stream result");
            });
            ready.wait();
            assert!(matches!(
                received.recv_timeout(Duration::from_millis(100)),
                Err(RecvTimeoutError::Timeout)
            ));
            drop(publication);
            received
                .recv_timeout(Duration::from_secs(5))
                .expect("stream session completes after publication")
                .expect("stream session");
        });
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
    fn absent_target_open_child() {
        if std::env::var("GF_ABSENT_TARGET_COOKIE").as_deref() != Ok(ABSENT_TARGET_COOKIE) {
            return;
        }
        let parent = PathBuf::from(
            std::env::var_os("GF_ABSENT_TARGET_PARENT")
                .expect("absent-target child canonical parent"),
        );
        let child_id =
            std::env::var("GF_ABSENT_TARGET_CHILD_ID").expect("absent-target child identifier");
        std::fs::write(parent.join(format!("ready-{child_id}")), b"ready\n")
            .expect("absent-target child publish readiness");
        wait_for_paths(&[parent.join("go")], "child-open-release");

        let root = parent.join("project");
        let graph = GraphForge::new(root.to_str()).expect("absent-target child open project");
        println!(
            "GF_ABSENT_TARGET_UUID {}",
            graph.resolved_generation.generation_uuid()
        );
    }

    #[test]
    fn concurrent_processes_open_one_absent_target_generation() {
        let fixture = tempfile::tempdir().expect("absent-target parent fixture");
        let parent = fixture.path().canonicalize().unwrap();
        let root = parent.join("project");
        assert!(!root.exists());

        let first = spawn_absent_target_child(&parent, "first");
        let second = spawn_absent_target_child(&parent, "second");
        wait_for_paths(
            &[parent.join("ready-first"), parent.join("ready-second")],
            "children-ready",
        );
        std::fs::write(parent.join("go"), b"open\n").expect("release absent-target children");

        let first_uuid = wait_for_absent_target_child(first, "first");
        let second_uuid = wait_for_absent_target_child(second, "second");
        assert_eq!(first_uuid, second_uuid);

        let current_before_reopen = std::fs::read(root.join(graphforge_storage::CURRENT_FILE))
            .expect("read CURRENT after concurrent admission");
        let current_record: serde_json::Value =
            serde_json::from_slice(&current_before_reopen).expect("CURRENT is canonical JSON");
        assert_eq!(
            current_record["generation_uuid"].as_str(),
            Some(first_uuid.hyphenated().to_string().as_str())
        );
        let generations = std::fs::read_dir(root.join("generations"))
            .expect("read admitted generations")
            .map(|entry| {
                entry
                    .expect("read admitted generation entry")
                    .file_name()
                    .into_string()
                    .expect("generation UUID is UTF-8")
            })
            .collect::<Vec<_>>();
        assert_eq!(generations, [first_uuid.to_string()]);

        let admission_locks = std::fs::read_dir(&parent)
            .expect("read canonical parent")
            .filter_map(|entry| {
                let entry = entry.expect("read canonical parent entry");
                let name = entry.file_name().into_string().ok()?;
                (name.starts_with(".graphforge-admission-") && name.ends_with(".lock"))
                    .then_some(entry.path())
            })
            .collect::<Vec<_>>();
        assert_eq!(admission_locks.len(), 1);
        assert!(
            std::fs::symlink_metadata(&admission_locks[0])
                .expect("inspect persistent admission lock")
                .file_type()
                .is_file()
        );

        let reopened = GraphForge::new(root.to_str()).expect("reopen concurrently admitted root");
        assert_eq!(reopened.resolved_generation.generation_uuid(), first_uuid);
        drop(reopened);
        assert_eq!(
            std::fs::read(root.join(graphforge_storage::CURRENT_FILE))
                .expect("read stable CURRENT after reopen"),
            current_before_reopen
        );
        assert!(admission_locks[0].is_file());
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

    #[test]
    fn clear_repopulation_does_not_reuse_private_adjacency_at_same_generation() {
        use graphforge_exec::AdjacencyProvider;
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute("CREATE (:Person)-[:KNOWS]->(:Person)")
            .unwrap();
        let retained = graph
            .adjacency_provider_for_session()
            .adjacency("KNOWS", graphforge_ir::Direction::Out)
            .unwrap();
        let generation =
            graphforge_storage::generation::read_topology_generation(&graph.dir).unwrap();
        graph.clear().unwrap();
        graph
            .execute("CREATE (:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person)")
            .unwrap();
        assert_eq!(
            graphforge_storage::generation::read_topology_generation(&graph.dir).unwrap(),
            generation
        );
        let count = graph
            .execute("MATCH ()-[r]->() RETURN count(r) AS n")
            .unwrap();
        let count = count.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 2);
        assert_eq!(
            graph
                .execute("MATCH ()-[r:KNOWS]->() RETURN r")
                .unwrap()
                .batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            2
        );
        // The old view has not touched a shard yet. Its first lazy read must
        // still see the old graph after a different CSR has been published.
        assert_eq!(retained.neighbors(1).unwrap().len(), 1);
        assert!(retained.neighbors(2).unwrap().is_empty());
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
        for stage in [
            ExplainStage::GraphIr,
            ExplainStage::LogicalPlan,
            ExplainStage::PhysicalPlan,
        ] {
            let error = gf
                .explain_stage("MATCH (n:NoSuchLabel) RETURN n.node_uuid AS u", stage)
                .unwrap_err();
            assert_eq!(error.code(), err.code());
            assert_eq!(error.to_string(), err.to_string());
            let GfError::Bind { diagnostics, .. } = error else {
                panic!("strict binder diagnostic lost")
            };
            assert!(
                diagnostics
                    .iter()
                    .any(|error| error.kind == graphforge_core::BindErrorKind::UnknownLabel)
            );
        }
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
            assert_eq!(err.code(), "GF_VALIDATION");
            assert!(
                matches!(err, GfError::Lowering(LoweringError::InvalidType(_))),
                "expected typed InvalidType validation error for {query}, got {err:?}"
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
        let query = "CALL test.double(2) YIELD out AS value RETURN value";
        for stage in [
            ExplainStage::GraphIr,
            ExplainStage::LogicalPlan,
            ExplainStage::PhysicalPlan,
        ] {
            assert!(!gf.explain_stage(query, stage).unwrap().is_empty());
        }
        assert!(gf.explain(query).unwrap().contains("PhysicalPlan"));
        let other = GraphForge::new(None).unwrap();
        let error = other
            .explain_stage(query, ExplainStage::GraphIr)
            .unwrap_err();
        assert_eq!(error.code(), "GF_PARSE");
        assert!(matches!(error, GfError::Bind { .. }));
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

    #[test]
    fn public_storage_attribution_receipt_is_identity_free_and_reconciled() {
        let project = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(project.path().to_str()).unwrap();
        let receipt = graph.storage_attribution_receipt().unwrap();
        receipt.validate_reconciliation().unwrap();
        let encoded = serde_json::to_string(&receipt).unwrap();
        assert!(!encoded.contains("generation_uuid"));
        assert!(!encoded.contains("sha256"));
        assert!(!encoded.contains(project.path().to_string_lossy().as_ref()));
    }
}

mod portable_oci;

pub use graphforge_core::portable::{
    PortableV2OciAuthenticityPolicy, PortableV2OciPhase, PortableV2OciProgress,
    PortableV2OciPullReceipt, PortableV2OciReference, PortableV2OciSignatureMaterial,
    PortableV2OciSignatureState,
};
pub use graphforge_portable_oci::{HttpOciRegistry, MemoryOciRegistry, PortableV2OciRegistry};
pub use portable_oci::{PortableV2OciPublishRequest, PortableV2OciPullRequest};

mod allocation_diagnostics;
#[doc(hidden)]
pub use allocation_diagnostics::StorageAllocationDiagnostics;
