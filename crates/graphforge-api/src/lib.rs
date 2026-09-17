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
//! Query binding, execution, streaming, and result sinks are owned by the private
//! `query_execution` module; `result_shaping` owns public Arrow schema shaping.
//! `workspace_hydration` owns authenticated read authority; `graph_publication`
//! owns publication and reset; `runtime_ownership` owns runtime and query admission.
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

#[cfg(test)]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use graphforge_core::GraphIdentity;
pub use graphforge_io::{
    ResultSinkFormat, ResultSinkOptions, ResultSinkProgress, ResultSinkReceipt,
};
use graphforge_ir::{
    CompositionBindingContext, CompositionBindingLimits, ProcedureRegistry, RuntimeCatalog,
};
pub use graphforge_ontology::{
    ActivationMode, ActivationRecord, ActivationScope, BridgeDocument, BridgeExportFormat,
    BridgeImportFormatHint, BridgeSelector, BridgeSetId, ExportFormat, ImportFormatHint,
    ModuleSelector, OntologyModuleId, SymbolKind,
};
use graphforge_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};
use graphforge_storage::ResolvedProjectGeneration;
pub use graphforge_storage::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ConstructionChunkReceipt,
    GraphConstructionBudgets, GraphConstructionEvidence, GraphConstructionState,
};
#[cfg(test)]
use sha2::Digest;

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
mod graph_publication;
mod graph_snapshot;
#[cfg(test)]
use graph_publication::participant_encoding;
use graph_publication::{persist_runtime_catalog, system_time_micros};
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
#[cfg(test)]
mod permanent_parquet_test_support;
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
mod query_execution;
mod repository;
mod resource_policy;
mod runtime_ownership;
pub use runtime_ownership::RuntimeGuard;
use runtime_ownership::{OwnedRuntime, build_runtime};
mod result_shaping;
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
mod workspace_hydration;
#[cfg(test)]
use workspace_hydration::read_runtime_catalog;
pub(crate) use workspace_hydration::rematerialize_graph_workspace;
use workspace_hydration::{
    GenerationPropertyAuthority, GraphWorkspace, PreparedGenerationReadAuthority,
    adjacency_provider_for_graph, decode_runtime_catalog, hydrate_graph_workspace,
    load_composition_binding, load_runtime_catalog, load_workspace_ontology,
    ordinal_identity_handle, ordinal_identity_resolver,
    property_and_graph_inventory_for_hydrated_generation,
    property_inventory_for_hydrated_generation,
};
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
pub use graphforge_storage::{ProjectVerifyReport, VerifyCategoryCounts};
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
    GraphImportSession, ImportCallTiming, ImportConstructionEvidence, ImportOperationTimings,
    ImportPhase, ImportProgress, ImportSessionLimits, ImportSourceKind, PublicationWorkComponents,
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
    /// Decoded epistemic ledgers cached for one immutable read generation.
    /// See `epistemic_snapshot::EpistemicLedgerCache` for the invalidation
    /// contract: keyed by `(generation_uuid, manifest_sha256)`, so a publish
    /// (which always mints a new `generation_uuid`) is a guaranteed cache
    /// miss rather than a stale hit.
    epistemic_ledger_cache: Mutex<Option<epistemic_snapshot::EpistemicLedgerCache>>,
    /// Exact generation-pinned ordinal destination identity authority shared
    /// by every fixed-hop session.
    ordinal_identities: Arc<graphforge_exec::V4OrdinalIdentityResolver>,
    /// Injected durable-write UTC microsecond clock.
    clock: Mutex<Arc<dyn Fn() -> Result<i64, GfError> + Send + Sync>>,
    /// Project directory backing topology/properties Parquet files. For an
    /// instance this is a private mutable workspace materialized from the pinned
    /// graph generation (file-backed tree or legacy snapshot).
    /// Readers capture one owner under graph visibility before publication may rotate it.
    workspace_guard: Arc<RwLock<GraphWorkspace>>,
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
            .field("dir", &self.dir())
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

    /// Explicitly re-authenticate the retained store on demand.
    ///
    /// This is the read-only, administrative check #1384 moved "is the
    /// store still intact" behind, so it never runs as part of ingest or
    /// construction. It re-reads and re-hashes the selected generation's
    /// manifest, every declared participant, and every retained
    /// content-addressed graph payload object, reporting what was checked,
    /// what passed, and what failed. It never mutates, repairs, or
    /// publishes anything.
    pub fn verify_project_store(&self) -> Result<graphforge_storage::ProjectVerifyReport, GfError> {
        let generation = self.generation_for_read()?;
        graphforge_storage::verify_project_store(&generation)
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
            epistemic_ledger_cache: Mutex::new(None),
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
            workspace_guard: Arc::new(RwLock::new(GraphWorkspace {
                dir,
                _owner: workspace,
            })),
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
        let (property_inventory, hydrated_inventory) =
            property_and_graph_inventory_for_hydrated_generation(&resolved_generation, &dir)?;
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
            bindings.validate_physical_routes_with_inventory(&dir, Some(&hydrated_inventory))?;
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
            epistemic_ledger_cache: Mutex::new(None),
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
            workspace_guard: Arc::new(RwLock::new(GraphWorkspace {
                dir,
                _owner: workspace,
            })),
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
                    &self.dir(),
                    self.ontology_mode,
                    self.property_inventory_for_session(),
                )?);
        }
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
        let before_files = files(&view.dir());
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
        assert_eq!(files(&view.dir()), before_files);
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
        assert_eq!(files(&view.dir()), before_files);
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
        assert!(gf.dir().is_dir());
        assert!(gf.dir().file_name().is_some_and(|name| {
            name.to_string_lossy()
                .starts_with("graphforge-graph-workspace-")
        }));
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

    #[test]
    fn public_verify_project_store_reports_a_clean_fresh_project() {
        let project = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(project.path().to_str()).unwrap();
        let report = graph.verify_project_store().unwrap();
        assert!(report.ok);
        assert_eq!(report.catalog_and_participants.objects_failed, 0);
        assert_eq!(report.content_addressed_objects.objects_checked, 0);
        assert_eq!(report.content_addressed_objects.objects_failed, 0);
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
