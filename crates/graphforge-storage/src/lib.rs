//! GraphForge `StorageProvider` trait, canonical Arrow schemas, and Parquet backend.
//!
//! - [`schemas`] — Arrow schema constants for every Parquet file
//! - [`catalog`] — DataFusion `TableProvider` / `CatalogProvider` implementations (#572)
//! - [`writer`] — buffered Parquet write path ([`GraphWriter`]) (#579)
//! - [`mutator`] — in-place rewrite primitives for `DELETE`/`DETACH DELETE` (#740)
//! - [`staging`] — temp-file + atomic-rename Parquet commit ([`RewriteBatch`]) (#790)
//! - [`adjacency`] — on-disk CSR format for the derived adjacency index (#758, ADR 0005)
//! - [`generation`] — `topology_generation` counter, the staleness signal for derived indexes (#759)
//! - [`search_manifest`] / [`search_publication`] — shared search search freshness and atomic publication
#![forbid(unsafe_code)]

mod durable_rewrite;
mod file_lock;
pub use durable_rewrite::AuxiliaryReceipt;
#[doc(hidden)]
pub mod filesystem_admission;

pub mod adjacency;
pub mod adjacency_delta;

pub mod generation;
pub use generation::{
    commit_topology_aware, commit_topology_aware_with_auxiliary, read_search_generation,
    read_topology_generation, touches_search_source,
};

pub mod graph_projection;
pub use graph_projection::{
    GraphProjectionClosure, GraphProjectionSelection, GraphProjectionSummary,
    materialize_graph_projection, materialize_portable_graph_tree_projection,
};

pub mod graph_files;
#[cfg(test)]
pub(crate) use graph_files::graph_files_root_participant;
pub use graph_files::{
    GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GRAPH_FILES_FAMILY, GRAPH_FILES_RECORD_VERSION,
    GRAPH_FILES_V2_RECORD_VERSION, GRAPH_TREE_DIR, GraphFileEntry, GraphFileRole,
    GraphFilesInventory, GraphFilesOpenEvidence, GraphFilesOpenStrategy, capture_graph_files,
    decode_inventory, encode_inventory, graph_tree_root, inventory_participant,
    materialize_graph_tree, pinned_open_evidence, stage_graph_tree, verify_graph_tree,
};
pub(crate) use graph_files::{GraphFilesParticipant, decode_versioned_graph_files_participant};

#[allow(
    dead_code,
    reason = "private node-v2 construction is consumed by the staged #932 integration"
)]
mod graph_manifest;
#[cfg(any(test, feature = "test-support"))]
pub use graph_manifest::encode_root as encode_graph_files_root_v2;
pub(crate) use graph_manifest::{
    GRAPH_FILES_V2_FORMAT, GRAPH_FILES_V2_VERSION, GRAPH_MANIFEST_NODE_FORMAT,
    GRAPH_MANIFEST_NODE_VERSION, GRAPH_RADIX_DEPTH, GraphFilesRootV2, GraphManifestLimits,
    GraphManifestNode, GraphManifestNodeKind, GraphManifestResolveEvidence,
    decode_node as decode_graph_manifest_node, decode_root as decode_graph_files_root_v2,
    encode_node as encode_graph_manifest_node, resolve_manifest as resolve_graph_manifest,
};

#[allow(
    dead_code,
    reason = "private CAS construction is consumed by the staged #932 integration"
)]
mod graph_object_store;
pub use graph_object_store::materialize_graph_objects;
#[cfg(any(test, feature = "test-support"))]
pub use graph_object_store::{
    GraphManifestState, append_graph_files_v2, install_graph_object_bytes, read_graph_object,
};
pub(crate) use graph_object_store::{
    GraphObjectPublicationLease, graph_object_publication_is_live, read_graph_object_by_digest,
    verify_graph_object,
};
#[cfg(not(any(test, feature = "test-support")))]
pub(crate) use graph_object_store::{begin_graph_object_publication, graph_object_path};
#[cfg(any(test, feature = "test-support"))]
pub use graph_object_store::{begin_graph_object_publication, graph_object_path};

pub mod semantic_bindings;
pub use semantic_bindings::{
    GRAPH_SEMANTIC_BINDINGS_FAMILY, GRAPH_SEMANTIC_BINDINGS_VERSION, LegacyRouteMigration,
    LegacySemanticProjection, MAX_SEMANTIC_BINDING_BYTES, MAX_SEMANTIC_BINDINGS,
    SEMANTIC_COMPOSITION_METADATA_KEY, SEMANTIC_ROUTE_METADATA_KEY, SemanticMigrationEvidence,
    SemanticMigrationLimits, SemanticMigrationOperation, SemanticMigrationPlan,
    SemanticMigrationPropertySchema, SemanticRouteKind, SemanticStorageBinding,
    SemanticStorageBindings, apply_legacy_route_moves, materialize_semantic_migration,
    require_atomic_legacy_migration, semantic_storage_bindings,
};

pub mod graph_delta_journal;
pub use graph_delta_journal::{
    GRAPH_DELTA_DIR, GRAPH_DELTA_RECORD_VERSION, GRAPH_DELTA_RUN_EXTENSION,
    GRAPH_DELTA_RUN_FORMAT_VERSION, GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind,
    GraphDeltaPayload, GraphDeltaPublicationReceipt, GraphDeltaPublishRequest,
    GraphDeltaReplayEvidence, GraphDeltaRun, MAX_GRAPH_DELTA_PAYLOAD_BYTES,
    MAX_GRAPH_DELTA_RECORDS_PER_RUN, MAX_GRAPH_DELTA_REPLAY_MEMORY_BYTES,
    MAX_GRAPH_DELTA_RUN_BYTES, MAX_GRAPH_DELTA_RUNS, PreparedGraphDelta, ReconstructedGraphState,
    apply_delta_runs, decode_delta_run, decode_graph_delta_value, delta_run_relative_path,
    encode_delta_run, encode_graph_delta_value, list_delta_runs, load_verified_delta_runs,
    materialize_replayed_graph_tree, prepare_graph_delta, publish_graph_delta,
    publish_graph_delta_with_mode, reconstruct_graph_state, stage_base_graph_workspace,
};

pub mod graph_delta_compaction;
pub use graph_delta_compaction::{
    DEFAULT_COMPACTION_CANCELLATION_CHECK_ROWS, DEFAULT_COMPACTION_MAX_DISK_BYTES,
    DEFAULT_COMPACTION_MAX_INPUT_BYTES, DEFAULT_COMPACTION_MAX_INPUT_RUNS,
    DEFAULT_COMPACTION_MAX_MEMORY_BYTES, DEFAULT_COMPACTION_MAX_OUTPUT_ROWS,
    DEFAULT_COMPACTION_MAX_SPILL_BYTES, GRAPH_DELTA_COMPACTION_SPILL_DIR,
    GraphDeltaCompactionLimits, GraphDeltaCompactionPolicy, GraphDeltaCompactionReport,
    GraphDeltaCompactionRequest, GraphDeltaCompactionStatus,
    MAX_COMPACTION_CANCELLATION_CHECK_ROWS, MAX_COMPACTION_DISK_BYTES, MAX_COMPACTION_INPUT_BYTES,
    MAX_COMPACTION_MEMORY_BYTES, MAX_COMPACTION_OUTPUT_ROWS, MAX_COMPACTION_SPILL_BYTES,
    compact_graph_delta, compact_graph_delta_with_mode, graph_delta_compaction_status,
    graph_delta_compaction_status_with_mode, preview_graph_delta_compaction,
    preview_graph_delta_compaction_with_mode,
};

pub mod project_generation;
pub use project_generation::{
    CURRENT_FILE, FORMAT_FILE, PROJECT_FORMAT_BYTES, ProjectCapabilityDescriptor,
    ProjectParticipantDescriptor, ProjectParticipantSnapshot, ResolvedProjectGeneration,
    open_or_initialize_ephemeral_project, open_or_initialize_project, resolve_generation_by_uuid,
    resolve_project_generation, resolve_verified_generation,
};

mod project_failpoint;

#[cfg(any(test, feature = "test-failpoints"))]
pub mod project_fault_oracle;

#[cfg(any(test, feature = "test-failpoints"))]
pub mod project_certification;

pub mod project_checkpoints;
pub use project_checkpoints::{
    CheckpointCreateRequest, CheckpointDeleteRequest, CheckpointReceipt, CheckpointRecord,
    CheckpointRevertRequest, create_checkpoint, create_checkpoint_with_mode, delete_checkpoint,
    delete_checkpoint_with_mode, list_checkpoints, list_checkpoints_with_mode,
    open_checkpoint_generation, open_checkpoint_generation_with_mode, revert_checkpoint,
    revert_checkpoint_with_mode,
};

pub mod project_publication;
pub use project_publication::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectPublicationReceipt, ProjectStageOutcome, StagedParticipant, StagedProjectGeneration,
    ValidatedProjectGeneration, published_project_transaction, stage_project_generation,
    stage_project_generation_optimistic, stage_project_generation_optimistic_with_graph_tree,
    stage_project_generation_optimistic_with_graph_tree_mode,
    stage_project_generation_with_graph_tree, stage_project_generation_with_graph_tree_mode,
};

pub mod project_recovery;
pub use project_recovery::{
    DEFAULT_RETAINED_ANCESTORS, MAX_RETAINED_ANCESTORS, ProjectOpenRecoveryEvidence,
    ProjectOpenRecoveryKind, ProjectRecoveryDeferral, ProjectRecoveryGenerationClass,
    ProjectRecoveryReport, open_or_initialize_ephemeral_project_with_recovery,
    open_or_initialize_project_with_recovery, recover_project_on_open,
    recover_project_transactions, remove_durable_project_root,
};

pub mod project_retention;
pub use project_retention::{
    DEFAULT_RETENTION_CLEANUP_BATCH, DEFAULT_RETENTION_MAX_BYTES, DEFAULT_RETENTION_MAX_ENTRIES,
    DEFAULT_RETENTION_MAX_WORK_UNITS, ProjectCleanupDisposition, ProjectCleanupEntry,
    ProjectCleanupLocation, ProjectCleanupReport, ProjectGraphObjectSweepDisposition,
    ProjectGraphObjectSweepReport, ProjectReachabilityReport, ProjectRetentionLimits,
    ProjectRetentionPolicy, execute_project_cleanup, execute_project_cleanup_with_mode,
    inspect_project_reachability, inspect_project_reachability_with_mode, preview_project_cleanup,
    preview_project_cleanup_with_mode,
};

pub mod project_portable;
pub mod project_portable_v2_export;
pub mod project_portable_v2_import;
pub use project_portable::{
    PortableExportReceipt, PortableImportReceipt, PortableProjectLimits, encode_portable_project,
    export_portable_project, import_portable_project, import_portable_project_file,
};
pub use project_portable_v2_export::{
    PortableV2ExportLimits, PortableV2ExportPlan, PortableV2ExportProgress,
    PortableV2ExportReceipt, PortableV2Output, export_complete_portable_v2,
    plan_complete_portable_v2, plan_selected_portable_v2, repack_verified_expanded_portable_v2,
};
pub use project_portable_v2_import::{
    PortableV2ImportPhase, PortableV2ImportProgress, PortableV2ImportReceipt,
    PortableV2SelectiveCandidate, PortableV2StagedCompositionReceipt,
    consume_selective_portable_v2, import_complete_portable_v2,
    import_complete_portable_v2_with_progress, load_portable_ontology_staging,
};

pub mod project_portable_v2;
pub use project_portable_v2::{
    PortableV2ActivationOverride, PortableV2ActivationProfile, PortableV2Authenticity,
    PortableV2BridgeSet, PortableV2Compatibility, PortableV2CompositionEntry, PortableV2Error,
    PortableV2ErrorCode, PortableV2ExactIdentity, PortableV2Integrity, PortableV2Limits,
    PortableV2Mode, PortableV2OntologyComposition, PortableV2OntologyModule,
    PortableV2PackageClass, PortableV2Report, PortableV2Representation,
    materialize_verified_portable_v2, verify_portable_v2,
};

pub mod discovery_portable_v2;
pub use discovery_portable_v2::{
    DiscoveredPortableV2, DiscoveryPortableV2Error, DiscoveryPortableV2Mismatch,
    DiscoveryPortableV2Request, verify_discovered_portable_v2,
};

mod project_portable_v2_selection;
pub use project_portable_v2_selection::{
    PortableV2ParticipantId, PortableV2ProjectedSelectionEntry, PortableV2SelectionEntry,
    PortableV2SelectionPlan, PortableV2SelectionProfile, PortableV2SelectionReason,
    PortableV2SelectionRequest, preview_portable_v2_selection,
};

pub mod project_portable_v2_subset;
pub use project_portable_v2_subset::{
    PortableV2GraphSelector, PortableV2GraphSubsetMeta, PortableV2PropertyProjection,
    PortableV2SubsetClosure, PortableV2SubsetPlan, PortableV2SubsetRequest,
    plan_graph_subset_portable_v2, portable_v2_graph_data_fingerprint,
    preview_portable_v2_graph_subset,
};

pub mod project_portable_v2_oci;
pub use project_portable_v2_oci::{
    HttpOciRegistry, MemoryOciRegistry, OCI_ARTIFACT_TYPE, OCI_CONFIG_MEDIA_TYPE,
    OCI_LAYER_MEDIA_TYPE, OCI_MANIFEST_MEDIA_TYPE, OCI_SIGNATURE_ARTIFACT_TYPE,
    OCI_SIGNATURE_CONFIG_MEDIA_TYPE, PortableV2OciAuthenticityPolicy, PortableV2OciPhase,
    PortableV2OciProgress, PortableV2OciPublishRequest, PortableV2OciPullReceipt,
    PortableV2OciPullRequest, PortableV2OciReference, PortableV2OciRegistry,
    PortableV2OciSignatureMaterial, PortableV2OciSignatureState,
    evaluate_portable_v2_oci_signature_state, publish_portable_v2_oci,
    publish_portable_v2_oci_with_progress, pull_portable_v2_oci,
    pull_portable_v2_oci_with_progress,
};

pub mod workspace_participants;
pub use workspace_participants::{
    GraphDirectedness, MAX_WORKSPACE_REPOSITORY_SNAPSHOT_BYTES,
    MAX_WORKSPACE_REPOSITORY_SNAPSHOT_ENTRIES, MAX_WORKSPACE_REPOSITORY_SNAPSHOT_ID_BYTES,
    WORKSPACE_CAPABILITY_ID, WORKSPACE_CAPABILITY_VERSION, WORKSPACE_CONFIGURATION_FAMILY,
    WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY, WORKSPACE_ONTOLOGY_COMPOSITION_VERSION,
    WORKSPACE_ONTOLOGY_FAMILY, WORKSPACE_PORTABLE_ONTOLOGY_STAGING_FAMILY,
    WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY, WORKSPACE_REPOSITORY_SNAPSHOT_VERSION,
    WorkspaceCompositionModule, WorkspaceConfiguration, WorkspaceOntology,
    WorkspaceOntologyComposition, WorkspaceOntologyMode, WorkspaceOntologySourceFormat,
    WorkspacePortableOntologyStaging, WorkspaceRepositoryDefinitionDigest,
    WorkspaceRepositoryGitProvenance, WorkspaceRepositorySnapshot, WorkspaceRepositorySourceDigest,
    empty_workspace_participants,
};

pub mod embedding_identity;
pub use embedding_identity::{
    ChunkingIdentity, EmbeddingCompatibilityDescriptor, EmbeddingCompatibilityId,
    EmbeddingCompatibilityInput, EmbeddingContentDigest, EmbeddingDisplayName, EmbeddingDistance,
    EmbeddingGenerationId, EmbeddingNormalization, EmbeddingProducerIdentity,
    EmbeddingSourceFingerprint, EmbeddingValueType, TokenCountClass, TokenizerIdentity,
};

pub mod embedding_catalog;
pub use embedding_catalog::{
    EMBEDDING_SPACE_CATALOG_VERSION, EmbeddingSpaceCatalog, EmbeddingSpaceCatalogEntry,
    EmbeddingSpaceCatalogLimits, EmbeddingSpaceCatalogUpdate,
    bind_existing_embedding_space_catalog_entry, read_embedding_space_catalog,
    remove_embedding_space_catalog_identity, update_embedding_space_catalog,
};

pub mod embedding_discovery;
pub use embedding_discovery::{
    DiscoveredEmbeddingSpace, EmbeddingSpaceDiscoveryLimits,
    MAX_DISCOVERED_EMBEDDING_DESCRIPTOR_BYTES, MAX_DISCOVERED_EMBEDDING_SPACES,
    MAX_EMBEDDING_SPACE_DIRECTORY_ENTRIES, discover_embedding_spaces,
};

pub mod embedding_batch;
pub use embedding_batch::{EmbeddingBatchRow, ValidatedEmbeddingBatch, validate_embedding_batch};

pub mod embedding_manifest;
pub use embedding_manifest::{
    EMBEDDING_GENERATION_MANIFEST_VERSION, EmbeddingGenerationManifest,
    EmbeddingGenerationManifestInput, EmbeddingPublicationFingerprint, EmbeddingSourceState,
    MAX_EMBEDDING_GENERATION_MANIFEST_BYTES,
};

pub mod embedding_publication;
pub use embedding_publication::{
    EmbeddingGenerationPublication, EmbeddingPublicationOutcome, EmbeddingPublicationRequest,
    current_embedding_generation, delete_embedding_space_lineage, publish_embedding_generation,
};

pub mod embedding_freshness;
pub use embedding_freshness::{
    EMBEDDING_SUBSTANTIAL_CHANGED_PERCENT, EMBEDDING_SUBSTANTIAL_MUTATION_BATCHES,
    EmbeddingForcedStaleDiagnostic, EmbeddingFreshness, EmbeddingFreshnessReason,
    EmbeddingFreshnessState, EmbeddingMutationObservation, EmbeddingReadDecision,
    classify_embedding_freshness, decide_embedding_read,
};

pub mod embedding_refresh_config;
pub use embedding_refresh_config::{
    DEFAULT_EMBEDDING_REFRESH_DEBOUNCE, EMBEDDING_REFRESH_CONFIG_VERSION, EmbeddingRefreshConfig,
    EmbeddingRefreshConfigLimits, EmbeddingRefreshConfigUpdate, EmbeddingRefreshFailureClass,
    EmbeddingRefreshOutcomeRecord, EmbeddingRefreshOutcomeStatus, EmbeddingRefreshProjectPolicy,
    EmbeddingRefreshSpacePolicy, EmbeddingRefreshSpaceState, MAX_EMBEDDING_REFRESH_CONFIG_BYTES,
    MAX_EMBEDDING_REFRESH_CONFIG_ENTRIES, MAX_EMBEDDING_REFRESH_JOBS,
    ResolvedEmbeddingRefreshPolicy, read_embedding_refresh_config, update_embedding_refresh_config,
};

pub mod embedding_mutations;
pub use embedding_mutations::{
    EMBEDDING_MUTATION_JOURNAL_VERSION, EmbeddingMutationBatch, EmbeddingMutationJournal,
    EmbeddingMutationJournalLimits, merge_embedding_mutation_batch,
    read_embedding_mutation_journal, reset_embedding_mutation_journal,
};

pub mod search_manifest;
pub use search_manifest::{
    MAX_SEARCH_ARTIFACT_KEY_BYTES, MAX_SEARCH_MANIFEST_BYTES, MAX_SEARCH_SELECTOR_BYTES,
    SEARCH_MANIFEST_VERSION, SearchArtifactError, SearchArtifactKey, SearchIndexKind,
    SearchManifest, SearchSourcePart, SearchSourceSnapshot, canonical_source_fingerprint,
};

pub mod search_publication;
pub use search_publication::{
    PublishedSearchArtifact, SearchCoordinationLimits, SearchPublicationMode,
    SearchPublicationOutcome, SearchPublicationPlan, SearchUpdateBuild,
    cleanup_abandoned_search_builds, coordinate_search_publication, coordinate_search_update,
    current_search_artifact,
};

pub mod vector_store;
pub use vector_store::{
    StoredVector, VECTOR_BACKEND_VERSION, VECTOR_CONTRACT_VERSION, VECTOR_DATA_FILE,
    VectorSearchHit, VectorStoreLimits, VectorUpsertChange, apply_vector_upsert,
    exact_cosine_search, read_vector_snapshot, search_published_vectors, upsert_published_vector,
    validate_published_vectors, validate_vector, vector_schema, write_vector_snapshot,
};

pub mod io_stats;
pub use io_stats::{IoSnapshot, snapshot as io_snapshot};

pub mod uuid_membership;
pub use uuid_membership::{
    AuthenticatedUuidIndexSnapshot, UuidIndexAppendMetrics, UuidIndexBuildLimits,
    UuidIndexBuildMetrics, UuidIndexKind, UuidIndexOrphanGcWork, UuidMembershipIndex,
    UuidProbeMetrics, maintain_uuid_membership_orphans, rebuild_uuid_membership_indexes,
    uuid_membership_index_is_fresh, uuid_membership_index_present,
};

pub mod property_overlay;
mod property_scan;
pub use property_overlay::{
    AuthenticatedPropertyInventory, PROPERTY_OVERLAY_FORMAT, PROPERTY_OVERLAY_FORMAT_KEY,
    PROPERTY_TOMBSTONE_FIELD, PropertyFragmentId, PropertyInventoryOpenMetrics,
    PropertyOverlayLimits, PropertyOverlayMetrics, PropertyRouteKind, PropertySnapshotRow,
    enumerate_property_fragments, read_authenticated_property_snapshots_for,
    read_authenticated_property_snapshots_for_inventory, visit_authenticated_property_snapshots,
};

pub mod catalog;
pub use catalog::{
    EdgePropertyTable, GraphCatalog, PropertyTable, TopologyNodeTable, TypedEdgeTable,
    UnionEdgeTable, list_edge_property_stems, list_property_stems, node_topology_present,
    read_edge_properties, read_edges, read_edges_filtered, read_edges_filtered_observed,
    read_nodes, read_nodes_filtered, read_nodes_filtered_observed, read_properties,
    read_properties_batched, visit_nodes_batched, visit_properties_batched,
};

pub mod runtime_entity_labels;
pub use runtime_entity_labels::{
    RUNTIME_ENTITY_LABEL_ENCODING_VERSION, RuntimeEntityLabelReconcile,
    has_runtime_entity_label_encoding_marker, reconcile_runtime_entity_label_ids,
    runtime_entity_plan_id_is_disjoint_from_ontology, validate_runtime_entity_label_ids,
    write_runtime_entity_label_encoding_marker,
};

pub mod parquet_scan;
pub use parquet_scan::{GraphForgeParquetExec, IoConcurrencyExt, ParquetFragment};

pub mod schemas;
pub use schemas::{
    ADJACENCY_CSR_SCHEMA, ADJACENCY_MANIFEST_SCHEMA, EDGE_PROPERTY_BASE_SCHEMA,
    EXPLORATORY_EDGE_SCHEMA, INTERNAL_SURROGATE_META_KEY, PROPERTY_BASE_SCHEMA,
    TOPOLOGY_NODES_SCHEMA, TYPED_EDGE_SCHEMA, is_internal_surrogate_field, property_schema,
    property_type_to_arrow, result_schema,
};

pub mod writer;
pub use writer::{
    GraphWriter, GraphWriterLimits, count_entity_properties, decode_spatial_property_value,
    read_entity_properties, read_entity_property_keys, read_node_property_rows,
    remove_edge_properties, remove_node_properties, set_edge_properties_rewrite,
    set_node_properties, stage_property_tombstones_authenticated, stage_remove_edge_properties,
    stage_remove_edge_properties_authenticated, stage_remove_node_properties,
    stage_remove_node_properties_authenticated, stage_set_edge_properties,
    stage_set_edge_properties_authenticated, stage_set_node_properties,
    stage_set_node_properties_authenticated,
};

pub mod mutator;
pub use mutator::{
    delete_edges, delete_nodes, delete_nodes_and_edges, incident_edge_uuids, stage_add_node_labels,
    stage_delete_edges, stage_delete_edges_authenticated, stage_delete_nodes,
    stage_delete_nodes_authenticated, stage_mutate_node_labels,
};

pub mod staging;
pub use staging::{RewriteBatch, remove_stale_temps};

pub use graphforge_core::GfError;

/// Minimal row type exchanged between the storage layer and the executor.
/// Will be replaced by Arrow `RecordBatch` in Milestone 13.
#[derive(Debug, Clone, Default)]
pub struct StorageRow {
    /// Column name → string value pairs (all types as strings at stub stage).
    pub columns: Vec<(String, String)>,
}

/// Abstraction over different storage backends.
pub trait StorageProvider: Send + Sync {
    /// Scan all rows for the given node label.
    ///
    /// # Errors
    /// Returns [`GfError`] on I/O failure or if the label is unknown.
    fn scan_nodes(&self, label: &str) -> Result<Vec<StorageRow>, GfError>;
}

/// Parquet-backed storage provider stub.
#[derive(Debug, Default)]
pub struct ParquetProvider {
    /// Optional path to the Parquet directory.
    pub path: Option<std::path::PathBuf>,
}

impl StorageProvider for ParquetProvider {
    fn scan_nodes(&self, _label: &str) -> Result<Vec<StorageRow>, GfError> {
        Err(GfError::NotImplemented("scan_nodes"))
    }
}
