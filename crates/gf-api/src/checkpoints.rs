//! Public named-checkpoint reads and deterministic manifest summaries.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringBuilder,
    TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use gf_core::{ApiErrorCode, GfError, ProjectErrorCode};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{ExecutionResult, GraphForge, OperationId, PageRequest, PageToken};

/// Create-checkpoint request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRequest {
    /// Canonical checkpoint name.
    pub name: String,
    /// Optional bounded description.
    pub description: Option<String>,
    /// Idempotent operation identity.
    pub idempotency_key: OperationId,
    /// Optional actor identity.
    pub actor_uuid: Option<Uuid>,
}

/// Paginated checkpoint-list request.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ListCheckpointsRequest {
    /// Bounded page and cancellation controls.
    pub page: PageRequest,
}

/// Show-checkpoint request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShowCheckpointRequest {
    /// Exact active checkpoint name.
    pub name: String,
}

/// Delete-checkpoint request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteCheckpointRequest {
    /// Exact active checkpoint name.
    pub name: String,
    /// Idempotent operation identity.
    pub idempotency_key: OperationId,
    /// Optional actor identity.
    pub actor_uuid: Option<Uuid>,
}

/// Complete-workspace checkpoint revert request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RevertCheckpointRequest {
    /// Exact active checkpoint name.
    pub name: String,
    /// Bounded human restoration reason.
    pub reason: String,
    /// Idempotent operation identity.
    pub idempotency_key: OperationId,
    /// Optional actor identity.
    pub actor_uuid: Option<Uuid>,
}

/// Non-mutating checkpoint-revert preview request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreviewRevertCheckpointRequest {
    /// Exact active checkpoint name.
    pub name: String,
}

/// Identities a caller must inspect before authorizing a checkpoint revert.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RevertCheckpointPreview {
    /// Stable checkpoint identity.
    pub checkpoint_uuid: Uuid,
    /// Complete generation pinned by the checkpoint.
    pub source_generation_uuid: Uuid,
    /// SHA-256 of the pinned generation's canonical manifest.
    pub source_manifest_sha256: String,
    /// Generation that is current at preview time.
    pub current_generation_uuid: Uuid,
}

/// Named checkpoint or the current committed generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointSelector {
    /// Active named checkpoint.
    Named(String),
    /// Current committed generation at call time.
    Current,
}

/// Participant domain selected for diffing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointDiffScope {
    /// All participant summary domains.
    Summary,
    /// Graph records.
    Graph,
    /// Ontology records.
    Ontology,
    /// Project configuration records.
    Configuration,
    /// Capability/workspace control records.
    Capabilities,
    /// Provenance and lineage records.
    Provenance,
    /// M20 knowledge records.
    Knowledge,
    /// M21 epistemic and valid-time records.
    Epistemic,
    /// Every registered domain.
    All,
}

/// Manifest-summary or logical-record detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointDiffDetail {
    /// Participant manifest summary.
    Summary,
    /// Owner-canonical logical records.
    Records,
}

/// Bounded checkpoint diff request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiffCheckpointsRequest {
    /// Earlier endpoint.
    pub from: CheckpointSelector,
    /// Later endpoint.
    pub to: CheckpointSelector,
    /// Selected participant domain.
    pub scope: CheckpointDiffScope,
    /// Summary or record detail.
    pub detail: CheckpointDiffDetail,
    /// Bounded page and cancellation controls.
    pub page: PageRequest,
}

/// Immutable lease-pinned checkpoint facade.
#[derive(Debug)]
pub struct CheckpointView {
    checkpoint: gf_storage::CheckpointRecord,
    graph: GraphForge,
}

impl CheckpointView {
    /// Stable checkpoint identity.
    #[must_use]
    pub fn checkpoint_uuid(&self) -> Uuid {
        self.checkpoint.checkpoint_uuid
    }
    /// Pinned committed generation identity.
    #[must_use]
    pub fn generation_uuid(&self) -> Uuid {
        self.checkpoint.generation_uuid
    }
    /// Execute a read-only Cypher statement against the pinned generation.
    pub fn execute(&self, cypher: &str) -> Result<ExecutionResult, GfError> {
        self.graph.execute_read_only(cypher)
    }
    /// Inspect capabilities from the pinned manifest only.
    pub fn project_capabilities(&self) -> Result<ExecutionResult, GfError> {
        self.graph.project_capabilities()
    }
    /// Return the pinned workspace ontology.
    pub fn workspace_ontology(&self) -> Result<gf_storage::WorkspaceOntology, GfError> {
        self.graph.workspace_ontology()
    }
    /// Return the pinned workspace configuration.
    pub fn workspace_configuration(&self) -> Result<gf_storage::WorkspaceConfiguration, GfError> {
        self.graph.workspace_configuration()
    }
    /// Run a pinned M18 ranking read. Write-back is rejected.
    pub fn rank(&self, label: &str, options: crate::RankOptions) -> Result<RecordBatch, GfError> {
        if options.write_property.is_some() {
            return read_only();
        }
        self.graph.rank(label, options)
    }
    /// Run a pinned M18 clustering read. Write-back is rejected.
    pub fn cluster(
        &self,
        label: &str,
        options: crate::ClusterOptions,
    ) -> Result<RecordBatch, GfError> {
        if options.write_property.is_some() {
            return read_only();
        }
        self.graph.cluster(label, options)
    }
    /// Run a pinned M18 path read.
    pub fn paths<'a>(
        &self,
        source: impl Into<Option<&'a crate::NodeSelector>>,
        target: Option<&crate::NodeSelector>,
        options: crate::PathsOptions,
    ) -> Result<RecordBatch, GfError> {
        self.graph.paths(source, target, options)
    }
    /// Run a pinned M18 graph analysis.
    pub fn analyze(
        &self,
        label: Option<&str>,
        options: crate::AnalyzeOptions,
    ) -> Result<RecordBatch, GfError> {
        self.graph.analyze(label, options)
    }
    /// Run a pinned M18 embedding analysis.
    pub fn analyze_embedding(
        &self,
        label: Option<&str>,
        options: &crate::EmbeddingAnalyzeOptions,
    ) -> Result<RecordBatch, GfError> {
        self.graph.analyze_embedding(label, options)
    }
    /// Run a pinned M18 similarity read.
    pub fn similar(
        &self,
        label: &str,
        options: crate::SimilarOptions,
    ) -> Result<RecordBatch, GfError> {
        self.graph.similar(label, options)
    }
    /// Run pinned M19 search.
    pub fn find(&self, options: crate::FindOptions) -> Result<RecordBatch, GfError> {
        self.graph.find(options)
    }
    /// List pinned embedding-space metadata.
    pub fn embedding_spaces(&self) -> Result<Vec<crate::EmbeddingSpaceInfo>, GfError> {
        self.graph.embedding_spaces()
    }
    /// Inspect one pinned embedding space.
    pub fn embedding_space(
        &self,
        display_name: Option<&str>,
    ) -> Result<crate::EmbeddingSpaceInfo, GfError> {
        self.graph.embedding_space(display_name)
    }
    /// Inspect pinned embedding freshness.
    pub fn inspect_embedding_space_freshness(
        &self,
        display_name: Option<&str>,
        force_stale: bool,
    ) -> Result<crate::EmbeddingSpaceFreshnessInspection, GfError> {
        self.graph
            .inspect_embedding_space_freshness(display_name, force_stale)
    }
    /// Inspect pinned embedding refresh state.
    pub fn inspect_embedding_refresh(
        &self,
        display_name: Option<&str>,
    ) -> Result<crate::EmbeddingRefreshInspection, GfError> {
        self.graph.inspect_embedding_refresh(display_name)
    }
    /// Return one pinned provenance event.
    pub fn provenance_event(
        &self,
        provenance_uuid: Uuid,
        cancellation: Option<crate::CancellationToken>,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.provenance_event(provenance_uuid, cancellation)
    }
    /// Return pinned provenance history.
    pub fn list_provenance_history(
        &self,
        request: crate::ProvenanceHistoryRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_provenance_history(request)
    }
    /// Return one pinned assertion.
    pub fn assertion(
        &self,
        assertion_uuid: Uuid,
        cancellation: Option<crate::CancellationToken>,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.assertion(assertion_uuid, cancellation)
    }
    /// List pinned assertions.
    pub fn list_assertions(
        &self,
        request: crate::ListAssertionsRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_assertions(request)
    }
    /// Return a pinned assertion's graph references.
    pub fn assertion_graph_refs(
        &self,
        assertion_uuid: Uuid,
        page: PageRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.assertion_graph_refs(assertion_uuid, page)
    }
    /// List pinned evidence links.
    pub fn list_evidence_links(
        &self,
        request: crate::ListEvidenceLinksRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_evidence_links(request)
    }
    /// List pinned confidence assessments.
    pub fn list_confidence_assessments(
        &self,
        request: crate::ListConfidenceAssessmentsRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_confidence_assessments(request)
    }
    /// Return one pinned confidence assessment.
    pub fn confidence_assessment(
        &self,
        confidence_uuid: Uuid,
        cancellation: Option<crate::CancellationToken>,
    ) -> Result<ExecutionResult, GfError> {
        self.graph
            .confidence_assessment(confidence_uuid, cancellation)
    }
    /// Return the pinned inputs for one confidence assessment.
    pub fn confidence_inputs(
        &self,
        confidence_uuid: Uuid,
        page: PageRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.confidence_inputs(confidence_uuid, page)
    }
    /// Return one pinned evidence link.
    pub fn evidence_link(
        &self,
        evidence_uuid: Uuid,
        cancellation: Option<crate::CancellationToken>,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.evidence_link(evidence_uuid, cancellation)
    }
    /// Return one pinned reasoning record.
    pub fn reasoning(
        &self,
        reasoning_uuid: Uuid,
        cancellation: Option<crate::CancellationToken>,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.reasoning(reasoning_uuid, cancellation)
    }
    /// List pinned reasoning records.
    pub fn list_reasoning(
        &self,
        request: crate::ListReasoningRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_reasoning(request)
    }
    /// Return one pinned assertion status.
    pub fn assertion_status(&self, assertion_uuid: Uuid) -> Result<ExecutionResult, GfError> {
        self.graph.assertion_status(assertion_uuid)
    }
    /// List pinned assertion status history.
    pub fn list_assertion_status(
        &self,
        request: crate::ListAssertionStatusRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_assertion_status(request)
    }
    /// List pinned assertion supersessions.
    pub fn list_assertion_supersessions(
        &self,
        request: crate::ListAssertionSupersessionsRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_assertion_supersessions(request)
    }
    /// Return one pinned algorithm run.
    pub fn algorithm_run(
        &self,
        run_uuid: Uuid,
        cancellation: Option<crate::CancellationToken>,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.algorithm_run(run_uuid, cancellation)
    }
    /// List pinned algorithm runs.
    pub fn list_algorithm_runs(
        &self,
        request: crate::ListAlgorithmRunsRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_algorithm_runs(request)
    }
    /// List pinned events for one algorithm run.
    pub fn algorithm_run_events(
        &self,
        run_uuid: Uuid,
        page: PageRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.algorithm_run_events(run_uuid, page)
    }
    /// Reconstruct the pinned M21 epistemic snapshot.
    pub fn epistemic_snapshot(&self, cutoff_micros: i64) -> Result<ExecutionResult, GfError> {
        self.graph.epistemic_snapshot(cutoff_micros)
    }
    /// List pinned hypothesis groups.
    pub fn list_hypothesis_groups(
        &self,
        request: &crate::ListHypothesisGroupsRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_hypothesis_groups(request)
    }
    /// List pinned hypothesis membership history.
    pub fn list_hypothesis_membership(
        &self,
        request: &crate::ListHypothesisMembershipRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_hypothesis_membership(request)
    }
    /// List pinned hypothesis selection history.
    pub fn list_hypothesis_selection(
        &self,
        request: &crate::ListHypothesisSelectionRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_hypothesis_selection(request)
    }
    /// Return the pinned current members of a hypothesis group.
    pub fn hypothesis_members(&self, group_uuid: Uuid) -> Result<ExecutionResult, GfError> {
        self.graph.hypothesis_members(group_uuid)
    }
    /// Return the pinned current selection of a hypothesis group.
    pub fn hypothesis_selection(&self, group_uuid: Uuid) -> Result<ExecutionResult, GfError> {
        self.graph.hypothesis_selection(group_uuid)
    }
    /// List pinned validity history.
    pub fn list_assertion_validity(
        &self,
        request: crate::ListAssertionValidityRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.list_assertion_validity(request)
    }
    /// Apply valid-time interpretation to the pinned snapshot.
    pub fn apply_valid_time(
        &self,
        request: crate::ApplyValidTimeRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph.apply_valid_time(request)
    }
    /// Resolve a pinned belief projection without publication.
    pub fn resolve_belief_projection(
        &self,
        request: crate::ResolveBeliefProjectionRequest,
    ) -> Result<crate::ResolvedBeliefProjection, GfError> {
        self.graph.resolve_belief_projection(request)
    }
    /// Deterministically reject mutation through the historical view.
    pub fn checkpoint(&self, _: CheckpointRequest) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Deterministically reject mutation through the historical view.
    pub fn delete_checkpoint(
        &self,
        _: DeleteCheckpointRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject capability mutation before project access.
    pub fn enable_capability(
        &self,
        _: crate::EnableCapabilityRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject node construction before project access.
    pub fn add_node(
        &self,
        _: &str,
        _: &HashMap<String, crate::PropValue>,
    ) -> Result<crate::NodeHandle, GfError> {
        read_only()
    }
    /// Reject edge construction before project access.
    pub fn add_edge(
        &self,
        _: &crate::NodeHandle,
        _: &str,
        _: &crate::NodeHandle,
        _: &HashMap<String, crate::PropValue>,
    ) -> Result<crate::EdgeHandle, GfError> {
        read_only()
    }
    /// Reject search-index publication before project access.
    pub fn index_search(
        &self,
        _: &str,
        _: crate::SearchIndexOptions,
    ) -> Result<Option<crate::TextIndexInspection>, GfError> {
        read_only()
    }
    /// Reject adjacency-index publication before project access.
    pub fn index_adjacency(&self) -> Result<crate::AdjacencyInspection, GfError> {
        read_only()
    }
    /// Inspect adjacency freshness from the immutable pinned generation.
    pub fn inspect_adjacency(&self) -> Result<crate::AdjacencyInspection, GfError> {
        self.graph.inspect_adjacency()
    }
    /// Reject embedding alias mutation before project access.
    pub fn bind_embedding_space_alias(
        &self,
        _: &str,
        _: &str,
        _: bool,
    ) -> Result<crate::EmbeddingSpaceInfo, GfError> {
        read_only()
    }
    /// Reject embedding alias removal before project access.
    pub fn remove_embedding_space_alias(&self, _: &str) -> Result<bool, GfError> {
        read_only()
    }
    /// Reject embedding deletion before project access.
    pub fn delete_embedding_space(&self, _: Option<&str>) -> Result<bool, GfError> {
        read_only()
    }
    /// Reject embedding-default mutation before project access.
    pub fn set_default_embedding_space(
        &self,
        _: Option<&str>,
    ) -> Result<Option<crate::EmbeddingSpaceInfo>, GfError> {
        read_only()
    }
    /// Reject assertion creation before project access.
    pub fn create_assertion(
        &self,
        _: crate::CreateAssertionRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject atomic assertion/status creation before project access.
    pub fn create_assertion_with_status(
        &self,
        _: crate::CreateAssertionWithStatusRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject atomic assertion/evidence creation before project access.
    pub fn create_assertion_with_evidence(
        &self,
        _: crate::CreateAssertionWithEvidenceRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject confidence writes before project access.
    pub fn assess_confidence(
        &self,
        _: crate::AssessConfidenceRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject evidence writes before project access.
    pub fn attach_evidence(
        &self,
        _: crate::AttachEvidenceRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject reasoning writes before project access.
    pub fn record_reasoning(
        &self,
        _: crate::RecordReasoningRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject status writes before project access.
    pub fn record_assertion_status(
        &self,
        _: crate::RecordAssertionStatusRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject supersession writes before project access.
    pub fn supersede_assertion(
        &self,
        _: crate::SupersedeAssertionRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject hypothesis writes before project access.
    pub fn create_hypothesis_group(
        &self,
        _: crate::CreateHypothesisGroupRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject hypothesis-membership writes before project access.
    pub fn record_hypothesis_membership(
        &self,
        _: &crate::RecordHypothesisMembershipRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject hypothesis-selection writes before project access.
    pub fn record_hypothesis_selection(
        &self,
        _: &crate::RecordHypothesisSelectionRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject hypothesis-member removal before project access.
    pub fn remove_hypothesis_member(
        &self,
        _: &crate::RemoveHypothesisMemberRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject valid-time writes before project access.
    pub fn record_assertion_validity(
        &self,
        _: crate::RecordAssertionValidityRequest,
    ) -> Result<ExecutionResult, GfError> {
        read_only()
    }
    /// Reject ontology adoption before project access.
    pub fn adopt_ontology(&mut self, _: crate::AdoptOntologyRequest) -> Result<(), GfError> {
        read_only()
    }
    /// Reject ontology clearing before project access.
    pub fn clear_ontology(&mut self, _: crate::ClearOntologyRequest) -> Result<(), GfError> {
        read_only()
    }
}

impl GraphForge {
    /// Create a durable named checkpoint.
    pub fn checkpoint(&self, request: CheckpointRequest) -> Result<ExecutionResult, GfError> {
        let receipt = gf_storage::create_checkpoint(
            self.resolved_generation.container_root(),
            &gf_storage::CheckpointCreateRequest {
                operation_uuid: request.idempotency_key.0,
                name: request.name,
                description: request.description,
                actor_uuid: request.actor_uuid,
            },
        )?;
        Ok(receipt_result(&receipt))
    }

    /// List active checkpoints in canonical order.
    pub fn list_checkpoints(
        &self,
        request: ListCheckpointsRequest,
    ) -> Result<ExecutionResult, GfError> {
        let ListCheckpointsRequest { page } = request;
        cancellation(&page)?;
        let rows = gf_storage::list_checkpoints(self.resolved_generation.container_root())?;
        let snapshot = checkpoint_list_snapshot(&rows);
        let binding = request_binding("checkpoint-list", 0, 0);
        let cursors = rows
            .iter()
            .map(|row| page_cursor(&[row.name.as_bytes(), row.checkpoint_uuid.as_bytes()]))
            .collect::<Vec<_>>();
        let (start, end) = page_bounds("checkpoint-list", binding, &page, snapshot, &cursors)?;
        let next = (end < rows.len()).then(|| {
            PageToken::new_bound(
                "checkpoint-list",
                binding,
                snapshot,
                page.limit,
                end,
                cursors[end - 1],
            )
        });
        let result = checkpoint_rows(&rows[start..end], next.as_ref())?;
        // Re-check after the storage read so an AbortSignal that lands during a
        // short list still surfaces GF_CANCELLED instead of a late success.
        cancellation(&page)?;
        Ok(result)
    }

    /// Show the authoritative metadata for one active named checkpoint.
    pub fn show_checkpoint(
        &self,
        request: ShowCheckpointRequest,
    ) -> Result<ExecutionResult, GfError> {
        let ShowCheckpointRequest { name } = request;
        let (checkpoint, _) = gf_storage::open_checkpoint_generation(
            self.resolved_generation.container_root(),
            &name,
        )?;
        checkpoint_rows(std::slice::from_ref(&checkpoint), None)
    }

    /// Inspect checkpoint and current-generation identities without mutation.
    pub fn preview_revert_to_checkpoint(
        path: impl AsRef<std::path::Path>,
        request: PreviewRevertCheckpointRequest,
    ) -> Result<RevertCheckpointPreview, GfError> {
        let PreviewRevertCheckpointRequest { name } = request;
        let current = gf_storage::resolve_project_generation(path.as_ref())?;
        let (checkpoint, _) = gf_storage::open_checkpoint_generation(path.as_ref(), &name)?;
        Ok(RevertCheckpointPreview {
            checkpoint_uuid: checkpoint.checkpoint_uuid,
            source_generation_uuid: checkpoint.generation_uuid,
            source_manifest_sha256: checkpoint.generation_manifest_sha256,
            current_generation_uuid: current.generation_uuid(),
        })
    }

    /// Open an immutable view pinned to the named checkpoint generation.
    pub fn open_checkpoint(&self, name: &str) -> Result<CheckpointView, GfError> {
        let (checkpoint, generation) = gf_storage::open_checkpoint_generation(
            self.resolved_generation.container_root(),
            name,
        )?;
        let graph = Self::open_resolved_with_mode(
            self.resolved_generation.container_root().to_owned(),
            generation,
            true,
        )?;
        Ok(CheckpointView { checkpoint, graph })
    }

    /// Delete an active checkpoint reference.
    pub fn delete_checkpoint(
        &self,
        request: DeleteCheckpointRequest,
    ) -> Result<ExecutionResult, GfError> {
        let receipt = gf_storage::delete_checkpoint(
            self.resolved_generation.container_root(),
            &gf_storage::CheckpointDeleteRequest {
                operation_uuid: request.idempotency_key.0,
                name: request.name,
                actor_uuid: request.actor_uuid,
            },
        )?;
        Ok(receipt_result(&receipt))
    }

    /// Restore every authoritative participant from a checkpoint into a new generation.
    pub fn revert_to_checkpoint(
        &mut self,
        request: RevertCheckpointRequest,
    ) -> Result<ExecutionResult, GfError> {
        if self.read_only {
            return read_only();
        }
        let container_root = self.resolved_generation.container_root().to_path_buf();
        let clock = self.clock.lock().expect("clock lock poisoned").clone();
        let write_options = self.write_options;
        let select_clock = Arc::clone(&clock);
        let prepared = std::cell::RefCell::new(None);
        let (receipt, resolved) = gf_storage::revert_checkpoint(
            &container_root,
            &gf_storage::CheckpointRevertRequest {
                operation_uuid: request.idempotency_key.0,
                name: request.name,
                reason: request.reason,
                actor_uuid: request.actor_uuid,
            },
            move || select_clock(),
            |generation| {
                validate_revert_source(generation)?;
                prepared.replace(Some(GraphForge::open_resolved_with_options(
                    container_root.clone(),
                    generation.clone(),
                    true,
                    write_options,
                )?));
                Ok(())
            },
        )?;
        let result = receipt_result(&receipt);

        let mut reopened = prepared
            .into_inner()
            .expect("successful revert validation prepares the replacement facade");
        reopened.resolved_generation = resolved;
        *reopened
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") =
            reopened.resolved_generation.generation_uuid();
        reopened.read_only = false;
        let procedures = Arc::clone(&self.procedures);
        let provider_refresh_runtimes = Arc::clone(&self.provider_refresh_runtimes);
        let provider_find_runtimes = Arc::clone(&self.provider_find_runtimes);
        reopened.path.clone_from(&self.path);
        reopened.tempdir.clone_from(&self.tempdir);
        reopened.clock = std::sync::Mutex::new(clock);
        reopened.procedures = procedures;
        reopened.provider_refresh_runtimes = provider_refresh_runtimes;
        reopened.provider_find_runtimes = provider_find_runtimes;
        *self = reopened;
        Ok(result)
    }

    /// Compare two checkpoint/current manifest inventories.
    pub fn diff_checkpoints(
        &self,
        request: DiffCheckpointsRequest,
    ) -> Result<ExecutionResult, GfError> {
        let DiffCheckpointsRequest {
            from,
            to,
            scope,
            detail,
            page,
        } = request;
        cancellation(&page)?;
        let binding = diff_request_binding_parts(&from, &to, scope, detail);
        let resolve = |selector| {
            self.resolve_selector(selector).map_err(|error| {
                if page.after.is_some() && error.code() == "GF_CHECKPOINT_NOT_FOUND" {
                    GfError::Api {
                        code: ApiErrorCode::PageSnapshotGone,
                        message: "checkpoint diff endpoint no longer exists".into(),
                    }
                } else {
                    error
                }
            })
        };
        let from = resolve(&from)?;
        let to = resolve(&to)?;
        match detail {
            CheckpointDiffDetail::Summary => summary_diff(&from, &to, scope, binding, &page),
            CheckpointDiffDetail::Records => record_diff(&from, &to, scope, binding, &page),
        }
    }

    fn resolve_selector(&self, selector: &CheckpointSelector) -> Result<DiffEndpoint, GfError> {
        let (checkpoint_uuid, generation) = match selector {
            CheckpointSelector::Named(name) => {
                let (row, generation) = gf_storage::open_checkpoint_generation(
                    self.resolved_generation.container_root(),
                    name,
                )?;
                (row.checkpoint_uuid, generation)
            }
            CheckpointSelector::Current => {
                let generation = gf_storage::resolve_project_generation(
                    self.resolved_generation.container_root(),
                )?;
                (current_endpoint_uuid(&generation), generation)
            }
        };
        Ok(DiffEndpoint {
            checkpoint_uuid,
            generation,
        })
    }
}

struct DiffEndpoint {
    checkpoint_uuid: Uuid,
    generation: gf_storage::ResolvedProjectGeneration,
}

fn summary_diff(
    from: &DiffEndpoint,
    to: &DiffEndpoint,
    scope: CheckpointDiffScope,
    binding: Uuid,
    page: &PageRequest,
) -> Result<ExecutionResult, GfError> {
    let left = inventory(&from.generation, scope)?;
    let right = inventory(&to.generation, scope)?;
    let mut keys = left.keys().chain(right.keys()).cloned().collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    let snapshot = diff_endpoint_snapshot(from, to);
    let cursors = keys
        .iter()
        .map(|key| page_cursor(&[key.0.as_bytes(), key.1.as_bytes(), key.2.as_bytes()]))
        .collect::<Vec<_>>();
    let (start, end) = page_bounds("checkpoint-diff-summary", binding, page, snapshot, &cursors)?;
    let next = (end < keys.len()).then(|| {
        PageToken::new_bound(
            "checkpoint-diff-summary",
            binding,
            snapshot,
            page.limit,
            end,
            cursors[end - 1],
        )
    });
    summary_batch(
        from.checkpoint_uuid,
        to.checkpoint_uuid,
        &keys[start..end],
        &left,
        &right,
        next.as_ref(),
    )
}

#[derive(Clone)]
struct RecordAdapter {
    capability_version: u32,
    record_version: u32,
    encoding: &'static str,
    schema: SchemaRef,
    schema_fingerprint: [u8; 32],
    identity_fields: &'static [&'static str],
    record_uuid_field: Option<&'static str>,
    identity_fingerprint_domain: gf_core::canonical::CanonicalDomain,
    record_fingerprint_domain: gf_core::canonical::CanonicalDomain,
    max_rows: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LogicalRecord {
    record_uuid: Option<Uuid>,
    fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordChange {
    scope: String,
    family: String,
    record_uuid: Option<Uuid>,
    identity: [u8; 32],
    kind: &'static str,
    from: Option<[u8; 32]>,
    to: Option<[u8; 32]>,
}

fn record_diff(
    from: &DiffEndpoint,
    to: &DiffEndpoint,
    scope: CheckpointDiffScope,
    binding: Uuid,
    page: &PageRequest,
) -> Result<ExecutionResult, GfError> {
    let left = logical_records(&from.generation, scope, page)?;
    let right = logical_records(&to.generation, scope, page)?;
    let mut keys = left.keys().chain(right.keys()).cloned().collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    let mut changes = Vec::new();
    for key in keys {
        cancellation(page)?;
        let old = left.get(&key);
        let new = right.get(&key);
        let kind = match (old, new) {
            (None, Some(_)) => "added",
            (Some(_), None) => "removed",
            (Some(a), Some(b)) if a.fingerprint != b.fingerprint => "modified",
            _ => continue,
        };
        changes.push(RecordChange {
            scope: key.0,
            family: key.1,
            identity: key.2,
            record_uuid: old
                .and_then(|row| row.record_uuid)
                .or_else(|| new.and_then(|row| row.record_uuid)),
            kind,
            from: old.map(|row| row.fingerprint),
            to: new.map(|row| row.fingerprint),
        });
    }
    changes.sort_by(|a, b| {
        (&a.scope, &a.family, a.record_uuid, a.identity, a.kind).cmp(&(
            &b.scope,
            &b.family,
            b.record_uuid,
            b.identity,
            b.kind,
        ))
    });
    let snapshot = diff_endpoint_snapshot(from, to);
    let cursors = changes
        .iter()
        .map(|row| {
            let uuid = row.record_uuid.map_or([0; 16], |value| *value.as_bytes());
            page_cursor(&[
                row.scope.as_bytes(),
                row.family.as_bytes(),
                &uuid,
                &row.identity,
                row.kind.as_bytes(),
            ])
        })
        .collect::<Vec<_>>();
    let (start, end) = page_bounds("checkpoint-diff-records", binding, page, snapshot, &cursors)?;
    let next = (end < changes.len()).then(|| {
        PageToken::new_bound(
            "checkpoint-diff-records",
            binding,
            snapshot,
            page.limit,
            end,
            cursors[end - 1],
        )
    });
    record_batch(
        from.checkpoint_uuid,
        to.checkpoint_uuid,
        &changes[start..end],
        next.as_ref(),
    )
}

type LogicalRecords = BTreeMap<(String, String, [u8; 32]), LogicalRecord>;

#[allow(clippy::too_many_lines)]
fn logical_records(
    generation: &gf_storage::ResolvedProjectGeneration,
    scope: CheckpointDiffScope,
    page: &PageRequest,
) -> Result<LogicalRecords, GfError> {
    let adapters = record_adapters()?;
    let mut out = BTreeMap::new();
    for descriptor in generation.participant_descriptors()? {
        let domain = participant_scope(&descriptor.capability_id, &descriptor.record_family_id);
        if !scope_matches(scope, domain) {
            continue;
        }
        cancellation(page)?;
        if descriptor.capability_id == "graph" && descriptor.record_family_id == "snapshot" {
            let records = crate::checkpoint_graph_diff::extract_logical_graph_records(
                generation,
                page.cancellation.as_ref(),
            )?;
            for (family, records) in [("nodes", records.nodes), ("edges", records.edges)] {
                for record in records {
                    let identity: [u8; 32] = Sha256::digest(record.record_uuid.as_bytes()).into();
                    out.insert(
                        (domain.into(), family.into(), identity),
                        LogicalRecord {
                            record_uuid: Some(record.record_uuid),
                            fingerprint: record.fingerprint,
                        },
                    );
                }
            }
            continue;
        }
        if descriptor.capability_id == gf_storage::WORKSPACE_CAPABILITY_ID {
            if descriptor.record_family_id == "restoration_transition" {
                // Revert validation treats this canonical, storage-owned row as
                // the sole permitted delta from the checkpoint snapshot.
                continue;
            }
            let snapshot = generation
                .participant_snapshot(&descriptor.capability_id, &descriptor.record_family_id)?
                .ok_or_else(|| GfError::Api {
                    code: ApiErrorCode::SchemaMismatch,
                    message: "workspace participant disappeared during checkpoint diff".into(),
                })?;
            match descriptor.record_family_id.as_str() {
                gf_storage::WORKSPACE_ONTOLOGY_FAMILY => {
                    gf_storage::WorkspaceOntology::from_canonical_json(&snapshot.bytes)?;
                }
                gf_storage::WORKSPACE_CONFIGURATION_FAMILY => {
                    gf_storage::WorkspaceConfiguration::from_canonical_json(&snapshot.bytes)?;
                }
                gf_storage::WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY => {
                    gf_storage::WorkspaceRepositorySnapshot::from_canonical_json(&snapshot.bytes)?;
                }
                _ => {
                    return Err(GfError::Api {
                        code: ApiErrorCode::SchemaMismatch,
                        message: "unregistered workspace checkpoint diff participant".into(),
                    });
                }
            }
            let identity: [u8; 32] =
                Sha256::digest(format!("workspace:{}", descriptor.record_family_id).as_bytes())
                    .into();
            let fingerprint: [u8; 32] = Sha256::digest(&snapshot.bytes).into();
            out.insert(
                (domain.into(), descriptor.record_family_id.clone(), identity),
                LogicalRecord {
                    record_uuid: None,
                    fingerprint,
                },
            );
            continue;
        }
        let key = (
            descriptor.capability_id.as_str(),
            descriptor.record_family_id.as_str(),
        );
        let adapter = adapters.get(&key).ok_or_else(|| GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: format!(
                "no logical checkpoint diff adapter for {}@{}",
                descriptor.capability_id, descriptor.record_family_id
            ),
        })?;
        if descriptor.encoding != adapter.encoding
            || descriptor.capability_version != adapter.capability_version
            || descriptor.record_version != adapter.record_version
            || descriptor.schema_fingerprint != adapter.schema_fingerprint
            || descriptor.row_count > adapter.max_rows as u64
        {
            return Err(GfError::Api {
                code: ApiErrorCode::SchemaMismatch,
                message: format!(
                    "checkpoint diff contract mismatch for {}@{}",
                    descriptor.capability_id, descriptor.record_family_id
                ),
            });
        }
        let snapshot = generation
            .participant_snapshot(&descriptor.capability_id, &descriptor.record_family_id)?
            .ok_or_else(|| GfError::Api {
                code: ApiErrorCode::SchemaMismatch,
                message: "manifest participant disappeared during checkpoint diff".into(),
            })?;
        let batches = read_parquet(&snapshot.bytes, page)?;
        let decoded_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let expected_rows = usize::try_from(descriptor.row_count).map_err(|_| GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: "checkpoint participant row count exceeds this platform".into(),
        })?;
        if decoded_rows != expected_rows {
            return Err(GfError::Api {
                code: ApiErrorCode::SchemaMismatch,
                message: "checkpoint participant row count does not match its manifest".into(),
            });
        }
        for batch in batches {
            if batch.schema().fields() != adapter.schema.fields() {
                return Err(GfError::Api {
                    code: ApiErrorCode::SchemaMismatch,
                    message: "checkpoint participant Arrow schema is incompatible".into(),
                });
            }
            for row in 0..batch.num_rows() {
                if row % 4096 == 0 {
                    cancellation(page)?;
                }
                let identity_batch = project_row(&batch, row, adapter.identity_fields)?;
                let identity_payload =
                    crate::canonical_arrow::result_fingerprint(&[identity_batch])
                        .map_err(|error| GfError::Validation(error.to_string()))?;
                let identity = gf_core::canonical::fingerprint(
                    adapter.identity_fingerprint_domain,
                    gf_core::canonical::CANONICAL_CONTRACT_VERSION,
                    &identity_payload,
                )
                .map_err(|error| GfError::Validation(error.to_string()))?;
                let record = batch.slice(row, 1);
                let record_payload = crate::canonical_arrow::result_fingerprint(&[record])
                    .map_err(|error| GfError::Validation(error.to_string()))?;
                let fingerprint = gf_core::canonical::fingerprint(
                    adapter.record_fingerprint_domain,
                    gf_core::canonical::CANONICAL_CONTRACT_VERSION,
                    &record_payload,
                )
                .map_err(|error| GfError::Validation(error.to_string()))?;
                let record_uuid = adapter
                    .record_uuid_field
                    .map(|field| record_uuid(&batch, row, field))
                    .transpose()?;
                if out
                    .insert(
                        (domain.into(), descriptor.record_family_id.clone(), identity),
                        LogicalRecord {
                            record_uuid,
                            fingerprint,
                        },
                    )
                    .is_some()
                {
                    return Err(GfError::Api {
                        code: ApiErrorCode::SchemaMismatch,
                        message: "checkpoint participant has duplicate logical identity".into(),
                    });
                }
            }
        }
    }
    Ok(out)
}

fn record_adapters() -> Result<BTreeMap<(&'static str, &'static str), RecordAdapter>, GfError> {
    let mut out = BTreeMap::new();
    for entry in gf_knowledge::schema_registry() {
        if entry.diff_identity_fields.is_empty() {
            return Err(GfError::Api {
                code: ApiErrorCode::SchemaMismatch,
                message: "checkpoint diff adapter has no identity fields".into(),
            });
        }
        let prior = out.insert(
            (entry.capability_id, entry.record_family),
            RecordAdapter {
                capability_version: entry.capability_version,
                record_version: entry.record_version,
                encoding: "parquet",
                schema: Arc::clone(&entry.schema),
                schema_fingerprint: entry.schema_fingerprint,
                identity_fields: entry.diff_identity_fields,
                record_uuid_field: entry.diff_record_uuid_field,
                identity_fingerprint_domain: entry.diff_identity_fingerprint_domain(),
                record_fingerprint_domain: entry.diff_record_fingerprint_domain(),
                max_rows: entry.max_rows,
            },
        );
        if prior.is_some() {
            return Err(GfError::Api {
                code: ApiErrorCode::SchemaMismatch,
                message: "duplicate checkpoint diff adapter registration".into(),
            });
        }
    }
    for entry in gf_provenance::schema_registry() {
        if entry.diff_identity_fields.is_empty() {
            return Err(GfError::Api {
                code: ApiErrorCode::SchemaMismatch,
                message: "checkpoint diff adapter has no identity fields".into(),
            });
        }
        let prior = out.insert(
            (entry.capability_id, entry.record_family),
            RecordAdapter {
                capability_version: entry.capability_version,
                record_version: entry.record_version,
                encoding: "parquet",
                schema: Arc::clone(&entry.schema),
                schema_fingerprint: entry.schema_fingerprint,
                identity_fields: entry.diff_identity_fields,
                record_uuid_field: entry.diff_record_uuid_field,
                identity_fingerprint_domain: entry.diff_identity_fingerprint_domain(),
                record_fingerprint_domain: entry.diff_record_fingerprint_domain(),
                max_rows: entry.max_rows,
            },
        );
        if prior.is_some() {
            return Err(GfError::Api {
                code: ApiErrorCode::SchemaMismatch,
                message: "duplicate checkpoint diff adapter registration".into(),
            });
        }
    }
    Ok(out)
}

fn read_parquet(bytes: &[u8], page: &PageRequest) -> Result<Vec<RecordBatch>, GfError> {
    let file =
        tempfile::NamedTempFile::new().map_err(|error| GfError::Storage(error.to_string()))?;
    std::fs::write(file.path(), bytes).map_err(|error| GfError::Storage(error.to_string()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        file.reopen()
            .map_err(|error| GfError::Storage(error.to_string()))?,
    )
    .map_err(|error| GfError::Validation(format!("invalid checkpoint parquet: {error}")))?
    .with_batch_size(4096)
    .build()
    .map_err(|error| GfError::Validation(format!("invalid checkpoint parquet: {error}")))?;
    let mut batches = Vec::new();
    for batch in reader {
        cancellation(page)?;
        batches.push(batch.map_err(|error| {
            GfError::Validation(format!("invalid checkpoint parquet: {error}"))
        })?);
    }
    Ok(batches)
}

fn project_row(batch: &RecordBatch, row: usize, fields: &[&str]) -> Result<RecordBatch, GfError> {
    let mut projected_fields = Vec::with_capacity(fields.len());
    let mut columns = Vec::with_capacity(fields.len());
    for name in fields {
        let index = batch.schema().index_of(name).map_err(|_| GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: format!("checkpoint diff identity field {name} is absent"),
        })?;
        projected_fields.push(batch.schema().field(index).clone());
        columns.push(batch.column(index).slice(row, 1));
    }
    RecordBatch::try_new(Arc::new(Schema::new(projected_fields)), columns)
        .map_err(|error| GfError::Execution(error.to_string()))
}

fn record_uuid(batch: &RecordBatch, row: usize, field: &str) -> Result<Uuid, GfError> {
    let values = batch
        .column_by_name(field)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: format!("checkpoint diff UUID field {field} is incompatible"),
        })?;
    if values.is_null(row) || values.value_length() != 16 {
        return Err(GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: format!("checkpoint diff UUID field {field} is invalid"),
        });
    }
    Uuid::from_slice(values.value(row)).map_err(|_| GfError::Api {
        code: ApiErrorCode::SchemaMismatch,
        message: format!("checkpoint diff UUID field {field} is invalid"),
    })
}

type Inventory = BTreeMap<(String, String, String), gf_storage::ProjectParticipantDescriptor>;

fn validate_revert_source(
    generation: &gf_storage::ResolvedProjectGeneration,
) -> Result<(), GfError> {
    generation.validate_complete_participant_inventory()?;
    let _workspace = crate::hydrate_graph_workspace(generation)?;
    let _ontology = generation
        .participant_snapshot(
            gf_storage::WORKSPACE_CAPABILITY_ID,
            gf_storage::WORKSPACE_ONTOLOGY_FAMILY,
        )?
        .map(|snapshot| gf_storage::WorkspaceOntology::from_canonical_json(&snapshot.bytes))
        .transpose()?;
    let _configuration = generation
        .participant_snapshot(
            gf_storage::WORKSPACE_CAPABILITY_ID,
            gf_storage::WORKSPACE_CONFIGURATION_FAMILY,
        )?
        .map(|snapshot| gf_storage::WorkspaceConfiguration::from_canonical_json(&snapshot.bytes))
        .transpose()?;
    let _records = logical_records(
        generation,
        CheckpointDiffScope::All,
        &PageRequest::default(),
    )?;
    // Run each domain owner's decoder as well as the generic checkpoint adapters.
    // These readers enforce each ledger's schema and ledger-local invariants.
    let provenance = generation
        .capability("provenance")?
        .map(|_| crate::provenance::read_ledger(generation))
        .transpose()?;
    let mut knowledge = None;
    let mut confidence = None;
    let mut evidence = None;
    let mut algorithm_runs = None;
    if generation.capability("knowledge")?.is_some() {
        knowledge = Some(crate::knowledge::read_ledger(generation)?);
        confidence = Some(crate::knowledge::read_confidence_ledger(generation)?);
        evidence = Some(crate::knowledge::read_evidence_ledger(generation)?);
        algorithm_runs = Some(crate::algorithm_runs::read_ledger(generation)?);
    }
    let mut reasoning = None;
    let mut statuses = None;
    let mut supersessions = None;
    let mut hypotheses = None;
    if generation.capability("epistemic")?.is_some() {
        reasoning = Some(crate::knowledge::read_reasoning_ledger(generation)?);
        statuses = Some(crate::knowledge::read_status_ledger(generation)?);
        supersessions = Some(crate::knowledge::read_supersession_ledger(generation)?);
        hypotheses = Some(crate::hypotheses::read_ledger(generation)?);
    }
    let mut valid_time = None;
    if generation.capability("valid_time")?.is_some() {
        valid_time = Some(crate::valid_time::read_ledger(generation)?);
    }
    validate_composite_references(CompositeLedgers {
        provenance: provenance.as_ref(),
        knowledge: knowledge.as_ref(),
        confidence: confidence.as_ref(),
        evidence: evidence.as_ref(),
        reasoning: reasoning.as_ref(),
        statuses: statuses.as_ref(),
        supersessions: supersessions.as_ref(),
        hypotheses: hypotheses.as_ref(),
        valid_time: valid_time.as_ref(),
        algorithm_runs: algorithm_runs.as_ref(),
    })
}

#[derive(Clone, Copy)]
struct CompositeLedgers<'a> {
    provenance: Option<&'a gf_provenance::ProvenanceLedger>,
    knowledge: Option<&'a gf_knowledge::AssertionLedger>,
    confidence: Option<&'a gf_knowledge::ConfidenceLedger>,
    evidence: Option<&'a gf_knowledge::EvidenceLedger>,
    reasoning: Option<&'a gf_knowledge::ReasoningLedger>,
    statuses: Option<&'a gf_knowledge::AssertionStatusLedger>,
    supersessions: Option<&'a gf_knowledge::AssertionSupersessionLedger>,
    hypotheses: Option<&'a gf_knowledge::HypothesisLedger>,
    valid_time: Option<&'a gf_knowledge::AssertionValidityLedger>,
    algorithm_runs: Option<&'a gf_knowledge::AlgorithmRunLedger>,
}

#[expect(
    clippy::too_many_lines,
    reason = "one linear pass keeps the complete cross-ledger reference matrix auditable"
)]
fn validate_composite_references(ledgers: CompositeLedgers<'_>) -> Result<(), GfError> {
    let assertion_ids = ledgers
        .knowledge
        .into_iter()
        .flat_map(|ledger| ledger.assertions.iter().map(|row| row.assertion_uuid))
        .collect::<HashSet<_>>();
    let confidence_ids = ledgers
        .confidence
        .into_iter()
        .flat_map(|ledger| ledger.assessments.iter().map(|row| row.confidence_uuid))
        .collect::<HashSet<_>>();
    let reasoning_ids = ledgers
        .reasoning
        .into_iter()
        .flat_map(|ledger| ledger.records.iter().map(|row| row.reasoning_uuid))
        .collect::<HashSet<_>>();
    let provenance_ids = ledgers
        .provenance
        .into_iter()
        .flat_map(|ledger| ledger.events.iter().map(|row| row.provenance_uuid))
        .collect::<HashSet<_>>();
    let status_ids = ledgers
        .statuses
        .into_iter()
        .flat_map(|ledger| ledger.events.iter().map(|row| row.status_event_uuid))
        .collect::<HashSet<_>>();

    let require = |present: bool, kind: &'static str| {
        if present {
            Ok(())
        } else {
            Err(GfError::Validation(format!(
                "checkpoint source has a dangling {kind} reference"
            )))
        }
    };
    let provenance = |uuid| require(provenance_ids.contains(&uuid), "provenance");
    for row in ledgers
        .confidence
        .into_iter()
        .flat_map(|value| &value.assessments)
    {
        require(
            assertion_ids.contains(&row.assertion_uuid),
            "confidence assertion",
        )?;
        provenance(row.provenance_uuid)?;
    }
    for row in ledgers.evidence.into_iter().flat_map(|value| &value.links) {
        require(
            assertion_ids.contains(&row.assertion_uuid),
            "evidence assertion",
        )?;
        provenance(row.provenance_uuid)?;
    }
    for row in ledgers
        .reasoning
        .into_iter()
        .flat_map(|value| &value.records)
    {
        require(
            assertion_ids.contains(&row.assertion_uuid),
            "reasoning assertion",
        )?;
        provenance(row.provenance_uuid)?;
    }
    for row in ledgers.statuses.into_iter().flat_map(|value| &value.events) {
        require(
            assertion_ids.contains(&row.assertion_uuid),
            "status assertion",
        )?;
        if let Some(uuid) = row.confidence_uuid {
            require(confidence_ids.contains(&uuid), "status confidence")?;
        }
        if let Some(uuid) = row.reasoning_uuid {
            require(reasoning_ids.contains(&uuid), "status reasoning")?;
        }
        provenance(row.provenance_uuid)?;
    }
    for row in ledgers
        .supersessions
        .into_iter()
        .flat_map(gf_knowledge::AssertionSupersessionLedger::relations)
    {
        require(
            assertion_ids.contains(&row.prior_assertion_uuid),
            "supersession assertion",
        )?;
        require(
            assertion_ids.contains(&row.replacement_assertion_uuid),
            "supersession assertion",
        )?;
        require(
            status_ids.contains(&row.status_event_uuid),
            "supersession status",
        )?;
        require(
            reasoning_ids.contains(&row.reasoning_uuid),
            "supersession reasoning",
        )?;
        provenance(row.provenance_uuid)?;
    }
    if let Some(ledger) = ledgers.hypotheses {
        for row in ledger.groups() {
            provenance(row.provenance_uuid)?;
        }
        for row in ledger.membership_events() {
            require(
                assertion_ids.contains(&row.assertion_uuid),
                "hypothesis assertion",
            )?;
            require(
                reasoning_ids.contains(&row.reasoning_uuid),
                "hypothesis reasoning",
            )?;
            provenance(row.provenance_uuid)?;
        }
        for row in ledger.selection_events() {
            if let Some(uuid) = row.selected_assertion_uuid {
                require(
                    assertion_ids.contains(&uuid),
                    "hypothesis selection assertion",
                )?;
            }
            require(
                reasoning_ids.contains(&row.reasoning_uuid),
                "hypothesis reasoning",
            )?;
            provenance(row.provenance_uuid)?;
        }
    }
    for row in ledgers
        .valid_time
        .into_iter()
        .flat_map(|value| &value.events)
    {
        require(
            assertion_ids.contains(&row.assertion_uuid),
            "valid-time assertion",
        )?;
        if let Some(uuid) = row.reasoning_uuid {
            require(reasoning_ids.contains(&uuid), "valid-time reasoning")?;
        }
        provenance(row.provenance_uuid)?;
    }
    if let Some(ledger) = ledgers.algorithm_runs {
        for row in &ledger.runs {
            provenance(row.provenance_uuid)?;
        }
        for row in &ledger.events {
            provenance(row.provenance_uuid)?;
        }
    }
    Ok(())
}

fn inventory(
    generation: &gf_storage::ResolvedProjectGeneration,
    scope: CheckpointDiffScope,
) -> Result<Inventory, GfError> {
    let mut out = BTreeMap::new();
    for row in generation.participant_descriptors()? {
        let domain = participant_scope(&row.capability_id, &row.record_family_id);
        if scope_matches(scope, domain) {
            out.insert(
                (
                    domain.into(),
                    row.capability_id.clone(),
                    row.record_family_id.clone(),
                ),
                row,
            );
        }
    }
    Ok(out)
}

fn participant_scope(capability: &str, family: &str) -> &'static str {
    match capability {
        "graph" => "graph",
        "ontology" => "ontology",
        "provenance" => "provenance",
        "knowledge" => "knowledge",
        "epistemic" | "valid_time" => "epistemic",
        "workspace" if family == "ontology" => "ontology",
        "workspace" if family == "configuration" => "configuration",
        _ => "capabilities",
    }
}
fn scope_matches(requested: CheckpointDiffScope, actual: &str) -> bool {
    matches!(
        requested,
        CheckpointDiffScope::Summary | CheckpointDiffScope::All
    ) || matches!(
        (requested, actual),
        (CheckpointDiffScope::Graph, "graph")
            | (CheckpointDiffScope::Ontology, "ontology")
            | (CheckpointDiffScope::Configuration, "configuration")
            | (CheckpointDiffScope::Capabilities, "capabilities")
            | (CheckpointDiffScope::Provenance, "provenance")
            | (CheckpointDiffScope::Knowledge, "knowledge")
            | (CheckpointDiffScope::Epistemic, "epistemic")
    )
}

fn checkpoint_list_snapshot(rows: &[gf_storage::CheckpointRecord]) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"graphforge-checkpoint-list-page/1");
    for row in rows {
        h.update(row.checkpoint_uuid.as_bytes());
        h.update(row.created_revision.to_be_bytes());
    }
    gf_core::canonical::uuid_v8(h.finalize().into())
}
fn current_endpoint_uuid(g: &gf_storage::ResolvedProjectGeneration) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"graphforge-current-checkpoint-endpoint/1");
    h.update(g.generation_uuid().as_bytes());
    h.update(g.manifest_sha256());
    gf_core::canonical::uuid_v8(h.finalize().into())
}
fn diff_endpoint_snapshot(from: &DiffEndpoint, to: &DiffEndpoint) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"graphforge-checkpoint-diff-page/1");
    h.update(from.checkpoint_uuid.as_bytes());
    h.update(from.generation.manifest_sha256());
    h.update(to.checkpoint_uuid.as_bytes());
    h.update(to.generation.manifest_sha256());
    gf_core::canonical::uuid_v8(h.finalize().into())
}
fn request_binding(method: &str, scope: u8, detail: u8) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"graphforge-page-request-binding/1");
    h.update(method.as_bytes());
    h.update([scope, detail]);
    gf_core::canonical::uuid_v8(h.finalize().into())
}
fn diff_request_binding_parts(
    from: &CheckpointSelector,
    to: &CheckpointSelector,
    scope: CheckpointDiffScope,
    detail: CheckpointDiffDetail,
) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"graphforge-checkpoint-diff-request/1");
    for selector in [from, to] {
        match selector {
            CheckpointSelector::Named(name) => {
                h.update([0]);
                h.update((name.len() as u64).to_be_bytes());
                h.update(name.as_bytes());
            }
            CheckpointSelector::Current => h.update([1]),
        }
    }
    h.update([scope as u8, detail as u8]);
    gf_core::canonical::uuid_v8(h.finalize().into())
}
fn page_cursor(parts: &[&[u8]]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"graphforge-page-last-sort-tuple/1");
    for part in parts {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part);
    }
    h.finalize().into()
}
fn page_bounds(
    method: &str,
    binding: Uuid,
    page: &PageRequest,
    snapshot: Uuid,
    cursors: &[[u8; 32]],
) -> Result<(usize, usize), GfError> {
    if !(1..=crate::paging::MAX_PAGE_LIMIT).contains(&page.limit) {
        return Err(GfError::Validation(format!(
            "page limit must be in 1..={}",
            crate::paging::MAX_PAGE_LIMIT
        )));
    }
    cancellation(page)?;
    let start = match &page.after {
        Some(token) => {
            let (offset, cursor) = token.decode_bound(method, binding, snapshot, page.limit)?;
            if offset == 0 || cursors.get(offset - 1) != Some(&cursor) {
                return Err(GfError::Api {
                    code: ApiErrorCode::PageInvalid,
                    message: "page token cursor is not the last complete sort tuple".into(),
                });
            }
            offset
        }
        None => 0,
    };
    let count = cursors.len();
    if start > count {
        return Err(GfError::Api {
            code: gf_core::ApiErrorCode::PageInvalid,
            message: "page token offset exceeds result rows".into(),
        });
    }
    Ok((start, start.saturating_add(page.limit as usize).min(count)))
}
fn cancellation(page: &PageRequest) -> Result<(), GfError> {
    if let Some(c) = &page.cancellation {
        c.checkpoint()?;
    }
    Ok(())
}
fn read_only<T>() -> Result<T, GfError> {
    Err(GfError::Project {
        code: ProjectErrorCode::ReadOnlyView,
        message: "checkpoint views are read-only".into(),
    })
}

fn execution(batch: RecordBatch) -> ExecutionResult {
    let rows = batch.num_rows() as u64;
    ExecutionResult {
        schema: batch.schema(),
        batches: vec![batch],
        stats: gf_exec::ExecutionStats {
            rows_produced: rows,
            execution_time_ms: 0,
        },
        side_effects: None,
        mutation_receipt: None,
    }
}

fn checkpoint_rows(
    rows: &[gf_storage::CheckpointRecord],
    next: Option<&PageToken>,
) -> Result<ExecutionResult, GfError> {
    let mut id = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut name = StringBuilder::new();
    let mut desc = StringBuilder::new();
    let mut generation = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let mut digest = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
    let mut at = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    let mut by = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    for row in rows {
        id.append_value(row.checkpoint_uuid.as_bytes())
            .map_err(arrow_error)?;
        name.append_value(&row.name);
        match &row.description {
            Some(v) => desc.append_value(v),
            None => desc.append_null(),
        }
        generation
            .append_value(row.generation_uuid.as_bytes())
            .map_err(arrow_error)?;
        digest
            .append_value(decode_hex(&row.generation_manifest_sha256)?)
            .map_err(arrow_error)?;
        at.append_value(row.created_at);
        match row.created_by {
            Some(v) => by.append_value(v.as_bytes()).map_err(arrow_error)?,
            None => by.append_null(),
        }
    }
    let fields = vec![
        Field::new("checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("generation_uuid", DataType::FixedSizeBinary(16), false),
        Field::new(
            "generation_manifest_sha256",
            DataType::FixedSizeBinary(32),
            false,
        ),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("created_by", DataType::FixedSizeBinary(16), true),
    ];
    let batch = make_batch(
        "checkpoint",
        fields,
        vec![
            Arc::new(id.finish()),
            Arc::new(name.finish()),
            Arc::new(desc.finish()),
            Arc::new(generation.finish()),
            Arc::new(digest.finish()),
            Arc::new(at.finish()),
            Arc::new(by.finish()),
        ],
        next,
    )?;
    Ok(execution(batch))
}

fn receipt_result(row: &gf_storage::CheckpointReceipt) -> ExecutionResult {
    let mut op = StringBuilder::new();
    op.append_value(row.operation);
    let mut operation = FixedSizeBinaryBuilder::with_capacity(1, 16);
    operation
        .append_value(row.operation_uuid.as_bytes())
        .expect("UUID width is fixed by the checkpoint receipt contract");
    let mut checkpoint = FixedSizeBinaryBuilder::with_capacity(1, 16);
    checkpoint
        .append_value(row.checkpoint_uuid.as_bytes())
        .expect("UUID width is fixed by the checkpoint receipt contract");
    let mut name = StringBuilder::new();
    name.append_value(&row.name);
    let mut source = FixedSizeBinaryBuilder::with_capacity(1, 16);
    source
        .append_value(row.source_generation_uuid.as_bytes())
        .expect("UUID width is fixed by the checkpoint receipt contract");
    let mut prior_current = FixedSizeBinaryBuilder::with_capacity(1, 16);
    match row.prior_current_generation_uuid {
        Some(value) => prior_current
            .append_value(value.as_bytes())
            .expect("UUID width is fixed by the checkpoint receipt contract"),
        None => prior_current.append_null(),
    }
    let mut result = FixedSizeBinaryBuilder::with_capacity(1, 16);
    match row.result_generation_uuid {
        Some(value) => result
            .append_value(value.as_bytes())
            .expect("UUID width is fixed by the checkpoint receipt contract"),
        None => result.append_null(),
    }
    let mut revision = UInt64Builder::new();
    revision.append_value(row.registry_revision);
    let mut at = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    at.append_value(row.committed_at);
    let fields = vec![
        Field::new("operation", DataType::Utf8, false),
        Field::new("operation_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "source_generation_uuid",
            DataType::FixedSizeBinary(16),
            false,
        ),
        Field::new(
            "prior_current_generation_uuid",
            DataType::FixedSizeBinary(16),
            true,
        ),
        Field::new(
            "result_generation_uuid",
            DataType::FixedSizeBinary(16),
            true,
        ),
        Field::new("registry_revision", DataType::UInt64, false),
        Field::new(
            "committed_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ];
    execution(
        make_batch(
            "checkpoint_receipt",
            fields,
            vec![
                Arc::new(op.finish()),
                Arc::new(operation.finish()),
                Arc::new(checkpoint.finish()),
                Arc::new(name.finish()),
                Arc::new(source.finish()),
                Arc::new(prior_current.finish()),
                Arc::new(result.finish()),
                Arc::new(revision.finish()),
                Arc::new(at.finish()),
            ],
            None,
        )
        .expect("checkpoint receipt columns are constructed from its fixed schema"),
    )
}

fn summary_batch(
    from_id: Uuid,
    to_id: Uuid,
    keys: &[(String, String, String)],
    left: &Inventory,
    right: &Inventory,
    next: Option<&PageToken>,
) -> Result<ExecutionResult, GfError> {
    let row_count = keys.len();
    let mut from = FixedSizeBinaryBuilder::with_capacity(row_count, 16);
    let mut to = FixedSizeBinaryBuilder::with_capacity(row_count, 16);
    let mut scopes = StringBuilder::new();
    let mut caps = StringBuilder::new();
    let mut families = StringBuilder::new();
    let mut kinds = StringBuilder::new();
    let mut lrows = UInt64Builder::new();
    let mut rrows = UInt64Builder::new();
    let mut lschema = FixedSizeBinaryBuilder::with_capacity(row_count, 32);
    let mut rschema = FixedSizeBinaryBuilder::with_capacity(row_count, 32);
    let mut lcontent = FixedSizeBinaryBuilder::with_capacity(row_count, 32);
    let mut rcontent = FixedSizeBinaryBuilder::with_capacity(row_count, 32);
    for key in keys {
        let left_row = left.get(key);
        let right_row = right.get(key);
        from.append_value(from_id.as_bytes()).map_err(arrow_error)?;
        to.append_value(to_id.as_bytes()).map_err(arrow_error)?;
        scopes.append_value(&key.0);
        caps.append_value(&key.1);
        families.append_value(&key.2);
        let kind = match (left_row, right_row) {
            (None, Some(_)) => "added",
            (Some(_), None) => "removed",
            (Some(left_value), Some(right_value)) if left_value == right_value => "unchanged",
            _ => "modified",
        };
        kinds.append_value(kind);
        append_u64(&mut lrows, left_row.map(|value| value.row_count));
        append_u64(&mut rrows, right_row.map(|value| value.row_count));
        append_fixed(
            &mut lschema,
            left_row.map(|value| &value.schema_fingerprint),
        )
        .map_err(arrow_error)?;
        append_fixed(
            &mut rschema,
            right_row.map(|value| &value.schema_fingerprint),
        )
        .map_err(arrow_error)?;
        append_fixed(&mut lcontent, left_row.map(|value| &value.content_sha256))
            .map_err(arrow_error)?;
        append_fixed(&mut rcontent, right_row.map(|value| &value.content_sha256))
            .map_err(arrow_error)?;
    }
    let fields = vec![
        Field::new("from_checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("to_checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("scope", DataType::Utf8, false),
        Field::new("capability_id", DataType::Utf8, false),
        Field::new("record_family_id", DataType::Utf8, false),
        Field::new("change_kind", DataType::Utf8, false),
        Field::new("from_row_count", DataType::UInt64, true),
        Field::new("to_row_count", DataType::UInt64, true),
        Field::new(
            "from_schema_fingerprint",
            DataType::FixedSizeBinary(32),
            true,
        ),
        Field::new("to_schema_fingerprint", DataType::FixedSizeBinary(32), true),
        Field::new("from_content_sha256", DataType::FixedSizeBinary(32), true),
        Field::new("to_content_sha256", DataType::FixedSizeBinary(32), true),
    ];
    Ok(execution(make_batch(
        "checkpoint_summary_diff",
        fields,
        vec![
            Arc::new(from.finish()),
            Arc::new(to.finish()),
            Arc::new(scopes.finish()),
            Arc::new(caps.finish()),
            Arc::new(families.finish()),
            Arc::new(kinds.finish()),
            Arc::new(lrows.finish()),
            Arc::new(rrows.finish()),
            Arc::new(lschema.finish()),
            Arc::new(rschema.finish()),
            Arc::new(lcontent.finish()),
            Arc::new(rcontent.finish()),
        ],
        next,
    )?))
}

fn record_batch(
    from_id: Uuid,
    to_id: Uuid,
    rows: &[RecordChange],
    next: Option<&PageToken>,
) -> Result<ExecutionResult, GfError> {
    let n = rows.len();
    let mut from_checkpoint = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut to_checkpoint = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut scopes = StringBuilder::new();
    let mut families = StringBuilder::new();
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut identities = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut kinds = StringBuilder::new();
    let mut from_fingerprints = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut to_fingerprints = FixedSizeBinaryBuilder::with_capacity(n, 32);
    for row in rows {
        from_checkpoint
            .append_value(from_id.as_bytes())
            .map_err(arrow_error)?;
        to_checkpoint
            .append_value(to_id.as_bytes())
            .map_err(arrow_error)?;
        scopes.append_value(&row.scope);
        families.append_value(&row.family);
        match row.record_uuid {
            Some(value) => uuids.append_value(value.as_bytes()).map_err(arrow_error)?,
            None => uuids.append_null(),
        }
        identities.append_value(row.identity).map_err(arrow_error)?;
        kinds.append_value(row.kind);
        append_fixed(&mut from_fingerprints, row.from.as_ref()).map_err(arrow_error)?;
        append_fixed(&mut to_fingerprints, row.to.as_ref()).map_err(arrow_error)?;
    }
    let fields = vec![
        Field::new("from_checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("to_checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("scope", DataType::Utf8, false),
        Field::new("record_family_id", DataType::Utf8, false),
        Field::new("record_uuid", DataType::FixedSizeBinary(16), true),
        Field::new(
            "record_identity_fingerprint",
            DataType::FixedSizeBinary(32),
            false,
        ),
        Field::new("change_kind", DataType::Utf8, false),
        Field::new(
            "from_record_fingerprint",
            DataType::FixedSizeBinary(32),
            true,
        ),
        Field::new("to_record_fingerprint", DataType::FixedSizeBinary(32), true),
    ];
    Ok(execution(make_batch(
        "checkpoint_record_diff",
        fields,
        vec![
            Arc::new(from_checkpoint.finish()),
            Arc::new(to_checkpoint.finish()),
            Arc::new(scopes.finish()),
            Arc::new(families.finish()),
            Arc::new(uuids.finish()),
            Arc::new(identities.finish()),
            Arc::new(kinds.finish()),
            Arc::new(from_fingerprints.finish()),
            Arc::new(to_fingerprints.finish()),
        ],
        next,
    )?))
}
fn append_u64(b: &mut UInt64Builder, v: Option<u64>) {
    match v {
        Some(v) => b.append_value(v),
        None => b.append_null(),
    }
}
fn append_fixed(
    b: &mut FixedSizeBinaryBuilder,
    v: Option<&[u8; 32]>,
) -> Result<(), arrow::error::ArrowError> {
    if let Some(v) = v {
        b.append_value(v)?;
    } else {
        b.append_null();
    }
    Ok(())
}

fn arrow_error(error: arrow::error::ArrowError) -> GfError {
    let message = error.to_string();
    drop(error);
    GfError::Execution(message)
}
fn make_batch(
    id: &str,
    fields: Vec<Field>,
    columns: Vec<ArrayRef>,
    next: Option<&PageToken>,
) -> Result<RecordBatch, GfError> {
    let mut metadata = HashMap::from([
        ("graphforge.contract.id".into(), id.into()),
        ("graphforge.contract.version".into(), "1".into()),
    ]);
    if let Some(next) = next {
        metadata.insert("graphforge.next_page_token".into(), next.as_str().into());
    }
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, metadata)),
        columns,
    )
    .map_err(|e| GfError::Execution(e.to_string()))
}
fn decode_hex(value: &str) -> Result<[u8; 32], GfError> {
    if value.len() != 64 {
        return Err(GfError::Validation("invalid checkpoint digest".into()));
    }
    let mut out = [0; 32];
    for (i, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        out[i] = u8::from_str_radix(
            std::str::from_utf8(pair)
                .map_err(|_| GfError::Validation("invalid checkpoint digest".into()))?,
            16,
        )
        .map_err(|_| GfError::Validation("invalid checkpoint digest".into()))?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, FixedSizeBinaryArray, StringArray};
    use gf_core::OntologyMode;
    use gf_knowledge::{AssertionGraphRole, AssertionStatus, GraphObjectKind};
    use tempfile::tempdir;

    fn operation(value: u128) -> OperationId {
        OperationId(Uuid::from_u128(value))
    }

    fn enable(graph: &GraphForge, capability_id: crate::CapabilityId, seed: u128) {
        graph
            .enable_capability(crate::EnableCapabilityRequest {
                context: crate::WriteContext {
                    operation_uuid: operation(seed),
                    actor_uuid: None,
                },
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }

    fn uuid7(seed: u8) -> Uuid {
        let mut bytes = [seed; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn write_ontology(root: &std::path::Path) -> std::path::PathBuf {
        let path = root.join("shape.yaml");
        std::fs::write(
            &path,
            "ontology_id: checkpoint-shape\nversion: v1\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types: []\nproperties:\n  - owner: Person\n    name: name\n    type: utf8\n    nullable: false\nconstraints: []\nmigrations: []\n",
        )
        .unwrap();
        path
    }

    #[test]
    fn revert_source_validation_rejects_cross_domain_dangling_assertion() {
        let event = gf_provenance::ProvenanceEvent::new(
            Uuid::from_u128(1),
            gf_provenance::EventKind::AssessConfidence,
            None,
            1,
        )
        .unwrap();
        let provenance = gf_provenance::ProvenanceLedger::new(vec![event.clone()], vec![]).unwrap();
        let confidence = gf_knowledge::ConfidenceLedger::explicit(
            Uuid::now_v7(),
            Uuid::now_v7(),
            0.5,
            event.provenance_uuid,
            1,
        )
        .unwrap();

        let error = validate_composite_references(CompositeLedgers {
            provenance: Some(&provenance),
            knowledge: Some(&gf_knowledge::AssertionLedger::default()),
            confidence: Some(&confidence),
            evidence: None,
            reasoning: None,
            statuses: None,
            supersessions: None,
            hypotheses: None,
            valid_time: None,
            algorithm_runs: None,
        })
        .unwrap_err();

        assert_eq!(error.code(), "GF_VALIDATION");
        assert!(error.to_string().contains("dangling confidence assertion"));
    }

    #[test]
    fn revert_preview_is_non_mutating_and_receipt_identifies_prior_current() {
        let directory = tempdir().unwrap();
        let path = directory.path().to_str().unwrap();
        let mut graph = GraphForge::new(Some(path)).unwrap();
        graph
            .execute("CREATE (:State {value: 'checkpoint'})")
            .unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "Before".into(),
                description: Some("preview target".into()),
                idempotency_key: operation(140),
                actor_uuid: None,
            })
            .unwrap();
        let (checkpoint, _) =
            gf_storage::open_checkpoint_generation(directory.path(), "Before").unwrap();
        graph.execute("CREATE (:State {value: 'current'})").unwrap();
        let current_before = graph.generation_for_read().unwrap().generation_uuid();
        let generations_before = std::fs::read_dir(directory.path().join("generations"))
            .unwrap()
            .count();

        let preview = GraphForge::preview_revert_to_checkpoint(
            directory.path(),
            PreviewRevertCheckpointRequest {
                name: "Before".into(),
            },
        )
        .unwrap();
        assert_eq!(preview.checkpoint_uuid, checkpoint.checkpoint_uuid);
        assert_eq!(preview.source_generation_uuid, checkpoint.generation_uuid);
        assert_eq!(
            preview.source_manifest_sha256,
            checkpoint.generation_manifest_sha256
        );
        assert_eq!(preview.current_generation_uuid, current_before);
        assert_eq!(
            graph.generation_for_read().unwrap().generation_uuid(),
            current_before
        );
        assert_eq!(
            std::fs::read_dir(directory.path().join("generations"))
                .unwrap()
                .count(),
            generations_before
        );

        let missing = GraphForge::preview_revert_to_checkpoint(
            directory.path(),
            PreviewRevertCheckpointRequest {
                name: "Missing".into(),
            },
        )
        .unwrap_err();
        assert_eq!(missing.code(), "GF_CHECKPOINT_NOT_FOUND");

        let receipt = graph
            .revert_to_checkpoint(RevertCheckpointRequest {
                name: "Before".into(),
                reason: "previewed identities".into(),
                idempotency_key: operation(141),
                actor_uuid: None,
            })
            .unwrap();
        let prior_current = receipt.batches[0]
            .column_by_name("prior_current_generation_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(prior_current.value(0), current_before.as_bytes());
    }

    #[test]
    fn revert_is_visible_on_the_same_graphforge_instance() {
        let directory = tempdir().unwrap();
        let write_options = crate::GraphForgeOptions {
            write_mode: crate::ProjectWriteMode::QueuedWriter,
            write_queue_capacity: 7,
            max_rebase_attempts: 2,
        };
        let mut graph =
            GraphForge::new_with_options(Some(directory.path().to_str().unwrap()), write_options)
                .unwrap();
        graph.execute("CREATE (:Person {name: 'before'})").unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "Before".into(),
                description: None,
                idempotency_key: operation(100),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(graph.write_options, write_options);
        graph.execute("CREATE (:Person {name: 'after'})").unwrap();
        let post_checkpoint_handle = graph.add_node("Transient", &HashMap::new()).unwrap();

        let receipt = graph
            .revert_to_checkpoint(RevertCheckpointRequest {
                name: "Before".into(),
                reason: "return to known state".into(),
                idempotency_key: operation(101),
                actor_uuid: None,
            })
            .unwrap();
        assert!(
            !receipt.batches[0]
                .column_by_name("result_generation_uuid")
                .unwrap()
                .is_null(0)
        );
        let rows = graph
            .execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
            .unwrap();
        let names = rows.batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.len(), 1);
        assert_eq!(names.value(0), "before");
        assert!(
            graph
                .add_edge(
                    &post_checkpoint_handle,
                    "STALE",
                    &post_checkpoint_handle,
                    &HashMap::new(),
                )
                .is_err(),
            "revert must invalidate handles owned by the replaced facade"
        );
    }

    #[test]
    fn in_memory_revert_preserves_its_backing_project_owner() {
        let mut graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Memory {state: 'before'})").unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "Memory".into(),
                description: None,
                idempotency_key: operation(130),
                actor_uuid: None,
            })
            .unwrap();
        graph.execute("CREATE (:Memory {state: 'after'})").unwrap();
        graph
            .revert_to_checkpoint(RevertCheckpointRequest {
                name: "Memory".into(),
                reason: "in-memory ownership".into(),
                idempotency_key: operation(131),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(
            graph
                .execute("MATCH (n:Memory) RETURN count(n) AS total")
                .unwrap()
                .stats
                .rows_produced,
            1
        );
        assert!(
            graph
                .checkpoint(CheckpointRequest {
                    name: "AfterMemoryRevert".into(),
                    description: None,
                    idempotency_key: operation(132),
                    actor_uuid: None,
                })
                .is_ok()
        );
    }

    #[test]
    fn revert_accepts_graph_m20_and_full_m21_checkpoint_shapes_after_reopen() {
        for (index, capabilities) in [
            vec![],
            vec![
                crate::CapabilityId::Provenance,
                crate::CapabilityId::Knowledge,
            ],
            vec![
                crate::CapabilityId::Provenance,
                crate::CapabilityId::Knowledge,
                crate::CapabilityId::Epistemic,
                crate::CapabilityId::ValidTime,
            ],
        ]
        .into_iter()
        .enumerate()
        {
            let directory = tempdir().unwrap();
            let mut graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
            for (offset, capability) in capabilities.into_iter().enumerate() {
                enable(
                    &graph,
                    capability,
                    1_000 + index as u128 * 10 + offset as u128,
                );
            }
            graph
                .execute("CREATE (:Shape {state: 'checkpoint'})")
                .unwrap();
            graph
                .checkpoint(CheckpointRequest {
                    name: "Shape".into(),
                    description: None,
                    idempotency_key: operation(1_100 + index as u128),
                    actor_uuid: None,
                })
                .unwrap();
            graph.execute("CREATE (:Shape {state: 'later'})").unwrap();
            graph
                .revert_to_checkpoint(RevertCheckpointRequest {
                    name: "Shape".into(),
                    reason: "shape acceptance".into(),
                    idempotency_key: operation(1_200 + index as u128),
                    actor_uuid: None,
                })
                .unwrap();
            drop(graph);

            let reopened = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
            let count = reopened
                .execute("MATCH (n:Shape) RETURN count(n) AS total")
                .unwrap();
            let totals = count.batches[0]
                .column_by_name("total")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(totals.value(0), 1, "shape {index}");
            assert!(reopened.open_checkpoint("Shape").is_ok());
        }
    }

    #[test]
    fn populated_checkpoint_shapes_restore_real_domain_records() {
        for shape in [
            "ontology-free",
            "emergent",
            "advisory",
            "strict",
            "m20",
            "m21",
        ] {
            let directory = tempdir().unwrap();
            let mut graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
            graph.set_clock_for_test(|| Ok(1_000));

            if matches!(shape, "advisory" | "strict") {
                graph
                    .adopt_ontology(crate::AdoptOntologyRequest {
                        context: crate::WriteContext {
                            operation_uuid: operation(2_000),
                            actor_uuid: None,
                        },
                        path: write_ontology(directory.path()),
                        mode: if shape == "strict" {
                            OntologyMode::Strict
                        } else {
                            OntologyMode::Advisory
                        },
                    })
                    .unwrap();
            }

            let node_uuid = graph.add_node("Person", &HashMap::new()).unwrap().uuid;

            if matches!(shape, "m20" | "m21") {
                enable(&graph, crate::CapabilityId::Provenance, 2_100);
                enable(&graph, crate::CapabilityId::Knowledge, 2_101);
                if shape == "m21" {
                    enable(&graph, crate::CapabilityId::Epistemic, 2_102);
                }
                let assertion = crate::CreateAssertionRequest {
                    context: crate::WriteContext {
                        operation_uuid: operation(2_110),
                        actor_uuid: None,
                    },
                    assertion_uuid: uuid7(110),
                    claim: format!("{shape} checkpoint claim"),
                    graph_refs: vec![crate::AssertionGraphRefInput {
                        graph_uuid: node_uuid,
                        graph_kind: GraphObjectKind::Node,
                        role: AssertionGraphRole::Subject,
                        ordinal: 0,
                    }],
                };
                if shape == "m21" {
                    graph
                        .create_assertion_with_status(crate::CreateAssertionWithStatusRequest {
                            assertion,
                            first_status: crate::FirstAssertionStatusInput {
                                status_event_uuid: uuid7(111),
                                status: AssertionStatus::Hypothesis,
                            },
                        })
                        .unwrap();
                } else {
                    graph.create_assertion(assertion).unwrap();
                }
            }

            graph
                .checkpoint(CheckpointRequest {
                    name: "Populated".into(),
                    description: Some(shape.into()),
                    idempotency_key: operation(2_200),
                    actor_uuid: None,
                })
                .unwrap();
            graph.execute("MATCH (n) DETACH DELETE n").unwrap();
            graph
                .revert_to_checkpoint(RevertCheckpointRequest {
                    name: "Populated".into(),
                    reason: format!("restore {shape}"),
                    idempotency_key: operation(2_201),
                    actor_uuid: None,
                })
                .unwrap_or_else(|error| panic!("{shape}: {error:?}"));
            drop(graph);

            let reopened = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
            let restored = reopened
                .execute("MATCH (n:Person) RETURN count(n) AS total")
                .unwrap();
            assert_eq!(restored.stats.rows_produced, 1, "{shape}");
            if matches!(shape, "m20" | "m21") {
                assert_eq!(
                    reopened
                        .assertion(uuid7(110), None)
                        .unwrap()
                        .stats
                        .rows_produced,
                    1
                );
            }
            if shape == "m21" {
                assert_eq!(
                    reopened
                        .assertion_status(uuid7(110))
                        .unwrap()
                        .stats
                        .rows_produced,
                    1
                );
            }
        }
    }

    #[test]
    fn checkpoint_view_stays_pinned_and_rejects_writes() {
        let directory = tempdir().unwrap();
        let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
        graph.execute("CREATE (:Person {name: 'before'})").unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "Before".into(),
                description: Some("stable view".into()),
                idempotency_key: operation(1),
                actor_uuid: None,
            })
            .unwrap();
        graph.execute("CREATE (:Person {name: 'after'})").unwrap();

        let view = graph.open_checkpoint("Before").unwrap();
        let result = view
            .execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
            .unwrap();
        let names = result.batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.len(), 1);
        assert_eq!(names.value(0), "before");
        assert_eq!(
            view.inspect_adjacency().unwrap().state,
            gf_storage::adjacency::AdjacencyFreshnessState::Missing
        );
        assert_eq!(
            view.execute("CREATE (:Person)").unwrap_err().code(),
            "GF_READ_ONLY_VIEW"
        );

        graph
            .delete_checkpoint(DeleteCheckpointRequest {
                name: "Before".into(),
                idempotency_key: operation(2),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(
            graph.open_checkpoint("Before").unwrap_err().code(),
            "GF_CHECKPOINT_NOT_FOUND"
        );
        std::fs::write(directory.path().join("CURRENT"), b"invalid\n").unwrap();
        assert!(!view.project_capabilities().unwrap().batches.is_empty());
        assert_eq!(
            view.enable_capability(crate::EnableCapabilityRequest {
                context: crate::WriteContext {
                    operation_uuid: operation(3),
                    actor_uuid: None,
                },
                capability_id: crate::CapabilityId::Knowledge,
                capability_version: 1,
            })
            .unwrap_err()
            .code(),
            "GF_READ_ONLY_VIEW"
        );
        assert_eq!(
            view.add_node("Person", &HashMap::new()).unwrap_err().code(),
            "GF_READ_ONLY_VIEW"
        );
        assert_eq!(
            view.index_adjacency().unwrap_err().code(),
            "GF_READ_ONLY_VIEW"
        );
        assert_eq!(
            view.execute("MATCH (n) RETURN count(n) AS total")
                .unwrap()
                .batches[0]
                .num_rows(),
            1
        );
    }

    #[test]
    fn list_and_summary_diff_are_arrow_ordered_and_page_bound() {
        let directory = tempdir().unwrap();
        let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "A".into(),
                description: None,
                idempotency_key: operation(10),
                actor_uuid: None,
            })
            .unwrap();
        graph.execute("CREATE (:Person {name: 'changed'})").unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "B".into(),
                description: None,
                idempotency_key: operation(11),
                actor_uuid: None,
            })
            .unwrap();

        let listed = graph
            .list_checkpoints(ListCheckpointsRequest {
                page: PageRequest {
                    limit: 1,
                    after: None,
                    cancellation: None,
                },
            })
            .unwrap();
        assert_eq!(listed.batches[0].num_rows(), 1);
        assert!(
            listed
                .schema
                .metadata()
                .contains_key("graphforge.next_page_token")
        );

        let diff = graph
            .diff_checkpoints(DiffCheckpointsRequest {
                from: CheckpointSelector::Named("A".into()),
                to: CheckpointSelector::Named("B".into()),
                scope: CheckpointDiffScope::All,
                detail: CheckpointDiffDetail::Summary,
                page: PageRequest::default(),
            })
            .unwrap();
        assert!(diff.batches[0].num_rows() >= 3);
        assert_eq!(diff.schema.field(0).name(), "from_checkpoint_uuid");

        let first_page = graph
            .diff_checkpoints(DiffCheckpointsRequest {
                from: CheckpointSelector::Named("A".into()),
                to: CheckpointSelector::Named("B".into()),
                scope: CheckpointDiffScope::All,
                detail: CheckpointDiffDetail::Summary,
                page: PageRequest {
                    limit: 1,
                    after: None,
                    cancellation: None,
                },
            })
            .unwrap();
        let token =
            PageToken::parse(first_page.schema.metadata()["graphforge.next_page_token"].as_str())
                .unwrap();
        graph
            .delete_checkpoint(DeleteCheckpointRequest {
                name: "B".into(),
                idempotency_key: operation(12),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(
            graph
                .diff_checkpoints(DiffCheckpointsRequest {
                    from: CheckpointSelector::Named("A".into()),
                    to: CheckpointSelector::Named("B".into()),
                    scope: CheckpointDiffScope::All,
                    detail: CheckpointDiffDetail::Summary,
                    page: PageRequest {
                        limit: 1,
                        after: Some(token),
                        cancellation: None,
                    },
                })
                .unwrap_err()
                .code(),
            "GF_PAGE_SNAPSHOT_GONE"
        );
    }

    #[test]
    fn show_checkpoint_returns_the_exact_list_metadata_row_and_rejects_unknown_names() {
        let directory = tempdir().unwrap();
        let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
        let actor_uuid = Uuid::from_u128(91);
        graph
            .checkpoint(CheckpointRequest {
                name: "Release".into(),
                description: Some("ready to publish".into()),
                idempotency_key: operation(90),
                actor_uuid: Some(actor_uuid),
            })
            .unwrap();

        let listed = graph
            .list_checkpoints(ListCheckpointsRequest::default())
            .unwrap();
        let shown = graph
            .show_checkpoint(ShowCheckpointRequest {
                name: "Release".into(),
            })
            .unwrap();

        assert_eq!(shown.schema, listed.schema);
        assert_eq!(shown.batches, listed.batches);
        assert_eq!(shown.batches[0].num_rows(), 1);

        let error = graph
            .show_checkpoint(ShowCheckpointRequest {
                name: "release".into(),
            })
            .unwrap_err();
        assert_eq!(error.code(), "GF_CHECKPOINT_NOT_FOUND");
    }

    #[test]
    fn record_diff_uses_registered_logical_adapters() {
        let directory = tempdir().unwrap();
        let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
        graph
            .enable_capability(crate::EnableCapabilityRequest {
                context: crate::WriteContext {
                    operation_uuid: operation(20),
                    actor_uuid: None,
                },
                capability_id: crate::CapabilityId::Knowledge,
                capability_version: 1,
            })
            .unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "A".into(),
                description: None,
                idempotency_key: operation(21),
                actor_uuid: None,
            })
            .unwrap();
        graph.execute("CREATE (:Person {name: 'added'})").unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "B".into(),
                description: None,
                idempotency_key: operation(22),
                actor_uuid: None,
            })
            .unwrap();
        let diff = graph
            .diff_checkpoints(DiffCheckpointsRequest {
                from: CheckpointSelector::Named("A".into()),
                to: CheckpointSelector::Named("B".into()),
                scope: CheckpointDiffScope::All,
                detail: CheckpointDiffDetail::Records,
                page: PageRequest::default(),
            })
            .unwrap();
        assert_eq!(diff.batches[0].num_rows(), 1);
        assert_eq!(diff.schema.field(5).name(), "record_identity_fingerprint");
    }

    #[test]
    fn record_diff_pagination_cancellation_and_bounds() {
        let directory = tempdir().unwrap();
        let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "BeforeRecords".into(),
                description: None,
                idempotency_key: operation(3_000),
                actor_uuid: None,
            })
            .unwrap();
        graph
            .execute("CREATE (:Person), (:Person), (:Person)")
            .unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "AfterRecords".into(),
                description: None,
                idempotency_key: operation(3_001),
                actor_uuid: None,
            })
            .unwrap();

        let request = |page| DiffCheckpointsRequest {
            from: CheckpointSelector::Named("BeforeRecords".into()),
            to: CheckpointSelector::Named("AfterRecords".into()),
            scope: CheckpointDiffScope::Graph,
            detail: CheckpointDiffDetail::Records,
            page,
        };
        let first = graph
            .diff_checkpoints(request(PageRequest {
                limit: 1,
                after: None,
                cancellation: None,
            }))
            .unwrap();
        assert_eq!(first.stats.rows_produced, 1);
        let token =
            PageToken::parse(first.schema.metadata()["graphforge.next_page_token"].as_str())
                .unwrap();
        let second = graph
            .diff_checkpoints(request(PageRequest {
                limit: 1,
                after: Some(token),
                cancellation: None,
            }))
            .unwrap();
        assert_eq!(second.stats.rows_produced, 1);
        let token =
            PageToken::parse(second.schema.metadata()["graphforge.next_page_token"].as_str())
                .unwrap();
        let third = graph
            .diff_checkpoints(request(PageRequest {
                limit: 1,
                after: Some(token),
                cancellation: None,
            }))
            .unwrap();
        assert_eq!(third.stats.rows_produced, 1);
        let first_ids = first.batches[0]
            .column_by_name("record_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let second_ids = second.batches[0]
            .column_by_name("record_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let third_ids = third.batches[0]
            .column_by_name("record_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!(first_ids.value(0) < second_ids.value(0));
        assert!(second_ids.value(0) < third_ids.value(0));

        let cancelled = crate::CancellationToken::new();
        cancelled.cancel();
        assert_eq!(
            graph
                .diff_checkpoints(request(PageRequest {
                    limit: 1,
                    after: None,
                    cancellation: Some(cancelled),
                }))
                .unwrap_err()
                .code(),
            "GF_CANCELLED"
        );
        for limit in [0, 10_001] {
            let error = graph
                .diff_checkpoints(request(PageRequest {
                    limit,
                    after: None,
                    cancellation: None,
                }))
                .unwrap_err();
            assert_eq!(error.code(), "GF_VALIDATION");
            assert!(error.to_string().contains("1..=10000"));
        }
    }

    #[test]
    fn checkpoint_revert_corruption_matrix_fails_closed() {
        for corrupt in ["registry", "checksum", "participant"] {
            let directory = tempdir().unwrap();
            let mut graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
            graph.execute("CREATE (:Stable {value: 1})").unwrap();
            graph
                .checkpoint(CheckpointRequest {
                    name: "Stable".into(),
                    description: None,
                    idempotency_key: operation(4_000),
                    actor_uuid: None,
                })
                .unwrap();

            match corrupt {
                "registry" => std::fs::write(
                    directory.path().join("checkpoints/registry.json"),
                    b"{invalid\n",
                )
                .unwrap(),
                "checksum" => std::fs::write(
                    directory.path().join("checkpoints/registry.json.sha256"),
                    b"00\n",
                )
                .unwrap(),
                "participant" => {
                    let (_, generation) =
                        gf_storage::open_checkpoint_generation(directory.path(), "Stable").unwrap();
                    let path = generation.participant_path("graph", "snapshot").unwrap();
                    std::fs::write(path, b"corrupt checkpoint participant").unwrap();
                }
                _ => unreachable!(),
            }

            let before = gf_storage::resolve_project_generation(directory.path())
                .unwrap()
                .generation_uuid();
            let error = graph
                .revert_to_checkpoint(RevertCheckpointRequest {
                    name: "Stable".into(),
                    reason: format!("reject {corrupt}"),
                    idempotency_key: operation(4_001),
                    actor_uuid: None,
                })
                .unwrap_err();
            let expected = if corrupt == "participant" {
                "GF_PROJECT_CORRUPT"
            } else {
                "GF_CHECKPOINT_REGISTRY_CORRUPT"
            };
            assert_eq!(error.code(), expected, "{corrupt}: {error}");
            assert_eq!(
                gf_storage::resolve_project_generation(directory.path())
                    .unwrap()
                    .generation_uuid(),
                before,
                "failed revert must not advance CURRENT for {corrupt}"
            );
        }
    }
}
