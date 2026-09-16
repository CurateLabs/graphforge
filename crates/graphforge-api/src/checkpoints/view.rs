//! Immutable checkpoint-pinned facade and write refusals.

use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use std::collections::HashMap;
use uuid::Uuid;

use super::{CheckpointRequest, DeleteCheckpointRequest, read_only};
use crate::{ExecutionResult, GraphForge, PageRequest};

/// Immutable lease-pinned checkpoint facade.
#[derive(Debug)]
pub struct CheckpointView {
    checkpoint: graphforge_storage::CheckpointRecord,
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
    /// Structural evidence for how the pinned graph workspace was opened.
    #[must_use]
    pub fn graph_open_evidence(&self) -> &graphforge_storage::GraphFilesOpenEvidence {
        self.graph.graph_open_evidence()
    }
    /// Recovery evidence for this read-only checkpoint open (cleanup skipped).
    #[must_use]
    pub fn project_open_recovery(&self) -> &graphforge_storage::ProjectOpenRecoveryEvidence {
        self.graph.project_open_recovery()
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
    pub fn workspace_ontology(&self) -> Result<graphforge_storage::WorkspaceOntology, GfError> {
        self.graph.workspace_ontology()
    }
    /// Return the pinned workspace configuration.
    pub fn workspace_configuration(
        &self,
    ) -> Result<graphforge_storage::WorkspaceConfiguration, GfError> {
        self.graph.workspace_configuration()
    }
    /// Run a pinned algorithm ranking read. Write-back is rejected.
    pub fn rank(&self, label: &str, options: crate::RankOptions) -> Result<RecordBatch, GfError> {
        if options.write_property.is_some() {
            return read_only();
        }
        self.graph.rank(label, options)
    }
    /// Run a pinned algorithm clustering read. Write-back is rejected.
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
    /// Run a pinned algorithm path read.
    pub fn paths<'a>(
        &self,
        source: impl Into<Option<&'a crate::NodeSelector>>,
        target: Option<&crate::NodeSelector>,
        options: crate::PathsOptions,
    ) -> Result<RecordBatch, GfError> {
        self.graph.paths(source, target, options)
    }
    /// Run a pinned algorithm graph analysis.
    pub fn analyze(
        &self,
        label: Option<&str>,
        options: crate::AnalyzeOptions,
    ) -> Result<RecordBatch, GfError> {
        self.graph.analyze(label, options)
    }
    /// Run a pinned algorithm embedding analysis.
    pub fn analyze_embedding(
        &self,
        label: Option<&str>,
        options: &crate::EmbeddingAnalyzeOptions,
    ) -> Result<RecordBatch, GfError> {
        self.graph.analyze_embedding(label, options)
    }
    /// Run a pinned algorithm similarity read.
    pub fn similar(
        &self,
        label: &str,
        options: crate::SimilarOptions,
    ) -> Result<RecordBatch, GfError> {
        self.graph.similar(label, options)
    }
    /// Run pinned search.
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
    /// Reconstruct the pinned epistemic snapshot.
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
    /// Open an immutable view pinned to the named checkpoint generation.
    pub fn open_checkpoint(&self, name: &str) -> Result<CheckpointView, GfError> {
        let (checkpoint, generation) = graphforge_storage::open_checkpoint_generation_with_mode(
            self.resolved_generation.container_root(),
            name,
            self.lifecycle_mode,
        )?;
        let graph = Self::open_resolved_with_lifecycle_mode(
            self.resolved_generation.container_root().to_owned(),
            generation,
            true,
            self.lifecycle_mode,
        )?;
        Ok(CheckpointView { checkpoint, graph })
    }
}

#[cfg(test)]
mod tests;
