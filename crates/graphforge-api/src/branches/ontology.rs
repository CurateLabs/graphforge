//! Branch-local composition validation through the existing native owner.
use super::{ChangeResearchBranchOntologyRequest, edit, publication};
use crate::{
    CancellationToken, CompositionChangeRequest, GfError, GraphForge, OperationId, WriteContext,
};
use graphforge_storage::research_versions::ResearchOperationReceipt;
impl GraphForge {
    /// Validate and install one Branch's exact composition, preserving parent authority.
    pub fn change_research_branch_ontology(
        &mut self,
        request: &ChangeResearchBranchOntologyRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        if request.version_uuid.is_nil() {
            return Err(GfError::Validation("Branch Version must be non-nil".into()));
        }
        let command = publication::begin(
            self,
            request.operation_uuid,
            request.expected_generation_uuid,
            request,
            cancellation,
        )?;
        if let Some(receipt) = command.replay(self)? {
            return Ok(receipt);
        }
        let (mut graph, mut version) = edit::prepare(self, &command, request.branch_uuid)?;
        let change = CompositionChangeRequest {
            context: WriteContext {
                operation_uuid: OperationId(request.operation_uuid),
                actor_uuid: None,
            },
            expected_project_generation_uuid: graph.generation_for_read()?.generation_uuid(),
            expected_composition_fingerprint: request.expected_composition_fingerprint.clone(),
            candidate: request.candidate.clone(),
            data_disposition: request.data_disposition.clone(),
        };
        let preview = graph.preview_ontology_composition_change(&change, Some(cancellation))?;
        graph.publish_ontology_composition_change(&change, &preview, Some(cancellation))?;
        version.version_uuid = request.version_uuid;
        version.created_at = request.created_at;
        edit::finish(
            self,
            command,
            &graph,
            version,
            request.operation_uuid,
            cancellation,
        )
    }
}
