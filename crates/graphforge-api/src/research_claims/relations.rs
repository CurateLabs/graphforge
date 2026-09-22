//! Explicit coexisting claim relations retain native assertion/supersession ownership.
use super::{RelateResearchClaimsRequest, ledger, publication};
use crate::{
    CancellationToken, ExecutionResult, GfError, GraphForge, OperationId,
    knowledge::{self, knowledge_error, ledger as k},
};
use graphforge_knowledge::research::ResearchClaimLedger;
impl GraphForge {
    /// Append a relation without changing canonical choices, confidence or raw graph results.
    pub fn relate_research_claims(
        &mut self,
        request: &RelateResearchClaimsRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionResult, GfError> {
        self.graph_visibility.health.check()?;
        cancellation.checkpoint()?;
        if self.read_only {
            return Err(GfError::Project {
                code: graphforge_core::ProjectErrorCode::ReadOnlyView,
                message: "read-only research cannot append claim relations".into(),
            });
        }
        if request.operation_uuid.is_nil() || request.expected_generation_uuid.is_nil() {
            return Err(GfError::Validation(
                "claim relation requires operation and CURRENT identities".into(),
            ));
        }
        let parent = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        let staged = ResearchClaimLedger::new(vec![], vec![request.relation.clone()])
            .map_err(knowledge_error)?;
        let attempt = publication::Attempt::begin(
            &parent,
            request.operation_uuid,
            b"graphforge-relate-research-claims/1",
            request,
        )?;
        if attempt.replay {
            let root = parent.container_root().to_path_buf();
            self.refresh_current_authority(
                &root,
                None,
                None,
                request.expected_generation_uuid,
                true,
            )?;
            return Ok(knowledge::assertion_result(
                staged.relation_batch().map_err(knowledge_error)?,
            ));
        }
        parent.require_capability("epistemic", 1)?;
        let existing = ledger::read_claims(&parent)?;
        let merged = existing.merge(&staged).map_err(knowledge_error)?;
        merged
            .validate_references(
                &k::read_ledger(&parent)?,
                &k::read_supersession_ledger(&parent)?,
            )
            .map_err(knowledge_error)?;
        if parent.generation_uuid() != request.expected_generation_uuid
            || self.generation_for_read()?.generation_uuid() != parent.generation_uuid()
        {
            return Err(publication::conflict());
        }
        if !crate::provenance::read_ledger(&parent)?
            .events
            .iter()
            .any(|event| event.provenance_uuid == request.relation.provenance_uuid)
        {
            return Err(GfError::Validation(
                "claim relation producing provenance is unavailable".into(),
            ));
        }
        let mut replacements = ledger::encode_claims(&merged)?;
        let generation = k::knowledge_generation_uuid(
            b"research_relation",
            OperationId(request.operation_uuid),
            &replacements,
        );
        replacements.push(attempt.receipt(generation)?);
        publication::publish(
            self,
            &parent,
            request.operation_uuid,
            generation,
            replacements,
            cancellation,
        )?;
        Ok(knowledge::assertion_result(
            staged.relation_batch().map_err(knowledge_error)?,
        ))
    }
}
