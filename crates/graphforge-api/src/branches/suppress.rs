//! Assertion suppression changes selected knowledge, never graph topology.
use super::{
    SuppressResearchBranchAssertionRequest, baseline, domains, edit, fields, private_view,
    publication,
};
use crate::{CancellationToken, GfError, GraphForge};
use graphforge_storage::research_versions::{
    ResearchMutation, ResearchOperationReceipt, prepare_branch_content,
    replace_prepared_branch_domains,
};
impl GraphForge {
    /// Suppress an assertion and its owned interpretation rows in one Branch.
    /// Dependency conflicts fail before publication; graph objects are preserved.
    pub fn suppress_research_branch_assertion(
        &mut self,
        request: &SuppressResearchBranchAssertionRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        if request.version_uuid.is_nil() || request.assertion_uuid.is_nil() {
            return Err(GfError::Validation(
                "Branch suppression requires non-nil identities".into(),
            ));
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
        let (graph, mut version) = edit::prepare(self, &command, request.branch_uuid)?;
        graph.assertion(request.assertion_uuid, Some(cancellation.clone()))?;
        let generation = graph.generation_for_read()?;
        let ids = fields::read(&graph, cancellation)?
            .into_keys()
            .filter(|(kind, id, _)| kind != "assertion" || *id != request.assertion_uuid)
            .map(|(kind, id, _)| (kind, id))
            .collect();
        let replacements = domains::retained(&generation, ids)?;
        // An unsupported dependent family must not survive with dangling state.
        super::domain_bounds::refuse_unsupported_selected(
            &generation,
            &std::collections::BTreeSet::from([("assertion".into(), request.assertion_uuid)]),
            &replacements,
        )?;
        version.version_uuid = request.version_uuid;
        version.created_at = request.created_at;
        let mut prepared =
            prepare_branch_content(&command.root, &generation, version, cancellation.flag())?;
        let keep = prepared
            .version
            .content
            .participants
            .iter()
            .map(|p| p.key.clone())
            .collect();
        replace_prepared_branch_domains(
            &command.root,
            &mut prepared,
            &keep,
            &replacements,
            cancellation.flag(),
        )?;
        let suppressed = private_view::open(self, &prepared)?;
        baseline::update(
            self,
            &suppressed,
            &mut prepared,
            request.operation_uuid,
            cancellation,
        )?;
        let mutation = ResearchMutation::PublishBranch {
            intent_sha256: command.intent,
            origin_capture: None,
            creation: None,
            version: Box::new(prepared.version.clone()),
        };
        let outcome = command.publish(self, mutation, cancellation);
        drop(prepared);
        outcome
    }
}
