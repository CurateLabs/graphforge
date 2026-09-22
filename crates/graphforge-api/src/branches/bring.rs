//! Explicit selected incorporation; citations alone never invoke this path.
use super::{
    BringResearchBranchRequest, baseline, edit, import_graph, merge_domains, private_view,
    publication, selection,
};
use crate::{CancellationToken, GfError, GraphForge, slices::branch};
use graphforge_storage::research_versions::{
    RegisterResearchVersion, ResearchMutation, ResearchOperationReceipt, prepare_branch_content,
    replace_prepared_branch_domains,
};
use std::collections::BTreeSet;
use uuid::Uuid;
impl GraphForge {
    /// Incorporate an exact retained Slice, rejecting conflicting existing objects.
    pub fn bring_research_branch(
        &mut self,
        request: &BringResearchBranchRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        if request.version_uuid.is_nil() || request.frozen_ipc.len() > 16 * 1024 * 1024 {
            return Err(GfError::Validation(
                "invalid Branch incorporation identity or Slice size".into(),
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
        let selected = branch::authenticate(self, &request.frozen_ipc, cancellation)?;
        let active = selected.active.clone();
        let source_version = selected.version.version_uuid;
        let mut spec = RegisterResearchVersion {
            version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
            source_generation_uuid: selected.version.content.generation_uuid,
            source_version: Some(source_version),
            selection: None,
            required_versions: BTreeSet::new(),
            label: None,
            description: None,
            created_at: request.created_at,
            evidence: selected.evidence.clone(),
        };
        let source_content = selection::prepare(&command.root, selected, &mut spec, cancellation)?;
        let source = private_view::open(self, &source_content)?;
        let (destination, mut version) = edit::prepare(self, &command, request.branch_uuid)?;
        if source.workspace_ontology()? != destination.workspace_ontology()?
            || source.workspace_ontology_composition()?
                != destination.workspace_ontology_composition()?
        {
            return Err(GfError::Validation("Bring requires the exact source ontology context; compose it in the destination Branch explicitly first".into()));
        }
        let domains = merge_domains::merge(&destination, &source)?;
        import_graph::incorporate(&destination, &source, cancellation)?;
        version.version_uuid = request.version_uuid;
        version.created_at = request.created_at;
        let generation = destination.generation_for_read()?;
        let mut prepared =
            prepare_branch_content(&command.root, &generation, version, cancellation.flag())?;
        for evidence in &source_content.version.content.evidence {
            if !prepared.version.content.evidence.contains(evidence) {
                prepared.version.content.evidence.push(evidence.clone());
            }
        }
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
            &domains,
            cancellation.flag(),
        )?;
        baseline::incorporate(
            self,
            &source,
            &mut prepared,
            source_version,
            &active,
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
        drop(source_content);
        outcome
    }
}
