//! Exact native submission with public-intent replay before private preparation.
use super::{SubmitResearchProposalRequest, invalid, key, selection};
use crate::{CancellationToken, GfError, GraphForge, branches::publication};
use graphforge_storage::research_versions::{
    ResearchMutation, ResearchOperationReceipt, ResearchProposalDestination, ResearchProposalItem,
    ResearchProposalRecord, ResearchProposalUnit,
};
use std::collections::BTreeSet;

impl GraphForge {
    /// Freeze explicitly selected Branch fields and evidence for parent review.
    pub fn submit_research_proposal(
        &mut self,
        request: &SubmitResearchProposalRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
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
        if request.proposal_uuid.is_nil()
            || request.actor_uuid.is_nil()
            || request.source_branch_uuid.is_nil()
            || request.source_version_uuid.is_nil()
            || request.fields.is_empty()
            || request.fields.len() > 256
            || request.motivation.len() > 4096
            || request.policy.len() > 4096
            || request
                .fields
                .iter()
                .map(key)
                .collect::<BTreeSet<_>>()
                .len()
                != request.fields.len()
        {
            return Err(invalid(
                "invalid Proposal identity, selection or metadata bounds",
            ));
        }
        let branch = command
            .registry
            .branches
            .get(&request.source_branch_uuid)
            .ok_or_else(|| invalid("Proposal source Branch is unavailable"))?;
        let destination = match branch.parent_branch_uuid {
            Some(branch_uuid) => ResearchProposalDestination::Branch { branch_uuid },
            None => ResearchProposalDestination::Project {
                project_uuid: branch.project_uuid,
            },
        };
        let selected = selection::freeze(self, &command, request, cancellation)?;
        let mut items = Vec::new();
        for field in &request.fields {
            let key = key(field);
            let baseline = selected.baseline.get(&key).ok_or_else(|| {
                invalid("selected Proposal field has no native contribution identity")
            })?;
            items.push(ResearchProposalItem {
                item_uuid: selection::item_identity(request.proposal_uuid, &key)?,
                unit: ResearchProposalUnit {
                    object_kind: field.object_kind.clone(),
                    object_uuid: field.object_uuid,
                    field: field.field.clone(),
                },
                contribution_uuid: baseline.contribution,
                value_sha256: selected.values.get(&key).copied(),
                baseline_sha256: selection::digest(&baseline.baseline)?,
                required_items: BTreeSet::new(),
            });
        }
        items.sort_by_key(|item| item.item_uuid);
        let proposal = ResearchProposalRecord {
            proposal_uuid: request.proposal_uuid,
            source_branch_uuid: request.source_branch_uuid,
            source_version_uuid: request.source_version_uuid,
            destination,
            payload_version_uuid: selected.prepared.version.version_uuid,
            operation_uuid: request.operation_uuid,
            actor_uuid: request.actor_uuid,
            created_at: request.created_at,
            motivation: request.motivation.clone(),
            policy: request.policy.clone(),
            items,
        };
        let intent_sha256 = command.intent;
        let result = command.publish(
            self,
            ResearchMutation::SubmitProposal {
                intent_sha256,
                proposal: Box::new(proposal),
                payload: Box::new(selected.prepared.version.clone()),
            },
            cancellation,
        );
        drop(selected);
        result
    }
}
