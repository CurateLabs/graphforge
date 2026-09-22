//! Validate a pinned review and publish content, proof and history together.
use super::{ReviewResearchProposalRequest, invalid, preview};
use crate::{CancellationToken, GfError, GraphForge, branches::publication};
use graphforge_storage::research_versions::{
    ResearchAcceptedMapping, ResearchMutation, ResearchOperationReceipt, ResearchProposalDecision,
    ResearchProposalReview,
};
use std::collections::BTreeSet;

impl GraphForge {
    /// Atomically integrate exact reviewed contributions into their immediate parent.
    pub fn review_research_proposal(
        &mut self,
        request: &ReviewResearchProposalRequest,
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
        validate_metadata(request)?;
        let preview = preview::load(self, request.proposal_uuid, cancellation)?;
        validate(&preview, request)?;
        let items = newly_accepted(&preview, request);
        let proof = if items.is_empty() {
            None
        } else {
            Some(super::accepted_subset::prepare(
                self,
                &command,
                &preview,
                request,
                &items,
                cancellation,
            )?)
        };
        let destination = proof
            .as_ref()
            .map(|proof| {
                super::destination::prepare(
                    self,
                    &command,
                    &preview,
                    request,
                    proof,
                    &items,
                    cancellation,
                )
            })
            .transpose()?;
        let mappings = accepted_mappings(
            &preview,
            request,
            &items,
            proof.as_ref(),
            destination.as_ref(),
        )?;
        let review = ResearchProposalReview {
            sequence: command.registry.proposals.reviews.len() as u64 + 1,
            operation_uuid: request.operation_uuid,
            proposal_uuid: request.proposal_uuid,
            preview_generation_uuid: preview.generation,
            preview_sha256: preview.digest,
            resolved_conflicts: request.resolve_conflicts.clone(),
            acknowledged_evidence: request.acknowledge_evidence.clone(),
            destination_version_uuid: destination
                .as_ref()
                .map(|prepared| prepared.version.version_uuid),
            actor_uuid: request.actor_uuid,
            created_at: request.created_at,
            explanation: request.explanation.clone(),
            policy: request.policy.clone(),
            decisions: request.decisions.clone(),
            mappings: mappings.iter().map(|row| row.mapping_uuid).collect(),
        };
        let decisions = super::canonical::prepare(
            self,
            &command,
            &preview,
            request,
            destination.as_ref(),
            &items,
            cancellation,
        )?;
        let intent_sha256 = command.intent;
        let outcome = command.publish(
            self,
            ResearchMutation::ReviewProposal {
                intent_sha256,
                review: Box::new(review),
                destination: destination
                    .as_ref()
                    .map(|prepared| Box::new(prepared.version.clone())),
                proof: proof
                    .as_ref()
                    .map(|prepared| Box::new(prepared.version.clone())),
                mappings,
                decisions,
            },
            cancellation,
        );
        drop(destination);
        drop(proof);
        outcome
    }
}

fn validate(
    preview: &preview::Preview,
    request: &ReviewResearchProposalRequest,
) -> Result<(), GfError> {
    if preview.generation != request.expected_generation_uuid
        || preview.digest != request.preview_sha256
    {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::WriteConflict,
            message: "Proposal destination or preview changed; request a new preview".into(),
        });
    }
    let ids: BTreeSet<_> = preview.rows.iter().map(|row| row.item_uuid).collect();
    if request.decisions.keys().copied().collect::<BTreeSet<_>>() != ids
        || !request.resolve_conflicts.is_subset(&ids)
    {
        return Err(invalid("review must decide each exact submitted item once"));
    }
    if request.resolve_conflicts.iter().any(|id| {
        preview
            .rows
            .iter()
            .find(|row| row.item_uuid == *id)
            .is_none_or(|row| {
                !row.conflict
                    || row.already_accepted
                    || request.decisions[id] != ResearchProposalDecision::Accept
            })
    }) {
        return Err(invalid(
            "conflict resolutions must name newly accepted conflicting items",
        ));
    }
    let mut needed_evidence = BTreeSet::new();
    for row in &preview.rows {
        let accepted = request.decisions[&row.item_uuid] == ResearchProposalDecision::Accept;
        if !accepted || row.already_accepted {
            continue;
        }
        if !row.unavailable.is_empty() {
            return Err(invalid(
                "accepted item has unavailable dependencies; select or resolve them before review",
            ));
        }
        if row.conflict && !request.resolve_conflicts.contains(&row.item_uuid) {
            return Err(invalid(
                "conflicting accepted item requires explicit use-proposed resolution",
            ));
        }
        for id in &row.required_items {
            let dependency = preview
                .rows
                .iter()
                .find(|candidate| candidate.item_uuid == *id)
                .ok_or_else(|| invalid("review dependency is not a submitted item"))?;
            if request.decisions[id] != ResearchProposalDecision::Accept
                && !dependency.already_accepted
            {
                return Err(invalid(
                    "accepted item depends on a rejected or deferred item",
                ));
            }
            if dependency.already_accepted {
                let item = preview
                    .proposal
                    .items
                    .iter()
                    .find(|item| item.item_uuid == *id)
                    .expect("review coverage");
                if dependency.destination_value != item.value_sha256 {
                    return Err(invalid(
                        "previously accepted dependency is no longer available in the destination",
                    ));
                }
            }
        }
        needed_evidence.extend(row.evidence_gaps.iter().map(|(id, _)| *id));
    }
    if needed_evidence != request.acknowledge_evidence {
        return Err(invalid(
            "review must explicitly acknowledge exactly its unavailable evidence context",
        ));
    }
    Ok(())
}

fn accepted_mappings(
    preview: &preview::Preview,
    request: &ReviewResearchProposalRequest,
    items: &[graphforge_storage::research_versions::ResearchProposalItem],
    proof: Option<&graphforge_storage::research_versions::PreparedResearchContent>,
    destination: Option<&graphforge_storage::research_versions::PreparedResearchContent>,
) -> Result<Vec<ResearchAcceptedMapping>, GfError> {
    if let (Some(proof), Some(destination)) = (proof, destination) {
        items
            .iter()
            .map(|item| {
                let mut mapping = ResearchAcceptedMapping {
                    mapping_uuid: uuid::Uuid::nil(),
                    destination: preview.proposal.destination.clone(),
                    unit: item.unit.clone(),
                    contribution_uuid: item.contribution_uuid,
                    value_sha256: item.value_sha256,
                    source_branch_uuid: preview.proposal.source_branch_uuid,
                    source_version_uuid: preview.proposal.source_version_uuid,
                    destination_version_uuid: destination.version.version_uuid,
                    proof_version_uuid: proof.version.version_uuid,
                    operation_uuid: request.operation_uuid,
                    item_uuid: item.item_uuid,
                };
                mapping.mapping_uuid = mapping.identity()?;
                Ok(mapping)
            })
            .collect::<Result<Vec<_>, GfError>>()
    } else {
        Ok(vec![])
    }
}

fn newly_accepted(
    preview: &preview::Preview,
    request: &ReviewResearchProposalRequest,
) -> Vec<graphforge_storage::research_versions::ResearchProposalItem> {
    preview
        .proposal
        .items
        .iter()
        .zip(&preview.rows)
        .filter(|(item, row)| {
            request.decisions.get(&item.item_uuid) == Some(&ResearchProposalDecision::Accept)
                && !row.already_accepted
        })
        .map(|(item, _)| item.clone())
        .collect()
}

fn validate_metadata(request: &ReviewResearchProposalRequest) -> Result<(), GfError> {
    if request.actor_uuid.is_nil()
        || request.explanation.len() > 4096
        || request.policy.len() > 4096
        || request.acknowledge_evidence.len() > 256
    {
        return Err(invalid(
            "invalid Proposal reviewer or review metadata bounds",
        ));
    }
    Ok(())
}
