//! Structural and append-only obligations for the native Proposal history owner.
use super::{
    GfError, ResearchProposalDecision, ResearchProposalDestination, ResearchRegistry,
    ResearchRootKind, Uuid, invalid,
};
use std::collections::BTreeSet;

pub(super) fn validate(registry: &ResearchRegistry) -> Result<(), GfError> {
    let history = &registry.proposals;
    if history.proposals.len() > super::MAX_RECEIPTS
        || history.reviews.len() > super::MAX_RECEIPTS
        || history.accepted.len() > 16_384
    {
        return Err(super::error(
            super::ProjectErrorCode::ResourceLimit,
            "Proposal history capacity exceeded; deduplication evidence does not expire",
        ));
    }
    validate_proposals(registry)?;
    validate_reviews(registry)?;
    validate_mappings(registry)
}

fn validate_proposals(registry: &ResearchRegistry) -> Result<(), GfError> {
    let history = &registry.proposals;
    for (id, proposal) in &history.proposals {
        let branch = registry
            .branches
            .get(&proposal.source_branch_uuid)
            .ok_or_else(|| invalid("Proposal source Branch is unavailable"))?;
        let destination = match branch.parent_branch_uuid {
            Some(branch_uuid) => ResearchProposalDestination::Branch { branch_uuid },
            None => ResearchProposalDestination::Project {
                project_uuid: branch.project_uuid,
            },
        };
        if id.is_nil()
            || *id != proposal.proposal_uuid
            || proposal.actor_uuid.is_nil()
            || proposal.destination != destination
            || !registry
                .identities
                .contains_key(&proposal.source_version_uuid)
            || !registry.receipts.contains_key(&proposal.operation_uuid)
            || !registry
                .identities
                .contains_key(&proposal.payload_version_uuid)
            || proposal.motivation.len() > 4096
            || proposal.policy.len() > 4096
            || proposal.items.is_empty()
            || proposal.items.len() > 256
        {
            return Err(invalid(
                "invalid frozen Proposal identity, destination or bounds",
            ));
        }
        if let Some(payload) = registry.versions.get(&proposal.payload_version_uuid)
            && (payload.content.source_version != Some(proposal.source_version_uuid)
                || registry.branches.contains_key(&payload.context_uuid)
                || registry
                    .heads
                    .values()
                    .any(|head| *head == payload.version_uuid))
        {
            return Err(invalid(
                "frozen Proposal payload must be a distinct headless projection",
            ));
        }
        if history.released.contains_key(id) {
            if registry.roots.contains_key(id) {
                return Err(invalid("released Proposal still retains its frozen root"));
            }
        } else if registry.roots.get(id).is_none_or(|root| {
            root.kind != ResearchRootKind::FrozenProposal
                || root.versions != BTreeSet::from([proposal.payload_version_uuid])
        }) {
            return Err(invalid("pending Proposal selected payload is not rooted"));
        }
        let items: BTreeSet<_> = proposal.items.iter().map(|item| item.item_uuid).collect();
        let units: BTreeSet<_> = proposal.items.iter().map(|item| &item.unit).collect();
        if items.len() != proposal.items.len()
            || units.len() != proposal.items.len()
            || proposal
                .items
                .windows(2)
                .any(|pair| pair[0].item_uuid >= pair[1].item_uuid)
        {
            return Err(invalid(
                "Proposal items must have unique units and sorted identities",
            ));
        }
        for item in &proposal.items {
            if item.item_uuid.is_nil()
                || item.contribution_uuid.is_nil()
                || item.unit.object_uuid.is_nil()
                || item.unit.object_kind.is_empty()
                || item.unit.object_kind.len() > 64
                || item.unit.field.is_empty()
                || item.unit.field.len() > 4096
                || !item.required_items.is_subset(&items)
                || item.required_items.contains(&item.item_uuid)
            {
                return Err(invalid("Proposal selected unit or dependency is invalid"));
            }
        }
    }
    Ok(())
}

fn validate_reviews(registry: &ResearchRegistry) -> Result<(), GfError> {
    let history = &registry.proposals;
    let sequences: BTreeSet<_> = history
        .reviews
        .values()
        .map(|review| review.sequence)
        .collect();
    if sequences != (1..=history.reviews.len() as u64).collect() {
        return Err(invalid(
            "Proposal review publication sequence is not contiguous",
        ));
    }
    for (id, review) in &history.reviews {
        let proposal = history
            .proposals
            .get(&review.proposal_uuid)
            .ok_or_else(|| invalid("review Proposal is unavailable"))?;
        if *id != review.operation_uuid
            || id.is_nil()
            || review.actor_uuid.is_nil()
            || review.preview_generation_uuid.is_nil()
            || !registry.receipts.contains_key(id)
            || review.explanation.len() > 4096
            || review.policy.len() > 4096
            || review.decisions.keys().copied().collect::<BTreeSet<_>>()
                != proposal.items.iter().map(|item| item.item_uuid).collect()
            || review
                .destination_version_uuid
                .is_some_and(|id| !registry.identities.contains_key(&id))
        {
            return Err(invalid("invalid Proposal review identity or item coverage"));
        }
        for mapping in &review.mappings {
            if history
                .accepted
                .get(mapping)
                .is_none_or(|row| row.operation_uuid != *id)
            {
                return Err(invalid(
                    "review accepted mapping is unavailable or owned by another review",
                ));
            }
        }
    }
    Ok(())
}

fn validate_mappings(registry: &ResearchRegistry) -> Result<(), GfError> {
    let history = &registry.proposals;
    for (id, mapping) in &history.accepted {
        let review = history
            .reviews
            .get(&mapping.operation_uuid)
            .ok_or_else(|| invalid("accepted mapping review is unavailable"))?;
        let proposal = &history.proposals[&review.proposal_uuid];
        let item = proposal
            .items
            .iter()
            .find(|item| item.item_uuid == mapping.item_uuid)
            .ok_or_else(|| invalid("accepted mapping item is unavailable"))?;
        if *id != mapping.mapping_uuid
            || *id != mapping.identity()?
            || !review.mappings.contains(id)
            || review.decisions.get(&mapping.item_uuid) != Some(&ResearchProposalDecision::Accept)
            || review.destination_version_uuid != Some(mapping.destination_version_uuid)
            || mapping.destination != proposal.destination
            || mapping.source_branch_uuid != proposal.source_branch_uuid
            || mapping.source_version_uuid != proposal.source_version_uuid
            || mapping.unit != item.unit
            || mapping.contribution_uuid != item.contribution_uuid
            || mapping.value_sha256 != item.value_sha256
            || !registry.versions.contains_key(&mapping.proof_version_uuid)
            || !registry.roots.values().any(|root| {
                root.kind == ResearchRootKind::AcceptedProvenance
                    && root.versions.contains(&mapping.proof_version_uuid)
            })
        {
            return Err(invalid(
                "accepted mapping differs from its committed review or retained proof",
            ));
        }
    }
    for (proposal, operation) in &history.released {
        if !history.proposals.contains_key(proposal) || !registry.receipts.contains_key(operation) {
            return Err(invalid(
                "Proposal release history has no submission or receipt",
            ));
        }
    }
    Ok(())
}

pub(super) fn insert_payload(
    root: &std::path::Path,
    registry: &mut ResearchRegistry,
    version: &super::ResearchVersionRecord,
) -> Result<Uuid, GfError> {
    if version.content.source_version.is_none()
        || registry.branches.contains_key(&version.context_uuid)
        || registry.heads.contains_key(&version.context_uuid)
        || registry.identities.contains_key(&version.version_uuid)
    {
        return Err(invalid(
            "Proposal payload requires a fresh distinct projection identity",
        ));
    }
    super::retained_content::inspect(root, version, None)?;
    let id = super::insert_content(registry, version.clone())?;
    registry.materialized.insert(id);
    Ok(id)
}
