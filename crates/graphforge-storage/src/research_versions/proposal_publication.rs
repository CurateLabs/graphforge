//! Single-registry transition paired with reviewed content in the same CURRENT.
use super::{
    GfError, ResearchMutation, ResearchProposalDecision, ResearchProposalDestination,
    ResearchRegistry, ResearchRetentionRoot, ResearchRootKind, Uuid, invalid,
};
use std::{collections::BTreeSet, path::Path};

pub(super) fn apply(
    root: &Path,
    registry: &mut ResearchRegistry,
    operation: Uuid,
    mutation: &ResearchMutation,
) -> Result<Option<Uuid>, GfError> {
    match mutation {
        ResearchMutation::SubmitProposal {
            proposal, payload, ..
        } => {
            if proposal.operation_uuid != operation
                || proposal.payload_version_uuid != payload.version_uuid
                || registry
                    .proposals
                    .proposals
                    .contains_key(&proposal.proposal_uuid)
                || registry.roots.contains_key(&proposal.proposal_uuid)
                || registry
                    .versions
                    .get(&proposal.source_version_uuid)
                    .is_none_or(|version| version.context_uuid != proposal.source_branch_uuid)
            {
                return Err(invalid(
                    "Proposal submission identity or exact source conflicts",
                ));
            }
            super::proposal_validation::insert_payload(root, registry, payload)?;
            registry.roots.insert(
                proposal.proposal_uuid,
                ResearchRetentionRoot {
                    root_uuid: proposal.proposal_uuid,
                    kind: ResearchRootKind::FrozenProposal,
                    versions: BTreeSet::from([payload.version_uuid]),
                },
            );
            registry
                .proposals
                .proposals
                .insert(proposal.proposal_uuid, *proposal.clone());
            Ok(Some(payload.version_uuid))
        }
        ResearchMutation::ReviewProposal {
            review,
            destination,
            proof,
            mappings,
            ..
        } => review_proposal(
            root,
            registry,
            operation,
            review,
            destination.as_deref(),
            proof.as_deref(),
            mappings,
        ),
        ResearchMutation::ReleaseProposal { proposal_uuid, .. } => {
            if !registry.proposals.proposals.contains_key(proposal_uuid)
                || registry.proposals.released.contains_key(proposal_uuid)
            {
                return Err(invalid(
                    "Proposal payload is unavailable or already released",
                ));
            }
            registry.roots.remove(proposal_uuid);
            registry
                .proposals
                .released
                .insert(*proposal_uuid, operation);
            Ok(None)
        }
        _ => Err(invalid("unsupported Proposal publication")),
    }
}

fn review_proposal(
    root: &Path,
    registry: &mut ResearchRegistry,
    operation: Uuid,
    review: &super::ResearchProposalReview,
    destination: Option<&super::ResearchVersionRecord>,
    proof: Option<&super::ResearchVersionRecord>,
    mappings: &[super::ResearchAcceptedMapping],
) -> Result<Option<Uuid>, GfError> {
    let proposal = registry
        .proposals
        .proposals
        .get(&review.proposal_uuid)
        .cloned()
        .ok_or_else(|| invalid("review Proposal is unavailable"))?;
    if review.operation_uuid != operation
        || review.sequence != registry.proposals.reviews.len() as u64 + 1
        || registry
            .proposals
            .released
            .contains_key(&review.proposal_uuid)
        || registry.proposals.reviews.contains_key(&operation)
        || review.destination_version_uuid != destination.as_ref().map(|v| v.version_uuid)
        || mappings.is_empty() != destination.is_none()
        || mappings.is_empty() != proof.is_none()
        || review.mappings != mappings.iter().map(|m| m.mapping_uuid).collect()
        || review.mappings.len() != mappings.len()
    {
        return Err(invalid(
            "review publication content, proof or mapping identities conflict",
        ));
    }
    // The domain owner proves typed values against prepared content. Storage
    // independently enforces coverage and destination-scoped non-reapplication.
    for item in &proposal.items {
        if review.decisions.get(&item.item_uuid) != Some(&ResearchProposalDecision::Accept) {
            continue;
        }
        for dependency in &item.required_items {
            if review.decisions.get(dependency) != Some(&ResearchProposalDecision::Accept)
                && !already_accepted(registry, &proposal, *dependency)
            {
                return Err(invalid(
                    "accepted Proposal item has an unaccepted required dependency",
                ));
            }
        }
        if !already_accepted(registry, &proposal, item.item_uuid)
            && !mappings.iter().any(|m| m.item_uuid == item.item_uuid)
        {
            return Err(invalid(
                "accepted Proposal item has no exact destination mapping",
            ));
        }
    }
    if let Some(proof) = proof {
        super::proposal_validation::insert_payload(root, registry, proof)?;
        if registry.roots.contains_key(&proof.version_uuid)
            || mappings
                .iter()
                .any(|m| m.proof_version_uuid != proof.version_uuid)
        {
            return Err(invalid("accepted proof identity conflicts"));
        }
        registry.roots.insert(
            proof.version_uuid,
            ResearchRetentionRoot {
                root_uuid: proof.version_uuid,
                kind: ResearchRootKind::AcceptedProvenance,
                versions: BTreeSet::from([proof.version_uuid]),
            },
        );
    }
    if let Some(destination) = destination {
        publish_destination(root, registry, &proposal.destination, destination)?;
    }
    for mapping in mappings {
        if registry
            .proposals
            .accepted
            .contains_key(&mapping.mapping_uuid)
        {
            return Err(invalid(
                "review cannot apply an already accepted contribution revision",
            ));
        }
        registry
            .proposals
            .accepted
            .insert(mapping.mapping_uuid, mapping.clone());
    }
    registry.proposals.reviews.insert(operation, review.clone());
    Ok(review.destination_version_uuid)
}

fn publish_destination(
    root: &Path,
    registry: &mut ResearchRegistry,
    authority: &ResearchProposalDestination,
    destination: &super::ResearchVersionRecord,
) -> Result<(), GfError> {
    match authority {
        ResearchProposalDestination::Branch { branch_uuid } => {
            if destination.context_uuid != *branch_uuid {
                return Err(invalid(
                    "accepted destination is not the immediate parent Branch",
                ));
            }
            super::branches::publish(root, registry, None, destination)?;
        }
        ResearchProposalDestination::Project { project_uuid } => {
            if destination.content.source_version.is_some()
                || destination.context_uuid != *project_uuid
                || registry.identities.contains_key(&destination.version_uuid)
            {
                return Err(invalid(
                    "accepted Project destination is not a fresh complete Version",
                ));
            }
            super::retained_content::inspect(root, destination, None)?;
            let id = super::insert_version(registry, destination.clone())?;
            registry.materialized.insert(id);
        }
    }
    Ok(())
}

fn already_accepted(
    registry: &ResearchRegistry,
    proposal: &super::ResearchProposalRecord,
    item_uuid: Uuid,
) -> bool {
    let Some(item) = proposal
        .items
        .iter()
        .find(|item| item.item_uuid == item_uuid)
    else {
        return false;
    };
    registry.proposals.accepted.values().any(|mapping| {
        mapping.destination == proposal.destination
            && mapping.unit == item.unit
            && mapping.contribution_uuid == item.contribution_uuid
            && mapping.value_sha256 == item.value_sha256
    })
}

pub(super) fn decisions(
    mutation: &ResearchMutation,
    publication: &mut crate::ProjectGenerationRequest,
) -> Result<(), GfError> {
    let ResearchMutation::ReviewProposal {
        decisions: Some(decisions),
        ..
    } = mutation
    else {
        return Ok(());
    };
    if decisions.record_version != 1
        || decisions.row_count == 0
        || decisions.bytes.len() > 8 * 1024 * 1024
        || !decisions.bytes.starts_with(b"PAR1")
    {
        return Err(invalid(
            "review canonical decision participant has invalid bounds or encoding",
        ));
    }
    publication.participants.retain(|p| {
        !(p.capability_id == super::RESEARCH_CAPABILITY
            && p.record_family_id == "canonical_decisions")
    });
    publication.participants.push(crate::ProjectParticipant {
        capability_id: super::RESEARCH_CAPABILITY.into(),
        capability_version: super::RESEARCH_VERSION,
        record_family_id: "canonical_decisions".into(),
        record_version: decisions.record_version,
        encoding: crate::ProjectParticipantEncoding::Parquet,
        schema_fingerprint: decisions.schema_sha256,
        row_count: decisions.row_count,
        bytes: decisions.bytes.clone(),
    });
    Ok(())
}

pub(super) fn validate_preview_generation(
    request: &super::ResearchOperation,
) -> Result<(), GfError> {
    if let ResearchMutation::ReviewProposal { review, .. } = &request.mutation
        && review.preview_generation_uuid != request.expected_generation_uuid
    {
        return Err(super::error(
            super::ProjectErrorCode::WriteConflict,
            "Proposal review does not identify the previewed CURRENT",
        ));
    }
    Ok(())
}
