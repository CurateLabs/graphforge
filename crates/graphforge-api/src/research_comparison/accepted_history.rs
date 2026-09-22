//! Restore-independent accepted proof, authenticated by the native publication ledger.
use super::{
    ResearchComparisonEndpoint, ResearchComparisonRequest,
    accepted::Accepted,
    state::{self, State},
};
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::fields::{Key, Objects},
};
use graphforge_storage::{
    ResolvedProjectGeneration,
    research_versions::{ResearchAcceptedMapping, ResearchProposalDestination, ResearchRegistry},
};
use std::collections::BTreeMap;

pub(super) fn verify(
    owner: &GraphForge,
    current: &ResolvedProjectGeneration,
    registry: &ResearchRegistry,
    request: &ResearchComparisonRequest,
    left: &State,
    right: &State,
    cancel: &CancellationToken,
) -> Result<BTreeMap<Key, Accepted>, GfError> {
    let selected = select(registry, request, left, right, cancel)?;
    let mut groups = BTreeMap::new();
    for (key, (_, mapping)) in selected {
        let proof_id = if registry.versions.contains_key(&mapping.proof_version_uuid) {
            mapping.proof_version_uuid
        } else {
            registry
                .interchange
                .values()
                .find_map(|archive| {
                    (archive.accepted.get(&mapping.mapping_uuid) == Some(mapping))
                        .then(|| {
                            archive
                                .proof_exports
                                .get(&mapping.proof_version_uuid)
                                .copied()
                        })
                        .flatten()
                })
                .ok_or_else(|| super::invalid("historical accepted proof is unavailable"))?
        };
        groups
            .entry(proof_id)
            .or_insert_with(Vec::new)
            .push((key, mapping));
    }
    let mut result = BTreeMap::new();
    let mut count = 0usize;
    let mut bytes = 0usize;
    for (proof_id, mappings) in groups {
        cancel.checkpoint()?;
        let objects: Objects = mappings.iter().map(|(k, _)| (k.0.clone(), k.1)).collect();
        let proof = state::load(
            owner,
            current,
            registry,
            &ResearchComparisonEndpoint::Version {
                version_uuid: proof_id,
            },
            Some(&objects),
            cancel,
        )?;
        count = count
            .saturating_add(proof.fields.len())
            .saturating_add(proof.baseline.len());
        for key in proof.fields.keys().chain(proof.baseline.keys()) {
            bytes = bytes
                .saturating_add(1536)
                .saturating_add(key.0.len().saturating_mul(3))
                .saturating_add(key.2.len().saturating_mul(3));
        }
        if count > request.max_fields || bytes > request.max_bytes {
            return Err(super::limit());
        }
        for (key, mapping) in mappings {
            let expected_source = if proof_id == mapping.proof_version_uuid {
                mapping.source_version_uuid
            } else {
                mapping.proof_version_uuid
            };
            if registry.versions[&proof_id].content.source_version != Some(expected_source)
                || proof
                    .baseline
                    .get(&key)
                    .is_none_or(|b| b.contribution != mapping.contribution_uuid)
                || left
                    .baseline
                    .get(&key)
                    .is_none_or(|b| b.contribution != mapping.contribution_uuid)
                || proof.fields.get(&key).copied() != mapping.value_sha256
            {
                return Err(super::invalid(
                    "accepted selected proof does not match its immutable mapping",
                ));
            }
            result.insert(
                key,
                Accepted {
                    value: mapping.value_sha256,
                    source: mapping.source_version_uuid,
                    destination: mapping.destination_version_uuid,
                    contribution: mapping.contribution_uuid,
                },
            );
        }
    }
    Ok(result)
}

fn select<'a>(
    registry: &'a ResearchRegistry,
    request: &ResearchComparisonRequest,
    left: &State,
    right: &State,
    cancel: &CancellationToken,
) -> Result<BTreeMap<Key, (u64, &'a ResearchAcceptedMapping)>, GfError> {
    let mut selected: BTreeMap<Key, (u64, &ResearchAcceptedMapping)> = BTreeMap::new();
    let live = matches!(request.left, ResearchComparisonEndpoint::Branch { .. })
        && !matches!(request.right, ResearchComparisonEndpoint::Version { .. });
    for mapping in registry.proposals.accepted.values().filter(|_| live) {
        cancel.checkpoint()?;
        let authority = match mapping.destination {
            ResearchProposalDestination::Project { project_uuid } => (0, project_uuid),
            ResearchProposalDestination::Branch { branch_uuid } => (1, branch_uuid),
        };
        let key = (
            mapping.unit.object_kind.clone(),
            mapping.unit.object_uuid,
            mapping.unit.field.clone(),
        );
        if authority != right.authority_context
            || left
                .baseline
                .get(&key)
                .is_none_or(|b| b.contribution != mapping.contribution_uuid)
        {
            continue;
        }
        let review = registry
            .proposals
            .reviews
            .get(&mapping.operation_uuid)
            .ok_or_else(|| super::invalid("accepted mapping lacks its native review"))?;
        if selected
            .get(&key)
            .is_none_or(|(sequence, _)| *sequence < review.sequence)
        {
            selected.insert(key, (review.sequence, mapping));
        }
    }
    // An explicitly requested historical acceptance takes precedence over the latest one.
    for explicit in &request.accepted {
        let key = (
            explicit.unit.object_kind.clone(),
            explicit.unit.object_uuid,
            explicit.unit.field.clone(),
        );
        let historical = registry
            .interchange
            .values()
            .flat_map(|archive| archive.accepted.values())
            .filter(|_| matches!(request.right, ResearchComparisonEndpoint::Version { .. }));
        if let Some(mapping) = registry
            .proposals
            .accepted
            .values()
            .chain(historical)
            .find(|m| {
                let authority = match m.destination {
                    ResearchProposalDestination::Project { project_uuid } => (0, project_uuid),
                    ResearchProposalDestination::Branch { branch_uuid } => (1, branch_uuid),
                };
                authority == right.authority_context
                    && m.source_version_uuid == explicit.source_version_uuid
                    && m.destination_version_uuid == explicit.destination_version_uuid
                    && m.contribution_uuid == explicit.contribution_uuid
                    && (
                        m.unit.object_kind.as_str(),
                        m.unit.object_uuid,
                        m.unit.field.as_str(),
                    ) == (key.0.as_str(), key.1, key.2.as_str())
            })
        {
            selected.insert(key, (0, mapping));
        }
    }
    Ok(selected)
}
