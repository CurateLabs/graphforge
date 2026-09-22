//! Exact per-unit source/destination proof, with no whole-Version acceptance shortcut.
use super::{
    ResearchComparisonEndpoint, ResearchComparisonRequest,
    state::{self, State},
};
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::fields::{Key, Objects},
};
use graphforge_storage::{ResolvedProjectGeneration, research_versions::ResearchRegistry};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;
#[derive(Clone)]
pub(super) struct Accepted {
    pub value: Option<[u8; 32]>,
    pub source: Uuid,
    pub destination: Uuid,
    pub contribution: Uuid,
}
pub(super) fn verify(
    owner: &GraphForge,
    current: &ResolvedProjectGeneration,
    registry: &ResearchRegistry,
    request: &ResearchComparisonRequest,
    left: &State,
    right: &State,
    cancel: &CancellationToken,
) -> Result<BTreeMap<Key, Accepted>, GfError> {
    let mut accepted =
        super::accepted_history::verify(owner, current, registry, request, left, right, cancel)?;
    let mut explicit_keys = BTreeSet::new();
    for mapping in &request.accepted {
        cancel.checkpoint()?;
        let key = (
            mapping.unit.object_kind.clone(),
            mapping.unit.object_uuid,
            mapping.unit.field.clone(),
        );
        if !explicit_keys.insert(key.clone()) {
            return Err(super::invalid(
                "supply one current accepted mapping per field and destination",
            ));
        }
        if accepted.get(&key).is_some_and(|a| {
            a.source == mapping.source_version_uuid
                && a.destination == mapping.destination_version_uuid
                && a.contribution == mapping.contribution_uuid
        }) {
            continue;
        }
        let objects = Objects::from([(key.0.clone(), key.1)]);
        let source = state::load(
            owner,
            current,
            registry,
            &ResearchComparisonEndpoint::Version {
                version_uuid: mapping.source_version_uuid,
            },
            Some(&objects),
            cancel,
        )?;
        let destination = state::load(
            owner,
            current,
            registry,
            &ResearchComparisonEndpoint::Version {
                version_uuid: mapping.destination_version_uuid,
            },
            Some(&objects),
            cancel,
        )?;
        let origin = source.baseline.get(&key).ok_or_else(|| {
            super::invalid("accepted source does not contain the stated native contribution")
        })?;
        if origin.contribution != mapping.contribution_uuid
            || mapping.contribution_uuid.is_nil()
            || left
                .baseline
                .get(&key)
                .is_none_or(|row| row.contribution != mapping.contribution_uuid)
            || destination.authority_context != right.authority_context
            || source.fields.get(&key) != destination.fields.get(&key)
        {
            return Err(super::invalid(
                "accepted mapping does not match its exact contribution, value or destination context",
            ));
        }
        accepted.insert(
            key.clone(),
            Accepted {
                value: source.fields.get(&key).copied(),
                source: mapping.source_version_uuid,
                destination: mapping.destination_version_uuid,
                contribution: mapping.contribution_uuid,
            },
        );
    }
    Ok(accepted)
}
