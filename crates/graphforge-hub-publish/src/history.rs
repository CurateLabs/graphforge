//! Shared structural admission for explicit destructive publication. No host permissions live here.
use crate::{
    Capability, HISTORY_MUTATION_CAPABILITY, HubPublishError, HubPublishErrorCode,
    PublishCapabilities, error::invalid, wire::validate_ref_list,
};
use graphforge_discovery::{DiscoveryLimits, LineageVersion, ResearchLineage};
use std::collections::{BTreeMap, BTreeSet};
type Versions<'a> = BTreeMap<&'a str, &'a LineageVersion>;

/// Refuse destructive publication before uploading to a server that did not offer it.
/// This is negotiation, not permission: the host still authenticates every commit.
pub fn require_history_capability(
    capabilities: &PublishCapabilities,
    rewrite: bool,
    prune: bool,
) -> Result<(), HubPublishError> {
    if (rewrite || prune)
        && !capabilities
            .requirements
            .iter()
            .chain(&capabilities.capabilities)
            .any(|item| item.capability == HISTORY_MUTATION_CAPABILITY && item.major == 1)
    {
        return Err(HubPublishError::new(
            HubPublishErrorCode::UnsupportedFuture,
            "the Hub does not offer explicit history mutation",
        ));
    }
    Ok(())
}

/// Require destructive semantics on the authenticated session, not only in advertisement.
/// This is checked again at commit when the explicit flags are known.
pub fn require_history_requirement(
    requirements: &[Capability],
    rewrite: bool,
    prune: bool,
) -> Result<(), HubPublishError> {
    crate::check_requirements(requirements)?;
    if (rewrite || prune)
        && !requirements
            .iter()
            .any(|item| item.capability == HISTORY_MUTATION_CAPABILITY && item.major == 1)
    {
        return Err(HubPublishError::new(
            HubPublishErrorCode::UnsupportedFuture,
            "destructive publication requires history-mutation semantics",
        ));
    }
    Ok(())
}

/// Admit a proposed complete repository lineage before any object/ref mutation.
/// The caller authenticates the repository and checks the current revision lease.
/// Returns exactly the removed Version UUIDs; this does not collect their objects.
/// A rewrite changes heads only, never immutable identities or Branch creation.
pub fn validate_history_transition(
    previous: &ResearchLineage,
    next: &ResearchLineage,
    refs: &[String],
    rewrite: bool,
    prune: bool,
    limits: DiscoveryLimits,
) -> Result<Vec<String>, HubPublishError> {
    previous
        .validate(limits)
        .map_err(|_| invalid("stored research lineage is invalid"))?;
    next.validate(limits)
        .map_err(|_| invalid("proposed research lineage is invalid"))?;
    validate_ref_list(refs)?;
    if previous.repository != next.repository
        || previous.project_uuid != next.project_uuid
        || previous.fork != next.fork
    {
        return Err(invalid(
            "publication changes repository or research origin authority",
        ));
    }
    let versions: Versions<'_> = next
        .versions
        .iter()
        .map(|v| (v.version_uuid.as_str(), v))
        .collect();
    validate_branches(previous, next, refs, rewrite, &versions)?;
    let mut removed = Vec::new();
    for old in &previous.versions {
        if let Some(new) = versions.get(old.version_uuid.as_str()) {
            let mut immutable = (**new).clone();
            // Optional transport references are not native identity. Preserve
            // the existing ordinary metadata-only ancestor wire behavior; only
            // explicit prune can retire unreachable repository bytes.
            let mut prior = old.clone();
            if old.package.is_none() || immutable.package.is_none() {
                immutable.package = None;
                prior.package = None;
            }
            if immutable != prior {
                return Err(invalid(
                    "publication changes immutable Version history or payload",
                ));
            }
        } else {
            removed.push(old.version_uuid.clone());
        }
    }
    if !prune && !removed.is_empty() {
        return Err(invalid(
            "removing published Versions requires explicit prune intent",
        ));
    }
    validate_anchors(previous, next, prune, &versions)?;
    let proposals: BTreeMap<_, _> = next
        .proposals
        .iter()
        .map(|p| (&p.proposal_uuid, p))
        .collect();
    for old in &previous.proposals {
        if proposals
            .get(&old.proposal_uuid)
            .is_none_or(|new| **new != *old)
        {
            return Err(invalid("publication changes a published Proposal"));
        }
    }
    let mut removed_dependencies = removed.clone();
    let anchors: BTreeSet<_> = next
        .boundary_anchors
        .iter()
        .map(|a| a.version_uuid.as_str())
        .collect();
    removed_dependencies.extend(
        previous
            .boundary_anchors
            .iter()
            .filter(|a| {
                !anchors.contains(a.version_uuid.as_str())
                    && !versions.contains_key(a.version_uuid.as_str())
            })
            .map(|a| a.version_uuid.clone()),
    );
    validate_dependencies(next, &removed_dependencies)?;
    Ok(removed)
}

/// Remove only history outside the closure of every current Branch and native dependency.
/// This selects candidates; callers still admit the complete old/new transition.
pub fn prune_unreferenced_history(lineage: &mut ResearchLineage) {
    let versions: Versions<'_> = lineage
        .versions
        .iter()
        .map(|v| (v.version_uuid.as_str(), v))
        .collect();
    let mut pending: Vec<_> = lineage
        .branches
        .iter()
        .flat_map(|b| {
            [
                b.head_version_uuid.as_str(),
                b.base_version_uuid.as_str(),
                b.origin_version_uuid.as_str(),
            ]
        })
        .chain(lineage.proposals.iter().flat_map(|p| {
            [
                p.source_version_uuid.as_str(),
                p.payload_version_uuid.as_str(),
            ]
        }))
        .collect();
    let mut retained = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if retained.insert(id.to_owned())
            && let Some(version) = versions.get(id)
        {
            pending.extend(version.parents.iter().map(String::as_str));
            pending.extend(version.source_version_uuid.as_deref());
        }
    }
    drop(versions);
    lineage
        .versions
        .retain(|v| retained.contains(&v.version_uuid));
    lineage
        .boundary_anchors
        .retain(|a| retained.contains(&a.version_uuid));
}

fn validate_branches(
    previous: &ResearchLineage,
    next: &ResearchLineage,
    refs: &[String],
    rewrite: bool,
    versions: &Versions<'_>,
) -> Result<(), HubPublishError> {
    let branches: BTreeMap<_, _> = next.branches.iter().map(|b| (&b.branch_uuid, b)).collect();
    for old in &previous.branches {
        let new = branches
            .get(&old.branch_uuid)
            .ok_or_else(|| invalid("publication removes a published Branch"))?;
        let mut creation = (**new).clone();
        creation
            .head_version_uuid
            .clone_from(&old.head_version_uuid);
        if creation != *old {
            return Err(invalid(
                "publication changes immutable Branch creation authority",
            ));
        }
        let selected = refs.contains(&old.ref_name);
        if !selected && old.head_version_uuid != new.head_version_uuid {
            return Err(invalid(
                "publication changes a ref outside the committed ref set",
            ));
        }
        if selected
            && !rewrite
            && !descends_from(versions, &new.head_version_uuid, &old.head_version_uuid)
        {
            return Err(HubPublishError::new(
                HubPublishErrorCode::NonFastForward,
                "publication is not a structural fast-forward",
            ));
        }
    }
    Ok(())
}

fn validate_anchors(
    previous: &ResearchLineage,
    next: &ResearchLineage,
    prune: bool,
    versions: &Versions<'_>,
) -> Result<(), HubPublishError> {
    let anchors: BTreeMap<_, _> = next
        .boundary_anchors
        .iter()
        .map(|a| (&a.version_uuid, a))
        .collect();
    for old in &previous.boundary_anchors {
        if let Some(new) = anchors.get(&old.version_uuid) {
            if *old != **new {
                return Err(invalid("publication changes a Project-origin anchor"));
            }
        } else if let Some(record) = versions.get(old.version_uuid.as_str()) {
            if record.identity_digest != old.identity_digest || !record.parents.is_empty() {
                return Err(invalid("publication changes a Project-origin identity"));
            }
        } else if !prune {
            return Err(invalid(
                "removing Project-origin anchors requires explicit prune intent",
            ));
        }
    }
    Ok(())
}

fn descends_from(versions: &Versions<'_>, head: &str, ancestor: &str) -> bool {
    let mut pending = vec![head];
    let mut seen = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if id == ancestor {
            return true;
        }
        if seen.insert(id)
            && let Some(version) = versions.get(id)
        {
            pending.extend(version.parents.iter().map(String::as_str));
        }
    }
    false
}

fn validate_dependencies(
    next: &ResearchLineage,
    removed: &[String],
) -> Result<(), HubPublishError> {
    let removed: BTreeSet<_> = removed.iter().map(String::as_str).collect();
    let protected = next
        .branches
        .iter()
        .flat_map(|b| {
            [
                b.head_version_uuid.as_str(),
                b.base_version_uuid.as_str(),
                b.origin_version_uuid.as_str(),
            ]
        })
        .chain(next.proposals.iter().flat_map(|p| {
            [
                p.source_version_uuid.as_str(),
                p.payload_version_uuid.as_str(),
            ]
        }))
        .chain(next.versions.iter().flat_map(|v| {
            v.parents
                .iter()
                .map(String::as_str)
                .chain(v.source_version_uuid.as_deref())
        }))
        .chain(
            next.boundary_anchors
                .iter()
                .map(|a| a.version_uuid.as_str()),
        );
    if protected.into_iter().any(|id| removed.contains(id)) {
        return Err(invalid(
            "prune would remove a retained Branch, Proposal, projection or history dependency",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
