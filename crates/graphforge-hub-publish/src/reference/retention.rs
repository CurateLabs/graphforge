//! Reference-only fixed grace over ordinary repository maps; no scheduler or host policy.
use super::*;

/// New reference retention rule, unrelated to token/device/upload-session expiry.
pub(super) const GRACE_SECONDS: u64 = 900;
#[derive(Default, Clone)]
pub(super) struct Retirement {
    objects: BTreeMap<String, u64>,
    manifests: BTreeMap<String, u64>,
}

/// Everything reachable from a set of current refs: every manifest inventory,
/// including controls and shared payloads, and every native package a live
/// lineage still names (Version, Branch-creation and Proposal dependencies).
#[derive(Default)]
pub(super) struct Live {
    objects: BTreeSet<String>,
    manifests: BTreeSet<String>,
}

impl Live {
    /// Mark one live manifest and, when its lineage is known, the lineage's packages.
    pub(super) fn mark(
        &mut self,
        validator: &str,
        manifest: &DiscoveryManifest,
        lineage: Option<&graphforge_discovery::ResearchLineage>,
    ) {
        self.manifests.insert(validator.to_owned());
        self.objects
            .extend(manifest.objects.iter().map(|o| o.digest.0.clone()));
        // Native package references are validated by their control owner; mark
        // them as well rather than interpreting old metadata as garbage.
        if let Some(lineage) = lineage {
            self.objects.extend(
                lineage
                    .versions
                    .iter()
                    .filter_map(|v| v.package.as_ref())
                    .chain(lineage.proposals.iter().map(|p| &p.package))
                    .map(|p| p.object_digest.0.clone()),
            );
        }
    }
}

impl Retirement {
    /// Cancel the pending retirement of everything that is reachable again.
    pub(super) fn cancel(&mut self, live: &Live) {
        self.objects.retain(|id, _| !live.objects.contains(id));
        self.manifests.retain(|id, _| !live.manifests.contains(id));
    }

    fn due(&self, now: u64) -> bool {
        self.objects
            .values()
            .chain(self.manifests.values())
            .any(|at| *at <= now)
    }
}

/// Reachability from `refs` over the repository's stored documents. A
/// `candidate` manifest not yet stored stands in for its own validator; its
/// packages must already appear in its inventory.
fn live(
    current: &Repository,
    refs: &BTreeMap<String, RepositoryRef>,
    candidate: Option<(&str, &DiscoveryManifest)>,
    limits: DiscoveryLimits,
) -> Result<Live, HubPublishError> {
    let mut live = Live::default();
    let validators: BTreeSet<_> = refs.values().map(|r| r.validator.0.as_str()).collect();
    for id in validators {
        if let Some((_, manifest)) = candidate.filter(|(validator, _)| *validator == id) {
            live.mark(id, manifest, None);
            continue;
        }
        let manifest = DiscoveryManifest::from_json(
            current
                .manifests
                .get(id)
                .ok_or_else(|| invalid("live ref manifest is missing"))?,
            limits,
        )
        .map_err(|_| invalid("live ref manifest is invalid"))?;
        let lineage = manifest
            .lineage
            .as_ref()
            .map(|reference| {
                graphforge_discovery::ResearchLineage::from_json(
                    &bounded_blob_read(
                        current
                            .objects
                            .get(&reference.object_digest.0)
                            .ok_or_else(|| invalid("live lineage object is missing"))?,
                        limits.max_lineage_bytes,
                    )?,
                    limits,
                )
                .map_err(|_| invalid("live lineage object is invalid"))
            })
            .transpose()?;
        live.mark(id, &manifest, lineage.as_ref());
    }
    Ok(live)
}

/// Complete fallible marking before the atomic publication.
pub(super) fn schedule(
    current: Option<&Repository>,
    refs: &BTreeMap<String, RepositoryRef>,
    validator: &str,
    manifest: &DiscoveryManifest,
    now: u64,
    prune: bool,
    limits: DiscoveryLimits,
) -> Result<Retirement, HubPublishError> {
    let Some(current) = current else {
        return Ok(Retirement::default());
    };
    let live = live(current, refs, Some((validator, manifest)), limits)?;
    let mut result = current.retirement.clone();
    result.cancel(&live);
    if prune {
        let deadline = now
            .checked_add(GRACE_SECONDS)
            .ok_or_else(|| invalid("retention clock overflow"))?;
        for id in current
            .objects
            .keys()
            .filter(|id| !live.objects.contains(*id))
        {
            result.objects.entry(id.clone()).or_insert(deadline);
        }
        for id in current
            .manifests
            .keys()
            .filter(|id| !live.manifests.contains(*id))
        {
            result.manifests.entry(id.clone()).or_insert(deadline);
        }
    }
    Ok(result)
}

/// Session Blob Arcs retain their held data independently; only repository
/// reachability is collected. Sessions still face ordinary auth and final CAS.
/// A passed deadline is not proof of garbage: reachability from the current
/// refs is re-derived immediately before anything is deleted. If it cannot be
/// derived, nothing is deleted.
pub(super) fn collect(state: &mut State, limits: DiscoveryLimits) {
    let now = state.now;
    for repository in state.repositories.values_mut() {
        if !repository.retirement.due(now) {
            continue;
        }
        let Ok(live) = live(repository, &repository.refs, None, limits) else {
            continue;
        };
        repository.retirement.cancel(&live);
        let retirement = &mut repository.retirement;
        repository
            .objects
            .retain(|id, _| retirement.objects.get(id).is_none_or(|at| now < *at));
        repository
            .manifests
            .retain(|id, _| retirement.manifests.get(id).is_none_or(|at| now < *at));
        retirement.objects.retain(|_, at| now < *at);
        retirement.manifests.retain(|_, at| now < *at);
    }
}
