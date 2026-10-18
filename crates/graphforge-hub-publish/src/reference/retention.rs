//! Reference-only fixed grace over ordinary repository maps; no scheduler or host policy.
use super::*;

/// New reference retention rule, unrelated to token/device/upload-session expiry.
pub(super) const GRACE_SECONDS: u64 = 900;
#[derive(Default, Clone)]
pub(super) struct Retirement {
    objects: BTreeMap<String, u64>,
    manifests: BTreeMap<String, u64>,
}

/// Complete fallible marking before the atomic publication. Every current ref
/// retains its entire manifest inventory, including controls and shared payloads.
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
    let mut result = current.retirement.clone();
    let mut live_objects = BTreeSet::new();
    let live_manifests: BTreeSet<_> = refs.values().map(|r| r.validator.0.clone()).collect();
    for id in &live_manifests {
        let stored;
        let selected = if id == validator {
            manifest
        } else {
            stored = DiscoveryManifest::from_json(
                current
                    .manifests
                    .get(id)
                    .ok_or_else(|| invalid("live ref manifest is missing"))?,
                limits,
            )
            .map_err(|_| invalid("live ref manifest is invalid"))?;
            &stored
        };
        live_objects.extend(selected.objects.iter().map(|o| o.digest.0.clone()));
        // Native package references are validated by their control owner; mark
        // them as well rather than interpreting old metadata as garbage.
        if id != validator
            && let Some(reference) = &selected.lineage
        {
            let lineage = graphforge_discovery::ResearchLineage::from_json(
                &bounded_blob_read(
                    current
                        .objects
                        .get(&reference.object_digest.0)
                        .ok_or_else(|| invalid("live lineage object is missing"))?,
                    limits.max_lineage_bytes,
                )?,
                limits,
            )
            .map_err(|_| invalid("live lineage object is invalid"))?;
            live_objects.extend(
                lineage
                    .versions
                    .iter()
                    .filter_map(|v| v.package.as_ref())
                    .chain(lineage.proposals.iter().map(|p| &p.package))
                    .map(|p| p.object_digest.0.clone()),
            );
        }
    }
    result.objects.retain(|id, _| !live_objects.contains(id));
    result
        .manifests
        .retain(|id, _| !live_manifests.contains(id));
    if prune {
        let deadline = now
            .checked_add(GRACE_SECONDS)
            .ok_or_else(|| invalid("retention clock overflow"))?;
        for id in current
            .objects
            .keys()
            .filter(|id| !live_objects.contains(*id))
        {
            result.objects.entry(id.clone()).or_insert(deadline);
        }
        for id in current
            .manifests
            .keys()
            .filter(|id| !live_manifests.contains(*id))
        {
            result.manifests.entry(id.clone()).or_insert(deadline);
        }
    }
    Ok(result)
}

/// Session Blob Arcs retain their held data independently; only repository
/// reachability is collected. Sessions still face ordinary auth and final CAS.
pub(super) fn collect(state: &mut State) {
    for repository in state.repositories.values_mut() {
        repository.objects.retain(|id, _| {
            repository
                .retirement
                .objects
                .get(id)
                .is_none_or(|at| state.now < *at)
        });
        repository.manifests.retain(|id, _| {
            repository
                .retirement
                .manifests
                .get(id)
                .is_none_or(|at| state.now < *at)
        });
        repository
            .retirement
            .objects
            .retain(|_, at| state.now < *at);
        repository
            .retirement
            .manifests
            .retain(|_, at| state.now < *at);
    }
}
