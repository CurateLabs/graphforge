//! Same-Branch remote review uses the unique shared native Version, not parent field history.
use super::{PreviewResearchUpstreamRequest, invalid};
use crate::{
    CancellationToken, GfError, GraphForge, ResearchComparisonEndpoint,
    branches::fields,
    research_comparison::{delta, state},
};
use graphforge_storage::{ResolvedProjectGeneration, research_versions::ResearchRegistry};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub(super) fn load(
    owner: &GraphForge,
    current: &ResolvedProjectGeneration,
    registry: &ResearchRegistry,
    request: &PreviewResearchUpstreamRequest,
    local: &state::State,
    objects: &fields::Objects,
    cancel: &CancellationToken,
) -> Result<Option<state::State>, GfError> {
    let Some(remote) = &request.remote else {
        return Ok(None);
    };
    let local_head = local
        .version
        .ok_or_else(|| invalid("local Branch Version is unavailable"))?;
    let base = common(registry, local_head, remote.reference.head_version_uuid)?;
    require_retained(registry, base)?;
    let state = state::load(
        owner,
        current,
        registry,
        &ResearchComparisonEndpoint::Version { version_uuid: base },
        Some(objects),
        cancel,
    )?;
    if !state.missing.is_empty() {
        return Err(unavailable(
            "shared remote review base has unavailable dependencies",
        ));
    }
    Ok(Some(state))
}

pub(super) fn compare(
    local: &state::State,
    upstream: &state::State,
    base: Option<&state::State>,
    cancel: &CancellationToken,
) -> Result<Vec<delta::Row>, GfError> {
    if let Some(base) = base {
        delta::compare_from_base(local, upstream, &base.fields, cancel)
    } else {
        delta::compare(local, upstream, &BTreeMap::new(), cancel)
    }
}

fn require_retained(registry: &ResearchRegistry, base: Uuid) -> Result<(), GfError> {
    if !registry.materialized.contains(&base) || !registry.versions.contains_key(&base) {
        return Err(unavailable(
            "shared remote review base content is not retained",
        ));
    }
    Ok(())
}
fn unavailable(message: &str) -> GfError {
    GfError::Api {
        code: graphforge_core::ApiErrorCode::ResultNotRetained,
        message: message.into(),
    }
}

/// The input is the existing validated immutable DAG. Walk each endpoint once.
fn common(registry: &ResearchRegistry, left: Uuid, right: Uuid) -> Result<Uuid, GfError> {
    if !registry.identities.contains_key(&left) || !registry.identities.contains_key(&right) {
        return Err(unavailable(
            "remote review endpoint ancestry is unavailable",
        ));
    }
    let left: BTreeSet<_> = std::iter::once(left)
        .chain(registry.ancestors(left))
        .collect();
    let right: BTreeSet<_> = std::iter::once(right)
        .chain(registry.ancestors(right))
        .collect();
    let common: BTreeSet<_> = left.intersection(&right).copied().collect();
    // Common closure is ancestor-closed: every non-maximal member is a parent
    // of another common member. No per-candidate transitive walk or UUID tie-break.
    let parents: BTreeSet<_> = common
        .iter()
        .flat_map(|id| registry.ancestry.get(id).into_iter().flatten().copied())
        .collect();
    let mut maximal = common.difference(&parents).copied();
    let base = maximal
        .next()
        .ok_or_else(|| unavailable("remote heads have no authenticated common Version"))?;
    if maximal.next().is_some() {
        return Err(GfError::Api {
            code: graphforge_core::ApiErrorCode::AmbiguousProjection,
            message: "remote review has multiple maximal common Versions; explicit resolution is required".into(),
        });
    }
    Ok(base)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn dag(edges: &[(u128, &[u128])]) -> ResearchRegistry {
        let mut registry = ResearchRegistry::default();
        for (id, parents) in edges {
            registry.identities.insert(Uuid::from_u128(*id), [0; 32]);
            for parent in *parents {
                registry
                    .identities
                    .insert(Uuid::from_u128(*parent), [0; 32]);
            }
            registry.ancestry.insert(
                Uuid::from_u128(*id),
                parents.iter().map(|id| Uuid::from_u128(*id)).collect(),
            );
        }
        registry
    }
    #[test]
    fn shared_heads_ancestors_and_crisscross_are_explicit() {
        let registry = dag(&[(2, &[1]), (3, &[2]), (4, &[2])]);
        assert_eq!(
            common(&registry, Uuid::from_u128(3), Uuid::from_u128(4)).unwrap(),
            Uuid::from_u128(2)
        );
        assert_eq!(
            common(&registry, Uuid::from_u128(3), Uuid::from_u128(3)).unwrap(),
            Uuid::from_u128(3)
        );
        assert_eq!(
            common(&registry, Uuid::from_u128(2), Uuid::from_u128(4)).unwrap(),
            Uuid::from_u128(2)
        );
        let registry = dag(&[(2, &[1]), (3, &[1]), (4, &[2, 3]), (5, &[3, 2])]);
        assert!(matches!(
            common(&registry, Uuid::from_u128(4), Uuid::from_u128(5)),
            Err(GfError::Api {
                code: graphforge_core::ApiErrorCode::AmbiguousProjection,
                ..
            })
        ));
        assert!(matches!(
            require_retained(&registry, Uuid::from_u128(2)),
            Err(GfError::Api {
                code: graphforge_core::ApiErrorCode::ResultNotRetained,
                ..
            })
        ));
    }
    #[test]
    fn deep_shared_history_uses_the_latest_common_version() {
        let mut registry = ResearchRegistry::default();
        for n in 1..=4096 {
            let id = Uuid::from_u128(n);
            registry.identities.insert(id, [0; 32]);
            if n > 1 {
                registry.ancestry.insert(id, vec![Uuid::from_u128(n - 1)]);
            }
        }
        assert_eq!(
            common(&registry, Uuid::from_u128(4096), Uuid::from_u128(3072)).unwrap(),
            Uuid::from_u128(3072)
        );
    }
}
