//! Lifetime claims over advertised ontology identities, never package contents.
use super::{ReferenceHub, Reject, State};
use crate::{HubPublishError, HubRequest, IdTakenItem};
use graphforge_discovery::{DiscoveryManifest, ItemId, ItemKind, RepositoryIdentity};
use std::collections::BTreeSet;

pub(super) struct Claim {
    owner: String,
    published: BTreeSet<(String, String)>,
}

fn advertised(manifest: &DiscoveryManifest) -> impl Iterator<Item = (ItemKind, &str, &str, &str)> {
    manifest.ontology.iter().flat_map(|ontology| {
        ontology
            .modules
            .iter()
            .map(|item| {
                (
                    ItemKind::OntologyModule,
                    item.id.as_str(),
                    item.version.as_str(),
                    item.content_digest.0.as_str(),
                )
            })
            .chain(ontology.bridge_sets.iter().map(|item| {
                (
                    ItemKind::BridgeSet,
                    item.id.as_str(),
                    item.version.as_str(),
                    item.content_digest.0.as_str(),
                )
            }))
    })
}

pub(super) fn check(
    state: &State,
    repository: &RepositoryIdentity,
    request: &HubRequest,
    manifest: &DiscoveryManifest,
) -> Result<(), Reject> {
    let mut refused = std::collections::BTreeMap::new();
    let token = request
        .header("authorization")
        .unwrap_or("")
        .strip_prefix("Bearer ")
        .unwrap_or("");
    for (kind, raw, version, digest) in advertised(manifest) {
        let Ok(id) = ItemId::parse_reference(raw) else {
            continue;
        };
        if id.kind() != kind {
            return Err(crate::error::invalid(
                "permanent ID kind differs from its ontology descriptor",
            )
            .into());
        }
        let Some(claim) = state.permanent_ids.get(id.as_str()) else {
            continue;
        };
        if claim.owner == repository.owner
            || claim
                .published
                .contains(&(version.to_owned(), digest.to_owned()))
        {
            continue;
        }
        let (visible, publish_as) = state
            .item_permissions
            .get(&(token.to_owned(), id.to_string()))
            .copied()
            .unwrap_or_default();
        refused.insert(
            id.to_string(),
            IdTakenItem {
                kind,
                id: id.to_string(),
                owner: visible.then(|| claim.owner.clone()),
                publish_as_owner: publish_as.then_some(true),
            },
        );
    }
    if !refused.is_empty() {
        return Err(HubPublishError::id_taken(refused.into_values().collect())?.into());
    }
    Ok(())
}

pub(super) fn claim(
    state: &mut State,
    repository: &RepositoryIdentity,
    manifest: &DiscoveryManifest,
) {
    for (kind, raw, version, digest) in advertised(manifest) {
        let Ok(id) = ItemId::parse_kind(raw, kind) else {
            continue;
        };
        let claim = state
            .permanent_ids
            .entry(id.to_string())
            .or_insert_with(|| Claim {
                owner: repository.owner.clone(),
                published: BTreeSet::new(),
            });
        // Exact foreign reuse never changes ownership or expands the owner's tuples.
        if claim.owner == repository.owner {
            claim
                .published
                .insert((version.to_owned(), digest.to_owned()));
        }
    }
}

impl ReferenceHub {
    /// Set this token's visibility and owning-organization publish permission for
    /// an item. The reference Hub's embedding host owns this authorization policy.
    pub fn set_item_permissions(
        &self,
        token: &str,
        id: &str,
        visible: bool,
        publish_as_owner: bool,
    ) -> Result<(), HubPublishError> {
        let id =
            ItemId::parse_reference(id).map_err(|_| crate::error::invalid("item ID is invalid"))?;
        self.state
            .lock()
            .expect("hub state")
            .item_permissions
            .insert(
                (token.to_owned(), id.to_string()),
                (visible, publish_as_owner),
            );
        Ok(())
    }
}
