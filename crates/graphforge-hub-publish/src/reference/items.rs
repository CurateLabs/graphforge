//! Permanent-ID ownership facts and atomic commit admission.
use super::*;
use crate::IdTakenItem;
use graphforge_discovery::{ItemId, ItemKind};

pub(super) struct PermanentItem {
    owner: String,
    versions: BTreeSet<(String, String)>,
}

pub(super) struct Claim {
    id: String,
    exact: (String, String),
}

impl ReferenceHub {
    /// Set the reference permission fact that this publisher can see this item.
    /// This does not grant ownership or publication permission.
    pub fn set_item_visibility(
        &self,
        publisher: &str,
        id: &ItemId,
        visible: bool,
    ) -> Result<(), HubPublishError> {
        validate_handle(publisher)?;
        let mut state = self.state.lock().expect("hub state");
        let key = (publisher.to_owned(), id.as_str().to_owned());
        if visible {
            state.visible_items.insert(key);
        } else {
            state.visible_items.remove(&key);
        }
        Ok(())
    }

    /// Set an already-established permission to publish as an organization.
    /// These explicit facts are a conformance hook, not an identity provider.
    pub fn set_organization_publish_permission(
        &self,
        publisher: &str,
        organization: &str,
        allowed: bool,
    ) -> Result<(), HubPublishError> {
        validate_handle(publisher)?;
        validate_handle(organization)?;
        let mut state = self.state.lock().expect("hub state");
        let key = (publisher.to_owned(), organization.to_owned());
        if allowed {
            state.organization_publishers.insert(key);
        } else {
            state.organization_publishers.remove(&key);
        }
        Ok(())
    }
}

fn validate_handle(handle: &str) -> Result<(), HubPublishError> {
    RepositoryIdentity {
        owner: handle.into(),
        repository: "item".into(),
    }
    .validate()
    .map_err(|_| invalid("reference publisher handle is invalid"))
}

/// Plan every claim without changing the registry. Call under the commit lock,
/// then apply only after all other fallible admission checks have succeeded.
pub(super) fn plan(
    state: &State,
    manifest: &DiscoveryManifest,
    publisher: &str,
) -> Result<Vec<Claim>, HubPublishError> {
    let Some(ontology) = &manifest.ontology else {
        return Ok(Vec::new());
    };
    let entries = ontology
        .modules
        .iter()
        .map(|item| {
            (
                ItemKind::OntologyModule,
                &item.id,
                &item.version,
                &item.content_digest,
            )
        })
        .chain(ontology.bridge_sets.iter().map(|item| {
            (
                ItemKind::BridgeSet,
                &item.id,
                &item.version,
                &item.content_digest,
            )
        }));
    let mut claims = Vec::new();
    let mut refused = BTreeMap::new();
    for (kind, value, version, digest) in entries {
        let Ok(id) = ItemId::parse_reference(value) else {
            continue;
        };
        if id.kind() != kind {
            return Err(invalid("advertised permanent ID has the wrong item kind"));
        }
        let exact = (version.clone(), digest.0.clone());
        if let Some(item) = state.permanent_items.get(id.as_str())
            && item.owner != publisher
        {
            if item.versions.contains(&exact) {
                continue;
            }
            let visible = state
                .visible_items
                .contains(&(publisher.into(), id.as_str().into()));
            let organization = state
                .organization_publishers
                .contains(&(publisher.into(), item.owner.clone()));
            refused.insert(
                id.as_str().to_owned(),
                IdTakenItem {
                    kind,
                    id: id.as_str().into(),
                    owner: visible.then(|| item.owner.clone()),
                    publish_as_owner: organization.then_some(true),
                },
            );
            continue;
        }
        claims.push(Claim {
            id: id.as_str().into(),
            exact,
        });
    }
    if !refused.is_empty() {
        return Err(HubPublishError::id_taken(refused.into_values().collect())?);
    }
    Ok(claims)
}

pub(super) fn apply(state: &mut State, publisher: &str, claims: Vec<Claim>) {
    for claim in claims {
        state
            .permanent_items
            .entry(claim.id)
            .or_insert_with(|| PermanentItem {
                owner: publisher.into(),
                versions: BTreeSet::new(),
            })
            .versions
            .insert(claim.exact);
    }
}
