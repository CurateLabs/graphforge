//! Permanent-ID ownership facts and atomic commit admission.
use super::*;
use crate::{
    ItemAdmissionInput, ItemAdmissionPlan, ItemClaimAction, ItemClaimFact, ItemClaimState,
    ItemTupleFact, StableAccountId, item_admission_requirements, plan_item_admission,
};
use graphforge_discovery::ItemId;

pub(super) struct PermanentItem {
    owner: StableAccountId,
    versions: BTreeSet<(String, String)>,
}

impl ReferenceHub {
    /// Bind a reference-only verified publication account to a namespace.
    ///
    /// This explicit conformance hook models account aliases, handle changes and
    /// lease retirement; it is not an identity provider. Existing grants retain
    /// their issuing account and fail authorization after namespace reassignment.
    pub fn set_publication_account(
        &self,
        namespace: &str,
        account: &StableAccountId,
        canonical_handle: &str,
    ) -> Result<(), HubPublishError> {
        validate_handle(namespace)?;
        validate_handle(canonical_handle)?;
        StableAccountId::new(account.as_str())?;
        let mut state = self.state.lock().expect("hub state");
        state
            .namespace_accounts
            .insert(namespace.into(), account.clone());
        state
            .account_handles
            .insert(account.clone(), canonical_handle.into());
        Ok(())
    }

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
        let account = account_for_namespace(&mut state, publisher);
        let key = (account, id.as_str().to_owned());
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
        let publisher = account_for_namespace(&mut state, publisher);
        let organization = account_for_namespace(&mut state, organization);
        let key = (publisher, organization);
        if allowed {
            state.organization_publishers.insert(key);
        } else {
            state.organization_publishers.remove(&key);
        }
        Ok(())
    }
}

pub(super) fn account_for_namespace(state: &mut State, namespace: &str) -> StableAccountId {
    if let Some(account) = state.namespace_accounts.get(namespace) {
        return account.clone();
    }
    // Separate allocation leaves existing session/token and fixture IDs unchanged.
    let account = loop {
        state.account_counter += 1;
        let account =
            StableAccountId::new(format!("reference-account-{:016x}", state.account_counter))
                .expect("bounded reference account identity");
        if !state.account_handles.contains_key(&account) {
            break account;
        }
    };
    state
        .namespace_accounts
        .insert(namespace.into(), account.clone());
    state
        .account_handles
        .insert(account.clone(), namespace.into());
    account
}

fn validate_handle(handle: &str) -> Result<(), HubPublishError> {
    RepositoryIdentity {
        owner: handle.into(),
        repository: "item".into(),
    }
    .validate()
    .map_err(|_| invalid("reference publisher handle is invalid"))
}

/// Read only the advertised query tuples, then delegate every decision to the
/// public stateless planner. The caller holds the publication transaction lock.
pub(super) fn plan(
    state: &State,
    manifest: &DiscoveryManifest,
    publisher: &StableAccountId,
) -> Result<ItemAdmissionPlan, HubPublishError> {
    let facts = item_admission_requirements(manifest)?
        .into_iter()
        .map(|requirement| {
            let existing = state.permanent_items.get(&requirement.id);
            let claim = existing.map_or(ItemClaimState::Unclaimed {}, |item| {
                ItemClaimState::Claimed {
                    owner_account_id: item.owner.clone(),
                    owner_handle: state.account_handles.get(&item.owner).cloned(),
                }
            });
            let owner_visible = existing.is_some()
                && state
                    .visible_items
                    .contains(&(publisher.clone(), requirement.id.clone()));
            let publish_as_owner = existing.is_some_and(|item| {
                state
                    .organization_publishers
                    .contains(&(publisher.clone(), item.owner.clone()))
            });
            let tuples = requirement
                .tuples
                .into_iter()
                .map(|tuple| ItemTupleFact {
                    published: existing.is_some_and(|item| {
                        item.versions
                            .contains(&(tuple.version.clone(), tuple.content_digest.0.clone()))
                    }),
                    version: tuple.version,
                    content_digest: tuple.content_digest,
                })
                .collect();
            ItemClaimFact {
                id: requirement.id,
                claim,
                owner_visible,
                publish_as_owner,
                tuples,
            }
        })
        .collect();
    plan_item_admission(&ItemAdmissionInput {
        manifest: manifest.clone(),
        publisher_account_id: publisher.clone(),
        facts,
    })
}

pub(super) fn apply(state: &mut State, plan: ItemAdmissionPlan) {
    for decision in plan.decisions {
        if decision.action == ItemClaimAction::ExactReuse {
            continue;
        }
        state
            .permanent_items
            .entry(decision.id)
            .or_insert_with(|| PermanentItem {
                owner: plan.publisher_account_id.clone(),
                versions: BTreeSet::new(),
            })
            .versions
            .insert((decision.version, decision.content_digest.0));
    }
}
