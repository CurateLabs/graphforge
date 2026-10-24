//! Pure per-ID source changes over the same fresh host transaction as admission.
//! No plan reserves state or proves host authorization, retention or atomicity.
use crate::error::invalid;
use crate::item_admission::{AdmissionByteBudget, check_fact};
use crate::{
    HubPublishError, ItemAdmissionInput, ItemAdmissionPlan, ItemClaimAction, ItemClaimFact,
    ItemClaimState, MAX_ID_TAKEN_ITEMS, MAX_ITEM_ADMISSION_BYTES, StableAccountId,
    item_admission_requirements, parse_unique_json, plan_item_admission,
};
use graphforge_discovery::{
    DiscoveryLimits, ItemId, ItemRepositoryFact, ItemSourceDesignation, RefSet, StableRepositoryId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Fresh host lifecycle; deleted permanent IDs cannot be initialized or rehomed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum ItemSourceLifecycle {
    /// Item may be published and rehomed under current host authorization.
    Active,
    /// Permanent deletion; never initialize or change its source.
    Tombstoned,
}

/// Explicit conditional absence is different from a missing source fact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ItemSourceRecord {
    /// Host established that this ID has no source row; use create-if-absent CAS.
    Absent {},
    /// Existing per-ID repository, independent of authored versions and pins.
    Designated {
        /// Current per-ID source row and its CAS revision.
        designation: ItemSourceDesignation,
    },
}

/// One complete source/ownership epoch read alongside an admission claim fact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemSourceMutationFact {
    /// Canonical bare permanent ID, also the source-state key.
    pub id: String,
    /// Changes across ownership and lifecycle ABA, even if the owner returns.
    pub ownership_revision: String,
    /// Current permanent-item lifecycle.
    pub lifecycle: ItemSourceLifecycle,
    /// Complete source row, including explicit absence.
    pub source: ItemSourceRecord,
}

/// Internal host inputs, never client-authored authorization.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ItemSourceTransitionInput {
    /// Evaluate admission once and initialize only newly claimed permanent IDs.
    Publication {
        /// Existing complete admission request; its policy is not duplicated.
        admission: ItemAdmissionInput,
        /// Stable binding for the repository being committed.
        repository: ItemRepositoryFact,
        /// Proposed post-commit refs, not the previous/current refs snapshot.
        refs: RefSet,
        /// Exactly one source row per advertised canonical permanent ID.
        item_facts: Vec<ItemSourceMutationFact>,
    },
    /// Explicit item-owner operation; no historical-version migration is implied.
    Rehome {
        /// Account from fresh verified owner-scoped host authorization.
        verified_owner_account_id: StableAccountId,
        /// Existing claim fact; ownership is stable-account based.
        ownership_fact: ItemClaimFact,
        /// Current source and lifecycle read in the same transaction.
        current: ItemSourceMutationFact,
        /// Expected ownership/lifecycle epoch, guarding owner-transfer ABA.
        expected_ownership_revision: String,
        /// Expected source epoch, guarding source ABA.
        expected_source_revision: String,
        /// Expected current repository; revision equality alone is insufficient.
        expected_repository_id: StableRepositoryId,
        /// Fresh authorized target binding, including its CAS revision.
        target_repository: ItemRepositoryFact,
    },
}

impl ItemSourceTransitionInput {
    /// Bounded closed JSON with duplicate members refused by the native parser.
    pub fn from_json(bytes: &[u8]) -> Result<Self, HubPublishError> {
        if bytes.len() > MAX_ITEM_ADMISSION_BYTES {
            return Err(invalid("item source input exceeds its byte bound"));
        }
        serde_json::from_value(parse_unique_json(bytes)?)
            .map_err(|_| invalid("item source input is malformed"))
    }
}

/// Every precondition must still hold when the host applies the plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemSourcePrecondition {
    /// Correlated source, lifecycle and ownership revision, including absence.
    pub fact: ItemSourceMutationFact,
    /// Exact ownership state evaluated by admission or rehome.
    pub claim: ItemClaimState,
}

/// A source write; the host allocates a fresh non-reused source revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemSourceChange {
    /// Canonical permanent-ID state key.
    pub id: String,
    /// Desired repository for every version; no tuple or snapshot pins.
    pub repository_id: StableRepositoryId,
}

/// Apply only under the existing commit/ref lease with fresh authorization and
/// retention/lifecycle checks. Compare all preconditions and repository binding,
/// then commit admission, source, objects, manifests, refs and receipt atomically.
/// Revisions must distinguish ABA; descriptor hashes are not transaction epochs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemSourceTransitionPlan {
    /// Existing owning-admission result for publication; absent for rehome.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "typescript", tsify(optional))]
    pub admission: Option<ItemAdmissionPlan>,
    /// Complete ownership/source/lifecycle expectations, including no-op rows.
    pub preconditions: Vec<ItemSourcePrecondition>,
    /// Fresh target/publication repository identity and mapping revision.
    pub repository: ItemRepositoryFact,
    /// Sorted one-per-ID changes; omitted IDs retain their current source.
    pub changes: Vec<ItemSourceChange>,
}

fn revision(value: &str) -> Result<(), HubPublishError> {
    if value.is_empty()
        || value.len() > DiscoveryLimits::default().max_string_bytes
        || value.chars().any(char::is_control)
    {
        return Err(invalid("item source revision is invalid"));
    }
    Ok(())
}
fn validate_fact(fact: &ItemSourceMutationFact) -> Result<(), HubPublishError> {
    let id = ItemId::parse(&fact.id).map_err(|_| invalid("item source ID is invalid"))?;
    if id.as_str() != fact.id {
        return Err(invalid("item source ID is not canonical"));
    }
    revision(&fact.ownership_revision)?;
    if let ItemSourceRecord::Designated { designation } = &fact.source {
        designation
            .validate(DiscoveryLimits::default())
            .map_err(|_| invalid("item source designation is invalid"))?;
        if designation.id != fact.id {
            return Err(invalid("item source IDs disagree"));
        }
    }
    Ok(())
}
fn correlate(fact: &ItemSourceMutationFact, claim: &ItemClaimFact) -> Result<(), HubPublishError> {
    if fact.id != claim.id {
        return Err(invalid("item source and claim IDs disagree"));
    }
    if !matches!(
        (&claim.claim, &fact.source),
        (ItemClaimState::Unclaimed {}, ItemSourceRecord::Absent {})
            | (
                ItemClaimState::Claimed { .. },
                ItemSourceRecord::Designated { .. }
            )
    ) {
        return Err(invalid("item claim and source states disagree"));
    }
    Ok(())
}

/// Plan one bounded source transition without reading or writing host state.
/// Completed receipt replay must return BEFORE either this or admission planning;
/// replay never restores receipt-era ownership, source, lifecycle or snapshot pins.
pub fn plan_item_source_transition(
    input: &ItemSourceTransitionInput,
) -> Result<ItemSourceTransitionPlan, HubPublishError> {
    serde_json::to_writer(AdmissionByteBudget(MAX_ITEM_ADMISSION_BYTES), input)
        .map_err(|_| invalid("item source input exceeds its byte bound"))?;
    let limits = DiscoveryLimits::default();
    match input {
        ItemSourceTransitionInput::Publication {
            admission,
            repository,
            refs,
            item_facts,
        } => {
            repository
                .validate(limits)
                .map_err(|_| invalid("publication repository fact is invalid"))?;
            refs.validate(limits)
                .map_err(|_| invalid("post-commit refs are invalid"))?;
            let requirements = item_admission_requirements(&admission.manifest)?;
            if item_facts.len() > crate::MAX_ID_TAKEN_ITEMS
                || item_facts.len() != requirements.len()
            {
                return Err(invalid("item source facts are incomplete"));
            }
            let mut facts = BTreeMap::new();
            for fact in item_facts {
                validate_fact(fact)?;
                if facts.insert(fact.id.as_str(), fact).is_some() {
                    return Err(invalid("item source facts are duplicated"));
                }
            }
            if requirements
                .iter()
                .any(|r| !facts.contains_key(r.id.as_str()))
            {
                return Err(invalid("item source facts do not match admission"));
            }
            if refs.repository != repository.repository
                || admission.manifest.repository != repository.repository
            {
                return Err(invalid("publication repository bindings disagree"));
            }
            refs.validate_manifest(&admission.manifest)
                .map_err(|_| invalid("post-commit refs do not bind the publication"))?;
            let validator = admission
                .manifest
                .canonical_digest()
                .map_err(|_| invalid("publication manifest is invalid"))?;
            if !refs
                .refs
                .iter()
                .any(|r| r.name == admission.manifest.resolved_ref && r.validator == validator)
            {
                return Err(invalid("post-commit manifest validator disagrees"));
            }
            // Validate/correlate every source record before any admission refusal.
            for claim in &admission.facts {
                check_fact(claim)?;
                let fact = facts
                    .get(claim.id.as_str())
                    .ok_or_else(|| invalid("source fact is missing"))?;
                correlate(fact, claim)?;
            }
            let admission_plan = plan_item_admission(admission)?;
            if item_facts
                .iter()
                .any(|f| f.lifecycle != ItemSourceLifecycle::Active)
            {
                return Err(invalid("item is tombstoned"));
            }
            let mut changes = BTreeMap::new();
            for decision in &admission_plan.decisions {
                if decision.action == ItemClaimAction::NewClaim {
                    changes.insert(
                        decision.id.clone(),
                        ItemSourceChange {
                            id: decision.id.clone(),
                            repository_id: repository.repository_id.clone(),
                        },
                    );
                }
            }
            let preconditions = admission
                .facts
                .iter()
                .map(|claim| ItemSourcePrecondition {
                    fact: facts[claim.id.as_str()].clone(),
                    claim: claim.claim.clone(),
                })
                .collect();
            Ok(ItemSourceTransitionPlan {
                admission: Some(admission_plan),
                preconditions,
                repository: repository.clone(),
                changes: changes.into_values().collect(),
            })
        }
        ItemSourceTransitionInput::Rehome {
            verified_owner_account_id,
            ownership_fact,
            current,
            expected_ownership_revision,
            expected_source_revision,
            expected_repository_id,
            target_repository,
        } => {
            StableAccountId::new(verified_owner_account_id.as_str())?;
            if ownership_fact.tuples.len() > crate::MAX_ID_TAKEN_ITEMS {
                return Err(invalid("item source claim tuples exceed their bound"));
            }
            check_fact(ownership_fact)?;
            validate_fact(current)?;
            correlate(current, ownership_fact)?;
            revision(expected_ownership_revision)?;
            revision(expected_source_revision)?;
            expected_repository_id
                .validate(limits)
                .map_err(|_| invalid("expected source repository is invalid"))?;
            target_repository
                .validate(limits)
                .map_err(|_| invalid("target repository fact is invalid"))?;
            let ItemClaimState::Claimed {
                owner_account_id, ..
            } = &ownership_fact.claim
            else {
                return Err(invalid("item has no owner"));
            };
            let ItemSourceRecord::Designated { designation } = &current.source else {
                return Err(invalid("item has no source"));
            };
            if current.lifecycle != ItemSourceLifecycle::Active
                || owner_account_id != verified_owner_account_id
                || current.ownership_revision != *expected_ownership_revision
                || designation.revision != *expected_source_revision
                || designation.repository_id != *expected_repository_id
            {
                return Err(invalid("item source rehome preconditions disagree"));
            }
            let changes = if designation.repository_id == target_repository.repository_id {
                vec![]
            } else {
                vec![ItemSourceChange {
                    id: current.id.clone(),
                    repository_id: target_repository.repository_id.clone(),
                }]
            };
            Ok(ItemSourceTransitionPlan {
                admission: None,
                preconditions: vec![ItemSourcePrecondition {
                    fact: current.clone(),
                    claim: ownership_fact.claim.clone(),
                }],
                repository: target_repository.clone(),
                changes,
            })
        }
    }
}
