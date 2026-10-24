//! Stateless permanent-item admission over fresh, authoritative host facts.
//!
//! These are internal host inputs, not caller-supplied authorization. The host
//! must obtain complete facts and recheck its verified scoped account, live
//! organization permissions and entitlement in the same transaction that applies
//! this plan with publication state. A plan is neither authority nor a cacheable
//! reservation. Exact reuse skips ownership writes, never publication writes.

use crate::error::invalid;
use crate::{HubPublishError, IdTakenItem, MAX_ID_TAKEN_ITEMS, parse_unique_json};
use graphforge_discovery::{
    DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ItemId, ItemKind, RepositoryIdentity,
    Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Maximum encoded internal admission request, including manifest and facts.
pub const MAX_ITEM_ADMISSION_BYTES: usize = 16 * 1024 * 1024;

/// Opaque stable account identity; never a display handle or repository name.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(transparent)]
pub struct StableAccountId(String);

impl StableAccountId {
    /// Construct an opaque nonempty, bounded ID without normalization.
    pub fn new(value: impl Into<String>) -> Result<Self, HubPublishError> {
        let id = Self(value.into());
        id.validate()?;
        Ok(id)
    }

    /// The exact host-owned identity, never a public owner display label.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), HubPublishError> {
        text(&self.0)?;
        if self.0.chars().any(char::is_control) {
            return Err(invalid("stable account identity is invalid"));
        }
        Ok(())
    }
}

/// One exact tuple under an already-canonical permanent item ID.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemExactTuple {
    /// Opaque authored version, compared exactly.
    pub version: String,
    /// Canonical module/bridge content digest, never a package digest.
    pub content_digest: Sha256Digest,
}

/// The complete canonical query a host must answer before planning admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemClaimRequirement {
    /// Canonical bare permanent ID.
    pub id: String,
    /// Kind derived from the ID and advertised inventory.
    pub kind: ItemKind,
    /// Sorted unique advertised exact tuples, not the item's whole history.
    pub tuples: Vec<ItemExactTuple>,
}

/// Explicit ownership absence is different from a missing database fact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ItemClaimState {
    /// The transaction established that no account owns this ID.
    Unclaimed {},
    /// The transaction read this ID's existing stable account owner.
    Claimed {
        /// Stable ownership key, independent of mutable handles.
        owner_account_id: StableAccountId,
        /// Current canonical display handle; required when visible.
        #[serde(
            default,
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        #[cfg_attr(feature = "typescript", tsify(optional))]
        owner_handle: Option<String>,
    },
}

fn present<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

/// Host database membership evidence for one specifically advertised tuple.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemTupleFact {
    /// Opaque authored version.
    pub version: String,
    /// Exact canonical content digest being queried.
    pub content_digest: Sha256Digest,
    /// Explicit current membership, not a host admission verdict.
    pub published: bool,
}

/// One complete per-ID fact row for this authenticated commit transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemClaimFact {
    /// Canonical bare permanent ID; exactly one row is required per query ID.
    pub id: String,
    /// Explicit absence or existing stable ownership.
    pub claim: ItemClaimState,
    /// A currently authorized admitted publication witness makes the owner visible.
    pub owner_visible: bool,
    /// Independently proven owning-org publication permission for the actor's
    /// current valid lease. This hint never authorizes foreign changed content.
    pub publish_as_owner: bool,
    /// Exactly one explicit membership fact per advertised tuple, no history dump.
    pub tuples: Vec<ItemTupleFact>,
}

/// Internal host request; authorization fields must never come from caller JSON.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemAdmissionInput {
    /// Only this commit's advertised ontology inventory participates.
    pub manifest: DiscoveryManifest,
    /// Account derived from verified scoped publication authorization.
    pub publisher_account_id: StableAccountId,
    /// Fresh complete transaction facts; missing rows are never unclaimed IDs.
    pub facts: Vec<ItemClaimFact>,
}

impl ItemAdmissionInput {
    /// Parse bounded closed JSON without duplicate members.
    pub fn from_json(bytes: &[u8]) -> Result<Self, HubPublishError> {
        if bytes.len() > MAX_ITEM_ADMISSION_BYTES {
            return Err(invalid("item admission input exceeds its byte bound"));
        }
        serde_json::from_value(parse_unique_json(bytes)?)
            .map_err(|_| invalid("item admission input is malformed"))
    }
}

/// Ownership/exact-membership action, distinct from publication persistence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum ItemClaimAction {
    /// Conditionally create an owner claim and append this exact tuple.
    NewClaim,
    /// Preserve existing ownership and append this exact tuple.
    OwnerUpdate,
    /// No ownership/membership mutation; still commit the publication witness.
    ExactReuse,
}

/// A deterministic decision for one canonical advertised exact tuple.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemClaimDecision {
    /// Canonical bare ID.
    pub id: String,
    /// Kind matching the advertised inventory.
    pub kind: ItemKind,
    /// Opaque authored version.
    pub version: String,
    /// Canonical content identity.
    pub content_digest: Sha256Digest,
    /// Core-owned decision; exact reuse cannot transfer ownership.
    pub action: ItemClaimAction,
}

/// Valid only with the same fresh facts and authorization in the host mutation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemAdmissionPlan {
    /// Verified publication account to use for new/owned tuple updates.
    pub publisher_account_id: StableAccountId,
    /// Sorted by canonical ID, opaque version and content digest.
    pub decisions: Vec<ItemClaimDecision>,
}

fn text(value: &str) -> Result<(), HubPublishError> {
    if value.is_empty() || value.len() > DiscoveryLimits::default().max_string_bytes {
        return Err(invalid("item admission string exceeds its bound"));
    }
    Ok(())
}

/// Derive complete query keys without host-language alias or legacy rules.
pub fn item_admission_requirements(
    manifest: &DiscoveryManifest,
) -> Result<Vec<ItemClaimRequirement>, HubPublishError> {
    manifest
        .validate(DiscoveryLimits::default())
        .map_err(|error| {
            if error.code == DiscoveryErrorCode::UnsupportedFuture {
                HubPublishError::new(
                    crate::HubPublishErrorCode::UnsupportedFuture,
                    "manifest requires an unsupported discovery capability",
                )
            } else {
                invalid("item admission manifest is invalid")
            }
        })?;
    serde_json::to_writer(
        AdmissionByteBudget(DiscoveryLimits::default().max_response_bytes),
        manifest,
    )
    .map_err(|_| invalid("item admission manifest exceeds its byte bound"))?;
    let mut grouped: BTreeMap<String, (ItemKind, BTreeSet<ItemExactTuple>)> = BTreeMap::new();
    if let Some(ontology) = &manifest.ontology {
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
        for (kind, value, version, digest) in entries {
            let Ok(id) = ItemId::parse_reference(value) else {
                continue;
            };
            if id.kind() != kind {
                return Err(invalid("advertised permanent ID has the wrong item kind"));
            }
            grouped
                .entry(id.as_str().into())
                .or_insert_with(|| (kind, BTreeSet::new()))
                .1
                .insert(ItemExactTuple {
                    version: version.clone(),
                    content_digest: digest.clone(),
                });
        }
    }
    Ok(grouped
        .into_iter()
        .map(|(id, (kind, tuples))| ItemClaimRequirement {
            id,
            kind,
            tuples: tuples.into_iter().collect(),
        })
        .collect())
}

pub(crate) fn check_fact(
    fact: &ItemClaimFact,
) -> Result<BTreeMap<ItemExactTuple, bool>, HubPublishError> {
    let id = ItemId::parse(&fact.id).map_err(|_| invalid("claim fact ID is invalid"))?;
    if id.as_str() != fact.id {
        return Err(invalid("claim fact ID is not canonical"));
    }
    match &fact.claim {
        ItemClaimState::Unclaimed {} => {
            if fact.owner_visible
                || fact.publish_as_owner
                || fact.tuples.iter().any(|t| t.published)
            {
                return Err(invalid("unclaimed item facts are inconsistent"));
            }
        }
        ItemClaimState::Claimed {
            owner_account_id,
            owner_handle,
        } => {
            owner_account_id.validate()?;
            if fact.owner_visible && owner_handle.is_none() {
                return Err(invalid("visible owner handle is missing"));
            }
            if let Some(handle) = owner_handle {
                RepositoryIdentity {
                    owner: handle.clone(),
                    repository: "item".into(),
                }
                .validate()
                .map_err(|_| invalid("owner display handle is invalid"))?;
            }
        }
    }
    let mut tuples = BTreeMap::new();
    for tuple in &fact.tuples {
        text(&tuple.version)?;
        tuple
            .content_digest
            .validate()
            .map_err(|_| invalid("claim tuple digest is invalid"))?;
        let key = ItemExactTuple {
            version: tuple.version.clone(),
            content_digest: tuple.content_digest.clone(),
        };
        if tuples.insert(key, tuple.published).is_some() {
            return Err(invalid("claim tuple facts are duplicated"));
        }
    }
    Ok(tuples)
}

// Count serialized bytes without allocating an unbounded second copy of typed
// host input. This keeps the native entry point and JSON/WASM limits identical.
pub(crate) struct AdmissionByteBudget(pub(crate) usize);
impl std::io::Write for AdmissionByteBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("item admission byte bound"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Evaluate the same policy used by ReferenceHub, without reading or writing state.
///
/// Hosts must not pass stale, partial, client-authored or defaulted facts. Check
/// the current account/org leases and entitlement and apply any ownership delta
/// with refs, manifests, objects, receipt and actual publication witnesses in one
/// atomic mutation. Completed idempotent replay must bypass new claim application.
pub fn plan_item_admission(
    input: &ItemAdmissionInput,
) -> Result<ItemAdmissionPlan, HubPublishError> {
    input.publisher_account_id.validate()?;
    let requirements = item_admission_requirements(&input.manifest)?;
    if input.facts.len() > MAX_ID_TAKEN_ITEMS
        || input.facts.len() != requirements.len()
        || input
            .facts
            .iter()
            .try_fold(0_usize, |count, fact| count.checked_add(fact.tuples.len()))
            .is_none_or(|count| count > MAX_ID_TAKEN_ITEMS)
    {
        return Err(invalid("claim facts are incomplete or exceed their bound"));
    }
    serde_json::to_writer(AdmissionByteBudget(MAX_ITEM_ADMISSION_BYTES), input)
        .map_err(|_| invalid("item admission input exceeds its byte bound"))?;
    let mut facts = BTreeMap::new();
    for fact in &input.facts {
        if facts.insert(fact.id.as_str(), fact).is_some() {
            return Err(invalid("claim ID facts are duplicated"));
        }
    }
    let mut decisions = Vec::new();
    let mut refused = Vec::new();
    for requirement in requirements {
        let fact = facts
            .get(requirement.id.as_str())
            .ok_or_else(|| invalid("claim ID fact is missing"))?;
        let tuples = check_fact(fact)?;
        if tuples.keys().ne(requirement.tuples.iter()) {
            return Err(invalid("claim tuple facts do not match the advertisement"));
        }
        let mut refusal = false;
        for tuple in requirement.tuples {
            let action = match &fact.claim {
                ItemClaimState::Unclaimed {} => ItemClaimAction::NewClaim,
                ItemClaimState::Claimed {
                    owner_account_id, ..
                } if owner_account_id == &input.publisher_account_id => {
                    ItemClaimAction::OwnerUpdate
                }
                ItemClaimState::Claimed { .. } if tuples[&tuple] => ItemClaimAction::ExactReuse,
                ItemClaimState::Claimed { .. } => {
                    refusal = true;
                    continue;
                }
            };
            decisions.push(ItemClaimDecision {
                id: requirement.id.clone(),
                kind: requirement.kind,
                version: tuple.version,
                content_digest: tuple.content_digest,
                action,
            });
        }
        if refusal {
            let owner = match &fact.claim {
                ItemClaimState::Claimed { owner_handle, .. } if fact.owner_visible => {
                    owner_handle.clone()
                }
                _ => None,
            };
            refused.push(IdTakenItem {
                kind: requirement.kind,
                id: requirement.id,
                owner,
                publish_as_owner: fact.publish_as_owner.then_some(true),
            });
        }
    }
    if !refused.is_empty() {
        return Err(HubPublishError::id_taken(refused)?);
    }
    Ok(ItemAdmissionPlan {
        publisher_account_id: input.publisher_account_id.clone(),
        decisions,
    })
}
