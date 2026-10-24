//! Internal host facts for one repository designation per permanent item ID.
use crate::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, ItemId, RepositoryIdentity, check_string,
};
use serde::{Deserialize, Serialize};

/// Host-owned stable repository key. Never an account, display path or Project ID.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(transparent)]
pub struct StableRepositoryId(String);

impl StableRepositoryId {
    /// Construct an opaque key without normalization.
    pub fn new(value: impl Into<String>) -> Result<Self, DiscoveryError> {
        let id = Self(value.into());
        id.validate(DiscoveryLimits::default())?;
        Ok(id)
    }
    /// Exact internal host key, never a public item URL.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Validate a typed host input under the same bounds as JSON.
    pub fn validate(&self, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
        check_host_revision(&self.0, "repository_id", limits)
    }
}

/// Fresh binding from stable repository identity to its canonical display path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemRepositoryFact {
    /// Stable across repository renames and transfers.
    pub repository_id: StableRepositoryId,
    /// Current canonical discovery path.
    pub repository: RepositoryIdentity,
    /// Host CAS epoch; must distinguish rename/transfer ABA changes.
    pub revision: String,
}
impl ItemRepositoryFact {
    /// Intrinsic validation cannot establish freshness or authorization.
    pub fn validate(&self, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
        self.repository_id.validate(limits)?;
        self.repository.validate()?;
        check_host_revision(&self.revision, "repository.revision", limits)
    }
}

/// The single source for every version of one permanent item ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemSourceDesignation {
    /// Canonical bare permanent ID, never an exact-tuple key.
    pub id: String,
    /// Repository identity, independent of display names and retained pins.
    pub repository_id: StableRepositoryId,
    /// Host CAS epoch; changes across every source transition, including ABA.
    pub revision: String,
}
impl ItemSourceDesignation {
    /// Validate the source row without treating it as host authorization.
    pub fn validate(&self, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
        let id = ItemId::parse(&self.id).map_err(|_| invalid_source())?;
        check_string(&self.id, "item_source.id", limits)?;
        if id.as_str() != self.id {
            return Err(invalid_source());
        }
        self.repository_id.validate(limits)?;
        check_host_revision(&self.revision, "item_source.revision", limits)
    }
}

/// Current authorized lifecycle of the requested version. Hidden-only facts are
/// excluded by the host before enumeration and bounds, including tombstones.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ItemVersionState {
    /// Established version absence and all-hidden are indistinguishable.
    AbsentOrHidden {},
    /// Current caller may see the permanent item's deletion tombstone.
    Tombstoned {},
    /// Known visible publication; zero eligible witnesses means unavailable.
    Published {
        /// One per-ID source, even if its repository currently has no witness.
        designation: ItemSourceDesignation,
    },
}

fn check_host_revision(
    value: &str,
    field: &'static str,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    check_string(value, field, limits)?;
    if value.chars().any(char::is_control) {
        return Err(invalid_source());
    }
    Ok(())
}
fn invalid_source() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::MalformedResponse,
        Some("item_source"),
        "item source fact is invalid",
    )
}
