//! Digest-free exact-version resolution over a host-attested eligible inventory.

use crate::input_budget::InputBudget;
use crate::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ExactIdentity,
    ItemDescriptor, ItemId, ItemRepositoryFact, ItemVersionState, RefSet, check_response_bound,
    check_string, limit, malformed, parse_unique_json,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Fixed bound on eligible publications, before deduplication.
pub const MAX_ITEM_VERSION_CANDIDATES: usize = 4096;
/// Reserved provenance of validated complete absent/all-hidden facts.
pub const ITEM_VERSION_ABSENCE_FIELD: &str = "item_version_inventory.candidates";
/// Reserved provenance of distinct digests in a validated complete inventory.
pub const ITEM_VERSION_AMBIGUITY_FIELD: &str = "item_version_inventory.candidates.content_digest";
/// Reserved provenance of a currently visible permanent-item tombstone.
pub const ITEM_VERSION_GONE_FIELD: &str = "item_version_inventory.state";
/// Fixed upper bound when reading a resolution refusal or its diagnostic input.
pub const ITEM_RESOLUTION_ERROR_MAX_BYTES: usize = 1024;
const INPUT_FIELD: &str = "item_version_inventory";

/// Complete eligible publications for one canonical ID and opaque version.
///
/// The host filters current authorization and retention before constructing this
/// input, including before its bounds. Core cannot prove those host facts or
/// enumeration completeness. A truncated inventory must never claim completeness.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemVersionInventory {
    /// Canonical lowercase bare permanent ID.
    pub id: String,
    /// Nonempty opaque authored version; compared exactly, never ordered.
    pub version: String,
    /// Host attestation that no eligible publication was omitted.
    pub complete: bool,
    /// Fresh authorized lifecycle, distinct from an empty eligible inventory.
    pub state: ItemVersionState,
    /// Exactly one fresh stable-ID/path binding per candidate repository.
    pub repositories: Vec<ItemRepositoryFact>,
    /// Eligible admitted descriptors, in any order; identical copies are allowed.
    pub candidates: Vec<ItemDescriptor>,
}

impl ItemVersionInventory {
    /// Parse bounded, closed JSON without duplicate members.
    pub fn from_json(bytes: &[u8], limits: DiscoveryLimits) -> Result<Self, DiscoveryError> {
        check_response_bound(bytes, limits)?;
        let inventory: Self = parse_unique_json(bytes)?;
        inventory.validate(limits)?;
        Ok(inventory)
    }

    fn validate(&self, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
        if self.candidates.len() > MAX_ITEM_VERSION_CANDIDATES
            || self.repositories.len() > MAX_ITEM_VERSION_CANDIDATES
        {
            return Err(limit(INPUT_FIELD));
        }
        InputBudget::new(limits.max_response_bytes, INPUT_FIELD).account(self)?;
        check_string(&self.id, "item_version_inventory.id", limits)?;
        check_string(&self.version, "item_version_inventory.version", limits)?;
        let id = ItemId::parse(&self.id).map_err(|_| invalid_inventory())?;
        if id.as_str() != self.id || !self.complete {
            return Err(invalid_inventory());
        }
        let mut repository_ids = BTreeSet::new();
        let mut repository_paths = BTreeSet::new();
        for fact in &self.repositories {
            fact.validate(limits)?;
            if !repository_ids.insert(&fact.repository_id)
                || !repository_paths.insert(&fact.repository)
            {
                return Err(invalid_inventory());
            }
        }
        if let ItemVersionState::Published { designation } = &self.state {
            designation.validate(limits)?;
            if designation.id != self.id {
                return Err(invalid_inventory());
            }
        }
        let uri = id.to_uri();
        // Validate every record before deciding identity. Never let an earlier
        // pair of different digests mask a corrupt later record.
        for candidate in &self.candidates {
            candidate.validate(limits)?;
            if candidate.id != self.id
                || candidate.module.id != uri
                || candidate.module.version != self.version
            {
                return Err(invalid_inventory());
            }
        }
        let candidate_paths: BTreeSet<_> = self
            .candidates
            .iter()
            .map(|candidate| &candidate.repository)
            .collect();
        if candidate_paths != repository_paths {
            return Err(invalid_inventory());
        }
        if !matches!(self.state, ItemVersionState::Published { .. }) && !self.candidates.is_empty()
        {
            return Err(invalid_inventory());
        }
        Ok(())
    }
}

fn invalid_inventory() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::MalformedResponse,
        Some(INPUT_FIELD),
        "item version inventory is invalid or incomplete",
    )
}

/// Resolve identity without choosing a publishing source or fetching a package.
///
/// Only this operation produces the reserved absence/ambiguity provenance.
/// Repeated triples do not create ambiguity, even across repositories and pins.
pub fn resolve_item_version(
    inventory: &ItemVersionInventory,
    limits: DiscoveryLimits,
) -> Result<ExactIdentity, DiscoveryError> {
    inventory.validate(limits)?;
    match &inventory.state {
        ItemVersionState::AbsentOrHidden {} => {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MissingObject,
                Some(ITEM_VERSION_ABSENCE_FIELD),
                "item version is not found",
            ));
        }
        ItemVersionState::Tombstoned {} => {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::Gone,
                Some(ITEM_VERSION_GONE_FIELD),
                "item is gone",
            ));
        }
        ItemVersionState::Published { .. } => {}
    }
    let Some(first) = inventory.candidates.first() else {
        return Err(unavailable_source());
    };
    if inventory
        .candidates
        .iter()
        .any(|candidate| candidate.module.content_digest != first.module.content_digest)
    {
        return Err(DiscoveryError::new(
            DiscoveryErrorCode::AmbiguousIdentity,
            Some(ITEM_VERSION_AMBIGUITY_FIELD),
            "item version identity is ambiguous",
        ));
    }
    if !inventory
        .candidates
        .iter()
        .any(|candidate| designated_repository(inventory, candidate))
    {
        return Err(unavailable_source());
    }
    // All records have the same identity; this does not select the first source.
    Ok(first.module.clone())
}

fn unavailable_source() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::IntegrityFailure,
        Some("item_version_source"),
        "designated item source is unavailable",
    )
}

fn designated_repository(inventory: &ItemVersionInventory, candidate: &ItemDescriptor) -> bool {
    let ItemVersionState::Published { designation } = &inventory.state else {
        return false;
    };
    inventory.repositories.iter().any(|fact| {
        fact.repository == candidate.repository && fact.repository_id == designation.repository_id
    })
}

/// Validate an explicitly nominated eligible witness and its actual snapshots.
///
/// The returned descriptor is unchanged. Repository, refs, manifest and package
/// facts must never be assembled from different equivalent publications.
pub fn bind_item_version_source(
    inventory: &ItemVersionInventory,
    nominated: &ItemDescriptor,
    refs: &RefSet,
    manifest: &DiscoveryManifest,
    limits: DiscoveryLimits,
) -> Result<ItemDescriptor, DiscoveryError> {
    let mut budget = InputBudget::new(limits.max_response_bytes, INPUT_FIELD);
    budget.account(inventory)?;
    budget.account(nominated)?;
    budget.account(refs)?;
    budget.account(manifest)?;
    inventory.validate(limits)?;
    nominated.validate(limits)?;
    refs.validate(limits)?;
    manifest.validate(limits)?;
    let identity = resolve_item_version(inventory, limits)?;
    if nominated.module != identity
        || !inventory.candidates.contains(nominated)
        || !designated_repository(inventory, nominated)
    {
        return Err(DiscoveryError::new(
            DiscoveryErrorCode::IntegrityFailure,
            Some("item_version_source"),
            "nominated source is not an eligible witness",
        ));
    }
    nominated.bind_snapshot(refs, manifest)?;
    Ok(nominated.clone())
}

/// Bounded duplicate-aware JSON boundary for nomination and snapshot binding.
pub fn bind_item_version_source_json(
    inventory: &[u8],
    nominated: &[u8],
    refs: &[u8],
    manifest: &[u8],
    limits: DiscoveryLimits,
) -> Result<ItemDescriptor, DiscoveryError> {
    let mut budget = InputBudget::new(limits.max_response_bytes, INPUT_FIELD);
    for bytes in [inventory, nominated, refs, manifest] {
        budget.account_bytes(bytes)?;
    }
    let inventory = ItemVersionInventory::from_json(inventory, limits)?;
    // Parse every supplied record before logical resolution or source selection.
    let nominated = ItemDescriptor::from_json(nominated, limits)?;
    let refs = RefSet::from_json(refs, limits)?;
    let manifest = DiscoveryManifest::from_json(manifest, limits)?;
    bind_item_version_source(&inventory, &nominated, &refs, &manifest, limits)
}

/// Core-owned HTTP projection; `body` is exact JSON text or empty for 503.
///
/// 503 denotes an unavailable valid resolution, not a promise that malformed
/// input will repair itself. Transport retries remain bounded by client policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
pub struct ItemResolutionHttpResponse {
    /// 404 absence, 409 ambiguity, 410 visible deletion, otherwise 503.
    pub status: u16,
    /// Fixed sanitized JSON for logical refusals, or an empty string for 503.
    pub body: String,
}

impl DiscoveryError {
    /// Total resolution HTTP projection, retaining this error's provenance.
    #[must_use]
    pub fn item_resolution_http_response(&self) -> ItemResolutionHttpResponse {
        if self.version.is_some() {
            return item_resolution_error_response(None, None);
        }
        let code = match self.code {
            DiscoveryErrorCode::MissingObject => Some("missing_object"),
            DiscoveryErrorCode::AmbiguousIdentity => Some("ambiguous_identity"),
            DiscoveryErrorCode::Gone => Some("gone"),
            _ => None,
        };
        item_resolution_error_response(code, self.field)
    }
}

/// Project a resolution/binding diagnostic by BOTH stable code and provenance.
/// Unknown or missing facts, including unrelated MissingObject, always yield 503.
#[must_use]
pub fn item_resolution_error_response(
    code: Option<&str>,
    field: Option<&str>,
) -> ItemResolutionHttpResponse {
    let (status, body) = match (code, field) {
        (Some("missing_object"), Some(ITEM_VERSION_ABSENCE_FIELD)) => {
            (404, r#"{"code":"missing_object"}"#)
        }
        (Some("ambiguous_identity"), Some(ITEM_VERSION_AMBIGUITY_FIELD)) => {
            (409, r#"{"code":"ambiguous_identity"}"#)
        }
        (Some("gone"), Some(ITEM_VERSION_GONE_FIELD)) => (410, r#"{"code":"gone"}"#),
        _ => (503, ""),
    };
    ItemResolutionHttpResponse {
        status,
        body: body.into(),
    }
}

/// Total bounded projection of the existing WASM ProtocolError JSON shape.
/// Malformed JSON, duplicate/unknown members or malformed code/path yield 503.
#[must_use]
pub fn item_resolution_error_response_json(bytes: &[u8]) -> ItemResolutionHttpResponse {
    let unavailable = || item_resolution_error_response(None, None);
    if bytes.len() > ITEM_RESOLUTION_ERROR_MAX_BYTES {
        return unavailable();
    }
    let Ok(value) = parse_unique_json::<serde_json::Value>(bytes) else {
        return unavailable();
    };
    let Some(object) = value.as_object() else {
        return unavailable();
    };
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "code" | "path" | "message" | "version"))
        || object.get("message").is_some_and(|v| !v.is_string())
        || object.get("version").is_some_and(|v| !v.is_null())
    {
        return unavailable();
    }
    item_resolution_error_response(
        object.get("code").and_then(serde_json::Value::as_str),
        object.get("path").and_then(serde_json::Value::as_str),
    )
}

/// Parse only a closed HTTP 409 resolution ambiguity refusal.
/// This reader never manufactures the reserved absence provenance from HTTP.
/// A generic 409, malformed body or body/status disagreement is never ambiguity.
pub fn parse_item_resolution_refusal(
    status: u16,
    bytes: &[u8],
) -> Result<DiscoveryError, DiscoveryError> {
    if bytes.len() > ITEM_RESOLUTION_ERROR_MAX_BYTES {
        return Err(limit("item_resolution_error"));
    }
    let value: serde_json::Value = parse_unique_json(bytes)?;
    let object = value.as_object().ok_or_else(malformed)?;
    if object.len() != 1 {
        return Err(malformed());
    }
    let (code, field, detail) = match (
        status,
        object.get("code").and_then(serde_json::Value::as_str),
    ) {
        (409, Some("ambiguous_identity")) => (
            DiscoveryErrorCode::AmbiguousIdentity,
            ITEM_VERSION_AMBIGUITY_FIELD,
            "item version identity is ambiguous",
        ),
        _ => return Err(malformed()),
    };
    Ok(DiscoveryError::new(code, Some(field), detail))
}
