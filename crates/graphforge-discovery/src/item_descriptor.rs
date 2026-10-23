//! Bounded, Core-owned discovery of an item by permanent ID.

use crate::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ExactIdentity, ItemId,
    ItemKind, RefSet, RepositoryIdentity, Sha256Digest, canonical_json, check_response_bound,
    check_string, malformed, parse_unique_json, sha256_digest,
};
use serde::{Deserialize, Serialize};

/// Item descriptor format returned by content negotiation on a permanent ID.
pub const ITEM_DESCRIPTOR_FORMAT: &str = "graphforge-item/1";

/// Media type requested by tools resolving a permanent item ID.
pub const ITEM_DESCRIPTOR_MEDIA_TYPE: &str = "application/vnd.graphforge.item+json";

/// Immutable discovery metadata required to verify this item version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemDiscoverySnapshot {
    /// SHA-256 of the canonical retained refs snapshot.
    pub refs_revision: Sha256Digest,
    /// SHA-256 of the canonical retained manifest named by that snapshot.
    pub manifest_validator: Sha256Digest,
}

/// The publishing repository and exact identity of one permanent item's version.
///
/// This document is discovery metadata, never authority for native package
/// bytes: a client still binds refs and a manifest and verifies the advertised
/// module package through the existing portable verifier.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "typescript", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ItemDescriptor {
    /// Must equal `graphforge-item/1`.
    pub format: String,
    /// Canonical lowercase bare permanent ID, without an owner or version.
    pub id: String,
    /// Kind named by the permanent ID's prefix.
    pub kind: ItemKind,
    /// Repository publishing this exact version.
    pub repository: RepositoryIdentity,
    /// Exact identity; `module.id` is the permanent ID's canonical URI.
    pub module: ExactIdentity,
    /// Immutable discovery snapshot; contains digests, never transport URLs.
    pub discovery: ItemDiscoverySnapshot,
}

impl ItemDescriptor {
    /// Parse bounded JSON, refusing duplicate members and unsupported formats.
    pub fn from_json(bytes: &[u8], limits: DiscoveryLimits) -> Result<Self, DiscoveryError> {
        check_response_bound(bytes, limits)?;
        let value: serde_json::Value = parse_unique_json(bytes)?;
        if value.get("format").and_then(serde_json::Value::as_str) != Some(ITEM_DESCRIPTOR_FORMAT) {
            return Err(unsupported_format());
        }
        let descriptor: Self = serde_json::from_value(value).map_err(|_| malformed())?;
        descriptor.validate(limits)?;
        Ok(descriptor)
    }

    /// Require canonical ID spellings, a matching kind/URI, and exact identity.
    pub fn validate(&self, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
        if self.format != ITEM_DESCRIPTOR_FORMAT {
            return Err(unsupported_format());
        }
        check_string(&self.id, "id", limits)?;
        let id = ItemId::parse(&self.id).map_err(|_| invalid_id("id"))?;
        if id.as_str() != self.id {
            return Err(invalid_id("id"));
        }
        if id.kind() != self.kind {
            return Err(invalid_id("kind"));
        }
        self.repository.validate()?;
        check_string(&self.module.id, "module.id", limits)?;
        check_string(&self.module.version, "module.version", limits)?;
        self.module.content_digest.validate()?;
        self.discovery.refs_revision.validate()?;
        self.discovery.manifest_validator.validate()?;
        if self.module.id != id.to_uri() {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::IntegrityFailure,
                Some("module.id"),
                "item and module identities disagree",
            ));
        }
        Ok(())
    }

    /// Encode validated, deterministic compact JSON.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, DiscoveryError> {
        self.validate(DiscoveryLimits::default())?;
        canonical_json(self)
    }

    /// Bind retained refs and manifest bytes to this descriptor's exact module.
    ///
    /// Digests are recomputed from canonical documents. HTTP validators alone
    /// do not establish this binding, and current metadata cannot substitute
    /// for the requested snapshot after refs have advanced.
    pub fn bind_snapshot(
        &self,
        refs: &RefSet,
        manifest: &DiscoveryManifest,
    ) -> Result<(), DiscoveryError> {
        self.validate(DiscoveryLimits::default())?;
        if refs.repository != self.repository || manifest.repository != self.repository {
            return Err(mismatch("repository"));
        }
        if sha256_digest(&refs.to_canonical_json()?) != self.discovery.refs_revision {
            return Err(mismatch("discovery.refs_revision"));
        }
        if manifest.canonical_digest()? != self.discovery.manifest_validator {
            return Err(mismatch("discovery.manifest_validator"));
        }
        refs.validate_manifest(manifest)?;
        let reference = refs
            .refs
            .iter()
            .find(|reference| reference.name == manifest.resolved_ref)
            .ok_or_else(|| mismatch("resolved_ref"))?;
        if reference.validator != self.discovery.manifest_validator {
            return Err(mismatch("discovery.manifest_validator"));
        }
        let matches = manifest
            .ontology
            .as_ref()
            .is_some_and(|inventory| match self.kind {
                ItemKind::OntologyModule => inventory
                    .modules
                    .iter()
                    .any(|module| module.identity() == self.module),
                ItemKind::BridgeSet => inventory.bridge_sets.iter().any(|bridge| {
                    bridge.id == self.module.id
                        && bridge.version == self.module.version
                        && bridge.content_digest == self.module.content_digest
                }),
            });
        if !matches {
            return Err(mismatch("module"));
        }
        Ok(())
    }
}

fn mismatch(field: &'static str) -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::IntegrityFailure,
        Some(field),
        "item descriptor disagrees with discovery snapshot",
    )
}

fn unsupported_format() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::UnsupportedFuture,
        Some("format"),
        "item descriptor format is unsupported",
    )
}

fn invalid_id(field: &'static str) -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::InvalidIdentity,
        Some(field),
        "item descriptor permanent ID or kind is invalid",
    )
}
