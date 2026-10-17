//! Repository identity moves and explicit digest-addressed control locations.
use crate::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, RepositoryIdentity, Sha256Digest,
    parse_unique_json, validate_location,
};
use serde::{Deserialize, Serialize};
use url::Url;

/// Machine-readable body accompanying an HTTP 308 repository move.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryMoved {
    /// Must equal `graphforge-repository-moved/1`.
    pub format: String,
    /// Requested canonical identity, retained as an unreusable tombstone.
    pub moved_from: RepositoryIdentity,
    /// Current canonical identity on the same Hub origin.
    pub moved_to: RepositoryIdentity,
}
fn invalid() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::InvalidIdentity,
        Some("moved"),
        "repository move is invalid",
    )
}
impl RepositoryMoved {
    /// Parse a bounded move marker before interpreting any destination.
    pub fn from_json(bytes: &[u8], limits: DiscoveryLimits) -> Result<Self, DiscoveryError> {
        if bytes.len() > limits.max_response_bytes {
            return Err(crate::limit("moved"));
        }
        let value: serde_json::Value = parse_unique_json(bytes)?;
        if value.get("format").and_then(serde_json::Value::as_str)
            != Some("graphforge-repository-moved/1")
        {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::UnsupportedFuture,
                Some("moved.format"),
                "repository move format is unsupported",
            ));
        }
        let moved: Self = serde_json::from_value(value).map_err(|_| invalid())?;
        moved.validate()?;
        Ok(moved)
    }
    /// Validate distinct canonical identities.
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        if self.format != "graphforge-repository-moved/1" {
            return Err(invalid());
        }
        self.moved_from.validate()?;
        self.moved_to.validate()?;
        if self.moved_from == self.moved_to {
            return Err(invalid());
        }
        Ok(())
    }
    /// Validate a 308 Location against the exact requested repository endpoint.
    /// No host, credential, query, suffix, percent-encoding or scheme rewrite is
    /// inferred: the destination is one canonical identity on the same origin.
    pub fn destination(&self, requested: &Url, location: &str) -> Result<Url, DiscoveryError> {
        self.validate()?;
        validate_location(requested.as_str())?;
        let prefix = format!("/{}/.gf/", self.moved_from.canonical_name());
        let suffix = requested.path().strip_prefix(&prefix).ok_or_else(invalid)?;
        if suffix.is_empty() {
            return Err(invalid());
        }
        let destination = Url::parse(location).map_err(|_| invalid())?;
        validate_location(destination.as_str())?;
        let expected_path = format!("/{}/.gf/{suffix}", self.moved_to.canonical_name());
        let expected = format!(
            "{}{expected_path}",
            requested.origin().ascii_serialization()
        );
        if requested.origin() != destination.origin()
            || destination.path() != expected_path
            || location != expected
        {
            return Err(invalid());
        }
        Ok(destination)
    }
}

/// Validated explicit location derivation for newly re-homed control objects.
/// Native package/module locations are never inferred or rewritten.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectLocationTemplate(String);
impl ObjectLocationTemplate {
    /// Admit exactly one `{digest}` or `{hex}` placeholder in a credential-free
    /// HTTPS URL, using the same location rules as discovery objects.
    pub fn parse(value: &str) -> Result<Self, DiscoveryError> {
        if value.len() > DiscoveryLimits::default().max_string_bytes {
            return Err(crate::limit("object_location_template"));
        }
        let count = value.matches("{digest}").count() + value.matches("{hex}").count();
        let rest = value.replace("{digest}", "").replace("{hex}", "");
        if count != 1 || rest.contains(['{', '}']) {
            return Err(invalid());
        }
        let template = Self(value.into());
        template.expand(&Sha256Digest(format!("sha256:{}", "0".repeat(64))))?;
        Ok(template)
    }
    /// Whether this explicitly uses unprefixed lower-case SHA-256 hex.
    #[must_use]
    pub fn uses_hex(&self) -> bool {
        self.0.contains("{hex}")
    }
    /// Derive one validated location from a canonical digest.
    pub fn expand(&self, digest: &Sha256Digest) -> Result<String, DiscoveryError> {
        digest.validate()?;
        let location = self
            .0
            .replace("{digest}", &digest.0)
            .replace("{hex}", &digest.0[7..]);
        validate_location(&location)?;
        Ok(location)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn move_marker_binds_exact_origin_identity_and_endpoint_without_rewriting() {
        let moved = RepositoryMoved {
            format: "graphforge-repository-moved/1".into(),
            moved_from: RepositoryIdentity::parse("a/x").unwrap(),
            moved_to: RepositoryIdentity::parse("b/y").unwrap(),
        };
        let old = Url::parse("https://hub.example/a/x/.gf/refs").unwrap();
        assert_eq!(
            moved
                .destination(&old, "https://hub.example/b/y/.gf/refs")
                .unwrap()
                .path(),
            "/b/y/.gf/refs"
        );
        for invalid in [
            "https://other.example/b/y/.gf/refs",
            "http://hub.example/b/y/.gf/refs",
            "https://hub.example/b/y/.gf/manifest",
            "https://hub.example/b/../b/y/.gf/refs",
            "https://hub.example/%62/y/.gf/refs",
            "https://secret@hub.example/b/y/.gf/refs",
            "https://hub.example/b/y/.gf/refs?x=1",
        ] {
            assert!(moved.destination(&old, invalid).is_err());
        }
    }
}
