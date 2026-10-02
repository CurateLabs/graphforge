//! Provider-neutral GraphForge Hub publish wire contract.
//!
//! This crate owns publish error classification, transport traits, and an
//! in-memory Hub for tests. It deliberately contains no HTTP client, project
//! I/O, portable verification, or discovery parsing.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

/// Publish protocol format emitted and accepted by this release.
pub const HUB_PUBLISH_FORMAT: &str = "graphforge-hub-publish/1";

/// GraphForge-wide idempotency conflict code for changed content under a stable operation identity.
pub const GF_IDEMPOTENCY_CONFLICT: &str = "GF_IDEMPOTENCY_CONFLICT";

/// Stable machine-readable publish failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HubPublishErrorCode {
    /// Publish credential missing, expired, or rejected.
    AuthDenied,
    /// Entitlement or quota denied publication.
    EntitlementDenied,
    /// Same operation identity with a conflicting request commitment.
    IdempotencyConflict,
    /// Ref advance rejected by an expected-revision precondition.
    RefConflict,
    /// Required future protocol version or capability is unsupported.
    UnsupportedVersion,
    /// Declared digest or length disagrees with transported bytes.
    IntegrityFailure,
    /// Input failed structural validation before I/O.
    InvalidInput,
    /// Transport or registry internal failure.
    Internal,
}

/// Sanitized publish error suitable for CLI, Hub, and telemetry projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HubPublishError {
    /// Stable machine-readable code.
    pub code: HubPublishErrorCode,
    #[serde(skip)]
    detail: &'static str,
}

impl HubPublishError {
    /// Construct a publish error with a stable detail string (never untrusted input).
    #[must_use]
    pub const fn new(code: HubPublishErrorCode, detail: &'static str) -> Self {
        Self { code, detail }
    }

    /// Sanitized, non-input-bearing diagnostic text.
    #[must_use]
    pub const fn detail(&self) -> &'static str {
        self.detail
    }

    /// Wire-level token used in Hub JSON and logs (`hub.publish.*`).
    #[must_use]
    pub const fn wire_code(&self) -> &'static str {
        match self.code {
            HubPublishErrorCode::AuthDenied => "hub.publish.auth_denied",
            HubPublishErrorCode::EntitlementDenied => "hub.publish.entitlement_denied",
            HubPublishErrorCode::IdempotencyConflict => "hub.publish.idempotency_conflict",
            HubPublishErrorCode::RefConflict => "hub.publish.ref_conflict",
            HubPublishErrorCode::UnsupportedVersion => "hub.publish.unsupported_future",
            HubPublishErrorCode::IntegrityFailure => "hub.publish.integrity_failure",
            HubPublishErrorCode::InvalidInput => "hub.publish.invalid_input",
            HubPublishErrorCode::Internal => "hub.publish.internal",
        }
    }

    /// Native GraphForge error code projected to callers (`GF_*`), when distinct from wire tokens.
    #[must_use]
    pub const fn gf_code(&self) -> &'static str {
        match self.code {
            HubPublishErrorCode::IdempotencyConflict => GF_IDEMPOTENCY_CONFLICT,
            HubPublishErrorCode::AuthDenied => "hub.publish.auth_denied",
            HubPublishErrorCode::EntitlementDenied => "hub.publish.entitlement_denied",
            HubPublishErrorCode::RefConflict => "hub.publish.ref_conflict",
            HubPublishErrorCode::UnsupportedVersion => "hub.publish.unsupported_future",
            HubPublishErrorCode::IntegrityFailure => "hub.publish.integrity_failure",
            HubPublishErrorCode::InvalidInput => "hub.publish.invalid_input",
            HubPublishErrorCode::Internal => "hub.publish.internal",
        }
    }
}

impl fmt::Display for HubPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} ({}): {}",
            self.gf_code(),
            self.wire_code(),
            self.detail
        )
    }
}

impl std::error::Error for HubPublishError {}

/// Data-plane transport for immutable publish objects (digest-addressed bytes).
pub trait HubPublishTransport: Send + Sync {
    /// Store immutable object bytes under `object_digest`.
    ///
    /// Implementations must verify digest and length before retention. Storing
    /// the same digest with identical bytes is idempotent; conflicting bytes
    /// for an existing digest fail with [`HubPublishErrorCode::IdempotencyConflict`].
    fn put_object(&self, object_digest: &str, bytes: &[u8]) -> Result<(), HubPublishError>;

    /// Read immutable object bytes previously stored under `object_digest`.
    fn get_object(&self, object_digest: &str) -> Result<Vec<u8>, HubPublishError>;

    /// Return whether `object_digest` is already stored.
    fn object_exists(&self, object_digest: &str) -> Result<bool, HubPublishError>;
}

/// In-process Hub object store for conformance without network.
#[derive(Default)]
pub struct MemoryHub {
    inner: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl MemoryHub {
    /// Create an empty in-memory Hub object store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl HubPublishTransport for MemoryHub {
    fn put_object(&self, object_digest: &str, bytes: &[u8]) -> Result<(), HubPublishError> {
        validate_object_digest(object_digest)?;
        let actual = digest_sha256(bytes);
        if actual != object_digest {
            return Err(HubPublishError::new(
                HubPublishErrorCode::IntegrityFailure,
                "object bytes do not match declared digest",
            ));
        }

        let mut guard = self.inner.lock().map_err(|_| {
            HubPublishError::new(HubPublishErrorCode::Internal, "hub lock poisoned")
        })?;

        if let Some(existing) = guard.get(object_digest) {
            if existing.as_slice() == bytes {
                return Ok(());
            }
            return Err(HubPublishError::new(
                HubPublishErrorCode::IdempotencyConflict,
                "object digest was already stored with different bytes",
            ));
        }

        guard.insert(object_digest.to_owned(), bytes.to_vec());
        Ok(())
    }

    fn get_object(&self, object_digest: &str) -> Result<Vec<u8>, HubPublishError> {
        validate_object_digest(object_digest)?;
        let guard = self.inner.lock().map_err(|_| {
            HubPublishError::new(HubPublishErrorCode::Internal, "hub lock poisoned")
        })?;
        guard.get(object_digest).cloned().ok_or_else(|| {
            HubPublishError::new(
                HubPublishErrorCode::InvalidInput,
                "object digest is not present in the hub store",
            )
        })
    }

    fn object_exists(&self, object_digest: &str) -> Result<bool, HubPublishError> {
        validate_object_digest(object_digest)?;
        let guard = self.inner.lock().map_err(|_| {
            HubPublishError::new(HubPublishErrorCode::Internal, "hub lock poisoned")
        })?;
        Ok(guard.contains_key(object_digest))
    }
}

fn validate_object_digest(object_digest: &str) -> Result<(), HubPublishError> {
    if !object_digest.starts_with("sha256:") {
        return Err(HubPublishError::new(
            HubPublishErrorCode::InvalidInput,
            "object digest must be a sha256 content hash",
        ));
    }
    if object_digest.len() != 71
        || !object_digest[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(HubPublishError::new(
            HubPublishErrorCode::InvalidInput,
            "object digest must be sha256:<64 hex>",
        ));
    }
    Ok(())
}

fn digest_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{}", encode_hex(digest))
}

fn encode_hex(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_hub_stores_one_object() {
        let hub = MemoryHub::new();
        let bytes = b"graphforge-hub-publish-fixture";
        let digest = digest_sha256(bytes);

        assert!(!hub.object_exists(&digest).expect("exists"));
        hub.put_object(&digest, bytes).expect("put");
        assert!(hub.object_exists(&digest).expect("exists"));
        assert_eq!(hub.get_object(&digest).expect("get"), bytes);

        hub.put_object(&digest, bytes).expect("idempotent put");
    }

    #[test]
    fn idempotency_conflict_uses_gf_code() {
        let error = HubPublishError::new(
            HubPublishErrorCode::IdempotencyConflict,
            "operation identity was reused with a conflicting request commitment",
        );
        assert_eq!(error.gf_code(), GF_IDEMPOTENCY_CONFLICT);
        assert_eq!(error.wire_code(), "hub.publish.idempotency_conflict");
    }
}
