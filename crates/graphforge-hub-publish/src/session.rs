//! Control-plane idempotency and ref preconditions for the in-memory Hub.

use crate::{HubPublishError, HubPublishErrorCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::sync::Mutex;
use uuid::Uuid;

/// Stable publish receipt returned on successful publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishReceipt {
    /// Caller-stable operation identity.
    pub operation_uuid: Uuid,
    /// Canonical request commitment digest (`sha256:…`).
    pub request_commitment: String,
    /// Manifest validator digest advanced by this publication.
    pub manifest_validator: String,
}

/// In-memory control plane for idempotent publish and ref guards.
#[derive(Default)]
pub struct MemoryPublishSession {
    receipts: Mutex<BTreeMap<Uuid, StoredReceipt>>,
    refs: Mutex<BTreeMap<RefKey, String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct RefKey {
    owner: String,
    repository: String,
    ref_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredReceipt {
    commitment: [u8; 32],
    receipt: PublishReceipt,
}

impl MemoryPublishSession {
    /// Create an empty publish session registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful publication, keyed by `operation_uuid`.
    ///
    /// Retries with the same commitment return the original receipt. A different
    /// commitment under the same operation identity fails with idempotency conflict.
    pub fn record_publication(
        &self,
        receipt: PublishReceipt,
    ) -> Result<PublishReceipt, HubPublishError> {
        let commitment = parse_commitment(&receipt.request_commitment)?;
        let mut guard = self.receipts.lock().map_err(|_| {
            HubPublishError::new(HubPublishErrorCode::Internal, "hub lock poisoned")
        })?;
        if let Some(stored) = guard.get(&receipt.operation_uuid) {
            if stored.commitment == commitment {
                return Ok(stored.receipt.clone());
            }
            return Err(HubPublishError::new(
                HubPublishErrorCode::IdempotencyConflict,
                "operation identity was reused with a conflicting request commitment",
            ));
        }
        guard.insert(
            receipt.operation_uuid,
            StoredReceipt {
                commitment,
                receipt: receipt.clone(),
            },
        );
        Ok(receipt)
    }

    /// Advance one ref only when `expected_validator` matches the current value.
    ///
    /// `expected_validator: None` requires the ref to be absent (create). A mismatch
    /// fails with ref conflict and leaves state unchanged.
    pub fn advance_ref(
        &self,
        owner: &str,
        repository: &str,
        ref_name: &str,
        expected_validator: Option<&str>,
        new_validator: &str,
    ) -> Result<(), HubPublishError> {
        validate_validator(new_validator)?;
        if let Some(expected) = expected_validator {
            validate_validator(expected)?;
        }
        let key = RefKey {
            owner: owner.into(),
            repository: repository.into(),
            ref_name: ref_name.into(),
        };
        let mut guard = self.refs.lock().map_err(|_| {
            HubPublishError::new(HubPublishErrorCode::Internal, "hub lock poisoned")
        })?;
        match (guard.get(&key).map(String::as_str), expected_validator) {
            (None, None) => {
                guard.insert(key, new_validator.to_owned());
                Ok(())
            }
            (Some(current), Some(expected)) if current == expected => {
                guard.insert(key, new_validator.to_owned());
                Ok(())
            }
            _ => Err(HubPublishError::new(
                HubPublishErrorCode::RefConflict,
                "ref advance rejected by expected revision precondition",
            )),
        }
    }

    /// Read the current validator digest for one ref, if present.
    pub fn ref_validator(
        &self,
        owner: &str,
        repository: &str,
        ref_name: &str,
    ) -> Result<Option<String>, HubPublishError> {
        let key = RefKey {
            owner: owner.into(),
            repository: repository.into(),
            ref_name: ref_name.into(),
        };
        let guard = self.refs.lock().map_err(|_| {
            HubPublishError::new(HubPublishErrorCode::Internal, "hub lock poisoned")
        })?;
        Ok(guard.get(&key).cloned())
    }
}

fn parse_commitment(digest: &str) -> Result<[u8; 32], HubPublishError> {
    if !digest.starts_with("sha256:") || digest.len() != 71 {
        return Err(HubPublishError::new(
            HubPublishErrorCode::InvalidInput,
            "request commitment must be sha256:<64 hex>",
        ));
    }
    let mut out = [0_u8; 32];
    let hex_body = &digest.as_bytes()[7..];
    for (index, pair) in hex_body.chunks(2).enumerate().take(32) {
        let hex = std::str::from_utf8(pair).map_err(|_| {
            HubPublishError::new(
                HubPublishErrorCode::InvalidInput,
                "request commitment must be hex",
            )
        })?;
        out[index] = u8::from_str_radix(hex, 16).map_err(|_| {
            HubPublishError::new(
                HubPublishErrorCode::InvalidInput,
                "request commitment must be hex",
            )
        })?;
    }
    Ok(out)
}

fn validate_validator(digest: &str) -> Result<(), HubPublishError> {
    parse_commitment(digest).map(|_| ())
}

/// Derive a request commitment from canonical publish intent bytes.
#[must_use]
pub fn request_commitment(intent: &[u8]) -> String {
    format!("sha256:{}", hex32(Sha256::digest(intent)))
}

fn hex32(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    bytes
        .as_ref()
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
            out
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_publication_returns_original_receipt() {
        let session = MemoryPublishSession::new();
        let operation = Uuid::now_v7();
        let commitment = request_commitment(b"publish-intent/v1");
        let receipt = PublishReceipt {
            operation_uuid: operation,
            request_commitment: commitment.clone(),
            manifest_validator: commitment.clone(),
        };
        let first = session.record_publication(receipt.clone()).expect("first");
        let second = session.record_publication(receipt).expect("second");
        assert_eq!(first, second);
    }

    #[test]
    fn conflicting_commitment_uses_gf_idempotency_code() {
        let session = MemoryPublishSession::new();
        let operation = Uuid::now_v7();
        session
            .record_publication(PublishReceipt {
                operation_uuid: operation,
                request_commitment: request_commitment(b"a"),
                manifest_validator: request_commitment(b"manifest-a"),
            })
            .expect("first");
        let error = session
            .record_publication(PublishReceipt {
                operation_uuid: operation,
                request_commitment: request_commitment(b"b"),
                manifest_validator: request_commitment(b"manifest-b"),
            })
            .expect_err("conflict");
        assert_eq!(error.code, HubPublishErrorCode::IdempotencyConflict);
        assert_eq!(error.gf_code(), crate::GF_IDEMPOTENCY_CONFLICT);
    }

    #[test]
    fn ref_advance_requires_expected_revision() {
        let session = MemoryPublishSession::new();
        session
            .advance_ref("openalex", "demo", "main", None, &request_commitment(b"v1"))
            .expect("create");
        assert!(
            session
                .advance_ref("openalex", "demo", "main", None, &request_commitment(b"v2"))
                .is_err()
        );
        session
            .advance_ref(
                "openalex",
                "demo",
                "main",
                Some(&request_commitment(b"v1")),
                &request_commitment(b"v2"),
            )
            .expect("advance");
    }
}
