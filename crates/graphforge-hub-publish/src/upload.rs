//! Resumable data-plane uploads into an in-memory Hub object store.

use crate::{HubPublishError, HubPublishErrorCode, HubPublishTransport, MemoryHub};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::sync::Mutex;
use uuid::Uuid;

/// Opaque handle for one in-flight resumable upload.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UploadSessionId(Uuid);

impl UploadSessionId {
    /// Create a fresh upload session identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for UploadSessionId {
    fn default() -> Self {
        Self::new()
    }
}

struct PartialObject {
    object_digest: String,
    expected_length: u64,
    bytes: Vec<u8>,
}

/// Resumable upload coordinator backed by a [`MemoryHub`].
#[derive(Default)]
pub struct ResumableUploadHub {
    hub: MemoryHub,
    partial: Mutex<BTreeMap<UploadSessionId, PartialObject>>,
}

impl ResumableUploadHub {
    /// Create an empty resumable upload Hub.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Borrow the committed object store.
    #[must_use]
    pub fn objects(&self) -> &MemoryHub {
        &self.hub
    }

    /// Begin uploading `length` bytes that must hash to `object_digest`.
    pub fn begin_upload(
        &self,
        object_digest: &str,
        length: u64,
    ) -> Result<UploadSessionId, HubPublishError> {
        crate::validate_object_digest(object_digest)?;
        if length == 0 {
            return Err(HubPublishError::new(
                HubPublishErrorCode::InvalidInput,
                "upload length must be positive",
            ));
        }
        let session = UploadSessionId::new();
        let mut guard = self.partial.lock().map_err(|_| {
            HubPublishError::new(HubPublishErrorCode::Internal, "hub lock poisoned")
        })?;
        guard.insert(
            session.clone(),
            PartialObject {
                object_digest: object_digest.to_owned(),
                expected_length: length,
                bytes: Vec::new(),
            },
        );
        Ok(session)
    }

    /// Append `chunk` at `offset`. The offset must equal bytes already received.
    pub fn append(
        &self,
        session: &UploadSessionId,
        offset: u64,
        chunk: &[u8],
    ) -> Result<(), HubPublishError> {
        let mut guard = self.partial.lock().map_err(|_| {
            HubPublishError::new(HubPublishErrorCode::Internal, "hub lock poisoned")
        })?;
        let partial = guard.get_mut(session).ok_or_else(|| {
            HubPublishError::new(
                HubPublishErrorCode::InvalidInput,
                "upload session is not active",
            )
        })?;
        if u64::try_from(partial.bytes.len()).expect("usize fits in u64") != offset {
            return Err(HubPublishError::new(
                HubPublishErrorCode::InvalidInput,
                "upload offset does not match received length",
            ));
        }
        let next = offset
            .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                HubPublishError::new(HubPublishErrorCode::InvalidInput, "chunk length overflow")
            })?)
            .ok_or_else(|| {
                HubPublishError::new(
                    HubPublishErrorCode::InvalidInput,
                    "upload would exceed declared length",
                )
            })?;
        if next > partial.expected_length {
            return Err(HubPublishError::new(
                HubPublishErrorCode::InvalidInput,
                "upload would exceed declared length",
            ));
        }
        partial.bytes.extend_from_slice(chunk);
        Ok(())
    }

    /// Verify digest and length, then commit bytes to the object store.
    pub fn finalize(&self, session: &UploadSessionId) -> Result<(), HubPublishError> {
        let partial = {
            let mut guard = self.partial.lock().map_err(|_| {
                HubPublishError::new(HubPublishErrorCode::Internal, "hub lock poisoned")
            })?;
            guard.remove(session).ok_or_else(|| {
                HubPublishError::new(
                    HubPublishErrorCode::InvalidInput,
                    "upload session is not active",
                )
            })?
        };
        if u64::try_from(partial.bytes.len()).expect("usize fits in u64") != partial.expected_length
        {
            return Err(HubPublishError::new(
                HubPublishErrorCode::IntegrityFailure,
                "upload length does not match declared length",
            ));
        }
        self.hub.put_object(&partial.object_digest, &partial.bytes)
    }
}

pub(crate) fn digest_sha256(bytes: &[u8]) -> String {
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
    fn resumable_upload_commits_after_resume() {
        let hub = ResumableUploadHub::new();
        let payload = b"graphforge-resumable-upload-fixture-bytes";
        let digest = digest_sha256(payload);
        let session = hub
            .begin_upload(&digest, payload.len() as u64)
            .expect("begin");
        hub.append(&session, 0, &payload[..10]).expect("first");
        hub.append(&session, 10, &payload[10..]).expect("second");
        hub.finalize(&session).expect("finalize");
        assert_eq!(
            hub.objects().get_object(&digest).expect("get"),
            payload.as_slice()
        );
    }

    #[test]
    fn corrupt_finalize_leaves_object_absent() {
        let hub = ResumableUploadHub::new();
        let payload = b"expected-bytes";
        let digest = digest_sha256(b"wrong-bytes");
        let session = hub
            .begin_upload(&digest, payload.len() as u64)
            .expect("begin");
        hub.append(&session, 0, payload).expect("append");
        assert!(matches!(
            hub.finalize(&session).expect_err("finalize"),
            HubPublishError {
                code: HubPublishErrorCode::IntegrityFailure,
                ..
            }
        ));
        assert!(!hub.objects().object_exists(&digest).expect("exists"));
    }
}
