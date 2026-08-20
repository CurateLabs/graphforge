//! Optional OCI Distribution / ORAS-compatible transport for portable-v2 packages.
//!
//! GraphForge package digests remain authoritative for package identity. The OCI
//! manifest digest is transport/distribution identity only. Tags are mutable
//! references and never substitute for a recorded digest.
#![allow(missing_docs)]

use crate::{
    PortableV2Error, PortableV2ErrorCode, PortableV2Limits, PortableV2Mode, PortableV2PackageClass,
    PortableV2Report, verify_portable_v2,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Artifact type for a GraphForge portable-v2 package in an OCI registry.
pub const OCI_ARTIFACT_TYPE: &str = "application/vnd.graphforge.project.v2";
/// Config media type carrying GraphForge semantic identity.
pub const OCI_CONFIG_MEDIA_TYPE: &str = "application/vnd.graphforge.project.v2.config+json";
/// Layer media type for the deterministic `.gfpb` bundle bytes.
pub const OCI_LAYER_MEDIA_TYPE: &str = "application/vnd.graphforge.project.v2+tar";
/// OCI image manifest media type.
pub const OCI_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

const CONFIG_CONTRACT: &str = "graphforge-portable-v2-oci-config/1";
const SIGNATURE_CONTRACT: &str = "graphforge-portable-v2-oci-signature/1";
/// Artifact type for GraphForge OCI signature/provenance attachments.
pub const OCI_SIGNATURE_ARTIFACT_TYPE: &str = "application/vnd.graphforge.project.v2.signature";
/// Config media type for signature attachments.
pub const OCI_SIGNATURE_CONFIG_MEDIA_TYPE: &str =
    "application/vnd.graphforge.project.v2.signature+json";

/// Digest-pinned OCI reference returned after a successful publish.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2OciReference {
    /// Registry host (no credentials).
    pub registry: String,
    /// Repository path inside the registry.
    pub repository: String,
    /// OCI manifest digest (`sha256:…`).
    pub oci_manifest_digest: String,
    /// Authoritative GraphForge package digest (`sha256:…`).
    pub package_digest: String,
    /// Package class carried in the config blob.
    pub package_class: PortableV2PackageClass,
    /// Optional mutable tag that was also written; never used as identity.
    pub tag: Option<String>,
    /// Bytes uploaded across config + layer + manifest.
    pub bytes_transferred: u64,
    /// Blob count excluding the manifest itself.
    pub blob_count: u64,
}

/// Sanitized receipt after a successful pull + local verification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2OciPullReceipt {
    /// Digest-pinned reference that was resolved.
    pub reference: PortableV2OciReference,
    /// Destination path written after verification.
    pub destination: PathBuf,
    /// Local verifier report for the pulled package.
    pub report: PortableV2Report,
    /// Signature evaluation outcome (distinct from integrity).
    pub signature_state: PortableV2OciSignatureState,
}

/// Optional authenticity policy for referrer/signature attachments.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PortableV2OciAuthenticityPolicy {
    /// When set, unsigned content is integrity-valid but authenticity-absent.
    pub require_named_signer: Option<String>,
    /// Caller-owned verification key material. Never logged or persisted.
    pub verification_key: Option<Vec<u8>>,
}

/// Explicit signature states. Never reused as integrity outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2OciSignatureState {
    /// Signature present, signer matches policy, MAC verifies.
    Valid,
    /// Signature present but MAC verification failed.
    Invalid,
    /// No signature attachment was observed.
    Absent,
    /// Signature present for a different signer than the policy requires.
    PolicyMismatched,
}

/// Progress phases for sanitized observability (no credentials or bodies).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2OciPhase {
    VerifyLocal,
    UploadBlob,
    UploadManifest,
    AttachSignature,
    Observe,
    DownloadManifest,
    DownloadBlob,
    VerifyPulled,
    EvaluateAuthenticity,
}

/// Sanitized progress event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2OciProgress {
    /// Current phase.
    pub phase: PortableV2OciPhase,
    /// Cumulative bytes moved in this operation.
    pub bytes_transferred: u64,
    /// Optional blob/manifest digest under consideration.
    pub digest: Option<String>,
}

/// Caller-owned material used to attach an OCI signature referrer on publish.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableV2OciSignatureMaterial {
    /// Signer identity recorded in the attachment.
    pub signer: String,
    /// Key identifier (not secret material).
    pub key_id: String,
    /// Secret key bytes used to compute the MAC. Never logged.
    pub secret: Vec<u8>,
}

/// Publish request for a locally verified portable-v2 bundle.
#[derive(Clone, Debug)]
pub struct PortableV2OciPublishRequest<'a> {
    /// Verified local package path (bundle or expanded).
    pub package_path: &'a Path,
    /// Registry host, e.g. `127.0.0.1:5000` or `ghcr.io`.
    pub registry: &'a str,
    /// Repository name, e.g. `curatelabs/graphforge-packages`.
    pub repository: &'a str,
    /// Optional mutable tag annotation; pull-by-digest remains authoritative.
    pub tag: Option<&'a str>,
    /// Verifier limits applied before upload.
    pub limits: PortableV2Limits,
    /// Optional authenticity policy (signature verification is explicit).
    pub authenticity: PortableV2OciAuthenticityPolicy,
    /// Optional signature to attach as an OCI referrer after publish.
    pub signature: Option<PortableV2OciSignatureMaterial>,
    /// Optional bearer token or basic `user:pass` supplied by the caller.
    /// Never persisted or logged by GraphForge.
    pub credential: Option<&'a str>,
}

/// Pull request resolved by OCI manifest digest (or tag that is immediately
/// re-resolved to a digest before materialization).
#[derive(Clone, Debug)]
pub struct PortableV2OciPullRequest<'a> {
    /// Registry host without scheme or credentials.
    pub registry: &'a str,
    /// Repository path.
    pub repository: &'a str,
    /// Prefer an OCI manifest digest (`sha256:…`). Tags are allowed only as a
    /// mutable lookup that must be re-recorded as a digest.
    pub reference: &'a str,
    /// When set with a tag reference, disagreeing resolved digests fail closed.
    pub expected_oci_digest: Option<&'a str>,
    /// Destination path for the verified local package.
    pub destination: &'a Path,
    /// Verifier limits.
    pub limits: PortableV2Limits,
    /// Optional authenticity policy.
    pub authenticity: PortableV2OciAuthenticityPolicy,
    /// Optional credential (never logged).
    pub credential: Option<&'a str>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct OciConfig {
    contract: String,
    package_digest: String,
    package_class: String,
    representation: String,
    transport_digest: Option<String>,
    layer_media_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OciDescriptor {
    #[serde(rename = "mediaType")]
    media_type: String,
    digest: String,
    size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OciManifest {
    #[serde(rename = "schemaVersion")]
    schema_version: u32,
    #[serde(rename = "mediaType")]
    media_type: String,
    #[serde(rename = "artifactType")]
    artifact_type: String,
    config: OciDescriptor,
    layers: Vec<OciDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    subject: Option<OciDescriptor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct OciSignaturePayload {
    contract: String,
    signer: String,
    key_id: String,
    subject_manifest_digest: String,
    package_digest: String,
    mac: String,
}

/// Backend used by publish/pull. Production uses HTTP; tests use memory.
pub trait PortableV2OciRegistry: Send + Sync {
    fn put_blob(&self, repository: &str, digest: &str, bytes: &[u8])
    -> Result<(), PortableV2Error>;
    fn get_blob(&self, repository: &str, digest: &str) -> Result<Vec<u8>, PortableV2Error>;
    fn blob_exists(&self, repository: &str, digest: &str) -> Result<bool, PortableV2Error>;
    fn put_manifest(
        &self,
        repository: &str,
        reference: &str,
        media_type: &str,
        bytes: &[u8],
    ) -> Result<String, PortableV2Error>;
    fn get_manifest(
        &self,
        repository: &str,
        reference: &str,
    ) -> Result<(String, Vec<u8>), PortableV2Error>;
    /// Attach a referrer manifest for `subject_digest`. Default: unsupported.
    fn put_referrer_manifest(
        &self,
        _repository: &str,
        _subject_digest: &str,
        _media_type: &str,
        _bytes: &[u8],
    ) -> Result<String, PortableV2Error> {
        Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
            "registry does not support referrer attachments",
        ))
    }
    /// List referrer manifests for a subject. Default: empty.
    fn list_referrers(
        &self,
        _repository: &str,
        _subject_digest: &str,
    ) -> Result<Vec<Vec<u8>>, PortableV2Error> {
        Ok(Vec::new())
    }
}

/// In-process registry for conformance without network or Docker.
#[derive(Default)]
pub struct MemoryOciRegistry {
    inner: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    blobs: BTreeMap<(String, String), Vec<u8>>,
    manifests: BTreeMap<(String, String), (String, Vec<u8>)>,
    tags: BTreeMap<(String, String), String>,
    referrers: BTreeMap<(String, String), Vec<Vec<u8>>>,
}

impl PortableV2OciRegistry for MemoryOciRegistry {
    fn put_blob(
        &self,
        repository: &str,
        digest: &str,
        bytes: &[u8],
    ) -> Result<(), PortableV2Error> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        let actual = digest_sha256(bytes);
        if actual != digest {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::DigestMismatch,
                "blob bytes do not match declared digest",
            ));
        }
        self.inner
            .lock()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "registry lock poisoned"))?
            .blobs
            .insert((repository.to_owned(), digest.to_owned()), bytes.to_vec());
        Ok(())
    }

    fn get_blob(&self, repository: &str, digest: &str) -> Result<Vec<u8>, PortableV2Error> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        self.inner
            .lock()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "registry lock poisoned"))?
            .blobs
            .get(&(repository.to_owned(), digest.to_owned()))
            .cloned()
            .ok_or_else(|| {
                PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "blob not found")
            })
    }

    fn blob_exists(&self, repository: &str, digest: &str) -> Result<bool, PortableV2Error> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        Ok(self
            .inner
            .lock()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "registry lock poisoned"))?
            .blobs
            .contains_key(&(repository.to_owned(), digest.to_owned())))
    }

    fn put_manifest(
        &self,
        repository: &str,
        reference: &str,
        _media_type: &str,
        bytes: &[u8],
    ) -> Result<String, PortableV2Error> {
        validate_repository(repository)?;
        validate_reference(reference)?;
        let digest = digest_sha256(bytes);
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "registry lock poisoned"))?;
        guard.manifests.insert(
            (repository.to_owned(), digest.clone()),
            (OCI_MANIFEST_MEDIA_TYPE.to_owned(), bytes.to_vec()),
        );
        if !reference.starts_with("sha256:") {
            guard.tags.insert(
                (repository.to_owned(), reference.to_owned()),
                digest.clone(),
            );
        }
        Ok(digest)
    }

    fn get_manifest(
        &self,
        repository: &str,
        reference: &str,
    ) -> Result<(String, Vec<u8>), PortableV2Error> {
        validate_repository(repository)?;
        validate_reference(reference)?;
        let guard = self
            .inner
            .lock()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "registry lock poisoned"))?;
        let digest = if reference.starts_with("sha256:") {
            reference.to_owned()
        } else {
            guard
                .tags
                .get(&(repository.to_owned(), reference.to_owned()))
                .cloned()
                .ok_or_else(|| {
                    PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "tag not found")
                })?
        };
        guard
            .manifests
            .get(&(repository.to_owned(), digest))
            .cloned()
            .ok_or_else(|| {
                PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "manifest not found")
            })
    }

    fn put_referrer_manifest(
        &self,
        repository: &str,
        subject_digest: &str,
        media_type: &str,
        bytes: &[u8],
    ) -> Result<String, PortableV2Error> {
        validate_repository(repository)?;
        validate_digest(subject_digest)?;
        let digest = self.put_manifest(repository, &digest_sha256(bytes), media_type, bytes)?;
        self.inner
            .lock()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "registry lock poisoned"))?
            .referrers
            .entry((repository.to_owned(), subject_digest.to_owned()))
            .or_default()
            .push(bytes.to_vec());
        Ok(digest)
    }

    fn list_referrers(
        &self,
        repository: &str,
        subject_digest: &str,
    ) -> Result<Vec<Vec<u8>>, PortableV2Error> {
        validate_repository(repository)?;
        validate_digest(subject_digest)?;
        Ok(self
            .inner
            .lock()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "registry lock poisoned"))?
            .referrers
            .get(&(repository.to_owned(), subject_digest.to_owned()))
            .cloned()
            .unwrap_or_default())
    }
}

impl MemoryOciRegistry {
    /// Resolve a mutable tag to the current digest without claiming package identity.
    pub fn resolve_tag(&self, repository: &str, tag: &str) -> Result<String, PortableV2Error> {
        validate_repository(repository)?;
        validate_reference(tag)?;
        self.inner
            .lock()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "registry lock poisoned"))?
            .tags
            .get(&(repository.to_owned(), tag.to_owned()))
            .cloned()
            .ok_or_else(|| {
                PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "tag not found")
            })
    }

    #[cfg(test)]
    fn corrupt_first_blob(&self) {
        let mut guard = self.inner.lock().unwrap();
        if let Some(bytes) = guard.blobs.values_mut().next() {
            if !bytes.is_empty() {
                bytes[0] ^= 0xff;
            }
        }
    }
}

/// HTTP OCI Distribution client (ureq). Credentials are only sent as headers.
pub struct HttpOciRegistry {
    base_url: String,
    credential: Option<String>,
    agent: ureq::Agent,
}

impl HttpOciRegistry {
    /// Construct a client for `host[:port]`. Defaults to HTTPS unless `insecure_http`.
    pub fn new(
        registry: &str,
        credential: Option<&str>,
        insecure_http: bool,
    ) -> Result<Self, PortableV2Error> {
        let host = registry.trim().trim_end_matches('/');
        if host.is_empty() || host.contains("://") || host.contains('@') {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::InvalidPath,
                "registry must be a host[:port] without scheme or credentials",
            ));
        }
        if let Some(cred) = credential
            && (cred.contains('\n') || cred.contains('\r'))
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::InvalidPath,
                "credential contains control characters",
            ));
        }
        let scheme = if insecure_http { "http" } else { "https" };
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_mins(1)))
            .http_status_as_error(false)
            .max_redirects(0)
            .build();
        Ok(Self {
            base_url: format!("{scheme}://{host}"),
            credential: credential.map(str::to_owned),
            agent: ureq::Agent::new_with_config(config),
        })
    }

    fn auth_header(&self) -> Option<String> {
        match &self.credential {
            Some(token) if token.contains(':') => {
                Some(format!("Basic {}", encode_base64(token.as_bytes())))
            }
            Some(token) => Some(format!("Bearer {token}")),
            None => None,
        }
    }

    fn map_http(_err: ureq::Error) -> PortableV2Error {
        PortableV2Error::new(PortableV2ErrorCode::Io, "registry transport failed")
    }
}

impl PortableV2OciRegistry for HttpOciRegistry {
    fn put_blob(
        &self,
        repository: &str,
        digest: &str,
        bytes: &[u8],
    ) -> Result<(), PortableV2Error> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        if self.blob_exists(repository, digest)? {
            return Ok(());
        }
        let mut start = self
            .agent
            .post(format!("{}/v2/{repository}/blobs/uploads/", self.base_url));
        if let Some(auth) = self.auth_header() {
            start = start.header("Authorization", auth);
        }
        let start_resp = start.send(&[] as &[u8]).map_err(Self::map_http)?;
        if !start_resp.status().is_success() && start_resp.status().as_u16() != 202 {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "blob upload session rejected",
            ));
        }
        let location = start_resp
            .headers()
            .get("Location")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                PortableV2Error::new(PortableV2ErrorCode::Io, "upload session missing Location")
            })?
            .to_owned();
        let upload_url = if location.starts_with("http://") || location.starts_with("https://") {
            location
        } else {
            format!("{}{location}", self.base_url)
        };
        let sep = if upload_url.contains('?') { '&' } else { '?' };
        let put_url = format!("{upload_url}{sep}digest={digest}");
        let mut put = self.agent.put(put_url);
        if let Some(auth) = self.auth_header() {
            put = put.header("Authorization", auth);
        }
        let put_resp = put
            .header("Content-Type", "application/octet-stream")
            .send(bytes)
            .map_err(Self::map_http)?;
        let status = put_resp.status().as_u16();
        if !(put_resp.status().is_success() || status == 201 || status == 202) {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "blob upload rejected",
            ));
        }
        Ok(())
    }

    fn get_blob(&self, repository: &str, digest: &str) -> Result<Vec<u8>, PortableV2Error> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        let mut req = self
            .agent
            .get(format!("{}/v2/{repository}/blobs/{digest}", self.base_url));
        if let Some(auth) = self.auth_header() {
            req = req.header("Authorization", auth);
        }
        let mut resp = req.call().map_err(Self::map_http)?;
        if !resp.status().is_success() {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::InvalidStructure,
                "blob not found",
            ));
        }
        let body = resp
            .body_mut()
            .read_to_vec()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "blob download failed"))?;
        let actual = digest_sha256(&body);
        if actual != digest {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::DigestMismatch,
                "downloaded blob digest mismatch",
            ));
        }
        Ok(body)
    }

    fn blob_exists(&self, repository: &str, digest: &str) -> Result<bool, PortableV2Error> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        let mut req = self
            .agent
            .head(format!("{}/v2/{repository}/blobs/{digest}", self.base_url));
        if let Some(auth) = self.auth_header() {
            req = req.header("Authorization", auth);
        }
        match req.call() {
            Ok(resp) => Ok(resp.status().is_success()),
            Err(ureq::Error::StatusCode(404)) => Ok(false),
            Err(_) => Err(PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "blob existence check failed",
            )),
        }
    }

    fn put_manifest(
        &self,
        repository: &str,
        reference: &str,
        media_type: &str,
        bytes: &[u8],
    ) -> Result<String, PortableV2Error> {
        validate_repository(repository)?;
        validate_reference(reference)?;
        let mut req = self.agent.put(format!(
            "{}/v2/{repository}/manifests/{reference}",
            self.base_url
        ));
        if let Some(auth) = self.auth_header() {
            req = req.header("Authorization", auth);
        }
        let resp = req
            .header("Content-Type", media_type)
            .send(bytes)
            .map_err(Self::map_http)?;
        if !resp.status().is_success() && resp.status().as_u16() != 201 {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "manifest upload rejected",
            ));
        }
        Ok(digest_sha256(bytes))
    }

    fn get_manifest(
        &self,
        repository: &str,
        reference: &str,
    ) -> Result<(String, Vec<u8>), PortableV2Error> {
        validate_repository(repository)?;
        validate_reference(reference)?;
        let mut req = self.agent.get(format!(
            "{}/v2/{repository}/manifests/{reference}",
            self.base_url
        ));
        if let Some(auth) = self.auth_header() {
            req = req.header("Authorization", auth);
        }
        let mut resp = req
            .header(
                "Accept",
                format!("{OCI_MANIFEST_MEDIA_TYPE}, application/vnd.oci.image.manifest.v1+json"),
            )
            .call()
            .map_err(Self::map_http)?;
        if !resp.status().is_success() {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::InvalidStructure,
                "manifest not found",
            ));
        }
        let media = resp
            .headers()
            .get("Content-Type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or(OCI_MANIFEST_MEDIA_TYPE)
            .to_owned();
        let body = resp.body_mut().read_to_vec().map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "manifest download failed")
        })?;
        Ok((media, body))
    }
}

fn encode_base64(raw: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(raw.len().div_ceil(3) * 4);
    let mut i = 0;
    while i < raw.len() {
        let b0 = raw[i];
        let b1 = if i + 1 < raw.len() { raw[i + 1] } else { 0 };
        let b2 = if i + 2 < raw.len() { raw[i + 2] } else { 0 };
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 63) as usize] as char);
        out.push(if i + 1 < raw.len() {
            ALPHABET[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if i + 2 < raw.len() {
            ALPHABET[(n & 63) as usize] as char
        } else {
            '='
        });
        i += 3;
    }
    out
}

/// Verify locally, map to OCI descriptors, and publish by digest (optional tag).
pub fn publish_portable_v2_oci(
    registry: &dyn PortableV2OciRegistry,
    request: &PortableV2OciPublishRequest<'_>,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableV2OciReference, PortableV2Error> {
    publish_portable_v2_oci_with_progress(registry, request, cancelled, |_| {})
}

/// Publish with sanitized progress callbacks.
#[expect(
    clippy::too_many_lines,
    reason = "keeps verify/upload/observe and signature attach in one fail-closed publish path"
)]
pub fn publish_portable_v2_oci_with_progress(
    registry: &dyn PortableV2OciRegistry,
    request: &PortableV2OciPublishRequest<'_>,
    cancelled: Option<&AtomicBool>,
    mut progress: impl FnMut(PortableV2OciProgress),
) -> Result<PortableV2OciReference, PortableV2Error> {
    check_cancelled(cancelled)?;
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::VerifyLocal,
        bytes_transferred: 0,
        digest: None,
    });
    let report = verify_portable_v2(
        request.package_path,
        PortableV2Mode::Full,
        request.limits,
        cancelled,
    )?;
    if report.integrity != crate::PortableV2Integrity::Verified {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "package integrity is not verified",
        ));
    }

    let layer_bytes = read_package_bytes(request.package_path, &report)?;
    check_cancelled(cancelled)?;
    let layer_digest = digest_sha256(&layer_bytes);
    let config = OciConfig {
        contract: CONFIG_CONTRACT.to_owned(),
        package_digest: report.package_digest.clone(),
        package_class: package_class_str(report.package_class).to_owned(),
        representation: match report.representation {
            crate::PortableV2Representation::Bundle => "bundle".to_owned(),
            crate::PortableV2Representation::Expanded => "expanded".to_owned(),
        },
        transport_digest: report.transport_digest.clone(),
        layer_media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
    };
    let config_bytes = serde_json::to_vec(&config).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "failed to encode OCI config")
    })?;
    let config_digest = digest_sha256(&config_bytes);
    let mut bytes_transferred = 0_u64;

    if !registry.blob_exists(request.repository, &config_digest)? {
        registry.put_blob(request.repository, &config_digest, &config_bytes)?;
        bytes_transferred += config_bytes.len() as u64;
        progress(PortableV2OciProgress {
            phase: PortableV2OciPhase::UploadBlob,
            bytes_transferred,
            digest: Some(config_digest.clone()),
        });
    }
    check_cancelled(cancelled)?;
    if !registry.blob_exists(request.repository, &layer_digest)? {
        registry.put_blob(request.repository, &layer_digest, &layer_bytes)?;
        bytes_transferred += layer_bytes.len() as u64;
        progress(PortableV2OciProgress {
            phase: PortableV2OciPhase::UploadBlob,
            bytes_transferred,
            digest: Some(layer_digest.clone()),
        });
    }
    check_cancelled(cancelled)?;

    let manifest = OciManifest {
        schema_version: 2,
        media_type: OCI_MANIFEST_MEDIA_TYPE.to_owned(),
        artifact_type: OCI_ARTIFACT_TYPE.to_owned(),
        config: OciDescriptor {
            media_type: OCI_CONFIG_MEDIA_TYPE.to_owned(),
            digest: config_digest,
            size: config_bytes.len() as u64,
        },
        layers: vec![OciDescriptor {
            media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
            digest: layer_digest,
            size: layer_bytes.len() as u64,
        }],
        subject: None,
    };
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "failed to encode OCI manifest")
    })?;
    let digest_ref = digest_sha256(&manifest_bytes);
    let published = registry.put_manifest(
        request.repository,
        &digest_ref,
        OCI_MANIFEST_MEDIA_TYPE,
        &manifest_bytes,
    )?;
    if published != digest_ref {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "registry returned a different manifest digest",
        ));
    }
    bytes_transferred += manifest_bytes.len() as u64;
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::UploadManifest,
        bytes_transferred,
        digest: Some(digest_ref.clone()),
    });
    if let Some(tag) = request.tag {
        validate_reference(tag)?;
        let _ = registry.put_manifest(
            request.repository,
            tag,
            OCI_MANIFEST_MEDIA_TYPE,
            &manifest_bytes,
        )?;
    }

    if let Some(material) = &request.signature {
        attach_signature_referrer(
            registry,
            request.repository,
            &digest_ref,
            &report.package_digest,
            material,
            &mut bytes_transferred,
            &mut progress,
        )?;
        progress(PortableV2OciProgress {
            phase: PortableV2OciPhase::AttachSignature,
            bytes_transferred,
            digest: Some(digest_ref.clone()),
        });
    }

    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::Observe,
        bytes_transferred,
        digest: Some(digest_ref.clone()),
    });
    let (media, observed) = registry.get_manifest(request.repository, &digest_ref)?;
    if media != OCI_MANIFEST_MEDIA_TYPE && !media.starts_with(OCI_MANIFEST_MEDIA_TYPE) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "incompatible manifest media type",
        ));
    }
    if digest_sha256(&observed) != digest_ref {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "fresh registry observation does not match published digest",
        ));
    }

    Ok(PortableV2OciReference {
        registry: request.registry.to_owned(),
        repository: request.repository.to_owned(),
        oci_manifest_digest: digest_ref,
        package_digest: report.package_digest,
        package_class: report.package_class,
        tag: request.tag.map(str::to_owned),
        bytes_transferred,
        blob_count: 2,
    })
}

/// Pull by digest (or tag→digest), verify fully, then materialize to destination.
pub fn pull_portable_v2_oci(
    registry: &dyn PortableV2OciRegistry,
    request: &PortableV2OciPullRequest<'_>,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableV2OciPullReceipt, PortableV2Error> {
    pull_portable_v2_oci_with_progress(registry, request, cancelled, |_| {})
}

/// Pull with sanitized progress callbacks.
#[expect(
    clippy::too_many_lines,
    reason = "keeps download/verify/authenticity evaluation in one fail-closed pull path"
)]
pub fn pull_portable_v2_oci_with_progress(
    registry: &dyn PortableV2OciRegistry,
    request: &PortableV2OciPullRequest<'_>,
    cancelled: Option<&AtomicBool>,
    mut progress: impl FnMut(PortableV2OciProgress),
) -> Result<PortableV2OciPullReceipt, PortableV2Error> {
    check_cancelled(cancelled)?;
    validate_repository(request.repository)?;
    validate_reference(request.reference)?;
    if let Some(expected) = request.expected_oci_digest {
        validate_digest(expected)?;
    }

    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::DownloadManifest,
        bytes_transferred: 0,
        digest: None,
    });
    let (_media, manifest_bytes) = registry.get_manifest(request.repository, request.reference)?;
    let oci_manifest_digest = digest_sha256(&manifest_bytes);
    let mut bytes_transferred = manifest_bytes.len() as u64;
    if request.reference.starts_with("sha256:") && request.reference != oci_manifest_digest {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "requested digest does not match downloaded manifest",
        ));
    }
    if let Some(expected) = request.expected_oci_digest
        && expected != oci_manifest_digest
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "tag or reference disagrees with expected OCI digest",
        ));
    }
    let manifest: OciManifest = serde_json::from_slice(&manifest_bytes).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "manifest is not valid OCI JSON",
        )
    })?;
    if manifest.artifact_type != OCI_ARTIFACT_TYPE {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "unsupported OCI artifact type",
        ));
    }
    if manifest.layers.len() != 1 {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "portable-v2 OCI artifacts require exactly one layer",
        ));
    }
    if manifest.layers[0].media_type != OCI_LAYER_MEDIA_TYPE {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "incompatible layer media type",
        ));
    }
    if manifest.config.media_type != OCI_CONFIG_MEDIA_TYPE {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "incompatible config media type",
        ));
    }

    check_cancelled(cancelled)?;
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::DownloadBlob,
        bytes_transferred,
        digest: Some(manifest.config.digest.clone()),
    });
    let config_bytes = registry.get_blob(request.repository, &manifest.config.digest)?;
    bytes_transferred += config_bytes.len() as u64;
    if digest_sha256(&config_bytes) != manifest.config.digest {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "config blob digest mismatch",
        ));
    }
    let config: OciConfig = serde_json::from_slice(&config_bytes).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "config blob is not valid GraphForge OCI config",
        )
    })?;
    if config.contract != CONFIG_CONTRACT {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
            "unsupported OCI config contract",
        ));
    }

    check_cancelled(cancelled)?;
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::DownloadBlob,
        bytes_transferred,
        digest: Some(manifest.layers[0].digest.clone()),
    });
    let layer_bytes = registry.get_blob(request.repository, &manifest.layers[0].digest)?;
    bytes_transferred += layer_bytes.len() as u64;
    if digest_sha256(&layer_bytes) != manifest.layers[0].digest {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "layer blob digest mismatch",
        ));
    }

    let parent = request.destination.parent().ok_or_else(|| {
        PortableV2Error::new(
            PortableV2ErrorCode::InvalidPath,
            "destination has no parent",
        )
    })?;
    fs::create_dir_all(parent).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "failed to create destination parent",
        )
    })?;
    let stage = parent.join(format!(
        ".{}.oci.partial",
        request
            .destination
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("package")
    ));
    if let Err(error) = write_exclusive(&stage, &layer_bytes) {
        let _ = fs::remove_file(&stage);
        return Err(error);
    }
    if check_cancelled(cancelled).is_err() {
        let _ = fs::remove_file(&stage);
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Cancelled,
            "portable OCI pull cancelled",
        ));
    }
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::VerifyPulled,
        bytes_transferred,
        digest: Some(oci_manifest_digest.clone()),
    });
    let report = match verify_portable_v2(&stage, PortableV2Mode::Full, request.limits, cancelled) {
        Ok(report) => report,
        Err(error) => {
            let _ = fs::remove_file(&stage);
            return Err(error);
        }
    };
    if report.package_digest != config.package_digest {
        let _ = fs::remove_file(&stage);
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "pulled package digest does not match OCI config",
        ));
    }

    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::EvaluateAuthenticity,
        bytes_transferred,
        digest: Some(oci_manifest_digest.clone()),
    });
    let signature_state = evaluate_signature_state(
        registry,
        request.repository,
        &oci_manifest_digest,
        &report.package_digest,
        &request.authenticity,
    )?;
    if request.authenticity.require_named_signer.is_some()
        && signature_state != PortableV2OciSignatureState::Valid
    {
        let _ = fs::remove_file(&stage);
        return Err(authenticity_error(signature_state));
    }

    if request.destination.exists() {
        let _ = fs::remove_file(&stage);
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidPath,
            "destination already exists",
        ));
    }
    fs::rename(&stage, request.destination).map_err(|_| {
        let _ = fs::remove_file(&stage);
        PortableV2Error::new(PortableV2ErrorCode::Io, "failed to publish pulled package")
    })?;

    let tag = if request.reference.starts_with("sha256:") {
        None
    } else {
        Some(request.reference.to_owned())
    };
    Ok(PortableV2OciPullReceipt {
        reference: PortableV2OciReference {
            registry: request.registry.to_owned(),
            repository: request.repository.to_owned(),
            oci_manifest_digest,
            package_digest: report.package_digest.clone(),
            package_class: report.package_class,
            tag,
            bytes_transferred,
            blob_count: 2,
        },
        destination: request.destination.to_path_buf(),
        report,
        signature_state,
    })
}

/// Inspect signature referrers without mutating local packages.
pub fn evaluate_portable_v2_oci_signature_state(
    registry: &dyn PortableV2OciRegistry,
    repository: &str,
    subject_manifest_digest: &str,
    package_digest: &str,
    policy: &PortableV2OciAuthenticityPolicy,
) -> Result<PortableV2OciSignatureState, PortableV2Error> {
    evaluate_signature_state(
        registry,
        repository,
        subject_manifest_digest,
        package_digest,
        policy,
    )
}

fn attach_signature_referrer(
    registry: &dyn PortableV2OciRegistry,
    repository: &str,
    subject_digest: &str,
    package_digest: &str,
    material: &PortableV2OciSignatureMaterial,
    bytes_transferred: &mut u64,
    progress: &mut impl FnMut(PortableV2OciProgress),
) -> Result<(), PortableV2Error> {
    if material.signer.is_empty() || material.key_id.is_empty() || material.secret.is_empty() {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidPath,
            "signature material is incomplete",
        ));
    }
    let payload = OciSignaturePayload {
        contract: SIGNATURE_CONTRACT.to_owned(),
        signer: material.signer.clone(),
        key_id: material.key_id.clone(),
        subject_manifest_digest: subject_digest.to_owned(),
        package_digest: package_digest.to_owned(),
        mac: signature_mac(
            &material.secret,
            &material.signer,
            subject_digest,
            package_digest,
        ),
    };
    let payload_bytes = serde_json::to_vec(&payload).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "failed to encode signature payload",
        )
    })?;
    let payload_digest = digest_sha256(&payload_bytes);
    registry.put_blob(repository, &payload_digest, &payload_bytes)?;
    *bytes_transferred += payload_bytes.len() as u64;
    progress(PortableV2OciProgress {
        phase: PortableV2OciPhase::UploadBlob,
        bytes_transferred: *bytes_transferred,
        digest: Some(payload_digest.clone()),
    });
    let empty_config = b"{}";
    let config_digest = digest_sha256(empty_config);
    if !registry.blob_exists(repository, &config_digest)? {
        registry.put_blob(repository, &config_digest, empty_config)?;
        *bytes_transferred += empty_config.len() as u64;
    }
    let referrer = OciManifest {
        schema_version: 2,
        media_type: OCI_MANIFEST_MEDIA_TYPE.to_owned(),
        artifact_type: OCI_SIGNATURE_ARTIFACT_TYPE.to_owned(),
        config: OciDescriptor {
            media_type: OCI_SIGNATURE_CONFIG_MEDIA_TYPE.to_owned(),
            digest: config_digest,
            size: empty_config.len() as u64,
        },
        layers: vec![OciDescriptor {
            media_type: OCI_SIGNATURE_CONFIG_MEDIA_TYPE.to_owned(),
            digest: payload_digest,
            size: payload_bytes.len() as u64,
        }],
        subject: Some(OciDescriptor {
            media_type: OCI_MANIFEST_MEDIA_TYPE.to_owned(),
            digest: subject_digest.to_owned(),
            size: 0,
        }),
    };
    let referrer_bytes = serde_json::to_vec(&referrer).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "failed to encode signature referrer",
        )
    })?;
    registry.put_referrer_manifest(
        repository,
        subject_digest,
        OCI_MANIFEST_MEDIA_TYPE,
        &referrer_bytes,
    )?;
    *bytes_transferred += referrer_bytes.len() as u64;
    Ok(())
}

fn evaluate_signature_state(
    registry: &dyn PortableV2OciRegistry,
    repository: &str,
    subject_digest: &str,
    package_digest: &str,
    policy: &PortableV2OciAuthenticityPolicy,
) -> Result<PortableV2OciSignatureState, PortableV2Error> {
    let referrers = registry.list_referrers(repository, subject_digest)?;
    let mut saw_signature = false;
    let mut mismatched = false;
    let mut invalid = false;
    for bytes in referrers {
        let Ok(manifest) = serde_json::from_slice::<OciManifest>(&bytes) else {
            continue;
        };
        if manifest.artifact_type != OCI_SIGNATURE_ARTIFACT_TYPE {
            continue;
        }
        saw_signature = true;
        let Some(layer) = manifest.layers.first() else {
            invalid = true;
            continue;
        };
        let Ok(payload_bytes) = registry.get_blob(repository, &layer.digest) else {
            invalid = true;
            continue;
        };
        let Ok(payload) = serde_json::from_slice::<OciSignaturePayload>(&payload_bytes) else {
            invalid = true;
            continue;
        };
        if payload.contract != SIGNATURE_CONTRACT
            || payload.subject_manifest_digest != subject_digest
            || payload.package_digest != package_digest
        {
            invalid = true;
            continue;
        }
        if let Some(required) = &policy.require_named_signer
            && &payload.signer != required
        {
            mismatched = true;
            continue;
        }
        let Some(key) = &policy.verification_key else {
            if policy.require_named_signer.is_some() {
                mismatched = true;
            }
            continue;
        };
        let expected = signature_mac(key, &payload.signer, subject_digest, package_digest);
        if expected == payload.mac {
            return Ok(PortableV2OciSignatureState::Valid);
        }
        invalid = true;
    }
    if !saw_signature {
        return Ok(PortableV2OciSignatureState::Absent);
    }
    if mismatched {
        return Ok(PortableV2OciSignatureState::PolicyMismatched);
    }
    if invalid {
        return Ok(PortableV2OciSignatureState::Invalid);
    }
    Ok(PortableV2OciSignatureState::Absent)
}

fn authenticity_error(state: PortableV2OciSignatureState) -> PortableV2Error {
    match state {
        PortableV2OciSignatureState::Absent => PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "authenticity policy requires a named signer but signature is absent",
        ),
        PortableV2OciSignatureState::Invalid => PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "authenticity policy rejected an invalid signature",
        ),
        PortableV2OciSignatureState::PolicyMismatched => PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "authenticity policy mismatched the attached signer",
        ),
        PortableV2OciSignatureState::Valid => PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "authenticity policy unexpectedly failed a valid signature",
        ),
    }
}

fn signature_mac(secret: &[u8], signer: &str, subject: &str, package_digest: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-oci-sig/1\0");
    hasher.update(secret);
    hasher.update(b"\0");
    hasher.update(signer.as_bytes());
    hasher.update(b"\0");
    hasher.update(subject.as_bytes());
    hasher.update(b"\0");
    hasher.update(package_digest.as_bytes());
    format!("sha256:{}", encode_hex(hasher.finalize()))
}

fn read_package_bytes(path: &Path, report: &PortableV2Report) -> Result<Vec<u8>, PortableV2Error> {
    match report.representation {
        crate::PortableV2Representation::Bundle => {
            let mut file = File::open(path).map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::Io, "failed to open package")
            })?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::Io, "failed to read package")
            })?;
            Ok(bytes)
        }
        crate::PortableV2Representation::Expanded => Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "OCI publish currently requires a portable-v2 bundle representation",
        )),
    }
}

fn write_exclusive(path: &Path, bytes: &[u8]) -> Result<(), PortableV2Error> {
    let mut file = File::create_new(path).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "failed to create staging file")
    })?;
    file.write_all(bytes).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "failed to write staging file")
    })?;
    file.sync_all().map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "failed to sync staging file")
    })?;
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
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

fn package_class_str(class: PortableV2PackageClass) -> &'static str {
    match class {
        PortableV2PackageClass::Complete => "complete",
        PortableV2PackageClass::OntologyOnly => "ontology-only",
        PortableV2PackageClass::ComponentSelective => "component-selective",
        PortableV2PackageClass::GraphDataSubset => "graph-data-subset",
    }
}

fn validate_repository(repository: &str) -> Result<(), PortableV2Error> {
    if repository.is_empty()
        || repository.len() > 255
        || repository.contains("..")
        || repository.starts_with('/')
        || repository.contains('\\')
        || repository.contains('\n')
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidPath,
            "invalid repository name",
        ));
    }
    Ok(())
}

fn validate_digest(digest: &str) -> Result<(), PortableV2Error> {
    if !digest.starts_with("sha256:") || digest.len() != 71 {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "digest must be sha256:<64 hex>",
        ));
    }
    if !digest[7..].bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "digest must be sha256:<64 hex>",
        ));
    }
    Ok(())
}

fn validate_reference(reference: &str) -> Result<(), PortableV2Error> {
    if reference.starts_with("sha256:") {
        return validate_digest(reference);
    }
    if reference.is_empty()
        || reference.len() > 128
        || reference.contains('/')
        || reference.contains('\\')
        || reference.contains('\n')
        || reference.contains('@')
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidPath,
            "invalid tag reference",
        ));
    }
    Ok(())
}

fn check_cancelled(cancelled: Option<&AtomicBool>) -> Result<(), PortableV2Error> {
    if cancelled.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Cancelled,
            "portable OCI operation cancelled",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GraphWriter, PortableV2ExportLimits,
        PortableV2GraphSelector, PortableV2Output, PortableV2PropertyProjection,
        PortableV2SelectionProfile, PortableV2SelectionRequest, PortableV2SubsetClosure,
        PortableV2SubsetRequest, ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
        capture_graph_files, empty_workspace_participants, export_complete_portable_v2,
        open_or_initialize_project, plan_complete_portable_v2, plan_graph_subset_portable_v2,
        plan_selected_portable_v2, preview_portable_v2_graph_subset, preview_portable_v2_selection,
        resolve_project_generation, stage_project_generation_with_graph_tree,
    };
    use graphforge_core::{OntologyMode, TypeId};
    use graphforge_ir::IrLiteral;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn export_bundle_for(
        profile: PortableV2SelectionProfile,
    ) -> (tempfile::TempDir, PathBuf, PortableV2Report) {
        let dir = tempdir().unwrap();
        let root = dir.path().join("project");
        open_or_initialize_project(&root).unwrap();
        let generation = resolve_project_generation(&root).unwrap();
        let limits = PortableV2ExportLimits::default();
        let plan = match profile {
            PortableV2SelectionProfile::Complete => {
                plan_complete_portable_v2(&generation, limits).unwrap()
            }
            other => {
                let selection = preview_portable_v2_selection(
                    &generation,
                    &PortableV2SelectionRequest {
                        profile: other,
                        strict: false,
                    },
                    limits,
                )
                .unwrap();
                plan_selected_portable_v2(&generation, &selection, limits).unwrap()
            }
        };
        let bundle = dir.path().join("pkg.gfpb");
        export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let report = verify_portable_v2(
            &bundle,
            PortableV2Mode::Full,
            PortableV2Limits::default(),
            None,
        )
        .unwrap();
        (dir, bundle, report)
    }

    fn export_bundle() -> (tempfile::TempDir, PathBuf, PortableV2Report) {
        export_bundle_for(PortableV2SelectionProfile::Complete)
    }

    fn publish_req<'a>(
        bundle: &'a Path,
        tag: Option<&'a str>,
        signature: Option<PortableV2OciSignatureMaterial>,
        credential: Option<&'a str>,
    ) -> PortableV2OciPublishRequest<'a> {
        PortableV2OciPublishRequest {
            package_path: bundle,
            registry: "memory.local",
            repository: "tests/portable",
            tag,
            limits: PortableV2Limits::default(),
            authenticity: PortableV2OciAuthenticityPolicy::default(),
            signature,
            credential,
        }
    }

    fn pull_req<'a>(
        reference: &'a str,
        destination: &'a Path,
        expected: Option<&'a str>,
        authenticity: PortableV2OciAuthenticityPolicy,
    ) -> PortableV2OciPullRequest<'a> {
        PortableV2OciPullRequest {
            registry: "memory.local",
            repository: "tests/portable",
            reference,
            expected_oci_digest: expected,
            destination,
            limits: PortableV2Limits::default(),
            authenticity,
            credential: None,
        }
    }

    #[test]
    fn publish_and_pull_by_digest_preserves_package_identity() {
        let (_dir, bundle, report) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let published = publish_portable_v2_oci(
            &registry,
            &publish_req(&bundle, Some("latest"), None, Some("user:secret-token")),
            None,
        )
        .unwrap();
        assert_eq!(published.package_digest, report.package_digest);

        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("pulled.gfpb");
        let pulled = pull_portable_v2_oci(
            &registry,
            &PortableV2OciPullRequest {
                registry: "memory.local",
                repository: "tests/portable",
                reference: &published.oci_manifest_digest,
                expected_oci_digest: None,
                destination: &destination,
                limits: PortableV2Limits::default(),
                authenticity: PortableV2OciAuthenticityPolicy::default(),
                credential: Some("user:secret-token"),
            },
            None,
        )
        .unwrap();
        assert_eq!(pulled.report.package_digest, report.package_digest);
        assert_eq!(pulled.signature_state, PortableV2OciSignatureState::Absent);
        let json = serde_json::to_string(&pulled).unwrap();
        assert!(!json.contains("secret-token"));
        assert!(!json.contains("user:"));
    }

    #[test]
    fn selective_package_classes_round_trip_through_local_registry() {
        for profile in [
            PortableV2SelectionProfile::Complete,
            PortableV2SelectionProfile::OntologyOnly,
            PortableV2SelectionProfile::Settings,
        ] {
            let (_dir, bundle, report) = export_bundle_for(profile);
            let registry = MemoryOciRegistry::default();
            let published =
                publish_portable_v2_oci(&registry, &publish_req(&bundle, None, None, None), None)
                    .unwrap();
            assert_eq!(published.package_class, report.package_class);
            let out_dir = tempdir().unwrap();
            let destination = out_dir.path().join("pulled.gfpb");
            let pulled = pull_portable_v2_oci(
                &registry,
                &pull_req(
                    &published.oci_manifest_digest,
                    &destination,
                    None,
                    Default::default(),
                ),
                None,
            )
            .unwrap();
            assert_eq!(pulled.report.package_digest, report.package_digest);
            assert_eq!(pulled.report.package_class, report.package_class);
        }
    }

    #[test]
    fn graph_data_subset_package_round_trips_through_local_registry() {
        let root = tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let workspace = tempdir().unwrap();
        let nodes = {
            let mut bytes = [0_u8; 16];
            [
                {
                    bytes[15] = 1;
                    Uuid::from_bytes(bytes)
                },
                {
                    bytes[15] = 2;
                    Uuid::from_bytes(bytes)
                },
            ]
        };
        let edge = {
            let mut bytes = [0_u8; 16];
            bytes[15] = 11;
            Uuid::from_bytes(bytes)
        };
        let mut writer = GraphWriter::open_at(
            workspace.path(),
            OntologyMode::Exploratory,
            1_700_000_000_000_000,
        )
        .unwrap();
        for (index, node) in nodes.iter().enumerate() {
            writer.create_node(*node, TypeId(1)).unwrap();
            writer
                .set_properties(
                    node,
                    None,
                    HashMap::from([("value".into(), IrLiteral::Int(index as i64))]),
                )
                .unwrap();
        }
        writer
            .create_edge(edge, "KNOWS", &nodes[0], &nodes[1])
            .unwrap();
        writer.flush().unwrap();
        let (_, files) = capture_graph_files(workspace.path()).unwrap();
        let mut participants = empty_workspace_participants().unwrap();
        participants.insert(0, files);
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: vec![
                ProjectCapability {
                    capability_id: GRAPH_CAPABILITY_ID.into(),
                    capability_version: GRAPH_CAPABILITY_VERSION,
                },
                ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation_with_graph_tree(root.path(), &request, Some(workspace.path()))
                .unwrap()
        else {
            panic!("expected staged publication");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        let generation = resolve_project_generation(root.path()).unwrap();
        let limits = PortableV2Limits::default();
        let mut selected = [nodes[0], nodes[1]].map(|uuid| uuid.hyphenated().to_string());
        selected.sort();
        let subset = PortableV2SubsetRequest {
            selector: PortableV2GraphSelector {
                node_uuids: selected.to_vec(),
                edge_uuids: vec![],
            },
            closure: PortableV2SubsetClosure::InducedEdges,
            projection: PortableV2PropertyProjection { exclude: vec![] },
        };
        let preview = preview_portable_v2_graph_subset(&generation, &subset, limits).unwrap();
        let plan = plan_graph_subset_portable_v2(&generation, &preview, limits).unwrap();
        let bundle = root.path().join("subset.gfpb");
        export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        let report = verify_portable_v2(&bundle, PortableV2Mode::Full, limits, None).unwrap();
        assert_eq!(
            report.package_class,
            PortableV2PackageClass::GraphDataSubset
        );
        let registry = MemoryOciRegistry::default();
        let published =
            publish_portable_v2_oci(&registry, &publish_req(&bundle, None, None, None), None)
                .unwrap();
        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("pulled.gfpb");
        let pulled = pull_portable_v2_oci(
            &registry,
            &pull_req(
                &published.oci_manifest_digest,
                &destination,
                None,
                Default::default(),
            ),
            None,
        )
        .unwrap();
        assert_eq!(pulled.report.package_digest, report.package_digest);
        assert_eq!(
            pulled.report.package_class,
            PortableV2PackageClass::GraphDataSubset
        );
    }

    #[test]
    fn mutable_tag_move_does_not_alter_digest_pinned_pull() {
        let (_dir, bundle, report) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let first = publish_portable_v2_oci(
            &registry,
            &publish_req(&bundle, Some("moving"), None, None),
            None,
        )
        .unwrap();

        let mut alt = serde_json::to_vec(&OciManifest {
            schema_version: 2,
            media_type: OCI_MANIFEST_MEDIA_TYPE.to_owned(),
            artifact_type: OCI_ARTIFACT_TYPE.to_owned(),
            config: OciDescriptor {
                media_type: OCI_CONFIG_MEDIA_TYPE.to_owned(),
                digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .into(),
                size: 1,
            },
            layers: vec![OciDescriptor {
                media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
                digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                    .into(),
                size: 1,
            }],
            subject: None,
        })
        .unwrap();
        alt.push(b'\n');
        let moved = registry
            .put_manifest("tests/portable", "moving", OCI_MANIFEST_MEDIA_TYPE, &alt)
            .unwrap();
        assert_ne!(moved, first.oci_manifest_digest);

        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("pinned.gfpb");
        let pulled = pull_portable_v2_oci(
            &registry,
            &pull_req(
                &first.oci_manifest_digest,
                &destination,
                None,
                Default::default(),
            ),
            None,
        )
        .unwrap();
        assert_eq!(pulled.report.package_digest, report.package_digest);

        let disagree = out_dir.path().join("disagree.gfpb");
        let error = pull_portable_v2_oci(
            &registry,
            &pull_req(
                "moving",
                &disagree,
                Some(&first.oci_manifest_digest),
                Default::default(),
            ),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch);
    }

    #[test]
    fn failure_modes_cannot_claim_successful_receipt() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let published =
            publish_portable_v2_oci(&registry, &publish_req(&bundle, None, None, None), None)
                .unwrap();

        // Missing blob / manifest
        let empty = MemoryOciRegistry::default();
        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("missing.gfpb");
        let error = pull_portable_v2_oci(
            &empty,
            &pull_req(
                &published.oci_manifest_digest,
                &destination,
                None,
                Default::default(),
            ),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure);
        assert!(!destination.exists());

        // Incompatible media type
        let bad = serde_json::to_vec(&OciManifest {
            schema_version: 2,
            media_type: OCI_MANIFEST_MEDIA_TYPE.to_owned(),
            artifact_type: "application/vnd.other".into(),
            config: OciDescriptor {
                media_type: OCI_CONFIG_MEDIA_TYPE.to_owned(),
                digest: published.oci_manifest_digest.clone(),
                size: 1,
            },
            layers: vec![OciDescriptor {
                media_type: OCI_LAYER_MEDIA_TYPE.to_owned(),
                digest: published.oci_manifest_digest.clone(),
                size: 1,
            }],
            subject: None,
        })
        .unwrap();
        let digest = registry
            .put_manifest(
                "tests/portable",
                &digest_sha256(&bad),
                OCI_MANIFEST_MEDIA_TYPE,
                &bad,
            )
            .unwrap();
        let destination = out_dir.path().join("bad-media.gfpb");
        let error = pull_portable_v2_oci(
            &registry,
            &pull_req(&digest, &destination, None, Default::default()),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
        assert!(!destination.exists());

        // Digest mismatch on layer by corrupting stored blob after publish
        let registry2 = MemoryOciRegistry::default();
        let published2 =
            publish_portable_v2_oci(&registry2, &publish_req(&bundle, None, None, None), None)
                .unwrap();
        registry2.corrupt_first_blob();
        let destination = out_dir.path().join("corrupt.gfpb");
        let error = pull_portable_v2_oci(
            &registry2,
            &pull_req(
                &published2.oci_manifest_digest,
                &destination,
                None,
                Default::default(),
            ),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch);
        assert!(!destination.exists());
    }

    #[test]
    fn authenticity_distinguishes_absent_valid_invalid_and_mismatched() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let secret = b"test-signing-key".to_vec();
        let published = publish_portable_v2_oci(
            &registry,
            &publish_req(
                &bundle,
                None,
                Some(PortableV2OciSignatureMaterial {
                    signer: "releases@curatelabs.ai".into(),
                    key_id: "test-key".into(),
                    secret: secret.clone(),
                }),
                None,
            ),
            None,
        )
        .unwrap();

        let absent = evaluate_portable_v2_oci_signature_state(
            &MemoryOciRegistry::default(),
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableV2OciAuthenticityPolicy {
                require_named_signer: Some("releases@curatelabs.ai".into()),
                verification_key: Some(secret.clone()),
            },
        )
        .unwrap();
        assert_eq!(absent, PortableV2OciSignatureState::Absent);

        let valid = evaluate_portable_v2_oci_signature_state(
            &registry,
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableV2OciAuthenticityPolicy {
                require_named_signer: Some("releases@curatelabs.ai".into()),
                verification_key: Some(secret.clone()),
            },
        )
        .unwrap();
        assert_eq!(valid, PortableV2OciSignatureState::Valid);

        let mismatched = evaluate_portable_v2_oci_signature_state(
            &registry,
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableV2OciAuthenticityPolicy {
                require_named_signer: Some("other@example.com".into()),
                verification_key: Some(secret.clone()),
            },
        )
        .unwrap();
        assert_eq!(mismatched, PortableV2OciSignatureState::PolicyMismatched);

        let invalid = evaluate_portable_v2_oci_signature_state(
            &registry,
            "tests/portable",
            &published.oci_manifest_digest,
            &published.package_digest,
            &PortableV2OciAuthenticityPolicy {
                require_named_signer: Some("releases@curatelabs.ai".into()),
                verification_key: Some(b"wrong-key".to_vec()),
            },
        )
        .unwrap();
        assert_eq!(invalid, PortableV2OciSignatureState::Invalid);

        // Policy requiring signer with unsigned package: integrity would pass, authenticity absent.
        let unsigned_registry = MemoryOciRegistry::default();
        let unsigned = publish_portable_v2_oci(
            &unsigned_registry,
            &publish_req(&bundle, None, None, None),
            None,
        )
        .unwrap();
        let out_dir = tempdir().unwrap();
        let destination = out_dir.path().join("unsigned.gfpb");
        let error = pull_portable_v2_oci(
            &unsigned_registry,
            &pull_req(
                &unsigned.oci_manifest_digest,
                &destination,
                None,
                PortableV2OciAuthenticityPolicy {
                    require_named_signer: Some("releases@curatelabs.ai".into()),
                    verification_key: Some(secret),
                },
            ),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
        assert!(!destination.exists());
        let detail = format!("{error:?}");
        assert!(!detail.contains("DigestMismatch"));
    }

    #[test]
    fn cancellation_before_upload_does_not_claim_publication() {
        let (_dir, bundle, _) = export_bundle();
        let registry = MemoryOciRegistry::default();
        let cancelled = AtomicBool::new(true);
        let error = publish_portable_v2_oci(
            &registry,
            &publish_req(&bundle, None, None, None),
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Cancelled);
    }

    #[test]
    fn http_registry_rejects_credential_in_host() {
        let error = match HttpOciRegistry::new("user:pass@ghcr.io", None, false) {
            Err(error) => error,
            Ok(_) => panic!("expected invalid registry host"),
        };
        assert_eq!(error.code, PortableV2ErrorCode::InvalidPath);
    }
}
