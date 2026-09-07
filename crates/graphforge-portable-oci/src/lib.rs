//! OCI Distribution protocol, bounded HTTP transport, and authenticity.
#![allow(missing_docs)]
use graphforge_core::portable::{
    PortableV2Error, PortableV2ErrorCode, PortableV2OciAuthenticityPolicy, PortableV2OciPhase,
    PortableV2OciProgress, PortableV2OciSignatureMaterial, PortableV2OciSignatureState,
    PortableV2PackageClass,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
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

pub const CONFIG_CONTRACT: &str = "graphforge-portable-v2-oci-config/1";
const SIGNATURE_CONTRACT: &str = "graphforge-portable-v2-oci-signature/1";
/// Artifact type for GraphForge OCI signature/provenance attachments.
pub const OCI_SIGNATURE_ARTIFACT_TYPE: &str = "application/vnd.graphforge.project.v2.signature";
/// Config media type for signature attachments.
pub const OCI_SIGNATURE_CONFIG_MEDIA_TYPE: &str =
    "application/vnd.graphforge.project.v2.signature+json";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OciConfig {
    pub contract: String,
    pub package_digest: String,
    pub package_class: String,
    pub representation: String,
    pub transport_digest: Option<String>,
    pub layer_media_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OciDescriptor {
    #[serde(rename = "mediaType")]
    pub media_type: String,
    pub digest: String,
    pub size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OciManifest {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u32,
    #[serde(rename = "mediaType")]
    pub media_type: String,
    #[serde(rename = "artifactType")]
    pub artifact_type: String,
    pub config: OciDescriptor,
    pub layers: Vec<OciDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<OciDescriptor>,
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
}

/// Maximum response bytes, preserving the prior ureq default.
pub const MAX_RESPONSE_BYTES: u64 = 10 * 1024 * 1024;

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
        let origin = url::Url::parse(&format!("{scheme}://{host}")).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "invalid registry origin")
        })?;
        if origin.host_str().is_none()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
            || !origin.username().is_empty()
            || origin.password().is_some()
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::InvalidPath,
                "invalid registry origin",
            ));
        }
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
        let base = url::Url::parse(&self.base_url).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "invalid registry origin")
        })?;
        let upload = base.join(&location).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "invalid upload location")
        })?;
        if upload.origin() != base.origin()
            || !upload.username().is_empty()
            || upload.password().is_some()
            || upload.fragment().is_some()
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::InvalidPath,
                "upload location is outside the registry origin",
            ));
        }
        let upload_url = upload.as_str();
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
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
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
        let body = resp
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
            .read_to_vec()
            .map_err(|_| {
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

pub fn attach_signature_referrer(
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

pub fn evaluate_signature_state(
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

#[must_use]
pub fn authenticity_error(state: PortableV2OciSignatureState) -> PortableV2Error {
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

#[must_use]
pub fn digest_sha256(bytes: &[u8]) -> String {
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

#[must_use]
pub fn package_class_str(class: PortableV2PackageClass) -> &'static str {
    match class {
        PortableV2PackageClass::Complete => "complete",
        PortableV2PackageClass::OntologyOnly => "ontology-only",
        PortableV2PackageClass::ComponentSelective => "component-selective",
        PortableV2PackageClass::GraphDataSubset => "graph-data-subset",
    }
}

pub fn validate_repository(repository: &str) -> Result<(), PortableV2Error> {
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

pub fn validate_digest(digest: &str) -> Result<(), PortableV2Error> {
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

pub fn validate_reference(reference: &str) -> Result<(), PortableV2Error> {
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

pub fn check_cancelled(cancelled: Option<&AtomicBool>) -> Result<(), PortableV2Error> {
    if cancelled.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Cancelled,
            "portable OCI operation cancelled",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
