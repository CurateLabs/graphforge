//! OCI Distribution protocol, bounded HTTP transport, and authenticity.
#![allow(missing_docs)]
use graphforge_core::hash_observation::ArtifactSha256 as Sha256;
use graphforge_core::portable::{
    PortableError, PortableErrorCode, PortableOciAuthenticityPolicy, PortableOciPhase,
    PortableOciProgress, PortableOciSignatureMaterial, PortableOciSignatureState,
    PortablePackageClass,
};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::BTreeMap;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use ureq::unversioned::resolver::DefaultResolver;
use ureq::unversioned::transport::{
    Buffers, ConnectionDetails, Connector, DefaultConnector, NextTimeout, Transport,
    time::Duration as WaitDuration,
};
/// Artifact type for a GraphForge portable package in an OCI registry.
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
pub trait PortableOciRegistry: Send + Sync {
    fn put_blob(&self, repository: &str, digest: &str, bytes: &[u8]) -> Result<(), PortableError>;
    fn get_blob(&self, repository: &str, digest: &str) -> Result<Vec<u8>, PortableError>;
    /// Upload a layer without buffering it. `source` must yield exactly `size`
    /// bytes whose sha256 is `digest`; otherwise the blob is not admitted.
    fn put_blob_stream(
        &self,
        repository: &str,
        digest: &str,
        size: u64,
        source: &mut dyn Read,
    ) -> Result<(), PortableError>;
    /// Stream `size` bytes of a layer. The caller verifies length and digest
    /// of what it consumed before admitting it.
    fn get_blob_stream<'a>(
        &'a self,
        repository: &str,
        digest: &str,
        size: u64,
    ) -> Result<Box<dyn Read + 'a>, PortableError>;
    fn blob_exists(&self, repository: &str, digest: &str) -> Result<bool, PortableError>;
    fn put_manifest(
        &self,
        repository: &str,
        reference: &str,
        media_type: &str,
        bytes: &[u8],
    ) -> Result<String, PortableError>;
    fn get_manifest(
        &self,
        repository: &str,
        reference: &str,
    ) -> Result<(String, Vec<u8>), PortableError>;
    /// Attach a referrer manifest for `subject_digest`. Default: unsupported.
    fn put_referrer_manifest(
        &self,
        _repository: &str,
        _subject_digest: &str,
        _media_type: &str,
        _bytes: &[u8],
    ) -> Result<String, PortableError> {
        Err(PortableError::new(
            PortableErrorCode::UnsupportedFuture,
            "registry does not support referrer attachments",
        ))
    }
    /// List referrer manifests for a subject. Default: empty.
    fn list_referrers(
        &self,
        _repository: &str,
        _subject_digest: &str,
    ) -> Result<Vec<Vec<u8>>, PortableError> {
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
    blobs: BTreeMap<(String, String), Arc<Vec<u8>>>,
    manifests: BTreeMap<(String, String), (String, Vec<u8>)>,
    tags: BTreeMap<(String, String), String>,
    referrers: BTreeMap<(String, String), Vec<Vec<u8>>>,
}

impl PortableOciRegistry for MemoryOciRegistry {
    fn put_blob(&self, repository: &str, digest: &str, bytes: &[u8]) -> Result<(), PortableError> {
        self.store_blob(repository, digest, bytes.to_vec())
    }

    fn put_blob_stream(
        &self,
        repository: &str,
        digest: &str,
        size: u64,
        source: &mut dyn Read,
    ) -> Result<(), PortableError> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        if size > MEMORY_REGISTRY_MAX_BLOB_BYTES {
            return Err(PortableError::new(
                PortableErrorCode::LimitExceeded,
                "blob exceeds the memory registry limit",
            ));
        }
        let io = |detail| PortableError::new(PortableErrorCode::Io, detail);
        // Reserve the declared length once (fallibly, so a lying size cannot
        // abort the process) and move the bytes into storage without a copy.
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(
                usize::try_from(size).map_err(|_| io("blob size is not addressable"))?,
            )
            .map_err(|_| io("blob size cannot be reserved"))?;
        (&mut *source)
            .take(size.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|_| io("blob stream failed"))?;
        if bytes.len() as u64 != size {
            return Err(io("blob stream length does not match declared size"));
        }
        self.store_blob(repository, digest, bytes)
    }

    fn get_blob_stream<'a>(
        &'a self,
        repository: &str,
        digest: &str,
        _size: u64,
    ) -> Result<Box<dyn Read + 'a>, PortableError> {
        Ok(Box::new(SharedBlobReader {
            blob: self.stored_blob(repository, digest)?,
            position: 0,
        }))
    }

    fn get_blob(&self, repository: &str, digest: &str) -> Result<Vec<u8>, PortableError> {
        self.stored_blob(repository, digest)
            .map(|blob| blob.to_vec())
    }

    fn blob_exists(&self, repository: &str, digest: &str) -> Result<bool, PortableError> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        Ok(self
            .inner
            .lock()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "registry lock poisoned"))?
            .blobs
            .contains_key(&(repository.to_owned(), digest.to_owned())))
    }

    fn put_manifest(
        &self,
        repository: &str,
        reference: &str,
        _media_type: &str,
        bytes: &[u8],
    ) -> Result<String, PortableError> {
        validate_repository(repository)?;
        validate_reference(reference)?;
        let digest = digest_sha256(bytes);
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "registry lock poisoned"))?;
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
    ) -> Result<(String, Vec<u8>), PortableError> {
        validate_repository(repository)?;
        validate_reference(reference)?;
        let guard = self
            .inner
            .lock()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "registry lock poisoned"))?;
        let digest = if reference.starts_with("sha256:") {
            reference.to_owned()
        } else {
            guard
                .tags
                .get(&(repository.to_owned(), reference.to_owned()))
                .cloned()
                .ok_or_else(|| {
                    PortableError::new(PortableErrorCode::InvalidStructure, "tag not found")
                })?
        };
        guard
            .manifests
            .get(&(repository.to_owned(), digest))
            .cloned()
            .ok_or_else(|| {
                PortableError::new(PortableErrorCode::InvalidStructure, "manifest not found")
            })
    }

    fn put_referrer_manifest(
        &self,
        repository: &str,
        subject_digest: &str,
        media_type: &str,
        bytes: &[u8],
    ) -> Result<String, PortableError> {
        validate_repository(repository)?;
        validate_digest(subject_digest)?;
        let digest = self.put_manifest(repository, &digest_sha256(bytes), media_type, bytes)?;
        self.inner
            .lock()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "registry lock poisoned"))?
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
    ) -> Result<Vec<Vec<u8>>, PortableError> {
        validate_repository(repository)?;
        validate_digest(subject_digest)?;
        Ok(self
            .inner
            .lock()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "registry lock poisoned"))?
            .referrers
            .get(&(repository.to_owned(), subject_digest.to_owned()))
            .cloned()
            .unwrap_or_default())
    }
}

/// Reads a stored blob in place; the blob is shared, not copied.
struct SharedBlobReader {
    blob: Arc<Vec<u8>>,
    position: usize,
}

impl Read for SharedBlobReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let rest = &self.blob[self.position..];
        let read = rest.len().min(buf.len());
        buf[..read].copy_from_slice(&rest[..read]);
        self.position += read;
        Ok(read)
    }
}

impl MemoryOciRegistry {
    fn store_blob(
        &self,
        repository: &str,
        digest: &str,
        bytes: Vec<u8>,
    ) -> Result<(), PortableError> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        if digest_sha256(&bytes) != digest {
            return Err(PortableError::new(
                PortableErrorCode::DigestMismatch,
                "blob bytes do not match declared digest",
            ));
        }
        self.inner
            .lock()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "registry lock poisoned"))?
            .blobs
            .insert((repository.to_owned(), digest.to_owned()), Arc::new(bytes));
        Ok(())
    }

    fn stored_blob(&self, repository: &str, digest: &str) -> Result<Arc<Vec<u8>>, PortableError> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        self.inner
            .lock()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "registry lock poisoned"))?
            .blobs
            .get(&(repository.to_owned(), digest.to_owned()))
            .cloned()
            .ok_or_else(|| {
                PortableError::new(PortableErrorCode::InvalidStructure, "blob not found")
            })
    }

    /// Resolve a mutable tag to the current digest without claiming package identity.
    pub fn resolve_tag(&self, repository: &str, tag: &str) -> Result<String, PortableError> {
        validate_repository(repository)?;
        validate_reference(tag)?;
        self.inner
            .lock()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "registry lock poisoned"))?
            .tags
            .get(&(repository.to_owned(), tag.to_owned()))
            .cloned()
            .ok_or_else(|| PortableError::new(PortableErrorCode::InvalidStructure, "tag not found"))
    }
}

/// Largest blob the in-process [`MemoryOciRegistry`] accepts from a stream. It
/// bounds what a declared size may make the registry reserve up front.
pub const MEMORY_REGISTRY_MAX_BLOB_BYTES: u64 = 256 * 1024 * 1024;

/// Maximum response bytes for small JSON documents (manifests, config blobs).
/// Layer blobs are streamed and never bounded by this.
pub const MAX_RESPONSE_BYTES: u64 = 10 * 1024 * 1024;

/// How layer blobs move over HTTP. Layers are transferred as a sequence of
/// requests of at most `chunk_bytes` (OCI chunked upload, ranged download).
/// There is no deadline on a request or a transfer: connecting, waiting for
/// response headers, and every single socket read or write must each make
/// progress within `idle_timeout`, so a long transfer completes as long as
/// bytes keep moving and a stalled one fails after `idle_timeout`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpOciTransfer {
    pub chunk_bytes: u64,
    pub idle_timeout: Duration,
}

impl Default for HttpOciTransfer {
    fn default() -> Self {
        Self {
            chunk_bytes: 8 * 1024 * 1024,
            idle_timeout: Duration::from_mins(1),
        }
    }
}

/// HTTP OCI Distribution client (ureq). Credentials are only sent as headers.
pub struct HttpOciRegistry {
    base_url: String,
    credential: Option<String>,
    agent: ureq::Agent,
    chunk_bytes: u64,
}

impl HttpOciRegistry {
    /// Construct a client for `host[:port]`. Defaults to HTTPS unless `insecure_http`.
    pub fn new(
        registry: &str,
        credential: Option<&str>,
        insecure_http: bool,
    ) -> Result<Self, PortableError> {
        Self::with_transfer(
            registry,
            credential,
            insecure_http,
            HttpOciTransfer::default(),
        )
    }

    /// Like [`Self::new`] with explicit layer transfer chunking and per-request timeout.
    pub fn with_transfer(
        registry: &str,
        credential: Option<&str>,
        insecure_http: bool,
        transfer: HttpOciTransfer,
    ) -> Result<Self, PortableError> {
        if transfer.chunk_bytes == 0 || transfer.idle_timeout.is_zero() {
            return Err(PortableError::new(
                PortableErrorCode::InvalidPath,
                "transfer chunk size and idle timeout must be positive",
            ));
        }
        let host = registry.trim().trim_end_matches('/');
        if host.is_empty() || host.contains("://") || host.contains('@') {
            return Err(PortableError::new(
                PortableErrorCode::InvalidPath,
                "registry must be a host[:port] without scheme or credentials",
            ));
        }
        // An exported-but-unset credential arrives as an empty string; it is no
        // credential, not an empty bearer token.
        let credential = credential.filter(|cred| !cred.trim().is_empty());
        if let Some(cred) = credential
            && cred.chars().any(char::is_control)
        {
            return Err(PortableError::new(
                PortableErrorCode::InvalidPath,
                "credential contains control characters",
            ));
        }
        let scheme = if insecure_http { "http" } else { "https" };
        let origin = url::Url::parse(&format!("{scheme}://{host}")).map_err(|_| {
            PortableError::new(PortableErrorCode::InvalidPath, "invalid registry origin")
        })?;
        if origin.host_str().is_none()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
            || !origin.username().is_empty()
            || origin.password().is_some()
        {
            return Err(PortableError::new(
                PortableErrorCode::InvalidPath,
                "invalid registry origin",
            ));
        }
        // No per-call, global or body deadline: a body may take as long as it
        // keeps moving. Every wait is bounded by `IdleTransport` instead, and
        // the phases before the body are bounded here.
        let config = ureq::Agent::config_builder()
            .timeout_resolve(Some(transfer.idle_timeout))
            .timeout_connect(Some(transfer.idle_timeout))
            .timeout_send_request(Some(transfer.idle_timeout))
            .timeout_recv_response(Some(transfer.idle_timeout))
            .http_status_as_error(false)
            .max_redirects(0)
            .build();
        let connector = DefaultConnector::default().chain(IdleConnector {
            idle: transfer.idle_timeout,
        });
        Ok(Self {
            base_url: format!("{scheme}://{host}"),
            credential: credential.map(str::to_owned),
            agent: ureq::Agent::with_parts(config, connector, DefaultResolver::default()),
            chunk_bytes: transfer.chunk_bytes,
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

    fn map_http(_err: ureq::Error) -> PortableError {
        PortableError::new(PortableErrorCode::Io, "registry transport failed")
    }

    /// Open an upload session and return its validated same-origin location.
    fn start_upload(&self, repository: &str) -> Result<url::Url, PortableError> {
        let mut start = self
            .agent
            .post(format!("{}/v2/{repository}/blobs/uploads/", self.base_url));
        if let Some(auth) = self.auth_header() {
            start = start.header("Authorization", auth);
        }
        let start_resp = start.send(&[] as &[u8]).map_err(Self::map_http)?;
        if !start_resp.status().is_success() && start_resp.status().as_u16() != 202 {
            return Err(PortableError::new(
                PortableErrorCode::Io,
                "blob upload session rejected",
            ));
        }
        let location = start_resp
            .headers()
            .get("Location")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                PortableError::new(PortableErrorCode::Io, "upload session missing Location")
            })?;
        self.upload_location(location)
    }

    fn upload_location(&self, location: &str) -> Result<url::Url, PortableError> {
        let base = url::Url::parse(&self.base_url).map_err(|_| {
            PortableError::new(PortableErrorCode::InvalidPath, "invalid registry origin")
        })?;
        let upload = base.join(location).map_err(|_| {
            PortableError::new(PortableErrorCode::InvalidPath, "invalid upload location")
        })?;
        if upload.origin() != base.origin()
            || !upload.username().is_empty()
            || upload.password().is_some()
            || upload.fragment().is_some()
        {
            return Err(PortableError::new(
                PortableErrorCode::InvalidPath,
                "upload location is outside the registry origin",
            ));
        }
        Ok(upload)
    }

    /// Commit an upload session; the registry admits the blob only if the
    /// session's bytes (plus `bytes`) match `digest`.
    fn finish_upload(
        &self,
        upload: &url::Url,
        digest: &str,
        bytes: &[u8],
    ) -> Result<(), PortableError> {
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
            return Err(PortableError::new(
                PortableErrorCode::Io,
                "blob upload rejected",
            ));
        }
        Ok(())
    }
}

impl PortableOciRegistry for HttpOciRegistry {
    fn put_blob(&self, repository: &str, digest: &str, bytes: &[u8]) -> Result<(), PortableError> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        if self.blob_exists(repository, digest)? {
            return Ok(());
        }
        let upload = self.start_upload(repository)?;
        self.finish_upload(&upload, digest, bytes)
    }

    fn put_blob_stream(
        &self,
        repository: &str,
        digest: &str,
        size: u64,
        source: &mut dyn Read,
    ) -> Result<(), PortableError> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        if self.blob_exists(repository, digest)? {
            return Ok(());
        }
        let mut upload = self.start_upload(repository)?;
        let mut source = DigestingReader::new(source);
        let mut offset = 0_u64;
        while offset < size {
            let length = self.chunk_bytes.min(size - offset);
            let mut req = self.agent.patch(upload.as_str());
            if let Some(auth) = self.auth_header() {
                req = req.header("Authorization", auth);
            }
            let mut chunk = ExactChunk {
                inner: &mut source,
                remaining: length,
            };
            let resp = req
                .header("Content-Type", "application/octet-stream")
                .header("Content-Length", length.to_string())
                .header("Content-Range", format!("{offset}-{}", offset + length - 1))
                .send(ureq::SendBody::from_reader(&mut chunk))
                .map_err(Self::map_http)?;
            if !resp.status().is_success() {
                return Err(PortableError::new(
                    PortableErrorCode::Io,
                    "blob chunk rejected",
                ));
            }
            offset += length;
            if let Some(next) = resp.headers().get("Location") {
                upload = self.upload_location(next.to_str().unwrap_or_default())?;
            }
        }
        let trailing = source
            .read(&mut [0_u8])
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "blob stream failed"))?;
        if trailing != 0 {
            return Err(PortableError::new(
                PortableErrorCode::Io,
                "blob stream is longer than its declared size",
            ));
        }
        if source.finish().0 != digest {
            return Err(PortableError::new(
                PortableErrorCode::DigestMismatch,
                "blob bytes do not match declared digest",
            ));
        }
        self.finish_upload(&upload, digest, &[])
    }

    fn get_blob_stream<'a>(
        &'a self,
        repository: &str,
        digest: &str,
        size: u64,
    ) -> Result<Box<dyn Read + 'a>, PortableError> {
        validate_repository(repository)?;
        validate_digest(digest)?;
        let mut reader = RangedBlobReader {
            registry: self,
            url: format!("{}/v2/{repository}/blobs/{digest}", self.base_url),
            size,
            offset: 0,
            range: None,
        };
        // Open the first range now so a missing blob is a typed error.
        reader.open_next_range().map_err(|failure| match failure {
            RangeFailure::Missing => {
                PortableError::new(PortableErrorCode::InvalidStructure, "blob not found")
            }
            RangeFailure::Transport => {
                PortableError::new(PortableErrorCode::Io, "blob download failed")
            }
        })?;
        Ok(Box::new(reader))
    }

    fn get_blob(&self, repository: &str, digest: &str) -> Result<Vec<u8>, PortableError> {
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
            return Err(PortableError::new(
                PortableErrorCode::InvalidStructure,
                "blob not found",
            ));
        }
        let body = resp
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
            .read_to_vec()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "blob download failed"))?;
        let actual = digest_sha256(&body);
        if actual != digest {
            return Err(PortableError::new(
                PortableErrorCode::DigestMismatch,
                "downloaded blob digest mismatch",
            ));
        }
        Ok(body)
    }

    fn blob_exists(&self, repository: &str, digest: &str) -> Result<bool, PortableError> {
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
            Err(_) => Err(PortableError::new(
                PortableErrorCode::Io,
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
    ) -> Result<String, PortableError> {
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
            return Err(PortableError::new(
                PortableErrorCode::Io,
                "manifest upload rejected",
            ));
        }
        Ok(digest_sha256(bytes))
    }

    fn get_manifest(
        &self,
        repository: &str,
        reference: &str,
    ) -> Result<(String, Vec<u8>), PortableError> {
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
            return Err(PortableError::new(
                PortableErrorCode::InvalidStructure,
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
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "manifest download failed"))?;
        Ok((media, body))
    }
}

/// Caps every socket wait of the wrapped transport at `idle`. ureq's own
/// timeouts are deadlines measured from the start of a phase and are not
/// restarted by progress; each transport read or write is the unit that must
/// make progress, so bounding those gives an idle timeout.
#[derive(Debug)]
struct IdleConnector {
    idle: Duration,
}

impl<In: Transport> Connector<In> for IdleConnector {
    type Out = IdleTransport<In>;

    fn connect(
        &self,
        _details: &ConnectionDetails,
        chained: Option<In>,
    ) -> Result<Option<Self::Out>, ureq::Error> {
        Ok(chained.map(|inner| IdleTransport {
            inner,
            idle: self.idle,
        }))
    }
}

#[derive(Debug)]
struct IdleTransport<T> {
    inner: T,
    idle: Duration,
}

impl<T> IdleTransport<T> {
    fn capped(&self, timeout: NextTimeout) -> NextTimeout {
        if timeout.after.is_not_happening() || *timeout.after > self.idle {
            NextTimeout {
                after: WaitDuration::Exact(self.idle),
                reason: timeout.reason,
            }
        } else {
            timeout
        }
    }
}

impl<T: Transport> Transport for IdleTransport<T> {
    fn buffers(&mut self) -> &mut dyn Buffers {
        self.inner.buffers()
    }

    fn transmit_output(&mut self, amount: usize, timeout: NextTimeout) -> Result<(), ureq::Error> {
        let timeout = self.capped(timeout);
        self.inner.transmit_output(amount, timeout)
    }

    fn await_input(&mut self, timeout: NextTimeout) -> Result<bool, ureq::Error> {
        let timeout = self.capped(timeout);
        self.inner.await_input(timeout)
    }

    fn is_open(&mut self) -> bool {
        self.inner.is_open()
    }

    fn is_tls(&self) -> bool {
        self.inner.is_tls()
    }
}

/// Whether a `Content-Range` value is exactly `bytes first-last/total` for the
/// requested range and a blob of `size` bytes (`*` leaves the total unstated).
fn content_range_matches(value: &str, first: u64, last: u64, size: u64) -> bool {
    let Some((range, total)) = value.strip_prefix("bytes ").and_then(|v| v.split_once('/')) else {
        return false;
    };
    range == format!("{first}-{last}") && (total == "*" || total.parse() == Ok(size))
}

enum RangeFailure {
    Missing,
    Transport,
}

/// Reads a blob as consecutive ranged GETs of at most `chunk_bytes`, each
/// bounded by the registry's per-request timeout.
struct RangedBlobReader<'a> {
    registry: &'a HttpOciRegistry,
    url: String,
    size: u64,
    offset: u64,
    /// Open response body and the bytes still expected from it.
    range: Option<(ureq::BodyReader<'static>, u64)>,
}

impl RangedBlobReader<'_> {
    fn open_next_range(&mut self) -> Result<(), RangeFailure> {
        let length = self.registry.chunk_bytes.min(self.size - self.offset);
        if length == 0 {
            return Ok(());
        }
        let last = self.offset + length - 1;
        let mut req = self.registry.agent.get(&self.url);
        if let Some(auth) = self.registry.auth_header() {
            req = req.header("Authorization", auth);
        }
        let resp = req
            .header("Range", format!("bytes={}-{last}", self.offset))
            .call()
            .map_err(|_| RangeFailure::Transport)?;
        let status = resp.status().as_u16();
        if status == 404 {
            return Err(RangeFailure::Missing);
        }
        let ranged = status == 206
            && resp
                .headers()
                .get("Content-Range")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| content_range_matches(value, self.offset, last, self.size));
        // A registry that ignores `Range` answers the first request with 200
        // and the whole blob; stream all of it, bounded by the declared size.
        let whole = status == 200 && self.offset == 0;
        if !ranged && !whole {
            return Err(RangeFailure::Transport);
        }
        let length = if whole { self.size } else { length };
        let declared = resp
            .headers()
            .get("Content-Length")
            .and_then(|value| value.to_str().ok())
            .map(str::parse::<u64>);
        if declared.is_some_and(|declared| declared != Ok(length)) {
            return Err(RangeFailure::Transport);
        }
        // One byte past the expected length, so a body that does not end
        // there is seen rather than silently cut off.
        let body = resp
            .into_parts()
            .1
            .into_with_config()
            .limit(length.saturating_add(1))
            .reader();
        self.range = Some((body, length));
        Ok(())
    }
}

impl Read for RangedBlobReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if buf.is_empty() {
                return Ok(0);
            }
            if let Some((body, remaining)) = &mut self.range {
                let want = buf
                    .len()
                    .min(usize::try_from(*remaining).unwrap_or(usize::MAX));
                let read = body.read(&mut buf[..want])?;
                if read == 0 {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                }
                *remaining -= read as u64;
                self.offset += read as u64;
                if *remaining == 0 {
                    // The body must end exactly here.
                    if body.read(&mut [0_u8])? != 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "blob body is longer than declared",
                        ));
                    }
                    self.range = None;
                }
                return Ok(read);
            }
            if self.offset >= self.size {
                return Ok(0);
            }
            self.open_next_range()
                .map_err(|_| std::io::Error::other("blob download failed"))?;
        }
    }
}

/// Yields exactly `remaining` bytes or fails. A body that ends short of its
/// declared `Content-Length` would otherwise leave the server waiting for the
/// rest until the request times out.
struct ExactChunk<'a, R> {
    inner: &'a mut R,
    remaining: u64,
}

impl<R: Read> Read for ExactChunk<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let want = buf
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let read = self.inner.read(&mut buf[..want])?;
        if read == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        self.remaining -= read as u64;
        Ok(read)
    }
}

/// Hashes and counts everything read through it.
pub struct DigestingReader<R> {
    inner: R,
    hasher: Sha256,
    bytes: u64,
}

impl<R: Read> DigestingReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    /// The `sha256:<hex>` digest and length of the bytes read so far.
    #[must_use]
    pub fn finish(self) -> (String, u64) {
        (
            format!("sha256:{}", encode_hex(self.hasher.finalize())),
            self.bytes,
        )
    }
}

impl<R: Read> Read for DigestingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        self.bytes += read as u64;
        Ok(read)
    }
}

/// Digest and measure a stream using one fixed `buffer_bytes` buffer.
pub fn digest_reader(
    source: &mut impl Read,
    buffer_bytes: usize,
) -> std::io::Result<(String, u64)> {
    let mut reader = DigestingReader::new(source);
    let mut buffer = vec![0_u8; buffer_bytes.max(1)];
    while reader.read(&mut buffer)? != 0 {}
    Ok(reader.finish())
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
pub fn evaluate_portable_oci_signature_state(
    registry: &dyn PortableOciRegistry,
    repository: &str,
    subject_manifest_digest: &str,
    package_digest: &str,
    policy: &PortableOciAuthenticityPolicy,
) -> Result<PortableOciSignatureState, PortableError> {
    evaluate_signature_state(
        registry,
        repository,
        subject_manifest_digest,
        package_digest,
        policy,
    )
}

pub fn attach_signature_referrer(
    registry: &dyn PortableOciRegistry,
    repository: &str,
    subject_digest: &str,
    package_digest: &str,
    material: &PortableOciSignatureMaterial,
    bytes_transferred: &mut u64,
    progress: &mut impl FnMut(PortableOciProgress),
) -> Result<(), PortableError> {
    if material.signer.is_empty() || material.key_id.is_empty() || material.secret.is_empty() {
        return Err(PortableError::new(
            PortableErrorCode::InvalidPath,
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
        PortableError::new(PortableErrorCode::Io, "failed to encode signature payload")
    })?;
    let payload_digest = digest_sha256(&payload_bytes);
    registry.put_blob(repository, &payload_digest, &payload_bytes)?;
    *bytes_transferred += payload_bytes.len() as u64;
    progress(PortableOciProgress {
        phase: PortableOciPhase::UploadBlob,
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
        PortableError::new(PortableErrorCode::Io, "failed to encode signature referrer")
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
    registry: &dyn PortableOciRegistry,
    repository: &str,
    subject_digest: &str,
    package_digest: &str,
    policy: &PortableOciAuthenticityPolicy,
) -> Result<PortableOciSignatureState, PortableError> {
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
            return Ok(PortableOciSignatureState::Valid);
        }
        invalid = true;
    }
    if !saw_signature {
        return Ok(PortableOciSignatureState::Absent);
    }
    if mismatched {
        return Ok(PortableOciSignatureState::PolicyMismatched);
    }
    if invalid {
        return Ok(PortableOciSignatureState::Invalid);
    }
    Ok(PortableOciSignatureState::Absent)
}

#[must_use]
pub fn authenticity_error(state: PortableOciSignatureState) -> PortableError {
    match state {
        PortableOciSignatureState::Absent => PortableError::new(
            PortableErrorCode::Incompatible,
            "authenticity policy requires a named signer but signature is absent",
        ),
        PortableOciSignatureState::Invalid => PortableError::new(
            PortableErrorCode::Incompatible,
            "authenticity policy rejected an invalid signature",
        ),
        PortableOciSignatureState::PolicyMismatched => PortableError::new(
            PortableErrorCode::Incompatible,
            "authenticity policy mismatched the attached signer",
        ),
        PortableOciSignatureState::Valid => PortableError::new(
            PortableErrorCode::Incompatible,
            "authenticity policy unexpectedly failed a valid signature",
        ),
    }
}

fn signature_mac(secret: &[u8], signer: &str, subject: &str, package_digest: &str) -> String {
    let mut hasher = graphforge_core::hash_observation::ControlSha256::new();
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
pub fn package_class_str(class: PortablePackageClass) -> &'static str {
    match class {
        PortablePackageClass::Complete => "complete",
        PortablePackageClass::OntologyOnly => "ontology-only",
        PortablePackageClass::ComponentSelective => "component-selective",
        PortablePackageClass::GraphDataSubset => "graph-data-subset",
    }
}

pub fn validate_repository(repository: &str) -> Result<(), PortableError> {
    if repository.is_empty()
        || repository.len() > 255
        || repository.contains("..")
        || repository.starts_with('/')
        || repository.contains('\\')
        || repository.contains('\n')
    {
        return Err(PortableError::new(
            PortableErrorCode::InvalidPath,
            "invalid repository name",
        ));
    }
    Ok(())
}

pub fn validate_digest(digest: &str) -> Result<(), PortableError> {
    if !digest.starts_with("sha256:") || digest.len() != 71 {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "digest must be sha256:<64 hex>",
        ));
    }
    if !digest[7..].bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "digest must be sha256:<64 hex>",
        ));
    }
    Ok(())
}

pub fn validate_reference(reference: &str) -> Result<(), PortableError> {
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
        return Err(PortableError::new(
            PortableErrorCode::InvalidPath,
            "invalid tag reference",
        ));
    }
    Ok(())
}

pub fn check_cancelled(cancelled: Option<&AtomicBool>) -> Result<(), PortableError> {
    if cancelled.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
        return Err(PortableError::new(
            PortableErrorCode::Cancelled,
            "portable OCI operation cancelled",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
