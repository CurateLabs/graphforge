//! Temporary-file-backed reference Hub implementing publish and bounded reads.
//!
//! [`ReferenceHub::handle`] maps one [`HubRequest`] to one [`HubResponse`]
//! under a single lock, so every commit is one atomic critical section. It is
//! a conformance target, not a production Hub: identifiers derive from a
//! configurable seed and are predictable. Control state is in memory; object
//! bytes spill to unlinked temporary files.

mod retention;

use crate::canonical::{sha256_digest, validate_digest};
use crate::error::{HubPublishError, HubPublishErrorCode, invalid};
use crate::exchange::{HubExchange, HubMethod, HubRequest, HubResponse};
use crate::wire::{
    Capability, CommitRequest, DEVICE_CODE_GRANT_TYPE, DIGEST_PLACEHOLDER,
    DeviceAuthorizationResponse, HEX_PLACEHOLDER, HUB_PUBLISH_FORMAT, OAuthErrorBody,
    OpenSessionRequest, OperationStatus, PUBLISH_NEGOTIATION_CAPABILITY, PublishAuthorization,
    PublishCapabilities, PublishIntent, PublishLimits, PublishReceipt, PublishSessionId,
    SessionResponse, TokenResponse, UPLOAD_LENGTH_HEADER, UPLOAD_OFFSET_HEADER, UploadStatus,
    UploadTarget, parse_content_range, publish_scope,
};
use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ProtocolVersion,
    RefSet, RepositoryIdentity, RepositoryRef, Sha256Digest,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::sync::Arc;
use std::sync::Mutex;
use url::Url;
use uuid::Uuid;

/// Reference Hub configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceHubConfig {
    /// Advertised and enforced OAuth client identifier.
    pub authorization_client_id: Option<String>,
    /// Advertise algorithm-separated object paths with `{hex}`.
    pub object_location_hex: bool,
    /// HTTPS origin the Hub serves, without a path (e.g. `https://hub.example`).
    pub base_url: String,
    /// Opt into bounded asynchronous commit and uniform multipart uploads.
    pub large_objects: bool,
    /// Advertised and enforced upload bounds.
    pub limits: PublishLimits,
    /// Lifetime of issued publish tokens.
    pub token_ttl_seconds: u64,
    /// Lifetime of device codes.
    pub device_code_ttl_seconds: u64,
    /// Initial minimum device-code poll interval.
    pub device_poll_interval_seconds: u64,
    /// Seed for session, upload, device, and token identifiers.
    pub id_seed: String,
}

impl Default for ReferenceHubConfig {
    fn default() -> Self {
        Self {
            authorization_client_id: None,
            large_objects: false,
            object_location_hex: false,
            base_url: "https://hub.example".to_owned(),
            limits: PublishLimits {
                max_object_bytes: 64 * 1024 * 1024,
                max_objects: 1024,
                max_session_bytes: 256 * 1024 * 1024,
                max_chunk_bytes: 8 * 1024 * 1024,
                preferred_chunk_bytes: None,
                max_document_bytes: 4 * 1024 * 1024,
            },
            token_ttl_seconds: 900,
            device_code_ttl_seconds: 600,
            device_poll_interval_seconds: 5,
            id_seed: "graphforge-reference-hub".to_owned(),
        }
    }
}

/// Reference implementation of publish and bounded buffered read HTTP mapping.
pub struct ReferenceHub {
    config: ReferenceHubConfig,
    base: Url,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    now: u64,
    counter: u64,
    tokens: BTreeMap<String, IssuedToken>,
    devices: BTreeMap<String, DeviceGrant>,
    sessions: BTreeMap<String, Session>,
    operations: BTreeMap<Uuid, String>,
    uploads: BTreeMap<String, (String, usize)>,
    repositories: BTreeMap<RepositoryIdentity, Repository>,
    moved: BTreeMap<RepositoryIdentity, RepositoryIdentity>,
    quotas: BTreeMap<String, u64>,
}

struct IssuedToken {
    scope: String,
    expires_at: u64,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum DeviceStatus {
    Pending,
    Approved,
    Denied,
    Issued,
}

struct DeviceGrant {
    user_code: String,
    scope: String,
    expires_at: u64,
    interval: u64,
    last_poll: Option<u64>,
    status: DeviceStatus,
}

struct Session {
    request: OpenSessionRequest,
    uploads: Vec<Upload>,
    receipt: Option<PublishReceipt>,
    pending: Option<PendingCommit>,
    verified: bool,
}

struct PendingCommit {
    request: HubRequest,
    object: usize,
    offset: u64,
    hash: Sha256,
}

struct Upload {
    id: String,
    bytes: Blob,
    complete: bool,
}

/// Reference storage spills to an unlinked temporary file. Zero extents stay sparse;
/// ordinary bytes use the same reader and SHA-256 path, without object-sized buffers.
#[derive(Clone)]
struct Blob {
    file: Option<Arc<std::fs::File>>,
    length: u64,
    hash: Sha256,
    intact: Arc<std::sync::atomic::AtomicBool>,
}
impl Default for Blob {
    fn default() -> Self {
        Self {
            file: None,
            length: 0,
            hash: Sha256::new(),
            intact: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }
}
impl Blob {
    fn len(&self) -> u64 {
        self.length
    }
    fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if self.file.is_none() {
            self.file = Some(Arc::new(tempfile::tempfile()?));
        }
        let mut file = self.file.as_ref().expect("created above").try_clone()?;
        file.seek(SeekFrom::Start(self.length))?;
        if bytes.iter().all(|b| *b == 0) {
            file.set_len(self.length + bytes.len() as u64)?;
        } else {
            file.write_all(bytes)?;
        }
        self.length += bytes.len() as u64;
        self.hash.update(bytes);
        Ok(())
    }
    fn read(&self, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
        let mut file = self
            .file
            .as_ref()
            .ok_or_else(|| std::io::Error::other("missing reference storage"))?
            .try_clone()?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
    fn corrupt(&self) -> bool {
        if self.length == 0 {
            return false;
        }
        let Some(file) = &self.file else {
            return false;
        };
        let Ok(mut file) = file.try_clone() else {
            return false;
        };
        let Ok(mut bytes) = self.read(0, 1) else {
            return false;
        };
        bytes[0] ^= 0xff;
        self.intact
            .store(false, std::sync::atomic::Ordering::Relaxed);
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.write_all(&bytes))
            .is_ok()
    }
}
const VERIFY_BYTES_PER_POLL: u64 = 64 * 1024 * 1024;
const REFERENCE_READ_LIMIT: u64 = 95 * 1024 * 1024;
fn hash_digest(hash: Sha256) -> String {
    let mut output = String::from("sha256:");
    for byte in hash.finalize() {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("string write");
    }
    output
}
fn storage_failure(_: std::io::Error) -> HubPublishError {
    HubPublishError::new(
        HubPublishErrorCode::Internal,
        "reference storage unavailable",
    )
}
fn storage_error(error: std::io::Error) -> Reject {
    storage_failure(error).into()
}
fn bounded_blob_read(blob: &Blob, limit: usize) -> Result<Vec<u8>, HubPublishError> {
    if blob.len() > limit as u64 {
        return Err(invalid("control object byte budget exceeded"));
    }
    let length =
        usize::try_from(blob.len()).map_err(|_| invalid("control object byte budget exceeded"))?;
    blob.read(0, length).map_err(storage_failure)
}
fn stored_control<'a>(
    repository: &'a Repository,
    object: &graphforge_discovery::ObjectDescriptor,
    limit: usize,
) -> Result<&'a Blob, HubPublishError> {
    let blob = repository
        .objects
        .get(&object.digest.0)
        .ok_or_else(|| invalid("stored control object is absent"))?;
    if blob.len() != object.length || object.length > limit as u64 {
        return Err(invalid("stored control object length is invalid"));
    }
    Ok(blob)
}

struct Repository {
    default_ref: String,
    refs: BTreeMap<String, RepositoryRef>,
    revision: String,
    refs_document: Vec<u8>,
    manifests: BTreeMap<String, Vec<u8>>,
    objects: BTreeMap<String, Blob>,
    history: Vec<String>,
    retirement: retention::Retirement,
}

struct Reject {
    status: u16,
    error: HubPublishError,
    headers: Vec<(String, String)>,
}

impl From<HubPublishError> for Reject {
    fn from(error: HubPublishError) -> Self {
        Self {
            status: error.code.http_status(),
            error,
            headers: Vec::new(),
        }
    }
}

fn reject(status: u16, code: HubPublishErrorCode, detail: &'static str) -> Reject {
    Reject {
        status,
        error: HubPublishError::new(code, detail),
        headers: Vec::new(),
    }
}

fn not_found(detail: &'static str) -> Reject {
    reject(404, HubPublishErrorCode::InvalidInput, detail)
}

fn too_large(detail: &'static str) -> Reject {
    reject(413, HubPublishErrorCode::InvalidInput, detail)
}

fn integrity(detail: &'static str) -> Reject {
    HubPublishError::new(HubPublishErrorCode::IntegrityFailure, detail).into()
}

type Handled = Result<HubResponse, Reject>;

fn json_response<T: Serialize>(status: u16, value: &T) -> HubResponse {
    HubResponse {
        status,
        headers: vec![
            ("content-type".to_owned(), "application/json".to_owned()),
            ("cache-control".to_owned(), "no-store".to_owned()),
        ],
        body: serde_json::to_vec(value).expect("publish documents serialize"),
    }
}

fn oauth_error(error: &str) -> HubResponse {
    json_response(
        400,
        &OAuthErrorBody {
            error: error.to_owned(),
        },
    )
}

fn quoted(validator: &str) -> String {
    format!("\"{validator}\"")
}

fn rehome_inputs(
    repository: &Repository,
    limits: DiscoveryLimits,
) -> Result<(RefSet, Vec<graphforge_discovery::RepositorySnapshot>), HubPublishError> {
    let refs = RefSet::from_json(&repository.refs_document, limits)
        .map_err(|_| invalid("stored refs are invalid"))?;
    let validators = refs
        .refs
        .iter()
        .map(|item| item.validator.0.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut pending = Vec::new();
    let mut bytes = repository.refs_document.len() as u64;
    for validator in validators {
        let raw = repository
            .manifests
            .get(&validator)
            .ok_or_else(|| invalid("referenced manifest is absent"))?;
        let manifest = DiscoveryManifest::from_json(raw, limits)
            .map_err(|_| invalid("stored manifest is invalid"))?;
        let summary = if manifest.summary.is_some() {
            Some(stored_control(
                repository,
                manifest
                    .summary_object()
                    .map_err(|_| invalid("stored summary is invalid"))?,
                limits.max_response_bytes,
            )?)
        } else {
            None
        };
        let lineage = if manifest.lineage.is_some() {
            Some(stored_control(
                repository,
                manifest
                    .lineage_object()
                    .map_err(|_| invalid("stored lineage is invalid"))?,
                limits.max_response_bytes,
            )?)
        } else {
            None
        };
        bytes = bytes
            .checked_add(raw.len() as u64)
            .and_then(|n| n.checked_add(summary.map_or(0, Blob::len)))
            .and_then(|n| n.checked_add(lineage.map_or(0, Blob::len)))
            .ok_or_else(|| invalid("rehome byte budget exceeded"))?;
        if bytes > limits.max_response_bytes as u64 {
            return Err(invalid("rehome byte budget exceeded"));
        }
        pending.push((manifest, summary, lineage));
    }
    // Admit the complete aggregate before materializing any file-backed control.
    let snapshots = pending
        .into_iter()
        .map(|(manifest, summary, lineage)| {
            Ok(graphforge_discovery::RepositorySnapshot {
                manifest,
                summary: summary
                    .map(|blob| bounded_blob_read(blob, limits.max_response_bytes))
                    .transpose()?,
                lineage: lineage
                    .map(|blob| bounded_blob_read(blob, limits.max_response_bytes))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, HubPublishError>>()?;
    Ok((refs, snapshots))
}

impl ReferenceHub {
    /// Hub with the default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(ReferenceHubConfig::default()).expect("default config is valid")
    }

    /// Hub with `config`; `base_url` must be an HTTPS origin without a path.
    pub fn with_config(config: ReferenceHubConfig) -> Result<Self, HubPublishError> {
        let base = crate::wire::validate_https_url(&config.base_url)?;
        if base.path() != "/" || config.base_url.ends_with('/') {
            return Err(invalid("reference hub base URL must be an origin"));
        }
        let hub = Self {
            config,
            base,
            state: Mutex::new(State::default()),
        };
        hub.capabilities(&RepositoryIdentity::parse("validation/probe").expect("static identity"))
            .validate()?;
        Ok(hub)
    }

    /// Repository URL, e.g. `https://hub.example/openalex/demo`.
    #[must_use]
    pub fn repository_url(&self, repository: &RepositoryIdentity) -> String {
        format!(
            "{}/{}/{}",
            self.config.base_url, repository.owner, repository.repository
        )
    }

    /// Capabilities document this Hub serves for `repository`.
    #[must_use]
    pub fn capabilities(&self, repository: &RepositoryIdentity) -> PublishCapabilities {
        let mut requirements = vec![
            Capability::new("publish-session", 1),
            Capability::new("resumable-upload", 1),
        ];
        if self.config.authorization_client_id.is_some()
            || self.config.object_location_hex
            || self.config.limits.preferred_chunk_bytes.is_some()
        {
            requirements.push(Capability::new(PUBLISH_NEGOTIATION_CAPABILITY, 1));
        }
        if self.config.large_objects {
            requirements.push(Capability::new("large-object-publish", 1));
        }
        PublishCapabilities {
            format: HUB_PUBLISH_FORMAT.to_owned(),
            repository: repository.clone(),
            requirements,
            capabilities: vec![
                Capability::new("device-authorization", 1),
                Capability::new("version-previews", 1),
                Capability::new(crate::HISTORY_MUTATION_CAPABILITY, 1),
            ],
            authorization: PublishAuthorization {
                client_id: self.config.authorization_client_id.clone(),
                device_authorization_endpoint: format!(
                    "{}/.gf/oauth/device_authorization",
                    self.config.base_url
                ),
                token_endpoint: format!("{}/.gf/oauth/token", self.config.base_url),
                scope: publish_scope(repository),
            },
            object_location_template: if self.config.object_location_hex {
                format!(
                    "{}/.gf/objects/sha256/{HEX_PLACEHOLDER}",
                    self.repository_url(repository)
                )
            } else {
                format!(
                    "{}/.gf/objects/{DIGEST_PLACEHOLDER}",
                    self.repository_url(repository)
                )
            },
            limits: self.config.limits,
        }
    }

    /// Handle one request. Every state change happens under one lock.
    pub fn handle(&self, request: &HubRequest) -> HubResponse {
        let Ok(mut state) = self.state.lock() else {
            let error = HubPublishError::new(HubPublishErrorCode::Internal, "hub state poisoned");
            return json_response(500, &error.body());
        };
        retention::collect(&mut state);
        match self.route(&mut state, request) {
            Ok(response) => response,
            Err(rejected) => {
                let mut response = json_response(rejected.status, &rejected.error.body());
                response.headers.extend(rejected.headers);
                response
            }
        }
    }

    fn route(&self, state: &mut State, request: &HubRequest) -> Handled {
        let url = Url::parse(&request.url).map_err(|_| invalid("request URL is invalid"))?;
        if url.origin() != self.base.origin() {
            return Err(not_found("resource is not served by this hub"));
        }
        let segments: Vec<&str> = url.path_segments().into_iter().flatten().collect();
        match (request.method, segments.as_slice()) {
            (HubMethod::Post, [".gf", "oauth", "device_authorization"]) => {
                Ok(self.device_authorization(state, request))
            }
            (HubMethod::Post, [".gf", "oauth", "token"]) => Ok(self.token(state, request)),
            (HubMethod::Head, [".gf", "uploads", id]) => upload_head(state, id),
            (HubMethod::Put, [".gf", "uploads", id]) => self.upload_put(state, id, request),
            (method, [owner, repository, ".gf", rest @ ..]) => {
                let identity = RepositoryIdentity {
                    owner: (*owner).to_owned(),
                    repository: (*repository).to_owned(),
                };
                if identity.validate().is_err() {
                    return Err(not_found("repository identity is invalid"));
                }
                if let Some(current) = state.moved.get(&identity) {
                    // Immutable object URLs survive a move. Only repository
                    // control endpoints redirect; no write is ever replayed.
                    match rest {
                        ["objects", "sha256", hex] => {
                            return read_object(state, current, &format!("sha256:{hex}"), request);
                        }
                        ["objects", digest] => return read_object(state, current, digest, request),
                        ["refs" | "manifest"] | ["publish", ..] => {
                            let moved = graphforge_discovery::RepositoryMoved {
                                format: "graphforge-repository-moved/1".into(),
                                moved_from: identity,
                                moved_to: current.clone(),
                            };
                            let mut response = json_response(308, &moved);
                            response.headers.push((
                                "location".into(),
                                format!("{}/.gf/{}", self.repository_url(current), rest.join("/")),
                            ));
                            return Ok(response);
                        }
                        _ => return Err(not_found("resource does not exist")),
                    }
                }
                match (method, rest) {
                    (HubMethod::Get, ["publish"]) => {
                        Ok(json_response(200, &self.capabilities(&identity)))
                    }
                    (HubMethod::Post, ["publish", "sessions"]) => {
                        self.open_session(state, &identity, request)
                    }
                    (HubMethod::Post, ["publish", "sessions", id, "commit"]) => {
                        self.commit(state, &identity, id, request)
                    }
                    (HubMethod::Get, ["publish", "operations", id]) => {
                        self.operation_status(state, &identity, id, request)
                    }
                    (HubMethod::Get, ["refs"]) => read_refs(state, &identity),
                    (HubMethod::Get, ["manifest"]) => read_manifest(state, &identity),
                    (HubMethod::Get, ["objects", "sha256", hex]) => {
                        read_object(state, &identity, &format!("sha256:{hex}"), request)
                    }
                    (HubMethod::Get, ["objects", digest]) => {
                        read_object(state, &identity, digest, request)
                    }
                    _ => Err(not_found("resource does not exist")),
                }
            }
            _ => Err(not_found("resource does not exist")),
        }
    }

    fn next_id(&self, state: &mut State, kind: &str) -> String {
        state.counter += 1;
        let digest =
            sha256_digest(format!("{}\0{kind}\0{}", self.config.id_seed, state.counter).as_bytes());
        digest[7..39].to_owned()
    }

    fn authorize(
        state: &State,
        request: &HubRequest,
        repository: &RepositoryIdentity,
    ) -> Result<(), Reject> {
        let Some(token) = request
            .header("authorization")
            .and_then(|value| value.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, token)| token.trim())
        else {
            let mut rejected = reject(
                401,
                HubPublishErrorCode::AuthDenied,
                "publish credential is missing",
            );
            rejected.headers.push((
                "www-authenticate".to_owned(),
                "Bearer realm=\"graphforge-hub\"".to_owned(),
            ));
            return Err(rejected);
        };
        let Some(issued) = state
            .tokens
            .get(token)
            .filter(|issued| state.now < issued.expires_at)
        else {
            let mut rejected = reject(
                401,
                HubPublishErrorCode::AuthDenied,
                "publish credential is invalid or expired",
            );
            rejected.headers.push((
                "www-authenticate".to_owned(),
                "Bearer error=\"invalid_token\"".to_owned(),
            ));
            return Err(rejected);
        };
        if issued.scope != publish_scope(repository) {
            let mut rejected = reject(
                403,
                HubPublishErrorCode::AuthDenied,
                "publish credential is not scoped to this repository",
            );
            rejected.headers.push((
                "www-authenticate".to_owned(),
                "Bearer error=\"insufficient_scope\"".to_owned(),
            ));
            return Err(rejected);
        }
        Ok(())
    }

    fn check_document_size(&self, request: &HubRequest) -> Result<(), Reject> {
        if u64::try_from(request.body.len()).unwrap_or(u64::MAX)
            > self.config.limits.max_document_bytes
        {
            return Err(too_large(
                "request body exceeds the advertised document limit",
            ));
        }
        Ok(())
    }

    fn check_large_inventory(&self, open: &OpenSessionRequest) -> Result<(), Reject> {
        let limits = self.config.limits;
        if self.config.large_objects {
            if !open
                .requirements
                .iter()
                .any(|c| c.capability == "large-object-publish" && c.major == 1)
            {
                return Err(invalid("large-object capability must be negotiated").into());
            }
            let chunk = limits
                .preferred_chunk_bytes
                .ok_or_else(|| invalid("uniform upload chunk must be configured"))?;
            if chunk > 95 * 1024 * 1024
                || open
                    .objects
                    .iter()
                    .any(|o| o.length.div_ceil(chunk) > 10_000)
            {
                return Err(too_large("object exceeds the uniform multipart ceiling"));
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn open_session(
        &self,
        state: &mut State,
        repository: &RepositoryIdentity,
        request: &HubRequest,
    ) -> Handled {
        Self::authorize(state, request, repository)?;
        self.check_document_size(request)?;
        let open = OpenSessionRequest::from_json(&request.body)?;
        if &open.repository != repository {
            return Err(invalid("request repository does not match the URL").into());
        }
        let limits = self.config.limits;
        if u64::try_from(open.objects.len()).unwrap_or(u64::MAX) > limits.max_objects {
            return Err(too_large("object count exceeds the advertised limit"));
        }
        self.check_large_inventory(&open)?;
        let mut total = 0_u64;
        for object in &open.objects {
            if object.length > limits.max_object_bytes {
                return Err(too_large(
                    "declared object length exceeds the advertised limit",
                ));
            }
            total = total.saturating_add(object.length);
        }
        if total > limits.max_session_bytes {
            return Err(too_large(
                "declared session length exceeds the advertised limit",
            ));
        }

        // An operation is one content identity (its intent); a session is one
        // attempt at committing it. A different intent conflicts, a committed
        // operation replays its receipt, the same request resumes its session,
        // and a new attempt of the same intent supersedes the open session.
        let superseded = match state.operations.get(&open.operation_uuid) {
            Some(session_id) => {
                let session = &state.sessions[session_id];
                if session.request.intent_digest != open.intent_digest
                    || session.request.repository != open.repository
                {
                    return Err(HubPublishError::new(
                        HubPublishErrorCode::IdempotencyConflict,
                        "operation identity was reused with a different intent",
                    )
                    .into());
                }
                if let Some(receipt) = &session.receipt {
                    return Ok(json_response(
                        200,
                        &SessionResponse::Complete {
                            receipt: receipt.clone(),
                        },
                    ));
                }
                if session.request == open {
                    let session_id = session_id.clone();
                    let session = state
                        .sessions
                        .get_mut(&session_id)
                        .expect("operation session");
                    let mut reset = false;
                    for upload in &mut session.uploads {
                        if !upload
                            .bytes
                            .intact
                            .load(std::sync::atomic::Ordering::Relaxed)
                        {
                            upload.bytes = Blob::default();
                            upload.complete = false;
                            reset = true;
                        }
                    }
                    if reset {
                        session.pending = None;
                        session.verified = false;
                    }
                    return Ok(json_response(
                        200,
                        &self.open_response(&session_id, session),
                    ));
                }
                Some(session_id.clone())
            }
            None => None,
        };

        let admitted = state.repositories.get(repository);
        if let Some(quota) = state.quotas.get(&repository.owner) {
            let used: u64 = state
                .repositories
                .iter()
                .filter(|(identity, _)| identity.owner == repository.owner)
                .flat_map(|(_, stored)| stored.objects.values())
                .map(Blob::len)
                .sum();
            let added: u64 = open
                .objects
                .iter()
                .filter(|object| admitted.is_none_or(|r| !r.objects.contains_key(&object.digest)))
                .map(|object| object.length)
                .sum();
            if used.saturating_add(added) > *quota {
                return Err(HubPublishError::new(
                    HubPublishErrorCode::EntitlementDenied,
                    "publication exceeds the owner's storage entitlement",
                )
                .into());
            }
        }

        let prior = initial_uploads(state, repository, &open, superseded.as_deref());
        let session_id = self.next_id(state, "session");
        let mut uploads = Vec::with_capacity(open.objects.len());
        for (index, (bytes, complete)) in prior.into_iter().enumerate() {
            let id = self.next_id(state, "upload");
            state
                .uploads
                .insert(id.clone(), (session_id.clone(), index));
            uploads.push(Upload {
                id,
                bytes,
                complete,
            });
        }
        state
            .operations
            .insert(open.operation_uuid, session_id.clone());
        let session = Session {
            request: open,
            uploads,
            receipt: None,
            pending: None,
            verified: false,
        };
        let response = self.open_response(&session_id, &session);
        state.sessions.insert(session_id, session);
        Ok(json_response(201, &response))
    }

    /// Report an operation's intent and, once committed, its original receipt.
    fn operation_status(
        &self,
        state: &mut State,
        repository: &RepositoryIdentity,
        operation: &str,
        request: &HubRequest,
    ) -> Handled {
        Self::authorize(state, request, repository)?;
        let unknown = || not_found("publish operation is unknown");
        let operation_uuid = Uuid::parse_str(operation).map_err(|_| unknown())?;
        let session_id = state
            .operations
            .get(&operation_uuid)
            .cloned()
            .ok_or_else(unknown)?;
        let session = state
            .sessions
            .get(&session_id)
            .filter(|s| &s.request.repository == repository)
            .ok_or_else(unknown)?;
        if session.pending.is_some() {
            self.advance_verification(state, repository, &session_id, request)?;
        }
        let session = state
            .operations
            .get(&operation_uuid)
            .and_then(|session_id| state.sessions.get(session_id))
            .filter(|session| &session.request.repository == repository)
            .ok_or_else(unknown)?;
        Ok(json_response(
            200,
            &OperationStatus {
                format: HUB_PUBLISH_FORMAT.to_owned(),
                operation_uuid,
                repository: repository.clone(),
                intent_digest: session.request.intent_digest.clone(),
                receipt: session.receipt.clone(),
            },
        ))
    }

    fn advance_verification(
        &self,
        state: &mut State,
        repository: &RepositoryIdentity,
        id: &str,
        request: &HubRequest,
    ) -> Result<(), Reject> {
        // Each poll authenticates anew. Only bounded retained bytes are hashed;
        // refs are untouched until all verification and the final CAS succeed.
        Self::authorize(state, request, repository)?;
        let session = state.sessions.get_mut(id).expect("operation session");
        let mut pending = session.pending.take().expect("pending commit");
        let mut remaining = VERIFY_BYTES_PER_POLL;
        while pending.object < session.uploads.len() && remaining > 0 {
            let object = &session.request.objects[pending.object];
            let upload = &session.uploads[pending.object];
            if !upload.complete || upload.bytes.len() != object.length {
                return Err(integrity("an inventory object is missing or incomplete"));
            }
            let count = remaining
                .min(object.length - pending.offset)
                .min(1024 * 1024);
            let bytes = upload
                .bytes
                .read(pending.offset, count as usize)
                .map_err(storage_error)?;
            pending.hash.update(&bytes);
            pending.offset += count;
            remaining -= count;
            if pending.offset == object.length {
                if hash_digest(pending.hash.clone()) != object.digest {
                    upload
                        .bytes
                        .intact
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                    return Err(integrity("an inventory object failed digest verification"));
                }
                pending.object += 1;
                pending.offset = 0;
                pending.hash = Sha256::new();
            }
        }
        if pending.object == session.uploads.len() {
            session.verified = true;
            // Preserve fresh authentication headers rather than a stale bearer.
            pending.request.headers.clone_from(&request.headers);
            let result = self.commit(state, repository, id, &pending.request);
            if result.is_err() {
                state
                    .sessions
                    .get_mut(id)
                    .expect("operation session")
                    .verified = false;
            }
            result?;
        } else {
            session.pending = Some(pending);
        }
        Ok(())
    }

    fn open_response(&self, session_id: &str, session: &Session) -> SessionResponse {
        SessionResponse::Open {
            session_id: PublishSessionId::parse(session_id).expect("hub ids are valid"),
            uploads: session
                .request
                .objects
                .iter()
                .zip(&session.uploads)
                .map(|(object, upload)| UploadTarget {
                    digest: object.digest.clone(),
                    length: object.length,
                    upload_url: format!("{}/.gf/uploads/{}", self.config.base_url, upload.id),
                    received: upload.bytes.len(),
                    chunk_bytes: session
                        .request
                        .requirements
                        .iter()
                        .any(|capability| {
                            capability.capability == PUBLISH_NEGOTIATION_CAPABILITY
                                && capability.major == 1
                        })
                        .then_some(self.config.limits.preferred_chunk_bytes)
                        .flatten()
                        .map(|bytes| bytes.min(object.length)),
                })
                .collect(),
        }
    }

    fn upload_put(&self, state: &mut State, id: &str, request: &HubRequest) -> Handled {
        let (session_id, index) = state
            .uploads
            .get(id)
            .cloned()
            .ok_or_else(|| not_found("upload location is unknown or closed"))?;
        let session = state
            .sessions
            .get_mut(&session_id)
            .ok_or_else(|| not_found("upload location is unknown or closed"))?;
        let declared = session.request.objects[index].clone();
        let upload = &mut session.uploads[index];
        let (start, end, total) = request
            .header("content-range")
            .and_then(parse_content_range)
            .ok_or_else(|| invalid("Content-Range must be bytes <start>-<end>/<length>"))?;
        if total != declared.length {
            return Err(invalid("Content-Range length does not match the declared length").into());
        }
        let chunk = request.body.len() as u64;
        if end - start + 1 != chunk {
            return Err(invalid("Content-Range does not match the body length").into());
        }
        if chunk > self.config.limits.max_chunk_bytes {
            return Err(too_large("upload chunk exceeds the advertised limit"));
        }
        let received = upload.bytes.len();
        if upload.complete || start != received {
            let mut rejected = reject(
                416,
                HubPublishErrorCode::InvalidInput,
                "upload must resume at the received offset",
            );
            rejected
                .headers
                .push((UPLOAD_OFFSET_HEADER.to_owned(), received.to_string()));
            return Err(rejected);
        }
        if self.config.large_objects {
            let part = self
                .config
                .limits
                .preferred_chunk_bytes
                .expect("validated at open")
                .min(declared.length);
            if start % part != 0 || (end + 1 != total && chunk != part) || chunk > part {
                return Err(
                    invalid("nonfinal upload parts must use the negotiated uniform size").into(),
                );
            }
        }
        upload.bytes.append(&request.body).map_err(storage_error)?;

        if upload.bytes.len() == declared.length {
            if hash_digest(upload.bytes.hash.clone()) != declared.digest {
                upload.bytes = Blob::default();

                let mut rejected = integrity("uploaded bytes do not match the declared digest");
                rejected
                    .headers
                    .push((UPLOAD_OFFSET_HEADER.to_owned(), "0".to_owned()));
                return Err(rejected);
            }
            upload.complete = true;
        }
        let received = upload.bytes.len();
        let mut response = json_response(
            200,
            &UploadStatus {
                digest: declared.digest,
                length: declared.length,
                received,
            },
        );
        response
            .headers
            .push((UPLOAD_OFFSET_HEADER.to_owned(), received.to_string()));
        Ok(response)
    }

    #[allow(clippy::too_many_lines)]
    fn commit(
        &self,
        state: &mut State,
        repository: &RepositoryIdentity,
        session_id: &str,
        request: &HubRequest,
    ) -> Handled {
        Self::authorize(state, request, repository)?;
        self.check_document_size(request)?;
        let session_id = PublishSessionId::parse(session_id)
            .map_err(|_| not_found("publish session is unknown"))?;
        let session = state
            .sessions
            .get(session_id.as_str())
            .filter(|session| &session.request.repository == repository)
            .ok_or_else(|| not_found("publish session is unknown"))?;
        let commit = CommitRequest::from_json(&request.body)?;
        // Destructive semantics must be offered and required on this exact
        // authenticated session before even a committed receipt can replay.
        crate::require_history_capability(
            &self.capabilities(repository),
            commit.rewrite,
            commit.prune,
        )?;
        crate::require_history_requirement(
            &session.request.requirements,
            commit.rewrite,
            commit.prune,
        )?;

        // Structural manifest validation is pure; it precedes the replay check
        // only because the commitment binds the manifest's canonical digest.
        let limits = self.config.limits;
        let discovery_limits = DiscoveryLimits {
            max_response_bytes: usize::try_from(limits.max_document_bytes).unwrap_or(usize::MAX),
            max_objects: usize::try_from(limits.max_objects).unwrap_or(usize::MAX),
            max_cumulative_object_bytes: limits.max_session_bytes,
            ..DiscoveryLimits::default()
        };
        let manifest_bytes =
            serde_json::to_vec(&commit.manifest).map_err(|_| invalid("manifest is malformed"))?;
        let manifest = DiscoveryManifest::from_json(&manifest_bytes, discovery_limits).map_err(
            |error| -> Reject {
                if error.code == DiscoveryErrorCode::UnsupportedFuture {
                    HubPublishError::new(
                        HubPublishErrorCode::UnsupportedFuture,
                        "manifest requires an unsupported discovery version or capability",
                    )
                    .into()
                } else {
                    invalid("manifest is not a valid discovery manifest").into()
                }
            },
        )?;
        let manifest_document = manifest
            .to_canonical_json()
            .map_err(|_| invalid("manifest is not a valid discovery manifest"))?;
        let manifest_validator = sha256_digest(&manifest_document);
        let intent = PublishIntent {
            rewrite: commit.rewrite,
            prune: commit.prune,
            repository: repository.clone(),
            intent_digest: session.request.intent_digest.clone(),
            objects: session.request.objects.clone(),
            manifest_validator: manifest_validator.clone(),
            refs: commit.refs.clone(),
            expected_revision: commit.expected_revision.clone(),
        };

        // 1. Replay: the same operation with the same commitment returns the
        //    original receipt; changed content conflicts.
        if intent.request_commitment() != session.request.request_commitment {
            return Err(HubPublishError::new(
                HubPublishErrorCode::IdempotencyConflict,
                "operation identity was reused with a different request commitment",
            )
            .into());
        }
        if let Some(receipt) = &session.receipt {
            return Ok(json_response(
                200,
                &SessionResponse::Complete {
                    receipt: receipt.clone(),
                },
            ));
        }

        // The negotiated asynchronous path never hashes an entire object in
        // the commit request. The operation status endpoint advances it instead.
        if self.config.large_objects && !session.verified {
            if session
                .uploads
                .iter()
                .zip(&session.request.objects)
                .any(|(u, o)| !u.complete || u.bytes.len() != o.length)
            {
                return Err(integrity("an inventory object is missing or incomplete"));
            }
            let operation_uuid = session.request.operation_uuid;
            let session = state
                .sessions
                .get_mut(session_id.as_str())
                .expect("session");
            if session.pending.is_none() {
                session.pending = Some(PendingCommit {
                    request: request.clone(),
                    object: 0,
                    offset: 0,
                    hash: Sha256::new(),
                });
            }
            return Ok(json_response(
                202,
                &SessionResponse::Pending { operation_uuid },
            ));
        }
        if !self.config.large_objects {
            for (object, upload) in session.request.objects.iter().zip(&session.uploads) {
                if !upload.complete || upload.bytes.len() != object.length {
                    return Err(integrity("an inventory object is missing or incomplete"));
                }
                let mut hash = Sha256::new();
                let mut offset = 0;
                while offset < object.length {
                    let bytes = upload
                        .bytes
                        .read(offset, (object.length - offset).min(1024 * 1024) as usize)
                        .map_err(storage_error)?;
                    offset += bytes.len() as u64;
                    hash.update(bytes);
                }
                if hash_digest(hash) != object.digest {
                    upload
                        .bytes
                        .intact
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                    return Err(integrity("an inventory object failed digest verification"));
                }
            }
        }

        // Optional presentation bytes have a separate, closed Core admission
        // boundary. Digest validation alone cannot make arbitrary files images.
        let previews = manifest
            .preview_descriptors()
            .map_err(|_| invalid("preview descriptors are invalid"))?;
        for object in &session.request.objects {
            if matches!(object.media_type.as_str(), "image/png" | "image/svg+xml")
                && !previews.iter().any(|p| p.digest == object.digest)
            {
                return Err(invalid("image objects require Version preview descriptors").into());
            }
        }
        if !previews.is_empty() {
            if !session
                .request
                .requirements
                .iter()
                .any(|r| r.capability == "version-previews" && r.major == 1)
            {
                return Err(invalid("previews require version-previews capability").into());
            }
            let lineage_object = manifest
                .lineage_object()
                .map_err(|_| invalid("previews require native Version lineage"))?;
            let lineage_upload = session
                .request
                .objects
                .iter()
                .zip(&session.uploads)
                .find(|(object, _)| object.digest == lineage_object.digest.0)
                .ok_or_else(|| invalid("preview lineage is absent"))?;
            if lineage_upload.0.length != lineage_object.length
                || lineage_upload.1.bytes.len() != lineage_object.length
            {
                return Err(invalid("preview lineage length is invalid").into());
            }
            let lineage_bytes =
                bounded_blob_read(&lineage_upload.1.bytes, discovery_limits.max_response_bytes)?;
            let lineage =
                graphforge_discovery::ResearchLineage::from_json(&lineage_bytes, discovery_limits)
                    .map_err(|_| invalid("preview lineage is invalid"))?;
            let reference = manifest
                .lineage
                .as_ref()
                .ok_or_else(|| invalid("preview lineage reference is absent"))?;
            if lineage.repository != manifest.repository
                || lineage.immutable_version != manifest.immutable_version
                || lineage
                    .canonical_digest()
                    .map_err(|_| invalid("preview lineage digest is invalid"))?
                    != reference.lineage_digest
            {
                return Err(invalid("preview lineage does not bind this manifest").into());
            }
            for preview in &previews {
                if lineage.version(&preview.version_uuid).is_none() {
                    return Err(invalid("preview Version is absent from native lineage").into());
                }
                let upload = session
                    .request
                    .objects
                    .iter()
                    .zip(&session.uploads)
                    .find(|(object, _)| object.digest == preview.digest)
                    .ok_or_else(|| invalid("preview bytes are absent"))?;
                if upload.0.length != preview.length
                    || upload.1.bytes.len() != preview.length
                    || upload.0.media_type != preview.media_type
                {
                    return Err(invalid("preview object binding is invalid").into());
                }
                let bytes = bounded_blob_read(&upload.1.bytes, crate::preview::MAX_PREVIEW_BYTES)?;
                crate::preview::validate_preview(preview, &bytes)
                    .map_err(|_| invalid("preview bytes fail Core image admission"))?;
            }
        }

        // 3. The manifest names this repository and lists exactly the admitted
        //    objects at this Hub's locations.
        if &manifest.repository != repository {
            return Err(invalid("manifest repository does not match the URL").into());
        }
        let template = self.capabilities(repository);
        let exact =
            manifest.objects.len() == session.request.objects.len()
                && manifest.objects.iter().zip(&session.request.objects).all(
                    |(listed, admitted)| {
                        listed.digest.0 == admitted.digest
                            && listed.length == admitted.length
                            && listed.media_type == admitted.media_type
                            && listed.locations == [template.object_location(&admitted.digest)]
                    },
                );
        if !exact {
            return Err(invalid(
                "manifest objects must equal the session inventory at this hub's locations",
            )
            .into());
        }
        if !commit.refs.contains(&manifest.resolved_ref) {
            return Err(invalid("advanced refs must include the manifest's resolved ref").into());
        }

        // 4. Expected-revision precondition.
        let current = state.repositories.get(repository);
        match (current, &commit.expected_revision) {
            (None, None) => {}
            (Some(stored), Some(expected)) if &stored.revision == expected => {}
            _ => {
                return Err(HubPublishError::new(
                    HubPublishErrorCode::RefConflict,
                    "expected revision does not match the repository",
                )
                .into());
            }
        }
        // The summary's people list is derived from the lineage's credited Versions
        // (ADR 0055). Refuse a conflicting list when the publication is accepted,
        // including a summary published with no lineage, rather than storing it for
        // a later rehome to reject.
        if let Some(reference) = &manifest.summary {
            let upload = session
                .request
                .objects
                .iter()
                .zip(&session.uploads)
                .find(|(object, _)| object.digest == reference.object_digest.0)
                .ok_or_else(|| invalid("native summary object is absent"))?;
            let summary = graphforge_discovery::ProjectSummary::from_json(
                &bounded_blob_read(&upload.1.bytes, discovery_limits.max_summary_bytes)?,
                discovery_limits,
            )
            .map_err(|_| invalid("native summary is invalid"))?;
            let lineage = manifest
                .lineage
                .as_ref()
                .map(|lineage| {
                    let upload = session
                        .request
                        .objects
                        .iter()
                        .zip(&session.uploads)
                        .find(|(object, _)| object.digest == lineage.object_digest.0)
                        .ok_or_else(|| invalid("native lineage object is absent"))?;
                    graphforge_discovery::ResearchLineage::from_json(
                        &bounded_blob_read(&upload.1.bytes, discovery_limits.max_lineage_bytes)?,
                        discovery_limits,
                    )
                    .map_err(|_| invalid("native lineage is invalid"))
                })
                .transpose()?;
            summary.bind_lineage(lineage.as_ref()).map_err(|_| {
                invalid("summary people differ from the credited Versions of the research lineage")
            })?;
        }
        if let Some(reference) = &manifest.lineage {
            let upload = session
                .request
                .objects
                .iter()
                .zip(&session.uploads)
                .find(|(object, _)| object.digest == reference.object_digest.0)
                .ok_or_else(|| invalid("native lineage object is absent"))?;
            let next = graphforge_discovery::ResearchLineage::from_json(
                &bounded_blob_read(&upload.1.bytes, discovery_limits.max_lineage_bytes)?,
                discovery_limits,
            )
            .map_err(|_| invalid("native lineage is invalid"))?;
            // The exact manifest/session inventory was admitted above. Bind all
            // advertised native packages before transition checks or retirement;
            // merely retaining their lineage descriptors cannot retain bytes.
            manifest
                .bind_lineage_document(&next)
                .map_err(|_| invalid("native lineage does not bind this publication inventory"))?;
            if let Some(stored) = current {
                let mut checked = BTreeSet::new();
                for old_ref in stored.refs.values() {
                    if checked.insert(old_ref.validator.0.clone()) {
                        let old_manifest = DiscoveryManifest::from_json(
                            &stored.manifests[&old_ref.validator.0],
                            discovery_limits,
                        )
                        .map_err(|_| invalid("stored manifest is invalid"))?;
                        if old_manifest.lineage.is_some() {
                            let object = old_manifest
                                .lineage_object()
                                .map_err(|_| invalid("stored lineage is absent"))?;
                            let old = graphforge_discovery::ResearchLineage::from_json(
                                &bounded_blob_read(
                                    stored_control(
                                        stored,
                                        object,
                                        discovery_limits.max_lineage_bytes,
                                    )?,
                                    discovery_limits.max_lineage_bytes,
                                )?,
                                discovery_limits,
                            )
                            .map_err(|_| invalid("stored lineage is invalid"))?;
                            let authoritative_heads = stored
                                .refs
                                .iter()
                                .filter(|(_, reference)| reference.validator == old_ref.validator)
                                .map(|(name, _)| name.clone())
                                .collect::<Vec<_>>();
                            crate::history::validate_snapshot_history_transition(
                                &old,
                                &next,
                                &commit.refs,
                                &authoritative_heads,
                                commit.rewrite,
                                commit.prune,
                                discovery_limits,
                            )?;
                        }
                    }
                }
            }
        } else if current.is_some_and(|stored| {
            commit.refs.iter().any(|name| {
                stored.refs.get(name).is_some_and(|r| {
                    DiscoveryManifest::from_json(
                        &stored.manifests[&r.validator.0],
                        discovery_limits,
                    )
                    .is_ok_and(|m| m.lineage.is_some())
                })
            })
        }) {
            return Err(HubPublishError::new(
                HubPublishErrorCode::NonFastForward,
                "publication cannot erase native Branch history",
            )
            .into());
        }
        let default_ref = match current {
            Some(stored) if stored.default_ref != manifest.default_ref => {
                return Err(invalid("manifest default ref does not match the repository").into());
            }
            Some(stored) => stored.default_ref.clone(),
            None if !commit.refs.contains(&manifest.default_ref) => {
                return Err(invalid("creating a repository must advance its default ref").into());
            }
            None => manifest.default_ref.clone(),
        };

        // 5. Ref advance, computed completely before any state changes.
        let mut refs = current
            .map(|stored| stored.refs.clone())
            .unwrap_or_default();
        for name in &commit.refs {
            refs.insert(
                name.clone(),
                RepositoryRef {
                    name: name.clone(),
                    target: manifest.immutable_version.clone(),
                    validator: Sha256Digest(manifest_validator.clone()),
                },
            );
        }
        let ref_set = RefSet {
            format: DISCOVERY_FORMAT.to_owned(),
            version: ProtocolVersion::CURRENT,
            repository: repository.clone(),
            default_ref: default_ref.clone(),
            refs: refs.values().cloned().collect(),
            extensions: BTreeMap::new(),
        };
        let refs_document = ref_set
            .to_canonical_json()
            .map_err(|_| invalid("advanced refs are not valid discovery refs"))?;
        let retirement = retention::schedule(
            current,
            &refs,
            &manifest_validator,
            &manifest,
            state.now,
            commit.prune,
            discovery_limits,
        )?;
        let revision = sha256_digest(&refs_document);
        let receipt = PublishReceipt {
            format: HUB_PUBLISH_FORMAT.to_owned(),
            operation_uuid: session.request.operation_uuid,
            request_commitment: session.request.request_commitment.clone(),
            repository: repository.clone(),
            manifest_validator: manifest_validator.clone(),
            refs: commit.refs.clone(),
            previous_revision: commit.expected_revision.clone(),
            revision: revision.clone(),
        };

        // 6. Apply: objects, manifest, refs, and receipt together.
        let session = state
            .sessions
            .get_mut(session_id.as_str())
            .expect("session was found above");
        let objects: Vec<(String, Blob)> = session
            .request
            .objects
            .iter()
            .zip(session.uploads.iter_mut())
            .map(|(object, upload)| (object.digest.clone(), std::mem::take(&mut upload.bytes)))
            .collect();
        let upload_ids: Vec<String> = session.uploads.iter().map(|u| u.id.clone()).collect();
        session.receipt = Some(receipt.clone());
        for id in upload_ids {
            state.uploads.remove(&id);
        }
        let stored = state
            .repositories
            .entry(repository.clone())
            .or_insert_with(|| Repository {
                default_ref,
                refs: BTreeMap::new(),
                revision: String::new(),
                refs_document: Vec::new(),
                manifests: BTreeMap::new(),
                objects: BTreeMap::new(),
                history: Vec::new(),
                retirement: retention::Retirement::default(),
            });
        stored.retirement = retirement;
        stored.objects.extend(objects);
        stored
            .manifests
            .insert(manifest_validator, manifest_document);
        stored.refs = refs;
        if stored.revision != revision {
            stored.history.push(revision.clone());
        }
        stored.revision = revision;
        stored.refs_document = refs_document;
        Ok(json_response(200, &SessionResponse::Complete { receipt }))
    }

    fn device_authorization(&self, state: &mut State, request: &HubRequest) -> HubResponse {
        let form = parse_form(&request.body);
        if form.get("client_id").map(String::as_str)
            != Some(
                self.config
                    .authorization_client_id
                    .as_deref()
                    .unwrap_or("graphforge-cli"),
            )
        {
            return oauth_error("invalid_request");
        }
        let Some(scope) = form
            .get("scope")
            .filter(|scope| scope_repository(scope).is_some())
        else {
            return oauth_error("invalid_scope");
        };
        let device_code = self.next_id(state, "device");
        let user_code = user_code(&self.next_id(state, "user"));
        state.devices.insert(
            device_code.clone(),
            DeviceGrant {
                user_code: user_code.clone(),
                scope: scope.clone(),
                expires_at: state.now + self.config.device_code_ttl_seconds,
                interval: self.config.device_poll_interval_seconds,
                last_poll: None,
                status: DeviceStatus::Pending,
            },
        );
        let verification_uri = format!("{}/device", self.config.base_url);
        json_response(
            200,
            &DeviceAuthorizationResponse {
                device_code,
                verification_uri_complete: Some(format!(
                    "{verification_uri}?user_code={user_code}"
                )),
                user_code,
                verification_uri,
                expires_in: self.config.device_code_ttl_seconds,
                interval: self.config.device_poll_interval_seconds,
            },
        )
    }

    fn token(&self, state: &mut State, request: &HubRequest) -> HubResponse {
        let form = parse_form(&request.body);
        if form.get("client_id").map(String::as_str)
            != Some(
                self.config
                    .authorization_client_id
                    .as_deref()
                    .unwrap_or("graphforge-cli"),
            )
        {
            return oauth_error("invalid_client");
        }
        if form.get("grant_type").map(String::as_str) != Some(DEVICE_CODE_GRANT_TYPE) {
            return oauth_error("unsupported_grant_type");
        }
        let now = state.now;
        let Some(grant) = form
            .get("device_code")
            .and_then(|code| state.devices.get_mut(code))
        else {
            return oauth_error("invalid_grant");
        };
        if now >= grant.expires_at {
            return oauth_error("expired_token");
        }
        match grant.status {
            DeviceStatus::Denied => oauth_error("access_denied"),
            DeviceStatus::Issued => oauth_error("invalid_grant"),
            DeviceStatus::Pending => {
                let too_fast = grant
                    .last_poll
                    .is_some_and(|last| now < last + grant.interval);
                grant.last_poll = Some(now);
                if too_fast {
                    grant.interval += 5;
                    oauth_error("slow_down")
                } else {
                    oauth_error("authorization_pending")
                }
            }
            DeviceStatus::Approved => {
                grant.status = DeviceStatus::Issued;
                let scope = grant.scope.clone();
                let token = format!("gfp_{}", self.next_id(state, "token"));
                state.tokens.insert(
                    token.clone(),
                    IssuedToken {
                        scope: scope.clone(),
                        expires_at: now + self.config.token_ttl_seconds,
                    },
                );
                json_response(
                    200,
                    &TokenResponse {
                        access_token: token,
                        token_type: "Bearer".to_owned(),
                        expires_in: self.config.token_ttl_seconds,
                        scope,
                    },
                )
            }
        }
    }

    // ---- Operator actions (not part of the HTTP mapping) ----

    /// Issue a token for `scope` directly, bypassing the device flow.
    pub fn issue_token(&self, scope: &str, ttl_seconds: u64) -> String {
        let mut state = self.state.lock().expect("hub state");
        let token = format!("gfp_{}", self.next_id(&mut state, "token"));
        let expires_at = state.now + ttl_seconds;
        state.tokens.insert(
            token.clone(),
            IssuedToken {
                scope: scope.to_owned(),
                expires_at,
            },
        );
        token
    }

    /// Advance the Hub clock.
    pub fn advance_clock(&self, seconds: u64) {
        let mut state = self.state.lock().expect("hub state");
        state.now = state.now.saturating_add(seconds);
        retention::collect(&mut state);
    }

    /// Approve a pending device authorization; `false` if `user_code` is unknown.
    pub fn approve_device(&self, user_code: &str) -> bool {
        self.decide_device(user_code, DeviceStatus::Approved)
    }

    /// Deny a pending device authorization; `false` if `user_code` is unknown.
    pub fn deny_device(&self, user_code: &str) -> bool {
        self.decide_device(user_code, DeviceStatus::Denied)
    }

    fn decide_device(&self, user_code: &str, status: DeviceStatus) -> bool {
        let mut state = self.state.lock().expect("hub state");
        state
            .devices
            .values_mut()
            .find(|grant| grant.user_code == user_code && grant.status == DeviceStatus::Pending)
            .map(|grant| grant.status = status)
            .is_some()
    }

    /// Limit the bytes all of `owner`'s repositories may store.
    pub fn set_quota(&self, owner: &str, bytes: u64) {
        self.state
            .lock()
            .expect("hub state")
            .quotas
            .insert(owner.to_owned(), bytes);
    }

    /// Flip one byte of every retained copy of `digest` (uploads and stored
    /// objects), simulating corruption at rest. Returns how many copies changed.
    pub fn corrupt_object(&self, digest: &str) -> usize {
        let mut state = self.state.lock().expect("hub state");
        let mut changed = 0;
        let mut seen = BTreeSet::new();
        for session in state.sessions.values_mut() {
            for (object, upload) in session.request.objects.iter().zip(&mut session.uploads) {
                if object.digest != digest {
                    continue;
                }
                // Reused objects share one immutable storage handle. Flip each
                // retained file once, and invalidate every verification cursor.
                if let Some(file) = &upload.bytes.file
                    && seen.insert(Arc::as_ptr(file) as usize)
                    && upload.bytes.corrupt()
                {
                    changed += 1;
                }
                session.verified = false;
                if let Some(pending) = &mut session.pending {
                    pending.object = 0;
                    pending.offset = 0;
                    pending.hash = Sha256::new();
                }
            }
        }
        for repository in state.repositories.values_mut() {
            if let Some(bytes) = repository.objects.get(digest)
                && let Some(file) = &bytes.file
                && seen.insert(Arc::as_ptr(file) as usize)
                && bytes.corrupt()
            {
                changed += 1;
            }
        }
        changed
    }

    /// Atomically rename one current repository using Core's pure re-homing
    /// contract. Old names remain permanent, flattened move tombstones; native
    /// object locations continue serving their original bytes. No upload,
    /// credential, operation commitment or historical receipt is rewritten.
    pub fn move_repository(
        &self,
        old: &RepositoryIdentity,
        new: &RepositoryIdentity,
    ) -> Result<(), HubPublishError> {
        old.validate()
            .map_err(|_| invalid("old repository identity is invalid"))?;
        new.validate()
            .map_err(|_| invalid("new repository identity is invalid"))?;
        let mut state = self.state.lock().expect("hub state");
        if old == new
            || state.moved.contains_key(old)
            || state.moved.contains_key(new)
            || state.repositories.contains_key(new)
        {
            return Err(invalid(
                "repository move conflicts with an existing identity",
            ));
        }
        let repository = state
            .repositories
            .get(old)
            .ok_or_else(|| invalid("repository to move is absent"))?;
        let limits = DiscoveryLimits::default();
        let (refs, snapshots) = rehome_inputs(repository, limits)?;
        let template = graphforge_discovery::ObjectLocationTemplate::parse(
            &self.capabilities(new).object_location_template,
        )
        .map_err(|_| invalid("new object location template is invalid"))?;
        let result =
            graphforge_discovery::rehome_repository(&refs, &snapshots, new, &template, limits)
                .map_err(|_| invalid("stored repository re-homing bindings failed"))?;
        let refs_document = result
            .refs
            .to_canonical_json()
            .map_err(|_| invalid("re-homed refs are invalid"))?;
        let mut documents = Vec::new();
        for snapshot in result.snapshots {
            let validator = snapshot
                .manifest
                .canonical_digest()
                .map_err(|_| invalid("re-homed manifest is invalid"))?
                .0;
            let manifest = snapshot
                .manifest
                .to_canonical_json()
                .map_err(|_| invalid("re-homed manifest is invalid"))?;
            let mut controls = Vec::new();
            for bytes in [snapshot.summary, snapshot.lineage].into_iter().flatten() {
                let digest = sha256_digest(&bytes);
                let mut blob = Blob::default();
                blob.append(&bytes).map_err(storage_failure)?;
                controls.push((digest, blob));
            }
            documents.push((validator, manifest, controls));
        }
        // Everything above is read-only and fallible. Apply only the complete
        // validated replacement set while holding the same repository lock.
        let mut repository = state
            .repositories
            .remove(old)
            .expect("validated repository");
        repository.refs = result
            .refs
            .refs
            .into_iter()
            .map(|item| (item.name.clone(), item))
            .collect();
        repository.revision = result.revision.0;
        repository.refs_document = refs_document;
        repository.history.push(repository.revision.clone());
        for (validator, manifest, controls) in documents {
            repository.manifests.insert(validator, manifest);
            repository.objects.extend(controls);
        }
        state.repositories.insert(new.clone(), repository);
        for current in state.moved.values_mut() {
            if current == old {
                *current = new.clone();
            }
        }
        state.moved.insert(old.clone(), new.clone());
        Ok(())
    }

    /// Revisions `repository` has moved through, oldest first.
    #[must_use]
    pub fn revisions(&self, repository: &RepositoryIdentity) -> Vec<String> {
        self.state
            .lock()
            .expect("hub state")
            .repositories
            .get(repository)
            .map(|stored| stored.history.clone())
            .unwrap_or_default()
    }
}

impl Default for ReferenceHub {
    fn default() -> Self {
        Self::new()
    }
}

impl HubExchange for ReferenceHub {
    fn exchange(&self, request: HubRequest) -> Result<HubResponse, HubPublishError> {
        Ok(self.handle(&request))
    }
}

/// Retained bytes and completion for each object of a new session.
///
/// Sealed objects already stored in this repository may reuse their immutable
/// handle; every commit re-verifies actual bytes. A known corrupted copy must
/// be uploaded again. A superseded
/// attempt hands its retained bytes to the new session by digest, so identical
/// bytes are never sent twice.
fn initial_uploads(
    state: &mut State,
    repository: &RepositoryIdentity,
    open: &OpenSessionRequest,
    superseded: Option<&str>,
) -> Vec<(Blob, bool)> {
    let mut carried: BTreeMap<String, (Blob, bool)> = BTreeMap::new();
    if let Some(previous) = superseded {
        let previous = state.sessions.remove(previous).expect("operation session");
        for (object, upload) in previous.request.objects.iter().zip(previous.uploads) {
            state.uploads.remove(&upload.id);
            carried.insert(object.digest.clone(), (upload.bytes, upload.complete));
        }
    }
    let admitted = state.repositories.get(repository);
    open.objects
        .iter()
        .map(|object| {
            admitted
                .and_then(|r| r.objects.get(&object.digest))
                .filter(|bytes| {
                    bytes.len() == object.length
                        && bytes.intact.load(std::sync::atomic::Ordering::Relaxed)
                })
                .map(|bytes| (bytes.clone(), true))
                .or_else(|| {
                    carried.remove(&object.digest).filter(|(bytes, _)| {
                        bytes.intact.load(std::sync::atomic::Ordering::Relaxed)
                    })
                })
                .unwrap_or_default()
        })
        .collect()
}

fn upload_head(state: &State, id: &str) -> Handled {
    let (session_id, index) = state
        .uploads
        .get(id)
        .ok_or_else(|| not_found("upload location is unknown or closed"))?;
    let session = &state.sessions[session_id];
    Ok(HubResponse {
        status: 200,
        headers: vec![
            ("cache-control".to_owned(), "no-store".to_owned()),
            (
                UPLOAD_OFFSET_HEADER.to_owned(),
                session.uploads[*index].bytes.len().to_string(),
            ),
            (
                UPLOAD_LENGTH_HEADER.to_owned(),
                session.request.objects[*index].length.to_string(),
            ),
        ],
        body: Vec::new(),
    })
}

fn published<'a>(
    state: &'a State,
    repository: &RepositoryIdentity,
) -> Result<&'a Repository, Reject> {
    state
        .repositories
        .get(repository)
        .ok_or_else(|| not_found("repository is not published"))
}

fn read_refs(state: &State, repository: &RepositoryIdentity) -> Handled {
    let stored = published(state, repository)?;
    Ok(document(&stored.refs_document, &stored.revision))
}

fn read_manifest(state: &State, repository: &RepositoryIdentity) -> Handled {
    let stored = published(state, repository)?;
    let validator = &stored.refs[&stored.default_ref].validator.0;
    Ok(document(&stored.manifests[validator], validator))
}

fn document(bytes: &[u8], validator: &str) -> HubResponse {
    HubResponse {
        status: 200,
        headers: vec![
            ("content-type".to_owned(), "application/json".to_owned()),
            ("etag".to_owned(), quoted(validator)),
        ],
        body: bytes.to_vec(),
    }
}

fn read_object(
    state: &State,
    repository: &RepositoryIdentity,
    digest: &str,
    request: &HubRequest,
) -> Handled {
    validate_digest(digest).map_err(|_| not_found("object is not published"))?;
    let bytes = published(state, repository)?
        .objects
        .get(digest)
        .ok_or_else(|| not_found("object is not published"))?;
    let etag = quoted(digest);
    let length = bytes.len();
    let mut headers = vec![
        ("accept-ranges".to_owned(), "bytes".to_owned()),
        ("etag".to_owned(), etag.clone()),
    ];
    let range_applies = request
        .header("if-range")
        .is_none_or(|validator| validator == etag);
    let start = request
        .header("range")
        .filter(|_| range_applies)
        .and_then(|range| range.strip_prefix("bytes="))
        .and_then(|range| range.strip_suffix('-'))
        .and_then(|start| start.parse::<u64>().ok());
    if length.saturating_sub(start.unwrap_or(0)) > REFERENCE_READ_LIMIT {
        return Err(too_large("reference buffered GET exceeds response limit"));
    }
    match start {
        None => Ok(HubResponse {
            status: 200,
            headers,
            body: bytes
                .read(
                    0,
                    usize::try_from(length)
                        .map_err(|_| too_large("reference response too large"))?,
                )
                .map_err(storage_error)?,
        }),
        Some(start) if start < length => {
            headers.push((
                "content-range".to_owned(),
                format!("bytes {start}-{}/{length}", length - 1),
            ));
            Ok(HubResponse {
                status: 206,
                headers,
                body: bytes
                    .read(
                        start,
                        usize::try_from(length - start)
                            .map_err(|_| too_large("reference response too large"))?,
                    )
                    .map_err(storage_error)?,
            })
        }
        Some(_) => {
            headers.push(("content-range".to_owned(), format!("bytes */{length}")));
            Ok(HubResponse {
                status: 416,
                headers,
                body: Vec::new(),
            })
        }
    }
}

fn parse_form(body: &[u8]) -> BTreeMap<String, String> {
    url::form_urlencoded::parse(body).into_owned().collect()
}

fn scope_repository(scope: &str) -> Option<RepositoryIdentity> {
    RepositoryIdentity::parse(scope.strip_prefix("publish:")?)
        .ok()
        .filter(|identity| publish_scope(identity) == scope)
}

fn user_code(id: &str) -> String {
    const ALPHABET: &[u8; 20] = b"BCDFGHJKLMNPQRSTVWXZ";
    let letters: String = id
        .bytes()
        .take(8)
        .map(|byte| ALPHABET[usize::from(byte) % ALPHABET.len()] as char)
        .collect();
    format!("{}-{}", &letters[..4], &letters[4..])
}
