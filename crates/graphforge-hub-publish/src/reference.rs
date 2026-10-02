//! In-memory reference Hub implementing the full publish and read mapping.
//!
//! [`ReferenceHub::handle`] maps one [`HubRequest`] to one [`HubResponse`]
//! under a single lock, so every commit is one atomic critical section. It is
//! a conformance target, not a production Hub: identifiers derive from a
//! configurable seed and are predictable, and all state lives in memory.

use crate::canonical::{sha256_digest, validate_digest};
use crate::error::{HubPublishError, HubPublishErrorCode, invalid};
use crate::exchange::{HubExchange, HubMethod, HubRequest, HubResponse};
use crate::wire::{
    Capability, CommitRequest, DEVICE_CODE_GRANT_TYPE, DIGEST_PLACEHOLDER,
    DeviceAuthorizationResponse, HUB_PUBLISH_FORMAT, OAuthErrorBody, OpenSessionRequest,
    OperationStatus, PublishAuthorization, PublishCapabilities, PublishIntent, PublishLimits,
    PublishReceipt, PublishSessionId, SessionResponse, TokenResponse, UPLOAD_LENGTH_HEADER,
    UPLOAD_OFFSET_HEADER, UploadStatus, UploadTarget, parse_content_range, publish_scope,
};
use graphforge_discovery::{
    DISCOVERY_FORMAT, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ProtocolVersion,
    RefSet, RepositoryIdentity, RepositoryRef, Sha256Digest,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Mutex;
use url::Url;
use uuid::Uuid;

/// Reference Hub configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceHubConfig {
    /// HTTPS origin the Hub serves, without a path (e.g. `https://hub.example`).
    pub base_url: String,
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
            base_url: "https://hub.example".to_owned(),
            limits: PublishLimits {
                max_object_bytes: 64 * 1024 * 1024,
                max_objects: 1024,
                max_session_bytes: 256 * 1024 * 1024,
                max_chunk_bytes: 8 * 1024 * 1024,
                max_document_bytes: 4 * 1024 * 1024,
            },
            token_ttl_seconds: 900,
            device_code_ttl_seconds: 600,
            device_poll_interval_seconds: 5,
            id_seed: "graphforge-reference-hub".to_owned(),
        }
    }
}

/// In-memory reference implementation of the publish and read HTTP mapping.
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
}

struct Upload {
    id: String,
    bytes: Vec<u8>,
    complete: bool,
}

struct Repository {
    default_ref: String,
    refs: BTreeMap<String, RepositoryRef>,
    revision: String,
    refs_document: Vec<u8>,
    manifests: BTreeMap<String, Vec<u8>>,
    objects: BTreeMap<String, Vec<u8>>,
    history: Vec<String>,
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
        Ok(Self {
            config,
            base,
            state: Mutex::new(State::default()),
        })
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
        PublishCapabilities {
            format: HUB_PUBLISH_FORMAT.to_owned(),
            repository: repository.clone(),
            requirements: vec![
                Capability::new("publish-session", 1),
                Capability::new("resumable-upload", 1),
            ],
            capabilities: vec![Capability::new("device-authorization", 1)],
            authorization: PublishAuthorization {
                device_authorization_endpoint: format!(
                    "{}/.gf/oauth/device_authorization",
                    self.config.base_url
                ),
                token_endpoint: format!("{}/.gf/oauth/token", self.config.base_url),
                scope: publish_scope(repository),
            },
            object_location_template: format!(
                "{}/.gf/objects/{DIGEST_PLACEHOLDER}",
                self.repository_url(repository)
            ),
            limits: self.config.limits,
        }
    }

    /// Handle one request. Every state change happens under one lock.
    pub fn handle(&self, request: &HubRequest) -> HubResponse {
        let Ok(mut state) = self.state.lock() else {
            let error = HubPublishError::new(HubPublishErrorCode::Internal, "hub state poisoned");
            return json_response(500, &error.body());
        };
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
                        Self::operation_status(state, &identity, id, request)
                    }
                    (HubMethod::Get, ["refs"]) => read_refs(state, &identity),
                    (HubMethod::Get, ["manifest"]) => read_manifest(state, &identity),
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

        if let Some(session_id) = state.operations.get(&open.operation_uuid) {
            let session = &state.sessions[session_id];
            if session.request != open {
                return Err(HubPublishError::new(
                    HubPublishErrorCode::IdempotencyConflict,
                    "operation identity was reused with a different request",
                )
                .into());
            }
            return Ok(match &session.receipt {
                Some(receipt) => json_response(
                    200,
                    &SessionResponse::Complete {
                        receipt: receipt.clone(),
                    },
                ),
                None => json_response(200, &self.open_response(session_id, session)),
            });
        }

        let admitted = state.repositories.get(repository);
        if let Some(quota) = state.quotas.get(&repository.owner) {
            let used: u64 = state
                .repositories
                .iter()
                .filter(|(identity, _)| identity.owner == repository.owner)
                .flat_map(|(_, stored)| stored.objects.values())
                .map(|bytes| bytes.len() as u64)
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

        let prior: Vec<Option<Vec<u8>>> = open
            .objects
            .iter()
            .map(|object| admitted.and_then(|r| r.objects.get(&object.digest).cloned()))
            .collect();
        let session_id = self.next_id(state, "session");
        let mut uploads = Vec::with_capacity(open.objects.len());
        for (index, bytes) in prior.into_iter().enumerate() {
            let id = self.next_id(state, "upload");
            state
                .uploads
                .insert(id.clone(), (session_id.clone(), index));
            let complete = bytes.is_some();
            uploads.push(Upload {
                id,
                bytes: bytes.unwrap_or_default(),
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
        };
        let response = self.open_response(&session_id, &session);
        state.sessions.insert(session_id, session);
        Ok(json_response(201, &response))
    }

    /// Report an operation's intent and, once committed, its original receipt.
    fn operation_status(
        state: &State,
        repository: &RepositoryIdentity,
        operation: &str,
        request: &HubRequest,
    ) -> Handled {
        Self::authorize(state, request, repository)?;
        let unknown = || not_found("publish operation is unknown");
        let operation_uuid = Uuid::parse_str(operation).map_err(|_| unknown())?;
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
                    received: upload.bytes.len() as u64,
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
        let received = upload.bytes.len() as u64;
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
        upload.bytes.extend_from_slice(&request.body);
        if upload.bytes.len() as u64 == declared.length {
            if sha256_digest(&upload.bytes) != declared.digest {
                upload.bytes = Vec::new();
                let mut rejected = integrity("uploaded bytes do not match the declared digest");
                rejected
                    .headers
                    .push((UPLOAD_OFFSET_HEADER.to_owned(), "0".to_owned()));
                return Err(rejected);
            }
            upload.complete = true;
        }
        let received = upload.bytes.len() as u64;
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

        // 2. Every inventory object finalized and re-verified.
        for (object, upload) in session.request.objects.iter().zip(&session.uploads) {
            if !upload.complete || upload.bytes.len() as u64 != object.length {
                return Err(integrity("an inventory object is missing or incomplete"));
            }
            if sha256_digest(&upload.bytes) != object.digest {
                return Err(integrity("an inventory object failed digest verification"));
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
        let objects: Vec<(String, Vec<u8>)> = session
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
            });
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
        if form.get("client_id").is_none_or(String::is_empty) {
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
        self.state.lock().expect("hub state").now += seconds;
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
        for session in state.sessions.values_mut() {
            for (object, upload) in session.request.objects.iter().zip(&mut session.uploads) {
                if object.digest == digest
                    && let Some(byte) = upload.bytes.first_mut()
                {
                    *byte ^= 0xff;
                    changed += 1;
                }
            }
        }
        for repository in state.repositories.values_mut() {
            if let Some(byte) = repository
                .objects
                .get_mut(digest)
                .and_then(|bytes| bytes.first_mut())
            {
                *byte ^= 0xff;
                changed += 1;
            }
        }
        changed
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
    let length = bytes.len() as u64;
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
    match start {
        None => Ok(HubResponse {
            status: 200,
            headers,
            body: bytes.clone(),
        }),
        Some(start) if start < length => {
            headers.push((
                "content-range".to_owned(),
                format!("bytes {start}-{}/{length}", length - 1),
            ));
            Ok(HubResponse {
                status: 206,
                headers,
                body: bytes[usize::try_from(start).expect("bounded by length")..].to_vec(),
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
