//! Versioned publish documents (`graphforge-hub-publish/1`).
//!
//! Every type here is a wire document with a checked-in JSON Schema under
//! `docs/reference/hub-publish/v1/`. Parsers check `format` and required
//! capabilities before any other member, so a future document fails
//! `unsupported_future` rather than `invalid_input`.

use crate::canonical::{canonical_json, parse_unique_json, sha256_digest, validate_digest};
use crate::error::{HubPublishError, HubPublishErrorCode, invalid, unsupported};
use graphforge_discovery::RepositoryIdentity;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fmt;
use url::Url;
use uuid::Uuid;

/// Publish protocol format emitted and accepted by this release.
pub const HUB_PUBLISH_FORMAT: &str = "graphforge-hub-publish/1";
const HUB_PUBLISH_FORMAT_NAME: &str = "graphforge-hub-publish";
const HUB_PUBLISH_MAJOR: u16 = 1;

/// RFC 8628 device-code grant type.
pub const DEVICE_CODE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:device_code";

/// Environment variable a CI job uses to hand a publish token to `gf publish`.
pub const PUBLISH_TOKEN_ENV: &str = "GRAPHFORGE_HUB_PUBLISH_TOKEN";

/// Response header carrying the bytes an upload location has received.
pub const UPLOAD_OFFSET_HEADER: &str = "upload-offset";

/// Response header carrying an upload location's declared length.
pub const UPLOAD_LENGTH_HEADER: &str = "upload-length";

/// Placeholder in [`PublishCapabilities::object_location_template`].
pub const DIGEST_PLACEHOLDER: &str = "{digest}";

/// Capability names this release understands, with the major it supports.
pub const SUPPORTED_CAPABILITIES: [(&str, u16); 3] = [
    ("device-authorization", 1),
    ("publish-session", 1),
    ("resumable-upload", 1),
];

const MAX_REQUIREMENTS: usize = 64;
const MAX_NAME_BYTES: usize = 128;
const MAX_MEDIA_TYPE_BYTES: usize = 255;
const MAX_REFS_PER_COMMIT: usize = 64;

/// Scope string a publish token must carry for one repository.
#[must_use]
pub fn publish_scope(repository: &RepositoryIdentity) -> String {
    format!("publish:{}/{}", repository.owner, repository.repository)
}

/// Require `format` to be exactly [`HUB_PUBLISH_FORMAT`].
///
/// Another major of the same document family fails `unsupported_future`; any
/// other value fails `invalid_input`.
pub fn check_format(format: &str) -> Result<(), HubPublishError> {
    if format == HUB_PUBLISH_FORMAT {
        return Ok(());
    }
    match format
        .strip_prefix(HUB_PUBLISH_FORMAT_NAME)
        .and_then(|rest| rest.strip_prefix('/'))
        .and_then(|major| major.parse::<u16>().ok())
    {
        Some(major) if major != HUB_PUBLISH_MAJOR => {
            Err(unsupported("publish format major is unsupported"))
        }
        _ => Err(invalid("publish format is invalid")),
    }
}

/// One named, major-versioned protocol capability.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capability {
    /// Capability name.
    pub capability: String,
    /// Required or offered major.
    pub major: u16,
}

impl Capability {
    /// Construct a capability.
    #[must_use]
    pub fn new(capability: &str, major: u16) -> Self {
        Self {
            capability: capability.to_owned(),
            major,
        }
    }
}

/// Fail `unsupported_future` when any requirement names a capability or major
/// this release does not implement.
pub fn check_requirements(requirements: &[Capability]) -> Result<(), HubPublishError> {
    if requirements.len() > MAX_REQUIREMENTS {
        return Err(invalid("too many required capabilities"));
    }
    for requirement in requirements {
        check_name(&requirement.capability, "capability name is invalid")?;
        if !SUPPORTED_CAPABILITIES
            .iter()
            .any(|(name, major)| *name == requirement.capability && *major == requirement.major)
        {
            return Err(unsupported("a required capability is unsupported"));
        }
    }
    Ok(())
}

fn check_name(value: &str, detail: &'static str) -> Result<(), HubPublishError> {
    if value.is_empty()
        || value.len() > MAX_NAME_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(invalid(detail));
    }
    Ok(())
}

/// Check `format` and `requirements` of an untrusted document before anything
/// else is interpreted.
fn check_header(value: &Value) -> Result<(), HubPublishError> {
    let Some(object) = value.as_object() else {
        return Err(invalid("document must be a JSON object"));
    };
    let Some(format) = object.get("format").and_then(Value::as_str) else {
        return Err(invalid("document format is missing"));
    };
    check_format(format)?;
    if let Some(requirements) = object.get("requirements") {
        let requirements: Vec<Capability> = serde_json::from_value(requirements.clone())
            .map_err(|_| invalid("required capabilities are malformed"))?;
        check_requirements(&requirements)?;
    }
    Ok(())
}

/// Require a credential-free HTTPS URL without query or fragment.
pub fn validate_https_url(value: &str) -> Result<Url, HubPublishError> {
    let url = Url::parse(value).map_err(|_| invalid("URL is invalid"))?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid("URL must be credential-free HTTPS"));
    }
    Ok(url)
}

/// Upload bounds a Hub advertises. Every bound is enforced from declared
/// lengths before any byte is buffered.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishLimits {
    /// Largest declared length of one object.
    pub max_object_bytes: u64,
    /// Largest number of objects in one session.
    pub max_objects: u64,
    /// Largest sum of declared lengths in one session.
    pub max_session_bytes: u64,
    /// Largest body of one upload `PUT`.
    pub max_chunk_bytes: u64,
    /// Largest control-plane JSON request body.
    pub max_document_bytes: u64,
}

impl PublishLimits {
    fn validate(&self) -> Result<(), HubPublishError> {
        if self.max_object_bytes == 0
            || self.max_objects == 0
            || self.max_session_bytes == 0
            || self.max_chunk_bytes == 0
            || self.max_document_bytes == 0
        {
            return Err(invalid("publish limits must be positive"));
        }
        Ok(())
    }
}

/// Where and how a client obtains a publish token (RFC 8628 device flow).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishAuthorization {
    /// RFC 8628 device authorization endpoint.
    pub device_authorization_endpoint: String,
    /// RFC 6749 token endpoint polled with the device-code grant.
    pub token_endpoint: String,
    /// Scope to request; always [`publish_scope`] of the repository.
    pub scope: String,
}

/// `GET {repository}/.gf/publish` capabilities document.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishCapabilities {
    /// Must equal [`HUB_PUBLISH_FORMAT`].
    pub format: String,
    /// Repository this document describes.
    pub repository: RepositoryIdentity,
    /// Capabilities a client must implement to publish here.
    pub requirements: Vec<Capability>,
    /// Optional capabilities the Hub offers.
    pub capabilities: Vec<Capability>,
    /// Credential acquisition endpoints.
    pub authorization: PublishAuthorization,
    /// HTTPS location of an admitted object, with one `{digest}` placeholder.
    /// Manifests published here must list exactly this location per object.
    pub object_location_template: String,
    /// Upload bounds.
    pub limits: PublishLimits,
}

impl PublishCapabilities {
    /// Parse and validate an untrusted capabilities document.
    ///
    /// `format` and `requirements` are checked first, so an unsupported future
    /// document fails `unsupported_future` before any other member is read.
    pub fn from_json(bytes: &[u8]) -> Result<Self, HubPublishError> {
        let value = parse_unique_json(bytes)?;
        check_header(&value)?;
        let document: Self = serde_json::from_value(value)
            .map_err(|_| invalid("capabilities document is malformed"))?;
        document.validate()?;
        Ok(document)
    }

    /// Validate every member.
    pub fn validate(&self) -> Result<(), HubPublishError> {
        check_format(&self.format)?;
        check_requirements(&self.requirements)?;
        if self.capabilities.len() > MAX_REQUIREMENTS {
            return Err(invalid("too many capabilities"));
        }
        for capability in &self.capabilities {
            check_name(&capability.capability, "capability name is invalid")?;
        }
        self.repository
            .validate()
            .map_err(|_| invalid("repository identity is invalid"))?;
        validate_https_url(&self.authorization.device_authorization_endpoint)?;
        validate_https_url(&self.authorization.token_endpoint)?;
        if self.authorization.scope != publish_scope(&self.repository) {
            return Err(invalid("authorization scope does not name the repository"));
        }
        if self
            .object_location_template
            .matches(DIGEST_PLACEHOLDER)
            .count()
            != 1
        {
            return Err(invalid("object location template needs one {digest}"));
        }
        validate_https_url(&self.object_location(&sha256_digest(b"")))?;
        self.limits.validate()
    }

    /// Location of the object with `digest`.
    #[must_use]
    pub fn object_location(&self, digest: &str) -> String {
        self.object_location_template
            .replace(DIGEST_PLACEHOLDER, digest)
    }
}

/// One object in a publish session inventory.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectDeclaration {
    /// Canonical `sha256:<64 lowercase hex>` content digest.
    pub digest: String,
    /// Exact byte length.
    pub length: u64,
    /// Media type, as the manifest lists it.
    pub media_type: String,
}

/// Opaque session identity issued by a Hub (URL-path safe).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PublishSessionId(String);

impl PublishSessionId {
    /// Validate an untrusted session identity.
    pub fn parse(value: &str) -> Result<Self, HubPublishError> {
        if value.is_empty()
            || value.len() > MAX_NAME_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(invalid("session id is invalid"));
        }
        Ok(Self(value.to_owned()))
    }

    /// Identity text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PublishSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// `POST {repository}/.gf/publish/sessions` request body.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenSessionRequest {
    /// Must equal [`HUB_PUBLISH_FORMAT`].
    pub format: String,
    /// Caller-stable identity of this publication; retries reuse it.
    pub operation_uuid: Uuid,
    /// [`PublishIntent::request_commitment`] of the whole publication.
    pub request_commitment: String,
    /// Repository to publish into; must equal the URL repository.
    pub repository: RepositoryIdentity,
    /// Object inventory, strictly ascending by digest.
    pub objects: Vec<ObjectDeclaration>,
    /// Capabilities the Hub must implement to accept this session.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requirements: Vec<Capability>,
}

impl OpenSessionRequest {
    /// Parse an untrusted session request; `format` and `requirements` first.
    pub fn from_json(bytes: &[u8]) -> Result<Self, HubPublishError> {
        let value = parse_unique_json(bytes)?;
        check_header(&value)?;
        let request: Self =
            serde_json::from_value(value).map_err(|_| invalid("session request is malformed"))?;
        request.validate()?;
        Ok(request)
    }

    /// Validate limit-independent structure.
    pub fn validate(&self) -> Result<(), HubPublishError> {
        check_format(&self.format)?;
        check_requirements(&self.requirements)?;
        validate_digest(&self.request_commitment)?;
        self.repository
            .validate()
            .map_err(|_| invalid("repository identity is invalid"))?;
        validate_inventory(&self.objects)
    }
}

/// Validate an object inventory: non-empty, strictly ascending, canonical digests.
pub fn validate_inventory(objects: &[ObjectDeclaration]) -> Result<(), HubPublishError> {
    if objects.is_empty() {
        return Err(invalid("object inventory is empty"));
    }
    let mut prior: Option<&str> = None;
    for object in objects {
        validate_digest(&object.digest)?;
        if prior.is_some_and(|prior| prior >= object.digest.as_str()) {
            return Err(invalid("objects are duplicated or not ascending by digest"));
        }
        prior = Some(&object.digest);
        if object.length == 0 {
            return Err(invalid("object length must be positive"));
        }
        if object.media_type.is_empty()
            || object.media_type.len() > MAX_MEDIA_TYPE_BYTES
            || object
                .media_type
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte == b' ')
        {
            return Err(invalid("object media type is invalid"));
        }
    }
    Ok(())
}

/// One data-plane upload location in an open session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadTarget {
    /// Object digest.
    pub digest: String,
    /// Declared length.
    pub length: u64,
    /// Capability URL for `HEAD` and `PUT`. Treat as a secret: never log it,
    /// and never send the publish token to it.
    pub upload_url: String,
    /// Bytes already received and retained.
    pub received: u64,
}

/// Session open and commit response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionResponse {
    /// Uploads may proceed.
    Open {
        /// Session to commit.
        session_id: PublishSessionId,
        /// One upload location per inventory object, in inventory order.
        uploads: Vec<UploadTarget>,
    },
    /// The publication already committed; this is its original receipt.
    Complete {
        /// Original receipt.
        receipt: PublishReceipt,
    },
}

/// `POST {repository}/.gf/publish/sessions/{session_id}/commit` request body.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    /// Discovery manifest (`graphforge-discovery/1`) the refs will select.
    pub manifest: Value,
    /// Ref names to advance to the manifest, strictly ascending.
    pub refs: Vec<String>,
    /// Current repository revision, or `null` to create an absent repository.
    /// The member is required even when `null`.
    pub expected_revision: Option<String>,
}

impl CommitRequest {
    /// Parse an untrusted commit body.
    pub fn from_json(bytes: &[u8]) -> Result<Self, HubPublishError> {
        let value = parse_unique_json(bytes)?;
        if !value
            .as_object()
            .is_some_and(|object| object.contains_key("expected_revision"))
        {
            return Err(invalid(
                "expected_revision is required; use null to create an absent repository",
            ));
        }
        let request: Self =
            serde_json::from_value(value).map_err(|_| invalid("commit request is malformed"))?;
        if let Some(expected) = &request.expected_revision {
            validate_digest(expected)?;
        }
        validate_ref_list(&request.refs)?;
        Ok(request)
    }
}

/// Validate a commit ref list: non-empty, bounded, strictly ascending.
pub fn validate_ref_list(refs: &[String]) -> Result<(), HubPublishError> {
    if refs.is_empty() || refs.len() > MAX_REFS_PER_COMMIT {
        return Err(invalid("commit must advance between 1 and 64 refs"));
    }
    if refs.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("refs are duplicated or not ascending"));
    }
    Ok(())
}

/// Receipt of one committed publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishReceipt {
    /// Must equal [`HUB_PUBLISH_FORMAT`].
    pub format: String,
    /// Operation identity.
    pub operation_uuid: Uuid,
    /// Request commitment the Hub verified.
    pub request_commitment: String,
    /// Repository published into.
    pub repository: RepositoryIdentity,
    /// Canonical digest of the published manifest (the refs' validator).
    pub manifest_validator: String,
    /// Refs advanced to the manifest.
    pub refs: Vec<String>,
    /// Revision the commit required (`null`: created the repository).
    pub previous_revision: Option<String>,
    /// Repository revision after the commit: the canonical digest of `.gf/refs`.
    pub revision: String,
}

/// Everything a request commitment binds.
///
/// A client computes the commitment before opening a session; the Hub
/// recomputes it at commit from the session inventory and commit body, so
/// changed content can never reuse an operation identity silently.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishIntent {
    /// Target repository.
    pub repository: RepositoryIdentity,
    /// Object inventory, strictly ascending by digest.
    pub objects: Vec<ObjectDeclaration>,
    /// Canonical digest of the discovery manifest.
    pub manifest_validator: String,
    /// Refs to advance, strictly ascending.
    pub refs: Vec<String>,
    /// Expected repository revision (`None`: create-if-absent).
    pub expected_revision: Option<String>,
}

impl PublishIntent {
    /// Canonical commitment bytes: compact JSON with sorted members.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        canonical_json(&json!({
            "format": HUB_PUBLISH_FORMAT,
            "repository": {"owner": self.repository.owner, "repository": self.repository.repository},
            "objects": self.objects,
            "manifest_validator": self.manifest_validator,
            "refs": self.refs,
            "expected_revision": self.expected_revision,
        }))
    }

    /// `sha256:` digest of [`Self::canonical_bytes`].
    #[must_use]
    pub fn request_commitment(&self) -> String {
        sha256_digest(&self.canonical_bytes())
    }
}

/// `PUT` upload response body.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadStatus {
    /// Object digest.
    pub digest: String,
    /// Declared length.
    pub length: u64,
    /// Bytes received and retained; equals `length` once verified.
    pub received: u64,
}

/// `Content-Range` value for a chunk of `chunk_len` bytes at `start`.
#[must_use]
pub fn content_range(start: u64, chunk_len: u64, total: u64) -> String {
    let end = start + chunk_len.saturating_sub(1);
    format!("bytes {start}-{end}/{total}")
}

/// Parse `bytes <start>-<end>/<total>`.
#[must_use]
pub fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let rest = value.trim().strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let parse = |text: &str| {
        (!text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| text.parse::<u64>().ok())
            .flatten()
    };
    let (start, end, total) = (parse(start)?, parse(end)?, parse(total)?);
    (start <= end && end < total).then_some((start, end, total))
}

/// RFC 8628 device authorization response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeviceAuthorizationResponse {
    /// Code the client polls the token endpoint with. Never displayed or logged.
    pub device_code: String,
    /// Short code the user enters at `verification_uri`.
    pub user_code: String,
    /// Page where the user approves the request.
    pub verification_uri: String,
    /// `verification_uri` with the user code filled in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_uri_complete: Option<String>,
    /// Seconds until `device_code` expires.
    pub expires_in: u64,
    /// Minimum seconds between token polls.
    #[serde(default = "default_interval")]
    pub interval: u64,
}

const fn default_interval() -> u64 {
    5
}

/// RFC 6749 successful token response.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenResponse {
    /// Short-lived bearer token.
    pub access_token: String,
    /// Always `Bearer`.
    pub token_type: String,
    /// Seconds until the token expires.
    pub expires_in: u64,
    /// Granted scope.
    pub scope: String,
}

impl fmt::Debug for TokenResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenResponse")
            .field("access_token", &"<redacted>")
            .field("token_type", &self.token_type)
            .field("expires_in", &self.expires_in)
            .field("scope", &self.scope)
            .finish()
    }
}

/// RFC 6749 section 5.2 error body returned by the token endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OAuthErrorBody {
    /// Error code, e.g. `authorization_pending`.
    pub error: String,
}

/// Outcome of one device-code token poll.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DevicePoll {
    /// The user approved; the token is held in memory only.
    Granted(PublishToken),
    /// Keep polling at the current interval.
    Pending,
    /// Keep polling, adding 5 seconds to the interval (RFC 8628 section 3.5).
    SlowDown,
}

impl DevicePoll {
    /// Classify a token endpoint response.
    ///
    /// `expired_token`, `access_denied`, and every other OAuth error end the
    /// flow with `auth_denied`.
    pub fn from_response(status: u16, body: &[u8]) -> Result<Self, HubPublishError> {
        if status == 200 {
            let token: TokenResponse =
                serde_json::from_slice(body).map_err(|_| invalid("token response is malformed"))?;
            if !token.token_type.eq_ignore_ascii_case("bearer") || token.access_token.is_empty() {
                return Err(invalid("token response is not a bearer token"));
            }
            return Ok(Self::Granted(PublishToken::new(token.access_token)));
        }
        let error: OAuthErrorBody = serde_json::from_slice(body)
            .map_err(|_| invalid("token error response is malformed"))?;
        match error.error.as_str() {
            "authorization_pending" => Ok(Self::Pending),
            "slow_down" => Ok(Self::SlowDown),
            "expired_token" => Err(HubPublishError::new(
                HubPublishErrorCode::AuthDenied,
                "device authorization expired before approval",
            )),
            "access_denied" => Err(HubPublishError::new(
                HubPublishErrorCode::AuthDenied,
                "device authorization was denied",
            )),
            _ => Err(HubPublishError::new(
                HubPublishErrorCode::AuthDenied,
                "token endpoint refused the device grant",
            )),
        }
    }
}

/// A publish bearer token held only in memory.
///
/// It has no `Display`, `Serialize`, or revealing `Debug`, so it cannot reach
/// project files, configuration participants, or logs by formatting.
#[derive(Clone, Eq, PartialEq)]
pub struct PublishToken(String);

impl PublishToken {
    /// Wrap a token value.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// Read [`PUBLISH_TOKEN_ENV`]; `None` when unset or empty.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        std::env::var(PUBLISH_TOKEN_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .map(Self)
    }

    /// `Authorization` header value.
    #[must_use]
    pub fn authorization(&self) -> String {
        format!("Bearer {}", self.0)
    }

    /// Raw token, for transports only.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PublishToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PublishToken(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_majors_classify() {
        check_format(HUB_PUBLISH_FORMAT).unwrap();
        assert_eq!(
            check_format("graphforge-hub-publish/2").unwrap_err().code,
            HubPublishErrorCode::UnsupportedFuture
        );
        assert_eq!(
            check_format("graphforge-hub/1").unwrap_err().code,
            HubPublishErrorCode::InvalidInput
        );
    }

    #[test]
    fn content_range_round_trips() {
        assert_eq!(content_range(10, 5, 20), "bytes 10-14/20");
        assert_eq!(parse_content_range("bytes 10-14/20"), Some((10, 14, 20)));
        assert_eq!(parse_content_range("bytes 10-9/20"), None);
        assert_eq!(parse_content_range("bytes 0-20/20"), None);
        assert_eq!(parse_content_range("bytes +0-1/20"), None);
        assert_eq!(parse_content_range("bytes */20"), None);
    }

    #[test]
    fn token_debug_is_redacted() {
        let token = PublishToken::new("secret-token".into());
        assert!(!format!("{token:?}").contains("secret"));
        let response = TokenResponse {
            access_token: "secret-token".into(),
            token_type: "Bearer".into(),
            expires_in: 1,
            scope: "publish:a/b".into(),
        };
        assert!(!format!("{response:?}").contains("secret"));
    }

    #[test]
    fn commit_requires_explicit_expected_revision() {
        let error = CommitRequest::from_json(br#"{"manifest":{},"refs":["main"]}"#).unwrap_err();
        assert_eq!(error.code, HubPublishErrorCode::InvalidInput);
        let request = CommitRequest::from_json(
            br#"{"manifest":{},"refs":["main"],"expected_revision":null}"#,
        )
        .unwrap();
        assert_eq!(request.expected_revision, None);
    }
}
