//! Canonical, provider-neutral GraphForge Hub repository discovery semantics.
//!
//! This crate owns the untrusted JSON boundary shared by Hub servers and native
//! GraphForge clients. It deliberately contains no HTTP client, storage backend,
//! authentication, billing, or portable-project verifier. A discovery manifest
//! names a portable-v2 package; `graphforge-storage` remains the authority that
//! verifies the downloaded package's integrity, compatibility, and authenticity.
//! Redirect bounds, HTTPS retention across redirects, and caller-configured host
//! policy belong to the transport/client adapter. This crate only admits each
//! protocol-visible location as an absolute, credential-free HTTPS URL.

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Write as _;
use url::Url;

/// Discovery protocol format emitted and accepted by this release.
pub const DISCOVERY_FORMAT: &str = "graphforge-discovery/1";
/// Portable project format referenced by discovery v1.
pub const PORTABLE_V2_FORMAT: &str = "graphforge-project/2";
/// Media type of the immutable portable-v2 package object selected by discovery.
pub const PORTABLE_V2_MEDIA_TYPE: &str = "application/vnd.graphforge.project";

/// Explicit resource bounds for untrusted discovery responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscoveryLimits {
    /// Maximum encoded manifest or refs response size.
    pub max_response_bytes: usize,
    /// Maximum bytes in any bounded string.
    pub max_string_bytes: usize,
    /// Maximum refs in one refs response.
    pub max_refs: usize,
    /// Maximum objects in one manifest.
    pub max_objects: usize,
    /// Maximum locations for one immutable object.
    pub max_locations_per_object: usize,
    /// Maximum declared bytes across the object inventory.
    pub max_cumulative_object_bytes: u64,
}

impl Default for DiscoveryLimits {
    fn default() -> Self {
        Self {
            max_response_bytes: 16 * 1024 * 1024,
            max_string_bytes: 4096,
            max_refs: 10_000,
            max_objects: 1_000_000,
            max_locations_per_object: 8,
            max_cumulative_object_bytes: 1024 * 1024_u64.pow(4),
        }
    }
}

/// Stable machine-readable discovery failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryErrorCode {
    /// The canonical owner/repository identity is invalid.
    InvalidIdentity,
    /// JSON or a field value is malformed.
    MalformedResponse,
    /// A required future protocol version or capability is unsupported.
    UnsupportedFuture,
    /// A requested ref is absent.
    MissingRef,
    /// A referenced object is absent.
    MissingObject,
    /// A digest, length, or strong validator disagrees.
    IntegrityFailure,
    /// An object location is not safe protocol input.
    UnsafeLocation,
    /// A configured input bound was exceeded.
    LimitExceeded,
    /// A canonical identity, ref, object, or location occurs more than once.
    Duplicate,
}

/// Semantic surface associated with structured version diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryVersionSubject {
    /// Discovery response protocol version.
    Protocol,
    /// Referenced portable package format version.
    PortablePackage,
    /// Required protocol capability version.
    Capability,
}

/// Sanitized supported/requested version metadata for compatibility failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DiscoveryVersionDetails {
    /// Versioned semantic surface that failed negotiation.
    pub subject: DiscoveryVersionSubject,
    /// Supported major version, or `None` when the semantic itself is unknown.
    pub supported_major: Option<u16>,
    /// Major version requested by the response.
    pub requested_major: u16,
}

/// Sanitized discovery error suitable for CLI, Hub, and telemetry projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DiscoveryError {
    /// Stable machine-readable code.
    pub code: DiscoveryErrorCode,
    /// Stable schema field associated with the failure, when applicable.
    pub field: Option<&'static str>,
    /// Structured version negotiation details, when the failure is versioned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<DiscoveryVersionDetails>,
    #[serde(skip)]
    detail: &'static str,
}

impl DiscoveryError {
    fn new(code: DiscoveryErrorCode, field: Option<&'static str>, detail: &'static str) -> Self {
        Self {
            code,
            field,
            version: None,
            detail,
        }
    }

    fn with_version(mut self, version: DiscoveryVersionDetails) -> Self {
        self.version = Some(version);
        self
    }

    /// Sanitized, non-input-bearing diagnostic text.
    #[must_use]
    pub const fn detail(&self) -> &'static str {
        self.detail
    }
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "discovery {:?}: {}", self.code, self.detail)
    }
}

impl std::error::Error for DiscoveryError {}

/// Canonical provider-independent repository identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryIdentity {
    /// Namespace owner segment.
    pub owner: String,
    /// Repository name segment.
    pub repository: String,
}

impl RepositoryIdentity {
    /// Parse `owner/repository` without accepting schemes, hosts, or extra path segments.
    pub fn parse(value: &str) -> Result<Self, DiscoveryError> {
        let (owner, repository) = value.split_once('/').ok_or_else(invalid_identity)?;
        if repository.contains('/') {
            return Err(invalid_identity());
        }
        let identity = Self {
            owner: owner.to_owned(),
            repository: repository.to_owned(),
        };
        identity.validate()?;
        Ok(identity)
    }

    /// Validate both canonical path segments.
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        if !valid_slug(&self.owner, 100) || !valid_slug(&self.repository, 100) {
            return Err(invalid_identity());
        }
        Ok(())
    }

    /// Return the canonical `owner/repository` spelling.
    #[must_use]
    pub fn canonical_name(&self) -> String {
        format!("{}/{}", self.owner, self.repository)
    }
}

fn invalid_identity() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::InvalidIdentity,
        Some("repository"),
        "repository identity is invalid",
    )
}

fn valid_slug(value: &str, max: usize) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= max
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
        && !value.ends_with(['-', '_', '.'])
        && !value.contains("..")
}

/// Major/minor discovery protocol version.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolVersion {
    /// Required major version.
    pub major: u16,
    /// Backward-compatible minor version.
    pub minor: u16,
}

impl ProtocolVersion {
    /// Version implemented by this crate.
    pub const CURRENT: Self = Self { major: 1, minor: 0 };

    fn validate(self) -> Result<(), DiscoveryError> {
        if self.major != Self::CURRENT.major {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::UnsupportedFuture,
                Some("version.major"),
                "protocol major version is unsupported",
            )
            .with_version(DiscoveryVersionDetails {
                subject: DiscoveryVersionSubject::Protocol,
                supported_major: Some(Self::CURRENT.major),
                requested_major: self.major,
            }));
        }
        Ok(())
    }
}

/// Lowercase SHA-256 identity with an explicit algorithm prefix.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sha256Digest(pub String);

impl Sha256Digest {
    /// Validate `sha256:` followed by exactly 64 lowercase hexadecimal digits.
    pub fn validate(&self) -> Result<(), DiscoveryError> {
        let Some(hex) = self.0.strip_prefix("sha256:") else {
            return Err(invalid_digest());
        };
        if hex.len() != 64
            || !hex
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(invalid_digest());
        }
        Ok(())
    }
}

fn invalid_digest() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::IntegrityFailure,
        Some("digest"),
        "SHA-256 digest is invalid",
    )
}

/// Required semantic understood by a reader before it may access objects.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolRequirement {
    /// Stable capability identifier.
    pub capability: String,
    /// Required capability major version.
    pub major: u16,
}

/// Capability a server advertises without requiring client support.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolCapability {
    /// Stable capability identifier.
    pub capability: String,
    /// Advertised capability major version.
    pub major: u16,
}

/// Reference to the semantic portable-v2 package verified after download.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortablePackageReference {
    /// Must be [`PORTABLE_V2_FORMAT`] in discovery v1.
    pub format: String,
    /// Portable semantic package identity, distinct from transport/object identities.
    pub package_digest: Sha256Digest,
    /// Transport object digest selecting exactly one entry from `objects`.
    ///
    /// This is the digest of downloaded bytes, not the semantic portable-v2
    /// package digest above.
    pub object_digest: Sha256Digest,
}

/// One immutable directly downloadable object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectDescriptor {
    /// Content digest and object identity.
    pub digest: Sha256Digest,
    /// Exact object length in bytes.
    pub length: u64,
    /// Lowercase Internet media type without parameters.
    pub media_type: String,
    /// Ordered absolute HTTPS alternatives. Locations are transport, never identity.
    pub locations: Vec<String>,
}

/// Validated repository discovery manifest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryManifest {
    /// Contract identifier; must equal [`DISCOVERY_FORMAT`].
    pub format: String,
    /// Protocol reader/writer version.
    pub version: ProtocolVersion,
    /// Canonical provider-independent repository identity.
    pub repository: RepositoryIdentity,
    /// Default ref at the represented snapshot.
    pub default_ref: String,
    /// Ref resolved for this manifest.
    pub resolved_ref: String,
    /// Immutable repository-version identity.
    pub immutable_version: Sha256Digest,
    /// Semantic portable-v2 package identity.
    pub package: PortablePackageReference,
    /// Required semantics checked before object access.
    pub requirements: Vec<ProtocolRequirement>,
    /// Optional advertised semantics.
    pub capabilities: Vec<ProtocolCapability>,
    /// Complete bounded transport inventory.
    pub objects: Vec<ObjectDescriptor>,
    /// Explicit optional extension values, preserved canonically by readers.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Value>,
}

/// One named ref and its immutable target plus strong HTTP validator.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryRef {
    /// Canonical ref name.
    pub name: String,
    /// Immutable repository version selected by this ref snapshot.
    pub target: Sha256Digest,
    /// HTTP-independent immutable revision validator.
    ///
    /// An HTTP adapter may map this digest to a quoted strong ETag; HTTP syntax
    /// is deliberately not part of the discovery response contract.
    pub validator: Sha256Digest,
}

/// Validated immutable snapshot of repository refs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefSet {
    /// Contract identifier; must equal [`DISCOVERY_FORMAT`].
    pub format: String,
    /// Protocol reader/writer version.
    pub version: ProtocolVersion,
    /// Repository bound by these refs.
    pub repository: RepositoryIdentity,
    /// Default ref, which must occur exactly once in `refs`.
    pub default_ref: String,
    /// Canonically ordered ref entries.
    pub refs: Vec<RepositoryRef>,
    /// Explicit optional extension values, preserved canonically by readers.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Value>,
}

impl DiscoveryManifest {
    /// Parse and fully validate untrusted JSON without performing object I/O.
    pub fn from_json(bytes: &[u8], limits: DiscoveryLimits) -> Result<Self, DiscoveryError> {
        check_response_bound(bytes, limits)?;
        let manifest: Self = parse_unique_json(bytes)?;
        manifest.validate(limits)?;
        Ok(manifest)
    }

    /// Validate all discovery-v1 invariants before object access.
    pub fn validate(&self, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
        validate_header(&self.format, self.version, &self.repository, limits)?;
        validate_ref_name(&self.default_ref, limits)?;
        validate_ref_name(&self.resolved_ref, limits)?;
        self.immutable_version.validate()?;
        if self.package.format != PORTABLE_V2_FORMAT {
            let error = DiscoveryError::new(
                DiscoveryErrorCode::UnsupportedFuture,
                Some("package.format"),
                "portable package format is unsupported",
            );
            return Err(
                match format_major(&self.package.format, "graphforge-project") {
                    Some(requested_major) => error.with_version(DiscoveryVersionDetails {
                        subject: DiscoveryVersionSubject::PortablePackage,
                        supported_major: Some(2),
                        requested_major,
                    }),
                    None => error,
                },
            );
        }
        self.package.package_digest.validate()?;
        self.package.object_digest.validate()?;
        validate_semantics(&self.requirements, &self.capabilities, limits)?;
        validate_extensions(&self.extensions, limits)?;
        validate_objects(&self.objects, limits)?;
        self.package_object().map(|_| ())
    }

    /// Return the uniquely selected portable-v2 transport object.
    ///
    /// Callers use this only after manifest validation and therefore never
    /// guess by inventory position, location host, or media type.
    pub fn package_object(&self) -> Result<&ObjectDescriptor, DiscoveryError> {
        let object = self
            .objects
            .iter()
            .find(|object| object.digest == self.package.object_digest)
            .ok_or_else(|| {
                DiscoveryError::new(
                    DiscoveryErrorCode::MissingObject,
                    Some("package.object_digest"),
                    "portable package object is absent",
                )
            })?;
        if object.media_type != PORTABLE_V2_MEDIA_TYPE {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MalformedResponse,
                Some("package.object_digest"),
                "portable package object media type is incompatible",
            ));
        }
        Ok(object)
    }

    /// Encode deterministic compact JSON with recursively sorted extension keys.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, DiscoveryError> {
        self.validate(DiscoveryLimits::default())?;
        canonical_json(self)
    }

    /// Compute SHA-256 over [`Self::to_canonical_json`].
    pub fn canonical_digest(&self) -> Result<Sha256Digest, DiscoveryError> {
        let digest = Sha256::digest(self.to_canonical_json()?);
        let hex = digest
            .iter()
            .fold(String::with_capacity(64), |mut hex, byte| {
                write!(hex, "{byte:02x}").expect("writing to a string cannot fail");
                hex
            });
        Ok(Sha256Digest(format!("sha256:{hex}")))
    }
}

impl RefSet {
    /// Parse and fully validate an untrusted refs response.
    pub fn from_json(bytes: &[u8], limits: DiscoveryLimits) -> Result<Self, DiscoveryError> {
        check_response_bound(bytes, limits)?;
        let refs: Self = parse_unique_json(bytes)?;
        refs.validate(limits)?;
        Ok(refs)
    }

    /// Validate refs, ordering, uniqueness, target digests, and strong validators.
    pub fn validate(&self, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
        validate_header(&self.format, self.version, &self.repository, limits)?;
        validate_ref_name(&self.default_ref, limits)?;
        validate_extensions(&self.extensions, limits)?;
        if self.refs.len() > limits.max_refs {
            return Err(limit("refs"));
        }
        let mut prior = None;
        let mut found_default = false;
        for reference in &self.refs {
            validate_ref_name(&reference.name, limits)?;
            reference.target.validate()?;
            reference.validator.validate()?;
            if prior.is_some_and(|name: &str| name >= reference.name.as_str()) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::Duplicate,
                    Some("refs.name"),
                    "refs are duplicated or not canonically ordered",
                ));
            }
            prior = Some(&reference.name);
            found_default |= reference.name == self.default_ref;
        }
        if !found_default {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MissingRef,
                Some("default_ref"),
                "default ref is absent",
            ));
        }
        Ok(())
    }

    /// Verify that a manifest is bound to this exact repository/ref snapshot.
    pub fn validate_manifest(&self, manifest: &DiscoveryManifest) -> Result<(), DiscoveryError> {
        if self.repository != manifest.repository || self.default_ref != manifest.default_ref {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::IntegrityFailure,
                Some("repository"),
                "manifest and refs snapshot disagree",
            ));
        }
        let Some(reference) = self
            .refs
            .iter()
            .find(|item| item.name == manifest.resolved_ref)
        else {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MissingRef,
                Some("resolved_ref"),
                "resolved ref is absent",
            ));
        };
        if reference.target != manifest.immutable_version {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::IntegrityFailure,
                Some("immutable_version"),
                "resolved ref target disagrees",
            ));
        }
        Ok(())
    }

    /// Encode deterministic compact JSON with recursively sorted extension keys.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, DiscoveryError> {
        self.validate(DiscoveryLimits::default())?;
        canonical_json(self)
    }
}

fn validate_header(
    format: &str,
    version: ProtocolVersion,
    repository: &RepositoryIdentity,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    if format != DISCOVERY_FORMAT {
        let error = DiscoveryError::new(
            DiscoveryErrorCode::UnsupportedFuture,
            Some("format"),
            "discovery format is unsupported",
        );
        return Err(match format_major(format, "graphforge-discovery") {
            Some(requested_major) => error.with_version(DiscoveryVersionDetails {
                subject: DiscoveryVersionSubject::Protocol,
                supported_major: Some(ProtocolVersion::CURRENT.major),
                requested_major,
            }),
            None => error,
        });
    }
    version.validate()?;
    repository.validate()?;
    check_string(format, "format", limits)
}

fn format_major(format: &str, expected_name: &str) -> Option<u16> {
    let (name, major) = format.rsplit_once('/')?;
    (name == expected_name)
        .then(|| major.parse().ok())
        .flatten()
}

fn validate_semantics(
    requirements: &[ProtocolRequirement],
    capabilities: &[ProtocolCapability],
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    if requirements.len() > 256 || capabilities.len() > 256 {
        return Err(limit("requirements"));
    }
    let mut prior: Option<(&str, u16)> = None;
    for requirement in requirements {
        check_capability_name(&requirement.capability, limits)?;
        let current = (requirement.capability.as_str(), requirement.major);
        if prior.is_some_and(|value| value >= current) {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::Duplicate,
                Some("requirements"),
                "requirements are duplicated or not canonically ordered",
            ));
        }
        prior = Some(current);
        if !matches!(
            (requirement.capability.as_str(), requirement.major),
            ("portable-v2", 1)
        ) {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::UnsupportedFuture,
                Some("requirements"),
                "required capability is unsupported",
            )
            .with_version(DiscoveryVersionDetails {
                subject: DiscoveryVersionSubject::Capability,
                supported_major: (requirement.capability == "portable-v2").then_some(1),
                requested_major: requirement.major,
            }));
        }
    }
    let mut prior: Option<(&str, u16)> = None;
    for capability in capabilities {
        check_capability_name(&capability.capability, limits)?;
        let current = (capability.capability.as_str(), capability.major);
        if prior.is_some_and(|value| value >= current) {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::Duplicate,
                Some("capabilities"),
                "capabilities are duplicated or not canonically ordered",
            ));
        }
        prior = Some(current);
    }
    Ok(())
}

fn check_capability_name(value: &str, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
    check_string(value, "capability", limits)?;
    if !valid_slug(value, 128) {
        return Err(malformed());
    }
    Ok(())
}

fn validate_objects(
    objects: &[ObjectDescriptor],
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    if objects.is_empty() {
        return Err(DiscoveryError::new(
            DiscoveryErrorCode::MissingObject,
            Some("objects"),
            "object inventory is empty",
        ));
    }
    if objects.len() > limits.max_objects {
        return Err(limit("objects"));
    }
    let mut prior: Option<&Sha256Digest> = None;
    let mut total = 0_u64;
    for object in objects {
        object.digest.validate()?;
        if prior.is_some_and(|digest| digest >= &object.digest) {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::Duplicate,
                Some("objects.digest"),
                "objects are duplicated or not canonically ordered",
            ));
        }
        prior = Some(&object.digest);
        total = total
            .checked_add(object.length)
            .ok_or_else(|| limit("objects.length"))?;
        if total > limits.max_cumulative_object_bytes {
            return Err(limit("objects.length"));
        }
        validate_media_type(&object.media_type, limits)?;
        if object.locations.is_empty() {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MissingObject,
                Some("objects.locations"),
                "object has no location",
            ));
        }
        if object.locations.len() > limits.max_locations_per_object {
            return Err(limit("objects.locations"));
        }
        let mut seen = BTreeSet::new();
        let mut prior_location: Option<&str> = None;
        for location in &object.locations {
            check_string(location, "objects.locations", limits)?;
            validate_location(location)?;
            if !seen.insert(location) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::Duplicate,
                    Some("objects.locations"),
                    "object location is duplicated",
                ));
            }
            if prior_location.is_some_and(|prior| prior >= location.as_str()) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::Duplicate,
                    Some("objects.locations"),
                    "object locations are not canonically ordered",
                ));
            }
            prior_location = Some(location);
        }
    }
    Ok(())
}

fn validate_location(value: &str) -> Result<(), DiscoveryError> {
    let parsed = Url::parse(value).map_err(|_| unsafe_location())?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(unsafe_location());
    }
    Ok(())
}

fn unsafe_location() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::UnsafeLocation,
        Some("objects.locations"),
        "object location is unsafe",
    )
}

fn validate_media_type(value: &str, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
    check_string(value, "objects.media_type", limits)?;
    let Some((kind, subtype)) = value.split_once('/') else {
        return Err(malformed());
    };
    if kind.is_empty()
        || subtype.is_empty()
        || value.contains([';', ' ', '\t', '\r', '\n'])
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"!#$&^_.+-/".contains(&byte)
        })
    {
        return Err(malformed());
    }
    Ok(())
}

fn validate_ref_name(value: &str, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
    check_string(value, "ref", limits)?;
    if value.starts_with('/')
        || value.ends_with('/')
        || value.contains("..")
        || value.contains("//")
        || value.bytes().any(|byte| {
            byte.is_ascii_control()
                || matches!(byte, b'\\' | b'?' | b'#' | b'~' | b'^' | b':' | b' ')
        })
    {
        return Err(DiscoveryError::new(
            DiscoveryErrorCode::MalformedResponse,
            Some("ref"),
            "ref name is invalid",
        ));
    }
    Ok(())
}

fn validate_extensions(
    extensions: &BTreeMap<String, Value>,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    if extensions.len() > 256 {
        return Err(limit("extensions"));
    }
    for (key, value) in extensions {
        check_string(key, "extensions", limits)?;
        if !key.starts_with("x-") || !valid_slug(key, 128) {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MalformedResponse,
                Some("extensions"),
                "extension key is invalid",
            ));
        }
        validate_extension_value(value, limits, 0)?;
    }
    Ok(())
}

fn validate_extension_value(
    value: &Value,
    limits: DiscoveryLimits,
    depth: u8,
) -> Result<(), DiscoveryError> {
    if depth > 16 {
        return Err(limit("extensions"));
    }
    match value {
        Value::String(value) => check_string(value, "extensions", limits),
        Value::Array(values) => {
            if values.len() > 1024 {
                return Err(limit("extensions"));
            }
            for value in values {
                validate_extension_value(value, limits, depth + 1)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            if values.len() > 1024 {
                return Err(limit("extensions"));
            }
            for (key, value) in values {
                check_string(key, "extensions", limits)?;
                validate_extension_value(value, limits, depth + 1)?;
            }
            Ok(())
        }
        Value::Number(number) if number.is_i64() || number.is_u64() => Ok(()),
        Value::Number(_) => Err(DiscoveryError::new(
            DiscoveryErrorCode::MalformedResponse,
            Some("extensions"),
            "extension number is not a canonical integer",
        )),
        Value::Null | Value::Bool(_) => Ok(()),
    }
}

fn check_response_bound(bytes: &[u8], limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
    if bytes.len() > limits.max_response_bytes {
        return Err(limit("response"));
    }
    Ok(())
}

fn check_string(
    value: &str,
    field: &'static str,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    if value.is_empty() || value.len() > limits.max_string_bytes {
        return Err(limit(field));
    }
    Ok(())
}

fn limit(field: &'static str) -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::LimitExceeded,
        Some(field),
        "discovery limit exceeded",
    )
}

fn malformed() -> DiscoveryError {
    DiscoveryError::new(
        DiscoveryErrorCode::MalformedResponse,
        None,
        "discovery response is malformed",
    )
}

fn parse_unique_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, DiscoveryError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let unique = UniqueJson::deserialize(&mut deserializer).map_err(|_| malformed())?;
    deserializer.end().map_err(|_| malformed())?;
    serde_json::from_value(unique.0).map_err(|_| malformed())
}

struct UniqueJson(Value);

impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(UniqueJsonVisitor)
    }
}

struct UniqueJsonVisitor;

impl<'de> Visitor<'de> for UniqueJsonVisitor {
    type Value = UniqueJson;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON without duplicate object members")
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Bool(value)))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Number(value.into())))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Number(value.into())))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueJson)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::String(value.to_owned())))
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::String(value)))
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Null))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueJson(Value::Null))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<UniqueJson>()? {
            values.push(value.0);
        }
        Ok(UniqueJson(Value::Array(values)))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom("duplicate JSON object member"));
            }
            values.insert(key, map.next_value::<UniqueJson>()?.0);
        }
        Ok(UniqueJson(Value::Object(values)))
    }
}

fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, DiscoveryError> {
    let mut value = serde_json::to_value(value).map_err(|_| malformed())?;
    sort_json(&mut value);
    serde_json::to_vec(&value).map_err(|_| malformed())
}

fn sort_json(value: &mut Value) {
    match value {
        Value::Array(values) => values.iter_mut().for_each(sort_json),
        Value::Object(values) => {
            for value in values.values_mut() {
                sort_json(value);
            }
            let old = std::mem::take(values);
            let sorted: BTreeMap<_, _> = old.into_iter().collect();
            values.extend(sorted);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn digest(marker: char) -> Sha256Digest {
        Sha256Digest(format!("sha256:{}", marker.to_string().repeat(64)))
    }

    fn manifest() -> DiscoveryManifest {
        DiscoveryManifest {
            format: DISCOVERY_FORMAT.to_owned(),
            version: ProtocolVersion::CURRENT,
            repository: RepositoryIdentity::parse("openalex/openalex").unwrap(),
            default_ref: "main".to_owned(),
            resolved_ref: "main".to_owned(),
            immutable_version: digest('a'),
            package: PortablePackageReference {
                format: PORTABLE_V2_FORMAT.to_owned(),
                package_digest: digest('b'),
                object_digest: digest('c'),
            },
            requirements: vec![ProtocolRequirement {
                capability: "portable-v2".to_owned(),
                major: 1,
            }],
            capabilities: vec![ProtocolCapability {
                capability: "range-requests".to_owned(),
                major: 1,
            }],
            objects: vec![ObjectDescriptor {
                digest: digest('c'),
                length: 42,
                media_type: "application/vnd.graphforge.project".to_owned(),
                locations: vec!["https://data.graphforge.sh/objects/sha256/cccc".to_owned()],
            }],
            extensions: BTreeMap::from([(
                "x-example".to_owned(),
                json!({"z": 1, "a": [true, "ok"]}),
            )]),
        }
    }

    #[test]
    fn identity_is_canonical_and_provider_independent() {
        let identity = RepositoryIdentity::parse("openalex/openalex").unwrap();
        assert_eq!(identity.canonical_name(), "openalex/openalex");
        for invalid in [
            "https://graphforge.sh/openalex/openalex",
            "OpenAlex/openalex",
            "a/../b",
            "a/",
        ] {
            assert_eq!(
                RepositoryIdentity::parse(invalid).unwrap_err().code,
                DiscoveryErrorCode::InvalidIdentity
            );
        }
    }

    #[test]
    fn canonical_json_and_digest_are_deterministic() {
        let first = manifest();
        let mut second = manifest();
        second
            .extensions
            .insert("x-other".to_owned(), json!({"b": 2, "a": 1}));
        second.extensions.remove("x-other");
        assert_eq!(
            first.to_canonical_json().unwrap(),
            second.to_canonical_json().unwrap()
        );
        assert_eq!(
            first.canonical_digest().unwrap(),
            second.canonical_digest().unwrap()
        );
        let encoded = String::from_utf8(first.to_canonical_json().unwrap()).unwrap();
        assert!(encoded.contains("\"x-example\":{\"a\":[true,\"ok\"],\"z\":1}"));
    }

    #[test]
    fn package_object_is_explicit_among_multiple_transport_objects() {
        let mut candidate = manifest();
        candidate.objects.insert(
            0,
            ObjectDescriptor {
                digest: digest('b'),
                length: 7,
                media_type: "application/octet-stream".to_owned(),
                locations: vec!["https://data.graphforge.sh/objects/sha256/bbbb".to_owned()],
            },
        );
        candidate.validate(DiscoveryLimits::default()).unwrap();
        assert_eq!(candidate.package_object().unwrap().digest, digest('c'));

        candidate.package.object_digest = digest('d');
        let error = candidate.validate(DiscoveryLimits::default()).unwrap_err();
        assert_eq!(error.code, DiscoveryErrorCode::MissingObject);
        assert_eq!(error.field, Some("package.object_digest"));
    }

    #[test]
    fn future_required_semantics_fail_before_locations_are_considered() {
        let mut candidate = manifest();
        candidate.requirements[0] = ProtocolRequirement {
            capability: "future-fetch".to_owned(),
            major: 1,
        };
        candidate.objects[0].locations[0] = "http://unsafe.example/object".to_owned();
        let error = candidate.validate(DiscoveryLimits::default()).unwrap_err();
        assert_eq!(error.code, DiscoveryErrorCode::UnsupportedFuture);
        assert_eq!(error.field, Some("requirements"));
        assert_eq!(
            error.version,
            Some(DiscoveryVersionDetails {
                subject: DiscoveryVersionSubject::Capability,
                supported_major: None,
                requested_major: 1,
            })
        );

        let mut future_protocol = manifest();
        future_protocol.version.major = 2;
        let error = future_protocol
            .validate(DiscoveryLimits::default())
            .unwrap_err();
        assert_eq!(
            error.version,
            Some(DiscoveryVersionDetails {
                subject: DiscoveryVersionSubject::Protocol,
                supported_major: Some(1),
                requested_major: 2,
            })
        );
        assert_eq!(
            serde_json::to_string(&error).unwrap(),
            r#"{"code":"unsupported_future","field":"version.major","version":{"subject":"protocol","supported_major":1,"requested_major":2}}"#
        );

        let mut future_package = manifest();
        future_package.package.format = "graphforge-project/3".to_owned();
        let error = future_package
            .validate(DiscoveryLimits::default())
            .unwrap_err();
        assert_eq!(
            error.version,
            Some(DiscoveryVersionDetails {
                subject: DiscoveryVersionSubject::PortablePackage,
                supported_major: Some(2),
                requested_major: 3,
            })
        );
    }

    #[test]
    fn unsafe_locations_and_invalid_inventory_fail_closed() {
        for location in [
            "http://data.graphforge.sh/object",
            "https://user:secret@data.graphforge.sh/object",
            "/objects/local",
            "https://data.graphforge.sh/object#fragment",
        ] {
            let mut candidate = manifest();
            candidate.objects[0].locations[0] = location.to_owned();
            assert_eq!(
                candidate
                    .validate(DiscoveryLimits::default())
                    .unwrap_err()
                    .code,
                DiscoveryErrorCode::UnsafeLocation
            );
        }
        let mut duplicate = manifest();
        duplicate.objects.push(duplicate.objects[0].clone());
        assert_eq!(
            duplicate
                .validate(DiscoveryLimits::default())
                .unwrap_err()
                .code,
            DiscoveryErrorCode::Duplicate
        );
        let mut overflow = manifest();
        overflow.objects[0].length = u64::MAX;
        assert_eq!(
            overflow
                .validate(DiscoveryLimits::default())
                .unwrap_err()
                .code,
            DiscoveryErrorCode::LimitExceeded
        );
    }

    #[test]
    fn refs_use_protocol_validators_and_are_consistent_with_manifest() {
        let refs = RefSet {
            format: DISCOVERY_FORMAT.to_owned(),
            version: ProtocolVersion::CURRENT,
            repository: RepositoryIdentity::parse("openalex/openalex").unwrap(),
            default_ref: "main".to_owned(),
            refs: vec![RepositoryRef {
                name: "main".to_owned(),
                target: digest('a'),
                validator: digest('d'),
            }],
            extensions: BTreeMap::new(),
        };
        refs.validate(DiscoveryLimits::default()).unwrap();
        refs.validate_manifest(&manifest()).unwrap();
        let mut invalid = refs.clone();
        invalid.refs[0].validator = Sha256Digest("W/\"weak\"".to_owned());
        assert_eq!(
            invalid
                .validate(DiscoveryLimits::default())
                .unwrap_err()
                .code,
            DiscoveryErrorCode::IntegrityFailure
        );
        let mut missing = refs;
        missing.default_ref = "trunk".to_owned();
        assert_eq!(
            missing
                .validate(DiscoveryLimits::default())
                .unwrap_err()
                .code,
            DiscoveryErrorCode::MissingRef
        );
    }

    #[test]
    fn parsing_rejects_unknown_fields_and_preserves_explicit_extensions() {
        let bytes = manifest().to_canonical_json().unwrap();
        let parsed = DiscoveryManifest::from_json(&bytes, DiscoveryLimits::default()).unwrap();
        assert_eq!(parsed.extensions["x-example"]["z"], 1);
        let mut value: Value = serde_json::from_slice(&bytes).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("surprise".to_owned(), Value::Bool(true));
        let error = DiscoveryManifest::from_json(
            &serde_json::to_vec(&value).unwrap(),
            DiscoveryLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.code, DiscoveryErrorCode::MalformedResponse);

        let duplicate = bytes
            .strip_suffix(b"}")
            .unwrap()
            .iter()
            .copied()
            .chain(b",\"format\":\"graphforge-discovery/1\"}".iter().copied())
            .collect::<Vec<_>>();
        let error =
            DiscoveryManifest::from_json(&duplicate, DiscoveryLimits::default()).unwrap_err();
        assert_eq!(error.code, DiscoveryErrorCode::MalformedResponse);
    }

    #[test]
    fn response_and_collection_limits_are_enforced() {
        let bytes = manifest().to_canonical_json().unwrap();
        let limits = DiscoveryLimits {
            max_response_bytes: bytes.len() - 1,
            ..DiscoveryLimits::default()
        };
        assert_eq!(
            DiscoveryManifest::from_json(&bytes, limits)
                .unwrap_err()
                .code,
            DiscoveryErrorCode::LimitExceeded
        );
        let mut candidate = manifest();
        candidate.objects[0]
            .locations
            .push("https://mirror.graphforge.sh/object".to_owned());
        let limits = DiscoveryLimits {
            max_locations_per_object: 1,
            ..DiscoveryLimits::default()
        };
        assert_eq!(
            candidate.validate(limits).unwrap_err().code,
            DiscoveryErrorCode::LimitExceeded
        );
    }

    #[test]
    fn error_serialization_is_stable_and_sanitized() {
        let error = RepositoryIdentity::parse("SECRET/invalid").unwrap_err();
        assert_eq!(
            serde_json::to_string(&error).unwrap(),
            r#"{"code":"invalid_identity","field":"repository"}"#
        );
        assert!(!error.to_string().contains("SECRET"));
    }
}
