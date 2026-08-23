//! Canonical, provider-neutral GraphForge Hub discovery contract.
//!
//! This module owns discovery parsing, validation, version negotiation, and
//! normalized errors. It deliberately references portable-v2 package identity;
//! [`crate::verify_portable_v2`] remains the authority for package compatibility,
//! integrity, and authenticity.

use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::IpAddr;

/// Current repository discovery protocol identifier.
pub const HUB_PROTOCOL: &str = "graphforge-hub/1";
/// Media type for `/{owner}/{repo}/.gf/manifest`.
pub const HUB_DISCOVERY_MANIFEST_MEDIA_TYPE: &str =
    "application/vnd.graphforge.hub-manifest.v1+json";
/// Media type for `/{owner}/{repo}/.gf/refs`.
pub const HUB_REFS_MEDIA_TYPE: &str = "application/vnd.graphforge.hub-refs.v1+json";
/// Media type for the canonical portable-v2 bundle selected for import.
pub const HUB_PACKAGE_BUNDLE_MEDIA_TYPE: &str = "application/vnd.graphforge.project.v2+tar";

const PORTABLE_FORMAT: &str = "graphforge-project/2";
const SUPPORTED_CAPABILITIES: &[(&str, u32)] = &[("portable-v2", 2), ("sha256", 1)];

/// Resource limits applied before a response is accepted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HubProtocolLimits {
    /// Maximum encoded response bytes.
    pub max_response_bytes: usize,
    /// Maximum number of objects or refs.
    pub max_entries: usize,
    /// Maximum bytes in one string.
    pub max_string_bytes: usize,
}

impl Default for HubProtocolLimits {
    fn default() -> Self {
        Self {
            max_response_bytes: 4 * 1024 * 1024,
            max_entries: 100_000,
            max_string_bytes: 4096,
        }
    }
}

/// Stable machine-readable protocol failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HubProtocolErrorCode {
    /// Owner or repository name is not canonical.
    InvalidIdentity,
    /// JSON or a required field is malformed.
    MalformedResponse,
    /// A required protocol or capability version is newer than this reader.
    UnsupportedFuture,
    /// A requested ref is absent.
    MissingRef,
    /// A referenced object is absent.
    MissingObject,
    /// Digest or byte length does not match the descriptor.
    IntegrityFailure,
    /// An object location violates transport policy.
    UnsafeLocation,
    /// A configured resource bound was exceeded.
    LimitExceeded,
}

/// Sanitized protocol error safe for CLI JSON and telemetry classification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HubProtocolError {
    /// Stable code.
    pub code: HubProtocolErrorCode,
    /// Stable diagnostic identifier.
    pub diagnostic: &'static str,
}

impl HubProtocolError {
    fn new(code: HubProtocolErrorCode, diagnostic: &'static str) -> Self {
        Self { code, diagnostic }
    }
}

impl fmt::Display for HubProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "hub protocol {:?}: {}", self.code, self.diagnostic)
    }
}

impl std::error::Error for HubProtocolError {}

/// Provider-independent repository identity.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubRepositoryIdentity {
    /// Canonical lower-case owner slug.
    pub owner: String,
    /// Canonical lower-case repository slug.
    pub name: String,
}

/// Parse `owner/repository` without consulting a network provider.
pub fn parse_hub_repository_identity(
    value: &str,
) -> Result<HubRepositoryIdentity, HubProtocolError> {
    let (owner, name) = value.split_once('/').ok_or_else(invalid_identity)?;
    if name.contains('/') || !valid_slug(owner) || !valid_slug(name) {
        return Err(invalid_identity());
    }
    Ok(HubRepositoryIdentity {
        owner: owner.to_owned(),
        name: name.to_owned(),
    })
}

/// A required discovery capability and its major version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubCapability {
    /// Capability name.
    pub name: String,
    /// Required major version.
    pub major: u32,
}

/// Existing portable-v2 semantic package identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubPackageIdentity {
    /// Must be `graphforge-project/2` for this protocol version.
    pub format: String,
    /// Portable-v2 semantic package digest.
    pub digest: String,
    /// Transport digest of the canonical portable-v2 bundle object.
    pub object_digest: String,
}

/// Immutable Hub version projected onto one portable package.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubVersionIdentity {
    /// Immutable, provider-neutral version identifier.
    pub id: String,
    /// Portable package identity verified by the portable-v2 authority.
    pub package: HubPackageIdentity,
}

/// One content-addressed transport object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubObjectDescriptor {
    /// `sha256:<64 lowercase hexadecimal characters>`.
    pub digest: String,
    /// Exact encoded byte length.
    pub size: u64,
    /// Provider-neutral media type.
    pub media_type: String,
    /// HTTPS data-plane location. This does not participate in repository identity.
    pub location: String,
}

/// Canonical response from `/{owner}/{repo}/.gf/manifest`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubDiscoveryManifest {
    /// Protocol identifier.
    pub protocol: String,
    /// Canonical repository identity.
    pub repository: HubRepositoryIdentity,
    /// Default ref name.
    pub default_ref: String,
    /// Immutable version and portable package identity.
    pub version: HubVersionIdentity,
    /// Required reader capabilities.
    pub required_capabilities: Vec<HubCapability>,
    /// Content-addressed data-plane objects.
    pub objects: Vec<HubObjectDescriptor>,
}

/// Canonical response from `/{owner}/{repo}/.gf/refs`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubRefs {
    /// Protocol identifier.
    pub protocol: String,
    /// Canonical repository identity.
    pub repository: HubRepositoryIdentity,
    /// Ref name to immutable version identifier.
    pub refs: BTreeMap<String, String>,
}

/// Parse and validate a discovery manifest before any object access or mutation.
pub fn parse_hub_manifest(
    bytes: &[u8],
    limits: HubProtocolLimits,
) -> Result<HubDiscoveryManifest, HubProtocolError> {
    let value = parse_bounded_json(bytes, limits)?;
    let manifest: HubDiscoveryManifest = serde_json::from_value(value).map_err(|_| malformed())?;
    validate_protocol(&manifest.protocol)?;
    validate_identity(&manifest.repository)?;
    validate_ref(&manifest.default_ref, limits)?;
    validate_version(&manifest.version, limits)?;
    if manifest.objects.len() > limits.max_entries
        || manifest.required_capabilities.len() > limits.max_entries
    {
        return Err(limit());
    }
    validate_capabilities(&manifest.required_capabilities, limits)?;
    let mut digests = BTreeSet::new();
    for object in &manifest.objects {
        validate_object(object, limits)?;
        if !digests.insert(object.digest.as_str()) {
            return Err(HubProtocolError::new(
                HubProtocolErrorCode::MalformedResponse,
                "hub.discovery.duplicate_object",
            ));
        }
    }
    let package_object = manifest
        .objects
        .iter()
        .find(|object| object.digest == manifest.version.package.object_digest)
        .ok_or_else(|| {
            HubProtocolError::new(
                HubProtocolErrorCode::MissingObject,
                "hub.discovery.missing_package_object",
            )
        })?;
    if package_object.media_type != HUB_PACKAGE_BUNDLE_MEDIA_TYPE {
        return Err(HubProtocolError::new(
            HubProtocolErrorCode::MalformedResponse,
            "hub.discovery.invalid_package_object",
        ));
    }
    Ok(manifest)
}

/// Parse and validate a refs response before resolving a version.
pub fn parse_hub_refs(
    bytes: &[u8],
    limits: HubProtocolLimits,
) -> Result<HubRefs, HubProtocolError> {
    let value = parse_bounded_json(bytes, limits)?;
    let refs: HubRefs = serde_json::from_value(value).map_err(|_| malformed())?;
    validate_protocol(&refs.protocol)?;
    validate_identity(&refs.repository)?;
    if refs.refs.len() > limits.max_entries {
        return Err(limit());
    }
    for (name, version) in &refs.refs {
        validate_ref(name, limits)?;
        validate_token(version, limits)?;
    }
    Ok(refs)
}

fn validate_protocol(protocol: &str) -> Result<(), HubProtocolError> {
    if protocol == HUB_PROTOCOL {
        return Ok(());
    }
    if protocol.starts_with("graphforge-hub/") {
        return Err(HubProtocolError::new(
            HubProtocolErrorCode::UnsupportedFuture,
            "hub.discovery.unsupported_future",
        ));
    }
    Err(malformed())
}

fn validate_identity(identity: &HubRepositoryIdentity) -> Result<(), HubProtocolError> {
    if valid_slug(&identity.owner) && valid_slug(&identity.name) {
        Ok(())
    } else {
        Err(invalid_identity())
    }
}

fn valid_slug(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 100
        && bytes[0].is_ascii_lowercase()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

fn validate_ref(value: &str, limits: HubProtocolLimits) -> Result<(), HubProtocolError> {
    validate_token(value, limits)?;
    if value.starts_with('.')
        || value.ends_with('.')
        || value.contains("..")
        || value.contains('@')
        || value.contains('\\')
    {
        return Err(malformed());
    }
    Ok(())
}

fn validate_token(value: &str, limits: HubProtocolLimits) -> Result<(), HubProtocolError> {
    if value.is_empty()
        || value.len() > limits.max_string_bytes
        || !value.is_ascii()
        || value
            .bytes()
            .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
    {
        return Err(malformed());
    }
    Ok(())
}

fn validate_version(
    version: &HubVersionIdentity,
    limits: HubProtocolLimits,
) -> Result<(), HubProtocolError> {
    validate_token(&version.id, limits)?;
    if version.package.format != PORTABLE_FORMAT {
        return Err(HubProtocolError::new(
            HubProtocolErrorCode::UnsupportedFuture,
            "hub.discovery.unsupported_portable_format",
        ));
    }
    validate_digest(&version.package.digest)
        .and_then(|()| validate_digest(&version.package.object_digest))
}

fn validate_capabilities(
    capabilities: &[HubCapability],
    limits: HubProtocolLimits,
) -> Result<(), HubProtocolError> {
    let mut names = BTreeSet::new();
    for capability in capabilities {
        validate_token(&capability.name, limits)?;
        if !names.insert(capability.name.as_str()) {
            return Err(malformed());
        }
        match SUPPORTED_CAPABILITIES
            .iter()
            .find(|(name, _)| *name == capability.name)
        {
            Some((_, major)) if *major == capability.major => {}
            _ => {
                return Err(HubProtocolError::new(
                    HubProtocolErrorCode::UnsupportedFuture,
                    "hub.discovery.unsupported_capability",
                ));
            }
        }
    }
    Ok(())
}

fn validate_object(
    object: &HubObjectDescriptor,
    limits: HubProtocolLimits,
) -> Result<(), HubProtocolError> {
    validate_digest(&object.digest)?;
    validate_token(&object.media_type, limits)?;
    if object.size == 0 {
        return Err(malformed());
    }
    validate_https_location(&object.location, limits)
}

fn validate_digest(value: &str) -> Result<(), HubProtocolError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(malformed());
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(malformed());
    }
    Ok(())
}

fn validate_https_location(
    location: &str,
    limits: HubProtocolLimits,
) -> Result<(), HubProtocolError> {
    if location.len() > limits.max_string_bytes || !location.is_ascii() {
        return Err(limit());
    }
    let Some(authority_and_path) = location.strip_prefix("https://") else {
        return Err(unsafe_location());
    };
    let authority = authority_and_path
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    let (host, port) = authority
        .split_once(':')
        .map_or((authority, None), |(host, port)| (host, Some(port)));
    if authority.is_empty()
        || authority.contains('@')
        || authority.matches(':').count() > 1
        || port.is_some_and(|port| port.parse::<u16>().is_err())
        || host.parse::<IpAddr>().is_ok()
        || !host.contains('.')
        || host.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || !label.as_bytes()[0].is_ascii_alphanumeric()
                || !label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                || !label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        || location.contains('?')
        || location.contains('#')
    {
        return Err(unsafe_location());
    }
    Ok(())
}

fn parse_bounded_json(bytes: &[u8], limits: HubProtocolLimits) -> Result<Value, HubProtocolError> {
    if bytes.len() > limits.max_response_bytes {
        return Err(limit());
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = UniqueValue
        .deserialize(&mut deserializer)
        .map_err(|_| malformed())?;
    deserializer.end().map_err(|_| malformed())?;
    validate_value_limits(&value, limits)?;
    Ok(value)
}

fn validate_value_limits(value: &Value, limits: HubProtocolLimits) -> Result<(), HubProtocolError> {
    match value {
        Value::String(value) => {
            if value.len() > limits.max_string_bytes {
                return Err(limit());
            }
        }
        Value::Array(values) => {
            if values.len() > limits.max_entries {
                return Err(limit());
            }
            for value in values {
                validate_value_limits(value, limits)?;
            }
        }
        Value::Object(values) => {
            if values.len() > limits.max_entries {
                return Err(limit());
            }
            for (key, value) in values {
                if key.len() > limits.max_string_bytes {
                    return Err(limit());
                }
                validate_value_limits(value, limits)?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct UniqueValue;

impl<'de> DeserializeSeed<'de> for UniqueValue {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueValueVisitor)
    }
}

struct UniqueValueVisitor;

impl<'de> Visitor<'de> for UniqueValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Value, E>
    where
        E: de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("non-finite number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueValue.deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element_seed(UniqueValue)? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom("duplicate object key"));
            }
            let value = map.next_value_seed(UniqueValue)?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

fn invalid_identity() -> HubProtocolError {
    HubProtocolError::new(
        HubProtocolErrorCode::InvalidIdentity,
        "hub.discovery.invalid_identity",
    )
}

fn malformed() -> HubProtocolError {
    HubProtocolError::new(
        HubProtocolErrorCode::MalformedResponse,
        "hub.discovery.malformed_response",
    )
}

fn unsafe_location() -> HubProtocolError {
    HubProtocolError::new(
        HubProtocolErrorCode::UnsafeLocation,
        "hub.discovery.unsafe_location",
    )
}

fn limit() -> HubProtocolError {
    HubProtocolError::new(
        HubProtocolErrorCode::LimitExceeded,
        "hub.discovery.limit_exceeded",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &[u8] = include_bytes!("../../../tests/fixtures/hub-protocol/v1/valid.json");
    const FUTURE: &[u8] = include_bytes!("../../../tests/fixtures/hub-protocol/v1/future.json");
    const DUPLICATE: &[u8] =
        include_bytes!("../../../tests/fixtures/hub-protocol/v1/duplicate.json");
    const UNSAFE: &[u8] = include_bytes!("../../../tests/fixtures/hub-protocol/v1/unsafe.json");
    const MINIMAL: &[u8] = include_bytes!("../../../tests/fixtures/hub-protocol/v1/minimal.json");
    const UNKNOWN: &[u8] =
        include_bytes!("../../../tests/fixtures/hub-protocol/v1/unknown-field.json");
    const INTEGRITY_CASE: &[u8] =
        include_bytes!("../../../tests/fixtures/hub-protocol/v1/integrity-failure.json");
    const REFS: &[u8] = include_bytes!("../../../tests/fixtures/hub-protocol/v1/refs.json");
    const MANIFEST_SCHEMA: &[u8] =
        include_bytes!("../../../docs/contracts/graphforge-hub-manifest-v1.schema.json");
    const REFS_SCHEMA: &[u8] =
        include_bytes!("../../../docs/contracts/graphforge-hub-refs-v1.schema.json");

    #[test]
    fn conformance_fixture_accepts_current_manifest() {
        let manifest = parse_hub_manifest(VALID, HubProtocolLimits::default()).unwrap();
        assert_eq!(manifest.repository.owner, "openalex");
        assert_eq!(manifest.repository.name, "openalex");
        assert_eq!(manifest.version.package.format, PORTABLE_FORMAT);
    }

    #[test]
    fn future_protocol_fails_before_object_consumption() {
        let error = parse_hub_manifest(FUTURE, HubProtocolLimits::default()).unwrap_err();
        assert_eq!(error.code, HubProtocolErrorCode::UnsupportedFuture);
    }

    #[test]
    fn duplicate_keys_are_rejected() {
        let error = parse_hub_manifest(DUPLICATE, HubProtocolLimits::default()).unwrap_err();
        assert_eq!(error.code, HubProtocolErrorCode::MalformedResponse);
    }

    #[test]
    fn unsafe_locations_are_rejected() {
        let error = parse_hub_manifest(UNSAFE, HubProtocolLimits::default()).unwrap_err();
        assert_eq!(error.code, HubProtocolErrorCode::UnsafeLocation);
        let mut manifest = parse_hub_manifest(VALID, HubProtocolLimits::default()).unwrap();
        for location in [
            "https://127.0.0.1/object",
            "https://data.graphforge.sh:invalid/object",
            "https://data.graphforge.sh:70000/object",
            "https://-data.graphforge.sh/object",
            "https://data..graphforge.sh/object",
        ] {
            manifest.objects[0].location = location.into();
            let error = parse_hub_manifest(
                &serde_json::to_vec(&manifest).unwrap(),
                HubProtocolLimits::default(),
            )
            .unwrap_err();
            assert_eq!(error.code, HubProtocolErrorCode::UnsafeLocation);
        }
    }

    #[test]
    fn provider_location_does_not_change_repository_identity() {
        let mut first = parse_hub_manifest(VALID, HubProtocolLimits::default()).unwrap();
        let identity = first.repository.clone();
        first.objects[0].location =
            "https://replacement.example/objects/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into();
        let reparsed = parse_hub_manifest(
            &serde_json::to_vec(&first).unwrap(),
            HubProtocolLimits::default(),
        )
        .unwrap();
        assert_eq!(reparsed.repository, identity);
    }

    #[test]
    fn serialization_is_deterministic() {
        let manifest = parse_hub_manifest(VALID, HubProtocolLimits::default()).unwrap();
        assert_eq!(
            serde_json::to_vec(&manifest).unwrap(),
            serde_json::to_vec(&manifest).unwrap()
        );
    }

    #[test]
    fn identity_parser_rejects_unicode_and_noncanonical_case() {
        assert!(parse_hub_repository_identity("openalex/openalex").is_ok());
        assert!(parse_hub_repository_identity("OpenAlex/openalex").is_err());
        assert!(parse_hub_repository_identity("øpenalex/openalex").is_err());
    }

    #[test]
    fn complete_conformance_corpus_has_expected_dispositions() {
        assert!(parse_hub_manifest(MINIMAL, HubProtocolLimits::default()).is_ok());
        assert!(parse_hub_manifest(INTEGRITY_CASE, HubProtocolLimits::default()).is_ok());
        assert_eq!(
            parse_hub_manifest(UNKNOWN, HubProtocolLimits::default())
                .unwrap_err()
                .code,
            HubProtocolErrorCode::MalformedResponse
        );
        let refs = parse_hub_refs(REFS, HubProtocolLimits::default()).unwrap();
        assert_eq!(refs.refs.get("main").map(String::as_str), Some("v1"));
    }

    #[test]
    fn package_bundle_object_is_explicit_and_must_exist() {
        let mut manifest = parse_hub_manifest(VALID, HubProtocolLimits::default()).unwrap();
        assert_eq!(
            manifest.version.package.object_digest,
            manifest.objects[0].digest
        );
        manifest.version.package.object_digest =
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into();
        let error = parse_hub_manifest(
            &serde_json::to_vec(&manifest).unwrap(),
            HubProtocolLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.code, HubProtocolErrorCode::MissingObject);
    }

    #[test]
    fn checked_in_schemas_track_rust_protocol_constants_and_required_fields() {
        for schema in [MANIFEST_SCHEMA, REFS_SCHEMA] {
            let schema: Value = serde_json::from_slice(schema).unwrap();
            assert_eq!(schema["properties"]["protocol"]["const"], HUB_PROTOCOL);
            assert_eq!(schema["additionalProperties"], false);
            assert!(schema["required"].as_array().is_some_and(|fields| {
                fields.contains(&Value::String("repository".to_owned()))
            }));
        }
        let schema: Value = serde_json::from_slice(MANIFEST_SCHEMA).unwrap();
        assert_eq!(
            schema["properties"]["version"]["properties"]["package"]["properties"]["format"]["const"],
            PORTABLE_FORMAT
        );
    }
}
