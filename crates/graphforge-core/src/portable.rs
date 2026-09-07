//! Neutral portable package contracts and sanitized lifecycle errors.
#![allow(missing_docs)]

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug)]
pub struct PortableV2Limits {
    pub max_components: u64,
    pub max_entries: u64,
    pub max_entry_bytes: u64,
    pub max_total_bytes: u64,
    pub max_manifest_bytes: u64,
    pub max_tag_manifest_bytes: u64,
    pub max_path_bytes: usize,
    pub copy_buffer_bytes: usize,
}

impl Default for PortableV2Limits {
    fn default() -> Self {
        Self {
            max_components: 10_000,
            max_entries: 1_000_000,
            max_entry_bytes: 16 * 1024_u64.pow(4),
            max_total_bytes: 1024 * 1024_u64.pow(4),
            max_manifest_bytes: 16 * 1024 * 1024,
            max_tag_manifest_bytes: 4 * 1024 * 1024,
            max_path_bytes: 4096,
            copy_buffer_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortableV2Mode {
    StructureOnly,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2Representation {
    Expanded,
    Bundle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PortableV2PackageClass {
    Complete,
    OntologyOnly,
    ComponentSelective,
    GraphDataSubset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2Integrity {
    NotChecked,
    Verified,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2Compatibility {
    Supported,
    UnsupportedFuture,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2Authenticity {
    NotEvaluated,
    Unsigned,
    Verified,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2Report {
    pub contract: &'static str,
    pub representation: PortableV2Representation,
    pub package_digest: String,
    pub package_class: PortableV2PackageClass,
    pub component_count: u64,
    pub entry_count: u64,
    pub payload_bytes: u64,
    pub integrity: PortableV2Integrity,
    pub compatibility: PortableV2Compatibility,
    pub authenticity: PortableV2Authenticity,
    pub transport_digest: Option<String>,
    /// Exact authenticated multi-ontology compatibility control, when present.
    pub ontology_composition: Option<PortableV2OntologyComposition>,
    /// Authenticated bounded payload entries available to explicit lifecycle consumers.
    pub ontology_composition_entries: Vec<PortableV2CompositionEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2CompositionEntry {
    pub kind: String,
    pub identity: PortableV2ExactIdentity,
    pub path: String,
    pub media_type: String,
    pub length: u64,
    pub sha256: String,
    pub required_dependencies: Vec<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableV2ExactIdentity {
    pub id: String,
    pub version: String,
    pub content_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableV2ActivationOverride {
    pub scope: String,
    pub subject: PortableV2ExactIdentity,
    pub mode: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableV2ActivationProfile {
    pub profile_default: String,
    pub overrides: Vec<PortableV2ActivationOverride>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableV2OntologyModule {
    pub ontology_id: String,
    pub version: String,
    pub content_digest: String,
    pub dialect: String,
    pub profile: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableV2BridgeSet {
    pub bridge_id: String,
    pub version: String,
    pub content_digest: String,
    pub source_modules: Vec<PortableV2ExactIdentity>,
    pub target_modules: Vec<PortableV2ExactIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableV2OntologyComposition {
    pub contract: String,
    pub activation_profile: PortableV2ActivationProfile,
    pub modules: Vec<PortableV2OntologyModule>,
    pub bridge_sets: Vec<PortableV2BridgeSet>,
    pub required_features: Vec<String>,
    pub optional_features: Vec<String>,
    pub composition_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2ErrorCode {
    Cancelled,
    LimitExceeded,
    Io,
    InvalidStructure,
    InvalidPath,
    DuplicateEntry,
    UnsupportedFuture,
    Incompatible,
    DigestMismatch,
    ConcurrentMutation,
}

#[derive(Debug)]
pub struct PortableV2Error {
    pub code: PortableV2ErrorCode,
    pub entry: Option<String>,
    detail: &'static str,
    /// Content-free native allocation evidence retained only for local
    /// lifecycle qualification of an interrupted operation.
    #[doc(hidden)]
    pub allocation_identity_allocated_bytes: std::collections::BTreeMap<String, u64>,
    /// Bytes actually read while reauthenticating an interrupted import.
    #[doc(hidden)]
    pub recovery_reauthentication_read_bytes: u64,
    /// Calls actually completed while reauthenticating an interrupted import.
    #[doc(hidden)]
    pub recovery_reauthentication_read_calls: u64,
}

impl PortableV2Error {
    /// Construct a sanitized package-level failure without a host path or payload value.
    #[must_use]
    pub fn new(code: PortableV2ErrorCode, detail: &'static str) -> Self {
        Self {
            code,
            entry: None,
            detail,
            allocation_identity_allocated_bytes: std::collections::BTreeMap::new(),
            recovery_reauthentication_read_bytes: 0,
            recovery_reauthentication_read_calls: 0,
        }
    }
    #[must_use]
    pub fn at(code: PortableV2ErrorCode, entry: &str, detail: &'static str) -> Self {
        Self {
            code,
            entry: Some(entry.chars().take(4096).collect()),
            detail,
            allocation_identity_allocated_bytes: std::collections::BTreeMap::new(),
            recovery_reauthentication_read_bytes: 0,
            recovery_reauthentication_read_calls: 0,
        }
    }

    #[must_use]
    pub fn with_allocation_identities(
        mut self,
        identities: std::collections::BTreeMap<String, u64>,
    ) -> Self {
        self.allocation_identity_allocated_bytes = identities;
        self
    }

    #[must_use]
    pub fn with_recovery_reauthentication(mut self, read_bytes: u64, read_calls: u64) -> Self {
        self.recovery_reauthentication_read_bytes = read_bytes;
        self.recovery_reauthentication_read_calls = read_calls;
        self
    }
}
impl fmt::Display for PortableV2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "portable-v2 {:?}: {}", self.code, self.detail)
    }
}
impl std::error::Error for PortableV2Error {}
impl From<crate::GfError> for PortableV2Error {
    fn from(_: crate::GfError) -> Self {
        Self::new(
            PortableV2ErrorCode::Incompatible,
            "pinned project generation is not exportable",
        )
    }
}

use std::path::PathBuf;
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
#[derive(Clone, Default, Eq, PartialEq)]
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
#[derive(Clone, Eq, PartialEq)]
pub struct PortableV2OciSignatureMaterial {
    /// Signer identity recorded in the attachment.
    pub signer: String,
    /// Key identifier (not secret material).
    pub key_id: String,
    /// Secret key bytes used to compute the MAC. Never logged.
    pub secret: Vec<u8>,
}

impl std::fmt::Debug for PortableV2OciAuthenticityPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableV2OciAuthenticityPolicy")
            .field("require_named_signer", &self.require_named_signer)
            .field("verification_key", &"[REDACTED]")
            .finish()
    }
}

impl std::fmt::Debug for PortableV2OciSignatureMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableV2OciSignatureMaterial")
            .field("signer", &self.signer)
            .field("key_id", &self.key_id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}
