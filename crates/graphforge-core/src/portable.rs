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

/// Stable semantic participant identity. Runtime catalog IDs and host paths are never selectors.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PortableV2ParticipantId {
    /// Owning portable capability contract.
    pub capability_id: String,
    /// Stable record-family contract.
    pub record_family_id: String,
}

/// Built-in deterministic selection profiles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PortableV2SelectionProfile {
    /// Every committed participant and graph-tree payload.
    Complete,
    /// Authored/adopted ontology plus required schema participants.
    OntologyOnly,
    /// Whole graph/data components. Row or subgraph selection belongs to #786.
    DataComponents,
    /// Derived and repository artifact participants.
    Artifacts,
    /// Closed-schema portable settings only.
    Settings,
    /// Explicit stable identities.
    Custom(Vec<PortableV2ParticipantId>),
    /// Exact projected ontology module or bridge identities. The immutable
    /// preview exposes the complete emitted composition closure.
    OntologyComposition(Vec<PortableV2ExactIdentity>),
}

/// Selection planning request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableV2SelectionRequest {
    /// Requested built-in or custom profile.
    pub profile: PortableV2SelectionProfile,
    /// Refuse any automatically required dependency instead of widening visibly.
    pub strict: bool,
}

/// Stable reason for inclusion/exclusion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PortableV2SelectionReason {
    /// Directly requested by profile or exact identity.
    Requested,
    /// Required ontology/schema closure.
    RequiredSchemaAuthority,
    /// Required exact multi-ontology composition closure.
    RequiredOntologyComposition,
    /// Not part of the requested profile.
    ProfileExcluded,
}

/// Content-free preview row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2SelectionEntry {
    /// Stable semantic identity.
    pub identity: PortableV2ParticipantId,
    /// Canonical component kind.
    pub kind: String,
    /// Stable selection reason.
    pub reason: PortableV2SelectionReason,
    /// Exact committed payload bytes.
    pub estimated_bytes: u64,
    /// Manifest row count.
    pub row_count: u64,
}

/// Exact ontology module or bridge emitted by composition projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2ProjectedSelectionEntry {
    /// Projected component kind (`ontology` or `schema`).
    pub kind: String,
    /// Exact semantic identity addressable by callers.
    pub identity: PortableV2ExactIdentity,
    /// Stable component identity used by the portable manifest.
    pub participant_id: String,
    /// Whether this exact identity was directly requested or closure-added.
    pub reason: PortableV2SelectionReason,
    /// Exact canonical projected payload bytes.
    pub estimated_bytes: u64,
}

/// Immutable deterministic preview consumed by both export representations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2SelectionPlan {
    /// Pinned source generation identity.
    pub source_generation_uuid: String,
    /// Pinned source generation manifest identity.
    pub source_manifest_sha256: String,
    /// Portable package class token.
    pub package_class: String,
    /// Included participants in canonical identity order.
    pub included: Vec<PortableV2SelectionEntry>,
    /// Excluded participants in canonical identity order.
    pub excluded: Vec<PortableV2SelectionEntry>,
    /// Exact projected ontology closure in canonical identity order.
    pub projected: Vec<PortableV2ProjectedSelectionEntry>,
    /// Explicit redaction reason codes; values are never retained.
    pub redactions: Vec<String>,
    /// Required portable capability contracts.
    pub required_capabilities: Vec<String>,
    /// Exact known participant bytes, excluding bounded control metadata.
    pub estimated_payload_bytes: u64,
    /// Stable digest over canonical content-free plan metadata.
    pub selection_fingerprint: String,
    include_graph_tree: bool,
}

impl PortableV2SelectionPlan {
    /// Construct a passive preview; this does not authorize storage execution.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_generation_uuid: String,
        source_manifest_sha256: String,
        package_class: String,
        included: Vec<PortableV2SelectionEntry>,
        excluded: Vec<PortableV2SelectionEntry>,
        projected: Vec<PortableV2ProjectedSelectionEntry>,
        redactions: Vec<String>,
        required_capabilities: Vec<String>,
        estimated_payload_bytes: u64,
        selection_fingerprint: String,
        include_graph_tree: bool,
    ) -> Self {
        Self {
            source_generation_uuid,
            source_manifest_sha256,
            package_class,
            included,
            excluded,
            projected,
            redactions,
            required_capabilities,
            estimated_payload_bytes,
            selection_fingerprint,
            include_graph_tree,
        }
    }
    /// Whether the semantic selection includes the graph tree.
    #[must_use]
    pub fn includes_graph_tree(&self) -> bool {
        self.include_graph_tree
    }
    /// Set the semantic graph-tree selection for a projected preview.
    pub fn set_include_graph_tree(&mut self, include: bool) {
        self.include_graph_tree = include;
    }
    /// Whether a semantic participant is included.
    #[must_use]
    pub fn includes(&self, capability: &str, family: &str) -> bool {
        self.included.iter().any(|entry| {
            entry.identity.capability_id == capability && entry.identity.record_family_id == family
        })
    }
}

/// Stable UUID selector for one pinned generation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct PortableV2GraphSelector {
    /// Ordered node UUIDs (hyphenated).
    pub node_uuids: Vec<String>,
    /// Ordered edge UUIDs (hyphenated).
    pub edge_uuids: Vec<String>,
}

/// Portable-v2 on-wire closure tokens.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PortableV2SubsetClosure {
    /// Selected nodes plus edges whose endpoints are both selected.
    InducedEdges,
    /// Selected edges plus both endpoint nodes.
    Referential,
}

/// Property projection/redaction for subset packages.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct PortableV2PropertyProjection {
    /// Property field names excluded from payloads.
    pub exclude: Vec<String>,
}

/// Subset planning request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableV2SubsetRequest {
    /// Stable UUID selector.
    pub selector: PortableV2GraphSelector,
    /// Closure mode.
    pub closure: PortableV2SubsetClosure,
    /// Property projection.
    pub projection: PortableV2PropertyProjection,
}

/// Content-free graph-subset receipt retained in the semantic manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2GraphSubsetMeta {
    /// Canonical content-free selector digest token.
    pub selector: String,
    /// On-wire closure token.
    pub closure: String,
}

/// Immutable subset preview consumed by planning and export.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2SubsetPreview {
    /// Component selection consumed by the exporter.
    pub selection: PortableV2SelectionPlan,
    /// Graph-subset metadata emitted into the semantic manifest.
    pub graph_subset: PortableV2GraphSubsetMeta,
    /// Resolved node count after closure.
    pub selected_node_count: u64,
    /// Resolved edge count after closure.
    pub selected_edge_count: u64,
    /// Endpoint nodes added beyond the caller's explicit node set.
    pub endpoint_node_count: u64,
    /// Domain-separated projected graph fingerprint.
    pub result_fingerprint: String,
    /// Stable digest over the full subset preview.
    pub subset_fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Portable-v2 transport representation.
pub enum PortableV2Output {
    /// Closed BagIt-compatible directory.
    Expanded,
    /// Canonical uncompressed PAX/ustar stream.
    Bundle,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Aggregate content-free progress observation.
pub struct PortableV2ExportProgress {
    /// Fully emitted entries.
    pub entries_completed: usize,
    /// Emitted source payload bytes.
    pub bytes_completed: u64,
    /// Planned entry count.
    pub entries_total: usize,
    /// Planned source payload bytes.
    pub bytes_total: u64,
}

#[cfg(test)]
mod preview_wire_tests {
    use super::*;

    #[test]
    fn moved_preview_contract_keeps_private_serialized_graph_decision() {
        let plan = PortableV2SelectionPlan::new(
            "generation".into(),
            "manifest".into(),
            "graph-data-subset".into(),
            vec![PortableV2SelectionEntry {
                identity: PortableV2ParticipantId {
                    capability_id: "graph@1".into(),
                    record_family_id: "files@1".into(),
                },
                kind: "graph-data".into(),
                reason: PortableV2SelectionReason::Requested,
                estimated_bytes: 7,
                row_count: 2,
            }],
            Vec::new(),
            Vec::new(),
            vec!["redacted".into()],
            vec!["graph@1".into()],
            7,
            "selection".into(),
            false,
        );
        let expected = serde_json::json!({
            "source_generation_uuid":"generation", "source_manifest_sha256":"manifest",
            "package_class":"graph-data-subset", "included":[{"identity":{"capability_id":"graph@1","record_family_id":"files@1"},
                "kind":"graph-data","reason":"requested","estimated_bytes":7,"row_count":2}],
            "excluded":[],"projected":[],"redactions":["redacted"],"required_capabilities":["graph@1"],
            "estimated_payload_bytes":7,"selection_fingerprint":"selection","include_graph_tree":false
        });
        assert_eq!(serde_json::to_value(&plan).unwrap(), expected);
        let subset = PortableV2SubsetPreview {
            selection: plan,
            graph_subset: PortableV2GraphSubsetMeta {
                selector: "selector".into(),
                closure: "referential".into(),
            },
            selected_node_count: 2,
            selected_edge_count: 1,
            endpoint_node_count: 1,
            result_fingerprint: "result".into(),
            subset_fingerprint: "subset".into(),
        };
        assert_eq!(
            serde_json::to_value(subset).unwrap(),
            serde_json::json!({
                "selection":expected,"graph_subset":{"selector":"selector","closure":"referential"},
                "selected_node_count":2,"selected_edge_count":1,"endpoint_node_count":1,
                "result_fingerprint":"result","subset_fingerprint":"subset"
            })
        );
    }
}
