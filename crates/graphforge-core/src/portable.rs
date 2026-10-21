//! Neutral portable package contracts and sanitized lifecycle errors.
#![allow(missing_docs)]

use serde::{Deserialize, Serialize};
use std::fmt;

/// Writer and reader admission limits for portable packages.
///
/// Limits that grow with the entry count admit every package a project can
/// produce (#900): the payload inventory streams, and the semantic manifest
/// is bounded per package by the entries it actually lists.
#[derive(Clone, Copy, Debug)]
pub struct PortableLimits {
    /// Components in the semantic manifest.
    pub max_components: u64,
    /// Regular entries in the package, tag files included.
    pub max_entries: u64,
    /// Bytes in one entry.
    pub max_entry_bytes: u64,
    /// Declared payload bytes across all entries.
    pub max_total_bytes: u64,
    /// Bytes in each retained JSON control whose size does not grow with the
    /// entry count: the runtime map, the ontology composition, and ontology
    /// and bridge documents in aggregate. It is also the semantic manifest's
    /// fixed allowance; see [`Self::semantic_manifest_bound`].
    pub max_manifest_bytes: u64,
    /// Ceiling on the semantic manifest, `data/graphforge-project.json`,
    /// whatever its entry count. It is parsed whole, so this bounds reader
    /// memory under untrusted input.
    pub max_semantic_manifest_bytes: u64,
    /// Bytes in each retained tag file: `bagit.txt`, `bag-info.txt` and
    /// `tagmanifest-sha256.txt`, which has three rows. The payload inventory,
    /// `manifest-sha256.txt`, has one row per `data/` entry; writer and reader
    /// stream it, and it is bounded by `max_entries` and `max_path_bytes`.
    pub max_tag_manifest_bytes: u64,
    /// Bytes the single-pass bundle reader may hold at once for the control
    /// documents Full verification reads after a payload has streamed past:
    /// ontology modules, bridge documents and saved-query definitions. It is a
    /// pool shared by those entries and is separate from the manifest, runtime
    /// map, composition control and tag files above. A document that does not
    /// fit is hashed but not retained; Full verification that then needs it
    /// fails with `LimitExceeded` at that entry. The same rule governs bundles
    /// read from a file and from a stream.
    pub max_retained_control_bytes: u64,
    /// Largest Parquet candidate entry a stream holds in memory while it walks
    /// the entry's pages before any decoder sees it. A larger entry streams
    /// into an unnamed scratch file instead and is walked there with the same
    /// routine, so the verdict never depends on this value or on the source:
    /// it only trades memory for scratch writes.
    pub max_parquet_walk_bytes: u64,
    /// Bytes of one research Version's retained objects that native validation
    /// may stage on disk: the sum over its entries under
    /// `data/components/research/research-retained/<version>/`. Objects only
    /// evidence needs (`research-retained-evidence/`) are authenticated as they
    /// stream and never staged, so they do not count. The default is no limit:
    /// verification accepts every package it accepted before. A Version is
    /// validated and its scratch removed before the next Version is read, so
    /// temporary disk is bounded by the largest Version, not by the package. An
    /// operator who sets a limit bounds that scratch: validating a Version holds
    /// the stage plus the further copies that
    /// [`PortableImportFootprint::VALIDATION_RESEARCH_COPIES`] counts, so peak
    /// temporary disk is that many times the limit plus one. A Version over it is
    /// refused with `LimitExceeded`, from a file and from a stream alike.
    pub max_research_version_bytes: u64,
    /// UTF-8 bytes in one portable path.
    pub max_path_bytes: usize,
    /// Bytes in the streaming copy buffer.
    pub copy_buffer_bytes: usize,
}

/// Semantic manifest budget per entry. A file record is
/// `{"length":L,"media_type":"M","path":"P","sha256":"H"},`: 50 bytes of
/// syntax, at most 20 length digits, a media type of at most 52 bytes and 64
/// digest bytes, 186 bytes plus the path. The S26 Graph500 project's records
/// average about 320 bytes (21,531 files, 6.9 MB), so 512 bytes admits paths
/// that average up to 326 bytes, nearly twice S26's 169.
pub const PORTABLE_MANIFEST_BYTES_PER_ENTRY: u64 = 512;
/// Most graph files one project holds: the storage graph-manifest default
/// (`GraphManifestLimits::max_entries`), the largest graph a package can carry.
const PRODUCER_MAX_GRAPH_FILES: u64 = 100_000;

impl PortableLimits {
    /// Largest semantic manifest admitted for a package of `entries` regular
    /// entries, checked before it is parsed: the fixed `max_manifest_bytes`
    /// allowance for components and selection, plus one file-record budget
    /// per entry, never above `max_semantic_manifest_bytes`. A small package
    /// therefore cannot present a large manifest.
    #[must_use]
    pub fn semantic_manifest_bound(&self, entries: u64) -> u64 {
        self.max_manifest_bytes
            .saturating_add(entries.saturating_mul(PORTABLE_MANIFEST_BYTES_PER_ENTRY))
            .min(self.max_semantic_manifest_bytes)
    }
}

impl Default for PortableLimits {
    fn default() -> Self {
        Self {
            max_components: 10_000,
            max_entries: 1_000_000,
            max_entry_bytes: 16 * 1024_u64.pow(4),
            max_total_bytes: 1024 * 1024_u64.pow(4),
            max_manifest_bytes: 16 * 1024 * 1024,
            // 100,000 graph files x 512 bytes = 51,200,000, rounded up to
            // 64 MiB: a manifest for the largest graph a project holds fits.
            max_semantic_manifest_bytes: (PRODUCER_MAX_GRAPH_FILES
                * PORTABLE_MANIFEST_BYTES_PER_ENTRY)
                .next_power_of_two(),
            max_tag_manifest_bytes: 4 * 1024 * 1024,
            max_retained_control_bytes: 256 * 1024 * 1024,
            max_parquet_walk_bytes: 64 * 1024 * 1024,
            max_research_version_bytes: u64::MAX,
            max_path_bytes: 4096,
            copy_buffer_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortableMode {
    StructureOnly,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableRepresentation {
    Expanded,
    Bundle,
}

/// How the verified bytes were read. The verdict does not depend on it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableSource {
    /// A path on the local filesystem: a bundle file or an expanded directory.
    File,
    /// A sequential reader with no path, size or identity.
    Stream,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PortablePackageClass {
    Complete,
    OntologyOnly,
    ComponentSelective,
    GraphDataSubset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableIntegrity {
    NotChecked,
    Verified,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableCompatibility {
    Supported,
    UnsupportedFuture,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableAuthenticity {
    NotEvaluated,
    Unsigned,
    Verified,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableReport {
    /// Whether full verification includes the registered research interchange component.
    pub research_interchange: bool,
    /// Manifest-authenticated research files for bounded native semantic verification.
    pub research_entries: Vec<PortableResearchEntry>,
    pub contract: &'static str,
    pub representation: PortableRepresentation,
    pub source: PortableSource,
    pub package_digest: String,
    pub package_class: PortablePackageClass,
    /// Exact GraphForge Core version that produced the package, as declared by
    /// its authenticated manifest.
    pub producing_core_version: String,
    pub component_count: u64,
    pub entry_count: u64,
    pub payload_bytes: u64,
    pub integrity: PortableIntegrity,
    pub compatibility: PortableCompatibility,
    pub authenticity: PortableAuthenticity,
    pub transport_digest: Option<String>,
    /// Exact authenticated multi-ontology compatibility control, when present.
    pub ontology_composition: Option<PortableOntologyComposition>,
    /// Authenticated bounded payload entries available to explicit lifecycle consumers.
    pub ontology_composition_entries: Vec<PortableCompositionEntry>,
}

/// The disk an import holds, at its peak, on the volume beside its target.
///
/// Counted from the import's copy protocol, not from a ratio of the package:
///
/// 1. The verified payload is extracted into a private stage beside the
///    target: one copy of every file. A package that carries an ontology
///    composition also gets two generated controls (the staged candidate and
///    the composition authority), each a re-encoding of the authored ontology
///    documents, written into the stage.
/// 2. A research package is validated before anything is published
///    (`native_validation::validate`, `project_portable_import/native_validation.rs`).
///    Every byte-holding copy, in order of lifetime:
///    - the stage (item 1), held to the end;
///    - the validation object store: the research files installed into an
///      ephemeral project (`native_validation.rs:74-79`), held until every
///      Version is validated;
///    - then, for each archived Version in turn (`native_validation.rs:82-89`,
///      `research_versions/project_restore.rs:113-232`), all alive together:
///      * the Version's private project (`native_validation.rs:82`), into which
///        the Version's graph files are installed (`project_restore.rs:171`)
///        and its evidence (`project_restore.rs:188`);
///      * for an expanded graph/files record (versions 5 and 7), a temporary
///        expanded copy of the graph tree (`project_restore.rs:148`, written by
///        `retained_content.rs:326`, held to the end of the rebuild), which
///        `stage_project_generation_with_graph_tree_mode`
///        (`project_restore.rs:221`, `project_publication/participants.rs:114`,
///        `graph_files.rs:419`) copies into the Version's project, and
///        `publish_with_graph_objects` (`project_restore.rs:230`) then installs.
///
///    The footprint cannot tell expanded-graph bytes from the rest of the
///    research files, so each of those later copies is bounded by the research
///    bytes: [`Self::VALIDATION_RESEARCH_COPIES`] of them are live at the
///    deepest point. All are removed before publication.
/// 3. The payload and the generated controls are published into the target:
///    a second copy of every file, while the stage still exists.
///
/// The stage is held throughout, so the peak is the stage plus the larger of
/// phases 2 and 3. Each file also occupies whole filesystem blocks in every
/// copy that is live at once, and a project's own bookkeeping (manifests,
/// journals, locks) adds a small fixed amount. The package being imported is
/// not counted: it is the caller's, and a clone adds its download.
///
/// An import of a package that carries no adjacency index also derives one,
/// which is not modelled here: its edge sort spills far more than the index it
/// writes (#1770).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortableImportFootprint {
    /// Bytes of every file the package extracts.
    pub payload_bytes: u64,
    /// Bytes of the files in the research component.
    pub research_bytes: u64,
    /// Bytes of the controls the import generates from the package's ontology
    /// composition, which its stage and the published project both keep.
    pub generated_bytes: u64,
    /// Number of files the package extracts.
    pub entries: u64,
}

impl PortableImportFootprint {
    /// Bytes a project's own bookkeeping needs beyond its payload.
    pub const FIXED_BYTES: u64 = 1024 * 1024;
    /// Filesystem allocation unit a file is rounded up to.
    pub const BLOCK_BYTES: u64 = 4096;
    /// Copies of the research files live at the deepest point of validating
    /// one Version, besides the stage: the validation store, the temporary
    /// expanded graph, and the Version's project with its staged tree.
    pub const VALIDATION_RESEARCH_COPIES: u64 = 3;
    /// Whole-file copies live at once while a research package validates: the
    /// stage plus the research copies above.
    const VALIDATION_LIVE_COPIES: u64 = 1 + Self::VALIDATION_RESEARCH_COPIES;
    /// Smallest bundle member that extracts to an allocated file: a header
    /// block and one block of content.
    const MIN_ALLOCATING_ENTRY_BYTES: u64 = 1024;

    /// What a package of `length` bytes may extract, when only its length is
    /// known: every byte of a research package may be research content, every
    /// allocating file occupies at least a header and a content block of the
    /// bundle (bounded by the entry limit), and each generated composition
    /// control is bounded by the manifest limit and by the package itself.
    #[must_use]
    pub fn of_package_length(length: u64, research: bool) -> Self {
        let limits = PortableLimits::default();
        Self {
            payload_bytes: length,
            research_bytes: if research { length } else { 0 },
            generated_bytes: 2 * length.min(limits.max_manifest_bytes),
            entries: (length / Self::MIN_ALLOCATING_ENTRY_BYTES).min(limits.max_entries),
        }
    }

    /// The most bytes the import holds beside its target at once.
    #[must_use]
    pub fn peak_bytes(&self) -> u64 {
        let stage = self.stage_bytes();
        let published = stage;
        let validation = self.validation_bytes();
        // Stage and published copy are live together; a research package
        // also holds the validation copies.
        let live_copies = if self.research_bytes == 0 {
            2
        } else {
            Self::VALIDATION_LIVE_COPIES
        };
        self.rounding(live_copies)
            .saturating_add(Self::FIXED_BYTES)
            .saturating_add(stage)
            .saturating_add(published.max(validation))
    }

    /// The most bytes a replay holds beside its target: an import whose
    /// transaction already committed extracts and validates the package again
    /// but publishes nothing, so it never holds the published copy.
    #[must_use]
    pub fn replay_peak_bytes(&self) -> u64 {
        let live_copies = if self.research_bytes == 0 {
            1
        } else {
            Self::VALIDATION_LIVE_COPIES
        };
        self.rounding(live_copies)
            .saturating_add(Self::FIXED_BYTES)
            .saturating_add(self.stage_bytes())
            .saturating_add(self.validation_bytes())
    }

    /// The most bytes validating a research package holds beside the place it
    /// is validated in, before any import begins: the package is extracted
    /// (one copy) and the research copies listed in the type documentation
    /// are live beside it, as in an import's validation. Nothing is
    /// published and no composition control is generated. A package without
    /// research content is verified without extracting anything.
    #[must_use]
    pub fn verification_peak_bytes(&self) -> u64 {
        if self.research_bytes == 0 {
            return 0;
        }
        self.rounding(Self::VALIDATION_LIVE_COPIES)
            .saturating_add(Self::FIXED_BYTES)
            .saturating_add(self.payload_bytes)
            .saturating_add(self.validation_bytes())
    }

    /// The research copies live beside the stage at the deepest point of
    /// validation (see the type documentation).
    fn validation_bytes(&self) -> u64 {
        self.research_bytes
            .saturating_mul(Self::VALIDATION_RESEARCH_COPIES)
    }

    fn stage_bytes(&self) -> u64 {
        self.payload_bytes.saturating_add(self.generated_bytes)
    }

    fn rounding(&self, live_copies: u64) -> u64 {
        self.entries
            .saturating_mul(Self::BLOCK_BYTES)
            .saturating_mul(live_copies)
    }
}

/// An authenticated file in the versioned research component.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableResearchEntry {
    pub component_id: String,
    pub path: String,
    pub length: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableCompositionEntry {
    pub kind: String,
    pub identity: PortableExactIdentity,
    pub path: String,
    pub media_type: String,
    pub length: u64,
    pub sha256: String,
    pub required_dependencies: Vec<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableExactIdentity {
    pub id: String,
    pub version: String,
    pub content_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableActivationOverride {
    pub scope: String,
    pub subject: PortableExactIdentity,
    pub mode: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableActivationProfile {
    pub profile_default: String,
    pub overrides: Vec<PortableActivationOverride>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableOntologyModule {
    pub ontology_id: String,
    pub version: String,
    pub content_digest: String,
    pub dialect: String,
    pub profile: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableBridgeSet {
    pub bridge_id: String,
    pub version: String,
    pub content_digest: String,
    pub source_modules: Vec<PortableExactIdentity>,
    pub target_modules: Vec<PortableExactIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableOntologyComposition {
    pub contract: String,
    pub activation_profile: PortableActivationProfile,
    pub modules: Vec<PortableOntologyModule>,
    pub bridge_sets: Vec<PortableBridgeSet>,
    pub required_features: Vec<String>,
    pub optional_features: Vec<String>,
    pub composition_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableErrorCode {
    Cancelled,
    LimitExceeded,
    Io,
    InvalidStructure,
    InvalidPath,
    DuplicateEntry,
    UnsupportedFuture,
    UnsupportedProjectFormat,
    Incompatible,
    DigestMismatch,
    ConcurrentMutation,
    /// The target's filesystem cannot provide the durable publication
    /// contract (`GF_UNSUPPORTED_FILESYSTEM`).
    UnsupportedFilesystem,
    /// The volume beside the target cannot hold the import's peak.
    InsufficientSpace,
}

#[derive(Debug)]
pub struct PortableError {
    /// Authenticated committed import, even when acknowledgement or cleanup failed.
    pub committed_import: Option<Box<PortableCommittedImport>>,
    pub code: PortableErrorCode,
    pub entry: Option<String>,
    detail: &'static str,
    /// The sanitized underlying cause of an I/O failure: the platform error
    /// text with every absolute host path reduced to its last two components.
    /// Diagnostic only; `detail` stays the stable public text.
    #[doc(hidden)]
    pub cause: Option<String>,
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

impl PortableError {
    /// Construct a sanitized package-level failure without a host path or payload value.
    #[must_use]
    pub fn new(code: PortableErrorCode, detail: &'static str) -> Self {
        Self {
            committed_import: None,
            code,
            entry: None,
            detail,
            cause: None,
            allocation_identity_allocated_bytes: std::collections::BTreeMap::new(),
            recovery_reauthentication_read_bytes: 0,
            recovery_reauthentication_read_calls: 0,
        }
    }
    #[must_use]
    pub fn at(code: PortableErrorCode, entry: &str, detail: &'static str) -> Self {
        Self {
            committed_import: None,
            code,
            entry: Some(entry.chars().take(4096).collect()),
            detail,
            cause: None,
            allocation_identity_allocated_bytes: std::collections::BTreeMap::new(),
            recovery_reauthentication_read_bytes: 0,
            recovery_reauthentication_read_calls: 0,
        }
    }

    /// Record the sanitized underlying cause of an I/O failure.
    #[must_use]
    pub fn with_cause(mut self, cause: impl Into<String>) -> Self {
        self.cause = Some(cause.into());
        self
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

    /// Attach exact publication evidence without claiming successful staging cleanup.
    #[must_use]
    pub fn with_committed_import(mut self, receipt: PortableCommittedImport) -> Self {
        self.committed_import = Some(Box::new(receipt));
        self
    }
}

/// Payload-free receipt evidence for an import whose CURRENT replacement committed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PortableCommittedImport {
    pub operation_uuid: uuid::Uuid,
    pub generation_uuid: uuid::Uuid,
    pub generation_manifest_sha256: [u8; 32],
    pub package_digest: String,
}
impl fmt::Display for PortableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "portable {:?}: {}", self.code, self.detail)?;
        if let Some(receipt) = &self.committed_import {
            write!(
                f,
                " committed=true operation_uuid={} generation_uuid={}",
                receipt.operation_uuid, receipt.generation_uuid
            )?;
        }
        Ok(())
    }
}
impl std::error::Error for PortableError {}
impl From<crate::GfError> for PortableError {
    fn from(_: crate::GfError) -> Self {
        Self::new(
            PortableErrorCode::Incompatible,
            "pinned project generation is not exportable",
        )
    }
}

use std::path::PathBuf;
/// Digest-pinned OCI reference returned after a successful publish.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableOciReference {
    /// Registry host (no credentials).
    pub registry: String,
    /// Repository path inside the registry.
    pub repository: String,
    /// OCI manifest digest (`sha256:…`).
    pub oci_manifest_digest: String,
    /// Authoritative GraphForge package digest (`sha256:…`).
    pub package_digest: String,
    /// Package class carried in the config blob.
    pub package_class: PortablePackageClass,
    /// Optional mutable tag that was also written; never used as identity.
    pub tag: Option<String>,
    /// Bytes uploaded across config + layer + manifest.
    pub bytes_transferred: u64,
    /// Blob count excluding the manifest itself.
    pub blob_count: u64,
}

/// Sanitized receipt after a successful pull + local verification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableOciPullReceipt {
    /// Digest-pinned reference that was resolved.
    pub reference: PortableOciReference,
    /// Destination path written after verification.
    pub destination: PathBuf,
    /// Local verifier report for the pulled package.
    pub report: PortableReport,
    /// Signature evaluation outcome (distinct from integrity).
    pub signature_state: PortableOciSignatureState,
}

/// Optional authenticity policy for referrer/signature attachments.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct PortableOciAuthenticityPolicy {
    /// When set, unsigned content is integrity-valid but authenticity-absent.
    pub require_named_signer: Option<String>,
    /// Caller-owned verification key material. Never logged or persisted.
    pub verification_key: Option<Vec<u8>>,
}

/// Explicit signature states. Never reused as integrity outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableOciSignatureState {
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
pub enum PortableOciPhase {
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
pub struct PortableOciProgress {
    /// Current phase.
    pub phase: PortableOciPhase,
    /// Cumulative bytes moved in this operation.
    pub bytes_transferred: u64,
    /// Optional blob/manifest digest under consideration.
    pub digest: Option<String>,
}

/// Caller-owned material used to attach an OCI signature referrer on publish.
#[derive(Clone, Eq, PartialEq)]
pub struct PortableOciSignatureMaterial {
    /// Signer identity recorded in the attachment.
    pub signer: String,
    /// Key identifier (not secret material).
    pub key_id: String,
    /// Secret key bytes used to compute the MAC. Never logged.
    pub secret: Vec<u8>,
}

impl std::fmt::Debug for PortableOciAuthenticityPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableOciAuthenticityPolicy")
            .field("require_named_signer", &self.require_named_signer)
            .field("verification_key", &"[REDACTED]")
            .finish()
    }
}

impl std::fmt::Debug for PortableOciSignatureMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableOciSignatureMaterial")
            .field("signer", &self.signer)
            .field("key_id", &self.key_id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

/// Stable semantic participant identity. Runtime catalog IDs and host paths are never selectors.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PortableParticipantId {
    /// Owning portable capability contract.
    pub capability_id: String,
    /// Stable record-family contract.
    pub record_family_id: String,
}

/// Built-in deterministic selection profiles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PortableSelectionProfile {
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
    Custom(Vec<PortableParticipantId>),
    /// Exact projected ontology module or bridge identities. The immutable
    /// preview exposes the complete emitted composition closure.
    OntologyComposition(Vec<PortableExactIdentity>),
}

/// Selection planning request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableSelectionRequest {
    /// Requested built-in or custom profile.
    pub profile: PortableSelectionProfile,
    /// Refuse any automatically required dependency instead of widening visibly.
    pub strict: bool,
}

/// Stable reason for inclusion/exclusion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PortableSelectionReason {
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
pub struct PortableSelectionEntry {
    /// Stable semantic identity.
    pub identity: PortableParticipantId,
    /// Canonical component kind.
    pub kind: String,
    /// Stable selection reason.
    pub reason: PortableSelectionReason,
    /// Exact committed payload bytes.
    pub estimated_bytes: u64,
    /// Manifest row count.
    pub row_count: u64,
}

/// Exact ontology module or bridge emitted by composition projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableProjectedSelectionEntry {
    /// Projected component kind (`ontology` or `schema`).
    pub kind: String,
    /// Exact semantic identity addressable by callers.
    pub identity: PortableExactIdentity,
    /// Stable component identity used by the portable manifest.
    pub participant_id: String,
    /// Whether this exact identity was directly requested or closure-added.
    pub reason: PortableSelectionReason,
    /// Exact canonical projected payload bytes.
    pub estimated_bytes: u64,
}

/// Immutable deterministic preview consumed by both export representations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableSelectionPlan {
    /// Pinned source generation identity.
    pub source_generation_uuid: String,
    /// Pinned source generation manifest identity.
    pub source_manifest_sha256: String,
    /// Portable package class token.
    pub package_class: String,
    /// Included participants in canonical identity order.
    pub included: Vec<PortableSelectionEntry>,
    /// Excluded participants in canonical identity order.
    pub excluded: Vec<PortableSelectionEntry>,
    /// Exact projected ontology closure in canonical identity order.
    pub projected: Vec<PortableProjectedSelectionEntry>,
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

impl PortableSelectionPlan {
    /// Construct a passive preview; this does not authorize storage execution.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_generation_uuid: String,
        source_manifest_sha256: String,
        package_class: String,
        included: Vec<PortableSelectionEntry>,
        excluded: Vec<PortableSelectionEntry>,
        projected: Vec<PortableProjectedSelectionEntry>,
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
pub struct PortableGraphSelector {
    /// Ordered node UUIDs (hyphenated).
    pub node_uuids: Vec<String>,
    /// Ordered edge UUIDs (hyphenated).
    pub edge_uuids: Vec<String>,
}

/// Portable on-wire closure tokens.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PortableSubsetClosure {
    /// Selected nodes plus edges whose endpoints are both selected.
    InducedEdges,
    /// Selected edges plus both endpoint nodes.
    Referential,
}

/// Property projection/redaction for subset packages.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct PortablePropertyProjection {
    /// Property field names excluded from payloads.
    pub exclude: Vec<String>,
}

/// Subset planning request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableSubsetRequest {
    /// Stable UUID selector.
    pub selector: PortableGraphSelector,
    /// Closure mode.
    pub closure: PortableSubsetClosure,
    /// Property projection.
    pub projection: PortablePropertyProjection,
}

/// Content-free graph-subset receipt retained in the semantic manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableGraphSubsetMeta {
    /// Canonical content-free selector digest token.
    pub selector: String,
    /// On-wire closure token.
    pub closure: String,
}

/// Immutable subset preview consumed by planning and export.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableSubsetPreview {
    /// Component selection consumed by the exporter.
    pub selection: PortableSelectionPlan,
    /// Graph-subset metadata emitted into the semantic manifest.
    pub graph_subset: PortableGraphSubsetMeta,
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
/// Portable transport representation.
pub enum PortableOutput {
    /// Closed BagIt-compatible directory.
    Expanded,
    /// Canonical uncompressed PAX/ustar stream.
    Bundle,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Aggregate content-free progress observation.
pub struct PortableExportProgress {
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
        let plan = PortableSelectionPlan::new(
            "generation".into(),
            "manifest".into(),
            "graph-data-subset".into(),
            vec![PortableSelectionEntry {
                identity: PortableParticipantId {
                    capability_id: "graph@1".into(),
                    record_family_id: "files@1".into(),
                },
                kind: "graph-data".into(),
                reason: PortableSelectionReason::Requested,
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
        let subset = PortableSubsetPreview {
            selection: plan,
            graph_subset: PortableGraphSubsetMeta {
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

#[cfg(test)]
mod limit_tests {
    use super::*;

    /// The longest file record the writer emits, with an empty path.
    fn longest_record_without_path() -> u64 {
        let record = serde_json::json!({
            "length": u64::MAX,
            "media_type": "application/vnd.graphforge.ontology-composition+json",
            "path": "",
            "sha256": "0".repeat(64),
        });
        (serde_json::to_vec(&record).unwrap().len() + ",".len()) as u64
    }

    #[test]
    fn default_limits_admit_every_package_a_project_can_produce() {
        let limits = PortableLimits::default();
        // The per-entry semantic manifest budget is one record plus its path.
        assert_eq!(longest_record_without_path(), 186);
        let path_budget = PORTABLE_MANIFEST_BYTES_PER_ENTRY - longest_record_without_path();
        assert_eq!(path_budget, 326);
        // Controls that do not grow with the entry count keep 16 MiB; the
        // semantic manifest ceiling holds the largest graph a project holds.
        assert_eq!(limits.max_manifest_bytes, 16 * 1024 * 1024);
        assert_eq!(limits.max_semantic_manifest_bytes, 64 * 1024 * 1024);
        assert!(
            PRODUCER_MAX_GRAPH_FILES * PORTABLE_MANIFEST_BYTES_PER_ENTRY
                <= limits.max_semantic_manifest_bytes
        );
        // S26 Graph500: 21,535 entries and a 6,874,538-byte manifest whose
        // paths average 169 bytes. S28 has about four times as many entries.
        assert!(6_874_538 <= limits.semantic_manifest_bound(21_535));
        let s28_entries = 4 * 21_535;
        let s28_manifest = s28_entries * (longest_record_without_path() + 169);
        assert!(s28_manifest <= limits.semantic_manifest_bound(s28_entries));
        assert!(169 < path_budget);
        // The bound grows with the package and stops at the ceiling.
        assert_eq!(limits.semantic_manifest_bound(0), limits.max_manifest_bytes);
        assert_eq!(
            limits.semantic_manifest_bound(1),
            limits.max_manifest_bytes + PORTABLE_MANIFEST_BYTES_PER_ENTRY
        );
        assert_eq!(
            limits.semantic_manifest_bound(limits.max_entries),
            limits.max_semantic_manifest_bytes
        );
        assert_eq!(
            limits.semantic_manifest_bound(u64::MAX),
            limits.max_semantic_manifest_bytes
        );
        // The tag files the tag limit bounds have a fixed size: the tag
        // manifest has three rows whatever the entry count.
        let tag_manifest = ["bag-info.txt", "bagit.txt", "manifest-sha256.txt"]
            .iter()
            .map(|path| 64 + 2 + path.len() as u64 + 1)
            .sum::<u64>();
        assert!(tag_manifest <= limits.max_tag_manifest_bytes);
    }
}
