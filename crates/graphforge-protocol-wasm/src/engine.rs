//! Native-callable engine behind every WebAssembly export.
//!
//! Each function takes JSON bytes (a document or an argument object) and
//! returns a serializable value or a [`ProtocolError`]. It only parses its
//! arguments and calls the Rust-owned validators in `graphforge-discovery` and
//! `graphforge-hub-publish`; it adds no protocol rule of its own. The
//! `wasm-bindgen` layer in `lib.rs` converts JavaScript values to and from
//! these types, and the native contract test calls these functions directly.

use graphforge_discovery::preview::{self, PreviewDescriptor, PreviewError};
use graphforge_discovery::{
    DiscoveryError, DiscoveryLimits, DiscoveryManifest, DiscoveryVersionDetails, ExactIdentity,
    LineageVersion, ObjectDescriptor, ObjectLocationTemplate, OntologyModuleDescriptor,
    ProjectSummary, RefSet, RehomedRepository, RepositoryIdentity, RepositoryMoved,
    RepositorySnapshot, ResearchLineage, Sha256Digest,
};
use graphforge_hub_publish::{
    CommitRequest, HubPublishError, ObjectDeclaration, OpenSessionRequest, OperationStatus,
    PublishCapabilities, PublishIntent, PublishReceipt,
};
use serde::{Deserialize, Serialize};
use tsify::Tsify;

/// Structured failure with the same code the Rust validators return.
///
/// `code` is the snake-case discovery or publish code (`integrity_failure`,
/// `invalid_input`, ...), the `preview.*` token of a preview refusal, or
/// `invalid_argument` for an argument the adapter itself cannot read.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Tsify)]
pub struct ProtocolError {
    /// Stable machine-readable code.
    pub code: String,
    /// Schema field the failure concerns, when the validator names one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Supported and requested versions of a compatibility failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<DiscoveryVersionDetails>,
    /// Sanitized, non-input-bearing diagnostic text.
    pub message: String,
}

impl ProtocolError {
    pub(crate) fn argument(message: &str) -> Self {
        Self {
            code: "invalid_argument".into(),
            path: None,
            version: None,
            message: message.into(),
        }
    }
}

impl From<DiscoveryError> for ProtocolError {
    fn from(error: DiscoveryError) -> Self {
        Self {
            code: serde_json::to_value(error.code)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_default(),
            path: error.field.map(str::to_owned),
            version: error.version,
            message: error.detail().into(),
        }
    }
}

impl From<HubPublishError> for ProtocolError {
    fn from(error: HubPublishError) -> Self {
        Self {
            code: error.code.as_str().into(),
            path: None,
            version: None,
            message: error.detail().into(),
        }
    }
}

impl From<PreviewError> for ProtocolError {
    fn from(error: PreviewError) -> Self {
        Self {
            code: error.to_string(),
            path: None,
            version: None,
            message: "preview refused by the closed image profile".into(),
        }
    }
}

/// Result of every engine function.
pub type Outcome<T> = Result<T, ProtocolError>;

/// Optional overrides of the default discovery resource bounds.
#[derive(Clone, Copy, Debug, Default, Deserialize, Tsify)]
#[serde(default, deny_unknown_fields)]
pub struct DiscoveryLimitsOverride {
    /// Maximum encoded manifest or refs response size.
    pub max_response_bytes: Option<usize>,
    /// Maximum bytes in any bounded string.
    pub max_string_bytes: Option<usize>,
    /// Maximum refs in one refs response.
    pub max_refs: Option<usize>,
    /// Maximum objects in one manifest.
    pub max_objects: Option<usize>,
    /// Maximum locations for one immutable object.
    pub max_locations_per_object: Option<usize>,
    /// Maximum declared bytes across the object inventory.
    pub max_cumulative_object_bytes: Option<u64>,
    /// Maximum bytes of one Project summary document.
    pub max_summary_bytes: Option<usize>,
    /// Maximum declared bytes of one per-module package object.
    pub max_module_package_bytes: Option<u64>,
    /// Maximum modules, and separately bridge sets, in one ontology inventory.
    pub max_ontology_entries: Option<usize>,
    /// Maximum bytes of one research lineage document.
    pub max_lineage_bytes: Option<usize>,
    /// Maximum Versions, Branches, or Proposals in one lineage document.
    pub max_lineage_entries: Option<usize>,
}

impl DiscoveryLimitsOverride {
    fn limits(self) -> DiscoveryLimits {
        let base = DiscoveryLimits::default();
        DiscoveryLimits {
            max_response_bytes: self.max_response_bytes.unwrap_or(base.max_response_bytes),
            max_string_bytes: self.max_string_bytes.unwrap_or(base.max_string_bytes),
            max_refs: self.max_refs.unwrap_or(base.max_refs),
            max_objects: self.max_objects.unwrap_or(base.max_objects),
            max_locations_per_object: self
                .max_locations_per_object
                .unwrap_or(base.max_locations_per_object),
            max_cumulative_object_bytes: self
                .max_cumulative_object_bytes
                .unwrap_or(base.max_cumulative_object_bytes),
            max_summary_bytes: self.max_summary_bytes.unwrap_or(base.max_summary_bytes),
            max_module_package_bytes: self
                .max_module_package_bytes
                .unwrap_or(base.max_module_package_bytes),
            max_ontology_entries: self
                .max_ontology_entries
                .unwrap_or(base.max_ontology_entries),
            max_lineage_bytes: self.max_lineage_bytes.unwrap_or(base.max_lineage_bytes),
            max_lineage_entries: self.max_lineage_entries.unwrap_or(base.max_lineage_entries),
        }
    }
}

/// Resolve optional JSON limits; `None` and JSON `null` mean the defaults.
pub fn limits(bytes: Option<&[u8]>) -> Outcome<DiscoveryLimits> {
    match bytes {
        None => Ok(DiscoveryLimits::default()),
        Some(bytes) => {
            let overrides: Option<DiscoveryLimitsOverride> = argument(bytes, "limits")?;
            Ok(overrides.unwrap_or_default().limits())
        }
    }
}

/// Deserialize a JSON argument object.
pub fn argument<T: serde::de::DeserializeOwned>(bytes: &[u8], name: &str) -> Outcome<T> {
    serde_json::from_slice(bytes)
        .map_err(|_| ProtocolError::argument(&format!("{name} argument is malformed")))
}

fn digest_string(digest: Sha256Digest) -> String {
    digest.0
}

fn canonical_text(bytes: Vec<u8>) -> Outcome<String> {
    String::from_utf8(bytes).map_err(|_| ProtocolError::argument("canonical bytes are not UTF-8"))
}

// ------------------------------------------------------------------ discovery

/// A validated manifest with its canonical form and validator.
#[derive(Serialize, Tsify)]
pub struct ValidatedManifest {
    /// The parsed manifest.
    pub document: DiscoveryManifest,
    /// Canonical JSON text; its SHA-256 is `digest`.
    pub canonical_json: String,
    /// Canonical manifest digest, the strong validator named by refs.
    pub digest: String,
}

/// Parse and validate a discovery manifest.
pub fn validate_manifest(bytes: &[u8], limits: DiscoveryLimits) -> Outcome<ValidatedManifest> {
    let document = DiscoveryManifest::from_json(bytes, limits)?;
    Ok(ValidatedManifest {
        canonical_json: canonical_text(document.to_canonical_json()?)?,
        digest: digest_string(document.canonical_digest()?),
        document,
    })
}

/// A validated refs snapshot with its canonical form and revision.
#[derive(Serialize, Tsify)]
pub struct ValidatedRefs {
    /// The parsed refs snapshot.
    pub document: RefSet,
    /// Canonical JSON text; its SHA-256 is `revision`.
    pub canonical_json: String,
    /// Repository revision: the canonical digest of the refs document.
    pub revision: String,
}

/// Parse and validate a refs snapshot.
pub fn validate_refs(bytes: &[u8], limits: DiscoveryLimits) -> Outcome<ValidatedRefs> {
    let document = RefSet::from_json(bytes, limits)?;
    let canonical = document.to_canonical_json()?;
    Ok(ValidatedRefs {
        revision: graphforge_hub_publish::sha256_digest(&canonical),
        canonical_json: canonical_text(canonical)?,
        document,
    })
}

/// A validated Project summary with its canonical form and digest.
#[derive(Serialize, Tsify)]
pub struct ValidatedSummary {
    /// The parsed summary.
    pub document: ProjectSummary,
    /// Canonical JSON text; its SHA-256 is `digest`.
    pub canonical_json: String,
    /// Canonical summary digest.
    pub digest: String,
}

/// Parse and validate a Project summary.
pub fn validate_summary(bytes: &[u8], limits: DiscoveryLimits) -> Outcome<ValidatedSummary> {
    let document = ProjectSummary::from_json(bytes, limits)?;
    Ok(ValidatedSummary {
        canonical_json: canonical_text(document.to_canonical_json()?)?,
        digest: digest_string(document.canonical_digest()?),
        document,
    })
}

/// A validated research lineage with its canonical form and digest.
#[derive(Serialize, Tsify)]
pub struct ValidatedLineage {
    /// The parsed lineage.
    pub document: ResearchLineage,
    /// Canonical JSON text; its SHA-256 is `digest`.
    pub canonical_json: String,
    /// Canonical lineage digest.
    pub digest: String,
}

/// Parse and validate a research lineage.
pub fn validate_lineage(bytes: &[u8], limits: DiscoveryLimits) -> Outcome<ValidatedLineage> {
    let document = ResearchLineage::from_json(bytes, limits)?;
    Ok(ValidatedLineage {
        canonical_json: canonical_text(document.to_canonical_json()?)?,
        digest: digest_string(document.canonical_digest()?),
        document,
    })
}

/// A validated repository move marker.
#[derive(Serialize, Tsify)]
pub struct ValidatedRepositoryMoved {
    /// The parsed move marker.
    pub document: RepositoryMoved,
}

/// Parse and validate a repository move marker.
pub fn validate_repository_moved(
    bytes: &[u8],
    limits: DiscoveryLimits,
) -> Outcome<ValidatedRepositoryMoved> {
    Ok(ValidatedRepositoryMoved {
        document: RepositoryMoved::from_json(bytes, limits)?,
    })
}

/// Check that a summary binds its manifest.
pub fn bind_summary(manifest: &[u8], summary: &[u8], limits: DiscoveryLimits) -> Outcome<()> {
    let manifest = DiscoveryManifest::from_json(manifest, limits)?;
    let summary = ProjectSummary::from_json(summary, limits)?;
    Ok(manifest.bind_summary(&summary)?)
}

/// Check that a lineage binds its manifest and refs snapshot.
pub fn bind_lineage(
    manifest: &[u8],
    refs: &[u8],
    lineage: &[u8],
    limits: DiscoveryLimits,
) -> Outcome<()> {
    let manifest = DiscoveryManifest::from_json(manifest, limits)?;
    let refs = RefSet::from_json(refs, limits)?;
    let lineage = ResearchLineage::from_json(lineage, limits)?;
    Ok(manifest.bind_lineage(&refs, &lineage)?)
}

/// Check that a summary's people agree with its lineage (absent: none).
pub fn bind_summary_lineage(
    summary: &[u8],
    lineage: Option<&[u8]>,
    limits: DiscoveryLimits,
) -> Outcome<()> {
    let summary = ProjectSummary::from_json(summary, limits)?;
    let lineage = lineage
        .map(|bytes| ResearchLineage::from_json(bytes, limits))
        .transpose()?;
    Ok(summary.bind_lineage(lineage.as_ref())?)
}

/// Check that a manifest is bound to a refs snapshot.
pub fn bind_refs(refs: &[u8], manifest: &[u8], limits: DiscoveryLimits) -> Outcome<()> {
    let refs = RefSet::from_json(refs, limits)?;
    let manifest = DiscoveryManifest::from_json(manifest, limits)?;
    Ok(refs.validate_manifest(&manifest)?)
}

/// One ontology module and the transport object that carries it.
#[derive(Serialize, Tsify)]
pub struct OntologyModuleObject {
    /// Module descriptor.
    pub descriptor: OntologyModuleDescriptor,
    /// Transport object of the module package.
    pub object: ObjectDescriptor,
}

/// Select the descriptor and transport object of one exact ontology module.
pub fn ontology_module_object(
    manifest: &[u8],
    identity: &[u8],
    limits: DiscoveryLimits,
) -> Outcome<OntologyModuleObject> {
    let manifest = DiscoveryManifest::from_json(manifest, limits)?;
    let identity: ExactIdentity = argument(identity, "identity")?;
    let (descriptor, object) = manifest.ontology_module_object(&identity)?;
    Ok(OntologyModuleObject {
        descriptor: descriptor.clone(),
        object: object.clone(),
    })
}

/// One research Version and the transport object of its package.
#[derive(Serialize, Tsify)]
pub struct ResearchVersionObject {
    /// Lineage Version entry.
    pub version: LineageVersion,
    /// Transport object of the Version package.
    pub object: ObjectDescriptor,
}

/// Select the package object of one research Version UUID.
pub fn research_version_object(
    manifest: &[u8],
    lineage: &[u8],
    version_uuid: &str,
    limits: DiscoveryLimits,
) -> Outcome<ResearchVersionObject> {
    let manifest = DiscoveryManifest::from_json(manifest, limits)?;
    let lineage = ResearchLineage::from_json(lineage, limits)?;
    let (version, object) = manifest.research_version_object(&lineage, version_uuid)?;
    Ok(ResearchVersionObject {
        version: version.clone(),
        object: object.clone(),
    })
}

/// A repository snapshot with its control objects as exact transport text.
#[derive(Deserialize, Serialize, Tsify)]
#[serde(deny_unknown_fields)]
pub struct RehomeSnapshot {
    /// Parsed original manifest.
    pub manifest: DiscoveryManifest,
    /// Exact summary transport text, iff the manifest advertises one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Exact lineage transport text, iff the manifest advertises one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<String>,
}

/// Complete replacement set of a re-homed repository.
#[derive(Serialize, Tsify)]
pub struct RehomeResult {
    /// Re-homed refs with freshly derived manifest validators.
    pub refs: RefSet,
    /// Snapshots ordered by their new canonical manifest digest.
    pub snapshots: Vec<RehomeSnapshot>,
    /// Canonical digest of the new refs document.
    pub revision: String,
}

/// Re-home a repository over its complete current ref closure.
pub fn rehome_repository(
    refs: &[u8],
    snapshots: &[u8],
    new_identity: &[u8],
    control_locations: &str,
    limits: DiscoveryLimits,
) -> Outcome<RehomeResult> {
    let refs = RefSet::from_json(refs, limits)?;
    let snapshots: Vec<RehomeSnapshot> = argument(snapshots, "snapshots")?;
    let new_identity: RepositoryIdentity = argument(new_identity, "identity")?;
    let template = ObjectLocationTemplate::parse(control_locations)?;
    let snapshots = snapshots
        .into_iter()
        .map(|snapshot| RepositorySnapshot {
            manifest: snapshot.manifest,
            summary: snapshot.summary.map(String::into_bytes),
            lineage: snapshot.lineage.map(String::into_bytes),
        })
        .collect::<Vec<_>>();
    let RehomedRepository {
        refs,
        snapshots,
        revision,
    } = graphforge_discovery::rehome_repository(
        &refs,
        &snapshots,
        &new_identity,
        &template,
        limits,
    )?;
    let text = |bytes: Option<Vec<u8>>| {
        bytes
            .map(String::from_utf8)
            .transpose()
            .map_err(|_| ProtocolError::argument("control object is not UTF-8"))
    };
    let snapshots = snapshots
        .into_iter()
        .map(|snapshot| {
            Ok(RehomeSnapshot {
                manifest: snapshot.manifest,
                summary: text(snapshot.summary)?,
                lineage: text(snapshot.lineage)?,
            })
        })
        .collect::<Outcome<Vec<_>>>()?;
    Ok(RehomeResult {
        refs,
        snapshots,
        revision: digest_string(revision),
    })
}

// -------------------------------------------------------------------- preview

/// Authoritative pixel dimensions of an admitted image.
#[derive(Serialize, Tsify)]
pub struct ImageDimensions {
    /// Pixel width.
    pub width: u32,
    /// Pixel height.
    pub height: u32,
}

/// Validate image bytes against the closed PNG/SVG profile.
pub fn validate_image(media_type: &str, bytes: &[u8]) -> Outcome<ImageDimensions> {
    let (width, height) = preview::validate_image(media_type, bytes)?;
    Ok(ImageDimensions { width, height })
}

/// Revalidate untrusted bytes against every field of their descriptor.
pub fn validate_preview(descriptor: &[u8], bytes: &[u8]) -> Outcome<()> {
    let descriptor: PreviewDescriptor = argument(descriptor, "descriptor")?;
    Ok(preview::validate_preview(&descriptor, bytes)?)
}

/// Validate a descriptor list without fetching image bytes.
pub fn validate_preview_descriptors(descriptors: &[u8]) -> Outcome<()> {
    let descriptors: Vec<PreviewDescriptor> = argument(descriptors, "descriptors")?;
    Ok(preview::validate_descriptors(&descriptors)?)
}

/// The preview descriptors a manifest selects.
pub fn manifest_preview_descriptors(
    manifest: &[u8],
    limits: DiscoveryLimits,
) -> Outcome<Vec<PreviewDescriptor>> {
    Ok(DiscoveryManifest::from_json(manifest, limits)?.preview_descriptors()?)
}

// ----------------------------------------------------------------- hub publish

/// A validated publish capabilities document.
#[derive(Serialize, Tsify)]
pub struct ValidatedCapabilities {
    /// The parsed document.
    pub document: PublishCapabilities,
}

/// Parse and validate a capabilities document.
pub fn validate_capabilities(bytes: &[u8]) -> Outcome<ValidatedCapabilities> {
    Ok(ValidatedCapabilities {
        document: PublishCapabilities::from_json(bytes)?,
    })
}

/// A validated session request.
#[derive(Serialize, Tsify)]
pub struct ValidatedSessionRequest {
    /// The parsed document.
    pub document: OpenSessionRequest,
}

/// Parse and validate a session request.
pub fn validate_session_request(bytes: &[u8]) -> Outcome<ValidatedSessionRequest> {
    Ok(ValidatedSessionRequest {
        document: OpenSessionRequest::from_json(bytes)?,
    })
}

/// A validated commit request.
#[derive(Serialize, Tsify)]
pub struct ValidatedCommitRequest {
    /// The parsed document.
    pub document: CommitRequest,
}

/// Parse and validate a commit request.
pub fn validate_commit_request(bytes: &[u8]) -> Outcome<ValidatedCommitRequest> {
    Ok(ValidatedCommitRequest {
        document: CommitRequest::from_json(bytes)?,
    })
}

/// A validated publish receipt.
#[derive(Serialize, Tsify)]
pub struct ValidatedReceipt {
    /// The parsed document.
    pub document: PublishReceipt,
}

/// Parse and validate a publish receipt.
pub fn validate_receipt(bytes: &[u8]) -> Outcome<ValidatedReceipt> {
    Ok(ValidatedReceipt {
        document: PublishReceipt::from_json(bytes)?,
    })
}

/// A validated operation status.
#[derive(Serialize, Tsify)]
pub struct ValidatedOperationStatus {
    /// The parsed document.
    pub document: OperationStatus,
}

/// Parse and validate an operation status.
pub fn validate_operation_status(bytes: &[u8]) -> Outcome<ValidatedOperationStatus> {
    Ok(ValidatedOperationStatus {
        document: OperationStatus::from_json(bytes)?,
    })
}

/// Everything a request commitment binds (`PublishIntent`).
#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
pub struct PublishIntentInput {
    /// Explicit structural rewrite intent.
    #[serde(default)]
    pub rewrite: bool,
    /// Explicit dependency-safe prune intent.
    #[serde(default)]
    pub prune: bool,
    /// Target repository.
    pub repository: RepositoryIdentity,
    /// Client-defined digest of what the operation publishes.
    pub intent_digest: String,
    /// Object inventory, strictly ascending by digest.
    pub objects: Vec<ObjectDeclaration>,
    /// Canonical digest of the discovery manifest.
    pub manifest_validator: String,
    /// Refs to advance, strictly ascending.
    pub refs: Vec<String>,
    /// Expected repository revision, or `null` to create an absent repository.
    pub expected_revision: Option<String>,
}

/// Compute the request commitment of a publish intent.
pub fn request_commitment(intent: &[u8]) -> Outcome<String> {
    let intent: PublishIntentInput = argument(intent, "intent")?;
    Ok(PublishIntent {
        rewrite: intent.rewrite,
        prune: intent.prune,
        repository: intent.repository,
        intent_digest: intent.intent_digest,
        objects: intent.objects,
        manifest_validator: intent.manifest_validator,
        refs: intent.refs,
        expected_revision: intent.expected_revision,
    }
    .request_commitment())
}

/// Inputs of the commit-time manifest check.
#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
pub struct CommitManifestInput {
    /// Discovery manifest named by the commit request.
    pub manifest: DiscoveryManifest,
    /// Repository the commit URL names.
    pub repository: RepositoryIdentity,
    /// This Hub's capabilities document for the repository.
    pub capabilities: PublishCapabilities,
    /// Session object inventory.
    pub inventory: Vec<ObjectDeclaration>,
    /// Refs the commit advances.
    pub refs: Vec<String>,
}

/// Check the manifest a commit would publish against the session.
pub fn check_commit_manifest(input: &[u8]) -> Outcome<()> {
    let input: CommitManifestInput = argument(input, "commit")?;
    input.manifest.validate(DiscoveryLimits::default())?;
    input.capabilities.validate()?;
    graphforge_hub_publish::validate_inventory(&input.inventory)?;
    graphforge_hub_publish::validate_ref_list(&input.refs)?;
    Ok(graphforge_hub_publish::check_commit_manifest(
        &input.manifest,
        &input.repository,
        &input.capabilities,
        &input.inventory,
        &input.refs,
    )?)
}
