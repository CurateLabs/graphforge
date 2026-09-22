//! Immutable research content, independent context heads and durable receipts.
//!
//! All records are authenticated participants of the Project's sole CURRENT.
//! Unmaterialized content pins source generations. Authenticated CAS placement
//! retains exact selected content independently of physical ancestors (ADR 0039).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

mod branch_content;
pub use branch_content::{
    PreparedBranchContent, PreparedResearchContent, materialize_prepared_branch,
    prepare_branch_content,
};
mod project_content;
pub use project_content::{PreparedProjectDraft, prepare_project_draft};
mod proposals;
pub use proposals::{
    ResearchAcceptedMapping, ResearchDecisionPublication, ResearchProposalDecision,
    ResearchProposalDestination, ResearchProposalHistory, ResearchProposalItem,
    ResearchProposalRecord, ResearchProposalReview, ResearchProposalUnit,
};
mod branch_selection;
mod proposal_publication;
mod proposal_validation;
pub use branch_selection::{prepare_branch_selection, replace_prepared_branch_domains};
mod branches;
pub use branches::ResearchBranchRecord;
mod project_restore;
pub use project_restore::materialize_research_project;
mod projection;
mod retained_content;
pub use projection::{ResearchGraphProjection, ResearchGraphSelection};
pub use retained_content::materialize_research_graph;

use crate::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectStageOutcome, ResolvedProjectGeneration,
};

/// Required research capability; unsupported readers must refuse it.
pub const RESEARCH_CAPABILITY: &str = "research";
/// Frozen research capability and registry record version.
pub const RESEARCH_VERSION: u32 = 4;
/// Authenticated registry record family.
pub const RESEARCH_REGISTRY: &str = "registry";
/// Maximum canonical registry payload; no unbounded history growth.
pub const MAX_REGISTRY_BYTES: usize = 8 * 1024 * 1024;
/// Maximum retained immutable Versions.
pub const MAX_VERSIONS: usize = 1_024;
/// Maximum durable receipts, retained for the Project lifetime.
pub const MAX_RECEIPTS: usize = 4_096;
/// Maximum independently addressed context heads.
pub const MAX_CONTEXTS: usize = 256;
/// Maximum explicit dependency roots.
pub const MAX_ROOTS: usize = 4_096;
const PRODUCER: &str = concat!(
    "graphforge-storage/",
    env!("CARGO_PKG_VERSION"),
    ";research/4"
);

/// Exact participant identity, never a filesystem path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchParticipantKey {
    /// Owning capability.
    pub capability: String,
    /// Record family.
    pub family: String,
}

/// Frozen manifest-authenticated participant commitment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchParticipantCommitment {
    /// Stable capability and family.
    pub key: ResearchParticipantKey,
    /// Required capability version.
    pub capability_version: u32,
    /// Required record version.
    pub record_version: u32,
    /// Persisted encoding.
    pub encoding: String,
    /// Exact schema identity.
    pub schema_sha256: [u8; 32],
    /// Logical rows in the participant.
    pub row_count: u64,
    /// Exact content digest.
    pub content_sha256: [u8; 32],
}

/// Frozen content, separate from context heads and later receipt history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchVersionContent {
    /// Conservative physical source locator, not the research identity.
    pub generation_uuid: Uuid,
    /// Authenticated source manifest digest.
    pub manifest_sha256: [u8; 32],
    /// None denotes complete Project research; Some denotes a projection's origin.
    pub source_version: Option<Uuid>,
    /// Selected graph identity when shared physical units have been repacked.
    pub graph_projection: Option<ResearchGraphProjection>,
    /// Exact frozen selected participants, in canonical key order.
    pub participants: Vec<ResearchParticipantCommitment>,
    /// Required retained research dependencies, distinct from provenance.
    pub required_versions: BTreeSet<Uuid>,
    /// Identity of the producer contract.
    pub producer: String,
    /// Domain-owner supplied required evidence closure, authenticated by storage.
    pub evidence: Vec<ResearchEvidenceReference>,
}

/// A required local object or explicit external-only evidence reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "availability", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchEvidenceReference {
    /// Exact locally retained bytes; storage verifies them without fetching.
    Local {
        /// Stable Artifact identity supplied by its owning domain.
        artifact_uuid: Uuid,
        /// Exact immutable content identity.
        sha256: [u8; 32],
        /// Exact retained length.
        byte_length: u64,
    },
    /// Historical external reference; bytes are not archived or fetched.
    ExternalOnly {
        /// Stable Artifact identity.
        artifact_uuid: Uuid,
        /// Known historical fingerprint, if one was recorded.
        fingerprint: Option<[u8; 32]>,
    },
    /// Evidence whose historical bytes cannot be verified.
    Unverifiable {
        /// Stable Artifact identity.
        artifact_uuid: Uuid,
    },
}

/// An immutable research Version and its citation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchVersionRecord {
    /// Immutable research identity, independent of storage and package IDs.
    pub version_uuid: Uuid,
    /// Owning research context.
    pub context_uuid: Uuid,
    /// Optional analyst label, frozen with this record.
    pub label: Option<String>,
    /// Optional description, frozen with this record.
    pub description: Option<String>,
    /// Caller-recorded UTC creation time in microseconds.
    pub created_at: i64,
    /// Exact content locator and commitments.
    pub content: ResearchVersionContent,
}

/// Lifetime class for a registered dependency root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchRootKind {
    /// Explicitly retained research, releasable by its owner.
    RetainedVersion,
    /// A live Branch base, released by its owning lifecycle.
    BranchBase,
    /// A live child context dependency, released by its owning lifecycle.
    ChildBranch,
    /// Frozen review dependency, released by its owning lifecycle.
    FrozenProposal,
    /// Acceptance evidence: never implicitly released or restored backwards.
    AcceptedProvenance,
}

/// Registered root fixture; genealogy alone never creates one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchRetentionRoot {
    /// Stable root identity.
    pub root_uuid: Uuid,
    /// Explicit lifetime class.
    pub kind: ResearchRootKind,
    /// Exact required Version dependencies.
    pub versions: BTreeSet<Uuid>,
}

/// One immutable operation outcome. Exact replay does not require its old payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchOperationReceipt {
    /// Native public Branch request commitment, separate from prepared content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_sha256: Option<[u8; 32]>,
    /// Caller-stable operation identity.
    pub operation_uuid: Uuid,
    /// Commitment to the complete request.
    pub request_sha256: [u8; 32],
    /// Generation installed by this operation, not current at replay time.
    pub generation_uuid: Uuid,
    /// Created or restored Version, if applicable.
    pub version_uuid: Option<Uuid>,
}

/// Complete authenticated current research metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchRegistry {
    /// Restore-independent frozen submissions and permanent accepted mappings.
    pub proposals: ResearchProposalHistory,
    /// Immutable Branch creation and genealogy; heads are stored separately below.
    pub branches: BTreeMap<Uuid, ResearchBranchRecord>,
    /// Immutable records still retained explicitly or by required dependencies.
    pub versions: BTreeMap<Uuid, ResearchVersionRecord>,
    /// Independently advancing context heads.
    pub heads: BTreeMap<Uuid, Uuid>,
    /// Explicit consumer-owned dependency roots.
    pub roots: BTreeMap<Uuid, ResearchRetentionRoot>,
    /// Project-lifetime replay and identity-conflict evidence.
    pub receipts: BTreeMap<Uuid, ResearchOperationReceipt>,
    /// Permanent immutable identity commitments, including released payloads.
    pub identities: BTreeMap<Uuid, [u8; 32]>,
    /// Versions whose exact participant and graph payloads are rooted in CAS.
    pub materialized: BTreeSet<Uuid>,
}

/// Request to capture frozen research from an authenticated generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterResearchVersion {
    /// New immutable research identity.
    pub version_uuid: Uuid,
    /// Context whose head advances.
    pub context_uuid: Uuid,
    /// Exact source generation in this Project container.
    pub source_generation_uuid: Uuid,
    /// None selects complete Project research; Some selects an explicit projection.
    pub selection: Option<BTreeSet<ResearchParticipantKey>>,
    /// Required source identity for a projection; provenance only.
    pub source_version: Option<Uuid>,
    /// Required retained Versions, such as a comparison baseline.
    pub required_versions: BTreeSet<Uuid>,
    /// Optional immutable citation label.
    pub label: Option<String>,
    /// Optional immutable description.
    pub description: Option<String>,
    /// UTC time in microseconds.
    pub created_at: i64,
    /// Required evidence closure supplied by the Source/Artifact domain owner.
    pub evidence: Vec<ResearchEvidenceReference>,
}

/// One bounded research mutation, published with its receipt in CURRENT.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchMutation {
    /// Freeze exact selected research and root it without advancing any head.
    SubmitProposal {
        /// Native public request commitment.
        intent_sha256: [u8; 32],
        /// Immutable review metadata, validated by the native research owner.
        proposal: Box<ResearchProposalRecord>,
        /// Authenticated independently selected payload.
        payload: Box<ResearchVersionRecord>,
    },
    /// Publish reviewed destination content and acceptance history atomically.
    ReviewProposal {
        /// Native public request commitment.
        intent_sha256: [u8; 32],
        /// Exact reviewed item decisions.
        review: Box<ResearchProposalReview>,
        /// Complete resulting destination, absent for a no-content review.
        destination: Option<Box<ResearchVersionRecord>>,
        /// Selected accepted source proof, independent of deferred content.
        proof: Option<Box<ResearchVersionRecord>>,
        /// Newly accepted exact contribution revisions.
        mappings: Vec<ResearchAcceptedMapping>,
        /// Optional native integration and explicit promotion history.
        decisions: Option<Box<ResearchDecisionPublication>>,
    },
    /// Explicitly release obsolete proposal payload; immutable history remains.
    ReleaseProposal {
        /// Native public request commitment.
        intent_sha256: [u8; 32],
        /// Proposal whose pending payload is withdrawn or terminal.
        proposal_uuid: Uuid,
    },
    /// Atomically publish domain-owner prepared Branch content and creation metadata.
    /// All committed content must already be authenticated in this Project CAS.
    PublishBranch {
        /// Canonical native public request, used before repeating preparation.
        intent_sha256: [u8; 32],
        /// Optional authenticated current-Project origin, recorded as genealogy only.
        origin_capture: Option<Box<RegisterResearchVersion>>,
        /// Present only for first publication; immutable after creation.
        creation: Option<ResearchBranchRecord>,
        /// Complete effective Branch state, not a mutable upstream lookup.
        version: Box<ResearchVersionRecord>,
    },
    /// Register a separately identified selected graph/evidence closure.
    RegisterGraphProjection {
        /// Metadata and explicit participant/evidence selection from a Version.
        spec: RegisterResearchVersion,
        /// Exact graph identities and closure.
        selection: ResearchGraphSelection,
    },
    /// Share exact selected content in CAS and release physical ancestor pins.
    Compact {
        /// Retained Versions to materialize; identity and replay history stay fixed.
        versions: BTreeSet<Uuid>,
    },
    /// Capture research and advance only the named context head.
    Register(RegisterResearchVersion),
    /// Restore frozen content into a new Version of its owning context.
    Restore {
        /// Owning context; a different context cannot be silently restored.
        context_uuid: Uuid,
        /// Retained immutable source.
        source_version: Uuid,
        /// Fresh immutable identity for the restored current state.
        version_uuid: Uuid,
        /// New Version's UTC creation time in microseconds.
        created_at: i64,
    },
    /// Replace complete Project research, preserving current history and other heads.
    RestoreProject {
        /// Owning context of the complete source Version.
        context_uuid: Uuid,
        /// Retained complete Version; projections are rejected.
        source_version: Uuid,
        /// Fresh immutable Version identity.
        version_uuid: Uuid,
        /// New Version creation time in UTC microseconds.
        created_at: i64,
    },
    /// Register an explicit immutable dependency root.
    RetainRoot {
        /// Consumer-owned root and dependencies.
        root: ResearchRetentionRoot,
    },
    /// Release an obsolete root; accepted provenance cannot be released here.
    ReleaseRoot {
        /// Exact releasable root identity.
        root_uuid: Uuid,
    },
    /// Release a Version when no head, root or required Version depends on it.
    DeleteVersion {
        /// Exact Version to release; receipts remain available.
        version_uuid: Uuid,
    },
}

/// Complete operation identity and optimistic precondition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchOperation {
    /// Caller-stable identity, retained for the Project lifetime.
    pub operation_uuid: Uuid,
    /// Required CURRENT before a new operation; ignored only for exact replay.
    pub expected_generation_uuid: Uuid,
    /// One bounded mutation.
    pub mutation: ResearchMutation,
}

fn error(code: ProjectErrorCode, message: &str) -> GfError {
    GfError::Project {
        code,
        message: message.into(),
    }
}

fn invalid(message: &str) -> GfError {
    error(ProjectErrorCode::ProjectCorrupt, message)
}

fn cancelled(flag: &AtomicBool) -> Result<(), GfError> {
    if flag.load(Ordering::Relaxed) {
        Err(GfError::Api {
            code: ApiErrorCode::Cancelled,
            message: "research operation cancelled before publication".into(),
        })
    } else {
        Ok(())
    }
}

fn json<T: Serialize>(value: &T) -> Result<Vec<u8>, GfError> {
    serde_json::to_vec(value).map_err(|_| invalid("research metadata cannot be encoded"))
}

fn bounded_string(value: Option<&str>) -> bool {
    value.is_none_or(|value| value.len() <= 4_096)
}

fn history(key: &ResearchParticipantKey) -> bool {
    key.capability == RESEARCH_CAPABILITY
        || (key.capability == "workspace" && key.family == "restoration_transition")
}

impl ResearchRegistry {
    /// Validate bounded identities and dependency closure before publication or use.
    pub fn validate(&self) -> Result<(), GfError> {
        self.validate_capacity()?;
        for (id, version) in &self.versions {
            if id.is_nil()
                || *id != version.version_uuid
                || version.context_uuid.is_nil()
                || version.content.generation_uuid.is_nil()
                || !bounded_string(version.label.as_deref())
                || !bounded_string(version.description.as_deref())
                || version.content.producer.is_empty()
                || version.content.producer.len() > 256
            {
                return Err(invalid("invalid research Version identity or metadata"));
            }
            let keys: Vec<_> = version
                .content
                .participants
                .iter()
                .map(|p| &p.key)
                .collect();
            let mut artifacts = BTreeSet::new();
            for evidence in &version.content.evidence {
                let (ResearchEvidenceReference::Local { artifact_uuid, .. }
                | ResearchEvidenceReference::ExternalOnly { artifact_uuid, .. }
                | ResearchEvidenceReference::Unverifiable { artifact_uuid }) = evidence;
                if artifact_uuid.is_nil() || !artifacts.insert(*artifact_uuid) {
                    return Err(invalid("research evidence identity is nil or duplicated"));
                }
            }
            if self.identities.get(id) != Some(&identity_digest(version)?) {
                return Err(invalid(
                    "research Version differs from its immutable identity commitment",
                ));
            }
            if keys.windows(2).any(|p| p[0] >= p[1]) || keys.iter().any(|key| history(key)) {
                return Err(invalid(
                    "research content has duplicate, unsorted or history participants",
                ));
            }
            for dependency in &version.content.required_versions {
                if !self.versions.contains_key(dependency) {
                    return Err(invalid(
                        "research Version required dependency is unavailable",
                    ));
                }
            }
        }
        if self
            .materialized
            .iter()
            .any(|id| !self.versions.contains_key(id))
        {
            return Err(invalid("materialized research identity is unavailable"));
        }
        branches::validate(self)?;
        proposal_validation::validate(self)?;
        self.validate_dependency_cycles()?;
        for (context, version) in &self.heads {
            if self
                .versions
                .get(version)
                .is_none_or(|record| record.context_uuid != *context)
            {
                return Err(invalid(
                    "research context head is dangling or belongs to another context",
                ));
            }
        }
        for (id, root) in &self.roots {
            if id.is_nil()
                || *id != root.root_uuid
                || root.versions.is_empty()
                || root
                    .versions
                    .iter()
                    .any(|id| !self.versions.contains_key(id))
            {
                return Err(invalid("research retention root is invalid or dangling"));
            }
        }
        for (id, receipt) in &self.receipts {
            if id.is_nil() || *id != receipt.operation_uuid || receipt.generation_uuid.is_nil() {
                return Err(invalid("research receipt identity is invalid"));
            }
        }
        if json(self)?.len() > MAX_REGISTRY_BYTES {
            return Err(error(
                ProjectErrorCode::ResourceLimit,
                "research registry byte limit exceeded",
            ));
        }
        Ok(())
    }

    fn validate_capacity(&self) -> Result<(), GfError> {
        if self.branches.len() > MAX_CONTEXTS
            || self.versions.len() > MAX_VERSIONS
            || self.receipts.len() > MAX_RECEIPTS
            || self.heads.len() > MAX_CONTEXTS
            || self.roots.len() > MAX_ROOTS
            || self.identities.len() > MAX_RECEIPTS
        {
            return Err(error(
                ProjectErrorCode::ResourceLimit,
                "research registry capacity exceeded; receipts do not expire implicitly",
            ));
        }
        Ok(())
    }

    fn validate_dependency_cycles(&self) -> Result<(), GfError> {
        // Explicit stack traversal avoids recursion on untrusted registry depth.
        for start in self.versions.keys() {
            let mut visiting = BTreeSet::new();
            let mut done = BTreeSet::new();
            let mut stack = vec![(*start, false)];
            while let Some((id, exiting)) = stack.pop() {
                if exiting {
                    visiting.remove(&id);
                    done.insert(id);
                    continue;
                }
                if done.contains(&id) {
                    continue;
                }
                if !visiting.insert(id) {
                    return Err(invalid("research dependencies contain a cycle"));
                }
                stack.push((id, true));
                for child in &self.versions[&id].content.required_versions {
                    stack.push((*child, false));
                }
            }
        }
        Ok(())
    }

    /// Encode the validated authenticated registry participant.
    pub fn participant(&self) -> Result<ProjectParticipant, GfError> {
        self.validate()?;
        Ok(ProjectParticipant {
            capability_id: RESEARCH_CAPABILITY.into(),
            capability_version: RESEARCH_VERSION,
            record_family_id: RESEARCH_REGISTRY.into(),
            record_version: RESEARCH_VERSION,
            encoding: ProjectParticipantEncoding::Json,
            schema_fingerprint: Sha256::digest(b"graphforge-research-registry/4").into(),
            row_count: 1,
            bytes: json(self)?,
        })
    }

    /// Explain direct deletion blockers without mutating retained research.
    #[must_use]
    pub fn deletion_blockers(&self, version: Uuid) -> Vec<String> {
        let mut blockers = Vec::new();
        for (id, branch) in &self.branches {
            if branch.base_version_uuid == version {
                blockers.push(format!("branch_base:{id}"));
            }
        }
        for (context, head) in &self.heads {
            if *head == version {
                blockers.push(format!("context:{context}"));
            }
        }
        for (id, root) in &self.roots {
            if root.versions.contains(&version) {
                blockers.push(format!("root:{id}"));
            }
        }
        for (id, record) in &self.versions {
            if record.content.required_versions.contains(&version) {
                blockers.push(format!("version:{id}"));
            }
        }
        blockers
    }
}

/// Read and validate CURRENT's registry without modifying the Project.
pub fn read_research_registry(
    generation: &ResolvedProjectGeneration,
) -> Result<ResearchRegistry, GfError> {
    let Some(capability) = generation.capability(RESEARCH_CAPABILITY)? else {
        return Ok(ResearchRegistry::default());
    };
    if capability.capability_version != RESEARCH_VERSION {
        return Err(error(
            ProjectErrorCode::UnsupportedCapabilityVersion,
            "unsupported research capability",
        ));
    }
    let path = generation.participant_path(RESEARCH_CAPABILITY, RESEARCH_REGISTRY)?;
    if std::fs::metadata(path)
        .map_err(|_| invalid("research registry is unreadable"))?
        .len()
        > MAX_REGISTRY_BYTES as u64
    {
        return Err(invalid("oversized research registry participant"));
    }
    let snapshot = generation
        .participant_snapshot(RESEARCH_CAPABILITY, RESEARCH_REGISTRY)?
        .ok_or_else(|| invalid("research capability has no registry participant"))?;
    if snapshot.record_version != RESEARCH_VERSION
        || snapshot.row_count != 1
        || snapshot.encoding != "json"
        || snapshot.schema_fingerprint
            != <[u8; 32]>::from(Sha256::digest(b"graphforge-research-registry/4"))
        || snapshot.bytes.len() > MAX_REGISTRY_BYTES
    {
        return Err(invalid(
            "unsupported or oversized research registry participant",
        ));
    }
    let registry: ResearchRegistry = serde_json::from_slice(&snapshot.bytes)
        .map_err(|_| invalid("malformed research registry"))?;
    registry.validate()?;
    if json(&registry)? != snapshot.bytes {
        return Err(invalid("research registry is not canonical"));
    }
    Ok(registry)
}

fn commitments(
    generation: &ResolvedProjectGeneration,
) -> Result<Vec<ResearchParticipantCommitment>, GfError> {
    Ok(generation
        .participant_descriptors()?
        .into_iter()
        .filter_map(|p| {
            let key = ResearchParticipantKey {
                capability: p.capability_id,
                family: p.record_family_id,
            };
            (!history(&key)).then_some(ResearchParticipantCommitment {
                key,
                capability_version: p.capability_version,
                record_version: p.record_version,
                encoding: p.encoding,
                schema_sha256: p.schema_fingerprint,
                row_count: p.row_count,
                content_sha256: p.content_sha256,
            })
        })
        .collect())
}

fn capture(
    root: &Path,
    request: &RegisterResearchVersion,
    registry: &ResearchRegistry,
) -> Result<ResearchVersionRecord, GfError> {
    let source = crate::resolve_generation_by_uuid(root, request.source_generation_uuid)?;
    source.validate_complete_participant_inventory()?;
    let mut participants = commitments(&source)?;
    if let Some(selection) = &request.selection {
        let origin = request
            .source_version
            .and_then(|id| registry.versions.get(&id))
            .ok_or_else(|| invalid("a projection requires a retained source Version"))?;
        if origin.content.generation_uuid != source.generation_uuid() {
            return Err(invalid(
                "projection source Version does not identify the selected generation",
            ));
        }
        if request
            .evidence
            .iter()
            .any(|reference| !origin.content.evidence.contains(reference))
        {
            return Err(invalid(
                "projection evidence differs from its source Version",
            ));
        }
        participants.retain(|p| selection.contains(&p.key));
        if participants.len() != selection.len()
            || participants
                .iter()
                .any(|p| !origin.content.participants.contains(p))
        {
            return Err(invalid(
                "projection contains unavailable or out-of-source participants",
            ));
        }
    } else if request.source_version.is_some() {
        return Err(invalid("complete Version cannot claim projection identity"));
    }
    if participants
        .iter()
        .any(|p| p.key.capability == "graph" && p.key.family == "files")
    {
        source.graph_files_inventory()?;
    }
    // Authenticate frozen bytes at capture; descriptors alone do not prove content.
    authenticate_evidence(root, &request.evidence)?;
    for p in &participants {
        source.participant_snapshot(&p.key.capability, &p.key.family)?;
    }
    Ok(ResearchVersionRecord {
        version_uuid: request.version_uuid,
        context_uuid: request.context_uuid,
        label: request.label.clone(),
        description: request.description.clone(),
        created_at: request.created_at,
        content: ResearchVersionContent {
            generation_uuid: source.generation_uuid(),
            manifest_sha256: source.manifest_sha256(),
            source_version: request.source_version,
            graph_projection: None,
            participants,
            required_versions: request.required_versions.clone(),
            producer: PRODUCER.into(),
            evidence: request.evidence.clone(),
        },
    })
}

fn insert_version(
    registry: &mut ResearchRegistry,
    version: ResearchVersionRecord,
) -> Result<Uuid, GfError> {
    let context = version.context_uuid;
    let id = insert_content(registry, version)?;
    registry.heads.insert(context, id);
    Ok(id)
}

fn insert_content(
    registry: &mut ResearchRegistry,
    version: ResearchVersionRecord,
) -> Result<Uuid, GfError> {
    let id = version.version_uuid;
    let digest = identity_digest(&version)?;
    if registry
        .identities
        .get(&id)
        .is_some_and(|old| *old != digest)
    {
        return Err(error(
            ProjectErrorCode::TransactionConflict,
            "immutable research Version identity has conflicting content",
        ));
    }
    registry.identities.insert(id, digest);
    registry.versions.insert(id, version);
    Ok(id)
}

fn identity_digest(version: &ResearchVersionRecord) -> Result<[u8; 32], GfError> {
    // Physical relocation does not change complete immutable content identity.
    let mut logical = version.clone();
    logical.content.generation_uuid = Uuid::nil();
    logical.content.manifest_sha256 = [0; 32];
    Ok(Sha256::digest(json(&logical)?).into())
}

fn restore_context(
    root: &Path,
    registry: &mut ResearchRegistry,
    mutation: &ResearchMutation,
    context_uuid: Uuid,
    source_version: Uuid,
    version_uuid: Uuid,
    created_at: i64,
) -> Result<Option<Uuid>, GfError> {
    let mut version = registry
        .versions
        .get(&source_version)
        .cloned()
        .ok_or_else(|| invalid("restore Version is unavailable"))?;
    if version.context_uuid != context_uuid || registry.versions.contains_key(&version_uuid) {
        return Err(invalid(
            "restore requires its owning context and a new Version identity",
        ));
    }
    if matches!(mutation, ResearchMutation::RestoreProject { .. })
        && version.content.source_version.is_some()
    {
        return Err(invalid(
            "Project restore requires a complete Version, not a projection",
        ));
    }
    inspect_research_version(root, &version)?;
    version.version_uuid = version_uuid;
    version.created_at = created_at;
    if registry.materialized.contains(&source_version) {
        registry.materialized.insert(version_uuid);
    }
    Ok(Some(insert_version(registry, version)?))
}

fn apply_mutation(
    root: &Path,
    operation_uuid: Uuid,
    mutation: &ResearchMutation,
    registry: &mut ResearchRegistry,
) -> Result<Option<Uuid>, GfError> {
    let version_uuid = match mutation {
        ResearchMutation::SubmitProposal { .. }
        | ResearchMutation::ReviewProposal { .. }
        | ResearchMutation::ReleaseProposal { .. } => {
            proposal_publication::apply(root, registry, operation_uuid, mutation)?
        }
        ResearchMutation::PublishBranch {
            origin_capture,
            creation,
            version,
            ..
        } => Some(branches::publish_with_origin(
            root,
            registry,
            origin_capture.as_deref(),
            creation.as_ref(),
            version,
        )?),
        ResearchMutation::RegisterGraphProjection { spec, selection } => {
            if registry.branches.contains_key(&spec.context_uuid) {
                return Err(invalid("raw projection cannot replace a Branch context"));
            }
            Some(projection::register(root, registry, spec, selection)?)
        }
        ResearchMutation::Compact { versions } => {
            retained_content::compact(root, registry, versions)?;
            None
        }
        ResearchMutation::Register(spec) => {
            if registry.branches.contains_key(&spec.context_uuid) {
                return Err(invalid("Project capture cannot replace a Branch context"));
            }
            let version = capture(root, spec, registry)?;
            Some(insert_version(registry, version)?)
        }
        ResearchMutation::Restore {
            context_uuid,
            source_version,
            version_uuid,
            created_at,
        }
        | ResearchMutation::RestoreProject {
            context_uuid,
            source_version,
            version_uuid,
            created_at,
        } => restore_context(
            root,
            registry,
            mutation,
            *context_uuid,
            *source_version,
            *version_uuid,
            *created_at,
        )?,
        ResearchMutation::RetainRoot { root } => {
            if registry
                .roots
                .get(&root.root_uuid)
                .is_some_and(|old| old != root)
            {
                return Err(error(
                    ProjectErrorCode::TransactionConflict,
                    "retention root identity has conflicting dependencies",
                ));
            }
            registry.roots.insert(root.root_uuid, root.clone());
            None
        }
        ResearchMutation::ReleaseRoot { root_uuid } => {
            let root = registry
                .roots
                .get(root_uuid)
                .ok_or_else(|| invalid("retention root is unavailable"))?;
            if root.kind == ResearchRootKind::AcceptedProvenance {
                return Err(invalid(
                    "accepted provenance cannot be released by research history cleanup",
                ));
            }
            registry.roots.remove(root_uuid);
            None
        }
        ResearchMutation::DeleteVersion { version_uuid } => {
            delete_version(registry, *version_uuid)?;
            None
        }
    };
    Ok(version_uuid)
}

fn delete_version(registry: &mut ResearchRegistry, version_uuid: Uuid) -> Result<(), GfError> {
    let blockers = registry.deletion_blockers(version_uuid);
    if !blockers.is_empty() {
        return Err(error(
            ProjectErrorCode::TransactionFailed,
            &format!(
                "research Version deletion blocked by {}",
                blockers.join(",")
            ),
        ));
    }
    registry.materialized.remove(&version_uuid);
    if registry.versions.remove(&version_uuid).is_none() {
        return Err(invalid("research Version is unavailable"));
    }
    Ok(())
}

fn pin_unmaterialized_versions(
    root: &Path,
    registry: &ResearchRegistry,
) -> Result<Vec<ResolvedProjectGeneration>, GfError> {
    registry
        .versions
        .values()
        .filter(|version| !registry.materialized.contains(&version.version_uuid))
        .map(|version| {
            let source = crate::resolve_generation_by_uuid(root, version.content.generation_uuid)?;
            if source.manifest_sha256() != version.content.manifest_sha256 {
                return Err(invalid("research source manifest identity changed"));
            }
            Ok(source)
        })
        .collect::<Result<Vec<_>, GfError>>()
}

fn publication_request(
    parent: &ResolvedProjectGeneration,
    request: &ResearchOperation,
    generation_uuid: Uuid,
    research: ProjectParticipant,
) -> Result<ProjectGenerationRequest, GfError> {
    parent.validate_complete_participant_inventory()?;
    let mut participants = Vec::new();
    for snapshot in parent.participant_snapshots()? {
        if snapshot.capability_id == RESEARCH_CAPABILITY
            && snapshot.record_family_id == RESEARCH_REGISTRY
        {
            continue;
        }
        participants.push(ProjectParticipant {
            capability_id: snapshot.capability_id,
            capability_version: snapshot.capability_version,
            record_family_id: snapshot.record_family_id,
            record_version: snapshot.record_version,
            encoding: match snapshot.encoding.as_str() {
                "json" => ProjectParticipantEncoding::Json,
                "parquet" => ProjectParticipantEncoding::Parquet,
                "arrow" => ProjectParticipantEncoding::Arrow,
                _ => return Err(invalid("unsupported retained participant encoding")),
            },
            schema_fingerprint: snapshot.schema_fingerprint,
            row_count: snapshot.row_count,
            bytes: snapshot.bytes,
        });
    }
    participants.push(research);
    let mut capabilities: Vec<_> = parent
        .capabilities()
        .into_iter()
        .map(|c| ProjectCapability {
            capability_id: c.capability_id,
            capability_version: c.capability_version,
        })
        .collect();
    if !capabilities
        .iter()
        .any(|c| c.capability_id == RESEARCH_CAPABILITY)
    {
        capabilities.push(ProjectCapability {
            capability_id: RESEARCH_CAPABILITY.into(),
            capability_version: RESEARCH_VERSION,
        });
    }
    Ok(ProjectGenerationRequest {
        transaction_uuid: request.operation_uuid,
        generation_uuid,
        capabilities,
        participants,
    })
}

/// Publish one mutation and its receipt atomically with the complete Project.
pub fn publish_research_operation(
    root: &Path,
    request: &ResearchOperation,
    cancellation: &AtomicBool,
) -> Result<ResearchOperationReceipt, GfError> {
    publish_research_operation_with_mode(
        root,
        request,
        cancellation,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Publish using the owning facade's admitted lifecycle mode.
/// Ephemeral mode is only for process-owned temporary Projects.
pub fn publish_research_operation_with_mode(
    root: &Path,
    request: &ResearchOperation,
    cancellation: &AtomicBool,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<ResearchOperationReceipt, GfError> {
    cancelled(cancellation)?;
    if request.operation_uuid.is_nil() {
        return Err(invalid("research operation identity is nil"));
    }
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        root,
        mode,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    let parent = crate::resolve_project_generation(root)?;
    let mut registry = read_research_registry(&parent)?;
    let request_sha256: [u8; 32] = Sha256::digest(json(request)?).into();
    if let Some(receipt) = registry.receipts.get(&request.operation_uuid) {
        return if receipt.request_sha256 == request_sha256 {
            Ok(receipt.clone())
        } else {
            Err(error(
                ProjectErrorCode::TransactionConflict,
                "research operation identity has conflicting request content",
            ))
        };
    }
    if parent.generation_uuid() != request.expected_generation_uuid {
        return Err(error(
            ProjectErrorCode::WriteConflict,
            "research operation CURRENT precondition changed",
        ));
    }
    let operation_fingerprint = branches::publication_fingerprint(request);
    proposal_publication::validate_preview_generation(request)?;
    let graph_objects = crate::begin_graph_object_publication(root)?;
    let version_uuid = apply_mutation(
        root,
        request.operation_uuid,
        &request.mutation,
        &mut registry,
    )?;
    let mut identity = Sha256::new();
    identity.update(b"graphforge-research-publication/1");
    identity.update(request.operation_uuid.as_bytes());
    identity.update(operation_fingerprint.unwrap_or(request_sha256));
    let generation_uuid = graphforge_core::canonical::uuid_v8(identity.finalize().into());
    let receipt = ResearchOperationReceipt {
        intent_sha256: match &request.mutation {
            ResearchMutation::PublishBranch { intent_sha256, .. }
            | ResearchMutation::SubmitProposal { intent_sha256, .. }
            | ResearchMutation::ReviewProposal { intent_sha256, .. }
            | ResearchMutation::ReleaseProposal { intent_sha256, .. } => Some(*intent_sha256),
            _ => None,
        },
        operation_uuid: request.operation_uuid,
        request_sha256,
        generation_uuid,
        version_uuid,
    };
    registry
        .receipts
        .insert(request.operation_uuid, receipt.clone());
    let research = registry.participant()?;
    // Keep exact source leases through publication. A source reclaimed while
    // preparing the request fails here before any authoritative mutation.
    let source_pins = pin_unmaterialized_versions(root, &registry)?;
    let mut publication = publication_request(&parent, request, generation_uuid, research)?;
    let restored_graph = project_restore::prepare(root, request, &registry, &mut publication)?;
    proposal_publication::decisions(&request.mutation, &mut publication)?;
    cancelled(cancellation)?;
    let graph_tree = if let Some(directory) = &restored_graph {
        Some(directory.path().to_path_buf())
    } else if project_restore::replaces_project(&request.mutation) {
        None
    } else {
        let tree = parent.graph_tree_root();
        tree.is_dir().then_some(tree)
    };
    let staged =
        crate::project_publication::stage_project_generation_from_admitted_parent_with_fingerprint(
            admission,
            parent,
            &publication,
            graph_tree.as_deref(),
            None,
            operation_fingerprint,
        )?;
    if let ProjectStageOutcome::Staged(staged) = staged {
        staged
            .validate(|_| registry.validate(), |_, _| Ok(()))?
            .publish_with_graph_objects_cancellable(&graph_objects, &mut || {
                cancellation.load(Ordering::Relaxed)
            })?;
    }
    drop(source_pins);
    Ok(receipt)
}

/// Authenticate and read exact historical participants; never follows current heads.
pub fn inspect_research_version(
    root: &Path,
    version: &ResearchVersionRecord,
) -> Result<Vec<crate::ProjectParticipantSnapshot>, GfError> {
    let current = crate::resolve_project_generation(root)?;
    let registry = read_research_registry(&current)?;
    inspect_with_registry(root, version, &registry)
}

fn inspect_with_registry(
    root: &Path,
    version: &ResearchVersionRecord,
    registry: &ResearchRegistry,
) -> Result<Vec<crate::ProjectParticipantSnapshot>, GfError> {
    if !registry.versions.contains_key(&version.version_uuid) {
        return Err(GfError::Api {
            code: ApiErrorCode::ResultNotRetained,
            message: "historical research Version content is not retained".into(),
        });
    }
    if registry.identities.get(&version.version_uuid) != Some(&identity_digest(version)?) {
        return Err(error(
            ProjectErrorCode::TransactionConflict,
            "historical research Version identity has conflicting content",
        ));
    }
    authenticate_evidence(root, &version.content.evidence)?;
    if registry.materialized.contains(&version.version_uuid) {
        return retained_content::inspect(root, version, None);
    }
    let source = crate::resolve_generation_by_uuid(root, version.content.generation_uuid)?;
    if source.manifest_sha256() != version.content.manifest_sha256 {
        return Err(invalid("research source manifest identity changed"));
    }
    let available = commitments(&source)?;
    if version
        .content
        .participants
        .iter()
        .any(|p| p.key.capability == "graph" && p.key.family == "files")
    {
        source.graph_files_inventory()?;
    }
    let mut snapshots = Vec::new();
    for p in &version.content.participants {
        if !available.contains(p) {
            return Err(invalid("research participant commitment changed"));
        }
        snapshots.push(
            source
                .participant_snapshot(&p.key.capability, &p.key.family)?
                .ok_or_else(|| invalid("research participant is unavailable"))?,
        );
    }
    Ok(snapshots)
}

fn authenticate_evidence(
    root: &Path,
    references: &[ResearchEvidenceReference],
) -> Result<(), GfError> {
    for evidence in references {
        if let ResearchEvidenceReference::Local {
            sha256,
            byte_length,
            ..
        } = evidence
        {
            crate::verify_graph_object(root, &hex(sha256), *byte_length)?;
        }
    }
    Ok(())
}

fn hex(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    digest
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

/// Exact local evidence objects required by the authenticated registry.
pub(crate) fn evidence_object_roots(
    generation: &ResolvedProjectGeneration,
    guard: Option<&crate::graph_object_store::GraphObjectGcGuard>,
) -> Result<BTreeSet<String>, GfError> {
    let registry = read_research_registry(generation)?;
    let mut roots: BTreeSet<_> = registry
        .versions
        .values()
        .flat_map(|v| &v.content.evidence)
        .filter_map(|reference| match reference {
            ResearchEvidenceReference::Local { sha256, .. } => Some(hex(sha256)),
            _ => None,
        })
        .collect();
    for id in &registry.materialized {
        roots.extend(retained_content::object_roots(
            generation.container_root(),
            &registry.versions[id],
            guard,
        )?);
    }
    Ok(roots)
}

/// Conservative authenticated source-generation roots for recovery and GC.
pub(crate) fn source_generation_roots(
    generation: &ResolvedProjectGeneration,
    guard: Option<&crate::graph_object_store::GraphObjectGcGuard>,
) -> Result<Vec<(Uuid, [u8; 32])>, GfError> {
    let registry = read_research_registry(generation)?;
    // Authenticate CAS closure before recovery/cleanup may release any generation.
    for id in &registry.materialized {
        retained_content::inspect(generation.container_root(), &registry.versions[id], guard)?;
    }
    Ok(registry
        .versions
        .values()
        .filter(|v| !registry.materialized.contains(&v.version_uuid))
        .map(|v| (v.content.generation_uuid, v.content.manifest_sha256))
        .collect())
}

/// Every publisher preserves permanent identity and receipt history, including
/// publishers which do not themselves implement research lifecycle operations.
pub(crate) fn validate_publication_transition(
    parent: &ResolvedProjectGeneration,
    participants: &[crate::StagedParticipant],
    directory: &Path,
    declares_research: bool,
) -> Result<(), GfError> {
    let before = read_research_registry(parent)?;
    let candidate = participants.iter().find(|p| {
        p.capability_id == RESEARCH_CAPABILITY && p.record_family_id == RESEARCH_REGISTRY
    });
    let Some(candidate) = candidate else {
        if declares_research || parent.capability(RESEARCH_CAPABILITY)?.is_some() {
            return Err(invalid(
                "publication cannot drop the research registry or its operation history",
            ));
        }
        return Ok(());
    };
    if !declares_research
        || candidate.capability_version != RESEARCH_VERSION
        || candidate.record_version != RESEARCH_VERSION
        || candidate.row_count != 1
        || candidate.encoding != "json"
        || candidate.byte_length > MAX_REGISTRY_BYTES as u64
        || candidate.schema_fingerprint
            != hex(&Sha256::digest(b"graphforge-research-registry/4").into())
    {
        return Err(invalid("unsupported research registry publication"));
    }
    let bytes = std::fs::read(directory.join(&candidate.relative_path))
        .map_err(|_| invalid("research registry publication is unreadable"))?;
    let after: ResearchRegistry = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("malformed research registry publication"))?;
    after.validate()?;
    proposals::preserve(&before, &after)?;
    if json(&after)? != bytes {
        return Err(invalid("research registry publication is not canonical"));
    }
    for (id, branch) in &before.branches {
        if after.branches.get(id) != Some(branch) {
            return Err(invalid(
                "publication cannot erase or rewrite Branch creation history",
            ));
        }
    }
    for (id, receipt) in &before.receipts {
        if after.receipts.get(id) != Some(receipt) {
            return Err(invalid(
                "publication cannot erase or rewrite research operation history",
            ));
        }
    }
    for (id, identity) in &before.identities {
        if after.identities.get(id) != Some(identity) {
            return Err(invalid(
                "publication cannot erase or rewrite immutable research identity",
            ));
        }
    }
    for (id, root) in &before.roots {
        if root.kind == ResearchRootKind::AcceptedProvenance && after.roots.get(id) != Some(root) {
            return Err(invalid(
                "publication cannot erase accepted provenance dependencies",
            ));
        }
    }
    for version in after.versions.values() {
        if after.materialized.contains(&version.version_uuid) {
            retained_content::inspect(parent.container_root(), version, None)?;
            continue;
        }
        authenticate_evidence(parent.container_root(), &version.content.evidence)?;
        let source = crate::resolve_generation_by_uuid(
            parent.container_root(),
            version.content.generation_uuid,
        )?;
        if source.manifest_sha256() != version.content.manifest_sha256 {
            return Err(invalid("research source manifest identity changed"));
        }
        let available = commitments(&source)?;
        if version
            .content
            .participants
            .iter()
            .any(|p| !available.contains(p))
            || (version.content.source_version.is_none()
                && version.content.participants != available)
        {
            return Err(invalid(
                "research Version content disagrees with its complete or projected identity",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Hold shared CAS lifecycle before writer acquisition when recovery must read
/// materialized research. Cleanup instead supplies its already-exclusive guard.
pub(crate) fn read_objects_before_writer(
    root: &Path,
) -> Result<Option<crate::graph_object_store::GraphObjectReadLease>, GfError> {
    if root
        .join(crate::graph_object_store::GRAPH_OBJECTS_DIR)
        .exists()
    {
        crate::graph_object_store::begin_graph_object_read(root).map(Some)
    } else {
        Ok(None)
    }
}
