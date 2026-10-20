//! Bounded, versioned research lineage document (`graphforge-research-lineage/2`).
//!
//! Lineage exposes published Versions, Branch heads, Fork origin citations, and
//! frozen Proposal projections without graph payload I/O. Transport locations
//! live only in the manifest `objects` inventory.

use crate::credit::{CreditSignature, ROLE_AUTHOR, ROLE_COMMITTER, SummaryPerson, derive_people};
use crate::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, DiscoveryVersionDetails,
    DiscoveryVersionSubject, PORTABLE_V2_FORMAT, PortablePackageReference, ProtocolCapability,
    ProtocolRequirement, ProtocolVersion, RepositoryIdentity, Sha256Digest, canonical_json,
    check_string, format_major, limit, parse_unique_json, sha256_digest, validate_extensions,
    validate_ref_name, validate_semantics,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Research lineage document format emitted and accepted by this release.
pub const RESEARCH_LINEAGE_FORMAT: &str = "graphforge-research-lineage/2";
/// Media type of an immutable research lineage object selected by discovery.
pub const RESEARCH_LINEAGE_MEDIA_TYPE: &str = "application/vnd.graphforge.research-lineage+json";
/// Required native commit-history semantics at major version 2.
pub const RESEARCH_LINEAGE_CAPABILITY: &str = "research-lineage";
const RESEARCH_LINEAGE_FORMAT_NAME: &str = "graphforge-research-lineage";
const VERSION_KINDS: [&str; 2] = ["complete", "projection"];

/// Fork origin citation for an independent repository identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageForkOrigin {
    /// Original Project repository identity.
    pub origin_repository: RepositoryIdentity,
    /// Original Project research authority UUID.
    pub origin_project_uuid: String,
    /// Source Version UUID from the origin Project.
    pub origin_version_uuid: String,
    /// Native immutable identity commitment for `origin_version_uuid`.
    pub origin_version_identity: Sha256Digest,
    /// Author of the cited origin Version, when it records one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_author: Option<CreditSignature>,
    /// Committer of the cited origin Version, when it records one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_committer: Option<CreditSignature>,
}

/// Immutable Branch genealogy and creation selection commitment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageBranch {
    /// Stable Branch context UUID.
    pub branch_uuid: String,
    /// Discovery ref name selecting this Branch head.
    pub ref_name: String,
    /// Ultimate Project authority UUID for this Branch.
    pub project_uuid: String,
    /// Current head Version UUID at this repository snapshot.
    pub head_version_uuid: String,
    /// Immediate parent Branch UUID, or absent for Project research.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_branch_uuid: Option<String>,
    /// Exact immediate origin Version UUID.
    pub origin_version_uuid: String,
    /// Selected initial Branch base Version UUID.
    pub base_version_uuid: String,
    /// Immutable Slice creation selection commitment (ADR 0040).
    pub selection_sha256: Sha256Digest,
    /// Human Branch label.
    pub label: String,
}

/// Identity-only boundary for a transient native Project capture at Branch creation.
/// No Version record, authorship, payload, or Branch creation authority is inferred.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageBoundaryAnchor {
    /// Transient Project capture UUID, not a missing intermediate Branch commit.
    pub version_uuid: String,
    /// Permanent native identity commitment.
    pub identity_digest: Sha256Digest,
    /// Native root Branch whose exact origin is this Project capture.
    pub origin_branch_uuid: String,
}

/// One published immutable research Version or distinct projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageVersion {
    /// Immutable research Version UUID.
    pub version_uuid: String,
    /// Ordered immutable parents, first parent before merge parents.
    pub parents: Vec<String>,
    /// Native identity commitment digest for this Version.
    pub identity_digest: Sha256Digest,
    /// `complete` for a retained Version, `projection` for a distinct subset.
    pub kind: String,
    /// Branch context UUID owning this Version.
    pub branch_uuid: String,
    /// Source Version UUID when `kind` is `projection`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version_uuid: Option<String>,
    /// Who wrote the change. Published credit is public and permanent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<CreditSignature>,
    /// Who recorded the Version. Published credit is public and permanent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committer: Option<CreditSignature>,
    /// Optional portable package carrying this Version or projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package: Option<PortablePackageReference>,
}

/// One published frozen Proposal payload as a distinct projection package.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageProposal {
    /// Stable Proposal UUID.
    pub proposal_uuid: String,
    /// Source Branch UUID.
    pub source_branch_uuid: String,
    /// Source Version UUID at submission.
    pub source_version_uuid: String,
    /// Distinct projection Version UUID for the Proposal payload.
    pub payload_version_uuid: String,
    /// Portable package for the frozen projection (never a complete Version).
    pub package: PortablePackageReference,
}

/// Validated research lineage document.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchLineage {
    /// Contract identifier; must equal [`RESEARCH_LINEAGE_FORMAT`].
    pub format: String,
    /// Protocol reader/writer version.
    pub version: ProtocolVersion,
    /// Repository the lineage describes.
    pub repository: RepositoryIdentity,
    /// Immutable repository version the lineage describes.
    pub immutable_version: Sha256Digest,
    /// Research Project authority UUID for this repository.
    pub project_uuid: String,
    /// Required semantics; only `research-lineage@2` is understood.
    pub requirements: Vec<ProtocolRequirement>,
    /// Optional advertised semantics.
    pub capabilities: Vec<ProtocolCapability>,
    /// Fork origin citation, when this repository is a Fork.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<LineageForkOrigin>,
    /// Branches in strictly ascending `branch_uuid` order.
    pub branches: Vec<LineageBranch>,
    /// Identity-only native Project-origin boundaries, sorted by Version UUID.
    pub boundary_anchors: Vec<LineageBoundaryAnchor>,
    /// Versions in strictly ascending `version_uuid` order.
    pub versions: Vec<LineageVersion>,
    /// Published Proposals in strictly ascending `proposal_uuid` order.
    pub proposals: Vec<LineageProposal>,
    /// Explicit optional extension values, preserved canonically by readers.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Value>,
}

impl ResearchLineage {
    /// The Project people list derived from this lineage's credited Versions:
    /// every distinct author or committer with the roles held, in canonical order.
    /// Fork origin credit cites another Project's Version and is not included.
    #[must_use]
    pub fn people(&self) -> Vec<SummaryPerson> {
        derive_people(self.versions.iter().flat_map(|version| {
            version
                .author
                .iter()
                .map(|signature| (signature, ROLE_AUTHOR))
                .chain(
                    version
                        .committer
                        .iter()
                        .map(|signature| (signature, ROLE_COMMITTER)),
                )
        }))
    }

    /// Parse and fully validate untrusted JSON without performing object I/O.
    pub fn from_json(bytes: &[u8], limits: DiscoveryLimits) -> Result<Self, DiscoveryError> {
        if bytes.len() > limits.max_response_bytes {
            return Err(limit("response"));
        }
        if bytes.len() > limits.max_lineage_bytes {
            return Err(limit("lineage"));
        }
        let header: Value = parse_unique_json(bytes)?;
        if header.get("format").and_then(Value::as_str) != Some(RESEARCH_LINEAGE_FORMAT) {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::UnsupportedFuture,
                Some("format"),
                "research lineage format is unsupported",
            ));
        }
        let lineage: Self = serde_json::from_value(header).map_err(|_| {
            DiscoveryError::new(
                DiscoveryErrorCode::MalformedResponse,
                None,
                "research lineage fields are invalid",
            )
        })?;
        lineage.validate(limits)?;
        Ok(lineage)
    }

    /// Validate lineage invariants. Unknown required semantics fail before content.
    #[allow(clippy::too_many_lines)]
    pub fn validate(&self, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
        if self.format != RESEARCH_LINEAGE_FORMAT {
            let error = DiscoveryError::new(
                DiscoveryErrorCode::UnsupportedFuture,
                Some("format"),
                "research lineage format is unsupported",
            );
            return Err(
                match format_major(&self.format, RESEARCH_LINEAGE_FORMAT_NAME) {
                    Some(requested_major) => error.with_version(DiscoveryVersionDetails {
                        subject: DiscoveryVersionSubject::ResearchLineage,
                        supported_major: Some(2),
                        requested_major,
                    }),
                    None => error,
                },
            );
        }
        self.version.validate()?;
        // Required semantics are negotiated before any other content is read.
        validate_semantics(
            &self.requirements,
            &self.capabilities,
            RESEARCH_LINEAGE_CAPABILITY,
            limits,
        )?;
        if !self
            .requirements
            .iter()
            .any(|r| r.capability == RESEARCH_LINEAGE_CAPABILITY && r.major == 2)
        {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::UnsupportedFuture,
                Some("requirements"),
                "research lineage 2 is required",
            ));
        }
        self.repository.validate()?;
        self.immutable_version.validate()?;
        check_uuid(&self.project_uuid, "project_uuid", limits)?;
        validate_extensions(&self.extensions, limits)?;
        if let Some(fork) = &self.fork {
            fork.origin_repository.validate()?;
            check_uuid(
                &fork.origin_project_uuid,
                "fork.origin_project_uuid",
                limits,
            )?;
            check_uuid(
                &fork.origin_version_uuid,
                "fork.origin_version_uuid",
                limits,
            )?;
            fork.origin_version_identity.validate()?;
            for signature in [&fork.origin_author, &fork.origin_committer]
                .into_iter()
                .flatten()
            {
                signature.validate("fork.origin_credit")?;
            }
        }
        if self.branches.len() > limits.max_lineage_entries {
            return Err(limit("branches"));
        }
        if self.boundary_anchors.len() > limits.max_lineage_entries {
            return Err(limit("boundary_anchors"));
        }
        if self.versions.len() > limits.max_lineage_entries {
            return Err(limit("versions"));
        }
        if self.proposals.len() > limits.max_lineage_entries {
            return Err(limit("proposals"));
        }
        let mut prior = None;
        let mut branch_refs = BTreeSet::new();
        let mut branch_uuids = BTreeSet::new();
        for branch in &self.branches {
            check_uuid(&branch.branch_uuid, "branches.branch_uuid", limits)?;
            validate_ref_name(&branch.ref_name, limits)?;
            check_uuid(&branch.project_uuid, "branches.project_uuid", limits)?;
            check_uuid(
                &branch.head_version_uuid,
                "branches.head_version_uuid",
                limits,
            )?;
            if let Some(parent) = &branch.parent_branch_uuid {
                check_uuid(parent, "branches.parent_branch_uuid", limits)?;
            }
            check_uuid(
                &branch.origin_version_uuid,
                "branches.origin_version_uuid",
                limits,
            )?;
            check_uuid(
                &branch.base_version_uuid,
                "branches.base_version_uuid",
                limits,
            )?;
            branch.selection_sha256.validate()?;
            check_string(&branch.label, "branches.label", limits)?;
            if prior.is_some_and(|name| name >= branch.branch_uuid.as_str()) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::Duplicate,
                    Some("branches"),
                    "entries are duplicated or not canonically ordered",
                ));
            }
            prior = Some(branch.branch_uuid.as_str());
            branch_uuids.insert(branch.branch_uuid.as_str());
            if !branch_refs.insert(branch.ref_name.as_str()) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::Duplicate,
                    Some("branches.ref_name"),
                    "branch ref names are duplicated",
                ));
            }
        }
        let mut prior = None;
        let mut version_ids = BTreeMap::new();
        for version in &self.versions {
            check_uuid(&version.version_uuid, "versions.version_uuid", limits)?;
            version.identity_digest.validate()?;
            if !VERSION_KINDS.contains(&version.kind.as_str()) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::MalformedResponse,
                    Some("versions.kind"),
                    "research version kind is unknown",
                ));
            }
            check_uuid(&version.branch_uuid, "versions.branch_uuid", limits)?;
            for signature in [&version.author, &version.committer].into_iter().flatten() {
                signature.validate("versions.credit")?;
            }
            if version.kind == "projection" {
                let Some(source) = &version.source_version_uuid else {
                    return Err(DiscoveryError::new(
                        DiscoveryErrorCode::MalformedResponse,
                        Some("versions.source_version_uuid"),
                        "projection requires a source Version",
                    ));
                };
                check_uuid(source, "versions.source_version_uuid", limits)?;
                if *source == version.version_uuid {
                    return Err(DiscoveryError::new(
                        DiscoveryErrorCode::MalformedResponse,
                        Some("versions.source_version_uuid"),
                        "projection must not cite itself as its source Version",
                    ));
                }
            } else if version.source_version_uuid.is_some() {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::MalformedResponse,
                    Some("versions.source_version_uuid"),
                    "complete Version must not name a projection source",
                ));
            }
            if let Some(package) = &version.package {
                validate_package_reference(package, "versions.package", limits)?;
            }
            if prior.is_some_and(|name| name >= version.version_uuid.as_str()) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::Duplicate,
                    Some("versions"),
                    "entries are duplicated or not canonically ordered",
                ));
            }
            prior = Some(version.version_uuid.as_str());
            version_ids.insert(version.version_uuid.as_str(), version);
        }
        let mut anchors = BTreeMap::new();
        let mut prior_anchor = None;
        for anchor in &self.boundary_anchors {
            check_uuid(
                &anchor.version_uuid,
                "boundary_anchors.version_uuid",
                limits,
            )?;
            check_uuid(
                &anchor.origin_branch_uuid,
                "boundary_anchors.origin_branch_uuid",
                limits,
            )?;
            anchor.identity_digest.validate()?;
            if version_ids.contains_key(anchor.version_uuid.as_str())
                || prior_anchor.is_some_and(|old: &str| old >= anchor.version_uuid.as_str())
            {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::IntegrityFailure,
                    Some("boundary_anchors"),
                    "boundary anchor overlaps native records or is unordered",
                ));
            }
            if let Some(branch) = self
                .branches
                .iter()
                .find(|branch| branch.branch_uuid == anchor.origin_branch_uuid)
                && (branch.parent_branch_uuid.is_some()
                    || branch.origin_version_uuid != anchor.version_uuid)
            {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::IntegrityFailure,
                    Some("boundary_anchors"),
                    "boundary anchor differs from Branch origin",
                ));
            }
            prior_anchor = Some(anchor.version_uuid.as_str());
            anchors.insert(anchor.version_uuid.as_str(), anchor);
        }
        if anchors.keys().any(|id| {
            !self
                .versions
                .iter()
                .any(|v| v.parents.iter().any(|parent| parent == id))
        }) {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::IntegrityFailure,
                Some("boundary_anchors"),
                "Project-origin anchor is outside the parent closure",
            ));
        }
        for version in &self.versions {
            if version.parents.len() > 8 {
                return Err(limit("versions.parents"));
            }
            let mut seen = BTreeSet::new();
            for parent in &version.parents {
                check_uuid(parent, "versions.parents", limits)?;
                if parent == &version.version_uuid
                    || !seen.insert(parent)
                    || (!version_ids.contains_key(parent.as_str())
                        && !anchors.contains_key(parent.as_str()))
                {
                    return Err(DiscoveryError::new(
                        DiscoveryErrorCode::IntegrityFailure,
                        Some("versions.parents"),
                        "Version parent is duplicated, unknown or itself",
                    ));
                }
            }
        }
        validate_parent_graph(&version_ids)?;
        let mut prior = None;
        for proposal in &self.proposals {
            check_uuid(&proposal.proposal_uuid, "proposals.proposal_uuid", limits)?;
            check_uuid(
                &proposal.source_branch_uuid,
                "proposals.source_branch_uuid",
                limits,
            )?;
            check_uuid(
                &proposal.source_version_uuid,
                "proposals.source_version_uuid",
                limits,
            )?;
            check_uuid(
                &proposal.payload_version_uuid,
                "proposals.payload_version_uuid",
                limits,
            )?;
            validate_package_reference(&proposal.package, "proposals.package", limits)?;
            if prior.is_some_and(|name| name >= proposal.proposal_uuid.as_str()) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::Duplicate,
                    Some("proposals"),
                    "entries are duplicated or not canonically ordered",
                ));
            }
            prior = Some(proposal.proposal_uuid.as_str());
            let Some(payload) = version_ids.get(proposal.payload_version_uuid.as_str()) else {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::MalformedResponse,
                    Some("proposals.payload_version_uuid"),
                    "proposal payload Version is absent from versions",
                ));
            };
            if payload.kind != "projection" {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::MalformedResponse,
                    Some("proposals.payload_version_uuid"),
                    "proposal payload is not a projection Version",
                ));
            }
            if payload.source_version_uuid.as_deref() != Some(proposal.source_version_uuid.as_str())
            {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::MalformedResponse,
                    Some("proposals.source_version_uuid"),
                    "proposal source Version disagrees with its payload projection",
                ));
            }
            if payload
                .package
                .as_ref()
                .is_some_and(|package| *package != proposal.package)
            {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::MalformedResponse,
                    Some("proposals.package"),
                    "proposal package disagrees with its payload Version package",
                ));
            }
            if !branch_uuids.contains(proposal.source_branch_uuid.as_str()) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::MalformedResponse,
                    Some("proposals.source_branch_uuid"),
                    "proposal source Branch is absent from branches",
                ));
            }
        }
        for branch in &self.branches {
            if !version_ids.contains_key(branch.head_version_uuid.as_str()) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::MalformedResponse,
                    Some("branches.head_version_uuid"),
                    "branch head Version is absent from versions",
                ));
            }
        }
        Ok(())
    }

    /// Encode deterministic compact JSON with recursively sorted keys.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, DiscoveryError> {
        self.validate(DiscoveryLimits::default())?;
        canonical_json(self)
    }

    /// Compute SHA-256 over [`Self::to_canonical_json`].
    pub fn canonical_digest(&self) -> Result<Sha256Digest, DiscoveryError> {
        Ok(sha256_digest(&self.to_canonical_json()?))
    }

    /// Prove advanced refs preserve their Branch identity and immutable ancestry.
    #[must_use]
    pub fn fast_forwards(&self, previous: &Self, refs: &[String]) -> bool {
        self.project_uuid == previous.project_uuid
            && previous.boundary_anchors.iter().all(|old| {
                self.boundary_anchors.iter().any(|new| new == old)
                    || self.version(&old.version_uuid).is_some_and(|record| {
                        record.identity_digest == old.identity_digest && record.parents.is_empty()
                    })
            })
            && previous.versions.iter().all(|old| {
                self.version(&old.version_uuid).is_some_and(|new| {
                    new.identity_digest == old.identity_digest
                        && new.parents == old.parents
                        && new.branch_uuid == old.branch_uuid
                })
            })
            && refs.iter().all(|name| {
                let old = previous
                    .branches
                    .iter()
                    .find(|branch| &branch.ref_name == name);
                old.is_none_or(|old| {
                    self.branches
                        .iter()
                        .find(|branch| &branch.ref_name == name)
                        .is_some_and(|new| {
                            new.branch_uuid == old.branch_uuid
                                && {
                                    let mut binding = new.clone();
                                    binding.head_version_uuid.clone_from(&old.head_version_uuid);
                                    binding == *old
                                }
                                && self
                                    .descends_from(&new.head_version_uuid, &old.head_version_uuid)
                        })
                })
            })
    }

    /// Test descent through the validated bounded native parent graph.
    #[must_use]
    pub fn descends_from(&self, head: &str, ancestor: &str) -> bool {
        let mut pending = vec![head];
        let mut seen = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if id == ancestor {
                return true;
            }
            if seen.insert(id)
                && let Some(record) = self.version(id)
            {
                pending.extend(record.parents.iter().map(String::as_str));
            }
        }
        false
    }

    /// Return one published Version, including metadata-only ancestors.
    #[must_use]
    pub fn version(&self, version_uuid: &str) -> Option<&LineageVersion> {
        self.versions
            .iter()
            .find(|version| version.version_uuid == version_uuid)
    }
}

/// Reference to a research lineage document carried as a transport object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchLineageReference {
    /// Must be [`RESEARCH_LINEAGE_FORMAT`] in discovery v1.1.
    pub format: String,
    /// Canonical digest of the lineage document.
    pub lineage_digest: Sha256Digest,
    /// Transport object digest selecting exactly one entry from `objects`.
    pub object_digest: Sha256Digest,
}

pub(super) fn validate_package_reference(
    package: &PortablePackageReference,
    field: &'static str,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    if package.format != PORTABLE_V2_FORMAT {
        let error = DiscoveryError::new(
            DiscoveryErrorCode::UnsupportedFuture,
            Some(field),
            "portable package format is unsupported",
        );
        return Err(match format_major(&package.format, "graphforge-project") {
            Some(requested_major) => error.with_version(DiscoveryVersionDetails {
                subject: DiscoveryVersionSubject::PortablePackage,
                supported_major: Some(2),
                requested_major,
            }),
            None => error,
        });
    }
    package.package_digest.validate()?;
    package.object_digest.validate()?;
    let _ = limits;
    Ok(())
}

/// Expand each record at most once, independent of ancestry depth.
fn validate_parent_graph(
    versions: &BTreeMap<&str, &LineageVersion>,
) -> Result<usize, DiscoveryError> {
    let mut done = BTreeSet::new();
    let mut expanded = 0;
    for start in versions.keys() {
        let mut visiting = BTreeSet::new();
        let mut stack = vec![(*start, false)];
        while let Some((id, exiting)) = stack.pop() {
            if exiting {
                visiting.remove(id);
                done.insert(id);
                continue;
            }
            if done.contains(id) {
                continue;
            }
            if !visiting.insert(id) {
                return Err(DiscoveryError::new(
                    DiscoveryErrorCode::IntegrityFailure,
                    Some("versions.parents"),
                    "Version ancestry is cyclic",
                ));
            }
            stack.push((id, true));
            if let Some(record) = versions.get(id) {
                expanded += 1;
                stack.extend(record.parents.iter().map(|parent| (parent.as_str(), false)));
            }
        }
    }
    Ok(expanded)
}

#[cfg(test)]
mod ancestry_tests;

fn check_uuid(
    value: &str,
    field: &'static str,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    check_string(value, field, limits)?;
    let bytes = value.as_bytes();
    if bytes.len() != 36
        || bytes[8] != b'-'
        || bytes[13] != b'-'
        || bytes[18] != b'-'
        || bytes[23] != b'-'
        || !bytes.iter().enumerate().all(|(index, byte)| {
            matches!(byte, b'0'..=b'9' | b'a'..=b'f' | b'-')
                && (*byte != b'-' || [8, 13, 18, 23].contains(&index))
        })
    {
        return Err(DiscoveryError::new(
            DiscoveryErrorCode::MalformedResponse,
            Some(field),
            "UUID is invalid",
        ));
    }
    Ok(())
}
