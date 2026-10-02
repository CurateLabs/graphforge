//! Build a discovery research lineage document from native registry state.
//!
//! Publishers attach per-Version portable package references after export and
//! upload; this module derives Branches, Versions, and optional Proposals from
//! the authoritative research registry without graph payload I/O.

use crate::{CancellationToken, GfError, GraphForge};
use graphforge_discovery::{
    DiscoveryError, DiscoveryLimits, LineageBranch, LineageForkOrigin, LineageProposal,
    LineageVersion, PortablePackageReference, ProtocolRequirement, ProtocolVersion,
    RESEARCH_LINEAGE_CAPABILITY, RESEARCH_LINEAGE_FORMAT, RepositoryIdentity, ResearchLineage,
    Sha256Digest,
};
use graphforge_storage::research_versions::ResearchRegistry;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use uuid::Uuid;

/// Inputs for one lineage document bound to a repository snapshot.
#[derive(Debug, Clone)]
pub struct BuildResearchLineageRequest {
    /// Hub repository identity for this snapshot.
    pub repository: RepositoryIdentity,
    /// Immutable repository version digest for this discovery snapshot.
    pub immutable_version: Sha256Digest,
    /// Research Project authority UUID for this repository.
    pub project_uuid: Uuid,
    /// Discovery ref name for each published Branch head.
    pub branch_ref_names: BTreeMap<Uuid, String>,
    /// Portable package reference for each published research Version.
    pub version_packages: BTreeMap<Uuid, PortablePackageReference>,
    /// Fork origin citation when this repository is a Fork.
    pub fork: Option<LineageForkOrigin>,
    /// Published Proposal projections carried as separate packages.
    pub proposals: Vec<LineageProposal>,
}

impl GraphForge {
    /// Derive a validated [`ResearchLineage`] from the current registry view.
    pub fn build_research_lineage_for_discovery(
        &self,
        request: &BuildResearchLineageRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchLineage, GfError> {
        cancellation.checkpoint()?;
        let current = self.generation_for_read()?;
        let registry = graphforge_storage::research_versions::read_research_registry(&current)?;
        build_research_lineage_from_registry(request, &registry, DiscoveryLimits::default())
    }
}

/// Derive lineage from an explicit registry snapshot and publisher package map.
pub fn build_research_lineage_from_registry(
    request: &BuildResearchLineageRequest,
    registry: &ResearchRegistry,
    limits: DiscoveryLimits,
) -> Result<ResearchLineage, GfError> {
    if request.project_uuid.is_nil() {
        return Err(GfError::Validation("project_uuid is invalid".into()));
    }
    let mut branches = Vec::new();
    for branch_uuid in registry.branches.keys() {
        let record = registry
            .branches
            .get(branch_uuid)
            .ok_or_else(|| GfError::Validation("branch record is missing".into()))?;
        let ref_name = request.branch_ref_names.get(branch_uuid).ok_or_else(|| {
            GfError::Validation("branch ref name is absent from discovery publication".into())
        })?;
        let head_version_uuid = registry
            .heads
            .get(branch_uuid)
            .ok_or_else(|| GfError::Validation("branch head is missing".into()))?;
        branches.push(LineageBranch {
            branch_uuid: branch_uuid.to_string(),
            ref_name: ref_name.clone(),
            project_uuid: record.project_uuid.to_string(),
            head_version_uuid: head_version_uuid.to_string(),
            parent_branch_uuid: record.parent_branch_uuid.map(|id| id.to_string()),
            origin_version_uuid: record.origin_version_uuid.to_string(),
            base_version_uuid: record.base_version_uuid.to_string(),
            selection_sha256: identity_digest(&record.selection_sha256)
                .map_err(|error| map_discovery_error(&error))?,
            label: record.label.clone(),
        });
    }
    branches.sort_by(|left, right| left.branch_uuid.cmp(&right.branch_uuid));

    let mut versions = Vec::new();
    for (version_uuid, package) in &request.version_packages {
        let version = registry
            .versions
            .get(version_uuid)
            .ok_or_else(|| GfError::Validation("version record is missing".into()))?;
        let identity = registry
            .identities
            .get(version_uuid)
            .ok_or_else(|| GfError::Validation("version identity is missing".into()))?;
        let is_projection = version.content.graph_projection.is_some();
        let kind = if is_projection {
            "projection"
        } else {
            "complete"
        };
        versions.push(LineageVersion {
            version_uuid: version_uuid.to_string(),
            identity_digest: identity_digest(identity)
                .map_err(|error| map_discovery_error(&error))?,
            kind: kind.into(),
            branch_uuid: version.context_uuid.to_string(),
            source_version_uuid: is_projection
                .then(|| {
                    version
                        .content
                        .source_version
                        .map(|id| id.to_string())
                        .ok_or_else(|| {
                            GfError::Validation(
                                "graph projection is missing its source Version".into(),
                            )
                        })
                })
                .transpose()?,
            package: Some(package.clone()),
        });
    }
    versions.sort_by(|left, right| left.version_uuid.cmp(&right.version_uuid));

    let mut proposals = request.proposals.clone();
    proposals.sort_by(|left, right| left.proposal_uuid.cmp(&right.proposal_uuid));

    let lineage = ResearchLineage {
        format: RESEARCH_LINEAGE_FORMAT.into(),
        version: ProtocolVersion { major: 1, minor: 1 },
        repository: request.repository.clone(),
        immutable_version: request.immutable_version.clone(),
        project_uuid: request.project_uuid.to_string(),
        requirements: vec![ProtocolRequirement {
            capability: RESEARCH_LINEAGE_CAPABILITY.into(),
            major: 1,
        }],
        capabilities: vec![],
        fork: request.fork.clone(),
        branches,
        versions,
        proposals,
        extensions: BTreeMap::new(),
    };
    lineage
        .validate(limits)
        .map_err(|error| map_discovery_error(&error))?;
    Ok(lineage)
}

fn identity_digest(bytes: &[u8; 32]) -> Result<Sha256Digest, DiscoveryError> {
    let hex = bytes
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a string cannot fail");
            output
        });
    let digest = Sha256Digest(format!("sha256:{hex}"));
    digest.validate()?;
    Ok(digest)
}

fn map_discovery_error(error: &DiscoveryError) -> GfError {
    GfError::Validation(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BranchSource, CreateResearchBranchRequest, ExecuteResearchBranchRequest};
    use graphforge_discovery::PORTABLE_V2_FORMAT;
    fn package_ref(package_digest: &str, object_digest: &str) -> PortablePackageReference {
        PortablePackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: Sha256Digest(package_digest.into()),
            object_digest: Sha256Digest(object_digest.into()),
        }
    }

    #[test]
    fn build_lineage_advertises_branch_heads_and_projection_kind() {
        let mut graph = GraphForge::new(None).unwrap();
        let cancellation = CancellationToken::new();
        graph.execute("CREATE (:Item {x:0})").unwrap();
        let branch_uuid = Uuid::now_v7();
        let base = Uuid::now_v7();
        let origin = Uuid::now_v7();
        graph
            .create_research_branch(
                &CreateResearchBranchRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: graph
                        .generation_for_read()
                        .unwrap()
                        .generation_uuid(),
                    branch_uuid,
                    version_uuid: base,
                    source: BranchSource::Current {
                        origin_version_uuid: origin,
                        context_uuid: Uuid::now_v7(),
                    },
                    creator_uuid: Uuid::now_v7(),
                    created_at: 1,
                    label: "main".into(),
                },
                &cancellation,
            )
            .unwrap();
        let generation = graph.generation_for_read().unwrap().generation_uuid();
        let head = Uuid::now_v7();
        graph
            .execute_research_branch(
                &ExecuteResearchBranchRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: generation,
                    branch_uuid,
                    version_uuid: head,
                    created_at: 2,
                    query: "MATCH (n:Item) SET n.x=1".into(),
                },
                &cancellation,
            )
            .unwrap();
        let registry = graphforge_storage::research_versions::read_research_registry(
            &graph.generation_for_read().unwrap(),
        )
        .unwrap();
        let project_uuid =
            crate::research_claims::authority::project_uuid(&graph.generation_for_read().unwrap())
                .unwrap();
        let immutable = Sha256Digest(format!("sha256:{}", "a".repeat(64)));
        let mut branch_ref_names = BTreeMap::new();
        branch_ref_names.insert(branch_uuid, "main".into());
        let mut version_packages = BTreeMap::new();
        version_packages.insert(
            head,
            package_ref(
                &format!("sha256:{}", "b".repeat(64)),
                &format!("sha256:{}", "c".repeat(64)),
            ),
        );
        version_packages.insert(
            base,
            package_ref(
                &format!("sha256:{}", "d".repeat(64)),
                &format!("sha256:{}", "e".repeat(64)),
            ),
        );
        let request = BuildResearchLineageRequest {
            repository: RepositoryIdentity {
                owner: "openalex".into(),
                repository: "demo".into(),
            },
            immutable_version: immutable.clone(),
            project_uuid,
            branch_ref_names,
            version_packages,
            fork: None,
            proposals: vec![],
        };
        let lineage =
            build_research_lineage_from_registry(&request, &registry, DiscoveryLimits::default())
                .unwrap();
        assert_eq!(
            graph
                .build_research_lineage_for_discovery(&request, &cancellation)
                .unwrap(),
            lineage
        );
        assert_eq!(lineage.branches.len(), 1);
        assert_eq!(lineage.branches[0].head_version_uuid, head.to_string());
        assert_eq!(lineage.branches[0].ref_name, "main");
        let complete = lineage.version(&head.to_string()).expect("head is listed");
        assert_eq!(complete.kind, "complete");
        assert_eq!(
            lineage
                .version(&base.to_string())
                .expect("base is listed")
                .kind,
            "complete"
        );
    }
}
