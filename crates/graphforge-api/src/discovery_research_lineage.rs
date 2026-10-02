//! Build a discovery research lineage document from native registry state.
//!
//! Branches, Version kinds and identities, Proposal records, and the Fork origin
//! citation all come from the authoritative research registry, without graph
//! payload I/O. The publisher supplies only hosting facts the registry cannot
//! know: discovery ref names for Branch heads, the portable package published
//! for each Version, which Proposals are published, and the origin repository
//! of a Fork.

use crate::{CancellationToken, GfError, GraphForge};
use graphforge_discovery::{
    DiscoveryError, DiscoveryLimits, LineageBranch, LineageForkOrigin, LineageProposal,
    LineageVersion, PortablePackageReference, ProtocolRequirement, ProtocolVersion,
    RESEARCH_LINEAGE_CAPABILITY, RESEARCH_LINEAGE_FORMAT, RepositoryIdentity, ResearchLineage,
    Sha256Digest,
};
use graphforge_storage::research_versions::ResearchRegistry;
use std::collections::{BTreeMap, BTreeSet};
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
    /// Hub repository of the origin Project, required exactly when the registry
    /// records that this Project was created by a Fork. The origin Project,
    /// Version, and identity commitment come from the registry Fork record.
    pub fork_origin_repository: Option<RepositoryIdentity>,
    /// Registry Proposals to publish. Each payload projection Version must be
    /// published in `version_packages`; its package is the Proposal package.
    pub published_proposals: BTreeSet<Uuid>,
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
        if crate::research_claims::authority::project_uuid(&current)? != request.project_uuid {
            return Err(GfError::Validation(
                "project_uuid is not this Project's research authority".into(),
            ));
        }
        let registry = graphforge_storage::research_versions::read_research_registry(&current)?;
        build_research_lineage_from_registry(request, &registry, DiscoveryLimits::default())
    }
}

/// Derive lineage from an explicit registry snapshot and publisher package map.
#[allow(clippy::too_many_lines)]
pub fn build_research_lineage_from_registry(
    request: &BuildResearchLineageRequest,
    registry: &ResearchRegistry,
    limits: DiscoveryLimits,
) -> Result<ResearchLineage, GfError> {
    if request.project_uuid.is_nil() {
        return Err(GfError::Validation("project_uuid is invalid".into()));
    }
    let mut branches = Vec::new();
    for (branch_uuid, record) in &registry.branches {
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
    if let Some(unknown) = request
        .branch_ref_names
        .keys()
        .find(|branch_uuid| !registry.branches.contains_key(branch_uuid))
    {
        return Err(GfError::Validation(format!(
            "branch {unknown} is not a registry Branch"
        )));
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
        let source_version = projection_source(registry, version)?;
        versions.push(LineageVersion {
            version_uuid: version_uuid.to_string(),
            identity_digest: identity_digest(identity)
                .map_err(|error| map_discovery_error(&error))?,
            kind: if source_version.is_some() {
                "projection"
            } else {
                "complete"
            }
            .into(),
            branch_uuid: version.context_uuid.to_string(),
            source_version_uuid: source_version.map(|id| id.to_string()),
            package: Some(package.clone()),
        });
    }
    versions.sort_by(|left, right| left.version_uuid.cmp(&right.version_uuid));

    let mut proposals = Vec::new();
    for proposal_uuid in &request.published_proposals {
        let record = registry
            .proposals
            .proposals
            .get(proposal_uuid)
            .ok_or_else(|| GfError::Validation("proposal record is missing".into()))?;
        let package = request
            .version_packages
            .get(&record.payload_version_uuid)
            .ok_or_else(|| {
                GfError::Validation("proposal payload Version is not published".into())
            })?;
        proposals.push(LineageProposal {
            proposal_uuid: proposal_uuid.to_string(),
            source_branch_uuid: record.source_branch_uuid.to_string(),
            source_version_uuid: record.source_version_uuid.to_string(),
            payload_version_uuid: record.payload_version_uuid.to_string(),
            package: package.clone(),
        });
    }

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
        fork: fork_origin(request, registry)?,
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

/// Return the source Version when a registry Version is a projection.
///
/// The registry has no single projection flag, so this is conservative: a
/// Version is complete only when it is Project research (no source Version) or a
/// Branch state (its context is an active or imported Branch) that is not a
/// repacked graph projection or an imported projection selection. Everything
/// else that names a source Version, such as a frozen Proposal payload or a
/// selected-participant projection, is a projection of that source.
pub(crate) fn projection_source(
    registry: &ResearchRegistry,
    version: &graphforge_storage::research_versions::ResearchVersionRecord,
) -> Result<Option<Uuid>, GfError> {
    let Some(source) = version.content.source_version else {
        if version.content.graph_projection.is_some() {
            return Err(GfError::Validation(
                "graph projection is missing its source Version".into(),
            ));
        }
        return Ok(None);
    };
    let imported_projection = registry.interchange.values().any(|archive| {
        archive.selected_version_uuid == version.version_uuid
            && matches!(
                archive.selection,
                graphforge_storage::research_versions::ResearchInterchangeSelection::Projection { .. }
            )
    });
    let branch_state = registry.historical_branch(version.context_uuid).is_some();
    if version.content.graph_projection.is_some() || imported_projection || !branch_state {
        Ok(Some(source))
    } else {
        Ok(None)
    }
}

/// Resolve the Fork origin citation from the registry's Fork record.
fn fork_origin(
    request: &BuildResearchLineageRequest,
    registry: &ResearchRegistry,
) -> Result<Option<LineageForkOrigin>, GfError> {
    let mut forks = registry.interchange.values().filter(|archive| {
        archive.fork.is_some() && archive.fork_project_uuid == Some(request.project_uuid)
    });
    let archive = forks.next();
    if forks.next().is_some() {
        return Err(GfError::Validation(
            "registry records more than one Fork creation for this Project".into(),
        ));
    }
    match (archive, &request.fork_origin_repository) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(GfError::Validation(
            "fork origin repository given for a Project that is not a Fork".into(),
        )),
        (Some(_), None) => Err(GfError::Validation(
            "a Fork must publish its origin repository".into(),
        )),
        (Some(archive), Some(origin_repository)) => {
            let identity = archive
                .identities
                .get(&archive.selected_version_uuid)
                .ok_or_else(|| GfError::Validation("fork origin identity is missing".into()))?;
            Ok(Some(LineageForkOrigin {
                origin_repository: origin_repository.clone(),
                origin_project_uuid: archive.source_project_uuid.to_string(),
                origin_version_uuid: archive.selected_version_uuid.to_string(),
                origin_version_identity: identity_digest(identity)
                    .map_err(|error| map_discovery_error(&error))?,
            }))
        }
    }
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

    struct History {
        graph: GraphForge,
        branch_uuid: Uuid,
        base: Uuid,
        head: Uuid,
    }

    fn history() -> History {
        let mut graph = GraphForge::new(None).unwrap();
        let cancellation = CancellationToken::new();
        graph.execute("CREATE (:Item {x:0})").unwrap();
        let branch_uuid = Uuid::now_v7();
        let base = Uuid::now_v7();
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
                        origin_version_uuid: Uuid::now_v7(),
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
        History {
            graph,
            branch_uuid,
            base,
            head,
        }
    }

    fn marker(marker: char) -> String {
        format!("sha256:{}", marker.to_string().repeat(64))
    }

    fn request(history: &History) -> BuildResearchLineageRequest {
        let project_uuid = crate::research_claims::authority::project_uuid(
            &history.graph.generation_for_read().unwrap(),
        )
        .unwrap();
        BuildResearchLineageRequest {
            repository: RepositoryIdentity {
                owner: "openalex".into(),
                repository: "demo".into(),
            },
            immutable_version: Sha256Digest(marker('a')),
            project_uuid,
            branch_ref_names: BTreeMap::from([(history.branch_uuid, "main".into())]),
            version_packages: BTreeMap::from([
                (history.head, package_ref(&marker('b'), &marker('c'))),
                (history.base, package_ref(&marker('d'), &marker('e'))),
            ]),
            fork_origin_repository: None,
            published_proposals: BTreeSet::new(),
        }
    }

    fn registry(history: &History) -> ResearchRegistry {
        graphforge_storage::research_versions::read_research_registry(
            &history.graph.generation_for_read().unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn build_lineage_advertises_branch_heads_and_complete_versions() {
        let history = history();
        let cancellation = CancellationToken::new();
        let request = request(&history);
        let registry = registry(&history);
        let lineage =
            build_research_lineage_from_registry(&request, &registry, DiscoveryLimits::default())
                .unwrap();
        assert_eq!(
            history
                .graph
                .build_research_lineage_for_discovery(&request, &cancellation)
                .unwrap(),
            lineage
        );
        assert_eq!(lineage.branches.len(), 1);
        assert_eq!(
            lineage.branches[0].head_version_uuid,
            history.head.to_string()
        );
        assert_eq!(lineage.branches[0].ref_name, "main");
        for version in [history.head, history.base] {
            let entry = lineage.version(&version.to_string()).expect("listed");
            assert_eq!(entry.kind, "complete");
            assert_eq!(entry.source_version_uuid, None);
            assert_eq!(
                entry.identity_digest.0,
                format!(
                    "sha256:{}",
                    registry.identities[&version]
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                )
            );
        }
        assert!(lineage.fork.is_none());

        // The Project authority comes from the Project, never from the caller.
        let mut wrong = request.clone();
        wrong.project_uuid = Uuid::now_v7();
        assert!(
            history
                .graph
                .build_research_lineage_for_discovery(&wrong, &cancellation)
                .is_err()
        );
        // A Fork citation can only come from a registry Fork record.
        let mut not_a_fork = request;
        not_a_fork.fork_origin_repository = Some(RepositoryIdentity {
            owner: "curate".into(),
            repository: "origin".into(),
        });
        assert!(
            build_research_lineage_from_registry(
                &not_a_fork,
                &registry,
                DiscoveryLimits::default()
            )
            .is_err()
        );
    }

    #[test]
    fn build_lineage_reports_registry_projection_as_projection_of_its_source() {
        let history = history();
        let mut registry = registry(&history);
        // A projection outside any Branch that the registry marks only by its
        // source Version (`graph_projection` stays absent), as
        // `register_graphless` and frozen Proposal payloads do. The head it
        // derives from also names a source Version, but is a Branch state.
        let projection = Uuid::now_v7();
        let mut record = registry.versions[&history.head].clone();
        record.version_uuid = projection;
        record.context_uuid = Uuid::now_v7();
        record.content.source_version = Some(history.head);
        assert!(record.content.graph_projection.is_none());
        let identity = record.identity_sha256().unwrap();
        assert_ne!(identity, registry.identities[&history.head]);
        registry.versions.insert(projection, record);
        registry.identities.insert(projection, identity);
        let mut request = request(&history);
        request
            .version_packages
            .insert(projection, package_ref(&marker('1'), &marker('2')));
        let lineage =
            build_research_lineage_from_registry(&request, &registry, DiscoveryLimits::default())
                .unwrap();
        let entry = lineage.version(&projection.to_string()).expect("listed");
        assert_eq!(entry.kind, "projection");
        assert_eq!(
            entry.source_version_uuid.as_deref(),
            Some(history.head.to_string().as_str())
        );
        let source = lineage.version(&history.head.to_string()).expect("listed");
        assert!(
            registry.versions[&history.head]
                .content
                .source_version
                .is_some()
        );
        assert_eq!(source.kind, "complete");
        assert_ne!(entry.identity_digest, source.identity_digest);
        assert_ne!(entry.package, source.package);
    }
}
