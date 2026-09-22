//! Local Fork research preserves foreign accepted lineage without foreign authority.
use super::*;
use graphforge_storage::research_versions::ResearchProposalDecision::Accept;

#[test]
fn fork_local_branch_exports_mixed_project_acceptance_genealogy() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = GraphForge::new(dir.path().join("source").to_str()).unwrap();
    source.execute("CREATE (:Character {score:0})").unwrap();
    let node_uuid = node(&mut source);
    let old_branch = branch(&mut source);
    let old_version = edit(&mut source, old_branch, "MATCH(n:Character) SET n.score=1");
    let proposal = submit(
        &mut source,
        old_branch,
        old_version,
        node_uuid,
        &["property:score"],
    );
    let review = decision(&source, proposal.proposal_uuid, |_| Accept);
    source
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    let original = source.research_version_retention().unwrap();
    let mapping = original.proposals.accepted.values().next().unwrap().clone();
    let old_project = original.branches[&old_branch].project_uuid;
    let project = Uuid::now_v7();
    let target = dir.path().join("fork");
    source
        .fork_research(
            &ForkResearchRequest {
                operation_uuid: Uuid::now_v7(),
                project_uuid: project,
                version_uuid: old_version,
                projection: None,
                target: target.clone(),
                actor_uuid: Uuid::now_v7(),
                governance: "Independent research".into(),
                adopt_selected_ontology: true,
                metadata: graphforge_storage::WorkspaceResearchMetadata::empty(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut fork = GraphForge::new(target.to_str()).unwrap();
    let local_branch = Uuid::now_v7();
    let local_version = Uuid::now_v7();
    fork.create_research_branch(
        &CreateResearchBranchRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&fork),
            branch_uuid: local_branch,
            version_uuid: local_version,
            source: BranchSource::Version {
                version_uuid: old_version,
            },
            creator_uuid: Uuid::now_v7(),
            created_at: 20,
            label: "Local continuation".into(),
        },
        &CancellationToken::new(),
    )
    .unwrap();
    let local = fork.research_version_retention().unwrap();
    assert_eq!(local.branches[&local_branch].project_uuid, project);
    assert_eq!(local.branches[&local_branch].parent_branch_uuid, None);
    assert!(local.proposals.accepted.is_empty());
    let output = dir.path().join("export");
    fork.export_research(
        &ExportResearchRequest {
            version_uuid: local_version,
            output: output.clone(),
            bundled: false,
            projection: None,
        },
        &CancellationToken::new(),
    )
    .unwrap();
    let imported_root = dir.path().join("reimport");
    GraphForge::import_portable_v2(
        &imported_root,
        &PortableV2ImportRequest {
            input: output,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    let imported = GraphForge::new(imported_root.to_str()).unwrap();
    let registry = imported.research_version_retention().unwrap();
    let archive = &registry.interchange[&local_version];
    assert_eq!(archive.accepted[&mapping.mapping_uuid], mapping);
    assert_eq!(archive.genealogy[&old_branch].project_uuid, old_project);
    assert_eq!(archive.genealogy[&local_branch].project_uuid, project);
    assert_eq!(registry.historical_project(local_version), Some(project));
    let proof = archive.proof_exports[&mapping.proof_version_uuid];
    assert_eq!(registry.historical_project(proof), Some(old_project));
    assert!(registry.branches.is_empty());
    assert!(registry.proposals.accepted.is_empty());
    assert!(registry.heads.is_empty());
    reject_conflicting_citations(&registry, local_version, proof);
}

fn reject_conflicting_citations(
    registry: &graphforge_storage::research_versions::ResearchRegistry,
    selected: Uuid,
    proof: Uuid,
) {
    let original = &registry.interchange[&selected];
    let foreign_project = original.version_projects[&proof];
    let mut reused_fork = original.clone();
    reused_fork.fork_project_uuid = Some(foreign_project);
    reused_fork.fork = Some(graphforge_storage::research_versions::ResearchForkRecord {
        project_uuid: foreign_project,
        operation_uuid: Uuid::now_v7(),
        intent_sha256: [1; 32],
        actor_uuid: Uuid::now_v7(),
        governance: "Cannot reuse a known foreign authority".into(),
        adopt_selected_ontology: true,
    });
    assert_ne!(foreign_project, reused_fork.source_project_uuid);
    assert!(reused_fork.validate().is_err());
    let mut malformed = original.clone();
    malformed.version_projects.remove(&proof);
    assert!(malformed.validate().is_err());
    let mut malformed = original.clone();
    malformed.version_projects.insert(proof, Uuid::now_v7());
    assert!(malformed.validate().is_err());
    // Two individually coherent historical archives cannot assign one immutable
    // Version conflicting Project citations when combined in an owning registry.
    let mut second = original.clone();
    second.selected_version_uuid = proof;
    second.selection =
        graphforge_storage::research_versions::ResearchInterchangeSelection::Complete {
            source_version_uuid: proof,
        };
    second.accepted.clear();
    second.proof_exports.clear();
    second.versions.retain(|id, _| *id == proof);
    second.version_projects.retain(|id, _| *id == proof);
    second.genealogy.clear();
    let changed_project = Uuid::now_v7();
    second.source_project_uuid = changed_project;
    second.version_projects.insert(proof, changed_project);
    second.validate().unwrap();
    let mut mixed = registry.clone();
    mixed.interchange.insert(proof, second);
    assert!(mixed.validate().is_err());
}
