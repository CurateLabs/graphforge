//! Research revision 6 Projects and packages keep working (ADR 0054).
//!
//! The fixture is a real Project and package written by the research/6 code;
//! see `tests/fixtures/research-v6/README.md` for how it is generated.
use graphforge_api::*;
use graphforge_knowledge::research::{ResearchDecisionKind, ResearchSubjectKind};
use graphforge_storage::research_versions::{RESEARCH_LEGACY_VERSION, RESEARCH_VERSION};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/research-v6"
);

struct Ids(serde_json::Value);

impl Ids {
    fn uuid(&self, key: &str) -> Uuid {
        self.0[key].as_str().unwrap().parse().unwrap()
    }
    fn identities(&self) -> Vec<(Uuid, String)> {
        self.0["identities"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(id, digest)| (id.parse().unwrap(), digest.as_str().unwrap().to_owned()))
            .collect()
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn fixture() -> (tempfile::TempDir, PathBuf, Ids) {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    copy_dir(&Path::new(FIXTURE).join("project"), &project);
    let ids = serde_json::from_slice(&std::fs::read(Path::new(FIXTURE).join("ids.json")).unwrap())
        .unwrap();
    (directory, project, Ids(ids))
}

fn revision(project: &Path) -> u32 {
    graphforge_storage::resolve_project_generation(project)
        .unwrap()
        .capability("research")
        .unwrap()
        .unwrap()
        .capability_version
}

fn current(graph: &GraphForge) -> Uuid {
    graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn signature(name: &str) -> ResearchSignature {
    ResearchSignature {
        name: name.into(),
        email: None,
        orcid: None,
    }
}

fn canonical_choices(graph: &GraphForge) -> usize {
    graph
        .research_canonical_choices(&ResearchContext::Project, None)
        .unwrap()
        .batches
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

/// Every revision 6 identity the registry knows is unchanged, and every
/// revision 6 Version reads as a parentless legacy root.
fn assert_legacy_roots(registry: &ResearchRegistry, ids: &Ids) {
    for (id, digest) in ids.identities() {
        let Some(committed) = registry.identities.get(&id) else {
            continue;
        };
        assert_eq!(hex(committed), digest, "{id}");
        if let Some(version) = registry.versions.get(&id) {
            assert_eq!(version.identity_sha256().map(|d| hex(&d)).unwrap(), digest);
            assert!(version.parents.is_empty() && version.provenance.is_none());
            assert!(version.author.is_none() && version.committer.is_none());
            assert!(!registry.ancestry.contains_key(&id));
        }
    }
}

#[test]
fn research_v6_project_opens_and_its_first_research_write_upgrades_it() {
    let (_directory, project, ids) = fixture();
    let branch = ids.uuid("branch_uuid");
    let v6_head = ids.uuid("branch_head_version_uuid");
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    assert_eq!(revision(&project), RESEARCH_LEGACY_VERSION);
    let before = graph.research_version_retention().unwrap();
    assert_eq!(before.identities.len(), ids.identities().len());
    assert!(before.ancestry.is_empty());
    assert_legacy_roots(&before, &ids);
    assert_eq!(
        graph.open_research_branch(branch).unwrap().version_uuid(),
        v6_head
    );
    assert_eq!(canonical_choices(&graph), 1, "revision 6 decisions read");
    // A graph write does not write research: revision 6 is carried unchanged.
    graph.execute("CREATE (:Unrelated)").unwrap();
    assert_eq!(revision(&project), RESEARCH_LEGACY_VERSION);
    // The first research write commits on the v6 head and upgrades the Project.
    let edit = ExecuteResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch,
        version_uuid: Uuid::now_v7(),
        query: "MATCH (n:ClaimSubject) SET n.score = 2".into(),
        created_at: 10,
        author: Some(signature("Ada")),
        committer: None,
    };
    graph
        .execute_research_branch(&edit, &CancellationToken::new())
        .unwrap();
    assert_eq!(revision(&project), RESEARCH_VERSION);
    let after = graph.research_version_retention().unwrap();
    let record = &after.versions[&edit.version_uuid];
    assert_eq!(record.parents, vec![v6_head]);
    assert_eq!(record.author, Some(signature("Ada")));
    assert_eq!(after.ancestry[&edit.version_uuid], vec![v6_head]);
    assert_legacy_roots(&after, &ids);
    for (id, version) in &before.versions {
        // Upgrading never rewrites a revision 6 record.
        assert_eq!(
            serde_json::to_vec(&after.versions[id]).unwrap(),
            serde_json::to_vec(version).unwrap()
        );
    }
    assert_eq!(
        canonical_choices(&graph),
        1,
        "decisions survive the upgrade"
    );
    // A Project capture on the v6 Project head records that head as its parent.
    let context = ids.uuid("project_context_uuid");
    let capture = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: Uuid::now_v7(),
            context_uuid: context,
            label: None,
            description: None,
            created_at: 11,
            required_versions: Default::default(),
            author: None,
            committer: None,
        })
        .unwrap();
    let captured = graph
        .commit_research_version_operation(capture, &CancellationToken::new())
        .unwrap()
        .version_uuid
        .unwrap();
    assert_eq!(
        graph.research_version(captured).unwrap().parents,
        vec![ids.uuid("project_version_uuid")]
    );
    // Reopen after the upgrade.
    drop(graph);
    let graph = GraphForge::new(project.to_str()).unwrap();
    assert_eq!(revision(&project), RESEARCH_VERSION);
    let reopened = graph.research_version_retention().unwrap();
    assert_legacy_roots(&reopened, &ids);
    assert_eq!(reopened.ancestors(edit.version_uuid), vec![v6_head]);
    assert_eq!(
        graph.open_research_branch(branch).unwrap().version_uuid(),
        edit.version_uuid
    );
    assert_eq!(canonical_choices(&graph), 1);
}

#[test]
fn research_decisions_on_a_v6_project_upgrade_it() {
    let (_directory, project, ids) = fixture();
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    let subject = graph
        .execute("MATCH (n:ClaimSubject) RETURN n.node_uuid AS id")
        .unwrap();
    let subject = Uuid::from_slice(
        subject.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    graph
        .record_research_decisions(
            &RecordResearchDecisionsRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                context: ResearchContext::Project,
                community_uuid: None,
                creator_uuid: Uuid::now_v7(),
                recorded_at: 20,
                decisions: vec![ResearchDecisionInput {
                    decision_uuid: Uuid::now_v7(),
                    subject_kind: ResearchSubjectKind::Node,
                    subject_uuid: subject,
                    kind: ResearchDecisionKind::Revoke,
                    source_version_uuid: None,
                }],
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(revision(&project), RESEARCH_VERSION);
    assert_eq!(canonical_choices(&graph), 0, "the revocation is recorded");
    drop(graph);
    let graph = GraphForge::new(project.to_str()).unwrap();
    assert_legacy_roots(&graph.research_version_retention().unwrap(), &ids);
}

#[test]
fn checkpoint_diff_spans_the_upgrade() {
    let (_directory, project, ids) = fixture();
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "research-v6".into(),
            description: None,
            idempotency_key: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        })
        .unwrap();
    assert_eq!(revision(&project), RESEARCH_LEGACY_VERSION);
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid: ids.uuid("branch_uuid"),
                version_uuid: Uuid::now_v7(),
                query: "MATCH (n:ClaimSubject) SET n.score = 3".into(),
                created_at: 10,
                author: None,
                committer: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(revision(&project), RESEARCH_VERSION);
    // Record-level diff has no research registry adapter at any revision;
    // the participant summary and graph records span the upgrade.
    for (scope, detail) in [
        (CheckpointDiffScope::All, CheckpointDiffDetail::Summary),
        (CheckpointDiffScope::Graph, CheckpointDiffDetail::Records),
    ] {
        let diff = graph
            .diff_checkpoints(DiffCheckpointsRequest {
                from: CheckpointSelector::Named("research-v6".into()),
                to: CheckpointSelector::Current,
                scope,
                detail,
                page: PageRequest::default(),
            })
            .unwrap();
        if detail == CheckpointDiffDetail::Summary {
            // The research participants changed revision and content.
            assert!(diff.batches.iter().map(|b| b.num_rows()).sum::<usize>() > 0);
        }
    }
}

#[test]
fn research_v6_package_imports_as_legacy_roots_at_the_current_revision() {
    let (directory, _project, ids) = fixture();
    let head = ids.uuid("branch_head_version_uuid");
    let target = directory.path().join("imported");
    GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: Path::new(FIXTURE).join("package.gfpb"),
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    // Imported at the current revision; the Versions stay legacy roots.
    assert_eq!(revision(&target), RESEARCH_VERSION);
    let imported = GraphForge::new(target.to_str()).unwrap();
    let registry = imported.research_version_retention().unwrap();
    assert_legacy_roots(&registry, &ids);
    let archive = &registry.interchange[&head];
    assert_eq!(archive.research_capability_version, RESEARCH_LEGACY_VERSION);
    assert!(registry.versions[&head].parents.is_empty());
    assert_eq!(
        hex(&registry.identities[&head]),
        ids.identities()
            .into_iter()
            .find(|(id, _)| *id == head)
            .unwrap()
            .1
    );
    // Exports are written at the current revision.
    let package = directory.path().join("reexported.gfpb");
    imported
        .export_research(
            &ExportResearchRequest {
                version_uuid: head,
                output: package.clone(),
                bundled: true,
                projection: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let again = directory.path().join("reimported");
    GraphForge::import_portable_v2(
        &again,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    assert_eq!(revision(&again), RESEARCH_VERSION);
    let reimported = GraphForge::new(again.to_str())
        .unwrap()
        .research_version_retention()
        .unwrap();
    assert_eq!(reimported.identities[&head], registry.identities[&head]);
    assert_legacy_roots(&reimported, &ids);
    assert_eq!(
        reimported.interchange[&head].research_capability_version,
        RESEARCH_VERSION
    );
}

#[test]
fn research_v6_project_exports_research_as_v7() {
    let (directory, project, ids) = fixture();
    let graph = GraphForge::new(project.to_str()).unwrap();
    let head = ids.uuid("branch_head_version_uuid");
    let package = directory.path().join("export.gfpb");
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid: head,
                output: package.clone(),
                bundled: true,
                projection: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(
        revision(&project),
        RESEARCH_LEGACY_VERSION,
        "export is read-only"
    );
    let target = directory.path().join("imported");
    GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    assert_eq!(revision(&target), RESEARCH_VERSION);
    let registry = GraphForge::new(target.to_str())
        .unwrap()
        .research_version_retention()
        .unwrap();
    assert_legacy_roots(&registry, &ids);
}

#[test]
fn research_v6_imported_project_opens_and_exports_as_v7() {
    let (directory, _project, ids) = fixture();
    let head = ids.uuid("branch_head_version_uuid");
    let source = directory.path().join("v6-imported");
    copy_dir(&Path::new(FIXTURE).join("imported"), &source);
    assert_eq!(revision(&source), RESEARCH_LEGACY_VERSION);
    let graph = GraphForge::new(source.to_str()).unwrap();
    let registry = graph.research_version_retention().unwrap();
    assert_legacy_roots(&registry, &ids);
    let archive = &registry.interchange[&head];
    assert_eq!(archive.research_capability_version, RESEARCH_LEGACY_VERSION);
    // A revision 6 archive cannot hold revision 7 commit data, even when the
    // forged record's identity is recommitted consistently.
    let mut forged = archive.clone();
    let record = forged.versions.get_mut(&head).unwrap();
    record.author = Some(signature("Forged"));
    let identity = record.identity_sha256().unwrap();
    forged.identities.insert(head, identity);
    let error = forged.validate().unwrap_err();
    assert!(error.to_string().contains("revision 6"), "{error}");
    let mut current = forged.clone();
    current.research_capability_version = RESEARCH_VERSION;
    current.validate().unwrap();
    // A whole-Project export of it is written at the current revision.
    let package = directory.path().join("whole.gfpb");
    graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    assert_eq!(
        revision(&source),
        RESEARCH_LEGACY_VERSION,
        "export is read-only"
    );
    let report = verify_portable_v2(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: graphforge_core::portable::PortableV2Mode::Full,
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap();
    assert!(report.research_interchange);
    let target = directory.path().join("reimported");
    GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    assert_eq!(revision(&target), RESEARCH_VERSION);
    assert_legacy_roots(
        &GraphForge::new(target.to_str())
            .unwrap()
            .research_version_retention()
            .unwrap(),
        &ids,
    );
}
