//! Research revision 6 records remain readable within the current producer contract (ADR 0055).
//!
//! The fixture is a real Project and package written by the research/6 code;
//! see `tests/fixtures/research-v6/README.md` for how it is generated. Its 0.5.2
//! interchange producer is intentionally incompatible with this pre-v1 build;
//! exporting the readable Project creates a package with the current producer.
use graphforge_api::*;
use graphforge_knowledge::research::{ResearchDecisionKind, ResearchSubjectKind};
use graphforge_storage::research_versions::{RESEARCH_LEGACY_VERSION, RESEARCH_VERSION};
use sha2::{Digest, Sha256};
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
            std::fs::copy(entry.path(), &target).unwrap();
            // Git does not preserve the producer's read-only CAS permissions.
            // Restore the seal on copied payloads before opening the Project.
            if from
                .parent()
                .is_some_and(|parent| parent.ends_with("graph-objects/sha256"))
            {
                let mut permissions = std::fs::metadata(&target).unwrap().permissions();
                permissions.set_readonly(true);
                std::fs::set_permissions(&target, permissions).unwrap();
            }
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
fn research_v6_package_from_old_producer_is_rejected_without_publication() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("imported");
    let error = GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: Path::new(FIXTURE).join("package.gfpb"),
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap_err();
    assert_eq!(
        error.code,
        graphforge_core::portable::PortableV2ErrorCode::Incompatible
    );
    assert!(error.committed_import.is_none());
    assert!(
        !target.exists(),
        "incompatible import must not create its target"
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
    let imported = GraphForge::new(target.to_str()).unwrap();
    let registry = imported.research_version_retention().unwrap();
    assert_legacy_roots(&registry, &ids);
    let archive = &registry.interchange[&head];
    assert_eq!(archive.research_capability_version, RESEARCH_VERSION);
    assert_eq!(
        archive.producer,
        concat!(
            "graphforge-storage/",
            env!("CARGO_PKG_VERSION"),
            ";research-interchange/1"
        )
    );
    // The producer-version contract and research-revision contract are separate.
    // Current-producer revision 6 archives admit unchanged legacy roots, but
    // cannot carry revision 7 commit data even with a recomputed identity.
    let mut forged = archive.clone();
    forged.research_capability_version = RESEARCH_LEGACY_VERSION;
    forged.validate().unwrap();
    let record = forged.versions.get_mut(&head).unwrap();
    record.author = Some(signature("Forged"));
    let identity = record.identity_sha256().unwrap();
    forged.identities.insert(head, identity);
    let error = forged.validate().unwrap_err();
    assert!(error.to_string().contains("revision 6"), "{error}");
    forged.research_capability_version = RESEARCH_VERSION;
    forged.validate().unwrap();

    // Imported legacy records also survive whole-Project export and reimport
    // when the interchange archive has the current producer contract.
    let whole = directory.path().join("whole.gfpb");
    imported
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: whole.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    let report = verify_portable_v2(
        &PortableVerifyRequest {
            input: whole.clone(),
            mode: graphforge_core::portable::PortableV2Mode::Full,
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap();
    assert!(report.research_interchange);
    let reimported = directory.path().join("reimported");
    GraphForge::import_portable_v2(
        &reimported,
        &PortableV2ImportRequest {
            input: whole,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    assert_eq!(revision(&reimported), RESEARCH_VERSION);
    assert_legacy_roots(
        &GraphForge::new(reimported.to_str())
            .unwrap()
            .research_version_retention()
            .unwrap(),
        &ids,
    );
}

#[test]
fn research_v6_imported_history_from_old_producer_is_rejected_without_publication() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("v6-imported");
    copy_dir(&Path::new(FIXTURE).join("imported"), &source);
    assert_eq!(revision(&source), RESEARCH_LEGACY_VERSION);
    let before = std::fs::read(source.join("CURRENT")).unwrap();
    let graph = GraphForge::new(source.to_str()).unwrap();
    let error = graph.research_version_retention().unwrap_err();
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
    assert!(
        error
            .to_string()
            .contains("unsupported or oversized research interchange manifest"),
        "{error}"
    );
    assert_eq!(std::fs::read(source.join("CURRENT")).unwrap(), before);
    assert_eq!(revision(&source), RESEARCH_LEGACY_VERSION);
}

/// Synthesize a current-producer revision 6 package from a native expanded
/// export. This is a test-only layout variant, not an original historical
/// export: committed fixture bytes and Version identities remain untouched.
fn synthesize_current_producer_revision_six(package: &Path) {
    let manifest_path = package.join("data/graphforge-project.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    let components = manifest["components"].as_array().unwrap();
    let registry_path = components
        .iter()
        .find(|component| {
            component["participant_id"]
                .as_str()
                .unwrap()
                .starts_with("research-registry-")
        })
        .unwrap()["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let runtime_path = components
        .iter()
        .find(|component| component["participant_id"] == "graphforge-runtime-map")
        .unwrap()["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut registry: ResearchRegistry =
        serde_json::from_slice(&std::fs::read(package.join(&registry_path)).unwrap()).unwrap();
    let identities = registry.identities.clone();
    for archive in registry.interchange.values_mut() {
        assert_eq!(archive.research_capability_version, RESEARCH_VERSION);
        assert_eq!(
            archive.producer,
            concat!(
                "graphforge-storage/",
                env!("CARGO_PKG_VERSION"),
                ";research-interchange/1"
            )
        );
        archive.research_capability_version = RESEARCH_LEGACY_VERSION;
    }
    registry.validate().unwrap();
    assert_eq!(registry.identities, identities);
    std::fs::write(
        package.join(&registry_path),
        serde_json::to_vec(&registry).unwrap(),
    )
    .unwrap();

    let mut runtime: serde_json::Value =
        serde_json::from_slice(&std::fs::read(package.join(&runtime_path)).unwrap()).unwrap();
    for capability in runtime["capabilities"].as_array_mut().unwrap() {
        if capability["capability_id"] == "research" {
            capability["capability_version"] = RESEARCH_LEGACY_VERSION.into();
        }
    }
    for participant in runtime["participants"].as_array_mut().unwrap() {
        if participant["capability_id"] == "research" {
            assert_eq!(participant["record_family_id"], "registry");
            participant["capability_version"] = RESEARCH_LEGACY_VERSION.into();
            participant["record_version"] = RESEARCH_LEGACY_VERSION.into();
            participant["schema_fingerprint"] =
                hex(&Sha256::digest(b"graphforge-research-registry/6").into()).into();
        }
    }
    std::fs::write(
        package.join(runtime_path),
        serde_json::to_vec(&runtime).unwrap(),
    )
    .unwrap();

    // Reseal the synthetic package's descriptors and semantic/BagIt hashes.
    for component in manifest["components"].as_array_mut().unwrap() {
        for file in component["files"].as_array_mut().unwrap() {
            let bytes = std::fs::read(package.join(file["path"].as_str().unwrap())).unwrap();
            file["length"] = (bytes.len() as u64).into();
            file["sha256"] = hex(&Sha256::digest(&bytes).into()).into();
        }
    }
    manifest.as_object_mut().unwrap().remove("package_digest");
    let semantic = serde_json::to_vec(&manifest).unwrap();
    let digest = Sha256::digest([b"graphforge-project/2\0".as_slice(), &semantic].concat());
    manifest["package_digest"] = format!("sha256:{}", hex(&digest.into())).into();
    std::fs::write(manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    for name in ["manifest-sha256.txt", "tagmanifest-sha256.txt"] {
        let old = std::fs::read_to_string(package.join(name)).unwrap();
        let mut resealed = String::new();
        for line in old.lines() {
            let (_, path) = line.split_once("  ").unwrap();
            let bytes = std::fs::read(package.join(path)).unwrap();
            resealed.push_str(&format!("{}  {path}\n", hex(&Sha256::digest(bytes).into())));
        }
        std::fs::write(package.join(name), resealed).unwrap();
    }
}

#[test]
fn current_producer_revision_six_package_imports_reopens_and_reexports() {
    let (directory, project, ids) = fixture();
    let graph = GraphForge::new(project.to_str()).unwrap();
    let head = ids.uuid("branch_head_version_uuid");
    let package = directory
        .path()
        .join("synthetic-current-producer-revision-six");
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid: head,
                output: package.clone(),
                bundled: false,
                projection: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    synthesize_current_producer_revision_six(&package);
    let report = verify_portable_v2(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: graphforge_core::portable::PortableV2Mode::Full,
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    assert!(report.research_interchange);
    let target = directory.path().join("imported-revision-six");
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
    let imported = GraphForge::new(target.to_str()).unwrap();
    let registry = imported.research_version_retention().unwrap();
    assert_legacy_roots(&registry, &ids);
    assert_eq!(
        registry.interchange[&head].research_capability_version,
        RESEARCH_LEGACY_VERSION
    );
    drop(imported);
    let reopened = GraphForge::new(target.to_str()).unwrap();
    assert_eq!(reopened.research_version_retention().unwrap(), registry);
    let current_package = directory.path().join("reexported-current.gfpb");
    reopened
        .export_research(
            &ExportResearchRequest {
                version_uuid: head,
                output: current_package.clone(),
                bundled: true,
                projection: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let again = directory.path().join("reimported-current");
    GraphForge::import_portable_v2(
        &again,
        &PortableV2ImportRequest {
            input: current_package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    assert_eq!(revision(&again), RESEARCH_VERSION);
    let registry = GraphForge::new(again.to_str())
        .unwrap()
        .research_version_retention()
        .unwrap();
    assert_legacy_roots(&registry, &ids);
    assert_eq!(
        registry.interchange[&head].research_capability_version,
        RESEARCH_VERSION
    );
}
