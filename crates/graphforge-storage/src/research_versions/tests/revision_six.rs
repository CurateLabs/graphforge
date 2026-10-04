//! Research revision 6 Projects stay readable and upgrade on their first research write.
use super::*;

/// A real research/6 Project written by the revision 6 code
/// (`tests/fixtures/research-v6/README.md`).
const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/research-v6"
);

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

fn legacy_project(root: &Path) -> (Uuid, Uuid) {
    copy_dir(&Path::new(FIXTURE).join("project"), root);
    let ids: serde_json::Value =
        serde_json::from_slice(&std::fs::read(Path::new(FIXTURE).join("ids.json")).unwrap())
            .unwrap();
    let uuid = |key: &str| ids[key].as_str().unwrap().parse::<Uuid>().unwrap();
    (uuid("project_context_uuid"), uuid("project_version_uuid"))
}

fn revision(root: &Path) -> u32 {
    let generation = crate::resolve_project_generation(root).unwrap();
    let capability = generation.capability(RESEARCH_CAPABILITY).unwrap().unwrap();
    let registry = generation
        .participant_snapshot(RESEARCH_CAPABILITY, RESEARCH_REGISTRY)
        .unwrap()
        .unwrap();
    assert_eq!(registry.record_version, capability.capability_version);
    assert_eq!(
        Some(registry.schema_fingerprint),
        legacy::registry_schema(capability.capability_version)
    );
    for participant in generation.participant_descriptors().unwrap() {
        if participant.capability_id == RESEARCH_CAPABILITY {
            assert_eq!(
                participant.capability_version,
                capability.capability_version
            );
        }
    }
    capability.capability_version
}

fn capture(root: &Path, context: Uuid) -> ResearchOperation {
    let mut operation = register(root, context);
    if let ResearchMutation::Register(spec) = &mut operation.mutation {
        spec.author = Some(ResearchSignature {
            name: "Ada".into(),
            email: None,
            orcid: None,
        });
    }
    operation
}

#[test]
fn research_v6_registry_reads_and_upgrades_on_the_first_research_write() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let (context, v6_head) = legacy_project(root);
    assert_eq!(revision(root), RESEARCH_LEGACY_VERSION);
    let before = state(root);
    assert!(before.ancestry.is_empty());
    assert!(before.versions.values().all(legacy::legacy_record));
    assert_eq!(before.heads[&context], v6_head);
    let receipt = execute(root, &capture(root, context));
    assert_eq!(revision(root), RESEARCH_VERSION);
    let after = state(root);
    let new = receipt.version_uuid.unwrap();
    assert_eq!(after.versions[&new].parents, vec![v6_head]);
    assert_eq!(after.ancestry, BTreeMap::from([(new, vec![v6_head])]));
    for (id, version) in &before.versions {
        assert_eq!(json(&after.versions[id]).unwrap(), json(version).unwrap());
        assert_eq!(after.identities[id], before.identities[id]);
    }
    crate::project_recovery::recover_project_on_open(root).unwrap();
    assert_eq!(revision(root), RESEARCH_VERSION);
    assert_eq!(state(root), after);
}

#[test]
fn research_v6_is_carried_but_never_written_anew() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    legacy_project(root);
    let republish = |root: &Path, change: &dyn Fn(&mut ProjectGenerationRequest)| {
        let parent = crate::resolve_project_generation(root).unwrap();
        let mut request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: parent
                .capabilities()
                .into_iter()
                .map(|c| ProjectCapability {
                    capability_id: c.capability_id,
                    capability_version: c.capability_version,
                })
                .collect(),
            participants: parent
                .participant_snapshots()
                .unwrap()
                .into_iter()
                .map(|p| ProjectParticipant {
                    capability_id: p.capability_id,
                    capability_version: p.capability_version,
                    record_family_id: p.record_family_id,
                    record_version: p.record_version,
                    encoding: match p.encoding.as_str() {
                        "json" => ProjectParticipantEncoding::Json,
                        "parquet" => ProjectParticipantEncoding::Parquet,
                        _ => ProjectParticipantEncoding::Arrow,
                    },
                    schema_fingerprint: p.schema_fingerprint,
                    row_count: p.row_count,
                    bytes: p.bytes,
                })
                .collect(),
        };
        change(&mut request);
        let lease = crate::begin_graph_object_publication(root).unwrap();
        let ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(root, &request).unwrap()
        else {
            panic!("unexpected replay")
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .map(|validated| validated.publish_with_graph_objects(&lease).unwrap())
    };
    // A publisher that does not write research carries revision 6 unchanged.
    republish(root, &|_| {}).unwrap();
    assert_eq!(revision(root), RESEARCH_LEGACY_VERSION);
    // Revision 7 data cannot be published under a revision 6 label.
    let mut forged = state(root);
    let id = *forged.versions.keys().next().unwrap();
    let version = forged.versions.get_mut(&id).unwrap();
    version.author = Some(ResearchSignature {
        name: "Forged".into(),
        email: None,
        orcid: None,
    });
    let digest = identity_digest(version).unwrap();
    forged.identities.insert(id, digest);
    let bytes = json(&forged).unwrap();
    let Err(error) = republish(root, &|request| {
        let registry = request
            .participants
            .iter_mut()
            .find(|p| p.record_family_id == RESEARCH_REGISTRY)
            .unwrap();
        registry.bytes.clone_from(&bytes);
    }) else {
        panic!("revision 7 data published as revision 6")
    };
    assert!(error.to_string().contains("revision 6"), "{error}");
    // Once upgraded, a Project is never published as revision 6 again.
    let other = tempfile::tempdir().unwrap();
    let upgraded = other.path();
    let (_, v6_head) = legacy_project(upgraded);
    // A research write that adds no Version: the registry has no commit data,
    // so only its revision label could make it revision 6 again.
    execute(
        upgraded,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(upgraded),
            mutation: ResearchMutation::RetainRoot {
                root: ResearchRetentionRoot {
                    root_uuid: Uuid::now_v7(),
                    kind: ResearchRootKind::RetainedVersion,
                    versions: BTreeSet::from([v6_head]),
                },
            },
        },
    );
    assert_eq!(revision(upgraded), RESEARCH_VERSION);
    state(upgraded).validate_legacy().unwrap();
    let Err(error) = republish(upgraded, &|request| {
        let mut capabilities = std::mem::take(&mut request.capabilities);
        for c in &mut capabilities {
            if c.capability_id == RESEARCH_CAPABILITY {
                c.capability_version = RESEARCH_LEGACY_VERSION;
            }
        }
        request.capabilities = capabilities;
        for p in &mut request.participants {
            if p.capability_id == RESEARCH_CAPABILITY {
                p.capability_version = RESEARCH_LEGACY_VERSION;
                if p.record_family_id == RESEARCH_REGISTRY {
                    p.record_version = RESEARCH_LEGACY_VERSION;
                    p.schema_fingerprint =
                        legacy::registry_schema(RESEARCH_LEGACY_VERSION).unwrap();
                }
            }
        }
    }) else {
        panic!("revision 7 Project republished as revision 6")
    };
    assert!(error.to_string().contains("revision 6"), "{error}");
    assert_eq!(revision(upgraded), RESEARCH_VERSION);
}

#[test]
fn a_crash_during_the_upgrading_write_leaves_a_readable_project() {
    for after in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let (context, v6_head) = legacy_project(root);
        let request = capture(root, context);
        let boundary = if after {
            "project.after_current_replace"
        } else {
            "project.before_current_replace"
        };
        // The subprocess exits at the boundary without returning: a crash.
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "research_versions::tests::research_operation_subprocess",
                "--nocapture",
            ])
            .env("GRAPHFORGE_RESEARCH_TEST_ROOT", root)
            .env(
                "GRAPHFORGE_RESEARCH_TEST_REQUEST",
                serde_json::to_string(&request).unwrap(),
            )
            .env("GRAPHFORGE_RESEARCH_TEST_COMMITTED", after.to_string())
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINT", boundary)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{boundary}");
        crate::project_recovery::recover_project_on_open(root).unwrap();
        let registry = state(root);
        assert_eq!(
            revision(root),
            if after {
                RESEARCH_VERSION
            } else {
                RESEARCH_LEGACY_VERSION
            }
        );
        assert_eq!(
            registry.receipts.contains_key(&request.operation_uuid),
            after
        );
        // Either way the write completes or replays, descending from the v6 head.
        let receipt = execute(root, &request);
        assert_eq!(
            state(root).versions[&receipt.version_uuid.unwrap()].parents,
            vec![v6_head]
        );
        assert_eq!(revision(root), RESEARCH_VERSION);
    }
}
