//! Real storage foundation fixtures, not Branch or Proposal lifecycle proof.
use super::*;

fn state(root: &Path) -> ResearchRegistry {
    read_research_registry(&crate::resolve_project_generation(root).unwrap()).unwrap()
}

fn current(root: &Path) -> Uuid {
    crate::resolve_project_generation(root)
        .unwrap()
        .generation_uuid()
}

fn fixture(root: &Path, value: &str) -> Uuid {
    crate::open_or_initialize_project(root).unwrap();
    let parent = crate::resolve_project_generation(root).unwrap();
    let old_registry = read_research_registry(&parent).unwrap();
    let mut participants = vec![ProjectParticipant {
        capability_id: "workspace".into(),
        capability_version: 1,
        record_family_id: "research_fixture".into(),
        record_version: 1,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: Sha256::digest(b"research-context-fixture/1").into(),
        row_count: 1,
        bytes: json(&value).unwrap(),
    }];
    let mut capabilities = vec![
        ProjectCapability {
            capability_id: "workspace".into(),
            capability_version: 1,
        },
        ProjectCapability {
            capability_id: "graph".into(),
            capability_version: 1,
        },
    ];
    if !old_registry.receipts.is_empty() {
        participants.push(old_registry.participant().unwrap());
        capabilities.push(ProjectCapability {
            capability_id: RESEARCH_CAPABILITY.into(),
            capability_version: RESEARCH_VERSION,
        });
    }
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities,
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        crate::stage_project_generation(root, &request).unwrap()
    else {
        panic!("unexpected replay");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    request.generation_uuid
}

fn register(root: &Path, context: Uuid) -> ResearchOperation {
    ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(root),
        mutation: ResearchMutation::Register(RegisterResearchVersion {
            version_uuid: Uuid::now_v7(),
            context_uuid: context,
            source_generation_uuid: current(root),
            selection: None,
            source_version: None,
            required_versions: BTreeSet::new(),
            label: Some("fixture".into()),
            description: None,
            created_at: 1,
            evidence: Vec::new(),
        }),
    }
}

fn execute(root: &Path, request: &ResearchOperation) -> ResearchOperationReceipt {
    publish_research_operation(root, request, &AtomicBool::new(false)).unwrap()
}

#[test]
fn restoration_preserves_unselected_context_and_operation_history_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "old A content");
    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    let original = register(root, a);
    let first = execute(root, &original);
    fixture(root, "new A content");
    let newer_a = execute(root, &register(root, a));
    fixture(root, "independent B and parent content");
    let newer_b = execute(root, &register(root, b));
    let before = state(root);
    let new_id = Uuid::now_v7();
    let restored = execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::Restore {
                context_uuid: a,
                source_version: first.version_uuid.unwrap(),
                version_uuid: new_id,
                created_at: 2,
            },
        },
    );
    let after = state(root);
    assert_eq!(after.heads[&a], new_id);
    assert_eq!(after.heads[&b], newer_b.version_uuid.unwrap());
    assert_eq!(
        after.versions[&newer_a.version_uuid.unwrap()],
        before.versions[&newer_a.version_uuid.unwrap()]
    );
    for (id, receipt) in &before.receipts {
        assert_eq!(&after.receipts[id], receipt);
    }
    let historical = inspect_research_version(root, &after.versions[&new_id]).unwrap();
    assert_eq!(historical[0].bytes, json(&"old A content").unwrap());
    let active = crate::resolve_project_generation(root)
        .unwrap()
        .participant_snapshot("workspace", "research_fixture")
        .unwrap()
        .unwrap();
    assert_eq!(
        active.bytes,
        json(&"independent B and parent content").unwrap()
    );
    assert_eq!(execute(root, &original), first);
    assert_eq!(current(root), restored.generation_uuid);
}

#[test]
fn conflicting_retry_and_cancellation_do_not_mutate_current() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "fixture");
    let mut request = register(root, Uuid::now_v7());
    execute(root, &request);
    let before = current(root);
    if let ResearchMutation::Register(spec) = &mut request.mutation {
        spec.label = Some("changed".into());
    }
    assert_eq!(
        publish_research_operation(root, &request, &AtomicBool::new(false))
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert_eq!(
        publish_research_operation(
            root,
            &register(root, Uuid::now_v7()),
            &AtomicBool::new(true)
        )
        .unwrap_err()
        .code(),
        "GF_CANCELLED"
    );
    assert_eq!(current(root), before);
}

#[test]
fn projection_identity_and_retention_blockers_are_explicit() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let source = fixture(root, "fixture");
    let context = Uuid::now_v7();
    let original = execute(root, &register(root, context));
    let original_id = original.version_uuid.unwrap();
    let mut request = register(root, Uuid::now_v7());
    if let ResearchMutation::Register(spec) = &mut request.mutation {
        spec.source_generation_uuid = source;
        spec.source_version = Some(original_id);
        spec.selection = Some(BTreeSet::from([ResearchParticipantKey {
            capability: "workspace".into(),
            family: "research_fixture".into(),
        }]));
    }
    let projection = execute(root, &request).version_uuid.unwrap();
    assert_ne!(projection, original_id);
    assert_eq!(
        state(root).versions[&projection].content.source_version,
        Some(original_id)
    );
    let deletion = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(root),
        mutation: ResearchMutation::DeleteVersion {
            version_uuid: original_id,
        },
    };
    let error = publish_research_operation(root, &deletion, &AtomicBool::new(false)).unwrap_err();
    assert!(error.to_string().contains(&format!("context:{context}")));
}

#[test]
fn projection_cannot_replace_frozen_evidence_digest_or_availability() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let source = fixture(root, "fixture");
    let artifact = Uuid::now_v7();
    let (original, changed) = {
        let _lease = crate::begin_graph_object_publication(root).unwrap();
        (
            crate::install_project_object_bytes(root, b"original").unwrap(),
            crate::install_project_object_bytes(root, b"changed!").unwrap(),
        )
    };
    let mut initial = register(root, Uuid::now_v7());
    if let ResearchMutation::Register(spec) = &mut initial.mutation {
        spec.evidence.push(ResearchEvidenceReference::Local {
            artifact_uuid: artifact,
            sha256: original.0,
            byte_length: original.1,
        });
    }
    let source_version = execute(root, &initial).version_uuid;
    for evidence in [
        ResearchEvidenceReference::Local {
            artifact_uuid: artifact,
            sha256: changed.0,
            byte_length: changed.1,
        },
        ResearchEvidenceReference::ExternalOnly {
            artifact_uuid: artifact,
            fingerprint: None,
        },
        ResearchEvidenceReference::Unverifiable {
            artifact_uuid: artifact,
        },
    ] {
        let mut projection = register(root, Uuid::now_v7());
        if let ResearchMutation::Register(spec) = &mut projection.mutation {
            spec.source_generation_uuid = source;
            spec.source_version = source_version;
            spec.selection = Some(BTreeSet::from([ResearchParticipantKey {
                capability: "workspace".into(),
                family: "research_fixture".into(),
            }]));
            spec.evidence = vec![evidence];
        }
        let before = current(root);
        assert!(
            publish_research_operation(root, &projection, &AtomicBool::new(false))
                .unwrap_err()
                .to_string()
                .contains("projection evidence")
        );
        assert_eq!(current(root), before);
    }
}

fn compact_fixture(root: &Path, value: &str) -> String {
    compact_fixture_path(root, value, "fixture.parquet")
}

fn compact_fixture_path(root: &Path, value: &str, legacy_path: &str) -> String {
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    crate::open_or_initialize_project(root).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let batch = RecordBatch::try_from_iter(vec![(
        "fixture",
        Arc::new(StringArray::from(vec![value])) as arrow::array::ArrayRef,
    )])
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let path = workspace.path().join(legacy_path.replace('\\', "/"));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, &bytes).unwrap();
    let digest = hex(&Sha256::digest(&bytes).into());
    let inventory = crate::GraphFilesInventory {
        format: "graphforge-graph-files".into(),
        format_version: 1,
        file_count: 1,
        total_byte_length: bytes.len() as u64,
        files: vec![crate::GraphFileEntry {
            relative_path: legacy_path.into(),
            byte_length: bytes.len() as u64,
            content_sha256: digest.clone(),
            role: crate::GraphFileRole::Other,
        }],
    };
    let lease = crate::begin_graph_object_publication(root).unwrap();
    let (graph, _) = crate::graph_object_store::migrate_graph_files_v1_to_v2(
        &lease,
        workspace.path(),
        &inventory,
    )
    .unwrap();
    let mut participants = vec![crate::graph_files::graph_files_root_participant(&graph).unwrap()];
    let mut capabilities = vec![ProjectCapability {
        capability_id: "graph".into(),
        capability_version: 1,
    }];
    let registry = state(root);
    if !registry.receipts.is_empty() {
        participants.push(registry.participant().unwrap());
        capabilities.push(ProjectCapability {
            capability_id: RESEARCH_CAPABILITY.into(),
            capability_version: RESEARCH_VERSION,
        });
    }
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities,
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        crate::stage_project_generation(root, &request).unwrap()
    else {
        panic!("unexpected replay")
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish_with_graph_objects(&lease)
        .unwrap();
    digest
}

#[test]
fn compact_graph_and_local_evidence_survive_gc_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let graph_digest = compact_fixture(root, "historical graph fixture");
    let local = b"exact local Artifact fixture";
    let (digest, length) = {
        let _lease = crate::begin_graph_object_publication(root).unwrap();
        crate::install_project_object_bytes(root, local).unwrap()
    };
    let mut request = register(root, Uuid::now_v7());
    if let ResearchMutation::Register(spec) = &mut request.mutation {
        spec.evidence = vec![
            ResearchEvidenceReference::Local {
                artifact_uuid: Uuid::now_v7(),
                sha256: digest,
                byte_length: length,
            },
            ResearchEvidenceReference::ExternalOnly {
                artifact_uuid: Uuid::now_v7(),
                fingerprint: None,
            },
            ResearchEvidenceReference::Unverifiable {
                artifact_uuid: Uuid::now_v7(),
            },
        ];
    }
    let original = execute(root, &request);
    compact_fixture(root, "new current graph fixture");
    crate::project_retention::execute_project_cleanup(
        root,
        crate::project_retention::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        crate::project_retention::ProjectRetentionLimits::default(),
    )
    .unwrap();
    crate::project_recovery::recover_project_on_open(root).unwrap();
    let version = state(root).versions[&original.version_uuid.unwrap()].clone();
    inspect_research_version(root, &version).unwrap();
    assert!(
        crate::graph_object_path(root, &graph_digest)
            .unwrap()
            .is_file()
    );
    assert_eq!(
        crate::graph_object_store::read_graph_object(root, &hex(&digest), length).unwrap(),
        local
    );
    assert!(matches!(
        version.content.evidence[1],
        ResearchEvidenceReference::ExternalOnly { .. }
    ));
    assert!(matches!(
        version.content.evidence[2],
        ResearchEvidenceReference::Unverifiable { .. }
    ));
    assert_eq!(execute(root, &request), original);
}

#[test]
fn historical_graph_corruption_is_rejected_even_with_healthy_current() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let digest = compact_fixture(root, "old graph");
    let receipt = execute(root, &register(root, Uuid::now_v7()));
    compact_fixture(root, "new graph");
    let version = state(root).versions[&receipt.version_uuid.unwrap()].clone();
    let path = crate::graph_object_path(root, &digest).unwrap();
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 1;
    crate::graph_object_store::corrupt_sealed_graph_object_for_test(&path, &bytes);
    assert!(inspect_research_version(root, &version).is_err());
    crate::resolve_project_generation(root)
        .unwrap()
        .graph_files_inventory()
        .unwrap();
}

#[test]
fn malformed_registry_is_refused_before_creating_publication_files() {
    fn inventory(root: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
        let mut result = BTreeMap::new();
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                result.extend(inventory(&path));
            } else {
                result.insert(path.clone(), std::fs::read(path).unwrap());
            }
        }
        result
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "fixture");
    execute(root, &register(root, Uuid::now_v7()));
    let request = register(root, Uuid::now_v7());
    let parent = crate::resolve_project_generation(root).unwrap();
    let registry_path = parent
        .participant_path(RESEARCH_CAPABILITY, RESEARCH_REGISTRY)
        .unwrap();
    std::fs::write(registry_path, b"unsupported registry").unwrap();
    let before = inventory(root);
    assert!(publish_research_operation(root, &request, &AtomicBool::new(false)).is_err());
    assert_eq!(inventory(root), before);
}

#[test]
fn ordinary_publication_cannot_drop_or_rewrite_research_receipts() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "fixture");
    execute(root, &register(root, Uuid::now_v7()));
    let before = current(root);
    for bad_schema in [false, true] {
        let mut registry = state(root);
        let mut participant = registry.participant().unwrap();
        if bad_schema {
            participant.schema_fingerprint = [7; 32];
        } else {
            registry.receipts.clear();
            participant = registry.participant().unwrap();
        }
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: vec![
                ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                ProjectCapability {
                    capability_id: RESEARCH_CAPABILITY.into(),
                    capability_version: RESEARCH_VERSION,
                },
            ],
            participants: vec![participant],
        };
        let ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(root, &request).unwrap()
        else {
            panic!("unexpected replay")
        };
        assert!(staged.validate(|_| Ok(()), |_, _| Ok(())).is_err());
        assert_eq!(current(root), before);
    }
}

#[test]
fn registry_limits_and_cycles_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "fixture");
    let receipt = execute(root, &register(root, Uuid::now_v7()));
    let id = receipt.version_uuid.unwrap();
    let mut registry = state(root);
    registry
        .versions
        .get_mut(&id)
        .unwrap()
        .content
        .required_versions
        .insert(id);
    registry
        .identities
        .insert(id, identity_digest(&registry.versions[&id]).unwrap());
    assert!(
        registry
            .validate()
            .unwrap_err()
            .to_string()
            .contains("cycle")
    );
    let mut registry = state(root);
    for _ in 0..MAX_RECEIPTS {
        let mut extra = receipt.clone();
        extra.operation_uuid = Uuid::now_v7();
        registry.receipts.insert(extra.operation_uuid, extra);
    }
    assert_eq!(registry.validate().unwrap_err().code(), "GF_RESOURCE_LIMIT");
}

#[test]
fn released_payload_keeps_identity_and_replay_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "fixture");
    let context = Uuid::now_v7();
    let request = register(root, context);
    let original = execute(root, &request);
    let id = original.version_uuid.unwrap();
    let record = state(root).versions[&id].clone();
    execute(root, &register(root, context));
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::DeleteVersion { version_uuid: id },
        },
    );
    assert_eq!(
        inspect_research_version(root, &record).unwrap_err().code(),
        "GF_RESULT_NOT_RETAINED"
    );
    assert_eq!(execute(root, &request), original);
    let mut conflicting = request.clone();
    conflicting.operation_uuid = Uuid::now_v7();
    conflicting.expected_generation_uuid = current(root);
    if let ResearchMutation::Register(spec) = &mut conflicting.mutation {
        spec.label = Some("different immutable label".into());
    }
    assert_eq!(
        publish_research_operation(root, &conflicting, &AtomicBool::new(false))
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
}

#[test]
fn root_lifetime_preserves_accepted_provenance_and_releases_only_explicit_roots() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "fixture");
    let context = Uuid::now_v7();
    let first = execute(root, &register(root, context))
        .version_uuid
        .unwrap();
    execute(root, &register(root, context));
    for kind in [
        ResearchRootKind::FrozenProposal,
        ResearchRootKind::AcceptedProvenance,
    ] {
        let root_id = Uuid::now_v7();
        execute(
            root,
            &ResearchOperation {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(root),
                mutation: ResearchMutation::RetainRoot {
                    root: ResearchRetentionRoot {
                        root_uuid: root_id,
                        kind,
                        versions: BTreeSet::from([first]),
                    },
                },
            },
        );
        assert!(
            state(root)
                .deletion_blockers(first)
                .contains(&format!("root:{root_id}"))
        );
        let release = ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::ReleaseRoot { root_uuid: root_id },
        };
        if kind == ResearchRootKind::AcceptedProvenance {
            let before = current(root);
            assert!(publish_research_operation(root, &release, &AtomicBool::new(false)).is_err());
            assert_eq!(current(root), before);
        } else {
            execute(root, &release);
            assert!(!state(root).roots.contains_key(&root_id));
        }
    }
}

#[test]
fn checkpoint_replay_survives_research_enablement_but_new_whole_revert_is_refused() {
    use crate::project_checkpoints::{
        CheckpointCreateRequest, CheckpointRevertRequest, create_checkpoint, revert_checkpoint,
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "checkpoint source");
    create_checkpoint(
        root,
        &CheckpointCreateRequest {
            operation_uuid: Uuid::now_v7(),
            name: "before-research".into(),
            description: None,
            actor_uuid: None,
        },
    )
    .unwrap();
    fixture(root, "advanced");
    let request = CheckpointRevertRequest {
        operation_uuid: Uuid::now_v7(),
        name: "before-research".into(),
        reason: "fixture restore".into(),
        actor_uuid: None,
    };
    let (receipt, generation) = revert_checkpoint(root, &request, || Ok(1), |_| Ok(())).unwrap();
    drop(generation);
    execute(root, &register(root, Uuid::now_v7()));
    let before = current(root);
    let (replayed, _) = revert_checkpoint(root, &request, || Ok(2), |_| Ok(())).unwrap();
    assert_eq!(receipt, replayed);
    let mut changed = request;
    changed.operation_uuid = Uuid::now_v7();
    assert!(revert_checkpoint(root, &changed, || Ok(2), |_| Ok(())).is_err());
    assert_eq!(current(root), before);
}

#[test]
fn research_operation_subprocess() {
    let Ok(root) = std::env::var("GRAPHFORGE_RESEARCH_TEST_ROOT") else {
        return;
    };
    let request: ResearchOperation =
        serde_json::from_str(&std::env::var("GRAPHFORGE_RESEARCH_TEST_REQUEST").unwrap()).unwrap();
    let result = publish_research_operation(Path::new(&root), &request, &AtomicBool::new(false));
    let expected = std::env::var("GRAPHFORGE_RESEARCH_TEST_COMMITTED").unwrap();
    let error = result.unwrap_err();
    assert!(
        error.to_string().contains(&format!("committed={expected}")),
        "{error}"
    );
}

#[test]
fn pre_and_post_linearization_faults_preserve_truthful_reopen_and_replay() {
    for after in [false, true] {
        for returned_error in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path();
            fixture(root, "fixture");
            let request = register(root, Uuid::now_v7());
            let before = current(root);
            let boundary = if after {
                "project.after_current_replace"
            } else {
                "project.before_current_replace"
            };
            let failpoint = if returned_error {
                format!("{boundary}.error")
            } else {
                boundary.into()
            };
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
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(if returned_error { 0 } else { 86 }));
            crate::project_recovery::recover_project_on_open(root).unwrap();
            assert_eq!(
                state(root).receipts.contains_key(&request.operation_uuid),
                after
            );
            if !after {
                assert_eq!(current(root), before);
            }
            let receipt = execute(root, &request);
            assert_eq!(execute(root, &request), receipt);
        }
    }
}

mod project_content;
mod proposals;
mod retention;

#[test]
fn project_restore_replaces_research_in_one_publication_and_replay_keeps_later_state() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path(), "frozen");
    let context = Uuid::now_v7();
    let capture = register(root.path(), context);
    let receipt =
        publish_research_operation(root.path(), &capture, &AtomicBool::new(false)).unwrap();
    let source = receipt.version_uuid.unwrap();
    fixture(root.path(), "newer");
    let other = register(root.path(), Uuid::now_v7());
    publish_research_operation(root.path(), &other, &AtomicBool::new(false)).unwrap();
    let before = state(root.path());
    let restore = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(root.path()),
        mutation: ResearchMutation::RestoreProject {
            context_uuid: context,
            source_version: source,
            version_uuid: Uuid::now_v7(),
            created_at: 42,
        },
    };
    let restored =
        publish_research_operation(root.path(), &restore, &AtomicBool::new(false)).unwrap();
    let generation = crate::resolve_project_generation(root.path()).unwrap();
    assert_eq!(generation.generation_uuid(), restored.generation_uuid);
    assert_eq!(
        generation
            .participant_snapshot("workspace", "research_fixture")
            .unwrap()
            .unwrap()
            .bytes,
        json(&"frozen").unwrap()
    );
    let after = state(root.path());
    assert_eq!(after.versions[&source], before.versions[&source]);
    for (owner, head) in before.heads {
        if owner != context {
            assert_eq!(after.heads[&owner], head);
        }
    }
    for (id, receipt) in before.receipts {
        assert_eq!(after.receipts[&id], receipt);
    }
    fixture(root.path(), "after-restore");
    let newest = current(root.path());
    assert_eq!(
        publish_research_operation(root.path(), &restore, &AtomicBool::new(false)).unwrap(),
        restored
    );
    assert_eq!(current(root.path()), newest);
}

#[test]
fn branch_registry_and_selected_base_publish_with_one_receipt() {
    // Storage contract fixture only; the facade must separately prove real
    // two-Branch graph edits, isolation and restoration before #1352 closes.
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fixture(root, "selected research");
    let origin = execute(root, &register(root, Uuid::now_v7()))
        .version_uuid
        .unwrap();
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::Compact {
                versions: BTreeSet::from([origin]),
            },
        },
    );
    let mut version = state(root).versions[&origin].clone();
    let branch_uuid = Uuid::now_v7();
    version.version_uuid = Uuid::now_v7();
    version.context_uuid = branch_uuid;
    version.content.source_version = Some(origin);
    let base = version.version_uuid;
    let creation = ResearchBranchRecord {
        branch_uuid,
        project_uuid: Uuid::now_v7(),
        parent_branch_uuid: None,
        origin_version_uuid: origin,
        base_version_uuid: base,
        creator_uuid: Uuid::now_v7(),
        created_at: 7,
        label: "selected branch".into(),
        selection_sha256: [9; 32],
    };
    let operation = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(root),
        mutation: ResearchMutation::PublishBranch {
            intent_sha256: [0; 32],
            origin_capture: None,
            creation: Some(creation.clone()),
            version: Box::new(version.clone()),
        },
    };
    let receipt = execute(root, &operation);
    let before = state(root);
    assert_eq!(before.branches[&branch_uuid], creation);
    assert_eq!(before.heads[&branch_uuid], base);
    assert!(before.materialized.contains(&base));
    assert_eq!(before.receipts[&operation.operation_uuid], receipt);
    // A child cannot name A as its parent while capturing a Project Version.
    let mut false_parent = creation.clone();
    false_parent.branch_uuid = Uuid::now_v7();
    false_parent.parent_branch_uuid = Some(branch_uuid);
    false_parent.base_version_uuid = Uuid::now_v7();
    let mut child_version = version.clone();
    child_version.version_uuid = false_parent.base_version_uuid;
    child_version.context_uuid = false_parent.branch_uuid;
    let false_lineage = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(root),
        mutation: ResearchMutation::PublishBranch {
            intent_sha256: [0; 32],
            origin_capture: None,
            creation: Some(false_parent),
            version: Box::new(child_version),
        },
    };
    let before_rejection = current(root);
    let error =
        publish_research_operation(root, &false_lineage, &AtomicBool::new(false)).unwrap_err();
    assert!(error.to_string().contains("immediate parent"));
    assert_eq!(current(root), before_rejection);
    assert_eq!(state(root), before);
    assert_eq!(execute(root, &operation), receipt);
    let project_capture = register(root, branch_uuid);
    let current_before = current(root);
    assert!(publish_research_operation(root, &project_capture, &AtomicBool::new(false)).is_err());
    assert_eq!(current(root), current_before);
    assert_eq!(state(root), before);
    let mut changed = operation.clone();
    let ResearchMutation::PublishBranch { version, .. } = &mut changed.mutation else {
        unreachable!()
    };
    version.label = Some("changed request".into());
    assert!(publish_research_operation(root, &changed, &AtomicBool::new(false)).is_err());
    assert_eq!(state(root), before);
    let next = Uuid::now_v7();
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::Restore {
                context_uuid: branch_uuid,
                source_version: base,
                version_uuid: next,
                created_at: 8,
            },
        },
    );
    assert!(
        state(root)
            .deletion_blockers(base)
            .contains(&format!("branch_base:{branch_uuid}"))
    );
    crate::project_recovery::recover_project_on_open(root).unwrap();
    let reopened = state(root);
    assert_eq!(reopened.branches[&branch_uuid], creation);
    assert_eq!(reopened.heads[&branch_uuid], next);
    assert_eq!(reopened.receipts[&operation.operation_uuid], receipt);
}

#[test]
fn prepared_branch_content_outlives_private_source_without_changing_parent_research() {
    let project = tempfile::tempdir().unwrap();
    let root = project.path();
    fixture(root, "parent remains unchanged");
    let origin_id = execute(root, &register(root, Uuid::now_v7()))
        .version_uuid
        .unwrap();
    let origin = state(root).versions[&origin_id].clone();
    let private = tempfile::tempdir().unwrap();
    fixture(private.path(), "prepared Branch research");
    let source = crate::resolve_project_generation(private.path()).unwrap();
    let branch_uuid = Uuid::now_v7();
    let mut version = origin.clone();
    version.version_uuid = Uuid::now_v7();
    version.context_uuid = branch_uuid;
    version.content.source_version = Some(origin_id);
    let prepared = prepare_branch_content(root, &source, version, &AtomicBool::new(false)).unwrap();
    let base = prepared.version.version_uuid;
    let parent_before = crate::resolve_project_generation(root).unwrap();
    let before_bytes = parent_before
        .participant_snapshot("workspace", "research_fixture")
        .unwrap()
        .unwrap()
        .bytes;
    assert!(!state(root).versions.contains_key(&base));
    let creation = ResearchBranchRecord {
        branch_uuid,
        project_uuid: Uuid::now_v7(),
        parent_branch_uuid: None,
        origin_version_uuid: origin_id,
        base_version_uuid: base,
        creator_uuid: Uuid::now_v7(),
        created_at: 9,
        label: "prepared branch".into(),
        selection_sha256: [8; 32],
    };
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::PublishBranch {
                intent_sha256: [0; 32],
                origin_capture: None,
                creation: Some(creation),
                version: Box::new(prepared.version.clone()),
            },
        },
    );
    drop(prepared);
    drop(source);
    drop(private);
    crate::project_recovery::recover_project_on_open(root).unwrap();
    let after = crate::resolve_project_generation(root).unwrap();
    assert_eq!(
        after
            .participant_snapshot("workspace", "research_fixture")
            .unwrap()
            .unwrap()
            .bytes,
        before_bytes
    );
    let stored = state(root).versions[&base].clone();
    let snapshots = inspect_research_version(root, &stored).unwrap();
    assert_eq!(
        snapshots[0].bytes,
        json(&"prepared Branch research").unwrap()
    );
    assert!(state(root).materialized.contains(&base));
}

#[test]
fn current_branch_origin_is_atomic_genealogy_without_an_implicit_whole_parent_pin() {
    let project = tempfile::tempdir().unwrap();
    let root = project.path();
    fixture(root, "current parent");
    let parent_context = Uuid::now_v7();
    let capture = register(root, parent_context);
    let ResearchMutation::Register(origin) = capture.mutation else {
        unreachable!()
    };
    let origin_uuid = origin.version_uuid;
    let branch_uuid = Uuid::now_v7();
    let mut spec = origin.clone();
    spec.version_uuid = Uuid::now_v7();
    spec.context_uuid = branch_uuid;
    spec.source_version = Some(origin_uuid);
    let before = current(root);
    let prepared = prepare_branch_selection(
        root,
        &spec,
        Some(&origin),
        None,
        &[],
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(current(root), before);
    assert!(state(root).identities.is_empty());
    let creation = ResearchBranchRecord {
        branch_uuid,
        project_uuid: Uuid::now_v7(),
        parent_branch_uuid: None,
        origin_version_uuid: origin_uuid,
        base_version_uuid: spec.version_uuid,
        creator_uuid: Uuid::now_v7(),
        created_at: 13,
        label: "current source".into(),
        selection_sha256: [6; 32],
    };
    let operation = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: before,
        mutation: ResearchMutation::PublishBranch {
            intent_sha256: [0; 32],
            origin_capture: Some(Box::new(origin)),
            creation: Some(creation),
            version: Box::new(prepared.version.clone()),
        },
    };
    assert!(publish_research_operation(root, &operation, &AtomicBool::new(true)).is_err());
    assert_eq!(current(root), before);
    assert!(state(root).identities.is_empty());
    let receipt = execute(root, &operation);
    let registry = state(root);
    assert!(registry.identities.contains_key(&origin_uuid));
    assert!(!registry.versions.contains_key(&origin_uuid));
    assert!(!registry.heads.contains_key(&parent_context));
    assert_eq!(registry.heads[&branch_uuid], spec.version_uuid);
    assert_eq!(execute(root, &operation), receipt);
    assert_eq!(registry.versions.len(), 1);
}
