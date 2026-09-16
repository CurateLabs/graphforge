use std::collections::BTreeMap;
use std::fs;

use super::*;
use crate::open_or_initialize_project;

fn allocation_fixture_files(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
            } else {
                files.push(entry.path());
            }
        }
    }
    files
}

#[test]
fn allocation_observed_admitted_publication_matches_reopened_file_union() {
    let root = project();
    let operation = crate::StorageAllocationOperation::default();
    for path in allocation_fixture_files(root.path()) {
        operation
            .replace_file_at(&path, &File::open(&path).unwrap())
            .unwrap();
    }
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        root.path(),
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )
    .unwrap();
    let parent = resolve_project_generation(root.path()).unwrap();
    let request = request(vec![participant("graph", "nodes", &vec![3_u8; 16384])]);
    let ProjectStageOutcome::Staged(staged) = stage_project_generation_from_admitted_parent(
        admission,
        parent,
        &request,
        None,
        Some(&operation),
    )
    .unwrap() else {
        panic!("unexpected replay")
    };
    let receipt = staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        receipt.generation_uuid
    );
    let mut identities = BTreeMap::new();
    for path in allocation_fixture_files(root.path()) {
        let file = File::open(path).unwrap();
        let identity = graphforge_filesystem::file_identity(&file).unwrap();
        identities.insert(
            (identity.volume_serial, identity.file_id),
            graphforge_filesystem::file_space_usage(&file)
                .unwrap()
                .allocated_bytes,
        );
    }
    let actual: u64 = identities.values().sum();
    let (current, peak) = operation.totals().unwrap();
    assert_eq!(current, actual);
    assert!(peak >= current);
}

pub(super) fn participant(capability: &str, family: &str, value: &[u8]) -> ProjectParticipant {
    ProjectParticipant {
        capability_id: capability.into(),
        capability_version: 1,
        record_family_id: family.into(),
        record_version: 1,
        encoding: ProjectParticipantEncoding::Parquet,
        schema_fingerprint: Sha256::digest(format!("{capability}/{family}")).into(),
        row_count: 1,
        bytes: value.to_vec(),
    }
}

pub(super) fn request(participants: Vec<ProjectParticipant>) -> ProjectGenerationRequest {
    let mut capabilities = vec![ProjectCapability {
        capability_id: "graph".into(),
        capability_version: 1,
    }];
    for participant in &participants {
        if participant.capability_id != "graph"
            && !capabilities
                .iter()
                .any(|entry| entry.capability_id == participant.capability_id)
        {
            capabilities.push(ProjectCapability {
                capability_id: participant.capability_id.clone(),
                capability_version: participant.capability_version,
            });
        }
    }
    ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities,
        participants,
    }
}

pub(super) fn project() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    open_or_initialize_project(root.path()).unwrap();
    root
}

pub(super) fn publish(root: &Path, request: ProjectGenerationRequest) -> ProjectPublicationReceipt {
    let ProjectStageOutcome::Staged(staged) = stage_project_generation(root, &request).unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap()
}

#[test]
fn publishes_and_reopens_compact_graph_files_v2_root() {
    let root = project();
    let workspace = tempfile::tempdir().unwrap();
    let relative = std::path::PathBuf::from("topology/edges/knows.parquet");
    fs::create_dir_all(workspace.path().join(relative.parent().unwrap())).unwrap();
    fs::write(
        workspace.path().join(&relative),
        b"immutable topology payload",
    )
    .unwrap();
    let mut state = crate::GraphManifestState::empty();
    let lease = crate::begin_graph_object_publication(root.path()).unwrap();
    let (files_root, _) =
        crate::append_graph_files_v2(&lease, workspace.path(), &mut state, &[relative], &[])
            .unwrap();
    let request = request(vec![
        crate::graph_files_root_participant(&files_root).unwrap(),
    ]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish_with_graph_objects(&lease)
        .unwrap();
    drop(lease);

    let reopened = resolve_project_generation(root.path()).unwrap();
    let inventory = reopened.graph_files_inventory().unwrap().unwrap();
    assert_eq!(
        inventory.files,
        state.entries().cloned().collect::<Vec<_>>()
    );
    assert!(!reopened.graph_tree_root().exists());
}

#[test]
fn corrupt_compact_root_never_advances_current() {
    let root = project();
    let prior = resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let compact = crate::GraphFilesRootV2 {
        format: crate::GRAPH_FILES_V2_FORMAT.into(),
        format_version: crate::GRAPH_FILES_V2_VERSION,
        root_node_sha256: "0".repeat(64),
        logical_file_count: 1,
        logical_byte_length: 1,
    };
    let request = request(vec![crate::graph_files_root_participant(&compact).unwrap()]);
    assert!(stage_project_generation(root.path(), &request).is_err());
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        prior
    );
}

#[test]
fn compact_payload_is_reverified_only_at_the_lease_backed_commit_boundary() {
    let root = project();
    let parent = resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let workspace = tempfile::tempdir().unwrap();
    let relative = std::path::PathBuf::from("topology/edges/knows.parquet");
    fs::create_dir_all(workspace.path().join(relative.parent().unwrap())).unwrap();
    fs::write(
        workspace.path().join(&relative),
        b"immutable topology payload",
    )
    .unwrap();
    let mut state = crate::GraphManifestState::empty();
    let lease = crate::begin_graph_object_publication(root.path()).unwrap();
    let (files_root, _) =
        crate::append_graph_files_v2(&lease, workspace.path(), &mut state, &[relative], &[])
            .unwrap();
    let payload_digest = state
        .entries()
        .next()
        .expect("one compact payload")
        .content_sha256
        .clone();
    let request = request(vec![
        crate::graph_files_root_participant(&files_root).unwrap(),
    ]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };

    crate::graph_object_store::corrupt_sealed_graph_object_for_test(
        &crate::graph_object_path(root.path(), &payload_digest).unwrap(),
        b"corrupt",
    );
    let validated = staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .expect("intermediate validation must not rehash compact payloads");
    let error = validated
        .publish_with_graph_objects(&lease)
        .expect_err("final lease-backed verification must reject corruption");
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );
    assert!(
        root.path()
            .join(GENERATIONS_DIR)
            .join(request.generation_uuid.hyphenated().to_string())
            .exists()
    );

    drop(lease);
    let report = crate::recover_project_transactions(root.path()).unwrap();
    assert_eq!(report.aborted_journals, 1);
    assert_eq!(report.removed_generations, 1);
    assert!(
        !root
            .path()
            .join(GENERATIONS_DIR)
            .join(request.generation_uuid.hyphenated().to_string())
            .exists()
    );
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );
}

#[test]
fn expanded_graph_tree_corruption_still_fails_intermediate_validation() {
    let root = project();
    let workspace = tempfile::tempdir().unwrap();
    let relative = std::path::PathBuf::from("topology/edges/knows.parquet");
    fs::create_dir_all(workspace.path().join(relative.parent().unwrap())).unwrap();
    fs::write(workspace.path().join(&relative), b"expanded graph payload").unwrap();
    let (_, files) = crate::capture_graph_files(workspace.path()).unwrap();
    let request = request(vec![files]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_with_graph_tree(root.path(), &request, Some(workspace.path()))
            .unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };
    fs::write(
        crate::graph_tree_root(&staged.generation_root).join(relative),
        b"corrupt",
    )
    .unwrap();

    let error = match staged.validate(|_| Ok(()), |_, _| Ok(())) {
        Ok(_) => panic!("expanded generation tree must remain verified"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
}

pub(super) fn journal_path(root: &Path, transaction_uuid: Uuid) -> PathBuf {
    root.join(TRANSACTIONS_DIR)
        .join(format!("{}.json", transaction_uuid.hyphenated()))
}

#[test]
fn publishes_graph_only_and_multi_domain_sets_atomically() {
    for participants in [
        vec![participant("graph", "nodes", b"graph")],
        vec![
            participant("graph", "nodes", b"graph"),
            participant("provenance", "events", b"provenance"),
        ],
        vec![
            participant("graph", "nodes", b"graph"),
            participant("provenance", "events", b"provenance"),
            participant("knowledge", "assertions", b"knowledge"),
        ],
    ] {
        let root = project();
        let request = request(participants);
        let expected = request.generation_uuid;
        publish(root.path(), request);
        let resolved = resolve_project_generation(root.path()).unwrap();
        assert_eq!(resolved.generation_uuid(), expected);
    }
}

#[test]
fn validation_failure_leaves_parent_authoritative() {
    let root = project();
    let parent = resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let request = request(vec![participant("graph", "nodes", b"new")]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };

    let error = staged
        .validate(
            |_| Err(GfError::Validation("domain rejected".into())),
            |_, _| Ok(()),
        )
        .err()
        .expect("validation must fail");

    assert!(matches!(error, GfError::Validation(_)));
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );
}

#[test]
fn reader_preparation_failure_preserves_current_and_recovers_candidate() {
    let root = project();
    let parent = resolve_project_generation(root.path()).unwrap();
    let current_bytes = fs::read(root.path().join("CURRENT")).unwrap();
    let request = request(vec![participant("graph", "nodes", b"candidate")]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };
    let validated = staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
    let refusal = || GfError::Project {
        code: ProjectErrorCode::WriteConflict,
        message: "reader preparation deliberately refused candidate".into(),
    };
    let mut calls = 0;
    let error = validated
        .publish_with_reader_preparation(None, &mut |candidate| {
            calls += 1;
            assert_eq!(candidate.generation_uuid(), request.generation_uuid);
            assert_eq!(
                candidate
                    .participant_snapshot("graph", "nodes")?
                    .unwrap()
                    .bytes,
                b"candidate"
            );
            assert_eq!(
                fs::read(root.path().join("CURRENT")).unwrap(),
                current_bytes
            );
            Err(refusal())
        })
        .unwrap_err();
    assert_eq!(calls, 1);
    assert_eq!(error.code(), refusal().code());
    assert_eq!(error.to_string(), refusal().to_string());
    assert_eq!(
        fs::read(root.path().join("CURRENT")).unwrap(),
        current_bytes
    );
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent.generation_uuid()
    );
    let candidate_path = root
        .path()
        .join(GENERATIONS_DIR)
        .join(request.generation_uuid.hyphenated().to_string());
    assert!(
        candidate_path.exists(),
        "preparation runs against a durable candidate"
    );
    let recovered = crate::recover_project_transactions(root.path()).unwrap();
    assert_eq!(recovered.aborted_journals, 1);
    assert_eq!(recovered.removed_generations, 1);
    assert!(!candidate_path.exists());
    assert_eq!(
        fs::read(root.path().join("CURRENT")).unwrap(),
        current_bytes
    );
    assert!(
        published_project_transaction(root.path(), request.transaction_uuid)
            .unwrap()
            .is_none()
    );
}

#[test]
fn durable_install_io_failure_is_wrapped_before_current_changes() {
    let root = project();
    let parent = resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let request = request(vec![participant("graph", "nodes", b"new")]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("new request unexpectedly replayed")
    };
    let validated = staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
    fs::remove_file(
        validated
            .0
            .generation_root
            .join(PARTICIPANTS_DIR)
            .join(&validated.0.participants[0].relative_path),
    )
    .unwrap();
    let error = validated.publish().unwrap_err();
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    assert!(error.to_string().contains("phase=DURABLE committed=false"));
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );
}

#[test]
fn journal_records_each_deterministic_publication_phase() {
    let root = project();
    let request = request(vec![participant("graph", "nodes", b"new")]);
    let journal_path = journal_path(root.path(), request.transaction_uuid);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };
    assert_eq!(
        read_journal(&journal_path).unwrap().phase,
        JournalPhase::Staged
    );

    let validated = staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
    assert_eq!(
        read_journal(&journal_path).unwrap().phase,
        JournalPhase::Validated
    );

    validated.publish().unwrap();
    let published = read_journal(&journal_path).unwrap();
    assert_eq!(published.phase, JournalPhase::Published);
    assert!(published.generation_manifest_sha256.is_some());
}

#[test]
fn identical_published_transaction_is_idempotent() {
    let root = project();
    let request = request(vec![participant("graph", "nodes", b"same")]);
    publish(root.path(), request.clone());

    let ProjectStageOutcome::AlreadyPublished(receipt) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("identical replay was not recognized");
    };
    assert!(receipt.idempotent_replay);
}

#[test]
fn historical_published_transaction_remains_idempotent() {
    let root = project();
    let first = request(vec![participant("graph", "nodes", b"first")]);
    publish(root.path(), first.clone());
    publish(
        root.path(),
        request(vec![participant("graph", "nodes", b"second")]),
    );

    let ProjectStageOutcome::AlreadyPublished(receipt) =
        stage_project_generation(root.path(), &first).unwrap()
    else {
        panic!("historical identical replay was not recognized");
    };
    assert!(receipt.idempotent_replay);
}

#[test]
fn changed_content_under_same_transaction_conflicts() {
    let root = project();
    let request = request(vec![participant("graph", "nodes", b"first")]);
    publish(root.path(), request.clone());
    let mut conflicting = request;
    conflicting.participants[0].bytes = b"different".to_vec();

    let error = stage_project_generation(root.path(), &conflicting)
        .err()
        .expect("conflicting replay must fail");

    assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
}

#[test]
fn interrupted_stage_and_tampered_published_replay_are_exactly_classified() {
    let root = project();
    let staged_request = request(vec![participant("graph", "nodes", b"staged")]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &staged_request).unwrap()
    else {
        panic!("fresh transaction unexpectedly replayed")
    };
    drop(staged);
    let interrupted = stage_project_generation(root.path(), &staged_request)
        .err()
        .expect("interrupted stage must fail");
    assert_eq!(interrupted.code(), "GF_PUBLICATION_FAILED");
    assert!(interrupted.to_string().contains("requires recovery"));

    let published_request = request(vec![participant("graph", "nodes", b"published")]);
    let receipt = publish(root.path(), published_request.clone());
    let manifest = root
        .path()
        .join(GENERATIONS_DIR)
        .join(receipt.generation_uuid.hyphenated().to_string())
        .join(MANIFEST_FILE);
    fs::write(&manifest, b"tampered\n").unwrap();
    assert_eq!(
        stage_project_generation(root.path(), &published_request)
            .err()
            .expect("tampered published replay must fail")
            .code(),
        "GF_PROJECT_CORRUPT"
    );
}

#[test]
fn optimistic_promotion_refuses_an_existing_generation_destination() {
    let root = project();
    let request = request(vec![participant("graph", "nodes", b"optimistic")]);
    let operation: [u8; 32] = Sha256::digest(b"wave7-existing-destination").into();
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_optimistic(root.path(), &request, operation).unwrap()
    else {
        panic!("fresh optimistic transaction unexpectedly replayed")
    };
    let destination = root
        .path()
        .join(GENERATIONS_DIR)
        .join(request.generation_uuid.hyphenated().to_string());
    fs::create_dir(&destination).unwrap();
    let error = promote_optimistic_generation(&staged).unwrap_err();
    assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
    assert!(error.to_string().contains("generation_exists"));
    assert!(staged.generation_root.is_dir());
}

#[test]
fn reader_pinned_to_parent_survives_publication() {
    let root = project();
    let parent = resolve_project_generation(root.path()).unwrap();
    let request = request(vec![participant("graph", "nodes", b"new")]);
    let child = request.generation_uuid;

    publish(root.path(), request);

    assert_ne!(parent.generation_uuid(), child);
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        child
    );
    assert!(parent.generation_root().exists());
}

#[test]
fn admitted_parent_from_another_root_is_a_publication_failure() {
    let admitted_root = project();
    let other_root = project();
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        admitted_root.path(),
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )
    .unwrap();
    let other_parent = resolve_project_generation(other_root.path()).unwrap();
    let request = request(vec![participant("graph", "nodes", b"wrong-root")]);

    let error = stage_project_generation_from_admitted_parent(
        admission,
        other_parent,
        &request,
        None,
        None,
    )
    .err()
    .expect("a prepared parent from another root must fail");

    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    assert!(
        error
            .to_string()
            .contains("prepared generation does not belong")
    );
}

#[test]
fn optimistic_attempts_stage_concurrently_and_compare_parent_at_commit() {
    let root = project();
    let first = request(vec![participant("graph", "nodes", b"first")]);
    let second = request(vec![participant("graph", "nodes", b"second")]);
    let first_operation: [u8; 32] = Sha256::digest(b"logical-first").into();
    let second_operation: [u8; 32] = Sha256::digest(b"logical-second").into();

    let ProjectStageOutcome::Staged(first_staged) =
        stage_project_generation_optimistic(root.path(), &first, first_operation).unwrap()
    else {
        panic!("first optimistic operation replayed unexpectedly");
    };
    let ProjectStageOutcome::Staged(second_staged) =
        stage_project_generation_optimistic(root.path(), &second, second_operation).unwrap()
    else {
        panic!("second optimistic operation replayed unexpectedly");
    };

    let first_validated = first_staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
    let second_validated = second_staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
    first_validated.publish().unwrap();
    let error = second_validated
        .publish()
        .expect_err("stale optimistic parent must not publish");
    assert_eq!(error.code(), "GF_WRITE_CONFLICT");
    assert!(
        !root
            .path()
            .join(GENERATIONS_DIR)
            .join(second.generation_uuid.hyphenated().to_string())
            .exists()
    );

    let mut rebased = second.clone();
    rebased.participants[0].bytes = b"second-rebased".to_vec();
    let ProjectStageOutcome::Staged(rebased) =
        stage_project_generation_optimistic(root.path(), &rebased, second_operation).unwrap()
    else {
        panic!("aborted optimistic operation did not permit a rebase attempt");
    };
    rebased
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        second.generation_uuid
    );
}

#[test]
fn optimistic_validation_conflict_aborts_only_its_own_rebase_attempt() {
    let root = project();
    let mut stale_request = request(vec![participant("graph", "nodes", b"attempt")]);
    let operation: [u8; 32] = Sha256::digest(b"logical-validation-rebase").into();
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_optimistic(root.path(), &stale_request, operation).unwrap()
    else {
        panic!("optimistic operation replayed unexpectedly");
    };

    publish(
        root.path(),
        request(vec![participant("graph", "nodes", b"concurrent")]),
    );
    let error = staged
        .validate(
            |_| Ok(()),
            |parent, _| {
                let current = resolve_project_generation(root.path())?;
                if current.generation_uuid() != parent.generation_uuid() {
                    return Err(project_error(
                        ProjectErrorCode::WriteConflict,
                        "project generation changed before composite validation",
                    ));
                }
                Ok(())
            },
        )
        .err()
        .expect("stale optimistic validation must request a rebase");
    assert_eq!(error.code(), "GF_WRITE_CONFLICT");
    assert_eq!(
        read_journal(&journal_path(root.path(), stale_request.transaction_uuid,))
            .unwrap()
            .phase,
        JournalPhase::Aborted
    );

    let different_operation: [u8; 32] = Sha256::digest(b"different-operation").into();
    let identity_error =
        stage_project_generation_optimistic(root.path(), &stale_request, different_operation)
            .err()
            .expect("different logical identity must not reuse the aborted transaction");
    assert_eq!(identity_error.code(), "GF_IDEMPOTENCY_CONFLICT");

    stale_request.participants[0].bytes = b"rebased-attempt".to_vec();
    let ProjectStageOutcome::Staged(rebased) =
        stage_project_generation_optimistic(root.path(), &stale_request, operation).unwrap()
    else {
        panic!("aborted optimistic operation did not permit a rebase attempt");
    };
    rebased
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
}

#[test]
fn aborted_optimistic_replay_fails_closed_when_generation_cleanup_is_incomplete() {
    let root = project();
    let request = request(vec![participant("graph", "nodes", b"attempt")]);
    let operation: [u8; 32] = Sha256::digest(b"aborted-cleanup-contract").into();
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_optimistic(root.path(), &request, operation).unwrap()
    else {
        panic!("optimistic operation replayed unexpectedly");
    };
    abort_stale_generation(&staged).unwrap();
    drop(staged);

    let leftover = root
        .path()
        .join(GENERATIONS_DIR)
        .join(request.generation_uuid.hyphenated().to_string());
    std::fs::create_dir(&leftover).unwrap();
    let error = stage_project_generation_optimistic(root.path(), &request, operation)
        .err()
        .expect("incomplete aborted cleanup must fail closed");
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    assert!(error.to_string().contains("cleanup is incomplete"));
    assert!(leftover.exists());
    assert_eq!(
        read_journal(&journal_path(root.path(), request.transaction_uuid))
            .unwrap()
            .phase,
        JournalPhase::Aborted
    );
}

#[test]
fn optimistic_promotion_closes_staged_handles_before_directory_rename() {
    let root = project();
    let request = request(vec![participant("graph", "nodes", b"promoted")]);
    let generation_uuid = request.generation_uuid;
    let operation: [u8; 32] = Sha256::digest(b"windows-promotion-handles").into();

    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_optimistic(root.path(), &request, operation).unwrap()
    else {
        panic!("optimistic operation replayed unexpectedly");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();

    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        generation_uuid
    );
}

#[test]
fn optimistic_transaction_identity_has_one_live_attempt() {
    let root = project();
    let request = request(vec![participant("graph", "nodes", b"attempt")]);
    let operation: [u8; 32] = Sha256::digest(b"logical-attempt").into();
    let first = stage_project_generation_optimistic(root.path(), &request, operation).unwrap();

    let error = stage_project_generation_optimistic(root.path(), &request, operation)
        .err()
        .expect("duplicate live attempt must be rejected");
    assert_eq!(error.code(), "GF_WRITER_BUSY");
    drop(first);
}

#[test]
fn recovery_preserves_live_optimistic_attempt_then_cleans_it_after_release() {
    let root = project();
    let request = request(vec![participant("graph", "nodes", b"live")]);
    let operation: [u8; 32] = Sha256::digest(b"logical-live").into();
    let (_, _, request_fingerprint) = request_metadata(&request).unwrap();
    let generation_path = root
        .path()
        .join(ATTEMPTS_DIR)
        .join(request.transaction_uuid.hyphenated().to_string())
        .join(request_fingerprint);
    let staged = stage_project_generation_optimistic(root.path(), &request, operation).unwrap();

    let live_report = crate::recover_project_transactions(root.path()).unwrap();
    assert_eq!(live_report.aborted_journals, 0);
    assert_eq!(live_report.removed_generations, 0);
    assert!(generation_path.exists());

    drop(staged);
    let abandoned_report = crate::recover_project_transactions(root.path()).unwrap();
    assert_eq!(abandoned_report.aborted_journals, 1);
    assert_eq!(abandoned_report.removed_generations, 1);
    assert!(!generation_path.exists());
}

#[test]
fn writer_lock_is_nonblocking_and_fail_closed() {
    let root = project();
    let lock_dir = root.path().join(LOCKS_DIR);
    std::fs::create_dir_all(&lock_dir).unwrap();
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_dir.join(WRITER_LOCK_FILE))
        .unwrap();
    crate::file_lock::lock_exclusive(&lock).unwrap();
    let parent = resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();

    let error = stage_project_generation(
        root.path(),
        &request(vec![participant("graph", "nodes", b"new")]),
    )
    .err()
    .expect("busy writer must fail");

    assert_eq!(error.code(), "GF_WRITER_BUSY");
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );
}

#[test]
fn commit_lock_guard_unlocks_before_a_cloned_descriptor_closes() {
    let root = project();
    let writer = wait_for_writer_lock(root.path()).unwrap();
    let inherited_descriptor = writer.try_clone().unwrap();

    drop(CommitLock(writer));

    let contender = open_regular_lock(&root.path().join(LOCKS_DIR).join(WRITER_LOCK_FILE)).unwrap();
    assert!(crate::file_lock::try_lock_exclusive(&contender).unwrap());
    crate::file_lock::unlock(&contender).unwrap();
    drop(inherited_descriptor);
}

#[test]
fn publication_error_redacts_unsafe_cause_and_digest_parser_is_canonical() {
    let transaction = Uuid::now_v7();
    let generation = Uuid::now_v7();
    let error = publication_error_from_parts(
        transaction,
        generation,
        "STAGED",
        false,
        "bad/path:\nsecret=<value>!",
    );
    let text = error.to_string();
    assert!(text.contains("phase=STAGED committed=false cause=badpathsecretvalue"));
    assert!(!text.contains('/') && !text.contains('<') && !text.contains('!'));

    let bytes = [0xabu8; 32];
    let canonical = hex_digest(bytes);
    assert_eq!(parse_digest(&canonical), Some(bytes));
    for malformed in ["", "ab", &"A".repeat(64), &"g".repeat(64)] {
        assert_eq!(parse_digest(malformed), None);
    }
}

#[test]
fn published_transaction_probe_verifies_durable_manifest_on_reopen() {
    let root = project();
    let input = request(vec![
        participant("graph", "nodes", b"nodes"),
        participant("graph", "edges", b"edges"),
    ]);
    let receipt = publish(root.path(), input.clone());
    let probed = published_project_transaction(root.path(), input.transaction_uuid)
        .unwrap()
        .unwrap();
    assert_eq!(probed.transaction_uuid, receipt.transaction_uuid);
    assert_eq!(probed.generation_uuid, receipt.generation_uuid);
    assert_eq!(
        probed.generation_manifest_sha256,
        receipt.generation_manifest_sha256
    );
    assert!(probed.idempotent_replay);
    assert!(
        published_project_transaction(root.path(), Uuid::now_v7())
            .unwrap()
            .is_none()
    );

    let reopened = resolve_project_generation(root.path()).unwrap();
    assert_eq!(reopened.generation_uuid(), receipt.generation_uuid);
    reopened.validate_complete_participant_inventory().unwrap();
    drop(reopened);

    let manifest = root
        .path()
        .join(GENERATIONS_DIR)
        .join(receipt.generation_uuid.hyphenated().to_string())
        .join(MANIFEST_FILE);
    fs::write(&manifest, b"tampered\n").unwrap();
    let error = published_project_transaction(root.path(), input.transaction_uuid).unwrap_err();
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
    assert!(error.to_string().contains("does not match its journal"));
}

#[test]
fn replacement_error_reconciliation_never_reports_a_committed_child_as_false() {
    let root = project();
    let receipt = publish(
        root.path(),
        request(vec![participant("graph", "nodes", b"nodes")]),
    );
    let state_unknown =
        AtomicPublishError::Replacement(graphforge_filesystem::ReplaceFileError::StateUnknown(
            std::io::Error::other("injected replacement status"),
        ));

    reconcile_current_replacement_error(
        root.path(),
        receipt.transaction_uuid,
        receipt.generation_uuid,
        receipt.generation_manifest_sha256,
        &state_unknown,
    )
    .unwrap();

    let error = reconcile_current_replacement_error(
        root.path(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        [0xabu8; 32],
        &state_unknown,
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    assert!(error.to_string().contains("committed=false"));

    std::fs::write(root.path().join(CURRENT_FILE), b"{torn\n").unwrap();
    let error = reconcile_current_replacement_error(
        root.path(),
        Uuid::now_v7(),
        receipt.generation_uuid,
        receipt.generation_manifest_sha256,
        &state_unknown,
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
}

#[test]
fn generic_eintr_is_reconciled_and_never_masquerades_as_cancellation() {
    let root = project();
    let interrupted = AtomicPublishError::from(std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "injected unrelated EINTR",
    ));
    assert!(matches!(interrupted, AtomicPublishError::Io(_)));
    let error = reconcile_current_replacement_error(
        root.path(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        [0x42; 32],
        &interrupted,
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    assert!(error.to_string().contains("committed=false"));

    let cancellation = AtomicPublishError::from(std::io::Error::other(CancelledBeforeReplace));
    assert!(matches!(
        cancellation,
        AtomicPublishError::CancelledBeforeReplace
    ));
}

#[test]
fn wave9_journal_metadata_and_lock_aliases_fail_closed() {
    let root = project();
    let journal = root.path().join(TRANSACTIONS_DIR).join("hostile.json");
    std::fs::create_dir_all(journal.parent().unwrap()).unwrap();

    std::fs::create_dir(&journal).unwrap();
    assert_eq!(
        read_journal(&journal).unwrap_err().code(),
        "GF_PROJECT_CORRUPT"
    );
    std::fs::remove_dir(&journal).unwrap();
    std::fs::write(&journal, vec![b'x'; MAX_JOURNAL_BYTES as usize + 1]).unwrap();
    assert_eq!(
        read_journal(&journal).unwrap_err().code(),
        "GF_PROJECT_CORRUPT"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        std::fs::remove_file(&journal).unwrap();
        let target = root.path().join("caller-journal");
        std::fs::write(&target, b"caller bytes").unwrap();
        symlink(&target, &journal).unwrap();
        assert_eq!(
            read_journal(&journal).unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );

        let owner = root.path().join("lock-owner");
        let alias = root.path().join("lock-alias");
        std::fs::write(&owner, b"").unwrap();
        std::fs::hard_link(&owner, &alias).unwrap();
        assert_eq!(
            open_regular_lock(&alias).unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );
        assert!(owner.exists());
    }
}

#[test]
fn machine_directory_and_lock_reject_hostile_path_components_without_replacement() {
    let root = tempfile::tempdir().unwrap();
    for relative in [
        Path::new("../escape"),
        Path::new("/absolute"),
        Path::new("safe/../escape"),
    ] {
        assert_eq!(
            ensure_machine_directory(root.path(), relative)
                .unwrap_err()
                .code(),
            "GF_PROJECT_CORRUPT"
        );
    }
    let file = root.path().join("owned");
    std::fs::write(&file, b"caller bytes").unwrap();
    assert_eq!(
        ensure_machine_directory(root.path(), Path::new("owned/child"))
            .unwrap_err()
            .code(),
        "GF_PROJECT_CORRUPT"
    );
    assert_eq!(std::fs::read(&file).unwrap(), b"caller bytes");

    let lock = root.path().join("lock");
    std::fs::create_dir(&lock).unwrap();
    assert_eq!(
        open_regular_lock(&lock).unwrap_err().code(),
        "GF_PROJECT_CORRUPT"
    );
    assert!(lock.is_dir());
}

#[test]
fn public_transaction_probe_distinguishes_absent_staged_and_durable_publication() {
    let root = project();
    let request = request(vec![participant("graph", "nodes", b"rows")]);

    assert!(
        published_project_transaction(root.path(), request.transaction_uuid)
            .unwrap()
            .is_none()
    );
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("fresh transaction unexpectedly replayed");
    };
    assert!(
        published_project_transaction(root.path(), request.transaction_uuid)
            .unwrap()
            .is_none()
    );
    let receipt = staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    let reopened = published_project_transaction(root.path(), request.transaction_uuid)
        .unwrap()
        .unwrap();
    assert_eq!(reopened.generation_uuid, receipt.generation_uuid);
    assert_eq!(
        reopened.generation_manifest_sha256,
        receipt.generation_manifest_sha256
    );
    assert!(reopened.idempotent_replay);
}
