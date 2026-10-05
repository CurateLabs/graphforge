use super::super::tests::{journal_path, participant, project, publish, request};
use super::super::*;
use super::*;
use std::fs;
#[cfg(unix)]
use std::process::Command;

#[test]
fn participant_hash_work_counts_sha_and_checksum_only_when_each_stream_completes() {
    let request = request(vec![participant("graph", "nodes", b"payload")]);
    let bytes = request.participants[0].bytes.len() as u64;
    let capture = crate::concurrency_attribution::RegionCapture::start("import_command");
    let identities = ParticipantIdentities::compute(&request.participants).unwrap();
    assert_eq!(
        capture.finish().regions["import_command"].work["hashed_bytes"],
        2 * bytes
    );
    for (prepared, expected) in [(None, 2 * bytes), (Some(&identities), bytes)] {
        let capture = crate::concurrency_attribution::RegionCapture::start("import_command");
        request_metadata_with_payloads(&request, ParticipantPayloads::Memory(prepared, &[]))
            .unwrap();
        let snapshot = capture.finish();
        assert_eq!(
            snapshot.regions["import_command"].work["hashed_bytes"],
            expected
        );
        assert!(
            !snapshot.regions["import_command"]
                .work
                .contains_key("written_bytes")
        );
    }
    let mut wrong = identities;
    wrong.0[0].content_xxh64 ^= 1;
    let capture = crate::concurrency_attribution::RegionCapture::start("import_command");
    assert!(
        request_metadata_with_payloads(&request, ParticipantPayloads::Memory(Some(&wrong), &[]))
            .is_err()
    );
    assert!(capture.finish().regions["import_command"].work.is_empty());
}

#[test]
#[cfg(unix)]
fn reused_parent_participants_are_hard_linked_without_payload_hash_or_write() {
    use std::os::unix::fs::MetadataExt;

    let root = project();
    let ledger = vec![b'k'; 128 * 1024];
    let first = request(vec![
        participant("graph", "nodes", b"first graph payload"),
        participant("knowledge", "assertions", &ledger),
    ]);
    publish(root.path(), first);
    let parent = crate::resolve_project_generation(root.path()).unwrap();
    let source = parent.participant_path("knowledge", "assertions").unwrap();
    let source_metadata = fs::metadata(&source).unwrap();
    let changed = vec![participant("graph", "nodes", b"second graph payload")];
    let prepared = PreparedGenerationRequest::new_reusing_parent(
        Uuid::now_v7(),
        parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        &parent,
        &[("graph".into(), "nodes".into())],
        changed,
        |_, _| Uuid::now_v7(),
    )
    .unwrap();
    let capture = crate::concurrency_attribution::RegionCapture::start("publish");
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &prepared).unwrap()
    else {
        panic!("request unexpectedly replayed");
    };
    let staged = staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
    staged.publish().unwrap();
    let evidence = capture.finish();
    let current = crate::resolve_project_generation(root.path()).unwrap();
    let target = current.participant_path("knowledge", "assertions").unwrap();
    let target_metadata = fs::metadata(&target).unwrap();
    assert_eq!(fs::read(&target).unwrap(), ledger);
    assert_eq!(source_metadata.ino(), target_metadata.ino());
    assert_eq!(
        parent
            .participant_snapshot("knowledge", "assertions")
            .unwrap()
            .unwrap()
            .bytes,
        ledger
    );
    crate::recover_project_transactions(root.path()).unwrap();
    assert_eq!(
        current
            .participant_snapshot("knowledge", "assertions")
            .unwrap()
            .unwrap()
            .bytes,
        ledger
    );
    let (_, carry_forward) = evidence
        .regions
        .iter()
        .find(|(name, _)| name.ends_with("participant_carry_forward"))
        .expect("carry-forward phase is attributed");
    assert_eq!(
        carry_forward.work.get("participant_reused_bytes"),
        Some(&(ledger.len() as u64))
    );
    assert!(
        carry_forward
            .work
            .get("written_bytes")
            .copied()
            .unwrap_or_default()
            < ledger.len() as u64
    );
    assert!(
        carry_forward
            .work
            .get("hashed_bytes")
            .copied()
            .unwrap_or_default()
            < 2 * ledger.len() as u64
    );
    let mut corrupt = ledger.clone();
    corrupt[0] ^= 1;
    fs::write(&target, corrupt).unwrap();
    assert_eq!(
        current
            .participant_snapshot("knowledge", "assertions")
            .unwrap_err()
            .code(),
        "GF_PROJECT_CORRUPT"
    );
    fs::write(&target, &ledger).unwrap();

    let portable_path = root.path().join("round-trip.gfportable");
    crate::export_portable_project(
        &current,
        &portable_path,
        crate::PortableProjectLimits::default(),
    )
    .unwrap();
    let portable_target = root.path().join("portable-round-trip");
    let capabilities = current
        .capabilities()
        .into_iter()
        .map(|capability| ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect::<Vec<_>>();
    crate::import_portable_project_file(
        &portable_path,
        &portable_target,
        Uuid::now_v7(),
        Uuid::now_v7(),
        &capabilities,
        crate::PortableProjectLimits::default(),
    )
    .unwrap();
    assert_eq!(
        crate::resolve_project_generation(&portable_target)
            .unwrap()
            .participant_snapshot("knowledge", "assertions")
            .unwrap()
            .unwrap()
            .bytes,
        ledger
    );

    let parent_generation_uuid = parent.generation_uuid();
    let held_cleanup = crate::execute_project_cleanup(
        root.path(),
        crate::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        crate::ProjectRetentionLimits::default(),
    )
    .unwrap();
    assert!(held_cleanup.skipped_live > 0);
    assert_eq!(
        parent
            .participant_snapshot("knowledge", "assertions")
            .unwrap()
            .unwrap()
            .bytes,
        ledger
    );
    drop(parent);
    let cleanup = crate::execute_project_cleanup(
        root.path(),
        crate::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        crate::ProjectRetentionLimits::default(),
    )
    .unwrap();
    assert!(cleanup.removed > 0);
    assert!(
        !root
            .path()
            .join("generations")
            .join(parent_generation_uuid.hyphenated().to_string())
            .exists()
    );
    let after_cleanup = crate::resolve_project_generation(root.path()).unwrap();
    assert_eq!(
        after_cleanup
            .participant_snapshot("knowledge", "assertions")
            .unwrap()
            .unwrap()
            .bytes,
        ledger
    );
}

#[test]
fn reused_parent_publication_replays_immediately_and_historically() {
    let root = project();
    let ledger = b"unchanged sibling ledger";
    publish(
        root.path(),
        request(vec![
            participant("graph", "nodes", b"initial graph"),
            participant("knowledge", "assertions", ledger),
        ]),
    );
    let parent = crate::resolve_project_generation(root.path()).unwrap();
    let transaction_uuid = Uuid::from_u128(18_100_181_000);
    let child_uuid = Uuid::from_u128(18_100_181_001);
    let prepared = PreparedGenerationRequest::new_reusing_parent(
        transaction_uuid,
        parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        &parent,
        &[("graph".into(), "nodes".into())],
        vec![participant("graph", "nodes", b"updated graph")],
        |_, _| child_uuid,
    )
    .unwrap();
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &prepared).unwrap()
    else {
        panic!("first publication unexpectedly replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();

    let ProjectStageOutcome::AlreadyPublished(immediate) =
        stage_project_generation(root.path(), &prepared).unwrap()
    else {
        panic!("immediate reuse retry was not idempotent");
    };
    assert!(immediate.idempotent_replay);
    assert_eq!(immediate.generation_uuid, child_uuid);

    publish(
        root.path(),
        request(vec![
            participant("graph", "nodes", b"later graph"),
            participant("knowledge", "assertions", ledger),
        ]),
    );
    assert_ne!(
        crate::resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        child_uuid
    );
    let ProjectStageOutcome::AlreadyPublished(historical) =
        stage_project_generation(root.path(), &prepared).unwrap()
    else {
        panic!("historical reuse retry was not idempotent");
    };
    assert!(historical.idempotent_replay);
    assert_eq!(historical.generation_uuid, child_uuid);

    let conflicting = PreparedGenerationRequest::new_reusing_parent_with_generation_uuid(
        transaction_uuid,
        child_uuid,
        parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        &parent,
        &[("graph".into(), "nodes".into())],
        vec![participant("graph", "nodes", b"conflicting graph")],
    )
    .unwrap();
    let error = stage_project_generation(root.path(), &conflicting)
        .err()
        .expect("different reused request under the same transaction must conflict");
    assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
}

#[test]
#[cfg(unix)]
fn reused_completed_retry_rejects_a_symlinked_transactions_directory() {
    use std::os::unix::fs::symlink;

    let root = project();
    publish(
        root.path(),
        request(vec![
            participant("graph", "nodes", b"initial graph"),
            participant("knowledge", "assertions", b"sibling ledger"),
        ]),
    );
    let parent = crate::resolve_project_generation(root.path()).unwrap();
    let transaction_uuid = Uuid::now_v7();
    let prepared = PreparedGenerationRequest::new_reusing_parent(
        transaction_uuid,
        parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        &parent,
        &[("graph".into(), "nodes".into())],
        vec![participant("graph", "nodes", b"updated graph")],
        |_, _| Uuid::now_v7(),
    )
    .unwrap();
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &prepared).unwrap()
    else {
        panic!("new reuse request unexpectedly replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();

    let transactions = root.path().join(TRANSACTIONS_DIR);
    let transaction_copy = root.path().join("transaction-copy");
    fs::create_dir(&transaction_copy).unwrap();
    fs::copy(
        transactions.join(format!("{}.json", transaction_uuid.hyphenated())),
        transaction_copy.join(format!("{}.json", transaction_uuid.hyphenated())),
    )
    .unwrap();
    fs::remove_dir_all(&transactions).unwrap();
    symlink(&transaction_copy, &transactions).unwrap();

    let error = stage_project_generation(root.path(), &prepared)
        .err()
        .expect("a completed retry must still reject a linked transactions directory");
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
}

#[test]
fn reused_json_participant_requires_the_manifest_sha256() {
    let root = project();
    let mut json = participant("knowledge", "policy", br#"{"mode":"strict"}"#);
    json.encoding = ProjectParticipantEncoding::Json;
    publish(
        root.path(),
        request(vec![participant("graph", "nodes", b"initial graph"), json]),
    );
    let parent = crate::resolve_project_generation(root.path()).unwrap();
    let parent_uuid = parent.generation_uuid();
    let mut prepared = PreparedGenerationRequest::new_reusing_parent(
        Uuid::now_v7(),
        parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        &parent,
        &[("graph".into(), "nodes".into())],
        vec![participant("graph", "nodes", b"changed graph")],
        |_, _| Uuid::now_v7(),
    )
    .unwrap();
    assert_eq!(prepared.reused.len(), 1);
    let reused_index = prepared.reused[0].index;
    prepared.reused[0].identity.content_sha256[0] ^= 1;
    prepared.identities.0[reused_index].content_sha256[0] ^= 1;

    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &prepared).unwrap()
    else {
        panic!("new JSON reuse request unexpectedly replayed");
    };
    let validated = staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
    let error = validated
        .publish()
        .expect_err("altered JSON SHA-256 must refuse publication");
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    crate::recover_project_transactions(root.path()).unwrap();
    let current = crate::resolve_project_generation(root.path()).unwrap();
    assert_eq!(current.generation_uuid(), parent_uuid);
    assert_eq!(
        current
            .participant_snapshot("knowledge", "policy")
            .unwrap()
            .unwrap()
            .bytes,
        br#"{"mode":"strict"}"#
    );
}

#[test]
#[cfg(unix)]
fn reused_publication_reports_cross_process_writer_conflict() {
    use std::io::{BufRead, BufReader, Write};
    use std::process::Stdio;

    let root = project();
    publish(
        root.path(),
        request(vec![
            participant("graph", "nodes", b"initial graph"),
            participant("knowledge", "assertions", b"sibling ledger"),
        ]),
    );
    let parent = crate::resolve_project_generation(root.path()).unwrap();
    let prepared = PreparedGenerationRequest::new_reusing_parent(
        Uuid::now_v7(),
        parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        &parent,
        &[("graph".into(), "nodes".into())],
        vec![participant("graph", "nodes", b"updated graph")],
        |_, _| Uuid::now_v7(),
    )
    .unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "project_publication::participants::tests::reuse_writer_lock_helper",
            "--ignored",
            "--nocapture",
        ])
        .env("GRAPHFORGE_REUSE_WRITER_TEST_ROOT", root.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let mut stderr = BufReader::new(stderr);
    let mut line = String::new();
    loop {
        line.clear();
        let count = stderr.read_line(&mut line).unwrap();
        assert_ne!(count, 0, "writer lock helper exited before taking the lock");
        if line.trim() == "GRAPHFORGE_REUSE_WRITER_LOCKED" {
            break;
        }
    }

    let result = stage_project_generation(root.path(), &prepared);
    child.stdin.as_mut().unwrap().write_all(b"release").unwrap();
    assert!(child.wait().unwrap().success());
    let error = result
        .err()
        .expect("another process holding the writer lock must block reuse staging");
    assert_eq!(error.code(), "GF_WRITER_BUSY");
}

#[test]
#[ignore = "subprocess helper for reuse writer-lock contention regression"]
fn reuse_writer_lock_helper() {
    use std::io::{Read, Write};

    let Ok(root) = std::env::var("GRAPHFORGE_REUSE_WRITER_TEST_ROOT") else {
        return;
    };
    let _writer = crate::project_publication::wait_for_writer_lock(Path::new(&root)).unwrap();
    writeln!(std::io::stderr(), "GRAPHFORGE_REUSE_WRITER_LOCKED").unwrap();
    let mut release = [0_u8; 1];
    std::io::stdin().read_exact(&mut release).unwrap();
}

#[test]
fn prepared_reuse_rejects_same_bytes_from_an_injected_hard_link() {
    let root = project();
    let ledger = b"manifest-authenticated immutable ledger";
    publish(
        root.path(),
        request(vec![
            participant("graph", "nodes", b"parent graph payload"),
            participant("knowledge", "assertions", ledger),
        ]),
    );
    let parent = crate::resolve_project_generation(root.path()).unwrap();
    let parent_generation_uuid = parent.generation_uuid();
    let source = parent.participant_path("knowledge", "assertions").unwrap();
    let prepared = PreparedGenerationRequest::new_reusing_parent(
        Uuid::now_v7(),
        parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        &parent,
        &[("graph".into(), "nodes".into())],
        vec![participant("graph", "nodes", b"child graph payload")],
        |_, _| Uuid::now_v7(),
    )
    .unwrap();
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &prepared).unwrap()
    else {
        panic!("request unexpectedly replayed");
    };
    let relative = source
        .strip_prefix(parent.participants_root())
        .expect("participant source is inside the parent generation");
    let destination = staged.generation_root.join("participants").join(relative);
    let injected_dir = root.path().join("injected");
    fs::create_dir_all(&injected_dir).unwrap();
    let injected_source = injected_dir.join("same-bytes.parquet");
    fs::write(&injected_source, ledger).unwrap();
    fs::remove_file(&destination).unwrap();
    fs::hard_link(&injected_source, &destination).unwrap();
    assert!(staged.validate(|_| Ok(()), |_, _| Ok(())).is_err());

    let selected = crate::resolve_project_generation(root.path()).unwrap();
    assert_eq!(selected.generation_uuid(), parent_generation_uuid);
    assert_eq!(
        parent
            .participant_snapshot("knowledge", "assertions")
            .unwrap()
            .unwrap()
            .bytes,
        ledger
    );
    crate::recover_project_transactions(root.path()).unwrap();
}

#[test]
#[cfg(unix)]
fn failed_reuse_stage_preserves_pinned_parent_reader() {
    use std::os::unix::fs::MetadataExt;

    let root = project();
    let ledger = b"pinned reader survives failed carry-forward stage";
    publish(
        root.path(),
        request(vec![
            participant("graph", "nodes", b"parent graph payload"),
            // Sort this unchanged participant before graph/nodes so the
            // participant-fsync failpoint fires after its reuse link exists.
            participant("graph", "aaa_sibling", ledger),
        ]),
    );
    let parent = crate::resolve_project_generation(root.path()).unwrap();
    let parent_uuid = parent.generation_uuid();
    let source = parent.participant_path("graph", "aaa_sibling").unwrap();
    let source_identity = fs::metadata(&source).unwrap().ino();
    let child_uuid = Uuid::from_u128(18_100_181_002);
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "project_publication::participants::tests::reuse_failure_helper",
            "--ignored",
        ])
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINTS",
            "graphforge-internal-subprocess-v1",
        )
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINT",
            "project.after_participant_fsync.error",
        )
        .env("GRAPHFORGE_REUSE_FAILURE_TEST_ROOT", root.path())
        .env(
            "GRAPHFORGE_REUSE_FAILURE_TEST_GENERATION",
            child_uuid.hyphenated().to_string(),
        )
        .status()
        .unwrap();
    assert!(status.success(), "reuse failpoint helper failed: {status}");

    let relative = source
        .strip_prefix(parent.participants_root())
        .expect("reused participant is below the parent participant root");
    let child_participant = root
        .path()
        .join(GENERATIONS_DIR)
        .join(child_uuid.hyphenated().to_string())
        .join("participants")
        .join(relative);
    assert!(
        child_participant.exists(),
        "reuse link was not staged before failure"
    );
    assert_eq!(
        fs::metadata(&child_participant).unwrap().ino(),
        source_identity,
        "failed child staging must contain the actual pinned-parent hard link"
    );

    crate::recover_project_transactions(root.path()).unwrap();
    assert!(
        !child_participant.exists(),
        "recovery removes the failed child link"
    );
    let current = crate::resolve_project_generation(root.path()).unwrap();
    assert_eq!(current.generation_uuid(), parent_uuid);
    assert_eq!(fs::metadata(&source).unwrap().ino(), source_identity);
    assert_eq!(
        parent
            .participant_snapshot("graph", "aaa_sibling")
            .unwrap()
            .unwrap()
            .bytes,
        ledger
    );
    assert_eq!(
        current
            .participant_snapshot("graph", "aaa_sibling")
            .unwrap()
            .unwrap()
            .bytes,
        ledger
    );
}

#[test]
#[ignore = "subprocess helper for failed reuse-stage regression"]
fn reuse_failure_helper() {
    let Ok(root) = std::env::var("GRAPHFORGE_REUSE_FAILURE_TEST_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let generation_uuid =
        Uuid::parse_str(&std::env::var("GRAPHFORGE_REUSE_FAILURE_TEST_GENERATION").unwrap())
            .unwrap();
    let parent = crate::resolve_project_generation(root).unwrap();
    let changed = vec![participant("graph", "nodes", b"changed graph payload")];
    let prepared = PreparedGenerationRequest::new_reusing_parent_with_generation_uuid(
        Uuid::now_v7(),
        generation_uuid,
        parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        &parent,
        &[("graph".into(), "nodes".into())],
        changed,
    )
    .unwrap();
    let error = match stage_project_generation(root, &prepared) {
        Ok(_) => panic!("configured failpoint did not abort the carry-forward stage"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
}

#[test]
fn reused_participant_validation_rejects_injected_links_and_changed_bytes() {
    let root = tempfile::tempdir().unwrap();
    let source_dir = root.path().join("source");
    let destination_dir = root.path().join("destination");
    fs::create_dir_all(&source_dir).unwrap();
    fs::create_dir_all(&destination_dir).unwrap();
    let source = source_dir.join("payload.parquet");
    let destination = destination_dir.join("payload.parquet");
    let original = b"exact immutable payload";
    fs::write(&source, original).unwrap();
    fs::hard_link(&source, &destination).unwrap();
    let request = request(vec![participant("knowledge", "assertions", original)]);
    let (_, metadata, _) = request_metadata(&request).unwrap();
    let expected = metadata[0].clone();
    let reference = ReusedParticipantPayload {
        index: 0,
        capability_id: "knowledge".into(),
        record_family_id: "assertions".into(),
        source: source.clone(),
        parent_generation_uuid: Uuid::now_v7(),
        parent_manifest_sha256: [0; 32],
        identity: ParticipantIdentity {
            byte_length: expected.byte_length,
            content_sha256: [0; 32],
            content_xxh64: expected.content_xxh64,
        },
    };
    verify_staged_participant_file(&destination, &expected, Some(&reference), false).unwrap();

    fs::write(&source, b"altered immutable payload").unwrap();
    assert!(
        verify_staged_participant_file(&destination, &expected, Some(&reference), true).is_err()
    );

    fs::remove_file(&destination).unwrap();
    fs::write(&destination, b"altered immutable payload").unwrap();
    assert!(
        verify_staged_participant_file(&destination, &expected, Some(&reference), false).is_err()
    );
}

#[test]
fn request_fingerprint_is_independent_of_participant_input_order() {
    let mut request = request(vec![
        participant("provenance", "events", b"provenance"),
        participant("graph", "nodes", b"graph"),
    ]);
    let (_, first_metadata, first_fingerprint) = request_metadata(&request).unwrap();
    request.participants.reverse();
    let (_, second_metadata, second_fingerprint) = request_metadata(&request).unwrap();

    assert_eq!(first_metadata, second_metadata);
    assert_eq!(first_fingerprint, second_fingerprint);
    assert_eq!(first_metadata[0].capability_id, "graph");
    assert_eq!(first_metadata[1].capability_id, "provenance");
}

#[test]
fn machine_ids_match_the_committed_generation_reader_contract() {
    let root = project();
    let valid = request(vec![participant("graph", "node-properties", b"properties")]);
    let generation_uuid = valid.generation_uuid;
    publish(root.path(), valid);
    let resolved = resolve_project_generation(root.path()).unwrap();
    assert_eq!(resolved.generation_uuid(), generation_uuid);
    assert!(
        resolved
            .participant_path("graph", "node-properties")
            .unwrap()
            .is_file()
    );

    let underscore = request(vec![participant(
        "graph_data",
        "node_properties",
        b"properties",
    )]);
    publish(root.path(), underscore);
    let resolved = resolve_project_generation(root.path()).unwrap();
    assert!(
        resolved
            .participant_path("graph_data", "node_properties")
            .unwrap()
            .is_file()
    );

    let invalid = request(vec![participant("graph", "NodeProperties", b"properties")]);
    let error = stage_project_generation(root.path(), &invalid)
        .err()
        .expect("reader-incompatible machine ID must be rejected");
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
}

#[test]
fn tampered_staged_bytes_fail_before_publication() {
    let root = project();
    let parent = resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let initial_request = request(vec![participant("graph", "nodes", b"original")]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &initial_request).unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };
    std::fs::write(
        staged.generation_root.join(PARTICIPANTS_DIR).join(
            staged
                .participants
                .first()
                .expect("participant")
                .relative_path
                .as_str(),
        ),
        b"tampered",
    )
    .unwrap();

    let error = staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .err()
        .expect("tampered bytes must fail validation");

    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );

    let request = request(vec![participant("graph", "nodes", b"original")]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("new request unexpectedly replayed");
    };
    let path = staged
        .generation_root
        .join(PARTICIPANTS_DIR)
        .join(&staged.participants[0].relative_path);
    std::fs::write(path, b"short").unwrap();
    assert_eq!(
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .err()
            .expect("truncated staged bytes must fail")
            .code(),
        "GF_PUBLICATION_FAILED"
    );
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );
}

#[cfg(unix)]
#[test]
fn staged_participant_hard_link_fails_before_current_mutation() {
    let root = project();
    let parent = resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let request = request(vec![participant("graph", "nodes", b"stable")]);
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation(root.path(), &request).unwrap()
    else {
        panic!("unexpected replay")
    };
    let path = staged
        .generation_root
        .join(PARTICIPANTS_DIR)
        .join(&staged.participants[0].relative_path);
    let external = root.path().join("external-participant");
    fs::rename(&path, &external).unwrap();
    fs::hard_link(&external, &path).unwrap();

    assert_eq!(
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .err()
            .expect("hard-linked staged bytes must fail")
            .code(),
        "GF_PUBLICATION_FAILED"
    );
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        parent
    );
}

#[test]
fn malformed_generation_contracts_fail_before_staging_or_current_change() {
    let root = project();
    let before = fs::read(root.path().join(CURRENT_FILE)).unwrap();

    let mut cases = Vec::new();
    let mut no_capability = request(vec![]);
    no_capability.capabilities.clear();
    cases.push((no_capability, "at least one capability"));

    let mut zero_capability = request(vec![]);
    zero_capability.capabilities[0].capability_version = 0;
    cases.push((zero_capability, "capability contract versions"));

    let mut missing_graph = request(vec![]);
    missing_graph.capabilities[0].capability_id = "knowledge".into();
    cases.push((missing_graph, "graph capability version 1"));

    let mut duplicate_capability = request(vec![]);
    duplicate_capability
        .capabilities
        .push(duplicate_capability.capabilities[0].clone());
    cases.push((duplicate_capability, "duplicate capability identity"));

    let mut undeclared = request(vec![participant("knowledge", "events", b"event")]);
    undeclared
        .capabilities
        .retain(|entry| entry.capability_id == "graph");
    cases.push((undeclared, "participant capability is not declared"));

    let mut version_mismatch = request(vec![participant("knowledge", "events", b"event")]);
    version_mismatch.participants[0].capability_version = 2;
    cases.push((version_mismatch, "version conflicts with declaration"));

    let duplicate = participant("graph", "nodes", b"same");
    cases.push((
        request(vec![duplicate.clone(), duplicate]),
        "duplicate participant identity",
    ));

    let mut zero_record = request(vec![participant("graph", "nodes", b"node")]);
    zero_record.participants[0].record_version = 0;
    cases.push((zero_record, "participant contract versions"));

    let mut invalid_id = request(vec![participant("graph", "nodes", b"node")]);
    invalid_id.participants[0].record_family_id = "../nodes".into();
    cases.push((invalid_id, "machine ID"));

    for (candidate, expected) in cases {
        let error = stage_project_generation(root.path(), &candidate)
            .err()
            .expect("malformed request must fail");
        assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(fs::read(root.path().join(CURRENT_FILE)).unwrap(), before);
        assert!(!journal_path(root.path(), candidate.transaction_uuid).exists());
    }
}

#[test]
fn staged_participant_file_kind_matrix_fails_before_current_mutation() {
    for kind in ["missing", "directory", "symlink"] {
        let root = project();
        let parent = resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        let request = request(vec![participant("graph", "nodes", b"stable")]);
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("unexpected replay")
        };
        let path = staged
            .generation_root
            .join(PARTICIPANTS_DIR)
            .join(&staged.participants.first().unwrap().relative_path);
        std::fs::remove_file(&path).unwrap();
        match kind {
            "missing" => {}
            "directory" => std::fs::create_dir(&path).unwrap(),
            "symlink" => {
                #[cfg(unix)]
                std::os::unix::fs::symlink(root.path().join(CURRENT_FILE), &path).unwrap();
                #[cfg(not(unix))]
                std::fs::create_dir(&path).unwrap();
            }
            _ => unreachable!(),
        }
        let error = match staged.validate(|_| Ok(()), |_, _| Ok(())) {
            Ok(_) => panic!("hostile staged participant must fail"),
            Err(error) => error,
        };
        let expected_code = if kind == "missing" {
            "GF_IO"
        } else {
            "GF_PUBLICATION_FAILED"
        };
        assert_eq!(error.code(), expected_code);
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            parent
        );
    }
}
