use super::super::tests::{node_batch, open, shape_temporary_names};
use super::super::*;
use super::*;

use tempfile::TempDir;

#[test]
fn completed_shape_from_another_session_clock_is_refused() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 14_160);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    assert_eq!(
        shape.runtime_catalog_now_micros,
        session.checkpoint.session_now_micros
    );
    recover_shape_intent(&session.root, &mut session.checkpoint).unwrap();

    // Isolate the clock binding: all other checkpoint, manifest, receipt and
    // payload authorities remain valid. Removing only the recovery comparison
    // must make this refusal assertion fail, not another authentication check.
    session.checkpoint.session_now_micros += 1;
    let error = recover_shape_intent(&session.root, &mut session.checkpoint)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("complete shape manifest inventory is incomplete"),
        "{error}"
    );
}

#[test]
fn shaped_writer_capability_resume_adopts_only_the_exact_receipt() {
    let temporary = TempDir::new().unwrap();
    let root = StableDirectory::open(temporary.path()).unwrap();
    std::fs::write(temporary.path().join("shaped-identities.run"), b"identity").unwrap();
    let (receipt, _) = receipt_for_existing_with_work(&root, "shaped-identities.run").unwrap();

    persist_shape_receipt(&root, &receipt).unwrap();
    persist_shape_receipt(&root, &receipt).unwrap();

    let mut mismatched = receipt;
    mismatched.xxh64 = "0".repeat(16);
    let error = persist_shape_receipt(&root, &mismatched)
        .unwrap_err()
        .to_string();
    assert!(error.contains("shaped writer capability changed"));
}

#[test]
fn shaping_cancellation_is_recovered_on_reopen() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 8_003);
    for chunk in 0..4 {
        session
            .append(
                ConstructionChunkKind::Node,
                &format!("nodes-{chunk}"),
                &node_batch(1 + chunk * 8, 8),
            )
            .unwrap();
    }
    session.seal().unwrap();
    let mut polls = 0;
    assert!(
        session
            .shape_canonical_with_cancellation(|| {
                polls += 1;
                polls > 6
            })
            .is_err()
    );
    drop(session);
    let mut resumed = GraphConstructionSession::open(
        root.path(),
        Uuid::from_u128(8_003),
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    resumed.shape_canonical_with_cancellation(|| false).unwrap();
}

/// A session with two staged identities routed into one partition, ready for
/// the output publication under test.
fn routed_partitioner<'a>(
    root: &'a StableDirectory,
    plan: &super::super::partition::PartitionPlan,
    evidence: &mut GraphConstructionEvidence,
) -> super::super::partition_shaping::FixedRangePartitioner<'a, 16> {
    use super::super::partition_shaping::{FixedRangePartitioner, PartitionFamily};
    let keys = [1_u128.to_be_bytes(), 2_u128.to_be_bytes()];
    let mut partitioner =
        FixedRangePartitioner::<16>::new(root, PartitionFamily::Identities, 1, None, true).unwrap();
    for key in &keys {
        partitioner.route(plan, key, key, evidence).unwrap();
    }
    partitioner
}

#[test]
fn partition_output_failures_finalize_release_and_remove_unpublished_outputs() {
    use super::super::partition::PartitionPlan;
    let plan = PartitionPlan::single(1);

    let root = TempDir::new().unwrap();
    let mut session = open(&root, 8_031);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let GraphConstructionSession {
        root: session_root,
        checkpoint,
        ..
    } = &mut session;
    let partitioner = routed_partitioner(session_root, &plan, &mut checkpoint.evidence);
    let error = partitioner
        .finish_optional(
            "staged-identities.run",
            0,
            false,
            &mut || true,
            &mut checkpoint.evidence,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("construction cancelled"), "{error}");
    assert!(!error.contains(root.path().to_string_lossy().as_ref()));
    assert!(shape_temporary_names(&session.root).is_empty());
    assert!(
        session
            .root
            .open_child_file(OsStr::new("staged-identities.run"))
            .is_err()
    );
    drop(session);

    let root = TempDir::new().unwrap();
    let mut session = open(&root, 8_033);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let GraphConstructionSession {
        root: session_root,
        checkpoint,
        ..
    } = &mut session;
    let partitioner = routed_partitioner(session_root, &plan, &mut checkpoint.evidence);
    inject_shape_cleanup_failures(true, true);
    let error = partitioner
        .finish_optional(
            "staged-identities.run",
            0,
            false,
            &mut || false,
            &mut checkpoint.evidence,
        )
        .unwrap_err()
        .to_string();
    let primary = error.find("injected shape input release failure").unwrap();
    let cleanup = error
        .find("partition output cleanup also failed")
        .unwrap_or_else(|| panic!("{error}"));
    assert!(primary < cleanup, "{error}");
    assert!(
        error.contains("unpublished artifact cleanup finalization failed"),
        "{error}"
    );
    assert!(!error.contains(root.path().to_string_lossy().as_ref()));
    assert!(shape_temporary_names(&session.root).is_empty());
    assert!(
        session
            .root
            .open_child_file(OsStr::new("staged-identities.run"))
            .is_err()
    );
    #[cfg(target_os = "linux")]
    assert!(session.checkpoint.evidence.cache_release_operations >= 4);
}

#[test]
fn partition_publication_guard_covers_setup_and_post_rename_failures() {
    use super::super::partition::PartitionPlan;
    let plan = PartitionPlan::single(1);
    let points = [
        "initial_file_identity",
        "writer_construction",
        "window_validation",
        "fsync_evidence_overflow",
        "final_file_identity",
        "file_space_usage",
        "install_child",
        "directory_sync",
        "post_publication_metric_overflow",
        "manifest_update",
    ];
    for (index, point) in points.into_iter().enumerate() {
        let output = "staged-identities.run";
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 8_100 + index as u128);
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
            .unwrap();
        session.seal().unwrap();
        let GraphConstructionSession {
            root: session_root,
            checkpoint,
            ..
        } = &mut session;
        let partitioner = routed_partitioner(session_root, &plan, &mut checkpoint.evidence);
        if matches!(point, "initial_file_identity" | "writer_construction") {
            inject_shape_cleanup_failures(false, true);
        }
        inject_shape_publication_failure(point);
        let error = partitioner
            .finish_optional(output, 0, false, &mut || false, &mut checkpoint.evidence)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(&format!("injected shape publication failure at {point}")),
            "{error}"
        );
        if matches!(point, "initial_file_identity" | "writer_construction") {
            let primary = error.find("injected shape publication failure").unwrap();
            let cleanup = error
                .find("unpublished artifact cleanup finalization failed")
                .unwrap();
            assert!(primary < cleanup, "{error}");
        }
        assert!(!error.contains(root.path().to_string_lossy().as_ref()));
        assert!(shape_temporary_names(&session.root).is_empty());
        assert!(session.root.open_child_file(OsStr::new(output)).is_err());
    }
}

#[cfg(unix)]
#[test]
fn symlink_substitution_is_rejected_on_independent_seal() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new().unwrap();
    let mut session = open(&root, 500);
    let receipt = session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    let operation_root = root
        .path()
        .join(PRIVATE_ROOT)
        .join(Uuid::from_u128(500).simple().to_string());
    let artifact = operation_root.join(&receipt.identities.name);
    let displaced = operation_root.join("displaced.run");
    std::fs::rename(&artifact, &displaced).unwrap();
    symlink(&displaced, &artifact).unwrap();
    assert!(session.seal().is_err());
}

#[test]
fn truncated_staged_artifact_is_refused_by_checksum() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 9_100);
    let chunk = session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 4))
        .unwrap();
    let receipt = &chunk.identities;
    let path = session.root.path().join(&receipt.name);
    let mut bytes = std::fs::read(&path).unwrap();
    assert!(!bytes.is_empty(), "staged artifact must be non-empty");
    bytes.pop();
    std::fs::write(&path, &bytes).unwrap();
    let error = authenticate_artifact(&session.root, receipt, DetailCodec::Compact)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("truncated") || error.contains("artifact digest or size changed"),
        "{error}"
    );
}

#[test]
fn same_length_flipped_staged_artifact_is_refused_by_checksum() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 9_101);
    let chunk = session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 4))
        .unwrap();
    let receipt = &chunk.identities;
    let path = session.root.path().join(&receipt.name);
    let mut bytes = std::fs::read(&path).unwrap();
    assert!(
        bytes.len() >= 2,
        "staged artifact must have at least two bytes"
    );
    bytes[0] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    let error = authenticate_artifact(&session.root, receipt, DetailCodec::Compact)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("not strictly sorted") || error.contains("artifact digest or size changed"),
        "{error}"
    );
}

#[test]
fn checkpoint_with_previous_format_version_fails_closed_with_restart_error() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(9_200);
    let mut session = open(&root, 9_200);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.checkpoint.format_version = FORMAT_VERSION - 1;
    replace_checkpoint_control(&session.root, &session.checkpoint).unwrap();
    drop(session);
    let result = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    );
    let error = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected a checkpoint format error"),
    };
    assert!(error.contains("restart"), "{error}");
}

fn assert_resumed_seal_refuses_without_changing_authority(
    root: &TempDir,
    session: GraphConstructionSession,
    expected_error: &str,
) {
    let operation = session.checkpoint.operation_uuid;
    let session_path = session.root.path().to_path_buf();
    let controls = session
        .root
        .child_names()
        .unwrap()
        .into_iter()
        .filter(|name| name.to_string_lossy().ends_with(".json"))
        .map(|name| {
            let bytes = std::fs::read(session_path.join(&name)).unwrap();
            (name, bytes)
        })
        .collect::<BTreeMap<_, _>>();
    let current = std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap();
    drop(session);
    let mut resumed = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    let error = resumed.seal().unwrap_err().to_string();
    assert!(error.contains(expected_error), "{error}");
    assert_eq!(resumed.checkpoint.state, GraphConstructionState::Staging);
    for (name, bytes) in controls {
        assert_eq!(std::fs::read(session_path.join(name)).unwrap(), bytes);
    }
    assert_eq!(
        std::fs::read(root.path().join(crate::CURRENT_FILE)).unwrap(),
        current
    );
}

#[test]
fn resumed_seal_refuses_whole_record_truncation_by_exact_length() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 9_300);
    let chunk = session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 4))
        .unwrap();
    let path = session.root.path().join(&chunk.identities.name);
    let before = file_identity(&File::open(&path).unwrap()).unwrap();
    let mut bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes.len(), 4 * IDENTITY_WIDTH);
    // Removing one complete UUID leaves a well-formed, strictly sorted run.
    bytes.truncate(bytes.len() - IDENTITY_WIDTH);
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(file_identity(&File::open(&path).unwrap()).unwrap(), before);
    assert_resumed_seal_refuses_without_changing_authority(
        &root,
        session,
        "artifact digest or size changed",
    );
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[test]
fn resumed_seal_refuses_same_inode_sorted_uuid_change_by_checksum() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 9_301);
    let chunk = session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 4))
        .unwrap();
    let path = session.root.path().join(&chunk.identities.name);
    let before = file_identity(&File::open(&path).unwrap()).unwrap();
    let mut bytes = std::fs::read(&path).unwrap();
    let original_len = bytes.len();
    assert_eq!(
        bytes,
        (1_u128..=4).flat_map(u128::to_be_bytes).collect::<Vec<_>>()
    );
    // UUID 4 -> 5 preserves width, UUID validity, and strict ordering.
    *bytes.last_mut().unwrap() ^= 1;
    assert_eq!(
        bytes,
        [1_u128, 2, 3, 5]
            .into_iter()
            .flat_map(u128::to_be_bytes)
            .collect::<Vec<_>>()
    );
    std::fs::write(&path, &bytes).unwrap();
    let after = File::open(&path).unwrap();
    assert_eq!(file_identity(&after).unwrap(), before);
    assert_eq!(after.metadata().unwrap().len(), original_len as u64);
    drop(after);
    assert_resumed_seal_refuses_without_changing_authority(
        &root,
        session,
        "artifact digest or size changed",
    );
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[test]
fn resumed_seal_refuses_missing_checksum_metadata() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 9_302);
    let chunk = session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 4))
        .unwrap();
    let path = session.root.path().join(receipt_name(chunk.sequence));
    let mut receipt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    receipt["identities"]
        .as_object_mut()
        .unwrap()
        .remove("xxh64");
    std::fs::write(&path, serde_json::to_vec(&receipt).unwrap()).unwrap();
    assert_resumed_seal_refuses_without_changing_authority(&root, session, "missing field `xxh64`");
}

#[test]
fn resumed_seal_refuses_malformed_checksum_metadata() {
    for checksum in [
        "",
        "0123456789abcde",
        "0123456789abcdef0",
        "0123456789abcdeG",
    ] {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, 9_303);
        let mut chunk = session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 4))
            .unwrap();
        chunk.identities.xxh64 = checksum.to_owned();
        let path = session.root.path().join(receipt_name(chunk.sequence));
        std::fs::write(&path, serde_json::to_vec(&chunk).unwrap()).unwrap();
        assert_resumed_seal_refuses_without_changing_authority(
            &root,
            session,
            "invalid construction artifact receipt",
        );
    }
}

fn retained_retirement_fixture() -> (
    TempDir,
    StableDirectory,
    ArtifactReceipt,
    RetainedShapeArtifact,
) {
    let temporary = TempDir::new().unwrap();
    let root = StableDirectory::open(temporary.path()).unwrap();
    let name = "shaped-identities.run";
    std::fs::write(temporary.path().join(name), b"identity").unwrap();
    let receipt = receipt_for_existing(&root, name).unwrap();
    persist_shape_receipt(&root, &receipt).unwrap();
    let (actual, _, retained) = receipt_for_existing_retained(&root, name).unwrap();
    assert_eq!(receipt, actual);
    (temporary, root, receipt, retained.unwrap())
}

#[test]
fn retained_retirement_removes_exact_artifact_and_capability() {
    let (temporary, root, receipt, retained) = retained_retirement_fixture();
    retained.unlink(&root, &receipt).unwrap();
    assert!(!temporary.path().join(&receipt.name).exists());
    assert!(
        !temporary
            .path()
            .join(shape_receipt_name(&receipt.name))
            .exists()
    );
}

#[test]
fn retained_retirement_rereads_capability_contents() {
    let (temporary, root, receipt, mut retained) = retained_retirement_fixture();
    assert_eq!(retained.fresh_receipt().unwrap(), receipt);
    let mut changed = receipt.clone();
    changed.xxh64 = "0".repeat(16);
    std::fs::write(
        temporary.path().join(shape_receipt_name(&receipt.name)),
        serde_json::to_vec(&changed).unwrap(),
    )
    .unwrap();
    assert_eq!(retained.fresh_receipt().unwrap(), changed);
    assert!(retained.unlink(&root, &receipt).is_err());
    assert!(temporary.path().join(&receipt.name).exists());
    assert!(
        temporary
            .path()
            .join(shape_receipt_name(&receipt.name))
            .exists()
    );
}

#[test]
fn retained_retirement_preserves_equivalent_fresh_receipt_encoding() {
    let (temporary, root, receipt, retained) = retained_retirement_fixture();
    std::fs::write(
        temporary.path().join(shape_receipt_name(&receipt.name)),
        serde_json::to_vec_pretty(&receipt).unwrap(),
    )
    .unwrap();
    retained.unlink(&root, &receipt).unwrap();
}

#[test]
fn retained_retirement_refuses_changed_artifact_length() {
    let (temporary, root, receipt, retained) = retained_retirement_fixture();
    std::fs::write(temporary.path().join(&receipt.name), b"longer identity").unwrap();
    assert!(retained.unlink(&root, &receipt).is_err());
    assert!(temporary.path().join(&receipt.name).exists());
    assert!(
        temporary
            .path()
            .join(shape_receipt_name(&receipt.name))
            .exists()
    );
}

#[test]
fn retained_retirement_refuses_fresh_extra_links() {
    for capability in [false, true] {
        let (temporary, root, receipt, retained) = retained_retirement_fixture();
        let name = if capability {
            shape_receipt_name(&receipt.name)
        } else {
            receipt.name.clone()
        };
        std::fs::hard_link(
            temporary.path().join(name),
            temporary.path().join("extra.link"),
        )
        .unwrap();
        assert!(retained.unlink(&root, &receipt).is_err());
        assert!(temporary.path().join(&receipt.name).exists());
        assert!(
            temporary
                .path()
                .join(shape_receipt_name(&receipt.name))
                .exists()
        );
    }
}

#[test]
fn retirement_without_writer_capability_keeps_payload_authentication() {
    let temporary = TempDir::new().unwrap();
    let root = StableDirectory::open(temporary.path()).unwrap();
    let name = "shaped-identities.run";
    std::fs::write(temporary.path().join(name), b"identity").unwrap();
    let expected = receipt_for_existing(&root, name).unwrap();
    let actual = unlink_shape_artifact_files(&root, name).unwrap();
    assert_eq!(actual, expected);
    assert!(!temporary.path().join(name).exists());
}

#[cfg(unix)]
#[test]
fn retained_retirement_refuses_disappeared_capability_before_unlink() {
    let (temporary, root, receipt, retained) = retained_retirement_fixture();
    std::fs::remove_file(temporary.path().join(shape_receipt_name(&receipt.name))).unwrap();
    assert!(retained.unlink(&root, &receipt).is_err());
    assert!(temporary.path().join(&receipt.name).exists());
}
