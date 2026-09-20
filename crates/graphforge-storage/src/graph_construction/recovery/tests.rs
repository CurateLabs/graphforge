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
    mismatched.sha256 = "0".repeat(64);
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
