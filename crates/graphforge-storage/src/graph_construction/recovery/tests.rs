use super::super::tests::{node_batch, open, shape_temporary_names};
use super::super::*;
use super::*;

use tempfile::TempDir;

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

#[test]
fn shaping_copy_failures_finalize_release_and_remove_unpublished_outputs() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 8_031);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let receipt = session.read_receipt(0).unwrap();

    let mut cancelled = || true;
    let error = convert_identity_run(
        &session.root,
        &receipt,
        "cancelled-identities.run",
        &mut cancelled,
        &mut session.checkpoint.evidence,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("construction cancelled"), "{error}");
    assert!(!error.contains(root.path().to_string_lossy().as_ref()));
    assert!(shape_temporary_names(&session.root).is_empty());
    assert!(
        session
            .root
            .open_child_file(OsStr::new("cancelled-identities.run"))
            .is_err()
    );

    inject_shape_cleanup_failures(true, true);
    let mut never_cancelled = || false;
    let error = copy_authenticated_run_with_codec::<NODE_DETAIL_WIDTH>(
        &session.root,
        &receipt.details,
        "failed-details.run",
        &mut never_cancelled,
        &mut session.checkpoint.evidence,
        Some(DetailCodec::from_version(session.checkpoint.format_version).unwrap()),
    )
    .unwrap_err()
    .to_string();
    let primary = error.find("injected shape input release failure").unwrap();
    let cleanup = error
        .find("authenticated output cleanup also failed")
        .unwrap();
    assert!(primary < cleanup, "{error}");
    assert!(
        error.contains("authenticated output cleanup also failed"),
        "{error}"
    );
    assert!(
        error.contains("unpublished artifact cleanup finalization failed"),
        "{error}"
    );
    assert!(!error.contains(root.path().to_string_lossy().as_ref()));
    assert!(shape_temporary_names(&session.root).is_empty());
    assert!(
        session
            .root
            .open_child_file(OsStr::new("failed-details.run"))
            .is_err()
    );
    #[cfg(target_os = "linux")]
    assert!(session.checkpoint.evidence.cache_release_operations >= 4);
}

#[test]
fn shaping_publication_guard_covers_setup_and_post_rename_failures() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 8_032);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let receipt = session.read_receipt(0).unwrap();
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
    for point in points {
        let output = format!("guard-identity-{point}.run");
        if matches!(point, "initial_file_identity" | "writer_construction") {
            inject_shape_cleanup_failures(false, true);
        }
        inject_shape_publication_failure(point);
        let error = convert_identity_run(
            &session.root,
            &receipt,
            &output,
            &mut || false,
            &mut session.checkpoint.evidence,
        )
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
        assert!(session.root.open_child_file(OsStr::new(&output)).is_err());

        let output = format!("guard-authenticated-{point}.run");
        if matches!(point, "initial_file_identity" | "writer_construction") {
            inject_shape_cleanup_failures(false, true);
        }
        inject_shape_publication_failure(point);
        let error = copy_authenticated_run_with_codec::<NODE_DETAIL_WIDTH>(
            &session.root,
            &receipt.details,
            &output,
            &mut || false,
            &mut session.checkpoint.evidence,
            Some(DetailCodec::from_version(session.checkpoint.format_version).unwrap()),
        )
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
        assert!(session.root.open_child_file(OsStr::new(&output)).is_err());
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
