//! Private encoded source/copy authority regressions.

use super::*;

#[test]
fn captured_encoded_copy_and_known_reuse_do_not_repeat_payload_sha_but_unknown_cas_does() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 14_171);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 4))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    let directory = session
        .root
        .open_child_directory(OsStr::new(&encoded.root))
        .unwrap()
        .open_child_directory(OsStr::new("graph"))
        .unwrap();
    let inventory = crate::graph_construction::CapturedEncodedInventory {
        root: &directory,
        artifacts: &encoded.artifacts,
        active_identities: &session
            .checkpoint
            .evidence
            .storage_active_identity_allocated_bytes,
    };
    let source = inventory
        .open(Path::new("topology/generation.json"))
        .unwrap();
    let lease = crate::begin_graph_object_publication(root.path()).unwrap();
    for reused in [false, true] {
        let capture = graphforge_core::hash_observation::operation::Capture::start();
        let evidence = crate::graph_object_store::install_captured_encoded_artifact_with_lease(
            &lease,
            &source,
            &mut || false,
        )
        .unwrap();
        let observed = capture.snapshot();
        assert_eq!(evidence.reused_existing, reused);
        assert_eq!(evidence.bytes_hashed, 0);
        assert_eq!(observed.artifact_payload_sha256_bytes, 0);
        assert_eq!(observed.unclassified_sha256_bytes, 0);
        // Unix links the encoder's file, so a first install reads nothing; Windows copies
        // it and reads it once back, and a reuse reads the existing object once.
        let passes = match (cfg!(windows), reused) {
            (true, false) => 2,
            (false, false) => 0,
            (_, true) => 1,
        };
        assert_eq!(observed.checksum_bytes, passes * source.bytes());
        assert_eq!(evidence.checksum_read_bytes, passes * source.bytes());
        assert_eq!(evidence.content_xxh64, Some(source.checksum()));
    }
    drop(lease);
    // A new lease has no native capture authority for this otherwise-valid CAS object.
    // Its independent trust check remains genuine SHA, even with an admitted source.
    let lease = crate::begin_graph_object_publication(root.path()).unwrap();
    let capture = graphforge_core::hash_observation::operation::Capture::start();
    let evidence = crate::graph_object_store::install_captured_encoded_artifact_with_lease(
        &lease,
        &source,
        &mut || false,
    )
    .unwrap();
    let observed = capture.snapshot();
    assert!(evidence.reused_existing);
    assert_eq!(evidence.bytes_hashed, source.bytes());
    assert_eq!(observed.artifact_payload_sha256_bytes, source.bytes());
    assert_eq!(observed.checksum_bytes, source.bytes());
    assert_eq!(observed.unclassified_sha256_bytes, 0);
}

// Only Windows still copies an encoded source; unix links the encoder's file and
// has no copy pass to mutate (see `direct_install`).
#[cfg(windows)]
#[test]
fn captured_encoded_copy_refuses_valid_same_inode_mutate_read_restore_and_preserves_current() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 14_172);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    let graph = session
        .root
        .open_child_directory(OsStr::new(&encoded.root))
        .unwrap()
        .open_child_directory(OsStr::new("graph"))
        .unwrap();
    let path = graph.path().join("topology/generation.json");
    let original = std::fs::read(&path).unwrap();
    let mut changed = original.clone();
    let index = changed.iter().position(|byte| *byte == b'1').unwrap();
    changed[index] = b'2';
    assert!(serde_json::from_slice::<serde_json::Value>(&changed).is_ok());
    let identity = graphforge_filesystem::path_identity(&path).unwrap();
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    let hook_path = path.clone();
    let hook_original = original.clone();
    let mut changed_once = false;
    let mut restored = false;
    crate::graph_object_store::set_captured_copy_hook(Some(Box::new(move |phase| {
        if phase == "before_read" && !changed_once {
            std::fs::write(&hook_path, &changed).unwrap();
            changed_once = true;
        } else if phase == "after_read" && changed_once && !restored {
            std::fs::write(&hook_path, &hook_original).unwrap();
            restored = true;
        }
    })));
    let inventory = crate::graph_construction::CapturedEncodedInventory {
        root: &graph,
        artifacts: &encoded.artifacts,
        active_identities: &session
            .checkpoint
            .evidence
            .storage_active_identity_allocated_bytes,
    };
    let source = inventory
        .open(Path::new("topology/generation.json"))
        .unwrap();
    let lease = crate::begin_graph_object_publication(root.path()).unwrap();
    let result = crate::graph_object_store::install_captured_encoded_artifact_with_lease(
        &lease,
        &source,
        &mut || false,
    );
    crate::graph_object_store::set_captured_copy_hook(None);
    let error = result.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("checksum or length changed during copy"),
        "{error}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(
        graphforge_filesystem::path_identity(&path).unwrap(),
        identity
    );
    assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
}

#[test]
fn known_captured_cas_inode_refuses_valid_same_length_corruption() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 14_173);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    let graph = session
        .root
        .open_child_directory(OsStr::new(&encoded.root))
        .unwrap()
        .open_child_directory(OsStr::new("graph"))
        .unwrap();
    let inventory = crate::graph_construction::CapturedEncodedInventory {
        root: &graph,
        artifacts: &encoded.artifacts,
        active_identities: &session
            .checkpoint
            .evidence
            .storage_active_identity_allocated_bytes,
    };
    let source = inventory
        .open(Path::new("topology/generation.json"))
        .unwrap();
    let lease = crate::begin_graph_object_publication(root.path()).unwrap();
    crate::graph_object_store::install_captured_encoded_artifact_with_lease(
        &lease,
        &source,
        &mut || false,
    )
    .unwrap();
    let path = crate::graph_object_path(root.path(), source.content_sha256()).unwrap();
    let identity = graphforge_filesystem::path_identity(&path).unwrap();
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    let permissions = std::fs::metadata(&path).unwrap().permissions();
    let mut writable = permissions.clone();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        writable.set_mode(0o600);
    }
    #[cfg(not(unix))]
    writable.set_readonly(false);
    std::fs::set_permissions(&path, writable).unwrap();
    let mut changed = std::fs::read(&path).unwrap();
    let index = changed.iter().position(|byte| *byte == b'1').unwrap();
    changed[index] = b'2';
    assert!(serde_json::from_slice::<serde_json::Value>(&changed).is_ok());
    std::fs::write(&path, changed).unwrap();
    std::fs::set_permissions(&path, permissions).unwrap();
    let capture = graphforge_core::hash_observation::operation::Capture::start();
    let error = crate::graph_object_store::install_captured_encoded_artifact_with_lease(
        &lease,
        &source,
        &mut || false,
    )
    .unwrap_err();
    let observed = capture.snapshot();
    assert!(error.to_string().contains("checksum"), "{error}");
    assert_eq!(observed.artifact_payload_sha256_bytes, 0);
    assert_eq!(observed.unclassified_sha256_bytes, 0);
    assert_eq!(observed.checksum_bytes, source.bytes());
    assert_eq!(
        graphforge_filesystem::path_identity(&path).unwrap(),
        identity
    );
    assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
}
