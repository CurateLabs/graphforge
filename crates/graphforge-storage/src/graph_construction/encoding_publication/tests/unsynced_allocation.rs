//! Allocated blocks of an unsynced encoded artifact are not its identity (#1928).

use super::*;

/// Reserve blocks past EOF without changing length or content: the allocation
/// change ext4 delayed allocation makes on an unsynced file.
fn grow_allocation_keeping_content(path: &std::path::Path) {
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    let length = file.metadata().unwrap().len();
    let before = graphforge_filesystem::file_space_usage(&file)
        .unwrap()
        .allocated_bytes;
    rustix::fs::fallocate(
        &file,
        rustix::fs::FallocateFlags::KEEP_SIZE,
        length,
        1 << 20,
    )
    .unwrap();
    let usage = graphforge_filesystem::file_space_usage(&file).unwrap();
    assert_eq!(usage.logical_bytes, length);
    assert_eq!(file.metadata().unwrap().len(), length);
    assert!(
        usage.allocated_bytes > before,
        "allocation must change: {before} -> {}",
        usage.allocated_bytes
    );
}

/// A capture, its revalidation, a fresh capture, the install and the
/// supersession sweep all hold when only the allocation moved.
#[test]
fn captured_encoded_source_survives_allocation_change_with_unchanged_content() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 14_174);
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
    let inventory = crate::graph_construction::CapturedEncodedInventory {
        root: &graph,
        artifacts: &encoded.artifacts,
        active_identities: &session
            .checkpoint
            .evidence
            .storage_active_identity_allocated_bytes,
    };
    let first = inventory
        .open(Path::new("topology/generation.json"))
        .unwrap();
    let identity = graphforge_filesystem::path_identity(&path).unwrap();
    grow_allocation_keeping_content(&path);
    assert_eq!(
        graphforge_filesystem::path_identity(&path).unwrap(),
        identity
    );

    first.revalidate().unwrap();
    let second = inventory
        .open(Path::new("topology/generation.json"))
        .unwrap();
    let lease = crate::begin_graph_object_publication(root.path()).unwrap();
    crate::graph_object_store::install_captured_encoded_artifact_with_lease(
        &lease,
        &second,
        &mut || false,
    )
    .unwrap();
    drop(lease);
    // A retried encode records the inventory again: the ledger keeps the
    // allocation observed first, and the entries it returns agree with it.
    let recorded = session
        .checkpoint
        .evidence
        .storage_active_identity_allocated_bytes
        .clone();
    let again =
        record_encoded_active_artifacts(&session.root, &encoded, &mut session.checkpoint.evidence)
            .unwrap();
    assert_eq!(
        session
            .checkpoint
            .evidence
            .storage_active_identity_allocated_bytes,
        recorded
    );
    assert!(!again.is_empty());
    for (key, allocated) in &again {
        assert_eq!(recorded.get(key), Some(allocated));
    }
    session
        .reclaim_superseded_payloads_cancellable(&mut || false)
        .unwrap();
}

/// A real change after capture is still refused, with the allocation moved
/// as well: a length change by revalidation, same-length content by the
/// commit boundary's exact length and XXH64 admission.
#[test]
fn captured_encoded_source_still_refuses_content_change_after_capture() {
    for append in [true, false] {
        let root = TempDir::new().unwrap();
        let mut session = open(&root, if append { 14_175 } else { 14_176 });
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
        let identity = graphforge_filesystem::path_identity(&path).unwrap();
        grow_allocation_keeping_content(&path);
        if append {
            use std::io::Write as _;
            std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(b" ")
                .unwrap();
        } else {
            let mut changed = std::fs::read(&path).unwrap();
            let index = changed.iter().position(|byte| *byte == b'1').unwrap();
            changed[index] = b'2';
            std::fs::write(&path, changed).unwrap();
        }
        assert_eq!(
            graphforge_filesystem::path_identity(&path).unwrap(),
            identity
        );
        let lease = crate::begin_graph_object_publication(root.path()).unwrap();
        let result = crate::graph_object_store::install_captured_encoded_artifact_with_lease(
            &lease,
            &source,
            &mut || false,
        );
        if append {
            // A length change is refused by revalidation before any link.
            let error = result.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("captured encoded source identity or length changed"),
                "{error}"
            );
            assert!(source.revalidate().is_err());
        } else {
            // The install links the encoder's file without reading it back; a
            // same-length edit is refused by exact XXH64 at the commit boundary.
            result.unwrap();
            let entry = crate::GraphFileEntry {
                content_xxh64: source.checksum(),
                relative_path: "topology/generation.json".to_owned(),
                byte_length: source.bytes(),
                content_sha256: source.content_sha256().to_owned(),
                role: crate::graph_files::infer_role(Path::new("topology/generation.json")),
            };
            let error = crate::graph_object_store::admit_graph_object_with_lease(&lease, &entry)
                .unwrap_err();
            assert!(error.to_string().contains("checksum"), "{error}");
        }
    }
}
