use super::*;
use crate::graph_object_store::GraphManifestState;
use crate::graph_object_store::PathBuf;
use crate::graph_object_store::append_graph_files_v2;
use crate::graph_object_store::begin_graph_object_publication;
use crate::graph_object_store::corrupt_sealed_graph_object_for_test;
use crate::graph_object_store::fs;
use crate::graph_object_store::graph_object_path;
use crate::graph_object_store::install_graph_object_bytes;

#[test]
fn gc_traces_segment_and_payload_roots_before_sweeping() {
    let container = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let relative = PathBuf::from("topology/nodes/1-1.parquet");
    let source = workspace.path().join(&relative);
    fs::create_dir_all(source.parent().unwrap()).unwrap();
    fs::write(&source, b"node").unwrap();
    let mut state = GraphManifestState::empty();
    let (root, _) =
        append_graph_files_v2(&lease, workspace.path(), &mut state, &[relative], &[]).unwrap();
    drop(lease);
    let (orphan, _) = install_graph_object_bytes(container.path(), b"orphan").unwrap();
    let evidence = gc_graph_objects(
        container.path(),
        std::slice::from_ref(&root),
        crate::GraphManifestLimits::default(),
    )
    .unwrap();
    assert_eq!(evidence.objects_marked, 2);
    // The initial empty root and the explicit orphan are both unreachable.
    assert_eq!(evidence.objects_removed, 3);
    assert!(
        !graph_object_path(container.path(), &orphan)
            .unwrap()
            .exists()
    );

    let (another_orphan, _) = install_graph_object_bytes(container.path(), b"another").unwrap();
    corrupt_sealed_graph_object_for_test(
        &graph_object_path(container.path(), &root.root_node_sha256).unwrap(),
        b"tampered",
    );
    assert!(
        gc_graph_objects(
            container.path(),
            &[root],
            crate::GraphManifestLimits::default()
        )
        .is_err()
    );
    assert!(
        graph_object_path(container.path(), &another_orphan)
            .unwrap()
            .exists()
    );
}
