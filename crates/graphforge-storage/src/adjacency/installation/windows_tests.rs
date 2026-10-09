//! CSR publication through ordinary Win32 paths beyond MAX_PATH.

use super::super::{
    csr_path, encode_csr_shard_bytes, read_csr, write_csr_shard_bytes,
    write_csr_shard_bytes_observed, write_sharded_csr, CsrIndex, Direction,
    DEFAULT_CSR_SHARD_EDGES,
};
use tempfile::TempDir;

#[test]
fn csr_shard_publication_supports_long_windows_paths() {
    let root = TempDir::new().unwrap();
    let graph_tree = root
        .path()
        .join("private-portable-stage-".repeat(3))
        .join("private-graph-materialization-".repeat(3))
        .join("data/components/graph-data/graph-tree");
    let index_path = csr_path(&graph_tree, "KNOWS", Direction::Out);
    assert!(index_path.as_os_str().len() > 260);
    let shard_parent = index_path.parent().unwrap().join(format!(
        "{}.{}.d",
        index_path.file_name().unwrap().to_str().unwrap(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&shard_parent).unwrap();
    let path = shard_parent.join("00000000000000000000.csr");
    assert!(path.as_os_str().len() > 260);
    assert!(matches!(
        path.components().next(),
        Some(std::path::Component::Prefix(prefix)) if !prefix.kind().is_verbatim()
    ));
    let parent = graphforge_filesystem::StableDirectory::open(&shard_parent).unwrap();
    let identity = parent.identity();
    let original = CsrIndex {
        offsets: vec![0, 2],
        edge_ids: vec![10, 11],
        neighbor_ids: vec![1, 2],
    };
    write_csr_shard_bytes(&path, &encode_csr_shard_bytes(&original).unwrap()).unwrap();
    let allocation = crate::StorageAllocationOperation::default();
    for csr in [
        original,
        CsrIndex {
            offsets: vec![0, 1],
            edge_ids: vec![99],
            neighbor_ids: vec![0],
        },
    ] {
        let bytes = encode_csr_shard_bytes(&csr).unwrap();
        write_csr_shard_bytes_observed(&path, &bytes, Some(&allocation)).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        let allocated_bytes =
            graphforge_filesystem::file_space_usage(&std::fs::File::open(&path).unwrap())
                .unwrap()
                .allocated_bytes;
        assert_eq!(allocation.totals().unwrap().0, allocated_bytes);
        assert_eq!(
            graphforge_filesystem::path_identity(&shard_parent).unwrap(),
            identity
        );
        let names = parent.child_names_bounded(2).unwrap();
        assert_eq!(names, vec![path.file_name().unwrap().to_owned()]);
        // The manifest uses a second NamedTempFile creation site.
        write_sharded_csr(&index_path, &csr, DEFAULT_CSR_SHARD_EDGES).unwrap();
        assert_eq!(read_csr(&index_path).unwrap(), csr);
        assert!(std::fs::read_dir(index_path.parent().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .all(|entry| entry.extension().is_none_or(|extension| extension != "tmp")));
    }
    drop(parent);
    root.close().unwrap();
}
