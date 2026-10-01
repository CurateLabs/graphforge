//! Direct proof of final CSR writer captures.

use super::*;
use tempfile::TempDir;

#[test]
fn csr_final_writer_returns_single_pass_shard_identity_captures() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = CsrIndex {
        offsets: vec![0, 2, 2, 4],
        edge_ids: vec![10, 11, 12, 13],
        neighbor_ids: vec![1, 2, 0, 1],
    };
    let capture = graphforge_core::hash_observation::operation::Capture::start();
    let mut writer = ShardedCsrWriter::create(&path, 2, DEFAULT_CSR_SHARD_NODES).unwrap();
    for node in 0..expected.node_count() {
        for (edge, neighbor) in expected.row(node).iter() {
            writer.emit((node, edge, neighbor)).unwrap();
        }
    }
    let (shards, _, _, captures) = writer.finish(expected.node_count()).unwrap();
    let observed = capture.snapshot();
    drop(capture);
    assert_eq!(shards as usize, captures.len());
    let mut bytes = 0;
    for artifact in captures {
        let actual = std::fs::read(&artifact.path).unwrap();
        assert_eq!(actual.len() as u64, artifact.bytes);
        assert_eq!(sha256_hex(&actual), artifact.sha256);
        assert_eq!(
            crate::corruption_checksum::checksum(&actual),
            artifact.xxh64
        );
        bytes += artifact.bytes;
    }
    assert_eq!(observed.artifact_payload_sha256_bytes, bytes);
    assert_eq!(observed.unclassified_sha256_bytes, 0);
    assert_eq!(observed.checksum_bytes, bytes);
    assert_eq!(
        ShardedCsrIndex::open(&path).unwrap().edge_count(),
        expected.edge_count()
    );
}
