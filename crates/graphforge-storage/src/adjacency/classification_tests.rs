//! Classification of an unusable derived adjacency index (#1388): damage is a
//! `GF_VALIDATION` refusal, an unsupported format or an I/O failure is a storage
//! error, and neither is ever a reason to rebuild.

use super::*;
use tempfile::TempDir;

/// 3 nodes / 4 entries: node 0 → two neighbors, node 1 → none, node 2 → two.
fn sample_csr() -> CsrIndex {
    CsrIndex {
        offsets: vec![0, 2, 2, 4],
        edge_ids: vec![10, 11, 12, 13],
        neighbor_ids: vec![1, 2, 0, 1],
    }
}

/// How an index object that cannot serve a read is classified (#1388).
/// Damage is `GF_VALIDATION` and an unsupported format or I/O failure is a
/// storage error. Neither is ever a reason to rebuild.
#[test]
fn unusable_index_objects_are_classified_by_cause() {
    let fixture = || {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("KNOWS.out.csr");
        write_sharded_csr(&path, &sample_csr(), 2).unwrap();
        let reader = ShardedCsrIndex::open(&path).unwrap();
        let shard = reader.root.join(&reader.manifest.shards[0].file);
        (directory, path, reader, shard)
    };
    let validation = |error: GfError| {
        assert!(matches!(error, GfError::Validation(_)), "{error}");
        assert_eq!(error.code(), "GF_VALIDATION");
    };

    // Checksum mismatch, same length.
    let (_dir, path, _reader, shard) = fixture();
    let mut bytes = std::fs::read(&shard).unwrap();
    bytes[0] ^= 1;
    std::fs::write(&shard, bytes).unwrap();
    validation(ShardedCsrIndex::open(&path).unwrap().row(0).unwrap_err());

    // Length mismatch: refused at open, where presence and length are read.
    let (_dir, path, _reader, shard) = fixture();
    std::fs::write(&shard, b"short").unwrap();
    validation(ShardedCsrIndex::open(&path).unwrap_err());

    // A declared shard that is absent.
    let (_dir, path, _reader, shard) = fixture();
    std::fs::remove_file(&shard).unwrap();
    validation(ShardedCsrIndex::open(&path).unwrap_err());

    // Authenticated bytes (the manifest records their checksum) that are
    // not a shard: a correct writer never produces them.
    let (_dir, _path, mut reader, shard) = fixture();
    let length = std::fs::metadata(&shard).unwrap().len() as usize;
    let garbage = vec![7_u8; length];
    std::fs::write(&shard, &garbage).unwrap();
    reader.manifest.shards[0].xxh64 = crate::corruption_checksum::checksum(&garbage);
    reader.manifest.shards[0].sha256 = sha256_hex(&garbage);
    validation(reader.row(0).unwrap_err());

    // The shard manifest itself does not parse.
    let (_dir, path, _reader, _shard) = fixture();
    let manifest_path = path.with_extension("csr.json");
    let mut bytes = std::fs::read(&manifest_path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(&manifest_path, bytes).unwrap();
    validation(ShardedCsrIndex::open(&path).unwrap_err());

    // The shard manifest is self-inconsistent (counts versus shards).
    let (_dir, path, _reader, _shard) = fixture();
    let manifest_path = path.with_extension("csr.json");
    let mut manifest: CsrShardManifest =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest.edge_count += 1;
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    validation(ShardedCsrIndex::open(&path).unwrap_err());

    // An older format is not damage, and is not silently rebuilt either:
    // it is a storage error that names the remedy.
    let (_dir, path, _reader, _shard) = fixture();
    let manifest_path = path.with_extension("csr.json");
    let mut manifest: CsrShardManifest =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest.version = 1;
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let error = ShardedCsrIndex::open(&path).unwrap_err();
    assert!(matches!(error, GfError::Storage(_)), "{error}");
    assert!(error.to_string().contains("recreate the index"), "{error}");

    // An index manifest that does not decode as Parquet is damaged.
    let directory = TempDir::new().unwrap();
    write_manifest(directory.path(), &[]).unwrap();
    std::fs::write(super::manifest_path(directory.path()), b"PAR1 not parquet").unwrap();
    validation(read_manifest(directory.path()).unwrap_err());
}
