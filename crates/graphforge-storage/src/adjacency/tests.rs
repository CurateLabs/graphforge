use tempfile::TempDir;

use super::*;

/// 3 nodes / 4 entries: node 0 → two neighbors, node 1 → none, node 2 → two.
fn sample_csr() -> CsrIndex {
    CsrIndex {
        offsets: vec![0, 2, 2, 4],
        edge_ids: vec![10, 11, 12, 13],
        neighbor_ids: vec![1, 2, 0, 1],
    }
}

#[test]
fn sharded_csr_crosses_boundaries_and_rejects_missing_or_corrupt_shards() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = sample_csr();
    write_sharded_csr(&path, &expected, 2).unwrap();
    let reader = ShardedCsrIndex::open(&path).unwrap();
    assert_eq!((reader.node_count(), reader.edge_count()), (3, 4));
    for node in 0..expected.node_count() {
        assert_eq!(
            reader.row(node).unwrap(),
            expected.row(node).iter().collect::<Vec<_>>()
        );
    }

    let first = reader.root.join(&reader.manifest.shards[0].file);
    let original = std::fs::read(&first).unwrap();
    let mut corrupt = original.clone();
    corrupt[0] ^= 1;
    std::fs::write(&first, corrupt).unwrap();
    // The warm reader legitimately serves shard 0 from its authenticated
    // decoded cache; a fresh session (cold cache) must re-touch the file.
    let fresh = ShardedCsrIndex::open(&path).unwrap();
    assert!(fresh.row(0).unwrap_err().to_string().contains("checksum"));
    assert!(fresh
        .row_len(0)
        .unwrap_err()
        .to_string()
        .contains("checksum"));
    assert!(fresh
        .row_chunk(0, 0, 1)
        .unwrap_err()
        .to_string()
        .contains("checksum"));
    std::fs::write(&first, original).unwrap();
    let fresh = ShardedCsrIndex::open(&path).unwrap();
    std::fs::remove_file(&first).unwrap();
    assert!(fresh
        .row(0)
        .unwrap_err()
        .to_string()
        .contains("missing CSR shard"));
    assert!(fresh
        .row_len(0)
        .unwrap_err()
        .to_string()
        .contains("missing CSR shard"));
    assert!(fresh
        .row_chunk(0, 0, 1)
        .unwrap_err()
        .to_string()
        .contains("missing CSR shard"));
}

#[test]
fn opening_sharded_csr_does_not_read_or_decode_shard_payloads() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = sample_csr();
    write_sharded_csr(&path, &expected, 2).unwrap();
    let published = ShardedCsrIndex::open(&path).unwrap();
    assert_eq!(
        (published.node_count(), published.edge_count()),
        (expected.node_count(), expected.edge_count())
    );
    for record in &published.manifest.shards {
        let payload = published.root.join(&record.file);
        let mut bytes = std::fs::read(&payload).unwrap();
        bytes[0] ^= 1;
        std::fs::write(payload, bytes).unwrap();
    }
    let reader = ShardedCsrIndex::open(&path).expect("open must not decode shard payloads");
    assert_eq!(
        (reader.node_count(), reader.edge_count()),
        (expected.node_count(), expected.edge_count())
    );
    assert!(reader.row(0).unwrap_err().to_string().contains("checksum"));
}

#[test]
fn serving_a_row_attributes_its_shard_payload_read() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    write_sharded_csr(&path, &sample_csr(), 2).unwrap();
    // Open outside the capture: presence-only, records nothing but the
    // small manifest read, and proves payload bytes stay untouched until
    // a row asks for them.
    let reader = ShardedCsrIndex::open(&path).unwrap();

    let _capture = crate::lifecycle_io::CaptureScope::install();
    let before = crate::lifecycle_io::snapshot().expect("requested lifecycle measurement");
    assert!(!reader.row(0).unwrap().is_empty());
    let region = crate::lifecycle_io::snapshot()
        .expect("requested lifecycle measurement")
        .since(&before)
        .unwrap();
    region.validate_for_qualification().unwrap();
    // #1449: first-row-touch authentication is real read-path work and
    // used to be invisible to the counters.
    assert!(
        region.phases[&crate::StorageIoPhase::ReadPathScan].read_bytes > 0,
        "serving read unattributed: {region:#?}"
    );
}

#[test]
fn sequential_rows_reuse_only_the_current_authenticated_shard() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = sample_csr();
    write_sharded_csr(&path, &expected, 8).unwrap();
    let reader = ShardedCsrIndex::open(&path).unwrap();
    assert_eq!(reader.row(0).unwrap(), vec![(10, 1), (11, 2)]);
    std::fs::remove_file(reader.root.join(&reader.manifest.shards[0].file)).unwrap();
    assert_eq!(reader.row(2).unwrap(), vec![(12, 0), (13, 1)]);
}

#[test]
fn deterministic_rebuild_repairs_a_corrupt_stable_shard_set() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = sample_csr();
    write_sharded_csr(&path, &expected, 2).unwrap();
    let reader = ShardedCsrIndex::open(&path).unwrap();
    std::fs::write(
        reader.root.join(&reader.manifest.shards[0].file),
        b"corrupt",
    )
    .unwrap();

    write_sharded_csr(&path, &expected, 2).unwrap();
    assert_eq!(read_csr(&path).unwrap(), expected);
}

#[test]
fn failed_manifest_republish_does_not_delete_reused_live_shards() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let manifest_path = path.with_extension("csr.json");
    let saved_manifest = directory.path().join("saved-manifest.json");
    let expected = sample_csr();
    write_sharded_csr(&path, &expected, 2).unwrap();
    std::fs::rename(&manifest_path, &saved_manifest).unwrap();
    std::fs::create_dir(&manifest_path).unwrap();

    assert!(write_sharded_csr(&path, &expected, 2).is_err());
    std::fs::remove_dir(&manifest_path).unwrap();
    std::fs::rename(&saved_manifest, &manifest_path).unwrap();
    assert_eq!(read_csr(&path).unwrap(), expected);
}

#[test]
fn high_degree_row_spans_hard_capped_shards_in_order() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = CsrIndex {
        offsets: vec![0, 7],
        edge_ids: (10..17).collect(),
        neighbor_ids: (20..27).collect(),
    };
    write_sharded_csr(&path, &expected, 2).unwrap();
    let reader = ShardedCsrIndex::open(&path).unwrap();
    assert_eq!(reader.manifest.shards.len(), 4);
    assert!(reader
        .manifest
        .shards
        .iter()
        .all(|shard| shard.edge_count <= 2));
    assert!(reader
        .manifest
        .shards
        .iter()
        .all(|shard| shard.first_node == 0));
    assert_eq!(
        reader.row(0).unwrap(),
        expected.row(0).iter().collect::<Vec<_>>()
    );
    assert_eq!(reader.row_len(0).unwrap(), 7);
    assert_eq!(reader.row_len(1).unwrap(), 0);
    let mut chunks = Vec::new();
    for offset in (0..7).step_by(3) {
        let chunk = reader.row_chunk(0, offset, 3).unwrap();
        assert!(chunk.len() <= 3);
        chunks.extend(chunk);
        let cache = reader.cache.lock().unwrap();
        assert!(cache
            .entries
            .iter()
            .all(|entry| entry.csr.edge_count() <= 2));
    }
    assert_eq!(chunks, expected.row(0).iter().collect::<Vec<_>>());
    assert!(reader.row_chunk(0, 7, 3).unwrap().is_empty());
}

/// A three-shard index whose frontier alternates shards every row — the
/// S18 two-hop shape (#1518). Shard decodes must be bounded by the shard
/// count, not by the frontier length.
#[test]
fn alternating_shard_frontier_decodes_each_shard_once() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = CsrIndex {
        offsets: vec![0, 1, 2, 3, 4, 5, 6],
        edge_ids: (100..106).collect(),
        neighbor_ids: (200..206).collect(),
    };
    write_sharded_csr(&path, &expected, 2).unwrap();
    let reader = ShardedCsrIndex::open(&path).unwrap();
    let shard_count = reader.manifest.shards.len();
    assert_eq!(shard_count, 3);

    // Neighbour-ordered frontiers interleave shards every row; several
    // rounds prove the cache retains the decoded shards across passes.
    let frontier = [0_u64, 3, 1, 4, 2, 5];
    for _ in 0..4 {
        for node in frontier {
            assert_eq!(
                reader.row(node).unwrap(),
                expected.row(node).iter().collect::<Vec<_>>()
            );
            assert_eq!(reader.row_len(node).unwrap(), 1);
            assert_eq!(
                reader.row_chunk(node, 0, usize::MAX).unwrap(),
                expected.row(node).iter().collect::<Vec<_>>()
            );
        }
    }
    assert_eq!(
        reader.shard_decode_count(),
        u64::try_from(shard_count).unwrap(),
        "an alternating frontier must not re-decode per row"
    );
}

/// S18-shaped measurement for #1518: four default-capped shards (the S18
/// four-shard adjacency) probed by an alternating frontier. Manual/scale:
/// run explicitly, before and after a candidate change, e.g.
/// `cargo test -p graphforge-storage --release --lib s18_shape -- --ignored --nocapture`.
#[test]
#[ignore = "manual/scale: S18 four-shard alternating-frontier cost; run explicitly for #1518 evidence"]
fn s18_shape_alternating_frontier_measurement() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let shards = 4_usize;
    let rows = shards * DEFAULT_CSR_SHARD_EDGES;
    let frontier_rows = 4_096_usize;
    let expected = CsrIndex {
        offsets: (0..=rows as u64).collect(),
        edge_ids: (0..rows as u64).collect(),
        neighbor_ids: (0..rows as u64).collect(),
    };
    let started = std::time::Instant::now();
    write_sharded_csr(&path, &expected, DEFAULT_CSR_SHARD_EDGES).unwrap();
    let reader = ShardedCsrIndex::open(&path).unwrap();
    assert_eq!(reader.manifest.shards.len(), shards);
    let encoded: u64 = reader
        .manifest
        .shards
        .iter()
        .map(|shard| shard.encoded_bytes)
        .sum();
    // Neighbour-ordered frontier: consecutive rows land in different
    // shards, the shape the S18 two-hop run paid 741 GB of re-reads for.
    for index in 0..frontier_rows {
        let node = ((index * 1_000_003) % rows) as u64;
        assert_eq!(reader.row(node).unwrap().len(), 1);
    }
    println!(
        "S18_ALTERNATING_FRONTIER {}",
        serde_json::json!({
            "shards": shards,
            "edges": rows,
            "frontier_rows": frontier_rows,
            "encoded_bytes": encoded,
            "shard_decodes": reader.shard_decode_count(),
            "retained_decoded_bytes": reader.retained_decoded_bytes(),
            "build_and_probe_ms": started.elapsed().as_millis() as u64,
        })
    );
}

/// Sequential traversal still pays exactly one decode per shard.
#[test]
fn sequential_traversal_decodes_each_shard_once() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = CsrIndex {
        offsets: vec![0, 1, 2, 3, 4, 5, 6],
        edge_ids: (100..106).collect(),
        neighbor_ids: (200..206).collect(),
    };
    write_sharded_csr(&path, &expected, 2).unwrap();
    let reader = ShardedCsrIndex::open(&path).unwrap();
    for node in 0..reader.node_count() {
        assert_eq!(
            reader.row(node).unwrap(),
            expected.row(node).iter().collect::<Vec<_>>()
        );
    }
    assert_eq!(reader.shard_decode_count(), 3);
}

/// The byte-budgeted cache evicts least-recently-used shards and never
/// retains more than `max(budget, one shard)` decoded bytes.
#[test]
fn decoded_shard_cache_evicts_by_bytes_and_serves_correct_rows() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = CsrIndex {
        offsets: vec![0, 1, 2, 3, 4, 5, 6],
        edge_ids: (100..106).collect(),
        neighbor_ids: (200..206).collect(),
    };
    write_sharded_csr(&path, &expected, 2).unwrap();
    let manifest_bytes = std::fs::read(path.with_extension("csr.json")).unwrap();
    let manifest: CsrShardManifest = serde_json::from_slice(&manifest_bytes).unwrap();
    let one_shard = manifest.shards[0].decoded_bytes;
    let reader = open_with_budget(&path, one_shard.saturating_mul(3) / 2).unwrap();
    assert_eq!(reader.manifest.shards.len(), 3);

    // Every read stays correct while the cache churns: the budget holds
    // one shard, so an alternating frontier degenerates to one decode per
    // access — bounded memory, honest cost.
    for _ in 0..3 {
        for node in [0_u64, 3, 1, 4, 2, 5] {
            assert_eq!(
                reader.row(node).unwrap(),
                expected.row(node).iter().collect::<Vec<_>>()
            );
        }
    }
    let cache = reader.cache.lock().unwrap();
    assert!(
        cache.retained_bytes <= one_shard,
        "retained {} exceeds the one-shard bound",
        cache.retained_bytes
    );
    assert_eq!(cache.entries.len(), 1);
}

/// A budget that fits every shard retains them all: the S18 shape.
#[test]
fn decoded_shard_cache_holds_every_shard_within_budget() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = CsrIndex {
        offsets: vec![0, 1, 2, 3, 4, 5, 6],
        edge_ids: (100..106).collect(),
        neighbor_ids: (200..206).collect(),
    };
    write_sharded_csr(&path, &expected, 2).unwrap();
    let manifest_bytes = std::fs::read(path.with_extension("csr.json")).unwrap();
    let manifest: CsrShardManifest = serde_json::from_slice(&manifest_bytes).unwrap();
    let total: u64 = manifest
        .shards
        .iter()
        .map(|shard| shard.decoded_bytes)
        .sum();
    let reader = open_with_budget(&path, total).unwrap();
    for node in [0_u64, 3, 1, 4, 2, 5, 0, 5, 1, 4, 2, 3] {
        reader.row(node).unwrap();
    }
    let cache = reader.cache.lock().unwrap();
    assert_eq!(cache.entries.len(), 3);
    assert_eq!(cache.retained_bytes, total);
    assert!(cache.retained_bytes <= cache.budget_bytes);
}

/// Opening with the default budget wires the documented 1 GiB bound.
#[test]
fn default_cache_budget_matches_documented_bytes() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    write_sharded_csr(&path, &sample_csr(), 2).unwrap();
    let reader = ShardedCsrIndex::open(&path).unwrap();
    assert_eq!(
        reader.cache.lock().unwrap().budget_bytes,
        DEFAULT_DECODED_SHARD_CACHE_BYTES
    );
    assert_eq!(DEFAULT_DECODED_SHARD_CACHE_BYTES, 1024 * 1024 * 1024);
}

/// A failed shard authentication leaves every retained entry in place:
/// eviction only happens for a fully validated replacement.
#[test]
fn failed_decode_evicts_nothing() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let expected = CsrIndex {
        offsets: vec![0, 1, 2, 3, 4, 5, 6],
        edge_ids: (100..106).collect(),
        neighbor_ids: (200..206).collect(),
    };
    write_sharded_csr(&path, &expected, 2).unwrap();
    let manifest_bytes = std::fs::read(path.with_extension("csr.json")).unwrap();
    let manifest: CsrShardManifest = serde_json::from_slice(&manifest_bytes).unwrap();
    let total: u64 = manifest
        .shards
        .iter()
        .map(|shard| shard.decoded_bytes)
        .sum();
    let mut reader = open_with_budget(&path, total).unwrap();

    // Retain the first two shards, then corrupt the third shard payload.
    reader.row(0).unwrap();
    reader.row(3).unwrap();
    let victim = &mut reader.manifest.shards[2];
    let victim_path = reader.root.join(&victim.file);
    let mut bytes = std::fs::read(&victim_path).unwrap();
    bytes[0] ^= 0xFF;
    victim.sha256 = sha256_hex(&bytes);
    victim.xxh64 = crate::corruption_checksum::checksum(&bytes);
    std::fs::write(&victim_path, bytes).unwrap();

    assert!(reader.row(5).is_err());
    let cache = reader.cache.lock().unwrap();
    assert_eq!(cache.entries.len(), 2);
    assert_eq!(cache.decodes, 2);
    assert_eq!(
        cache.retained_bytes,
        total - manifest.shards[2].decoded_bytes
    );
}

#[cfg(test)]
fn open_with_budget(path: &Path, budget_bytes: u64) -> Result<ShardedCsrIndex, GfError> {
    let reader = ShardedCsrIndex::open(path)?;
    let manifest = reader.manifest.clone();
    Ok(ShardedCsrIndex {
        root: reader.root.clone(),
        manifest,
        cache: std::sync::Arc::new(std::sync::Mutex::new(DecodedShardCache::new(budget_bytes))),
    })
}

#[test]
fn sparse_surrogate_gap_cannot_expand_one_shard_offsets() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    let mut writer = ShardedCsrWriter::create(&path, 8, 2).unwrap();
    writer.emit((0, 1, 100)).unwrap();
    writer.emit((1_000_000, 2, 0)).unwrap();
    let (shards, _, peak_nodes, _) = writer.finish(1_000_001).unwrap();
    assert_eq!(shards, 2);
    assert!(peak_nodes <= 2);
    let reader = ShardedCsrIndex::open(&path).unwrap();
    assert_eq!(reader.row(0).unwrap(), vec![(1, 100)]);
    assert_eq!(reader.row(999_999).unwrap(), Vec::<(u64, u64)>::new());
    assert_eq!(reader.row(1_000_000).unwrap(), vec![(2, 0)]);
}

#[test]
fn unsupported_csr_manifest_version_is_refused() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("KNOWS.out.csr");
    write_sharded_csr(&path, &sample_csr(), 2).unwrap();
    let manifest_path = path.with_extension("csr.json");
    let mut manifest: CsrShardManifest =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest.version = 1;
    std::fs::write(manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert!(ShardedCsrIndex::open(&path).is_err());
    assert!(read_csr(&path).is_err());
}

#[test]
fn entries_from_out_csr_rejects_malformed_offset_bounds() {
    let past_targets = CsrIndex {
        offsets: vec![0, 2],
        edge_ids: vec![7],
        neighbor_ids: vec![9],
    };
    assert!(entries_from_out_csr(&past_targets).is_none());

    let descending = CsrIndex {
        offsets: vec![1, 0],
        edge_ids: vec![7],
        neighbor_ids: vec![9],
    };
    assert!(entries_from_out_csr(&descending).is_none());

    let mismatched_columns = CsrIndex {
        offsets: vec![0, 1],
        edge_ids: vec![7],
        neighbor_ids: vec![],
    };
    assert!(entries_from_out_csr(&mismatched_columns).is_none());

    let missing_offsets = CsrIndex {
        offsets: vec![],
        edge_ids: vec![],
        neighbor_ids: vec![],
    };
    assert_eq!(entries_from_out_csr(&missing_offsets), Some(Vec::new()));
}

#[test]
fn wave10_effective_entry_decode_rejects_malformed_csr_bounds() {
    for malformed in [
        CsrIndex {
            offsets: vec![0, 2],
            edge_ids: vec![1],
            neighbor_ids: vec![2],
        },
        CsrIndex {
            offsets: vec![1, 0],
            edge_ids: vec![1],
            neighbor_ids: vec![2],
        },
        CsrIndex {
            offsets: vec![0, 1],
            edge_ids: vec![1],
            neighbor_ids: vec![],
        },
    ] {
        assert!(entries_from_out_csr(&malformed).is_none());
    }
}

#[test]
fn public_csr_writer_rejects_every_inconsistent_topology_shape_without_file() {
    let dir = TempDir::new().unwrap();
    let path = csr_path(dir.path(), "BROKEN", Direction::Out);
    for malformed in [
        CsrIndex {
            offsets: vec![],
            edge_ids: vec![],
            neighbor_ids: vec![],
        },
        CsrIndex {
            offsets: vec![0, 2],
            edge_ids: vec![1],
            neighbor_ids: vec![2],
        },
        CsrIndex {
            offsets: vec![0, 1],
            edge_ids: vec![1],
            neighbor_ids: vec![],
        },
        CsrIndex {
            offsets: vec![1],
            edge_ids: vec![],
            neighbor_ids: vec![],
        },
    ] {
        assert_eq!(
            write_sharded_csr(&path, &malformed, DEFAULT_CSR_SHARD_EDGES)
                .unwrap_err()
                .code(),
            "GF_IO"
        );
        assert!(!path.exists());
    }
}

#[test]
fn csr_round_trip_preserves_offsets_and_targets() {
    let dir = TempDir::new().unwrap();
    let path = csr_path(dir.path(), "KNOWS", Direction::Out);
    let csr = sample_csr();
    write_sharded_csr(&path, &csr, DEFAULT_CSR_SHARD_EDGES).unwrap();
    assert_eq!(read_csr(&path).unwrap(), csr);
}

#[test]
fn csr_row_lookup_is_o1_and_handles_empty_boundary_and_oor() {
    let csr = sample_csr();
    let row0 = csr.row(0);
    assert_eq!(row0.len(), 2);
    assert_eq!(row0.get(0), Some((csr.edge_ids[0], csr.neighbor_ids[0])));
    assert_eq!(row0.get(1), Some((csr.edge_ids[1], csr.neighbor_ids[1])));
    assert!(csr.row(1).is_empty(), "empty interior row");
    assert_eq!(csr.row(2).len(), 2);
    assert!(csr.row(3).is_empty(), "out of range");
    assert!(csr.row(u64::MAX).is_empty());
    let empty = CsrIndex {
        offsets: vec![0],
        ..CsrIndex::default()
    };
    assert!(empty.row(0).is_empty());
}

#[test]
fn empty_graph_round_trips_as_offsets_zero() {
    let dir = TempDir::new().unwrap();
    let path = csr_path(dir.path(), "KNOWS", Direction::In);
    let csr = CsrIndex {
        offsets: vec![0],
        ..CsrIndex::default()
    };
    write_sharded_csr(&path, &csr, DEFAULT_CSR_SHARD_EDGES).unwrap();
    let back = read_csr(&path).unwrap();
    assert_eq!(back, csr);
    assert_eq!(back.node_count(), 0);
    assert_eq!(back.edge_count(), 0);
}

#[test]
fn node_with_no_neighbors_round_trips() {
    let dir = TempDir::new().unwrap();
    let path = csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out);
    let csr = sample_csr(); // node 1 has an empty range
    write_sharded_csr(&path, &csr, DEFAULT_CSR_SHARD_EDGES).unwrap();
    let back = read_csr(&path).unwrap();
    assert_eq!(back.offsets[1], back.offsets[2], "node 1 has no neighbors");
    assert_eq!(back.node_count(), 3);
    assert_eq!(back.edge_count(), 4);
}

#[test]
fn write_csr_replaces_existing_file_atomically() {
    let dir = TempDir::new().unwrap();
    let path = csr_path(dir.path(), "KNOWS", Direction::Out);
    write_sharded_csr(&path, &sample_csr(), DEFAULT_CSR_SHARD_EDGES).unwrap();

    let newer = CsrIndex {
        offsets: vec![0, 1],
        edge_ids: vec![99],
        neighbor_ids: vec![0],
    };
    write_sharded_csr(&path, &newer, DEFAULT_CSR_SHARD_EDGES).unwrap();
    assert_eq!(read_csr(&path).unwrap(), newer, "second write wins");

    let temps = std::fs::read_dir(adjacency_dir(dir.path()))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
        .count();
    assert_eq!(temps, 0, "no temp residue");
}

#[test]
fn read_csr_rejects_wrong_schema() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bogus.csr");
    std::fs::write(&path, b"not an arrow ipc file").unwrap();
    assert!(matches!(read_csr(&path), Err(GfError::Storage(_))));
}

#[test]
fn read_csr_missing_file_is_an_error() {
    let dir = TempDir::new().unwrap();
    let path = csr_path(dir.path(), "ABSENT", Direction::Out);
    assert!(matches!(read_csr(&path), Err(GfError::Storage(_))));
}

#[test]
fn write_csr_rejects_invalid_offsets() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bad.csr");

    // Non-monotone offsets.
    let non_monotone = CsrIndex {
        offsets: vec![0, 3, 1],
        edge_ids: vec![1],
        neighbor_ids: vec![1],
    };
    assert!(matches!(
        write_sharded_csr(&path, &non_monotone, DEFAULT_CSR_SHARD_EDGES),
        Err(GfError::Storage(_))
    ));

    // Final offset disagrees with target lengths.
    let length_mismatch = CsrIndex {
        offsets: vec![0, 2],
        edge_ids: vec![1],
        neighbor_ids: vec![1],
    };
    assert!(matches!(
        write_sharded_csr(&path, &length_mismatch, DEFAULT_CSR_SHARD_EDGES),
        Err(GfError::Storage(_))
    ));

    // Empty offsets (the empty graph must be [0], not []).
    let empty_offsets = CsrIndex::default();
    assert!(matches!(
        write_sharded_csr(&path, &empty_offsets, DEFAULT_CSR_SHARD_EDGES),
        Err(GfError::Storage(_))
    ));

    // Targets of differing lengths.
    let ragged = CsrIndex {
        offsets: vec![0, 2],
        edge_ids: vec![1, 2],
        neighbor_ids: vec![1],
    };
    assert!(matches!(
        write_sharded_csr(&path, &ragged, DEFAULT_CSR_SHARD_EDGES),
        Err(GfError::Storage(_))
    ));

    assert!(!path.exists(), "no file written for invalid CSR");
}

#[test]
fn manifest_round_trip_multi_relation() {
    const TS: i64 = 1_700_000_000_000_000;
    let dir = TempDir::new().unwrap();
    let rows = vec![
        AdjacencyManifestRow {
            relation_type: "WORKS_AT".to_owned(),
            direction: Direction::Out,
            topology_generation: 7,
            built_at_micros: TS,
            node_count: 100,
            edge_count: 250,
        },
        AdjacencyManifestRow {
            relation_type: "WORKS_AT".to_owned(),
            direction: Direction::In,
            topology_generation: 7,
            built_at_micros: TS,
            node_count: 100,
            edge_count: 250,
        },
        AdjacencyManifestRow {
            relation_type: "OWNS".to_owned(),
            direction: Direction::Out,
            topology_generation: 7,
            built_at_micros: TS + 1,
            node_count: 40,
            edge_count: 41,
        },
        AdjacencyManifestRow {
            relation_type: ALL_RELATIONS_STEM.to_owned(),
            direction: Direction::Out,
            topology_generation: 7,
            built_at_micros: TS + 2,
            node_count: 100,
            edge_count: 291,
        },
    ];
    write_manifest(dir.path(), &rows).unwrap();
    assert_eq!(read_manifest(dir.path()).unwrap(), rows);
}

#[test]
fn read_manifest_absent_returns_empty() {
    let dir = TempDir::new().unwrap();
    assert_eq!(read_manifest(dir.path()).unwrap(), Vec::new());
}

#[test]
fn write_manifest_replaces_existing() {
    let dir = TempDir::new().unwrap();
    let first = vec![AdjacencyManifestRow {
        relation_type: "KNOWS".to_owned(),
        direction: Direction::Out,
        topology_generation: 1,
        built_at_micros: 0,
        node_count: 1,
        edge_count: 1,
    }];
    write_manifest(dir.path(), &first).unwrap();

    let second = vec![AdjacencyManifestRow {
        relation_type: "KNOWS".to_owned(),
        direction: Direction::Out,
        topology_generation: 2,
        built_at_micros: 1,
        node_count: 2,
        edge_count: 3,
    }];
    write_manifest(dir.path(), &second).unwrap();
    assert_eq!(read_manifest(dir.path()).unwrap(), second);
}

#[test]
fn read_manifest_rejects_wrong_schema() {
    let dir = TempDir::new().unwrap();
    // Write a valid Parquet file with the wrong schema at the manifest path.
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "v",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut staged = RewriteBatch::new();
    staged
        .stage(&manifest_path(dir.path()), schema, &batch)
        .unwrap();
    staged.commit_at(dir.path()).unwrap();

    assert!(matches!(
        read_manifest(dir.path()),
        Err(GfError::Storage(_))
    ));
}

#[test]
fn direction_round_trips_through_str() {
    for d in [Direction::Out, Direction::In] {
        assert_eq!(Direction::parse(d.as_str()).unwrap(), d);
    }
    assert!(matches!(
        Direction::parse("sideways"),
        Err(GfError::Storage(_))
    ));
}

#[test]
fn csr_path_layout() {
    let p = csr_path(Path::new("/proj"), "WORKS_AT", Direction::In);
    assert_eq!(
        p,
        Path::new("/proj/indexes/adjacency").join(format!(
            "{}.in.csr",
            crate::route_component::component("WORKS_AT")
        ))
    );
    assert_eq!(
        manifest_path(Path::new("/proj")),
        Path::new("/proj/indexes/adjacency/index_manifest.parquet")
    );
}

// -----------------------------------------------------------------------
// build_adjacency_index (#761)
// -----------------------------------------------------------------------

use crate::GraphWriter;
use graphforge_core::uuid::{new_v7, Uuid};
use graphforge_core::OntologyMode;
use graphforge_core::TypeId;

/// Fixed timestamp for deterministic fixtures and manifests.
pub(super) const BUILD_TS: i64 = 1_700_000_000_000_000;

/// Strict-mode diamond a->b, a->c, b->d, c->d plus a parallel a->b and a
/// self-loop d->d, all KNOWS. Returns the surrogate node ids.
pub(super) fn write_diamond(dir: &Path) -> [u64; 4] {
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, BUILD_TS).unwrap();
    let uuids: Vec<Uuid> = (0..4).map(|_| new_v7()).collect();
    let ids: Vec<u64> = uuids
        .iter()
        .map(|u| {
            w.create_node(
                *u,
                graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
            )
            .unwrap()
        })
        .collect();
    let (a, b, c, d) = (&uuids[0], &uuids[1], &uuids[2], &uuids[3]);
    for (src, dst) in [(a, b), (a, c), (b, d), (c, d), (a, b), (d, d)] {
        w.create_edge(new_v7(), "KNOWS", src, dst).unwrap();
    }
    w.flush().unwrap();
    [ids[0], ids[1], ids[2], ids[3]]
}

// -----------------------------------------------------------------------
// validate_adjacency_index (#766)
// -----------------------------------------------------------------------

#[test]
fn validate_reports_clean_on_valid_index() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    assert_eq!(validate_adjacency_index(dir.path()).unwrap(), Vec::new());
}

#[test]
fn validate_reports_clean_on_absent_index() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    assert_eq!(validate_adjacency_index(dir.path()).unwrap(), Vec::new());
}

#[test]
fn inspection_moves_from_missing_to_current_with_stable_identity() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    let missing = inspect_adjacency_index(dir.path()).unwrap();
    assert_eq!(missing.state, AdjacencyFreshnessState::Missing);
    assert_eq!(missing.reason, Some(AdjacencyFreshnessReason::NotBuilt));
    assert!(missing.source_fingerprint.starts_with("sha256:"));

    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    let current = inspect_adjacency_index(dir.path()).unwrap();
    assert_eq!(current.state, AdjacencyFreshnessState::Current);
    assert_eq!(current.reason, None);
    assert_eq!(current.artifact_generation, Some(current.source_generation));
    assert_eq!(
        current.artifact_fingerprint.as_deref(),
        Some(current.source_fingerprint.as_str())
    );
}

#[test]
fn inspection_checks_every_manifest_referenced_csr() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    let path = csr_path(dir.path(), "KNOWS", Direction::In);
    let reader = ShardedCsrIndex::open(&path).unwrap();
    std::fs::write(
        reader.root.join(&reader.manifest.shards[0].file),
        b"garbage",
    )
    .unwrap();

    let inspection = inspect_adjacency_index(dir.path()).unwrap();
    assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
    assert_eq!(
        inspection.reason,
        Some(AdjacencyFreshnessReason::UnreadableArtifact)
    );
}

#[test]
fn inspection_distinguishes_manifest_union_absence_and_union_corruption_after_reopen() {
    for case in ["manifest", "missing-union", "corrupt-union"] {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        match case {
            "manifest" => std::fs::write(manifest_path(dir.path()), b"corrupt").unwrap(),
            "missing-union" => {
                std::fs::remove_file(
                    csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out)
                        .with_extension("csr.json"),
                )
                .unwrap();
            }
            "corrupt-union" => {
                let path = csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out);
                let reader = ShardedCsrIndex::open(&path).unwrap();
                std::fs::write(
                    reader.root.join(&reader.manifest.shards[0].file),
                    b"corrupt",
                )
                .unwrap();
            }
            _ => unreachable!(),
        }

        let inspection = inspect_adjacency_index(dir.path()).unwrap();
        assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
        assert_eq!(
            inspection.reason,
            Some(if case == "missing-union" {
                AdjacencyFreshnessReason::MissingCsr
            } else {
                AdjacencyFreshnessReason::UnreadableArtifact
            })
        );
        assert_eq!(inspection.artifact_fingerprint, None);
    }
}

#[test]
fn inspection_accepts_only_a_complete_delta_chain_as_current() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();

    let inspection = inspect_adjacency_index(dir.path()).unwrap();
    assert_eq!(inspection.state, AdjacencyFreshnessState::Stale);
    assert_eq!(
        inspection.reason,
        Some(AdjacencyFreshnessReason::IncompleteDeltaChain)
    );
}

#[test]
fn freshness_vocabulary_and_manifest_generation_failures_are_exact() {
    assert_eq!(AdjacencyFreshnessState::Current.as_str(), "current");
    assert_eq!(AdjacencyFreshnessState::Missing.as_str(), "missing");
    assert_eq!(AdjacencyFreshnessState::Stale.as_str(), "stale");
    assert_eq!(
        AdjacencyFreshnessState::Incompatible.as_str(),
        "incompatible"
    );
    for (reason, token) in [
        (AdjacencyFreshnessReason::NotBuilt, "not_built"),
        (
            AdjacencyFreshnessReason::MixedArtifactGeneration,
            "mixed_artifact_generation",
        ),
        (
            AdjacencyFreshnessReason::IncompleteDeltaChain,
            "incomplete_delta_chain",
        ),
        (AdjacencyFreshnessReason::MissingCsr, "missing_csr"),
        (
            AdjacencyFreshnessReason::UnreadableArtifact,
            "unreadable_artifact",
        ),
        (
            AdjacencyFreshnessReason::ContentMismatch,
            "content_mismatch",
        ),
        (
            AdjacencyFreshnessReason::FutureArtifactGeneration,
            "future_artifact_generation",
        ),
    ] {
        assert_eq!(reason.as_str(), token);
    }

    let mixed = TempDir::new().unwrap();
    write_diamond(mixed.path());
    build_adjacency_index(mixed.path(), BUILD_TS).unwrap();
    let mut manifest = read_manifest(mixed.path()).unwrap();
    manifest[0].topology_generation += 1;
    write_manifest(mixed.path(), &manifest).unwrap();
    let inspection = inspect_adjacency_index(mixed.path()).unwrap();
    assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
    assert_eq!(
        inspection.reason,
        Some(AdjacencyFreshnessReason::MixedArtifactGeneration)
    );
    assert_eq!(inspection.artifact_generation, None);

    let future = TempDir::new().unwrap();
    write_diamond(future.path());
    build_adjacency_index(future.path(), BUILD_TS).unwrap();
    let mut manifest = read_manifest(future.path()).unwrap();
    for row in &mut manifest {
        row.topology_generation += 1;
    }
    write_manifest(future.path(), &manifest).unwrap();
    let inspection = inspect_adjacency_index(future.path()).unwrap();
    assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
    assert_eq!(
        inspection.reason,
        Some(AdjacencyFreshnessReason::FutureArtifactGeneration)
    );
    assert_eq!(inspection.artifact_generation, Some(2));
    assert_eq!(inspection.artifact_fingerprint, None);
}

#[test]
fn validate_detects_corrupted_csr() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();

    // Overwrite KNOWS.out.csr with a VALID but WRONG CSR (content swap).
    let bogus = CsrIndex {
        offsets: vec![0, 1],
        edge_ids: vec![99],
        neighbor_ids: vec![1],
    };
    write_sharded_csr(
        &csr_path(dir.path(), "KNOWS", Direction::Out),
        &bogus,
        DEFAULT_CSR_SHARD_EDGES,
    )
    .unwrap();

    let issues = validate_adjacency_index(dir.path()).unwrap();
    assert_eq!(
        issues,
        vec![AdjacencyValidationIssue::Mismatch {
            rel: "KNOWS".to_owned(),
            direction: Direction::Out,
        }]
    );
}

#[test]
fn validate_detects_unreadable_csr() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    let path = csr_path(dir.path(), "KNOWS", Direction::In);
    let reader = ShardedCsrIndex::open(&path).unwrap();
    std::fs::write(
        reader.root.join(&reader.manifest.shards[0].file),
        b"garbage",
    )
    .unwrap();

    let issues = validate_adjacency_index(dir.path()).unwrap();
    assert_eq!(issues.len(), 1);
    assert!(matches!(
        &issues[0],
        AdjacencyValidationIssue::UnreadableCsr { rel, direction: Direction::In, .. }
            if rel == "KNOWS"
    ));
}

#[test]
fn validate_detects_missing_csr() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    std::fs::remove_file(
        csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out).with_extension("csr.json"),
    )
    .unwrap();

    let issues = validate_adjacency_index(dir.path()).unwrap();
    assert_eq!(
        issues,
        vec![AdjacencyValidationIssue::MissingCsr {
            rel: ALL_RELATIONS_STEM.to_owned(),
            direction: Direction::Out,
        }]
    );
}

#[test]
fn validate_detects_stale_generation_only_once() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    // Bump the counter without touching topology content: the index is
    // stale but its content still matches the (unchanged) edge files.
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();

    let issues = validate_adjacency_index(dir.path()).unwrap();
    assert_eq!(
        issues,
        vec![AdjacencyValidationIssue::StaleGeneration {
            manifest: 1,
            current: 2,
        }]
    );
}

/// #765: a delta-covered index (stale by generation but an intact chain
/// covers the gap) validates **clean** — no `StaleGeneration`, no
/// `Mismatch` — because the base CSR + chain overlay equals a rebuild at
/// the current generation.
#[test]
fn validate_clean_on_delta_covered_index() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();

    // A pure-append flush writes a delta segment (bumps the generation).
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, BUILD_TS).unwrap();
    let (a, b) = (new_v7(), new_v7());
    w.create_node(
        a,
        graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
    )
    .unwrap();
    w.create_node(
        b,
        graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
    )
    .unwrap();
    w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
    w.flush().unwrap();

    assert!(
        validate_adjacency_index(dir.path()).unwrap().is_empty(),
        "base + intact chain == current rebuild ⇒ no issues"
    );
    let inspection = inspect_adjacency_index(dir.path()).unwrap();
    assert_eq!(inspection.state, AdjacencyFreshnessState::Current);
    assert_eq!(
        inspection.artifact_effective_generation,
        Some(inspection.source_generation)
    );
    assert_eq!(
        inspection.artifact_fingerprint.as_deref(),
        Some(inspection.source_fingerprint.as_str())
    );
}

/// A corrupt base CSR under a delta chain is still caught: the overlay diff
/// against the current rebuild reports a `Mismatch` (delta coverage does not
/// mask corruption).
#[test]
fn validate_detects_mismatch_under_delta_chain() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, BUILD_TS).unwrap();
    let (a, b) = (new_v7(), new_v7());
    w.create_node(
        a,
        graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
    )
    .unwrap();
    w.create_node(
        b,
        graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
    )
    .unwrap();
    w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
    w.flush().unwrap();

    // Corrupt the base KNOWS.out CSR by overwriting it with an empty index
    // (the diamond has only KNOWS, so the _all CSR is identical and would
    // not be a corruption).
    let knows_out = csr_path(dir.path(), "KNOWS", Direction::Out);
    write_sharded_csr(
        &knows_out,
        &csr_from_entries(&[], Direction::Out),
        DEFAULT_CSR_SHARD_EDGES,
    )
    .unwrap();

    let issues = validate_adjacency_index(dir.path()).unwrap();
    assert!(
        issues.contains(&AdjacencyValidationIssue::Mismatch {
            rel: "KNOWS".to_owned(),
            direction: Direction::Out,
        }),
        "corruption under a chain is still detected: {issues:?}"
    );
    let inspection = inspect_adjacency_index(dir.path()).unwrap();
    assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
    assert_eq!(
        inspection.artifact_effective_generation,
        Some(inspection.source_generation)
    );
    assert!(inspection.artifact_fingerprint.is_some());
}

#[test]
fn wave13_csr_io_rejects_parentless_destination_and_wrong_arrow_schema() {
    let empty = CsrIndex {
        offsets: vec![0],
        edge_ids: vec![],
        neighbor_ids: vec![],
    };
    assert!(write_sharded_csr(Path::new("/"), &empty, DEFAULT_CSR_SHARD_EDGES).is_err());

    let root = TempDir::new().unwrap();
    let path = root.path().join("wrong-schema.arrow");
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "wrong",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(vec![1]))],
    )
    .unwrap();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    assert!(read_csr(&path).is_err());
}

// -----------------------------------------------------------------------
// Streaming / spill build (#336)
// -----------------------------------------------------------------------

pub(super) fn write_multi_row_group_knows(dir: &Path, edges: &[(u64, u64, u64)]) -> PathBuf {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    // Bootstrap project layout + generation via the writer, then replace the
    // typed edge file with a multi-row-group Parquet that still carries UUID
    // FixedSizeBinary columns (the concat hazard #336 removes).
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, BUILD_TS).unwrap();
    let max_node = edges.iter().map(|&(s, _, d)| s.max(d)).max().unwrap_or(0);
    let mut node_uuids = Vec::new();
    for _ in 0..=max_node {
        node_uuids.push(new_v7());
        w.create_node(
            *node_uuids.last().unwrap(),
            graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
        )
        .unwrap();
    }
    // At least one edge so flush creates topology/edges/.
    w.create_edge(
        new_v7(),
        "KNOWS",
        &node_uuids[0],
        &node_uuids[usize::try_from(max_node.min(1)).unwrap()],
    )
    .unwrap();
    w.flush().unwrap();

    drop(w);
    let inventory = capture_adjacency_inventory(dir).unwrap();
    let paths = inventory.edge_files(Some("KNOWS"));
    assert_eq!(paths.len(), 1);
    let edges_path = paths[0].1.clone();
    let schema = crate::schemas::TYPED_EDGE_SCHEMA.clone();
    let n = edges.len();
    let edge_uuid = arrow::array::FixedSizeBinaryArray::try_from_iter((0..n).map(|i| {
        let mut bytes = [0u8; 16];
        bytes[12..].copy_from_slice(&(i as u32).to_be_bytes());
        bytes
    }))
    .unwrap();
    let src_uuid =
        arrow::array::FixedSizeBinaryArray::try_from_iter((0..n).map(|_| [0u8; 16])).unwrap();
    let dst_uuid =
        arrow::array::FixedSizeBinaryArray::try_from_iter((0..n).map(|_| [1u8; 16])).unwrap();
    let edge_id = UInt64Array::from(edges.iter().map(|e| e.1).collect::<Vec<_>>());
    let src_id = UInt64Array::from(edges.iter().map(|e| e.0).collect::<Vec<_>>());
    let dst_id = UInt64Array::from(edges.iter().map(|e| e.2).collect::<Vec<_>>());
    let created = TimestampMicrosecondArray::from(vec![BUILD_TS; n]).with_timezone("UTC");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(edge_uuid),
            Arc::new(src_uuid),
            Arc::new(dst_uuid),
            Arc::new(edge_id),
            Arc::new(src_id),
            Arc::new(dst_id),
            Arc::new(created),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let file = std::fs::File::create(&edges_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    edges_path
}

#[test]
fn streaming_reader_emits_multiple_batches_without_uuid_columns() {
    let dir = TempDir::new().unwrap();
    // 5 edges → 5 row groups with max_row_group_row_count=1.
    let edges = [(0, 1, 1), (0, 2, 2), (1, 3, 2), (2, 4, 3), (3, 5, 0)];
    let path = write_multi_row_group_knows(dir.path(), &edges);

    let mut batches = 0usize;
    let mut rows = 0usize;
    let count = stream_projected_parquet_batches(
        &path,
        &["edge_id", "src_id", "dst_id"],
        /* batch_size */ 1,
        &mut |batch| {
            batches += 1;
            rows += batch.num_rows();
            for field in batch.schema().fields() {
                assert!(
                    !matches!(field.data_type(), DataType::FixedSizeBinary(_)),
                    "UUID column {} must not be projected",
                    field.name()
                );
            }
            assert!(batch.column_by_name("edge_uuid").is_none());
            assert!(batch.column_by_name("src_uuid").is_none());
            assert!(batch.column_by_name("dst_uuid").is_none());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(count, batches);
    assert!(
        batches >= 5,
        "expected one batch per tiny row-group, got {batches}"
    );
    assert_eq!(rows, 5);
}

/// Deterministic seam: projected streaming never materializes a single
/// FixedSizeBinary(16) buffer covering every edge. CI uses a tiny fixture;
/// the ignored companion below documents the >134M boundary simulation.
#[test]
fn arrow_uuid_concat_boundary_is_avoided_by_projection() {
    let dir = TempDir::new().unwrap();
    let edges: Vec<(u64, u64, u64)> = (0..64).map(|i| (i % 8, i + 1, (i + 1) % 8)).collect();
    let path = write_multi_row_group_knows(dir.path(), &edges);

    // Full-schema eager path (what the old builder did) would concat UUID
    // columns to `edges.len()` values. The streaming path must not.
    let full =
        crate::catalog::read_parquet_or_empty(&path, crate::schemas::TYPED_EDGE_SCHEMA.clone())
            .unwrap();
    assert_eq!(full.len(), 1, "legacy helper still concats to one batch");
    let uuid = full[0]
        .column_by_name("edge_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(uuid.len(), edges.len());

    let mut projected_rows = 0usize;
    stream_projected_parquet_batches(&path, &["edge_id", "src_id", "dst_id"], 8, &mut |batch| {
        projected_rows += batch.num_rows();
        assert!(batch.column_by_name("edge_uuid").is_none());
        // Each projected batch stays well below the Arrow 2GiB UUID
        // buffer ceiling (134,217,728 FixedSizeBinary(16) values).
        assert!(batch.num_rows() <= 8);
        Ok(())
    })
    .unwrap();
    assert_eq!(projected_rows, edges.len());

    let options = AdjacencyBuildOptions {
        chunk_rows: 4,
        batch_size: 8,
        ..AdjacencyBuildOptions::default()
    };
    build_adjacency_index_into_with_options(
        dir.path(),
        dir.path(),
        BUILD_TS,
        &options,
        &mut || Ok(()),
    )
    .unwrap();
    let expected = csr_from_entries(
        &edges.iter().map(|&(s, e, d)| (s, e, d)).collect::<Vec<_>>(),
        Direction::Out,
    );
    assert_eq!(
        read_csr(&csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap(),
        expected
    );
}

/// Optional large-boundary simulation: tiny flush threshold stands in for
/// the 134,217,728 FixedSizeBinary concat ceiling without allocating it.
#[test]
#[ignore = "manual/scale: exercises many spill runs; run explicitly for #336 evidence"]
fn ignored_arrow_boundary_simulation_via_tiny_flush_threshold() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, BUILD_TS).unwrap();
    let nodes: Vec<_> = (0..256).map(|_| new_v7()).collect();
    for u in &nodes {
        w.create_node(
            *u,
            graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
        )
        .unwrap();
    }
    for i in 0..4_096 {
        let src = &nodes[i % nodes.len()];
        let dst = &nodes[(i * 7) % nodes.len()];
        w.create_edge(new_v7(), "KNOWS", src, dst).unwrap();
    }
    w.flush().unwrap();
    let options = AdjacencyBuildOptions {
        chunk_rows: 17, // awkward prime to stress merge
        batch_size: 13,
        spill_max_bytes: Some(64 * 1024 * 1024),
        ..AdjacencyBuildOptions::default()
    };
    build_adjacency_index_into_with_options(
        dir.path(),
        dir.path(),
        BUILD_TS,
        &options,
        &mut || Ok(()),
    )
    .unwrap();
    assert!(validate_adjacency_index(dir.path()).unwrap().is_empty());
}
