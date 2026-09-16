use super::super::INDEX_DIR;
use super::super::MANIFEST;
use super::super::Manifest;
use super::super::TopologyIndexReceipt;
use super::super::UuidIndexBuildLimits;
use super::super::UuidIndexKind;
use super::super::UuidMembershipIndex;
use super::super::V4_ORDINAL_BLOCK_BYTES;
use super::super::V4_ORDINAL_MANIFEST;
use super::super::V4_ORDINAL_RECEIPT;
use super::super::V4OrdinalRebuildDisposition;
use super::super::tests::fixture;
use super::super::tests::make_installed_manifest_stale;
use super::super::tests::receipt_manifest_digest;
use super::super::tests::write_node_parquet;
use super::super::tests::write_node_parquet_with_ids;
use super::super::tests::write_uuid_parquet;
use super::super::topology_delta::hex_sha256;
use super::V4RebuildScratchAccounting;
use super::rebuild_uuid_membership_indexes;
use super::rebuild_v4_ordinal_identity;
use super::rebuild_v4_ordinal_identity_with_evidence;
use std::fs;
use std::sync::Arc;
use std::sync::Barrier;
use uuid::Uuid;

#[test]
fn forced_rebuild_receipt_binds_newly_staged_manifest() {
    let (dir, _, _) = fixture();
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let stale_digest = make_installed_manifest_stale(dir.path());
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let installed_digest = receipt_manifest_digest(dir.path());
    assert_ne!(installed_digest, stale_digest);
}

#[test]
fn unpublished_build_artifacts_do_not_change_concurrent_readers() {
    let (dir, nodes, _) = fixture();
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let reader_root = dir.path().to_path_buf();
    let reader_barrier = barrier.clone();
    let expected = nodes[0];
    let reader = std::thread::spawn(move || {
        let mut index = UuidMembershipIndex::open(&reader_root).unwrap();
        reader_barrier.wait();
        index.probe(UuidIndexKind::Node, &[expected]).unwrap().0
    });
    fs::write(
        dir.path().join(INDEX_DIR).join("nodes-unpublished.uuidx"),
        [7_u8; 16],
    )
    .unwrap();
    barrier.wait();
    assert_eq!(reader.join().unwrap(), vec![true]);
    let mut reopened = UuidMembershipIndex::open(dir.path()).unwrap();
    assert_eq!(
        reopened.probe(UuidIndexKind::Node, &[expected]).unwrap().0,
        vec![true]
    );
}

#[test]
fn concurrent_rebuilds_publish_one_authenticated_snapshot() {
    let (dir, _, _) = fixture();
    let root = Arc::new(dir.path().to_path_buf());
    let barrier = Arc::new(Barrier::new(3));
    let workers = (0..2)
        .map(|_| {
            let root = root.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                rebuild_uuid_membership_indexes(
                    &root,
                    UuidIndexBuildLimits {
                        scan_batch_rows: 1,
                        run_records: 1,
                        merge_fan_in: 2,
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    for worker in workers {
        worker.join().unwrap().unwrap();
    }
    let index = UuidMembershipIndex::open(&root).unwrap();
    assert_eq!(index.count(UuidIndexKind::Node), 3);
    assert_eq!(index.count(UuidIndexKind::Edge), 2);
    let names = fs::read_dir(root.join(INDEX_DIR))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(names.iter().all(|name| !name.ends_with(".tmp")));
}

#[test]
fn duplicate_and_cross_kind_identities_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let repeated = Uuid::from_u128(7);
    write_node_parquet(
        &dir.path().join("topology/nodes.parquet"),
        &[repeated, repeated],
    );
    let duplicate = rebuild_uuid_membership_indexes(
        dir.path(),
        UuidIndexBuildLimits {
            scan_batch_rows: 1,
            run_records: 1,
            merge_fan_in: 2,
        },
    )
    .unwrap_err();
    assert!(duplicate.to_string().contains("duplicate"));

    let dir = tempfile::tempdir().unwrap();
    write_node_parquet(&dir.path().join("topology/nodes.parquet"), &[repeated]);
    write_uuid_parquet(
        &dir.path().join("topology/edges/R.parquet"),
        "edge_uuid",
        &[repeated],
    );
    let cross_kind =
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap_err();
    assert!(cross_kind.to_string().contains("both node and edge"));
}

#[test]
fn duplicate_and_zero_node_surrogates_fail_closed_across_bounded_runs() {
    let limits = UuidIndexBuildLimits {
        scan_batch_rows: 1,
        run_records: 1,
        merge_fan_in: 2,
    };
    let dir = tempfile::tempdir().unwrap();
    let nodes = [Uuid::from_u128(1), Uuid::from_u128(2)];
    write_node_parquet_with_ids(&dir.path().join("topology/nodes.parquet"), &nodes, &[7, 7]);
    assert!(
        rebuild_uuid_membership_indexes(dir.path(), limits)
            .unwrap_err()
            .to_string()
            .contains("duplicate node surrogate")
    );

    let dir = tempfile::tempdir().unwrap();
    write_node_parquet_with_ids(
        &dir.path().join("topology/nodes.parquet"),
        &[Uuid::from_u128(3)],
        &[0],
    );
    assert!(
        rebuild_uuid_membership_indexes(dir.path(), limits)
            .unwrap_err()
            .to_string()
            .contains("invalid node surrogate")
    );
}

#[test]
fn explicit_v4_rebuild_uses_topology_and_independent_projection_orders() {
    let dir = tempfile::tempdir().unwrap();
    let nodes = [
        Uuid::from_u128(30),
        Uuid::from_u128(10),
        Uuid::from_u128(20),
    ];
    write_node_parquet_with_ids(
        &dir.path().join("topology/nodes.parquet"),
        &nodes,
        &[2, 7, 3],
    );
    fs::write(
        dir.path().join("topology/generation.json"),
        b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
    )
    .unwrap();

    let evidence = rebuild_v4_ordinal_identity_with_evidence(
        dir.path(),
        UuidIndexBuildLimits {
            scan_batch_rows: 1,
            run_records: 1,
            merge_fan_in: 2,
        },
    )
    .unwrap();
    assert_eq!(
        evidence.disposition,
        V4OrdinalRebuildDisposition::CanonicalTopology
    );
    assert_eq!(evidence.topology_generation, 7);
    assert_eq!(evidence.input_identities, 3);
    assert_eq!(evidence.ordinal_ranges, 2);
    assert!(evidence.artifact_bytes >= 3 * (24 + 16));
    assert!(evidence.write_blocks >= 3);
    assert!(evidence.peak_buffer_bytes <= 3 * V4_ORDINAL_BLOCK_BYTES as u64);
    assert_eq!(evidence.build.node_count, 3);
    let root = dir.path().join(INDEX_DIR);
    let manifest_bytes = fs::read(root.join(V4_ORDINAL_MANIFEST)).unwrap();
    let manifest: crate::V4OrdinalIdentityManifest =
        serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(manifest.topology_generation, 7);
    assert_eq!(manifest.ordinal_ranges.len(), 2);
    assert_eq!(manifest.ordinal_ranges[0].first_node_id, 2);
    assert_eq!(manifest.ordinal_ranges[0].count, 2);
    assert_eq!(manifest.ordinal_ranges[1].first_node_id, 7);
    let forward = fs::read(root.join(&manifest.forward_identities[0].name)).unwrap();
    let forward_uuids = forward
        .chunks_exact(24)
        .map(|record| Uuid::from_bytes(record[..16].try_into().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(
        forward_uuids,
        vec![
            Uuid::from_u128(10),
            Uuid::from_u128(20),
            Uuid::from_u128(30)
        ]
    );
    let receipt: TopologyIndexReceipt =
        serde_json::from_slice(&fs::read(root.join(V4_ORDINAL_RECEIPT)).unwrap()).unwrap();
    assert_eq!(receipt.expected_generation, 7);
    assert_eq!(receipt.manifest_sha256, hex_sha256(&manifest_bytes));
    assert!(!dir.path().join(".graphforge-rewrite-v1.json").exists());
}

#[test]
fn explicit_v4_rebuild_does_not_trust_corrupt_v3_reverse_state() {
    let (dir, _, _) = fixture();
    fs::write(
        dir.path().join("topology/generation.json"),
        b"{\"topology_generation\":3,\"search_generation\":0,\"property_generation\":0}\n",
    )
    .unwrap();
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let root = dir.path().join(INDEX_DIR);
    let v3: Manifest = serde_json::from_slice(&fs::read(root.join(MANIFEST)).unwrap()).unwrap();
    fs::write(
        root.join(&v3.runs[0].node_surrogates.name),
        b"planted-corrupt-v3-reverse",
    )
    .unwrap();

    let metrics = rebuild_v4_ordinal_identity(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    assert_eq!(metrics.node_count, 3);
    assert!(root.join(V4_ORDINAL_MANIFEST).is_file());
    assert!(root.join(V4_ORDINAL_RECEIPT).is_file());
}

#[test]
fn v4_rebuild_scratch_accounting_is_exact_linear_and_overflow_bounded() {
    for identities in [1_u64, 2, 4] {
        let dir = tempfile::tempdir().unwrap();
        let uuids = (1..=identities)
            .map(|identity| Uuid::from_u128(u128::from(identity)))
            .collect::<Vec<_>>();
        let node_ids = (1..=identities).collect::<Vec<_>>();
        write_node_parquet_with_ids(
            &dir.path().join("topology/nodes.parquet"),
            &uuids,
            &node_ids,
        );
        fs::write(
            dir.path().join("topology/generation.json"),
            b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();
        let evidence = rebuild_v4_ordinal_identity_with_evidence(
            dir.path(),
            UuidIndexBuildLimits {
                scan_batch_rows: 1,
                run_records: 1,
                merge_fan_in: 2,
            },
        )
        .unwrap();
        let index = dir.path().join(INDEX_DIR);
        let manifest_bytes = fs::metadata(index.join(V4_ORDINAL_MANIFEST)).unwrap().len();
        let receipt_bytes = fs::metadata(index.join(V4_ORDINAL_RECEIPT)).unwrap().len();
        let with_live_scratch = identities * 48 + evidence.artifact_bytes * 2 + manifest_bytes;
        let after_scratch_release = evidence.artifact_bytes + manifest_bytes + receipt_bytes;
        assert_eq!(
            evidence.peak_temporary_bytes,
            with_live_scratch.max(after_scratch_release)
        );
    }

    let mut projection_overflow = V4RebuildScratchAccounting {
        sorted_projection_bytes: u64::MAX,
        ..Default::default()
    };
    assert!(projection_overflow.register_artifacts(1).is_err());
    let mut artifact_overflow = V4RebuildScratchAccounting {
        retained_staged_bytes: u64::MAX,
        ..Default::default()
    };
    assert!(artifact_overflow.register_staged_artifact(1).is_err());
    let mut control_overflow = V4RebuildScratchAccounting {
        staged_control_bytes: u64::MAX,
        ..Default::default()
    };
    assert!(control_overflow.register_staged_control(1).is_err());
}
