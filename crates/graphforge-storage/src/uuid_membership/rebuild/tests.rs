use super::super::INDEX_DIR;
use super::super::TopologyIndexReceipt;
use super::super::UuidIndexBuildLimits;
use super::super::V4_ORDINAL_BLOCK_BYTES;
use super::super::V4_ORDINAL_MANIFEST;
use super::super::V4_ORDINAL_RECEIPT;
use super::super::V4OrdinalRebuildDisposition;
use super::super::tests::fixture;
use super::super::tests::write_node_parquet_with_ids;
use super::super::topology_delta::hex_sha256;
use super::V4RebuildScratchAccounting;
use super::rebuild_v4_ordinal_identity;
use super::rebuild_v4_ordinal_identity_with_evidence;
use std::fs;
use uuid::Uuid;

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
fn explicit_v4_rebuild_ignores_legacy_membership_files() {
    let (dir, _, _) = fixture();
    fs::write(
        dir.path().join("topology/generation.json"),
        b"{\"topology_generation\":3,\"search_generation\":0,\"property_generation\":0}\n",
    )
    .unwrap();
    let root = dir.path().join(INDEX_DIR);
    // A project built before #1902 carries the membership index. Whatever its
    // bytes are, the rebuild reads canonical topology and never them.
    fs::write(root.join("manifest.json"), b"planted-corrupt-manifest").unwrap();
    fs::write(
        root.join("node-surrogates-v5-3-0000000000000000.uuidx"),
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
