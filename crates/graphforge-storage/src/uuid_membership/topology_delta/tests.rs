use super::super::CommittedUuidTopologyRewrite;
use super::super::INDEX_DIR;
use super::super::UuidIndexBuildLimits;
use super::super::UuidTopologyDelta;
use super::super::V4_ORDINAL_MANIFEST;
use super::super::maintenance::standalone_v4_pinned_update;
use super::super::ordinal_artifacts::stage_v4_ordinal_artifacts;
use super::super::ordinal_artifacts::stage_v4_ordinal_artifacts_unordered;
use super::super::rebuild::rebuild_v4_ordinal_identity;
use super::super::tests::fixture;
use super::super::tests::install_v4_plan;
use super::super::tests::pinned_v4_update;
use super::commit_uuid_topology_rewrite;
use super::hex_sha256;
use super::prepare_v4_ordinal_delta;
use std::fs;
use uuid::Uuid;

#[test]
fn v4_delta_append_delete_binary_carry_reopens_without_resurrection() {
    let root = tempfile::tempdir().unwrap();
    let index_path = root.path().join(INDEX_DIR);
    fs::create_dir_all(&index_path).unwrap();
    let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
    let (base, _) = stage_v4_ordinal_artifacts(
        [(Uuid::from_u128(1), 1_u64), (Uuid::from_u128(2), 2_u64)],
        1,
        &index,
        || false,
    )
    .unwrap();
    fs::write(
        index_path.join(V4_ORDINAL_MANIFEST),
        serde_json::to_vec(&base).unwrap(),
    )
    .unwrap();
    fs::write(index_path.join("ordinal-v4.lock"), []).unwrap();

    let pinned = pinned_v4_update(root.path(), base);
    let mut second = crate::staging::RewriteBatch::new();
    let planned_second = prepare_v4_ordinal_delta(
        root.path(),
        1,
        2,
        &pinned,
        &mut second,
        &[(Uuid::from_u128(3), 3_u64), (Uuid::from_u128(4), 4_u64)],
        &[1],
        &"11".repeat(32),
    )
    .unwrap();
    assert_eq!(planned_second.metrics.input_identities, 2);
    assert_eq!(planned_second.metrics.input_tombstones, 1);
    assert_eq!(planned_second.metrics.prior_topology_rows_decoded, 0);
    assert_eq!(planned_second.metrics.per_record_seeks, 0);
    assert_eq!(planned_second.metrics.compactions, 0);
    install_v4_plan(&second);

    let pinned = pinned_v4_update(root.path(), planned_second.manifest);
    let mut third = crate::staging::RewriteBatch::new();
    let planned_third = prepare_v4_ordinal_delta(
        root.path(),
        2,
        3,
        &pinned,
        &mut third,
        &[(Uuid::from_u128(5), 5_u64)],
        &[2],
        &"22".repeat(32),
    )
    .unwrap();
    assert_eq!(planned_third.metrics.compactions, 1);
    assert_eq!(
        planned_third.metrics.peak_configured_cache_window_bytes,
        graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
    );
    assert!(
        planned_third.metrics.cache_release.peak_window_bytes
            <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
    );
    #[cfg(target_os = "linux")]
    assert!(planned_third.metrics.cache_release.release_operations > 0);
    assert_eq!(planned_third.manifest.forward_identities.len(), 2);
    assert_eq!(planned_third.manifest.ordinal_ranges.len(), 2);
    assert_eq!(planned_third.manifest.tombstones.len(), 2);
    assert_eq!(
        planned_third.metrics.peak_buffer_bytes,
        crate::staging::STAGE_FILE_BLOCK_BYTES
    );
    install_v4_plan(&third);

    let manifest_bytes = serde_json::to_vec(&planned_third.manifest).unwrap();
    fs::write(index_path.join(V4_ORDINAL_MANIFEST), &manifest_bytes).unwrap();
    let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
        topology_generation: 3,
        manifest_sha256: hex_sha256(&manifest_bytes),
    };
    let mut handle = match crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
        root.path(),
        &authority,
        crate::V4OrdinalIdentityLimits::default(),
    )
    .unwrap()
    {
        crate::V4OrdinalIdentityOpen::Ready(handle) => handle,
        crate::V4OrdinalIdentityOpen::RebuildRequired { .. } => panic!("v4 expected"),
    };
    let lookup = handle.lookup_node_uuids(&[1, 2, 3, 4, 5]).unwrap();
    assert_eq!(
        lookup.values,
        vec![
            None,
            None,
            Some(Uuid::from_u128(3)),
            Some(Uuid::from_u128(4)),
            Some(Uuid::from_u128(5)),
        ]
    );
}

/// Publish one append and return the manifest it planned.
fn append_v4_generation(
    root: &std::path::Path,
    prior: crate::V4OrdinalIdentityManifest,
    nodes: &[(Uuid, u64)],
    tombstones: &[u64],
) -> crate::V4OrdinalIdentityManifest {
    let generation = prior.topology_generation + 1;
    let pinned = pinned_v4_update(root, prior);
    let mut batch = crate::staging::RewriteBatch::new();
    let planned = prepare_v4_ordinal_delta(
        root,
        generation - 1,
        generation,
        &pinned,
        &mut batch,
        nodes,
        tombstones,
        &"44".repeat(32),
    )
    .unwrap();
    install_v4_plan(&batch);
    planned.manifest
}

/// Like [`append_v4_generation`], but the pinned inputs come from an opened,
/// completely admitted handle, as a real writer obtains them.
fn append_v4_generation_through_handle(
    root: &std::path::Path,
    prior: crate::V4OrdinalIdentityManifest,
    nodes: &[(Uuid, u64)],
    tombstones: &[u64],
) -> crate::V4OrdinalIdentityManifest {
    let index = root.join(INDEX_DIR);
    let body = serde_json::to_vec(&prior).unwrap();
    fs::write(index.join(V4_ORDINAL_MANIFEST), &body).unwrap();
    fs::write(index.join("ordinal-v4.lock"), []).unwrap();
    let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
        topology_generation: prior.topology_generation,
        manifest_sha256: hex_sha256(&body),
    };
    let crate::V4OrdinalIdentityOpen::Ready(mut handle) =
        crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
            root,
            &authority,
            crate::V4OrdinalIdentityLimits::default(),
        )
        .unwrap()
    else {
        panic!("v4 expected");
    };
    let pinned = handle.pinned_update_inputs().unwrap();
    let generation = prior.topology_generation + 1;
    let mut batch = crate::staging::RewriteBatch::new();
    let planned = prepare_v4_ordinal_delta(
        root,
        generation - 1,
        generation,
        &pinned,
        &mut batch,
        nodes,
        tombstones,
        &"44".repeat(32),
    )
    .unwrap();
    install_v4_plan(&batch);
    planned.manifest
}

#[test]
fn v4_delta_publishes_the_uuid_order_it_derived_never_one_it_assumed() {
    let new_project = |records: &[(u128, u64)]| {
        let root = tempfile::tempdir().unwrap();
        let index_path = root.path().join(INDEX_DIR);
        fs::create_dir_all(&index_path).unwrap();
        let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
        let base = stage_v4_ordinal_artifacts_unordered(
            records
                .iter()
                .map(|(uuid, id)| (Uuid::from_u128(*uuid), *id))
                .collect(),
            1,
            &index,
        )
        .unwrap();
        (root, base)
    };
    let u = Uuid::from_u128;

    // Construction records what it streamed: ascending, then not.
    let (ordered_root, ordered) = new_project(&[(10, 1), (20, 2), (30, 3)]);
    assert_eq!(ordered.uuid_order_matches_ordinals, Some(true));
    let (_, shuffled) = new_project(&[(30, 1), (10, 2), (20, 3)]);
    assert_eq!(shuffled.uuid_order_matches_ordinals, Some(false));

    // An append whose UUIDs ascend past the parent's last keeps the order...
    let next = append_v4_generation(ordered_root.path(), ordered.clone(), &[(u(40), 4)], &[]);
    assert_eq!(next.uuid_order_matches_ordinals, Some(true));
    // ...and one that sorts below it, only at the boundary, breaks it. The
    // tombstone changes nothing: a deleted identity still holds its place.
    let (root, base) = new_project(&[(10, 1), (20, 2), (30, 3)]);
    let broken = append_v4_generation(root.path(), base.clone(), &[(u(25), 4)], &[3]);
    assert_eq!(broken.uuid_order_matches_ordinals, Some(false));
    // A recorded inversion is permanent: later ascending appends cannot undo it.
    let after = append_v4_generation(root.path(), broken, &[(u(99), 5)], &[]);
    assert_eq!(after.uuid_order_matches_ordinals, Some(false));

    // A delta that is itself unordered breaks an ordered parent.
    let (root, base) = new_project(&[(10, 1), (20, 2)]);
    let unordered_delta = append_v4_generation(root.path(), base, &[(u(50), 3), (u(40), 4)], &[]);
    assert_eq!(unordered_delta.uuid_order_matches_ordinals, Some(false));

    // An unknown parent is promoted by the first generation built on it,
    // because the writer derives the fact from the authenticated parent when it
    // opens it: true for ordered data, false for unordered data.
    for (records, expected) in [
        (&[(10_u128, 1_u64), (20, 2)][..], Some(true)),
        (&[(20, 1), (10, 2)][..], Some(false)),
    ] {
        let (root, mut unknown) = new_project(records);
        unknown.uuid_order_matches_ordinals = None;
        let promoted =
            append_v4_generation_through_handle(root.path(), unknown, &[(u(30), 3)], &[]);
        assert_eq!(
            promoted.uuid_order_matches_ordinals, expected,
            "{records:?}"
        );
    }
    // Inputs that bypassed admission are never promoted: unknown stays unknown.
    let (root, mut unknown) = new_project(&[(10, 1), (20, 2)]);
    unknown.uuid_order_matches_ordinals = None;
    let bypassed = append_v4_generation(root.path(), unknown, &[(u(30), 3)], &[]);
    assert_eq!(bypassed.uuid_order_matches_ordinals, None);

    // A tombstone-only generation adds no ordinals and keeps the claim.
    let (root, base) = new_project(&[(10, 1), (20, 2)]);
    let deleted = append_v4_generation(root.path(), base, &[], &[2]);
    assert_eq!(deleted.uuid_order_matches_ordinals, Some(true));
}

#[test]
fn v4_delta_rejects_reuse_and_invalid_tombstones_before_staging() {
    let root = tempfile::tempdir().unwrap();
    let index_path = root.path().join(INDEX_DIR);
    fs::create_dir_all(&index_path).unwrap();
    let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
    let (base, _) =
        stage_v4_ordinal_artifacts([(Uuid::from_u128(1), 7_u64)], 1, &index, || false).unwrap();
    let pinned = pinned_v4_update(root.path(), base);

    let mut reused_uuid = crate::staging::RewriteBatch::new();
    let reused = prepare_v4_ordinal_delta(
        root.path(),
        1,
        2,
        &pinned,
        &mut reused_uuid,
        &[(Uuid::from_u128(1), 8_u64)],
        &[],
        &"33".repeat(32),
    );
    assert!(reused.is_err(), "prior UUID reuse unexpectedly passed");
    assert!(reused_uuid.is_empty());

    for (nodes, tombstones) in [
        (vec![(Uuid::from_u128(2), 7_u64)], vec![]),
        (vec![(Uuid::from_u128(2), 8_u64)], vec![0]),
        (vec![(Uuid::from_u128(2), 8_u64)], vec![99]),
        (
            vec![(Uuid::from_u128(2), 8_u64), (Uuid::from_u128(2), 9_u64)],
            vec![],
        ),
    ] {
        let mut batch = crate::staging::RewriteBatch::new();
        assert!(
            prepare_v4_ordinal_delta(
                root.path(),
                1,
                2,
                &pinned,
                &mut batch,
                &nodes,
                &tombstones,
                &"33".repeat(32),
            )
            .is_err()
        );
        assert!(batch.is_empty());
    }
}

#[test]
fn v4_delta_one_two_four_history_is_bounded_and_binary_carried() {
    let root = tempfile::tempdir().unwrap();
    let index_path = root.path().join(INDEX_DIR);
    fs::create_dir_all(&index_path).unwrap();
    let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
    const ROWS_PER_GENERATION: u64 = 257;
    let base = (1..=ROWS_PER_GENERATION)
        .map(|id| (Uuid::from_u128(u128::from(id)), id))
        .collect::<Vec<_>>();
    let (mut manifest, _) = stage_v4_ordinal_artifacts(base, 1, &index, || false).unwrap();
    let mut observed = Vec::new();
    let mut next_node_id = ROWS_PER_GENERATION + 1;
    for generation in 2..=5_u64 {
        let pinned = pinned_v4_update(root.path(), manifest);
        let mut batch = crate::staging::RewriteBatch::new();
        let multiplier = 1_u64 << (generation - 2);
        let input_rows = ROWS_PER_GENERATION * multiplier;
        let first = next_node_id;
        let last = first + input_rows - 1;
        next_node_id = last + 1;
        let nodes = (first..=last)
            .map(|id| (Uuid::from_u128(u128::from(id)), id))
            .collect::<Vec<_>>();
        let planned = prepare_v4_ordinal_delta(
            root.path(),
            generation - 1,
            generation,
            &pinned,
            &mut batch,
            &nodes,
            &[],
            &"44".repeat(32),
        )
        .unwrap();
        assert_eq!(planned.metrics.input_identities, input_rows);
        assert_eq!(planned.metrics.prior_topology_rows_decoded, 0);
        assert_eq!(planned.metrics.per_record_seeks, 0);
        assert_eq!(
            planned.metrics.peak_buffer_bytes,
            crate::staging::STAGE_FILE_BLOCK_BYTES
        );
        assert!(planned.metrics.peak_temporary_bytes != 0);
        assert!(planned.metrics.fsync_operations >= 3);
        assert!(planned.metrics.created_artifacts >= 3);
        assert!(planned.metrics.write_blocks != 0);
        assert_eq!(
            planned.metrics.write_bytes,
            planned.metrics.physical_bytes_written
        );
        if planned.metrics.compactions == 0 {
            assert_eq!(planned.metrics.sequential_read_bytes, 0);
            assert_eq!(planned.metrics.sequential_read_calls, 0);
            assert_eq!(planned.metrics.sequential_read_blocks, 0);
        } else {
            assert!(planned.metrics.sequential_read_bytes != 0);
            assert!(planned.metrics.sequential_read_calls != 0);
            assert!(planned.metrics.sequential_read_blocks != 0);
        }
        let control_bytes = planned.auxiliary.bytes
            + u64::try_from(serde_json::to_vec(&planned.manifest).unwrap().len()).unwrap();
        let data_bytes = planned.metrics.physical_bytes_written - control_bytes;
        let new_payload_bytes = input_rows * 40;
        assert!(data_bytes >= new_payload_bytes * 2);
        assert!(
            data_bytes
                <= (new_payload_bytes + planned.metrics.sequential_read_bytes).saturating_mul(2)
        );
        assert!(planned.metrics.write_blocks <= data_bytes.div_ceil(16) + 2);
        observed.push((
            planned.metrics.compactions,
            planned.metrics.physical_bytes_written,
            planned.manifest.forward_identities.len(),
        ));
        install_v4_plan(&batch);
        manifest = planned.manifest;
    }
    assert_eq!(
        observed.iter().map(|row| row.0).collect::<Vec<_>>(),
        [0, 1, 0, 2]
    );
    assert_eq!(manifest.forward_identities.len(), 2);
    assert_eq!(manifest.ordinal_ranges.len(), 2);
    assert_eq!(next_node_id - 1, ROWS_PER_GENERATION * 16);
    assert!(observed.iter().all(|row| row.1 != 0));
}

#[test]
fn standalone_existing_v4_advances_with_topology_transaction() {
    let (dir, _, _) = fixture();
    fs::write(
        dir.path().join("topology/generation.json"),
        b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
    )
    .unwrap();
    rebuild_v4_ordinal_identity(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let topology = dir.path().join("topology/nodes.parquet");
    let mut staged = crate::staging::RewriteBatch::new();
    staged.stage_file(&topology, &topology).unwrap();
    let mut snapshot = None;
    let committed = commit_uuid_topology_rewrite(
        dir.path(),
        staged,
        &UuidTopologyDelta {
            nodes: Vec::new(),
            edges: Vec::new(),
            deleted_nodes: Vec::new(),
            deleted_edges: Vec::new(),
        },
        &mut snapshot,
    )
    .unwrap();
    assert!(matches!(
        committed,
        CommittedUuidTopologyRewrite::Committed { generation: 8, .. }
    ));
    let manifest: crate::V4OrdinalIdentityManifest = serde_json::from_slice(
        &fs::read(dir.path().join(INDEX_DIR).join(V4_ORDINAL_MANIFEST)).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest.topology_generation, 8);
    assert!(
        standalone_v4_pinned_update(dir.path(), 8)
            .unwrap()
            .is_some()
    );
}

#[test]
fn topology_delta_digest_accounts_canonical_logical_contract_bytes() {
    use graphforge_core::hash_observation::operation::{Capture, Snapshot};
    use sha2::{Digest, Sha256};
    let nodes = [(Uuid::from_u128(2), 20_u64), (Uuid::from_u128(1), 10)];
    let edges = [Uuid::from_u128(4), Uuid::from_u128(3)];
    let deleted_nodes = [(Uuid::from_u128(6), 60_u64), (Uuid::from_u128(5), 50)];
    let deleted_edges = [Uuid::from_u128(8), Uuid::from_u128(7)];
    let mut preimage = b"graphforge/uuid-index-topology-delta/v1".to_vec();
    for (tag, tuples) in [
        (0, [nodes[1], nodes[0]]),
        (2, [deleted_nodes[1], deleted_nodes[0]]),
    ] {
        if tag == 2 {
            for uuid in [edges[1], edges[0]] {
                preimage.push(1);
                preimage.extend_from_slice(uuid.as_bytes());
            }
        }
        for (uuid, surrogate) in tuples {
            preimage.push(tag);
            preimage.extend_from_slice(uuid.as_bytes());
            preimage.extend_from_slice(&surrogate.to_be_bytes());
        }
    }
    for uuid in [deleted_edges[1], deleted_edges[0]] {
        preimage.push(3);
        preimage.extend_from_slice(uuid.as_bytes());
    }
    let expected = Sha256::digest(&preimage)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let capture = Capture::start();
    let actual = super::topology_delta_sha256(&nodes, &edges, &deleted_nodes, &deleted_edges);
    assert_eq!(actual, expected);
    assert_eq!(
        capture.snapshot(),
        Snapshot {
            contract_identity_sha256_bytes: preimage.len() as u64,
            ..Snapshot::default()
        }
    );
    drop(capture);
    assert_eq!(
        super::topology_delta_sha256(
            &[nodes[1], nodes[0]],
            &[edges[1], edges[0]],
            &[deleted_nodes[1], deleted_nodes[0]],
            &[deleted_edges[1], deleted_edges[0]]
        ),
        actual
    );
    assert_ne!(
        super::topology_delta_sha256(
            &[(nodes[0].0, 21), nodes[1]],
            &edges,
            &deleted_nodes,
            &deleted_edges
        ),
        actual
    );
}
