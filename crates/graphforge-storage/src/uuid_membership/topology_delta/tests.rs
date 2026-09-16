use super::super::AuthenticatedUuidIndexSnapshot;
use super::super::CommittedUuidTopologyRewrite;
use super::super::INDEX_DIR;
use super::super::MANIFEST;
use super::super::Manifest;
use super::super::UuidIndexBuildLimits;
use super::super::UuidIndexKind;
use super::super::UuidMembershipIndex;
use super::super::UuidTopologyDelta;
use super::super::V4_ORDINAL_MANIFEST;
use super::super::maintenance::manifest_file_names;
use super::super::maintenance::standalone_v4_pinned_update;
use super::super::ordinal_artifacts::stage_v4_ordinal_artifacts;
use super::super::rebuild::rebuild_uuid_membership_indexes;
use super::super::rebuild::rebuild_v4_ordinal_identity;
use super::super::tests::fixture;
use super::super::tests::install_v4_plan;
use super::super::tests::pinned_v4_update;
use super::super::tests::singleton_append_series;
use super::append_uuid_membership_delta;
use super::commit_uuid_topology_rewrite;
use super::hex_sha256;
use super::plan_uuid_membership_delta;
use super::prepare_uuid_membership_delta;
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
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
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
fn v3_leveled_append_has_bounded_runs_and_nonquadratic_doubling() {
    let (_, small) = singleton_append_series(64);
    let (large, large_bytes) = singleton_append_series(128);
    assert!(large_bytes <= small * 5 / 2);
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(large.path().join(INDEX_DIR).join(MANIFEST)).unwrap())
            .unwrap();
    assert!(manifest.runs.len() <= 9);
    assert_eq!(manifest.runs.iter().filter(|run| run.base).count(), 1);
    let mut index = UuidMembershipIndex::open(large.path()).unwrap();
    assert_eq!(index.count(UuidIndexKind::Node), 128);
    assert_eq!(
        index
            .lookup_node_surrogates(&[Uuid::from_u128(1), Uuid::from_u128(128)])
            .unwrap()
            .0,
        [Some(1), Some(128)]
    );
}

#[test]
fn append_rejects_cross_run_uuid_and_surrogate_collisions_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    append_uuid_membership_delta(
        dir.path(),
        1,
        &[(Uuid::from_u128(1), 1)],
        &[Uuid::from_u128(2)],
    )
    .unwrap();
    let manifest_before = fs::read(dir.path().join(INDEX_DIR).join(MANIFEST)).unwrap();

    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    assert!(
        append_uuid_membership_delta(dir.path(), 2, &[], &[Uuid::from_u128(1)],)
            .unwrap_err()
            .to_string()
            .contains("already exists")
    );
    assert_eq!(
        fs::read(dir.path().join(INDEX_DIR).join(MANIFEST)).unwrap(),
        manifest_before
    );
    assert!(
        append_uuid_membership_delta(dir.path(), 2, &[(Uuid::from_u128(3), 1)], &[],)
            .unwrap_err()
            .to_string()
            .contains("surrogate already exists")
    );

    let reverse = tempfile::tempdir().unwrap();
    crate::generation::force_bump_topology_generation_for_test(reverse.path()).unwrap();
    append_uuid_membership_delta(reverse.path(), 1, &[], &[Uuid::from_u128(9)]).unwrap();
    crate::generation::force_bump_topology_generation_for_test(reverse.path()).unwrap();
    assert!(
        append_uuid_membership_delta(reverse.path(), 2, &[(Uuid::from_u128(9), 9)], &[])
            .unwrap_err()
            .to_string()
            .contains("already exists")
    );
}

#[test]
fn bulk_append_validation_uses_sequential_megabyte_blocks_and_zero_random_seeks() {
    let dir = tempfile::tempdir().unwrap();
    let retained = (1_u64..=40_000)
        .map(|value| (Uuid::from_u128(u128::from(value)), value))
        .collect::<Vec<_>>();
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    append_uuid_membership_delta(dir.path(), 1, &retained, &[]).unwrap();

    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    let metrics =
        append_uuid_membership_delta(dir.path(), 2, &[(Uuid::from_u128(50_000), 50_000)], &[])
            .unwrap();
    assert_eq!(metrics.validation_random_seeks, 0);
    assert_eq!(metrics.validation_scan_bytes, 40_000 * (25 + 24));
    assert_eq!(metrics.validation_scan_blocks, 2);
}

#[test]
fn retained_planner_stages_only_new_and_binary_carry_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(INDEX_DIR);
    let first_nodes = (1_u64..=40_000)
        .map(|value| (Uuid::from_u128(u128::from(value)), value))
        .collect::<Vec<_>>();

    let mut first = crate::RewriteBatch::new();
    prepare_uuid_membership_delta(
        dir.path(),
        0,
        1,
        None,
        &mut first,
        &first_nodes,
        &[],
        &[],
        &[],
    )
    .unwrap();
    // Two empty base files, two L0 files, manifest, and receipt. No copy of
    // any retained corpus exists on the initial plan.
    assert_eq!(first.staged_paths().count(), 6);
    first.commit_unsealed_for_test().unwrap();
    fs::create_dir_all(dir.path().join("topology")).unwrap();
    fs::write(
        crate::generation::generation_path(dir.path()),
        crate::generation::encode_generation_state(1, 1, 0).unwrap(),
    )
    .unwrap();
    let before = manifest_file_names(
        &serde_json::from_slice::<Manifest>(&fs::read(root.join(MANIFEST)).unwrap()).unwrap(),
    );

    let scratch = tempfile::tempdir_in(dir.path()).unwrap();
    let mut snapshot = AuthenticatedUuidIndexSnapshot::open_at_generation(dir.path(), 1).unwrap();
    let second_nodes = (40_001_u64..=80_000)
        .map(|value| (Uuid::from_u128(u128::from(value)), value))
        .collect::<Vec<_>>();
    let (planned, outputs, superseded, metrics) = plan_uuid_membership_delta(
        &root,
        1,
        2,
        Some(&mut snapshot),
        scratch.path(),
        &second_nodes,
        &[],
        &[],
        &[],
    )
    .unwrap();
    // Generation two carries L0+L0 into exactly one L1 pair. Retained base
    // files are descriptor-reused, not copied into planner outputs.
    assert_eq!(outputs.len(), 2);
    assert_eq!(superseded.len(), 2);
    assert_eq!(planned.runs.iter().filter(|run| !run.base).count(), 1);
    assert_eq!(planned.runs.iter().find(|run| !run.base).unwrap().level, 1);
    assert!(
        before
            .iter()
            .all(|name| !outputs.iter().any(|(out, _)| &out.name == name))
    );
    assert!(metrics.validation_scan_bytes > 0);
    assert_eq!(metrics.validation_random_seeks, 0);
    assert_eq!(metrics.prior_topology_rows_decoded, 0);
    assert!(metrics.snapshot_admission_authentication_bytes > 0);
    assert!(metrics.validation_scan_bytes <= metrics.snapshot_admission_authentication_bytes * 2);
    assert!(metrics.new_output_authentication_bytes <= metrics.physical_bytes_written);

    let mut install = crate::RewriteBatch::new();
    for (record, path) in &outputs {
        install.stage_file(&root.join(&record.name), path).unwrap();
    }
    install
        .stage_bytes(&root.join(MANIFEST), &serde_json::to_vec(&planned).unwrap())
        .unwrap();
    install.commit_unsealed_for_test().unwrap();
    snapshot.advance_to(planned).unwrap();

    let scratch = tempfile::tempdir_in(dir.path()).unwrap();
    let (_, _, _, subsequent) = plan_uuid_membership_delta(
        &root,
        2,
        3,
        Some(&mut snapshot),
        scratch.path(),
        &[(Uuid::from_u128(80_001), 80_001)],
        &[],
        &[],
        &[],
    )
    .unwrap();
    assert_eq!(subsequent.snapshot_admission_authentication_bytes, 0);
    assert_eq!(subsequent.snapshot_admission_authentication_blocks, 0);
    assert_eq!(subsequent.validation_scan_bytes, 0);
    assert_eq!(subsequent.validation_scan_blocks, 0);
}
