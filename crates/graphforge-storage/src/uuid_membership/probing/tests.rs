use super::super::AuthenticatedUuidIndexSnapshot;
use super::super::ConstructionReferenceAuthentication;
use super::super::ConstructionUuidIdentity;
use super::super::IDENTITY_RECORD_BYTES;
use super::super::INDEX_DIR;
use super::super::MANIFEST;
use super::super::Manifest;
use super::super::NODE_LOOKUP_RECORD_BYTES;
use super::super::UuidIndexBuildLimits;
use super::super::UuidIndexKind;
use super::super::UuidMembershipIndex;
use super::super::append_uuid_membership_delta;
use super::super::describe_blocks;
use super::super::open_uuid_construction_snapshot;
use super::super::rebuild::rebuild_uuid_membership_indexes;
use super::super::tests::fixture;
use super::super::tests::write_node_parquet;
use super::super::topology_delta::append_uuid_membership_delta_with_tombstones;
use super::super::topology_delta::plan_uuid_membership_delta;
use super::super::uuid_membership_index_is_fresh;
use std::fs;
use std::fs::File;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

#[test]
fn construction_snapshot_streams_live_uuid_authority_in_order() {
    let (dir, nodes, edges) = fixture();
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let mut streamed = Vec::new();
    let (snapshot, work) = open_uuid_construction_snapshot(dir.path(), 0, |identity| {
        streamed.push(identity);
        Ok(())
    })
    .unwrap();

    let mut expected = nodes
        .iter()
        .copied()
        .zip(1_u64..)
        .map(|(uuid, surrogate)| ConstructionUuidIdentity {
            uuid,
            kind: UuidIndexKind::Node,
            surrogate,
        })
        .chain(edges.iter().copied().map(|uuid| ConstructionUuidIdentity {
            uuid,
            kind: UuidIndexKind::Edge,
            surrogate: 0,
        }))
        .collect::<Vec<_>>();
    expected.sort_by_key(|identity| identity.uuid);
    assert_eq!(streamed, expected);
    assert_eq!(work.live_nodes, nodes.len() as u64);
    assert_eq!(work.live_edges, edges.len() as u64);
    assert_eq!(work.max_node_surrogate, nodes.len() as u64);
    assert!(work.authentication_bytes > 0);
    assert!(work.authentication_blocks > 0);
    snapshot.revalidate().unwrap();
}

#[test]
fn bounded_build_reopens_and_probes_in_caller_order() {
    let (dir, nodes, _) = fixture();
    let metrics = rebuild_uuid_membership_indexes(
        dir.path(),
        UuidIndexBuildLimits {
            scan_batch_rows: 1,
            run_records: 1,
            merge_fan_in: 2,
        },
    )
    .unwrap();
    assert_eq!((metrics.node_count, metrics.edge_count), (3, 2));
    assert_eq!(metrics.peak_buffered_records, 1);
    let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
    let missing = Uuid::from_u128(99);
    let (found, probe) = index
        .probe(UuidIndexKind::Node, &[nodes[1], missing, nodes[1]])
        .unwrap();
    assert_eq!(found, vec![true, false, true]);
    let (surrogates, lookup) = index
        .lookup_node_surrogates(&[nodes[1], missing, nodes[0]])
        .unwrap();
    assert_eq!(surrogates, vec![Some(2), None, Some(1)]);
    assert_eq!(lookup.found, 2);
    assert_eq!(probe.per_record_seeks, 0);
    assert_eq!(lookup.per_record_seeks, 0);
    assert_eq!(lookup.surrogate_blocks_read, 1);
    assert_eq!(
        (probe.requested, probe.unique_requested, probe.found),
        (3, 2, 1)
    );
}

#[test]
fn probe_work_is_fence_selected_block_merge_not_per_record_seeks() {
    let dir = tempfile::tempdir().unwrap();
    let nodes = (1..=8_192).map(Uuid::from_u128).collect::<Vec<_>>();
    write_node_parquet(&dir.path().join("topology/nodes.parquet"), &nodes);
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();

    let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
    let (found, metrics) = index
        .probe(UuidIndexKind::Node, &[nodes[4_095], Uuid::from_u128(9_000)])
        .unwrap();
    assert_eq!(found, [true, false]);
    assert_eq!(metrics.unique_requested, 2);
    assert_eq!(metrics.per_record_seeks, 0);
    assert_eq!(metrics.identity_blocks_read, 1);
    assert_eq!(metrics.file_seeks, metrics.identity_blocks_read);
    assert_eq!(metrics.identity_bytes_read, 8_192 * IDENTITY_RECORD_BYTES);
}

#[test]
fn batch_lookup_restores_duplicates_and_applies_newest_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    let retained = (1_u64..=40_000)
        .map(|value| (Uuid::from_u128(u128::from(value)), value))
        .collect::<Vec<_>>();
    append_uuid_membership_delta(dir.path(), 1, &retained, &[]).unwrap();
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    append_uuid_membership_delta_with_tombstones(
        dir.path(),
        2,
        &[],
        &[],
        &[(retained[19_999].0, retained[19_999].1)],
        &[],
    )
    .unwrap();

    let present = retained[39_999].0;
    let deleted = retained[19_999].0;
    let missing = Uuid::from_u128(50_000);
    let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
    let (resolved, metrics) = index
        .lookup_node_surrogates(&[present, deleted, present, missing])
        .unwrap();
    assert_eq!(resolved, [Some(40_000), None, Some(40_000), None]);
    assert_eq!(
        (metrics.requested, metrics.unique_requested, metrics.found),
        (4, 3, 1)
    );
    assert_eq!(metrics.per_record_seeks, 0);
    assert!(metrics.identity_blocks_read <= 3);
    assert_eq!(metrics.surrogate_blocks_read, 1);
    assert_eq!(metrics.file_seeks, metrics.identity_blocks_read + 1);
}

#[test]
fn corrupt_data_fails_closed_without_replacing_manifest() {
    let (dir, _, _) = fixture();
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let root = dir.path().join(INDEX_DIR);
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(root.join(MANIFEST)).unwrap()).unwrap();
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(root.join(&manifest.runs[0].identities.name))
        .unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[0xff]).unwrap();
    assert!(
        UuidMembershipIndex::open(dir.path())
            .unwrap_err()
            .to_string()
            .contains("authentication failed")
    );
}

#[test]
fn retained_snapshot_rehashes_manifest_and_authenticates_only_candidate_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let nodes = (1_u64..=40_000)
        .map(|value| (Uuid::from_u128(u128::from(value)), value))
        .collect::<Vec<_>>();
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    append_uuid_membership_delta(dir.path(), 1, &nodes, &[]).unwrap();
    let root = dir.path().join(INDEX_DIR);
    let mut snapshot = AuthenticatedUuidIndexSnapshot::open_at_generation(dir.path(), 1).unwrap();

    let manifest_path = root.join(MANIFEST);
    let original_manifest = fs::read(&manifest_path).unwrap();
    fs::write(
        &manifest_path,
        [original_manifest.as_slice(), b"\n"].concat(),
    )
    .unwrap();
    assert!(
        snapshot
            .revalidate()
            .unwrap_err()
            .to_string()
            .contains("manifest authentication")
    );
    fs::write(&manifest_path, original_manifest).unwrap();

    let run_path = root.join(
        &snapshot
            .manifest
            .runs
            .iter()
            .find(|run| run.identities.count > 0)
            .unwrap()
            .identities
            .name,
    );
    let mut run = fs::OpenOptions::new().write(true).open(run_path).unwrap();
    run.seek(SeekFrom::Start(0)).unwrap();
    run.write_all(&[0xff]).unwrap();
    run.sync_all().unwrap();
    let scratch = tempfile::tempdir_in(dir.path()).unwrap();
    let error = plan_uuid_membership_delta(
        &root,
        1,
        2,
        Some(&mut snapshot),
        scratch.path(),
        &[(nodes[0].0, nodes[0].1)],
        &[],
        &[],
        &[],
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("block authentication"),
        "{error}"
    );
}

#[test]
fn compact_retained_reference_authentication_is_batched_and_linear() {
    let source = tempfile::tempdir().unwrap();
    crate::generation::force_bump_topology_generation_for_test(source.path()).unwrap();
    append_uuid_membership_delta(
        source.path(),
        1,
        &[(Uuid::from_u128(1), 1), (Uuid::from_u128(2), 2)],
        &[Uuid::from_u128(100)],
    )
    .unwrap();
    crate::generation::force_bump_topology_generation_for_test(source.path()).unwrap();
    append_uuid_membership_delta(
        source.path(),
        2,
        &[(Uuid::from_u128(3), 3)],
        &[Uuid::from_u128(101)],
    )
    .unwrap();
    let (inventory, _) = crate::capture_graph_files(source.path()).unwrap();

    let container = tempfile::tempdir().unwrap();
    crate::open_or_initialize_project(container.path()).unwrap();
    let lease = crate::begin_graph_object_publication(container.path()).unwrap();
    let paths = inventory
        .files
        .iter()
        .map(|entry| PathBuf::from(&entry.relative_path))
        .collect::<Vec<_>>();
    crate::append_graph_files_v2(
        &lease,
        source.path(),
        &mut crate::GraphManifestState::empty(),
        &paths,
        &[],
    )
    .unwrap();
    drop(lease);

    let snapshot = AuthenticatedUuidIndexSnapshot::open_from_compact_inventory(
        container.path(),
        &inventory,
        2,
    )
    .unwrap();
    let retained = snapshot
        .manifest
        .runs
        .iter()
        .flat_map(|run| [&run.identities, &run.node_surrogates])
        .map(|record| snapshot.retained_reference(record).unwrap())
        .collect::<Vec<_>>();
    assert!(retained.len() > 2);
    let references = retained
        .iter()
        .map(|reference| ConstructionReferenceAuthentication {
            source_root: &reference.source_root,
            source_root_volume: reference.source_root_volume,
            source_root_file_id: &reference.source_root_file_id,
            source_path: &reference.source_path,
            source_volume: reference.source_volume,
            source_file_id: &reference.source_file_id,
            target_path: &reference.target_path,
            bytes: reference.bytes,
            sha256: &reference.sha256,
            parent_manifest_sha256: &reference.parent_manifest_sha256,
        })
        .collect::<Vec<_>>();
    let work = snapshot
        .authenticate_construction_references(&references)
        .unwrap();
    assert_eq!(
        work.global_revalidation_bytes,
        snapshot.snapshot_authentication_bytes() * 2
    );
    assert_eq!(
        work.referenced_payload_bytes,
        retained
            .iter()
            .map(|reference| reference.bytes)
            .sum::<u64>()
    );

    let victim_reference = retained
        .iter()
        .find(|reference| reference.bytes > 0)
        .expect("one retained UUID payload is non-empty");
    let victim = Path::new(&victim_reference.source_root).join(&victim_reference.source_path);
    let original = fs::read(&victim).unwrap();
    let mut permissions = fs::metadata(&victim).unwrap().permissions();
    permissions.set_readonly(false);
    fs::set_permissions(&victim, permissions).unwrap();
    fs::write(&victim, vec![0_u8; original.len()]).unwrap();
    assert!(
        snapshot
            .authenticate_construction_references(&references)
            .is_err()
    );
    fs::write(&victim, &original).unwrap();
    let mut permissions = fs::metadata(&victim).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&victim, permissions).unwrap();

    let error = snapshot
        .authenticate_construction_references_with(&references, || {
            let mut permissions = fs::metadata(&victim).unwrap().permissions();
            permissions.set_readonly(false);
            fs::set_permissions(&victim, permissions).unwrap();
            fs::write(&victim, vec![0_u8; original.len()]).unwrap();
            let mut permissions = fs::metadata(&victim).unwrap().permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&victim, permissions).unwrap();
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("digest does not match its address"),
        "{error}"
    );
}

#[test]
fn missing_and_stale_manifests_fail_closed() {
    let (dir, _, _) = fixture();
    assert!(!uuid_membership_index_is_fresh(dir.path()).unwrap());
    assert!(UuidMembershipIndex::open(dir.path()).is_err());
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    assert!(uuid_membership_index_is_fresh(dir.path()).unwrap());
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    assert!(!uuid_membership_index_is_fresh(dir.path()).unwrap());
    assert!(
        UuidMembershipIndex::open(dir.path())
            .unwrap_err()
            .to_string()
            .contains("stale index generation")
    );
}

#[test]
fn lookup_lazily_rejects_authenticated_pair_inconsistency() {
    let (dir, nodes, _) = fixture();
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let root = dir.path().join(INDEX_DIR);
    let mut manifest: Manifest =
        serde_json::from_slice(&fs::read(root.join(MANIFEST)).unwrap()).unwrap();
    let run = &mut manifest.runs[0];
    let path = root.join(&run.node_surrogates.name);
    let mut bytes = fs::read(&path).unwrap();
    bytes[8..24].copy_from_slice(Uuid::from_u128(999).as_bytes());
    fs::write(&path, &bytes).unwrap();
    let mut file = File::open(&path).unwrap();
    let (sha256, blocks, count) = describe_blocks(&mut file, NODE_LOOKUP_RECORD_BYTES).unwrap();
    assert_eq!(count, run.node_surrogates.count);
    run.node_surrogates.sha256 = sha256;
    run.node_surrogates.blocks = blocks;
    fs::write(root.join(MANIFEST), serde_json::to_vec(&manifest).unwrap()).unwrap();

    // Ordinary open remains a bounded linear stream and does not perform
    // one random identity probe per surrogate record.
    let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
    assert!(
        index
            .lookup_node_surrogates(&[nodes[0]])
            .unwrap_err()
            .to_string()
            .contains("pair is inconsistent")
    );
}
