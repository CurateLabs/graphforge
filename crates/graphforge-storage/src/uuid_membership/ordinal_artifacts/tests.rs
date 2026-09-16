use super::super::V4_MAX_RANGES;
use super::super::V4_ORDINAL_BLOCK_BYTES;
use super::super::inject_v4_authority_failure;
use super::super::inject_v4_output_cleanup_failure;
use super::super::inject_v4_publication_failure;
use super::super::tests::assert_no_v4_temporary;
use super::super::topology_delta::hex_sha256;
use super::V4PublicationGuard;
use super::admit_v4_construction_manifest;
use super::publish_v4_construction_artifacts;
use super::stage_v4_ordinal_artifacts;
use super::stage_v4_ordinal_bundle;
use super::v4_manifest_artifact_names;
use std::fs;
use std::io::Write;
use uuid::Uuid;

#[test]
fn streamed_v4_publication_guard_covers_setup_and_post_rename_failures() {
    let points = [
        "initial_file_identity",
        "writer_construction",
        "window_validation",
        "fsync_evidence_overflow",
        "final_file_identity",
        "file_space_usage",
        "replace_child",
        "directory_sync",
        "post_publication_metric_overflow",
        "manifest_update",
    ];
    for point in points {
        let root = tempfile::tempdir().unwrap();
        let index = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
        if matches!(point, "initial_file_identity" | "writer_construction") {
            inject_v4_output_cleanup_failure();
        }
        inject_v4_publication_failure(point);
        let error = stage_v4_ordinal_artifacts([(Uuid::from_u128(1), 1)], 7, &index, || false)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(&format!("injected v4 publication failure at {point}")),
            "{error}"
        );
        if matches!(point, "initial_file_identity" | "writer_construction") {
            let primary = error.find("injected v4 publication failure").unwrap();
            let cleanup = error
                .find("unpublished artifact cleanup finalization failed")
                .unwrap();
            assert!(primary < cleanup, "{error}");
        }
        assert!(!error.contains(root.path().to_string_lossy().as_ref()));
        let names = index
            .child_names()
            .unwrap()
            .into_iter()
            .filter_map(|name| name.into_string().ok())
            .collect::<Vec<_>>();
        assert!(
            names
                .iter()
                .all(|name| !name.starts_with(".v4-") && !name.ends_with(".uuidx")),
            "{point}: {names:?}"
        );
    }
}

#[test]
fn content_addressed_v4_install_reuses_identical_and_preserves_collisions() {
    let root = tempfile::tempdir().unwrap();
    let encoded = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
    let graph = encoded
        .create_child_directory(std::ffi::OsStr::new("graph"))
        .unwrap();
    let topology = graph
        .create_child_directory(std::ffi::OsStr::new("topology"))
        .unwrap();
    let index = topology
        .create_child_directory(std::ffi::OsStr::new("uuid-membership"))
        .unwrap();
    let mappings = [(Uuid::from_u128(1), 1_u64), (Uuid::from_u128(2), 2_u64)];
    let (manifest, _) = stage_v4_ordinal_artifacts(mappings, 1, &index, || false).unwrap();
    let names = v4_manifest_artifact_names(&manifest);
    let originals = names
        .iter()
        .map(|name| {
            let file = index.open_child_file(std::ffi::OsStr::new(name)).unwrap();
            (
                name.clone(),
                graphforge_filesystem::file_identity(&file).unwrap(),
                fs::read(
                    root.path()
                        .join("graph/topology/uuid-membership")
                        .join(name),
                )
                .unwrap(),
            )
        })
        .collect::<Vec<_>>();

    let reused = stage_v4_ordinal_bundle(mappings, 1, &index, &mut || false).unwrap();
    assert!(reused.publications.is_empty());
    inject_v4_authority_failure("after_artifacts");
    let error = publish_v4_construction_artifacts(
        &encoded,
        reused,
        1,
        &hex_sha256(b"delta"),
        None,
        &mut || false,
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("injected v4 authority failure at after_artifacts"));
    for (name, identity, bytes) in &originals {
        let file = index.open_child_file(std::ffi::OsStr::new(name)).unwrap();
        assert_eq!(
            graphforge_filesystem::file_identity(&file).unwrap(),
            *identity
        );
        assert_eq!(
            fs::read(
                root.path()
                    .join("graph/topology/uuid-membership")
                    .join(name)
            )
            .unwrap(),
            *bytes
        );
    }

    let reused = stage_v4_ordinal_bundle(mappings, 1, &index, &mut || false).unwrap();
    assert!(reused.publications.is_empty());
    let (outputs, _, _) = publish_v4_construction_artifacts(
        &encoded,
        reused,
        1,
        &hex_sha256(b"delta"),
        None,
        &mut || false,
        None,
    )
    .unwrap();
    for (name, identity, bytes) in &originals {
        let output = outputs
            .iter()
            .find(|output| output.name == *name)
            .expect("reused payload remains in the complete publication inventory");
        assert_eq!(output.bytes, bytes.len() as u64);
        assert_eq!(output.sha256, hex_sha256(bytes));
        let file = index.open_child_file(std::ffi::OsStr::new(name)).unwrap();
        assert_eq!(
            graphforge_filesystem::file_identity(&file).unwrap(),
            *identity
        );
    }

    let (conflict_name, conflict_identity, _) = &originals[0];
    let conflict = b"occupied conflicting authority";
    fs::write(
        root.path()
            .join("graph/topology/uuid-membership")
            .join(conflict_name),
        conflict,
    )
    .unwrap();
    let error = stage_v4_ordinal_bundle(mappings, 1, &index, &mut || false)
        .unwrap_err()
        .to_string();
    assert!(error.contains("authentication failed"), "{error}");
    let file = index
        .open_child_file(std::ffi::OsStr::new(conflict_name))
        .unwrap();
    assert_eq!(
        graphforge_filesystem::file_identity(&file).unwrap(),
        *conflict_identity
    );
    assert_eq!(
        fs::read(
            root.path()
                .join("graph/topology/uuid-membership")
                .join(conflict_name)
        )
        .unwrap(),
        conflict
    );
    assert_no_v4_temporary(&index);
}

#[test]
fn shared_v4_builder_rejects_sparse_manifest_above_reader_bound() {
    let artifact = |name: String| crate::V4OrdinalArtifact {
        name,
        kind: crate::V4OrdinalArtifactKind::OrdinalUuids,
        generation: 1,
        bytes: 16,
        sha256: "00".repeat(32),
    };
    let ranges = (0..V4_MAX_RANGES)
        .map(|ordinal| crate::V4OrdinalRange {
            first_node_id: u64::try_from(ordinal).unwrap() * 2 + 1,
            count: 1,
            artifact: artifact(format!("ordinal-v4-1-{ordinal:016x}.uuidx")),
            blocks: vec![crate::V4OrdinalBlock {
                offset: 0,
                count: 1,
                sha256: "11".repeat(32),
            }],
        })
        .collect::<Vec<_>>();
    let manifest = crate::V4OrdinalIdentityManifest {
        format_version: crate::ORDINAL_IDENTITY_V4,
        topology_generation: 1,
        forward_identities: vec![crate::V4OrdinalArtifact {
            name: "forward-v4-1-0000000000000000.uuidx".to_owned(),
            kind: crate::V4OrdinalArtifactKind::ForwardIdentities,
            generation: 1,
            bytes: 24,
            sha256: "22".repeat(32),
        }],
        ordinal_ranges: ranges,
        tombstones: Vec::new(),
    };
    assert!(
        serde_json::to_vec(&manifest).unwrap().len() as u64
            > crate::ordinal_identity_v4::MAX_MANIFEST_BYTES
    );
    assert!(admit_v4_construction_manifest(&manifest).is_err());
}

#[test]
fn v4_observed_guard_tracks_install_and_failed_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let directory = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
    let operation = crate::StorageAllocationOperation::default();
    let mut guard = V4PublicationGuard::create(&directory, ".temporary", Some(&operation)).unwrap();
    let mut file = guard.take_file().unwrap();
    file.write_all(&vec![7_u8; 32768]).unwrap();
    file.sync_all().unwrap();
    guard.observe(&file).unwrap();
    let actual = graphforge_filesystem::file_space_usage(&file)
        .unwrap()
        .allocated_bytes;
    assert!(actual > 0);
    assert_eq!(operation.totals().unwrap(), (actual, actual));
    drop(file);
    guard
        .install_child(std::ffi::OsStr::new("installed"))
        .unwrap();
    let expected =
        crate::StorageAllocationOperation::from_paths(&[root.path().to_path_buf()]).unwrap();
    assert_eq!(operation.snapshot().unwrap(), expected.snapshot().unwrap());
    let error = guard
        .cleanup_checked(|| Err(std::io::Error::other("after unlink")))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unpublished artifact cleanup finalization failed")
    );
    assert!(!root.path().join("installed").exists());
    assert_eq!(operation.totals().unwrap(), (0, actual));
    drop(guard);
    assert_eq!(operation.totals().unwrap(), (0, actual));
}

#[test]
fn v4_stream_builder_emits_maximal_authenticated_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("index");
    fs::create_dir(&root).unwrap();
    let index = graphforge_filesystem::StableDirectory::open(&root).unwrap();
    let records = (1_u128..=4_100)
        .map(|value| {
            let node_id = if value <= 4_096 {
                value as u64
            } else {
                value as u64 + 10
            };
            (Uuid::from_u128(value), node_id)
        })
        .collect::<Vec<_>>();
    let (manifest, metrics) =
        stage_v4_ordinal_artifacts(records.clone(), 7, &index, || false).unwrap();

    assert_eq!(metrics.input_records, 4_100);
    assert_eq!(metrics.ranges, 2);
    assert_eq!(metrics.cancellation_polls, 4_100);
    assert_eq!(metrics.peak_buffer_bytes, 3 * V4_ORDINAL_BLOCK_BYTES);
    assert_eq!(metrics.peak_temporary_bytes, 4_100 * (24 + 16));
    assert_eq!(metrics.fsync_operations, 4);
    assert_eq!(manifest.ordinal_ranges[0].first_node_id, 1);
    assert_eq!(manifest.ordinal_ranges[0].count, 4_096);
    assert_eq!(manifest.ordinal_ranges[0].blocks.len(), 1);
    assert_eq!(manifest.ordinal_ranges[0].blocks[0].count, 4_096);
    assert_eq!(manifest.ordinal_ranges[1].first_node_id, 4_107);
    assert_eq!(manifest.ordinal_ranges[1].count, 4);
    assert_eq!(manifest.tombstones.len(), 1);
    assert_eq!(manifest.tombstones[0].artifact.bytes, 0);
    assert_eq!(manifest.tombstones[0].blocks, Vec::new());

    let forward = &manifest.forward_identities[0];
    let forward_bytes = fs::read(root.join(&forward.name)).unwrap();
    assert_eq!(forward_bytes.len(), records.len() * 24);
    assert_eq!(hex_sha256(&forward_bytes), forward.sha256);
    for range in &manifest.ordinal_ranges {
        let bytes = fs::read(root.join(&range.artifact.name)).unwrap();
        assert_eq!(hex_sha256(&bytes), range.artifact.sha256);
        assert_eq!(bytes.len() as u64, range.count * 16);
    }
}

#[test]
fn v4_stream_builder_rejects_noncanonical_and_cancelled_input() {
    let dir = tempfile::tempdir().unwrap();
    let root_a = dir.path().join("index-a");
    fs::create_dir(&root_a).unwrap();
    let index_a = graphforge_filesystem::StableDirectory::open(&root_a).unwrap();
    let error = stage_v4_ordinal_artifacts(
        [(Uuid::from_u128(2), 1), (Uuid::from_u128(1), 2)],
        1,
        &index_a,
        || false,
    )
    .unwrap_err();
    assert!(error.to_string().contains("not canonical"));

    let root_b = dir.path().join("index-b");
    fs::create_dir(&root_b).unwrap();
    let index_b = graphforge_filesystem::StableDirectory::open(&root_b).unwrap();
    let mut polls = 0;
    let error = stage_v4_ordinal_artifacts(
        [(Uuid::from_u128(1), 1), (Uuid::from_u128(2), 2)],
        1,
        &index_b,
        || {
            polls += 1;
            polls == 2
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
}

#[test]
fn v4_stream_builder_packs_sparse_ids_without_sparse_max_allocation() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("index");
    fs::create_dir(&root).unwrap();
    let index = graphforge_filesystem::StableDirectory::open(&root).unwrap();
    let (manifest, metrics) = stage_v4_ordinal_artifacts(
        [
            (Uuid::from_u128(1), 1),
            (Uuid::from_u128(2), u64::MAX - 1),
            (Uuid::from_u128(3), u64::MAX),
        ],
        9,
        &index,
        || false,
    )
    .unwrap();
    assert_eq!(manifest.ordinal_ranges.len(), 2);
    assert_eq!(manifest.ordinal_ranges[0].artifact.bytes, 16);
    assert_eq!(manifest.ordinal_ranges[1].artifact.bytes, 32);
    assert_eq!(metrics.artifact_bytes, 3 * 24 + 3 * 16);
    assert_eq!(metrics.peak_temporary_bytes, metrics.artifact_bytes);
    assert_eq!(metrics.fsync_operations, 4);
}
