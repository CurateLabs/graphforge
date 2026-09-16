use super::super::INDEX_DIR;
use super::super::inject_v4_compaction_post_write_failure;
use super::super::inject_v4_input_release_failure;
use super::super::inject_v4_output_cleanup_failure;
use super::super::ordinal_artifacts::V4TombstoneStreamWriter;
use super::super::tests::assert_no_v4_temporary;
use super::super::tests::pinned_v4_update;
use super::super::tests::write_v4_test_artifact;
use super::V4CompactionWork;
use super::compact_v4_ordinal_interval;
use super::compact_v4_tombstone_interval;
use super::merge_v4_forward_artifacts;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use uuid::Uuid;

#[test]
fn v4_compaction_fan_in_budget_uses_opened_reader_and_writer_windows() {
    let root = tempfile::tempdir().unwrap();
    let index = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
    let input_count = 8_usize;
    let window = graphforge_filesystem::cache_release_window_for_streams(input_count + 1).unwrap();
    let mut readers = Vec::new();
    for ordinal in 0..input_count {
        let path = root.path().join(format!("input-{ordinal}"));
        std::fs::write(&path, []).unwrap();
        readers.push(
            graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                File::open(path).unwrap(),
                window,
                graphforge_filesystem::FileCacheReleaseTracker::default(),
            )
            .unwrap(),
        );
    }
    let writer = V4TombstoneStreamWriter::new_with_cache_window(&index, 9, window, None).unwrap();
    let mut windows = readers
        .iter()
        .map(graphforge_filesystem::FileCacheReleasingReader::window_bytes)
        .collect::<Vec<_>>();
    windows.push(writer.artifact.writer.get_ref().window_bytes());

    let aggregate =
        graphforge_filesystem::validate_cache_release_operation_windows(&windows).unwrap();
    assert!(aggregate <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES);
    assert!(windows.iter().all(|configured| configured.get() > 0));
}

#[test]
fn v4_forward_compaction_failure_releases_consumed_input_without_masking_primary() {
    let root = tempfile::tempdir().unwrap();
    let index = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
    let left_path = root.path().join("left");
    let right_path = root.path().join("right");
    let mut left = Vec::new();
    left.extend_from_slice(Uuid::from_u128(1).as_bytes());
    left.extend_from_slice(&1_u64.to_be_bytes());
    left.push(0xff);
    let mut right = Vec::new();
    right.extend_from_slice(Uuid::from_u128(2).as_bytes());
    right.extend_from_slice(&2_u64.to_be_bytes());
    std::fs::write(&left_path, left).unwrap();
    std::fs::write(&right_path, right).unwrap();
    let mut work = V4CompactionWork::default();

    let error = merge_v4_forward_artifacts(
        File::open(left_path).unwrap(),
        File::open(right_path).unwrap(),
        &index,
        2,
        &mut work,
        &mut || false,
        None,
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("truncated"), "{error}");
    assert!(work.peak_configured_cache_window_bytes > 0);
    assert!(
        work.peak_configured_cache_window_bytes
            <= graphforge_filesystem::DEFAULT_CACHE_RELEASE_WINDOW_BYTES
    );
    assert!(
        index
            .child_names()
            .unwrap()
            .into_iter()
            .all(|name| !name.to_string_lossy().starts_with(".v4-"))
    );
    assert!(work.cache_release.sync_operations > 0);
    #[cfg(target_os = "linux")]
    assert!(work.cache_release.release_operations >= 3);
}

#[test]
fn v4_forward_cancellation_finalizes_and_removes_unpublished_output() {
    let root = tempfile::tempdir().unwrap();
    let index = graphforge_filesystem::StableDirectory::open(root.path()).unwrap();
    let left = root.path().join("left");
    let right = root.path().join("right");
    let mut left_bytes = Vec::new();
    left_bytes.extend_from_slice(Uuid::from_u128(1).as_bytes());
    left_bytes.extend_from_slice(&1_u64.to_be_bytes());
    let mut right_bytes = Vec::new();
    right_bytes.extend_from_slice(Uuid::from_u128(2).as_bytes());
    right_bytes.extend_from_slice(&2_u64.to_be_bytes());
    fs::write(&left, left_bytes).unwrap();
    fs::write(&right, right_bytes).unwrap();
    inject_v4_compaction_post_write_failure("forward");
    inject_v4_output_cleanup_failure();
    let mut work = V4CompactionWork::default();

    let error = merge_v4_forward_artifacts(
        File::open(left).unwrap(),
        File::open(right).unwrap(),
        &index,
        2,
        &mut work,
        &mut || false,
        None,
    )
    .unwrap_err()
    .to_string();

    let primary = error
        .find("v4 compaction cancelled after forward output")
        .unwrap();
    let cleanup = error.find("v4 forward output cleanup also failed").unwrap();
    assert!(primary < cleanup, "{error}");
    assert!(
        error.contains("unpublished artifact cleanup finalization failed"),
        "{error}"
    );
    assert!(!error.contains(root.path().to_string_lossy().as_ref()));
    assert_no_v4_temporary(&index);
    assert!(work.cache_release.sync_operations > 0);
    #[cfg(target_os = "linux")]
    assert!(work.cache_release.release_operations >= 3);

    inject_v4_input_release_failure();
    let mut release_work = V4CompactionWork::default();
    let error = merge_v4_forward_artifacts(
        File::open(root.path().join("left")).unwrap(),
        File::open(root.path().join("right")).unwrap(),
        &index,
        3,
        &mut release_work,
        &mut || false,
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("injected v4 input release failure"),
        "{error}"
    );
    assert_no_v4_temporary(&index);
    assert!(release_work.cache_release.sync_operations > 0);
}

#[test]
fn v4_ordinal_cancellation_finalizes_and_removes_unpublished_output() {
    let root = tempfile::tempdir().unwrap();
    let index_path = root.path().join(INDEX_DIR);
    fs::create_dir_all(&index_path).unwrap();
    let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
    let first_uuid = *Uuid::from_u128(1).as_bytes();
    let second_uuid = *Uuid::from_u128(2).as_bytes();
    let first = write_v4_test_artifact(
        &index_path,
        "ordinal-one.uuidx",
        crate::V4OrdinalArtifactKind::OrdinalUuids,
        1,
        &first_uuid,
    );
    let second = write_v4_test_artifact(
        &index_path,
        "ordinal-two.uuidx",
        crate::V4OrdinalArtifactKind::OrdinalUuids,
        2,
        &second_uuid,
    );
    let mut manifest = crate::V4OrdinalIdentityManifest {
        format_version: crate::ORDINAL_IDENTITY_V4,
        topology_generation: 2,
        forward_identities: Vec::new(),
        ordinal_ranges: vec![
            crate::V4OrdinalRange {
                first_node_id: 1,
                count: 1,
                artifact: first,
                blocks: Vec::new(),
            },
            crate::V4OrdinalRange {
                first_node_id: 2,
                count: 1,
                artifact: second,
                blocks: Vec::new(),
            },
        ],
        tombstones: Vec::new(),
    };
    let pinned = pinned_v4_update(root.path(), manifest.clone());
    let mut created = HashMap::new();
    let mut work = V4CompactionWork::default();
    inject_v4_compaction_post_write_failure("ordinal");

    let error = compact_v4_ordinal_interval(
        &pinned,
        &index,
        &index_path,
        &mut manifest,
        &mut created,
        1,
        2,
        &mut work,
        &mut || false,
        None,
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("v4 compaction cancelled after ordinal output"));
    assert!(!error.contains(root.path().to_string_lossy().as_ref()));
    assert_no_v4_temporary(&index);
    assert!(created.is_empty());
    assert!(work.cache_release.sync_operations > 0);
    #[cfg(target_os = "linux")]
    assert!(work.cache_release.release_operations >= 2);
}

#[test]
fn v4_tombstone_cancellation_finalizes_and_removes_unpublished_output() {
    let root = tempfile::tempdir().unwrap();
    let index_path = root.path().join(INDEX_DIR);
    fs::create_dir_all(&index_path).unwrap();
    let index = graphforge_filesystem::StableDirectory::open(&index_path).unwrap();
    let first_bytes = 1_u64.to_be_bytes();
    let second_bytes = 2_u64.to_be_bytes();
    let first = write_v4_test_artifact(
        &index_path,
        "tombstone-one.uuidx",
        crate::V4OrdinalArtifactKind::NodeTombstones,
        1,
        &first_bytes,
    );
    let second = write_v4_test_artifact(
        &index_path,
        "tombstone-two.uuidx",
        crate::V4OrdinalArtifactKind::NodeTombstones,
        2,
        &second_bytes,
    );
    let mut manifest = crate::V4OrdinalIdentityManifest {
        format_version: crate::ORDINAL_IDENTITY_V4,
        topology_generation: 2,
        forward_identities: Vec::new(),
        ordinal_ranges: Vec::new(),
        tombstones: vec![
            crate::V4OrdinalTombstones {
                generation: 1,
                artifact: first,
                blocks: Vec::new(),
            },
            crate::V4OrdinalTombstones {
                generation: 2,
                artifact: second,
                blocks: Vec::new(),
            },
        ],
    };
    let pinned = pinned_v4_update(root.path(), manifest.clone());
    let mut created = HashMap::new();
    let mut work = V4CompactionWork::default();
    inject_v4_compaction_post_write_failure("tombstone");

    let error = compact_v4_tombstone_interval(
        &pinned,
        &index,
        &index_path,
        &mut manifest,
        &mut created,
        1,
        2,
        &mut work,
        &mut || false,
        None,
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("v4 compaction cancelled after tombstone output"));
    assert!(!error.contains(root.path().to_string_lossy().as_ref()));
    assert_no_v4_temporary(&index);
    assert!(created.is_empty());
    assert!(work.cache_release.sync_operations > 0);
    #[cfg(target_os = "linux")]
    {
        assert!(work.cache_release.release_operations >= 2);
        assert!(
            work.cache_release.released_bytes >= 24,
            "{:?}",
            work.cache_release
        );
    }
}
