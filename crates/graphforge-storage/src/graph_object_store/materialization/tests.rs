use super::*;
use crate::graph_object_store::File;
use crate::graph_object_store::fs;
use crate::graph_object_store::graph_object_path;
use crate::graph_object_store::install_graph_object_bytes;

#[cfg(windows)]
#[test]
fn windows_materialization_rejects_regular_cas_substitution() {
    let objects = tempfile::tempdir().unwrap();
    let (digest, _) = install_graph_object_bytes(objects.path(), b"payload").unwrap();
    let source = graph_object_path(objects.path(), &digest).unwrap();
    fs::remove_file(&source).unwrap();
    fs::write(&source, b"hostile").unwrap();
    let owner = tempfile::tempdir().unwrap();
    let target_path = owner.path().join("materialized");
    let target = open_empty_materialization_target(&target_path).unwrap();
    assert!(link_materialized_object(&target, &source, "payload.bin", &digest, 7).is_err());
    assert!(!target_path.join("payload.bin").exists());
}

#[cfg(unix)]
#[test]
fn materialization_rejects_target_and_intermediate_symlink_escape() {
    use std::os::unix::fs::symlink;

    let objects = tempfile::tempdir().unwrap();
    let (digest, _) = install_graph_object_bytes(objects.path(), b"payload").unwrap();
    let entry = crate::GraphFileEntry {
        relative_path: "nested/payload.bin".into(),
        byte_length: 7,
        content_sha256: digest,
        role: crate::GraphFileRole::Other,
    };
    let inventory = crate::graph_files::inventory_from_entries(vec![entry.clone()]).unwrap();

    let owner = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let linked_target = owner.path().join("linked-target");
    symlink(outside.path(), &linked_target).unwrap();
    assert!(materialize_graph_objects(objects.path(), &inventory, &linked_target).is_err());
    assert!(!outside.path().join("nested/payload.bin").exists());

    let target = owner.path().join("real-target");
    let directory = open_empty_materialization_target(&target).unwrap();
    symlink(outside.path(), target.join("nested")).unwrap();
    assert!(
        link_materialized_object(
            &directory,
            &graph_object_path(objects.path(), &entry.content_sha256).unwrap(),
            &entry.relative_path,
            &entry.content_sha256,
            entry.byte_length,
        )
        .is_err()
    );
    assert!(!outside.path().join("payload.bin").exists());
}

#[test]
fn materialization_translates_legacy_topology_route_and_counts_owned_copy() {
    let root = tempfile::tempdir().unwrap();
    let payload = b"ordinary topology payload";
    let (digest, _) = install_graph_object_bytes(root.path(), payload).unwrap();
    let source_path = graph_object_path(root.path(), &digest).unwrap();
    let source_identity = graphforge_filesystem::path_identity(&source_path).unwrap();
    let inventory = crate::graph_files::inventory_from_entries(vec![crate::GraphFileEntry {
        relative_path: "topology/edges/knows.parquet".into(),
        byte_length: payload.len() as u64,
        content_sha256: digest,
        role: crate::GraphFileRole::Topology,
    }])
    .unwrap();
    let owner = tempfile::tempdir().unwrap();
    let target = owner.path().join("workspace");

    let evidence = materialize_graph_objects(root.path(), &inventory, &target).unwrap();

    assert_eq!(
        fs::read(target.join("topology/edges").join(format!(
            "{}.parquet",
            crate::route_component::component("knows")
        )))
        .unwrap(),
        payload
    );
    assert!(!target.join("topology/edges/knows.parquet").exists());
    let table_bytes = fs::read(target.join(crate::route_component::TABLE_FILE))
        .unwrap()
        .len() as u64;
    assert_eq!(fs::read(&source_path).unwrap(), payload);
    assert_eq!(
        graphforge_filesystem::path_identity(&source_path).unwrap(),
        source_identity
    );
    assert_ne!(
        graphforge_filesystem::path_identity(&target.join("topology/edges").join(format!(
            "{}.parquet",
            crate::route_component::component("knows")
        )))
        .unwrap(),
        source_identity
    );
    assert!(!target.join("files").exists());
    assert_eq!(evidence.files_reused, 0);
    assert_eq!(evidence.bytes_reused, 0);
    assert_eq!(evidence.files_copied, 1);
    assert_eq!(evidence.application_read_bytes, payload.len() as u64 * 2);
    assert_eq!(evidence.application_read_calls, 2);
    assert_eq!(
        evidence.application_write_bytes,
        payload.len() as u64 + table_bytes
    );
    assert_eq!(evidence.application_write_calls, 2);
    assert_eq!(evidence.fsync_calls, 4);
    assert_eq!(evidence.file_fsync_calls, 2);
    assert_eq!(evidence.directory_fsync_calls, 2);
}

#[test]
fn materialization_gives_mutable_uuid_controls_private_single_link_inodes() {
    let root = tempfile::tempdir().unwrap();
    let files = [
        (
            "topology/uuid-membership/manifest.json",
            &b"v5 manifest"[..],
        ),
        (
            "topology/uuid-membership/topology-receipt.json",
            &b"v5 receipt"[..],
        ),
        ("topology/uuid-membership/ordinal-v4.lock", &b""[..]),
        (
            "topology/uuid-membership/ordinal-v4-manifest.json",
            &b"manifest"[..],
        ),
        (
            "topology/uuid-membership/ordinal-v4-receipt.json",
            &b"receipt"[..],
        ),
        (
            "topology/uuid-membership/ordinal-v4-1-0123456789abcdef.uuidx",
            &b"ordinal"[..],
        ),
    ];
    let mut entries = Vec::new();
    for (relative_path, payload) in files {
        let (digest, _) = install_graph_object_bytes(root.path(), payload).unwrap();
        entries.push(crate::GraphFileEntry {
            relative_path: relative_path.to_owned(),
            byte_length: payload.len() as u64,
            content_sha256: digest,
            role: crate::GraphFileRole::Topology,
        });
    }
    entries.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let inventory = crate::graph_files::inventory_from_entries(entries).unwrap();
    let owner = tempfile::tempdir().unwrap();
    let target = owner.path().join("workspace");

    let evidence = materialize_graph_objects(root.path(), &inventory, &target).unwrap();

    assert_eq!(evidence.files_copied, inventory.file_count);
    assert_eq!(evidence.files_reused, 0);
    let nonempty_bytes = inventory.total_byte_length;
    let nonempty_files = inventory
        .files
        .iter()
        .filter(|entry| entry.byte_length != 0)
        .count() as u64;
    assert_eq!(evidence.application_read_bytes, nonempty_bytes * 2);
    assert_eq!(evidence.application_read_calls, nonempty_files * 2);
    let table_bytes = fs::read(target.join(crate::route_component::TABLE_FILE))
        .unwrap()
        .len() as u64;
    assert_eq!(
        evidence.application_write_bytes,
        nonempty_bytes + table_bytes
    );
    assert_eq!(evidence.application_write_calls, nonempty_files + 1);
    assert_eq!(evidence.fsync_calls, inventory.file_count * 2 + 2);
    assert_eq!(evidence.file_fsync_calls, inventory.file_count + 1);
    assert_eq!(evidence.directory_fsync_calls, inventory.file_count + 1);
    for entry in &inventory.files {
        let file = File::open(target.join(&entry.relative_path)).unwrap();
        assert_eq!(graphforge_filesystem::file_link_count(&file).unwrap(), 1);
        assert_eq!(
            fs::read(target.join(&entry.relative_path)).unwrap().len() as u64,
            entry.byte_length
        );
    }
}

#[cfg(unix)]
#[test]
fn materialization_rejects_substituted_cas_source_and_removes_destination() {
    use std::os::unix::fs::symlink;

    let objects = tempfile::tempdir().unwrap();
    let (digest, _) = install_graph_object_bytes(objects.path(), b"payload").unwrap();
    let source = graph_object_path(objects.path(), &digest).unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let target = target_root.path().join("materialized");

    let original = source.with_extension("original");
    fs::rename(&source, &original).unwrap();
    let malicious = objects.path().join("malicious");
    fs::write(&malicious, b"hostile").unwrap();
    symlink(&malicious, &source).unwrap();
    let directory = open_empty_materialization_target(&target).unwrap();
    assert!(link_materialized_object(&directory, &source, "payload.bin", &digest, 7).is_err());
    assert!(!target.join("payload.bin").exists());

    fs::remove_file(&source).unwrap();
    fs::write(&source, b"hostile").unwrap();
    let second_target = target_root.path().join("materialized-regular");
    let directory = open_empty_materialization_target(&second_target).unwrap();
    assert!(link_materialized_object(&directory, &source, "payload.bin", &digest, 7).is_err());
    assert!(!second_target.join("payload.bin").exists());
}
