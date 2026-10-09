use super::super::INDEX_DIR;
use super::super::TopologyIndexReceipt;
use super::super::V4_ORDINAL_MANIFEST;
use super::super::V4_ORDINAL_RECEIPT;
use super::super::tests::fixture;
use super::super::tests::install_test_v4_facet;
use super::super::topology_delta::hex_sha256;
use super::maintain_uuid_membership_orphans_with_ordinal_authority;
use std::fs;

#[test]
fn orphan_collection_authenticates_and_preserves_the_selected_v4_facet() {
    let (dir, nodes, _) = fixture();
    fs::write(
        dir.path().join("topology/generation.json"),
        b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
    )
    .unwrap();
    let (selected, authority) = install_test_v4_facet(dir.path(), 7, &nodes);
    let orphan = dir
        .path()
        .join(INDEX_DIR)
        .join("ordinal-v4-7-0000000000000000.uuidx");
    fs::write(&orphan, b"orphan").unwrap();

    let work =
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 16, Some(&authority))
            .unwrap();
    assert_eq!(work.removed, 1);
    assert!(!orphan.exists());
    for name in selected {
        assert!(dir.path().join(INDEX_DIR).join(name).exists());
    }
    let receipt_path = dir.path().join(INDEX_DIR).join(V4_ORDINAL_RECEIPT);
    let manifest_path = dir.path().join(INDEX_DIR).join(V4_ORDINAL_MANIFEST);
    let mut manifest: crate::V4OrdinalIdentityManifest =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest.topology_generation = 8;
    let replacement = serde_json::to_vec(&manifest).unwrap();
    fs::write(&manifest_path, &replacement).unwrap();
    let mut receipt: TopologyIndexReceipt =
        serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    receipt.expected_generation = 8;
    receipt.manifest_sha256 = hex_sha256(&replacement);
    fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
    assert!(
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 16, Some(&authority))
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn ordinal_orphan_admission_rejects_link_fifo_and_oversized_manifest() {
    use std::os::unix::fs::symlink;

    let (dir, nodes, _) = fixture();
    fs::write(
        dir.path().join("topology/generation.json"),
        b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
    )
    .unwrap();
    let (_, authority) = install_test_v4_facet(dir.path(), 7, &nodes);
    let manifest = dir.path().join(INDEX_DIR).join(V4_ORDINAL_MANIFEST);
    let original = fs::read(&manifest).unwrap();
    let replacement = dir.path().join(INDEX_DIR).join("replacement.json");
    fs::write(&replacement, &original).unwrap();

    fs::remove_file(&manifest).unwrap();
    symlink(&replacement, &manifest).unwrap();
    assert!(
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 16, Some(&authority))
            .is_err()
    );
    fs::remove_file(&manifest).unwrap();

    assert!(
        std::process::Command::new("mkfifo")
            .arg(&manifest)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 16, Some(&authority))
            .is_err()
    );
    fs::remove_file(&manifest).unwrap();

    fs::write(
        &manifest,
        vec![b'x'; crate::ordinal_identity_v4::MAX_MANIFEST_BYTES as usize + 1],
    )
    .unwrap();
    assert!(
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 16, Some(&authority))
            .is_err()
    );
}

/// Hydration hard-links forward and ordinal runs from the content store. The
/// collector may only retire private names: a shared run, live or orphaned, and
/// the store's name for it, survive untouched (#1388).
#[cfg(unix)]
#[test]
fn orphan_collection_never_unlinks_shared_v4_runs_or_their_store_names() {
    let (dir, nodes, _) = fixture();
    fs::write(
        dir.path().join("topology/generation.json"),
        b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
    )
    .unwrap();
    let (selected, authority) = install_test_v4_facet(dir.path(), 7, &nodes);
    let index = dir.path().join(INDEX_DIR);
    let store = dir.path().join("store");
    fs::create_dir(&store).unwrap();
    let orphan_name = "ordinal-v4-7-0000000000000000.uuidx";
    fs::write(index.join(orphan_name), b"orphan").unwrap();
    let mut shared = selected
        .iter()
        .filter(|name| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|e| e == "uuidx")
        })
        .cloned()
        .collect::<Vec<_>>();
    assert!(!shared.is_empty());
    shared.push(orphan_name.to_owned());
    let mut before = Vec::new();
    for name in &shared {
        let linked = store.join(name);
        fs::hard_link(index.join(name), &linked).unwrap();
        let mut permissions = fs::metadata(&linked).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&linked, permissions).unwrap();
        before.push((name.clone(), fs::read(&linked).unwrap()));
    }

    let work =
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 16, Some(&authority))
            .unwrap();
    assert_eq!(work.removed, 0);
    assert_eq!(
        work.deferred_linked, 1,
        "the shared orphan is deferred, not unlinked"
    );
    for (name, bytes) in before {
        assert_eq!(fs::read(index.join(&name)).unwrap(), bytes, "{name}");
        assert_eq!(
            fs::read(store.join(&name)).unwrap(),
            bytes,
            "{name} store name"
        );
    }
}

#[test]
fn orphan_maintenance_is_bounded_and_never_collects_legacy_membership_files() {
    let (dir, nodes, _) = fixture();
    fs::write(
        dir.path().join("topology/generation.json"),
        b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
    )
    .unwrap();
    let (selected, authority) = install_test_v4_facet(dir.path(), 7, &nodes);
    let root = dir.path().join(INDEX_DIR);
    let orphan_one = root.join("ordinal-v4-7-0000000000000001.uuidx");
    let orphan_two = root.join("forward-v4-7-0000000000000002.uuidx");
    fs::write(&orphan_one, b"orphan one").unwrap();
    fs::write(&orphan_two, b"orphan two").unwrap();
    // A project built before #1902 still carries the membership index. Its
    // files are not authority and not collectable.
    let legacy = [
        root.join("manifest.json"),
        root.join("topology-receipt.json"),
        root.join("identities-v5-1-0000000000000003.uuidx"),
        root.join("node-surrogates-v5-1-0000000000000004.uuidx"),
    ];
    for path in &legacy {
        fs::write(path, b"legacy membership bytes").unwrap();
    }

    let first =
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 1, Some(&authority))
            .unwrap();
    assert_eq!(first.candidates, 2);
    assert_eq!(first.removed, 1);
    assert_eq!(first.deferred_limit, 1);
    let second =
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 64, Some(&authority))
            .unwrap();
    assert_eq!(second.removed, 1);
    assert!(!orphan_one.exists());
    assert!(!orphan_two.exists());
    for name in selected {
        assert!(root.join(name).exists());
    }
    for path in legacy {
        assert_eq!(fs::read(path).unwrap(), b"legacy membership bytes");
    }
}
