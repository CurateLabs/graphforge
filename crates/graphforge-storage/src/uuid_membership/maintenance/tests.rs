use super::super::INDEX_DIR;
use super::super::MANIFEST;
use super::super::MAX_MANIFEST_BYTES;
use super::super::Manifest;
use super::super::TopologyIndexReceipt;
use super::super::UuidIndexBuildLimits;
use super::super::UuidIndexKind;
use super::super::UuidMembershipIndex;
use super::super::V4_ORDINAL_MANIFEST;
use super::super::V4_ORDINAL_RECEIPT;
use super::super::append_uuid_membership_delta;
use super::super::rebuild::rebuild_uuid_membership_indexes;
use super::super::tests::fixture;
use super::super::tests::install_test_v4_facet;
use super::super::topology_delta::hex_sha256;
use super::maintain_uuid_membership_orphans;
use super::maintain_uuid_membership_orphans_with_ordinal_authority;
use std::fs;
use uuid::Uuid;

#[test]
fn orphan_collection_authenticates_and_preserves_union_of_v3_and_v4_facets() {
    let (dir, nodes, edges) = fixture();
    fs::write(
        dir.path().join("topology/generation.json"),
        b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
    )
    .unwrap();
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
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
    let mut v3 = UuidMembershipIndex::open(dir.path()).unwrap();
    assert_eq!(v3.count(UuidIndexKind::Node), nodes.len() as u64);
    assert_eq!(v3.count(UuidIndexKind::Edge), edges.len() as u64);
    assert_eq!(
        v3.probe(UuidIndexKind::Edge, &[edges[0]]).unwrap().0,
        vec![true]
    );

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
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
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

    fs::write(&manifest, vec![b'x'; MAX_MANIFEST_BYTES as usize + 1]).unwrap();
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
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let (selected, authority) = install_test_v4_facet(dir.path(), 7, &nodes);
    let index = dir.path().join(INDEX_DIR);
    let store = dir.path().join("store");
    fs::create_dir(&store).unwrap();
    let orphan_name = "ordinal-v4-7-0000000000000000.uuidx";
    fs::write(index.join(orphan_name), b"orphan").unwrap();
    let mut shared = selected
        .iter()
        .filter(|name| name.ends_with(".uuidx"))
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
    assert_eq!(work.deferred_linked, 1, "the shared orphan is deferred, not unlinked");
    for (name, bytes) in before {
        assert_eq!(fs::read(index.join(&name)).unwrap(), bytes, "{name}");
        assert_eq!(fs::read(store.join(&name)).unwrap(), bytes, "{name} store name");
    }
}

#[test]
fn orphan_maintenance_is_bounded_and_preserves_linked_and_live_runs() {
    let dir = tempfile::tempdir().unwrap();
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    append_uuid_membership_delta(
        dir.path(),
        1,
        &[(Uuid::from_u128(1), 1)],
        &[Uuid::from_u128(2)],
    )
    .unwrap();
    let root = dir.path().join(INDEX_DIR);
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(root.join(MANIFEST)).unwrap()).unwrap();
    let live = root.join(&manifest.runs[0].identities.name);
    let orphan_one = root.join("identities-v5-orphan-0000000000000001.uuidx");
    let orphan_two = root.join("node-surrogates-v5-orphan-0000000000000002.uuidx");
    fs::copy(&live, &orphan_one).unwrap();
    fs::copy(&live, &orphan_two).unwrap();

    assert!(maintain_uuid_membership_orphans(dir.path(), 1).is_err());

    let first =
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 1, None).unwrap();
    assert_eq!(first.candidates, 2);
    assert_eq!(first.removed, 1);
    assert_eq!(first.deferred_limit, 1);
    assert!(live.is_file());
    let second =
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 64, None).unwrap();
    assert_eq!(second.removed, 1);
    assert!(live.is_file());
    assert!(!orphan_one.exists());
    assert!(!orphan_two.exists());
}

#[test]
fn orphan_collection_refuses_unsupported_manifest_before_deleting_any_file() {
    let (dir, _, _) = fixture();
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let root = dir.path().join(INDEX_DIR);
    let path = root.join(MANIFEST);
    let original = fs::read(&path).unwrap();
    let mut manifest: Manifest = serde_json::from_slice(&original).unwrap();
    let orphan = root.join("identities-v5-orphan-0000000000000001.uuidx");
    fs::write(&orphan, b"must survive unsupported metadata").unwrap();
    manifest.format_version = super::super::FORMAT_VERSION + 1;
    let bytes = serde_json::to_vec(&manifest).unwrap();
    fs::write(&path, &bytes).unwrap();
    let authority = super::AuthenticatedV3MembershipAuthority {
        topology_generation: manifest.current_generation,
        manifest_sha256: hex_sha256(&bytes),
    };
    let error = super::maintain_uuid_membership_orphans_with_authorities(
        dir.path(),
        16,
        Some(&authority),
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("unsupported UUID membership format version 8"),
        "{error}"
    );
    assert!(error.contains("recreate the index"), "{error}");
    assert_eq!(
        fs::read(&orphan).unwrap(),
        b"must survive unsupported metadata"
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let error = super::super::validate_run_descriptors(&manifest)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("unsupported UUID membership format version 8"),
        "{error}"
    );
    fs::write(&path, original).unwrap();
    let work =
        maintain_uuid_membership_orphans_with_ordinal_authority(dir.path(), 16, None).unwrap();
    assert_eq!(work.removed, 1);
    assert!(!orphan.exists());
}
