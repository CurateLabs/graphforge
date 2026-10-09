mod cancellation;

#[test]
fn checksum_inventory_requires_versioned_metadata_and_admits_without_payload_sha256() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("topology")).unwrap();
    let payload = b"versioned published topology payload";
    fs::write(root.path().join("topology/nodes.parquet"), payload).unwrap();
    crate::payload_digest::take_hashed_bytes();
    let (inventory, participant) = capture_graph_files(root.path()).unwrap();
    assert_eq!(
        crate::payload_digest::take_hashed_bytes(),
        payload.len() as u64
    );
    assert_eq!(
        inventory.format_version,
        GRAPH_FILES_CHECKSUM_RECORD_VERSION
    );
    assert_eq!(
        participant.record_version,
        GRAPH_FILES_CHECKSUM_RECORD_VERSION
    );
    assert_eq!(
        inventory.files[0].content_xxh64,
        crate::corruption_checksum::checksum(payload)
    );
    assert_eq!(
        inventory.files[0].content_sha256,
        hex_digest(Sha256::digest(payload).into())
    );
    verify_graph_tree(root.path(), &inventory).unwrap();
    assert_eq!(crate::payload_digest::take_hashed_bytes(), 0);
    assert!(decode_versioned_graph_files_participant(1, &participant.bytes).is_err());

    // Retired formats cannot opt into current checksums or SHA-256 fallback.
    for version in 1..=4 {
        let mut legacy = serde_json::to_value(&inventory).unwrap();
        legacy["format_version"] = serde_json::json!(version);
        let mut bytes = serde_json::to_vec(&legacy).unwrap();
        bytes.push(b'\n');
        assert!(decode_inventory(&bytes).is_err());
        assert!(decode_versioned_graph_files_participant(version, &bytes).is_err());
    }
    let mut missing = serde_json::to_value(&inventory).unwrap();
    missing["files"][0]
        .as_object_mut()
        .unwrap()
        .remove("content_xxh64");
    let mut missing_bytes = serde_json::to_vec(&missing).unwrap();
    missing_bytes.push(b'\n');
    assert!(decode_inventory(&missing_bytes).is_err());
    for malformed in [
        serde_json::json!("bad"),
        serde_json::json!(-1),
        serde_json::json!(1.5),
    ] {
        let mut value = serde_json::to_value(&inventory).unwrap();
        value["files"][0]["content_xxh64"] = malformed;
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        assert!(decode_inventory(&bytes).is_err());
    }
    let mut mismatch = inventory.clone();
    mismatch.files[0].content_xxh64 = inventory.files[0].content_xxh64 ^ 1;
    assert!(verify_graph_tree(root.path(), &mismatch).is_err());
    assert_eq!(crate::payload_digest::take_hashed_bytes(), 0);
    let mut future = inventory;
    future.format_version = 9;
    assert!(encode_inventory(&future).is_err());
}

#[test]
fn graph_file_copy_readonly_source_preserves_authority_and_barriers() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    fs::write(&source, b"authenticated graph payload").unwrap();
    let mut permissions = fs::metadata(&source).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&source, permissions).unwrap();
    let output = root.path().join("private");
    fs::create_dir(&output).unwrap();
    let nested = output.join("nested");
    fs::create_dir(&nested).unwrap();
    let destination = nested.join("payload");
    let copied = copy_regular_file(&source, &destination).unwrap();
    assert_eq!(
        fs::read(&destination).unwrap(),
        b"authenticated graph payload"
    );
    assert_eq!(
        copied.checksum,
        crate::corruption_checksum::checksum(&fs::read(&source).unwrap())
    );
    assert_eq!(copied.write_bytes, 27);
    assert_eq!(copied.read_bytes, 27);
    assert_eq!(copied.fsync_calls, 1);
    assert!(fs::metadata(&source).unwrap().permissions().readonly());
    #[cfg(windows)]
    assert!(!fs::metadata(&destination).unwrap().permissions().readonly());
    assert_eq!(sync_directory_tree(&output).unwrap(), 2);
}

use super::*;

#[test]
fn raw_checksum_contract_admits_reserved_routes_without_relaxing_mapped_wire() {
    let mut files = [
        "CON", "AUX", "COM1", "LPT9", "CON.foo", "tail.", "tail ", "Name", "name",
    ]
    .into_iter()
    .map(|route| GraphFileEntry {
        content_xxh64: 0,
        relative_path: format!("properties/{route}.parquet"),
        byte_length: 1,
        content_sha256: "0".repeat(64),
        role: GraphFileRole::Properties,
    })
    .collect::<Vec<_>>();
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: GRAPH_FILES_CHECKSUM_RECORD_VERSION,
        file_count: files.len() as u64,
        total_byte_length: files.len() as u64,
        files,
    };
    let bytes = encode_inventory(&inventory).unwrap();
    assert_eq!(decode_inventory(&bytes).unwrap(), inventory);
    for entry in &inventory.files {
        assert!(legacy_route_destination(&entry.relative_path).is_ok());
    }
    assert!(wire_relative_path("properties/CON.parquet").is_err());
    assert!(inventory_relative_path_candidates("properties/../secret.parquet").is_err());
    assert!(inventory_relative_path_candidates("properties/x/nested/secret.parquet").is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn authenticated_raw_reserved_files_keep_native_identity_and_checksum_checks() {
    let source = tempfile::tempdir().unwrap();
    fs::create_dir(source.path().join("properties")).unwrap();
    let mut files = Vec::new();
    for route in ["CON", "con", "AUX"] {
        let relative_path = format!("properties/{route}.parquet");
        let path = source.path().join(&relative_path);
        fs::write(&path, route.as_bytes()).unwrap();
        files.push(GraphFileEntry {
            content_xxh64: crate::corruption_checksum::checksum(route.as_bytes()),
            relative_path,
            byte_length: route.len() as u64,
            content_sha256: hex_digest(hash_file(&path).unwrap()),
            role: GraphFileRole::Properties,
        });
    }
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: GRAPH_FILES_CHECKSUM_RECORD_VERSION,
        file_count: files.len() as u64,
        total_byte_length: files.iter().map(|entry| entry.byte_length).sum(),
        files,
    };
    verify_graph_tree(source.path(), &inventory).unwrap();
    let victim = source.path().join(&inventory.files[0].relative_path);
    fs::write(&victim, b"bad").unwrap();
    assert!(verify_graph_tree(source.path(), &inventory).is_err());
}

#[test]
fn mapped_inventory_requires_authenticated_exact_route_authority() {
    let source = tempfile::tempdir().unwrap();
    let mut table = crate::route_component::RouteTable::default();
    let component = table.insert("CON", 4096, 10).unwrap();
    fs::create_dir(source.path().join("properties")).unwrap();
    fs::write(
        source
            .path()
            .join(format!("properties/{component}.parquet")),
        b"property",
    )
    .unwrap();
    fs::write(
        source.path().join(crate::route_component::TABLE_FILE),
        table.encode(4096).unwrap(),
    )
    .unwrap();
    let (inventory, _) = build_inventory(source.path()).unwrap();
    assert_eq!(
        inventory.format_version,
        GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION
    );
    let mut raw = inventory.clone();
    raw.format_version = GRAPH_FILES_CHECKSUM_RECORD_VERSION;
    assert!(authenticate_route_table(source.path(), &raw).is_err());
    assert!(encode_inventory(&raw).is_err());
    verify_graph_tree(source.path(), &inventory).unwrap();
    let bytes = encode_inventory(&inventory).unwrap();
    let participant = inventory_participant(bytes.clone(), inventory.file_count).unwrap();
    assert_eq!(
        participant.record_version,
        GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION
    );
    assert!(decode_versioned_graph_files_participant(
        GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION,
        &bytes
    )
    .is_ok());
    assert!(
        decode_versioned_graph_files_participant(GRAPH_FILES_CHECKSUM_RECORD_VERSION, &bytes)
            .is_err()
    );
    let mut missing = inventory.clone();
    missing
        .files
        .retain(|entry| entry.relative_path != crate::route_component::TABLE_FILE);
    missing.file_count = missing.files.len() as u64;
    missing.total_byte_length = missing.files.iter().map(|entry| entry.byte_length).sum();
    assert!(encode_inventory(&missing).is_err());
    let mut hidden = inventory.clone();
    let property = hidden
        .files
        .iter_mut()
        .find(|entry| entry.relative_path.starts_with("properties/"))
        .unwrap();
    property.relative_path = property.relative_path.replace('/', "\\");
    hidden
        .files
        .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    assert!(encode_inventory(&hidden).is_err());
    fs::write(
        source.path().join(crate::route_component::TABLE_FILE),
        b"{}\n",
    )
    .unwrap();
    assert!(authenticate_route_table(source.path(), &inventory).is_err());
}

#[test]
fn inventory_round_trip_is_canonical_and_path_ordered() {
    let source = tempfile::tempdir().unwrap();
    fs::create_dir_all(source.path().join("topology/edges")).unwrap();
    fs::write(source.path().join("topology/nodes.parquet"), b"nodes").unwrap();
    fs::write(source.path().join("topology/edges/knows.parquet"), b"edges").unwrap();
    fs::write(source.path().join(".graphforge-rewrite.lock"), b"ignored").unwrap();
    fs::create_dir_all(source.path().join(".graphforge-cache/uuid-membership")).unwrap();
    fs::write(
        source
            .path()
            .join(".graphforge-cache/uuid-membership/manifest.json"),
        b"derived",
    )
    .unwrap();

    let (inventory, participant) = capture_graph_files(source.path()).unwrap();
    assert_eq!(inventory.file_count, 2);
    assert_eq!(
        inventory.files[0].relative_path,
        "topology/edges/knows.parquet"
    );
    assert_eq!(inventory.files[1].relative_path, "topology/nodes.parquet");
    assert_eq!(inventory.files[1].role, GraphFileRole::Topology);
    assert_eq!(participant.record_family_id, GRAPH_FILES_FAMILY);
    assert_eq!(decode_inventory(&participant.bytes).unwrap(), inventory);
}

#[test]
fn wire_paths_are_forward_slash_canonical_and_resolve_v4_controls() {
    for name in [
        "topology/uuid-membership/ordinal-v4-manifest.json",
        "topology/uuid-membership/ordinal-v4-receipt.json",
        "topology/uuid-membership/ordinal-v4.lock",
    ] {
        let decoded = wire_relative_path(name).unwrap();
        assert_eq!(
            decoded.components().count(),
            3,
            "wire separators must decode as components on every host"
        );
        assert_eq!(path_text(&decoded).unwrap(), name);
    }

    for ambiguous in [
        "topology\\uuid-membership\\ordinal-v4-manifest.json",
        "topology//ordinal-v4-manifest.json",
        "topology/../ordinal-v4-manifest.json",
        "topology/NUL.json",
        "topology/trailing.",
    ] {
        assert!(wire_relative_path(ambiguous).is_err(), "{ambiguous}");
    }
}

#[test]
fn canonical_inventory_bytes_and_order_do_not_depend_on_host_separators() {
    let source = tempfile::tempdir().unwrap();
    fs::create_dir_all(source.path().join("topology/uuid-membership")).unwrap();
    fs::write(
        source
            .path()
            .join("topology/uuid-membership/ordinal-v4-receipt.json"),
        b"receipt",
    )
    .unwrap();
    fs::write(
        source
            .path()
            .join("topology/uuid-membership/ordinal-v4-manifest.json"),
        b"manifest",
    )
    .unwrap();

    let (inventory, participant) = capture_graph_files(source.path()).unwrap();
    assert_eq!(
        inventory
            .files
            .iter()
            .map(|entry| entry.relative_path.as_str())
            .collect::<Vec<_>>(),
        vec![
            "topology/uuid-membership/ordinal-v4-manifest.json",
            "topology/uuid-membership/ordinal-v4-receipt.json",
        ]
    );
    let expected = concat!(
        "{\"format\":\"graphforge-graph-files\",\"format_version\":5,\"files\":[",
        "{\"relative_path\":\"topology/uuid-membership/ordinal-v4-manifest.json\",",
        "\"byte_length\":8,\"content_sha256\":",
        "\"05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f\",",
        "\"content_xxh64\":\"d96e6aeb1d6b5f70\",",
        "\"role\":\"topology\"},",
        "{\"relative_path\":\"topology/uuid-membership/ordinal-v4-receipt.json\",",
        "\"byte_length\":7,\"content_sha256\":",
        "\"6f32860910ca0fb2a20c7fda143666b09dbf8db5238195c90a586fb542ff0cad\",",
        "\"content_xxh64\":\"85321a5f17c56483\",",
        "\"role\":\"topology\"}],\"file_count\":2,\"total_byte_length\":15}\n"
    );
    assert_eq!(participant.bytes, expected.as_bytes());
    assert_eq!(participant.bytes, encode_inventory(&inventory).unwrap());
    assert_eq!(decode_inventory(&participant.bytes).unwrap(), inventory);
}

#[cfg(unix)]
#[test]
fn raw_checksum_windows_path_reopens_after_move_and_republishes_canonically() {
    let graph_root = tempfile::tempdir().unwrap();
    let canonical = graph_root.path().join("topology/nodes.parquet");
    fs::create_dir_all(canonical.parent().unwrap()).unwrap();
    fs::write(&canonical, b"nodes").unwrap();
    let digest = hex_digest(hash_file(&canonical).unwrap());
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: GRAPH_FILES_CHECKSUM_RECORD_VERSION,
        files: vec![GraphFileEntry {
            content_xxh64: crate::corruption_checksum::checksum(b"nodes"),
            relative_path: "topology\\nodes.parquet".into(),
            byte_length: 5,
            content_sha256: digest,
            role: GraphFileRole::Topology,
        }],
        file_count: 1,
        total_byte_length: 5,
    };

    verify_graph_tree(graph_root.path(), &inventory).unwrap();
    let private = tempfile::tempdir().unwrap();
    materialize_graph_tree(graph_root.path(), &inventory, private.path()).unwrap();
    let (republished, participant) = capture_graph_files(private.path()).unwrap();
    assert_eq!(
        republished.format_version,
        GRAPH_FILES_MAPPED_CHECKSUM_RECORD_VERSION
    );
    assert_eq!(
        republished
            .files
            .iter()
            .map(|entry| entry.relative_path.as_str())
            .collect::<Vec<_>>(),
        [crate::route_component::TABLE_FILE, "topology/nodes.parquet"]
    );
    assert_eq!(
        fs::read(private.path().join("topology/nodes.parquet")).unwrap(),
        b"nodes"
    );
    authenticate_route_table(private.path(), &republished).unwrap();
    assert!(!participant.bytes.contains(&b'\\'));
}

#[cfg(unix)]
#[test]
fn raw_checksum_windows_path_rejects_authenticated_literal_ambiguity() {
    let graph_root = tempfile::tempdir().unwrap();
    let canonical = graph_root.path().join("topology/nodes.parquet");
    fs::create_dir_all(canonical.parent().unwrap()).unwrap();
    fs::write(&canonical, b"nodes").unwrap();
    fs::write(graph_root.path().join("topology\\nodes.parquet"), b"nodes").unwrap();
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: GRAPH_FILES_CHECKSUM_RECORD_VERSION,
        files: vec![GraphFileEntry {
            content_xxh64: crate::corruption_checksum::checksum(b"nodes"),
            relative_path: "topology\\nodes.parquet".into(),
            byte_length: 5,
            content_sha256: hex_digest(hash_file(&canonical).unwrap()),
            role: GraphFileRole::Topology,
        }],
        file_count: 1,
        total_byte_length: 5,
    };

    assert!(verify_graph_tree(graph_root.path(), &inventory).is_err());
}

#[test]
fn raw_checksum_windows_path_rejects_duplicate_canonical_destinations() {
    let digest = hex_digest(Sha256::digest(b"nodes").into());
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: GRAPH_FILES_CHECKSUM_RECORD_VERSION,
        files: vec![
            GraphFileEntry {
                content_xxh64: 0,
                relative_path: "topology/nodes.parquet".into(),
                byte_length: 5,
                content_sha256: digest.clone(),
                role: GraphFileRole::Topology,
            },
            GraphFileEntry {
                content_xxh64: 0,
                relative_path: "topology\\nodes.parquet".into(),
                byte_length: 5,
                content_sha256: digest,
                role: GraphFileRole::Topology,
            },
        ],
        file_count: 2,
        total_byte_length: 10,
    };

    assert!(validate_inventory_contract(&inventory).is_err());
}

#[test]
fn raw_checksum_paths_reject_portable_unicode_case_collisions_before_staging() {
    let digest = hex_digest(Sha256::digest(b"nodes").into());
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: GRAPH_FILES_CHECKSUM_RECORD_VERSION,
        files: vec![
            GraphFileEntry {
                content_xxh64: 0,
                relative_path: "topology/Å.parquet".into(),
                byte_length: 5,
                content_sha256: digest.clone(),
                role: GraphFileRole::Topology,
            },
            GraphFileEntry {
                content_xxh64: 0,
                relative_path: "topology/å.parquet".into(),
                byte_length: 5,
                content_sha256: digest,
                role: GraphFileRole::Topology,
            },
        ],
        file_count: 2,
        total_byte_length: 10,
    };
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();

    assert!(stage_graph_tree(source.path(), target.path(), &inventory).is_err());
    assert!(materialize_graph_tree(source.path(), &inventory, target.path()).is_err());
    assert!(target.path().read_dir().unwrap().next().is_none());
}

#[test]
fn portable_case_key_collides_greek_sigma_and_final_sigma() {
    assert_eq!(
        portable_case_collision_key(Path::new("topology/σ.parquet")).unwrap(),
        portable_case_collision_key(Path::new("topology/ς.parquet")).unwrap()
    );
}

#[cfg(unix)]
#[test]
fn legacy_windows_v1_literal_unix_name_migrates_to_canonical_path() {
    let source = tempfile::tempdir().unwrap();
    let literal = source.path().join("topology\\nodes.parquet");
    fs::write(&literal, b"nodes").unwrap();
    let inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: GRAPH_FILES_CHECKSUM_RECORD_VERSION,
        files: vec![GraphFileEntry {
            content_xxh64: crate::corruption_checksum::checksum(b"nodes"),
            relative_path: "topology\\nodes.parquet".into(),
            byte_length: 5,
            content_sha256: hex_digest(hash_file(&literal).unwrap()),
            role: GraphFileRole::Topology,
        }],
        file_count: 1,
        total_byte_length: 5,
    };

    verify_graph_tree(source.path(), &inventory).unwrap();
    let generation = tempfile::tempdir().unwrap();
    stage_graph_tree(source.path(), generation.path(), &inventory).unwrap();
    let staged = graph_tree_root(generation.path());
    assert_eq!(
        fs::read(staged.join("topology/nodes.parquet")).unwrap(),
        b"nodes"
    );
    assert!(!staged.join("topology\\nodes.parquet").exists());
}

#[test]
fn staging_like_parquet_name_is_authenticated_and_tamper_detected() {
    let source = tempfile::tempdir().unwrap();
    let edge_dir = source.path().join("topology/edges/KNOWS");
    fs::create_dir_all(&edge_dir).unwrap();
    let injected = edge_dir.join(".gf-stage-injected.parquet");
    fs::write(&injected, b"untrusted-edge-bytes").unwrap();

    let (inventory, _) = capture_graph_files(source.path()).unwrap();
    assert!(inventory
        .files
        .iter()
        .any(|entry| { entry.relative_path == "topology/edges/KNOWS/.gf-stage-injected.parquet" }));
    assert!(
        crate::mutator::edge_parquet_files(source.path(), None).is_err(),
        "non-canonical staged-looking names are authenticated but never topology"
    );

    let generation = tempfile::tempdir().unwrap();
    stage_graph_tree(source.path(), generation.path(), &inventory).unwrap();
    let graph_root = graph_tree_root(generation.path());
    fs::write(
        graph_root.join("topology/edges/KNOWS/.gf-stage-late.parquet"),
        b"late injection",
    )
    .unwrap();
    assert!(verify_graph_tree(&graph_root, &inventory).is_err());
}

#[test]
fn operational_classifier_rejects_noncanonical_identities() {
    assert!(!is_graph_operational_file(Path::new(&format!(
        "embeddings/.writer-{}.lock",
        "A".repeat(64)
    ))));
    assert!(!is_graph_operational_file(Path::new(&format!(
        "embeddings/spaces/{}/.writer.lock",
        "F".repeat(64)
    ))));
    let canonical = "018f1f39-7b2a-7ab0-8000-000000000001";
    assert!(is_graph_operational_file(Path::new(&format!(
        "graph-objects/active/{canonical}.lock"
    ))));
    assert!(!is_graph_operational_file(Path::new(&format!(
        "graph-objects/active/{}.lock",
        canonical.to_ascii_uppercase()
    ))));
    assert!(!is_graph_operational_file(Path::new(
        "graph-objects/active/018f1f397b2a7ab08000000000000001.lock"
    )));
}

#[cfg(unix)]
#[test]
fn operational_classifier_never_collapses_non_utf8_components() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt as _;

    let path = PathBuf::from("indexes")
        .join("search")
        .join(OsString::from_vec(vec![0xff]))
        .join(".writer.lock");
    assert!(!is_graph_operational_file(&path));
}

#[test]
fn legacy_monolith_and_shards_sort_by_canonical_wire_path() {
    let source = tempfile::tempdir().unwrap();
    fs::create_dir_all(source.path().join("topology/nodes")).unwrap();
    fs::write(source.path().join("topology/nodes.parquet"), b"legacy").unwrap();
    fs::write(
        source
            .path()
            .join("topology/nodes/00000000000000000001.parquet"),
        b"shard",
    )
    .unwrap();

    let (inventory, participant) = capture_graph_files(source.path()).unwrap();
    assert_eq!(
        inventory
            .files
            .iter()
            .map(|entry| entry.relative_path.as_str())
            .collect::<Vec<_>>(),
        vec![
            "topology/nodes.parquet",
            "topology/nodes/00000000000000000001.parquet",
        ]
    );
    assert_eq!(decode_inventory(&participant.bytes).unwrap(), inventory);
}

#[test]
fn stage_and_materialize_never_assembles_one_payload() {
    let source = tempfile::tempdir().unwrap();
    fs::create_dir_all(source.path().join("properties")).unwrap();
    fs::write(source.path().join("properties/Person.parquet"), b"person").unwrap();
    fs::write(source.path().join("runtime_catalog.parquet"), b"catalog").unwrap();
    let (inventory, _) = capture_graph_files(source.path()).unwrap();

    let generation = tempfile::tempdir().unwrap();
    let evidence = stage_graph_tree(source.path(), generation.path(), &inventory).unwrap();
    assert_eq!(evidence.files_copied, 2);
    assert_eq!(evidence.bytes_copied, inventory.total_byte_length);
    assert_eq!(evidence.application_read_bytes, inventory.total_byte_length);
    assert_eq!(evidence.application_read_calls, 2);
    assert_eq!(
        evidence.application_write_bytes,
        inventory.total_byte_length
    );
    assert_eq!(evidence.application_write_calls, 2);
    assert_eq!(evidence.fsync_calls, 4);
    assert_eq!(evidence.file_fsync_calls, 2);
    assert_eq!(evidence.directory_fsync_calls, 2);

    let sealed_source = graph_tree_root(generation.path()).join("properties/Person.parquet");
    let mut sealed_permissions = fs::metadata(&sealed_source).unwrap().permissions();
    sealed_permissions.set_readonly(true);
    fs::set_permissions(&sealed_source, sealed_permissions).unwrap();

    let private = tempfile::tempdir().unwrap();
    let opened = materialize_graph_tree(
        &graph_tree_root(generation.path()),
        &inventory,
        private.path(),
    )
    .unwrap();
    assert_eq!(opened.strategy, GraphFilesOpenStrategy::PrivateMaterialize);
    assert_eq!(opened.files_copied, 2);
    // Count the bounded source stream and destination checksum readback.
    assert_eq!(
        opened.application_read_bytes,
        2 * inventory.total_byte_length
    );
    assert_eq!(opened.application_read_calls, 4);
    let table_bytes = fs::read(private.path().join(crate::route_component::TABLE_FILE)).unwrap();
    assert_eq!(
        opened.application_write_bytes,
        inventory.total_byte_length + table_bytes.len() as u64
    );
    assert_eq!(opened.application_write_calls, 3);
    assert_eq!(opened.fsync_calls, 6);
    assert_eq!(opened.file_fsync_calls, 5);
    assert_eq!(opened.directory_fsync_calls, 1);
    let private_copy = private.path().join(format!(
        "properties/{}.parquet",
        crate::route_component::component("Person")
    ));
    assert!(fs::metadata(&sealed_source)
        .unwrap()
        .permissions()
        .readonly());
    assert!(!fs::metadata(&private_copy)
        .unwrap()
        .permissions()
        .readonly());
    assert_eq!(fs::read(&private_copy).unwrap(), b"person");
    assert_eq!(
        hash_file(&private_copy).unwrap(),
        hash_file(&sealed_source).unwrap()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        assert_ne!(
            fs::metadata(&private_copy).unwrap().ino(),
            fs::metadata(&sealed_source).unwrap().ino()
        );
    }
    fs::write(&private_copy, b"private rewrite").unwrap();
    assert_eq!(fs::read(&sealed_source).unwrap(), b"person");
    assert_eq!(pinned_open_evidence(&inventory).files_opened_in_place, 2);
}

#[test]
fn verify_rejects_digest_mismatch() {
    let source = tempfile::tempdir().unwrap();
    fs::write(source.path().join("topology.parquet"), b"a").unwrap();
    let (inventory, _) = capture_graph_files(source.path()).unwrap();
    let generation = tempfile::tempdir().unwrap();
    stage_graph_tree(source.path(), generation.path(), &inventory).unwrap();
    fs::write(
        graph_tree_root(generation.path()).join("topology.parquet"),
        b"b",
    )
    .unwrap();
    assert!(verify_graph_tree(&graph_tree_root(generation.path()), &inventory).is_err());
}

#[test]
fn checkpoint_restores_exact_legacy_routes_without_layout_upgrade() {
    let target = tempfile::tempdir().unwrap();
    fs::create_dir(target.path().join("properties")).unwrap();
    let path = target.path().join("properties/Person.parquet");
    fs::write(&path, b"original").unwrap();
    let before = capture_graph_files(target.path()).unwrap().0;
    assert_eq!(before.format_version, GRAPH_FILES_CHECKSUM_RECORD_VERSION);
    let mut checkpoint = GraphWorkspaceCheckpoint::capture(target.path()).unwrap();
    fs::write(&path, b"changed").unwrap();
    checkpoint.restore(target.path()).unwrap();
    assert_eq!(capture_graph_files(target.path()).unwrap().0, before);
    assert_eq!(fs::read(path).unwrap(), b"original");
    assert!(!target
        .path()
        .join(crate::route_component::TABLE_FILE)
        .exists());
}

#[test]
fn checkpoint_does_not_treat_disappeared_existing_target_as_originally_absent() {
    let parent = tempfile::tempdir().unwrap();
    let target = parent.path().join("existing");
    fs::create_dir(&target).unwrap();
    let mut checkpoint = GraphWorkspaceCheckpoint::capture(&target).unwrap();
    let backup = checkpoint.backup.as_ref().unwrap().path().to_path_buf();
    fs::remove_dir(&target).unwrap();
    let error = checkpoint.restore(&target).unwrap_err();
    assert!(
        error.to_string().contains("rollback backup retained"),
        "{error}"
    );
    drop(checkpoint);
    assert!(backup.exists());
    fs::remove_dir_all(backup).unwrap();
}

#[cfg(unix)]
#[test]
fn checkpoint_rejects_substituted_root_symlink_before_touching_outside_files() {
    use std::os::unix::fs::symlink;
    for originally_absent in [false, true] {
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("target");
        if !originally_absent {
            fs::create_dir(&target).unwrap();
        }
        let mut checkpoint = GraphWorkspaceCheckpoint::capture(&target).unwrap();
        let backup = checkpoint.backup.as_ref().unwrap().path().to_path_buf();
        if !originally_absent {
            fs::remove_dir(&target).unwrap();
        }
        let outside = parent.path().join("outside");
        fs::create_dir(&outside).unwrap();
        let sentinel = outside.join("sentinel");
        fs::write(&sentinel, b"outside remains unchanged").unwrap();
        symlink(&outside, &target).unwrap();
        let error = checkpoint.restore(&target).unwrap_err();
        assert!(error.to_string().contains("symbolic link"), "{error}");
        assert_eq!(
            fs::read(&sentinel).ok(),
            Some(b"outside remains unchanged".to_vec())
        );
        drop(checkpoint);
        assert!(
            backup.exists(),
            "failed restore retains the recoverable backup after owner drop"
        );
        fs::remove_dir_all(backup).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn capture_rejects_symlinks() {
    use std::os::unix::fs::symlink;

    let source = tempfile::tempdir().unwrap();
    symlink("/tmp", source.path().join("escape")).unwrap();
    assert!(capture_graph_files(source.path()).is_err());
}

#[test]
fn unsupported_inventory_version_is_structured() {
    let mut inventory = GraphFilesInventory {
        format: GRAPH_FILES_FORMAT.into(),
        format_version: 99,
        files: vec![],
        file_count: 0,
        total_byte_length: 0,
    };
    let error = validate_inventory_contract(&inventory).unwrap_err();
    assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
    inventory.format_version = GRAPH_FILES_CHECKSUM_RECORD_VERSION;
    validate_inventory_contract(&inventory).unwrap();
}

#[test]
fn mid_graph_tree_staging_failure_leaves_current_and_recovery_cleans() {
    const ENABLE_COOKIE: &str = "graphforge-internal-subprocess-v1";
    const HELPER: &str = "graph_files::tests::subprocess_mid_graph_tree_staging_writer";

    let root = tempfile::tempdir().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let parent = publish_graph_files_fixture(root.path(), &[("a.parquet", b"aaa")]);

    let source = tempfile::tempdir().unwrap();
    fs::write(source.path().join("a.parquet"), b"aaa").unwrap();
    fs::write(source.path().join("b.parquet"), b"bbb").unwrap();
    let (inventory, files) = capture_graph_files(source.path()).unwrap();
    assert!(inventory.file_count >= 2);

    let transaction_uuid = uuid::Uuid::now_v7();
    let generation_uuid = uuid::Uuid::now_v7();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(HELPER)
        .arg("--nocapture")
        .env("GRAPHFORGE_TEST_GRAPH_FILES_ROOT", root.path())
        .env("GRAPHFORGE_TEST_GRAPH_TREE_SOURCE", source.path())
        .env(
            "GRAPHFORGE_TEST_TRANSACTION_UUID",
            transaction_uuid.hyphenated().to_string(),
        )
        .env(
            "GRAPHFORGE_TEST_GENERATION_UUID",
            generation_uuid.hyphenated().to_string(),
        )
        .env(
            "GRAPHFORGE_TEST_GRAPH_FILES_PARTICIPANT",
            serde_json::to_string(&participant_json(&files)).unwrap(),
        )
        .env("GRAPHFORGE_PROJECT_FAILPOINTS", ENABLE_COOKIE)
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINT",
            "project.after_graph_file_staged.error",
        )
        .status()
        .unwrap();
    assert!(
        status.success(),
        "mid-tree staging helper must exit after the injected error"
    );

    let current = crate::resolve_project_generation(root.path()).unwrap();
    assert_eq!(current.generation_uuid(), parent);
    assert!(
        root.path()
            .join("generations")
            .join(generation_uuid.hyphenated().to_string())
            .exists(),
        "incomplete attempt should still be on disk before recovery"
    );

    let report = crate::recover_project_transactions(root.path()).unwrap();
    assert_eq!(report.selected_generation_uuid, parent);
    assert!(
        !root
            .path()
            .join("generations")
            .join(generation_uuid.hyphenated().to_string())
            .exists(),
        "recovery must clean the incomplete graph-tree attempt"
    );
    let reopened = crate::resolve_project_generation(root.path()).unwrap();
    assert_eq!(reopened.generation_uuid(), parent);
    let inventory = reopened.graph_files_inventory().unwrap().unwrap();
    assert_eq!(inventory.file_count, 1);
}

#[test]
fn subprocess_mid_graph_tree_staging_writer() {
    let Ok(root) = std::env::var("GRAPHFORGE_TEST_GRAPH_FILES_ROOT") else {
        return;
    };
    let source = PathBuf::from(std::env::var("GRAPHFORGE_TEST_GRAPH_TREE_SOURCE").unwrap());
    let transaction_uuid =
        uuid::Uuid::parse_str(&std::env::var("GRAPHFORGE_TEST_TRANSACTION_UUID").unwrap()).unwrap();
    let generation_uuid =
        uuid::Uuid::parse_str(&std::env::var("GRAPHFORGE_TEST_GENERATION_UUID").unwrap()).unwrap();
    let participant_raw = std::env::var("GRAPHFORGE_TEST_GRAPH_FILES_PARTICIPANT").unwrap();
    let files = participant_from_json(&participant_raw);
    let mut participants = crate::empty_workspace_participants().unwrap();
    participants.insert(0, files);
    let request = crate::ProjectGenerationRequest {
        transaction_uuid,
        generation_uuid,
        capabilities: vec![
            crate::ProjectCapability {
                capability_id: GRAPH_CAPABILITY_ID.into(),
                capability_version: GRAPH_CAPABILITY_VERSION,
            },
            crate::ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let error = (|| {
        let outcome =
            crate::stage_project_generation_with_graph_tree(&root, &request, Some(&source))?;
        let crate::ProjectStageOutcome::Staged(staged) = outcome else {
            panic!("fresh graph/files transaction unexpectedly replayed");
        };
        staged.validate(|_| Ok(()), |_, _| Ok(()))?.publish()?;
        Ok::<(), GfError>(())
    })()
    .expect_err("configured graph-tree failpoint did not fire");
    assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    assert!(error.to_string().contains("GRAPH_TREE_STAGING"));
}

fn publish_graph_files_fixture(container: &Path, files: &[(&str, &[u8])]) -> uuid::Uuid {
    let source = tempfile::tempdir().unwrap();
    for (relative, bytes) in files {
        let path = source.path().join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, bytes).unwrap();
    }
    let (_, files_participant) = capture_graph_files(source.path()).unwrap();
    let mut participants = crate::empty_workspace_participants().unwrap();
    participants.insert(0, files_participant);
    let request = crate::ProjectGenerationRequest {
        transaction_uuid: uuid::Uuid::now_v7(),
        generation_uuid: uuid::Uuid::now_v7(),
        capabilities: vec![
            crate::ProjectCapability {
                capability_id: GRAPH_CAPABILITY_ID.into(),
                capability_version: GRAPH_CAPABILITY_VERSION,
            },
            crate::ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let expected = request.generation_uuid;
    let crate::ProjectStageOutcome::Staged(staged) =
        crate::stage_project_generation_with_graph_tree(container, &request, Some(source.path()))
            .unwrap()
    else {
        panic!("fresh graph/files fixture unexpectedly replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    expected
}

#[derive(Serialize, Deserialize)]
struct ParticipantWire {
    capability_id: String,
    capability_version: u32,
    record_family_id: String,
    record_version: u32,
    encoding: String,
    schema_fingerprint: [u8; 32],
    row_count: u64,
    bytes: Vec<u8>,
}

fn participant_json(participant: &ProjectParticipant) -> ParticipantWire {
    ParticipantWire {
        capability_id: participant.capability_id.clone(),
        capability_version: participant.capability_version,
        record_family_id: participant.record_family_id.clone(),
        record_version: participant.record_version,
        encoding: match participant.encoding {
            ProjectParticipantEncoding::Json => "json".into(),
            ProjectParticipantEncoding::Arrow => "arrow".into(),
            ProjectParticipantEncoding::Parquet => "parquet".into(),
        },
        schema_fingerprint: participant.schema_fingerprint,
        row_count: participant.row_count,
        bytes: participant.bytes.clone(),
    }
}

fn participant_from_json(raw: &str) -> ProjectParticipant {
    let wire: ParticipantWire = serde_json::from_str(raw).unwrap();
    ProjectParticipant {
        capability_id: wire.capability_id,
        capability_version: wire.capability_version,
        record_family_id: wire.record_family_id,
        record_version: wire.record_version,
        encoding: match wire.encoding.as_str() {
            "json" => ProjectParticipantEncoding::Json,
            "arrow" => ProjectParticipantEncoding::Arrow,
            "parquet" => ProjectParticipantEncoding::Parquet,
            other => panic!("unknown encoding {other}"),
        },
        schema_fingerprint: wire.schema_fingerprint,
        row_count: wire.row_count,
        bytes: wire.bytes,
    }
}

#[test]
fn descriptor_record_version_must_match_graph_files_payload_version() {
    let inventory = inventory_from_entries(Vec::new()).unwrap();
    let v1 = encode_inventory(&inventory).unwrap();
    let v2 = crate::encode_graph_files_root_v2(&crate::GraphFilesRootV2 {
        format: crate::GRAPH_FILES_V2_FORMAT.into(),
        format_version: GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION,
        root_node_sha256: "0".repeat(64),
        logical_file_count: 0,
        logical_byte_length: 0,
    })
    .unwrap();
    assert!(
        decode_versioned_graph_files_participant(GRAPH_FILES_CHECKSUM_RECORD_VERSION, &v1).is_ok()
    );
    assert!(decode_versioned_graph_files_participant(
        GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION,
        &v2
    )
    .is_ok());
    assert!(
        decode_versioned_graph_files_participant(GRAPH_FILES_CHECKSUM_RECORD_VERSION, &v2).is_err()
    );
    assert!(decode_versioned_graph_files_participant(
        GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION,
        &v1
    )
    .is_err());
}
