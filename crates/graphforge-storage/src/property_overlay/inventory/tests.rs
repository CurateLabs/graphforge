use super::super::*;
use super::*;
use arrow::array::{FixedSizeBinaryArray, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;

#[test]
fn fragment_identity_is_numeric_canonical_and_total() {
    let id = PropertyFragmentId {
        generation: 2,
        ordinal: 10,
    };
    assert_eq!(PropertyFragmentId::parse(&id.file_name()).unwrap(), id);
    for invalid in [
        "2-10.parquet",
        "00000000000000000002-00000000000000000010.PARQUET",
        "00000000000000000002-00000000000000000010.parquet.tmp",
        "00000000000000000002-00000000000000000010-0.parquet",
    ] {
        assert!(PropertyFragmentId::parse(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn route_inventory_rejects_mixed_authority_and_noncanonical_ordinals() {
    let mixed = TempDir::new().unwrap();
    fs::create_dir_all(mixed.path().join("properties/Person")).unwrap();
    fs::write(mixed.path().join("properties/Person.parquet"), b"legacy").unwrap();
    fs::write(
        mixed.path().join("properties/Person").join(
            PropertyFragmentId {
                generation: 1,
                ordinal: 0,
            }
            .file_name(),
        ),
        b"immutable",
    )
    .unwrap();
    let migrated =
        enumerate_property_fragments(mixed.path(), PropertyRouteKind::Node, "Person").unwrap();
    assert_eq!(migrated.len(), 2);
    assert_eq!(
        migrated[0].id,
        PropertyFragmentId {
            generation: 0,
            ordinal: 0,
        }
    );
    assert_eq!(migrated[1].id.generation, 1);

    let gap = TempDir::new().unwrap();
    fs::create_dir_all(gap.path().join("properties/Person")).unwrap();
    fs::write(
        gap.path().join("properties/Person").join(
            PropertyFragmentId {
                generation: 4,
                ordinal: 1,
            }
            .file_name(),
        ),
        b"gap",
    )
    .unwrap();
    assert!(
        enumerate_property_fragments(gap.path(), PropertyRouteKind::Node, "Person")
            .unwrap_err()
            .to_string()
            .contains("ordinal zero")
    );
}

#[test]
fn authenticated_fragment_sequence_rejects_mixed_gapped_and_nonzero_starts() {
    let id = |generation, ordinal| PropertyFragmentId {
        generation,
        ordinal,
    };
    assert!(validate_fragment_id_sequence([id(0, 0), id(1, 0)]).is_ok());
    assert!(validate_fragment_id_sequence([id(7, 1)]).is_err());
    assert!(validate_fragment_id_sequence([id(7, 0), id(7, 2)]).is_err());
    assert!(validate_fragment_id_sequence([id(7, 0), id(8, 1)]).is_err());
    assert!(validate_fragment_id_sequence([id(7, 0), id(7, 1), id(8, 0)]).is_ok());
    assert!(
        validate_fragment_id_sequence([id(7, u64::MAX - 1), id(7, u64::MAX), id(7, 0),]).is_err()
    );
}

#[test]
fn enumeration_uses_numeric_authority_and_rejects_near_misses() {
    let dir = TempDir::new().unwrap();
    let route = dir.path().join("properties/Person");
    fs::create_dir_all(&route).unwrap();
    for id in [
        PropertyFragmentId {
            generation: 10,
            ordinal: 0,
        },
        PropertyFragmentId {
            generation: 2,
            ordinal: 0,
        },
    ] {
        fs::write(route.join(id.file_name()), b"x").unwrap();
    }
    let found =
        enumerate_property_fragments(dir.path(), PropertyRouteKind::Node, "Person").unwrap();
    assert_eq!(found[0].id.generation, 2);
    assert_eq!(found[1].id.generation, 10);
    fs::write(route.join("junk.parquet"), b"x").unwrap();
    assert!(enumerate_property_fragments(dir.path(), PropertyRouteKind::Node, "Person").is_err());
}

#[test]
fn mapped_route_admission_does_not_open_unrelated_missing_payload() {
    let dir = TempDir::new().unwrap();
    let mut table = crate::route_component::RouteTable::default();
    let selected = table.insert("CON", 64 * 1024 * 1024, 100_000).unwrap();
    let unrelated = table.insert("con", 64 * 1024 * 1024, 100_000).unwrap();
    let relative = format!("properties/{selected}.parquet");
    fs::create_dir_all(dir.path().join("properties")).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            FixedSizeBinaryArray::try_from_iter(vec![vec![4; 16]].into_iter()).unwrap(),
        )],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(dir.path().join(&relative)).unwrap(),
        schema,
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let payload = fs::read(dir.path().join(&relative)).unwrap();
    let table_bytes = table.encode(64 * 1024 * 1024).unwrap();
    fs::write(
        dir.path().join(crate::route_component::TABLE_FILE),
        &table_bytes,
    )
    .unwrap();
    let entry = |relative_path: String, bytes: &[u8], role| crate::GraphFileEntry {
        relative_path,
        byte_length: bytes.len() as u64,
        content_sha256: digest_hex(&Sha256::digest(bytes)),
        role,
    };
    let inventory = crate::graph_files::inventory_from_entries_with_version(
        vec![
            entry(relative, &payload, crate::GraphFileRole::Properties),
            entry(
                format!("properties/{unrelated}.parquet"),
                b"absent",
                crate::GraphFileRole::Properties,
            ),
            entry(
                crate::route_component::TABLE_FILE.into(),
                &table_bytes,
                crate::GraphFileRole::Other,
            ),
        ],
        crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION,
    )
    .unwrap();
    let admitted = AuthenticatedPropertyInventory::from_inventory_at_root(
        dir.path(),
        inventory.clone(),
        Some((PropertyRouteKind::Node, "CON")),
    )
    .unwrap();
    assert!(
        admitted
            .route_schema(PropertyRouteKind::Node, "CON")
            .is_some()
    );
    assert!(
        admitted
            .route_schema(PropertyRouteKind::Node, "con")
            .is_none()
    );
    assert_eq!(
        admitted.open_metrics().property_authentication_bytes,
        payload.len() as u64
    );
    assert!(
        AuthenticatedPropertyInventory::from_inventory_at_root(
            dir.path(),
            inventory,
            Some((PropertyRouteKind::Node, "con"))
        )
        .is_err()
    );
}

#[test]
fn route_schema_unions_nullability_and_retains_historical_fields() {
    let older = Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("historical", DataType::Utf8, false),
        Field::new("shared", DataType::Int64, false),
    ]);
    let newer = Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("newer", DataType::Boolean, true),
        Field::new("shared", DataType::Int64, true),
    ]);
    let merged =
        merge_property_route_schemas(PropertyRouteKind::Node, "Person", [&older, &newer]).unwrap();
    assert!(merged.field_with_name("historical").is_ok());
    assert!(merged.field_with_name("newer").is_ok());
    let shared = merged.field_with_name("shared").unwrap();
    assert_eq!(shared.data_type(), &DataType::Int64);
    assert!(shared.is_nullable());
}

#[test]
fn live_schema_summary_tracks_last_owner_and_fails_closed() {
    // Maintenance/GFDR producers can begin from the canonical UUID-only
    // schema, which intentionally carries no route metadata. Route identity
    // is authenticated by the caller/inventory rather than inferred here.
    let inferred = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("shared", DataType::Int64, true),
        ],
        HashMap::new(),
    ));
    let first = PropertySnapshotRow {
        uuid: [1; 16],
        tombstone: false,
        values: BTreeMap::from([("shared".into(), IrLiteral::Int(1))]),
    };
    let second = PropertySnapshotRow {
        uuid: [2; 16],
        tombstone: false,
        values: BTreeMap::from([("shared".into(), IrLiteral::Int(2))]),
    };
    let authority = update_live_route_schema(
        PropertyRouteKind::Node,
        "Person",
        None,
        Arc::clone(&inferred),
        &BTreeMap::new(),
        &[first.clone(), second.clone()],
    )
    .unwrap();
    assert_eq!(
        decode_live_schema_summary(authority.as_ref())
            .unwrap()
            .unwrap()
            .counts["shared"],
        2
    );

    let one_owner = update_live_route_schema(
        PropertyRouteKind::Node,
        "Person",
        Some(&authority),
        Arc::clone(&inferred),
        &BTreeMap::from([(first.uuid, first.clone())]),
        &[PropertySnapshotRow {
            uuid: first.uuid,
            tombstone: false,
            values: BTreeMap::new(),
        }],
    )
    .unwrap();
    assert_eq!(
        decode_live_schema_summary(one_owner.as_ref())
            .unwrap()
            .unwrap()
            .counts["shared"],
        1
    );
    assert_eq!(
        one_owner.field_with_name("shared").unwrap().data_type(),
        &DataType::Int64
    );

    let no_owner = update_live_route_schema(
        PropertyRouteKind::Node,
        "Person",
        Some(&one_owner),
        inferred,
        &BTreeMap::from([(second.uuid, second.clone())]),
        &[PropertySnapshotRow {
            uuid: second.uuid,
            tombstone: false,
            values: BTreeMap::new(),
        }],
    )
    .unwrap();
    assert!(
        decode_live_schema_summary(no_owner.as_ref())
            .unwrap()
            .unwrap()
            .counts
            .is_empty()
    );
    assert!(no_owner.field_with_name("shared").is_err());

    let malformed = Schema::new_with_metadata(
        vec![Field::new(
            "node_uuid",
            DataType::FixedSizeBinary(16),
            false,
        )],
        HashMap::from([(PROPERTY_LIVE_SCHEMA_KEY.into(), "{}".into())]),
    );
    assert!(decode_live_schema_summary(&malformed).is_err());

    let inconsistent = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("shared", DataType::Int64, true),
        ],
        HashMap::from([
            ("graphforge.entity_type".into(), "Person".into()),
            (
                PROPERTY_LIVE_SCHEMA_KEY.into(),
                encode_live_schema_summary(BTreeMap::from([("other".into(), 1)])).unwrap(),
            ),
        ]),
    ));
    assert!(
        update_live_route_schema(
            PropertyRouteKind::Node,
            "Person",
            Some(&inconsistent),
            Arc::new(Schema::new_with_metadata(
                vec![Field::new(
                    "node_uuid",
                    DataType::FixedSizeBinary(16),
                    false,
                )],
                HashMap::from([("graphforge.entity_type".into(), "Person".into())]),
            )),
            &BTreeMap::from([(second.uuid, second)]),
            &[PropertySnapshotRow {
                uuid: [2; 16],
                tombstone: false,
                values: BTreeMap::new(),
            }],
        )
        .is_err()
    );

    for (uuid_field, route_key) in [
        ("node_uuid", "graphforge.entity_type"),
        ("edge_uuid", "graphforge.rel_type"),
    ] {
        let route_metadata = |summary: Option<BTreeMap<String, u64>>| {
            let mut metadata = HashMap::from([(route_key.to_owned(), "Route".to_owned())]);
            if let Some(counts) = summary {
                metadata.insert(
                    PROPERTY_LIVE_SCHEMA_KEY.to_owned(),
                    encode_live_schema_summary(counts).unwrap(),
                );
            }
            Schema::new_with_metadata(
                vec![
                    Field::new(uuid_field, DataType::FixedSizeBinary(16), false),
                    Field::new("shared", DataType::Int64, true),
                ],
                metadata,
            )
        };
        let legacy = route_metadata(None);
        let exact = route_metadata(Some(BTreeMap::from([("shared".into(), 2)])));
        let missing_after = route_metadata(None);
        assert!(validate_live_schema_sequence(&[(&legacy, 1), (&exact, 1)]).is_ok());
        assert!(
            validate_live_schema_sequence(&[(&exact, 1), (&missing_after, 1)]).is_err(),
            "{uuid_field} summary authority cannot disappear"
        );

        let impossible = route_metadata(Some(BTreeMap::from([("shared".into(), u64::MAX)])));
        assert!(
            validate_live_schema_sequence(&[(&impossible, 2)]).is_err(),
            "{uuid_field} impossible owner count must fail closed"
        );
        let boundary = route_metadata(Some(BTreeMap::from([("shared".into(), 2)])));
        assert!(
            validate_live_schema_sequence(&[(&boundary, 2)]).is_ok(),
            "{uuid_field} exact physical-row boundary is valid"
        );
    }
}

#[test]
fn only_flat_property_inventory_entries_receive_legacy_validation() {
    fn write_legacy_shape(
        root: &Path,
        kind: PropertyRouteKind,
        relative: &str,
    ) -> crate::GraphFileEntry {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new(kind.uuid_field(), DataType::FixedSizeBinary(16), false),
            Field::new("value", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_iter([vec![7; 16]].into_iter()).unwrap()),
                Arc::new(Int64Array::from(vec![Some(1)])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = fs::read(path).unwrap();
        crate::GraphFileEntry {
            relative_path: relative.to_owned(),
            byte_length: u64::try_from(bytes.len()).unwrap(),
            content_sha256: digest_hex(&Sha256::digest(&bytes)),
            role: crate::GraphFileRole::Properties,
        }
    }

    for kind in [PropertyRouteKind::Node, PropertyRouteKind::Edge] {
        let flat = TempDir::new().unwrap();
        let flat_relative = format!("{}/Route.parquet", kind.subdir());
        let flat_entry = write_legacy_shape(flat.path(), kind, &flat_relative);
        let inventory =
            AuthenticatedPropertyInventory::from_entries_at_root(flat.path(), vec![flat_entry])
                .unwrap();
        let (rows, _) = read_authenticated_property_snapshots_for_inventory(
            &inventory,
            kind,
            "Route",
            &BTreeSet::from([[7; 16]]),
        )
        .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "flat {:?} layout remains legacy-compatible",
            kind
        );

        let nested = TempDir::new().unwrap();
        let nested_relative = format!(
            "{}/Route/{}",
            kind.subdir(),
            PropertyFragmentId {
                generation: 0,
                ordinal: 0,
            }
            .file_name()
        );
        let nested_entry = write_legacy_shape(nested.path(), kind, &nested_relative);
        let error =
            AuthenticatedPropertyInventory::from_entries_at_root(nested.path(), vec![nested_entry])
                .unwrap_err();
        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert!(
            error.to_string().contains("metadata conflicts"),
            "canonical {:?} fragment must not bypass metadata validation: {error}",
            kind
        );
    }
}

#[test]
fn property_authentication_reaches_the_lifecycle_counters() {
    let dir = TempDir::new().unwrap();
    let mut table = crate::route_component::RouteTable::default();
    let selected = table.insert("CON", 64 * 1024 * 1024, 100_000).unwrap();
    let relative = format!("properties/{selected}.parquet");
    fs::create_dir_all(dir.path().join("properties")).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            FixedSizeBinaryArray::try_from_iter(vec![vec![7; 16]].into_iter()).unwrap(),
        )],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(dir.path().join(&relative)).unwrap(),
        schema,
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let payload = fs::read(dir.path().join(&relative)).unwrap();
    let table_bytes = table.encode(64 * 1024 * 1024).unwrap();
    fs::write(
        dir.path().join(crate::route_component::TABLE_FILE),
        &table_bytes,
    )
    .unwrap();
    let entry = |relative_path: String, bytes: &[u8], role| crate::GraphFileEntry {
        relative_path,
        byte_length: bytes.len() as u64,
        content_sha256: digest_hex(&Sha256::digest(bytes)),
        role,
    };
    let inventory = crate::graph_files::inventory_from_entries_with_version(
        vec![
            entry(relative.clone(), &payload, crate::GraphFileRole::Properties),
            entry(
                crate::route_component::TABLE_FILE.into(),
                &table_bytes,
                crate::GraphFileRole::Other,
            ),
        ],
        crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION,
    )
    .unwrap();

    // #1449: the streamed SHA-256 over the property payload was counted into
    // the inventory's own metrics but never into the lifecycle phase rows, so
    // a property-bearing open under-reported its verification work.
    let _capture = crate::lifecycle_io::CaptureScope::install();
    let before = crate::lifecycle_io::snapshot();
    let admitted = AuthenticatedPropertyInventory::from_inventory_at_root(
        dir.path(),
        inventory,
        Some((PropertyRouteKind::Node, "CON")),
    )
    .unwrap();
    let region = crate::lifecycle_io::snapshot().since(&before).unwrap();
    region.validate_for_qualification().unwrap();

    let hydration = &region.phases[&crate::StorageIoPhase::HydrationVerification];
    assert_eq!(
        hydration.read_bytes,
        payload.len() as u64,
        "authentication bytes unattributed: {region:#?}"
    );
    assert!(
        hydration.block_count >= 1,
        "authentication blocks unattributed: {region:#?}"
    );
    assert_eq!(
        admitted.open_metrics().property_authentication_bytes,
        payload.len() as u64
    );
}
