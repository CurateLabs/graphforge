use super::super::snapshot_merge::SNAPSHOT_CHARGE_CALLS;
use super::super::*;
use super::*;
use arrow::array::{BooleanArray, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;

#[test]
fn targeted_presence_retains_removed_owner_without_reviving_values() {
    let root = TempDir::new().unwrap();
    let mut entries = Vec::new();
    for generation in [1, 2] {
        let id = PropertyFragmentId {
            generation,
            ordinal: 0,
        };
        let relative = format!("edge_properties/REL/{}", id.file_name());
        let path = root.path().join(&relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                Field::new("value", DataType::Int64, true),
            ],
            HashMap::from([
                (
                    PROPERTY_OVERLAY_FORMAT_KEY.into(),
                    PROPERTY_OVERLAY_FORMAT.into(),
                ),
                (PROPERTY_ROUTE_KEY.into(), "REL".into()),
                (PROPERTY_KIND_KEY.into(), "edge".into()),
                (PROPERTY_GENERATION_KEY.into(), generation.to_string()),
                (PROPERTY_ORDINAL_KEY.into(), "0".into()),
            ]),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([[1; 16], [2; 16]].into_iter()).unwrap(),
                ),
                Arc::new(BooleanArray::from(vec![generation == 2, false])),
                Arc::new(Int64Array::from(vec![(generation == 1).then_some(7), None])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema,
            Some(crate::permanent_parquet::writer_properties().build()),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = fs::read(&path).unwrap();
        entries.push(crate::GraphFileEntry {
            relative_path: relative,
            byte_length: bytes.len() as u64,
            content_sha256: digest_hex(&Sha256::digest(&bytes)),
            role: crate::GraphFileRole::Properties,
        });
    }
    let inventory =
        AuthenticatedPropertyInventory::from_entries_at_root(root.path(), entries.clone()).unwrap();
    let targets = BTreeSet::from([[1; 16], [2; 16], [3; 16]]);
    let (live, live_work) = read_authenticated_property_snapshots_for_inventory(
        &inventory,
        PropertyRouteKind::Edge,
        "REL",
        &targets,
    )
    .unwrap();
    let (present, work) = read_authenticated_property_presence_for_inventory(
        &inventory,
        PropertyRouteKind::Edge,
        "REL",
        &targets,
    )
    .unwrap();
    assert_eq!(live.keys().copied().collect::<Vec<_>>(), vec![[2; 16]]);
    assert_eq!(present, BTreeSet::from([[1; 16], [2; 16]]));
    assert_eq!(
        work, live_work,
        "presence adds no second scan or value decode"
    );
    assert_eq!(work.fragments_considered, 2);
    assert_eq!(work.tombstones, 1);
    let selected = read_authenticated_property_targets_for_inventory(
        &inventory,
        PropertyRouteKind::Edge,
        "REL",
        &targets,
    )
    .unwrap();
    assert_eq!(selected.present, present);
    assert_eq!(selected.rows, live);
    assert_eq!(
        selected.metrics, work,
        "combined values and ownership scan once"
    );
    let schema = Schema::new(vec![
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("value", DataType::Int64, true),
        Field::new("null_only", DataType::Null, true),
    ]);
    let batch = selected.edge_batch(&schema).unwrap();
    assert_eq!(
        batch.num_rows(),
        1,
        "tombstones never revive a property row"
    );
    assert_eq!(batch.column(1).null_count(), 1);
    assert_eq!(batch.column(2).logical_null_count(), 1);
    assert_eq!(batch.schema().as_ref(), &schema);
    drop(inventory);
    // Re-admit a deliberately malformed fixture so this checks row
    // validation, rather than merely failing its prior digest authority.
    let last = entries.last_mut().unwrap();
    let path = root.path().join(&last.relative_path);
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
    let schema = Arc::clone(reader.schema());
    let batch = reader.build().unwrap().next().unwrap().unwrap();
    let malformed = RecordBatch::try_new(
        schema,
        vec![
            Arc::clone(batch.column(0)),
            Arc::clone(batch.column(1)),
            Arc::new(Int64Array::from(vec![Some(9), None])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        malformed.schema(),
        Some(crate::permanent_parquet::writer_properties().build()),
    )
    .unwrap();
    writer.write(&malformed).unwrap();
    writer.close().unwrap();
    let bytes = fs::read(&path).unwrap();
    last.byte_length = bytes.len() as u64;
    last.content_sha256 = digest_hex(&Sha256::digest(&bytes));
    let inventory =
        AuthenticatedPropertyInventory::from_entries_at_root(root.path(), entries).unwrap();
    let error = read_authenticated_property_presence_for_inventory(
        &inventory,
        PropertyRouteKind::Edge,
        "REL",
        &targets,
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
    assert!(
        error
            .to_string()
            .contains("property tombstone carries values")
    );
}

#[test]
fn targeted_reader_validates_unselected_rows_before_value_pruning() {
    for (uuids, tombstones, names, expected) in [
        (
            vec![vec![2; 16], vec![1; 16]],
            vec![false, false],
            vec![Some("target"), Some("out-of-order")],
            "strictly sorted",
        ),
        (
            vec![vec![1; 16], vec![2; 16]],
            vec![false, true],
            vec![Some("target"), Some("forbidden")],
            "tombstone carries values",
        ),
    ] {
        let dir = TempDir::new().unwrap();
        let route_dir = dir.path().join("properties/Person");
        fs::create_dir_all(&route_dir).unwrap();
        let id = PropertyFragmentId {
            generation: 1,
            ordinal: 0,
        };
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                Field::new("name", DataType::Utf8, true),
            ],
            HashMap::from([
                (
                    PROPERTY_OVERLAY_FORMAT_KEY.into(),
                    PROPERTY_OVERLAY_FORMAT.into(),
                ),
                (PROPERTY_ROUTE_KEY.into(), "Person".into()),
                (PROPERTY_KIND_KEY.into(), "node".into()),
                (PROPERTY_GENERATION_KEY.into(), "1".into()),
                (PROPERTY_ORDINAL_KEY.into(), "0".into()),
            ]),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_iter(uuids.into_iter()).unwrap()),
                Arc::new(BooleanArray::from(tombstones)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(route_dir.join(id.file_name())).unwrap(),
            schema,
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let error = read_authenticated_property_snapshots_for(
            dir.path(),
            PropertyRouteKind::Node,
            "Person",
            &BTreeSet::from([[1; 16]]),
        )
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn targeted_reader_validates_older_generations_after_target_resolves() {
    let dir = TempDir::new().unwrap();
    let route_dir = dir.path().join("properties/Person");
    fs::create_dir_all(&route_dir).unwrap();
    for (generation, uuids) in [(1, vec![[2; 16], [1; 16]]), (2, vec![[9; 16]])] {
        let id = PropertyFragmentId {
            generation,
            ordinal: 0,
        };
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                Field::new("name", DataType::Utf8, true),
            ],
            HashMap::from([
                (
                    PROPERTY_OVERLAY_FORMAT_KEY.into(),
                    PROPERTY_OVERLAY_FORMAT.into(),
                ),
                (PROPERTY_ROUTE_KEY.into(), "Person".into()),
                (PROPERTY_KIND_KEY.into(), "node".into()),
                (PROPERTY_GENERATION_KEY.into(), generation.to_string()),
                (PROPERTY_ORDINAL_KEY.into(), "0".into()),
            ]),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(uuids.iter().map(|uuid| uuid.to_vec()))
                        .unwrap(),
                ),
                Arc::new(BooleanArray::from(vec![false; uuids.len()])),
                Arc::new(StringArray::from(vec![Some("value"); uuids.len()])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(route_dir.join(id.file_name())).unwrap(),
            schema,
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    let error = read_authenticated_property_snapshots_for(
        dir.path(),
        PropertyRouteKind::Node,
        "Person",
        &BTreeSet::from([[9; 16]]),
    )
    .unwrap_err();
    assert!(error.to_string().contains("strictly sorted"), "{error}");
}

#[test]
fn targeted_reader_n_2n_4n_has_bounded_retention_and_exact_work() {
    let mut prior_bytes = 0;
    let mut byte_deltas = Vec::new();
    let mut prior_peak = None;
    for rows in [128_usize, 256, 512] {
        let dir = TempDir::new().unwrap();
        let route_dir = dir.path().join("properties/Person");
        fs::create_dir_all(&route_dir).unwrap();
        let id = PropertyFragmentId {
            generation: 1,
            ordinal: 0,
        };
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                Field::new("name", DataType::Utf8, true),
            ],
            HashMap::from([
                (
                    PROPERTY_OVERLAY_FORMAT_KEY.into(),
                    PROPERTY_OVERLAY_FORMAT.into(),
                ),
                (PROPERTY_ROUTE_KEY.into(), "Person".into()),
                (PROPERTY_KIND_KEY.into(), "node".into()),
                (PROPERTY_GENERATION_KEY.into(), "1".into()),
                (PROPERTY_ORDINAL_KEY.into(), "0".into()),
            ]),
        ));
        let uuids = (0..rows)
            .map(|value| {
                let mut uuid = [0_u8; 16];
                uuid[14..].copy_from_slice(&u16::try_from(value).unwrap().to_be_bytes());
                uuid
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(uuids.iter().map(|uuid| uuid.to_vec()))
                        .unwrap(),
                ),
                Arc::new(BooleanArray::from(vec![false; rows])),
                Arc::new(StringArray::from(vec![Some("value"); rows])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(route_dir.join(id.file_name())).unwrap(),
            schema,
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let expected_authentication_bytes =
            fs::metadata(route_dir.join(id.file_name())).unwrap().len();

        let (found, metrics) = read_authenticated_property_snapshots_for(
            dir.path(),
            PropertyRouteKind::Node,
            "Person",
            &BTreeSet::from([uuids[0]]),
        )
        .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(metrics.physical_rows, u64::try_from(rows * 2).unwrap());
        assert_eq!(metrics.fragments_considered, 1);
        // Raw graph-tree adapters capture/hash the authority, admission
        // authenticates the fragment, and bounded reopening authenticates
        // before plus verifies after decoding.
        assert_eq!(
            metrics.authentication_bytes,
            expected_authentication_bytes * 3
        );
        assert_eq!(
            metrics.authentication_block_equivalents,
            expected_authentication_bytes.div_ceil(64 * 1024) * 3
        );
        assert_eq!(metrics.row_groups_considered, 1);
        assert_eq!(metrics.row_groups_selected, 1);
        assert_eq!(metrics.decoder_peak_rows, 2);
        assert_eq!(metrics.peak_buffered_rows, 2);
        assert!(metrics.peak_buffered_bytes <= PropertyOverlayLimits::default().max_buffered_bytes);
        if let Some(prior) = prior_peak {
            assert!(metrics.peak_buffered_bytes <= prior + 64 * 1024);
        }
        prior_peak = Some(metrics.peak_buffered_bytes);
        assert!(metrics.physical_bytes > prior_bytes);
        assert_eq!(
            metrics.physical_bytes,
            metrics.authentication_bytes + metrics.validation_bytes + metrics.selected_value_bytes
        );
        assert!(metrics.read_calls > 0);
        assert_eq!(
            metrics.physical_blocks,
            metrics.authentication_read_calls
                + metrics.validation_read_calls
                + metrics.selected_value_read_calls
        );
        assert!(metrics.validation_bytes > 0);
        assert!(metrics.selected_value_bytes > 0);
        if prior_bytes != 0 {
            byte_deltas.push(metrics.physical_bytes - prior_bytes);
            assert!(
                metrics.physical_bytes <= prior_bytes.saturating_mul(2).saturating_add(16_384),
                "doubling rows must remain linear within fixed Parquet metadata tolerance"
            );
        }
        assert_eq!(metrics.per_record_seeks, 0);
        prior_bytes = metrics.physical_bytes;

        let inventory = authenticated_property_inventory_for_route(
            dir.path(),
            PropertyRouteKind::Node,
            "Person",
        )
        .unwrap();
        for targets in [BTreeSet::from([uuids[0]]), uuids.iter().copied().collect()] {
            SNAPSHOT_CHARGE_CALLS.with(|calls| calls.set(Some(0)));
            let selected = read_authenticated_property_targets_for_inventory(
                &inventory,
                PropertyRouteKind::Node,
                "Person",
                &targets,
            )
            .unwrap();
            let charge_calls = SNAPSHOT_CHARGE_CALLS.with(|calls| calls.replace(None).unwrap());
            assert_eq!(selected.present, targets);
            assert_eq!(
                selected.rows.keys().copied().collect::<BTreeSet<_>>(),
                targets
            );
            assert_eq!(selected.metrics.physical_rows, (rows * 2) as u64);
            assert_eq!(selected.metrics.decoder_peak_rows, 2);
            // Each decoded row is charged for its individual limit, live
            // decode admission, and peak evidence. Each retained target is
            // charged when inserted and once by the independent test-only
            // final counter audit. Rescanning all retained rows for every
            // two-row batch exceeds this linear work ceiling.
            assert!(
                charge_calls <= 3 * rows + 2 * targets.len(),
                "rows={rows} targets={} charge_calls={charge_calls}",
                targets.len()
            );
            assert!(charge_calls >= rows, "counter must observe real row work");
        }
    }
    assert_eq!(byte_deltas.len(), 2);
    assert!(
        byte_deltas[1] <= byte_deltas[0].saturating_mul(2).saturating_add(8_192),
        "first differences must reject superlinear repeated reads"
    );
}
