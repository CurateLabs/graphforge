use super::super::inventory::resolve_v1_property_entries_for_route;
use super::super::*;
use super::*;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;

#[test]
fn authenticated_reader_reports_actual_io_and_decodes_snapshot() {
    let dir = TempDir::new().unwrap();
    let scratch = TempDir::new().unwrap();
    let id = PropertyFragmentId {
        generation: 7,
        ordinal: 0,
    };
    let route_dir = dir.path().join("properties/Person");
    fs::create_dir_all(&route_dir).unwrap();
    let metadata = HashMap::from([
        (
            PROPERTY_OVERLAY_FORMAT_KEY.into(),
            PROPERTY_OVERLAY_FORMAT.into(),
        ),
        (PROPERTY_ROUTE_KEY.into(), "Person".into()),
        (PROPERTY_KIND_KEY.into(), "node".into()),
        (PROPERTY_GENERATION_KEY.into(), "7".into()),
        (PROPERTY_ORDINAL_KEY.into(), "0".into()),
    ]);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
            Field::new("name", DataType::Utf8, true),
        ],
        metadata,
    ));
    let large_name = "A".repeat(100_000);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![4; 16]].into_iter()).unwrap())
                as ArrayRef,
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some(large_name.as_str())])) as ArrayRef,
        ],
    )
    .unwrap();
    let path = route_dir.join(id.file_name());
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut rows = Vec::new();
    let metrics = visit_authenticated_property_snapshots(
        dir.path(),
        PropertyRouteKind::Node,
        "Person",
        scratch.path(),
        PropertyOverlayLimits::default(),
        |row| {
            rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values.get("name"),
        Some(&IrLiteral::Str(large_name))
    );
    assert_eq!(metrics.physical_rows, 1);
    assert!(metrics.physical_bytes > 0);
    assert!(metrics.read_calls > 0);
    assert!(metrics.decoder_peak_bytes > 100_000);
    assert!(metrics.peak_buffered_bytes >= metrics.decoder_peak_bytes);
    assert_eq!(metrics.per_record_seeks, 0);
    let error = visit_authenticated_property_snapshots(
        dir.path(),
        PropertyRouteKind::Node,
        "Person",
        scratch.path(),
        PropertyOverlayLimits {
            max_buffered_rows: 8,
            max_open_runs: 2,
            max_buffered_bytes: 1024,
            max_row_bytes: 512,
        },
        |_| Ok(()),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("pre-decode byte admission"),
        "{error}"
    );

    let bytes = fs::read(&path).unwrap();
    let entry = crate::GraphFileEntry {
        relative_path: format!("properties/Person/{}", id.file_name()),
        byte_length: u64::try_from(bytes.len()).unwrap(),
        content_sha256: digest_hex(&Sha256::digest(&bytes)),
        role: crate::GraphFileRole::Properties,
    };
    let missing_unrelated = crate::GraphFileEntry {
        relative_path: format!("properties/Unrelated/{}", id.file_name()),
        byte_length: u64::MAX,
        content_sha256: "00".repeat(32),
        role: crate::GraphFileRole::Properties,
    };
    let resolved = resolve_v1_property_entries_for_route(
        dir.path(),
        vec![entry.clone(), missing_unrelated.clone()],
        Some((PropertyRouteKind::Node, "Person")),
    )
    .unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].0.relative_path, entry.relative_path);
    assert!(
        resolve_v1_property_entries_for_route(
            dir.path(),
            vec![entry.clone(), missing_unrelated],
            None,
        )
        .is_err(),
        "full V1 admission must continue authenticating unrelated graph payloads"
    );
    let inventory = Arc::new(
        AuthenticatedPropertyInventory::from_entries_at_root(dir.path(), vec![entry.clone()])
            .unwrap(),
    );
    let mut inventory_rows = Vec::new();
    inventory
        .visit_route(
            PropertyRouteKind::Node,
            "Person",
            scratch.path(),
            PropertyOverlayLimits::default(),
            |row| {
                inventory_rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(inventory_rows.len(), 1);
    // Schema discovery and every targeted mutation baseline reuse this one
    // admitted inventory; neither operation resolves CURRENT nor captures
    // the project tree again.
    assert!(
        inventory
            .route_schema(PropertyRouteKind::Node, "Person")
            .is_some()
    );
    let targets = BTreeSet::from([[4; 16]]);
    let open_metrics = inventory.open_metrics();
    assert!(open_metrics.authentication_bytes > 0);
    let mut repeated_scan_metrics = Vec::new();
    for _ in 0..2 {
        let (rows, metrics) = read_authenticated_property_snapshots_for_inventory(
            &inventory,
            PropertyRouteKind::Node,
            "Person",
            &targets,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(metrics.per_record_seeks, 0);
        assert_eq!(metrics.authentication_bytes, entry.byte_length);
        assert!(metrics.authentication_read_calls > 0);
        assert_eq!(
            metrics.authentication_block_equivalents,
            entry.byte_length.div_ceil(64 * 1024)
        );
        assert_eq!(
            metrics.physical_bytes,
            metrics.authentication_bytes + metrics.validation_bytes + metrics.selected_value_bytes
        );
        repeated_scan_metrics.push(metrics);
    }
    assert_eq!(repeated_scan_metrics[0], repeated_scan_metrics[1]);
    let concurrent = (0..8)
        .map(|_| {
            let inventory = Arc::clone(&inventory);
            std::thread::spawn(move || {
                let scratch = TempDir::new().unwrap();
                for _ in 0..8 {
                    let mut rows = Vec::new();
                    inventory
                        .visit_route(
                            PropertyRouteKind::Node,
                            "Person",
                            scratch.path(),
                            PropertyOverlayLimits::default(),
                            |row| {
                                rows.push(row);
                                Ok(())
                            },
                        )
                        .unwrap();
                    assert_eq!(rows.len(), 1);
                }
            })
        })
        .collect::<Vec<_>>();
    for thread in concurrent {
        thread.join().unwrap();
    }

    let unrelated_dir = dir.path().join("properties/Unrelated");
    fs::create_dir_all(&unrelated_dir).unwrap();
    let unrelated_path = unrelated_dir.join(id.file_name());
    let unrelated_bytes = b"authenticated but deliberately not parquet";
    fs::write(&unrelated_path, unrelated_bytes).unwrap();
    let unrelated_entry = crate::GraphFileEntry {
        relative_path: format!("properties/Unrelated/{}", id.file_name()),
        byte_length: u64::try_from(unrelated_bytes.len()).unwrap(),
        content_sha256: digest_hex(&Sha256::digest(unrelated_bytes)),
        role: crate::GraphFileRole::Properties,
    };
    let selected = AuthenticatedPropertyInventory::from_entries_at_root_for_route(
        dir.path(),
        vec![entry.clone(), unrelated_entry],
        PropertyRouteKind::Node,
        "Person",
    )
    .unwrap();
    let selected_metrics = selected
        .visit_route(
            PropertyRouteKind::Node,
            "Person",
            scratch.path(),
            PropertyOverlayLimits::default(),
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(
        selected.open_metrics().authentication_bytes,
        entry.byte_length
    );
    assert_eq!(selected_metrics.authentication_bytes, entry.byte_length);
    assert!(selected_metrics.authentication_read_calls > 0);
    assert_eq!(
        selected_metrics.authentication_block_equivalents,
        entry.byte_length.div_ceil(64 * 1024)
    );
    assert_eq!(
        selected_metrics.authenticated_snapshot_peak_bytes,
        entry.byte_length
    );
    assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);

    #[cfg(unix)]
    {
        let linked_scratch = dir.path().join("linked-scratch");
        std::os::unix::fs::symlink(scratch.path(), &linked_scratch).unwrap();
        let error = selected
            .visit_route(
                PropertyRouteKind::Node,
                "Person",
                &linked_scratch,
                PropertyOverlayLimits::default(),
                |_| Ok(()),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("linked") || error.to_string().contains("Not a directory"),
            "{error}"
        );
    }

    let conflicting_id = PropertyFragmentId {
        generation: 8,
        ordinal: 0,
    };
    let conflicting_schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
            Field::new("name", DataType::Int64, true),
            Field::new("extra", DataType::Int64, true),
        ],
        HashMap::from([
            (
                PROPERTY_OVERLAY_FORMAT_KEY.into(),
                PROPERTY_OVERLAY_FORMAT.into(),
            ),
            (PROPERTY_ROUTE_KEY.into(), "Person".into()),
            (PROPERTY_KIND_KEY.into(), "node".into()),
            (PROPERTY_GENERATION_KEY.into(), "8".into()),
            (PROPERTY_ORDINAL_KEY.into(), "0".into()),
        ]),
    ));
    let conflicting_batch = RecordBatch::try_new(
        Arc::clone(&conflicting_schema),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![5; 16]].into_iter()).unwrap()),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(Int64Array::from(vec![Some(9)])),
            Arc::new(Int64Array::from(vec![Some(42)])),
        ],
    )
    .unwrap();
    let conflicting_path = route_dir.join(conflicting_id.file_name());
    let mut conflicting_writer = ArrowWriter::try_new(
        File::create(&conflicting_path).unwrap(),
        conflicting_schema,
        None,
    )
    .unwrap();
    conflicting_writer.write(&conflicting_batch).unwrap();
    conflicting_writer.close().unwrap();
    let conflicting_bytes = fs::read(&conflicting_path).unwrap();
    let conflicting_entry = crate::GraphFileEntry {
        relative_path: format!("properties/Person/{}", conflicting_id.file_name()),
        byte_length: u64::try_from(conflicting_bytes.len()).unwrap(),
        content_sha256: digest_hex(&Sha256::digest(&conflicting_bytes)),
        role: crate::GraphFileRole::Properties,
    };
    let tombstone_id = PropertyFragmentId {
        generation: 8,
        ordinal: 1,
    };
    let tombstone_schema = Arc::new(Schema::new_with_metadata(
        conflicting_batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
        HashMap::from([
            (
                PROPERTY_OVERLAY_FORMAT_KEY.into(),
                PROPERTY_OVERLAY_FORMAT.into(),
            ),
            (PROPERTY_ROUTE_KEY.into(), "Person".into()),
            (PROPERTY_KIND_KEY.into(), "node".into()),
            (PROPERTY_GENERATION_KEY.into(), "8".into()),
            (PROPERTY_ORDINAL_KEY.into(), "1".into()),
        ]),
    ));
    let tombstone_batch = RecordBatch::try_new(
        Arc::clone(&tombstone_schema),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![6; 16]].into_iter()).unwrap()),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Int64Array::from(vec![None])),
            Arc::new(Int64Array::from(vec![None])),
        ],
    )
    .unwrap();
    let tombstone_path = route_dir.join(tombstone_id.file_name());
    let mut tombstone_writer = ArrowWriter::try_new(
        File::create(&tombstone_path).unwrap(),
        tombstone_schema,
        None,
    )
    .unwrap();
    tombstone_writer.write(&tombstone_batch).unwrap();
    tombstone_writer.close().unwrap();
    let tombstone_bytes = fs::read(&tombstone_path).unwrap();
    let tombstone_entry = crate::GraphFileEntry {
        relative_path: format!("properties/Person/{}", tombstone_id.file_name()),
        byte_length: u64::try_from(tombstone_bytes.len()).unwrap(),
        content_sha256: digest_hex(&Sha256::digest(&tombstone_bytes)),
        role: crate::GraphFileRole::Properties,
    };
    let evolved = AuthenticatedPropertyInventory::from_entries_at_root(
        dir.path(),
        vec![entry, conflicting_entry, tombstone_entry],
    );
    let evolved = evolved.unwrap();
    assert_eq!(
        evolved
            .route_schema(PropertyRouteKind::Node, "Person")
            .unwrap()
            .field_with_name("name")
            .unwrap()
            .data_type(),
        &DataType::Struct(crate::writer::heterogeneous_scalar_fields())
    );
    let mut evolved_rows = Vec::new();
    let evolved_metrics = evolved
        .visit_route(
            PropertyRouteKind::Node,
            "Person",
            scratch.path(),
            PropertyOverlayLimits::default(),
            |row| {
                evolved_rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        evolved_rows.iter().map(|row| row.uuid).collect::<Vec<_>>(),
        vec![[4; 16], [5; 16]]
    );
    assert_eq!(evolved_metrics.fragments_considered, 3);
    assert_eq!(evolved_metrics.physical_rows, 3);
    assert_eq!(evolved_metrics.tombstones, 1);
    assert_eq!(
        evolved.route_row_upper_bound(PropertyRouteKind::Node, "Person"),
        3
    );
    let (targeted, targeted_metrics) = read_authenticated_property_snapshots_for_inventory(
        &evolved,
        PropertyRouteKind::Node,
        "Person",
        &BTreeSet::from([[4; 16], [5; 16], [6; 16]]),
    )
    .unwrap();
    assert_eq!(
        targeted.keys().copied().collect::<Vec<_>>(),
        vec![[4; 16], [5; 16]]
    );
    assert_eq!(targeted_metrics.fragments_considered, 3);

    let context = SessionContext::new();
    context
        .register_table(
            "evolved",
            Arc::new(crate::catalog::PropertyTable::open_authenticated(
                dir.path(),
                "Person",
                Arc::new(evolved),
            )),
        )
        .unwrap();
    let projected = tokio::runtime::Runtime::new().unwrap().block_on(async {
        context
            .sql("SELECT extra, name, node_uuid FROM evolved ORDER BY node_uuid")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    });
    let projected = arrow::compute::concat_batches(&projected[0].schema(), &projected).unwrap();
    assert_eq!(projected.num_rows(), 2);
    let extra = projected
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(extra.is_null(0));
    assert_eq!(extra.value(1), 42);
    assert_eq!(
        projected.schema().field(1).data_type(),
        &DataType::Struct(crate::writer::heterogeneous_scalar_fields())
    );
    let ids = projected
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(ids.value(0), &[4; 16]);
    assert_eq!(ids.value(1), &[5; 16]);

    fs::write(&path, b"same-name attacker replacement").unwrap();
    assert!(
        inventory
            .visit_route(
                PropertyRouteKind::Node,
                "Person",
                scratch.path(),
                PropertyOverlayLimits::default(),
                |_| Ok(()),
            )
            .is_err()
    );
}

#[test]
fn many_fragment_inventory_bounds_all_live_handles_without_rlimit_assumptions() {
    let dir = TempDir::new().unwrap();
    let scratch = TempDir::new().unwrap();
    let route_dir = dir.path().join("properties/Person");
    fs::create_dir_all(&route_dir).unwrap();
    let mut entries = Vec::new();
    for ordinal in 0_u64..96 {
        let id = PropertyFragmentId {
            generation: 1,
            ordinal,
        };
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                Field::new("value", DataType::Int64, true),
            ],
            HashMap::from([
                (
                    PROPERTY_OVERLAY_FORMAT_KEY.into(),
                    PROPERTY_OVERLAY_FORMAT.into(),
                ),
                (PROPERTY_ROUTE_KEY.into(), "Person".into()),
                (PROPERTY_KIND_KEY.into(), "node".into()),
                (PROPERTY_GENERATION_KEY.into(), "1".into()),
                (PROPERTY_ORDINAL_KEY.into(), ordinal.to_string()),
            ]),
        ));
        let mut uuid = [0_u8; 16];
        uuid[8..].copy_from_slice(&ordinal.to_be_bytes());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_iter([uuid].into_iter()).unwrap()),
                Arc::new(BooleanArray::from(vec![false])),
                Arc::new(Int64Array::from(vec![Some(
                    i64::try_from(ordinal).unwrap(),
                )])),
            ],
        )
        .unwrap();
        let path = route_dir.join(id.file_name());
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = fs::read(&path).unwrap();
        entries.push(crate::GraphFileEntry {
            relative_path: format!("properties/Person/{}", id.file_name()),
            byte_length: u64::try_from(bytes.len()).unwrap(),
            content_sha256: digest_hex(&Sha256::digest(&bytes)),
            role: crate::GraphFileRole::Properties,
        });
    }

    let inventory =
        AuthenticatedPropertyInventory::from_entries_at_root(dir.path(), entries).unwrap();
    assert_eq!(inventory.live_fragment_handles(), 0);
    assert_eq!(inventory.peak_fragment_handles(), 1);

    inventory.reset_peak_fragment_handles();
    let limits = PropertyOverlayLimits {
        max_open_runs: 2,
        ..PropertyOverlayLimits::default()
    };
    let mut rows = 0_usize;
    let metrics = inventory
        .visit_route(
            PropertyRouteKind::Node,
            "Person",
            scratch.path(),
            limits,
            |_| {
                rows += 1;
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(rows, 96);
    assert!(metrics.authentication_bytes > 0);
    assert_eq!(inventory.live_fragment_handles(), 0);
    assert!(inventory.peak_fragment_handles() <= 2);

    inventory.reset_peak_fragment_handles();
    let targets = BTreeSet::from([[0; 16]]);
    let _ = read_authenticated_property_snapshots_for_inventory(
        &inventory,
        PropertyRouteKind::Node,
        "Person",
        &targets,
    )
    .unwrap();
    assert_eq!(inventory.live_fragment_handles(), 0);
    assert!(inventory.peak_fragment_handles() <= 1);

    let attacked = route_dir.join(
        PropertyFragmentId {
            generation: 1,
            ordinal: 0,
        }
        .file_name(),
    );
    let displaced = route_dir.join("displaced.parquet");
    fs::rename(&attacked, &displaced).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&displaced, &attacked).unwrap();
    #[cfg(windows)]
    fs::copy(&displaced, &attacked).unwrap();
    let error = inventory
        .visit_route(
            PropertyRouteKind::Node,
            "Person",
            scratch.path(),
            limits,
            |_| Ok(()),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("linked")
            || error.to_string().contains("symbolic links")
            || error.to_string().contains("identity changed"),
        "{error}"
    );
    assert_eq!(inventory.live_fragment_handles(), 0);
}

#[tokio::test]
async fn late_authenticated_decoder_failure_emits_nothing_direct_or_through_limit() {
    let dir = TempDir::new().unwrap();
    let scratch = TempDir::new().unwrap();
    let id = PropertyFragmentId {
        generation: 1,
        ordinal: 0,
    };
    let route_dir = dir.path().join("properties/Person");
    fs::create_dir_all(&route_dir).unwrap();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
            Field::new("value", DataType::Utf8, true),
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
    let first_value = "x".repeat(70 * 1024);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([vec![1; 16], vec![2; 16]].into_iter())
                    .unwrap(),
            ),
            Arc::new(BooleanArray::from(vec![false, false])),
            Arc::new(StringArray::from(vec![
                Some(first_value.as_str()),
                Some("authenticated-two"),
            ])),
        ],
    )
    .unwrap();
    let path = route_dir.join(id.file_name());
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .set_dictionary_enabled(false)
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = fs::read(&path).unwrap();
    let inventory = Arc::new(
        AuthenticatedPropertyInventory::from_entries_at_root(
            dir.path(),
            vec![crate::GraphFileEntry {
                relative_path: format!("properties/Person/{}", id.file_name()),
                byte_length: u64::try_from(bytes.len()).unwrap(),
                content_sha256: digest_hex(&Sha256::digest(&bytes)),
                role: crate::GraphFileRole::Properties,
            }],
        )
        .unwrap(),
    );

    inventory.fail_decoder_on_row(2);
    let emitted = std::cell::Cell::new(0_usize);
    let error = inventory
        .visit_route(
            PropertyRouteKind::Node,
            "Person",
            scratch.path(),
            PropertyOverlayLimits::default(),
            |_| {
                emitted.set(emitted.get() + 1);
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(emitted.get(), 0);
    assert!(error.to_string().contains("injected late authenticated"));

    inventory.fail_decoder_on_row(2);
    let config = SessionConfig::new().with_batch_size(1);
    let context = SessionContext::new_with_config(config);
    context
        .register_table(
            "props",
            Arc::new(crate::catalog::PropertyTable::open_authenticated(
                dir.path(),
                "Person",
                Arc::clone(&inventory),
            )),
        )
        .unwrap();
    let error = context
        .sql("SELECT value FROM props LIMIT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected late authenticated"));

    inventory.fail_decoder_on_row(2);
    let error = context
        .sql("SELECT node_uuid FROM props LIMIT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected late authenticated"));

    let tampered_bytes = || {
        let mut tampered = bytes.clone();
        let needle = b"authenticated-two";
        let replacement = b"tampered-value-02";
        assert_eq!(needle.len(), replacement.len());
        let offset = tampered
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("uncompressed authenticated value is present");
        tampered[offset..offset + needle.len()].copy_from_slice(replacement);
        tampered
    };

    let barrier = inventory.arm_mutation_after_authentication();
    let direct_inventory = Arc::clone(&inventory);
    let direct = std::thread::spawn(move || {
        let scratch = TempDir::new().unwrap();
        let emitted = Arc::new(AtomicU64::new(0));
        let observed = Arc::clone(&emitted);
        let result = direct_inventory.visit_route(
            PropertyRouteKind::Node,
            "Person",
            scratch.path(),
            PropertyOverlayLimits::default(),
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        );
        (result, emitted.load(Ordering::SeqCst))
    });
    barrier.authenticated.wait();
    fs::write(&path, tampered_bytes()).unwrap();
    barrier.proceed.wait();
    barrier.copied.wait();
    fs::write(&path, &bytes).unwrap();
    barrier.restored.wait();
    let (result, emitted) = direct.join().unwrap();
    let error = result.unwrap_err();
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
    assert_eq!(emitted, 0);

    fs::write(&path, &bytes).unwrap();
    let barrier = inventory.arm_mutation_after_authentication();
    let targeted_inventory = Arc::clone(&inventory);
    let targeted = std::thread::spawn(move || {
        read_authenticated_property_snapshots_for_inventory(
            &targeted_inventory,
            PropertyRouteKind::Node,
            "Person",
            &BTreeSet::from([[1; 16]]),
        )
    });
    barrier.authenticated.wait();
    fs::write(&path, tampered_bytes()).unwrap();
    barrier.proceed.wait();
    barrier.copied.wait();
    fs::write(&path, &bytes).unwrap();
    barrier.restored.wait();
    let error = targeted.join().unwrap().unwrap_err();
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT");

    fs::write(&path, &bytes).unwrap();
    let barrier = inventory.arm_mutation_after_authentication();
    let limit_context = context.clone();
    let limited = tokio::spawn(async move {
        limit_context
            .sql("SELECT value FROM props LIMIT 1")
            .await
            .unwrap()
            .collect()
            .await
    });
    let mutation_path = path.clone();
    let mutation_bytes = tampered_bytes();
    let original_bytes = bytes.clone();
    tokio::task::spawn_blocking(move || {
        barrier.authenticated.wait();
        fs::write(&mutation_path, mutation_bytes).unwrap();
        barrier.proceed.wait();
        barrier.copied.wait();
        fs::write(mutation_path, original_bytes).unwrap();
        barrier.restored.wait();
    })
    .await
    .unwrap();
    let error = limited.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("GF_PROJECT_CORRUPT"), "{error}");
}

#[test]
fn projected_overlay_decodes_only_selected_values_and_mandatory_keys() {
    let root = TempDir::new().unwrap();
    let scratch = TempDir::new().unwrap();
    let id = PropertyFragmentId {
        generation: 1,
        ordinal: 0,
    };
    let route_dir = root.path().join("edge_properties/KNOWS");
    fs::create_dir_all(&route_dir).unwrap();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
            Field::new("keep", DataType::Utf8, true),
            Field::new("unused", DataType::Utf8, true),
        ],
        HashMap::from([
            (
                PROPERTY_OVERLAY_FORMAT_KEY.into(),
                PROPERTY_OVERLAY_FORMAT.into(),
            ),
            (PROPERTY_ROUTE_KEY.into(), "KNOWS".into()),
            (PROPERTY_KIND_KEY.into(), "edge".into()),
            (PROPERTY_GENERATION_KEY.into(), "1".into()),
            (PROPERTY_ORDINAL_KEY.into(), "0".into()),
        ]),
    ));
    let rows = 128;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    (0..rows).map(|row| vec![u8::try_from(row + 1).unwrap(); 16]),
                )
                .unwrap(),
            ),
            Arc::new(BooleanArray::from(vec![false; rows])),
            Arc::new(StringArray::from(vec![Some("kept"); rows])),
            Arc::new(StringArray::from(vec![Some("x".repeat(8_192)); rows])),
        ],
    )
    .unwrap();
    let path = route_dir.join(id.file_name());
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = fs::read(&path).unwrap();
    let inventory = AuthenticatedPropertyInventory::from_entries_at_root(
        root.path(),
        vec![crate::GraphFileEntry {
            relative_path: format!("edge_properties/KNOWS/{}", id.file_name()),
            byte_length: u64::try_from(bytes.len()).unwrap(),
            content_sha256: digest_hex(&Sha256::digest(&bytes)),
            role: crate::GraphFileRole::Properties,
        }],
    )
    .unwrap();

    let mut full_rows = Vec::new();
    let full = inventory
        .visit_route(
            PropertyRouteKind::Edge,
            "KNOWS",
            scratch.path(),
            PropertyOverlayLimits::default(),
            |row| {
                full_rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    let mut projected_rows = Vec::new();
    let projected = inventory
        .visit_route_projected(
            PropertyRouteKind::Edge,
            "KNOWS",
            scratch.path(),
            PropertyOverlayLimits::default(),
            Some(&BTreeSet::from(["keep".to_owned()])),
            |row| {
                projected_rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(projected_rows.len(), full_rows.len());
    assert!(
        projected_rows
            .iter()
            .all(|row| { row.values.contains_key("keep") && !row.values.contains_key("unused") })
    );
    assert!(projected.validation_bytes < full.validation_bytes);
    assert_eq!(projected.per_record_seeks, 0);

    // Cross the actual DataFusion scan boundary: direct-reader tests alone
    // cannot detect accidentally restoring unprojected scan materialization.
    let inventory = Arc::new(inventory);
    let schema = inventory
        .route_schema(PropertyRouteKind::Edge, "KNOWS")
        .unwrap();
    let keep = schema.index_of("keep").unwrap();
    let uuid = schema.index_of("edge_uuid").unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut full_spill = None;
    for projection in [None, Some(vec![keep, uuid, keep]), Some(vec![])] {
        let expected_schema = projection.as_ref().map_or_else(
            || schema.as_ref().clone(),
            |indices| schema.project(indices).unwrap(),
        );
        let plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> = Arc::new(
            crate::property_scan::PropertyOverlayExec::try_new(
                root.path().to_path_buf(),
                Some(Arc::clone(&inventory)),
                "KNOWS".into(),
                true,
                Arc::clone(&schema),
                crate::property_scan::PropertyScanOptions {
                    projection: projection.as_ref(),
                    limit: None,
                    batch_size: 17,
                },
            )
            .unwrap(),
        );
        let output = runtime
            .block_on(datafusion::physical_plan::collect(
                Arc::clone(&plan),
                Arc::new(datafusion::execution::TaskContext::default()),
            ))
            .unwrap();
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            rows
        );
        for batch in &output {
            assert_eq!(batch.schema().as_ref(), &expected_schema);
            if projection
                .as_ref()
                .is_some_and(|indices| !indices.is_empty())
            {
                assert_eq!(batch.column(0), batch.column(2));
                let kept = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert!(kept.iter().all(|value| value == Some("kept")));
            }
        }
        if projection
            .as_ref()
            .is_none_or(|indices| !indices.is_empty())
        {
            let identity_column = if projection.is_none() { uuid } else { 1 };
            let actual = output
                .iter()
                .flat_map(|batch| {
                    let ids = batch
                        .column(identity_column)
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap();
                    (0..batch.num_rows())
                        .map(|row| ids.value(row).to_vec())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                (1..=rows)
                    .map(|row| vec![u8::try_from(row).unwrap(); 16])
                    .collect::<Vec<_>>()
            );
        }
        let metrics = plan.metrics().unwrap();
        let value = |name| metrics.sum_by_name(name).unwrap().as_usize();
        assert_eq!(value("property_physical_rows"), rows);
        assert_eq!(value("property_authentication_bytes"), bytes.len());
        let spill = value("property_spill_bytes");
        if projection.is_none() {
            full_spill = Some(spill);
        } else {
            assert!(spill <= rows * 256);
            assert!(full_spill.unwrap() - spill >= rows * 8192);
            assert!(value("property_decoder_peak_bytes") <= rows * 256);
        }
    }
}

#[test]
fn hostile_authenticated_property_matrix_fails_closed_before_projection_or_limit() {
    fn write_fragment(
        root: &Path,
        kind: PropertyRouteKind,
        route: &str,
        id: PropertyFragmentId,
        uuid_field: Field,
        uuid: ArrayRef,
        property_field: Field,
        property: ArrayRef,
        extra_metadata: impl IntoIterator<Item = (String, String)>,
    ) -> PathBuf {
        let route_dir = root.join(kind.subdir()).join(route);
        fs::create_dir_all(&route_dir).unwrap();
        let mut metadata = HashMap::from([
            (
                PROPERTY_OVERLAY_FORMAT_KEY.into(),
                PROPERTY_OVERLAY_FORMAT.into(),
            ),
            (PROPERTY_ROUTE_KEY.into(), route.into()),
            (PROPERTY_KIND_KEY.into(), kind.metadata_value().into()),
            (PROPERTY_GENERATION_KEY.into(), id.generation.to_string()),
            (PROPERTY_ORDINAL_KEY.into(), id.ordinal.to_string()),
        ]);
        metadata.extend(extra_metadata);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                uuid_field,
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                property_field,
            ],
            metadata,
        ));
        let rows = uuid.len();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                uuid,
                Arc::new(BooleanArray::from(vec![false; rows])),
                property,
            ],
        )
        .unwrap();
        let path = route_dir.join(id.file_name());
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    fn assert_corrupt(error: &GfError, expected: &str) {
        assert_eq!(error.code(), "GF_PROJECT_CORRUPT", "{error}");
        assert!(error.to_string().contains(expected), "{error}");
    }

    let id = PropertyFragmentId {
        generation: 1,
        ordinal: 0,
    };
    let targets = BTreeSet::from([[1; 16]]);

    // A singleton target is the strongest projection/LIMIT-shaped direct
    // read. Admission must still reject a nullable UUID authority before
    // it can prune the later null slot.
    let null_uuid = TempDir::new().unwrap();
    let mut uuids = FixedSizeBinaryBuilder::new(16);
    uuids.append_value([1; 16]).unwrap();
    uuids.append_null();
    write_fragment(
        null_uuid.path(),
        PropertyRouteKind::Node,
        "Person",
        id,
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Arc::new(uuids.finish()),
        Field::new("name", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![Some("selected"), Some("hidden")])),
        [],
    );
    let error = read_authenticated_property_snapshots_for(
        null_uuid.path(),
        PropertyRouteKind::Node,
        "Person",
        &targets,
    )
    .unwrap_err();
    assert_corrupt(&error, "UUID field is nullable");

    let wrong_width = TempDir::new().unwrap();
    write_fragment(
        wrong_width.path(),
        PropertyRouteKind::Node,
        "Person",
        id,
        Field::new("node_uuid", DataType::FixedSizeBinary(15), false),
        Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![1; 15]].into_iter()).unwrap()),
        Field::new("name", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![Some("selected")])),
        [],
    );
    let error = read_authenticated_property_snapshots_for(
        wrong_width.path(),
        PropertyRouteKind::Node,
        "Person",
        &targets,
    )
    .unwrap_err();
    assert_corrupt(&error, "not fixed binary(16)");

    let cross_kind = TempDir::new().unwrap();
    write_fragment(
        cross_kind.path(),
        PropertyRouteKind::Node,
        "Person",
        id,
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![1; 16]].into_iter()).unwrap()),
        Field::new("name", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![Some("selected")])),
        [(PROPERTY_KIND_KEY.into(), "edge".into())],
    );
    let error = read_authenticated_property_snapshots_for(
        cross_kind.path(),
        PropertyRouteKind::Node,
        "Person",
        &targets,
    )
    .unwrap_err();
    assert_corrupt(&error, "metadata conflicts with its identity");

    // Authentication is authoritative: matching hostile bytes become a
    // typed corrupt-Parquet failure, while a mismatched committed digest
    // fails before the decoder sees those bytes.
    let hostile = TempDir::new().unwrap();
    let relative = format!("properties/Person/{}", id.file_name());
    let path = hostile.path().join(&relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let bytes = b"authenticated but not Parquet";
    fs::write(&path, bytes).unwrap();
    let entry = crate::GraphFileEntry {
        relative_path: relative,
        byte_length: u64::try_from(bytes.len()).unwrap(),
        content_sha256: digest_hex(&Sha256::digest(bytes)),
        role: crate::GraphFileRole::Properties,
    };
    let error =
        AuthenticatedPropertyInventory::from_entries_at_root(hostile.path(), vec![entry.clone()])
            .unwrap_err();
    assert_corrupt(&error, "Parquet is corrupt");
    let mut conflicting_digest = entry;
    conflicting_digest.content_sha256 = "00".repeat(32);
    let error = AuthenticatedPropertyInventory::from_entries_at_root(
        hostile.path(),
        vec![conflicting_digest],
    )
    .unwrap_err();
    assert_corrupt(&error, "digest conflicts with inventory");

    for semantic_conflict in [false, true] {
        let dir = TempDir::new().unwrap();
        for generation in [1_u64, 2] {
            let id = PropertyFragmentId {
                generation,
                ordinal: 0,
            };
            let (field, values): (Field, ArrayRef) = if semantic_conflict {
                (
                    Field::new("name", DataType::Utf8, true),
                    Arc::new(StringArray::from(vec![Some("selected")])),
                )
            } else if generation == 1 {
                (
                    Field::new("name", DataType::Utf8, true),
                    Arc::new(StringArray::from(vec![Some("older")])),
                )
            } else {
                (
                    Field::new("name", DataType::Binary, true),
                    Arc::new(BinaryArray::from(vec![Some(b"newer".as_slice())])),
                )
            };
            write_fragment(
                dir.path(),
                PropertyRouteKind::Node,
                "Person",
                id,
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(
                        vec![vec![u8::try_from(generation).unwrap(); 16]].into_iter(),
                    )
                    .unwrap(),
                ),
                field,
                values,
                semantic_conflict.then(|| {
                    (
                        "ARROW:extension:name".into(),
                        format!("graphforge.semantic.{generation}"),
                    )
                }),
            );
        }
        let emitted = std::cell::Cell::new(0_usize);
        let error = visit_authenticated_property_snapshots(
            dir.path(),
            PropertyRouteKind::Node,
            "Person",
            dir.path(),
            PropertyOverlayLimits::default(),
            |_| {
                emitted.set(emitted.get() + 1);
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(emitted.get(), 0, "LIMIT-like consumer observed a row");
        if semantic_conflict {
            assert_corrupt(&error, "semantic metadata conflicts");
        } else {
            assert_corrupt(&error, "field type or semantic metadata conflicts");
        }
    }
}
