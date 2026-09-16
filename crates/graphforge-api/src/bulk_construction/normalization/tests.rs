use super::super::tests::edge_batch;
use super::super::tests::node_batch;
use super::super::tests::operation;
use super::super::tests::uuid;
use super::super::*;
use super::*;

#[test]
fn spatial_bulk_preflight_validates_the_complete_array_before_row_normalization() {
    use graphforge_ontology::{SpatialCrs, SpatialGeometryType, SpatialType};
    let spatial = SpatialType {
        geometry: SpatialGeometryType::Point,
        crs: SpatialCrs::Epsg4326,
    };
    let field = spatial.field("location", false);
    let array: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![-105.0, 181.0])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![39.7, 0.0])) as ArrayRef,
        ),
    ]));
    let error =
        preflight_spatial_columns(BulkInputKind::Node, 40, &[(&field, &array)]).unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::PropertyTypeMismatch);
    assert_eq!(error.row_ordinal, Some(40));
    assert_eq!(error.field.as_deref(), Some("location"));
    assert_eq!(error.message, "GF_SPATIAL_COORDINATE_OUT_OF_RANGE");
}

#[test]
fn preserved_spatial_bulk_preflight_accepts_explicit_metadata_and_rejects_malformed() {
    use std::collections::HashMap;

    let array: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![-104.9903])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![39.7392])) as ArrayRef,
        ),
    ]));
    let field =
        Field::new("location", array.data_type().clone(), true).with_metadata(HashMap::from([
            (
                "ARROW:extension:name".into(),
                "geoarrow.vendor_point".into(),
            ),
            (
                "ARROW:extension:metadata".into(),
                "{\"crs\":\"OGC:CRS84\",\"edges\":\"spherical\"}".into(),
            ),
        ]));
    preflight_spatial_columns(BulkInputKind::Node, 0, &[(&field, &array)]).unwrap();
    let value = normalize_properties(
        BulkInputKind::Node,
        0,
        0,
        &[(&field, &array)],
        |_, _| Ok(()),
    )
    .unwrap();
    let PropValue::Spatial(value) = &value["location"] else {
        panic!("preserved field must remain spatial");
    };
    assert_eq!(
        value.extension_name.as_deref(),
        Some("geoarrow.vendor_point")
    );
    assert_eq!(
        value.extension_metadata.as_deref(),
        Some("{\"crs\":\"OGC:CRS84\",\"edges\":\"spherical\"}")
    );

    let malformed =
        Field::new("location", array.data_type().clone(), true).with_metadata(HashMap::from([
            (
                "ARROW:extension:name".into(),
                "geoarrow.vendor_point".into(),
            ),
            ("ARROW:extension:metadata".into(), "{}".into()),
        ]));
    let error =
        preflight_spatial_columns(BulkInputKind::Node, 0, &[(&malformed, &array)]).unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::PropertyTypeMismatch);
}

#[test]
fn strict_ontology_arrow_compatibility_matrix_is_closed() {
    let list_item = Arc::new(Field::new("item", DataType::Utf8, true));
    let compatible = [
        (PropertyValueType::Utf8, DataType::Utf8),
        (PropertyValueType::Utf8, DataType::LargeUtf8),
        (PropertyValueType::Int64, DataType::Int8),
        (PropertyValueType::Int64, DataType::Int16),
        (PropertyValueType::Int64, DataType::Int32),
        (PropertyValueType::Int64, DataType::Int64),
        (PropertyValueType::Int64, DataType::UInt8),
        (PropertyValueType::Int64, DataType::UInt16),
        (PropertyValueType::Int64, DataType::UInt32),
        (PropertyValueType::Float64, DataType::Float32),
        (PropertyValueType::Float64, DataType::Float64),
        (PropertyValueType::Bool, DataType::Boolean),
        (
            PropertyValueType::List,
            DataType::List(Arc::clone(&list_item)),
        ),
        (
            PropertyValueType::List,
            DataType::LargeList(Arc::clone(&list_item)),
        ),
    ];
    for (expected, actual) in compatible {
        validate_ontology_field(
            BulkInputKind::Node,
            7,
            "property",
            &Field::new("property", actual, false),
            &expected,
            false,
        )
        .unwrap();
    }

    for expected in [
        PropertyValueType::Duration,
        PropertyValueType::DateTime,
        PropertyValueType::Map,
    ] {
        let error = validate_ontology_field(
            BulkInputKind::Edge,
            9,
            "property",
            &Field::new("property", DataType::Utf8, false),
            &expected,
            true,
        )
        .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::PropertyTypeMismatch);
        assert_eq!(error.row_ordinal, Some(9));
    }

    let mismatch = validate_ontology_field(
        BulkInputKind::Node,
        11,
        "property",
        &Field::new("property", DataType::Boolean, false),
        &PropertyValueType::Utf8,
        false,
    )
    .unwrap_err();
    assert_eq!(mismatch.reason, BulkValidationReason::PropertyTypeMismatch);
    let nullable = validate_ontology_field(
        BulkInputKind::Node,
        12,
        "property",
        &Field::new("property", DataType::Utf8, true),
        &PropertyValueType::Utf8,
        false,
    )
    .unwrap_err();
    assert_eq!(nullable.reason, BulkValidationReason::NullabilityMismatch);

    for supported in [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
    ] {
        validate_property_type(BulkInputKind::Node, "property", &supported).unwrap();
    }
    for unsupported in [
        DataType::UInt64,
        DataType::Binary,
        DataType::Date32,
        DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
    ] {
        assert_eq!(
            validate_property_type(BulkInputKind::Edge, "property", &unsupported)
                .unwrap_err()
                .reason,
            BulkValidationReason::UnsupportedPropertyType
        );
    }
}

#[test]
fn generated_bulk_identities_are_deterministic_typed_and_domain_separated() {
    let operation_id = operation(42);
    let node_zero = generated_uuid(operation_id, BulkInputKind::Node, 0);
    assert_eq!(
        node_zero,
        generated_uuid(operation_id, BulkInputKind::Node, 0)
    );
    assert_ne!(
        node_zero,
        generated_uuid(operation_id, BulkInputKind::Node, 1)
    );
    assert_ne!(
        node_zero,
        generated_uuid(operation_id, BulkInputKind::Edge, 0)
    );
    assert_eq!(node_zero.get_version_num(), 7);
    assert!(validate_operation_uuid(BulkInputKind::Node, operation_id).is_ok());
    let invalid =
        validate_operation_uuid(BulkInputKind::Edge, OperationId(Uuid::from_u128(42))).unwrap_err();
    assert_eq!(invalid.reason, BulkValidationReason::InvalidUuid);
    assert_eq!(invalid.field.as_deref(), Some("operation_uuid"));

    for valid in ["a", "_a", "alpha_1", "Δelta"] {
        assert!(validate_property_name(BulkInputKind::Node, valid).is_ok());
    }
    for invalid in ["", "1a", "a-b", "a b", "\n"] {
        assert_eq!(
            validate_property_name(BulkInputKind::Edge, invalid)
                .unwrap_err()
                .reason,
            BulkValidationReason::InvalidIdentifier
        );
    }

    let graph = GraphForge::new(None).unwrap();
    let stale_nodes = ValidatedBulkNodes {
        rows: Vec::new(),
        operation_uuid: operation(43),
        source_generation_uuid: uuid(44),
    };
    let error = graph
        .validate_bulk_edges(operation(45), &[], &stale_nodes)
        .unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::GenerationMismatch);
    assert_eq!(error.kind, BulkInputKind::Edge);

    let null_uuid = FixedSizeBinaryArray::new_null(16, 1);
    assert_eq!(
        uuid_at(
            &null_uuid,
            0,
            BulkInputKind::Node,
            3,
            "node_uuid",
            operation_id,
        )
        .unwrap(),
        generated_uuid(operation_id, BulkInputKind::Node, 3)
    );
    assert_eq!(
        explicit_uuid_at(&null_uuid, 0, BulkInputKind::Edge, 4, "source_uuid")
            .unwrap_err()
            .reason,
        BulkValidationReason::InvalidUuid
    );
    let v4 =
        FixedSizeBinaryArray::try_from_iter([Uuid::from_u128(1).as_bytes().as_slice()].into_iter())
            .unwrap();
    assert_eq!(
        uuid_at(&v4, 0, BulkInputKind::Node, 5, "node_uuid", operation_id,)
            .unwrap_err()
            .reason,
        BulkValidationReason::InvalidUuid
    );
    assert_eq!(
        explicit_uuid_at(&v4, 0, BulkInputKind::Edge, 6, "target_uuid")
            .unwrap_err()
            .reason,
        BulkValidationReason::InvalidUuid
    );
    let short = FixedSizeBinaryArray::try_from_iter([b"12345678".as_slice()].into_iter()).unwrap();
    assert_eq!(
        uuid_at(&short, 0, BulkInputKind::Node, 7, "node_uuid", operation_id,)
            .unwrap_err()
            .reason,
        BulkValidationReason::InvalidUuid
    );
}

#[test]
fn property_value_normalization_covers_every_supported_arrow_scalar() {
    let cases: Vec<(ArrayRef, PropValue)> = vec![
        (
            Arc::new(BooleanArray::from(vec![true])),
            PropValue::Bool(true),
        ),
        (Arc::new(Int8Array::from(vec![-8])), PropValue::Int(-8)),
        (Arc::new(Int16Array::from(vec![-16])), PropValue::Int(-16)),
        (Arc::new(Int32Array::from(vec![-32])), PropValue::Int(-32)),
        (Arc::new(Int64Array::from(vec![-64])), PropValue::Int(-64)),
        (Arc::new(UInt8Array::from(vec![8])), PropValue::Int(8)),
        (Arc::new(UInt16Array::from(vec![16])), PropValue::Int(16)),
        (Arc::new(UInt32Array::from(vec![32])), PropValue::Int(32)),
        (
            Arc::new(Float32Array::from(vec![1.5])),
            PropValue::Float(1.5),
        ),
        (
            Arc::new(Float64Array::from(vec![2.5])),
            PropValue::Float(2.5),
        ),
        (
            Arc::new(StringArray::from(vec!["utf8"])),
            PropValue::Str("utf8".into()),
        ),
        (
            Arc::new(LargeStringArray::from(vec!["large-utf8"])),
            PropValue::Str("large-utf8".into()),
        ),
    ];
    for (array, expected) in cases {
        assert_eq!(property_value_at(array.as_ref(), 0).unwrap(), expected);
    }

    let utc = TimestampMicrosecondArray::from(vec![1_700_000_000_123_456_i64]).with_timezone("UTC");
    assert_eq!(
        property_value_at(&utc, 0).unwrap(),
        PropValue::Temporal(graphforge_core::TemporalValue::UtcDateTime {
            epoch_micros: 1_700_000_000_123_456,
        })
    );
    let local_time = Time64NanosecondArray::from(vec![12_345_i64]);
    assert_eq!(
        property_value_at(&local_time, 0).unwrap(),
        PropValue::Temporal(graphforge_core::TemporalValue::LocalTime { nanos: 12_345 })
    );
    let duration = StructArray::new(
        graphforge_storage::schemas::duration_struct_fields(),
        vec![
            Arc::new(Int64Array::from(vec![-2])),
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![-4])),
            Arc::new(Int64Array::from(vec![5])),
        ],
        None,
    );
    assert_eq!(
        property_value_at(&duration, 0).unwrap(),
        PropValue::Temporal(graphforge_core::TemporalValue::Duration {
            months: -2,
            days: 3,
            seconds: -4,
            nanos: 5,
        })
    );

    let nullable = StringArray::from(vec![None::<&str>]);
    assert_eq!(property_value_at(&nullable, 0).unwrap(), PropValue::Null);

    let unsupported = UInt64Array::from(vec![u64::MAX]);
    assert_eq!(
        property_value_at(&unsupported, 0).unwrap_err(),
        "unsupported property type UInt64"
    );

    let list = ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([Some(vec![
        Some(1),
        None,
        Some(3),
    ])]);
    assert_eq!(
        property_value_at(&list, 0).unwrap(),
        PropValue::List(vec![PropValue::Int(1), PropValue::Null, PropValue::Int(3)])
    );
    let large =
        LargeListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>([Some(vec![
            Some(4),
            Some(5),
        ])]);
    assert_eq!(
        property_value_at(&large, 0).unwrap(),
        PropValue::List(vec![PropValue::Int(4), PropValue::Int(5)])
    );
}

#[test]
fn wave13_property_normalization_and_strict_owner_failures_keep_bulk_context() {
    let field = Field::new("when", DataType::Date32, false);
    let array: ArrayRef = Arc::new(arrow::array::Date32Array::from(vec![1]));
    let columns = [(&field, &array)];
    let unsupported =
        normalize_properties(BulkInputKind::Node, 9, 0, &columns, |_, _| Ok(())).unwrap_err();
    assert_eq!(
        unsupported.reason,
        BulkValidationReason::UnsupportedPropertyType
    );
    assert_eq!(unsupported.row_ordinal, Some(9));
    assert_eq!(unsupported.field.as_deref(), Some("when"));

    let owner_error = normalize_properties(BulkInputKind::Edge, 11, 0, &columns, |name, _| {
        Err(row_error(
            BulkInputKind::Edge,
            BulkValidationReason::UnknownOntologyProperty,
            11,
            name,
            "owner rejected property",
        ))
    })
    .unwrap_err();
    assert_eq!(
        owner_error.reason,
        BulkValidationReason::UnknownOntologyProperty
    );

    let mut graph = GraphForge::new(None).unwrap();
    graph.ontology_mode = OntologyMode::Strict;
    graph.ontology = None;
    for error in [
        validate_node_owner(&graph, 1, "Person").unwrap_err(),
        validate_edge_owner(&graph, 2, "KNOWS").unwrap_err(),
    ] {
        assert_eq!(error.reason, BulkValidationReason::UnknownOntologyType);
    }
    let property_field = Field::new("name", DataType::Utf8, true);
    for error in [
        validate_node_property(&graph, 3, "Person", "name", &property_field).unwrap_err(),
        validate_edge_property(&graph, 4, "KNOWS", "weight", &property_field).unwrap_err(),
    ] {
        assert_eq!(error.reason, BulkValidationReason::ProjectState);
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "wrong",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    assert_eq!(
        uuid_column(&batch, BulkInputKind::Node, "wrong")
            .unwrap_err()
            .reason,
        BulkValidationReason::SchemaMismatch
    );
    assert_eq!(
        string_column(&batch, BulkInputKind::Edge, "wrong")
            .unwrap_err()
            .reason,
        BulkValidationReason::SchemaMismatch
    );
}

#[test]
fn canonical_schema_order_and_metadata_are_enforced() {
    let graph = GraphForge::new(None).unwrap();
    let missing_metadata = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
    ]));
    let empty_uuid = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        std::iter::empty::<Option<[u8; 16]>>(),
        16,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        missing_metadata,
        vec![
            Arc::new(empty_uuid),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .unwrap();
    let error = graph
        .validate_bulk_nodes(operation(890), &[batch])
        .unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::SchemaMismatch);
    assert_eq!(error.batch_index, Some(0));

    for metadata in [
        contract_metadata("edge"),
        HashMap::from([
            (
                "graphforge.bulk_contract_version".to_owned(),
                "2".to_owned(),
            ),
            ("graphforge.bulk_kind".to_owned(), "node".to_owned()),
            (
                "graphforge.row_order".to_owned(),
                "logical_input_order".to_owned(),
            ),
        ]),
    ] {
        let wrong_metadata = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
                Field::new("label", DataType::Utf8, false),
            ],
            metadata,
        ));
        let error = graph
            .validate_bulk_nodes(operation(890), &[RecordBatch::new_empty(wrong_metadata)])
            .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::SchemaMismatch);
    }

    let unsorted = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("label", DataType::Utf8, false),
            Field::new("zeta", DataType::Utf8, true),
            Field::new("alpha", DataType::Utf8, true),
        ],
        contract_metadata("node"),
    ));
    let batch = RecordBatch::new_empty(unsorted);
    let error = graph
        .validate_bulk_nodes(operation(891), &[batch])
        .unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::SchemaMismatch);
    assert!(error.message.contains("lexicographically ordered"));

    let unsupported_list = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("label", DataType::Utf8, false),
            Field::new(
                "events",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                    true,
                ))),
                true,
            ),
        ],
        contract_metadata("node"),
    ));
    let error = graph
        .validate_bulk_nodes(operation(891), &[RecordBatch::new_empty(unsupported_list)])
        .unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::UnsupportedPropertyType);
    assert_eq!(error.field.as_deref(), Some("events"));
}

#[test]
fn wave13_public_schema_validation_matrix_preserves_error_kind_field_and_partition() {
    let reserved =
        bulk_node_input_schema(vec![Field::new("label", DataType::Utf8, true)]).unwrap_err();
    assert_eq!(reserved.reason, BulkValidationReason::ReservedField);
    assert_eq!(reserved.field.as_deref(), Some("label"));

    let duplicate = bulk_edge_input_schema(vec![
        Field::new("weight", DataType::Float64, true),
        Field::new("weight", DataType::Float64, false),
    ])
    .unwrap_err();
    assert_eq!(duplicate.reason, BulkValidationReason::DuplicateField);
    assert_eq!(duplicate.field.as_deref(), Some("weight"));

    let invalid =
        bulk_node_input_schema(vec![Field::new("not valid", DataType::Utf8, true)]).unwrap_err();
    assert_eq!(invalid.reason, BulkValidationReason::InvalidIdentifier);
    assert_eq!(invalid.field.as_deref(), Some("not valid"));

    let unsupported =
        bulk_edge_input_schema(vec![Field::new("counter", DataType::UInt64, false)]).unwrap_err();
    assert_eq!(
        unsupported.reason,
        BulkValidationReason::UnsupportedPropertyType
    );
    assert_eq!(unsupported.field.as_deref(), Some("counter"));

    let graph = GraphForge::new(None).unwrap();
    let missing = Arc::new(Schema::new_with_metadata(
        vec![Field::new("node_uuid", DataType::FixedSizeBinary(16), true)],
        contract_metadata("node"),
    ));
    let missing = graph
        .validate_bulk_nodes(operation(892), &[RecordBatch::new_empty(missing)])
        .unwrap_err();
    assert_eq!(missing.reason, BulkValidationReason::SchemaMismatch);
    assert_eq!(missing.batch_index, Some(0));
    assert!(missing.message.contains("required topology fields"));

    let first = RecordBatch::new_empty(bulk_node_input_schema(vec![]).unwrap());
    let second = RecordBatch::new_empty(
        bulk_node_input_schema(vec![Field::new("name", DataType::Utf8, true)]).unwrap(),
    );
    let drift = graph
        .validate_bulk_nodes(operation(893), &[first, second])
        .unwrap_err();
    assert_eq!(drift.reason, BulkValidationReason::SchemaMismatch);
    assert_eq!(drift.batch_index, Some(1));
    assert_eq!(drift.message, "schema differs from batch 0");

    let wrong_edge = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("rel_type", DataType::LargeUtf8, false),
            Field::new("source_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("target_uuid", DataType::FixedSizeBinary(16), false),
        ],
        contract_metadata("edge"),
    ));
    let source_generation_uuid = *graph.current_generation_uuid.lock().unwrap();
    let wrong_edge = graph
        .validate_bulk_edges(
            operation(894),
            &[RecordBatch::new_empty(wrong_edge)],
            &ValidatedBulkNodes {
                rows: vec![],
                operation_uuid: operation(894),
                source_generation_uuid,
            },
        )
        .unwrap_err();
    assert_eq!(wrong_edge.reason, BulkValidationReason::SchemaMismatch);
    assert_eq!(wrong_edge.batch_index, Some(0));
    assert_eq!(wrong_edge.field.as_deref(), Some("rel_type"));
}

#[test]
fn property_and_ontology_type_registries_cover_every_supported_family() {
    let scalar_types = [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
    ];
    for data_type in scalar_types {
        assert!(validate_property_type(BulkInputKind::Node, "value", &data_type).is_ok());
    }
    for data_type in [
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
    ] {
        assert!(validate_property_type(BulkInputKind::Edge, "values", &data_type).is_ok());
    }
    let error = validate_property_type(BulkInputKind::Node, "when", &DataType::Date32).unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::UnsupportedPropertyType);
    assert_eq!(error.field.as_deref(), Some("when"));

    for identifier in ["Person", "_private", "Ångström2"] {
        assert!(validate_identifier(BulkInputKind::Node, 0, "label", identifier).is_ok());
        assert!(validate_property_name(BulkInputKind::Node, identifier).is_ok());
    }
    for identifier in ["", "9name", "has space", "has-dash"] {
        let row = validate_identifier(BulkInputKind::Edge, 7, "rel_type", identifier).unwrap_err();
        assert_eq!(row.reason, BulkValidationReason::InvalidIdentifier);
        assert_eq!(row.row_ordinal, Some(7));
        let field = validate_property_name(BulkInputKind::Edge, identifier).unwrap_err();
        assert_eq!(field.reason, BulkValidationReason::InvalidIdentifier);
        assert_eq!(field.field.as_deref(), Some(identifier));
    }
    let at_limit = format!("A{}", "é".repeat(127));
    let over_limit = format!("A{}x", "é".repeat(127));
    assert_eq!(at_limit.len(), 255);
    assert_eq!(over_limit.len(), 256);
    assert!(validate_identifier(BulkInputKind::Node, 0, "label", &at_limit).is_ok());
    assert!(validate_property_name(BulkInputKind::Node, &at_limit).is_ok());
    assert_eq!(
        validate_identifier(BulkInputKind::Edge, 7, "rel_type", &over_limit)
            .unwrap_err()
            .reason,
        BulkValidationReason::InvalidIdentifier
    );
    assert_eq!(
        validate_property_name(BulkInputKind::Edge, &over_limit)
            .unwrap_err()
            .reason,
        BulkValidationReason::InvalidIdentifier
    );

    let compatible = [
        (PropertyValueType::Utf8, DataType::LargeUtf8),
        (PropertyValueType::Int64, DataType::UInt32),
        (PropertyValueType::Float64, DataType::Float32),
        (PropertyValueType::Bool, DataType::Boolean),
        (
            PropertyValueType::List,
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
        ),
    ];
    for (expected, actual) in compatible {
        assert!(
            validate_ontology_field(
                BulkInputKind::Node,
                3,
                "value",
                &Field::new("value", actual, false),
                &expected,
                false,
            )
            .is_ok()
        );
    }
    for expected in [
        PropertyValueType::Duration,
        PropertyValueType::DateTime,
        PropertyValueType::Map,
    ] {
        let error = validate_ontology_field(
            BulkInputKind::Node,
            3,
            "value",
            &Field::new("value", DataType::Utf8, false),
            &expected,
            false,
        )
        .unwrap_err();
        assert_eq!(error.reason, BulkValidationReason::PropertyTypeMismatch);
    }
    let error = validate_ontology_field(
        BulkInputKind::Edge,
        4,
        "weight",
        &Field::new("weight", DataType::Float64, true),
        &PropertyValueType::Float64,
        false,
    )
    .unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::NullabilityMismatch);
    assert_eq!(error.row_ordinal, Some(4));
}

#[test]
fn null_identity_generation_is_operation_and_ordinal_deterministic() {
    let graph = GraphForge::new(None).unwrap();
    let schema = bulk_node_input_schema(vec![]).unwrap();
    let generated = |labels: Vec<&str>| {
        let row_count = labels.len();
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        std::iter::repeat(None::<[u8; 16]>).take(row_count),
                        16,
                    )
                    .unwrap(),
                ),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .unwrap()
    };
    let first = graph
        .validate_bulk_nodes(operation(892), &[generated(vec!["Person", "Person"])])
        .unwrap();
    let second = graph
        .validate_bulk_nodes(
            operation(892),
            &[generated(vec!["Person"]), generated(vec!["Person"])],
        )
        .unwrap();
    let different_operation = graph
        .validate_bulk_nodes(operation(893), &[generated(vec!["Person", "Person"])])
        .unwrap();
    assert_eq!(first.rows(), second.rows());
    assert_ne!(
        first.rows()[0].node_uuid,
        different_operation.rows()[0].node_uuid
    );
    assert_ne!(first.rows()[0].node_uuid, first.rows()[1].node_uuid);
    assert!(
        first
            .rows()
            .iter()
            .all(|row| row.node_uuid.get_version_num() == 7)
    );

    let source = uuid(894);
    let target = uuid(895);
    let nodes = graph
        .validate_bulk_nodes(
            operation(894),
            &[node_batch(
                &[source, target],
                &["Person", "Person"],
                &[None, None],
            )],
        )
        .unwrap();
    let edge = RecordBatch::try_new(
        bulk_edge_input_schema(vec![]).unwrap(),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [None::<[u8; 16]>].into_iter(),
                    16,
                )
                .unwrap(),
            ),
            Arc::new(StringArray::from(vec!["KNOWS"])),
            Arc::new(FixedSizeBinaryArray::try_from_iter([source.as_bytes()].into_iter()).unwrap()),
            Arc::new(FixedSizeBinaryArray::try_from_iter([target.as_bytes()].into_iter()).unwrap()),
        ],
    )
    .unwrap();
    let edge = graph
        .validate_bulk_edges(operation(892), &[edge], &nodes)
        .unwrap();
    assert_ne!(
        first.rows()[0].node_uuid,
        edge.rows()[0].edge_uuid,
        "validated node and edge generation domains must not collide"
    );
}

#[test]
fn empty_and_partitioned_nodes_preserve_logical_ordinals_and_values() {
    let graph = GraphForge::new(None).unwrap();
    assert!(
        graph
            .validate_bulk_nodes(operation(900), &[])
            .unwrap()
            .rows()
            .is_empty()
    );
    let first = node_batch(&[uuid(1)], &["Person"], &[Some("Alice")]);
    let second = node_batch(
        &[uuid(2), uuid(3)],
        &["Person", "Person"],
        &[None, Some("Cara")],
    );
    let validated = graph
        .validate_bulk_nodes(operation(900), &[first, second])
        .unwrap();
    assert_eq!(
        validated.source_generation_uuid(),
        *graph.current_generation_uuid.lock().unwrap()
    );
    assert_eq!(validated.operation_uuid(), operation(900));
    assert_eq!(
        validated
            .rows()
            .iter()
            .map(|row| row.row_ordinal)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert_eq!(
        validated.rows()[0].properties["name"],
        PropValue::Str("Alice".into())
    );
    assert_eq!(validated.rows()[1].properties["name"], PropValue::Null);
}

#[test]
fn deterministic_first_error_precedes_later_row_and_property_defects() {
    let graph = GraphForge::new(None).unwrap();
    let duplicate = uuid(9);
    let batch = node_batch(
        &[duplicate, duplicate],
        &["Person", "bad-label"],
        &[Some("ok"), Some("later")],
    );
    let error = graph
        .validate_bulk_nodes(operation(901), &[batch])
        .unwrap_err();
    assert_eq!(error.code(), "GF_BULK_VALIDATION");
    assert_eq!(error.kind, BulkInputKind::Node);
    assert_eq!(error.reason, BulkValidationReason::IdentityConflict);
    assert_eq!(error.row_ordinal, Some(1));
    assert_eq!(error.field.as_deref(), Some("node_uuid"));

    let first = node_batch(&[duplicate], &["Person"], &[Some("ok")]);
    let second = node_batch(&[duplicate], &["bad-label"], &[Some("later")]);
    assert_eq!(
        graph
            .validate_bulk_nodes(operation(901), &[first, second])
            .unwrap_err()
            .to_string(),
        error.to_string(),
        "logical error order must not depend on record-batch partitioning"
    );
}

#[test]
fn malformed_schema_and_non_v7_uuid_fail_with_field_context() {
    let graph = GraphForge::new(None).unwrap();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("node_uuid", DataType::Utf8, true),
            Field::new("label", DataType::Utf8, false),
        ],
        contract_metadata("node"),
    ));
    let malformed = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["not-a-uuid"])),
            Arc::new(StringArray::from(vec!["Person"])),
        ],
    )
    .unwrap();
    let malformed = graph
        .validate_bulk_nodes(operation(902), &[malformed])
        .unwrap_err();
    assert_eq!(malformed.reason, BulkValidationReason::SchemaMismatch);
    assert_eq!(malformed.field.as_deref(), Some("node_uuid"));

    let non_v7 = Uuid::from_u128(4);
    let error = graph
        .validate_bulk_nodes(
            operation(902),
            &[node_batch(&[non_v7], &["Person"], &[None])],
        )
        .unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::InvalidUuid);
    assert_eq!(error.field.as_deref(), Some("node_uuid"));
}

#[test]
fn edge_endpoints_accept_same_request_nodes_and_reject_missing_nodes() {
    let graph = GraphForge::new(None).unwrap();
    let source = uuid(20);
    let target = uuid(21);
    let nodes = graph
        .validate_bulk_nodes(
            operation(903),
            &[node_batch(
                &[source, target],
                &["Person", "Person"],
                &[None, None],
            )],
        )
        .unwrap();
    let valid = edge_batch(&[uuid(22)], &["KNOWS"], &[source], &[target]);
    let validated_edges = graph
        .validate_bulk_edges(operation(904), &[valid], &nodes)
        .unwrap();
    assert_eq!(validated_edges.rows().len(), 1);
    assert_eq!(
        validated_edges.source_generation_uuid(),
        nodes.source_generation_uuid()
    );
    assert_eq!(validated_edges.operation_uuid(), operation(904));

    let missing = edge_batch(&[uuid(23)], &["KNOWS"], &[source], &[uuid(99)]);
    let error = graph
        .validate_bulk_edges(operation(905), &[missing], &nodes)
        .unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::MissingEndpoint);
    assert_eq!(error.row_ordinal, Some(0));
    assert_eq!(error.field.as_deref(), Some("target_uuid"));

    let duplicate = uuid(24);
    let duplicate_edges = edge_batch(
        &[duplicate, duplicate],
        &["KNOWS", "KNOWS"],
        &[source, source],
        &[target, target],
    );
    let duplicate = graph
        .validate_bulk_edges(operation(906), &[duplicate_edges], &nodes)
        .unwrap_err();
    assert_eq!(duplicate.reason, BulkValidationReason::IdentityConflict);
    assert_eq!(duplicate.row_ordinal, Some(1));

    let cross_kind = edge_batch(&[source], &["KNOWS"], &[source], &[target]);
    let cross_kind = graph
        .validate_bulk_edges(operation(907), &[cross_kind], &nodes)
        .unwrap_err();
    assert_eq!(cross_kind.reason, BulkValidationReason::IdentityConflict);
    assert_eq!(cross_kind.row_ordinal, Some(0));
}

#[test]
fn wave13_strict_inherited_properties_and_types_are_validated_without_publication() {
    let dir = tempfile::TempDir::new().unwrap();
    let project_path = dir.path().join("project");
    std::fs::create_dir(&project_path).unwrap();
    let ontology_path = dir.path().join("strict.yaml");
    std::fs::write(
        &ontology_path,
        "ontology_id: bulk\nversion: \"1\"\nentity_types:\n  - name: Asset\n    abstract: false\n  - name: Host\n    abstract: false\n    parent: Asset\nrelation_types:\n  - name: CONNECTS\n    src: Host\n    dst: Host\nproperties:\n  - owner: Asset\n    name: name\n    type: utf8\n    nullable: true\n  - owner: Host\n    name: score\n    type: int64\n    nullable: false\n  - owner: CONNECTS\n    name: weight\n    type: float64\n    nullable: false\n",
    )
    .unwrap();
    let mut graph = GraphForge::new(project_path.to_str()).unwrap();
    graph
        .adopt_ontology(crate::AdoptOntologyRequest {
            context: crate::WriteContext {
                operation_uuid: crate::OperationId(uuid(40)),
                actor_uuid: None,
            },
            path: ontology_path,
            mode: OntologyMode::Strict,
        })
        .unwrap();

    let unknown_owner = graph
        .validate_bulk_nodes(
            operation(909),
            &[node_batch(&[uuid(409)], &["Unknown"], &[None])],
        )
        .unwrap_err();
    assert_eq!(
        unknown_owner.reason,
        BulkValidationReason::UnknownOntologyType
    );
    assert_eq!(unknown_owner.row_ordinal, Some(0));
    assert_eq!(unknown_owner.field.as_deref(), Some("label"));
    assert_eq!(unknown_owner.message, "unknown strict ontology entity type");

    let unknown_property_schema =
        bulk_node_input_schema(vec![Field::new("alias", DataType::Utf8, true)]).unwrap();
    let unknown_property_batch = RecordBatch::try_new(
        unknown_property_schema,
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(410).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Host"])),
            Arc::new(StringArray::from(vec![Some("gateway")])),
        ],
    )
    .unwrap();
    let unknown_property = graph
        .validate_bulk_nodes(operation(909), &[unknown_property_batch])
        .unwrap_err();
    assert_eq!(
        unknown_property.reason,
        BulkValidationReason::UnknownOntologyProperty
    );
    assert_eq!(unknown_property.row_ordinal, Some(0));
    assert_eq!(unknown_property.field.as_deref(), Some("alias"));
    assert_eq!(
        unknown_property.message,
        "property is not declared for strict entity type"
    );

    let schema = bulk_node_input_schema(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Int64, false),
    ])
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(41).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Host"])),
            Arc::new(StringArray::from(vec![Some("gateway")])),
            Arc::new(Int64Array::from(vec![7])),
        ],
    )
    .unwrap();
    assert_eq!(
        graph
            .validate_bulk_nodes(operation(910), &[batch])
            .unwrap()
            .rows()
            .len(),
        1
    );

    let before_graph = crate::graph_snapshot::capture(&graph.dir()).unwrap();
    let before_catalog = graph.runtime_catalog.lock().unwrap().to_record_batch();
    let before_generation = *graph.current_generation_uuid.lock().unwrap();
    let before_ontology = graph
        .workspace_ontology()
        .unwrap()
        .to_canonical_json()
        .unwrap();
    let before_configuration = graph
        .workspace_configuration()
        .unwrap()
        .to_canonical_json()
        .unwrap();

    let wrong = bulk_node_input_schema(vec![Field::new("score", DataType::Utf8, false)]).unwrap();
    let batch = RecordBatch::try_new(
        wrong,
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(42).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Host"])),
            Arc::new(StringArray::from(vec!["seven"])),
        ],
    )
    .unwrap();
    let error = graph
        .validate_bulk_nodes(operation(911), &[batch])
        .unwrap_err();
    assert_eq!(error.reason, BulkValidationReason::PropertyTypeMismatch);
    assert_eq!(error.field.as_deref(), Some("score"));
    assert_eq!(
        crate::graph_snapshot::capture(&graph.dir()).unwrap().bytes,
        before_graph.bytes
    );
    assert_eq!(
        graph.runtime_catalog.lock().unwrap().to_record_batch(),
        before_catalog
    );
    assert_eq!(
        *graph.current_generation_uuid.lock().unwrap(),
        before_generation
    );
    assert_eq!(
        graph
            .workspace_ontology()
            .unwrap()
            .to_canonical_json()
            .unwrap(),
        before_ontology
    );
    assert_eq!(
        graph
            .workspace_configuration()
            .unwrap()
            .to_canonical_json()
            .unwrap(),
        before_configuration
    );

    let nullable_score =
        bulk_node_input_schema(vec![Field::new("score", DataType::Int64, true)]).unwrap();
    let nullable_score = RecordBatch::try_new(
        nullable_score,
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(43).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Host"])),
            Arc::new(Int64Array::from(vec![Some(7)])),
        ],
    )
    .unwrap();
    assert_eq!(
        graph
            .validate_bulk_nodes(operation(912), &[nullable_score])
            .unwrap_err()
            .reason,
        BulkValidationReason::NullabilityMismatch
    );

    let edge_schema =
        bulk_edge_input_schema(vec![Field::new("weight", DataType::Float64, false)]).unwrap();
    let edge = RecordBatch::try_new(
        edge_schema,
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(45).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(StringArray::from(vec!["CONNECTS"])),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(41).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(41).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(arrow::array::Float64Array::from(vec![0.5])),
        ],
    )
    .unwrap();
    let same_request = ValidatedBulkNodes {
        rows: vec![BulkNodeRow {
            row_ordinal: 0,
            node_uuid: uuid(41),
            label: "Host".into(),
            properties: BTreeMap::new(),
        }],
        operation_uuid: operation(913),
        source_generation_uuid: *graph.current_generation_uuid.lock().unwrap(),
    };

    let unknown_edge_property_schema =
        bulk_edge_input_schema(vec![Field::new("alias", DataType::Utf8, true)]).unwrap();
    let unknown_edge_property = RecordBatch::try_new(
        unknown_edge_property_schema,
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(44).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(StringArray::from(vec!["CONNECTS"])),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(41).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(41).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(StringArray::from(vec![Some("uplink")])),
        ],
    )
    .unwrap();
    let unknown_edge_property = graph
        .validate_bulk_edges(operation(913), &[unknown_edge_property], &same_request)
        .unwrap_err();
    assert_eq!(
        unknown_edge_property.reason,
        BulkValidationReason::UnknownOntologyProperty
    );
    assert_eq!(unknown_edge_property.row_ordinal, Some(0));
    assert_eq!(unknown_edge_property.field.as_deref(), Some("alias"));
    assert_eq!(
        unknown_edge_property.message,
        "property is not declared for strict relationship type"
    );

    assert_eq!(
        graph
            .validate_bulk_edges(operation(913), &[edge], &same_request)
            .unwrap()
            .rows()
            .len(),
        1
    );

    let strict_node_schema = bulk_node_input_schema(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Int64, false),
    ])
    .unwrap();
    let strict_nodes = RecordBatch::try_new(
        strict_node_schema,
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    [uuid(46), uuid(47)].iter().map(Uuid::as_bytes),
                )
                .unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Host", "Host"])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    graph
        .publish_bulk_nodes(operation(914), &[strict_nodes])
        .unwrap();
    let generation = *graph.current_generation_uuid.lock().unwrap();
    let invalid_relation = edge_batch(&[uuid(48)], &["UNKNOWN"], &[uuid(46)], &[uuid(47)]);
    let error = graph
        .publish_bulk_edges(operation(915), &[invalid_relation])
        .unwrap_err();
    assert!(matches!(
        error,
        BulkEdgePublicationError::Validation(BulkValidationError {
            reason: BulkValidationReason::UnknownOntologyType,
            ..
        })
    ));
    assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);
    assert_eq!(
        indexed_uuid_count(&graph, graphforge_storage::UuidIndexKind::Edge),
        0
    );
}
