use super::super::tests::edge_batch;
use super::super::tests::node_batch;
use super::super::tests::operation;
use super::super::tests::uuid;
use super::super::*;
use super::*;

#[test]
fn preserved_spatial_bulk_publish_reopens_and_projects_exact_metadata() {
    use std::collections::HashMap;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("project");
    std::fs::create_dir(&path).unwrap();
    let extension_name = "geoarrow.vendor_point";
    let extension_metadata = "{\"crs\":\"OGC:CRS84\",\"edges\":\"spherical\"}";
    let location: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![-104.9903])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![39.7392])) as ArrayRef,
        ),
    ]));
    let location_field =
        Field::new("location", location.data_type().clone(), true).with_metadata(HashMap::from([
            ("ARROW:extension:name".into(), extension_name.into()),
            ("ARROW:extension:metadata".into(), extension_metadata.into()),
        ]));
    let schema = bulk_node_input_schema(vec![location_field]).unwrap();
    let input = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(8_020).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Place"])),
            location,
        ],
    )
    .unwrap();

    let graph = GraphForge::new(path.to_str()).unwrap();
    graph
        .publish_bulk_nodes(operation(8_021), &[input])
        .unwrap();
    drop(graph);

    let reopened = GraphForge::new(path.to_str()).unwrap();
    let result = reopened
        .execute("MATCH (n:Place) RETURN n.location AS location")
        .unwrap();
    let field = result.schema.field_with_name("location").unwrap();
    assert_eq!(field.metadata()["ARROW:extension:name"], extension_name);
    assert_eq!(
        field.metadata()["ARROW:extension:metadata"],
        extension_metadata
    );
    let values = result.batches[0]
        .column_by_name("location")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        values
            .column_by_name("x")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        -104.9903
    );
}

#[test]
fn temporal_bulk_publish_reopens_and_projects_calendar_and_zone_components() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("project");
    std::fs::create_dir(&path).unwrap();
    let duration: ArrayRef = Arc::new(StructArray::new(
        graphforge_storage::schemas::duration_struct_fields(),
        vec![
            Arc::new(Int64Array::from(vec![-2])),
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![-4])),
            Arc::new(Int64Array::from(vec![500_000_001])),
        ],
        None,
    ));
    let zoned: ArrayRef = Arc::new(StructArray::new(
        graphforge_storage::schemas::datetime_struct_fields(),
        vec![
            Arc::new(Int64Array::from(vec![20_001])),
            Arc::new(Time64NanosecondArray::from(vec![7_200_000_000_000])),
            Arc::new(Int32Array::from(vec![-21_600])),
            Arc::new(StringArray::from(vec![Some("America/Denver")])),
        ],
        None,
    ));
    let schema = bulk_node_input_schema(vec![
        Field::new("duration", duration.data_type().clone(), true),
        Field::new("zoned", zoned.data_type().clone(), true),
    ])
    .unwrap();
    let input = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([uuid(8_090).as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Event"])),
            duration,
            zoned,
        ],
    )
    .unwrap();

    let graph = GraphForge::new(path.to_str()).unwrap();
    graph
        .publish_bulk_nodes(operation(8_091), &[input])
        .unwrap();
    drop(graph);

    let reopened = GraphForge::new(path.to_str()).unwrap();
    let result = reopened
        .execute("MATCH (n:Event) RETURN n.duration AS duration, n.zoned AS zoned")
        .unwrap();
    assert_eq!(
        result
            .schema
            .field_with_name("duration")
            .unwrap()
            .data_type(),
        &DataType::Struct(graphforge_storage::schemas::duration_struct_fields())
    );
    assert_eq!(
        result.schema.field_with_name("zoned").unwrap().data_type(),
        &DataType::Struct(graphforge_storage::schemas::datetime_struct_fields())
    );
    let zoned = result.batches[0]
        .column_by_name("zoned")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        zoned
            .column_by_name("offset")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        -21_600
    );
    assert_eq!(
        zoned
            .column_by_name("zone")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "America/Denver"
    );
}

fn edge_batch_with_weights(
    ids: &[Uuid],
    rel_types: &[&str],
    sources: &[Uuid],
    targets: &[Uuid],
    weights: &[f64],
) -> RecordBatch {
    let schema =
        bulk_edge_input_schema(vec![Field::new("weight", DataType::Float64, false)]).unwrap();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(ids.iter().map(Uuid::as_bytes)).unwrap()),
            Arc::new(StringArray::from(rel_types.to_vec())),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(sources.iter().map(Uuid::as_bytes)).unwrap(),
            ),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(targets.iter().map(Uuid::as_bytes)).unwrap(),
            ),
            Arc::new(Float64Array::from(weights.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn node_and_edge_receipts_populate_only_kind_applicable_columns() {
    let operation_uuid = operation(889);
    let generation_uuid = uuid(888);
    let node_uuid = uuid(887);
    let node = node_receipt(
        &[BulkNodeRow {
            row_ordinal: 4,
            node_uuid,
            label: "Host".into(),
            properties: BTreeMap::new(),
        }],
        operation_uuid,
        generation_uuid,
    )
    .unwrap();
    assert_eq!(
        node.column_by_name("row_ordinal")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        4
    );
    assert_eq!(
        node.column_by_name("entity_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "node"
    );
    assert_eq!(
        node.column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "Host"
    );
    for name in ["rel_type", "source_uuid", "target_uuid"] {
        assert!(node.column_by_name(name).unwrap().is_null(0), "{name}");
    }

    let edge_uuid = uuid(886);
    let source_uuid = uuid(885);
    let target_uuid = uuid(884);
    let edge = edge_receipt(
        &[BulkEdgeRow {
            row_ordinal: 7,
            edge_uuid,
            rel_type: "CONNECTS".into(),
            source_uuid,
            target_uuid,
            properties: BTreeMap::new(),
        }],
        operation_uuid,
        generation_uuid,
    )
    .unwrap();
    assert_eq!(
        edge.column_by_name("entity_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "edge"
    );
    assert!(edge.column_by_name("label").unwrap().is_null(0));
    assert_eq!(
        edge.column_by_name("rel_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "CONNECTS"
    );
    for (name, expected) in [
        ("entity_uuid", edge_uuid),
        ("source_uuid", source_uuid),
        ("target_uuid", target_uuid),
        ("operation_uuid", operation_uuid.0),
        ("publication_generation_uuid", generation_uuid),
    ] {
        let values = edge
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(
            Uuid::from_slice(values.value(0)).unwrap(),
            expected,
            "{name}"
        );
    }
}

#[test]
fn publish_bulk_nodes_empty_is_a_zero_row_noop() {
    let graph = GraphForge::new(None).unwrap();
    let generation = *graph.current_generation_uuid.lock().unwrap();
    let receipt = graph.publish_bulk_nodes(operation(920), &[]).unwrap();
    assert_eq!(receipt.num_rows(), 0);
    assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);
}

#[test]
fn publish_bulk_nodes_is_atomic_ordered_and_idempotent_after_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("project");
    std::fs::create_dir(&path).unwrap();
    let batch = node_batch(
        &[uuid(921), uuid(922)],
        &["Person", "Person"],
        &[Some("Ada"), Some("Grace")],
    );
    let graph = GraphForge::new(path.to_str()).unwrap();
    let receipt = graph
        .publish_bulk_nodes(operation(923), std::slice::from_ref(&batch))
        .unwrap();
    assert_eq!(receipt.num_rows(), 2);
    let generation = *graph.current_generation_uuid.lock().unwrap();
    drop(graph);

    let reopened = GraphForge::new(path.to_str()).unwrap();
    let replay = reopened
        .publish_bulk_nodes(operation(923), std::slice::from_ref(&batch))
        .unwrap();
    assert_eq!(replay, receipt);
    assert_eq!(
        *reopened.current_generation_uuid.lock().unwrap(),
        generation
    );
    assert_eq!(
        indexed_uuid_count(&reopened, graphforge_storage::UuidIndexKind::Node),
        2
    );

    let changed = node_batch(&[uuid(924)], &["Person"], &[Some("Changed")]);
    let error = reopened
        .publish_bulk_nodes(operation(923), &[changed])
        .unwrap_err();
    assert!(matches!(
        error,
        BulkNodePublicationError::Publication(crate::GfError::Project {
            code: graphforge_core::ProjectErrorCode::TransactionConflict,
            ..
        })
    ));
    assert_eq!(
        indexed_uuid_count(&reopened, graphforge_storage::UuidIndexKind::Node),
        2
    );
}

#[test]
fn publish_bulk_edges_empty_is_a_zero_row_noop() {
    let graph = GraphForge::new(None).unwrap();
    let generation = *graph.current_generation_uuid.lock().unwrap();
    let receipt = graph.publish_bulk_edges(operation(930), &[]).unwrap();
    assert_eq!(receipt.num_rows(), 0);
    assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);
}

#[test]
fn publish_bulk_edges_is_one_generation_ordered_and_idempotent_after_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("project");
    std::fs::create_dir(&path).unwrap();
    let graph = GraphForge::new(path.to_str()).unwrap();
    let node_ids = (0..32).map(|index| uuid(1_000 + index)).collect::<Vec<_>>();
    let labels = vec!["Person"; node_ids.len()];
    let names = vec![None; node_ids.len()];
    graph
        .publish_bulk_nodes(operation(931), &[node_batch(&node_ids, &labels, &names)])
        .unwrap();
    let edge_ids = (0..256)
        .map(|index| uuid(2_000 + index))
        .collect::<Vec<_>>();
    let rel_types = vec!["KNOWS"; edge_ids.len()];
    let sources = (0..edge_ids.len())
        .map(|index| node_ids[index % node_ids.len()])
        .collect::<Vec<_>>();
    let targets = (0..edge_ids.len())
        .map(|index| node_ids[(index + 1) % node_ids.len()])
        .collect::<Vec<_>>();
    let weights = (0..edge_ids.len())
        .map(|index| index as f64 / 10.0)
        .collect::<Vec<_>>();
    let batch = edge_batch_with_weights(&edge_ids, &rel_types, &sources, &targets, &weights);
    let generation_count = std::fs::read_dir(path.join("generations")).unwrap().count();
    let transaction_count = std::fs::read_dir(path.join("transactions"))
        .unwrap()
        .count();

    let receipt = graph
        .publish_bulk_edges(operation(932), std::slice::from_ref(&batch))
        .unwrap();
    assert_eq!(receipt.num_rows(), 256);
    let receipt_ids = receipt
        .column_by_name("entity_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!(
        receipt_ids
            .iter()
            .zip(&edge_ids)
            .all(|(actual, expected)| actual == Some(expected.as_bytes().as_slice()))
    );
    assert_eq!(
        std::fs::read_dir(path.join("generations")).unwrap().count(),
        generation_count + 1
    );
    assert_eq!(
        std::fs::read_dir(path.join("transactions"))
            .unwrap()
            .count(),
        transaction_count + 1
    );
    let generation = *graph.current_generation_uuid.lock().unwrap();
    drop(graph);

    let reopened = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(
        indexed_uuid_count(&reopened, graphforge_storage::UuidIndexKind::Edge),
        256
    );
    assert_eq!(
        reopened
            .execute("MATCH ()-[r:KNOWS]->() RETURN r.weight")
            .unwrap()
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        256
    );
    let node_collision = edge_batch(&[node_ids[31]], &["KNOWS"], &[node_ids[0]], &[node_ids[1]]);
    let collision = reopened
        .publish_bulk_edges(operation(934), &[node_collision])
        .unwrap_err();
    assert!(matches!(
        collision,
        BulkEdgePublicationError::Validation(BulkValidationError {
            kind: BulkInputKind::Edge,
            reason: BulkValidationReason::IdentityConflict,
            ..
        })
    ));
    let replay = reopened
        .publish_bulk_edges(operation(932), std::slice::from_ref(&batch))
        .unwrap();
    assert_eq!(replay, receipt);
    assert_eq!(
        *reopened.current_generation_uuid.lock().unwrap(),
        generation
    );
    assert_eq!(
        std::fs::read_dir(path.join("generations")).unwrap().count(),
        generation_count + 1
    );

    let changed = edge_batch(&[uuid(3_000)], &["LIKES"], &[node_ids[0]], &[node_ids[1]]);
    let error = reopened
        .publish_bulk_edges(operation(932), &[changed])
        .unwrap_err();
    assert!(matches!(
        error,
        BulkEdgePublicationError::Publication(crate::GfError::Project {
            code: graphforge_core::ProjectErrorCode::TransactionConflict,
            ..
        })
    ));
    assert_eq!(
        indexed_uuid_count(&reopened, graphforge_storage::UuidIndexKind::Edge),
        256
    );
}

#[test]
fn publish_bulk_edges_rejects_missing_endpoint_without_a_generation() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("project");
    std::fs::create_dir(&path).unwrap();
    let graph = GraphForge::new(path.to_str()).unwrap();
    let nodes = [uuid(4_000), uuid(4_001)];
    graph
        .publish_bulk_nodes(
            operation(933),
            &[node_batch(&nodes, &["Person", "Person"], &[None, None])],
        )
        .unwrap();
    let generation = *graph.current_generation_uuid.lock().unwrap();
    let generation_count = std::fs::read_dir(path.join("generations")).unwrap().count();
    let batch = edge_batch(
        &[uuid(4_002), uuid(4_003)],
        &["KNOWS", "KNOWS"],
        &[nodes[0], nodes[0]],
        &[nodes[1], uuid(9_999)],
    );
    let error = graph
        .publish_bulk_edges(operation(934), &[batch])
        .unwrap_err();
    assert!(matches!(
        error,
        BulkEdgePublicationError::Validation(BulkValidationError {
            reason: BulkValidationReason::MissingEndpoint,
            row_ordinal: Some(1),
            ..
        })
    ));
    assert_eq!(*graph.current_generation_uuid.lock().unwrap(), generation);
    assert_eq!(
        indexed_uuid_count(&graph, graphforge_storage::UuidIndexKind::Edge),
        0
    );
    assert_eq!(
        std::fs::read_dir(path.join("generations")).unwrap().count(),
        generation_count
    );

    let duplicate = edge_batch(
        &[uuid(4_004), uuid(4_004)],
        &["KNOWS", "KNOWS"],
        &[nodes[0], nodes[1]],
        &[nodes[1], nodes[0]],
    );
    let error = graph
        .publish_bulk_edges(operation(935), &[duplicate])
        .unwrap_err();
    assert!(matches!(
        error,
        BulkEdgePublicationError::Validation(BulkValidationError {
            reason: BulkValidationReason::IdentityConflict,
            row_ordinal: Some(1),
            ..
        })
    ));
    assert_eq!(
        indexed_uuid_count(&graph, graphforge_storage::UuidIndexKind::Edge),
        0
    );
    assert_eq!(
        std::fs::read_dir(path.join("generations")).unwrap().count(),
        generation_count
    );
}

#[test]
fn wave13_bulk_publication_conflicts_preserve_the_committed_generation_and_rows() {
    let directory = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
    let left = uuid(9_100);
    let right = uuid(9_101);
    let node_operation = operation(9_102);
    let original = node_batch(
        &[left, right],
        &["Person", "Person"],
        &[Some("A"), Some("B")],
    );
    graph
        .publish_bulk_nodes(node_operation, &[original.clone()])
        .unwrap();
    let committed = graphforge_storage::resolve_project_generation(directory.path())
        .unwrap()
        .generation_uuid();

    let changed = node_batch(
        &[left, right],
        &["Person", "Person"],
        &[Some("A"), Some("changed")],
    );
    let error = graph
        .publish_bulk_nodes(node_operation, &[changed])
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "GF_IDEMPOTENCY_CONFLICT: bulk-node operation UUID was already used with different input"
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(directory.path())
            .unwrap()
            .generation_uuid(),
        committed
    );
    let collision = graph
        .validate_bulk_nodes(operation(9_103), &[original])
        .unwrap_err();
    assert_eq!(collision.reason, BulkValidationReason::IdentityConflict);
    assert_eq!(collision.row_ordinal, Some(0));

    let edge = uuid(9_104);
    let edge_operation = operation(9_105);
    let original_edge = edge_batch(&[edge], &["KNOWS"], &[left], &[right]);
    graph
        .publish_bulk_edges(edge_operation, &[original_edge.clone()])
        .unwrap();
    let edge_committed = graphforge_storage::resolve_project_generation(directory.path())
        .unwrap()
        .generation_uuid();
    let changed_edge = edge_batch(&[edge], &["LIKES"], &[left], &[right]);
    let error = graph
        .publish_bulk_edges(edge_operation, &[changed_edge])
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "GF_IDEMPOTENCY_CONFLICT: bulk-edge operation UUID was already used with different input"
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(directory.path())
            .unwrap()
            .generation_uuid(),
        edge_committed
    );
    let empty_nodes = graph.validate_bulk_nodes(operation(9_106), &[]).unwrap();
    let collision = graph
        .validate_bulk_edges(operation(9_107), &[original_edge], &empty_nodes)
        .unwrap_err();
    assert_eq!(collision.reason, BulkValidationReason::IdentityConflict);
    assert_eq!(collision.row_ordinal, Some(0));

    drop(graph);
    let reopened = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
    assert_eq!(reopened.node_count("Person").unwrap(), 2);
    assert_eq!(reopened.relationship_types().unwrap(), ["KNOWS"]);
}
