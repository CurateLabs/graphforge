//! Persisted public-facade lifecycle and checkpoint release contracts.

use std::collections::HashMap;

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use gf_api::{
    CancellationToken, CheckpointDiffDetail, CheckpointDiffScope, CheckpointRequest,
    CheckpointSelector, DeleteCheckpointRequest, DiffCheckpointsRequest, GraphForge,
    ListCheckpointsRequest, OperationId, PageRequest, PropValue, RevertCheckpointRequest,
};
use tempfile::TempDir;
use uuid::Uuid;

fn operation(seed: u128) -> OperationId {
    OperationId(Uuid::from_u128(seed))
}

fn string_values(batch: &arrow::record_batch::RecordBatch, column: &str) -> Vec<String> {
    let values = batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..values.len())
        .map(|row| values.value(row).to_owned())
        .collect()
}

fn assert_checkpoint_list_schema(schema: &arrow::datatypes::Schema) {
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type().clone(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("checkpoint_uuid", DataType::FixedSizeBinary(16), false),
            ("name", DataType::Utf8, false),
            ("description", DataType::Utf8, true),
            ("generation_uuid", DataType::FixedSizeBinary(16), false),
            (
                "generation_manifest_sha256",
                DataType::FixedSizeBinary(32),
                false
            ),
            (
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            ("created_by", DataType::FixedSizeBinary(16), true),
        ]
    );
}

fn assert_pinned_read_only_view(graph: &GraphForge) {
    let view = graph.open_checkpoint("Before").unwrap();
    assert_ne!(view.checkpoint_uuid(), view.generation_uuid());
    let pinned = view
        .execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
        .unwrap();
    assert_eq!(string_values(&pinned.batches[0], "name"), ["before"]);
    assert_eq!(
        view.execute("CREATE (:Person)").unwrap_err().code(),
        "GF_READ_ONLY_VIEW"
    );
    assert_eq!(
        view.add_node("Person", &HashMap::new()).unwrap_err().code(),
        "GF_READ_ONLY_VIEW"
    );
}

#[test]
fn persisted_construction_reopens_with_exact_uuid_properties_and_order() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().to_str().unwrap();
    let (alice, bob, edge) = {
        let graph = GraphForge::new(Some(path)).unwrap();
        let alice = graph
            .add_node(
                "Person",
                &HashMap::from([
                    ("name".into(), PropValue::Str("Alice".into())),
                    ("score".into(), PropValue::Int(7)),
                ]),
            )
            .unwrap();
        let bob = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("Bob".into()))]),
            )
            .unwrap();
        let edge = graph
            .add_edge(
                &alice,
                "KNOWS",
                &bob,
                &HashMap::from([("since".into(), PropValue::Int(2026))]),
            )
            .unwrap();
        (alice.uuid, bob.uuid, edge.uuid)
    };

    let graph = GraphForge::new(Some(path)).unwrap();
    let result = graph
        .execute(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
             RETURN a.node_uuid AS src, a.name AS src_name, a.score AS score, \
             r.edge_uuid AS edge, r.since AS since, b.node_uuid AS dst, b.name AS dst_name \
             ORDER BY src_name, dst_name",
        )
        .unwrap();
    assert_eq!(result.batches.len(), 1);
    assert_eq!(result.batches[0].num_rows(), 1);
    assert_eq!(
        result
            .schema
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type().clone(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("src", DataType::FixedSizeBinary(16), false),
            ("src_name", DataType::Utf8, true),
            ("score", DataType::Int64, true),
            ("edge", DataType::FixedSizeBinary(16), false),
            ("since", DataType::Int64, true),
            ("dst", DataType::FixedSizeBinary(16), false),
            ("dst_name", DataType::Utf8, true),
        ]
    );
    let batch = &result.batches[0];
    for (column, expected) in [("src", alice), ("edge", edge), ("dst", bob)] {
        assert_eq!(
            batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            expected.as_bytes()
        );
    }
    assert_eq!(string_values(batch, "src_name"), ["Alice"]);
    assert_eq!(string_values(batch, "dst_name"), ["Bob"]);
    assert_eq!(
        batch
            .column_by_name("since")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2026
    );
}

#[test]
fn checkpoint_lifecycle_is_persisted_pinned_read_only_and_revertible() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("before".into()))]),
        )
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "Before".into(),
            description: Some("known state".into()),
            idempotency_key: operation(1),
            actor_uuid: None,
        })
        .unwrap();
    graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("after".into()))]),
        )
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "After".into(),
            description: None,
            idempotency_key: operation(2),
            actor_uuid: None,
        })
        .unwrap();

    drop(graph);
    let mut graph = GraphForge::new(Some(path)).unwrap();
    let listed = graph
        .list_checkpoints(ListCheckpointsRequest::default())
        .unwrap();
    assert_eq!(
        string_values(&listed.batches[0], "name"),
        ["After", "Before"]
    );
    assert_checkpoint_list_schema(&listed.schema);
    assert_pinned_read_only_view(&graph);

    let diff = graph
        .diff_checkpoints(DiffCheckpointsRequest {
            from: CheckpointSelector::Named("Before".into()),
            to: CheckpointSelector::Named("After".into()),
            scope: CheckpointDiffScope::All,
            detail: CheckpointDiffDetail::Summary,
            page: PageRequest::default(),
        })
        .unwrap();
    assert!(diff.batches[0].num_rows() > 0);
    assert_eq!(diff.schema.field(0).name(), "from_checkpoint_uuid");
    assert!(string_values(&diff.batches[0], "change_kind").contains(&"modified".into()));

    graph
        .revert_to_checkpoint(RevertCheckpointRequest {
            name: "Before".into(),
            reason: "release conformance".into(),
            idempotency_key: operation(3),
            actor_uuid: None,
        })
        .unwrap();
    let restored = graph
        .execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
        .unwrap();
    assert_eq!(string_values(&restored.batches[0], "name"), ["before"]);

    graph
        .delete_checkpoint(DeleteCheckpointRequest {
            name: "After".into(),
            idempotency_key: operation(4),
            actor_uuid: None,
        })
        .unwrap();
    assert_eq!(
        graph.open_checkpoint("After").unwrap_err().code(),
        "GF_CHECKPOINT_NOT_FOUND"
    );
}

#[test]
fn checkpoint_empty_invalid_limits_and_cancellation_have_stable_contracts() {
    let directory = TempDir::new().unwrap();
    let graph = GraphForge::new(directory.path().to_str()).unwrap();
    let empty = graph
        .list_checkpoints(ListCheckpointsRequest::default())
        .unwrap();
    assert_eq!(empty.batches[0].num_rows(), 0);
    assert_eq!(empty.schema.field(1).name(), "name");

    assert_eq!(
        graph.open_checkpoint("missing").unwrap_err().code(),
        "GF_CHECKPOINT_NOT_FOUND"
    );
    assert_eq!(
        graph
            .list_checkpoints(ListCheckpointsRequest {
                page: PageRequest {
                    limit: 0,
                    after: None,
                    cancellation: None,
                },
            })
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        graph
            .list_checkpoints(ListCheckpointsRequest {
                page: PageRequest {
                    limit: 1,
                    after: None,
                    cancellation: Some(cancellation),
                },
            })
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
}
