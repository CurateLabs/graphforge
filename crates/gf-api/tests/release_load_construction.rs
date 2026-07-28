//! Prove release-load fixtures can be constructed under the recovery bound.
//!
//! Scalar add_node/add_edge would create one generation per entity and fail past
//! 10_000 publications. Bulk construction must remain two publications.

use std::sync::Arc;

use arrow::array::{
    Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float64Array, StringArray,
};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use gf_api::{GraphForge, OperationId, bulk_edge_input_schema, bulk_node_input_schema};
use gf_core::uuid::Uuid;

fn null_uuids(rows: usize) -> FixedSizeBinaryArray {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for _ in 0..rows {
        builder.append_null();
    }
    builder.finish()
}

#[test]
fn bulk_load_construction_stays_within_recovery_bound() {
    let directory = tempfile::TempDir::new().unwrap();
    let project = directory.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let graph = GraphForge::new(Some(project.to_str().unwrap())).unwrap();

    const NODES: usize = 128;
    const EDGES: usize = 12_000;
    let node_schema =
        bulk_node_input_schema(vec![Field::new("name", DataType::Utf8, false)]).unwrap();
    let node_batch = RecordBatch::try_new(
        node_schema,
        vec![
            Arc::new(null_uuids(NODES)),
            Arc::new(StringArray::from(vec!["Entity"; NODES])),
            Arc::new(StringArray::from(
                (0..NODES)
                    .map(|index| format!("n-{index:08}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let node_receipt = graph
        .publish_bulk_nodes(OperationId(Uuid::now_v7()), &[node_batch])
        .unwrap();
    let node_ids = node_receipt
        .column_by_name("entity_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();

    let edge_schema =
        bulk_edge_input_schema(vec![Field::new("weight", DataType::Float64, false)]).unwrap();
    let mut edge_ids = FixedSizeBinaryBuilder::with_capacity(EDGES, 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(EDGES, 16);
    let mut targets = FixedSizeBinaryBuilder::with_capacity(EDGES, 16);
    for index in 0..EDGES {
        edge_ids.append_null();
        sources.append_value(node_ids.value(index % NODES)).unwrap();
        targets
            .append_value(node_ids.value((index + 1) % NODES))
            .unwrap();
    }
    let edge_batch = RecordBatch::try_new(
        edge_schema,
        vec![
            Arc::new(edge_ids.finish()),
            Arc::new(StringArray::from(vec!["LINK"; EDGES])),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
            Arc::new(Float64Array::from(
                (0..EDGES).map(|index| index as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    graph
        .publish_bulk_edges(OperationId(Uuid::now_v7()), &[edge_batch])
        .unwrap();

    let generations = std::fs::read_dir(project.join("generations"))
        .unwrap()
        .count();
    assert!(
        generations <= 4,
        "expected a tiny generation count after two bulk publications, got {generations}"
    );
    let edge_rows = graph
        .execute("MATCH ()-[r:LINK]->() RETURN r")
        .unwrap()
        .batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    assert_eq!(edge_rows, EDGES);

    drop(graph);
    let reopened = GraphForge::new(Some(project.to_str().unwrap())).unwrap();
    let reopened_edges = reopened
        .execute("MATCH ()-[r:LINK]->() RETURN r")
        .unwrap()
        .batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    assert_eq!(reopened_edges, EDGES);
}
