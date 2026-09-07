//! Storage publication followed by actual facade reopen, independent of import
//! session phase accounting. The public import-session case belongs to #1156.

use std::sync::Arc;

use arrow::array::{FixedSizeBinaryArray, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_storage::filesystem_admission::ProjectLifecycleMode;
use graphforge_storage::{ConstructionChunkKind, GraphConstructionSession};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, GraphConstructionBudgets, GraphForge,
};

fn ids(values: &[u128]) -> FixedSizeBinaryArray {
    FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.to_be_bytes())).unwrap()
}

#[test]
fn construction_ordinal_append_reopens_with_exact_nodes_and_edge_endpoints() {
    let root = TempDir::new().unwrap();
    let project = root.path().join("project");
    drop(GraphForge::new(project.to_str()).unwrap());
    for generation in 1..=4_u64 {
        let selected = graphforge_storage::resolve_project_generation(&project).unwrap();
        let source = TempDir::new().unwrap();
        let graph_root = source.path().join("graph");
        std::fs::create_dir(&graph_root).unwrap();
        if let Some(inventory) = selected.graph_files_inventory().unwrap() {
            graphforge_storage::materialize_graph_objects(&project, &inventory, &graph_root)
                .unwrap();
        }
        let mut session = GraphConstructionSession::open_with_mode_and_lifecycle_from_graph(
            &project,
            &graph_root,
            Uuid::new_v4(),
            generation - 1,
            graphforge_core::OntologyMode::Exploratory,
            GraphConstructionBudgets {
                max_batch_rows: 2,
                max_run_records: 8,
                ..Default::default()
            },
            ProjectLifecycleMode::Durable,
        )
        .unwrap();
        let node = RecordBatch::try_new(
            CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![
                Arc::new(ids(&[u128::from(generation)])),
                Arc::new(StringArray::from(vec!["Person"])),
            ],
        )
        .unwrap();
        session
            .append(ConstructionChunkKind::Node, "node", &node)
            .unwrap();
        if generation > 1 {
            let edge = RecordBatch::try_new(
                CONSTRUCTION_EDGE_SCHEMA.clone(),
                vec![
                    Arc::new(ids(&[100 + u128::from(generation)])),
                    Arc::new(StringArray::from(vec!["R"])),
                    Arc::new(ids(&[u128::from(generation - 1)])),
                    Arc::new(ids(&[u128::from(generation)])),
                ],
            )
            .unwrap();
            session
                .append(ConstructionChunkKind::Edge, "edge", &edge)
                .unwrap();
        }
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        assert_eq!(shape.node_count, generation);
        assert_eq!(shape.edge_count, generation - 1);
        let encoded = session.encode_canonical(&shape, generation).unwrap();
        session
            .publish_canonical(&encoded, Uuid::new_v4(), Uuid::new_v4())
            .unwrap();
        drop(session);
        drop(selected);

        let reopened = GraphForge::new(project.to_str()).unwrap();
        assert_eq!(reopened.node_count("Person").unwrap(), generation);
        let rows = reopened.execute("MATCH (a:Person)-[r:R]->(b:Person) RETURN a.node_uuid AS source, r.edge_uuid AS edge, b.node_uuid AS target ORDER BY edge").unwrap();
        let actual = rows
            .batches
            .iter()
            .flat_map(|batch| {
                let source = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                let edge = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                let target = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| {
                        (
                            Uuid::from_slice(source.value(row)).unwrap(),
                            Uuid::from_slice(edge.value(row)).unwrap(),
                            Uuid::from_slice(target.value(row)).unwrap(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let expected = (2..=generation)
            .map(|id| {
                (
                    Uuid::from_u128(u128::from(id - 1)),
                    Uuid::from_u128(100 + u128::from(id)),
                    Uuid::from_u128(u128::from(id)),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}
