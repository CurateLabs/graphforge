//! A lazy traversal retains the generation that admitted its route readers.

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

use crate::{ExecutionResourcePolicy, GfError, GraphForge, GraphForgeOptions};

type EdgeRow = ([u8; 16], [u8; 16], [u8; 16], i64);

fn edge_rows(batches: &[RecordBatch]) -> Vec<EdgeRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let identities = ["source", "edge", "target"].map(|name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
        });
        let cost = batch
            .column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!cost.is_null(row));
            let [source, edge, target] = identities.map(|values| {
                assert!(!values.is_null(row));
                <[u8; 16]>::try_from(values.value(row)).unwrap()
            });
            rows.push((source, edge, target, cost.value(row)));
        }
    }
    rows.sort_unstable();
    rows
}

#[test]
fn mapped_warm_lazy_traversal_keeps_old_rows_after_same_owner_publication() {
    assert_retained_traversal(false);
}

#[test]
fn mapped_cold_lazy_traversal_keeps_old_rows_after_same_owner_publication() {
    assert_retained_traversal(true);
}

fn assert_retained_traversal(cold: bool) {
    let project = tempfile::tempdir().unwrap();
    let graph = GraphForge::new_with_options(
        project.path().to_str(),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    graph
        .execute("CREATE (a:AUX {name: 'a'}), (b:AUX {name: 'b'}) CREATE (a)-[:CON {cost: 1}]->(b)")
        .unwrap();
    const QUERY: &str = "MATCH (a:AUX)-[r:CON]->(b:AUX) RETURN a.node_uuid AS source, r.edge_uuid AS edge, b.node_uuid AS target, r.cost AS cost";
    let original = edge_rows(&graph.execute(QUERY).unwrap().batches);
    assert_eq!(original.len(), 1);
    assert_eq!(original[0].3, 1);
    let old_inventory = graph.property_inventory_for_session();
    if cold {
        // Remove the eager oracle query's cached adjacency before creating the
        // lazy traversal, so its first build must read the retained inventory.
        graph.adjacency_provider_for_session().invalidate();
    }
    let stream = graph.execute_stream(QUERY).unwrap();
    // No consumer polling occurs before both later publications. One partition
    // also avoids a repartition worker eagerly draining the traversal input.
    graph
        .execute("MATCH ()-[r:CON]->() SET r.cost = 2")
        .unwrap();
    graph
        .execute("MATCH (a:AUX {name: 'a'}), (b:AUX {name: 'b'}) CREATE (a)-[:CON {cost: 3}]->(b)")
        .unwrap();
    assert_ne!(
        old_inventory.generation_uuid(),
        graph.property_inventory_for_session().generation_uuid()
    );
    let retained: Vec<RecordBatch> = graph
        .block_on(async {
            stream
                .try_collect()
                .await
                .map_err(|error| GfError::Execution(error.to_string()))
        })
        .unwrap();
    assert_eq!(edge_rows(&retained), original);
    let current = edge_rows(&graph.execute(QUERY).unwrap().batches);
    assert_eq!(current.len(), 2);
    assert!(current.contains(&(original[0].0, original[0].1, original[0].2, 2)));
    let added = current.iter().find(|row| row.1 != original[0].1).unwrap();
    assert_eq!(
        (added.0, added.2, added.3),
        (original[0].0, original[0].2, 3)
    );
    drop(graph);
    let reopened = GraphForge::new(project.path().to_str()).unwrap();
    assert_eq!(
        edge_rows(&reopened.execute(QUERY).unwrap().batches),
        current
    );
}
