//! Small real-facade fixture for production fan-in attribution (#1282).
//! Environment-selected larger cases are preselected by the diagnostic runner.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, GraphConstructionBudgets, GraphForge,
};

fn fixed(id: usize) -> Arc<FixedSizeBinaryArray> {
    Arc::new(FixedSizeBinaryArray::try_from_iter([(id as u128).to_be_bytes()].into_iter()).unwrap())
}

fn selected_count(name: &str) -> usize {
    let count = std::env::var(name).map_or(33, |v| v.parse().unwrap());
    assert!((2..=1088).contains(&count));
    count
}

#[test]
fn ingestion_family_boundaries_preserve_facade_results() {
    let temporary = tempfile::TempDir::new().unwrap();
    let project = std::env::var("GF_1282_PROJECT").unwrap_or_else(|_| {
        temporary
            .path()
            .join("project")
            .to_str()
            .unwrap()
            .to_owned()
    });
    assert!(!std::path::Path::new(&project).exists());
    let nodes = selected_count("GF_1282_NODES");
    let edges = selected_count("GF_1282_EDGES");
    let graph = GraphForge::new(Some(&project)).unwrap();
    let budgets = GraphConstructionBudgets {
        max_batch_rows: 1,
        max_run_records: 4,
        ..Default::default()
    };
    let start = Instant::now();
    let mut construction = graph.begin_graph_construction(budgets).unwrap();
    for index in 0..nodes {
        let batch = RecordBatch::try_new(
            CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![fixed(index + 1), Arc::new(StringArray::from(vec!["Node"]))],
        )
        .unwrap();
        construction
            .append_nodes(&format!("node-{index}"), &batch)
            .unwrap();
    }
    let topology = (0..edges)
        .map(|index| (index % nodes + 1, (index + 1) % nodes + 1))
        .collect::<Vec<_>>();
    for (index, (source, target)) in topology.iter().enumerate() {
        let batch = RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                fixed(1_000_000 + index),
                Arc::new(StringArray::from(vec!["LINK"])),
                fixed(*source),
                fixed(*target),
            ],
        )
        .unwrap();
        construction
            .append_edges(&format!("edge-{index}"), &batch)
            .unwrap();
    }
    let append_ns = start.elapsed().as_nanos();
    let seal = Instant::now();
    construction.seal_and_publish().unwrap();
    let seal_ns = seal.elapsed().as_nanos();
    let progress = construction.progress();
    assert!(progress.publication_committed);
    assert_eq!(progress.accepted_chunks, (nodes + edges) as u64);
    let evidence = serde_json::to_value(&progress.evidence).unwrap();
    assert!(!evidence.to_string().contains("inclusive_wall_ns"));
    drop(construction);
    drop(graph);
    let graph = GraphForge::new(Some(&project)).unwrap();
    for (query, expected) in [
        ("MATCH (n) RETURN count(n)", nodes),
        ("MATCH ()-[r]->() RETURN count(r)", edges),
    ] {
        let result = graph.execute(query).unwrap();
        let array = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(array.value(0), expected as i64);
    }
    let mut one_hop = topology
        .iter()
        .map(|(_, target)| *target)
        .collect::<Vec<_>>();
    let mut two_hop = Vec::new();
    for (first, (_, middle)) in topology.iter().enumerate() {
        for (second, (source, target)) in topology.iter().enumerate() {
            if first != second && middle == source {
                two_hop.push(*target);
            }
        }
    }
    for (query, expected) in [
        (
            "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000",
            &mut one_hop,
        ),
        (
            "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000",
            &mut two_hop,
        ),
    ] {
        expected.sort_unstable();
        expected.truncate(1000);
        let result = graph.execute(query).unwrap();
        let actual = result
            .batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| u128::from_be_bytes(values.value(row).try_into().unwrap()) as usize)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, *expected);
    }
    println!(
        "INGEST_BOUNDARY {}",
        serde_json::json!({"nodes":nodes,"edges":edges,
        "append_ns":append_ns,"seal_publish_ns":seal_ns,"evidence":evidence})
    );
}
