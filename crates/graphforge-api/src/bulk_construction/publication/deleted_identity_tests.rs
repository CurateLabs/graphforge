//! Append validation against deleted and live identities (#1902).
//!
//! These pin what a bulk append may do with a UUID the graph once held, so the
//! source of the existing-identity check can change without changing answers.

use super::super::tests::edge_batch;
use super::super::tests::node_batch;
use super::super::tests::operation;
use super::super::tests::uuid;
use super::super::*;
use graphforge_ir::IrLiteral;
use std::collections::HashMap;

fn param(id: Uuid) -> HashMap<String, IrLiteral> {
    HashMap::from([("id".to_owned(), IrLiteral::Uuid(*id.as_bytes()))])
}

fn delete_node(graph: &GraphForge, id: Uuid) {
    graph
        .execute_with_params("MATCH (n) WHERE n.node_uuid = $id DELETE n", &param(id))
        .unwrap();
}

fn delete_edge(graph: &GraphForge, source: Uuid, target: Uuid) {
    graph
        .execute_with_params(
            "MATCH (a)-[r]->(b) WHERE a.node_uuid = $a AND b.node_uuid = $b DELETE r",
            &HashMap::from([
                ("a".to_owned(), IrLiteral::Uuid(*source.as_bytes())),
                ("b".to_owned(), IrLiteral::Uuid(*target.as_bytes())),
            ]),
        )
        .unwrap();
}

fn project() -> (tempfile::TempDir, GraphForge) {
    let directory = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
    (directory, graph)
}

fn outcome<T>(result: Result<T, BulkValidationError>) -> String {
    match result {
        Ok(_) => "accepted".to_owned(),
        Err(error) => format!("{:?}: {error}", error.reason),
    }
}

#[test]
fn probe_outcomes_for_deleted_and_live_identities() {
    let (_directory, graph) = project();
    let (a, b, c) = (uuid(1_001), uuid(1_002), uuid(1_003));
    let (e1, e2) = (uuid(2_001), uuid(2_002));
    graph
        .publish_bulk_nodes(
            operation(10),
            &[node_batch(&[a, b, c], &["P", "P", "P"], &[None, None, None])],
        )
        .unwrap();
    graph
        .publish_bulk_edges(
            operation(11),
            &[edge_batch(&[e1, e2], &["R", "R"], &[a, b], &[b, c])],
        )
        .unwrap();
    delete_edge(&graph, a, b);
    delete_node(&graph, c);
    let empty = graph.validate_bulk_nodes(operation(14), &[]).unwrap();
    let rows = [
        (
            "deleted node re-add",
            outcome(graph.validate_bulk_nodes(operation(12), &[node_batch(&[c], &["P"], &[None])])),
        ),
        (
            "live node dup",
            outcome(graph.validate_bulk_nodes(operation(13), &[node_batch(&[a], &["P"], &[None])])),
        ),
        (
            "deleted edge re-add",
            outcome(graph.validate_bulk_edges(
                operation(15),
                &[edge_batch(&[e1], &["R"], &[a], &[b])],
                &empty,
            )),
        ),
        (
            "live edge dup",
            outcome(graph.validate_bulk_edges(
                operation(16),
                &[edge_batch(&[e2], &["R"], &[a], &[b])],
                &empty,
            )),
        ),
        (
            "edge uuid == live node",
            outcome(graph.validate_bulk_edges(
                operation(17),
                &[edge_batch(&[a], &["R"], &[a], &[b])],
                &empty,
            )),
        ),
        (
            "edge uuid == deleted node",
            outcome(graph.validate_bulk_edges(
                operation(18),
                &[edge_batch(&[c], &["R"], &[a], &[b])],
                &empty,
            )),
        ),
        (
            "node uuid == live edge",
            outcome(graph.validate_bulk_nodes(operation(19), &[node_batch(&[e2], &["P"], &[None])])),
        ),
        (
            "node uuid == deleted edge",
            outcome(graph.validate_bulk_nodes(operation(20), &[node_batch(&[e1], &["P"], &[None])])),
        ),
        (
            "edge to deleted node",
            outcome(graph.validate_bulk_edges(
                operation(21),
                &[edge_batch(&[uuid(2_003)], &["R"], &[a], &[c])],
                &empty,
            )),
        ),
        (
            "edge from never-seen node",
            outcome(graph.validate_bulk_edges(
                operation(24),
                &[edge_batch(&[uuid(2_004)], &["R"], &[uuid(9_999)], &[a])],
                &empty,
            )),
        ),
    ];
    for (name, result) in &rows {
        eprintln!("PIN {name}: {result}");
    }
    let publish = graph.publish_bulk_nodes(operation(22), &[node_batch(&[c], &["P"], &[None])]);
    eprintln!("PIN publish deleted node re-add: {:?}", publish.map(|_| ()).map_err(|e| e.to_string()));
    let publish = graph.publish_bulk_edges(
        operation(23),
        &[edge_batch(&[e1], &["R"], &[a], &[b])],
    );
    eprintln!("PIN publish deleted edge re-add: {:?}", publish.map(|_| ()).map_err(|e| e.to_string()));
}
