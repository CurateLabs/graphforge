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
        .execute_with_params(
            "MATCH (n) WHERE n.node_uuid = $id DETACH DELETE n",
            &param(id),
        )
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
    let (e1, e2, e3) = (uuid(2_001), uuid(2_002), uuid(2_005));
    graph
        .publish_bulk_nodes(
            operation(10),
            &[node_batch(
                &[a, b, c],
                &["P", "P", "P"],
                &[None, None, None],
            )],
        )
        .unwrap();
    graph
        .publish_bulk_edges(
            operation(11),
            &[edge_batch(
                &[e1, e2, e3],
                &["R", "R", "R"],
                &[a, b, b],
                &[b, c, a],
            )],
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
                &[edge_batch(&[e3], &["R"], &[a], &[b])],
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
            outcome(
                graph.validate_bulk_nodes(operation(19), &[node_batch(&[e3], &["P"], &[None])]),
            ),
        ),
        (
            "node uuid == deleted edge",
            outcome(
                graph.validate_bulk_nodes(operation(20), &[node_batch(&[e1], &["P"], &[None])]),
            ),
        ),
        (
            "edge re-add cascade-deleted by DETACH",
            outcome(graph.validate_bulk_edges(
                operation(25),
                &[edge_batch(&[e2], &["R"], &[a], &[b])],
                &empty,
            )),
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
    let conflict = |kind: &str, field: &str| {
        format!(
            "IdentityConflict: GF_BULK_VALIDATION(identity_conflict): bulk {kind} row 0 field \"{field}\": duplicate or existing UUID"
        )
    };
    let missing = |field: &str| {
        format!(
            "MissingEndpoint: GF_BULK_VALIDATION(missing_endpoint): bulk edge row 0 field \"{field}\": endpoint does not exist"
        )
    };
    let expected = [
        ("deleted node re-add", conflict("node", "node_uuid")),
        ("live node dup", conflict("node", "node_uuid")),
        ("deleted edge re-add", conflict("edge", "edge_uuid")),
        ("live edge dup", conflict("edge", "edge_uuid")),
        ("edge uuid == live node", conflict("edge", "edge_uuid")),
        ("edge uuid == deleted node", conflict("edge", "edge_uuid")),
        ("node uuid == live edge", conflict("node", "node_uuid")),
        ("node uuid == deleted edge", conflict("node", "node_uuid")),
        ("edge to deleted node", missing("target_uuid")),
        ("edge from never-seen node", missing("source_uuid")),
    ];
    let observed = rows
        .iter()
        .filter(|(name, _)| *name != "edge re-add cascade-deleted by DETACH")
        .map(|(name, result)| (*name, result.clone()))
        .collect::<Vec<_>>();
    assert_eq!(observed.len(), expected.len());
    for ((name, got), (expected_name, want)) in observed.iter().zip(expected.iter()) {
        assert_eq!(name, expected_name);
        assert_eq!(got, want, "{name}");
    }
    let cascaded = rows
        .iter()
        .find(|(name, _)| *name == "edge re-add cascade-deleted by DETACH")
        .unwrap();
    assert_eq!(cascaded.1, conflict("edge", "edge_uuid"));
}

/// A UUID is spent for good once an entity holds it: validation refuses the
/// identity of a deleted entity with the same typed conflict the commit gives,
/// so validation never accepts what the commit rejects.
#[test]
fn deleted_identities_are_not_reused_at_commit() {
    let (_directory, graph) = project();
    let (a, b, c) = (uuid(1_001), uuid(1_002), uuid(1_003));
    let (e1, e2) = (uuid(2_001), uuid(2_002));
    graph
        .publish_bulk_nodes(
            operation(10),
            &[node_batch(
                &[a, b, c],
                &["P", "P", "P"],
                &[None, None, None],
            )],
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
    let node = graph.publish_bulk_nodes(operation(22), &[node_batch(&[c], &["P"], &[None])]);
    assert!(node.is_err(), "deleted node UUID must stay spent");
    let edge = graph.publish_bulk_edges(operation(23), &[edge_batch(&[e1], &["R"], &[a], &[b])]);
    assert!(edge.is_err(), "deleted edge UUID must stay spent");
    let cascaded =
        graph.publish_bulk_edges(operation(24), &[edge_batch(&[e2], &["R"], &[a], &[b])]);
    assert!(
        cascaded.is_err(),
        "cascade-deleted edge UUID must stay spent"
    );
    // The refusal is durable: a fresh open sees the same spent identities.
    drop(graph);
    let reopened = GraphForge::new(Some(_directory.path().to_str().unwrap())).unwrap();
    assert!(
        reopened
            .publish_bulk_nodes(operation(25), &[node_batch(&[c], &["P"], &[None])])
            .is_err()
    );
    assert!(
        reopened
            .publish_bulk_edges(operation(26), &[edge_batch(&[e1], &["R"], &[a], &[b])])
            .is_err()
    );
}
