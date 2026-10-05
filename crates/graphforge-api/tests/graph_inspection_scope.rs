//! Public inspection helpers should scan only the entity kind they report.

use graphforge_api::GraphForge;
use graphforge_storage::concurrency_attribution::{RegionCapture, RegionSnapshot};
use tempfile::TempDir;

fn create_fixture(node_count: usize, edge_count: usize) -> (TempDir, GraphForge) {
    let directory = TempDir::new().expect("fixture directory");
    let path = directory.path().to_str().expect("UTF-8 fixture path");
    let graph = GraphForge::new(Some(path)).expect("persistent graph");

    let mut nodes = vec![
        "(a:Anchor {key: 0})".to_owned(),
        "(b:Anchor {key: 1})".to_owned(),
    ];
    nodes.extend((2..node_count).map(|key| format!("(:Other {{key: {key}}})")));
    graph
        .execute(&format!("CREATE {}", nodes.join(", ")))
        .expect("create fixture nodes");

    if edge_count > 0 {
        let edges = std::iter::repeat_n("(a)-[:LINK]->(b)", edge_count)
            .collect::<Vec<_>>()
            .join(", ");
        graph
            .execute(&format!(
                "MATCH (a:Anchor {{key: 0}}), (b:Anchor {{key: 1}}) CREATE {edges}"
            ))
            .expect("create fixture relationships");
    }

    (directory, graph)
}

fn captured(operation: impl FnOnce(), name: &str) -> (u64, RegionSnapshot) {
    let capture = RegionCapture::start("facade_inspection");
    operation();
    let snapshot = capture.finish();
    let rows = snapshot
        .regions
        .get(name)
        .and_then(|region| region.work.get("rows"))
        .copied()
        .unwrap_or(0);
    (rows, snapshot)
}

fn assert_no_work(snapshot: &RegionSnapshot, region: &str) {
    assert_eq!(
        snapshot
            .regions
            .get(region)
            .and_then(|row| row.work.get("rows"))
            .copied()
            .unwrap_or(0),
        0,
        "unexpected logical scan in {region}: {snapshot:?}"
    );
}

#[test]
fn labels_and_node_count_do_not_scan_relationships_as_edge_count_grows() {
    let (_small_dir, small) = create_fixture(4, 2);
    let (_large_dir, large) = create_fixture(4, 64);

    for graph in [&small, &large] {
        let (node_rows, labels) = captured(
            || assert_eq!(graph.labels().unwrap(), ["Anchor", "Other"]),
            "facade_inspection/graph_inspection/nodes",
        );
        assert_eq!(node_rows, 4);
        assert_no_work(&labels, "facade_inspection/graph_inspection/relationships");

        let (node_rows, count) = captured(
            || assert_eq!(graph.node_count("Anchor").unwrap(), 2),
            "facade_inspection/graph_inspection/nodes",
        );
        assert_eq!(node_rows, 4);
        assert_no_work(&count, "facade_inspection/graph_inspection/relationships");
    }
}

#[test]
fn relationship_types_do_not_enumerate_nodes_as_node_count_grows() {
    let (_small_dir, small) = create_fixture(4, 3);
    let (_large_dir, large) = create_fixture(40, 3);

    for graph in [&small, &large] {
        let (edge_rows, types) = captured(
            || assert_eq!(graph.relationship_types().unwrap(), ["LINK"]),
            "facade_inspection/graph_inspection/relationships",
        );
        assert_eq!(edge_rows, 3);
        assert_no_work(&types, "facade_inspection/graph_inspection/nodes");
    }
}

#[test]
fn mutation_reopen_and_combined_inspections_keep_their_contracts() {
    let directory = TempDir::new().expect("fixture directory");
    let path = directory.path().to_str().expect("UTF-8 fixture path");
    let graph = GraphForge::new(Some(path)).expect("persistent graph");
    graph
        .execute("CREATE (:Before), (:Before)")
        .expect("seed nodes");
    graph
        .execute("MATCH (a:Before), (b:Before) CREATE (a)-[:OLD]->(b)")
        .expect("seed relationship");
    graph.execute("CREATE (:After)").expect("mutate nodes");
    graph
        .execute("MATCH (a:Before), (b:After) CREATE (a)-[:NEW]->(b)")
        .expect("mutate relationships");
    drop(graph);

    let reopened = GraphForge::new(Some(path)).expect("reopen graph");
    assert_eq!(reopened.labels().unwrap(), ["After", "Before"]);
    assert_eq!(reopened.relationship_types().unwrap(), ["NEW", "OLD"]);
    assert_eq!(reopened.node_count("").unwrap(), 3);
    assert_eq!(reopened.node_count("Before").unwrap(), 2);

    let (schema_rows, schema) = captured(
        || assert_eq!(reopened.schema().unwrap().num_rows(), 4),
        "facade_inspection/graph_inspection/nodes",
    );
    assert_eq!(schema_rows, 3);
    assert_eq!(
        schema
            .regions
            .get("facade_inspection/graph_inspection/relationships")
            .and_then(|region| region.work.get("rows")),
        Some(&6)
    );

    let (gsi_nodes, gsi) = captured(
        || {
            let profile = reopened.profile_gsi().unwrap();
            assert_eq!(profile.node_count, 3);
            assert_eq!(profile.edge_count, 6);
        },
        "facade_inspection/graph_inspection/nodes",
    );
    assert_eq!(gsi_nodes, 3);
    assert_eq!(
        gsi.regions
            .get("facade_inspection/graph_inspection/relationships")
            .and_then(|region| region.work.get("rows")),
        Some(&6)
    );
}
