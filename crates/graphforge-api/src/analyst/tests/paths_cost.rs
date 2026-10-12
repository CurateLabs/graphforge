//! `bfs_depth` and `dijkstra_cost` through the public facade (#1974): the same
//! rows and cost bits as `bfs` and `dijkstra`, without the `path` column.

use super::*;

fn cost_options(by: PathAlgorithm, directed: bool, weight: Option<&str>) -> PathsOptions {
    PathsOptions {
        by,
        directed,
        via: Some("ROAD".to_owned()),
        weight: weight.map(str::to_owned),
        ..PathsOptions::default()
    }
}

/// `(source, target, cost bits)` per row.
fn cost_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<([u8; 16], [u8; 16], u64)> {
    let costs = batch
        .column_by_name("cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| {
            (
                uuid_column(batch, "source_uuid")
                    .value(row)
                    .try_into()
                    .unwrap(),
                uuid_column(batch, "target_uuid")
                    .value(row)
                    .try_into()
                    .unwrap(),
                costs.value(row).to_bits(),
            )
        })
        .collect()
}

fn road_graph() -> (GraphForge, [NodeHandle; 6]) {
    let graph = GraphForge::new(None).unwrap();
    let nodes =
        ["Alice", "Bob", "Carol", "Dan", "Eve", "Frank"].map(|name| add_person(&graph, name));
    // Frank is on a separate road with no way in; Eve is reachable only against
    // the direction of travel.
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Frank'}) \
                 CREATE (a)-[:ROAD {cost:0.1}]->(c), (a)-[:ROAD {cost:0.2}]->(b), \
                 (b)-[:ROAD {cost:0.7}]->(d), (c)-[:ROAD {cost:0.6}]->(d), \
                 (a)-[:ROAD {cost:9.0}]->(d), (e)-[:ROAD {cost:0.3}]->(a), \
                 (d)-[:ROAD {cost:0.3}]->(d), (f)-[:ROAD {cost:1.5}]->(f)",
        )
        .unwrap();
    (graph, nodes)
}

#[test]
fn cost_only_paths_return_the_costs_of_bfs_and_dijkstra_without_a_path_column() {
    let (graph, nodes) = road_graph();
    let source = NodeSelector::Handle(nodes[0].clone());
    for directed in [true, false] {
        let bfs = graph
            .paths(
                &source,
                None,
                cost_options(PathAlgorithm::Bfs, directed, None),
            )
            .unwrap();
        let depth = graph
            .paths(
                &source,
                None,
                cost_options(PathAlgorithm::BfsDepth, directed, None),
            )
            .unwrap();
        let dijkstra = graph
            .paths(
                &source,
                None,
                cost_options(PathAlgorithm::Dijkstra, directed, Some("cost")),
            )
            .unwrap();
        let cost = graph
            .paths(
                &source,
                None,
                cost_options(PathAlgorithm::DijkstraCost, directed, Some("cost")),
            )
            .unwrap();

        assert_eq!(cost_rows(&depth), cost_rows(&bfs), "directed={directed}");
        assert_eq!(
            cost_rows(&cost),
            cost_rows(&dijkstra),
            "directed={directed}"
        );
        // Frank is unreachable from Alice in either direction; Eve only undirected.
        assert_eq!(depth.num_rows(), if directed { 4 } else { 5 });
        for (batch, name) in [(&depth, "bfs_depth"), (&cost, "dijkstra_cost")] {
            assert_eq!(batch.schema().metadata()["graphforge.algorithm"], name);
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| (field.name().as_str(), field.is_nullable()))
                    .collect::<Vec<_>>(),
                [
                    ("source_uuid", false),
                    ("target_uuid", false),
                    ("cost", false)
                ]
            );
        }

        for target in &nodes {
            let selector = NodeSelector::Handle(target.clone());
            for (by, weight, full) in [
                (PathAlgorithm::BfsDepth, None, &depth),
                (PathAlgorithm::DijkstraCost, Some("cost"), &cost),
            ] {
                let one = graph
                    .paths(&source, Some(&selector), cost_options(by, directed, weight))
                    .unwrap();
                let expected: Vec<_> = cost_rows(full)
                    .into_iter()
                    .filter(|row| row.1 == *target.uuid.as_bytes())
                    .collect();
                assert_eq!(cost_rows(&one), expected, "{by} directed={directed}");
            }
        }
    }
}

#[test]
fn cost_only_paths_keep_the_bfs_and_dijkstra_option_rules() {
    let (graph, nodes) = road_graph();
    let source = NodeSelector::Handle(nodes[0].clone());
    // No weight for a depth, and the weight rules of dijkstra for a cost.
    for options in [
        cost_options(PathAlgorithm::BfsDepth, true, Some("cost")),
        PathsOptions {
            k: 2,
            ..cost_options(PathAlgorithm::BfsDepth, true, None)
        },
        PathsOptions {
            k: 2,
            ..cost_options(PathAlgorithm::DijkstraCost, true, Some("cost"))
        },
        cost_options(PathAlgorithm::DijkstraCost, true, Some(" ")),
        cost_options(PathAlgorithm::DijkstraCost, true, Some("missing")),
    ] {
        assert!(
            matches!(
                graph.paths(&source, None, options.clone()),
                Err(GfError::Validation(_))
            ),
            "{:?}",
            options.by
        );
    }
    // An unweighted cost is the hop count.
    let unit = graph
        .paths(
            &source,
            None,
            cost_options(PathAlgorithm::DijkstraCost, true, None),
        )
        .unwrap();
    let depth = graph
        .paths(
            &source,
            None,
            cost_options(PathAlgorithm::BfsDepth, true, None),
        )
        .unwrap();
    assert_eq!(cost_rows(&unit), cost_rows(&depth));
    // A negative weight is refused, as `dijkstra` refuses it.
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:SLOPE {cost:-1.0}]->(b)",
        )
        .unwrap();
    let negative = PathsOptions {
        via: Some("SLOPE".to_owned()),
        ..cost_options(PathAlgorithm::DijkstraCost, true, Some("cost"))
    };
    assert!(matches!(
        graph.paths(&source, None, negative),
        Err(GfError::Algorithm(AlgorithmError::Execution { .. }))
    ));
}
