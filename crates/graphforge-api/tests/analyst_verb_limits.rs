//! Analyst verbs accept ordinary graphs at their fixed limits (#1922).
//!
//! * A UUID node selector names one node, so it resolves by identity lookup and
//!   is not bounded by the 1,000,000-row cap of a topology scan.
//! * Local clustering coefficient is a single pass, so it is not bounded by the
//!   10,000-iteration budget that caps iterative algorithms, whether the graph
//!   has many nodes or one high-degree hub.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float64Array, ListArray,
    StringArray,
};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, GfError, GraphConstructionBudgets,
    GraphForge, NodeSelector, PathAlgorithm, PathsOptions, RankAlgorithm, RankOptions,
};
use graphforge_core::ClusteringNormalization;
use graphforge_core::uuid::Uuid;

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

use bulk_fixture::{WRITE_WINDOW, fixture_edge_uuid, fixture_node_uuid};

/// More nodes than the 1,000,000-row selector scan cap.
const BFS_NODES: usize = 1_000_100;

fn bfs_options(directed: bool) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::Bfs,
        directed,
        k: 1,
        via: None,
        weight: None,
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    }
}

fn path_uuids(batch: &RecordBatch, row: usize) -> Vec<[u8; 16]> {
    let paths = batch
        .column_by_name("path")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = paths.value(row);
    let values = values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..values.len())
        .map(|index| values.value(index).try_into().unwrap())
        .collect()
}

#[test]
fn uuid_selector_resolves_by_identity_on_a_graph_over_one_million_nodes() {
    let dir = tempfile::tempdir().unwrap();
    // Over a million nodes and a short chain: last -> 0 -> 1 -> 2 -> 3. The edges
    // stay few so the fixture cost is the node count a selector must not scan.
    write_graph(
        dir.path(),
        BFS_NODES,
        &[(BFS_NODES - 1, 0), (0, 1), (1, 2), (2, 3)],
    );
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();

    // The last node sits at the far end of the identity index, three nodes
    // before the target by way of the wrap-around edge.
    let last = NodeSelector::Uuid(fixture_node_uuid(BFS_NODES - 1));
    let target = NodeSelector::Uuid(fixture_node_uuid(3));
    let result = graph
        .paths(&last, Some(&target), bfs_options(true))
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let cost = result
        .column_by_name("cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(cost.value(0), 4.0);
    assert_eq!(
        path_uuids(&result, 0),
        [BFS_NODES - 1, 0, 1, 2, 3].map(|node| *fixture_node_uuid(node).as_bytes())
    );

    // A UUID that names no node keeps the typed validation refusal.
    let missing = NodeSelector::Uuid(Uuid::from_u128(0x7000_0000_0000_0000_0000_0000_0000_0001));
    assert!(matches!(
        graph.paths(&missing, Some(&target), bfs_options(true)),
        Err(GfError::Validation(message)) if message == "node selector matched no nodes"
    ));
    assert!(matches!(
        graph.paths(&last, Some(&missing), bfs_options(true)),
        Err(GfError::Validation(message)) if message == "node selector matched no nodes"
    ));
}

/// Nodes for the clustering fixture: more than the 10,000-iteration budget.
const LCC_NODES: usize = 10_500;
/// Out-neighbours of the hub (node 0): pair work far above any per-1,024 charge.
const HUB_NEIGHBOURS: usize = 3_000;

/// A directed graph with chains, chords, reciprocal pairs, a parallel edge, a
/// self-loop, and one hub whose neighbourhood alone costs millions of pairs.
fn clustering_edges() -> Vec<(usize, usize)> {
    let mut edges = Vec::new();
    for neighbour in 1..=HUB_NEIGHBOURS {
        edges.push((0, neighbour));
        if neighbour % 3 == 0 {
            edges.push((neighbour, 0));
        }
    }
    for node in 1..LCC_NODES - 1 {
        edges.push((node, node + 1));
        if node % 2 == 0 && node + 2 < LCC_NODES {
            edges.push((node, node + 2));
        }
        if node % 7 == 0 {
            edges.push((node + 1, node));
        }
    }
    edges.push((5, 6));
    edges.push((7, 7));
    edges
}

fn write_graph(dir: &Path, nodes: usize, edges: &[(usize, usize)]) {
    let forge = GraphForge::new(Some(dir.to_str().unwrap())).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WRITE_WINDOW,
            max_run_records: 4 * WRITE_WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..nodes).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(nodes);
        let mut identities = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        for node in start..end {
            identities
                .append_value(fixture_node_uuid(node).as_bytes())
                .unwrap();
        }
        let columns = vec![
            Arc::new(identities.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Entity"; end - start])),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&CONSTRUCTION_NODE_SCHEMA), columns).unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    for start in (0..edges.len()).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges.len());
        let mut identities = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        for (index, &(source, target)) in edges.iter().enumerate().take(end).skip(start) {
            identities
                .append_value(fixture_edge_uuid(index).as_bytes())
                .unwrap();
            sources
                .append_value(fixture_node_uuid(source).as_bytes())
                .unwrap();
            targets
                .append_value(fixture_node_uuid(target).as_bytes())
                .unwrap();
        }
        let columns = vec![
            Arc::new(identities.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["LINK"; end - start])),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&CONSTRUCTION_EDGE_SCHEMA), columns).unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
}

/// Local clustering coefficient from its definition, sharing no code with the
/// engine. Self-loops and parallel edges collapse; `directed = false` adds each
/// edge's reverse.
fn expected_clustering(
    nodes: usize,
    edges: &[(usize, usize)],
    directed: bool,
    normalization: ClusteringNormalization,
) -> Vec<f64> {
    let mut arcs: HashSet<(usize, usize)> = HashSet::new();
    for &(source, target) in edges {
        if source != target {
            arcs.insert((source, target));
            if !directed {
                arcs.insert((target, source));
            }
        }
    }
    let mut out: Vec<HashSet<usize>> = vec![HashSet::new(); nodes];
    let mut incoming: Vec<HashSet<usize>> = vec![HashSet::new(); nodes];
    for &(source, target) in &arcs {
        out[source].insert(target);
        incoming[target].insert(source);
    }
    // S = A + A^T, as sparse rows.
    let strength =
        |a: usize, b: usize| u64::from(arcs.contains(&(a, b))) + u64::from(arcs.contains(&(b, a)));
    (0..nodes)
        .map(|node| {
            let neighbours: HashSet<usize> = out[node].union(&incoming[node]).copied().collect();
            match normalization {
                ClusteringNormalization::NeighborEdges => {
                    let degree = neighbours.len() as u64;
                    let denominator = degree * degree.saturating_sub(1);
                    let edges_among = neighbours
                        .iter()
                        .map(|a| out[*a].iter().filter(|b| neighbours.contains(b)).count() as u64)
                        .sum::<u64>();
                    if denominator == 0 {
                        0.0
                    } else {
                        edges_among as f64 / denominator as f64
                    }
                }
                ClusteringNormalization::Fagiolo => {
                    // (S^3)_ii through the row of S^2: sum_j S_ij * S_j.
                    let mut squared: BTreeMap<usize, u64> = BTreeMap::new();
                    for &middle in &neighbours {
                        let weight = strength(node, middle);
                        let middle_neighbours: HashSet<usize> =
                            out[middle].union(&incoming[middle]).copied().collect();
                        for end in middle_neighbours {
                            *squared.entry(end).or_default() += weight * strength(middle, end);
                        }
                    }
                    let triangles: u64 = squared
                        .iter()
                        .map(|(&end, &paths)| paths * strength(end, node))
                        .sum();
                    let total_degree = (out[node].len() + incoming[node].len()) as u64;
                    let reciprocal = out[node]
                        .iter()
                        .filter(|&&n| out[n].contains(&node))
                        .count() as u64;
                    let denominator =
                        2 * (total_degree * total_degree.saturating_sub(1) - 2 * reciprocal);
                    if denominator == 0 {
                        0.0
                    } else {
                        triangles as f64 / denominator as f64
                    }
                }
            }
        })
        .collect()
}

fn scores_by_node(batch: &RecordBatch) -> HashMap<[u8; 16], f64> {
    let uuids = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let scores = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (uuids.value(row).try_into().unwrap(), scores.value(row)))
        .collect()
}

#[test]
fn clustering_coefficient_covers_graphs_beyond_the_iteration_budget() {
    let dir = tempfile::tempdir().unwrap();
    let edges = clustering_edges();
    write_graph(dir.path(), LCC_NODES, &edges);
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();

    let cases = [
        (true, ClusteringNormalization::Fagiolo),
        (true, ClusteringNormalization::NeighborEdges),
        (false, ClusteringNormalization::NeighborEdges),
    ];
    for (directed, normalization) in cases {
        let result = graph
            .rank(
                "Entity",
                RankOptions {
                    by: RankAlgorithm::ClusteringCoefficient,
                    directed,
                    clustering_normalization: Some(normalization),
                    ..RankOptions::default()
                },
            )
            .unwrap_or_else(|error| panic!("directed={directed} {normalization:?}: {error:?}"));
        assert_eq!(result.num_rows(), LCC_NODES);
        let expected = expected_clustering(LCC_NODES, &edges, directed, normalization);
        let actual = scores_by_node(&result);
        let mut nonzero = 0;
        for (node, expected) in expected.iter().enumerate() {
            let observed = actual[fixture_node_uuid(node).as_bytes()];
            assert_eq!(
                observed, *expected,
                "node {node} directed={directed} {normalization:?}"
            );
            nonzero += usize::from(*expected > 0.0);
        }
        // The hub and the chain around it close triangles; the check is not vacuous.
        assert!(nonzero > LCC_NODES / 2, "only {nonzero} nonzero scores");
        assert!(expected[0] > 0.0, "the hub closes triangles");
    }
}
