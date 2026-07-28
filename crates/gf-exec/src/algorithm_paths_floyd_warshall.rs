use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;
use crate::algorithm_paths_dijkstra::DijkstraPath;

const CHECKPOINT_INTERVAL: usize = 4_096;

#[derive(Clone, Debug)]
struct BestPath {
    cost: f64,
    nodes: Vec<u64>,
    edges: Vec<u64>,
}

pub(crate) fn exact_floyd_warshall(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<DijkstraPath>, AlgorithmError> {
    control.checkpoint()?;
    let mut work = 0_usize;
    let mut best = HashMap::new();

    for &node in graph.node_ids() {
        best.insert(
            (node, node),
            BestPath {
                cost: 0.0,
                nodes: vec![node],
                edges: Vec::new(),
            },
        );
        for edge in graph.neighbors(node) {
            checkpoint(control, &mut work)?;
            if !edge.weight.is_finite() {
                return Err(execution("floyd_warshall requires finite edge weights"));
            }
            let candidate = BestPath {
                cost: edge.weight,
                nodes: vec![node, edge.neighbor_id],
                edges: vec![edge.edge_id],
            };
            if improves(&candidate, best.get(&(node, edge.neighbor_id))) {
                best.insert((node, edge.neighbor_id), candidate);
            }
        }
    }
    detect_negative_cycle(&best)?;

    for &middle in graph.node_ids() {
        checkpoint(control, &mut work)?;
        for &source in graph.node_ids() {
            checkpoint(control, &mut work)?;
            let Some(left) = best.get(&(source, middle)).cloned() else {
                continue;
            };
            for &target in graph.node_ids() {
                checkpoint(control, &mut work)?;
                let Some(right) = best.get(&(middle, target)).cloned() else {
                    continue;
                };
                let candidate = concatenate(&left, &right)?;
                if source != target && !is_simple(&candidate.nodes) {
                    continue;
                }
                if improves(&candidate, best.get(&(source, target))) {
                    best.insert((source, target), candidate);
                }
            }
        }
        detect_negative_cycle(&best)?;
    }

    let mut pairs = best
        .into_iter()
        .filter(|((source, target), _)| source != target)
        .collect::<Vec<_>>();
    control.check_output_rows(pairs.len())?;
    pairs.sort_unstable_by_key(|((source, target), _)| (*source, *target));
    Ok(pairs
        .into_iter()
        .map(|((source, target), path)| DijkstraPath {
            source,
            target,
            cost: path.cost,
            nodes: path.nodes,
        })
        .collect())
}

fn concatenate(left: &BestPath, right: &BestPath) -> Result<BestPath, AlgorithmError> {
    let cost = left.cost + right.cost;
    if !cost.is_finite() {
        return Err(execution("floyd_warshall accumulated cost is not finite"));
    }
    let mut nodes = left.nodes.clone();
    nodes.extend_from_slice(&right.nodes[1..]);
    let mut edges = left.edges.clone();
    edges.extend_from_slice(&right.edges);
    Ok(BestPath { cost, nodes, edges })
}

fn improves(candidate: &BestPath, known: Option<&BestPath>) -> bool {
    known.is_none_or(|known| {
        candidate.cost.total_cmp(&known.cost) == Ordering::Less
            || (candidate.cost.total_cmp(&known.cost) == Ordering::Equal
                && (candidate.nodes.as_slice(), candidate.edges.as_slice())
                    < (known.nodes.as_slice(), known.edges.as_slice()))
    })
}

fn is_simple(nodes: &[u64]) -> bool {
    let mut seen = HashSet::with_capacity(nodes.len());
    nodes.iter().all(|node| seen.insert(*node))
}

fn detect_negative_cycle(best: &HashMap<(u64, u64), BestPath>) -> Result<(), AlgorithmError> {
    if best
        .iter()
        .any(|((source, target), path)| source == target && path.cost < 0.0)
    {
        Err(execution(
            "floyd_warshall found a negative cycle in the selected graph",
        ))
    } else {
        Ok(())
    }
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work += 1;
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    Ok(())
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    #[test]
    fn negative_edges_produce_exact_ordered_reachable_pairs() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            5,
            &[(0, 2), (0, 1), (1, 2), (1, 3), (2, 3), (3, 4)],
        )
        .with_test_edge_weights(&[5.0, 4.0, -2.0, 6.0, 3.0, -1.0]);
        let paths = exact_floyd_warshall(&graph, &control()).unwrap();
        assert_eq!(
            paths
                .iter()
                .map(|path| (path.source, path.target, path.cost, path.nodes.as_slice()))
                .collect::<Vec<_>>(),
            vec![
                (0, 1, 4.0, [0, 1].as_slice()),
                (0, 2, 2.0, [0, 1, 2].as_slice()),
                (0, 3, 5.0, [0, 1, 2, 3].as_slice()),
                (0, 4, 4.0, [0, 1, 2, 3, 4].as_slice()),
                (1, 2, -2.0, [1, 2].as_slice()),
                (1, 3, 1.0, [1, 2, 3].as_slice()),
                (1, 4, 0.0, [1, 2, 3, 4].as_slice()),
                (2, 3, 3.0, [2, 3].as_slice()),
                (2, 4, 2.0, [2, 3, 4].as_slice()),
                (3, 4, -1.0, [3, 4].as_slice()),
            ]
        );
    }

    #[test]
    fn stable_complete_path_and_edge_ties_parallel_edges_and_disconnection() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(5, &[(0, 2), (0, 1), (0, 1), (1, 3), (2, 3)])
                .with_test_edge_weights(&[1.0, 4.0, 1.0, 1.0, 1.0]);
        let paths = exact_floyd_warshall(&graph, &control()).unwrap();
        let route = paths
            .iter()
            .find(|path| (path.source, path.target) == (0, 3))
            .unwrap();
        assert_eq!(route.cost, 2.0);
        assert_eq!(route.nodes, [0, 1, 3]);
        assert_eq!(paths, exact_floyd_warshall(&graph, &control()).unwrap());
        assert!(!paths.iter().any(|path| path.target == 4));
        assert!(!paths.iter().any(|path| path.source == path.target));

        let later_edge = BestPath {
            cost: 1.0,
            nodes: vec![0, 1],
            edges: vec![2],
        };
        let earlier_edge = BestPath {
            edges: vec![1],
            ..later_edge.clone()
        };
        assert!(improves(&earlier_edge, Some(&later_edge)));
    }

    #[test]
    fn graph_wide_negative_cycles_fail_for_directed_undirected_and_self_loops() {
        for graph in [
            AdjacencyGraph::with_test_directed_edges(2, &[(0, 0)]).with_test_edge_weights(&[-1.0]),
            AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)])
                .with_test_edge_weights(&[-1.0, -1.0]),
            AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (2, 3), (3, 2)])
                .with_test_edge_weights(&[2.0, -2.0, 1.0]),
        ] {
            assert!(matches!(
                exact_floyd_warshall(&graph, &control()),
                Err(AlgorithmError::Execution { message })
                    if message.contains("negative cycle")
            ));
        }
    }

    #[test]
    fn empty_invalid_overflow_limits_and_cancellation_are_structured() {
        assert!(
            exact_floyd_warshall(&AdjacencyGraph::default(), &control())
                .unwrap()
                .is_empty()
        );
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        for weight in [f64::NAN, f64::INFINITY] {
            let invalid = graph.clone().with_test_edge_weights(&[weight, 1.0]);
            assert!(matches!(
                exact_floyd_warshall(&invalid, &control()),
                Err(AlgorithmError::Execution { .. })
            ));
        }
        let overflow = graph.clone().with_test_edge_weights(&[f64::MAX, f64::MAX]);
        assert!(matches!(
            exact_floyd_warshall(&overflow, &control()),
            Err(AlgorithmError::Execution { .. })
        ));

        let output_limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_floyd_warshall(&graph, &output_limited),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let dense = AdjacencyGraph::with_test_edges(2, &vec![(0, 1); 4_096]);
        let iteration_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_floyd_warshall(&dense, &iteration_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let sparse = AdjacencyGraph::with_test_counts(64, 0);
        let sparse_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_floyd_warshall(&sparse, &sparse_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            exact_floyd_warshall(
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
