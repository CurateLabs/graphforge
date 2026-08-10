//! Bellman-Ford remains serial (#537). Relaxation rounds walk nodes and edges
//! in canonical order, mutating a best-path map whose state determines later
//! relaxations and negative-cycle detection.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;
use crate::algorithm_paths_dijkstra::DijkstraPath;

const CHECKPOINT_INTERVAL: usize = 4_096;

type BestPath = (f64, Vec<u64>, u64);

pub(crate) fn exact_bellman_ford(
    graph: &AdjacencyGraph,
    source: u64,
    target: Option<u64>,
    control: &AlgorithmControl,
) -> Result<Vec<DijkstraPath>, AlgorithmError> {
    control.checkpoint()?;
    validate_endpoint(graph, source, "source")?;
    if let Some(target) = target {
        validate_endpoint(graph, target, "target")?;
    }

    let mut work = 0_usize;
    validate_weights(graph, control, &mut work)?;
    let mut best = HashMap::from([(source, (0.0, vec![source], 0_u64))]);

    for _ in 0..graph.node_ids().len().saturating_sub(1) {
        checkpoint(control, &mut work)?;
        let mut changed = false;
        for &node in graph.node_ids() {
            let Some(current) = best.get(&node).cloned() else {
                continue;
            };
            for edge in graph.neighbors(node) {
                checkpoint(control, &mut work)?;
                if current.1.contains(&edge.neighbor_id) {
                    continue;
                }
                let candidate = candidate(&current, edge.neighbor_id, edge.edge_id, edge.weight)?;
                if improves(&candidate, best.get(&edge.neighbor_id)) {
                    best.insert(edge.neighbor_id, candidate);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }

    detect_reachable_negative_cycle(graph, &best, control, &mut work)?;

    let mut targets = match target {
        Some(node) if best.contains_key(&node) => vec![node],
        Some(_) => Vec::new(),
        None => best.keys().copied().collect(),
    };
    targets.sort_unstable();
    let paths = targets
        .into_iter()
        .map(|node| {
            let (cost, nodes, _) = &best[&node];
            DijkstraPath {
                source,
                target: node,
                cost: *cost,
                nodes: nodes.clone(),
            }
        })
        .collect::<Vec<_>>();
    control.check_output_rows(paths.len())?;
    Ok(paths)
}

fn validate_endpoint(graph: &AdjacencyGraph, node: u64, role: &str) -> Result<(), AlgorithmError> {
    if graph.node_ids().contains(&node) {
        Ok(())
    } else {
        Err(execution(format!(
            "bellman_ford {role} is outside node selection"
        )))
    }
}

fn validate_weights(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    for &node in graph.node_ids() {
        for edge in graph.neighbors(node) {
            checkpoint(control, work)?;
            if !edge.weight.is_finite() {
                return Err(execution("bellman_ford requires finite edge weights"));
            }
        }
    }
    Ok(())
}

fn candidate(
    current: &BestPath,
    neighbor: u64,
    edge_id: u64,
    weight: f64,
) -> Result<BestPath, AlgorithmError> {
    let cost = current.0 + weight;
    if !cost.is_finite() {
        return Err(execution("bellman_ford accumulated cost is not finite"));
    }
    let mut path = current.1.clone();
    path.push(neighbor);
    Ok((cost, path, edge_id))
}

fn improves(candidate: &BestPath, known: Option<&BestPath>) -> bool {
    known.is_none_or(|known| {
        candidate.0.total_cmp(&known.0) == Ordering::Less
            || (candidate.0.total_cmp(&known.0) == Ordering::Equal
                && (candidate.1.as_slice(), candidate.2) < (known.1.as_slice(), known.2))
    })
}

fn detect_reachable_negative_cycle(
    graph: &AdjacencyGraph,
    best: &HashMap<u64, BestPath>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    for &node in graph.node_ids() {
        let Some(current) = best.get(&node) else {
            continue;
        };
        for edge in graph.neighbors(node) {
            checkpoint(control, work)?;
            let cost = current.0 + edge.weight;
            if !cost.is_finite() {
                return Err(execution("bellman_ford accumulated cost is not finite"));
            }
            if best
                .get(&edge.neighbor_id)
                .is_some_and(|known| cost.total_cmp(&known.0) == Ordering::Less)
            {
                return Err(execution(
                    "bellman_ford found a negative cycle reachable from the source",
                ));
            }
        }
    }
    Ok(())
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
    fn negative_edges_produce_exact_target_and_all_reachable_paths() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            6,
            &[(0, 2), (0, 1), (1, 2), (1, 3), (2, 3), (3, 4)],
        )
        .with_test_edge_weights(&[5.0, 4.0, -2.0, 6.0, 3.0, -1.0]);
        let all = exact_bellman_ford(&graph, 0, None, &control()).unwrap();
        assert_eq!(
            all,
            vec![
                DijkstraPath {
                    source: 0,
                    target: 0,
                    cost: 0.0,
                    nodes: vec![0],
                },
                DijkstraPath {
                    source: 0,
                    target: 1,
                    cost: 4.0,
                    nodes: vec![0, 1],
                },
                DijkstraPath {
                    source: 0,
                    target: 2,
                    cost: 2.0,
                    nodes: vec![0, 1, 2],
                },
                DijkstraPath {
                    source: 0,
                    target: 3,
                    cost: 5.0,
                    nodes: vec![0, 1, 2, 3],
                },
                DijkstraPath {
                    source: 0,
                    target: 4,
                    cost: 4.0,
                    nodes: vec![0, 1, 2, 3, 4],
                },
            ]
        );
        assert_eq!(
            exact_bellman_ford(&graph, 0, Some(4), &control()).unwrap(),
            vec![all[4].clone()]
        );
    }

    #[test]
    fn stable_ties_parallel_edges_disconnected_and_self_paths_are_deterministic() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(5, &[(0, 2), (0, 1), (0, 1), (1, 3), (2, 3)])
                .with_test_edge_weights(&[1.0, 4.0, 1.0, 1.0, 1.0]);
        let expected = vec![DijkstraPath {
            source: 0,
            target: 3,
            cost: 2.0,
            nodes: vec![0, 1, 3],
        }];
        assert_eq!(
            exact_bellman_ford(&graph, 0, Some(3), &control()).unwrap(),
            expected
        );
        assert_eq!(
            exact_bellman_ford(&graph, 0, Some(3), &control()).unwrap(),
            expected
        );
        assert!(
            exact_bellman_ford(&graph, 0, Some(4), &control())
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            exact_bellman_ford(&graph, 0, Some(0), &control()).unwrap()[0].nodes,
            [0]
        );
    }

    #[test]
    fn only_source_reachable_negative_cycles_fail() {
        for graph in [
            AdjacencyGraph::with_test_directed_edges(2, &[(0, 0)]).with_test_edge_weights(&[-1.0]),
            AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)])
                .with_test_edge_weights(&[-1.0, -1.0]),
            AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 1)])
                .with_test_edge_weights(&[1.0, -2.0, 1.0]),
        ] {
            assert!(matches!(
                exact_bellman_ford(&graph, 0, None, &control()),
                Err(AlgorithmError::Execution { message })
                    if message.contains("negative cycle")
            ));
        }

        let unreachable = AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (2, 3), (3, 2)])
            .with_test_edge_weights(&[2.0, -2.0, 1.0]);
        assert_eq!(
            exact_bellman_ford(&unreachable, 0, None, &control())
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn invalid_inputs_overflow_limits_and_cancellation_are_structured() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        assert!(exact_bellman_ford(&graph, 9, None, &control()).is_err());
        assert!(exact_bellman_ford(&graph, 0, Some(9), &control()).is_err());
        for weight in [f64::NAN, f64::INFINITY] {
            let invalid = graph.clone().with_test_edge_weights(&[weight, 1.0]);
            assert!(matches!(
                exact_bellman_ford(&invalid, 0, None, &control()),
                Err(AlgorithmError::Execution { .. })
            ));
        }
        let overflow = graph.clone().with_test_edge_weights(&[f64::MAX, f64::MAX]);
        assert!(matches!(
            exact_bellman_ford(&overflow, 0, None, &control()),
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
            exact_bellman_ford(&graph, 0, None, &output_limited),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let many_edges = AdjacencyGraph::with_test_edges(2, &vec![(0, 1); 4_096]);
        let iteration_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_bellman_ford(&many_edges, 0, None, &iteration_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            exact_bellman_ford(
                &graph,
                0,
                None,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
