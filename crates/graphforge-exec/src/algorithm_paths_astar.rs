//! A* remains serial (#536). The accepted path is defined by one priority
//! queue ordered by estimate, cost, path, and edge ID; each pop and relaxation
//! mutates the best-path map consumed by the next pop.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;
use crate::algorithm_paths_dijkstra::DijkstraPath;

const CHECKPOINT_INTERVAL: usize = 4_096;

#[derive(Clone, Debug)]
struct HeapEntry {
    estimate: f64,
    cost: f64,
    path: Vec<u64>,
    edge_id: u64,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.estimate.to_bits() == other.estimate.to_bits()
            && self.cost.to_bits() == other.cost.to_bits()
            && self.path == other.path
            && self.edge_id == other.edge_id
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .estimate
            .total_cmp(&self.estimate)
            .then_with(|| other.cost.total_cmp(&self.cost))
            .then_with(|| other.path.cmp(&self.path))
            .then_with(|| other.edge_id.cmp(&self.edge_id))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) fn exact_astar(
    graph: &AdjacencyGraph,
    source: u64,
    target: u64,
    heuristic: Option<&HashMap<u64, f64>>,
    control: &AlgorithmControl,
) -> Result<Option<DijkstraPath>, AlgorithmError> {
    control.checkpoint()?;
    let mut work = 0_usize;
    validate_inputs(graph, source, target, heuristic, control, &mut work)?;

    let source_path = vec![source];
    let source_estimate = heuristic_value(heuristic, source);
    let mut best = HashMap::from([(source, (0.0, source_path.clone(), 0_u64))]);
    let mut heap = BinaryHeap::from([HeapEntry {
        estimate: source_estimate,
        cost: 0.0,
        path: source_path,
        edge_id: 0,
    }]);

    while let Some(entry) = heap.pop() {
        let node = *entry.path.last().expect("heap paths are non-empty");
        let Some((known_cost, known_path, known_edge)) = best.get(&node) else {
            continue;
        };
        if entry.cost.total_cmp(known_cost) != Ordering::Equal
            || entry.path != *known_path
            || entry.edge_id != *known_edge
        {
            continue;
        }
        if node == target {
            let result = DijkstraPath {
                source,
                target,
                cost: entry.cost,
                nodes: entry.path,
            };
            control.check_output_rows(1)?;
            return Ok(Some(result));
        }

        for edge in graph.neighbors(node) {
            checkpoint(control, &mut work)?;
            if entry.path.contains(&edge.neighbor_id) {
                continue;
            }
            let cost = entry.cost + edge.weight;
            if !cost.is_finite() {
                return Err(execution("astar accumulated cost is not finite"));
            }
            let estimate = cost + heuristic_value(heuristic, edge.neighbor_id);
            if !estimate.is_finite() {
                return Err(execution("astar estimated cost is not finite"));
            }
            let mut path = entry.path.clone();
            path.push(edge.neighbor_id);
            let candidate = (cost, path, edge.edge_id);
            let improves = best.get(&edge.neighbor_id).is_none_or(|known| {
                candidate.0.total_cmp(&known.0) == Ordering::Less
                    || (candidate.0.total_cmp(&known.0) == Ordering::Equal
                        && (candidate.1.as_slice(), candidate.2) < (known.1.as_slice(), known.2))
            });
            if improves {
                best.insert(edge.neighbor_id, candidate.clone());
                heap.push(HeapEntry {
                    estimate,
                    cost: candidate.0,
                    path: candidate.1,
                    edge_id: candidate.2,
                });
            }
        }
    }

    control.check_output_rows(0)?;
    Ok(None)
}

fn validate_inputs(
    graph: &AdjacencyGraph,
    source: u64,
    target: u64,
    heuristic: Option<&HashMap<u64, f64>>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    if !graph.node_ids().contains(&source) {
        return Err(execution("astar source is outside node selection"));
    }
    if !graph.node_ids().contains(&target) {
        return Err(execution("astar target is outside node selection"));
    }
    if let Some(values) = heuristic {
        for &node in graph.node_ids() {
            checkpoint(control, work)?;
            let value = values
                .get(&node)
                .ok_or_else(|| execution("astar heuristic is missing a selected node"))?;
            if !value.is_finite() || *value < 0.0 {
                return Err(execution(
                    "astar heuristic values must be finite and non-negative",
                ));
            }
        }
        if values[&target] != 0.0 {
            return Err(execution("astar target heuristic must be zero"));
        }
    }
    for &node in graph.node_ids() {
        for edge in graph.neighbors(node) {
            checkpoint(control, work)?;
            if !edge.weight.is_finite() || edge.weight < 0.0 {
                return Err(execution("astar requires finite non-negative edge weights"));
            }
        }
    }
    Ok(())
}

fn heuristic_value(heuristic: Option<&HashMap<u64, f64>>, node: u64) -> f64 {
    heuristic.map_or(0.0, |values| values[&node])
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
    fn informed_and_zero_heuristics_return_the_same_exact_stable_path() {
        let graph = AdjacencyGraph::with_test_edges(5, &[(0, 2), (0, 1), (1, 3), (2, 3), (0, 3)])
            .with_test_edge_weights(&[1.0, 1.0, 2.0, 2.0, 9.0]);
        let heuristic = HashMap::from([(0, 3.0), (1, 2.0), (2, 2.0), (3, 0.0), (4, 8.0)]);
        let expected = DijkstraPath {
            source: 0,
            target: 3,
            cost: 3.0,
            nodes: vec![0, 1, 3],
        };

        assert_eq!(
            exact_astar(&graph, 0, 3, Some(&heuristic), &control()).unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            exact_astar(&graph, 0, 3, None, &control()).unwrap(),
            Some(expected)
        );
    }

    #[test]
    fn parallel_zero_self_disconnected_and_singleton_cases_are_stable() {
        let graph =
            AdjacencyGraph::with_test_edges(5, &[(0, 0), (0, 1), (0, 1), (1, 3), (0, 2), (2, 3)])
                .with_test_edge_weights(&[0.0, 4.0, 1.0, 1.0, 1.0, 1.0]);
        assert_eq!(
            exact_astar(&graph, 0, 3, None, &control()).unwrap(),
            Some(DijkstraPath {
                source: 0,
                target: 3,
                cost: 2.0,
                nodes: vec![0, 1, 3],
            })
        );
        assert_eq!(exact_astar(&graph, 0, 4, None, &control()).unwrap(), None);
        assert_eq!(
            exact_astar(&graph, 4, 4, None, &control())
                .unwrap()
                .unwrap()
                .nodes,
            [4]
        );
    }

    #[test]
    fn invalid_nodes_weights_and_heuristics_are_structured() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        for (source, target) in [(9, 2), (0, 9)] {
            assert!(matches!(
                exact_astar(&graph, source, target, None, &control()),
                Err(AlgorithmError::Execution { .. })
            ));
        }
        for values in [
            HashMap::from([(0, 2.0), (1, 1.0)]),
            HashMap::from([(0, -1.0), (1, 1.0), (2, 0.0)]),
            HashMap::from([(0, f64::NAN), (1, 1.0), (2, 0.0)]),
            HashMap::from([(0, 2.0), (1, 1.0), (2, 1.0)]),
        ] {
            assert!(matches!(
                exact_astar(&graph, 0, 2, Some(&values), &control()),
                Err(AlgorithmError::Execution { .. })
            ));
        }
        for weight in [-1.0, f64::NAN, f64::INFINITY] {
            let invalid =
                AdjacencyGraph::with_test_edges(2, &[(0, 1)]).with_test_edge_weights(&[weight]);
            assert!(matches!(
                exact_astar(&invalid, 0, 1, None, &control()),
                Err(AlgorithmError::Execution { .. })
            ));
        }
    }

    #[test]
    fn overflow_limits_and_cancellation_are_structured() {
        let overflow = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)])
            .with_test_edge_weights(&[f64::MAX, f64::MAX]);
        assert!(matches!(
            exact_astar(&overflow, 0, 2, None, &control()),
            Err(AlgorithmError::Execution { .. })
        ));

        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let output_limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_astar(&graph, 0, 1, None, &output_limited),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let iteration_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_astar(&graph, 0, 1, None, &iteration_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            exact_astar(
                &graph,
                0,
                1,
                None,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn informed_heuristic_avoids_work_that_zero_heuristic_cannot_finish() {
        let mut edges = vec![(0, 1), (1, 2), (0, 3)];
        edges.extend(std::iter::repeat_n((3, 3), 4_096));
        let mut weights = vec![1.0, 1.0, 0.0];
        weights.resize(edges.len(), 0.0);
        let graph = AdjacencyGraph::with_test_edges(4, &edges).with_test_edge_weights(&weights);
        let mut heuristic = (0..4).map(|node| (node, 10.0)).collect::<HashMap<_, _>>();
        heuristic.insert(0, 2.0);
        heuristic.insert(1, 1.0);
        heuristic.insert(2, 0.0);
        let limited = || {
            AlgorithmControl::new(
                AlgorithmLimits {
                    iterations: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            )
        };

        assert_eq!(
            exact_astar(&graph, 0, 2, Some(&heuristic), &limited())
                .unwrap()
                .unwrap()
                .nodes,
            [0, 1, 2]
        );
        assert!(matches!(
            exact_astar(&graph, 0, 2, None, &limited()),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }
}
