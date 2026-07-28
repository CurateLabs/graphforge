//! Deterministic one-exchange kernel for Rust-owned approximate maximum cut.

use std::collections::{HashMap, HashSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

pub(crate) const MAX_MAX_CUT_NODES: usize = 4_096;
pub(crate) const MAX_MAX_CUT_EDGE_ENTRIES: u64 = 16_777_216;

/// Return a deterministic, locally maximal two-way cut.
pub(crate) fn approximate_max_cut_labels(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    validate_limits(graph.node_ids().len(), graph.edge_entry_count())?;
    control.check_cancelled()?;

    let indices: HashMap<_, _> = graph
        .node_ids()
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut adjacency = vec![Vec::new(); graph.node_ids().len()];
    let mut seen_edges = HashSet::new();
    let mut work = 0_usize;
    for (source, &node) in graph.node_ids().iter().enumerate() {
        for edge in graph.neighbors(node) {
            check_cancelled_chunk(control, &mut work)?;
            if !seen_edges.insert(edge.edge_id) {
                continue;
            }
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            let weight = valid_weight(edge.weight)?;
            if source != target {
                adjacency[source].push((target, weight));
                adjacency[target].push((source, weight));
            }
        }
    }

    let mut labels = vec![0_usize; adjacency.len()];
    let mut gains = adjacency
        .iter()
        .map(|edges| {
            edges.iter().try_fold(0.0, |gain, &(_, weight)| {
                finite(gain + weight, "maximum-cut gain is not finite")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    loop {
        control.check_cancelled()?;
        let candidate = gains
            .iter()
            .enumerate()
            .filter(|(_, gain)| **gain > 0.0)
            .max_by(|(left_node, left), (right_node, right)| {
                left.total_cmp(right)
                    .then_with(|| right_node.cmp(left_node))
            });
        let Some((node, _)) = candidate else {
            break;
        };

        control.checkpoint()?;
        labels[node] = 1 - labels[node];
        gains[node] = finite(-gains[node], "maximum-cut gain is not finite")?;
        for &(neighbor, weight) in &adjacency[node] {
            let delta = if labels[neighbor] == labels[node] {
                2.0 * weight
            } else {
                -2.0 * weight
            };
            gains[neighbor] = finite(gains[neighbor] + delta, "maximum-cut gain is not finite")?;
        }
    }

    if labels.first() == Some(&1) {
        for label in &mut labels {
            *label = 1 - *label;
        }
    }
    for (node, edges) in adjacency.iter().enumerate() {
        if edges.is_empty() {
            labels[node] = 0;
        }
    }
    Ok(labels)
}

fn validate_limits(node_count: usize, edge_entries: u64) -> Result<(), AlgorithmError> {
    if node_count > MAX_MAX_CUT_NODES {
        return Err(AlgorithmError::NodeLimit {
            observed: u64::try_from(node_count).unwrap_or(u64::MAX),
            limit: u64::try_from(MAX_MAX_CUT_NODES).expect("maximum fits u64"),
        });
    }
    if edge_entries > MAX_MAX_CUT_EDGE_ENTRIES {
        return Err(AlgorithmError::EdgeLimit {
            observed: edge_entries,
            limit: MAX_MAX_CUT_EDGE_ENTRIES,
        });
    }
    Ok(())
}

fn valid_weight(weight: f64) -> Result<f64, AlgorithmError> {
    if !weight.is_finite() || weight < 0.0 {
        return Err(execution(
            "maximum-cut relationship weight must be finite and non-negative",
        ));
    }
    Ok(weight)
}

fn finite(value: f64, message: &str) -> Result<f64, AlgorithmError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(execution(message))
    }
}

fn check_cancelled_chunk(
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    if work.is_multiple_of(16_384) {
        control.check_cancelled()?;
    }
    *work += 1;
    Ok(())
}

fn execution(message: &str) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmControl, AlgorithmLimits};

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn run(nodes: u64, edges: &[(u64, u64)]) -> Vec<usize> {
        approximate_max_cut_labels(&AdjacencyGraph::with_test_edges(nodes, edges), &control())
            .unwrap()
    }

    fn cut_cost(labels: &[usize], edges: &[(u64, u64)]) -> usize {
        edges
            .iter()
            .filter(|&&(left, right)| labels[left as usize] != labels[right as usize])
            .count()
    }

    #[test]
    fn hand_verifiable_graphs_reach_stable_local_optima() {
        let triangle = [(0, 1), (1, 2), (2, 0)];
        let cycle = [(0, 1), (1, 2), (2, 3), (3, 0)];

        let triangle_labels = run(3, &triangle);
        assert_eq!(triangle_labels, [0, 1, 1]);
        assert_eq!(cut_cost(&triangle_labels, &triangle), 2);
        let cycle_labels = run(4, &cycle);
        assert_eq!(cycle_labels, [0, 1, 0, 1]);
        assert_eq!(cut_cost(&cycle_labels, &cycle), 4);
        assert_eq!(cycle_labels, run(4, &cycle));
    }

    #[test]
    fn ties_parallel_edges_self_loops_and_isolates_are_deterministic() {
        let edges = [(0, 1), (0, 1), (1, 1), (2, 3)];
        let labels = run(5, &edges);
        assert_eq!(labels, [0, 1, 0, 1, 0]);
        assert_eq!(cut_cost(&labels, &edges), 3);
        assert_eq!(run(3, &[]), [0, 0, 0]);
        assert!(run(0, &[]).is_empty());
    }

    #[test]
    fn every_final_one_node_move_has_non_positive_gain() {
        let edges = [(0, 1), (0, 2), (1, 2), (1, 3), (2, 4), (3, 4)];
        let labels = run(5, &edges);
        let baseline = cut_cost(&labels, &edges);
        for node in 0..labels.len() {
            let mut moved = labels.clone();
            moved[node] = 1 - moved[node];
            assert!(cut_cost(&moved, &edges) <= baseline);
        }
    }

    #[test]
    fn limits_cancellation_and_numeric_validation_are_structured() {
        assert!(matches!(
            validate_limits(MAX_MAX_CUT_NODES + 1, 0),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            validate_limits(0, MAX_MAX_CUT_EDGE_ENTRIES + 1),
            Err(AlgorithmError::EdgeLimit { .. })
        ));
        assert!(matches!(
            valid_weight(-1.0),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            valid_weight(f64::NAN),
            Err(AlgorithmError::Execution { .. })
        ));

        let cancelled = AlgorithmCancellation::default();
        cancelled.cancel();
        let cancelled_control = AlgorithmControl::new(AlgorithmLimits::default(), cancelled);
        assert_eq!(
            approximate_max_cut_labels(
                &AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
                &cancelled_control,
            ),
            Err(AlgorithmError::Cancelled)
        );

        let no_moves = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            approximate_max_cut_labels(&AdjacencyGraph::with_test_edges(2, &[(0, 1)]), &no_moves,),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        ));
    }
}
