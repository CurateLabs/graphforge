use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_weighted_undirected::{WeightedEdge, normalize_weighted_undirected};
use std::cmp::Ordering;
/// One exact spanning tree in canonical edge-UUID order.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpanningTree {
    pub edges: Vec<WeightedEdge>,
    pub total_weight: f64,
}
/// Enumerate the exact `k` cheapest distinct spanning trees.
/// Enumeration finishes before results are returned, making cancellation and
/// resource-limit failures atomic.
pub(crate) fn minimum_k_spanning_trees(
    nodes: &[[u8; 16]],
    edges: &[WeightedEdge],
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<SpanningTree>, AlgorithmError> {
    if k == 0 {
        return Err(execution("minimum-k spanning trees requires k > 0"));
    }
    if edges.iter().any(|edge| edge.weight < 0.0) {
        return Err(execution(
            "minimum-k spanning trees requires nonnegative edge weights",
        ));
    }
    control.checkpoint()?;
    let mut work = 0;
    let graph = normalize_weighted_undirected(nodes, edges, control, &mut work)?;
    let mut candidates = graph
        .edges
        .into_iter()
        .filter(|edge| edge.source_uuid != edge.target_uuid)
        .collect::<Vec<_>>();
    candidates.sort_by_key(|edge| edge.edge_uuid);
    if nodes.is_empty() {
        return Ok(Vec::new());
    }
    if nodes.len() == 1 {
        return Ok(vec![SpanningTree {
            edges: Vec::new(),
            total_weight: 0.0,
        }]);
    }
    let edge_count = nodes.len() - 1;
    if candidates.len() < edge_count {
        return Err(execution(
            "minimum-k spanning trees requires a connected graph",
        ));
    }
    let mut trees = Vec::new();
    enumerate(
        &candidates,
        edge_count,
        k,
        0,
        &mut Vec::with_capacity(edge_count),
        nodes,
        &graph.node_index,
        &mut trees,
        control,
    )?;
    if trees.is_empty() {
        return Err(execution(
            "minimum-k spanning trees requires a connected graph",
        ));
    }
    Ok(trees)
}
#[allow(clippy::too_many_arguments)]
fn enumerate(
    edges: &[WeightedEdge],
    needed: usize,
    k: usize,
    start: usize,
    selected: &mut Vec<WeightedEdge>,
    nodes: &[[u8; 16]],
    node_index: &std::collections::HashMap<[u8; 16], usize>,
    trees: &mut Vec<SpanningTree>,
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    control.checkpoint()?;
    if selected.len() == needed {
        if is_tree(selected, nodes.len(), node_index) {
            let total_weight = selected
                .iter()
                .try_fold(0.0_f64, |sum, edge| {
                    let total = sum + edge.weight;
                    total.is_finite().then_some(total)
                })
                .ok_or_else(|| execution("spanning-tree total weight overflowed"))?;
            let tree = SpanningTree {
                edges: selected.clone(),
                total_weight,
            };
            if trees.len() < k {
                control.check_output_rows((trees.len() + 1).saturating_mul(needed))?;
                trees.push(tree);
                trees.sort_by(compare_trees);
            } else if compare_trees(&tree, trees.last().expect("k is nonzero")).is_lt() {
                *trees.last_mut().expect("k is nonzero") = tree;
                trees.sort_by(compare_trees);
            }
        }
        return Ok(());
    }
    let remaining = needed - selected.len();
    if edges.len().saturating_sub(start) < remaining {
        return Ok(());
    }
    for index in start..=edges.len() - remaining {
        selected.push(edges[index]);
        enumerate(
            edges,
            needed,
            k,
            index + 1,
            selected,
            nodes,
            node_index,
            trees,
            control,
        )?;
        selected.pop();
    }
    Ok(())
}
fn is_tree(
    edges: &[WeightedEdge],
    node_count: usize,
    node_index: &std::collections::HashMap<[u8; 16], usize>,
) -> bool {
    let mut parent = (0..node_count).collect::<Vec<_>>();
    for edge in edges {
        let (mut left, mut right) = (node_index[&edge.source_uuid], node_index[&edge.target_uuid]);
        while parent[left] != left {
            left = parent[left];
        }
        while parent[right] != right {
            right = parent[right];
        }
        if left == right {
            return false;
        }
        parent[right] = left;
    }
    true
}

fn compare_trees(left: &SpanningTree, right: &SpanningTree) -> Ordering {
    left.total_weight
        .total_cmp(&right.total_weight)
        .then_with(|| {
            left.edges
                .iter()
                .map(|edge| edge.edge_uuid)
                .cmp(right.edges.iter().map(|edge| edge.edge_uuid))
        })
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

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn edge(id: u8, source: u8, target: u8, weight: f64) -> WeightedEdge {
        WeightedEdge {
            edge_uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
            weight,
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    #[test]
    fn returns_exact_cost_then_uuid_order_with_parallel_edges_and_loops() {
        let trees = minimum_k_spanning_trees(
            &[uuid(0), uuid(1), uuid(2)],
            &[
                edge(1, 0, 1, 1.0),
                edge(2, 0, 1, 1.0),
                edge(3, 1, 2, 1.0),
                edge(4, 0, 2, 2.0),
                edge(5, 2, 2, 0.0),
            ],
            3,
            &control(),
        )
        .unwrap();
        assert_eq!(
            trees
                .iter()
                .map(|tree| {
                    (
                        tree.total_weight,
                        tree.edges
                            .iter()
                            .map(|edge| edge.edge_uuid)
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
            [
                (2.0, vec![uuid(1), uuid(3)]),
                (2.0, vec![uuid(2), uuid(3)]),
                (3.0, vec![uuid(1), uuid(4)]),
            ]
        );
    }

    #[test]
    fn exhausts_trees_and_handles_empty_and_singleton() {
        let only =
            minimum_k_spanning_trees(&[uuid(0), uuid(1)], &[edge(1, 0, 1, 2.0)], 9, &control())
                .unwrap();
        assert_eq!(only.len(), 1);
        assert!(
            minimum_k_spanning_trees(&[], &[], 1, &control())
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            minimum_k_spanning_trees(&[uuid(0)], &[], 1, &control()).unwrap(),
            [SpanningTree {
                edges: Vec::new(),
                total_weight: 0.0,
            }]
        );
    }

    #[test]
    fn rejects_invalid_disconnected_and_negative_inputs() {
        assert!(minimum_k_spanning_trees(&[uuid(0)], &[], 0, &control()).is_err());
        assert!(minimum_k_spanning_trees(&[uuid(0), uuid(1)], &[], 1, &control()).is_err());
        for (nodes, edges) in [
            (vec![uuid(0), uuid(1)], vec![edge(1, 0, 1, -1.0)]),
            (vec![uuid(0), uuid(1)], vec![edge(1, 0, 1, f64::NAN)]),
            (vec![uuid(0), uuid(1)], vec![edge(1, 0, 1, f64::INFINITY)]),
            (vec![uuid(0), uuid(0)], vec![]),
            (vec![uuid(0)], vec![edge(1, 0, 1, 1.0)]),
            (
                vec![uuid(0), uuid(1)],
                vec![edge(1, 0, 1, 1.0), edge(1, 0, 0, 1.0)],
            ),
        ] {
            assert!(minimum_k_spanning_trees(&nodes, &edges, 1, &control()).is_err());
        }
    }

    #[test]
    fn cancellation_iteration_and_output_limits_fail_atomically() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        let edges = [edge(1, 0, 1, 1.0), edge(2, 1, 2, 1.0), edge(3, 0, 2, 1.0)];
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            minimum_k_spanning_trees(
                &nodes,
                &edges,
                2,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        for (iterations, output_rows, expected) in [
            (1, u64::MAX, "iteration limit"),
            (u64::MAX, 1, "output row limit"),
        ] {
            let limits = AlgorithmLimits {
                iterations,
                output_rows,
                ..AlgorithmLimits::default()
            };
            let error = minimum_k_spanning_trees(
                &nodes,
                &edges,
                2,
                &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected));
        }
    }
}
