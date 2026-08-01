use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_matching_state::{
    AlternatingDualState, ExactMatchingValue, IndexedWeightedEdge,
};

const CHECKPOINT_INTERVAL: usize = 4_096;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct WeightedEdge {
    pub edge_uuid: [u8; 16],
    pub source_uuid: [u8; 16],
    pub target_uuid: [u8; 16],
    pub weight: f64,
}
impl WeightedEdge {
    fn canonical(mut self) -> Self {
        if self.target_uuid < self.source_uuid {
            std::mem::swap(&mut self.source_uuid, &mut self.target_uuid);
        }
        self
    }
}

pub(crate) struct WeightedUndirectedGraph {
    pub node_index: HashMap<[u8; 16], usize>,
    pub edges: Vec<WeightedEdge>,
    pub matching_state: AlternatingDualState,
}

pub(crate) fn normalize_weighted_undirected(
    nodes: &[[u8; 16]],
    edges: &[WeightedEdge],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<WeightedUndirectedGraph, AlgorithmError> {
    control.check_cancelled()?;
    let mut node_index = HashMap::with_capacity(nodes.len());
    for (position, &uuid) in nodes.iter().enumerate() {
        checkpoint(control, work)?;
        if node_index.insert(uuid, position).is_some() {
            return Err(execution("weighted graph node UUIDs must be unique"));
        }
    }

    let mut by_uuid = BTreeMap::new();
    for &raw in edges {
        checkpoint(control, work)?;
        if !raw.weight.is_finite() {
            return Err(execution("weighted graph requires finite edge weights"));
        }
        let edge = raw.canonical();
        if !node_index.contains_key(&edge.source_uuid)
            || !node_index.contains_key(&edge.target_uuid)
        {
            return Err(execution(
                "weighted graph edge endpoint is outside node selection",
            ));
        }
        if let Some(previous) = by_uuid.insert(edge.edge_uuid, edge)
            && !same_stored_edge(previous, edge)
        {
            return Err(execution(
                "weighted graph edge UUID has inconsistent adjacency entries",
            ));
        }
    }

    let mut edges = by_uuid.into_values().collect::<Vec<_>>();
    edges.sort_by_key(|edge| (edge.source_uuid, edge.target_uuid, edge.edge_uuid));
    let matching_state = AlternatingDualState::new(nodes.len(), &edges)?;
    for (edge_index, edge) in edges.iter().enumerate() {
        if edge.source_uuid == edge.target_uuid {
            continue;
        }
        let source = node_index[&edge.source_uuid];
        let target = node_index[&edge.target_uuid];
        if matching_state.slack(&IndexedWeightedEdge {
            edge: edge_index,
            left: source,
            right: target,
            weight: edge.weight,
        })? < ExactMatchingValue::default()
        {
            return Err(execution("initial matching duals must be feasible"));
        }
    }
    Ok(WeightedUndirectedGraph {
        node_index,
        edges,
        matching_state,
    })
}

pub(crate) fn solve_exact_matching(
    graph: &WeightedUndirectedGraph,
    control: &AlgorithmControl,
) -> Result<Vec<WeightedEdge>, AlgorithmError> {
    let indexed = graph
        .edges
        .iter()
        .enumerate()
        .filter(|(_, edge)| edge.source_uuid != edge.target_uuid)
        .map(|(edge, value)| IndexedWeightedEdge {
            edge,
            left: graph.node_index[&value.source_uuid],
            right: graph.node_index[&value.target_uuid],
            weight: value.weight,
        })
        .collect::<Vec<_>>();
    let mut state = graph.matching_state.clone();
    let selected = state.solve_exact(&indexed, control)?;
    control.check_output_rows(selected.len())?;
    Ok(selected.into_iter().map(|edge| graph.edges[edge]).collect())
}

/// Solves with raw edge UUIDs as the canonical identity order.
///
/// The weighted matching contract uses normalized endpoint tuples for its
/// tertiary objective, so this separate entry point keeps that behavior intact.
pub(crate) fn solve_exact_matching_by_edge_uuid(
    graph: &WeightedUndirectedGraph,
    control: &AlgorithmControl,
) -> Result<Vec<WeightedEdge>, AlgorithmError> {
    let mut edges = graph.edges.clone();
    edges.sort_by_key(|edge| edge.edge_uuid);
    let reordered = WeightedUndirectedGraph {
        node_index: graph.node_index.clone(),
        matching_state: AlternatingDualState::new(graph.node_index.len(), &edges)?,
        edges,
    };
    solve_exact_matching(&reordered, control)
}

pub(crate) fn compare_weighted_edges(
    left: &WeightedEdge,
    right: &WeightedEdge,
    maximize: bool,
) -> Ordering {
    let weight = if maximize {
        right.weight.total_cmp(&left.weight)
    } else {
        left.weight.total_cmp(&right.weight)
    };
    weight
        .then_with(|| left.source_uuid.cmp(&right.source_uuid))
        .then_with(|| left.target_uuid.cmp(&right.target_uuid))
        .then_with(|| left.edge_uuid.cmp(&right.edge_uuid))
}

fn same_stored_edge(left: WeightedEdge, right: WeightedEdge) -> bool {
    left.edge_uuid == right.edge_uuid
        && left.source_uuid == right.source_uuid
        && left.target_uuid == right.target_uuid
        && left.weight.to_bits() == right.weight.to_bits()
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    } else {
        control.check_cancelled()?;
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

    fn normalize(
        nodes: &[[u8; 16]],
        edges: &[WeightedEdge],
    ) -> Result<WeightedUndirectedGraph, AlgorithmError> {
        normalize_weighted_undirected(nodes, edges, &control(), &mut 0)
    }

    fn solve(nodes: &[[u8; 16]], edges: &[WeightedEdge]) -> Vec<WeightedEdge> {
        solve_exact_matching(&normalize(nodes, edges).unwrap(), &control()).unwrap()
    }

    fn oracle(edges: &[WeightedEdge]) -> Vec<[u8; 16]> {
        let mut best = (0_i64, 0_usize, Vec::new());
        for mask in 0..(1_u64 << edges.len()) {
            let mut used = Vec::new();
            let mut weight = 0_i64;
            let mut selected = Vec::new();
            let mut valid = true;
            for (position, edge) in edges.iter().enumerate() {
                if mask & (1 << position) == 0 {
                    continue;
                }
                if edge.source_uuid == edge.target_uuid
                    || used.contains(&edge.source_uuid)
                    || used.contains(&edge.target_uuid)
                {
                    valid = false;
                    break;
                }
                used.extend([edge.source_uuid, edge.target_uuid]);
                weight += edge.weight as i64;
                selected.push(position);
            }
            if !valid {
                continue;
            }
            let candidate = (weight, selected.len(), selected);
            if candidate.0 > best.0
                || (candidate.0 == best.0 && candidate.1 > best.1)
                || (candidate.0 == best.0 && candidate.1 == best.1 && candidate.2 < best.2)
            {
                best = candidate;
            }
        }
        best.2
            .into_iter()
            .map(|position| edges[position].edge_uuid)
            .collect()
    }

    #[test]
    fn normalizes_mirrors_loops_and_parallel_ties() {
        let graph = normalize(
            &[uuid(0), uuid(1), uuid(2)],
            &[
                edge(9, 1, 0, 5.0),
                edge(9, 0, 1, 5.0),
                edge(8, 0, 1, 5.0),
                edge(7, 1, 0, 1.0),
                edge(6, 1, 2, -2.0),
                edge(5, 2, 2, 99.0),
            ],
        )
        .unwrap();
        assert_eq!(
            graph.edges,
            [
                edge(7, 0, 1, 1.0),
                edge(8, 0, 1, 5.0),
                edge(9, 0, 1, 5.0),
                edge(6, 1, 2, -2.0),
                edge(5, 2, 2, 99.0),
            ]
        );
        assert!(compare_weighted_edges(&graph.edges[0], &graph.edges[1], false).is_lt());
        assert!(compare_weighted_edges(&graph.edges[0], &graph.edges[1], true).is_gt());
        assert!(compare_weighted_edges(&graph.edges[1], &graph.edges[2], true).is_lt());
    }

    #[test]
    fn rejects_nonfinite_or_inconsistent_identity_and_honors_cancellation() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        for result in [
            normalize(&nodes, &[edge(1, 0, 1, f64::NAN)]),
            normalize(&nodes, &[edge(1, 0, 1, 1.0), edge(1, 0, 2, 1.0)]),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            normalize_weighted_undirected(
                &nodes,
                &[],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
                &mut 0,
            ),
            Err(AlgorithmError::Cancelled)
        ));
    }

    #[test]
    fn exact_solver_matches_independent_exhaustive_oracle() {
        for node_count in 0..=5 {
            let nodes = (0..node_count).map(uuid).collect::<Vec<_>>();
            let pairs = (0..node_count)
                .flat_map(|left| ((left + 1)..node_count).map(move |right| (left, right)))
                .collect::<Vec<_>>();
            for topology in 0..(1_u64 << pairs.len()) {
                for assignment in 0..3 {
                    let edges = pairs
                        .iter()
                        .enumerate()
                        .filter(|(position, _)| topology & (1 << position) != 0)
                        .map(|(position, &(left, right))| {
                            let patterns = [[-1.0, 0.0, 2.0], [2.0, 0.0, -1.0], [1.0; 3]];
                            edge(
                                u8::try_from(position + 1).unwrap(),
                                left,
                                right,
                                patterns[assignment][position % 3],
                            )
                        })
                        .collect::<Vec<_>>();
                    let graph = normalize(&nodes, &edges).unwrap();
                    let expected = oracle(&graph.edges);
                    let actual = solve_exact_matching(&graph, &control())
                        .unwrap_or_else(|error| {
                            panic!(
                                "nodes={node_count} topology={topology} assignment={assignment}: {error}"
                            )
                        })
                        .into_iter()
                        .map(|edge| edge.edge_uuid)
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual, expected,
                        "nodes={node_count} topology={topology} assignment={assignment}"
                    );
                }
            }
        }
    }

    #[test]
    fn exact_solver_preserves_parallel_and_permutation_ties() {
        let nodes = (0..4).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(9, 1, 0, 1.0),
            edge(8, 0, 1, 1.0),
            edge(7, 2, 3, 1.0),
            edge(6, 3, 0, 1.0),
            edge(5, 1, 2, 1.0),
        ];
        let expected = [uuid(8), uuid(7)];
        let selected = |nodes: &[[u8; 16]], edges: &[WeightedEdge]| {
            solve(nodes, edges)
                .into_iter()
                .map(|edge| edge.edge_uuid)
                .collect::<Vec<_>>()
        };
        assert_eq!(selected(&nodes, &edges), expected);

        let mut permuted_nodes = nodes;
        permuted_nodes.reverse();
        let mut permuted_edges = edges;
        permuted_edges.reverse();
        permuted_edges[0] = permuted_edges[0].canonical();
        assert_eq!(selected(&permuted_nodes, &permuted_edges), expected);
    }

    #[test]
    fn exact_solver_handles_odd_cycle_crossings_and_shared_limits() {
        let nodes = (0..5).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(1, 0, 1, 10.0),
            edge(2, 1, 2, 10.0),
            edge(3, 2, 0, 10.0),
            edge(4, 1, 3, 11.0),
            edge(5, 2, 4, 11.0),
        ];
        let graph = normalize(&nodes, &edges).unwrap();
        let selected = solve_exact_matching(&graph, &control()).unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|edge| edge.edge_uuid)
                .collect::<Vec<_>>(),
            oracle(&graph.edges)
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        for control in [
            AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            AlgorithmControl::new(
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            AlgorithmControl::new(
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
        ] {
            assert!(solve_exact_matching(&graph, &control).is_err());
        }
        assert_eq!(solve_exact_matching(&graph, &control()).unwrap(), selected);
    }
}
