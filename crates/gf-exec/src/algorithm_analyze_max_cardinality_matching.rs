use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_weighted_undirected::{
    WeightedEdge, normalize_weighted_undirected, solve_exact_matching_by_edge_uuid,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MatchingEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

/// Returns the canonical maximum-cardinality matching of an undirected multigraph.
///
/// A zero primary weight makes the shared exact blossom solver compare cardinality
/// first, followed by the lexicographically smallest sorted raw edge-UUID sequence.
/// Self-loops remain in normalization for identity validation but are never matching
/// candidates.
pub(crate) fn maximum_cardinality_matching(
    nodes: &[[u8; 16]],
    edges: &[MatchingEdge],
    control: &AlgorithmControl,
) -> Result<Vec<MatchingEdge>, AlgorithmError> {
    let weighted = edges
        .iter()
        .map(|edge| WeightedEdge {
            edge_uuid: edge.edge,
            source_uuid: edge.source,
            target_uuid: edge.target,
            weight: 0.0,
        })
        .collect::<Vec<_>>();
    let graph = normalize_weighted_undirected(nodes, &weighted, control, &mut 0)?;
    solve_exact_matching_by_edge_uuid(&graph, control).map(|mut selected| {
        selected.sort_by_key(|edge| edge.edge_uuid);
        selected
            .into_iter()
            .map(|edge| MatchingEdge {
                edge: edge.edge_uuid,
                source: edge.source_uuid,
                target: edge.target_uuid,
            })
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn edge(id: u8, source: u8, target: u8) -> MatchingEdge {
        MatchingEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn selected(nodes: &[[u8; 16]], edges: &[MatchingEdge]) -> Vec<[u8; 16]> {
        maximum_cardinality_matching(nodes, edges, &control())
            .unwrap()
            .into_iter()
            .map(|edge| edge.edge)
            .collect()
    }

    fn oracle(edges: &[MatchingEdge]) -> Vec<[u8; 16]> {
        let mut edges = edges
            .iter()
            .map(|edge| {
                let (source, target) = if edge.source <= edge.target {
                    (edge.source, edge.target)
                } else {
                    (edge.target, edge.source)
                };
                MatchingEdge {
                    edge: edge.edge,
                    source,
                    target,
                }
            })
            .collect::<Vec<_>>();
        edges.sort_by_key(|edge| edge.edge);
        let mut best = Vec::new();
        for mask in 0..(1_u64 << edges.len()) {
            let mut used = Vec::new();
            let mut candidate = Vec::new();
            let mut valid = true;
            for (position, edge) in edges.iter().enumerate() {
                if mask & (1 << position) == 0 {
                    continue;
                }
                if edge.source == edge.target
                    || used.contains(&edge.source)
                    || used.contains(&edge.target)
                {
                    valid = false;
                    break;
                }
                used.extend([edge.source, edge.target]);
                candidate.push(position);
            }
            if !valid {
                continue;
            }
            if candidate.len() > best.len() || (candidate.len() == best.len() && candidate < best) {
                best = candidate;
            }
        }
        best.into_iter()
            .map(|position| edges[position].edge)
            .collect()
    }

    #[test]
    fn handles_blossoms_components_loops_parallel_ties_and_repeatability() {
        let nodes = (0..8).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(9, 0, 1),
            edge(8, 1, 2),
            edge(7, 2, 0),
            edge(6, 1, 3),
            edge(5, 2, 4),
            edge(4, 5, 6),
            edge(3, 6, 5),
            edge(2, 7, 7),
        ];
        let expected = oracle(&edges);
        assert_eq!(selected(&nodes, &edges), expected);
        assert_eq!(selected(&nodes, &edges), expected);

        let mut reversed = edges;
        reversed.reverse();
        assert_eq!(selected(&nodes, &reversed), expected);
    }

    #[test]
    fn raw_edge_uuid_objective_overrides_endpoint_tuple_order() {
        let nodes = (0..4).map(uuid).collect::<Vec<_>>();
        let edges = [edge(9, 0, 1), edge(1, 0, 2), edge(2, 1, 3), edge(8, 2, 3)];

        assert_eq!(selected(&nodes, &edges), vec![uuid(1), uuid(2)]);
    }

    #[test]
    fn returns_empty_for_empty_and_edgeless_selections() {
        assert!(selected(&[], &[]).is_empty());
        assert!(selected(&[uuid(0), uuid(1)], &[]).is_empty());
        assert!(selected(&[uuid(0)], &[edge(1, 0, 0)]).is_empty());
    }

    #[test]
    fn honors_cancellation_iteration_and_output_limits() {
        let nodes = [uuid(0), uuid(1)];
        let edges = [edge(1, 0, 1)];
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            maximum_cardinality_matching(
                &nodes,
                &edges,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        ));
        assert!(matches!(
            maximum_cardinality_matching(
                &nodes,
                &edges,
                &AlgorithmControl::new(
                    AlgorithmLimits {
                        iterations: 0,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default()
                )
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            maximum_cardinality_matching(
                &nodes,
                &edges,
                &AlgorithmControl::new(
                    AlgorithmLimits {
                        output_rows: 0,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default()
                )
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
    }

    #[test]
    fn exact_solver_matches_strict_exhaustive_small_graph_oracle() {
        for node_count in 0..=5 {
            let nodes = (0..node_count).map(uuid).collect::<Vec<_>>();
            let pairs = (0..node_count)
                .flat_map(|left| ((left + 1)..node_count).map(move |right| (left, right)))
                .collect::<Vec<_>>();
            for topology in 0..(1_u64 << pairs.len()) {
                let edges = pairs
                    .iter()
                    .enumerate()
                    .filter(|(position, _)| topology & (1 << position) != 0)
                    .map(|(position, &(left, right))| {
                        edge(u8::try_from(pairs.len() - position).unwrap(), left, right)
                    })
                    .collect::<Vec<_>>();
                let expected = oracle(&edges);
                assert_eq!(
                    selected(&nodes, &edges),
                    expected,
                    "nodes={node_count} topology={topology}"
                );
            }
        }
    }
}
