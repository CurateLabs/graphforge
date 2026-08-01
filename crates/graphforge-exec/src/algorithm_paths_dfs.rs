//! Deterministic depth-first traversal over the shared adjacency graph.

use std::collections::HashSet;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

/// One node discovered by depth-first traversal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DfsVisit {
    pub(crate) node: u64,
    pub(crate) depth: u64,
    pub(crate) order: u64,
}

/// Visit the source component in deterministic preorder.
pub(crate) fn depth_first_search(
    graph: &AdjacencyGraph,
    source: u64,
    control: &AlgorithmControl,
) -> Result<Vec<DfsVisit>, AlgorithmError> {
    let mut visited = HashSet::new();
    let mut stack = vec![(source, 0_u64)];
    let mut visits = Vec::new();

    while let Some((node, depth)) = stack.pop() {
        if !visited.insert(node) {
            continue;
        }
        control.checkpoint()?;
        let order = u64::try_from(visits.len()).map_err(|_| AlgorithmError::Execution {
            message: "dfs discovery order exceeds the UInt64 range".into(),
        })?;
        visits.push(DfsVisit { node, depth, order });

        let mut neighbors = graph
            .neighbors(node)
            .iter()
            .map(|edge| edge.neighbor_id)
            .filter(|neighbor| !visited.contains(neighbor))
            .collect::<Vec<_>>();
        neighbors.sort_unstable();
        neighbors.dedup();
        for neighbor in neighbors.into_iter().rev() {
            stack.push((neighbor, depth.saturating_add(1)));
        }
    }

    Ok(visits)
}

#[cfg(test)]
mod tests {
    use crate::algorithm_dispatch::{
        AlgorithmCancellation, AlgorithmControl, AlgorithmError, AlgorithmLimits,
    };

    use super::*;

    fn control(limits: AlgorithmLimits, cancellation: AlgorithmCancellation) -> AlgorithmControl {
        AlgorithmControl::new(limits, cancellation)
    }

    fn visit(node: u64, depth: u64, order: u64) -> DfsVisit {
        DfsVisit { node, depth, order }
    }

    #[test]
    fn deterministic_preorder_tracks_depth_and_ignores_multigraph_revisits() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            7,
            &[
                (0, 2),
                (0, 1),
                (0, 1),
                (1, 3),
                (1, 4),
                (3, 0),
                (3, 3),
                (2, 5),
            ],
        );
        assert_eq!(
            depth_first_search(
                &graph,
                0,
                &control(AlgorithmLimits::default(), AlgorithmCancellation::default()),
            )
            .unwrap(),
            vec![
                visit(0, 0, 0),
                visit(1, 1, 1),
                visit(3, 2, 2),
                visit(4, 2, 3),
                visit(2, 1, 4),
                visit(5, 2, 5),
            ]
        );
    }

    #[test]
    fn cancellation_and_iteration_limits_abort_without_partial_output() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            depth_first_search(
                &graph,
                0,
                &control(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            depth_first_search(
                &graph,
                0,
                &control(
                    AlgorithmLimits {
                        iterations: 1,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default(),
                ),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 2,
                limit: 1
            })
        ));
    }

    #[test]
    fn exported_direction_controls_reverse_reachability() {
        let directed = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_undirected_multigraph(2, &[(0, 0, 1)]);
        let run = |graph: &AdjacencyGraph| {
            depth_first_search(
                graph,
                1,
                &control(AlgorithmLimits::default(), AlgorithmCancellation::default()),
            )
            .unwrap()
        };
        assert_eq!(run(&directed), vec![visit(1, 0, 0)]);
        assert_eq!(run(&undirected), vec![visit(1, 0, 0), visit(0, 1, 1)]);
    }
}
