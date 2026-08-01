//! Deterministic positive-length transitive closure over shared adjacency.

use std::collections::{HashSet, VecDeque};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

/// One reachable ordered node pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ClosurePair {
    /// Execution-internal source surrogate.
    pub(crate) source: u64,
    /// Execution-internal reachable target surrogate.
    pub(crate) target: u64,
}

/// Compute every distinct positive-length reachable pair.
///
/// Sources and targets are ordered lexicographically by public UUID. The
/// caller owns direction and relationship filtering when it exports the shared
/// adjacency graph.
///
/// This deterministic per-source traversal is `O(V(V + E))` time and uses
/// `O(V)` traversal state per source plus result storage.
pub(crate) fn positive_transitive_closure(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<ClosurePair>, AlgorithmError> {
    control.check_cancelled()?;

    let mut sources = graph
        .node_ids()
        .iter()
        .map(|&node| {
            graph
                .node_uuid(node)
                .map(|uuid| (uuid, node))
                .ok_or_else(|| execution("transitive_closure source has no UUID identity"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    sources.sort_unstable();

    let mut rows = Vec::new();
    let mut traversed_entries = 0_usize;
    for (_, source) in sources {
        control.check_cancelled()?;
        let mut reachable = HashSet::new();
        let mut queue = VecDeque::from([source]);

        while let Some(node) = queue.pop_front() {
            for edge in graph.neighbors(node) {
                if traversed_entries.is_multiple_of(4_096) {
                    control.checkpoint()?;
                }
                traversed_entries = traversed_entries.saturating_add(1);
                if reachable.insert(edge.neighbor_id) {
                    queue.push_back(edge.neighbor_id);
                }
            }
        }

        let mut targets = reachable
            .into_iter()
            .map(|node| {
                graph
                    .node_uuid(node)
                    .map(|uuid| (uuid, node))
                    .ok_or_else(|| execution("transitive_closure target has no UUID identity"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        targets.sort_unstable();
        let next_len = rows
            .len()
            .checked_add(targets.len())
            .ok_or_else(|| execution("transitive_closure output size exceeds platform range"))?;
        control.check_output_rows(next_len)?;
        rows.extend(
            targets
                .into_iter()
                .map(|(_, target)| target)
                .map(|target| ClosurePair { source, target }),
        );
    }
    Ok(rows)
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

    fn pairs(rows: &[ClosurePair]) -> Vec<(u64, u64)> {
        rows.iter().map(|row| (row.source, row.target)).collect()
    }

    #[test]
    fn directed_chain_cycle_self_loop_and_parallel_edges_are_positive_length() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(5, &[(0, 1), (0, 1), (1, 2), (2, 0), (3, 3)]);
        let rows = positive_transitive_closure(&graph, &control()).unwrap();
        assert_eq!(
            pairs(&rows),
            vec![
                (0, 0),
                (0, 1),
                (0, 2),
                (1, 0),
                (1, 1),
                (1, 2),
                (2, 0),
                (2, 1),
                (2, 2),
                (3, 3),
            ]
        );
    }

    #[test]
    fn symmetric_projection_reaches_both_directions_without_duplicates() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 1), (1, 2), (2, 1)]);
        let rows = positive_transitive_closure(&graph, &control()).unwrap();
        assert_eq!(
            pairs(&rows),
            vec![
                (0, 0),
                (0, 1),
                (0, 2),
                (1, 0),
                (1, 1),
                (1, 2),
                (2, 0),
                (2, 1),
                (2, 2),
            ]
        );
    }

    #[test]
    fn empty_disconnected_and_isolated_nodes_emit_only_reachable_pairs() {
        assert!(
            positive_transitive_closure(&AdjacencyGraph::default(), &control())
                .unwrap()
                .is_empty()
        );

        let graph = AdjacencyGraph::with_test_directed_edges(5, &[(0, 1), (2, 3)]);
        assert_eq!(
            pairs(&positive_transitive_closure(&graph, &control()).unwrap()),
            vec![(0, 1), (2, 3)]
        );
    }

    #[test]
    fn output_is_sorted_by_public_source_then_target_uuid() {
        let graph = AdjacencyGraph::with_test_directed_edges(4, &[(2, 3), (0, 3), (0, 1), (1, 2)]);
        let first = positive_transitive_closure(&graph, &control()).unwrap();
        let second = positive_transitive_closure(&graph, &control()).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            pairs(&first),
            vec![(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        );
    }

    #[test]
    fn output_limit_and_cancellation_abort_without_rows() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            positive_transitive_closure(&graph, &limited),
            Err(AlgorithmError::OutputLimit {
                observed: 2..,
                limit: 2
            })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            positive_transitive_closure(&graph, &cancelled),
            Err(AlgorithmError::Cancelled)
        );
    }
}
