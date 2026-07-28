//! Deterministic Kahn topology shared by the M18 DAG analysis family.

use std::collections::{BTreeSet, HashMap};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

const CHECKPOINT_INTERVAL: usize = 4_096;

/// Complete stable topological order and the position of every selected node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DagTopology {
    pub order: Vec<u64>,
    pub positions: HashMap<u64, usize>,
}

/// Compute a deterministic topological order with Kahn's algorithm.
///
/// Every selected adjacency entry contributes to indegree, including parallel
/// edges. Ready nodes are selected by public UUID, then internal node ID as a
/// defensive tie-break. Runtime is `O((V + E) log V)` with `O(V)` additional
/// state for indegrees, positions, and the ordered ready set.
pub(crate) fn stable_dag_topology(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<DagTopology, AlgorithmError> {
    control.checkpoint()?;
    if !graph.is_directed() {
        return Err(execution("DAG topology requires directed adjacency"));
    }

    let mut work = 0_usize;
    let mut indegrees = graph
        .node_ids()
        .iter()
        .copied()
        .map(|node| (node, 0_usize))
        .collect::<HashMap<_, _>>();

    for &source in graph.node_ids() {
        checkpoint(control, &mut work)?;
        node_uuid(graph, source)?;
        for edge in graph.neighbors(source) {
            checkpoint(control, &mut work)?;
            let indegree =
                indegrees
                    .get_mut(&edge.neighbor_id)
                    .ok_or_else(|| AlgorithmError::Execution {
                        message: "DAG adjacency references an unselected node".into(),
                    })?;
            *indegree = indegree
                .checked_add(1)
                .ok_or_else(|| AlgorithmError::Execution {
                    message: "DAG indegree exceeds platform range".into(),
                })?;
        }
    }

    let mut ready = BTreeSet::new();
    for &node in graph.node_ids() {
        checkpoint(control, &mut work)?;
        if indegrees[&node] == 0 {
            ready.insert((node_uuid(graph, node)?, node));
        }
    }

    let mut order = Vec::with_capacity(graph.node_ids().len());
    let mut positions = HashMap::with_capacity(graph.node_ids().len());
    while let Some(&(uuid, node)) = ready.first() {
        checkpoint(control, &mut work)?;
        ready.remove(&(uuid, node));
        positions.insert(node, order.len());
        order.push(node);

        for edge in graph.neighbors(node) {
            checkpoint(control, &mut work)?;
            let indegree = indegrees
                .get_mut(&edge.neighbor_id)
                .expect("selected adjacency target has an indegree");
            *indegree = indegree
                .checked_sub(1)
                .ok_or_else(|| AlgorithmError::Execution {
                    message: "DAG indegree underflow".into(),
                })?;
            if *indegree == 0 {
                ready.insert((node_uuid(graph, edge.neighbor_id)?, edge.neighbor_id));
            }
        }
    }

    if order.len() != graph.node_ids().len() {
        return Err(execution("selected graph contains a cycle"));
    }
    Ok(DagTopology { order, positions })
}

fn node_uuid(graph: &AdjacencyGraph, node: u64) -> Result<[u8; 16], AlgorithmError> {
    graph
        .node_uuid(node)
        .ok_or_else(|| execution("selected DAG node has no UUID identity"))
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
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

    fn uuids(values: &[u8]) -> Vec<[u8; 16]> {
        values.iter().map(|&value| [value; 16]).collect()
    }

    #[test]
    fn orders_ready_nodes_by_public_uuid_and_returns_positions() {
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
            &uuids(&[40, 10, 30, 20, 50, 60]),
            &[(0, 4), (1, 4), (2, 5), (3, 5)],
        );

        let topology = stable_dag_topology(&graph, &control()).unwrap();

        assert_eq!(topology.order, [1, 3, 2, 0, 4, 5]);
        assert_eq!(
            topology.positions,
            topology
                .order
                .iter()
                .copied()
                .enumerate()
                .map(|(position, node)| (node, position))
                .collect()
        );
    }

    #[test]
    fn counts_parallel_edges_before_releasing_a_node() {
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
            &uuids(&[10, 20, 30]),
            &[(0, 2), (0, 2), (1, 2)],
        );

        assert_eq!(
            stable_dag_topology(&graph, &control()).unwrap().order,
            [0, 1, 2]
        );
    }

    #[test]
    fn covers_empty_singleton_and_disconnected_graphs() {
        let empty = AdjacencyGraph::with_test_directed_edges(0, &[]);
        let singleton = AdjacencyGraph::with_test_directed_edges(1, &[]);
        let disconnected = AdjacencyGraph::with_test_directed_edges(5, &[(0, 1), (2, 3)]);

        assert!(
            stable_dag_topology(&empty, &control())
                .unwrap()
                .order
                .is_empty()
        );
        assert_eq!(
            stable_dag_topology(&singleton, &control()).unwrap().order,
            [0]
        );
        assert_eq!(
            stable_dag_topology(&disconnected, &control())
                .unwrap()
                .order,
            [0, 1, 2, 3, 4]
        );
    }

    #[test]
    fn rejects_undirected_self_loop_and_longer_cycles() {
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert_eq!(
            stable_dag_topology(&undirected, &control()).unwrap_err(),
            execution("DAG topology requires directed adjacency")
        );

        for graph in [
            AdjacencyGraph::with_test_directed_edges(1, &[(0, 0)]),
            AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]),
        ] {
            assert_eq!(
                stable_dag_topology(&graph, &control()).unwrap_err(),
                execution("selected graph contains a cycle")
            );
        }
    }

    #[test]
    fn cancellation_and_iteration_limits_are_structured() {
        let graph = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            stable_dag_topology(&graph, &cancelled).unwrap_err(),
            AlgorithmError::Cancelled
        );

        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            stable_dag_topology(&graph, &limited).unwrap_err(),
            AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            }
        );
    }

    #[test]
    fn deep_chain_is_stack_safe() {
        let nodes = 50_000_u64;
        let edges = (0..nodes - 1)
            .map(|source| (source, source + 1))
            .collect::<Vec<_>>();
        let graph = AdjacencyGraph::with_test_directed_edges(nodes, &edges);

        let topology = stable_dag_topology(&graph, &control()).unwrap();

        assert_eq!(topology.order.len(), usize::try_from(nodes).unwrap());
        assert_eq!(topology.order.first(), Some(&0));
        assert_eq!(topology.order.last(), Some(&(nodes - 1)));
    }
}
