//! Deterministic, non-recursive Tarjan strongly-connected-components kernel.

use std::collections::HashMap;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

const CANCELLATION_INTERVAL: usize = 16_384;

#[derive(Clone, Copy, Debug)]
struct DfsFrame {
    node: usize,
    next_neighbor: usize,
    parent: Option<usize>,
}

/// Return consecutive component labels ordered by first topology member.
pub(crate) fn strongly_connected_labels(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    control.check_cancelled()?;
    let node_count = graph.node_ids().len();
    let indices = graph
        .node_ids()
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect::<HashMap<_, _>>();
    let mut adjacency = vec![Vec::new(); node_count];
    let mut work = 0_usize;
    for (source, &node) in graph.node_ids().iter().enumerate() {
        for edge in graph.neighbors(node) {
            checkpoint(control, &mut work)?;
            adjacency[source].push(
                indices
                    .get(&edge.neighbor_id)
                    .copied()
                    .ok_or_else(|| execution("adjacency references an unselected node"))?,
            );
        }
    }

    let mut discovery = vec![None; node_count];
    let mut lowlink = vec![0_usize; node_count];
    let mut on_stack = vec![false; node_count];
    let mut component_stack = Vec::new();
    let mut frames = Vec::new();
    let mut raw_labels = vec![usize::MAX; node_count];
    let mut next_discovery = 0_usize;
    let mut component_count = 0_usize;

    for root in 0..node_count {
        if discovery[root].is_some() {
            continue;
        }
        discover(
            root,
            None,
            &mut next_discovery,
            &mut discovery,
            &mut lowlink,
            &mut on_stack,
            &mut component_stack,
            &mut frames,
        )?;

        while let Some(frame) = frames.last_mut() {
            checkpoint(control, &mut work)?;
            let node = frame.node;
            if let Some(&neighbor) = adjacency[node].get(frame.next_neighbor) {
                frame.next_neighbor += 1;
                if discovery[neighbor].is_none() {
                    discover(
                        neighbor,
                        Some(node),
                        &mut next_discovery,
                        &mut discovery,
                        &mut lowlink,
                        &mut on_stack,
                        &mut component_stack,
                        &mut frames,
                    )?;
                } else if on_stack[neighbor] {
                    lowlink[node] = lowlink[node].min(discovery[neighbor].expect("discovered"));
                }
                continue;
            }

            let completed = frames.pop().expect("current DFS frame exists");
            if let Some(parent) = completed.parent {
                lowlink[parent] = lowlink[parent].min(lowlink[node]);
            }
            if lowlink[node] == discovery[node].expect("completed node is discovered") {
                loop {
                    let member = component_stack
                        .pop()
                        .ok_or_else(|| execution("Tarjan component stack underflow"))?;
                    on_stack[member] = false;
                    raw_labels[member] = component_count;
                    if member == node {
                        break;
                    }
                }
                component_count = component_count
                    .checked_add(1)
                    .ok_or_else(|| execution("component count exceeds usize"))?;
            }
        }
    }

    canonical_labels(&raw_labels, component_count)
}

#[allow(clippy::too_many_arguments)]
fn discover(
    node: usize,
    parent: Option<usize>,
    next_discovery: &mut usize,
    discovery: &mut [Option<usize>],
    lowlink: &mut [usize],
    on_stack: &mut [bool],
    component_stack: &mut Vec<usize>,
    frames: &mut Vec<DfsFrame>,
) -> Result<(), AlgorithmError> {
    discovery[node] = Some(*next_discovery);
    lowlink[node] = *next_discovery;
    *next_discovery = next_discovery
        .checked_add(1)
        .ok_or_else(|| execution("Tarjan discovery index exceeds usize"))?;
    on_stack[node] = true;
    component_stack.push(node);
    frames.push(DfsFrame {
        node,
        next_neighbor: 0,
        parent,
    });
    Ok(())
}

fn canonical_labels(raw: &[usize], component_count: usize) -> Result<Vec<usize>, AlgorithmError> {
    let mut canonical = vec![usize::MAX; component_count];
    let mut next = 0_usize;
    raw.iter()
        .map(|&component| {
            let label = canonical
                .get_mut(component)
                .ok_or_else(|| execution("Tarjan produced an invalid component label"))?;
            if *label == usize::MAX {
                *label = next;
                next = next
                    .checked_add(1)
                    .ok_or_else(|| execution("canonical component count exceeds usize"))?;
            }
            Ok(*label)
        })
        .collect()
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    if work.is_multiple_of(CANCELLATION_INTERVAL) {
        control.checkpoint()?;
    }
    *work = work.saturating_add(1);
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
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn run(nodes: u64, edges: &[(u64, u64)]) -> Vec<usize> {
        strongly_connected_labels(
            &AdjacencyGraph::with_test_directed_edges(nodes, edges),
            &control(),
        )
        .unwrap()
    }

    #[test]
    fn directed_cycles_bridges_and_chains_have_exact_stable_labels() {
        let edges = [(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 3), (4, 5)];
        assert_eq!(run(6, &edges), [0, 0, 0, 1, 1, 2]);
        assert_eq!(run(6, &edges), [0, 0, 0, 1, 1, 2]);
        assert_eq!(run(3, &[(0, 1), (1, 2)]), [0, 1, 2]);
    }

    #[test]
    fn symmetrized_parallel_loops_disconnected_and_empty_are_stable() {
        assert_eq!(run(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]), [0, 0, 0]);
        assert_eq!(run(3, &[(0, 0), (0, 1), (0, 1), (1, 0)]), [0, 0, 1]);
        assert_eq!(run(3, &[]), [0, 1, 2]);
        assert!(run(0, &[]).is_empty());
    }

    #[test]
    fn relationship_weights_do_not_change_membership() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)])
            .with_test_edge_weights(&[f64::NAN, -1.0, f64::MAX]);
        assert_eq!(
            strongly_connected_labels(&graph, &control()).unwrap(),
            [0, 0, 0]
        );
    }

    #[test]
    fn every_three_node_digraph_matches_mutual_reachability() {
        for bits in 0_u16..(1 << 9) {
            let mut edges = Vec::new();
            let mut reach = [[false; 3]; 3];
            for (node, row) in reach.iter_mut().enumerate() {
                row[node] = true;
            }
            for (source, row) in reach.iter_mut().enumerate() {
                for (target, cell) in row.iter_mut().enumerate() {
                    if bits & (1 << (source * 3 + target)) != 0 {
                        *cell = true;
                        edges.push((source as u64, target as u64));
                    }
                }
            }
            for pivot in 0..3 {
                for source in 0..3 {
                    for target in 0..3 {
                        reach[source][target] |= reach[source][pivot] && reach[pivot][target];
                    }
                }
            }
            let mut expected = [usize::MAX; 3];
            let mut next = 0;
            for node in 0..3 {
                expected[node] = (0..node)
                    .find(|&other| reach[node][other] && reach[other][node])
                    .map_or_else(
                        || {
                            let label = next;
                            next += 1;
                            label
                        },
                        |other| expected[other],
                    );
            }
            assert_eq!(run(3, &edges), expected, "edge bitmap {bits:#x}");
        }
    }

    #[test]
    fn deep_graph_is_iterative_and_failures_are_structured() {
        let edges = (0..50_000).map(|node| (node, node + 1)).collect::<Vec<_>>();
        let labels = run(50_001, &edges);
        assert_eq!(labels.len(), 50_001);
        assert_eq!(labels[0], 0);
        assert_eq!(labels[50_000], 50_000);

        assert!(matches!(
            strongly_connected_labels(
                &AdjacencyGraph::with_test_directed_edges(2, &[(0, 2)]),
                &control(),
            ),
            Err(AlgorithmError::Execution { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            strongly_connected_labels(
                &AdjacencyGraph::with_test_directed_edges(1, &[]),
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
