//! Stack-safe, multigraph-aware low-link analysis shared by articulation points and bridges.

use std::collections::HashMap;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

const CHECKPOINT_INTERVAL: usize = 16_384;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LowLinkResult {
    pub articulation_nodes: Vec<u64>,
    pub bridge_edges: Vec<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProjectedEdge {
    edge_id: u64,
    edge_uuid: [u8; 16],
    left: usize,
    right: usize,
}

#[derive(Clone, Copy, Debug)]
struct DfsFrame {
    node: usize,
    next_neighbor: usize,
    child_count: usize,
}

/// Compute articulation-node and bridge-edge surrogates in one undirected traversal.
pub(crate) fn low_link(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<LowLinkResult, AlgorithmError> {
    control.check_cancelled()?;
    let mut work = 0_usize;
    let mut nodes = graph
        .node_ids()
        .iter()
        .map(|&node_id| {
            graph
                .node_uuid(node_id)
                .map(|node_uuid| (node_uuid, node_id))
                .ok_or_else(|| execution("low-link node has no UUID identity"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    nodes.sort_unstable();
    let node_index = nodes
        .iter()
        .enumerate()
        .map(|(index, &(_, node_id))| (node_id, index))
        .collect::<HashMap<_, _>>();

    let edges = project_edges(graph, &node_index, &nodes, control, &mut work)?;
    let mut adjacency = vec![Vec::<(usize, usize)>::new(); nodes.len()];
    for (edge_index, edge) in edges.iter().enumerate() {
        adjacency[edge.left].push((edge.right, edge_index));
        adjacency[edge.right].push((edge.left, edge_index));
    }
    for neighbors in &mut adjacency {
        neighbors.sort_unstable_by(|(left_node, left_edge), (right_node, right_edge)| {
            nodes[*left_node]
                .0
                .cmp(&nodes[*right_node].0)
                .then_with(|| {
                    edges[*left_edge]
                        .edge_uuid
                        .cmp(&edges[*right_edge].edge_uuid)
                })
        });
    }

    let (articulation, bridge) = classify_low_links(&adjacency, &edges, control, &mut work)?;
    Ok(LowLinkResult {
        articulation_nodes: nodes
            .iter()
            .enumerate()
            .filter_map(|(index, &(_, node_id))| articulation[index].then_some(node_id))
            .collect(),
        bridge_edges: edges
            .iter()
            .enumerate()
            .filter_map(|(index, edge)| bridge[index].then_some(edge.edge_id))
            .collect(),
    })
}

fn classify_low_links(
    adjacency: &[Vec<(usize, usize)>],
    edges: &[ProjectedEdge],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(Vec<bool>, Vec<bool>), AlgorithmError> {
    let mut discovery = vec![None; adjacency.len()];
    let mut low = vec![0_usize; adjacency.len()];
    let mut parent_edge = vec![None; adjacency.len()];
    let mut articulation = vec![false; adjacency.len()];
    let mut bridge = vec![false; edges.len()];
    let mut frames = Vec::new();
    let mut next_discovery = 0_usize;
    for root in 0..adjacency.len() {
        if discovery[root].is_some() {
            continue;
        }
        discover(
            root,
            &mut next_discovery,
            &mut discovery,
            &mut low,
            &mut frames,
        )?;
        while let Some(frame) = frames.last_mut() {
            checkpoint(control, work)?;
            let node = frame.node;
            if let Some(&(neighbor, edge_index)) = adjacency[node].get(frame.next_neighbor) {
                frame.next_neighbor += 1;
                if discovery[neighbor].is_none() {
                    frame.child_count = frame
                        .child_count
                        .checked_add(1)
                        .ok_or_else(|| execution("low-link child count exceeds usize"))?;
                    parent_edge[neighbor] = Some(edge_index);
                    discover(
                        neighbor,
                        &mut next_discovery,
                        &mut discovery,
                        &mut low,
                        &mut frames,
                    )?;
                } else if parent_edge[node] != Some(edge_index) {
                    low[node] =
                        low[node].min(discovery[neighbor].expect("visited node is discovered"));
                }
                continue;
            }

            let completed = frames.pop().expect("current low-link frame exists");
            if let Some(edge_index) = parent_edge[node] {
                let edge = edges[edge_index];
                let parent = if edge.left == node {
                    edge.right
                } else {
                    edge.left
                };
                low[parent] = low[parent].min(low[node]);
                let parent_discovery = discovery[parent].expect("parent is discovered");
                if low[node] > parent_discovery {
                    bridge[edge_index] = true;
                }
                if parent_edge[parent].is_some() && low[node] >= parent_discovery {
                    articulation[parent] = true;
                }
            } else if completed.child_count > 1 {
                articulation[node] = true;
            }
        }
    }

    Ok((articulation, bridge))
}

fn project_edges(
    graph: &AdjacencyGraph,
    node_index: &HashMap<u64, usize>,
    nodes: &[([u8; 16], u64)],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<ProjectedEdge>, AlgorithmError> {
    let mut by_uuid = HashMap::<[u8; 16], ProjectedEdge>::new();
    for &source_id in graph.node_ids() {
        let source = node_index
            .get(&source_id)
            .copied()
            .ok_or_else(|| execution("low-link source is outside node selection"))?;
        for edge in graph.neighbors(source_id) {
            checkpoint(control, work)?;
            let target = node_index
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("low-link edge endpoint is outside node selection"))?;
            if source == target {
                continue;
            }
            let (left, right) = if nodes[source].0 < nodes[target].0 {
                (source, target)
            } else {
                (target, source)
            };
            let candidate = ProjectedEdge {
                edge_id: edge.edge_id,
                edge_uuid: edge.edge_uuid,
                left,
                right,
            };
            match by_uuid.entry(edge.edge_uuid) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
                std::collections::hash_map::Entry::Occupied(entry) if *entry.get() != candidate => {
                    return Err(execution(
                        "low-link edge UUID has inconsistent adjacency entries",
                    ));
                }
                std::collections::hash_map::Entry::Occupied(_) => {}
            }
        }
    }
    let mut edges = by_uuid.into_values().collect::<Vec<_>>();
    edges.sort_unstable_by(|left, right| {
        nodes[left.left]
            .0
            .cmp(&nodes[right.left].0)
            .then_with(|| nodes[left.right].0.cmp(&nodes[right.right].0))
            .then_with(|| left.edge_uuid.cmp(&right.edge_uuid))
    });
    Ok(edges)
}

fn discover(
    node: usize,
    next_discovery: &mut usize,
    discovery: &mut [Option<usize>],
    low: &mut [usize],
    frames: &mut Vec<DfsFrame>,
) -> Result<(), AlgorithmError> {
    discovery[node] = Some(*next_discovery);
    low[node] = *next_discovery;
    *next_discovery = next_discovery
        .checked_add(1)
        .ok_or_else(|| execution("low-link discovery index exceeds usize"))?;
    frames.push(DfsFrame {
        node,
        next_neighbor: 0,
        child_count: 0,
    });
    Ok(())
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
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

    #[test]
    fn finds_low_links_across_undirected_multigraph_components() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(
            11,
            &[
                (0, 0, 1),
                (1, 1, 2),
                (2, 1, 1),
                (3, 3, 4),
                (4, 4, 5),
                (5, 5, 3),
                (6, 7, 8),
                (7, 7, 8),
                (8, 9, 10),
                (9, 10, 9),
            ],
        );

        let expected = LowLinkResult {
            articulation_nodes: vec![1],
            bridge_edges: vec![0, 1],
        };
        assert_eq!(low_link(&graph, &control()).unwrap(), expected);
        assert_eq!(low_link(&graph, &control()).unwrap(), expected);
    }

    #[test]
    fn deep_chain_is_stack_safe() {
        const NODES: u64 = 100_000;
        let edges = (0..NODES - 1)
            .map(|node| (node, node, node + 1))
            .collect::<Vec<_>>();
        let graph = AdjacencyGraph::with_test_undirected_multigraph(NODES, &edges);

        let result = low_link(&graph, &control()).unwrap();
        assert_eq!(result.articulation_nodes.len(), NODES as usize - 2);
        assert_eq!(result.articulation_nodes.first(), Some(&1));
        assert_eq!(result.articulation_nodes.last(), Some(&(NODES - 2)));
        assert_eq!(result.bridge_edges.len(), NODES as usize - 1);
        assert_eq!(result.bridge_edges.first(), Some(&0));
        assert_eq!(result.bridge_edges.last(), Some(&(NODES - 2)));
    }

    #[test]
    fn cancellation_and_iteration_limits_abort_without_output() {
        let graph = AdjacencyGraph::with_test_undirected_multigraph(2, &[(0, 0, 1)]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            low_link(
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );

        let limits = AlgorithmLimits {
            iterations: 0,
            ..AlgorithmLimits::default()
        };
        assert!(matches!(
            low_link(
                &graph,
                &AlgorithmControl::new(limits, AlgorithmCancellation::default())
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        ));
    }
}
