use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;
use std::collections::{HashMap, HashSet};

const CHECKPOINT_INTERVAL: usize = 16_384;
#[derive(Clone, Copy, Debug)]
struct DfsFrame(usize, usize);

pub(crate) fn biconnected_labels(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut work = 0;
    let blocks = biconnected_blocks(graph, control, &mut work)?;
    primary_labels(graph.node_ids().len(), &blocks, control, &mut work)
}

fn biconnected_blocks(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<Vec<usize>>, AlgorithmError> {
    control.check_cancelled()?;
    let node_count = graph.node_ids().len();
    let indices = graph
        .node_ids()
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect::<HashMap<_, _>>();
    let mut edges = HashSet::new();
    for (source, &node) in graph.node_ids().iter().enumerate() {
        for edge in graph.neighbors(node) {
            checkpoint(control, work)?;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            if source != target {
                edges.insert((source.min(target), source.max(target)));
            }
        }
    }
    let mut edges = edges.into_iter().collect::<Vec<_>>();
    edges.sort_unstable();
    let mut adjacency = vec![Vec::new(); node_count];
    for (left, right) in edges {
        adjacency[left].push(right);
        adjacency[right].push(left);
    }
    for neighbors in &mut adjacency {
        neighbors.sort_unstable();
    }
    let mut discovery = vec![None; node_count];
    let mut lowlink = vec![0_usize; node_count];
    let mut parent = vec![None; node_count];
    let mut frames = Vec::new();
    let mut edge_stack = Vec::new();
    let mut blocks = Vec::new();
    let mut next_discovery = 0_usize;
    for root in 0..node_count {
        if discovery[root].is_some() {
            continue;
        }
        discover(root, &mut next_discovery, &mut discovery, &mut lowlink)?;
        frames.push(DfsFrame(root, 0));
        while let Some(frame) = frames.last_mut() {
            checkpoint(control, work)?;
            let node = frame.0;
            if let Some(&neighbor) = adjacency[node].get(frame.1) {
                frame.1 += 1;
                if discovery[neighbor].is_none() {
                    parent[neighbor] = Some(node);
                    edge_stack.push((node, neighbor));
                    discover(neighbor, &mut next_discovery, &mut discovery, &mut lowlink)?;
                    frames.push(DfsFrame(neighbor, 0));
                } else if parent[node] != Some(neighbor) && discovery[neighbor] < discovery[node] {
                    lowlink[node] = lowlink[node]
                        .min(discovery[neighbor].expect("visited neighbor is discovered"));
                    edge_stack.push((node, neighbor));
                }
                continue;
            }
            frames.pop();
            if let Some(parent_node) = parent[node] {
                lowlink[parent_node] = lowlink[parent_node].min(lowlink[node]);
                if lowlink[node]
                    >= discovery[parent_node].expect("parent is discovered before child")
                {
                    blocks.push(pop_block(
                        &mut edge_stack,
                        (parent_node, node),
                        control,
                        work,
                    )?);
                }
            }
        }
        if !edge_stack.is_empty() {
            return Err(execution("biconnected edge stack was not exhausted"));
        }
    }
    blocks.sort_unstable();
    Ok(blocks)
}

fn discover(
    node: usize,
    next: &mut usize,
    discovery: &mut [Option<usize>],
    lowlink: &mut [usize],
) -> Result<(), AlgorithmError> {
    discovery[node] = Some(*next);
    lowlink[node] = *next;
    *next = next
        .checked_add(1)
        .ok_or_else(|| execution("biconnected discovery index exceeds usize"))?;
    Ok(())
}

fn pop_block(
    stack: &mut Vec<(usize, usize)>,
    boundary: (usize, usize),
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut block = Vec::new();
    loop {
        checkpoint(control, work)?;
        let edge = stack
            .pop()
            .ok_or_else(|| execution("biconnected edge stack underflow"))?;
        block.extend([edge.0, edge.1]);
        if edge == boundary {
            break;
        }
    }
    block.sort_unstable();
    block.dedup();
    Ok(block)
}

fn primary_labels(
    nodes: usize,
    blocks: &[Vec<usize>],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut owners = vec![None; nodes];
    for (block_id, block) in blocks.iter().enumerate() {
        for &node in block {
            checkpoint(control, work)?;
            let owner = owners
                .get_mut(node)
                .ok_or_else(|| execution("biconnected block contains an invalid node"))?;
            owner.get_or_insert(block_id);
        }
    }
    let mut next_owner = blocks.len();
    for owner in &mut owners {
        checkpoint(control, work)?;
        if owner.is_none() {
            *owner = Some(next_owner);
            next_owner = next_owner
                .checked_add(1)
                .ok_or_else(|| execution("biconnected primary count exceeds usize"))?;
        }
    }
    let mut canonical = HashMap::new();
    let mut next_label = 0_usize;
    owners
        .into_iter()
        .map(|owner| {
            let owner = owner.expect("every node receives a primary block");
            Ok(*canonical.entry(owner).or_insert_with(|| {
                let label = next_label;
                next_label += 1;
                label
            }))
        })
        .collect()
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

    fn blocks(graph: &AdjacencyGraph) -> Vec<Vec<usize>> {
        biconnected_blocks(graph, &control(), &mut 0).unwrap()
    }

    #[test]
    fn preserves_overlapping_blocks_then_projects_primary_membership() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            7,
            &[
                (0, 1),
                (1, 2),
                (2, 0),
                (2, 3),
                (3, 4),
                (4, 2),
                (4, 5),
                (1, 0),
                (2, 2),
                (0, 1),
            ],
        );
        let expected = [vec![0, 1, 2], vec![2, 3, 4], vec![4, 5]];
        assert_eq!(blocks(&graph), expected);
        assert_eq!(blocks(&graph), expected);
        assert_eq!(
            biconnected_labels(&graph, &control()).unwrap(),
            [0, 0, 0, 1, 1, 2, 3]
        );
    }

    #[test]
    fn covers_cliques_bridge_dyads_and_boundaries_deterministically() {
        let clique =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]);
        assert_eq!(blocks(&clique), [vec![0, 1, 2, 3]]);
        let path = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2), (2, 3)]);
        assert_eq!(blocks(&path), [vec![0, 1], vec![1, 2], vec![2, 3]]);
        assert_eq!(biconnected_labels(&path, &control()).unwrap(), [0, 0, 1, 2]);
        let empty_block =
            AdjacencyGraph::with_test_edges(6, &[(0, 3), (1, 4), (2, 5), (3, 4), (4, 5), (5, 3)]);
        assert_eq!(
            biconnected_labels(&empty_block, &control()).unwrap(),
            [0, 1, 2, 0, 1, 2]
        );
        assert_eq!(
            biconnected_labels(&AdjacencyGraph::with_test_edges(3, &[]), &control()).unwrap(),
            [0, 1, 2]
        );
        assert!(
            biconnected_labels(&AdjacencyGraph::default(), &control())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn iterative_depth_and_control_failures_are_structured() {
        let edges = (0..20_000).map(|node| (node, node + 1)).collect::<Vec<_>>();
        let labels =
            biconnected_labels(&AdjacencyGraph::with_test_edges(20_001, &edges), &control())
                .unwrap();
        assert_eq!((labels[0], labels[20_000]), (0, 19_999));

        let post_dfs = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            biconnected_labels(&AdjacencyGraph::with_test_edges(16_385, &[]), &post_dfs),
            Err(AlgorithmError::IterationLimit {
                observed: 3,
                limit: 2
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            biconnected_labels(
                &AdjacencyGraph::default(),
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
