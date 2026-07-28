//! Deterministic simple-adjacency normalization shared by Rust algorithms.

use std::collections::HashMap;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

pub(crate) fn simple_undirected_neighbors(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<Vec<usize>>, AlgorithmError> {
    simple_neighbors(graph, control, true)
}

pub(crate) fn simple_neighbors(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    symmetrize: bool,
) -> Result<Vec<Vec<usize>>, AlgorithmError> {
    let node_ids = graph.node_ids();
    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut neighbors = vec![Vec::new(); node_ids.len()];
    let mut traversed_edges = 0_usize;
    for (source, &node_id) in node_ids.iter().enumerate() {
        for edge in graph.neighbors(node_id) {
            if traversed_edges.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            traversed_edges += 1;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            if source != target {
                neighbors[source].push(target);
                if symmetrize {
                    neighbors[target].push(source);
                }
            }
        }
    }
    for adjacent in &mut neighbors {
        adjacent.sort_unstable();
        adjacent.dedup();
    }
    Ok(neighbors)
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}
