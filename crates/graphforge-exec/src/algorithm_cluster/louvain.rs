//! Louvain execution.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BTreeMap, BUILTIN_REVIEW, ClusterAlgorithm, LouvainExecutionPath,
    RustAlgorithm, checkpoint_chunk, community_output, condense, local_moves_from,
    normalized_communities_with_progress, select_louvain_path,
};

pub(super) struct Louvain;

impl RustAlgorithm for Louvain {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::Louvain),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = louvain_communities(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::Louvain, control)
    }
}

fn louvain_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    louvain_communities_with_progress(graph, control, |_| {})
}

fn louvain_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    progress: impl FnMut(usize),
) -> Result<Vec<usize>, AlgorithmError> {
    match select_louvain_path(control, graph.node_ids().len(), graph.edge_entry_count()) {
        LouvainExecutionPath::SerialLocalMoves => {}
    }
    let node_count = graph.node_ids().len();
    let (mut weights, mut members) =
        normalized_communities_with_progress(graph, control, progress)?;

    loop {
        let assignment = local_moves(&weights, control)?;
        let count = assignment.iter().copied().max().map_or(0, |id| id + 1);
        if count == weights.len() {
            break;
        }
        let (next_weights, next_members) =
            condense(&weights, &members, &assignment, count, control)?;
        weights = next_weights;
        members = next_members;
    }

    let mut result = vec![0; node_count];
    let mut work = 0_usize;
    for (community, nodes) in members.iter().enumerate() {
        for &node in nodes {
            checkpoint_chunk(control, &mut work)?;
            result[node] = community;
        }
    }
    Ok(result)
}

fn local_moves(
    weights: &[BTreeMap<usize, f64>],
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    local_moves_from(weights, None, "Louvain", control)
}

#[cfg(test)]
mod tests;
