//! Modularity optimization execution.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BUILTIN_REVIEW, ClusterAlgorithm, RustAlgorithm, community_output,
    local_moves_from, normalized_communities,
};

pub(super) struct ModularityOptimization;

impl RustAlgorithm for ModularityOptimization {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::ModularityOptimization),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = modularity_optimization_communities(graph, control)?;
        community_output(
            graph,
            &communities,
            ClusterAlgorithm::ModularityOptimization,
            control,
        )
    }
}

fn modularity_optimization_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let (weights, _) = normalized_communities(graph, control)?;
    local_moves_from(&weights, None, "Modularity optimization", control)
}

#[cfg(test)]
mod tests;
