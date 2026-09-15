//! K core rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BUILTIN_REVIEW, RankAlgorithm, RustAlgorithm, exact_u64_as_f64, execution,
    k_core_numbers, rank_scores_output,
};

pub(super) struct KCore;

impl RustAlgorithm for KCore {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::KCore),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::KCore);
        rank_scores_output(algorithm, graph, k_core_scores(graph, control)?, control)
    }
}

fn k_core_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    k_core_numbers(graph, control)?
        .into_iter()
        .map(|core| {
            let core = u64::try_from(core)
                .map_err(|_| execution("k-core score exceeds supported range"))?;
            exact_u64_as_f64(core, "k-core score")
        })
        .collect()
}

#[cfg(test)]
mod tests;
