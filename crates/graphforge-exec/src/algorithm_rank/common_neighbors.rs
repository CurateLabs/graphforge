//! Common neighbors rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AssertUnwindSafe, BUILTIN_REVIEW, COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK,
    IntoParallelRefIterator, ParallelIterator, RankAlgorithm, RustAlgorithm, catch_unwind,
    exact_u64_as_f64, execution, first_chunk_error, rank_scores_output, simple_neighbors,
    source_chunks, usize_to_u64_saturating,
};

pub(super) struct CommonNeighbors;

const COMMON_NEIGHBORS_CHECKPOINT_INTERVAL: usize = 1_024;

/// Selected common-neighbors execution path for observability and crossover tests (#505).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommonNeighborsExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

impl RustAlgorithm for CommonNeighbors {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::CommonNeighbors),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::CommonNeighbors);
        rank_scores_output(
            algorithm,
            graph,
            common_neighbor_scores(graph, control)?,
            control,
        )
    }
}

fn common_neighbor_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let estimated_work = estimated_common_neighbors_work(&neighbors);
    match select_common_neighbors_path(control, neighbors.len(), estimated_work) {
        CommonNeighborsExecutionPath::Serial => common_neighbor_scores_serial(&neighbors, control),
        CommonNeighborsExecutionPath::Parallel { .. } => {
            common_neighbor_scores_parallel(&neighbors, control)
        }
    }
}

fn common_neighbor_scores_serial(
    neighbors: &[Vec<usize>],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for source in 0..neighbors.len() {
        let score = common_neighbor_source_score(neighbors, source, || {
            common_neighbors_checkpoint(control, &mut visited)
        })?;
        scores.push(exact_u64_as_f64(score, "common-neighbors score")?);
    }
    Ok(scores)
}

fn common_neighbor_scores_parallel(
    neighbors: &[Vec<usize>],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel common-neighbors requires an instance-owned compute pool")
    })?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let chunk_results = run_common_neighbors_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut work = 0_usize;
                let mut local = Vec::with_capacity(end - start);
                for source in start..end {
                    let score = common_neighbor_source_score(neighbors, source, || {
                        common_neighbors_checkpoint(control, &mut work)
                    })?;
                    local.push(exact_u64_as_f64(score, "common-neighbors score")?);
                }
                Ok((start, local))
            })
            .collect::<Vec<Result<_, AlgorithmError>>>();
        first_chunk_error(results)
    })?;

    let mut scores = Vec::with_capacity(neighbors.len());
    for (_start, local) in chunk_results {
        scores.extend(local);
    }
    Ok(scores)
}

fn common_neighbor_source_score(
    neighbors: &[Vec<usize>],
    source: usize,
    mut checkpoint: impl FnMut() -> Result<(), AlgorithmError>,
) -> Result<u64, AlgorithmError> {
    let source_neighbors = &neighbors[source];
    let mut score = 0_u64;
    for (candidate, candidate_neighbors) in neighbors.iter().enumerate() {
        checkpoint()?;
        if source == candidate || source_neighbors.binary_search(&candidate).is_ok() {
            continue;
        }
        let (mut left, mut right) = (0, 0);
        while left < source_neighbors.len() && right < candidate_neighbors.len() {
            checkpoint()?;
            match source_neighbors[left].cmp(&candidate_neighbors[right]) {
                std::cmp::Ordering::Less => left += 1,
                std::cmp::Ordering::Greater => right += 1,
                std::cmp::Ordering::Equal => {
                    score = score.checked_add(1).ok_or_else(|| {
                        execution("common-neighbors score exceeds supported range")
                    })?;
                    left += 1;
                    right += 1;
                }
            }
        }
    }
    Ok(score)
}

fn common_neighbors_checkpoint(
    control: &AlgorithmControl,
    visited: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*visited).is_multiple_of(COMMON_NEIGHBORS_CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    *visited = visited.saturating_add(1);
    Ok(())
}

fn estimated_common_neighbors_work(neighbors: &[Vec<usize>]) -> u64 {
    let sources = usize_to_u64_saturating(neighbors.len());
    let degree_sum = neighbors.iter().fold(0_u64, |total, adjacent| {
        total.saturating_add(usize_to_u64_saturating(adjacent.len()))
    });
    sources
        .saturating_mul(sources)
        .saturating_add(sources.saturating_mul(degree_sum).saturating_mul(2))
}

/// Choose serial vs private-pool parallel execution for a common-neighbors workload.
pub(crate) fn select_common_neighbors_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_work: u64,
) -> CommonNeighborsExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || sources <= 1
        || estimated_work < COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return CommonNeighborsExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return CommonNeighborsExecutionPath::Serial;
    }
    CommonNeighborsExecutionPath::Parallel { threads, chunks }
}

fn run_common_neighbors_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("common-neighbors worker panicked")),
    }
}

#[cfg(test)]
mod tests;
