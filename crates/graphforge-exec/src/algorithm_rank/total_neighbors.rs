//! Total neighbors rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AssertUnwindSafe, BUILTIN_REVIEW, IntoParallelRefIterator, ParallelIterator,
    RankAlgorithm, RustAlgorithm, TOTAL_NEIGHBORS_PARALLEL_CROSSOVER_WORK, catch_unwind,
    exact_u64_as_f64, execution, first_chunk_error, rank_scores_output, simple_neighbors,
    source_chunks, usize_to_u64_saturating,
};

pub(super) struct TotalNeighbors;

const TOTAL_NEIGHBORS_CHECKPOINT_INTERVAL: usize = 1_024;

/// Selected total-neighbors execution path for observability and crossover tests (#514).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TotalNeighborsExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

impl RustAlgorithm for TotalNeighbors {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::TotalNeighbors),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::TotalNeighbors);
        rank_scores_output(
            algorithm,
            graph,
            total_neighbor_scores(graph, control)?,
            control,
        )
    }
}

fn total_neighbor_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let degrees: Vec<u64> = neighbors
        .iter()
        .map(|adjacent| {
            u64::try_from(adjacent.len())
                .map_err(|_| execution("total-neighbors degree exceeds supported range"))
        })
        .collect::<Result<_, _>>()?;
    let estimated_work = estimated_total_neighbors_work(&neighbors);
    match select_total_neighbors_path(control, neighbors.len(), estimated_work) {
        TotalNeighborsExecutionPath::Serial => {
            total_neighbor_scores_serial(&neighbors, &degrees, control)
        }
        TotalNeighborsExecutionPath::Parallel { .. } => {
            total_neighbor_scores_parallel(&neighbors, &degrees, control)
        }
    }
}

fn total_neighbor_scores_serial(
    neighbors: &[Vec<usize>],
    degrees: &[u64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for source in 0..neighbors.len() {
        let score = total_neighbor_source_score(neighbors, degrees, source, || {
            total_neighbors_checkpoint(control, &mut visited)
        })?;
        scores.push(exact_u64_as_f64(score, "total-neighbors score")?);
    }
    Ok(scores)
}

fn total_neighbor_scores_parallel(
    neighbors: &[Vec<usize>],
    degrees: &[u64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel total-neighbors requires an instance-owned compute pool")
    })?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let chunk_results = run_total_neighbors_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut work = 0_usize;
                let mut local = Vec::with_capacity(end - start);
                for source in start..end {
                    let score = total_neighbor_source_score(neighbors, degrees, source, || {
                        total_neighbors_checkpoint(control, &mut work)
                    })?;
                    local.push(exact_u64_as_f64(score, "total-neighbors score")?);
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

fn total_neighbor_source_score(
    neighbors: &[Vec<usize>],
    degrees: &[u64],
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
        let mut union = degrees[source]
            .checked_add(degrees[candidate])
            .ok_or_else(|| execution("total-neighbors pair score exceeds supported range"))?;
        let (mut left, mut right) = (0, 0);
        while left < source_neighbors.len() && right < candidate_neighbors.len() {
            checkpoint()?;
            match source_neighbors[left].cmp(&candidate_neighbors[right]) {
                std::cmp::Ordering::Less => left += 1,
                std::cmp::Ordering::Greater => right += 1,
                std::cmp::Ordering::Equal => {
                    union = union
                        .checked_sub(1)
                        .ok_or_else(|| execution("total-neighbors pair score underflow"))?;
                    left += 1;
                    right += 1;
                }
            }
        }
        score = score
            .checked_add(union)
            .ok_or_else(|| execution("total-neighbors score exceeds supported range"))?;
    }
    Ok(score)
}

fn total_neighbors_checkpoint(
    control: &AlgorithmControl,
    visited: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*visited).is_multiple_of(TOTAL_NEIGHBORS_CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    *visited = visited.saturating_add(1);
    Ok(())
}

fn estimated_total_neighbors_work(neighbors: &[Vec<usize>]) -> u64 {
    let sources = usize_to_u64_saturating(neighbors.len());
    let degree_sum = neighbors.iter().fold(0_u64, |total, adjacent| {
        total.saturating_add(usize_to_u64_saturating(adjacent.len()))
    });
    sources
        .saturating_mul(sources)
        .saturating_add(sources.saturating_mul(degree_sum).saturating_mul(2))
}

/// Choose serial vs private-pool parallel execution for a total-neighbors workload.
pub(crate) fn select_total_neighbors_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_work: u64,
) -> TotalNeighborsExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || sources <= 1
        || estimated_work < TOTAL_NEIGHBORS_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return TotalNeighborsExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return TotalNeighborsExecutionPath::Serial;
    }
    TotalNeighborsExecutionPath::Parallel { threads, chunks }
}

fn run_total_neighbors_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("total-neighbors worker panicked")),
    }
}

#[cfg(test)]
mod tests;
