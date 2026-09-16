//! Preferential attachment rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AssertUnwindSafe, BUILTIN_REVIEW, IntoParallelRefIterator,
    PREFERENTIAL_ATTACHMENT_PARALLEL_CROSSOVER_WORK, ParallelIterator, RankAlgorithm,
    RustAlgorithm, catch_unwind, exact_u64_as_f64, execution, first_chunk_error,
    rank_scores_output, simple_neighbors, source_chunks, usize_to_u64_saturating,
};

pub(super) struct PreferentialAttachment;

const PREFERENTIAL_ATTACHMENT_CHECKPOINT_INTERVAL: usize = 1_024;

impl RustAlgorithm for PreferentialAttachment {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::PreferentialAttachment),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::PreferentialAttachment);
        rank_scores_output(
            algorithm,
            graph,
            preferential_attachment_scores(graph, control)?,
            control,
        )
    }
}

fn preferential_attachment_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    // For each node u, sum deg(u) * deg(v) over every missing outgoing
    // candidate v. Algebraic aggregation avoids materializing O(V^2) pairs.
    let neighbors = simple_neighbors(graph, control, false)?;
    let degrees = preferential_attachment_degrees(&neighbors)?;
    let total_degree = preferential_attachment_total_degree(&degrees)?;
    let estimated_work = estimated_preferential_attachment_work(&neighbors);
    match select_preferential_attachment_path(control, neighbors.len(), estimated_work) {
        PreferentialAttachmentExecutionPath::Serial => {
            preferential_attachment_scores_serial(&neighbors, &degrees, total_degree, control)
        }
        PreferentialAttachmentExecutionPath::Parallel { .. } => {
            preferential_attachment_scores_parallel(&neighbors, &degrees, total_degree, control)
        }
    }
}

fn preferential_attachment_degrees(neighbors: &[Vec<usize>]) -> Result<Vec<u64>, AlgorithmError> {
    neighbors
        .iter()
        .map(|adjacent| {
            u64::try_from(adjacent.len())
                .map_err(|_| execution("preferential-attachment degree exceeds supported range"))
        })
        .collect()
}

fn preferential_attachment_total_degree(degrees: &[u64]) -> Result<u64, AlgorithmError> {
    degrees.iter().try_fold(0_u64, |total, degree| {
        total
            .checked_add(*degree)
            .ok_or_else(|| execution("preferential-attachment degree sum exceeds supported range"))
    })
}

fn preferential_attachment_scores_serial(
    neighbors: &[Vec<usize>],
    degrees: &[u64],
    total_degree: u64,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut visited_neighbors = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for source in 0..neighbors.len() {
        let score =
            preferential_attachment_source_score(neighbors, degrees, total_degree, source, || {
                preferential_attachment_checkpoint(control, &mut visited_neighbors)
            })?;
        scores.push(exact_u64_as_f64(score, "preferential-attachment score")?);
    }
    Ok(scores)
}

fn preferential_attachment_scores_parallel(
    neighbors: &[Vec<usize>],
    degrees: &[u64],
    total_degree: u64,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel preferential-attachment requires an instance-owned compute pool")
    })?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let chunk_results = run_preferential_attachment_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut work = 0_usize;
                let mut local = Vec::with_capacity(end - start);
                for source in start..end {
                    let score = preferential_attachment_source_score(
                        neighbors,
                        degrees,
                        total_degree,
                        source,
                        || preferential_attachment_checkpoint(control, &mut work),
                    )?;
                    local.push(exact_u64_as_f64(score, "preferential-attachment score")?);
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

fn preferential_attachment_source_score(
    neighbors: &[Vec<usize>],
    degrees: &[u64],
    total_degree: u64,
    source: usize,
    mut checkpoint: impl FnMut() -> Result<(), AlgorithmError>,
) -> Result<u64, AlgorithmError> {
    checkpoint()?;
    let linked_degree = neighbors[source]
        .iter()
        .try_fold(0_u64, |total, &neighbor| {
            checkpoint()?;
            total.checked_add(degrees[neighbor]).ok_or_else(|| {
                execution("preferential-attachment neighbor sum exceeds supported range")
            })
        })?;
    let candidate_degree = total_degree
        .checked_sub(degrees[source])
        .and_then(|remaining| remaining.checked_sub(linked_degree))
        .ok_or_else(|| execution("preferential-attachment candidate sum underflow"))?;
    degrees[source]
        .checked_mul(candidate_degree)
        .ok_or_else(|| execution("preferential-attachment score exceeds supported range"))
}

fn preferential_attachment_checkpoint(
    control: &AlgorithmControl,
    visited: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*visited).is_multiple_of(PREFERENTIAL_ATTACHMENT_CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    *visited = visited.saturating_add(1);
    Ok(())
}

fn estimated_preferential_attachment_work(neighbors: &[Vec<usize>]) -> u64 {
    neighbors.iter().fold(
        usize_to_u64_saturating(neighbors.len()),
        |total, adjacent| total.saturating_add(usize_to_u64_saturating(adjacent.len())),
    )
}

/// Choose serial vs private-pool parallel execution for preferential attachment.
pub(crate) fn select_preferential_attachment_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_work: u64,
) -> PreferentialAttachmentExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || sources <= 1
        || estimated_work < PREFERENTIAL_ATTACHMENT_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return PreferentialAttachmentExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return PreferentialAttachmentExecutionPath::Serial;
    }
    PreferentialAttachmentExecutionPath::Parallel { threads, chunks }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreferentialAttachmentExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

fn run_preferential_attachment_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("preferential-attachment worker panicked")),
    }
}

#[cfg(test)]
mod tests;
