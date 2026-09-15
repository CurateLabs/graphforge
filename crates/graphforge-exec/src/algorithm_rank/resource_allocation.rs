//! Resource allocation rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AssertUnwindSafe, BUILTIN_REVIEW, IntoParallelRefIterator, ParallelIterator,
    RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK, RankAlgorithm, RustAlgorithm, catch_unwind,
    execution, first_chunk_error, rank_scores_output, simple_neighbors, source_chunks,
    usize_to_u64_saturating,
};

pub(super) struct ResourceAllocation;

const RESOURCE_ALLOCATION_CHECKPOINT_INTERVAL: usize = 1_024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResourceAllocationExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

impl RustAlgorithm for ResourceAllocation {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::ResourceAllocation),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::ResourceAllocation);
        rank_scores_output(
            algorithm,
            graph,
            resource_allocation_scores(graph, control)?,
            control,
        )
    }
}

fn resource_allocation_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let discount_degrees = resource_allocation_discount_degrees(&neighbors)?;
    let estimated_work = estimated_pairwise_source_work(&neighbors);
    match select_resource_allocation_path(control, neighbors.len(), estimated_work) {
        ResourceAllocationExecutionPath::Serial => {
            resource_allocation_scores_serial(&neighbors, &discount_degrees, control)
        }
        ResourceAllocationExecutionPath::Parallel { .. } => {
            resource_allocation_scores_parallel(&neighbors, &discount_degrees, control)
        }
    }
}

fn resource_allocation_discount_degrees(
    neighbors: &[Vec<usize>],
) -> Result<Vec<u64>, AlgorithmError> {
    let mut discount_degrees = vec![0_u64; neighbors.len()];
    for adjacent in neighbors {
        for &neighbor in adjacent {
            discount_degrees[neighbor] = discount_degrees[neighbor]
                .checked_add(1)
                .ok_or_else(|| execution("resource-allocation degree exceeds supported range"))?;
        }
    }
    Ok(discount_degrees)
}

fn resource_allocation_scores_serial(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for source in 0..neighbors.len() {
        let score = resource_allocation_source_score(neighbors, discount_degrees, source, || {
            resource_allocation_serial_checkpoint(control, &mut visited)
        })?;
        scores.push(score);
    }
    Ok(scores)
}

fn resource_allocation_scores_parallel(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel resource-allocation requires an instance-owned compute pool")
    })?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let chunk_results = run_resource_allocation_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut work = 0_usize;
                let mut local = Vec::with_capacity(end - start);
                for source in start..end {
                    let score = resource_allocation_source_score(
                        neighbors,
                        discount_degrees,
                        source,
                        || resource_allocation_serial_checkpoint(control, &mut work),
                    )?;
                    local.push(score);
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

fn resource_allocation_source_score(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    source: usize,
    mut checkpoint: impl FnMut() -> Result<(), AlgorithmError>,
) -> Result<f64, AlgorithmError> {
    let source_neighbors = &neighbors[source];
    let mut score = 0.0_f64;
    let mut compensation = 0.0_f64;
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
                    let term =
                        resource_allocation_discount(discount_degrees[source_neighbors[left]])?;
                    let adjusted = term - compensation;
                    let updated = score + adjusted;
                    compensation = (updated - score) - adjusted;
                    score = updated;
                    left += 1;
                    right += 1;
                }
            }
        }
    }
    if !score.is_finite() {
        return Err(execution("resource-allocation score is not finite"));
    }
    Ok(score)
}

fn resource_allocation_serial_checkpoint(
    control: &AlgorithmControl,
    visited: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*visited).is_multiple_of(RESOURCE_ALLOCATION_CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    *visited = visited.saturating_add(1);
    Ok(())
}

fn estimated_pairwise_source_work(neighbors: &[Vec<usize>]) -> u64 {
    let sources = usize_to_u64_saturating(neighbors.len());
    let degree_sum = neighbors.iter().fold(0_u64, |total, adjacent| {
        total.saturating_add(usize_to_u64_saturating(adjacent.len()))
    });
    sources
        .saturating_mul(sources)
        .saturating_add(sources.saturating_mul(degree_sum).saturating_mul(2))
}

/// Choose serial vs private-pool parallel execution for resource allocation.
pub(crate) fn select_resource_allocation_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_work: u64,
) -> ResourceAllocationExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || sources <= 1
        || estimated_work < RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return ResourceAllocationExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return ResourceAllocationExecutionPath::Serial;
    }
    ResourceAllocationExecutionPath::Parallel { threads, chunks }
}

fn run_resource_allocation_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("resource-allocation worker panicked")),
    }
}

fn resource_allocation_discount(degree: u64) -> Result<f64, AlgorithmError> {
    if degree < 2 {
        return Err(execution(
            "resource-allocation common-neighbor degree must be at least two",
        ));
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "the reciprocal discount does not require an exact integer conversion"
    )]
    let term = 1.0 / degree as f64;
    if !term.is_finite() {
        return Err(execution("resource-allocation discount is not finite"));
    }
    Ok(term)
}

#[cfg(test)]
mod tests;
