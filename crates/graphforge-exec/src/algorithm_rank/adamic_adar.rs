//! Adamic adar rank execution and deterministic worker paths.

use super::{
    ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK, AdjacencyGraph, Algorithm, AlgorithmCapability,
    AlgorithmControl, AlgorithmError, AlgorithmOutput, AssertUnwindSafe, BUILTIN_REVIEW,
    IntoParallelRefIterator, ParallelIterator, RankAlgorithm, RustAlgorithm, catch_unwind,
    execution, first_chunk_error, rank_scores_output, simple_neighbors, source_chunks,
    usize_to_u64_saturating,
};

pub(super) struct AdamicAdar;

const ADAMIC_ADAR_CHECKPOINT_INTERVAL: usize = 1_024;

/// Selected Adamic-Adar execution path for observability and crossover tests (#499).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdamicAdarExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

impl RustAlgorithm for AdamicAdar {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::AdamicAdar),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::AdamicAdar);
        rank_scores_output(
            algorithm,
            graph,
            adamic_adar_scores(graph, control)?,
            control,
        )
    }
}

fn adamic_adar_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let discount_degrees = adamic_adar_discount_degrees(&neighbors)?;
    let estimated_work = estimated_adamic_adar_work(&neighbors);
    match select_adamic_adar_path(control, neighbors.len(), estimated_work) {
        AdamicAdarExecutionPath::Serial => {
            adamic_adar_scores_serial(&neighbors, &discount_degrees, control)
        }
        AdamicAdarExecutionPath::Parallel { .. } => {
            adamic_adar_scores_parallel(&neighbors, &discount_degrees, control)
        }
    }
}
fn adamic_adar_discount_degrees(neighbors: &[Vec<usize>]) -> Result<Vec<u64>, AlgorithmError> {
    let mut discount_degrees = vec![0_u64; neighbors.len()];
    for adjacent in neighbors {
        for &neighbor in adjacent {
            discount_degrees[neighbor] = discount_degrees[neighbor]
                .checked_add(1)
                .ok_or_else(|| execution("Adamic-Adar neighbor degree exceeds supported range"))?;
        }
    }
    Ok(discount_degrees)
}
fn adamic_adar_scores_serial(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for source in 0..neighbors.len() {
        let score = adamic_adar_source_score(neighbors, discount_degrees, source, || {
            adamic_adar_serial_checkpoint(control, &mut visited)
        })?;
        scores.push(score);
    }
    Ok(scores)
}
fn adamic_adar_scores_parallel(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel Adamic-Adar requires an instance-owned compute pool"))?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let chunk_results = run_adamic_adar_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut work = 0_usize;
                let mut local = Vec::with_capacity(end - start);
                for source in start..end {
                    let score =
                        adamic_adar_source_score(neighbors, discount_degrees, source, || {
                            adamic_adar_serial_checkpoint(control, &mut work)
                        })?;
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
fn adamic_adar_source_score(
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
                    let common = source_neighbors[left];
                    let term = adamic_discount(discount_degrees[common])?;
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
        return Err(execution("Adamic-Adar score is not finite"));
    }
    Ok(score)
}
fn adamic_adar_serial_checkpoint(
    control: &AlgorithmControl,
    visited: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*visited).is_multiple_of(ADAMIC_ADAR_CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    *visited = visited.saturating_add(1);
    Ok(())
}
fn estimated_adamic_adar_work(neighbors: &[Vec<usize>]) -> u64 {
    let sources = usize_to_u64_saturating(neighbors.len());
    let degree_sum = neighbors.iter().fold(0_u64, |total, adjacent| {
        total.saturating_add(usize_to_u64_saturating(adjacent.len()))
    });
    sources
        .saturating_mul(sources)
        .saturating_add(sources.saturating_mul(degree_sum).saturating_mul(2))
}
pub(crate) fn select_adamic_adar_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_work: u64,
) -> AdamicAdarExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || sources <= 1
        || estimated_work < ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return AdamicAdarExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return AdamicAdarExecutionPath::Serial;
    }
    AdamicAdarExecutionPath::Parallel { threads, chunks }
}
fn run_adamic_adar_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("Adamic-Adar worker panicked")),
    }
}

fn adamic_discount(degree: u64) -> Result<f64, AlgorithmError> {
    if degree < 2 {
        return Err(execution(
            "Adamic-Adar common-neighbor degree must be at least two",
        ));
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "the logarithmic discount does not require an exact integer conversion"
    )]
    let term = 1.0 / (degree as f64).ln();
    if !term.is_finite() {
        return Err(execution("Adamic-Adar discount is not finite"));
    }
    Ok(term)
}

#[cfg(test)]
mod tests;
