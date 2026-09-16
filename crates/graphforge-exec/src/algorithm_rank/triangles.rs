//! Triangles rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AssertUnwindSafe, AtomicUsize, BUILTIN_REVIEW, IntoParallelRefIterator,
    Ordering, ParallelIterator, RankAlgorithm, RustAlgorithm, TRIANGLES_PARALLEL_CROSSOVER_NODES,
    catch_unwind, destination_chunks, exact_u64_as_f64, execution, has_arc, rank_scores_output,
    simple_undirected_neighbors,
};

pub(super) struct Triangles;

const TRIANGLES_CHECKPOINT_PAIRS: usize = 1_024;

impl RustAlgorithm for Triangles {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Triangles),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Triangles);
        rank_scores_output(algorithm, graph, triangle_scores(graph, control)?, control)
    }
}

fn triangle_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_undirected_neighbors(graph, control)?;
    match select_triangles_path(control, neighbors.len()) {
        TrianglesExecutionPath::Serial => triangle_scores_serial(&neighbors, control),
        TrianglesExecutionPath::Parallel { .. } => triangle_scores_parallel(&neighbors, control),
    }
}

/// Selected triangles execution path for private-pool crossover tests (#515).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TrianglesExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Choose serial vs private-pool parallel execution for a triangles workload.
pub(crate) fn select_triangles_path(
    control: &AlgorithmControl,
    nodes: usize,
) -> TrianglesExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || nodes < TRIANGLES_PARALLEL_CROSSOVER_NODES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return TrianglesExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    TrianglesExecutionPath::Parallel { threads, chunks }
}

fn triangle_scores_serial(
    neighbors: &[Vec<usize>],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut scores = Vec::with_capacity(neighbors.len());
    let mut visited_pairs = 0_usize;
    for node in 0..neighbors.len() {
        control.checkpoint()?;
        let mut count = 0_u64;
        for (offset, &first) in neighbors[node].iter().enumerate() {
            for &second in &neighbors[node][offset + 1..] {
                if visited_pairs.is_multiple_of(TRIANGLES_CHECKPOINT_PAIRS) {
                    control.checkpoint()?;
                }
                visited_pairs += 1;
                if has_arc(neighbors, first, second) {
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| execution("triangle count exceeds supported range"))?;
                }
            }
        }
        scores.push(exact_u64_as_f64(count, "triangle count")?);
    }
    Ok(scores)
}

fn triangle_scores_parallel(
    neighbors: &[Vec<usize>],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel triangles requires an instance-owned compute pool"))?;
    let ranges = destination_chunks(neighbors.len(), control.compute_threads());
    let work = AtomicUsize::new(0);
    let chunk_results = run_triangles_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                for node in start..end {
                    control.check_cancelled()?;
                    local.push(triangle_score_node_parallel(
                        neighbors, node, control, &work,
                    )?);
                }
                Ok((start, local))
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;

    // Merge worker-local scores in ascending node-ordinal range order (canonical).
    let mut scores = vec![0.0; neighbors.len()];
    for (start, local) in chunk_results {
        scores[start..start + local.len()].copy_from_slice(&local);
    }
    Ok(scores)
}

fn triangle_score_node_parallel(
    neighbors: &[Vec<usize>],
    node: usize,
    control: &AlgorithmControl,
    work: &AtomicUsize,
) -> Result<f64, AlgorithmError> {
    let mut count = 0_u64;
    for (offset, &first) in neighbors[node].iter().enumerate() {
        for &second in &neighbors[node][offset + 1..] {
            let observed = work.fetch_add(1, Ordering::Relaxed) + 1;
            if observed.is_multiple_of(TRIANGLES_CHECKPOINT_PAIRS) {
                control.check_cancelled()?;
            }
            if has_arc(neighbors, first, second) {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| execution("triangle count exceeds supported range"))?;
            }
        }
    }
    exact_u64_as_f64(count, "triangle count")
}

fn run_triangles_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("triangles worker panicked")),
    }
}

#[cfg(test)]
mod tests;
