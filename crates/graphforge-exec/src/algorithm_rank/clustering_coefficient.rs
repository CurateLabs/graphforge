//! Clustering coefficient rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AssertUnwindSafe, BUILTIN_REVIEW,
    CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK, HashMap, IntoParallelRefIterator,
    ParallelIterator, RankAlgorithm, RustAlgorithm, catch_unwind, destination_chunks,
    exact_u64_as_f64, execution, has_arc, rank_scores_output,
};

pub(super) struct ClusteringCoefficient;

const CLUSTERING_COEFFICIENT_CHECKPOINT_WORK: usize = 1_024;

impl RustAlgorithm for ClusteringCoefficient {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::ClusteringCoefficient),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::ClusteringCoefficient);
        rank_scores_output(
            algorithm,
            graph,
            clustering_coefficient_scores(graph, control)?,
            control,
        )
    }
}

fn clustering_coefficient_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let prepared = prepare_clustering_coefficient(graph, control)?;
    match select_clustering_coefficient_path(control, prepared.work_units, prepared.len()) {
        ClusteringCoefficientExecutionPath::Serial => {
            clustering_coefficient_scores_serial(&prepared, control)
        }
        ClusteringCoefficientExecutionPath::Parallel { .. } => {
            clustering_coefficient_scores_parallel(&prepared, control)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClusteringCoefficientExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

struct PreparedClusteringCoefficient {
    outgoing: Vec<Vec<usize>>,
    incoming: Vec<Vec<usize>>,
    work_units: u64,
}

impl PreparedClusteringCoefficient {
    fn len(&self) -> usize {
        self.outgoing.len()
    }
}

fn prepare_clustering_coefficient(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<PreparedClusteringCoefficient, AlgorithmError> {
    let node_ids = graph.node_ids();
    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut outgoing = vec![Vec::new(); node_ids.len()];
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
                outgoing[source].push(target);
            }
        }
        outgoing[source].sort_unstable();
        outgoing[source].dedup();
    }

    let mut incoming = vec![Vec::new(); node_ids.len()];
    for (source, targets) in outgoing.iter().enumerate() {
        for &target in targets {
            incoming[target].push(source);
        }
    }
    for sources in &mut incoming {
        sources.sort_unstable();
    }

    let work_units = estimate_clustering_coefficient_work(&outgoing, &incoming)?;
    Ok(PreparedClusteringCoefficient {
        outgoing,
        incoming,
        work_units,
    })
}

fn estimate_clustering_coefficient_work(
    outgoing: &[Vec<usize>],
    incoming: &[Vec<usize>],
) -> Result<u64, AlgorithmError> {
    outgoing
        .iter()
        .zip(incoming)
        .try_fold(0_u64, |total, (outgoing, incoming)| {
            let degree = outgoing.len().checked_add(incoming.len()).ok_or_else(|| {
                execution("clustering coefficient degree exceeds supported range")
            })?;
            let degree = u64::try_from(degree)
                .map_err(|_| execution("clustering coefficient degree exceeds supported range"))?;
            Ok(total.saturating_add(degree.saturating_mul(degree)))
        })
}

/// Choose serial vs private-pool parallel execution for clustering coefficient.
pub(crate) fn select_clustering_coefficient_path(
    control: &AlgorithmControl,
    work_units: u64,
    nodes: usize,
) -> ClusteringCoefficientExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || work_units < CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return ClusteringCoefficientExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    if chunks <= 1 {
        return ClusteringCoefficientExecutionPath::Serial;
    }
    ClusteringCoefficientExecutionPath::Parallel { threads, chunks }
}

fn clustering_coefficient_scores_serial(
    prepared: &PreparedClusteringCoefficient,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut scores = Vec::with_capacity(prepared.len());
    let mut work = 0_usize;
    for node in 0..prepared.len() {
        scores.push(clustering_coefficient_score_node(
            prepared, node, control, &mut work,
        )?);
    }
    Ok(scores)
}

fn clustering_coefficient_scores_parallel(
    prepared: &PreparedClusteringCoefficient,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel clustering coefficient requires an instance-owned compute pool")
    })?;
    let ranges = destination_chunks(prepared.len(), control.compute_threads());
    let chunk_results = run_clustering_coefficient_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                let mut work = 0_usize;
                for node in start..end {
                    local.push(clustering_coefficient_score_node(
                        prepared, node, control, &mut work,
                    )?);
                }
                Ok((start, local))
            })
            .collect::<Vec<Result<_, AlgorithmError>>>()
    })?;

    let mut scores = vec![0.0; prepared.len()];
    for result in chunk_results {
        let (start, local) = result?;
        scores[start..start + local.len()].copy_from_slice(&local);
    }
    Ok(scores)
}

fn clustering_coefficient_score_node(
    prepared: &PreparedClusteringCoefficient,
    node: usize,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<f64, AlgorithmError> {
    control.checkpoint()?;
    let outgoing = &prepared.outgoing;
    let incoming = &prepared.incoming;
    let mut neighbors = outgoing[node].clone();
    neighbors.extend_from_slice(&incoming[node]);
    neighbors.sort_unstable();
    neighbors.dedup();

    let total_degree = outgoing[node]
        .len()
        .checked_add(incoming[node].len())
        .ok_or_else(|| execution("clustering coefficient degree exceeds supported range"))?;
    let total_degree = u64::try_from(total_degree)
        .map_err(|_| execution("clustering coefficient degree exceeds supported range"))?;
    let reciprocal_degree = u64::try_from(
        outgoing[node]
            .iter()
            .filter(|&&neighbor| has_arc(outgoing, neighbor, node))
            .count(),
    )
    .map_err(|_| execution("reciprocal degree exceeds supported range"))?;
    let denominator = total_degree
        .checked_mul(total_degree.saturating_sub(1))
        .and_then(|value| value.checked_sub(reciprocal_degree.checked_mul(2)?))
        .and_then(|value| value.checked_mul(2))
        .ok_or_else(|| execution("clustering coefficient denominator exceeds supported range"))?;

    let mut triangles = 0_u64;
    for &first in &neighbors {
        for &second in &neighbors {
            clustering_coefficient_checkpoint(control, work)?;
            let contribution = arc_strength(outgoing, node, first)
                * arc_strength(outgoing, first, second)
                * arc_strength(outgoing, second, node);
            triangles = triangles.checked_add(contribution).ok_or_else(|| {
                execution("clustering coefficient triangle count exceeds supported range")
            })?;
        }
    }
    let score = if denominator == 0 {
        0.0
    } else {
        exact_u64_as_f64(triangles, "clustering coefficient triangle count")?
            / exact_u64_as_f64(denominator, "clustering coefficient denominator")?
    };
    if !score.is_finite() {
        return Err(execution("clustering coefficient score is not finite"));
    }
    Ok(score)
}

fn clustering_coefficient_checkpoint(
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*work).is_multiple_of(CLUSTERING_COEFFICIENT_CHECKPOINT_WORK) {
        control.checkpoint()?;
    }
    *work = work.saturating_add(1);
    Ok(())
}

fn run_clustering_coefficient_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> R + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => Ok(result),
        Err(_) => Err(execution("clustering coefficient worker panicked")),
    }
}

fn arc_strength(outgoing: &[Vec<usize>], source: usize, target: usize) -> u64 {
    u64::from(has_arc(outgoing, source, target)) + u64::from(has_arc(outgoing, target, source))
}

#[cfg(test)]
mod tests;
