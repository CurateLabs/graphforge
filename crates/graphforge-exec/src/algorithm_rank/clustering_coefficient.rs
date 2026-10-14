//! Clustering coefficient rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AssertUnwindSafe, BUILTIN_REVIEW,
    CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK, IntoParallelRefIterator, ParallelIterator,
    RankAlgorithm, RustAlgorithm, catch_unwind, destination_chunks, exact_u64_as_f64, execution,
    rank_scores_output,
};

use crate::algorithm_graph::OrdinalRows;
use graphforge_core::ClusteringNormalization;

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
    outgoing: OrdinalRows,
    incoming: OrdinalRows,
    work_units: u64,
}

impl PreparedClusteringCoefficient {
    fn len(&self) -> usize {
        self.outgoing.len()
    }
}

fn csr_overflow() -> AlgorithmError {
    execution("clustering coefficient adjacency exceeds supported range")
}

fn prepare_clustering_coefficient(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<PreparedClusteringCoefficient, AlgorithmError> {
    let mut traversed_edges = 0_usize;
    let outgoing = OrdinalRows::distinct_out(graph, || {
        if traversed_edges.is_multiple_of(1024) {
            control.check_cancelled()?;
        }
        traversed_edges += 1;
        Ok(())
    })?;
    let incoming = outgoing.transpose()?;
    let work_units = estimate_clustering_coefficient_work(&outgoing, &incoming)?;
    Ok(PreparedClusteringCoefficient {
        outgoing,
        incoming,
        work_units,
    })
}

fn estimate_clustering_coefficient_work(
    outgoing: &OrdinalRows,
    incoming: &OrdinalRows,
) -> Result<u64, AlgorithmError> {
    (0..outgoing.len()).try_fold(0_u64, |total, node| {
        let degree = outgoing
            .row(node)
            .len()
            .checked_add(incoming.row(node).len())
            .ok_or_else(|| execution("clustering coefficient degree exceeds supported range"))?;
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
    control.check_cancelled()?;
    let outgoing = &prepared.outgoing;
    let incoming = &prepared.incoming;
    let degree_capacity = outgoing
        .row(node)
        .len()
        .checked_add(incoming.row(node).len())
        .ok_or_else(|| execution("clustering coefficient degree exceeds supported range"))?;
    let mut neighbors = Vec::with_capacity(degree_capacity);
    neighbors.extend_from_slice(outgoing.row(node));
    neighbors.extend_from_slice(incoming.row(node));
    neighbors.sort_unstable();
    neighbors.dedup();
    let node = u32::try_from(node).map_err(|_| csr_overflow())?;

    if control.clustering_normalization() == ClusteringNormalization::NeighborEdges {
        let degree = u64::try_from(neighbors.len())
            .map_err(|_| execution("clustering coefficient degree exceeds supported range"))?;
        let denominator = degree
            .checked_mul(degree.saturating_sub(1))
            .ok_or_else(|| {
                execution("clustering coefficient denominator exceeds supported range")
            })?;
        let mut edges = 0_u64;
        for &first in &neighbors {
            for_each_intersection(
                &neighbors,
                outgoing.row(first as usize),
                control,
                work,
                |_| {
                    edges = edges.checked_add(1).ok_or_else(|| {
                        execution("clustering coefficient edge count exceeds supported range")
                    })?;
                    Ok(())
                },
            )?;
        }
        return if denominator == 0 {
            Ok(0.0)
        } else {
            Ok(
                exact_u64_as_f64(edges, "clustering coefficient edge count")?
                    / exact_u64_as_f64(denominator, "clustering coefficient denominator")?,
            )
        };
    }

    let total_degree = outgoing
        .row(node as usize)
        .len()
        .checked_add(incoming.row(node as usize).len())
        .ok_or_else(|| execution("clustering coefficient degree exceeds supported range"))?;
    let total_degree = u64::try_from(total_degree)
        .map_err(|_| execution("clustering coefficient degree exceeds supported range"))?;
    let reciprocal_degree = u64::try_from(
        outgoing
            .row(node as usize)
            .iter()
            .filter(|&&neighbor| outgoing.has_arc(neighbor, node))
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
        let first_strength = arc_strength(outgoing, node, first);
        // Each direction contributes once; a reciprocal arc contributes twice,
        // exactly as the middle arc_strength in the ordered-pair formula.
        for rows in [outgoing, incoming] {
            for_each_intersection(
                &neighbors,
                rows.row(first as usize),
                control,
                work,
                |second| {
                    let contribution = first_strength * arc_strength(outgoing, second, node);
                    triangles = triangles.checked_add(contribution).ok_or_else(|| {
                        execution("clustering coefficient triangle count exceeds supported range")
                    })?;
                    Ok(())
                },
            )?;
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

/// Visit common ordinals in sorted, distinct rows. Searching only the smaller
/// row bounds a hub/leaf intersection by the leaf's degree, rather than scanning
/// the hub for every leaf. Advance the larger suffix after each search so near-
/// equal rows also make progress without revisiting earlier ordinals.
fn for_each_intersection(
    left: &[u32],
    right: &[u32],
    control: &AlgorithmControl,
    work: &mut usize,
    mut visit: impl FnMut(u32) -> Result<(), AlgorithmError>,
) -> Result<(), AlgorithmError> {
    let (small, mut large) = if left.len() <= right.len() {
        (left, right)
    } else {
        (right, left)
    };
    // Merge comparable rows in linear time; binary searches are useful only
    // when one row is much larger, as on a hub joined to a low-degree node.
    if large.len() <= small.len().saturating_mul(2) {
        let (mut a, mut b) = (0, 0);
        while a < small.len() && b < large.len() {
            clustering_coefficient_checkpoint(control, work)?;
            match small[a].cmp(&large[b]) {
                std::cmp::Ordering::Less => a += 1,
                std::cmp::Ordering::Greater => b += 1,
                std::cmp::Ordering::Equal => {
                    visit(small[a])?;
                    a += 1;
                    b += 1;
                }
            }
        }
        return Ok(());
    }
    for &candidate in small {
        clustering_coefficient_checkpoint(control, work)?;
        match large.binary_search(&candidate) {
            Ok(index) => {
                visit(candidate)?;
                large = &large[index + 1..];
            }
            Err(index) => large = &large[index..],
        }
        if large.is_empty() {
            break;
        }
    }
    Ok(())
}

/// Poll cancellation every [`CLUSTERING_COEFFICIENT_CHECKPOINT_WORK`] units of
/// intersection probes. Clustering coefficient is a single pass, so this never consumes the
/// iteration budget that bounds iterative algorithms (#1922).
fn clustering_coefficient_checkpoint(
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*work).is_multiple_of(CLUSTERING_COEFFICIENT_CHECKPOINT_WORK) {
        control.check_cancelled()?;
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

fn arc_strength(outgoing: &OrdinalRows, source: u32, target: u32) -> u64 {
    u64::from(outgoing.has_arc(source, target)) + u64::from(outgoing.has_arc(target, source))
}

#[cfg(test)]
mod tests;
