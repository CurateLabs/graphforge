//! Degree rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmValue, AtomicUsize, BUILTIN_REVIEW, DEGREE_PARALLEL_CROSSOVER_NODES,
    IntoParallelRefIterator, Ordering, ParallelIterator, RankAlgorithm, RustAlgorithm,
    destination_chunks, exact_u32, execution, run_rank_on_pool,
};

pub(super) struct Degree;

const DEGREE_CHECKPOINT_NODES: usize = 1_024;

impl RustAlgorithm for Degree {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Degree),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let node_ids = graph.node_ids();
        let denominator = exact_u32(node_ids.len().saturating_sub(1).max(1), "node count")?;
        let algorithm = Algorithm::Rank(RankAlgorithm::Degree);
        let mut sink = control.output_sink(algorithm)?;
        let path = select_degree_path(control, node_ids.len());
        match path {
            DegreeExecutionPath::Serial => {
                for (index, &node_id) in node_ids.iter().enumerate() {
                    if index.is_multiple_of(DEGREE_CHECKPOINT_NODES) {
                        control.checkpoint()?;
                    }
                    let uuid = graph
                        .node_uuid(node_id)
                        .ok_or_else(|| execution("selected node has no UUID identity"))?;
                    let degree = exact_u32(graph.neighbors(node_id).len(), "node degree")?;
                    sink.append_row(&[
                        AlgorithmValue::Uuid(uuid),
                        AlgorithmValue::Float64(f64::from(degree) / f64::from(denominator)),
                    ])?;
                }
            }
            DegreeExecutionPath::Parallel { .. } => {
                let rows = degree_scores_parallel(graph, denominator, control)?;
                for (uuid, score) in rows {
                    sink.append_row(&[AlgorithmValue::Uuid(uuid), AlgorithmValue::Float64(score)])?;
                }
            }
        }
        sink.finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DegreeExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Choose serial vs private-pool parallel execution for a Degree workload (#506).
pub(crate) fn select_degree_path(control: &AlgorithmControl, nodes: usize) -> DegreeExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes < DEGREE_PARALLEL_CROSSOVER_NODES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return DegreeExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    if chunks <= 1 {
        return DegreeExecutionPath::Serial;
    }
    DegreeExecutionPath::Parallel { threads, chunks }
}

fn degree_scores_parallel(
    graph: &AdjacencyGraph,
    denominator: u32,
    control: &AlgorithmControl,
) -> Result<Vec<([u8; 16], f64)>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel Degree requires an instance-owned compute pool"))?;
    let node_ids = graph.node_ids();
    let ranges = destination_chunks(node_ids.len(), control.compute_threads());
    let work = AtomicUsize::new(0);
    let chunk_results = run_rank_on_pool(pool, "Degree", || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                for &node_id in &node_ids[start..end] {
                    let observed = work.fetch_add(1, Ordering::Relaxed) + 1;
                    if observed.is_multiple_of(DEGREE_CHECKPOINT_NODES) {
                        control.check_cancelled()?;
                    }
                    let uuid = graph
                        .node_uuid(node_id)
                        .ok_or_else(|| execution("selected node has no UUID identity"))?;
                    let degree = exact_u32(graph.neighbors(node_id).len(), "node degree")?;
                    local.push((uuid, f64::from(degree) / f64::from(denominator)));
                }
                Ok((start, local))
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;
    // Merge chunk outputs in ascending node-ordinal order (canonical).
    let mut rows = Vec::with_capacity(node_ids.len());
    for (start, local) in chunk_results {
        debug_assert_eq!(start, rows.len());
        rows.extend(local);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests;
