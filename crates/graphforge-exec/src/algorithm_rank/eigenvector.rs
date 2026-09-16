//! Eigenvector rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmValue, AssertUnwindSafe, BUILTIN_REVIEW,
    EIGENVECTOR_PARALLEL_CROSSOVER_EDGES, HashMap, IndexedParallelIterator, ParallelIterator,
    ParallelSliceMut, RankAlgorithm, RustAlgorithm, catch_unwind, destination_chunks, exact_u32,
    execution,
};

pub(super) struct Eigenvector;

const EIGENVECTOR_MAX_ITERATIONS: usize = 20;
const EIGENVECTOR_TOLERANCE: f64 = 1.0e-7;

const EIGENVECTOR_CHECKPOINT_DESTINATIONS: usize = 4_096;
const EIGENVECTOR_SERIAL_WARMUP_ITERATIONS: usize = 2;

impl RustAlgorithm for Eigenvector {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Eigenvector),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Eigenvector);
        let node_ids = graph.node_ids();
        if node_ids.is_empty() {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let indices: HashMap<u64, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let node_count = f64::from(exact_u32(node_ids.len(), "node count")?);
        let mut scores = vec![1.0 / node_count; node_ids.len()];
        let path = select_eigenvector_path(control, graph.edge_entry_count(), node_ids.len());
        let mut inbound = None;
        for iteration in 0..EIGENVECTOR_MAX_ITERATIONS {
            control.checkpoint()?;
            let use_parallel = matches!(path, EigenvectorExecutionPath::Parallel { .. })
                && iteration >= EIGENVECTOR_SERIAL_WARMUP_ITERATIONS;
            let mut next = if use_parallel {
                if inbound.is_none() {
                    inbound = Some(prepare_eigenvector_inbound(graph, &indices)?);
                }
                eigenvector_pull_parallel(
                    inbound
                        .as_ref()
                        .ok_or_else(|| execution("parallel eigenvector requires inbound CSR"))?,
                    &scores,
                    control,
                )?
            } else {
                eigenvector_scatter_serial(graph, &indices, node_ids, &scores, control)?
            };
            if next.iter().any(|score| !score.is_finite()) {
                return Err(execution("eigenvector score exceeds supported range"));
            }

            let norm = next.iter().map(|score| score * score).sum::<f64>().sqrt();
            if !norm.is_finite() || norm == 0.0 {
                return Err(execution("eigenvector L2 norm is not finite and positive"));
            }
            for score in &mut next {
                *score /= norm;
            }
            let converged = next
                .iter()
                .zip(&scores)
                .all(|(current, previous)| (current - previous).abs() <= EIGENVECTOR_TOLERANCE);
            scores = next;
            if iteration > 0 && converged {
                break;
            }
        }

        let rows = node_ids
            .iter()
            .zip(scores)
            .map(|(&node, score)| {
                let uuid = graph
                    .node_uuid(node)
                    .ok_or_else(|| execution("selected node has no UUID identity"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(uuid),
                    AlgorithmValue::Float64(score),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(algorithm, control, rows)
    }
}

/// Selected eigenvector execution path for observability and crossover tests (#507).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EigenvectorExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Dense inbound CSR: source ordinals in canonical source/edge order per destination.
#[derive(Clone, Debug, Default)]
struct EigenvectorInboundCsr {
    offsets: Vec<u32>,
    sources: Vec<u32>,
}

fn select_eigenvector_path(
    control: &AlgorithmControl,
    edge_count: u64,
    nodes: usize,
) -> EigenvectorExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || edge_count < EIGENVECTOR_PARALLEL_CROSSOVER_EDGES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return EigenvectorExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    EigenvectorExecutionPath::Parallel { threads, chunks }
}

fn prepare_eigenvector_inbound(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
) -> Result<EigenvectorInboundCsr, AlgorithmError> {
    let node_ids = graph.node_ids();
    let node_len = node_ids.len();
    let mut inbound_counts = vec![0_u32; node_len];
    for &source in node_ids {
        for edge in graph.neighbors(source) {
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            inbound_counts[target] = inbound_counts[target]
                .checked_add(1)
                .ok_or_else(|| execution("inbound degree exceeds supported range"))?;
        }
    }

    let mut offsets = Vec::with_capacity(node_len + 1);
    offsets.push(0_u32);
    for &count in &inbound_counts {
        let next = offsets
            .last()
            .copied()
            .unwrap_or(0)
            .checked_add(count)
            .ok_or_else(|| execution("inbound CSR offsets exceed supported range"))?;
        offsets.push(next);
    }
    let total = usize::try_from(*offsets.last().unwrap_or(&0))
        .map_err(|_| execution("inbound CSR length exceeds supported range"))?;
    let mut sources = vec![0_u32; total];
    let mut write_at = offsets[..node_len].to_vec();
    for (source_index, &source) in node_ids.iter().enumerate() {
        let source_u32 = exact_u32(source_index, "source ordinal")?;
        for edge in graph.neighbors(source) {
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            let slot = usize::try_from(write_at[target])
                .map_err(|_| execution("inbound write cursor exceeds supported range"))?;
            sources[slot] = source_u32;
            write_at[target] = write_at[target]
                .checked_add(1)
                .ok_or_else(|| execution("inbound write cursor overflow"))?;
        }
    }

    Ok(EigenvectorInboundCsr { offsets, sources })
}

fn eigenvector_scatter_serial(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    node_ids: &[u64],
    scores: &[f64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut next = scores.to_vec();
    let mut traversed_edges = 0_usize;
    for (source_index, &source) in node_ids.iter().enumerate() {
        for edge in graph.neighbors(source) {
            if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            traversed_edges += 1;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            next[target] += scores[source_index];
            if !next[target].is_finite() {
                return Err(execution("eigenvector score exceeds supported range"));
            }
        }
    }
    Ok(next)
}

fn eigenvector_pull_destination(
    inbound: &EigenvectorInboundCsr,
    scores: &[f64],
    dest: usize,
) -> f64 {
    let start = usize::try_from(inbound.offsets[dest]).unwrap_or(0);
    let end = usize::try_from(inbound.offsets[dest + 1]).unwrap_or(start);
    let mut acc = scores[dest];
    for &source in &inbound.sources[start.min(end)..end.min(inbound.sources.len())] {
        let source = usize::try_from(source).unwrap_or(usize::MAX);
        if source < scores.len() {
            acc += scores[source];
        }
    }
    acc
}

fn eigenvector_pull_parallel(
    inbound: &EigenvectorInboundCsr,
    scores: &[f64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    if scores.is_empty() {
        return Ok(Vec::new());
    }
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel eigenvector requires an instance-owned compute pool"))?;
    let chunks = destination_chunks(scores.len(), control.compute_threads())
        .len()
        .max(1);
    let chunk_len = scores.len().div_ceil(chunks);
    let mut next = vec![0.0; scores.len()];
    run_eigenvector_on_pool(pool, || {
        next.par_chunks_mut(chunk_len)
            .enumerate()
            .try_for_each(|(chunk_index, local)| {
                control.check_cancelled()?;
                let start = chunk_index * chunk_len;
                for (offset, slot) in local.iter_mut().enumerate() {
                    let dest = start + offset;
                    if dest.is_multiple_of(EIGENVECTOR_CHECKPOINT_DESTINATIONS) {
                        control.check_cancelled()?;
                    }
                    let score = eigenvector_pull_destination(inbound, scores, dest);
                    if !score.is_finite() {
                        return Err(execution("eigenvector score exceeds supported range"));
                    }
                    *slot = score;
                }
                Ok(())
            })
    })?;
    Ok(next)
}

fn run_eigenvector_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("Eigenvector worker panicked")),
    }
}

#[cfg(test)]
mod tests;
