//! Hits rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AssertUnwindSafe, BUILTIN_REVIEW, HITS_PARALLEL_CROSSOVER_EDGES, HashMap,
    IntoParallelRefIterator, ParallelIterator, RankAlgorithm, RustAlgorithm, catch_unwind,
    destination_chunks, exact_u32, execution, rank_scores_output,
};

pub(super) struct HitsHub;

pub(super) struct HitsAuthority;

const HITS_ITERATIONS: usize = 20;

const HITS_CHECKPOINT_EDGES: usize = 4_096;

impl RustAlgorithm for HitsHub {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::HitsHub),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::HitsHub);
        let (_, hubs) = hits_scores(graph, control)?;
        rank_scores_output(algorithm, graph, hubs, control)
    }
}

impl RustAlgorithm for HitsAuthority {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::HitsAuthority),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::HitsAuthority);
        let (authorities, _) = hits_scores(graph, control)?;
        rank_scores_output(algorithm, graph, authorities, control)
    }
}

#[derive(Clone, Debug, Default)]
struct HitsCsr {
    offsets: Vec<u32>,
    neighbors: Vec<u32>,
}

#[derive(Clone, Debug, Default)]
struct PreparedHits {
    outgoing: HitsCsr,
    incoming: HitsCsr,
    edge_count: u64,
}

/// Selected HITS execution path for observability and crossover tests (#510).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HitsExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

fn hits_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<(Vec<f64>, Vec<f64>), AlgorithmError> {
    let node_ids = graph.node_ids();
    if node_ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let prepared = prepare_hits(graph, control)?;
    let path = select_hits_path(control, prepared.edge_count, node_ids.len());
    let mut authorities = vec![1.0; node_ids.len()];
    let mut hubs = vec![1.0; node_ids.len()];
    for _ in 0..HITS_ITERATIONS {
        control.checkpoint()?;
        match path {
            HitsExecutionPath::Serial => {
                hits_pull_serial(&prepared.incoming, &hubs, &mut authorities, control)?;
            }
            HitsExecutionPath::Parallel { .. } => {
                hits_pull_parallel(&prepared.incoming, &hubs, &mut authorities, control)?;
            }
        }
        normalize_hits(&mut authorities, "authority")?;

        control.checkpoint()?;
        let mut next_hubs = vec![0.0; node_ids.len()];
        match path {
            HitsExecutionPath::Serial => {
                hits_pull_serial(&prepared.outgoing, &authorities, &mut next_hubs, control)?;
            }
            HitsExecutionPath::Parallel { .. } => {
                hits_pull_parallel(&prepared.outgoing, &authorities, &mut next_hubs, control)?;
            }
        }
        normalize_hits(&mut next_hubs, "hub")?;
        hubs = next_hubs;
    }
    Ok((authorities, hubs))
}

fn prepare_hits(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<PreparedHits, AlgorithmError> {
    let node_ids = graph.node_ids();
    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut outgoing_offsets = Vec::with_capacity(node_ids.len() + 1);
    let mut outgoing_neighbors = Vec::with_capacity(
        usize::try_from(graph.edge_entry_count())
            .map_err(|_| execution("HITS edge count exceeds supported range"))?,
    );
    let mut incoming_counts = vec![0_u32; node_ids.len()];
    let mut edge_count = 0_u64;
    outgoing_offsets.push(0_u32);
    for &node in node_ids {
        for edge in graph.neighbors(node) {
            if edge_count > 0 && edge_count.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            incoming_counts[target] = incoming_counts[target]
                .checked_add(1)
                .ok_or_else(|| execution("HITS inbound degree exceeds supported range"))?;
            outgoing_neighbors.push(exact_u32(target, "HITS target ordinal")?);
            edge_count = edge_count
                .checked_add(1)
                .ok_or_else(|| execution("HITS edge count exceeds supported range"))?;
        }
        outgoing_offsets.push(exact_u32(
            outgoing_neighbors.len(),
            "HITS outgoing CSR length",
        )?);
    }

    let mut incoming_offsets = Vec::with_capacity(node_ids.len() + 1);
    incoming_offsets.push(0_u32);
    for &count in &incoming_counts {
        let next = incoming_offsets
            .last()
            .copied()
            .unwrap_or(0)
            .checked_add(count)
            .ok_or_else(|| execution("HITS incoming CSR offsets exceed supported range"))?;
        incoming_offsets.push(next);
    }
    let mut incoming_neighbors = vec![0_u32; outgoing_neighbors.len()];
    let mut write_at = incoming_offsets[..node_ids.len()].to_vec();
    for source in 0..node_ids.len() {
        let source_u32 = exact_u32(source, "HITS source ordinal")?;
        let start = usize::try_from(outgoing_offsets[source])
            .map_err(|_| execution("HITS outgoing offset exceeds supported range"))?;
        let end = usize::try_from(outgoing_offsets[source + 1])
            .map_err(|_| execution("HITS outgoing offset exceeds supported range"))?;
        for &target in &outgoing_neighbors[start..end] {
            let target = usize::try_from(target)
                .map_err(|_| execution("HITS target ordinal exceeds supported range"))?;
            let slot = usize::try_from(write_at[target])
                .map_err(|_| execution("HITS incoming write cursor exceeds supported range"))?;
            incoming_neighbors[slot] = source_u32;
            write_at[target] = write_at[target]
                .checked_add(1)
                .ok_or_else(|| execution("HITS incoming write cursor overflow"))?;
        }
    }

    Ok(PreparedHits {
        outgoing: HitsCsr {
            offsets: outgoing_offsets,
            neighbors: outgoing_neighbors,
        },
        incoming: HitsCsr {
            offsets: incoming_offsets,
            neighbors: incoming_neighbors,
        },
        edge_count,
    })
}

/// Choose serial vs private-pool parallel execution for a HITS workload.
pub(crate) fn select_hits_path(
    control: &AlgorithmControl,
    edge_count: u64,
    nodes: usize,
) -> HitsExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || edge_count < HITS_PARALLEL_CROSSOVER_EDGES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return HitsExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    if chunks <= 1 {
        return HitsExecutionPath::Serial;
    }
    HitsExecutionPath::Parallel { threads, chunks }
}

fn hits_pull_serial(
    csr: &HitsCsr,
    input: &[f64],
    output: &mut [f64],
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    let mut traversed_edges = 0_usize;
    for (node, score) in output.iter_mut().enumerate() {
        *score = 0.0;
        let start = usize::try_from(csr.offsets[node])
            .map_err(|_| execution("HITS CSR offset exceeds supported range"))?;
        let end = usize::try_from(csr.offsets[node + 1])
            .map_err(|_| execution("HITS CSR offset exceeds supported range"))?;
        for &neighbor in &csr.neighbors[start..end] {
            if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            traversed_edges += 1;
            let neighbor = usize::try_from(neighbor)
                .map_err(|_| execution("HITS neighbor ordinal exceeds supported range"))?;
            *score += input[neighbor];
        }
    }
    Ok(())
}

fn hits_pull_parallel(
    csr: &HitsCsr,
    input: &[f64],
    output: &mut [f64],
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel HITS requires an instance-owned compute pool"))?;
    let ranges = destination_chunks(output.len(), control.compute_threads());
    let chunk_results = run_hits_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                let mut traversed_edges = 0_usize;
                for node in start..end {
                    let edge_start = usize::try_from(csr.offsets[node])
                        .map_err(|_| execution("HITS CSR offset exceeds supported range"))?;
                    let edge_end = usize::try_from(csr.offsets[node + 1])
                        .map_err(|_| execution("HITS CSR offset exceeds supported range"))?;
                    let mut score = 0.0;
                    for &neighbor in &csr.neighbors[edge_start..edge_end] {
                        traversed_edges = traversed_edges.saturating_add(1);
                        if traversed_edges.is_multiple_of(HITS_CHECKPOINT_EDGES) {
                            control.check_cancelled()?;
                        }
                        let neighbor = usize::try_from(neighbor).map_err(|_| {
                            execution("HITS neighbor ordinal exceeds supported range")
                        })?;
                        score += input[neighbor];
                    }
                    local.push(score);
                }
                Ok((start, local))
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;
    // Merge chunk outputs in ascending dense-ordinal range order (canonical).
    for (start, local) in chunk_results {
        output[start..start + local.len()].copy_from_slice(&local);
    }
    Ok(())
}

fn run_hits_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("HITS worker panicked")),
    }
}

fn normalize_hits(scores: &mut [f64], kind: &str) -> Result<(), AlgorithmError> {
    let norm = scores.iter().map(|score| score * score).sum::<f64>().sqrt();
    if !norm.is_finite() {
        return Err(execution(format!("HITS {kind} norm is not finite")));
    }
    if norm == 0.0 {
        return Ok(());
    }
    for score in scores {
        *score /= norm;
        if !score.is_finite() {
            return Err(execution(format!("HITS {kind} score is not finite")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
