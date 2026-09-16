//! Pagerank rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmValue, AtomicUsize, BUILTIN_REVIEW, HashMap, IntoParallelRefIterator,
    Ordering, PAGERANK_PARALLEL_CROSSOVER_EDGES, ParallelIterator, RankAlgorithm, RustAlgorithm,
    destination_chunks, exact_u32, execution, run_rank_on_pool,
};

pub(super) struct PageRank;

const PAGERANK_DAMPING: f64 = 0.85;
const PAGERANK_TOLERANCE: f64 = 1.0e-10;

const PAGERANK_CHECKPOINT_DESTINATIONS: usize = 4_096;

impl RustAlgorithm for PageRank {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::PageRank),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::PageRank);
        let node_len = graph.node_ids().len();
        if node_len == 0 {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let prepared = prepare_pagerank(graph)?;
        let node_count = f64::from(exact_u32(node_len, "node count")?);
        let mut scores = vec![1.0 / node_count; node_len];
        let path = select_pagerank_path(control, prepared.edge_count, node_len);
        loop {
            control.checkpoint()?;
            // Serial dangling reduction in dense ordinal order (accepted oracle).
            let dangling: f64 = prepared.dangling.iter().map(|&index| scores[index]).sum();
            let base =
                (1.0 - PAGERANK_DAMPING) / node_count + PAGERANK_DAMPING * dangling / node_count;
            let mut next = vec![base; node_len];
            match path {
                PageRankExecutionPath::Serial => {
                    pagerank_scatter_serial(graph, &prepared.indices, &scores, &mut next)?;
                }
                PageRankExecutionPath::Parallel { .. } => {
                    pagerank_pull_parallel(
                        &prepared.inbound,
                        &prepared.outdegrees,
                        &scores,
                        base,
                        &mut next,
                        control,
                    )?;
                }
            }
            // Serial L1 delta in dense ordinal order (accepted oracle).
            let delta: f64 = scores
                .iter()
                .zip(&next)
                .map(|(previous, current)| (previous - current).abs())
                .sum();
            scores = next;
            if delta <= node_count * PAGERANK_TOLERANCE {
                break;
            }
        }

        let mut sink = control.output_sink(algorithm)?;
        for (index, &node) in graph.node_ids().iter().enumerate() {
            if index % 1024 == 0 {
                control.checkpoint()?;
            }
            let uuid = graph
                .node_uuid(node)
                .ok_or_else(|| execution("selected node has no UUID identity"))?;
            sink.append_row(&[
                AlgorithmValue::Uuid(uuid),
                AlgorithmValue::Float64(scores[index]),
            ])?;
        }
        sink.finish()
    }
}

/// Selected PageRank execution path for observability and crossover tests (#343).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageRankExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Dense inbound CSR: source ordinals in canonical source/edge order per destination.
#[derive(Clone, Debug, Default)]
struct PageRankInboundCsr {
    offsets: Vec<u32>,
    sources: Vec<u32>,
}

struct PreparedPageRank {
    indices: HashMap<u64, usize>,
    outdegrees: Vec<f64>,
    dangling: Vec<usize>,
    inbound: PageRankInboundCsr,
    edge_count: u64,
}

fn prepare_pagerank(graph: &AdjacencyGraph) -> Result<PreparedPageRank, AlgorithmError> {
    let node_ids = graph.node_ids();
    let node_len = node_ids.len();
    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut outdegrees = Vec::with_capacity(node_len);
    let mut dangling = Vec::new();
    let mut inbound_counts = vec![0_u32; node_len];
    let mut edge_count = 0_u64;
    for (source_index, &source) in node_ids.iter().enumerate() {
        let edges = graph.neighbors(source);
        let degree = exact_u32(edges.len(), "node degree")?;
        outdegrees.push(f64::from(degree));
        if edges.is_empty() {
            dangling.push(source_index);
            continue;
        }
        for edge in edges {
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            inbound_counts[target] = inbound_counts[target]
                .checked_add(1)
                .ok_or_else(|| execution("inbound degree exceeds supported range"))?;
            edge_count = edge_count
                .checked_add(1)
                .ok_or_else(|| execution("edge count exceeds supported range"))?;
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

    Ok(PreparedPageRank {
        indices,
        outdegrees,
        dangling,
        inbound: PageRankInboundCsr { offsets, sources },
        edge_count,
    })
}

/// Choose serial vs private-pool parallel execution for a PageRank workload.
pub(crate) fn select_pagerank_path(
    control: &AlgorithmControl,
    edge_count: u64,
    nodes: usize,
) -> PageRankExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || edge_count < PAGERANK_PARALLEL_CROSSOVER_EDGES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return PageRankExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    PageRankExecutionPath::Parallel { threads, chunks }
}

fn pagerank_scatter_serial(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    scores: &[f64],
    next: &mut [f64],
) -> Result<(), AlgorithmError> {
    for (source_index, &source) in graph.node_ids().iter().enumerate() {
        let edges = graph.neighbors(source);
        if edges.is_empty() {
            continue;
        }
        let outdegree = f64::from(exact_u32(edges.len(), "node degree")?);
        let contribution = PAGERANK_DAMPING * scores[source_index] / outdegree;
        for edge in edges {
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            next[target] += contribution;
        }
    }
    Ok(())
}

fn pagerank_pull_destination(
    inbound: &PageRankInboundCsr,
    outdegrees: &[f64],
    scores: &[f64],
    base: f64,
    dest: usize,
) -> f64 {
    let start = usize::try_from(inbound.offsets[dest]).unwrap_or(0);
    let end = usize::try_from(inbound.offsets[dest + 1]).unwrap_or(start);
    let mut acc = base;
    for &source in &inbound.sources[start.min(end)..end.min(inbound.sources.len())] {
        let source = usize::try_from(source).unwrap_or(usize::MAX);
        if source < scores.len() {
            acc += PAGERANK_DAMPING * scores[source] / outdegrees[source];
        }
    }
    acc
}

fn pagerank_pull_parallel(
    inbound: &PageRankInboundCsr,
    outdegrees: &[f64],
    scores: &[f64],
    base: f64,
    next: &mut [f64],
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel PageRank requires an instance-owned compute pool"))?;
    let ranges = destination_chunks(next.len(), control.compute_threads());
    let work = AtomicUsize::new(0);
    let chunk_results = run_pagerank_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                for dest in start..end {
                    let observed = work.fetch_add(1, Ordering::Relaxed) + 1;
                    if observed.is_multiple_of(PAGERANK_CHECKPOINT_DESTINATIONS) {
                        control.check_cancelled()?;
                    }
                    local.push(pagerank_pull_destination(
                        inbound, outdegrees, scores, base, dest,
                    ));
                }
                Ok((start, local))
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;
    // Merge chunk outputs in ascending destination-range order (canonical).
    for (start, local) in chunk_results {
        next[start..start + local.len()].copy_from_slice(&local);
    }
    Ok(())
}

fn run_pagerank_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    run_rank_on_pool(pool, "PageRank", op)
}

#[cfg(test)]
mod tests;
