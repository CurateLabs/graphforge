//! Article rank rank execution and deterministic worker paths.

use super::{
    ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES, AdjacencyGraph, Algorithm, AlgorithmCapability,
    AlgorithmControl, AlgorithmError, AlgorithmOutput, AlgorithmValue, AssertUnwindSafe,
    AtomicUsize, BUILTIN_REVIEW, HashMap, IntoParallelRefIterator, Ordering, ParallelIterator,
    RankAlgorithm, RustAlgorithm, catch_unwind, destination_chunks, exact_u32, exact_u64_as_f64,
    execution,
};

pub(super) struct ArticleRank;

const ARTICLE_RANK_DAMPING: f64 = 0.85;
const ARTICLE_RANK_ALPHA: f64 = 1.0 - ARTICLE_RANK_DAMPING;
const ARTICLE_RANK_MAX_ITERATIONS: usize = 20;
const ARTICLE_RANK_TOLERANCE: f64 = 1.0e-7;

const ARTICLE_RANK_CHECKPOINT_DESTINATIONS: usize = 4_096;

impl RustAlgorithm for ArticleRank {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::ArticleRank),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::ArticleRank);
        let node_ids = graph.node_ids();
        let node_len = node_ids.len();
        if node_len == 0 {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let prepared = prepare_article_rank(graph)?;
        let mut scores = vec![ARTICLE_RANK_ALPHA; node_len];
        let mut deltas = scores.clone();
        let path = select_article_rank_path(control, prepared.edge_count, node_len);

        for _ in 0..ARTICLE_RANK_MAX_ITERATIONS {
            control.checkpoint()?;
            let mut next = vec![0.0; node_len];
            match path {
                ArticleRankExecutionPath::Serial => {
                    article_rank_pull_serial(
                        &prepared.inbound,
                        &prepared.outdegrees,
                        prepared.average_degree,
                        &deltas,
                        &mut next,
                        control,
                    )?;
                }
                ArticleRankExecutionPath::Parallel { .. } => {
                    article_rank_pull_parallel(
                        &prepared.inbound,
                        &prepared.outdegrees,
                        prepared.average_degree,
                        &deltas,
                        &mut next,
                        control,
                    )?;
                }
            }
            let mut converged = true;
            for (score, delta) in scores.iter_mut().zip(&mut next) {
                *delta *= ARTICLE_RANK_DAMPING;
                if !delta.is_finite() {
                    return Err(execution("ArticleRank score exceeds supported range"));
                }
                *score += *delta;
                if !score.is_finite() {
                    return Err(execution("ArticleRank score exceeds supported range"));
                }
                converged &= *delta <= ARTICLE_RANK_TOLERANCE;
            }
            deltas = next;
            if converged {
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

/// Selected ArticleRank execution path for observability and crossover tests (#500).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArticleRankExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Dense inbound CSR: source ordinals in canonical source/edge order per destination.
#[derive(Clone, Debug, Default)]
struct ArticleRankInboundCsr {
    offsets: Vec<u32>,
    sources: Vec<u32>,
}

struct PreparedArticleRank {
    outdegrees: Vec<f64>,
    inbound: ArticleRankInboundCsr,
    average_degree: f64,
    edge_count: u64,
}

fn prepare_article_rank(graph: &AdjacencyGraph) -> Result<PreparedArticleRank, AlgorithmError> {
    let node_ids = graph.node_ids();
    let node_len = node_ids.len();
    let node_count = exact_u32(node_len, "node count")?;
    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut outdegrees = Vec::with_capacity(node_len);
    let mut inbound_counts = vec![0_u32; node_len];
    let mut edge_count = 0_u64;
    for &source in node_ids {
        let edges = graph.neighbors(source);
        outdegrees.push(f64::from(exact_u32(edges.len(), "node degree")?));
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
                .ok_or_else(|| execution("selected edge count exceeds supported score range"))?;
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

    let average_degree =
        exact_u64_as_f64(edge_count, "selected edge count")? / f64::from(node_count);
    Ok(PreparedArticleRank {
        outdegrees,
        inbound: ArticleRankInboundCsr { offsets, sources },
        average_degree,
        edge_count,
    })
}

/// Choose serial vs private-pool parallel execution for an ArticleRank workload.
pub(crate) fn select_article_rank_path(
    control: &AlgorithmControl,
    edge_count: u64,
    nodes: usize,
) -> ArticleRankExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || edge_count < ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return ArticleRankExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    ArticleRankExecutionPath::Parallel { threads, chunks }
}

fn article_rank_pull_destination(
    inbound: &ArticleRankInboundCsr,
    outdegrees: &[f64],
    average_degree: f64,
    deltas: &[f64],
    dest: usize,
) -> f64 {
    let start = usize::try_from(inbound.offsets[dest]).unwrap_or(0);
    let end = usize::try_from(inbound.offsets[dest + 1]).unwrap_or(start);
    let mut acc = 0.0;
    for &source in &inbound.sources[start.min(end)..end.min(inbound.sources.len())] {
        let source = usize::try_from(source).unwrap_or(usize::MAX);
        if source < deltas.len() {
            acc += deltas[source] / (outdegrees[source] + average_degree);
        }
    }
    acc
}

fn article_rank_pull_serial(
    inbound: &ArticleRankInboundCsr,
    outdegrees: &[f64],
    average_degree: f64,
    deltas: &[f64],
    next: &mut [f64],
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    for (dest, value) in next.iter_mut().enumerate() {
        if dest > 0 && dest.is_multiple_of(ARTICLE_RANK_CHECKPOINT_DESTINATIONS) {
            control.checkpoint()?;
        }
        *value = article_rank_pull_destination(inbound, outdegrees, average_degree, deltas, dest);
    }
    Ok(())
}

fn article_rank_pull_parallel(
    inbound: &ArticleRankInboundCsr,
    outdegrees: &[f64],
    average_degree: f64,
    deltas: &[f64],
    next: &mut [f64],
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel ArticleRank requires an instance-owned compute pool"))?;
    let ranges = destination_chunks(next.len(), control.compute_threads());
    let work = AtomicUsize::new(0);
    let chunk_results = run_article_rank_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                for dest in start..end {
                    let observed = work.fetch_add(1, Ordering::Relaxed) + 1;
                    if observed.is_multiple_of(ARTICLE_RANK_CHECKPOINT_DESTINATIONS) {
                        control.check_cancelled()?;
                    }
                    local.push(article_rank_pull_destination(
                        inbound,
                        outdegrees,
                        average_degree,
                        deltas,
                        dest,
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

fn run_article_rank_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("ArticleRank worker panicked")),
    }
}

#[cfg(test)]
mod tests;
