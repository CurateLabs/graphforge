//! Closeness rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AssertUnwindSafe, BUILTIN_REVIEW, CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS,
    HashMap, IntoParallelRefIterator, ParallelIterator, RankAlgorithm, RustAlgorithm, VecDeque,
    catch_unwind, exact_u32, execution, rank_scores_output, source_chunks,
};

pub(super) struct Closeness;

const CLOSENESS_CHECKPOINT_EDGES: usize = 1_024;

/// Selected closeness execution path for observability and crossover tests (#503).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClosenessExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

#[derive(Clone, Debug, Default)]
struct PreparedCloseness {
    offsets: Vec<u32>,
    targets: Vec<u32>,
    edge_count: u64,
}

impl PreparedCloseness {
    fn sources(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    fn neighbors(&self, source: usize) -> &[u32] {
        let start = usize::try_from(self.offsets[source]).unwrap_or(0);
        let end = usize::try_from(self.offsets[source + 1]).unwrap_or(start);
        &self.targets[start.min(end)..end.min(self.targets.len())]
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ClosenessSourceScore {
    score: f64,
    checkpoints: u64,
}

#[derive(Clone, Debug, PartialEq)]
struct ClosenessChunkScores {
    start: usize,
    scores: Vec<f64>,
    checkpoints: u64,
}

impl RustAlgorithm for Closeness {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Closeness),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Closeness);
        let node_ids = graph.node_ids();
        if node_ids.is_empty() {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let prepared = prepare_closeness(graph)?;
        let node_count = f64::from(exact_u32(node_ids.len(), "node count")?);
        let scores = match select_closeness_path(control, node_ids.len(), prepared.edge_count) {
            ClosenessExecutionPath::Serial => {
                closeness_scores_serial(&prepared, node_count, control)?
            }
            ClosenessExecutionPath::Parallel { .. } => {
                closeness_scores_parallel(&prepared, node_count, control)?
            }
        };
        rank_scores_output(algorithm, graph, scores, control)
    }
}

fn prepare_closeness(graph: &AdjacencyGraph) -> Result<PreparedCloseness, AlgorithmError> {
    let node_ids = graph.node_ids();
    let mut ordinals = HashMap::with_capacity(node_ids.len());
    for (index, &node) in node_ids.iter().enumerate() {
        ordinals.insert(node, exact_u32(index, "node index")?);
    }
    let capacity = usize::try_from(graph.edge_entry_count())
        .map_err(|_| execution("edge count exceeds supported range"))?;
    let mut offsets = Vec::with_capacity(node_ids.len() + 1);
    let mut targets = Vec::with_capacity(capacity);
    offsets.push(0);
    for &node in node_ids {
        for edge in graph.neighbors(node) {
            let target = ordinals
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            targets.push(target);
        }
        offsets.push(exact_u32(targets.len(), "adjacency offset")?);
    }
    let edge_count = u64::try_from(targets.len())
        .map_err(|_| execution("edge count exceeds supported range"))?;
    Ok(PreparedCloseness {
        offsets,
        targets,
        edge_count,
    })
}

/// Choose serial vs private-pool parallel execution for a closeness workload.
pub(crate) fn select_closeness_path(
    control: &AlgorithmControl,
    sources: usize,
    edge_count: u64,
) -> ClosenessExecutionPath {
    let threads = control.compute_threads();
    let estimated_edge_visits = estimated_closeness_edge_visits(sources, edge_count);
    if threads <= 1
        || sources <= 1
        || estimated_edge_visits < CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return ClosenessExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    ClosenessExecutionPath::Parallel { threads, chunks }
}

fn estimated_closeness_edge_visits(sources: usize, edge_count: u64) -> u64 {
    u64::try_from(sources)
        .unwrap_or(u64::MAX)
        .saturating_mul(edge_count)
}

fn closeness_scores_serial(
    prepared: &PreparedCloseness,
    node_count: f64,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut scores = Vec::with_capacity(prepared.sources());
    for source in 0..prepared.sources() {
        scores.push(closeness_score_source(prepared, source, node_count, control, true)?.score);
    }
    Ok(scores)
}

fn closeness_scores_parallel(
    prepared: &PreparedCloseness,
    node_count: f64,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel closeness requires an instance-owned compute pool"))?;
    let ranges = source_chunks(prepared.sources(), control.compute_threads());
    let chunk_results = run_closeness_on_pool(pool, || {
        Ok(ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut scores = Vec::with_capacity(end - start);
                let mut checkpoints = 0_u64;
                for source in start..end {
                    let result =
                        closeness_score_source(prepared, source, node_count, control, false)?;
                    checkpoints = checkpoints
                        .checked_add(result.checkpoints)
                        .ok_or_else(|| execution("closeness checkpoint count overflows"))?;
                    scores.push(result.score);
                }
                Ok(ClosenessChunkScores {
                    start,
                    scores,
                    checkpoints,
                })
            })
            .collect::<Vec<Result<_, AlgorithmError>>>())
    })?;
    let chunks = first_closeness_chunk_error(chunk_results)?;
    let mut scores = vec![0.0; prepared.sources()];
    for chunk in chunks {
        for _ in 0..chunk.checkpoints {
            control.checkpoint()?;
        }
        scores[chunk.start..chunk.start + chunk.scores.len()].copy_from_slice(&chunk.scores);
    }
    Ok(scores)
}

fn closeness_score_source(
    prepared: &PreparedCloseness,
    source: usize,
    node_count: f64,
    control: &AlgorithmControl,
    consume_checkpoints: bool,
) -> Result<ClosenessSourceScore, AlgorithmError> {
    let mut checkpoints = 0_u64;
    closeness_checkpoint(control, consume_checkpoints, &mut checkpoints)?;
    let mut distance = vec![usize::MAX; prepared.sources()];
    distance[source] = 0;
    let mut queue = VecDeque::from([source]);
    let mut traversed_edges = 0_usize;

    while let Some(vertex) = queue.pop_front() {
        for &target in prepared.neighbors(vertex) {
            if traversed_edges > 0 && traversed_edges.is_multiple_of(CLOSENESS_CHECKPOINT_EDGES) {
                closeness_checkpoint(control, consume_checkpoints, &mut checkpoints)?;
            }
            traversed_edges += 1;
            let target = usize::try_from(target)
                .map_err(|_| execution("adjacency index exceeds supported range"))?;
            if distance[target] == usize::MAX {
                distance[target] = distance[vertex] + 1;
                queue.push_back(target);
            }
        }
    }

    let mut reachable = 0_u32;
    let mut distance_sum = 0.0_f64;
    for hops in distance
        .into_iter()
        .filter(|&hops| hops != 0 && hops != usize::MAX)
    {
        reachable = reachable
            .checked_add(1)
            .ok_or_else(|| execution("reachable-node count exceeds supported score range"))?;
        distance_sum += f64::from(exact_u32(hops, "shortest-path distance")?);
    }
    let reachable = f64::from(reachable);
    let score = if node_count > 1.0 && reachable > 0.0 {
        reachable * reachable / ((node_count - 1.0) * distance_sum)
    } else {
        0.0
    };
    if !score.is_finite() {
        return Err(execution("closeness score exceeds supported range"));
    }
    Ok(ClosenessSourceScore { score, checkpoints })
}

fn closeness_checkpoint(
    control: &AlgorithmControl,
    consume: bool,
    checkpoints: &mut u64,
) -> Result<(), AlgorithmError> {
    if consume {
        control.checkpoint()?;
    } else {
        control.check_cancelled()?;
        *checkpoints = checkpoints
            .checked_add(1)
            .ok_or_else(|| execution("closeness checkpoint count overflows"))?;
    }
    Ok(())
}

fn first_closeness_chunk_error(
    results: Vec<Result<ClosenessChunkScores, AlgorithmError>>,
) -> Result<Vec<ClosenessChunkScores>, AlgorithmError> {
    let mut chunks = Vec::with_capacity(results.len());
    let mut first_error = None;
    for result in results {
        match result {
            Ok(chunk) => chunks.push(chunk),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    chunks.sort_unstable_by_key(|chunk| chunk.start);
    Ok(chunks)
}

fn run_closeness_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("Closeness worker panicked")),
    }
}

#[cfg(test)]
mod tests;
