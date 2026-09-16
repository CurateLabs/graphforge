//! Harmonic closeness rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AssertUnwindSafe, BUILTIN_REVIEW,
    HARMONIC_CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS, HashMap, IntoParallelRefIterator,
    ParallelIterator, RankAlgorithm, RustAlgorithm, VecDeque, catch_unwind, exact_u32, execution,
    rank_scores_output, source_chunks,
};

pub(super) struct HarmonicCloseness;

const HARMONIC_CLOSENESS_CHECKPOINT_EDGES: usize = 1_024;

/// Selected harmonic closeness path for observability and crossover tests (#508).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HarmonicClosenessExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

#[derive(Clone, Debug, Default)]
struct PreparedHarmonicCloseness {
    offsets: Vec<u32>,
    targets: Vec<u32>,
    edge_count: u64,
}

impl PreparedHarmonicCloseness {
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
struct HarmonicClosenessSourceScore {
    score: f64,
    checkpoints: u64,
}

#[derive(Clone, Debug, PartialEq)]
struct HarmonicClosenessChunkScores {
    start: usize,
    scores: Vec<f64>,
    checkpoints: u64,
}

impl RustAlgorithm for HarmonicCloseness {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::HarmonicCloseness),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::HarmonicCloseness);
        let node_ids = graph.node_ids();
        if node_ids.is_empty() {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let prepared = prepare_harmonic_closeness(graph)?;
        let denominator = f64::from(exact_u32(
            node_ids.len().saturating_sub(1).max(1),
            "node count",
        )?);
        let scores =
            match select_harmonic_closeness_path(control, node_ids.len(), prepared.edge_count) {
                HarmonicClosenessExecutionPath::Serial => {
                    harmonic_closeness_scores_serial(&prepared, denominator, control)?
                }
                HarmonicClosenessExecutionPath::Parallel { .. } => {
                    harmonic_closeness_scores_parallel(&prepared, denominator, control)?
                }
            };
        rank_scores_output(algorithm, graph, scores, control)
    }
}

fn prepare_harmonic_closeness(
    graph: &AdjacencyGraph,
) -> Result<PreparedHarmonicCloseness, AlgorithmError> {
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
    Ok(PreparedHarmonicCloseness {
        offsets,
        targets,
        edge_count,
    })
}

/// Choose serial vs private-pool parallel execution for harmonic closeness.
pub(crate) fn select_harmonic_closeness_path(
    control: &AlgorithmControl,
    sources: usize,
    edge_count: u64,
) -> HarmonicClosenessExecutionPath {
    let threads = control.compute_threads();
    let estimated_edge_visits = estimated_harmonic_closeness_edge_visits(sources, edge_count);
    if threads <= 1
        || sources <= 1
        || estimated_edge_visits < HARMONIC_CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return HarmonicClosenessExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    HarmonicClosenessExecutionPath::Parallel { threads, chunks }
}

fn estimated_harmonic_closeness_edge_visits(sources: usize, edge_count: u64) -> u64 {
    u64::try_from(sources)
        .unwrap_or(u64::MAX)
        .saturating_mul(edge_count)
}

fn harmonic_closeness_scores_serial(
    prepared: &PreparedHarmonicCloseness,
    denominator: f64,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut scores = Vec::with_capacity(prepared.sources());
    for source in 0..prepared.sources() {
        scores.push(
            harmonic_closeness_score_source(prepared, source, denominator, control, true)?.score,
        );
    }
    Ok(scores)
}

fn harmonic_closeness_scores_parallel(
    prepared: &PreparedHarmonicCloseness,
    denominator: f64,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel harmonic closeness requires an instance-owned compute pool")
    })?;
    let ranges = source_chunks(prepared.sources(), control.compute_threads());
    let chunk_results = run_harmonic_closeness_on_pool(pool, || {
        Ok(ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut scores = Vec::with_capacity(end - start);
                let mut checkpoints = 0_u64;
                for source in start..end {
                    let result = harmonic_closeness_score_source(
                        prepared,
                        source,
                        denominator,
                        control,
                        false,
                    )?;
                    checkpoints = checkpoints.checked_add(result.checkpoints).ok_or_else(|| {
                        execution("harmonic closeness checkpoint count overflows")
                    })?;
                    scores.push(result.score);
                }
                Ok(HarmonicClosenessChunkScores {
                    start,
                    scores,
                    checkpoints,
                })
            })
            .collect::<Vec<Result<_, AlgorithmError>>>())
    })?;
    let chunks = first_harmonic_closeness_chunk_error(chunk_results)?;
    let mut scores = vec![0.0; prepared.sources()];
    for chunk in chunks {
        for _ in 0..chunk.checkpoints {
            control.checkpoint()?;
        }
        scores[chunk.start..chunk.start + chunk.scores.len()].copy_from_slice(&chunk.scores);
    }
    Ok(scores)
}

fn harmonic_closeness_score_source(
    prepared: &PreparedHarmonicCloseness,
    source: usize,
    denominator: f64,
    control: &AlgorithmControl,
    consume_checkpoints: bool,
) -> Result<HarmonicClosenessSourceScore, AlgorithmError> {
    let mut checkpoints = 0_u64;
    harmonic_closeness_checkpoint(control, consume_checkpoints, &mut checkpoints)?;
    let mut distance = vec![usize::MAX; prepared.sources()];
    distance[source] = 0;
    let mut queue = VecDeque::from([source]);
    let mut traversed_edges = 0_usize;

    while let Some(vertex) = queue.pop_front() {
        for &target in prepared.neighbors(vertex) {
            if traversed_edges > 0
                && traversed_edges.is_multiple_of(HARMONIC_CLOSENESS_CHECKPOINT_EDGES)
            {
                harmonic_closeness_checkpoint(control, consume_checkpoints, &mut checkpoints)?;
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

    let mut reciprocal_sum = 0.0_f64;
    for hops in distance
        .into_iter()
        .filter(|&hops| hops != 0 && hops != usize::MAX)
    {
        reciprocal_sum += 1.0 / f64::from(exact_u32(hops, "shortest-path distance")?);
    }
    let score = reciprocal_sum / denominator;
    if !score.is_finite() {
        return Err(execution(
            "harmonic closeness score exceeds supported range",
        ));
    }
    Ok(HarmonicClosenessSourceScore { score, checkpoints })
}

fn harmonic_closeness_checkpoint(
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
            .ok_or_else(|| execution("harmonic closeness checkpoint count overflows"))?;
    }
    Ok(())
}

fn first_harmonic_closeness_chunk_error(
    results: Vec<Result<HarmonicClosenessChunkScores, AlgorithmError>>,
) -> Result<Vec<HarmonicClosenessChunkScores>, AlgorithmError> {
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

fn run_harmonic_closeness_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("Harmonic closeness worker panicked")),
    }
}

#[cfg(test)]
mod tests;
