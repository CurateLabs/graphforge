//! Betweenness rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, AlgorithmValue, AssertUnwindSafe, BETWEENNESS_PARALLEL_CROSSOVER_WORK,
    BUILTIN_REVIEW, HashMap, IntoParallelRefIterator, ParallelIterator, RankAlgorithm,
    RustAlgorithm, VecDeque, catch_unwind, exact_u32, execution, source_chunks,
};

pub(super) struct Betweenness;

impl RustAlgorithm for Betweenness {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Betweenness),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Betweenness);
        let node_ids = graph.node_ids();
        if node_ids.is_empty() {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let indices: HashMap<u64, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let mut scores =
            match select_betweenness_path(control, node_ids.len(), graph.edge_entry_count()) {
                BetweennessExecutionPath::Serial => {
                    betweenness_scores_serial(graph, &indices, control)?
                }
                BetweennessExecutionPath::Parallel { .. } => {
                    betweenness_scores_parallel(graph, &indices, control)?
                }
            };

        if node_ids.len() > 2 {
            let nodes = exact_u32(node_ids.len(), "node count")?;
            let scale = 1.0 / (f64::from(nodes - 1) * f64::from(nodes - 2));
            for score in &mut scores {
                *score *= scale;
            }
        }

        let rows = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| {
                let uuid = graph
                    .node_uuid(node)
                    .ok_or_else(|| execution("selected node has no UUID identity"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(uuid),
                    AlgorithmValue::Float64(scores[index]),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(algorithm, control, rows)
    }
}

/// Selected betweenness execution path for crossover tests and local observability (#501).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BetweennessExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BetweennessCheckpointMode {
    Consume,
    Defer,
}

#[derive(Debug)]
struct BetweennessSourceRun {
    source: usize,
    checkpoints: usize,
    contribution: Result<Vec<f64>, AlgorithmError>,
}

/// Choose serial vs private-pool parallel execution for a betweenness workload.
pub(crate) fn select_betweenness_path(
    control: &AlgorithmControl,
    nodes: usize,
    edge_count: u64,
) -> BetweennessExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || betweenness_work_estimate(nodes, edge_count) < BETWEENNESS_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return BetweennessExecutionPath::Serial;
    }
    let chunks = source_chunks(nodes, threads).len();
    BetweennessExecutionPath::Parallel { threads, chunks }
}

fn betweenness_work_estimate(nodes: usize, edge_count: u64) -> u64 {
    let nodes = u64::try_from(nodes).unwrap_or(u64::MAX);
    nodes.saturating_mul(nodes.saturating_add(edge_count))
}

fn betweenness_scores_serial(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut scores = vec![0.0; graph.node_ids().len()];
    for source in 0..graph.node_ids().len() {
        let mut checkpoints = 0_usize;
        let contribution = betweenness_source_contribution(
            graph,
            indices,
            source,
            control,
            BetweennessCheckpointMode::Consume,
            &mut checkpoints,
        )?;
        accumulate_betweenness_contribution(&mut scores, &contribution);
    }
    Ok(scores)
}

fn betweenness_scores_parallel(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel betweenness requires an instance-owned compute pool"))?;
    let ranges = source_chunks(graph.node_ids().len(), control.compute_threads());
    let mut chunk_results = run_betweenness_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                for source in start..end {
                    let mut checkpoints = 0_usize;
                    let contribution = betweenness_source_contribution(
                        graph,
                        indices,
                        source,
                        control,
                        BetweennessCheckpointMode::Defer,
                        &mut checkpoints,
                    );
                    local.push(BetweennessSourceRun {
                        source,
                        checkpoints,
                        contribution,
                    });
                }
                Ok((start, local))
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;
    chunk_results.sort_by_key(|(start, _)| *start);

    let mut scores = vec![0.0; graph.node_ids().len()];
    for (_, mut source_runs) in chunk_results {
        source_runs.sort_by_key(|run| run.source);
        for run in source_runs {
            for _ in 0..run.checkpoints {
                control.checkpoint()?;
            }
            let contribution = run.contribution?;
            accumulate_betweenness_contribution(&mut scores, &contribution);
        }
    }
    Ok(scores)
}

fn accumulate_betweenness_contribution(scores: &mut [f64], contribution: &[f64]) {
    for (score, delta) in scores.iter_mut().zip(contribution) {
        *score += *delta;
    }
}

fn betweenness_source_contribution(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    source: usize,
    control: &AlgorithmControl,
    checkpoint_mode: BetweennessCheckpointMode,
    checkpoints: &mut usize,
) -> Result<Vec<f64>, AlgorithmError> {
    let node_ids = graph.node_ids();
    betweenness_checkpoint(control, checkpoint_mode, checkpoints)?;
    let mut stack = Vec::with_capacity(node_ids.len());
    let mut predecessors = vec![Vec::new(); node_ids.len()];
    let mut paths = vec![0.0_f64; node_ids.len()];
    paths[source] = 1.0;
    let mut distance = vec![usize::MAX; node_ids.len()];
    distance[source] = 0;
    let mut queue = VecDeque::from([source]);
    let mut visited = 0_usize;
    let mut traversed_edges = 0_usize;

    while let Some(vertex) = queue.pop_front() {
        if visited > 0 && visited.is_multiple_of(1024) {
            betweenness_checkpoint(control, checkpoint_mode, checkpoints)?;
        }
        visited += 1;
        stack.push(vertex);
        for edge in graph.neighbors(node_ids[vertex]) {
            if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                betweenness_checkpoint(control, checkpoint_mode, checkpoints)?;
            }
            traversed_edges += 1;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            if distance[target] == usize::MAX {
                distance[target] = distance[vertex] + 1;
                queue.push_back(target);
            }
            if distance[target] == distance[vertex] + 1 {
                paths[target] += paths[vertex];
                if !paths[target].is_finite() {
                    return Err(execution(
                        "shortest-path multiplicity exceeds supported score range",
                    ));
                }
                predecessors[target].push(vertex);
            }
        }
    }

    let mut dependency = vec![0.0_f64; node_ids.len()];
    let mut contribution = vec![0.0_f64; node_ids.len()];
    let mut traversed_predecessors = 0_usize;
    while let Some(target) = stack.pop() {
        for &predecessor in &predecessors[target] {
            if traversed_predecessors > 0 && traversed_predecessors.is_multiple_of(1024) {
                betweenness_checkpoint(control, checkpoint_mode, checkpoints)?;
            }
            traversed_predecessors += 1;
            dependency[predecessor] +=
                paths[predecessor] / paths[target] * (1.0 + dependency[target]);
            if !dependency[predecessor].is_finite() {
                return Err(execution("betweenness dependency exceeds score range"));
            }
        }
        if target != source {
            contribution[target] = dependency[target];
        }
    }
    Ok(contribution)
}

fn betweenness_checkpoint(
    control: &AlgorithmControl,
    mode: BetweennessCheckpointMode,
    checkpoints: &mut usize,
) -> Result<(), AlgorithmError> {
    match mode {
        BetweennessCheckpointMode::Consume => {
            control.checkpoint()?;
        }
        BetweennessCheckpointMode::Defer => {
            control.check_cancelled()?;
            *checkpoints = checkpoints.saturating_add(1);
        }
    }
    Ok(())
}

fn run_betweenness_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("betweenness worker panicked")),
    }
}

#[cfg(test)]
mod tests;
