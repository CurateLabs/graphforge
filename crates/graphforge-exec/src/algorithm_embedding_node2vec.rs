//! Deterministic Node2Vec walk-corpus generation and serial SGNS training.
//!
//! Walk-corpus construction (#344) may partition independent `(start ordinal,
//! walk ordinal)` tasks across the instance-owned private compute pool above a
//! documented crossover. Seed derivation, candidate ordering, transition-mass
//! accumulation, sampling, and every generated walk remain identical to the
//! serial path. Skip-gram / negative-sampling training stays serial and
//! preserves embedding fingerprints; #562 precomputes canonical negative
//! sampling masses once per corpus so the serial trainer avoids per-sample mass
//! vector allocation without changing RNG keys or draw order.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use graphforge_core::embedding_options::Node2VecOptions;
use rayon::prelude::*;

use crate::algorithm_embedding_control::{EmbeddingControl, EmbeddingResourceError};
use crate::algorithm_embedding_output::EmbeddingOutputRow;
use crate::algorithm_embedding_rng::{EmbeddingRng, EmbeddingRngField};
use crate::algorithm_graph::{AdjacencyGraph, AlgorithmEdge};

/// Estimated walk transitions below which corpus generation stays serial (#344).
///
/// Small fixtures and micro-invocations stay off the worker pool; above this,
/// start/walk-parallel execution amortizes scheduling. Exact walks and token
/// counts remain identical either way.
pub const NODE2VEC_WALK_PARALLEL_CROSSOVER: u64 = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Node2VecCorpus {
    pub(crate) walks: Vec<Vec<u64>>,
    pub(crate) token_counts: HashMap<u64, u64>,
}

pub(crate) type Node2VecEmbeddingRow = EmbeddingOutputRow;

/// Selected walk-corpus execution path for observability and crossover tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WalkCorpusPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub(crate) enum Node2VecWalkError {
    #[error("node2vec edge weight must be finite and non-negative")]
    InvalidWeight,
    #[error("node2vec transition mass must be finite")]
    InvalidTransitionMass,
    #[error("node2vec token count exceeds UInt64 range")]
    TokenCountOverflow,
    #[error("node2vec training produced a non-finite embedding")]
    NonFiniteEmbedding,
    #[error("node2vec walk worker panicked")]
    WorkerPanic,
    #[error(transparent)]
    Resource(#[from] EmbeddingResourceError),
}

pub(crate) fn train_node2vec(
    graph: &AdjacencyGraph,
    options: &Node2VecOptions,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<Node2VecEmbeddingRow>, Node2VecWalkError> {
    let corpus = build_walk_corpus(graph, options, control)?;
    let nodes = canonical_nodes(graph);
    let node_index = nodes
        .iter()
        .enumerate()
        .map(|(index, &(_, node_id))| (node_id, index))
        .collect::<HashMap<_, _>>();
    let negative_table = NegativeSamplingTable::new(&nodes, &corpus.token_counts)?;
    let mut input = initialize_input(&nodes, options);
    let mut output = vec![vec![0.0_f32; options.dimensions]; nodes.len()];
    let learning_rate = f64_to_f32(options.learning_rate);

    for epoch in 0..options.epochs {
        let epoch = to_u64(epoch)?;
        for (walk_index, walk) in corpus.walks.iter().enumerate() {
            let walk_ordinal = to_u64(walk_index % options.walks_per_node)?;
            let start_uuid = graph
                .node_uuid(walk[0])
                .expect("walk start belongs to selected graph");
            for center_position in 0..walk.len() {
                let lower = center_position.saturating_sub(options.window_size);
                let upper = center_position
                    .saturating_add(options.window_size)
                    .saturating_add(1)
                    .min(walk.len());
                for context_position in lower..upper {
                    if center_position == context_position {
                        continue;
                    }
                    control.checkpoint(1)?;
                    let center_id = walk[center_position];
                    let context_id = walk[context_position];
                    let center_index = node_index[&center_id];
                    let context_index = node_index[&context_id];
                    let center = input[center_index].clone();
                    let mut delta = vec![0.0_f32; options.dimensions];

                    update_sample(
                        &center,
                        &mut output[context_index],
                        &mut delta,
                        1.0,
                        learning_rate,
                    );
                    control.checkpoint(1)?;

                    for negative_ordinal in 0..options.negative_samples {
                        let Some(negative_id) = sample_negative(
                            &negative_table,
                            context_id,
                            options.seed,
                            epoch,
                            start_uuid,
                            walk_ordinal,
                            to_u64(center_position)?,
                            to_u64(context_position)?,
                            to_u64(negative_ordinal)?,
                        )?
                        else {
                            break;
                        };
                        let negative_index = node_index[&negative_id];
                        update_sample(
                            &center,
                            &mut output[negative_index],
                            &mut delta,
                            0.0,
                            learning_rate,
                        );
                        control.checkpoint(1)?;
                    }
                    for (coordinate, delta) in delta.into_iter().enumerate() {
                        input[center_index][coordinate] += delta;
                    }
                }
            }
        }
    }

    control.before_publish()?;
    nodes
        .into_iter()
        .enumerate()
        .map(|(index, (node_uuid, _))| {
            let embedding = input[index].clone();
            if embedding.iter().all(|value| value.is_finite()) {
                Ok(Node2VecEmbeddingRow {
                    node_uuid,
                    embedding,
                })
            } else {
                Err(Node2VecWalkError::NonFiniteEmbedding)
            }
        })
        .collect()
}

fn initialize_input(nodes: &[([u8; 16], u64)], options: &Node2VecOptions) -> Vec<Vec<f32>> {
    let scale = 0.5_f64 / usize_to_f64(options.dimensions);
    nodes
        .iter()
        .map(|&(node_uuid, _)| {
            (0..options.dimensions)
                .map(|coordinate| {
                    let mut rng = EmbeddingRng::derive(
                        "node2vec",
                        "node2vec-init-input",
                        options.seed,
                        &[
                            EmbeddingRngField::Uuid(node_uuid),
                            EmbeddingRngField::U64(
                                u64::try_from(coordinate).expect("validated dimensions fit UInt64"),
                            ),
                        ],
                    );
                    f64_to_f32((rng.unit_f64() * 2.0 - 1.0) * scale)
                })
                .collect()
        })
        .collect()
}

#[derive(Clone, Debug, PartialEq)]
struct NegativeSamplingTable {
    masses: Vec<(u64, f64)>,
    total: f64,
}

impl NegativeSamplingTable {
    fn new(
        nodes: &[([u8; 16], u64)],
        token_counts: &HashMap<u64, u64>,
    ) -> Result<Self, Node2VecWalkError> {
        let masses = nodes
            .iter()
            .map(|&(_, node_id)| {
                let count = token_counts.get(&node_id).copied().unwrap_or(0);
                (node_id, u64_to_f64(count).powf(0.75))
            })
            .collect::<Vec<_>>();
        let total = masses.iter().map(|(_, mass)| mass).sum::<f64>();
        if !total.is_finite() {
            return Err(Node2VecWalkError::InvalidTransitionMass);
        }
        Ok(Self { masses, total })
    }

    fn context_mass(&self, context_id: u64) -> f64 {
        self.masses
            .iter()
            .find_map(|&(node_id, mass)| (node_id == context_id).then_some(mass))
            .unwrap_or(0.0)
    }
}

#[allow(clippy::too_many_arguments)]
fn sample_negative(
    table: &NegativeSamplingTable,
    context_id: u64,
    seed: u64,
    epoch: u64,
    start_uuid: [u8; 16],
    walk_ordinal: u64,
    center_position: u64,
    context_position: u64,
    negative_ordinal: u64,
) -> Result<Option<u64>, Node2VecWalkError> {
    let total = table.total - table.context_mass(context_id);
    if total == 0.0 {
        return Ok(None);
    }
    if !total.is_finite() {
        return Err(Node2VecWalkError::InvalidTransitionMass);
    }
    let mut rng = EmbeddingRng::derive(
        "node2vec",
        "negative",
        seed,
        &[
            EmbeddingRngField::U64(epoch),
            EmbeddingRngField::Uuid(start_uuid),
            EmbeddingRngField::U64(walk_ordinal),
            EmbeddingRngField::U64(center_position),
            EmbeddingRngField::U64(context_position),
            EmbeddingRngField::U64(negative_ordinal),
        ],
    );
    let draw = rng.unit_f64() * total;
    let mut cumulative = 0.0;
    Ok(table.masses.iter().find_map(|&(node_id, mass)| {
        if node_id == context_id {
            return None;
        }
        cumulative += mass;
        (draw < cumulative).then_some(node_id)
    }))
}

fn update_sample(
    center: &[f32],
    output: &mut [f32],
    delta: &mut [f32],
    label: f32,
    learning_rate: f32,
) {
    let mut dot = 0.0_f32;
    for coordinate in 0..center.len() {
        dot += center[coordinate] * output[coordinate];
    }
    let sigmoid = 1.0 / (1.0 + (-dot.clamp(-15.0, 15.0)).exp());
    let gradient = learning_rate * (label - sigmoid);
    for coordinate in 0..center.len() {
        let old_output = output[coordinate];
        delta[coordinate] += gradient * old_output;
        output[coordinate] = old_output + gradient * center[coordinate];
    }
}

fn to_u64(value: usize) -> Result<u64, Node2VecWalkError> {
    u64::try_from(value).map_err(|_| Node2VecWalkError::TokenCountOverflow)
}

#[allow(clippy::cast_possible_truncation)]
fn f64_to_f32(value: f64) -> f32 {
    value as f32
}

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

#[allow(clippy::cast_precision_loss)]
fn u64_to_f64(value: u64) -> f64 {
    value as f64
}

pub(crate) fn build_walk_corpus(
    graph: &AdjacencyGraph,
    options: &Node2VecOptions,
    control: &EmbeddingControl<'_>,
) -> Result<Node2VecCorpus, Node2VecWalkError> {
    let starts = canonical_nodes(graph);
    let capacity = starts
        .len()
        .checked_mul(options.walks_per_node)
        .ok_or(Node2VecWalkError::TokenCountOverflow)?;
    let estimated =
        estimated_walk_transitions(starts.len(), options.walks_per_node, options.walk_length);
    match select_walk_corpus_path(control, capacity, estimated) {
        WalkCorpusPath::Serial => {
            build_walk_corpus_serial(graph, options, control, &starts, capacity)
        }
        WalkCorpusPath::Parallel { .. } => {
            build_walk_corpus_parallel(graph, options, control, &starts, capacity)
        }
    }
}

fn build_walk_corpus_serial(
    graph: &AdjacencyGraph,
    options: &Node2VecOptions,
    control: &EmbeddingControl<'_>,
    starts: &[([u8; 16], u64)],
    capacity: usize,
) -> Result<Node2VecCorpus, Node2VecWalkError> {
    let mut walks = Vec::with_capacity(capacity);
    let mut token_counts = HashMap::with_capacity(starts.len());

    for &(start_uuid, start_id) in starts {
        for walk_ordinal in 0..options.walks_per_node {
            control.checkpoint(1)?;
            let walk = build_walk(
                graph,
                options,
                control,
                start_uuid,
                start_id,
                u64::try_from(walk_ordinal).map_err(|_| Node2VecWalkError::TokenCountOverflow)?,
            )?;
            accumulate_tokens(&mut token_counts, &walk)?;
            walks.push(walk);
        }
    }

    Ok(Node2VecCorpus {
        walks,
        token_counts,
    })
}

fn build_walk_corpus_parallel(
    graph: &AdjacencyGraph,
    options: &Node2VecOptions,
    control: &EmbeddingControl<'_>,
    starts: &[([u8; 16], u64)],
    capacity: usize,
) -> Result<Node2VecCorpus, Node2VecWalkError> {
    let pool = control.compute_pool().ok_or_else(|| {
        Node2VecWalkError::Resource(EmbeddingResourceError::Algorithm(
            crate::algorithm_dispatch::AlgorithmError::Execution {
                message: "parallel node2vec walk corpus requires an instance-owned compute pool"
                    .into(),
            },
        ))
    })?;
    let walks_per_node = options.walks_per_node;
    let ranges = walk_task_chunks(capacity, control.compute_threads());
    let chunk_results = run_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut walks = Vec::with_capacity(end.saturating_sub(start));
                let mut token_counts = HashMap::new();
                for task in start..end {
                    let start_ordinal = task / walks_per_node;
                    let walk_ordinal = task % walks_per_node;
                    let (start_uuid, start_id) = starts[start_ordinal];
                    control.checkpoint(1)?;
                    let walk = build_walk(
                        graph,
                        options,
                        control,
                        start_uuid,
                        start_id,
                        u64::try_from(walk_ordinal)
                            .map_err(|_| Node2VecWalkError::TokenCountOverflow)?,
                    )?;
                    accumulate_tokens(&mut token_counts, &walk)?;
                    walks.push(walk);
                }
                Ok(WalkChunk {
                    walks,
                    token_counts,
                })
            })
            .collect::<Result<Vec<_>, Node2VecWalkError>>()
    })?;
    merge_walk_chunks(chunk_results, starts.len())
}

#[derive(Debug)]
struct WalkChunk {
    walks: Vec<Vec<u64>>,
    token_counts: HashMap<u64, u64>,
}

fn merge_walk_chunks(
    chunks: Vec<WalkChunk>,
    start_capacity: usize,
) -> Result<Node2VecCorpus, Node2VecWalkError> {
    let mut walks = Vec::new();
    let mut token_counts = HashMap::with_capacity(start_capacity);
    for chunk in chunks {
        walks.extend(chunk.walks);
        merge_token_counts(&mut token_counts, chunk.token_counts)?;
    }
    Ok(Node2VecCorpus {
        walks,
        token_counts,
    })
}

fn accumulate_tokens(
    token_counts: &mut HashMap<u64, u64>,
    walk: &[u64],
) -> Result<(), Node2VecWalkError> {
    for &node_id in walk {
        let count = token_counts.entry(node_id).or_insert(0_u64);
        *count = count
            .checked_add(1)
            .ok_or(Node2VecWalkError::TokenCountOverflow)?;
    }
    Ok(())
}

fn merge_token_counts(
    into: &mut HashMap<u64, u64>,
    from: HashMap<u64, u64>,
) -> Result<(), Node2VecWalkError> {
    let mut entries = from.into_iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(node_id, _)| *node_id);
    for (node_id, count) in entries {
        let entry = into.entry(node_id).or_insert(0_u64);
        *entry = entry
            .checked_add(count)
            .ok_or(Node2VecWalkError::TokenCountOverflow)?;
    }
    Ok(())
}

fn run_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, Node2VecWalkError> + Send,
) -> Result<R, Node2VecWalkError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(Node2VecWalkError::WorkerPanic),
    }
}

/// Choose serial vs private-pool parallel walk-corpus generation.
pub(crate) fn select_walk_corpus_path(
    control: &EmbeddingControl<'_>,
    total_walks: usize,
    estimated_transitions: u64,
) -> WalkCorpusPath {
    let threads = control.compute_threads();
    if threads <= 1 || total_walks <= 1 || estimated_transitions < NODE2VEC_WALK_PARALLEL_CROSSOVER
    {
        return WalkCorpusPath::Serial;
    }
    if control
        .compute_pool()
        .is_none_or(|pool| !pool.is_parallel())
    {
        return WalkCorpusPath::Serial;
    }
    let chunks = walk_task_chunks(total_walks, threads).len();
    if chunks <= 1 {
        return WalkCorpusPath::Serial;
    }
    WalkCorpusPath::Parallel { threads, chunks }
}

fn estimated_walk_transitions(starts: usize, walks_per_node: usize, walk_length: usize) -> u64 {
    let starts = u64::try_from(starts).unwrap_or(u64::MAX);
    let walks_per_node = u64::try_from(walks_per_node).unwrap_or(u64::MAX);
    let walk_length = u64::try_from(walk_length).unwrap_or(u64::MAX);
    starts
        .saturating_mul(walks_per_node)
        .saturating_mul(walk_length)
}

fn walk_task_chunks(total_walks: usize, threads: usize) -> Vec<(usize, usize)> {
    if total_walks == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, total_walks);
    let base = total_walks / workers;
    let rem = total_walks % workers;
    let mut ranges = Vec::with_capacity(workers);
    let mut start = 0;
    for index in 0..workers {
        let len = base + usize::from(index < rem);
        let end = start + len;
        if start < end {
            ranges.push((start, end));
        }
        start = end;
    }
    ranges
}

fn build_walk(
    graph: &AdjacencyGraph,
    options: &Node2VecOptions,
    control: &EmbeddingControl<'_>,
    start_uuid: [u8; 16],
    start_id: u64,
    walk_ordinal: u64,
) -> Result<Vec<u64>, Node2VecWalkError> {
    let mut walk = Vec::with_capacity(options.walk_length.saturating_add(1));
    walk.push(start_id);

    for transition in 0..options.walk_length {
        control.checkpoint(1)?;
        let current = *walk.last().expect("a walk always contains its start");
        let previous = walk.get(walk.len().wrapping_sub(2)).copied();
        let candidates = canonical_candidates(graph, current);
        let masses = candidates
            .iter()
            .map(|edge| transition_mass(graph, previous, edge, options))
            .collect::<Result<Vec<_>, _>>()?;
        let total = masses.iter().try_fold(0.0_f64, |total, mass| {
            let next = total + mass;
            next.is_finite()
                .then_some(next)
                .ok_or(Node2VecWalkError::InvalidTransitionMass)
        })?;
        if total == 0.0 {
            break;
        }

        let transition =
            u64::try_from(transition).map_err(|_| Node2VecWalkError::TokenCountOverflow)?;
        let mut rng = EmbeddingRng::derive(
            "node2vec",
            "walk",
            options.seed,
            &[
                EmbeddingRngField::Uuid(start_uuid),
                EmbeddingRngField::U64(walk_ordinal),
                EmbeddingRngField::U64(transition),
            ],
        );
        let draw = rng.unit_f64() * total;
        let mut cumulative = 0.0;
        let selected = candidates
            .iter()
            .zip(masses)
            .find_map(|(edge, mass)| {
                cumulative += mass;
                (draw < cumulative).then_some(edge.neighbor_id)
            })
            .unwrap_or_else(|| {
                candidates
                    .last()
                    .expect("positive total requires a candidate")
                    .neighbor_id
            });
        walk.push(selected);
    }
    Ok(walk)
}

fn canonical_nodes(graph: &AdjacencyGraph) -> Vec<([u8; 16], u64)> {
    let mut nodes = graph
        .node_ids()
        .iter()
        .filter_map(|&node_id| graph.node_uuid(node_id).map(|uuid| (uuid, node_id)))
        .collect::<Vec<_>>();
    nodes.sort_unstable();
    nodes
}

fn canonical_candidates(graph: &AdjacencyGraph, node_id: u64) -> Vec<&AlgorithmEdge> {
    let mut candidates = graph.neighbors(node_id).iter().collect::<Vec<_>>();
    candidates.sort_unstable_by_key(|edge| {
        (
            graph
                .node_uuid(edge.neighbor_id)
                .expect("adjacency neighbor belongs to the selected graph"),
            edge.edge_uuid,
        )
    });
    candidates
}

fn transition_mass(
    graph: &AdjacencyGraph,
    previous: Option<u64>,
    edge: &AlgorithmEdge,
    options: &Node2VecOptions,
) -> Result<f64, Node2VecWalkError> {
    if !edge.weight.is_finite() || edge.weight < 0.0 {
        return Err(Node2VecWalkError::InvalidWeight);
    }
    let bias = match previous {
        None => 1.0,
        Some(previous) if edge.neighbor_id == previous => 1.0 / options.p,
        Some(previous) if adjacent_to_previous(graph, edge.neighbor_id, previous) => 1.0,
        Some(_) => 1.0 / options.q,
    };
    let mass = edge.weight * bias;
    mass.is_finite()
        .then_some(mass)
        .ok_or(Node2VecWalkError::InvalidTransitionMass)
}

fn adjacent_to_previous(graph: &AdjacencyGraph, candidate: u64, previous: u64) -> bool {
    graph
        .neighbors(candidate)
        .iter()
        .any(|edge| edge.neighbor_id == previous)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmControl, AlgorithmLimits};
    use crate::algorithm_embedding_control::EmbeddingResourceLimits;
    use crate::compute_pool::ComputePool;
    use std::sync::Arc;

    fn controls(
        cancellation: AlgorithmCancellation,
        work: u64,
    ) -> (AlgorithmControl, EmbeddingResourceLimits) {
        (
            AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            EmbeddingResourceLimits {
                work,
                ..EmbeddingResourceLimits::default()
            },
        )
    }

    fn control_with_threads(threads: usize) -> AlgorithmControl {
        let pool = Arc::new(ComputePool::new(threads).unwrap());
        AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool)
    }

    fn embedding_control(algorithm: &AlgorithmControl) -> EmbeddingControl<'_> {
        EmbeddingControl::new(algorithm, EmbeddingResourceLimits::default())
    }

    fn corpus(
        graph: &AdjacencyGraph,
        options: &Node2VecOptions,
    ) -> Result<Node2VecCorpus, Node2VecWalkError> {
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
        build_walk_corpus(graph, options, &EmbeddingControl::new(&algorithm, limits))
    }

    fn corpus_with_control(
        graph: &AdjacencyGraph,
        options: &Node2VecOptions,
        algorithm: &AlgorithmControl,
    ) -> Result<Node2VecCorpus, Node2VecWalkError> {
        build_walk_corpus(graph, options, &embedding_control(algorithm))
    }

    fn adversarial_graph(nodes: usize) -> AdjacencyGraph {
        let nodes_u64 = u64::try_from(nodes).expect("test node count fits u64");
        let edges = (0..nodes_u64)
            .flat_map(|source| {
                [
                    (source, (source + 1) % nodes_u64),
                    (source, (source + 3) % nodes_u64),
                    (source, (source * 5 + 7) % nodes_u64),
                ]
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_directed_edges(nodes_u64, &edges)
    }

    #[test]
    fn replay_seed_and_uuid_order_are_deterministic() {
        let uuids = [
            3_u128.to_be_bytes(),
            1_u128.to_be_bytes(),
            2_u128.to_be_bytes(),
        ];
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(&uuids, &[(0, 1), (0, 2)]);
        let options = Node2VecOptions {
            walk_length: 1,
            walks_per_node: 8,
            seed: 7,
            ..Node2VecOptions::default()
        };
        let first = corpus(&graph, &options).unwrap();
        assert_eq!(first, corpus(&graph, &options).unwrap());
        assert_ne!(
            first,
            corpus(&graph, &Node2VecOptions { seed: 8, ..options }).unwrap()
        );
        assert_eq!(first.walks[0][0], 1);
        assert_eq!(first.walks[8][0], 2);
        assert_eq!(first.walks[16][0], 0);
    }

    #[test]
    fn second_order_bias_uses_directed_candidate_to_previous_adjacency() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (1, 0), (1, 2), (1, 3), (2, 0)]);
        let options = Node2VecOptions {
            p: 0.01,
            q: 100.0,
            ..Node2VecOptions::default()
        };
        let candidates = canonical_candidates(&graph, 1);
        let masses = candidates
            .iter()
            .map(|edge| {
                (
                    edge.neighbor_id,
                    transition_mass(&graph, Some(0), edge, &options).unwrap(),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(masses[&0], 100.0, "return candidate uses 1/p");
        assert_eq!(
            masses[&2], 1.0,
            "selected 2 -> 0 edge makes candidate distance one"
        );
        assert_eq!(masses[&3], 0.01, "distant candidate uses 1/q");
    }

    #[test]
    fn parallel_edges_self_loops_zero_mass_and_isolates_are_total() {
        let graph =
            AdjacencyGraph::with_test_undirected_multigraph(3, &[(7, 0, 1), (8, 0, 1), (9, 0, 0)])
                .with_test_edge_weights(&[0.0, 0.0, 0.0, 0.0, 0.0]);
        let candidates = canonical_candidates(&graph, 0);
        assert_eq!(candidates.len(), 3, "parallel edges remain distinct");
        assert_eq!(
            candidates
                .iter()
                .filter(|edge| edge.neighbor_id == 0)
                .count(),
            1,
            "self-loop appears once"
        );
        let result = corpus(
            &graph,
            &Node2VecOptions {
                walk_length: 4,
                walks_per_node: 2,
                ..Node2VecOptions::default()
            },
        )
        .unwrap();
        assert!(result.walks.iter().all(|walk| walk.len() == 1));
        assert_eq!(result.token_counts.values().copied().sum::<u64>(), 6);
    }

    #[test]
    fn invalid_weights_cancellation_and_work_limits_fail_atomically() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]).with_test_edge_weights(&[-1.0]);
        assert_eq!(
            corpus(&graph, &Node2VecOptions::default()),
            Err(Node2VecWalkError::InvalidWeight)
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let (algorithm, limits) = controls(cancellation, u64::MAX);
        assert!(matches!(
            build_walk_corpus(
                &graph,
                &Node2VecOptions::default(),
                &EmbeddingControl::new(&algorithm, limits)
            ),
            Err(Node2VecWalkError::Resource(
                EmbeddingResourceError::Algorithm(_)
            ))
        ));

        let graph = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), 1);
        assert!(matches!(
            build_walk_corpus(
                &graph,
                &Node2VecOptions {
                    walk_length: 1,
                    walks_per_node: 1,
                    ..Node2VecOptions::default()
                },
                &EmbeddingControl::new(&algorithm, limits)
            ),
            Err(Node2VecWalkError::Resource(
                EmbeddingResourceError::WorkLimit { .. }
            ))
        ));
    }

    #[test]
    fn sgns_output_is_exact_finite_uuid_ordered_and_replayable() {
        let uuids = [
            3_u128.to_be_bytes(),
            1_u128.to_be_bytes(),
            2_u128.to_be_bytes(),
        ];
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(&uuids, &[(0, 1), (1, 0)]);
        let options = Node2VecOptions {
            dimensions: 2,
            walk_length: 2,
            walks_per_node: 1,
            window_size: 1,
            negative_samples: 1,
            epochs: 1,
            learning_rate: 0.025,
            seed: 3,
            ..Node2VecOptions::default()
        };
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
        let first =
            train_node2vec(&graph, &options, &EmbeddingControl::new(&algorithm, limits)).unwrap();
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
        assert_eq!(
            first,
            train_node2vec(&graph, &options, &EmbeddingControl::new(&algorithm, limits)).unwrap()
        );
        assert_eq!(
            first.iter().map(|row| row.node_uuid).collect::<Vec<_>>(),
            [
                1_u128.to_be_bytes(),
                2_u128.to_be_bytes(),
                3_u128.to_be_bytes()
            ]
        );
        assert!(first.iter().all(|row| {
            row.embedding.len() == 2 && row.embedding.iter().all(|value| value.is_finite())
        }));
        assert_eq!(
            first
                .iter()
                .flat_map(|row| row.embedding.iter().map(|value| value.to_bits()))
                .collect::<Vec<_>>(),
            [
                1_033_343_337,
                1_042_572_670,
                1_031_203_965,
                1_027_727_270,
                3_161_040_333,
                3_171_706_725,
            ]
        );
    }

    #[test]
    fn sgns_seed_epochs_isolates_cancellation_and_work_are_observable() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 0)]);
        let options = Node2VecOptions {
            dimensions: 3,
            walk_length: 2,
            walks_per_node: 1,
            window_size: 1,
            negative_samples: 1,
            epochs: 1,
            seed: 4,
            ..Node2VecOptions::default()
        };
        let trained = corpus_training(&graph, &options).unwrap();
        assert!(trained[2].embedding.iter().any(|value| *value != 0.0));
        assert_ne!(
            trained,
            corpus_training(
                &graph,
                &Node2VecOptions {
                    seed: 5,
                    ..options.clone()
                }
            )
            .unwrap()
        );
        assert_ne!(
            trained,
            corpus_training(
                &graph,
                &Node2VecOptions {
                    epochs: 2,
                    ..options
                }
            )
            .unwrap()
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let (algorithm, limits) = controls(cancellation, u64::MAX);
        assert!(matches!(
            train_node2vec(
                &graph,
                &Node2VecOptions::default(),
                &EmbeddingControl::new(&algorithm, limits)
            ),
            Err(Node2VecWalkError::Resource(
                EmbeddingResourceError::Algorithm(_)
            ))
        ));
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), 2);
        assert!(matches!(
            train_node2vec(
                &graph,
                &Node2VecOptions {
                    dimensions: 2,
                    walk_length: 1,
                    walks_per_node: 1,
                    window_size: 1,
                    negative_samples: 1,
                    epochs: 1,
                    ..Node2VecOptions::default()
                },
                &EmbeddingControl::new(&algorithm, limits)
            ),
            Err(Node2VecWalkError::Resource(
                EmbeddingResourceError::WorkLimit { .. }
            ))
        ));
    }

    #[test]
    fn negative_sampling_excludes_context_and_handles_no_remaining_mass() {
        let graph = AdjacencyGraph::with_test_directed_edges(2, &[]);
        let nodes = canonical_nodes(&graph);
        let only_context = HashMap::from([(0, 4)]);
        let only_context = NegativeSamplingTable::new(&nodes, &only_context).unwrap();
        assert_eq!(
            sample_negative(&only_context, 0, 0, 0, 0_u128.to_be_bytes(), 0, 0, 1, 0,).unwrap(),
            None
        );
        let both = HashMap::from([(0, 4), (1, 1)]);
        let both = NegativeSamplingTable::new(&nodes, &both).unwrap();
        assert_eq!(
            sample_negative(&both, 0, 0, 0, 0_u128.to_be_bytes(), 0, 0, 1, 0).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn negative_sampling_table_reuses_canonical_masses_without_context() {
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
            &[
                3_u128.to_be_bytes(),
                1_u128.to_be_bytes(),
                2_u128.to_be_bytes(),
            ],
            &[],
        );
        let nodes = canonical_nodes(&graph);
        let table =
            NegativeSamplingTable::new(&nodes, &HashMap::from([(0, 8), (1, 1), (2, 27)])).unwrap();
        assert_eq!(
            table
                .masses
                .iter()
                .map(|(node, _)| *node)
                .collect::<Vec<_>>(),
            [1, 2, 0]
        );
        assert!(table.context_mass(2) > 0.0);
        assert_eq!(table.context_mass(99), 0.0);
        assert!(
            sample_negative(&table, 1, 7, 0, 3_u128.to_be_bytes(), 0, 0, 1, 0)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn walk_task_chunks_are_stable() {
        assert_eq!(walk_task_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(walk_task_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(walk_task_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(walk_task_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
        assert_eq!(walk_task_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    fn token_count_merge_is_canonical_and_overflow_checked() {
        let mut into = HashMap::from([(2, 3_u64), (1, 1)]);
        merge_token_counts(&mut into, HashMap::from([(1, 4), (3, 2)])).unwrap();
        assert_eq!(into, HashMap::from([(1, 5), (2, 3), (3, 2)]));
        let err = merge_token_counts(&mut into, HashMap::from([(1, u64::MAX)]));
        assert_eq!(err, Err(Node2VecWalkError::TokenCountOverflow));
    }

    #[test]
    fn small_work_and_one_thread_select_serial_walk_path() {
        let small = select_walk_corpus_path(&embedding_control(&control_with_threads(4)), 8, 64);
        assert_eq!(small, WalkCorpusPath::Serial);
        let one = select_walk_corpus_path(
            &embedding_control(&control_with_threads(1)),
            64,
            NODE2VEC_WALK_PARALLEL_CROSSOVER,
        );
        assert_eq!(one, WalkCorpusPath::Serial);
        let large = select_walk_corpus_path(
            &embedding_control(&control_with_threads(4)),
            64,
            NODE2VEC_WALK_PARALLEL_CROSSOVER,
        );
        assert!(matches!(
            large,
            WalkCorpusPath::Parallel {
                threads: 4,
                chunks: 4
            }
        ));
    }

    #[test]
    fn thread_matrix_preserves_walk_corpus_and_embedding_fingerprints() {
        let graph = adversarial_graph(24);
        let options = Node2VecOptions {
            dimensions: 4,
            walk_length: 6,
            walks_per_node: 4,
            window_size: 2,
            negative_samples: 2,
            epochs: 1,
            seed: 11,
            ..Node2VecOptions::default()
        };
        let serial = control_with_threads(1);
        let serial_corpus = corpus_with_control(&graph, &options, &serial).unwrap();
        let serial_embedding =
            train_node2vec(&graph, &options, &embedding_control(&serial)).unwrap();
        let estimated = estimated_walk_transitions(24, options.walks_per_node, options.walk_length);
        assert!(estimated >= NODE2VEC_WALK_PARALLEL_CROSSOVER);
        for threads in [2_usize, 4, 8] {
            let parallel = control_with_threads(threads);
            assert!(matches!(
                select_walk_corpus_path(
                    &embedding_control(&parallel),
                    24 * options.walks_per_node,
                    estimated
                ),
                WalkCorpusPath::Parallel { .. }
            ));
            assert_eq!(
                corpus_with_control(&graph, &options, &parallel).unwrap(),
                serial_corpus
            );
            assert_eq!(
                train_node2vec(&graph, &options, &embedding_control(&parallel)).unwrap(),
                serial_embedding
            );
        }
    }

    #[test]
    fn parallel_cancellation_and_work_limits_leave_no_partial_corpus() {
        let graph = adversarial_graph(32);
        let options = Node2VecOptions {
            walk_length: 4,
            walks_per_node: 4,
            seed: 9,
            ..Node2VecOptions::default()
        };
        let pool = Arc::new(ComputePool::new(4).unwrap());
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            cancellation,
        )
        .with_compute_pool(pool.clone());
        assert!(matches!(
            corpus_with_control(&graph, &options, &cancelled),
            Err(Node2VecWalkError::Resource(
                EmbeddingResourceError::Algorithm(_)
            ))
        ));

        let limited = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool);
        let control = EmbeddingControl::new(
            &limited,
            EmbeddingResourceLimits {
                work: 3,
                ..EmbeddingResourceLimits::default()
            },
        );
        assert!(matches!(
            build_walk_corpus(&graph, &options, &control),
            Err(Node2VecWalkError::Resource(
                EmbeddingResourceError::WorkLimit { .. }
            ))
        ));
    }

    #[test]
    fn weighted_and_parallel_edge_fixtures_match_across_thread_counts() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            6,
            &[
                (0, 1),
                (0, 1),
                (1, 2),
                (2, 3),
                (3, 4),
                (4, 5),
                (5, 0),
                (1, 0),
            ],
        )
        .with_test_edge_weights(&[1.0, 2.5, 0.5, 3.0, 1.25, 0.75, 4.0, 1.5]);
        let options = Node2VecOptions {
            walk_length: 8,
            walks_per_node: 8,
            p: 0.5,
            q: 2.0,
            seed: 13,
            ..Node2VecOptions::default()
        };
        let estimated = estimated_walk_transitions(6, options.walks_per_node, options.walk_length);
        assert!(estimated >= NODE2VEC_WALK_PARALLEL_CROSSOVER);
        let serial = corpus_with_control(&graph, &options, &control_with_threads(1)).unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel = control_with_threads(threads);
            assert!(matches!(
                select_walk_corpus_path(
                    &embedding_control(&parallel),
                    6 * options.walks_per_node,
                    estimated
                ),
                WalkCorpusPath::Parallel { .. }
            ));
            assert_eq!(
                corpus_with_control(&graph, &options, &parallel).unwrap(),
                serial
            );
        }
    }

    fn corpus_training(
        graph: &AdjacencyGraph,
        options: &Node2VecOptions,
    ) -> Result<Vec<Node2VecEmbeddingRow>, Node2VecWalkError> {
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
        train_node2vec(graph, options, &EmbeddingControl::new(&algorithm, limits))
    }
}
