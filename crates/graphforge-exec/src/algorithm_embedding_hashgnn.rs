//! Deterministic HashGNN v1 binary minhash propagation.
//!
//! Propagation (#561) may partition independent node updates across the
//! instance-owned private compute pool above a documented crossover. Each node
//! still evaluates samples, self candidates, neighbor candidates, and ties in
//! canonical serial order; worker outputs merge by public UUID node order so
//! embedding fingerprints remain identical to the one-thread path.
#![allow(
    dead_code,
    reason = "the parent-owned HashGNN dispatch integration follows this isolated kernel"
)]

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use graphforge_core::embedding_options::HashGnnOptions;
use rayon::prelude::*;

use crate::algorithm_embedding_control::{EmbeddingControl, EmbeddingResourceError};
use crate::algorithm_embedding_output::EmbeddingOutputRow;
use crate::algorithm_embedding_rng::{EmbeddingRng, EmbeddingRngField};
use crate::algorithm_graph::AdjacencyGraph;

const ZERO_UUID: [u8; 16] = [0; 16];

/// Candidate evaluations below which HashGNN propagation stays serial (#561).
///
/// The unit is an upper-bound count of minhash candidate comparisons per
/// propagation iteration: `active_bits^2 * (nodes + adjacency_entries)`.
/// Smaller fixtures avoid Rayon install/merge tax; larger workloads can split
/// node-owned updates while preserving every per-node comparison order.
pub const HASHGNN_PROPAGATE_PARALLEL_CROSSOVER: u64 = 4_096;

/// Selected propagation execution path for observability and crossover tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HashGnnPropagationPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Explicit canonical UTF-8 type tokens resolved before heterogeneous dispatch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct HashGnnTypeTokens {
    pub(crate) nodes: BTreeMap<[u8; 16], String>,
    pub(crate) relationships: BTreeMap<[u8; 16], String>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HashGnnError {
    #[error("hashgnn dimensions must be greater than zero")]
    ZeroDimensions,
    #[error("hashgnn iterations must be greater than zero")]
    ZeroIterations,
    #[error("hashgnn embedding_density must be finite and in (0, 1]")]
    InvalidDensity,
    #[error("hashgnn active-bit count exceeds dimensions")]
    InvalidActiveBits,
    #[error("heterogeneous hashgnn is missing a node type for UUID {0:?}")]
    MissingNodeType([u8; 16]),
    #[error("heterogeneous hashgnn is missing a relationship type for UUID {0:?}")]
    MissingRelationshipType([u8; 16]),
    #[error("homogeneous hashgnn does not accept type tokens")]
    UnexpectedTypeTokens,
    #[error("hashgnn graph contains a node without UUID identity")]
    MissingNodeIdentity,
    #[error("hashgnn propagation worker panicked")]
    WorkerPanic,
    #[error(transparent)]
    Resource(#[from] EmbeddingResourceError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum CandidateRole {
    Edge,
    SelfNode,
}

impl CandidateRole {
    const fn token(self) -> &'static str {
        match self {
            Self::Edge => "EDGE",
            Self::SelfNode => "SELF",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Candidate {
    priority: u64,
    role: CandidateRole,
    source_uuid: [u8; 16],
    edge_uuid: [u8; 16],
    coordinate: usize,
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            self.priority,
            self.role,
            self.source_uuid,
            self.edge_uuid,
            self.coordinate,
        )
            .cmp(&(
                other.priority,
                other.role,
                other.source_uuid,
                other.edge_uuid,
                other.coordinate,
            ))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Run the pure HashGNN binary propagation kernel.
pub(crate) fn hashgnn_embeddings(
    graph: &AdjacencyGraph,
    options: &HashGnnOptions,
    type_tokens: Option<&HashGnnTypeTokens>,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<EmbeddingOutputRow>, HashGnnError> {
    let active_bits = active_bit_count(options)?;
    validate_type_tokens(graph, options, type_tokens)?;

    let mut nodes = graph
        .node_ids()
        .iter()
        .map(|&node_id| {
            graph
                .node_uuid(node_id)
                .map(|uuid| (uuid, node_id))
                .ok_or(HashGnnError::MissingNodeIdentity)
        })
        .collect::<Result<Vec<_>, _>>()?;
    nodes.sort_unstable();
    let node_indexes = nodes
        .iter()
        .enumerate()
        .map(|(index, &(_, node_id))| (node_id, index))
        .collect::<BTreeMap<_, _>>();
    let words = options.dimensions.div_ceil(64);
    let mut initial = Vec::with_capacity(nodes.len());
    for &(node_uuid, _) in &nodes {
        control.checkpoint(1)?;
        initial.push(initial_code(
            node_uuid,
            node_type(type_tokens, &node_uuid),
            options,
            active_bits,
            words,
        ));
    }

    let prior = propagate(
        graph,
        options,
        type_tokens,
        control,
        &nodes,
        &node_indexes,
        &initial,
    );
    let prior = prior?;

    control.before_publish()?;
    nodes
        .into_iter()
        .enumerate()
        .map(|(index, (node_uuid, _))| {
            control.checkpoint(1)?;
            Ok(EmbeddingOutputRow {
                node_uuid,
                embedding: (0..options.dimensions)
                    .map(|coordinate| {
                        if bit_is_set(&prior[index], coordinate) {
                            1.0
                        } else {
                            0.0
                        }
                    })
                    .collect(),
            })
        })
        .collect()
}

fn propagate(
    graph: &AdjacencyGraph,
    options: &HashGnnOptions,
    type_tokens: Option<&HashGnnTypeTokens>,
    control: &EmbeddingControl<'_>,
    nodes: &[([u8; 16], u64)],
    node_indexes: &BTreeMap<u64, usize>,
    initial: &[Vec<u64>],
) -> Result<Vec<Vec<u64>>, HashGnnError> {
    let active_bits = active_bit_count(options)?;
    let mut prior = initial.to_vec();
    let words = options.dimensions.div_ceil(64);
    let estimated_work =
        estimated_propagation_work(nodes.len(), graph.edge_entry_count(), active_bits);
    let path = select_hashgnn_propagation_path(control, nodes.len(), estimated_work);
    let mut next = vec![vec![0_u64; words]; nodes.len()];
    for iteration in 0..options.iterations {
        control.iteration_checkpoint()?;
        match path {
            HashGnnPropagationPath::Serial => propagate_iteration_serial(
                graph,
                options,
                type_tokens,
                control,
                nodes,
                node_indexes,
                initial,
                &prior,
                iteration,
                active_bits,
                words,
                &mut next,
            )?,
            HashGnnPropagationPath::Parallel { .. } => {
                next = propagate_iteration_parallel(
                    graph,
                    options,
                    type_tokens,
                    control,
                    nodes,
                    node_indexes,
                    initial,
                    &prior,
                    iteration,
                    active_bits,
                    words,
                )?;
            }
        }
        std::mem::swap(&mut prior, &mut next);
    }
    Ok(prior)
}

#[allow(clippy::too_many_arguments)]
fn propagate_iteration_serial(
    graph: &AdjacencyGraph,
    options: &HashGnnOptions,
    type_tokens: Option<&HashGnnTypeTokens>,
    control: &EmbeddingControl<'_>,
    nodes: &[([u8; 16], u64)],
    node_indexes: &BTreeMap<u64, usize>,
    initial: &[Vec<u64>],
    prior: &[Vec<u64>],
    iteration: usize,
    active_bits: usize,
    words: usize,
    next: &mut [Vec<u64>],
) -> Result<(), HashGnnError> {
    for node_index in 0..nodes.len() {
        next[node_index] = propagate_node(
            graph,
            options,
            type_tokens,
            control,
            nodes,
            node_indexes,
            initial,
            prior,
            iteration,
            active_bits,
            words,
            node_index,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn propagate_iteration_parallel(
    graph: &AdjacencyGraph,
    options: &HashGnnOptions,
    type_tokens: Option<&HashGnnTypeTokens>,
    control: &EmbeddingControl<'_>,
    nodes: &[([u8; 16], u64)],
    node_indexes: &BTreeMap<u64, usize>,
    initial: &[Vec<u64>],
    prior: &[Vec<u64>],
    iteration: usize,
    active_bits: usize,
    words: usize,
) -> Result<Vec<Vec<u64>>, HashGnnError> {
    let pool = control.compute_pool().ok_or_else(|| {
        HashGnnError::Resource(EmbeddingResourceError::Algorithm(
            crate::algorithm_dispatch::AlgorithmError::Execution {
                message: "parallel hashgnn propagation requires an instance-owned compute pool"
                    .into(),
            },
        ))
    })?;
    let ranges = node_chunks(nodes.len(), control.compute_threads());
    let chunk_results = run_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut chunk = Vec::with_capacity(end.saturating_sub(start));
                for node_index in start..end {
                    chunk.push(propagate_node(
                        graph,
                        options,
                        type_tokens,
                        control,
                        nodes,
                        node_indexes,
                        initial,
                        prior,
                        iteration,
                        active_bits,
                        words,
                        node_index,
                    )?);
                }
                Ok(chunk)
            })
            .collect::<Vec<Result<Vec<Vec<u64>>, HashGnnError>>>();
        first_chunk_error(results)
    })?;
    Ok(chunk_results.into_iter().flatten().collect())
}

#[allow(clippy::too_many_arguments)]
fn propagate_node(
    graph: &AdjacencyGraph,
    options: &HashGnnOptions,
    type_tokens: Option<&HashGnnTypeTokens>,
    control: &EmbeddingControl<'_>,
    nodes: &[([u8; 16], u64)],
    node_indexes: &BTreeMap<u64, usize>,
    initial: &[Vec<u64>],
    prior: &[Vec<u64>],
    iteration: usize,
    active_bits: usize,
    words: usize,
    node_index: usize,
) -> Result<Vec<u64>, HashGnnError> {
    control.checkpoint(1)?;
    let (node_uuid, node_id) = nodes[node_index];
    let neighbors = graph.neighbors(node_id);
    if neighbors.is_empty() {
        return Ok(initial[node_index].clone());
    }
    let mut output = vec![0_u64; words];
    for sample in 0..active_bits {
        control.checkpoint(1)?;
        let mut selected = None;
        for coordinate in active_coordinates(&prior[node_index], options.dimensions) {
            consider(
                &mut selected,
                candidate(
                    iteration,
                    sample,
                    CandidateRole::SelfNode,
                    node_uuid,
                    ZERO_UUID,
                    coordinate,
                    node_type(type_tokens, &node_uuid),
                    None,
                    options.seed,
                ),
            );
        }
        for edge in neighbors {
            control.checkpoint(1)?;
            let source_uuid = graph
                .node_uuid(edge.neighbor_id)
                .ok_or(HashGnnError::MissingNodeIdentity)?;
            let source_index = node_indexes[&edge.neighbor_id];
            for coordinate in active_coordinates(&prior[source_index], options.dimensions) {
                consider(
                    &mut selected,
                    candidate(
                        iteration,
                        sample,
                        CandidateRole::Edge,
                        source_uuid,
                        edge.edge_uuid,
                        coordinate,
                        node_type(type_tokens, &source_uuid),
                        relationship_type(type_tokens, &edge.edge_uuid),
                        options.seed,
                    ),
                );
            }
        }
        if let Some(candidate) = selected {
            set_bit(&mut output, candidate.coordinate);
        }
    }
    Ok(output)
}

fn run_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, HashGnnError> + Send,
) -> Result<R, HashGnnError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(HashGnnError::WorkerPanic),
    }
}

fn first_chunk_error<T>(results: Vec<Result<T, HashGnnError>>) -> Result<Vec<T>, HashGnnError> {
    results.into_iter().collect()
}

/// Choose serial vs private-pool parallel propagation for a HashGNN workload.
pub(crate) fn select_hashgnn_propagation_path(
    control: &EmbeddingControl<'_>,
    nodes: usize,
    estimated_work: u64,
) -> HashGnnPropagationPath {
    let threads = control.compute_threads();
    if threads <= 1 || nodes <= 1 || estimated_work < HASHGNN_PROPAGATE_PARALLEL_CROSSOVER {
        return HashGnnPropagationPath::Serial;
    }
    if control
        .compute_pool()
        .is_none_or(|pool| !pool.is_parallel())
    {
        return HashGnnPropagationPath::Serial;
    }
    let chunks = node_chunks(nodes, threads).len();
    if chunks <= 1 {
        return HashGnnPropagationPath::Serial;
    }
    HashGnnPropagationPath::Parallel { threads, chunks }
}

fn estimated_propagation_work(nodes: usize, adjacency_entries: u64, active_bits: usize) -> u64 {
    let nodes = u64::try_from(nodes).unwrap_or(u64::MAX);
    let active_bits = u64::try_from(active_bits).unwrap_or(u64::MAX);
    active_bits
        .saturating_mul(active_bits)
        .saturating_mul(nodes.saturating_add(adjacency_entries))
}

fn node_chunks(nodes: usize, threads: usize) -> Vec<(usize, usize)> {
    if nodes == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, nodes);
    let base = nodes / workers;
    let rem = nodes % workers;
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

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "validated HashGNN dimensions are at most 8192 and density is finite in (0, 1]"
)]
fn active_bit_count(options: &HashGnnOptions) -> Result<usize, HashGnnError> {
    if options.dimensions == 0 {
        return Err(HashGnnError::ZeroDimensions);
    }
    if options.iterations == 0 {
        return Err(HashGnnError::ZeroIterations);
    }
    if !options.embedding_density.is_finite()
        || options.embedding_density <= 0.0
        || options.embedding_density > 1.0
    {
        return Err(HashGnnError::InvalidDensity);
    }
    let active = (options.embedding_density * options.dimensions as f64)
        .ceil()
        .max(1.0) as usize;
    if active > options.dimensions {
        return Err(HashGnnError::InvalidActiveBits);
    }
    Ok(active)
}

fn validate_type_tokens(
    graph: &AdjacencyGraph,
    options: &HashGnnOptions,
    type_tokens: Option<&HashGnnTypeTokens>,
) -> Result<(), HashGnnError> {
    if !options.heterogeneous {
        if type_tokens
            .is_some_and(|tokens| !tokens.nodes.is_empty() || !tokens.relationships.is_empty())
        {
            return Err(HashGnnError::UnexpectedTypeTokens);
        }
        return Ok(());
    }
    for node_uuid in graph.node_uuids() {
        if !type_tokens.is_some_and(|tokens| tokens.nodes.contains_key(&node_uuid)) {
            return Err(HashGnnError::MissingNodeType(node_uuid));
        }
    }
    for &node_id in graph.node_ids() {
        for edge in graph.neighbors(node_id) {
            if !type_tokens.is_some_and(|tokens| tokens.relationships.contains_key(&edge.edge_uuid))
            {
                return Err(HashGnnError::MissingRelationshipType(edge.edge_uuid));
            }
        }
    }
    Ok(())
}

fn initial_code(
    node_uuid: [u8; 16],
    node_type: Option<&str>,
    options: &HashGnnOptions,
    active_bits: usize,
    words: usize,
) -> Vec<u64> {
    let mut ranked = (0..options.dimensions)
        .map(|coordinate| {
            let mut fields = vec![EmbeddingRngField::Uuid(node_uuid)];
            if let Some(node_type) = node_type {
                fields.push(EmbeddingRngField::Utf8(node_type));
            }
            fields.push(EmbeddingRngField::U64(coordinate as u64));
            let priority =
                EmbeddingRng::derive("hashgnn", "initial-code", options.seed, &fields).next();
            (priority, coordinate)
        })
        .collect::<Vec<_>>();
    ranked.sort_unstable();
    let mut output = vec![0_u64; words];
    for &(_, coordinate) in ranked.iter().take(active_bits) {
        set_bit(&mut output, coordinate);
    }
    output
}

#[allow(clippy::too_many_arguments)]
fn candidate(
    iteration: usize,
    sample: usize,
    role: CandidateRole,
    source_uuid: [u8; 16],
    edge_uuid: [u8; 16],
    coordinate: usize,
    node_type: Option<&str>,
    relationship_type: Option<&str>,
    seed: u64,
) -> Candidate {
    let mut fields = vec![
        EmbeddingRngField::U64(iteration as u64),
        EmbeddingRngField::U64(sample as u64),
        EmbeddingRngField::Utf8(role.token()),
        EmbeddingRngField::Uuid(source_uuid),
        EmbeddingRngField::Uuid(edge_uuid),
    ];
    if let Some(node_type) = node_type {
        fields.push(EmbeddingRngField::Utf8(node_type));
    }
    if let Some(relationship_type) = relationship_type {
        fields.push(EmbeddingRngField::Utf8(relationship_type));
    }
    fields.push(EmbeddingRngField::U64(coordinate as u64));
    let priority = EmbeddingRng::derive("hashgnn", "minhash-select", seed, &fields).next();
    Candidate {
        priority,
        role,
        source_uuid,
        edge_uuid,
        coordinate,
    }
}

fn consider(selected: &mut Option<Candidate>, candidate: Candidate) {
    if selected.is_none_or(|current| candidate < current) {
        *selected = Some(candidate);
    }
}

fn node_type<'a>(tokens: Option<&'a HashGnnTypeTokens>, uuid: &[u8; 16]) -> Option<&'a str> {
    tokens?.nodes.get(uuid).map(String::as_str)
}

fn relationship_type<'a>(
    tokens: Option<&'a HashGnnTypeTokens>,
    uuid: &[u8; 16],
) -> Option<&'a str> {
    tokens?.relationships.get(uuid).map(String::as_str)
}

fn active_coordinates(words: &[u64], dimensions: usize) -> impl Iterator<Item = usize> + '_ {
    (0..dimensions).filter(|&coordinate| bit_is_set(words, coordinate))
}

fn bit_is_set(words: &[u64], coordinate: usize) -> bool {
    words[coordinate / 64] & (1_u64 << (coordinate % 64)) != 0
}

fn set_bit(words: &mut [u64], coordinate: usize) {
    words[coordinate / 64] |= 1_u64 << (coordinate % 64);
}

#[cfg(test)]
mod tests {
    use graphforge_core::embedding_options::HashGnnOptions;

    use super::*;
    use crate::algorithm_dispatch::{
        AlgorithmCancellation, AlgorithmControl, AlgorithmError, AlgorithmLimits,
    };
    use crate::algorithm_embedding_control::EmbeddingResourceLimits;
    use crate::algorithm_graph::{ResolvedGraphEdge, ResolvedGraphProjection};
    use crate::compute_pool::ComputePool;
    use std::sync::Arc;

    fn options() -> HashGnnOptions {
        HashGnnOptions {
            dimensions: 8,
            iterations: 2,
            embedding_density: 0.25,
            heterogeneous: false,
            node_type_property: None,
            relationship_type_property: None,
            seed: 0,
        }
    }

    fn projection(
        directed: bool,
        nodes: &[[u8; 16]],
        edges: &[([u8; 16], [u8; 16], [u8; 16])],
    ) -> AdjacencyGraph {
        AdjacencyGraph::from_resolved_projection(ResolvedGraphProjection {
            directed,
            nodes: nodes.to_vec(),
            edges: edges
                .iter()
                .map(|&(edge_uuid, source_uuid, target_uuid)| ResolvedGraphEdge {
                    edge_uuid,
                    source_uuid,
                    target_uuid,
                    weight: 1.0,
                })
                .collect(),
        })
        .unwrap()
    }

    fn run(
        graph: &AdjacencyGraph,
        options: &HashGnnOptions,
        types: Option<&HashGnnTypeTokens>,
    ) -> Result<Vec<EmbeddingOutputRow>, HashGnnError> {
        let algorithm =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let control = EmbeddingControl::new(&algorithm, EmbeddingResourceLimits::default());
        hashgnn_embeddings(graph, options, types, &control)
    }

    fn run_with_threads(
        graph: &AdjacencyGraph,
        options: &HashGnnOptions,
        threads: usize,
    ) -> Result<Vec<EmbeddingOutputRow>, HashGnnError> {
        let pool = Arc::new(ComputePool::new(threads).unwrap());
        let algorithm = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool);
        let control = EmbeddingControl::new(&algorithm, EmbeddingResourceLimits::default());
        hashgnn_embeddings(graph, options, None, &control)
    }

    fn bits(rows: &[EmbeddingOutputRow]) -> Vec<([u8; 16], Vec<u32>)> {
        rows.iter()
            .map(|row| {
                (
                    row.node_uuid,
                    row.embedding.iter().map(|value| value.to_bits()).collect(),
                )
            })
            .collect()
    }

    fn active_positions(rows: &[EmbeddingOutputRow]) -> Vec<([u8; 16], Vec<usize>)> {
        rows.iter()
            .map(|row| {
                (
                    row.node_uuid,
                    row.embedding
                        .iter()
                        .enumerate()
                        .filter_map(|(index, &value)| (value == 1.0).then_some(index))
                        .collect(),
                )
            })
            .collect()
    }

    fn ring_graph(nodes: usize) -> AdjacencyGraph {
        let node_uuids = (0..nodes)
            .map(|node| (node as u128 + 1).to_be_bytes())
            .collect::<Vec<_>>();
        let edges = (0..nodes)
            .map(|source| {
                (
                    (1000_u128 + source as u128).to_be_bytes(),
                    node_uuids[source],
                    node_uuids[(source + 1) % nodes],
                )
            })
            .collect::<Vec<_>>();
        projection(true, &node_uuids, &edges)
    }

    #[test]
    fn initial_code_sets_exact_k_smallest_priorities() {
        let node = 7_u128.to_be_bytes();
        let mut value = options();
        value.dimensions = 9;
        value.embedding_density = 0.34;
        let code = initial_code(node, None, &value, 4, 1);
        let embedding = (0..value.dimensions)
            .map(|coordinate| {
                if bit_is_set(&code, coordinate) {
                    1.0
                } else {
                    0.0
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(embedding, vec![1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0]);
    }

    #[test]
    fn propagation_path_respects_crossover_threads_and_pool() {
        let serial_algorithm =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let serial_control =
            EmbeddingControl::new(&serial_algorithm, EmbeddingResourceLimits::default());
        assert_eq!(
            select_hashgnn_propagation_path(
                &serial_control,
                8,
                HASHGNN_PROPAGATE_PARALLEL_CROSSOVER
            ),
            HashGnnPropagationPath::Serial
        );

        let pool = Arc::new(ComputePool::new(4).unwrap());
        let parallel_algorithm = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool);
        let parallel_control =
            EmbeddingControl::new(&parallel_algorithm, EmbeddingResourceLimits::default());
        assert_eq!(
            select_hashgnn_propagation_path(
                &parallel_control,
                8,
                HASHGNN_PROPAGATE_PARALLEL_CROSSOVER - 1
            ),
            HashGnnPropagationPath::Serial
        );
        assert_eq!(
            select_hashgnn_propagation_path(
                &parallel_control,
                8,
                HASHGNN_PROPAGATE_PARALLEL_CROSSOVER
            ),
            HashGnnPropagationPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn thread_matrix_preserves_hashgnn_embedding_fingerprint() {
        let graph = ring_graph(16);
        let value = HashGnnOptions {
            dimensions: 128,
            iterations: 2,
            embedding_density: 0.25,
            seed: 99,
            ..options()
        };
        let serial = bits(&run_with_threads(&graph, &value, 1).unwrap());
        for threads in [2, 4, 8] {
            assert_eq!(
                bits(&run_with_threads(&graph, &value, threads).unwrap()),
                serial,
                "threads={threads}"
            );
        }
    }

    #[test]
    fn zero_iterations_are_rejected_before_initialization() {
        let graph = projection(true, &[1_u128.to_be_bytes()], &[]);
        assert_eq!(
            run(
                &graph,
                &HashGnnOptions {
                    iterations: 0,
                    ..options()
                },
                None
            ),
            Err(HashGnnError::ZeroIterations)
        );
    }

    #[test]
    fn minhash_replacement_collision_and_iterations_are_exact() {
        let a = 1_u128.to_be_bytes();
        let b = 2_u128.to_be_bytes();
        let edge = 11_u128.to_be_bytes();
        let graph = projection(true, &[a, b], &[(edge, a, b)]);
        let once = run(
            &graph,
            &HashGnnOptions {
                iterations: 1,
                ..options()
            },
            None,
        )
        .unwrap();
        let twice = run(&graph, &options(), None).unwrap();
        assert_eq!(
            bits(&once),
            vec![
                (a, vec![0, 0, 0, 0, 0, 1.0_f32.to_bits(), 0, 0]),
                (
                    b,
                    vec![0, 0, 1.0_f32.to_bits(), 0, 0, 1.0_f32.to_bits(), 0, 0]
                ),
            ]
        );
        assert_eq!(
            bits(&twice),
            vec![
                (a, vec![0, 0, 0, 0, 0, 1.0_f32.to_bits(), 0, 0]),
                (
                    b,
                    vec![0, 0, 1.0_f32.to_bits(), 0, 0, 1.0_f32.to_bits(), 0, 0]
                ),
            ]
        );
        assert_eq!(
            once[0]
                .embedding
                .iter()
                .filter(|&&value| value == 1.0)
                .count(),
            1
        );
    }

    #[test]
    fn candidate_ties_use_role_source_edge_and_coordinate_order() {
        let priority = 5;
        let node = 2_u128.to_be_bytes();
        let edge = 3_u128.to_be_bytes();
        let mut selected = None;
        for candidate in [
            Candidate {
                priority,
                role: CandidateRole::SelfNode,
                source_uuid: node,
                edge_uuid: ZERO_UUID,
                coordinate: 4,
            },
            Candidate {
                priority,
                role: CandidateRole::Edge,
                source_uuid: node,
                edge_uuid: edge,
                coordinate: 7,
            },
            Candidate {
                priority,
                role: CandidateRole::Edge,
                source_uuid: node,
                edge_uuid: edge,
                coordinate: 3,
            },
        ] {
            consider(&mut selected, candidate);
        }
        let selected = selected.unwrap();
        assert_eq!(selected.role, CandidateRole::Edge);
        assert_eq!(selected.coordinate, 3);
    }

    #[test]
    fn direction_parallel_edges_loops_and_isolates_are_canonical() {
        let a = 1_u128.to_be_bytes();
        let b = 2_u128.to_be_bytes();
        let isolate = 3_u128.to_be_bytes();
        let edges = [
            (10_u128.to_be_bytes(), a, b),
            (11_u128.to_be_bytes(), a, b),
            (12_u128.to_be_bytes(), a, a),
        ];
        let topology_options = HashGnnOptions {
            dimensions: 64,
            embedding_density: 0.25,
            seed: 17,
            ..options()
        };
        let directed = run(
            &projection(true, &[a, b, isolate], &edges),
            &topology_options,
            None,
        )
        .unwrap();
        let undirected = run(
            &projection(false, &[a, b, isolate], &edges),
            &topology_options,
            None,
        )
        .unwrap();
        assert_eq!(
            active_positions(&directed),
            vec![
                (a, vec![1, 6, 13, 15, 23, 31, 34, 40, 44, 53]),
                (
                    b,
                    vec![1, 6, 13, 16, 17, 21, 23, 31, 33, 34, 36, 40, 41, 42, 48, 53]
                ),
                (
                    isolate,
                    vec![
                        0, 10, 18, 25, 26, 32, 33, 34, 37, 38, 42, 45, 49, 54, 56, 57
                    ]
                ),
            ]
        );
        assert_ne!(bits(&directed), bits(&undirected));
        let single_edge = run(
            &projection(true, &[a, b, isolate], &edges[..1]),
            &topology_options,
            None,
        )
        .unwrap();
        let no_loop = run(
            &projection(true, &[a, b, isolate], &edges[..2]),
            &topology_options,
            None,
        )
        .unwrap();
        assert_ne!(bits(&directed), bits(&single_edge));
        assert_ne!(bits(&directed), bits(&no_loop));
        let initial = initial_code(
            isolate,
            None,
            &topology_options,
            16,
            topology_options.dimensions.div_ceil(64),
        );
        assert_eq!(
            directed[2].embedding,
            (0..topology_options.dimensions)
                .map(|coordinate| if bit_is_set(&initial, coordinate) {
                    1.0
                } else {
                    0.0
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn heterogeneous_type_bytes_change_hashes_and_are_required() {
        let a = 1_u128.to_be_bytes();
        let b = 2_u128.to_be_bytes();
        let edge = 5_u128.to_be_bytes();
        let graph = projection(true, &[a, b], &[(edge, a, b)]);
        let value = HashGnnOptions {
            heterogeneous: true,
            node_type_property: Some("kind".into()),
            relationship_type_property: Some("relation".into()),
            ..options()
        };
        assert!(matches!(
            run(&graph, &value, None),
            Err(HashGnnError::MissingNodeType(uuid)) if uuid == a
        ));
        let types = HashGnnTypeTokens {
            nodes: BTreeMap::from([
                (a, "string:6:person".to_owned()),
                (b, "integer:7".to_owned()),
            ]),
            relationships: BTreeMap::from([(edge, "string:5:knows".to_owned())]),
        };
        let changed = HashGnnTypeTokens {
            nodes: BTreeMap::from([
                (a, "string:6:person".to_owned()),
                (b, "integer:8".to_owned()),
            ]),
            relationships: BTreeMap::from([(edge, "string:5:knows".to_owned())]),
        };
        assert_ne!(
            bits(&run(&graph, &value, Some(&types)).unwrap()),
            bits(&run(&graph, &value, Some(&changed)).unwrap())
        );
        let missing_relationship = HashGnnTypeTokens {
            nodes: types.nodes.clone(),
            relationships: BTreeMap::new(),
        };
        assert!(matches!(
            run(&graph, &value, Some(&missing_relationship)),
            Err(HashGnnError::MissingRelationshipType(uuid)) if uuid == edge
        ));
        assert!(matches!(
            run(&graph, &options(), Some(&types)),
            Err(HashGnnError::UnexpectedTypeTokens)
        ));
    }

    #[test]
    fn replay_seed_and_public_uuid_order_are_deterministic() {
        let a = 10_u128.to_be_bytes();
        let b = 20_u128.to_be_bytes();
        let edge = 30_u128.to_be_bytes();
        let left = projection(true, &[a, b], &[(edge, a, b)]);
        let right = projection(true, &[b, a], &[(edge, a, b)]);
        let first = run(&left, &options(), None).unwrap();
        assert_eq!(bits(&first), bits(&run(&left, &options(), None).unwrap()));
        assert_eq!(bits(&first), bits(&run(&right, &options(), None).unwrap()));
        assert_ne!(
            bits(&first),
            bits(
                &run(
                    &left,
                    &HashGnnOptions {
                        seed: 1,
                        ..options()
                    },
                    None
                )
                .unwrap()
            )
        );
        assert!(first.iter().all(|row| {
            row.embedding
                .iter()
                .all(|value| value.is_finite() && (*value == 0.0 || *value == 1.0))
        }));
    }

    #[test]
    fn cancellation_and_work_limits_fail_at_kernel_checkpoints() {
        let graph = projection(true, &[1_u128.to_be_bytes()], &[]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let algorithm = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        let control = EmbeddingControl::new(&algorithm, EmbeddingResourceLimits::default());
        assert!(matches!(
            hashgnn_embeddings(&graph, &options(), None, &control),
            Err(HashGnnError::Resource(EmbeddingResourceError::Algorithm(_)))
        ));

        let algorithm =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let control = EmbeddingControl::new(
            &algorithm,
            EmbeddingResourceLimits {
                memory_bytes: u64::MAX,
                work: 1,
            },
        );
        assert!(matches!(
            hashgnn_embeddings(&graph, &options(), None, &control),
            Err(HashGnnError::Resource(
                EmbeddingResourceError::WorkLimit { .. }
            ))
        ));

        let algorithm = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        let control = EmbeddingControl::new(&algorithm, EmbeddingResourceLimits::default());
        assert!(matches!(
            hashgnn_embeddings(&graph, &options(), None, &control),
            Err(HashGnnError::Resource(EmbeddingResourceError::Algorithm(
                AlgorithmError::IterationLimit {
                    observed: 2,
                    limit: 1
                }
            )))
        ));
    }
}
