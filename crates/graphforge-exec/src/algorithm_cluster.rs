//! Rust-owned cluster handlers registered under the shared algorithm dispatch contract.
//!
//! Leiden (#527) is an order-sensitive multilevel modularity optimizer. Its
//! local moves, refinement, fixed random stream, and seeded aggregation consume
//! the accepted state from the previous step, so it keeps a serial execution
//! disposition unless a future contract explicitly changes the numeric/tie
//! semantics.
//! Louvain (#528) is an order-sensitive multilevel modularity optimizer. Its
//! local moves mutate community totals and accepted partitions one
//! topology-ordered node at a time, so it keeps a serial execution disposition
//! unless a future contract explicitly changes the numeric/tie semantics.

mod components;
use components::Components;
mod louvain;
use louvain::Louvain;
mod leiden;
use leiden::Leiden;
mod modularity_optimization;
use modularity_optimization::ModularityOptimization;
mod fastgreedy;
use fastgreedy::FastGreedy;
mod girvan_newman;
use girvan_newman::GirvanNewman;
mod infomap;
use infomap::InfoMap;
mod label_propagation;
use label_propagation::LabelPropagation;
mod speaker_listener;
use speaker_listener::SpeakerListener;

use graphforge_value::EntityTypeSelection;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, ClusterAlgorithm};
use graphforge_core::{ClusterOptions, GfError, OntologyMode};
use graphforge_ir::Direction;
use rayon::prelude::*;

use crate::AdjacencyProvider;
use crate::algorithm_cluster_biconnected::biconnected_labels;
use crate::algorithm_cluster_hdbscan::ReachabilityTree;
use crate::algorithm_cluster_kmeans::stable_labels as kmeans_labels;
use crate::algorithm_cluster_max_cut::approximate_max_cut_labels;
use crate::algorithm_cluster_scc::strongly_connected_labels;
use crate::algorithm_cluster_spectral::leading_eigenvector_communities;
use crate::algorithm_cluster_spinglass::spinglass_communities;
use crate::algorithm_cluster_walktrap::walktrap_communities;
use crate::algorithm_dispatch::{
    AlgorithmCancellation, AlgorithmCapability, AlgorithmControl, AlgorithmError, AlgorithmLimits,
    AlgorithmOutput, AlgorithmRegistry, AlgorithmValue, DependencyReview, RustAlgorithm,
};
use crate::algorithm_graph::{
    AdjacencyGraph, AdjacencySelection, export_adjacency, load_node_vectors,
};
use crate::algorithm_k_core::k_core_numbers;
use crate::algorithm_output::shape_algorithm_output;

const BUILTIN_REVIEW: DependencyReview = DependencyReview {
    implementation: "graphforge-exec built-in",
    license: "Apache-2.0",
    maintenance: "GraphForge workspace",
    security: "workspace cargo-deny and CodeQL",
    binary_size: "no additional dependency",
    determinism: "surrogate-ordered traversals, moves, rows, and community IDs",
    platforms: "Rust workspace targets",
};

type WeightedAdjacency = Vec<BTreeMap<usize, f64>>;
type CommunityMembers = Vec<Vec<usize>>;

/// Selected Leiden execution path for #527 disposition evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeidenExecutionPath {
    /// Local moves, refinement, and aggregation remain serial.
    SerialRefinement,
}

/// Selected Louvain execution path for #528 disposition evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LouvainExecutionPath {
    /// Topology-ordered local moves and condensation remain serial.
    SerialLocalMoves,
}

struct LeadingEigenvector;

struct Walktrap;

struct Spinglass;

struct Hdbscan;

struct KMeans;

struct ApproximateMaxKCut;

struct StronglyConnected;

struct Biconnected;

struct KCoreDecomposition;

/// Direction-expanded adjacency entries below which components stays serial (#518).
///
/// The private-pool path builds chunk-local union-find forests and merges them in
/// source order. Keeping small graphs serial avoids Rayon scheduling and local
/// hash-map setup costs; above this measured M4 crossover the independent source
/// scans amortize that overhead while preserving identical component labels.
pub const COMPONENTS_PARALLEL_CROSSOVER_EDGES: u64 = 16_384;
impl RustAlgorithm for KCoreDecomposition {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::KCoreDecomposition),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let cores = k_core_numbers(graph, control)?;
        community_output(graph, &cores, ClusterAlgorithm::KCoreDecomposition, control)
    }
}

impl RustAlgorithm for Biconnected {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::Biconnected),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = biconnected_labels(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::Biconnected, control)
    }
}

impl RustAlgorithm for StronglyConnected {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::StronglyConnected),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = strongly_connected_labels(graph, control)?;
        community_output(
            graph,
            &communities,
            ClusterAlgorithm::StronglyConnected,
            control,
        )
    }
}

impl RustAlgorithm for ApproximateMaxKCut {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::ApproximateMaxKCut),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = approximate_max_cut_labels(graph, control)?;
        community_output(
            graph,
            &communities,
            ClusterAlgorithm::ApproximateMaxKCut,
            control,
        )
    }
}

impl RustAlgorithm for KMeans {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::KMeans),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let vectors = graph
            .node_ids()
            .iter()
            .map(|&node| {
                graph
                    .node_vector(node)
                    .ok_or_else(|| execution("selected node has no validated feature vector"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let labels = kmeans_labels(&vectors, control)?;
        label_output(graph, &labels, ClusterAlgorithm::KMeans, control)
    }
}

impl RustAlgorithm for Hdbscan {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::Hdbscan),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let labels = ReachabilityTree::from_graph(graph, control)?
            .stable_labels(graph.node_ids().len(), control)?;
        label_output(graph, &labels, ClusterAlgorithm::Hdbscan, control)
    }
}

impl RustAlgorithm for Spinglass {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::Spinglass),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = spinglass_communities(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::Spinglass, control)
    }
}

impl RustAlgorithm for Walktrap {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::Walktrap),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = walktrap_communities(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::Walktrap, control)
    }
}

impl RustAlgorithm for LeadingEigenvector {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::LeadingEigenvector),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = leading_eigenvector_communities(graph, control)?;
        community_output(
            graph,
            &communities,
            ClusterAlgorithm::LeadingEigenvector,
            control,
        )
    }
}

fn community_output(
    graph: &AdjacencyGraph,
    communities: &[usize],
    algorithm: ClusterAlgorithm,
    control: &AlgorithmControl,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut sink = control.output_sink(Algorithm::Cluster(algorithm))?;
    let mut work = 0_usize;
    for (index, &node_id) in graph.node_ids().iter().enumerate() {
        checkpoint_chunk(control, &mut work)?;
        let uuid = graph
            .node_uuid(node_id)
            .ok_or_else(|| execution("selected node has no UUID identity"))?;
        let community = i64::try_from(communities[index])
            .map_err(|_| execution("community count exceeds Int64 result range"))?;
        sink.append_row(&[AlgorithmValue::Uuid(uuid), AlgorithmValue::Int64(community)])?;
    }
    sink.finish()
}

fn label_output(
    graph: &AdjacencyGraph,
    labels: &[i64],
    algorithm: ClusterAlgorithm,
    control: &AlgorithmControl,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut sink = control.output_sink(Algorithm::Cluster(algorithm))?;
    let mut work = 0_usize;
    for (&node_id, &community) in graph.node_ids().iter().zip(labels) {
        checkpoint_chunk(control, &mut work)?;
        let uuid = graph
            .node_uuid(node_id)
            .ok_or_else(|| execution("selected node has no UUID identity"))?;
        sink.append_row(&[AlgorithmValue::Uuid(uuid), AlgorithmValue::Int64(community)])?;
    }
    sink.finish()
}

pub(crate) fn register_cluster_algorithms(
    registry: &mut AlgorithmRegistry,
) -> Result<(), AlgorithmError> {
    registry.register(Arc::new(Components))?;
    registry.register(Arc::new(Louvain))?;
    registry.register(Arc::new(Leiden))?;
    registry.register(Arc::new(LabelPropagation))?;
    registry.register(Arc::new(SpeakerListener))?;
    registry.register(Arc::new(GirvanNewman))?;
    registry.register(Arc::new(ModularityOptimization))?;
    registry.register(Arc::new(FastGreedy))?;
    registry.register(Arc::new(InfoMap))?;
    registry.register(Arc::new(LeadingEigenvector))?;
    registry.register(Arc::new(Walktrap))?;
    registry.register(Arc::new(Spinglass))?;
    registry.register(Arc::new(Hdbscan))?;
    registry.register(Arc::new(KMeans))?;
    registry.register(Arc::new(ApproximateMaxKCut))?;
    registry.register(Arc::new(StronglyConnected))?;
    registry.register(Arc::new(Biconnected))?;
    registry.register(Arc::new(KCoreDecomposition))
}

/// Execute a typed cluster algorithm through Rust dispatch and return its
/// canonical UUID-only Arrow batch with node properties materialized.
///
/// # Errors
/// Returns structured validation/execution errors for invalid relationship
/// selection, unavailable algorithms, adjacency reads, limits, or shaping.
pub fn cluster_algorithm(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    property_stems: &[String],
    options: &ClusterOptions,
) -> Result<RecordBatch, GfError> {
    cluster_algorithm_with_limits(
        provider,
        dir,
        mode,
        label,
        property_stems,
        options,
        AlgorithmLimits::default(),
    )
}

/// Execute clustering with an explicit output/memory shaping policy (#341).
pub fn cluster_algorithm_with_limits(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    property_stems: &[String],
    options: &ClusterOptions,
    limits: AlgorithmLimits,
) -> Result<RecordBatch, GfError> {
    cluster_algorithm_with_compute(
        provider,
        dir,
        mode,
        label,
        property_stems,
        options,
        limits,
        None,
    )
}

/// Execute clustering with shaping limits and an optional private compute pool (#518).
/// Execute clustering with shaping limits and an optional private compute pool (#524).
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors cluster_algorithm_with_limits plus the instance compute pool handle"
)]
pub fn cluster_algorithm_with_compute(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    property_stems: &[String],
    options: &ClusterOptions,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<RecordBatch, GfError> {
    let graph = cluster_projection(provider, dir, mode, label, property_stems, options)?;
    let algorithm = Algorithm::Cluster(options.by);
    let output = execute_cluster_with_compute(&graph, algorithm, limits, compute)?;
    let batch = shape_algorithm_output(algorithm, &output)?;
    crate::algorithm_output::materialize_node_properties_with_batch_size(
        dir,
        property_stems,
        &batch,
        limits.batch_size,
    )
    .map_err(Into::into)
}

/// Fingerprint the exact topology and vector values consumed by clustering.
///
/// # Errors
/// Returns the same projection and option failures as [`cluster_algorithm`].
pub fn cluster_projection_fingerprint(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    property_stems: &[String],
    options: &ClusterOptions,
) -> Result<[u8; 32], GfError> {
    cluster_projection(provider, dir, mode, label, property_stems, options)
        .and_then(|graph| graph.descriptor_projection_fingerprint())
        .map(|fingerprint| *fingerprint.as_bytes())
}

fn cluster_projection(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    property_stems: &[String],
    options: &ClusterOptions,
) -> Result<AdjacencyGraph, GfError> {
    let vector_property = options.vector_property.as_deref();
    if let Some(property) = vector_property
        && (property.is_empty()
            || property.trim() != property
            || property.chars().any(char::is_control))
    {
        return Err(GfError::Validation(format!(
            "invalid cluster vector property {property:?}"
        )));
    }
    let vector_algorithm = matches!(
        options.by,
        ClusterAlgorithm::Hdbscan | ClusterAlgorithm::KMeans
    );
    match (vector_algorithm, vector_property) {
        (true, None) => {
            return Err(GfError::Validation(format!(
                "cluster.{} requires vector_property",
                options.by.as_str()
            )));
        }
        (false, Some(_)) => {
            return Err(GfError::Validation(format!(
                "cluster.{} does not accept vector_property",
                options.by.as_str()
            )));
        }
        _ => {}
    }
    if vector_algorithm && options.via.is_some() {
        return Err(GfError::Validation(format!(
            "cluster.{} does not accept via",
            options.by.as_str()
        )));
    }
    let via = options.via.as_deref().unwrap_or("*");
    if via.is_empty() || via.trim() != via || via.chars().any(char::is_control) {
        return Err(GfError::Validation(format!(
            "invalid cluster relationship selector {via:?}"
        )));
    }
    let direction = if options.directed
        && !vector_algorithm
        && !matches!(
            options.by,
            ClusterAlgorithm::Louvain
                | ClusterAlgorithm::Leiden
                | ClusterAlgorithm::LabelPropagation
                | ClusterAlgorithm::SpeakerListener
                | ClusterAlgorithm::GirvanNewman
                | ClusterAlgorithm::ModularityOptimization
                | ClusterAlgorithm::FastGreedy
                | ClusterAlgorithm::Spinglass
                | ClusterAlgorithm::ApproximateMaxKCut
                | ClusterAlgorithm::Biconnected
                | ClusterAlgorithm::KCoreDecomposition
        ) {
        Direction::Out
    } else {
        Direction::Undirected
    };
    let mut graph = export_adjacency(
        provider,
        dir,
        mode,
        AdjacencySelection {
            label,
            via,
            direction,
            weight: None,
        },
    )?;
    if let Some(property) = vector_property {
        load_node_vectors(&mut graph, dir, property_stems, property)?;
    }
    Ok(graph)
}

#[cfg(test)]
fn execute_cluster(
    graph: &AdjacencyGraph,
    algorithm: Algorithm,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute_cluster_with_compute(graph, algorithm, limits, None)
}

fn execute_cluster_with_compute(
    graph: &AdjacencyGraph,
    algorithm: Algorithm,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_cluster_algorithms(&mut registry)?;
    let mut control = AlgorithmControl::new(limits, AlgorithmCancellation::default());
    if let Some(pool) = compute {
        control = control.with_compute_pool(pool);
    }
    registry.execute(algorithm, graph, &control)
}

pub(crate) fn select_louvain_path(
    _control: &AlgorithmControl,
    _node_count: usize,
    _edge_count: u64,
) -> LouvainExecutionPath {
    LouvainExecutionPath::SerialLocalMoves
}

fn normalized_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<(WeightedAdjacency, CommunityMembers), AlgorithmError> {
    normalized_communities_with_progress(graph, control, |_| {})
}

fn normalized_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    mut progress: impl FnMut(usize),
) -> Result<(WeightedAdjacency, CommunityMembers), AlgorithmError> {
    let node_count = graph.node_ids().len();
    let mut indices = HashMap::with_capacity(node_count);
    let mut work = 0_usize;
    for (index, &node_id) in graph.node_ids().iter().enumerate() {
        checkpoint_chunk(control, &mut work)?;
        indices.insert(node_id, index);
    }
    let mut edges = BTreeSet::new();
    let mut observed = 0_usize;
    for (source, &node_id) in graph.node_ids().iter().enumerate() {
        for edge in graph.neighbors(node_id) {
            checkpoint_chunk(control, &mut observed)?;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            if source != target {
                edges.insert((source.min(target), source.max(target)));
            }
            if observed.is_multiple_of(16_384) {
                progress(observed);
            }
        }
    }

    let mut weights = vec![BTreeMap::new(); node_count];
    work = 0;
    for (left, right) in edges {
        checkpoint_chunk(control, &mut work)?;
        weights[left].insert(right, 1.0);
        weights[right].insert(left, 1.0);
    }
    let mut members = Vec::with_capacity(node_count);
    for node in 0..node_count {
        checkpoint_chunk(control, &mut work)?;
        members.push(vec![node]);
    }
    Ok((weights, members))
}

fn local_moves_from(
    weights: &[BTreeMap<usize, f64>],
    initial: Option<&[usize]>,
    name: &str,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut assignment = initial.map_or_else(|| (0..weights.len()).collect(), <[_]>::to_vec);
    if assignment.len() != weights.len()
        || assignment
            .iter()
            .any(|&community| community >= weights.len())
    {
        return Err(execution("invalid internal community partition"));
    }
    let mut degrees = Vec::with_capacity(weights.len());
    let mut total_weight = 0.0;
    let mut work = 0_usize;
    for neighbors in weights {
        let mut degree = 0.0;
        for weight in neighbors.values() {
            checkpoint_chunk(control, &mut work)?;
            degree += weight;
        }
        total_weight += degree;
        degrees.push(degree);
    }
    if !total_weight.is_finite() {
        return Err(execution(&format!(
            "{name} total edge weight is not finite"
        )));
    }
    if total_weight == 0.0 {
        return Ok(assignment);
    }
    let mut totals = vec![0.0; weights.len()];
    for (node, &community) in assignment.iter().enumerate() {
        totals[community] += degrees[node];
    }

    loop {
        control.checkpoint()?;
        let mut moved = false;
        work = 0;
        for node in 0..weights.len() {
            checkpoint_chunk(control, &mut work)?;
            let old = assignment[node];
            totals[old] -= degrees[node];
            let mut by_community = BTreeMap::new();
            for (&neighbor, &weight) in &weights[node] {
                checkpoint_chunk(control, &mut work)?;
                if neighbor != node {
                    *by_community.entry(assignment[neighbor]).or_insert(0.0) += weight;
                }
            }
            let mut best = old;
            let mut best_gain = 0.0;
            for (candidate, internal_weight) in by_community {
                checkpoint_chunk(control, &mut work)?;
                let gain = internal_weight - degrees[node] * totals[candidate] / total_weight;
                if !gain.is_finite() {
                    return Err(execution(&format!("{name} modularity gain is not finite")));
                }
                if gain > best_gain + 1e-12
                    || ((gain - best_gain).abs() <= 1e-12 && gain > 1e-12 && candidate < best)
                {
                    best = candidate;
                    best_gain = gain;
                }
            }
            assignment[node] = best;
            totals[best] += degrees[node];
            moved |= best != old;
        }
        if !moved {
            break;
        }
    }

    canonicalize_partition(&mut assignment, control)?;
    Ok(assignment)
}

pub(crate) fn select_leiden_path(
    _control: &AlgorithmControl,
    _node_count: usize,
    _edge_count: u64,
) -> LeidenExecutionPath {
    LeidenExecutionPath::SerialRefinement
}

fn partition_modularity(
    original: &WeightedAdjacency,
    partition: &[usize],
    name: &str,
    control: &AlgorithmControl,
) -> Result<f64, AlgorithmError> {
    let mut total = 0.0;
    let mut degrees = vec![0.0; original.len()];
    let mut internal = vec![0.0; original.len()];
    let mut community_degree = vec![0.0; original.len()];
    let mut work = 0_usize;
    for (node, neighbors) in original.iter().enumerate() {
        for (&neighbor, &weight) in neighbors {
            checkpoint_chunk(control, &mut work)?;
            degrees[node] += weight;
            total += weight;
            if partition[node] == partition[neighbor] {
                internal[partition[node]] += weight;
            }
        }
        community_degree[partition[node]] += degrees[node];
    }
    if total == 0.0 {
        return Ok(0.0);
    }
    let score = internal
        .iter()
        .zip(community_degree)
        .map(|(inside, degree)| inside / total - (degree / total).powi(2))
        .sum::<f64>();
    score
        .is_finite()
        .then_some(score)
        .ok_or_else(|| execution(&format!("{name} modularity is not finite")))
}

fn shuffle(
    order: &mut [usize],
    random: &mut u64,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    for end in (1..order.len()).rev() {
        checkpoint_chunk(control, work)?;
        order.swap(end, random_index(random, end + 1)?);
    }
    Ok(())
}

fn random_index(random: &mut u64, upper: usize) -> Result<usize, AlgorithmError> {
    let upper = u64::try_from(upper).map_err(|_| execution("label choice exceeds UInt64 range"))?;
    let threshold = upper.wrapping_neg() % upper;
    loop {
        let value = next_random(random);
        if value >= threshold {
            return usize::try_from(value % upper)
                .map_err(|_| execution("label choice exceeds platform range"));
        }
    }
}

fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    value
}

fn canonicalize_partition(
    assignment: &mut [usize],
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    let mut ids = BTreeMap::new();
    let mut work = 0;
    for community in assignment {
        checkpoint_chunk(control, &mut work)?;
        let next = ids.len();
        *community = *ids.entry(*community).or_insert(next);
    }
    Ok(())
}

fn condense(
    weights: &[BTreeMap<usize, f64>],
    members: &[Vec<usize>],
    assignment: &[usize],
    count: usize,
    control: &AlgorithmControl,
) -> Result<(WeightedAdjacency, CommunityMembers), AlgorithmError> {
    let mut next_weights = vec![BTreeMap::new(); count];
    let mut next_members = vec![Vec::new(); count];
    let mut work = 0_usize;
    for node in 0..weights.len() {
        checkpoint_chunk(control, &mut work)?;
        next_members[assignment[node]].extend_from_slice(&members[node]);
        for (&neighbor, &weight) in &weights[node] {
            checkpoint_chunk(control, &mut work)?;
            let condensed = next_weights[assignment[node]]
                .entry(assignment[neighbor])
                .or_insert(0.0);
            *condensed += weight;
            if !condensed.is_finite() {
                return Err(execution("Louvain condensed edge weight is not finite"));
            }
        }
    }
    Ok((next_weights, next_members))
}

fn execution(message: &str) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

fn checkpoint_chunk(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    if work.is_multiple_of(16_384) {
        control.checkpoint()?;
    }
    *work += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    pub(super) fn execute_louvain(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(graph, Algorithm::Cluster(ClusterAlgorithm::Louvain), limits)
    }

    use super::*;

    fn execute_leading_eigenvector(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::LeadingEigenvector),
            limits,
        )
    }

    fn execute_walktrap(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::Walktrap),
            limits,
        )
    }

    fn execute_spinglass(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::Spinglass),
            limits,
        )
    }

    fn execute_approximate_max_cut(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::ApproximateMaxKCut),
            limits,
        )
    }

    fn execute_strongly_connected(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::StronglyConnected),
            limits,
        )
    }

    fn execute_biconnected(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::Biconnected),
            limits,
        )
    }

    fn execute_k_core_decomposition(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::KCoreDecomposition),
            limits,
        )
    }

    fn execute_k_core_decomposition_with_compute_threads(
        graph: &AdjacencyGraph,
        threads: usize,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(threads).unwrap()));
        registry.execute(
            Algorithm::Cluster(ClusterAlgorithm::KCoreDecomposition),
            graph,
            &control,
        )
    }

    pub(super) fn community_ids(output: &AlgorithmOutput) -> Vec<i64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Int64(value) => value,
                _ => panic!("expected Int64 community id"),
            })
            .collect()
    }

    fn output_fingerprint(output: &AlgorithmOutput) -> String {
        format!("{:?}|{:?}", output.schema, output.rows())
    }

    #[test]
    fn leading_eigenvector_uses_shared_controls_and_single_rust_registration() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)],
        );
        let first = execute_leading_eigenvector(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            execute_leading_eigenvector(&graph, AlgorithmLimits::default()).unwrap(),
            first
        );
        assert!(matches!(
            execute_leading_eigenvector(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::LeadingEigenvector),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        let capabilities = registry.capabilities();
        assert_eq!(capabilities.len(), ClusterAlgorithm::ALL.len());
        let capability = capabilities
            .into_iter()
            .find(|entry| {
                entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::LeadingEigenvector)
            })
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn walktrap_uses_stable_uuid_output_and_single_rust_registration() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)],
        );
        let output = execute_walktrap(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&output), [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            output.schema,
            Algorithm::Cluster(ClusterAlgorithm::Walktrap).result_schema()
        );
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        let capabilities = registry.capabilities();
        assert_eq!(capabilities.len(), ClusterAlgorithm::ALL.len());
        let capability = capabilities
            .into_iter()
            .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::Walktrap))
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn spinglass_uses_stable_uuid_output_and_single_rust_registration() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)],
        );
        let output = execute_spinglass(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&output), [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            output.schema,
            Algorithm::Cluster(ClusterAlgorithm::Spinglass).result_schema()
        );
        assert!(matches!(
            execute_spinglass(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::Spinglass))
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn hdbscan_has_one_dependency_free_rust_registration() {
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        let capabilities = registry.capabilities();
        assert_eq!(capabilities.len(), ClusterAlgorithm::ALL.len());
        let capability = capabilities
            .into_iter()
            .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::Hdbscan))
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn kmeans_has_one_dependency_free_rust_registration() {
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        let owners = registry
            .capabilities()
            .into_iter()
            .filter(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::KMeans))
            .collect::<Vec<_>>();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].backend, "rust");
        assert_eq!(owners[0].dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn approximate_max_cut_has_one_dependency_free_rust_registration() {
        let graph =
            AdjacencyGraph::with_test_edges(5, &[(0, 1), (0, 1), (1, 1), (1, 2), (2, 3), (3, 0)]);
        let output = execute_approximate_max_cut(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&output), [0, 1, 0, 1, 0]);
        assert_eq!(
            output.schema,
            Algorithm::Cluster(ClusterAlgorithm::ApproximateMaxKCut).result_schema()
        );
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        let owners = registry
            .capabilities()
            .into_iter()
            .filter(|entry| {
                entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::ApproximateMaxKCut)
            })
            .collect::<Vec<_>>();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].backend, "rust");
        assert_eq!(owners[0].dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn strongly_connected_dispatches_with_stable_uuid_output_and_one_owner() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            6,
            &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 3), (4, 5)],
        );
        let output = execute_strongly_connected(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&output), [0, 0, 0, 1, 1, 2]);
        assert_eq!(
            output.schema,
            Algorithm::Cluster(ClusterAlgorithm::StronglyConnected).result_schema()
        );

        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        let owners = registry
            .capabilities()
            .into_iter()
            .filter(|entry| {
                entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::StronglyConnected)
            })
            .collect::<Vec<_>>();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].backend, "rust");
        assert_eq!(owners[0].dependency, BUILTIN_REVIEW);

        for (limits, expected) in [
            (
                AlgorithmLimits {
                    nodes: 5,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::NodeLimit {
                    observed: 6,
                    limit: 5,
                },
            ),
            (
                AlgorithmLimits {
                    edges: 6,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::EdgeLimit {
                    observed: 7,
                    limit: 6,
                },
            ),
            (
                AlgorithmLimits {
                    output_rows: 5,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::OutputLimit {
                    observed: 6,
                    limit: 5,
                },
            ),
            (
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::IterationLimit {
                    observed: 1,
                    limit: 0,
                },
            ),
        ] {
            assert_eq!(execute_strongly_connected(&graph, limits), Err(expected));
        }

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::StronglyConnected),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn biconnected_dispatches_stable_primary_labels_with_one_rust_owner() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            7,
            &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 2), (4, 5)],
        );
        let output = execute_biconnected(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&output), [0, 0, 0, 1, 1, 2, 3]);
        assert_eq!(
            output.schema,
            Algorithm::Cluster(ClusterAlgorithm::Biconnected).result_schema()
        );

        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        let owners = registry
            .capabilities()
            .into_iter()
            .filter(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::Biconnected))
            .collect::<Vec<_>>();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].backend, "rust");
        assert_eq!(owners[0].dependency, BUILTIN_REVIEW);

        for (limits, expected) in [
            (
                AlgorithmLimits {
                    nodes: 6,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::NodeLimit {
                    observed: 7,
                    limit: 6,
                },
            ),
            (
                AlgorithmLimits {
                    edges: 6,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::EdgeLimit {
                    observed: 7,
                    limit: 6,
                },
            ),
            (
                AlgorithmLimits {
                    output_rows: 6,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::OutputLimit {
                    observed: 7,
                    limit: 6,
                },
            ),
            (
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::IterationLimit {
                    observed: 1,
                    limit: 0,
                },
            ),
        ] {
            assert_eq!(execute_biconnected(&graph, limits), Err(expected));
        }
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::Biconnected),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn k_core_decomposition_dispatches_exact_numbers_with_one_rust_owner() {
        let graph = AdjacencyGraph::with_test_edges(
            10,
            &[
                (0, 1),
                (0, 2),
                (0, 3),
                (1, 2),
                (1, 3),
                (2, 3),
                (0, 4),
                (4, 5),
                (7, 8),
                (8, 9),
                (9, 7),
            ],
        );
        let output = execute_k_core_decomposition(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&output), [3, 3, 3, 3, 1, 1, 0, 2, 2, 2]);
        assert_eq!(
            output.schema,
            Algorithm::Cluster(ClusterAlgorithm::KCoreDecomposition).result_schema()
        );

        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        let owners = registry
            .capabilities()
            .into_iter()
            .filter(|entry| {
                entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::KCoreDecomposition)
            })
            .collect::<Vec<_>>();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].backend, "rust");
        assert_eq!(owners[0].dependency, BUILTIN_REVIEW);

        for (limits, expected) in [
            (
                AlgorithmLimits {
                    nodes: 9,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::NodeLimit {
                    observed: 10,
                    limit: 9,
                },
            ),
            (
                AlgorithmLimits {
                    edges: 10,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::EdgeLimit {
                    observed: 11,
                    limit: 10,
                },
            ),
            (
                AlgorithmLimits {
                    output_rows: 9,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::OutputLimit {
                    observed: 10,
                    limit: 9,
                },
            ),
            (
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmError::IterationLimit {
                    observed: 1,
                    limit: 0,
                },
            ),
        ] {
            assert_eq!(execute_k_core_decomposition(&graph, limits), Err(expected));
        }
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::KCoreDecomposition),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn k_core_decomposition_keeps_serial_fingerprint_under_thread_budgets() {
        let graph = AdjacencyGraph::with_test_edges(
            12,
            &[
                (0, 1),
                (0, 2),
                (0, 3),
                (1, 2),
                (1, 3),
                (2, 3),
                (3, 4),
                (4, 5),
                (5, 6),
                (6, 4),
                (7, 8),
                (8, 9),
                (9, 10),
                (10, 7),
                (10, 11),
                (0, 1),
                (2, 2),
            ],
        );
        let serial = execute_k_core_decomposition_with_compute_threads(&graph, 1).unwrap();
        assert_eq!(community_ids(&serial), [3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1]);
        let serial_fingerprint = output_fingerprint(&serial);

        for threads in [2_usize, 4, 8] {
            let output =
                execute_k_core_decomposition_with_compute_threads(&graph, threads).unwrap();
            assert_eq!(output.schema, serial.schema);
            assert_eq!(output_fingerprint(&output), serial_fingerprint);
        }
    }

    #[test]
    fn seeded_local_moves_validate_internal_partition() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let (weights, _) = normalized_communities(&graph, &control).unwrap();

        assert!(local_moves_from(&weights, Some(&[0, 0, 2]), "Leiden", &control).is_ok());
        for invalid in [&[0, 0][..], &[0, 0, 3][..]] {
            assert_eq!(
                local_moves_from(&weights, Some(invalid), "Leiden", &control),
                Err(execution("invalid internal community partition"))
            );
        }
    }

    fn execute_biconnected_with_compute_threads(
        graph: &AdjacencyGraph,
        threads: usize,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(threads).unwrap()));
        registry.execute(
            Algorithm::Cluster(ClusterAlgorithm::Biconnected),
            graph,
            &control,
        )
    }
    #[test]
    fn biconnected_keeps_serial_fingerprint_under_thread_budgets() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            9,
            &[
                (0, 1),
                (1, 2),
                (2, 0),
                (2, 3),
                (3, 4),
                (4, 2),
                (4, 5),
                (5, 6),
                (6, 7),
                (7, 5),
                (7, 8),
                (8, 8),
                (1, 0),
                (0, 1),
            ],
        );
        let serial = execute_biconnected_with_compute_threads(&graph, 1).unwrap();
        assert_eq!(community_ids(&serial), [0, 0, 0, 1, 1, 2, 3, 3, 4]);
        let serial_fingerprint = output_fingerprint(&serial);

        for threads in [2_usize, 4, 8] {
            let output = execute_biconnected_with_compute_threads(&graph, threads).unwrap();
            assert_eq!(output.schema, serial.schema);
            assert_eq!(output_fingerprint(&output), serial_fingerprint);
        }
    }
}
