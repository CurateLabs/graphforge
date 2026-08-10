//! Rust-owned cluster handlers registered under the shared M18 dispatch contract.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, ClusterAlgorithm};
use graphforge_core::{ClusterOptions, GfError, OntologyMode, TypeId};
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
type SimpleAdjacency = Vec<BTreeSet<usize>>;

const COMPONENTS_PARALLEL_CROSSOVER_EDGES: u64 = 16_384;
const COMPONENTS_CHECKPOINT_EDGES: usize = 16_384;

struct Components;

struct Louvain;

struct Leiden;

struct LabelPropagation;

struct SpeakerListener;

struct GirvanNewman;

struct ModularityOptimization;

struct FastGreedy;

struct InfoMap;

struct LeadingEigenvector;

struct Walktrap;

struct Spinglass;

struct Hdbscan;

struct KMeans;

struct ApproximateMaxKCut;

struct StronglyConnected;

struct Biconnected;

struct KCoreDecomposition;

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

impl RustAlgorithm for InfoMap {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::InfoMap),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = infomap_communities(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::InfoMap, control)
    }
}

impl RustAlgorithm for FastGreedy {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::FastGreedy),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = fastgreedy_communities(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::FastGreedy, control)
    }
}

impl RustAlgorithm for ModularityOptimization {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::ModularityOptimization),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = modularity_optimization_communities(graph, control)?;
        community_output(
            graph,
            &communities,
            ClusterAlgorithm::ModularityOptimization,
            control,
        )
    }
}

impl RustAlgorithm for GirvanNewman {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::GirvanNewman),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = girvan_newman_communities_with_progress(graph, control, || {})?;
        community_output(graph, &communities, ClusterAlgorithm::GirvanNewman, control)
    }
}

impl RustAlgorithm for SpeakerListener {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::SpeakerListener),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = speaker_listener_communities(graph, control)?;
        community_output(
            graph,
            &communities,
            ClusterAlgorithm::SpeakerListener,
            control,
        )
    }
}

impl RustAlgorithm for LabelPropagation {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::LabelPropagation),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = label_propagation_communities(graph, control)?;
        community_output(
            graph,
            &communities,
            ClusterAlgorithm::LabelPropagation,
            control,
        )
    }
}

impl RustAlgorithm for Leiden {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::Leiden),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = leiden_communities(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::Leiden, control)
    }
}

impl RustAlgorithm for Louvain {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::Louvain),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = louvain_communities(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::Louvain, control)
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

impl RustAlgorithm for Components {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::Components),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = component_labels(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::Components, control)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ComponentsExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

fn component_labels(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let node_count = graph.node_ids().len();
    let indices: HashMap<u64, usize> = graph
        .node_ids()
        .iter()
        .enumerate()
        .map(|(index, &node_id)| (node_id, index))
        .collect();
    let mut parents = match select_components_path(control, node_count, graph.edge_entry_count()) {
        ComponentsExecutionPath::Serial => component_parents_serial(graph, &indices, control)?,
        ComponentsExecutionPath::Parallel { .. } => {
            component_parents_parallel(graph, &indices, control)?
        }
    };
    component_ids_from_parents(&mut parents)
}

fn select_components_path(
    control: &AlgorithmControl,
    node_count: usize,
    edge_entries: u64,
) -> ComponentsExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || node_count <= 1
        || edge_entries < COMPONENTS_PARALLEL_CROSSOVER_EDGES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return ComponentsExecutionPath::Serial;
    }
    let chunks = components_source_chunks(node_count, threads).len();
    if chunks <= 1 {
        return ComponentsExecutionPath::Serial;
    }
    ComponentsExecutionPath::Parallel { threads, chunks }
}

fn component_parents_serial(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut parents: Vec<usize> = (0..graph.node_ids().len()).collect();
    let mut visited_edges = 0_usize;

    for (source_index, &source_id) in graph.node_ids().iter().enumerate() {
        for edge in graph.neighbors(source_id) {
            checkpoint_components_edge(control, &mut visited_edges)?;
            let target_index = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            union(&mut parents, source_index, target_index);
        }
    }
    Ok(parents)
}

fn component_parents_parallel(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel components requires an instance-owned compute pool"))?;
    let node_count = graph.node_ids().len();
    let ranges = components_source_chunks(node_count, control.compute_threads());
    let visited_edges = AtomicUsize::new(0);
    let local_parents = run_components_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut parents: Vec<usize> = (0..node_count).collect();
                for source_index in start..end {
                    control.check_cancelled()?;
                    let source_id = graph.node_ids()[source_index];
                    for edge in graph.neighbors(source_id) {
                        let observed = visited_edges.fetch_add(1, Ordering::Relaxed);
                        if observed.is_multiple_of(COMPONENTS_CHECKPOINT_EDGES) {
                            control.checkpoint()?;
                        }
                        let target_index = indices
                            .get(&edge.neighbor_id)
                            .copied()
                            .ok_or_else(|| execution("adjacency references an unselected node"))?;
                        union(&mut parents, source_index, target_index);
                    }
                }
                Ok(parents)
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;

    let mut parents: Vec<usize> = (0..node_count).collect();
    for mut local in local_parents {
        for index in 0..node_count {
            let root = find(&mut local, index);
            if root != index {
                union(&mut parents, index, root);
            }
        }
    }
    Ok(parents)
}

fn component_ids_from_parents(parents: &mut [usize]) -> Result<Vec<usize>, AlgorithmError> {
    let mut ids = HashMap::new();
    let mut labels = Vec::with_capacity(parents.len());
    for index in 0..parents.len() {
        let root = find(parents, index);
        let id = if let Some(&id) = ids.get(&root) {
            id
        } else {
            let id = ids.len();
            i64::try_from(id)
                .map_err(|_| execution("component count exceeds Int64 result range"))?;
            ids.insert(root, id);
            id
        };
        labels.push(id);
    }
    Ok(labels)
}

fn components_source_chunks(nodes: usize, threads: usize) -> Vec<(usize, usize)> {
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

fn run_components_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("components worker panicked")),
    }
}

fn checkpoint_components_edge(
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    if work.is_multiple_of(COMPONENTS_CHECKPOINT_EDGES) {
        control.checkpoint()?;
    }
    *work = work.saturating_add(1);
    Ok(())
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
    label: TypeId,
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
    label: TypeId,
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
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors cluster_algorithm_with_limits plus the instance compute pool handle"
)]
pub fn cluster_algorithm_with_compute(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: TypeId,
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
    label: TypeId,
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
    label: TypeId,
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
            label: Some(label),
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

fn find(parents: &mut [usize], index: usize) -> usize {
    if parents[index] != index {
        parents[index] = find(parents, parents[index]);
    }
    parents[index]
}

fn union(parents: &mut [usize], left: usize, right: usize) {
    let left = find(parents, left);
    let right = find(parents, right);
    let (first, second) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    parents[second] = first;
}

fn louvain_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let node_count = graph.node_ids().len();
    let (mut weights, mut members) = normalized_communities(graph, control)?;

    loop {
        let assignment = local_moves(&weights, control)?;
        let count = assignment.iter().copied().max().map_or(0, |id| id + 1);
        if count == weights.len() {
            break;
        }
        let (next_weights, next_members) =
            condense(&weights, &members, &assignment, count, control)?;
        weights = next_weights;
        members = next_members;
    }

    let mut result = vec![0; node_count];
    let mut work = 0_usize;
    for (community, nodes) in members.iter().enumerate() {
        for &node in nodes {
            checkpoint_chunk(control, &mut work)?;
            result[node] = community;
        }
    }
    Ok(result)
}

fn modularity_optimization_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let (weights, _) = normalized_communities(graph, control)?;
    local_moves_from(&weights, None, "Modularity optimization", control)
}

fn fastgreedy_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    fastgreedy_communities_with_progress(graph, control, || {})
}

fn fastgreedy_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    let (weights, _) = normalized_communities(graph, control)?;
    fastgreedy_from_weights(&weights, control, progress)
}

fn fastgreedy_from_weights(
    weights: &WeightedAdjacency,
    control: &AlgorithmControl,
    progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    fastgreedy_from_weights_with_updates(weights, control, progress, |_, _, _| {})
}

fn fastgreedy_from_weights_with_updates(
    weights: &WeightedAdjacency,
    control: &AlgorithmControl,
    mut progress: impl FnMut(),
    mut observe_updates: impl FnMut(usize, usize, usize),
) -> Result<Vec<usize>, AlgorithmError> {
    let singleton: Vec<_> = (0..weights.len()).collect();
    let mut best_score = partition_modularity(weights, &singleton, "Fastgreedy", control)?;
    let mut adjacency = weights.clone();
    let mut degrees = Vec::with_capacity(adjacency.len());
    let mut total = 0.0;
    let mut work = 0_usize;
    for neighbors in &adjacency {
        let mut degree = 0.0;
        for &weight in neighbors.values() {
            checkpoint_chunk(control, &mut work)?;
            degree += weight;
        }
        degrees.push(degree);
        total += degree;
    }
    if !total.is_finite() {
        return Err(execution("Fastgreedy total edge weight is not finite"));
    }
    if total == 0.0 {
        return Ok(singleton);
    }

    let mut gains = BTreeMap::new();
    for (source, neighbors) in adjacency.iter().enumerate() {
        for (&target, &weight) in neighbors {
            checkpoint_chunk(control, &mut work)?;
            if target >= adjacency.len() {
                return Err(execution("Fastgreedy adjacency index is out of range"));
            }
            if source < target {
                gains.insert(
                    (source, target),
                    fastgreedy_gain(weight, degrees[source], degrees[target], total)?,
                );
            }
        }
    }

    let mut merge_history = Vec::new();
    let mut best_merge_count = 0_usize;
    let mut current_score = best_score;
    while !gains.is_empty() {
        let mut selected = None;
        let mut selected_gain = f64::NEG_INFINITY;
        for (&pair, &gain) in &gains {
            checkpoint_chunk(control, &mut work)?;
            if gain > selected_gain + 1e-12 {
                selected = Some(pair);
                selected_gain = gain;
            }
        }
        let Some((left, right)) = selected else {
            break;
        };
        progress();
        control.checkpoint()?;

        let mut affected: BTreeSet<_> = adjacency[left]
            .keys()
            .chain(adjacency[right].keys())
            .copied()
            .collect();
        affected.retain(|&community| community != left && community != right);
        let right_neighbors = std::mem::take(&mut adjacency[right]);
        gains.remove(&(left, right));
        for &neighbor in &affected {
            checkpoint_chunk(control, &mut work)?;
            gains.remove(&(left.min(neighbor), left.max(neighbor)));
            gains.remove(&(right.min(neighbor), right.max(neighbor)));
            let combined = adjacency[left].remove(&neighbor).unwrap_or(0.0)
                + right_neighbors.get(&neighbor).copied().unwrap_or(0.0);
            adjacency[neighbor].remove(&left);
            adjacency[neighbor].remove(&right);
            if combined != 0.0 {
                adjacency[left].insert(neighbor, combined);
                adjacency[neighbor].insert(left, combined);
            }
        }
        adjacency[left].remove(&right);
        degrees[left] += degrees[right];
        degrees[right] = 0.0;

        let mut updated = 0_usize;
        for &neighbor in &affected {
            if let Some(&weight) = adjacency[left].get(&neighbor) {
                gains.insert(
                    (left.min(neighbor), left.max(neighbor)),
                    fastgreedy_gain(weight, degrees[left], degrees[neighbor], total)?,
                );
                updated += 1;
            }
        }
        observe_updates(left, right, updated);
        merge_history.push((left, right));
        current_score += selected_gain;
        if !current_score.is_finite() {
            return Err(execution("Fastgreedy modularity is not finite"));
        }
        if current_score > best_score + 1e-12 {
            best_score = current_score;
            best_merge_count = merge_history.len();
        }
    }

    fastgreedy_partition(weights.len(), &merge_history[..best_merge_count], control)
}

fn fastgreedy_gain(
    edge_weight: f64,
    left_degree: f64,
    right_degree: f64,
    total: f64,
) -> Result<f64, AlgorithmError> {
    let gain = 2.0 * edge_weight / total - 2.0 * left_degree * right_degree / total.powi(2);
    if gain.is_finite() {
        Ok(gain)
    } else {
        Err(execution("Fastgreedy modularity gain is not finite"))
    }
}

fn fastgreedy_partition(
    node_count: usize,
    merges: &[(usize, usize)],
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut parent: Vec<_> = (0..node_count).collect();
    let mut work = 0_usize;
    for &(left, right) in merges {
        checkpoint_chunk(control, &mut work)?;
        parent[right] = left;
    }
    let mut partition = Vec::with_capacity(node_count);
    for node in 0..node_count {
        let mut representative = node;
        while parent[representative] != representative {
            checkpoint_chunk(control, &mut work)?;
            representative = parent[representative];
        }
        let mut current = node;
        while parent[current] != current {
            checkpoint_chunk(control, &mut work)?;
            let next = parent[current];
            parent[current] = representative;
            current = next;
        }
        partition.push(representative);
    }
    canonicalize_partition(&mut partition, control)?;
    Ok(partition)
}

#[derive(Debug)]
struct InfomapFlow {
    outgoing: SimpleAdjacency,
    incident: SimpleAdjacency,
    components: Vec<Vec<usize>>,
    directed: bool,
}

fn infomap_flow(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<InfomapFlow, AlgorithmError> {
    let node_count = graph.node_ids().len();
    let indices: HashMap<_, _> = graph
        .node_ids()
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut outgoing = vec![BTreeSet::new(); node_count];
    let mut incident = vec![BTreeSet::new(); node_count];
    let mut work = 0_usize;
    for (source, &node) in graph.node_ids().iter().enumerate() {
        for edge in graph.neighbors(node) {
            checkpoint_chunk(control, &mut work)?;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            if source != target && outgoing[source].insert(target) {
                incident[source].insert(target);
                incident[target].insert(source);
            }
        }
    }
    let directed = graph.is_directed();
    let mut seen = vec![false; node_count];
    let mut components = Vec::new();
    for start in 0..node_count {
        if seen[start] {
            continue;
        }
        let mut component = Vec::new();
        let mut queue = VecDeque::from([start]);
        seen[start] = true;
        while let Some(node) = queue.pop_front() {
            checkpoint_chunk(control, &mut work)?;
            component.push(node);
            for &neighbor in &incident[node] {
                if !seen[neighbor] {
                    seen[neighbor] = true;
                    queue.push_back(neighbor);
                }
            }
        }
        components.push(component);
    }
    Ok(InfomapFlow {
        outgoing,
        incident,
        components,
        directed,
    })
}

fn infomap_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let flow = infomap_flow(graph, control)?;
    let mut assignment: Vec<_> = (0..graph.node_ids().len()).collect();
    for component in &flow.components {
        if component.len() == 1 {
            continue;
        }
        let visits = infomap_stationary(component, &flow.outgoing, flow.directed, control)?;
        let search = InfomapSearch {
            component,
            flow: &flow,
            visits: &visits,
        };
        loop {
            control.checkpoint()?;
            let moved = search.node_sweep(&mut assignment, control, || {})?;
            infomap_representative_labels(component, &mut assignment);
            let merged = search.module_merge(&mut assignment, control)?;
            if !moved && !merged {
                break;
            }
        }
    }
    canonicalize_partition(&mut assignment, control)?;
    Ok(assignment)
}

struct InfomapSearch<'a> {
    component: &'a [usize],
    flow: &'a InfomapFlow,
    visits: &'a [f64],
}

impl InfomapSearch<'_> {
    fn score(&self, assignment: &[usize]) -> Result<f64, AlgorithmError> {
        infomap_codelength(
            self.component,
            &self.flow.outgoing,
            self.visits,
            self.flow.directed,
            assignment,
        )
    }

    fn node_sweep(
        &self,
        assignment: &mut [usize],
        control: &AlgorithmControl,
        mut progress: impl FnMut(),
    ) -> Result<bool, AlgorithmError> {
        let mut changed = false;
        let mut work = 0_usize;
        for &node in self.component {
            let current = assignment[node];
            let current_score = self.score(assignment)?;
            let mut counts = BTreeMap::new();
            for &member in self.component {
                *counts.entry(assignment[member]).or_insert(0_usize) += 1;
            }
            let mut candidates: BTreeSet<_> = self.flow.incident[node]
                .iter()
                .map(|&neighbor| assignment[neighbor])
                .collect();
            if counts[&current] > 1
                && let Some(empty) = self
                    .component
                    .iter()
                    .copied()
                    .find(|label| !counts.contains_key(label))
            {
                candidates.insert(empty);
            }
            let mut best = (current_score, usize::MAX, current);
            for candidate in candidates {
                if candidate == current {
                    continue;
                }
                assignment[node] = candidate;
                let score = self.score(assignment)?;
                let representative = self
                    .component
                    .iter()
                    .copied()
                    .filter(|&member| assignment[member] == candidate)
                    .min()
                    .unwrap_or(node);
                if score < best.0 - 1e-12
                    || ((score - best.0).abs() <= 1e-12 && representative < best.1)
                {
                    best = (score, representative, candidate);
                }
                progress();
                checkpoint_chunk(control, &mut work)?;
            }
            assignment[node] = current;
            if best.0 < current_score - 1e-12 {
                assignment[node] = best.2;
                changed = true;
            }
        }
        Ok(changed)
    }

    fn module_merge(
        &self,
        assignment: &mut [usize],
        control: &AlgorithmControl,
    ) -> Result<bool, AlgorithmError> {
        let current = self.score(assignment)?;
        let mut pairs = BTreeSet::new();
        for &node in self.component {
            for &neighbor in &self.flow.incident[node] {
                let pair = (
                    assignment[node].min(assignment[neighbor]),
                    assignment[node].max(assignment[neighbor]),
                );
                if pair.0 != pair.1 {
                    pairs.insert(pair);
                }
            }
        }
        let mut best = (current, (usize::MAX, usize::MAX), None);
        let mut work = 0_usize;
        for (left, right) in pairs {
            let moved: Vec<_> = self
                .component
                .iter()
                .copied()
                .filter(|&node| assignment[node] == right)
                .collect();
            for &node in &moved {
                assignment[node] = left;
            }
            let score = self.score(assignment)?;
            for &node in &moved {
                assignment[node] = right;
            }
            if score < best.0 - 1e-12 || ((score - best.0).abs() <= 1e-12 && (left, right) < best.1)
            {
                best = (score, (left, right), Some((left, right)));
            }
            checkpoint_chunk(control, &mut work)?;
        }
        let Some((left, right)) = best.2.filter(|_| best.0 < current - 1e-12) else {
            return Ok(false);
        };
        for &node in self.component {
            if assignment[node] == right {
                assignment[node] = left;
            }
        }
        Ok(true)
    }
}

fn infomap_representative_labels(component: &[usize], assignment: &mut [usize]) {
    let mut representatives = BTreeMap::new();
    for &node in component {
        representatives
            .entry(assignment[node])
            .and_modify(|representative: &mut usize| *representative = (*representative).min(node))
            .or_insert(node);
    }
    for &node in component {
        assignment[node] = representatives[&assignment[node]];
    }
}

fn infomap_stationary(
    component: &[usize],
    outgoing: &SimpleAdjacency,
    directed: bool,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut visits = vec![0.0; outgoing.len()];
    if !directed {
        let total: usize = component.iter().map(|&node| outgoing[node].len()).sum();
        let total = infomap_count(total, "component edge-entry count")?;
        for &node in component {
            visits[node] = infomap_count(outgoing[node].len(), "node degree")? / total;
        }
        return Ok(visits);
    }
    let size = infomap_count(component.len(), "component node count")?;
    for &node in component {
        visits[node] = 1.0 / size;
    }
    for iteration in 1..=1_000 {
        control.checkpoint()?;
        let dangling: f64 = component
            .iter()
            .filter(|&&node| outgoing[node].is_empty())
            .map(|&node| visits[node])
            .sum();
        let base = (0.15 + 0.85 * dangling) / size;
        let mut next = vec![0.0; outgoing.len()];
        for &node in component {
            next[node] = base;
        }
        for &source in component {
            if !outgoing[source].is_empty() {
                let degree = infomap_count(outgoing[source].len(), "node outdegree")?;
                let share = 0.85 * visits[source] / degree;
                for &target in &outgoing[source] {
                    next[target] += share;
                }
            }
        }
        let delta: f64 = component
            .iter()
            .map(|&node| (next[node] - visits[node]).abs())
            .sum();
        visits = next;
        if delta <= 1e-12 {
            return Ok(visits);
        }
        if iteration == 1_000 {
            return Err(AlgorithmError::NonConvergence { iterations: 1_000 });
        }
    }
    unreachable!()
}

fn infomap_codelength(
    component: &[usize],
    outgoing: &SimpleAdjacency,
    visits: &[f64],
    directed: bool,
    assignment: &[usize],
) -> Result<f64, AlgorithmError> {
    let mut module_visits = BTreeMap::new();
    let mut sizes = BTreeMap::new();
    for &node in component {
        *module_visits.entry(assignment[node]).or_insert(0.0) += visits[node];
        *sizes.entry(assignment[node]).or_insert(0_usize) += 1;
    }
    let mut exits = BTreeMap::new();
    let size = infomap_count(component.len(), "component node count")?;
    for &source in component {
        let module = assignment[source];
        let external = infomap_count(
            outgoing[source]
                .iter()
                .filter(|&&target| assignment[target] != module)
                .count(),
            "external edge count",
        )?;
        let outside = infomap_count(
            component.len() - sizes[&module],
            "outside-module node count",
        )? / size;
        let exit_probability = if outgoing[source].is_empty() {
            outside
        } else if directed {
            0.15 * outside
                + 0.85 * external / infomap_count(outgoing[source].len(), "node outdegree")?
        } else {
            external / infomap_count(outgoing[source].len(), "node degree")?
        };
        *exits.entry(module).or_insert(0.0) += visits[source] * exit_probability;
    }
    let exit_total: f64 = exits.values().sum();
    let codelength = xlogx(exit_total)
        - 2.0 * exits.values().copied().map(xlogx).sum::<f64>()
        - component
            .iter()
            .map(|&node| xlogx(visits[node]))
            .sum::<f64>()
        + module_visits
            .iter()
            .map(|(module, visit)| xlogx(visit + exits.get(module).copied().unwrap_or(0.0)))
            .sum::<f64>();
    if codelength.is_finite() {
        Ok(codelength)
    } else {
        Err(execution("Infomap codelength is not finite"))
    }
}

fn xlogx(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        value * value.log2()
    }
}

fn infomap_count(value: usize, name: &str) -> Result<f64, AlgorithmError> {
    let value = u32::try_from(value)
        .map_err(|_| execution(&format!("Infomap {name} exceeds supported numeric range")))?;
    Ok(f64::from(value))
}

fn normalized_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
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

fn local_moves(
    weights: &[BTreeMap<usize, f64>],
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    local_moves_from(weights, None, "Louvain", control)
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

fn leiden_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let (mut weights, mut members) = normalized_communities(graph, control)?;
    let mut seed = None;
    let mut random = 0x4c45_4944_454e_u64;
    loop {
        let coarse = local_moves_from(&weights, seed.as_deref(), "Leiden", control)?;
        let refined = refine_partition(&weights, &coarse, &mut random, control)?;
        let count = refined.iter().copied().max().map_or(0, |id| id + 1);
        if count == weights.len() {
            return expand_partition(&members, &coarse, graph.node_ids().len(), control);
        }

        let mut next_seed = vec![0; count];
        let mut work = 0;
        for node in 0..weights.len() {
            checkpoint_chunk(control, &mut work)?;
            next_seed[refined[node]] = coarse[node];
        }
        canonicalize_partition(&mut next_seed, control)?;
        (weights, members) = condense(&weights, &members, &refined, count, control)?;
        seed = Some(next_seed);
    }
}

fn label_propagation_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    label_propagation_communities_with_progress(graph, control, || {})
}

fn label_propagation_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    mut progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    let (weights, _) = normalized_communities(graph, control)?;
    let mut labels: Vec<_> = (0..weights.len()).collect();
    let mut order: Vec<_> = (0..weights.len()).collect();
    let mut random = 0x004c_4142_454c_u64;
    let mut work = 0;

    loop {
        control.checkpoint()?;
        progress();
        shuffle(&mut order, &mut random, control, &mut work)?;
        for &node in &order {
            checkpoint_chunk(control, &mut work)?;
            let dominant = dominant_neighbor_labels(&weights, &labels, node, control, &mut work)?;
            if !dominant.is_empty() {
                labels[node] = dominant[random_index(&mut random, dominant.len())?];
            }
        }

        let mut stable = true;
        for node in 0..weights.len() {
            checkpoint_chunk(control, &mut work)?;
            let dominant = dominant_neighbor_labels(&weights, &labels, node, control, &mut work)?;
            if !dominant.is_empty() && !dominant.contains(&labels[node]) {
                stable = false;
                break;
            }
        }
        if stable {
            break;
        }
    }

    canonicalize_partition(&mut labels, control)?;
    Ok(labels)
}

fn speaker_listener_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    speaker_listener_communities_with_progress(graph, control, || {})
}

fn speaker_listener_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    mut progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    const SWEEPS: usize = 100;
    let (weights, _) = normalized_communities(graph, control)?;
    let mut memories: Vec<BTreeMap<usize, usize>> = (0..weights.len())
        .map(|label| BTreeMap::from([(label, 1)]))
        .collect();
    let mut lengths = vec![1_usize; weights.len()];
    let mut order: Vec<_> = (0..weights.len()).collect();
    let mut random = 0x0053_4c50_4101_u64;
    let mut work = 0_usize;

    for _ in 0..SWEEPS {
        control.checkpoint()?;
        progress();
        shuffle(&mut order, &mut random, control, &mut work)?;
        for &listener in &order {
            checkpoint_chunk(control, &mut work)?;
            let mut received = BTreeMap::new();
            for &speaker in weights[listener].keys() {
                checkpoint_chunk(control, &mut work)?;
                let label = sample_memory_label(&memories[speaker], lengths[speaker], &mut random)?;
                let count = received.entry(label).or_insert(0_usize);
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| execution("speaker label count exceeds platform range"))?;
            }
            let maximum = received.values().copied().max().unwrap_or(0);
            let dominant: Vec<_> = received
                .into_iter()
                .filter_map(|(label, count)| (count == maximum).then_some(label))
                .collect();
            if !dominant.is_empty() {
                let label = dominant[random_index(&mut random, dominant.len())?];
                let count = memories[listener].entry(label).or_insert(0);
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| execution("listener memory exceeds platform range"))?;
                lengths[listener] = lengths[listener]
                    .checked_add(1)
                    .ok_or_else(|| execution("listener memory exceeds platform range"))?;
            }
        }
    }

    let mut labels = Vec::with_capacity(memories.len());
    for (memory, length) in memories.iter().zip(lengths) {
        checkpoint_chunk(control, &mut work)?;
        let strongest = memory
            .iter()
            .filter(|(_, count)| count.checked_mul(20).is_some_and(|seen| seen >= length))
            .max_by_key(|(label, count)| (**count, std::cmp::Reverse(**label)))
            .or_else(|| {
                memory
                    .iter()
                    .max_by_key(|(label, count)| (**count, std::cmp::Reverse(**label)))
            })
            .map(|(&label, _)| label)
            .ok_or_else(|| execution("speaker-listener memory is empty"))?;
        labels.push(strongest);
    }
    canonicalize_partition(&mut labels, control)?;
    Ok(labels)
}

fn sample_memory_label(
    memory: &BTreeMap<usize, usize>,
    length: usize,
    random: &mut u64,
) -> Result<usize, AlgorithmError> {
    let mut selected = random_index(random, length)?;
    for (&label, &count) in memory {
        if selected < count {
            return Ok(label);
        }
        selected -= count;
    }
    Err(execution("speaker-listener memory length is inconsistent"))
}

fn girvan_newman_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    mut progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    let (original, _) = normalized_communities(graph, control)?;
    let mut current = original.clone();
    let mut level = component_partition(&current, control)?;
    let mut best = level.clone();
    let mut best_score = partition_modularity(&original, &best, "Girvan-Newman", control)?;

    while current.iter().any(|neighbors| !neighbors.is_empty()) {
        control.checkpoint()?;
        progress();
        let scores = edge_betweenness(&current, control)?;
        let ((left, right), _) = scores
            .into_iter()
            .reduce(|best, candidate| {
                let better = candidate.1 > best.1 + 1e-12
                    || ((candidate.1 - best.1).abs() <= 1e-12 && candidate.0 < best.0);
                if better { candidate } else { best }
            })
            .ok_or_else(|| execution("non-empty graph has no removable edge"))?;
        current[left].remove(&right);
        current[right].remove(&left);
        let candidate = component_partition(&current, control)?;
        if candidate != level {
            let score = partition_modularity(&original, &candidate, "Girvan-Newman", control)?;
            if score > best_score + 1e-12 {
                best_score = score;
                best.clone_from(&candidate);
            }
            level = candidate;
        }
    }
    Ok(best)
}

fn edge_betweenness(
    graph: &WeightedAdjacency,
    control: &AlgorithmControl,
) -> Result<BTreeMap<(usize, usize), f64>, AlgorithmError> {
    let mut scores = BTreeMap::new();
    for (node, neighbors) in graph.iter().enumerate() {
        for &neighbor in neighbors.keys().filter(|&&neighbor| node < neighbor) {
            scores.insert((node, neighbor), 0.0);
        }
    }
    let mut work = 0_usize;
    for source in 0..graph.len() {
        checkpoint_chunk(control, &mut work)?;
        let mut stack = Vec::new();
        let mut predecessors = vec![Vec::new(); graph.len()];
        let mut paths = vec![0.0_f64; graph.len()];
        let mut distance = vec![usize::MAX; graph.len()];
        let mut queue = VecDeque::from([source]);
        paths[source] = 1.0;
        distance[source] = 0;
        while let Some(node) = queue.pop_front() {
            checkpoint_chunk(control, &mut work)?;
            stack.push(node);
            let next = distance[node]
                .checked_add(1)
                .ok_or_else(|| execution("shortest-path depth exceeds platform range"))?;
            for &neighbor in graph[node].keys() {
                checkpoint_chunk(control, &mut work)?;
                if distance[neighbor] == usize::MAX {
                    distance[neighbor] = next;
                    queue.push_back(neighbor);
                }
                if distance[neighbor] == next {
                    paths[neighbor] += paths[node];
                    if !paths[neighbor].is_finite() {
                        return Err(execution("shortest-path count is not finite"));
                    }
                    predecessors[neighbor].push(node);
                }
            }
        }
        let mut dependency = vec![0.0_f64; graph.len()];
        while let Some(node) = stack.pop() {
            for &predecessor in &predecessors[node] {
                checkpoint_chunk(control, &mut work)?;
                let contribution = paths[predecessor] / paths[node] * (1.0 + dependency[node]);
                let edge = if predecessor < node {
                    (predecessor, node)
                } else {
                    (node, predecessor)
                };
                let score = scores
                    .get_mut(&edge)
                    .ok_or_else(|| execution("shortest path references a missing edge"))?;
                *score += contribution;
                dependency[predecessor] += contribution;
                if !score.is_finite() || !dependency[predecessor].is_finite() {
                    return Err(execution("edge betweenness is not finite"));
                }
            }
        }
    }
    Ok(scores)
}

fn component_partition(
    graph: &WeightedAdjacency,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut partition = vec![usize::MAX; graph.len()];
    let mut work = 0_usize;
    for start in 0..graph.len() {
        checkpoint_chunk(control, &mut work)?;
        if partition[start] != usize::MAX {
            continue;
        }
        let community = start;
        partition[start] = community;
        let mut queue = VecDeque::from([start]);
        while let Some(node) = queue.pop_front() {
            for &neighbor in graph[node].keys() {
                checkpoint_chunk(control, &mut work)?;
                if partition[neighbor] == usize::MAX {
                    partition[neighbor] = community;
                    queue.push_back(neighbor);
                }
            }
        }
    }
    canonicalize_partition(&mut partition, control)?;
    Ok(partition)
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

fn dominant_neighbor_labels(
    weights: &[BTreeMap<usize, f64>],
    labels: &[usize],
    node: usize,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut counts = BTreeMap::new();
    for &neighbor in weights[node].keys() {
        checkpoint_chunk(control, work)?;
        let count = counts.entry(labels[neighbor]).or_insert(0_usize);
        *count = count
            .checked_add(1)
            .ok_or_else(|| execution("label frequency exceeds platform range"))?;
    }
    let maximum = counts.values().copied().max().unwrap_or(0);
    Ok(counts
        .into_iter()
        .filter_map(|(label, count)| (count == maximum).then_some(label))
        .collect())
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

fn refine_partition(
    weights: &[BTreeMap<usize, f64>],
    coarse: &[usize],
    random: &mut u64,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut refined: Vec<usize> = (0..weights.len()).collect();
    let mut degrees = vec![0.0; weights.len()];
    let mut total_weight = 0.0;
    let mut work = 0;
    for (node, neighbors) in weights.iter().enumerate() {
        for weight in neighbors.values() {
            checkpoint_chunk(control, &mut work)?;
            degrees[node] += weight;
        }
        total_weight += degrees[node];
    }
    if !total_weight.is_finite() {
        return Err(execution("Leiden total edge weight is not finite"));
    }
    if total_weight == 0.0 {
        return Ok(refined);
    }

    let mut coarse_totals = vec![0.0; weights.len()];
    for (node, &community) in coarse.iter().enumerate() {
        coarse_totals[community] += degrees[node];
    }
    let mut refined_totals = degrees.clone();
    let mut sizes = vec![1_usize; weights.len()];
    for node in 0..weights.len() {
        checkpoint_chunk(control, &mut work)?;
        let old = refined[node];
        if sizes[old] != 1 || degrees[node] == 0.0 {
            continue;
        }
        let parent = coarse[node];
        let mut parent_weight = 0.0;
        let mut candidates = BTreeMap::new();
        for (&neighbor, &weight) in &weights[node] {
            checkpoint_chunk(control, &mut work)?;
            if neighbor != node && coarse[neighbor] == parent {
                parent_weight += weight;
                if refined[neighbor] != old {
                    *candidates.entry(refined[neighbor]).or_insert(0.0) += weight;
                }
            }
        }
        let threshold = degrees[node] * (coarse_totals[parent] - degrees[node]) / total_weight;
        if parent_weight + 1e-12 < threshold {
            continue;
        }

        let mut gains = Vec::new();
        for (candidate, internal) in candidates {
            checkpoint_chunk(control, &mut work)?;
            let gain = internal - degrees[node] * refined_totals[candidate] / total_weight;
            if !gain.is_finite() {
                return Err(execution("Leiden refinement gain is not finite"));
            }
            if gain > 1e-12 {
                gains.push((candidate, gain));
            }
        }
        if gains.is_empty() {
            continue;
        }
        let selected = weighted_choice(&gains, random)?;
        refined[node] = selected;
        refined_totals[old] -= degrees[node];
        refined_totals[selected] += degrees[node];
        sizes[old] = 0;
        sizes[selected] += 1;
    }
    canonicalize_partition(&mut refined, control)?;
    Ok(refined)
}

fn weighted_choice(gains: &[(usize, f64)], random: &mut u64) -> Result<usize, AlgorithmError> {
    let max_gain = gains
        .iter()
        .map(|entry| entry.1)
        .reduce(f64::max)
        .expect("non-empty refinement gains");
    let scaled: Vec<_> = gains
        .iter()
        .map(|&(candidate, gain)| (candidate, ((gain - max_gain) / 0.01).exp()))
        .collect();
    let total: f64 = scaled.iter().map(|entry| entry.1).sum();
    if !total.is_finite() || total <= 0.0 {
        return Err(execution("Leiden refinement probability is not finite"));
    }
    let mut draw = next_unit(random) * total;
    for &(candidate, probability) in &scaled {
        if draw < probability {
            return Ok(candidate);
        }
        draw -= probability;
    }
    Ok(scaled.last().expect("non-empty probabilities").0)
}

fn next_unit(state: &mut u64) -> f64 {
    let value = next_random(state);
    let high = u32::try_from(value >> 32).expect("upper random bits fit UInt32");
    let low = u32::try_from((value >> 11) & 0x1f_ffff).expect("lower random bits fit UInt32");
    (f64::from(high) * 2_097_152.0 + f64::from(low)) / 9_007_199_254_740_992.0
}

fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    value
}

fn expand_partition(
    members: &[Vec<usize>],
    assignment: &[usize],
    node_count: usize,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut result = vec![0; node_count];
    let mut work = 0;
    for (node, originals) in members.iter().enumerate() {
        for &original in originals {
            checkpoint_chunk(control, &mut work)?;
            result[original] = assignment[node];
        }
    }
    canonicalize_partition(&mut result, control)?;
    Ok(result)
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
    use super::*;

    fn execute_components(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::Components),
            limits,
        )
    }

    fn execute_components_with_threads(
        graph: &AdjacencyGraph,
        threads: usize,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        execute_cluster_with_compute(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::Components),
            AlgorithmLimits::default().with_compute_threads(threads),
            Some(pool),
        )
    }

    fn execute_louvain(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(graph, Algorithm::Cluster(ClusterAlgorithm::Louvain), limits)
    }

    fn execute_leiden(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(graph, Algorithm::Cluster(ClusterAlgorithm::Leiden), limits)
    }

    fn execute_label_propagation(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::LabelPropagation),
            limits,
        )
    }

    fn execute_speaker_listener(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::SpeakerListener),
            limits,
        )
    }

    fn execute_girvan_newman(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::GirvanNewman),
            limits,
        )
    }

    fn execute_modularity_optimization(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::ModularityOptimization),
            limits,
        )
    }

    fn execute_fastgreedy(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(
            graph,
            Algorithm::Cluster(ClusterAlgorithm::FastGreedy),
            limits,
        )
    }

    fn execute_infomap(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_cluster(graph, Algorithm::Cluster(ClusterAlgorithm::InfoMap), limits)
    }

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

    fn community_ids(output: &AlgorithmOutput) -> Vec<i64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Int64(value) => value,
                _ => panic!("expected Int64 community id"),
            })
            .collect()
    }

    #[test]
    fn leiden_refines_a_hand_verifiable_partition_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(
            8,
            &[
                (0, 4),
                (0, 6),
                (1, 2),
                (1, 5),
                (1, 6),
                (2, 6),
                (3, 6),
                (4, 6),
                (5, 6),
            ],
        );
        let first = execute_leiden(&graph, AlgorithmLimits::default()).unwrap();
        // Leiden refines Louvain's partition into the connected sets
        // {0,3,4,6}, {1,2,5}, and the isolate {7}.
        assert_eq!(community_ids(&first), [0, 1, 1, 0, 0, 1, 0, 2]);
        assert_ne!(
            community_ids(&execute_louvain(&graph, AlgorithmLimits::default()).unwrap()),
            community_ids(&first)
        );
        assert_eq!(
            execute_leiden(&graph, AlgorithmLimits::default()).unwrap(),
            first
        );
    }

    #[test]
    fn leiden_normalizes_boundaries_and_uses_shared_controls() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[
                (0, 1),
                (1, 0),
                (0, 1),
                (1, 2),
                (2, 0),
                (0, 0),
                (3, 4),
                (4, 5),
                (5, 3),
            ],
        );
        assert_eq!(
            community_ids(&execute_leiden(&graph, AlgorithmLimits::default()).unwrap()),
            [0, 0, 0, 1, 1, 1, 2]
        );
        assert!(
            execute_leiden(&AdjacencyGraph::default(), AlgorithmLimits::default())
                .unwrap()
                .rows()
                .is_empty()
        );
        assert_eq!(
            execute_leiden(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::Leiden),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::Leiden))
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn label_propagation_is_deterministic_and_normalizes_boundaries() {
        let simple =
            AdjacencyGraph::with_test_edges(7, &[(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)]);
        let noisy = AdjacencyGraph::with_test_edges(
            7,
            &[
                (0, 1),
                (1, 0),
                (0, 1),
                (1, 2),
                (2, 0),
                (0, 0),
                (3, 4),
                (4, 5),
                (5, 3),
                (5, 5),
            ],
        );
        let first = execute_label_propagation(&simple, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            execute_label_propagation(&simple, AlgorithmLimits::default()).unwrap(),
            first
        );
        assert_eq!(
            community_ids(&execute_label_propagation(&noisy, AlgorithmLimits::default()).unwrap()),
            [0, 0, 0, 1, 1, 1, 2]
        );
        assert_eq!(
            community_ids(
                &execute_label_propagation(
                    &AdjacencyGraph::with_test_edges(3, &[]),
                    AlgorithmLimits::default(),
                )
                .unwrap()
            ),
            [0, 1, 2]
        );
        assert!(
            execute_label_propagation(&AdjacencyGraph::default(), AlgorithmLimits::default())
                .unwrap()
                .rows()
                .is_empty()
        );
    }

    #[test]
    fn label_propagation_uses_shared_controls_and_rust_registration() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_label_propagation(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::LabelPropagation),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::LabelPropagation))
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn label_propagation_observes_cancellation_after_propagation_starts() {
        let graph = AdjacencyGraph::with_test_counts(2, 500_000);
        let cancellation = AlgorithmCancellation::default();
        let cancel = cancellation.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let control = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
            let mut rendezvous = Some((started_tx, resume_rx));
            result_tx
                .send(label_propagation_communities_with_progress(
                    &graph,
                    &control,
                    || {
                        if let Some((started, resume)) = rendezvous.take() {
                            started.send(()).unwrap();
                            resume.recv().unwrap();
                        }
                    },
                ))
                .unwrap();
        });
        started_rx.recv().unwrap();
        cancel.cancel();
        resume_tx.send(()).unwrap();
        assert_eq!(result_rx.recv().unwrap(), Err(AlgorithmError::Cancelled));
        worker.join().unwrap();
    }

    #[test]
    fn speaker_listener_is_deterministic_and_normalizes_boundaries() {
        let simple =
            AdjacencyGraph::with_test_edges(7, &[(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)]);
        let noisy = AdjacencyGraph::with_test_edges(
            7,
            &[
                (0, 1),
                (1, 0),
                (0, 1),
                (1, 2),
                (2, 0),
                (0, 0),
                (3, 4),
                (4, 5),
                (5, 3),
                (5, 5),
            ],
        );
        let first = execute_speaker_listener(&simple, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            execute_speaker_listener(&simple, AlgorithmLimits::default()).unwrap(),
            first
        );
        assert_eq!(
            community_ids(&execute_speaker_listener(&noisy, AlgorithmLimits::default()).unwrap()),
            [0, 0, 0, 1, 1, 1, 2]
        );
        assert_eq!(
            community_ids(
                &execute_speaker_listener(
                    &AdjacencyGraph::with_test_edges(3, &[]),
                    AlgorithmLimits::default(),
                )
                .unwrap()
            ),
            [0, 1, 2]
        );
        assert!(
            execute_speaker_listener(&AdjacencyGraph::default(), AlgorithmLimits::default())
                .unwrap()
                .rows()
                .is_empty()
        );
    }

    #[test]
    fn speaker_listener_uses_shared_controls_and_rust_registration() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_speaker_listener(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::SpeakerListener),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::SpeakerListener))
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn speaker_listener_observes_cancellation_after_propagation_starts() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let cancellation = AlgorithmCancellation::default();
        let cancel = cancellation.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let control = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
            let mut rendezvous = Some((started_tx, resume_rx));
            result_tx
                .send(speaker_listener_communities_with_progress(
                    &graph,
                    &control,
                    || {
                        if let Some((started, resume)) = rendezvous.take() {
                            started.send(()).unwrap();
                            resume.recv().unwrap();
                        }
                    },
                ))
                .unwrap();
        });
        started_rx.recv().unwrap();
        cancel.cancel();
        resume_tx.send(()).unwrap();
        assert_eq!(result_rx.recv().unwrap(), Err(AlgorithmError::Cancelled));
        worker.join().unwrap();
    }

    #[test]
    fn girvan_newman_selects_the_deterministic_best_modularity_level() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[
                (0, 1),
                (1, 0),
                (0, 1),
                (1, 2),
                (2, 0),
                (2, 3),
                (3, 4),
                (4, 5),
                (5, 3),
                (5, 5),
            ],
        );
        let first = execute_girvan_newman(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            execute_girvan_newman(&graph, AlgorithmLimits::default()).unwrap(),
            first
        );
        for (boundary, expected) in [
            (AdjacencyGraph::with_test_edges(3, &[]), vec![0, 1, 2]),
            (AdjacencyGraph::default(), vec![]),
        ] {
            assert_eq!(
                community_ids(
                    &execute_girvan_newman(&boundary, AlgorithmLimits::default()).unwrap()
                ),
                expected
            );
        }

        let single_edge = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let mut merges = 0;
        assert_eq!(
            fastgreedy_communities_with_progress(&single_edge, &control, || merges += 1).unwrap(),
            [0, 0]
        );
        assert_eq!(merges, 1, "the terminal candidate pass is not a merge");
    }

    #[test]
    fn girvan_newman_uses_shared_controls_cancellation_and_rust_registration() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_girvan_newman(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::GirvanNewman),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::GirvanNewman))
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);

        let cancellation = AlgorithmCancellation::default();
        let cancel = cancellation.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let control = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
            let mut rendezvous = Some((started_tx, resume_rx));
            girvan_newman_communities_with_progress(&graph, &control, || {
                if let Some((started, resume)) = rendezvous.take() {
                    started.send(()).unwrap();
                    resume.recv().unwrap();
                }
            })
        });
        started_rx.recv().unwrap();
        cancel.cancel();
        resume_tx.send(()).unwrap();
        assert_eq!(worker.join().unwrap(), Err(AlgorithmError::Cancelled));
    }

    #[test]
    fn modularity_optimization_is_deterministic_single_level_local_moving() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[
                (0, 1),
                (1, 0),
                (0, 1),
                (1, 2),
                (2, 0),
                (2, 3),
                (3, 4),
                (4, 5),
                (5, 3),
                (5, 5),
            ],
        );
        let first = execute_modularity_optimization(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            execute_modularity_optimization(&graph, AlgorithmLimits::default()).unwrap(),
            first
        );

        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let (weights, _) = normalized_communities(&graph, &control).unwrap();
        assert_eq!(
            modularity_optimization_communities(&graph, &control).unwrap(),
            local_moves_from(&weights, None, "Modularity optimization", &control).unwrap()
        );
        for (boundary, expected) in [
            (AdjacencyGraph::with_test_edges(3, &[]), vec![0, 1, 2]),
            (AdjacencyGraph::default(), vec![]),
        ] {
            assert_eq!(
                community_ids(
                    &execute_modularity_optimization(&boundary, AlgorithmLimits::default())
                        .unwrap()
                ),
                expected
            );
        }
    }

    #[test]
    fn modularity_optimization_uses_shared_controls_and_rust_registration() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_modularity_optimization(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::ModularityOptimization),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|entry| {
                entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::ModularityOptimization)
            })
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);

        let invalid = vec![
            BTreeMap::from([(1, f64::INFINITY)]),
            BTreeMap::from([(0, f64::INFINITY)]),
        ];
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        assert_eq!(
            local_moves_from(&invalid, None, "Modularity optimization", &control),
            Err(execution(
                "Modularity optimization total edge weight is not finite"
            ))
        );

        let graph = AdjacencyGraph::with_test_counts(2, 500_000);
        let cancellation = AlgorithmCancellation::default();
        let cancel = cancellation.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            modularity_optimization_communities(
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            )
        });
        started_rx.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        cancel.cancel();
        assert_eq!(worker.join().unwrap(), Err(AlgorithmError::Cancelled));
    }

    #[test]
    fn fastgreedy_selects_the_deterministic_best_agglomerative_partition() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[
                (0, 1),
                (1, 0),
                (0, 1),
                (1, 2),
                (2, 0),
                (2, 3),
                (3, 4),
                (4, 5),
                (5, 3),
                (5, 5),
            ],
        );
        let first = execute_fastgreedy(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            execute_fastgreedy(&graph, AlgorithmLimits::default()).unwrap(),
            first
        );
        for (boundary, expected) in [
            (AdjacencyGraph::with_test_edges(3, &[]), vec![0, 1, 2]),
            (AdjacencyGraph::default(), vec![]),
        ] {
            assert_eq!(
                community_ids(&execute_fastgreedy(&boundary, AlgorithmLimits::default()).unwrap()),
                expected
            );
        }
    }

    #[test]
    fn fastgreedy_updates_only_affected_candidates_without_stale_merges() {
        let edges: Vec<_> = (0..63).map(|node| (node, node + 1)).collect();
        let graph = AdjacencyGraph::with_test_edges(64, &edges);
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let (weights, _) = normalized_communities(&graph, &control).unwrap();
        let mut merges = Vec::new();

        let result = fastgreedy_from_weights_with_updates(
            &weights,
            &control,
            || {},
            |left, right, updates| merges.push((left, right, updates)),
        )
        .unwrap();

        let mut live: BTreeSet<_> = (0..64).collect();
        for &(left, right, _) in &merges {
            assert!(live.contains(&left), "surviving community must be live");
            assert!(live.remove(&right), "absorbed community must be live");
        }
        assert_eq!(merges.len(), 63, "each legal path merge happens once");
        assert!(
            merges.iter().all(|&(_, _, updates)| updates <= 2),
            "path merges update only the surviving community's neighbors: {merges:?}"
        );
        let expected: Vec<_> = (0..64).map(|node| node / 8).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn fastgreedy_uses_shared_controls_cancellation_and_rust_registration() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_fastgreedy(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::FastGreedy),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::FastGreedy))
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);

        let invalid = vec![
            BTreeMap::from([(1, f64::INFINITY)]),
            BTreeMap::from([(0, f64::INFINITY)]),
        ];
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        assert_eq!(
            fastgreedy_from_weights(&invalid, &control, || {}),
            Err(execution("Fastgreedy modularity is not finite"))
        );

        let cancellation = AlgorithmCancellation::default();
        let cancel = cancellation.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let control = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
            let mut rendezvous = Some((started_tx, resume_rx));
            fastgreedy_communities_with_progress(&graph, &control, || {
                if let Some((started, resume)) = rendezvous.take() {
                    started.send(()).unwrap();
                    resume.recv().unwrap();
                }
            })
        });
        started_rx.recv().unwrap();
        cancel.cancel();
        resume_tx.send(()).unwrap();
        assert_eq!(worker.join().unwrap(), Err(AlgorithmError::Cancelled));
    }

    #[test]
    fn infomap_flow_normalizes_topology_and_orders_weak_components() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(5, &[(0, 0), (0, 1), (0, 1), (1, 0), (2, 3)]);
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let flow = infomap_flow(&graph, &control).unwrap();

        assert!(flow.directed);
        assert_eq!(flow.outgoing[0], BTreeSet::from([1]));
        assert_eq!(flow.incident[3], BTreeSet::from([2]));
        assert_eq!(flow.components, [vec![0, 1], vec![2, 3], vec![4]]);
    }

    #[test]
    fn infomap_stationary_flow_and_map_equation_are_hand_verifiable() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]);
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let flow = infomap_flow(&graph, &control).unwrap();
        let component = &flow.components[0];
        let visits =
            infomap_stationary(component, &flow.outgoing, flow.directed, &control).unwrap();

        assert!(!flow.directed);
        assert_eq!(visits, [0.25, 0.5, 0.25]);
        let singleton =
            infomap_codelength(component, &flow.outgoing, &visits, false, &[0, 1, 2]).unwrap();
        let joined =
            infomap_codelength(component, &flow.outgoing, &visits, false, &[0, 0, 0]).unwrap();
        assert!(joined < singleton);
    }

    #[test]
    fn infomap_directed_flow_is_deterministic_bounded_and_finite() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let flow = infomap_flow(&graph, &control).unwrap();
        let first =
            infomap_stationary(&flow.components[0], &flow.outgoing, true, &control).unwrap();
        let second =
            infomap_stationary(&flow.components[0], &flow.outgoing, true, &control).unwrap();
        assert_eq!(first, second);
        assert!(first.iter().all(|value| value.is_finite() && *value > 0.0));
        assert!((first.iter().sum::<f64>() - 1.0).abs() <= 1e-12);

        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            infomap_stationary(&flow.components[0], &flow.outgoing, true, &limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            infomap_flow(
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        ));
        assert_eq!(
            infomap_codelength(
                &flow.components[0],
                &flow.outgoing,
                &[f64::INFINITY, 0.0, 0.0],
                true,
                &[0, 1, 2],
            ),
            Err(execution("Infomap codelength is not finite"))
        );
    }

    #[test]
    fn infomap_selects_a_stable_two_level_flow_partition() {
        let graph = AdjacencyGraph::with_test_edges(5, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
        let first = execute_infomap(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&first), [0, 0, 1, 1, 2]);
        assert_eq!(
            execute_infomap(&graph, AlgorithmLimits::default()).unwrap(),
            first
        );
        let directed = AdjacencyGraph::with_test_directed_edges(
            6,
            &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)],
        );
        assert_eq!(
            community_ids(&execute_infomap(&directed, AlgorithmLimits::default()).unwrap()),
            [0, 0, 0, 1, 1, 1]
        );
        for (boundary, expected) in [
            (AdjacencyGraph::with_test_edges(3, &[]), vec![0, 1, 2]),
            (AdjacencyGraph::default(), vec![]),
        ] {
            assert_eq!(
                community_ids(&execute_infomap(&boundary, AlgorithmLimits::default()).unwrap()),
                expected
            );
        }
    }

    #[test]
    fn infomap_uses_shared_controls_and_single_rust_registration() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        assert!(matches!(
            execute_infomap(
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
        let setup =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let flow = infomap_flow(&graph, &setup).unwrap();
        let visits = infomap_stationary(&flow.components[0], &flow.outgoing, true, &setup).unwrap();
        let search = InfomapSearch {
            component: &flow.components[0],
            flow: &flow,
            visits: &visits,
        };
        let cancellation = AlgorithmCancellation::default();
        let cancel = cancellation.clone();
        let mut assignment = vec![0, 1, 2];
        assert_eq!(
            search.node_sweep(
                &mut assignment,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
                || cancel.cancel(),
            ),
            Err(AlgorithmError::Cancelled)
        );
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::InfoMap))
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
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
    fn components_assigns_stable_ids_for_weak_components() {
        let graph = AdjacencyGraph::with_test_edges(6, &[(0, 1), (2, 3), (2, 3), (3, 3)]);
        let output = execute_components(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(
            output.rows(),
            [0_i64, 0, 1, 1, 2, 3]
                .into_iter()
                .enumerate()
                .map(|(node, community)| vec![
                    AlgorithmValue::Uuid((node as u128).to_be_bytes()),
                    AlgorithmValue::Int64(community),
                ])
                .collect::<Vec<_>>()
        );
        assert_eq!(
            output.schema,
            Algorithm::Cluster(ClusterAlgorithm::Components).result_schema()
        );

        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        assert_eq!(registry.capabilities()[0].dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn components_parallel_path_preserves_serial_output_across_thread_matrix() {
        let mut edges = Vec::new();
        for start in [0_u64, 5_000] {
            for node in start..start + 4_999 {
                edges.push((node, node + 1));
                edges.push((node + 1, node));
            }
        }
        let graph = AdjacencyGraph::with_test_edges(10_000, &edges);
        let serial = execute_components(&graph, AlgorithmLimits::default()).unwrap();
        let mut expected = vec![0_i64; 5_000];
        expected.extend(vec![1_i64; 5_000]);
        assert_eq!(community_ids(&serial), expected);

        for threads in [2, 4, 8] {
            let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
            let control = AlgorithmControl::new(
                AlgorithmLimits::default().with_compute_threads(threads),
                AlgorithmCancellation::default(),
            )
            .with_compute_pool(pool);
            assert!(matches!(
                select_components_path(&control, graph.node_ids().len(), graph.edge_entry_count()),
                ComponentsExecutionPath::Parallel { .. }
            ));
            assert_eq!(
                execute_components_with_threads(&graph, threads).unwrap(),
                serial
            );
        }
    }

    #[test]
    fn components_parallel_selector_uses_private_pool_and_canonical_chunks() {
        assert_eq!(components_source_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(components_source_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(components_source_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(
            components_source_chunks(8, 4),
            vec![(0, 2), (2, 4), (4, 6), (6, 8)]
        );
        assert_eq!(components_source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);

        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let serial = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_components_path(&serial, graph.node_ids().len(), graph.edge_entry_count()),
            ComponentsExecutionPath::Serial
        );

        let pool = Arc::new(crate::ComputePool::new(4).unwrap());
        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool);
        assert_eq!(
            select_components_path(&parallel, graph.node_ids().len(), graph.edge_entry_count()),
            ComponentsExecutionPath::Serial
        );
    }

    #[test]
    fn components_handles_empty_graphs_and_shared_limits() {
        assert!(
            execute_components(&AdjacencyGraph::default(), AlgorithmLimits::default())
                .unwrap()
                .rows()
                .is_empty()
        );
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_components(
                &graph,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::NodeLimit {
                observed: 3,
                limit: 2,
            })
        );
        assert_eq!(
            execute_components(
                &graph,
                AlgorithmLimits {
                    edges: 1,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::EdgeLimit {
                observed: 2,
                limit: 1,
            })
        );
        assert_eq!(
            execute_components(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                }
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::Components),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
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

    #[test]
    fn louvain_finds_stable_multilevel_communities() {
        let graph = AdjacencyGraph::with_test_edges(
            6,
            &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)],
        );
        let first = execute_louvain(&graph, AlgorithmLimits::default()).unwrap();
        assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1]);
        assert_eq!(
            execute_louvain(&graph, AlgorithmLimits::default()).unwrap(),
            first
        );
        assert_eq!(
            first.schema,
            Algorithm::Cluster(ClusterAlgorithm::Louvain).result_schema()
        );
        assert_eq!(
            first.rows()[0][0],
            AlgorithmValue::Uuid(0_u128.to_be_bytes())
        );
    }

    #[test]
    fn louvain_normalizes_multigraphs_and_retains_boundaries() {
        let simple =
            AdjacencyGraph::with_test_edges(7, &[(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)]);
        let noisy = AdjacencyGraph::with_test_edges(
            7,
            &[
                (0, 1),
                (1, 0),
                (0, 1),
                (1, 2),
                (2, 0),
                (0, 0),
                (3, 4),
                (4, 5),
                (5, 3),
                (5, 5),
            ],
        );
        let expected = [0, 0, 0, 1, 1, 1, 2];
        assert_eq!(
            community_ids(&execute_louvain(&simple, AlgorithmLimits::default()).unwrap()),
            expected
        );
        assert_eq!(
            community_ids(&execute_louvain(&noisy, AlgorithmLimits::default()).unwrap()),
            expected
        );
        assert_eq!(
            community_ids(
                &execute_louvain(
                    &AdjacencyGraph::with_test_edges(3, &[]),
                    AlgorithmLimits::default(),
                )
                .unwrap()
            ),
            [0, 1, 2]
        );
        assert!(
            execute_louvain(&AdjacencyGraph::default(), AlgorithmLimits::default())
                .unwrap()
                .rows()
                .is_empty()
        );
    }

    #[test]
    fn louvain_uses_shared_controls_and_single_rust_registration() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_louvain(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let mut registry = AlgorithmRegistry::default();
        register_cluster_algorithms(&mut registry).unwrap();
        assert_eq!(
            registry.execute(
                Algorithm::Cluster(ClusterAlgorithm::Louvain),
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        let capabilities = registry.capabilities();
        assert_eq!(capabilities.len(), ClusterAlgorithm::ALL.len());
        let capability = capabilities
            .into_iter()
            .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::Louvain))
            .unwrap();
        assert_eq!(capability.backend, "rust");
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn louvain_observes_cancellation_during_high_degree_work() {
        let graph = AdjacencyGraph::with_test_counts(2, 500_000);
        let cancellation = AlgorithmCancellation::default();
        let cancel = cancellation.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut registry = AlgorithmRegistry::default();
            register_cluster_algorithms(&mut registry).unwrap();
            started_tx.send(()).unwrap();
            result_tx
                .send(registry.execute(
                    Algorithm::Cluster(ClusterAlgorithm::Louvain),
                    &graph,
                    &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
                ))
                .unwrap();
        });
        started_rx.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(
            result_rx.try_recv().is_err(),
            "execution finished before cancellation"
        );
        cancel.cancel();
        assert_eq!(result_rx.recv().unwrap(), Err(AlgorithmError::Cancelled));
        worker.join().unwrap();
    }
}
