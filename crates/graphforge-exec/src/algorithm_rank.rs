//! Rank registration, public invocation, and shared deterministic controls.
//!
//! Private algorithm owners retain iterative rank, distance centrality,
//! neighborhood scoring, triangle/core scoring, and CELF execution with direct
//! tests. Serial contribution order, ordered worker merges, instance-owned pools,
//! resource checkpoints, and bit-identical fingerprints remain algorithm-owned.
//! CELF and k-core remain serial under every compute-thread budget.

mod degree;
use degree::Degree;
mod pagerank;
use pagerank::PageRank;
mod betweenness;
use betweenness::Betweenness;
mod closeness;
use closeness::Closeness;
mod harmonic_closeness;
use harmonic_closeness::HarmonicCloseness;
mod eigenvector;
use eigenvector::Eigenvector;
mod article_rank;
use article_rank::ArticleRank;
mod hits;
use hits::{HitsAuthority, HitsHub};
mod celf;
use celf::Celf;
mod clustering_coefficient;
use clustering_coefficient::ClusteringCoefficient;
mod triangles;
use triangles::Triangles;
mod k_core;
use k_core::KCore;
mod preferential_attachment;
use preferential_attachment::PreferentialAttachment;
mod adamic_adar;
use adamic_adar::AdamicAdar;
mod common_neighbors;
use common_neighbors::CommonNeighbors;
mod resource_allocation;
use resource_allocation::ResourceAllocation;
mod total_neighbors;
use total_neighbors::TotalNeighbors;

use graphforge_value::EntityTypeSelection;
use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, RankAlgorithm};
use graphforge_core::{GfError, OntologyMode, RankOptions};
use graphforge_ir::Direction;
use rayon::prelude::*;

use crate::AdjacencyProvider;
use crate::algorithm_dispatch::{
    AlgorithmCancellation, AlgorithmCapability, AlgorithmControl, AlgorithmError, AlgorithmLimits,
    AlgorithmOutput, AlgorithmRegistry, AlgorithmValue, DependencyReview, RustAlgorithm,
};
use crate::algorithm_graph::{AdjacencyGraph, AdjacencySelection, export_adjacency};
use crate::algorithm_k_core::k_core_numbers;
use crate::algorithm_neighbors::{simple_neighbors, simple_undirected_neighbors};
use crate::algorithm_output::{
    materialize_node_properties_with_batch_size, shape_algorithm_output,
};

const BUILTIN_REVIEW: DependencyReview = DependencyReview {
    implementation: "graphforge-exec built-in",
    license: "Apache-2.0",
    maintenance: "GraphForge workspace",
    security: "workspace cargo-deny and CodeQL",
    binary_size: "no additional dependency",
    determinism: "stable surrogate-ordered rows and iterative accumulation",
    platforms: "Rust workspace targets",
};

/// Selected adjacency entries below which PageRank stays on the serial path (#343).
///
/// Keeps accepted small fixtures and micro-invocations off the worker pool; above
/// this, destination-owned parallel updates amortize scheduling on typical
/// embedded hosts. Numeric results remain identical either way.
pub const PAGERANK_PARALLEL_CROSSOVER_EDGES: u64 = 4_096;
/// Estimated local neighbor-pair probes below which clustering coefficient stays serial (#504).
///
/// Keeps small fixtures and sparse public invocations off the worker pool; above this,
/// independent node-local triangle/wedge counting amortizes private-pool scheduling.
pub const CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK: u64 = 32_768;
/// Selected node count below which triangle ranking stays on the serial path (#515).
///
/// Keeps small fixtures and sparse micro-invocations off the worker pool; above
/// this, dense-ordinal node partitions amortize scheduling on typical embedded
/// hosts. Exact triangle counts remain identical either way.
pub const TRIANGLES_PARALLEL_CROSSOVER_NODES: usize = 256;
/// Selected nodes below which Degree stays on the serial path (#506).
///
/// Degree work is O(1) per node (neighbor-length lookup + normalize). Parallel
/// scheduling only amortizes once node count clears this threshold on typical
/// embedded hosts; smaller fixtures stay serial with no pool tax. Numeric
/// results remain identical either way.
pub const DEGREE_PARALLEL_CROSSOVER_NODES: usize = 4_096;
/// Estimated Brandes source work below which betweenness stays serial (#501).
///
/// The estimate is `sources * (selected_nodes + selected_adjacency_entries)`.
/// The crossover keeps small fixtures off the private pool; parallel workers
/// still run each source's Brandes BFS serially and reduce in source order.
pub const BETWEENNESS_PARALLEL_CROSSOVER_WORK: u64 = 65_536;
/// Algebraic source/neighbor work below which preferential attachment stays serial (#512).
///
/// Chosen from release-mode serial-vs-parallel timings on this M4 agent host
/// (4x Xeon vCPU, directed ring-lattice fixtures, 4 private workers; see
/// ignored `measure_preferential_attachment_parallel_crossover`):
/// - ~17k work units: effectively neutral (~0.99x serial)
/// - ~68k work units: effectively neutral (~1.00x serial)
/// - ~266k work units: first modest measured win (~0.96x serial)
/// - >=2.1M work units: modest win improves to ~0.93x serial
///
/// `262_144` is the nearest power-of-two work estimate below the measured win
/// boundary. Exact integer scores and row order remain identical on either path.
pub const PREFERENTIAL_ATTACHMENT_PARALLEL_CROSSOVER_WORK: u64 = 262_144;
/// Estimated pair/intersection work below which common-neighbors stays serial (#505).
///
/// Chosen from manual serial-vs-parallel timings on this M4 agent host
/// (4x Xeon vCPU, directed ring-lattice fixtures, 4 private workers, debug
/// test profile after a clean target-dir build; see
/// ignored `measure_common_neighbors_parallel_crossover`):
/// - ~230k estimated units: parallel still slower (~1.80x serial)
/// - ~540k estimated units: parallel still slower (~1.20x serial)
/// - ~1.2M estimated units: first clear win (~0.70x serial)
/// - >=2.1M estimated units: >=1.8x speedup
///
/// `1_048_576` is the smallest power-of-two work estimate below that measured
/// win boundary. Each source keeps serial candidate/intersection order, so
/// exact counts remain identical on either path.
pub const COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK: u64 = 1_048_576;
/// Estimated pair/intersection work below which total-neighbors stays serial (#514).
///
/// Chosen from manual serial-vs-parallel timings on this M4 agent host
/// (4x Xeon vCPU, directed ring-lattice fixtures, 4 private workers, debug
/// test profile after a clean target-dir build; see
/// ignored `measure_total_neighbors_parallel_crossover`):
/// - ~230k estimated units: parallel still slower (~1.06x serial)
/// - ~540k estimated units: parallel still slower (~1.31x serial)
/// - ~1.2M estimated units: first clear win (~0.84x serial)
/// - >=2.1M estimated units: >=1.4x speedup
///
/// `1_048_576` is the smallest power-of-two work estimate below that measured
/// win boundary. Each source keeps serial candidate/intersection order, so
/// exact union counts remain identical on either path.
pub const TOTAL_NEIGHBORS_PARALLEL_CROSSOVER_WORK: u64 = 1_048_576;
/// Estimated pair/intersection work below which Adamic-Adar stays serial (#499).
///
/// Chosen from release-mode serial-vs-parallel timings on this M4 agent host
/// (4x Xeon vCPU, directed ring-lattice fixtures, 4 private workers; see
/// ignored `measure_adamic_adar_parallel_crossover`):
/// - ~230k estimated units: parallel still neutral/slower (pool scheduling tax)
/// - ~540k estimated units: first clear win (~0.61x serial)
/// - >=2.1M estimated units: >=2.8x speedup
///
/// `524_288` is the smallest power-of-two work estimate below that measured win
/// boundary. Each source keeps serial candidate/intersection order, so exact
/// scores remain bit-identical on either path.
pub const ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK: u64 = 524_288;
/// Estimated edge visits below which closeness stays on the serial path (#503).
///
/// Closeness has independent source BFS work. Release-mode measurements on the
/// M4 agent host showed the private-pool scheduling and merge tax losing below
/// roughly 32k estimated edge visits, first clear wins around 65k, and stable
/// wins beyond that on dense-ring fixtures. Numeric results remain identical
/// because each BFS is still serial and source scores merge in ordinal order.
pub const CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS: u64 = 65_536;
/// Estimated edge visits below which harmonic closeness stays serial (#508).
///
/// Harmonic closeness has independent source BFS work. The threshold mirrors
/// the #503 closeness disposition: private-pool scheduling and merge overhead
/// lost below roughly 32k estimated edge visits, first cleared wins around 65k,
/// and stayed beneficial beyond that on dense-ring fixtures. Score arithmetic is
/// unchanged because each BFS remains serial and source scores merge in ordinal
/// order.
pub const HARMONIC_CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS: u64 = 65_536;

/// Estimated pair/intersection work below which resource allocation stays serial (#513).
///
/// Uses the same source-owned partition regime as Adamic-Adar: each worker owns
/// complete source ordinals while candidate order, intersections, reciprocal
/// discounts, and compensated summation remain serial per source. Release-mode
/// measurements on this M4 agent host with 4 private workers showed ~230k units
/// still neutral and ~540k units as the first clear win, so this keeps small
/// fixtures off the pool while naming the measured crossover used by docs and
/// tests.
pub const RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK: u64 = 524_288;

/// Selected adjacency entries below which eigenvector stays on the serial path (#507).
///
/// The shifted `A^T + I` update has independent destination rows, but the inbound
/// CSR build and private-pool scheduling only amortize on edge-heavy workloads.
/// Above this measured crossover, destination-owned parallel updates preserve the
/// exact serial contribution order for each destination. The first two required
/// power iterations stay serial so quickly converging regular graphs avoid the
/// inbound CSR setup cost entirely.
pub const EIGENVECTOR_PARALLEL_CROSSOVER_EDGES: u64 = 8_192;
/// Selected adjacency entries below which ArticleRank stays on the serial path (#500).
///
/// Release-mode shared-pool timings on the M4 agent host first show a clear
/// parallel win at 131k selected entries; smaller fixtures stay serial because
/// their tiny deltas are within timing noise.
pub const ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES: u64 = 131_072;
/// Selected adjacency entries below which HITS stays on the serial path (#510).
///
/// HITS performs two full sparse matrix-vector phases per fixed iteration, so
/// this keeps small invocations off the worker pool while large embedded
/// workloads can partition independent dense-ordinal node updates.
pub const HITS_PARALLEL_CROSSOVER_EDGES: u64 = 4_096;

fn destination_chunks(nodes: usize, threads: usize) -> Vec<(usize, usize)> {
    ordinal_chunks(nodes, threads)
}

fn source_chunks(nodes: usize, threads: usize) -> Vec<(usize, usize)> {
    ordinal_chunks(nodes, threads)
}

fn ordinal_chunks(nodes: usize, threads: usize) -> Vec<(usize, usize)> {
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

fn run_rank_on_pool<R>(
    pool: &crate::ComputePool,
    kernel: &str,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution(format!("{kernel} worker panicked"))),
    }
}

/// Prefer the lowest-index chunk error so parallel failures stay deterministic.
fn first_chunk_error<T>(results: Vec<Result<T, AlgorithmError>>) -> Result<Vec<T>, AlgorithmError> {
    let mut ok = Vec::with_capacity(results.len());
    let mut first_error: Option<AlgorithmError> = None;
    for result in results {
        match result {
            Ok(value) if first_error.is_none() => ok.push(value),
            Err(error) if first_error.is_none() => first_error = Some(error),
            Ok(_) | Err(_) => {}
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(ok),
    }
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn has_arc(outgoing: &[Vec<usize>], source: usize, target: usize) -> bool {
    outgoing[source].binary_search(&target).is_ok()
}

fn rank_scores_output(
    algorithm: Algorithm,
    graph: &AdjacencyGraph,
    scores: Vec<f64>,
    control: &AlgorithmControl,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut sink = control.output_sink(algorithm)?;
    for (&node, score) in graph.node_ids().iter().zip(scores) {
        let uuid = graph
            .node_uuid(node)
            .ok_or_else(|| execution("selected node has no UUID identity"))?;
        sink.append_row(&[AlgorithmValue::Uuid(uuid), AlgorithmValue::Float64(score)])?;
    }
    sink.finish()
}

pub(crate) fn register_rank_algorithms(
    registry: &mut AlgorithmRegistry,
) -> Result<(), AlgorithmError> {
    registry.register(Arc::new(Degree))?;
    registry.register(Arc::new(PageRank))?;
    registry.register(Arc::new(Betweenness))?;
    registry.register(Arc::new(Closeness))?;
    registry.register(Arc::new(HarmonicCloseness))?;
    registry.register(Arc::new(Eigenvector))?;
    registry.register(Arc::new(ArticleRank))?;
    registry.register(Arc::new(HitsHub))?;
    registry.register(Arc::new(HitsAuthority))?;
    registry.register(Arc::new(Celf))?;
    registry.register(Arc::new(ClusteringCoefficient))?;
    registry.register(Arc::new(Triangles))?;
    registry.register(Arc::new(KCore))?;
    registry.register(Arc::new(PreferentialAttachment))?;
    registry.register(Arc::new(AdamicAdar))?;
    registry.register(Arc::new(CommonNeighbors))?;
    registry.register(Arc::new(ResourceAllocation))?;
    registry.register(Arc::new(TotalNeighbors))
}

/// Execute a typed rank algorithm through Rust dispatch and return its
/// canonical UUID-only Arrow batch with node properties materialized.
///
/// # Errors
/// Returns structured validation/execution errors for invalid relationship
/// selection, unavailable algorithms, adjacency reads, limits, or shaping.
pub fn rank_algorithm(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    property_stems: &[String],
    options: &RankOptions,
) -> Result<RecordBatch, GfError> {
    rank_algorithm_with_limits(
        provider,
        dir,
        mode,
        label,
        property_stems,
        options,
        AlgorithmLimits::default(),
    )
}

/// Execute rank with an explicit output/memory shaping policy (#341).
pub fn rank_algorithm_with_limits(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    property_stems: &[String],
    options: &RankOptions,
    limits: AlgorithmLimits,
) -> Result<RecordBatch, GfError> {
    rank_algorithm_with_compute(
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

/// Execute rank with shaping limits and an optional private compute pool (#343).
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors rank_algorithm_with_limits plus the instance compute pool handle"
)]
pub fn rank_algorithm_with_compute(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    property_stems: &[String],
    options: &RankOptions,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<RecordBatch, GfError> {
    let graph = rank_projection(provider, dir, mode, label, options)?;
    let algorithm = Algorithm::Rank(options.by);
    let output = execute_rank_with_compute(&graph, algorithm, limits, compute)?;
    let batch = shape_algorithm_output(algorithm, &output)?;
    materialize_node_properties_with_batch_size(dir, property_stems, &batch, limits.batch_size)
        .map_err(Into::into)
}

/// Fingerprint the exact logical topology consumed by a rank invocation.
///
/// # Errors
/// Returns the same projection and selector failures as [`rank_algorithm`].
pub fn rank_projection_fingerprint(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    options: &RankOptions,
) -> Result<[u8; 32], GfError> {
    rank_projection(provider, dir, mode, label, options)
        .and_then(|graph| graph.descriptor_projection_fingerprint())
        .map(|fingerprint| *fingerprint.as_bytes())
}

fn rank_projection(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    options: &RankOptions,
) -> Result<AdjacencyGraph, GfError> {
    let via = options.via.as_deref().unwrap_or("*");
    if via.is_empty() || via.trim() != via || via.chars().any(char::is_control) {
        return Err(GfError::Validation(format!(
            "invalid rank relationship selector {via:?}"
        )));
    }
    let direction = if options.directed {
        Direction::Out
    } else {
        Direction::Undirected
    };
    export_adjacency(
        provider,
        dir,
        mode,
        AdjacencySelection {
            label,
            via,
            direction,
            weight: None,
        },
    )
}

fn execute_rank_with_compute(
    graph: &AdjacencyGraph,
    algorithm: Algorithm,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    let mut control = AlgorithmControl::new(limits, AlgorithmCancellation::default());
    if let Some(pool) = compute {
        control = control.with_compute_pool(pool);
    }
    registry.execute(algorithm, graph, &control)
}

fn exact_u32(value: usize, kind: &str) -> Result<u32, AlgorithmError> {
    u32::try_from(value).map_err(|_| execution(format!("{kind} exceeds supported score range")))
}

fn exact_u64_as_f64(value: u64, kind: &str) -> Result<f64, AlgorithmError> {
    const MAX_EXACT_INTEGER: u64 = 1_u64 << 53;
    if value > MAX_EXACT_INTEGER {
        return Err(execution(format!("{kind} exceeds supported score range")));
    }
    // Guarded by the exact IEEE-754 integer range above.
    #[allow(clippy::cast_precision_loss)]
    let converted = value as f64;
    Ok(converted)
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn hits_hub_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("HITS hub score must be Float64"),
            })
            .collect()
    }

    pub(super) fn assert_scores_close(actual: &[f64], expected: &[f64]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert!(
                (actual - expected).abs() < 1.0e-12,
                "{actual} != {expected}"
            );
        }
    }

    #[test]
    fn source_chunks_cover_canonical_ranges() {
        assert_eq!(source_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(source_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(source_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(source_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
        assert_eq!(source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
    }
}
