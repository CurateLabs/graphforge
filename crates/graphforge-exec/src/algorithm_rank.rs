//! Rust-owned rank handlers registered under the shared algorithm dispatch contract.
//!
//! PageRank (#343), clustering coefficient (#504), triangles (#515), Degree
//! Closeness (#503) may partition independent source BFS work across the same private pool; each BFS stays serial and worker scores merge by source ordinal.
//! (#506), and betweenness (#501) may partition independent score updates across
//! the instance-owned private compute pool while preserving serial contribution
//! order, ordered merges, reductions, and bit-identical fingerprints.
//! PageRank (#343) and eigenvector (#507) may partition destination-owned score
//! updates across the instance-owned private compute pool while preserving the
//! serial contribution order, serial convergence reductions, and bit-identical
//! PageRank (#343) and HITS hub (#510) may partition destination-owned score
//! updates across the instance-owned private compute pool while preserving the
//! serial contribution order, deterministic reductions, and bit-identical
//! fingerprints.
//! PageRank (#343), HITS hub (#510), and HITS authority (#509) may partition
//! destination-owned score updates across the instance-owned private compute
//! pool while preserving the serial contribution order, deterministic
//! reductions, and bit-identical fingerprints.
//! PageRank (#343) and ArticleRank (#500) may partition destination-owned score
//! updates across the instance-owned private compute pool while preserving the
//! serial contribution order, reductions, and bit-identical fingerprints.

use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, RankAlgorithm};
use graphforge_core::{GfError, OntologyMode, RankOptions, TypeId};
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

struct Degree;

struct PageRank;

struct Betweenness;

struct Closeness;

struct HarmonicCloseness;

struct Eigenvector;

struct ArticleRank;

struct HitsHub;

struct HitsAuthority;

struct Celf;

struct ClusteringCoefficient;

struct Triangles;

struct KCore;

struct PreferentialAttachment;

struct AdamicAdar;

struct CommonNeighbors;

struct ResourceAllocation;

struct TotalNeighbors;

const PAGERANK_DAMPING: f64 = 0.85;
const PAGERANK_TOLERANCE: f64 = 1.0e-10;
/// Selected adjacency entries below which PageRank stays on the serial path (#343).
///
/// Keeps accepted small fixtures and micro-invocations off the worker pool; above
/// this, destination-owned parallel updates amortize scheduling on typical
/// embedded hosts. Numeric results remain identical either way.
pub const PAGERANK_PARALLEL_CROSSOVER_EDGES: u64 = 4_096;
const PAGERANK_CHECKPOINT_DESTINATIONS: usize = 4_096;
/// Estimated local neighbor-pair probes below which clustering coefficient stays serial (#504).
///
/// Keeps small fixtures and sparse public invocations off the worker pool; above this,
/// independent node-local triangle/wedge counting amortizes private-pool scheduling.
pub const CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK: u64 = 32_768;
const CLUSTERING_COEFFICIENT_CHECKPOINT_WORK: usize = 1_024;
/// Selected node count below which triangle ranking stays on the serial path (#515).
///
/// Keeps small fixtures and sparse micro-invocations off the worker pool; above
/// this, dense-ordinal node partitions amortize scheduling on typical embedded
/// hosts. Exact triangle counts remain identical either way.
pub const TRIANGLES_PARALLEL_CROSSOVER_NODES: usize = 256;
const TRIANGLES_CHECKPOINT_PAIRS: usize = 1_024;
/// Selected nodes below which Degree stays on the serial path (#506).
///
/// Degree work is O(1) per node (neighbor-length lookup + normalize). Parallel
/// scheduling only amortizes once node count clears this threshold on typical
/// embedded hosts; smaller fixtures stay serial with no pool tax. Numeric
/// results remain identical either way.
pub const DEGREE_PARALLEL_CROSSOVER_NODES: usize = 4_096;
const DEGREE_CHECKPOINT_NODES: usize = 1_024;
/// Estimated Brandes source work below which betweenness stays serial (#501).
///
/// The estimate is `sources * (selected_nodes + selected_adjacency_entries)`.
/// The crossover keeps small fixtures off the private pool; parallel workers
/// still run each source's Brandes BFS serially and reduce in source order.
pub const BETWEENNESS_PARALLEL_CROSSOVER_WORK: u64 = 65_536;
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
const COMMON_NEIGHBORS_CHECKPOINT_INTERVAL: usize = 1_024;
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
const ADAMIC_ADAR_CHECKPOINT_INTERVAL: usize = 1_024;
/// Estimated edge visits below which closeness stays on the serial path (#503).
///
/// Closeness has independent source BFS work. Release-mode measurements on the
/// M4 agent host showed the private-pool scheduling and merge tax losing below
/// roughly 32k estimated edge visits, first clear wins around 65k, and stable
/// wins beyond that on dense-ring fixtures. Numeric results remain identical
/// because each BFS is still serial and source scores merge in ordinal order.
pub const CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS: u64 = 65_536;
const CLOSENESS_CHECKPOINT_EDGES: usize = 1_024;

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
const RESOURCE_ALLOCATION_CHECKPOINT_INTERVAL: usize = 1_024;

const EIGENVECTOR_MAX_ITERATIONS: usize = 20;
const EIGENVECTOR_TOLERANCE: f64 = 1.0e-7;
/// Selected adjacency entries below which eigenvector stays on the serial path (#507).
///
/// The shifted `A^T + I` update has independent destination rows, but the inbound
/// CSR build and private-pool scheduling only amortize on edge-heavy workloads.
/// Above this measured crossover, destination-owned parallel updates preserve the
/// exact serial contribution order for each destination. The first two required
/// power iterations stay serial so quickly converging regular graphs avoid the
/// inbound CSR setup cost entirely.
pub const EIGENVECTOR_PARALLEL_CROSSOVER_EDGES: u64 = 8_192;
const EIGENVECTOR_CHECKPOINT_DESTINATIONS: usize = 4_096;
const EIGENVECTOR_SERIAL_WARMUP_ITERATIONS: usize = 2;
const ARTICLE_RANK_DAMPING: f64 = 0.85;
const ARTICLE_RANK_ALPHA: f64 = 1.0 - ARTICLE_RANK_DAMPING;
const ARTICLE_RANK_MAX_ITERATIONS: usize = 20;
const ARTICLE_RANK_TOLERANCE: f64 = 1.0e-7;
/// Selected adjacency entries below which ArticleRank stays on the serial path (#500).
///
/// Release-mode shared-pool timings on the M4 agent host first show a clear
/// parallel win at 131k selected entries; smaller fixtures stay serial because
/// their tiny deltas are within timing noise.
pub const ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES: u64 = 131_072;
const ARTICLE_RANK_CHECKPOINT_DESTINATIONS: usize = 4_096;
const HITS_ITERATIONS: usize = 20;
/// Selected adjacency entries below which HITS stays on the serial path (#510).
///
/// HITS performs two full sparse matrix-vector phases per fixed iteration, so
/// this keeps small invocations off the worker pool while large embedded
/// workloads can partition independent dense-ordinal node updates.
pub const HITS_PARALLEL_CROSSOVER_EDGES: u64 = 4_096;
const HITS_CHECKPOINT_EDGES: usize = 4_096;
const CELF_SIMULATIONS: u32 = 100;
const CELF_LIVE_EDGE_THRESHOLD: u64 = u64::MAX / 10;

impl RustAlgorithm for Degree {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Degree),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let node_ids = graph.node_ids();
        let denominator = exact_u32(node_ids.len().saturating_sub(1).max(1), "node count")?;
        let algorithm = Algorithm::Rank(RankAlgorithm::Degree);
        let mut sink = control.output_sink(algorithm)?;
        let path = select_degree_path(control, node_ids.len());
        match path {
            DegreeExecutionPath::Serial => {
                for (index, &node_id) in node_ids.iter().enumerate() {
                    if index.is_multiple_of(DEGREE_CHECKPOINT_NODES) {
                        control.checkpoint()?;
                    }
                    let uuid = graph
                        .node_uuid(node_id)
                        .ok_or_else(|| execution("selected node has no UUID identity"))?;
                    let degree = exact_u32(graph.neighbors(node_id).len(), "node degree")?;
                    sink.append_row(&[
                        AlgorithmValue::Uuid(uuid),
                        AlgorithmValue::Float64(f64::from(degree) / f64::from(denominator)),
                    ])?;
                }
            }
            DegreeExecutionPath::Parallel { .. } => {
                let rows = degree_scores_parallel(graph, denominator, control)?;
                for (uuid, score) in rows {
                    sink.append_row(&[AlgorithmValue::Uuid(uuid), AlgorithmValue::Float64(score)])?;
                }
            }
        }
        sink.finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DegreeExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Choose serial vs private-pool parallel execution for a Degree workload (#506).
pub(crate) fn select_degree_path(control: &AlgorithmControl, nodes: usize) -> DegreeExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes < DEGREE_PARALLEL_CROSSOVER_NODES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return DegreeExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    if chunks <= 1 {
        return DegreeExecutionPath::Serial;
    }
    DegreeExecutionPath::Parallel { threads, chunks }
}

fn degree_scores_parallel(
    graph: &AdjacencyGraph,
    denominator: u32,
    control: &AlgorithmControl,
) -> Result<Vec<([u8; 16], f64)>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel Degree requires an instance-owned compute pool"))?;
    let node_ids = graph.node_ids();
    let ranges = destination_chunks(node_ids.len(), control.compute_threads());
    let work = AtomicUsize::new(0);
    let chunk_results = run_rank_on_pool(pool, "Degree", || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                for &node_id in &node_ids[start..end] {
                    let observed = work.fetch_add(1, Ordering::Relaxed) + 1;
                    if observed.is_multiple_of(DEGREE_CHECKPOINT_NODES) {
                        control.check_cancelled()?;
                    }
                    let uuid = graph
                        .node_uuid(node_id)
                        .ok_or_else(|| execution("selected node has no UUID identity"))?;
                    let degree = exact_u32(graph.neighbors(node_id).len(), "node degree")?;
                    local.push((uuid, f64::from(degree) / f64::from(denominator)));
                }
                Ok((start, local))
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;
    // Merge chunk outputs in ascending node-ordinal order (canonical).
    let mut rows = Vec::with_capacity(node_ids.len());
    for (start, local) in chunk_results {
        debug_assert_eq!(start, rows.len());
        rows.extend(local);
    }
    Ok(rows)
}

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

/// Selected common-neighbors execution path for observability and crossover tests (#505).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommonNeighborsExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Selected Adamic-Adar execution path for observability and crossover tests (#499).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdamicAdarExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResourceAllocationExecutionPath {
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

        let indices: HashMap<u64, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let denominator = f64::from(exact_u32(
            node_ids.len().saturating_sub(1).max(1),
            "node count",
        )?);
        let mut scores = Vec::with_capacity(node_ids.len());
        for source in 0..node_ids.len() {
            control.checkpoint()?;
            let mut distance = vec![usize::MAX; node_ids.len()];
            distance[source] = 0;
            let mut queue = VecDeque::from([source]);
            let mut traversed_edges = 0_usize;

            while let Some(vertex) = queue.pop_front() {
                for edge in graph.neighbors(node_ids[vertex]) {
                    if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                        control.checkpoint()?;
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
            scores.push(score);
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

impl RustAlgorithm for Eigenvector {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Eigenvector),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Eigenvector);
        let node_ids = graph.node_ids();
        if node_ids.is_empty() {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let indices: HashMap<u64, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let node_count = f64::from(exact_u32(node_ids.len(), "node count")?);
        let mut scores = vec![1.0 / node_count; node_ids.len()];
        let path = select_eigenvector_path(control, graph.edge_entry_count(), node_ids.len());
        let mut inbound = None;
        for iteration in 0..EIGENVECTOR_MAX_ITERATIONS {
            control.checkpoint()?;
            let use_parallel = matches!(path, EigenvectorExecutionPath::Parallel { .. })
                && iteration >= EIGENVECTOR_SERIAL_WARMUP_ITERATIONS;
            let mut next = if use_parallel {
                if inbound.is_none() {
                    inbound = Some(prepare_eigenvector_inbound(graph, &indices)?);
                }
                eigenvector_pull_parallel(
                    inbound
                        .as_ref()
                        .ok_or_else(|| execution("parallel eigenvector requires inbound CSR"))?,
                    &scores,
                    control,
                )?
            } else {
                eigenvector_scatter_serial(graph, &indices, node_ids, &scores, control)?
            };
            if next.iter().any(|score| !score.is_finite()) {
                return Err(execution("eigenvector score exceeds supported range"));
            }

            let norm = next.iter().map(|score| score * score).sum::<f64>().sqrt();
            if !norm.is_finite() || norm == 0.0 {
                return Err(execution("eigenvector L2 norm is not finite and positive"));
            }
            for score in &mut next {
                *score /= norm;
            }
            let converged = next
                .iter()
                .zip(&scores)
                .all(|(current, previous)| (current - previous).abs() <= EIGENVECTOR_TOLERANCE);
            scores = next;
            if iteration > 0 && converged {
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

/// Selected eigenvector execution path for observability and crossover tests (#507).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EigenvectorExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Dense inbound CSR: source ordinals in canonical source/edge order per destination.
#[derive(Clone, Debug, Default)]
struct EigenvectorInboundCsr {
    offsets: Vec<u32>,
    sources: Vec<u32>,
}

fn select_eigenvector_path(
    control: &AlgorithmControl,
    edge_count: u64,
    nodes: usize,
) -> EigenvectorExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || edge_count < EIGENVECTOR_PARALLEL_CROSSOVER_EDGES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return EigenvectorExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    EigenvectorExecutionPath::Parallel { threads, chunks }
}

fn prepare_eigenvector_inbound(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
) -> Result<EigenvectorInboundCsr, AlgorithmError> {
    let node_ids = graph.node_ids();
    let node_len = node_ids.len();
    let mut inbound_counts = vec![0_u32; node_len];
    for &source in node_ids {
        for edge in graph.neighbors(source) {
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            inbound_counts[target] = inbound_counts[target]
                .checked_add(1)
                .ok_or_else(|| execution("inbound degree exceeds supported range"))?;
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

    Ok(EigenvectorInboundCsr { offsets, sources })
}

fn eigenvector_scatter_serial(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    node_ids: &[u64],
    scores: &[f64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut next = scores.to_vec();
    let mut traversed_edges = 0_usize;
    for (source_index, &source) in node_ids.iter().enumerate() {
        for edge in graph.neighbors(source) {
            if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            traversed_edges += 1;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            next[target] += scores[source_index];
            if !next[target].is_finite() {
                return Err(execution("eigenvector score exceeds supported range"));
            }
        }
    }
    Ok(next)
}

fn eigenvector_pull_destination(
    inbound: &EigenvectorInboundCsr,
    scores: &[f64],
    dest: usize,
) -> f64 {
    let start = usize::try_from(inbound.offsets[dest]).unwrap_or(0);
    let end = usize::try_from(inbound.offsets[dest + 1]).unwrap_or(start);
    let mut acc = scores[dest];
    for &source in &inbound.sources[start.min(end)..end.min(inbound.sources.len())] {
        let source = usize::try_from(source).unwrap_or(usize::MAX);
        if source < scores.len() {
            acc += scores[source];
        }
    }
    acc
}

fn eigenvector_pull_parallel(
    inbound: &EigenvectorInboundCsr,
    scores: &[f64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    if scores.is_empty() {
        return Ok(Vec::new());
    }
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel eigenvector requires an instance-owned compute pool"))?;
    let chunks = destination_chunks(scores.len(), control.compute_threads())
        .len()
        .max(1);
    let chunk_len = scores.len().div_ceil(chunks);
    let mut next = vec![0.0; scores.len()];
    run_eigenvector_on_pool(pool, || {
        next.par_chunks_mut(chunk_len)
            .enumerate()
            .try_for_each(|(chunk_index, local)| {
                control.check_cancelled()?;
                let start = chunk_index * chunk_len;
                for (offset, slot) in local.iter_mut().enumerate() {
                    let dest = start + offset;
                    if dest.is_multiple_of(EIGENVECTOR_CHECKPOINT_DESTINATIONS) {
                        control.check_cancelled()?;
                    }
                    let score = eigenvector_pull_destination(inbound, scores, dest);
                    if !score.is_finite() {
                        return Err(execution("eigenvector score exceeds supported range"));
                    }
                    *slot = score;
                }
                Ok(())
            })
    })?;
    Ok(next)
}

fn run_eigenvector_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("Eigenvector worker panicked")),
    }
}

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

impl RustAlgorithm for HitsHub {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::HitsHub),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::HitsHub);
        let (_, hubs) = hits_scores(graph, control)?;
        rank_scores_output(algorithm, graph, hubs, control)
    }
}

impl RustAlgorithm for HitsAuthority {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::HitsAuthority),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::HitsAuthority);
        let (authorities, _) = hits_scores(graph, control)?;
        rank_scores_output(algorithm, graph, authorities, control)
    }
}

impl RustAlgorithm for Celf {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Celf),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Celf);
        rank_scores_output(algorithm, graph, celf_scores(graph, control)?, control)
    }
}

impl RustAlgorithm for ClusteringCoefficient {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::ClusteringCoefficient),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::ClusteringCoefficient);
        rank_scores_output(
            algorithm,
            graph,
            clustering_coefficient_scores(graph, control)?,
            control,
        )
    }
}

impl RustAlgorithm for Triangles {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Triangles),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Triangles);
        rank_scores_output(algorithm, graph, triangle_scores(graph, control)?, control)
    }
}

impl RustAlgorithm for KCore {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::KCore),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::KCore);
        rank_scores_output(algorithm, graph, k_core_scores(graph, control)?, control)
    }
}

impl RustAlgorithm for PreferentialAttachment {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::PreferentialAttachment),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::PreferentialAttachment);
        rank_scores_output(
            algorithm,
            graph,
            preferential_attachment_scores(graph, control)?,
            control,
        )
    }
}

impl RustAlgorithm for AdamicAdar {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::AdamicAdar),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::AdamicAdar);
        rank_scores_output(
            algorithm,
            graph,
            adamic_adar_scores(graph, control)?,
            control,
        )
    }
}

impl RustAlgorithm for CommonNeighbors {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::CommonNeighbors),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::CommonNeighbors);
        rank_scores_output(
            algorithm,
            graph,
            common_neighbor_scores(graph, control)?,
            control,
        )
    }
}

impl RustAlgorithm for ResourceAllocation {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::ResourceAllocation),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::ResourceAllocation);
        rank_scores_output(
            algorithm,
            graph,
            resource_allocation_scores(graph, control)?,
            control,
        )
    }
}

impl RustAlgorithm for TotalNeighbors {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::TotalNeighbors),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::TotalNeighbors);
        rank_scores_output(
            algorithm,
            graph,
            total_neighbor_scores(graph, control)?,
            control,
        )
    }
}

fn triangle_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_undirected_neighbors(graph, control)?;
    match select_triangles_path(control, neighbors.len()) {
        TrianglesExecutionPath::Serial => triangle_scores_serial(&neighbors, control),
        TrianglesExecutionPath::Parallel { .. } => triangle_scores_parallel(&neighbors, control),
    }
}

/// Selected triangles execution path for private-pool crossover tests (#515).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TrianglesExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Choose serial vs private-pool parallel execution for a triangles workload.
pub(crate) fn select_triangles_path(
    control: &AlgorithmControl,
    nodes: usize,
) -> TrianglesExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || nodes < TRIANGLES_PARALLEL_CROSSOVER_NODES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return TrianglesExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    TrianglesExecutionPath::Parallel { threads, chunks }
}

fn triangle_scores_serial(
    neighbors: &[Vec<usize>],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut scores = Vec::with_capacity(neighbors.len());
    let mut visited_pairs = 0_usize;
    for node in 0..neighbors.len() {
        control.checkpoint()?;
        let mut count = 0_u64;
        for (offset, &first) in neighbors[node].iter().enumerate() {
            for &second in &neighbors[node][offset + 1..] {
                if visited_pairs.is_multiple_of(TRIANGLES_CHECKPOINT_PAIRS) {
                    control.checkpoint()?;
                }
                visited_pairs += 1;
                if has_arc(neighbors, first, second) {
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| execution("triangle count exceeds supported range"))?;
                }
            }
        }
        scores.push(exact_u64_as_f64(count, "triangle count")?);
    }
    Ok(scores)
}

fn triangle_scores_parallel(
    neighbors: &[Vec<usize>],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel triangles requires an instance-owned compute pool"))?;
    let ranges = destination_chunks(neighbors.len(), control.compute_threads());
    let work = AtomicUsize::new(0);
    let chunk_results = run_triangles_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                for node in start..end {
                    control.check_cancelled()?;
                    local.push(triangle_score_node_parallel(
                        neighbors, node, control, &work,
                    )?);
                }
                Ok((start, local))
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;

    // Merge worker-local scores in ascending node-ordinal range order (canonical).
    let mut scores = vec![0.0; neighbors.len()];
    for (start, local) in chunk_results {
        scores[start..start + local.len()].copy_from_slice(&local);
    }
    Ok(scores)
}

fn triangle_score_node_parallel(
    neighbors: &[Vec<usize>],
    node: usize,
    control: &AlgorithmControl,
    work: &AtomicUsize,
) -> Result<f64, AlgorithmError> {
    let mut count = 0_u64;
    for (offset, &first) in neighbors[node].iter().enumerate() {
        for &second in &neighbors[node][offset + 1..] {
            let observed = work.fetch_add(1, Ordering::Relaxed) + 1;
            if observed.is_multiple_of(TRIANGLES_CHECKPOINT_PAIRS) {
                control.check_cancelled()?;
            }
            if has_arc(neighbors, first, second) {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| execution("triangle count exceeds supported range"))?;
            }
        }
    }
    exact_u64_as_f64(count, "triangle count")
}

fn run_triangles_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("triangles worker panicked")),
    }
}

fn preferential_attachment_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    // For each node u, sum deg(u) * deg(v) over every missing outgoing
    // candidate v. Algebraic aggregation avoids materializing O(V^2) pairs.
    let neighbors = simple_neighbors(graph, control, false)?;
    let degrees: Vec<u64> = neighbors
        .iter()
        .map(|adjacent| {
            u64::try_from(adjacent.len())
                .map_err(|_| execution("preferential-attachment degree exceeds supported range"))
        })
        .collect::<Result<_, _>>()?;
    let total_degree = degrees.iter().try_fold(0_u64, |total, degree| {
        total
            .checked_add(*degree)
            .ok_or_else(|| execution("preferential-attachment degree sum exceeds supported range"))
    })?;
    let mut visited_neighbors = 0_usize;
    neighbors
        .iter()
        .enumerate()
        .map(|(node, adjacent)| {
            if node.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            let linked_degree = adjacent.iter().try_fold(0_u64, |total, &neighbor| {
                if visited_neighbors.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                visited_neighbors += 1;
                total.checked_add(degrees[neighbor]).ok_or_else(|| {
                    execution("preferential-attachment neighbor sum exceeds supported range")
                })
            })?;
            let candidate_degree = total_degree
                .checked_sub(degrees[node])
                .and_then(|remaining| remaining.checked_sub(linked_degree))
                .ok_or_else(|| execution("preferential-attachment candidate sum underflow"))?;
            let score = degrees[node].checked_mul(candidate_degree).ok_or_else(|| {
                execution("preferential-attachment score exceeds supported range")
            })?;
            exact_u64_as_f64(score, "preferential-attachment score")
        })
        .collect()
}

fn adamic_adar_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let discount_degrees = adamic_adar_discount_degrees(&neighbors)?;
    let estimated_work = estimated_adamic_adar_work(&neighbors);
    match select_adamic_adar_path(control, neighbors.len(), estimated_work) {
        AdamicAdarExecutionPath::Serial => {
            adamic_adar_scores_serial(&neighbors, &discount_degrees, control)
        }
        AdamicAdarExecutionPath::Parallel { .. } => {
            adamic_adar_scores_parallel(&neighbors, &discount_degrees, control)
        }
    }
}
fn adamic_adar_discount_degrees(neighbors: &[Vec<usize>]) -> Result<Vec<u64>, AlgorithmError> {
    let mut discount_degrees = vec![0_u64; neighbors.len()];
    for adjacent in neighbors {
        for &neighbor in adjacent {
            discount_degrees[neighbor] = discount_degrees[neighbor]
                .checked_add(1)
                .ok_or_else(|| execution("Adamic-Adar neighbor degree exceeds supported range"))?;
        }
    }
    Ok(discount_degrees)
}
fn adamic_adar_scores_serial(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for source in 0..neighbors.len() {
        let score = adamic_adar_source_score(neighbors, discount_degrees, source, || {
            adamic_adar_serial_checkpoint(control, &mut visited)
        })?;
        scores.push(score);
    }
    Ok(scores)
}
fn adamic_adar_scores_parallel(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel Adamic-Adar requires an instance-owned compute pool"))?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let chunk_results = run_adamic_adar_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut work = 0_usize;
                let mut local = Vec::with_capacity(end - start);
                for source in start..end {
                    let score =
                        adamic_adar_source_score(neighbors, discount_degrees, source, || {
                            adamic_adar_serial_checkpoint(control, &mut work)
                        })?;
                    local.push(score);
                }
                Ok((start, local))
            })
            .collect::<Vec<Result<_, AlgorithmError>>>();
        first_chunk_error(results)
    })?;

    let mut scores = Vec::with_capacity(neighbors.len());
    for (_start, local) in chunk_results {
        scores.extend(local);
    }
    Ok(scores)
}
fn adamic_adar_source_score(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    source: usize,
    mut checkpoint: impl FnMut() -> Result<(), AlgorithmError>,
) -> Result<f64, AlgorithmError> {
    let source_neighbors = &neighbors[source];
    let mut score = 0.0_f64;
    let mut compensation = 0.0_f64;
    for (candidate, candidate_neighbors) in neighbors.iter().enumerate() {
        checkpoint()?;
        if source == candidate || source_neighbors.binary_search(&candidate).is_ok() {
            continue;
        }
        let (mut left, mut right) = (0, 0);
        while left < source_neighbors.len() && right < candidate_neighbors.len() {
            checkpoint()?;
            match source_neighbors[left].cmp(&candidate_neighbors[right]) {
                std::cmp::Ordering::Less => left += 1,
                std::cmp::Ordering::Greater => right += 1,
                std::cmp::Ordering::Equal => {
                    let common = source_neighbors[left];
                    let term = adamic_discount(discount_degrees[common])?;
                    let adjusted = term - compensation;
                    let updated = score + adjusted;
                    compensation = (updated - score) - adjusted;
                    score = updated;
                    left += 1;
                    right += 1;
                }
            }
        }
    }
    if !score.is_finite() {
        return Err(execution("Adamic-Adar score is not finite"));
    }
    Ok(score)
}
fn adamic_adar_serial_checkpoint(
    control: &AlgorithmControl,
    visited: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*visited).is_multiple_of(ADAMIC_ADAR_CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    *visited = visited.saturating_add(1);
    Ok(())
}
fn estimated_adamic_adar_work(neighbors: &[Vec<usize>]) -> u64 {
    let sources = usize_to_u64_saturating(neighbors.len());
    let degree_sum = neighbors.iter().fold(0_u64, |total, adjacent| {
        total.saturating_add(usize_to_u64_saturating(adjacent.len()))
    });
    sources
        .saturating_mul(sources)
        .saturating_add(sources.saturating_mul(degree_sum).saturating_mul(2))
}
pub(crate) fn select_adamic_adar_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_work: u64,
) -> AdamicAdarExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || sources <= 1
        || estimated_work < ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return AdamicAdarExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return AdamicAdarExecutionPath::Serial;
    }
    AdamicAdarExecutionPath::Parallel { threads, chunks }
}
fn run_adamic_adar_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("Adamic-Adar worker panicked")),
    }
}

fn adamic_discount(degree: u64) -> Result<f64, AlgorithmError> {
    if degree < 2 {
        return Err(execution(
            "Adamic-Adar common-neighbor degree must be at least two",
        ));
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "the logarithmic discount does not require an exact integer conversion"
    )]
    let term = 1.0 / (degree as f64).ln();
    if !term.is_finite() {
        return Err(execution("Adamic-Adar discount is not finite"));
    }
    Ok(term)
}

fn common_neighbor_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let estimated_work = estimated_common_neighbors_work(&neighbors);
    match select_common_neighbors_path(control, neighbors.len(), estimated_work) {
        CommonNeighborsExecutionPath::Serial => common_neighbor_scores_serial(&neighbors, control),
        CommonNeighborsExecutionPath::Parallel { .. } => {
            common_neighbor_scores_parallel(&neighbors, control)
        }
    }
}

fn common_neighbor_scores_serial(
    neighbors: &[Vec<usize>],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for source in 0..neighbors.len() {
        let score = common_neighbor_source_score(neighbors, source, || {
            common_neighbors_checkpoint(control, &mut visited)
        })?;
        scores.push(exact_u64_as_f64(score, "common-neighbors score")?);
    }
    Ok(scores)
}

fn common_neighbor_scores_parallel(
    neighbors: &[Vec<usize>],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel common-neighbors requires an instance-owned compute pool")
    })?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let chunk_results = run_common_neighbors_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut work = 0_usize;
                let mut local = Vec::with_capacity(end - start);
                for source in start..end {
                    let score = common_neighbor_source_score(neighbors, source, || {
                        common_neighbors_checkpoint(control, &mut work)
                    })?;
                    local.push(exact_u64_as_f64(score, "common-neighbors score")?);
                }
                Ok((start, local))
            })
            .collect::<Vec<Result<_, AlgorithmError>>>();
        first_chunk_error(results)
    })?;

    let mut scores = Vec::with_capacity(neighbors.len());
    for (_start, local) in chunk_results {
        scores.extend(local);
    }
    Ok(scores)
}

fn common_neighbor_source_score(
    neighbors: &[Vec<usize>],
    source: usize,
    mut checkpoint: impl FnMut() -> Result<(), AlgorithmError>,
) -> Result<u64, AlgorithmError> {
    let source_neighbors = &neighbors[source];
    let mut score = 0_u64;
    for (candidate, candidate_neighbors) in neighbors.iter().enumerate() {
        checkpoint()?;
        if source == candidate || source_neighbors.binary_search(&candidate).is_ok() {
            continue;
        }
        let (mut left, mut right) = (0, 0);
        while left < source_neighbors.len() && right < candidate_neighbors.len() {
            checkpoint()?;
            match source_neighbors[left].cmp(&candidate_neighbors[right]) {
                std::cmp::Ordering::Less => left += 1,
                std::cmp::Ordering::Greater => right += 1,
                std::cmp::Ordering::Equal => {
                    score = score.checked_add(1).ok_or_else(|| {
                        execution("common-neighbors score exceeds supported range")
                    })?;
                    left += 1;
                    right += 1;
                }
            }
        }
    }
    Ok(score)
}

fn common_neighbors_checkpoint(
    control: &AlgorithmControl,
    visited: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*visited).is_multiple_of(COMMON_NEIGHBORS_CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    *visited = visited.saturating_add(1);
    Ok(())
}

fn estimated_common_neighbors_work(neighbors: &[Vec<usize>]) -> u64 {
    let sources = usize_to_u64_saturating(neighbors.len());
    let degree_sum = neighbors.iter().fold(0_u64, |total, adjacent| {
        total.saturating_add(usize_to_u64_saturating(adjacent.len()))
    });
    sources
        .saturating_mul(sources)
        .saturating_add(sources.saturating_mul(degree_sum).saturating_mul(2))
}

/// Choose serial vs private-pool parallel execution for a common-neighbors workload.
pub(crate) fn select_common_neighbors_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_work: u64,
) -> CommonNeighborsExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || sources <= 1
        || estimated_work < COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return CommonNeighborsExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return CommonNeighborsExecutionPath::Serial;
    }
    CommonNeighborsExecutionPath::Parallel { threads, chunks }
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

fn run_common_neighbors_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("common-neighbors worker panicked")),
    }
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn resource_allocation_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let discount_degrees = resource_allocation_discount_degrees(&neighbors)?;
    let estimated_work = estimated_pairwise_source_work(&neighbors);
    match select_resource_allocation_path(control, neighbors.len(), estimated_work) {
        ResourceAllocationExecutionPath::Serial => {
            resource_allocation_scores_serial(&neighbors, &discount_degrees, control)
        }
        ResourceAllocationExecutionPath::Parallel { .. } => {
            resource_allocation_scores_parallel(&neighbors, &discount_degrees, control)
        }
    }
}

fn resource_allocation_discount_degrees(
    neighbors: &[Vec<usize>],
) -> Result<Vec<u64>, AlgorithmError> {
    let mut discount_degrees = vec![0_u64; neighbors.len()];
    for adjacent in neighbors {
        for &neighbor in adjacent {
            discount_degrees[neighbor] = discount_degrees[neighbor]
                .checked_add(1)
                .ok_or_else(|| execution("resource-allocation degree exceeds supported range"))?;
        }
    }
    Ok(discount_degrees)
}

fn resource_allocation_scores_serial(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for source in 0..neighbors.len() {
        let score = resource_allocation_source_score(neighbors, discount_degrees, source, || {
            resource_allocation_serial_checkpoint(control, &mut visited)
        })?;
        scores.push(score);
    }
    Ok(scores)
}

fn resource_allocation_scores_parallel(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel resource-allocation requires an instance-owned compute pool")
    })?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let chunk_results = run_resource_allocation_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut work = 0_usize;
                let mut local = Vec::with_capacity(end - start);
                for source in start..end {
                    let score = resource_allocation_source_score(
                        neighbors,
                        discount_degrees,
                        source,
                        || resource_allocation_serial_checkpoint(control, &mut work),
                    )?;
                    local.push(score);
                }
                Ok((start, local))
            })
            .collect::<Vec<Result<_, AlgorithmError>>>();
        first_chunk_error(results)
    })?;

    let mut scores = Vec::with_capacity(neighbors.len());
    for (_start, local) in chunk_results {
        scores.extend(local);
    }
    Ok(scores)
}

fn resource_allocation_source_score(
    neighbors: &[Vec<usize>],
    discount_degrees: &[u64],
    source: usize,
    mut checkpoint: impl FnMut() -> Result<(), AlgorithmError>,
) -> Result<f64, AlgorithmError> {
    let source_neighbors = &neighbors[source];
    let mut score = 0.0_f64;
    let mut compensation = 0.0_f64;
    for (candidate, candidate_neighbors) in neighbors.iter().enumerate() {
        checkpoint()?;
        if source == candidate || source_neighbors.binary_search(&candidate).is_ok() {
            continue;
        }
        let (mut left, mut right) = (0, 0);
        while left < source_neighbors.len() && right < candidate_neighbors.len() {
            checkpoint()?;
            match source_neighbors[left].cmp(&candidate_neighbors[right]) {
                std::cmp::Ordering::Less => left += 1,
                std::cmp::Ordering::Greater => right += 1,
                std::cmp::Ordering::Equal => {
                    let term =
                        resource_allocation_discount(discount_degrees[source_neighbors[left]])?;
                    let adjusted = term - compensation;
                    let updated = score + adjusted;
                    compensation = (updated - score) - adjusted;
                    score = updated;
                    left += 1;
                    right += 1;
                }
            }
        }
    }
    if !score.is_finite() {
        return Err(execution("resource-allocation score is not finite"));
    }
    Ok(score)
}

fn resource_allocation_serial_checkpoint(
    control: &AlgorithmControl,
    visited: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*visited).is_multiple_of(RESOURCE_ALLOCATION_CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    *visited = visited.saturating_add(1);
    Ok(())
}

fn estimated_pairwise_source_work(neighbors: &[Vec<usize>]) -> u64 {
    let sources = usize_to_u64_saturating(neighbors.len());
    let degree_sum = neighbors.iter().fold(0_u64, |total, adjacent| {
        total.saturating_add(usize_to_u64_saturating(adjacent.len()))
    });
    sources
        .saturating_mul(sources)
        .saturating_add(sources.saturating_mul(degree_sum).saturating_mul(2))
}

/// Choose serial vs private-pool parallel execution for resource allocation.
pub(crate) fn select_resource_allocation_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_work: u64,
) -> ResourceAllocationExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || sources <= 1
        || estimated_work < RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return ResourceAllocationExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return ResourceAllocationExecutionPath::Serial;
    }
    ResourceAllocationExecutionPath::Parallel { threads, chunks }
}

fn run_resource_allocation_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("resource-allocation worker panicked")),
    }
}

fn resource_allocation_discount(degree: u64) -> Result<f64, AlgorithmError> {
    if degree < 2 {
        return Err(execution(
            "resource-allocation common-neighbor degree must be at least two",
        ));
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "the reciprocal discount does not require an exact integer conversion"
    )]
    let term = 1.0 / degree as f64;
    if !term.is_finite() {
        return Err(execution("resource-allocation discount is not finite"));
    }
    Ok(term)
}

fn total_neighbor_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let degrees: Vec<u64> = neighbors
        .iter()
        .map(|adjacent| {
            u64::try_from(adjacent.len())
                .map_err(|_| execution("total-neighbors degree exceeds supported range"))
        })
        .collect::<Result<_, _>>()?;
    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for (source, source_neighbors) in neighbors.iter().enumerate() {
        let mut score = 0_u64;
        for (candidate, candidate_neighbors) in neighbors.iter().enumerate() {
            if visited.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            visited += 1;
            if source == candidate || source_neighbors.binary_search(&candidate).is_ok() {
                continue;
            }
            let mut union = degrees[source]
                .checked_add(degrees[candidate])
                .ok_or_else(|| execution("total-neighbors pair score exceeds supported range"))?;
            let (mut left, mut right) = (0, 0);
            while left < source_neighbors.len() && right < candidate_neighbors.len() {
                if visited.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                visited += 1;
                match source_neighbors[left].cmp(&candidate_neighbors[right]) {
                    std::cmp::Ordering::Less => left += 1,
                    std::cmp::Ordering::Greater => right += 1,
                    std::cmp::Ordering::Equal => {
                        union = union
                            .checked_sub(1)
                            .ok_or_else(|| execution("total-neighbors pair score underflow"))?;
                        left += 1;
                        right += 1;
                    }
                }
            }
            score = score
                .checked_add(union)
                .ok_or_else(|| execution("total-neighbors score exceeds supported range"))?;
        }
        scores.push(exact_u64_as_f64(score, "total-neighbors score")?);
    }
    Ok(scores)
}

fn k_core_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    k_core_numbers(graph, control)?
        .into_iter()
        .map(|core| {
            let core = u64::try_from(core)
                .map_err(|_| execution("k-core score exceeds supported range"))?;
            exact_u64_as_f64(core, "k-core score")
        })
        .collect()
}

fn clustering_coefficient_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let prepared = prepare_clustering_coefficient(graph, control)?;
    match select_clustering_coefficient_path(control, prepared.work_units, prepared.len()) {
        ClusteringCoefficientExecutionPath::Serial => {
            clustering_coefficient_scores_serial(&prepared, control)
        }
        ClusteringCoefficientExecutionPath::Parallel { .. } => {
            clustering_coefficient_scores_parallel(&prepared, control)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClusteringCoefficientExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

struct PreparedClusteringCoefficient {
    outgoing: Vec<Vec<usize>>,
    incoming: Vec<Vec<usize>>,
    work_units: u64,
}

impl PreparedClusteringCoefficient {
    fn len(&self) -> usize {
        self.outgoing.len()
    }
}

fn prepare_clustering_coefficient(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<PreparedClusteringCoefficient, AlgorithmError> {
    let node_ids = graph.node_ids();
    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut outgoing = vec![Vec::new(); node_ids.len()];
    let mut traversed_edges = 0_usize;
    for (source, &node_id) in node_ids.iter().enumerate() {
        for edge in graph.neighbors(node_id) {
            if traversed_edges.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            traversed_edges += 1;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            if source != target {
                outgoing[source].push(target);
            }
        }
        outgoing[source].sort_unstable();
        outgoing[source].dedup();
    }

    let mut incoming = vec![Vec::new(); node_ids.len()];
    for (source, targets) in outgoing.iter().enumerate() {
        for &target in targets {
            incoming[target].push(source);
        }
    }
    for sources in &mut incoming {
        sources.sort_unstable();
    }

    let work_units = estimate_clustering_coefficient_work(&outgoing, &incoming)?;
    Ok(PreparedClusteringCoefficient {
        outgoing,
        incoming,
        work_units,
    })
}

fn estimate_clustering_coefficient_work(
    outgoing: &[Vec<usize>],
    incoming: &[Vec<usize>],
) -> Result<u64, AlgorithmError> {
    outgoing
        .iter()
        .zip(incoming)
        .try_fold(0_u64, |total, (outgoing, incoming)| {
            let degree = outgoing.len().checked_add(incoming.len()).ok_or_else(|| {
                execution("clustering coefficient degree exceeds supported range")
            })?;
            let degree = u64::try_from(degree)
                .map_err(|_| execution("clustering coefficient degree exceeds supported range"))?;
            Ok(total.saturating_add(degree.saturating_mul(degree)))
        })
}

/// Choose serial vs private-pool parallel execution for clustering coefficient.
pub(crate) fn select_clustering_coefficient_path(
    control: &AlgorithmControl,
    work_units: u64,
    nodes: usize,
) -> ClusteringCoefficientExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || work_units < CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return ClusteringCoefficientExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    if chunks <= 1 {
        return ClusteringCoefficientExecutionPath::Serial;
    }
    ClusteringCoefficientExecutionPath::Parallel { threads, chunks }
}

fn clustering_coefficient_scores_serial(
    prepared: &PreparedClusteringCoefficient,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut scores = Vec::with_capacity(prepared.len());
    let mut work = 0_usize;
    for node in 0..prepared.len() {
        scores.push(clustering_coefficient_score_node(
            prepared, node, control, &mut work,
        )?);
    }
    Ok(scores)
}

fn clustering_coefficient_scores_parallel(
    prepared: &PreparedClusteringCoefficient,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel clustering coefficient requires an instance-owned compute pool")
    })?;
    let ranges = destination_chunks(prepared.len(), control.compute_threads());
    let chunk_results = run_clustering_coefficient_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                let mut work = 0_usize;
                for node in start..end {
                    local.push(clustering_coefficient_score_node(
                        prepared, node, control, &mut work,
                    )?);
                }
                Ok((start, local))
            })
            .collect::<Vec<Result<_, AlgorithmError>>>()
    })?;

    let mut scores = vec![0.0; prepared.len()];
    for result in chunk_results {
        let (start, local) = result?;
        scores[start..start + local.len()].copy_from_slice(&local);
    }
    Ok(scores)
}

fn clustering_coefficient_score_node(
    prepared: &PreparedClusteringCoefficient,
    node: usize,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<f64, AlgorithmError> {
    control.checkpoint()?;
    let outgoing = &prepared.outgoing;
    let incoming = &prepared.incoming;
    let mut neighbors = outgoing[node].clone();
    neighbors.extend_from_slice(&incoming[node]);
    neighbors.sort_unstable();
    neighbors.dedup();

    let total_degree = outgoing[node]
        .len()
        .checked_add(incoming[node].len())
        .ok_or_else(|| execution("clustering coefficient degree exceeds supported range"))?;
    let total_degree = u64::try_from(total_degree)
        .map_err(|_| execution("clustering coefficient degree exceeds supported range"))?;
    let reciprocal_degree = u64::try_from(
        outgoing[node]
            .iter()
            .filter(|&&neighbor| has_arc(outgoing, neighbor, node))
            .count(),
    )
    .map_err(|_| execution("reciprocal degree exceeds supported range"))?;
    let denominator = total_degree
        .checked_mul(total_degree.saturating_sub(1))
        .and_then(|value| value.checked_sub(reciprocal_degree.checked_mul(2)?))
        .and_then(|value| value.checked_mul(2))
        .ok_or_else(|| execution("clustering coefficient denominator exceeds supported range"))?;

    let mut triangles = 0_u64;
    for &first in &neighbors {
        for &second in &neighbors {
            clustering_coefficient_checkpoint(control, work)?;
            let contribution = arc_strength(outgoing, node, first)
                * arc_strength(outgoing, first, second)
                * arc_strength(outgoing, second, node);
            triangles = triangles.checked_add(contribution).ok_or_else(|| {
                execution("clustering coefficient triangle count exceeds supported range")
            })?;
        }
    }
    let score = if denominator == 0 {
        0.0
    } else {
        exact_u64_as_f64(triangles, "clustering coefficient triangle count")?
            / exact_u64_as_f64(denominator, "clustering coefficient denominator")?
    };
    if !score.is_finite() {
        return Err(execution("clustering coefficient score is not finite"));
    }
    Ok(score)
}

fn clustering_coefficient_checkpoint(
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    if (*work).is_multiple_of(CLUSTERING_COEFFICIENT_CHECKPOINT_WORK) {
        control.checkpoint()?;
    }
    *work = work.saturating_add(1);
    Ok(())
}

fn run_clustering_coefficient_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> R + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => Ok(result),
        Err(_) => Err(execution("clustering coefficient worker panicked")),
    }
}

fn has_arc(outgoing: &[Vec<usize>], source: usize, target: usize) -> bool {
    outgoing[source].binary_search(&target).is_ok()
}

fn arc_strength(outgoing: &[Vec<usize>], source: usize, target: usize) -> u64 {
    u64::from(has_arc(outgoing, source, target)) + u64::from(has_arc(outgoing, target, source))
}

#[derive(Clone, Copy)]
struct CelfCandidate {
    node: usize,
    uuid: [u8; 16],
    gain: f64,
    updated_at: usize,
    selected: bool,
}

fn celf_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let node_ids = graph.node_ids();
    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut candidates = Vec::with_capacity(node_ids.len());
    for (node, &node_id) in node_ids.iter().enumerate() {
        let uuid = graph
            .node_uuid(node_id)
            .ok_or_else(|| execution("selected node has no UUID identity"))?;
        candidates.push(CelfCandidate {
            node,
            uuid,
            gain: celf_spread(graph, &indices, &[node], control)?,
            updated_at: 0,
            selected: false,
        });
    }

    let mut seeds = Vec::with_capacity(node_ids.len());
    let mut scores = vec![0.0; node_ids.len()];
    let mut current_spread = 0.0;
    for round in 0..node_ids.len() {
        loop {
            let best = celf_best(&candidates)
                .ok_or_else(|| execution("CELF candidate queue became empty"))?;
            if candidates[best].updated_at == round {
                let candidate = &mut candidates[best];
                candidate.selected = true;
                scores[candidate.node] = candidate.gain;
                seeds.push(candidate.node);
                current_spread += candidate.gain;
                break;
            }
            let mut candidate_seeds = seeds.clone();
            candidate_seeds.push(candidates[best].node);
            let total_spread = celf_spread(graph, &indices, &candidate_seeds, control)?;
            let gain = total_spread - current_spread;
            if !gain.is_finite() || gain < -1.0e-12 {
                return Err(execution("CELF marginal spread is negative or non-finite"));
            }
            candidates[best].gain = gain.max(0.0);
            candidates[best].updated_at = round;
        }
    }
    Ok(scores)
}

fn celf_best(candidates: &[CelfCandidate]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (index, candidate) in candidates
        .iter()
        .enumerate()
        .filter(|(_, value)| !value.selected)
    {
        let replace = best.is_none_or(|current| {
            let current = &candidates[current];
            match candidate.gain.total_cmp(&current.gain) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => candidate.uuid < current.uuid,
                std::cmp::Ordering::Less => false,
            }
        });
        if replace {
            best = Some(index);
        }
    }
    best
}

fn celf_spread(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    seeds: &[usize],
    control: &AlgorithmControl,
) -> Result<f64, AlgorithmError> {
    control.checkpoint()?;
    let node_ids = graph.node_ids();
    let mut total = 0.0_f64;
    let mut traversed_edges = 0_usize;
    for simulation in 0..u64::from(CELF_SIMULATIONS) {
        let mut active = vec![false; node_ids.len()];
        let mut queue = VecDeque::new();
        for &seed in seeds {
            active[seed] = true;
            queue.push_back(seed);
        }
        while let Some(source) = queue.pop_front() {
            let source_uuid = graph
                .node_uuid(node_ids[source])
                .ok_or_else(|| execution("selected node has no UUID identity"))?;
            for edge in graph.neighbors(node_ids[source]) {
                if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                traversed_edges += 1;
                let target = indices
                    .get(&edge.neighbor_id)
                    .copied()
                    .ok_or_else(|| execution("adjacency references an unselected node"))?;
                if !active[target] && celf_live_edge(simulation, source_uuid, edge.edge_uuid) {
                    active[target] = true;
                    queue.push_back(target);
                }
            }
        }
        total += f64::from(exact_u32(
            active.iter().filter(|&&value| value).count(),
            "CELF activated-node count",
        )?);
    }
    Ok(total / f64::from(CELF_SIMULATIONS))
}

fn celf_live_edge(simulation: u64, source_uuid: [u8; 16], edge_uuid: [u8; 16]) -> bool {
    let mut state = splitmix64(simulation);
    for bytes in [source_uuid, edge_uuid] {
        for chunk in bytes.chunks_exact(8) {
            state = splitmix64(state ^ u64::from_be_bytes(chunk.try_into().expect("eight bytes")));
        }
    }
    state < CELF_LIVE_EDGE_THRESHOLD
}

fn splitmix64(value: u64) -> u64 {
    let mut mixed = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

#[derive(Clone, Debug, Default)]
struct HitsCsr {
    offsets: Vec<u32>,
    neighbors: Vec<u32>,
}

#[derive(Clone, Debug, Default)]
struct PreparedHits {
    outgoing: HitsCsr,
    incoming: HitsCsr,
    edge_count: u64,
}

/// Selected HITS execution path for observability and crossover tests (#510).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HitsExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

fn hits_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<(Vec<f64>, Vec<f64>), AlgorithmError> {
    let node_ids = graph.node_ids();
    if node_ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let prepared = prepare_hits(graph, control)?;
    let path = select_hits_path(control, prepared.edge_count, node_ids.len());
    let mut authorities = vec![1.0; node_ids.len()];
    let mut hubs = vec![1.0; node_ids.len()];
    for _ in 0..HITS_ITERATIONS {
        control.checkpoint()?;
        match path {
            HitsExecutionPath::Serial => {
                hits_pull_serial(&prepared.incoming, &hubs, &mut authorities, control)?;
            }
            HitsExecutionPath::Parallel { .. } => {
                hits_pull_parallel(&prepared.incoming, &hubs, &mut authorities, control)?;
            }
        }
        normalize_hits(&mut authorities, "authority")?;

        control.checkpoint()?;
        let mut next_hubs = vec![0.0; node_ids.len()];
        match path {
            HitsExecutionPath::Serial => {
                hits_pull_serial(&prepared.outgoing, &authorities, &mut next_hubs, control)?;
            }
            HitsExecutionPath::Parallel { .. } => {
                hits_pull_parallel(&prepared.outgoing, &authorities, &mut next_hubs, control)?;
            }
        }
        normalize_hits(&mut next_hubs, "hub")?;
        hubs = next_hubs;
    }
    Ok((authorities, hubs))
}

fn prepare_hits(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<PreparedHits, AlgorithmError> {
    let node_ids = graph.node_ids();
    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut outgoing_offsets = Vec::with_capacity(node_ids.len() + 1);
    let mut outgoing_neighbors = Vec::with_capacity(
        usize::try_from(graph.edge_entry_count())
            .map_err(|_| execution("HITS edge count exceeds supported range"))?,
    );
    let mut incoming_counts = vec![0_u32; node_ids.len()];
    let mut edge_count = 0_u64;
    outgoing_offsets.push(0_u32);
    for &node in node_ids {
        for edge in graph.neighbors(node) {
            if edge_count > 0 && edge_count.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            incoming_counts[target] = incoming_counts[target]
                .checked_add(1)
                .ok_or_else(|| execution("HITS inbound degree exceeds supported range"))?;
            outgoing_neighbors.push(exact_u32(target, "HITS target ordinal")?);
            edge_count = edge_count
                .checked_add(1)
                .ok_or_else(|| execution("HITS edge count exceeds supported range"))?;
        }
        outgoing_offsets.push(exact_u32(
            outgoing_neighbors.len(),
            "HITS outgoing CSR length",
        )?);
    }

    let mut incoming_offsets = Vec::with_capacity(node_ids.len() + 1);
    incoming_offsets.push(0_u32);
    for &count in &incoming_counts {
        let next = incoming_offsets
            .last()
            .copied()
            .unwrap_or(0)
            .checked_add(count)
            .ok_or_else(|| execution("HITS incoming CSR offsets exceed supported range"))?;
        incoming_offsets.push(next);
    }
    let mut incoming_neighbors = vec![0_u32; outgoing_neighbors.len()];
    let mut write_at = incoming_offsets[..node_ids.len()].to_vec();
    for source in 0..node_ids.len() {
        let source_u32 = exact_u32(source, "HITS source ordinal")?;
        let start = usize::try_from(outgoing_offsets[source])
            .map_err(|_| execution("HITS outgoing offset exceeds supported range"))?;
        let end = usize::try_from(outgoing_offsets[source + 1])
            .map_err(|_| execution("HITS outgoing offset exceeds supported range"))?;
        for &target in &outgoing_neighbors[start..end] {
            let target = usize::try_from(target)
                .map_err(|_| execution("HITS target ordinal exceeds supported range"))?;
            let slot = usize::try_from(write_at[target])
                .map_err(|_| execution("HITS incoming write cursor exceeds supported range"))?;
            incoming_neighbors[slot] = source_u32;
            write_at[target] = write_at[target]
                .checked_add(1)
                .ok_or_else(|| execution("HITS incoming write cursor overflow"))?;
        }
    }

    Ok(PreparedHits {
        outgoing: HitsCsr {
            offsets: outgoing_offsets,
            neighbors: outgoing_neighbors,
        },
        incoming: HitsCsr {
            offsets: incoming_offsets,
            neighbors: incoming_neighbors,
        },
        edge_count,
    })
}

/// Choose serial vs private-pool parallel execution for a HITS workload.
pub(crate) fn select_hits_path(
    control: &AlgorithmControl,
    edge_count: u64,
    nodes: usize,
) -> HitsExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes <= 1
        || edge_count < HITS_PARALLEL_CROSSOVER_EDGES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return HitsExecutionPath::Serial;
    }
    let chunks = destination_chunks(nodes, threads).len();
    if chunks <= 1 {
        return HitsExecutionPath::Serial;
    }
    HitsExecutionPath::Parallel { threads, chunks }
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

fn hits_pull_serial(
    csr: &HitsCsr,
    input: &[f64],
    output: &mut [f64],
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    let mut traversed_edges = 0_usize;
    for (node, score) in output.iter_mut().enumerate() {
        *score = 0.0;
        let start = usize::try_from(csr.offsets[node])
            .map_err(|_| execution("HITS CSR offset exceeds supported range"))?;
        let end = usize::try_from(csr.offsets[node + 1])
            .map_err(|_| execution("HITS CSR offset exceeds supported range"))?;
        for &neighbor in &csr.neighbors[start..end] {
            if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            traversed_edges += 1;
            let neighbor = usize::try_from(neighbor)
                .map_err(|_| execution("HITS neighbor ordinal exceeds supported range"))?;
            *score += input[neighbor];
        }
    }
    Ok(())
}

fn hits_pull_parallel(
    csr: &HitsCsr,
    input: &[f64],
    output: &mut [f64],
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel HITS requires an instance-owned compute pool"))?;
    let ranges = destination_chunks(output.len(), control.compute_threads());
    let chunk_results = run_hits_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut local = Vec::with_capacity(end - start);
                let mut traversed_edges = 0_usize;
                for node in start..end {
                    let edge_start = usize::try_from(csr.offsets[node])
                        .map_err(|_| execution("HITS CSR offset exceeds supported range"))?;
                    let edge_end = usize::try_from(csr.offsets[node + 1])
                        .map_err(|_| execution("HITS CSR offset exceeds supported range"))?;
                    let mut score = 0.0;
                    for &neighbor in &csr.neighbors[edge_start..edge_end] {
                        traversed_edges = traversed_edges.saturating_add(1);
                        if traversed_edges.is_multiple_of(HITS_CHECKPOINT_EDGES) {
                            control.check_cancelled()?;
                        }
                        let neighbor = usize::try_from(neighbor).map_err(|_| {
                            execution("HITS neighbor ordinal exceeds supported range")
                        })?;
                        score += input[neighbor];
                    }
                    local.push(score);
                }
                Ok((start, local))
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;
    // Merge chunk outputs in ascending dense-ordinal range order (canonical).
    for (start, local) in chunk_results {
        output[start..start + local.len()].copy_from_slice(&local);
    }
    Ok(())
}

fn run_hits_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("HITS worker panicked")),
    }
}

fn normalize_hits(scores: &mut [f64], kind: &str) -> Result<(), AlgorithmError> {
    let norm = scores.iter().map(|score| score * score).sum::<f64>().sqrt();
    if !norm.is_finite() {
        return Err(execution(format!("HITS {kind} norm is not finite")));
    }
    if norm == 0.0 {
        return Ok(());
    }
    for score in scores {
        *score /= norm;
        if !score.is_finite() {
            return Err(execution(format!("HITS {kind} score is not finite")));
        }
    }
    Ok(())
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
    label: TypeId,
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
    label: TypeId,
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
    label: TypeId,
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
    label: TypeId,
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
    label: TypeId,
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
            label: Some(label),
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

    fn execute_degree(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_rank_with_compute(graph, Algorithm::Rank(RankAlgorithm::Degree), limits, None)
    }

    fn execute_degree_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            cancellation,
        )
        .with_compute_pool(pool);
        registry.execute(Algorithm::Rank(RankAlgorithm::Degree), graph, &control)
    }

    fn degree_bits(output: &AlgorithmOutput) -> Vec<u64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score.to_bits(),
                _ => panic!("degree score must be Float64"),
            })
            .collect()
    }

    fn degree_parallel_graph(nodes: usize) -> AdjacencyGraph {
        let edges = (0..nodes)
            .map(|node| (node as u64, ((node + 1) % nodes) as u64))
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }

    fn execute_pagerank(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::PageRank),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_pagerank_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            cancellation,
        )
        .with_compute_pool(pool);
        registry.execute(Algorithm::Rank(RankAlgorithm::PageRank), graph, &control)
    }

    fn pagerank_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("pagerank score must be Float64"),
            })
            .collect()
    }

    fn pagerank_bits(output: &AlgorithmOutput) -> Vec<u64> {
        pagerank_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    fn dense_cycle_graph(nodes: usize) -> AdjacencyGraph {
        // Enough parallel edges per source to clear the documented crossover on
        // modest node counts while keeping the fixture deterministic.
        let fanout =
            ((PAGERANK_PARALLEL_CROSSOVER_EDGES as usize) / nodes.max(1)).saturating_add(2);
        let edges = (0..nodes)
            .flat_map(|node| {
                (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }

    fn triangle_thread_matrix_graph() -> AdjacencyGraph {
        let nodes = TRIANGLES_PARALLEL_CROSSOVER_NODES + 32;
        let mut edges = Vec::with_capacity(nodes * 4);
        for node in 0..nodes {
            let a = node as u64;
            let b = ((node + 1) % nodes) as u64;
            let c = ((node + 2) % nodes) as u64;
            edges.push((a, b));
            edges.push((b, c));
            edges.push((c, a));
            if node.is_multiple_of(17) {
                edges.push((a, b));
            }
            if node.is_multiple_of(23) {
                edges.push((a, a));
            }
        }
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }

    fn execute_betweenness(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::Betweenness),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_betweenness_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(limits.with_compute_threads(threads), cancellation)
            .with_compute_pool(pool);
        registry.execute(Algorithm::Rank(RankAlgorithm::Betweenness), graph, &control)
    }

    fn betweenness_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("betweenness score must be Float64"),
            })
            .collect()
    }

    fn betweenness_bits(output: &AlgorithmOutput) -> Vec<u64> {
        betweenness_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    fn betweenness_parallel_graph() -> AdjacencyGraph {
        let nodes = 72_usize;
        let mut edges = Vec::new();
        for source in 0..nodes {
            let degree = 8 + (source % 17);
            for hop in 1..=degree {
                edges.push((source as u64, ((source + hop) % nodes) as u64));
            }
            if source.is_multiple_of(5) {
                edges.push((source as u64, source as u64));
                edges.push((source as u64, ((source + 1) % nodes) as u64));
            }
        }
        let graph = AdjacencyGraph::with_test_edges(nodes as u64, &edges);
        assert!(
            betweenness_work_estimate(graph.node_ids().len(), graph.edge_entry_count())
                >= BETWEENNESS_PARALLEL_CROSSOVER_WORK
        );
        graph
    }

    fn execute_closeness(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::Closeness),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_closeness_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_closeness_with_pool_and_limits(
            graph,
            threads,
            AlgorithmLimits::default(),
            cancellation,
        )
    }

    fn execute_closeness_with_pool_and_limits(
        graph: &AdjacencyGraph,
        threads: usize,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(limits.with_compute_threads(threads), cancellation)
            .with_compute_pool(pool);
        registry.execute(Algorithm::Rank(RankAlgorithm::Closeness), graph, &control)
    }
    fn dense_closeness_graph(nodes: usize) -> AdjacencyGraph {
        let fanout = ((CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS as usize) / nodes.max(1).pow(2))
            .saturating_add(2)
            .max(2);
        let edges = (0..nodes)
            .flat_map(|node| {
                (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }
    fn closeness_bits(output: &AlgorithmOutput) -> Vec<u64> {
        closeness_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    fn closeness_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("closeness score must be Float64"),
            })
            .collect()
    }

    fn execute_harmonic_closeness(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::HarmonicCloseness),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn harmonic_closeness_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("harmonic closeness score must be Float64"),
            })
            .collect()
    }

    fn execute_eigenvector(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::Eigenvector),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_eigenvector_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            cancellation,
        )
        .with_compute_pool(pool);
        registry.execute(Algorithm::Rank(RankAlgorithm::Eigenvector), graph, &control)
    }

    fn eigenvector_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("eigenvector score must be Float64"),
            })
            .collect()
    }

    fn eigenvector_bits(output: &AlgorithmOutput) -> Vec<u64> {
        eigenvector_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    fn eigenvector_scores_for(graph: &AdjacencyGraph) -> Vec<f64> {
        eigenvector_scores(
            &execute_eigenvector(
                graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        )
    }

    fn dense_eigenvector_graph(nodes: usize) -> AdjacencyGraph {
        let fanout =
            ((EIGENVECTOR_PARALLEL_CROSSOVER_EDGES as usize) / nodes.max(1)).saturating_add(2);
        let edges = (0..nodes)
            .flat_map(|node| {
                (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }

    fn execute_article_rank(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::ArticleRank),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_article_rank_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        execute_article_rank_with_shared_pool(graph, pool, cancellation)
    }

    fn execute_article_rank_with_shared_pool(
        graph: &AdjacencyGraph,
        pool: Arc<crate::ComputePool>,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let threads = pool.num_threads();
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            cancellation,
        )
        .with_compute_pool(pool);
        registry.execute(Algorithm::Rank(RankAlgorithm::ArticleRank), graph, &control)
    }

    fn article_rank_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("ArticleRank score must be Float64"),
            })
            .collect()
    }

    fn article_rank_bits(output: &AlgorithmOutput) -> Vec<u64> {
        article_rank_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    fn article_rank_fingerprint(output: &AlgorithmOutput) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update((output.rows().len() as u64).to_le_bytes());
        for row in output.rows() {
            match row.as_slice() {
                [AlgorithmValue::Uuid(uuid), AlgorithmValue::Float64(score)] => {
                    hasher.update(uuid);
                    hasher.update(score.to_bits().to_le_bytes());
                }
                _ => panic!("ArticleRank output row must contain uuid and score"),
            }
        }
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn dense_article_rank_graph(nodes: usize) -> AdjacencyGraph {
        let fanout =
            ((ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES as usize) / nodes.max(1)).saturating_add(2);
        let edges = (0..nodes)
            .flat_map(|node| {
                (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }

    fn execute_hits_hub(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::HitsHub),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_hits_hub_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            cancellation,
        )
        .with_compute_pool(pool);
        registry.execute(Algorithm::Rank(RankAlgorithm::HitsHub), graph, &control)
    }

    fn hits_hub_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("HITS hub score must be Float64"),
            })
            .collect()
    }

    fn hits_hub_bits(output: &AlgorithmOutput) -> Vec<u64> {
        hits_hub_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    fn dense_hits_graph(nodes: usize) -> AdjacencyGraph {
        let fanout = ((HITS_PARALLEL_CROSSOVER_EDGES as usize) / nodes.max(1)).saturating_add(3);
        let edges = (0..nodes)
            .flat_map(|source| {
                (0..fanout).map(move |hop| {
                    let target = (source + hop + usize::from(hop % 3 == 0)) % nodes;
                    (source as u64, target as u64)
                })
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }

    fn execute_hits_authority(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::HitsAuthority),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_hits_authority_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            cancellation,
        )
        .with_compute_pool(pool);
        registry.execute(
            Algorithm::Rank(RankAlgorithm::HitsAuthority),
            graph,
            &control,
        )
    }

    fn hits_authority_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("HITS authority score must be Float64"),
            })
            .collect()
    }

    fn hits_authority_bits(output: &AlgorithmOutput) -> Vec<u64> {
        hits_authority_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    fn execute_celf(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::Celf),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn celf_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_clustering_coefficient(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::ClusteringCoefficient),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_clustering_coefficient_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            cancellation,
        )
        .with_compute_pool(pool);
        registry.execute(
            Algorithm::Rank(RankAlgorithm::ClusteringCoefficient),
            graph,
            &control,
        )
    }

    fn clustering_coefficient_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn clustering_coefficient_bits(output: &AlgorithmOutput) -> Vec<u64> {
        clustering_coefficient_output_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    fn dense_clustering_graph(nodes: usize) -> AdjacencyGraph {
        let fanout = 32_usize.min(nodes.saturating_sub(1));
        let edges = (0..nodes)
            .flat_map(|node| {
                (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }

    fn execute_triangles(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::Triangles),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_triangles_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            cancellation,
        )
        .with_compute_pool(pool);
        registry.execute(Algorithm::Rank(RankAlgorithm::Triangles), graph, &control)
    }

    fn triangle_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn triangle_bits(output: &AlgorithmOutput) -> Vec<u64> {
        triangle_output_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    fn execute_k_core(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::KCore),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn k_core_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_preferential_attachment(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::PreferentialAttachment),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn preferential_attachment_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_adamic_adar(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::AdamicAdar),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_adamic_adar_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(limits.with_compute_threads(threads), cancellation)
            .with_compute_pool(pool);
        registry.execute(Algorithm::Rank(RankAlgorithm::AdamicAdar), graph, &control)
    }
    fn dense_adamic_adar_graph(nodes: usize) -> AdjacencyGraph {
        let max_fanout = nodes.saturating_sub(1).max(1) / 2;
        let fanout = ((ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK as usize)
            / (nodes.max(1) * nodes.max(1)))
        .clamp(4, max_fanout.max(1));
        adamic_adar_ring_graph(nodes, fanout)
    }

    fn adamic_adar_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn adamic_adar_bits(output: &AlgorithmOutput) -> Vec<u64> {
        adamic_adar_output_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }
    fn adamic_adar_ring_graph(nodes: usize, fanout: usize) -> AdjacencyGraph {
        let fanout = fanout.clamp(1, (nodes.saturating_sub(1) / 2).max(1));
        let edges = (0..nodes)
            .flat_map(|node| {
                (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }

    fn execute_common_neighbors(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::CommonNeighbors),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn execute_common_neighbors_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(limits.with_compute_threads(threads), cancellation)
            .with_compute_pool(pool);
        registry.execute(
            Algorithm::Rank(RankAlgorithm::CommonNeighbors),
            graph,
            &control,
        )
    }

    fn common_neighbor_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn common_neighbor_bits(output: &AlgorithmOutput) -> Vec<u64> {
        common_neighbor_output_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    fn dense_common_neighbors_graph(nodes: usize) -> AdjacencyGraph {
        let max_fanout = nodes.saturating_sub(1).max(1) / 2;
        let fanout = ((COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK as usize)
            / (nodes.max(1) * nodes.max(1)))
        .clamp(4, max_fanout.max(1));
        common_neighbors_ring_graph(nodes, fanout)
    }

    fn common_neighbors_ring_graph(nodes: usize, fanout: usize) -> AdjacencyGraph {
        let fanout = fanout.clamp(1, (nodes.saturating_sub(1) / 2).max(1));
        let edges = (0..nodes)
            .flat_map(|node| {
                (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }

    fn execute_resource_allocation(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::ResourceAllocation),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn resource_allocation_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_total_neighbors(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::TotalNeighbors),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn total_neighbor_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn assert_scores_close(actual: &[f64], expected: &[f64]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert!(
                (actual - expected).abs() < 1.0e-12,
                "{actual} != {expected}"
            );
        }
    }

    fn assert_scores_within(actual: &[f64], expected: &[f64], tolerance: f64) {
        assert_eq!(actual.len(), expected.len());
        assert!(
            actual
                .iter()
                .zip(expected)
                .all(|(actual, expected)| (actual - expected).abs() <= tolerance)
        );
    }

    #[test]
    fn degree_scores_a_hand_verifiable_fixture_in_stable_uuid_order() {
        let output = execute_degree(
            &AdjacencyGraph::with_test_counts(3, 4),
            AlgorithmLimits::default(),
        )
        .unwrap();
        assert_eq!(
            output.rows(),
            vec![
                vec![AlgorithmValue::Uuid([0; 16]), AlgorithmValue::Float64(2.0)],
                vec![
                    AlgorithmValue::Uuid(u128::from(1_u64).to_be_bytes()),
                    AlgorithmValue::Float64(0.0),
                ],
                vec![
                    AlgorithmValue::Uuid(u128::from(2_u64).to_be_bytes()),
                    AlgorithmValue::Float64(0.0),
                ],
            ]
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        assert_eq!(registry.capabilities()[0].dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn degree_handles_empty_graphs_and_shared_resource_limits() {
        assert!(
            execute_degree(&AdjacencyGraph::default(), AlgorithmLimits::default())
                .unwrap()
                .rows()
                .is_empty()
        );
        let limits = AlgorithmLimits {
            nodes: 2,
            ..AlgorithmLimits::default()
        };
        assert_eq!(
            execute_degree(&AdjacencyGraph::with_test_counts(3, 0), limits),
            Err(AlgorithmError::NodeLimit {
                observed: 3,
                limit: 2,
            })
        );
    }

    #[test]
    fn degree_path_selection_respects_threads_crossover_and_pool() {
        let serial_control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_degree_path(&serial_control, DEGREE_PARALLEL_CROSSOVER_NODES - 1),
            DegreeExecutionPath::Serial
        );

        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_degree_path(&one, DEGREE_PARALLEL_CROSSOVER_NODES),
            DegreeExecutionPath::Serial
        );

        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert!(matches!(
            select_degree_path(&parallel, DEGREE_PARALLEL_CROSSOVER_NODES),
            DegreeExecutionPath::Parallel { threads: 4, chunks }
            if chunks > 1
        ));
    }

    #[test]
    fn degree_parallel_matches_one_thread_bits_at_supported_thread_counts() {
        let graph = degree_parallel_graph(DEGREE_PARALLEL_CROSSOVER_NODES);
        let oracle = degree_bits(
            &execute_degree_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap(),
        );
        for threads in [2_usize, 4, 8] {
            let actual = degree_bits(
                &execute_degree_with_pool(&graph, threads, AlgorithmCancellation::default())
                    .unwrap(),
            );
            assert_eq!(actual, oracle, "threads={threads}");
        }
    }

    #[test]
    fn degree_parallel_path_honors_cancellation() {
        let graph = degree_parallel_graph(DEGREE_PARALLEL_CROSSOVER_NODES);
        let cancel = AlgorithmCancellation::default();
        cancel.cancel();
        let err = execute_degree_with_pool(&graph, 4, cancel).unwrap_err();
        assert_eq!(err, AlgorithmError::Cancelled);
    }

    #[test]
    fn pagerank_scores_hand_verifiable_graphs_deterministically() {
        let cycle = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        let first = execute_pagerank(
            &cycle,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(pagerank_scores(&first), [0.5, 0.5]);
        assert_eq!(
            first,
            execute_pagerank(
                &cycle,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::PageRank).result_schema()
        );

        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
        assert_eq!(
            pagerank_scores(
                &execute_pagerank(
                    &disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [0.25, 0.25, 0.25, 0.25]
        );

        let empty = execute_pagerank(
            &AdjacencyGraph::default(),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert!(empty.rows().is_empty());
    }

    #[test]
    fn pagerank_handles_dangling_parallel_self_loop_and_direction_semantics() {
        let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]);
        let scores = pagerank_scores(
            &execute_pagerank(
                &multigraph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        assert!((scores.iter().sum::<f64>() - 1.0).abs() < 1.0e-9);
        assert!(scores[1] > scores[2]);
        assert!(scores[1] > scores[0]);

        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        assert_ne!(
            pagerank_scores(
                &execute_pagerank(
                    &directed,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            pagerank_scores(
                &execute_pagerank(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn pagerank_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert_eq!(
            execute_pagerank(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        assert!(matches!(
            execute_pagerank(
                &graph,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_pagerank(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let registry = {
            let mut registry = AlgorithmRegistry::default();
            register_rank_algorithms(&mut registry).unwrap();
            registry
        };
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::PageRank))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn pagerank_path_selection_respects_crossover_and_one_thread() {
        let serial_control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_pagerank_path(&serial_control, PAGERANK_PARALLEL_CROSSOVER_EDGES - 1, 64),
            PageRankExecutionPath::Serial
        );
        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_pagerank_path(&one, PAGERANK_PARALLEL_CROSSOVER_EDGES, 64),
            PageRankExecutionPath::Serial
        );
        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_pagerank_path(&parallel, PAGERANK_PARALLEL_CROSSOVER_EDGES, 64),
            PageRankExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn pagerank_thread_matrix_matches_one_thread_bits_and_ordering() {
        // Above crossover so multi-thread policies exercise the parallel path.
        let graph = dense_cycle_graph(128);
        assert!(graph.edge_entry_count() >= PAGERANK_PARALLEL_CROSSOVER_EDGES);
        let serial =
            execute_pagerank_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
        let serial_bits = pagerank_bits(&serial);
        let serial_rows = serial.rows();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_pagerank_with_pool(&graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(pagerank_bits(&parallel), serial_bits);
        }
    }

    #[test]
    fn pagerank_parallel_preserves_dangling_parallel_self_loop_and_direction_bits() {
        let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]);
        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        let empty = AdjacencyGraph::default();
        let single = AdjacencyGraph::with_test_edges(1, &[]);
        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
        for graph in [
            &multigraph,
            &directed,
            &undirected,
            &empty,
            &single,
            &disconnected,
        ] {
            let serial =
                execute_pagerank_with_pool(graph, 1, AlgorithmCancellation::default()).unwrap();
            for threads in [2_usize, 4, 8] {
                // Force parallel path selection by attaching a multi-thread pool even
                // when edge counts are below the crossover: call pull through registry
                // with an oversized synthetic control only when edges meet crossover,
                // otherwise verify serial path still matches across thread budgets.
                let parallel =
                    execute_pagerank_with_pool(graph, threads, AlgorithmCancellation::default())
                        .unwrap();
                assert_eq!(pagerank_bits(&parallel), pagerank_bits(&serial));
                assert_eq!(parallel.rows(), serial.rows());
            }
        }

        // Adversarial magnitudes: force parallel on a graph above crossover with
        // dangling nodes and uneven outdegrees.
        let nodes = 512_u64;
        let mut edges = Vec::new();
        for source in 0..(nodes - 64) {
            let degree = 12 + (source % 17) as usize;
            for hop in 0..degree {
                edges.push((source, (source + 1 + hop as u64) % nodes));
            }
        }
        // Leave high-index nodes dangling.
        let adversarial = AdjacencyGraph::with_test_edges(nodes, &edges);
        assert!(adversarial.edge_entry_count() >= PAGERANK_PARALLEL_CROSSOVER_EDGES);
        let serial =
            execute_pagerank_with_pool(&adversarial, 1, AlgorithmCancellation::default()).unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_pagerank_with_pool(&adversarial, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(pagerank_bits(&parallel), pagerank_bits(&serial));
        }
    }

    #[test]
    fn pagerank_parallel_cancellation_returns_structured_cancelled() {
        let graph = dense_cycle_graph(128);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_pagerank_with_pool(&graph, 4, cancellation),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn pagerank_destination_chunks_cover_canonical_ranges() {
        assert_eq!(destination_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(destination_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(destination_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(
            destination_chunks(8, 4),
            vec![(0, 2), (2, 4), (4, 6), (6, 8)]
        );
        assert_eq!(destination_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    fn pagerank_pull_matches_serial_scatter_contribution_order() {
        let fixtures = [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]),
            AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
            AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]),
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
            AdjacencyGraph::with_test_edges(1, &[]),
            dense_cycle_graph(64),
        ];
        for graph in &fixtures {
            if graph.node_ids().is_empty() {
                continue;
            }
            let prepared = prepare_pagerank(graph).unwrap();
            let scores = vec![1.0 / graph.node_ids().len() as f64; graph.node_ids().len()];
            let base = 0.15 / graph.node_ids().len() as f64;
            let mut scatter = vec![base; scores.len()];
            pagerank_scatter_serial(graph, &prepared.indices, &scores, &mut scatter).unwrap();
            let mut pull = vec![0.0; scores.len()];
            for dest in 0..scores.len() {
                pull[dest] = pagerank_pull_destination(
                    &prepared.inbound,
                    &prepared.outdegrees,
                    &scores,
                    base,
                    dest,
                );
            }
            assert_eq!(
                scatter.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                pull.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                "pull must apply contributions in serial source/edge order"
            );
        }
    }

    #[test]
    fn betweenness_scores_directed_and_undirected_chains_deterministically() {
        let directed = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let first = execute_betweenness(
            &directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(&betweenness_scores(&first), &[0.0, 0.5, 0.0]);
        assert_eq!(
            first,
            execute_betweenness(
                &directed,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::Betweenness).result_schema()
        );

        let undirected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]);
        assert_scores_close(
            &betweenness_scores(
                &execute_betweenness(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 1.0, 0.0],
        );
    }

    #[test]
    fn betweenness_handles_parallel_self_loop_disconnected_and_empty_graphs() {
        let multigraph =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 2), (0, 3), (3, 2), (1, 1)]);
        assert_scores_close(
            &betweenness_scores(
                &execute_betweenness(
                    &multigraph,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 1.0 / 9.0, 0.0, 1.0 / 18.0],
        );

        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2)]);
        assert_scores_close(
            &betweenness_scores(
                &execute_betweenness(
                    &disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 1.0 / 6.0, 0.0, 0.0],
        );
        assert!(
            execute_betweenness(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn betweenness_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_betweenness(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        assert!(matches!(
            execute_betweenness(
                &graph,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            execute_betweenness(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_betweenness(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert_eq!(
            execute_betweenness(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 2,
                limit: 1,
            })
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Betweenness))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn betweenness_path_selection_respects_crossover_and_one_thread() {
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_betweenness_path(&no_pool, 128, BETWEENNESS_PARALLEL_CROSSOVER_WORK),
            BetweennessExecutionPath::Serial
        );
        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_betweenness_path(&one, 128, BETWEENNESS_PARALLEL_CROSSOVER_WORK),
            BetweennessExecutionPath::Serial
        );
        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_betweenness_path(&parallel, 128, 1),
            BetweennessExecutionPath::Serial
        );
        assert_eq!(
            select_betweenness_path(&parallel, 128, BETWEENNESS_PARALLEL_CROSSOVER_WORK),
            BetweennessExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn betweenness_thread_matrix_matches_one_thread_bits_and_ordering() {
        let graph = betweenness_parallel_graph();
        let serial = execute_betweenness_with_pool(
            &graph,
            1,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        let serial_bits = betweenness_bits(&serial);
        let serial_rows = serial.rows();
        for threads in [2_usize, 4, 8] {
            let parallel = execute_betweenness_with_pool(
                &graph,
                threads,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(betweenness_bits(&parallel), serial_bits);
        }
    }

    #[test]
    fn betweenness_parallel_limits_and_cancellation_are_structured() {
        let graph = betweenness_parallel_graph();
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_betweenness_with_pool(&graph, 4, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert_eq!(
            execute_betweenness_with_pool(
                &graph,
                4,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
    }

    #[test]
    fn source_chunks_cover_canonical_ranges() {
        assert_eq!(source_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(source_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(source_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(source_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
        assert_eq!(source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    fn closeness_scores_directed_and_undirected_chains_deterministically() {
        let directed = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let first = execute_closeness(
            &directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(&closeness_scores(&first), &[2.0 / 3.0, 0.5, 0.0]);
        assert_eq!(
            first,
            execute_closeness(
                &directed,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::Closeness).result_schema()
        );

        let undirected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]);
        assert_scores_close(
            &closeness_scores(
                &execute_closeness(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[2.0 / 3.0, 1.0, 2.0 / 3.0],
        );
    }

    #[test]
    fn closeness_handles_parallel_self_loop_disconnected_and_empty_graphs() {
        let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 2), (1, 1)]);
        assert_scores_close(
            &closeness_scores(
                &execute_closeness(
                    &graph,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[4.0 / 9.0, 1.0 / 3.0, 0.0, 0.0],
        );
        assert!(
            execute_closeness(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
        assert_eq!(
            closeness_scores(
                &execute_closeness(
                    &AdjacencyGraph::with_test_counts(1, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [0.0]
        );
    }

    #[test]
    fn closeness_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_closeness(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        assert!(matches!(
            execute_closeness(
                &graph,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            execute_closeness(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_closeness(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert_eq!(
            execute_closeness(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 2,
                limit: 1,
            })
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Closeness))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn closeness_path_selection_respects_crossover_and_one_thread() {
        let serial_control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_closeness_path(
                &serial_control,
                64,
                (CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS / 64) - 1
            ),
            ClosenessExecutionPath::Serial
        );
        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_closeness_path(&one, 64, CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS),
            ClosenessExecutionPath::Serial
        );
        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_closeness_path(&parallel, 64, CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS),
            ClosenessExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }
    #[test]
    fn closeness_thread_matrix_matches_one_thread_bits_and_ordering() {
        let graph = dense_closeness_graph(128);
        assert!(
            estimated_closeness_edge_visits(graph.node_ids().len(), graph.edge_entry_count())
                >= CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS
        );
        let serial =
            execute_closeness_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
        let serial_bits = closeness_bits(&serial);
        let serial_rows = serial.rows();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_closeness_with_pool(&graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(closeness_bits(&parallel), serial_bits);
        }
    }
    #[test]
    fn closeness_parallel_preserves_boundary_graph_bits() {
        let multigraph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 2), (1, 1)]);
        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        let empty = AdjacencyGraph::default();
        let single = AdjacencyGraph::with_test_edges(1, &[]);
        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2)]);
        for graph in [
            &multigraph,
            &directed,
            &undirected,
            &empty,
            &single,
            &disconnected,
        ] {
            let serial =
                execute_closeness_with_pool(graph, 1, AlgorithmCancellation::default()).unwrap();
            for threads in [2_usize, 4, 8] {
                let parallel =
                    execute_closeness_with_pool(graph, threads, AlgorithmCancellation::default())
                        .unwrap();
                assert_eq!(closeness_bits(&parallel), closeness_bits(&serial));
                assert_eq!(parallel.rows(), serial.rows());
            }
        }
    }
    #[test]
    fn closeness_parallel_cancellation_and_limits_are_structured() {
        let graph = dense_closeness_graph(128);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_closeness_with_pool(&graph, 4, cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert_eq!(
            execute_closeness_with_pool_and_limits(
                &graph,
                4,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        assert!(matches!(
            execute_closeness_with_pool_and_limits(
                &graph,
                4,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
    }
    #[test]
    fn closeness_source_chunks_cover_canonical_ranges() {
        assert_eq!(source_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(source_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(source_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(source_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
        assert_eq!(source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
    }
    #[test]
    fn closeness_worker_panic_returns_structured_error() {
        let pool = crate::ComputePool::new(2).unwrap();
        assert_eq!(
            run_closeness_on_pool(&pool, || -> Result<(), AlgorithmError> {
                panic!("synthetic closeness panic");
            }),
            Err(execution("Closeness worker panicked"))
        );
    }

    #[test]
    fn harmonic_closeness_scores_directed_and_undirected_chains_deterministically() {
        let directed = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let first = execute_harmonic_closeness(
            &directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(&harmonic_closeness_scores(&first), &[0.75, 0.5, 0.0]);
        assert_eq!(
            first,
            execute_harmonic_closeness(
                &directed,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::HarmonicCloseness).result_schema()
        );

        let undirected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]);
        assert_scores_close(
            &harmonic_closeness_scores(
                &execute_harmonic_closeness(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.75, 1.0, 0.75],
        );
    }

    #[test]
    fn harmonic_closeness_handles_parallel_self_loop_disconnected_and_empty_graphs() {
        let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 2), (1, 1)]);
        assert_scores_close(
            &harmonic_closeness_scores(
                &execute_harmonic_closeness(
                    &graph,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.5, 1.0 / 3.0, 0.0, 0.0],
        );
        assert!(
            execute_harmonic_closeness(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
        assert_eq!(
            harmonic_closeness_scores(
                &execute_harmonic_closeness(
                    &AdjacencyGraph::with_test_counts(1, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [0.0]
        );
    }

    #[test]
    fn harmonic_closeness_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_harmonic_closeness(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        assert!(matches!(
            execute_harmonic_closeness(
                &graph,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            execute_harmonic_closeness(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_harmonic_closeness(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert_eq!(
            execute_harmonic_closeness(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 2,
                limit: 1,
            })
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::HarmonicCloseness)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn eigenvector_scores_shifted_power_fixtures_deterministically() {
        let cycle = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        let first = execute_eigenvector(
            &cycle,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &eigenvector_scores(&first),
            &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt()],
        );
        assert_eq!(
            first,
            execute_eigenvector(
                &cycle,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::Eigenvector).result_schema()
        );

        let star = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 2), (1, 0), (2, 0)]);
        assert_scores_within(
            &eigenvector_scores_for(&star),
            &[1.0 / 2.0_f64.sqrt(), 0.5, 0.5],
            EIGENVECTOR_TOLERANCE,
        );
    }

    #[test]
    fn eigenvector_handles_direction_multigraph_disconnected_and_empty_graphs() {
        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let denominator = (1.0_f64 + 21.0_f64.powi(2)).sqrt();
        assert_scores_close(
            &eigenvector_scores_for(&directed),
            &[1.0 / denominator, 21.0 / denominator],
        );
        let parallel = AdjacencyGraph::with_test_edges(2, &[(0, 1), (0, 1)]);
        assert!(eigenvector_scores_for(&parallel)[1] > eigenvector_scores_for(&directed)[1]);
        let with_self_loop = AdjacencyGraph::with_test_edges(2, &[(0, 1), (0, 0)]);
        assert_ne!(
            eigenvector_scores_for(&with_self_loop),
            eigenvector_scores_for(&directed)
        );

        let disconnected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0)]);
        assert_scores_close(
            &eigenvector_scores_for(&disconnected),
            &[
                1.0 / (2.0 + 2.0_f64.powi(-40)).sqrt(),
                1.0 / (2.0 + 2.0_f64.powi(-40)).sqrt(),
                2.0_f64.powi(-20) / (2.0 + 2.0_f64.powi(-40)).sqrt(),
            ],
        );
        assert_scores_close(
            &eigenvector_scores_for(&AdjacencyGraph::with_test_counts(4, 0)),
            &[0.5; 4],
        );
        assert_eq!(
            eigenvector_scores_for(&AdjacencyGraph::with_test_counts(1, 0)),
            [1.0]
        );
        assert!(
            execute_eigenvector(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn eigenvector_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_eigenvector(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        assert!(matches!(
            execute_eigenvector(
                &graph,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            execute_eigenvector(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_eigenvector(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert_eq!(
            execute_eigenvector(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 2,
                limit: 1,
            })
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Eigenvector))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn eigenvector_path_selection_respects_crossover_and_one_thread() {
        let serial_control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_eigenvector_path(
                &serial_control,
                EIGENVECTOR_PARALLEL_CROSSOVER_EDGES - 1,
                64
            ),
            EigenvectorExecutionPath::Serial
        );
        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_eigenvector_path(&one, EIGENVECTOR_PARALLEL_CROSSOVER_EDGES, 64),
            EigenvectorExecutionPath::Serial
        );
        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_eigenvector_path(&parallel, EIGENVECTOR_PARALLEL_CROSSOVER_EDGES, 64),
            EigenvectorExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn eigenvector_thread_matrix_matches_one_thread_bits_and_ordering() {
        let graph = dense_eigenvector_graph(128);
        assert!(graph.edge_entry_count() >= EIGENVECTOR_PARALLEL_CROSSOVER_EDGES);
        let serial =
            execute_eigenvector_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
        let serial_bits = eigenvector_bits(&serial);
        let serial_rows = serial.rows();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_eigenvector_with_pool(&graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(eigenvector_bits(&parallel), serial_bits);
        }
    }

    #[test]
    fn eigenvector_parallel_preserves_multigraph_self_loop_and_disconnected_bits() {
        let fixtures = [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]),
            AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
            AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]),
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
            AdjacencyGraph::default(),
            AdjacencyGraph::with_test_edges(1, &[]),
        ];
        for graph in &fixtures {
            let serial =
                execute_eigenvector_with_pool(graph, 1, AlgorithmCancellation::default()).unwrap();
            for threads in [2_usize, 4, 8] {
                let parallel =
                    execute_eigenvector_with_pool(graph, threads, AlgorithmCancellation::default())
                        .unwrap();
                assert_eq!(eigenvector_bits(&parallel), eigenvector_bits(&serial));
                assert_eq!(parallel.rows(), serial.rows());
            }
        }

        let nodes = 512_u64;
        let mut edges = Vec::new();
        for source in 0..nodes {
            let degree = 16 + (source % 11) as usize;
            for hop in 0..degree {
                edges.push((source, (source + hop as u64) % nodes));
            }
        }
        let adversarial = AdjacencyGraph::with_test_edges(nodes, &edges);
        assert!(adversarial.edge_entry_count() >= EIGENVECTOR_PARALLEL_CROSSOVER_EDGES);
        let serial =
            execute_eigenvector_with_pool(&adversarial, 1, AlgorithmCancellation::default())
                .unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel = execute_eigenvector_with_pool(
                &adversarial,
                threads,
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(eigenvector_bits(&parallel), eigenvector_bits(&serial));
        }
    }

    #[test]
    fn eigenvector_parallel_cancellation_returns_structured_cancelled() {
        let graph = dense_eigenvector_graph(128);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_eigenvector_with_pool(&graph, 4, cancellation),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn eigenvector_pull_matches_serial_scatter_contribution_order() {
        let fixtures = [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]),
            AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
            AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]),
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
            AdjacencyGraph::with_test_edges(1, &[]),
            dense_eigenvector_graph(64),
        ];
        for graph in &fixtures {
            if graph.node_ids().is_empty() {
                continue;
            }
            let indices = graph
                .node_ids()
                .iter()
                .enumerate()
                .map(|(index, &node)| (node, index))
                .collect::<HashMap<_, _>>();
            let inbound = prepare_eigenvector_inbound(graph, &indices).unwrap();
            let scores = (0..graph.node_ids().len())
                .map(|index| (index + 1) as f64 / (graph.node_ids().len() + 1) as f64)
                .collect::<Vec<_>>();
            let control =
                AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
            let scatter =
                eigenvector_scatter_serial(graph, &indices, graph.node_ids(), &scores, &control)
                    .unwrap();
            let pull = (0..scores.len())
                .map(|dest| eigenvector_pull_destination(&inbound, &scores, dest))
                .collect::<Vec<_>>();
            assert_eq!(
                scatter
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                pull.iter().map(|value| value.to_bits()).collect::<Vec<_>>(),
                "pull must apply contributions in serial source/edge order"
            );
        }
    }

    #[test]
    fn eigenvector_worker_panic_returns_structured_error() {
        let pool = crate::ComputePool::new(2).unwrap();
        assert_eq!(
            run_eigenvector_on_pool(&pool, || -> Result<(), AlgorithmError> {
                panic!("worker panic is converted")
            }),
            Err(AlgorithmError::Execution {
                message: "Eigenvector worker panicked".into()
            })
        );
    }

    #[test]
    #[ignore = "manual crossover measurement; run in release with --ignored --nocapture"]
    fn measure_eigenvector_parallel_crossover() {
        use std::time::Instant;

        let mut cases = Vec::new();
        for (nodes, fanout) in [(128_usize, 32_usize), (512, 64)] {
            let edges = (0..nodes)
                .flat_map(|node| {
                    (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
                })
                .collect::<Vec<_>>();
            cases.push((format!("regular nodes={nodes} fanout={fanout}"), edges));
        }
        for (nodes, base, spread) in [
            (512_usize, 9_usize, 17_usize),
            (512, 32, 33),
            (2_048, 16, 33),
            (2_048, 32, 65),
        ] {
            let edges = (0..nodes)
                .flat_map(|node| {
                    let degree = base + (node % spread);
                    (0..degree).map(move |hop| {
                        let step = 1 + ((hop * 17 + node) % nodes);
                        (node as u64, ((node + step) % nodes) as u64)
                    })
                })
                .collect::<Vec<_>>();
            cases.push((
                format!("irregular nodes={nodes} base={base} spread={spread}"),
                edges,
            ));
        }

        for (label, edges) in cases {
            let nodes = edges
                .iter()
                .flat_map(|(source, target)| [*source, *target])
                .max()
                .unwrap_or(0)
                + 1;
            let graph = AdjacencyGraph::with_test_edges(nodes, &edges);
            let edges = graph.edge_entry_count();
            let _ =
                execute_eigenvector_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
            let _ =
                execute_eigenvector_with_pool(&graph, 4, AlgorithmCancellation::default()).unwrap();

            let mut serial_ns = u128::MAX;
            let mut parallel_ns = u128::MAX;
            for _ in 0..5 {
                let t0 = Instant::now();
                let serial =
                    execute_eigenvector_with_pool(&graph, 1, AlgorithmCancellation::default())
                        .unwrap();
                serial_ns = serial_ns.min(t0.elapsed().as_nanos());

                let t1 = Instant::now();
                let parallel =
                    execute_eigenvector_with_pool(&graph, 4, AlgorithmCancellation::default())
                        .unwrap();
                parallel_ns = parallel_ns.min(t1.elapsed().as_nanos());
                assert_eq!(eigenvector_bits(&parallel), eigenvector_bits(&serial));
            }
            println!(
                "{label} edges={edges} serial_ns={serial_ns} parallel_ns={parallel_ns} ratio={}",
                parallel_ns as f64 / serial_ns as f64
            );
        }
    }

    #[test]
    fn article_rank_scores_the_canonical_recurrence_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let first = execute_article_rank(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(&article_rank_scores(&first), &[0.15, 0.235]);
        assert_eq!(
            first,
            execute_article_rank(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::ArticleRank).result_schema()
        );
    }

    #[test]
    fn article_rank_handles_direction_multigraph_disconnected_and_empty_graphs() {
        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        assert_ne!(
            article_rank_scores(
                &execute_article_rank(
                    &directed,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default()
                )
                .unwrap()
            ),
            article_rank_scores(
                &execute_article_rank(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default()
                )
                .unwrap()
            )
        );
        let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]);
        let scores = article_rank_scores(
            &execute_article_rank(
                &multigraph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        assert!(scores[1] > scores[2]);
        assert!(scores[1] > scores[0]);
        let disconnected = AdjacencyGraph::with_test_edges(3, &[(0, 1)]);
        assert_scores_close(
            &article_rank_scores(
                &execute_article_rank(
                    &disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.15, 0.245_625, 0.15],
        );
        assert_scores_close(
            &article_rank_scores(
                &execute_article_rank(
                    &AdjacencyGraph::with_test_counts(3, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.15; 3],
        );
        assert!(
            execute_article_rank(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default()
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn article_rank_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert_eq!(
            execute_article_rank(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        assert!(matches!(
            execute_article_rank(
                &graph,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_article_rank(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert!(matches!(
            execute_article_rank(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::ArticleRank))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn article_rank_path_selection_respects_crossover_and_one_thread() {
        let serial_control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_article_rank_path(
                &serial_control,
                ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES - 1,
                64
            ),
            ArticleRankExecutionPath::Serial
        );
        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_article_rank_path(&one, ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES, 64),
            ArticleRankExecutionPath::Serial
        );
        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_article_rank_path(&parallel, ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES, 64),
            ArticleRankExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn article_rank_thread_matrix_matches_one_thread_bits_and_ordering() {
        let graph = dense_article_rank_graph(128);
        assert!(graph.edge_entry_count() >= ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES);
        let serial =
            execute_article_rank_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
        let serial_bits = article_rank_bits(&serial);
        let serial_rows = serial.rows();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_article_rank_with_pool(&graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(article_rank_bits(&parallel), serial_bits);
        }
    }

    #[test]
    fn article_rank_parallel_preserves_multigraph_direction_and_disconnected_bits() {
        let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]);
        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        let empty = AdjacencyGraph::default();
        let single = AdjacencyGraph::with_test_edges(1, &[]);
        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
        for graph in [
            &multigraph,
            &directed,
            &undirected,
            &empty,
            &single,
            &disconnected,
        ] {
            let serial =
                execute_article_rank_with_pool(graph, 1, AlgorithmCancellation::default()).unwrap();
            for threads in [2_usize, 4, 8] {
                let parallel = execute_article_rank_with_pool(
                    graph,
                    threads,
                    AlgorithmCancellation::default(),
                )
                .unwrap();
                assert_eq!(article_rank_bits(&parallel), article_rank_bits(&serial));
                assert_eq!(parallel.rows(), serial.rows());
            }
        }

        let nodes = 512_u64;
        let mut edges = Vec::new();
        for source in 0..nodes {
            let degree = 256 + (source % 13) as usize;
            for hop in 0..degree {
                edges.push((source, (source + hop as u64) % nodes));
            }
        }
        let adversarial = AdjacencyGraph::with_test_edges(nodes, &edges);
        assert!(adversarial.edge_entry_count() >= ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES);
        let serial =
            execute_article_rank_with_pool(&adversarial, 1, AlgorithmCancellation::default())
                .unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel = execute_article_rank_with_pool(
                &adversarial,
                threads,
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(article_rank_bits(&parallel), article_rank_bits(&serial));
        }
    }

    #[test]
    #[ignore = "manual crossover measurement; run with --ignored --nocapture"]
    fn measure_article_rank_parallel_crossover() {
        use std::time::Instant;

        let serial_pool = Arc::new(crate::ComputePool::new(1).unwrap());
        let parallel_pool = Arc::new(crate::ComputePool::new(4).unwrap());
        for &(nodes, fanout) in &[
            (64usize, 16usize),
            (64, 32),
            (128, 32),
            (128, 64),
            (256, 64),
            (512, 64),
            (1024, 128),
            (2048, 128),
        ] {
            let edges = (0..nodes)
                .flat_map(|node| {
                    (0..fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
                })
                .collect::<Vec<_>>();
            let graph = AdjacencyGraph::with_test_edges(nodes as u64, &edges);
            let edge_count = graph.edge_entry_count();
            let serial = execute_article_rank_with_shared_pool(
                &graph,
                serial_pool.clone(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            let expected = article_rank_fingerprint(&serial);
            // Warm once so timings emphasize the kernel path over first-use setup.
            let parallel = execute_article_rank_with_shared_pool(
                &graph,
                parallel_pool.clone(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(article_rank_fingerprint(&parallel), expected);

            let mut serial_ns = u128::MAX;
            let mut parallel_ns = u128::MAX;
            for _ in 0..5 {
                let t0 = Instant::now();
                let serial = execute_article_rank_with_shared_pool(
                    &graph,
                    serial_pool.clone(),
                    AlgorithmCancellation::default(),
                )
                .unwrap();
                serial_ns = serial_ns.min(t0.elapsed().as_nanos());

                let t1 = Instant::now();
                let parallel = execute_article_rank_with_shared_pool(
                    &graph,
                    parallel_pool.clone(),
                    AlgorithmCancellation::default(),
                )
                .unwrap();
                parallel_ns = parallel_ns.min(t1.elapsed().as_nanos());
                assert_eq!(article_rank_fingerprint(&serial), expected);
                assert_eq!(article_rank_fingerprint(&parallel), expected);
            }
            eprintln!(
                "article_rank nodes={nodes} fanout={fanout} edges={edge_count} serial_ns={serial_ns} parallel_ns={parallel_ns} ratio={} fingerprint={expected}",
                parallel_ns as f64 / serial_ns as f64
            );
        }
    }

    #[test]
    fn article_rank_parallel_cancellation_and_worker_panic_are_structured() {
        let graph = dense_article_rank_graph(128);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_article_rank_with_pool(&graph, 4, cancellation),
            Err(AlgorithmError::Cancelled)
        );

        let pool = crate::ComputePool::new(2).unwrap();
        assert_eq!(
            run_article_rank_on_pool(&pool, || -> Result<(), AlgorithmError> {
                panic!("synthetic ArticleRank worker panic")
            }),
            Err(execution("ArticleRank worker panicked"))
        );
    }

    #[test]
    fn hits_hub_scores_the_canonical_recurrence_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let first = execute_hits_hub(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &hits_hub_scores(&first),
            &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0],
        );
        assert_eq!(
            first,
            execute_hits_hub(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::HitsHub).result_schema()
        );
    }

    #[test]
    fn hits_hub_handles_direction_multigraph_disconnected_and_empty_graphs() {
        let parallel = AdjacencyGraph::with_test_edges(3, &[(0, 2), (0, 2), (1, 2)]);
        assert_scores_close(
            &hits_hub_scores(
                &execute_hits_hub(
                    &parallel,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[2.0 / 5.0_f64.sqrt(), 1.0 / 5.0_f64.sqrt(), 0.0],
        );
        let self_loop = AdjacencyGraph::with_test_edges(1, &[(0, 0)]);
        assert_scores_close(
            &hits_hub_scores(
                &execute_hits_hub(
                    &self_loop,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0],
        );
        let undirected_disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0)]);
        assert_scores_close(
            &hits_hub_scores(
                &execute_hits_hub(
                    &undirected_disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0, 0.0],
        );
        assert_scores_close(
            &hits_hub_scores(
                &execute_hits_hub(
                    &AdjacencyGraph::with_test_counts(2, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 0.0],
        );
        assert!(
            execute_hits_hub(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default()
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn hits_hub_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert_eq!(
            execute_hits_hub(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        assert!(matches!(
            execute_hits_hub(
                &graph,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_hits_hub(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert!(matches!(
            execute_hits_hub(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::HitsHub))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn hits_hub_path_selection_respects_crossover_and_one_thread() {
        let serial_control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_hits_path(&serial_control, HITS_PARALLEL_CROSSOVER_EDGES - 1, 64),
            HitsExecutionPath::Serial
        );
        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_hits_path(&one, HITS_PARALLEL_CROSSOVER_EDGES, 64),
            HitsExecutionPath::Serial
        );
        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_hits_path(&parallel, HITS_PARALLEL_CROSSOVER_EDGES, 64),
            HitsExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn hits_hub_thread_matrix_matches_one_thread_bits_and_ordering() {
        let graph = dense_hits_graph(128);
        assert!(graph.edge_entry_count() >= HITS_PARALLEL_CROSSOVER_EDGES);
        let serial =
            execute_hits_hub_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
        let serial_bits = hits_hub_bits(&serial);
        let serial_rows = serial.rows();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_hits_hub_with_pool(&graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(hits_hub_bits(&parallel), serial_bits);
        }
    }

    #[test]
    fn hits_hub_parallel_preserves_multigraph_self_loop_and_disconnected_bits() {
        let mut edges = Vec::new();
        let nodes = 256_u64;
        for source in 0..nodes {
            edges.push((source, source));
            edges.push((source, (source + 1) % nodes));
            edges.push((source, (source + 1) % nodes));
            for hop in 2..18 {
                edges.push((source, (source + hop) % nodes));
            }
        }
        let graph = AdjacencyGraph::with_test_edges(nodes, &edges);
        assert!(graph.edge_entry_count() >= HITS_PARALLEL_CROSSOVER_EDGES);
        let serial =
            execute_hits_hub_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_hits_hub_with_pool(&graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(hits_hub_bits(&parallel), hits_hub_bits(&serial));
            assert_eq!(parallel.rows(), serial.rows());
        }
    }

    #[test]
    fn hits_hub_parallel_cancellation_and_worker_panic_are_structured() {
        let graph = dense_hits_graph(128);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_hits_hub_with_pool(&graph, 4, cancellation),
            Err(AlgorithmError::Cancelled)
        );

        let pool = crate::ComputePool::new(2).unwrap();
        assert_eq!(
            run_hits_on_pool(&pool, || -> Result<(), AlgorithmError> {
                panic!("synthetic HITS worker panic");
            }),
            Err(AlgorithmError::Execution {
                message: "HITS worker panicked".to_string()
            })
        );
    }

    #[test]
    fn hits_authority_scores_the_canonical_recurrence() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let first = execute_hits_authority(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &hits_authority_scores(&first),
            &[0.0, 1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt()],
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::HitsAuthority).result_schema()
        );
    }

    #[test]
    fn hits_authority_handles_multigraph_disconnected_and_empty_graphs() {
        let parallel = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2)]);
        assert_scores_close(
            &hits_authority_scores(
                &execute_hits_authority(
                    &parallel,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 2.0 / 5.0_f64.sqrt(), 1.0 / 5.0_f64.sqrt()],
        );
        let self_loop = AdjacencyGraph::with_test_edges(1, &[(0, 0)]);
        assert_scores_close(
            &hits_authority_scores(
                &execute_hits_authority(
                    &self_loop,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0],
        );
        let undirected_disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0)]);
        assert_scores_close(
            &hits_authority_scores(
                &execute_hits_authority(
                    &undirected_disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0, 0.0],
        );
        assert_scores_close(
            &hits_authority_scores(
                &execute_hits_authority(
                    &AdjacencyGraph::with_test_counts(2, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 0.0],
        );
        assert!(
            execute_hits_authority(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default()
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn hits_authority_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert_eq!(
            execute_hits_authority(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_hits_authority(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::HitsAuthority)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn hits_authority_path_selection_reuses_shared_hits_crossover() {
        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(8),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(8).unwrap()));
        assert_eq!(
            select_hits_path(&parallel, HITS_PARALLEL_CROSSOVER_EDGES, 128),
            HitsExecutionPath::Parallel {
                threads: 8,
                chunks: 8
            }
        );
        assert_eq!(
            select_hits_path(&parallel, HITS_PARALLEL_CROSSOVER_EDGES - 1, 128),
            HitsExecutionPath::Serial
        );
    }

    #[test]
    fn hits_authority_thread_matrix_matches_one_thread_bits_and_ordering() {
        let graph = dense_hits_graph(128);
        assert!(graph.edge_entry_count() >= HITS_PARALLEL_CROSSOVER_EDGES);
        let serial =
            execute_hits_authority_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
        let serial_bits = hits_authority_bits(&serial);
        let serial_rows = serial.rows();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_hits_authority_with_pool(&graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(hits_authority_bits(&parallel), serial_bits);
        }
    }

    #[test]
    fn hits_authority_parallel_preserves_multigraph_self_loop_and_disconnected_bits() {
        let mut edges = Vec::new();
        let nodes = 256_u64;
        for source in 0..nodes {
            edges.push((source, source));
            edges.push((source, (source + 1) % nodes));
            edges.push((source, (source + 1) % nodes));
            for hop in 2..18 {
                edges.push((source, (source + hop) % nodes));
            }
        }
        let graph = AdjacencyGraph::with_test_edges(nodes, &edges);
        assert!(graph.edge_entry_count() >= HITS_PARALLEL_CROSSOVER_EDGES);
        let serial =
            execute_hits_authority_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_hits_authority_with_pool(&graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(hits_authority_bits(&parallel), hits_authority_bits(&serial));
            assert_eq!(parallel.rows(), serial.rows());
        }
    }

    #[test]
    fn hits_authority_parallel_cancellation_returns_structured_cancelled() {
        let graph = dense_hits_graph(128);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_hits_authority_with_pool(&graph, 4, cancellation),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn celf_scores_edgeless_graphs_deterministically() {
        let graph = AdjacencyGraph::with_test_counts(3, 0);
        let first = execute_celf(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(&celf_output_scores(&first), &[1.0, 1.0, 1.0]);
    }

    #[test]
    fn celf_handles_direction_multigraph_self_loop_and_empty_graphs() {
        let single = celf_output_scores(
            &execute_celf(
                &AdjacencyGraph::with_test_edges(3, &[(0, 1)]),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        assert!(single[0] > 1.0 && single[1] < 1.0 && (single[2] - 1.0).abs() <= 1.0e-12);
        let parallel = celf_output_scores(
            &execute_celf(
                &AdjacencyGraph::with_test_edges(2, &[(0, 1), (0, 1)]),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        assert!(parallel[0] > single[0]);
        assert_scores_close(
            &celf_output_scores(
                &execute_celf(
                    &AdjacencyGraph::with_test_edges(1, &[(0, 0)]),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0],
        );
        let directed_chain = celf_output_scores(
            &execute_celf(
                &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        let symmetric_chain = celf_output_scores(
            &execute_celf(
                &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        assert_ne!(symmetric_chain, directed_chain);
    }

    #[test]
    fn celf_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert!(matches!(
            execute_celf(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_celf(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Celf))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn clustering_coefficient_matches_directed_and_undirected_triangles() {
        let directed_cycle = execute_clustering_coefficient(
            &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &clustering_coefficient_output_scores(&directed_cycle),
            &[0.5, 0.5, 0.5],
        );

        let undirected_triangle = execute_clustering_coefficient(
            &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2)]),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &clustering_coefficient_output_scores(&undirected_triangle),
            &[1.0, 1.0, 1.0],
        );
    }

    #[test]
    fn clustering_coefficient_simplifies_multigraphs_and_retains_all_nodes() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 1),
                (0, 1),
                (1, 0),
                (1, 2),
                (2, 1),
                (2, 0),
                (0, 2),
                (0, 0),
                (3, 4),
            ],
        );
        let first = execute_clustering_coefficient(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &clustering_coefficient_output_scores(&first),
            &[1.0, 1.0, 1.0, 0.0, 0.0],
        );
        assert_eq!(
            first,
            execute_clustering_coefficient(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert!(
            execute_clustering_coefficient(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn clustering_coefficient_path_selection_respects_crossover_and_one_thread() {
        let serial_control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_clustering_coefficient_path(
                &serial_control,
                CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK,
                64
            ),
            ClusteringCoefficientExecutionPath::Serial
        );

        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_clustering_coefficient_path(
                &one,
                CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK,
                64
            ),
            ClusteringCoefficientExecutionPath::Serial
        );

        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_clustering_coefficient_path(
                &parallel,
                CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK - 1,
                64
            ),
            ClusteringCoefficientExecutionPath::Serial
        );
        assert_eq!(
            select_clustering_coefficient_path(
                &parallel,
                CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK,
                64
            ),
            ClusteringCoefficientExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn clustering_coefficient_thread_matrix_matches_one_thread_bits_and_ordering() {
        let graph = dense_clustering_graph(128);
        let prepared = prepare_clustering_coefficient(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
        )
        .unwrap();
        assert!(
            prepared.work_units >= CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK,
            "fixture should exercise the parallel path"
        );

        let serial =
            execute_clustering_coefficient_with_pool(&graph, 1, AlgorithmCancellation::default())
                .unwrap();
        let serial_bits = clustering_coefficient_bits(&serial);
        let serial_rows = serial.rows();
        for threads in [2_usize, 4, 8] {
            let parallel = execute_clustering_coefficient_with_pool(
                &graph,
                threads,
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(clustering_coefficient_bits(&parallel), serial_bits);
        }
    }

    #[test]
    fn clustering_coefficient_parallel_cancels_and_worker_panics_are_structured() {
        let graph = dense_clustering_graph(128);
        let prepared = prepare_clustering_coefficient(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
        )
        .unwrap();
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled_control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            cancellation,
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            clustering_coefficient_scores_parallel(&prepared, &cancelled_control),
            Err(AlgorithmError::Cancelled)
        );

        let pool = crate::ComputePool::new(2).unwrap();
        assert_eq!(
            run_clustering_coefficient_on_pool(&pool, || -> () { panic!("boom") }),
            Err(AlgorithmError::Execution {
                message: "clustering coefficient worker panicked".into()
            })
        );
    }

    #[test]
    fn clustering_coefficient_uses_shared_controls_and_canonical_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        assert!(matches!(
            execute_clustering_coefficient(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_clustering_coefficient(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            execute_clustering_coefficient(
                &graph,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::ClusteringCoefficient)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "clustering_coefficient");
    }

    #[test]
    fn triangles_count_overlapping_cliques_in_stable_node_order() {
        let graph = AdjacencyGraph::with_test_edges(
            6,
            &[(0, 1), (1, 2), (2, 0), (0, 2), (2, 3), (3, 0), (4, 5)],
        );
        let output = execute_triangles(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &triangle_output_scores(&output),
            &[2.0, 1.0, 2.0, 1.0, 0.0, 0.0],
        );
        assert_eq!(
            output,
            execute_triangles(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn triangles_ignore_direction_multiplicity_and_self_loops() {
        let directed =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 0), (1, 2), (2, 0), (0, 0)]);
        let reciprocal =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2)]);
        for graph in [&directed, &reciprocal] {
            assert_scores_close(
                &triangle_output_scores(
                    &execute_triangles(
                        graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                ),
                &[1.0, 1.0, 1.0, 0.0],
            );
        }
        assert!(
            execute_triangles(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn triangles_use_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        assert!(matches!(
            execute_triangles(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_triangles(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Triangles))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "triangles");
    }

    #[test]
    fn triangles_path_selection_respects_crossover_pool_and_one_thread() {
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_triangles_path(&no_pool, TRIANGLES_PARALLEL_CROSSOVER_NODES),
            TrianglesExecutionPath::Serial
        );

        let below = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_triangles_path(&below, TRIANGLES_PARALLEL_CROSSOVER_NODES - 1),
            TrianglesExecutionPath::Serial
        );

        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_triangles_path(&one, TRIANGLES_PARALLEL_CROSSOVER_NODES),
            TrianglesExecutionPath::Serial
        );

        let parallel = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_triangles_path(&parallel, TRIANGLES_PARALLEL_CROSSOVER_NODES),
            TrianglesExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn triangles_thread_matrix_matches_one_thread_fingerprints_and_ordering() {
        let graph = triangle_thread_matrix_graph();
        assert!(graph.node_ids().len() >= TRIANGLES_PARALLEL_CROSSOVER_NODES);

        let one_thread =
            execute_triangles_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
        let expected_rows = one_thread.rows();
        let expected_bits = triangle_bits(&one_thread);

        for threads in [1_usize, 2, 4, 8] {
            let output =
                execute_triangles_with_pool(&graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(output.schema, one_thread.schema);
            assert_eq!(output.rows(), expected_rows);
            assert_eq!(triangle_bits(&output), expected_bits);
        }
    }

    #[test]
    fn triangles_parallel_cancellation_returns_structured_cancelled() {
        let graph = AdjacencyGraph::with_test_edges(TRIANGLES_PARALLEL_CROSSOVER_NODES as u64, &[]);
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert!(matches!(
            select_triangles_path(&control, graph.node_ids().len()),
            TrianglesExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_triangles_with_pool(&graph, 4, cancellation),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn k_core_peels_hand_verifiable_disconnected_layers() {
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
        let output = execute_k_core(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &k_core_output_scores(&output),
            &[3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 0.0, 2.0, 2.0, 2.0],
        );
        assert_eq!(
            output,
            execute_k_core(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn k_core_ignores_direction_multiplicity_and_self_loops() {
        let directed =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 0), (1, 2), (2, 0), (0, 0)]);
        let reciprocal =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2)]);
        for graph in [&directed, &reciprocal] {
            assert_scores_close(
                &k_core_output_scores(
                    &execute_k_core(
                        graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                ),
                &[2.0, 2.0, 2.0, 0.0],
            );
        }
        assert!(
            execute_k_core(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn k_core_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        assert!(matches!(
            execute_k_core(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_k_core(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::KCore))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "k_core");
    }

    #[test]
    fn k_core_batches_heap_entry_checkpoints() {
        let edges: Vec<(u64, u64)> = (1..=6_000).map(|leaf| (leaf, 0)).collect();
        let output = execute_k_core(
            &AdjacencyGraph::with_test_edges(6_001, &edges),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert!(
            k_core_output_scores(&output)
                .into_iter()
                .all(|score| score == 1.0)
        );
    }

    #[test]
    fn preferential_attachment_aggregates_missing_directed_links() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[(0, 1), (0, 1), (0, 2), (0, 0), (1, 2), (2, 0), (3, 2)],
        );
        let output = execute_preferential_attachment(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &preferential_attachment_output_scores(&output),
            &[2.0, 3.0, 2.0, 3.0, 0.0],
        );
        assert_eq!(
            output,
            execute_preferential_attachment(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn preferential_attachment_obeys_undirected_and_boundary_contracts() {
        let undirected = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 1),
                (1, 0),
                (0, 2),
                (2, 0),
                (1, 2),
                (2, 1),
                (2, 3),
                (3, 2),
            ],
        );
        assert_scores_close(
            &preferential_attachment_output_scores(
                &execute_preferential_attachment(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[2.0, 2.0, 0.0, 4.0, 0.0],
        );

        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
        assert_scores_close(
            &preferential_attachment_output_scores(
                &execute_preferential_attachment(
                    &disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[2.0, 2.0, 2.0, 2.0],
        );
        let complete =
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]);
        assert!(
            preferential_attachment_output_scores(
                &execute_preferential_attachment(
                    &complete,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            )
            .into_iter()
            .all(|score| score == 0.0)
        );
        assert!(
            preferential_attachment_output_scores(
                &execute_preferential_attachment(
                    &AdjacencyGraph::with_test_counts(3, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            )
            .into_iter()
            .all(|score| score == 0.0)
        );
        assert!(
            execute_preferential_attachment(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn preferential_attachment_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert!(matches!(
            execute_preferential_attachment(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_preferential_attachment(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            exact_u64_as_f64((1_u64 << 53) + 1, "preferential-attachment score"),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::PreferentialAttachment)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "preferential_attachment");
    }

    #[test]
    fn adamic_adar_aggregates_missing_directed_links_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let output = execute_adamic_adar(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        let inverse_log_two = 1.0 / 2.0_f64.ln();
        assert_scores_close(
            &adamic_adar_output_scores(&output),
            &[
                2.0 * inverse_log_two,
                2.0 * inverse_log_two,
                inverse_log_two,
                inverse_log_two,
                0.0,
            ],
        );
        assert_eq!(
            output,
            execute_adamic_adar(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn adamic_adar_obeys_undirected_and_boundary_contracts() {
        let undirected = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (2, 0),
                (0, 3),
                (3, 0),
                (1, 2),
                (2, 1),
                (1, 3),
                (3, 1),
                (2, 4),
                (4, 2),
                (3, 4),
                (4, 3),
            ],
        );
        let inverse_log_two = 1.0 / 2.0_f64.ln();
        let inverse_log_three = 1.0 / 3.0_f64.ln();
        assert_scores_close(
            &adamic_adar_output_scores(
                &execute_adamic_adar(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[
                4.0 * inverse_log_three,
                4.0 * inverse_log_three,
                3.0 * inverse_log_two,
                3.0 * inverse_log_two,
                4.0 * inverse_log_three,
            ],
        );
        for graph in [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]),
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
            AdjacencyGraph::with_test_counts(3, 0),
        ] {
            assert!(
                adamic_adar_output_scores(
                    &execute_adamic_adar(
                        &graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                )
                .into_iter()
                .all(|score| score == 0.0)
            );
        }
        assert!(
            execute_adamic_adar(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn adamic_adar_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 2), (1, 2)]);
        assert!(matches!(
            execute_adamic_adar(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_adamic_adar(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_adamic_adar(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            adamic_discount(1),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::AdamicAdar))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "adamic_adar");
    }

    #[test]
    fn adamic_adar_path_selection_respects_crossover_and_one_thread() {
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_adamic_adar_path(&no_pool, 64, ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK),
            AdamicAdarExecutionPath::Serial
        );
        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_adamic_adar_path(&one, 64, ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK),
            AdamicAdarExecutionPath::Serial
        );
        let small = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_adamic_adar_path(&small, 64, ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK - 1),
            AdamicAdarExecutionPath::Serial
        );
        assert_eq!(
            select_adamic_adar_path(&small, 64, ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK),
            AdamicAdarExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }
    #[test]
    fn adamic_adar_thread_matrix_matches_one_thread_bits_and_ordering() {
        let graph = dense_adamic_adar_graph(128);
        let neighbors = simple_neighbors(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
            false,
        )
        .unwrap();
        assert!(estimated_adamic_adar_work(&neighbors) >= ADAMIC_ADAR_PARALLEL_CROSSOVER_WORK);
        let serial = execute_adamic_adar_with_pool(
            &graph,
            1,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        let serial_rows = serial.rows();
        let serial_bits = adamic_adar_bits(&serial);
        for threads in [2_usize, 4, 8] {
            let parallel = execute_adamic_adar_with_pool(
                &graph,
                threads,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(adamic_adar_bits(&parallel), serial_bits);
        }
    }
    #[test]
    fn adamic_adar_parallel_preserves_boundary_bits() {
        let multigraph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        let complete =
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]);
        let empty = AdjacencyGraph::default();
        let single = AdjacencyGraph::with_test_edges(1, &[]);
        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
        for graph in [
            &multigraph,
            &directed,
            &undirected,
            &complete,
            &empty,
            &single,
            &disconnected,
        ] {
            let serial = execute_adamic_adar_with_pool(
                graph,
                1,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            for threads in [2_usize, 4, 8] {
                let parallel = execute_adamic_adar_with_pool(
                    graph,
                    threads,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap();
                assert_eq!(adamic_adar_bits(&parallel), adamic_adar_bits(&serial));
                assert_eq!(parallel.rows(), serial.rows());
            }
        }
    }
    #[test]
    fn adamic_adar_parallel_limits_and_cancellation_return_structured() {
        let graph = dense_adamic_adar_graph(128);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_adamic_adar_with_pool(&graph, 4, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            execute_adamic_adar_with_pool(
                &graph,
                4,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_adamic_adar_with_pool(
                &graph,
                4,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
    }
    #[test]
    fn adamic_adar_source_chunks_cover_canonical_ranges() {
        assert_eq!(source_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(source_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(source_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(source_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
        assert_eq!(source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
    }
    #[test]
    #[ignore = "manual crossover measurement; run with --ignored --nocapture"]
    fn measure_adamic_adar_parallel_crossover() {
        use std::time::Instant;

        for (nodes, fanout) in [
            (64_usize, 8_usize),
            (96, 12),
            (128, 16),
            (192, 16),
            (256, 16),
            (512, 32),
            (1024, 32),
        ] {
            let graph = adamic_adar_ring_graph(nodes, fanout);
            let neighbors = simple_neighbors(
                &graph,
                &AlgorithmControl::new(
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                ),
                false,
            )
            .unwrap();
            let work = estimated_adamic_adar_work(&neighbors);
            let measurement_limits = AlgorithmLimits {
                iterations: 1_000_000,
                ..AlgorithmLimits::default()
            };
            let serial_ctl = AlgorithmControl::new(
                measurement_limits.with_compute_threads(1),
                AlgorithmCancellation::default(),
            )
            .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
            let parallel_ctl = AlgorithmControl::new(
                measurement_limits.with_compute_threads(4),
                AlgorithmCancellation::default(),
            )
            .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
            let mut serial_ns = u128::MAX;
            let mut parallel_ns = u128::MAX;
            for _ in 0..5 {
                let t0 = Instant::now();
                let serial = adamic_adar_scores(&graph, &serial_ctl).unwrap();
                serial_ns = serial_ns.min(t0.elapsed().as_nanos());
                let serial_bits = serial.iter().copied().map(f64::to_bits).collect::<Vec<_>>();

                let t1 = Instant::now();
                let parallel = adamic_adar_scores(&graph, &parallel_ctl).unwrap();
                parallel_ns = parallel_ns.min(t1.elapsed().as_nanos());
                let parallel_bits = parallel
                    .iter()
                    .copied()
                    .map(f64::to_bits)
                    .collect::<Vec<_>>();
                assert_eq!(parallel_bits, serial_bits);
            }
            println!(
                "nodes={nodes} fanout={fanout} work={work} serial_ns={serial_ns} parallel_ns={parallel_ns} ratio={}",
                parallel_ns as f64 / serial_ns as f64
            );
        }
    }

    #[test]
    fn common_neighbors_aggregates_missing_directed_links_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let output = execute_common_neighbors(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            common_neighbor_output_scores(&output),
            [2.0, 2.0, 1.0, 1.0, 0.0]
        );
        assert_eq!(
            output,
            execute_common_neighbors(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn common_neighbors_obeys_undirected_and_boundary_contracts() {
        let undirected = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (2, 0),
                (0, 3),
                (3, 0),
                (1, 2),
                (2, 1),
                (1, 3),
                (3, 1),
                (2, 4),
                (4, 2),
                (3, 4),
                (4, 3),
            ],
        );
        assert_eq!(
            common_neighbor_output_scores(
                &execute_common_neighbors(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            [4.0, 4.0, 3.0, 3.0, 4.0]
        );
        for graph in [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]),
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
            AdjacencyGraph::with_test_counts(3, 0),
        ] {
            assert!(
                common_neighbor_output_scores(
                    &execute_common_neighbors(
                        &graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                )
                .into_iter()
                .all(|score| score == 0.0)
            );
        }
        assert!(
            execute_common_neighbors(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn common_neighbors_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 2), (1, 2)]);
        assert!(matches!(
            execute_common_neighbors(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_common_neighbors(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_common_neighbors(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            exact_u64_as_f64((1_u64 << 53) + 1, "common-neighbors score"),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::CommonNeighbors)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "common_neighbors");
    }

    #[test]
    fn common_neighbors_path_selection_respects_crossover_and_one_thread() {
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_common_neighbors_path(&no_pool, 64, COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK),
            CommonNeighborsExecutionPath::Serial
        );
        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_common_neighbors_path(&one, 64, COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK),
            CommonNeighborsExecutionPath::Serial
        );
        let small = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_common_neighbors_path(&small, 64, COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK - 1),
            CommonNeighborsExecutionPath::Serial
        );
        assert_eq!(
            select_common_neighbors_path(&small, 64, COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK),
            CommonNeighborsExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn common_neighbors_thread_matrix_matches_one_thread_bits_and_ordering() {
        let graph = dense_common_neighbors_graph(128);
        let neighbors = simple_neighbors(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
            false,
        )
        .unwrap();
        assert!(
            estimated_common_neighbors_work(&neighbors) >= COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK
        );
        let serial = execute_common_neighbors_with_pool(
            &graph,
            1,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        let serial_rows = serial.rows();
        let serial_bits = common_neighbor_bits(&serial);
        for threads in [2_usize, 4, 8] {
            let parallel = execute_common_neighbors_with_pool(
                &graph,
                threads,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(common_neighbor_bits(&parallel), serial_bits);
        }
    }

    #[test]
    fn common_neighbors_parallel_preserves_boundary_bits() {
        let multigraph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        let complete =
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]);
        let empty = AdjacencyGraph::default();
        let single = AdjacencyGraph::with_test_edges(1, &[]);
        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
        for graph in [
            &multigraph,
            &directed,
            &undirected,
            &complete,
            &empty,
            &single,
            &disconnected,
        ] {
            let serial = execute_common_neighbors_with_pool(
                graph,
                1,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            for threads in [2_usize, 4, 8] {
                let parallel = execute_common_neighbors_with_pool(
                    graph,
                    threads,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap();
                assert_eq!(
                    common_neighbor_bits(&parallel),
                    common_neighbor_bits(&serial)
                );
                assert_eq!(parallel.rows(), serial.rows());
            }
        }
    }

    #[test]
    fn common_neighbors_parallel_limits_and_cancellation_return_structured() {
        let graph = dense_common_neighbors_graph(128);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_common_neighbors_with_pool(&graph, 4, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            execute_common_neighbors_with_pool(
                &graph,
                4,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_common_neighbors_with_pool(
                &graph,
                4,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
    }

    #[test]
    fn common_neighbors_source_chunks_cover_canonical_ranges() {
        assert_eq!(source_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(source_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(source_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(source_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
        assert_eq!(source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    #[ignore = "manual crossover measurement; run with --ignored --nocapture"]
    fn measure_common_neighbors_parallel_crossover() {
        use std::time::Instant;

        for (nodes, fanout) in [
            (64_usize, 8_usize),
            (96, 12),
            (128, 16),
            (192, 16),
            (256, 16),
            (512, 32),
            (1024, 32),
        ] {
            let graph = common_neighbors_ring_graph(nodes, fanout);
            let neighbors = simple_neighbors(
                &graph,
                &AlgorithmControl::new(
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                ),
                false,
            )
            .unwrap();
            let work = estimated_common_neighbors_work(&neighbors);
            let measurement_limits = AlgorithmLimits {
                iterations: 1_000_000,
                ..AlgorithmLimits::default()
            };
            let serial_ctl = AlgorithmControl::new(
                measurement_limits.with_compute_threads(1),
                AlgorithmCancellation::default(),
            )
            .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
            let parallel_ctl = AlgorithmControl::new(
                measurement_limits.with_compute_threads(4),
                AlgorithmCancellation::default(),
            )
            .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
            let mut serial_ns = u128::MAX;
            let mut parallel_ns = u128::MAX;
            for _ in 0..5 {
                let t0 = Instant::now();
                let serial = common_neighbor_scores_serial(&neighbors, &serial_ctl).unwrap();
                serial_ns = serial_ns.min(t0.elapsed().as_nanos());
                let serial_bits = serial.iter().copied().map(f64::to_bits).collect::<Vec<_>>();

                let t1 = Instant::now();
                let parallel = common_neighbor_scores_parallel(&neighbors, &parallel_ctl).unwrap();
                parallel_ns = parallel_ns.min(t1.elapsed().as_nanos());
                let parallel_bits = parallel
                    .iter()
                    .copied()
                    .map(f64::to_bits)
                    .collect::<Vec<_>>();
                assert_eq!(parallel_bits, serial_bits);
            }
            println!(
                "nodes={nodes} fanout={fanout} work={work} serial_ns={serial_ns} parallel_ns={parallel_ns} ratio={}",
                parallel_ns as f64 / serial_ns as f64
            );
        }
    }

    #[test]
    fn resource_allocation_aggregates_missing_directed_links_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let output = execute_resource_allocation(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &resource_allocation_output_scores(&output),
            &[1.0, 1.0, 0.5, 0.5, 0.0],
        );
        assert_eq!(
            output,
            execute_resource_allocation(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn resource_allocation_obeys_undirected_and_boundary_contracts() {
        let undirected = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (2, 0),
                (0, 3),
                (3, 0),
                (1, 2),
                (2, 1),
                (1, 3),
                (3, 1),
                (2, 4),
                (4, 2),
                (3, 4),
                (4, 3),
            ],
        );
        assert_scores_close(
            &resource_allocation_output_scores(
                &execute_resource_allocation(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[4.0 / 3.0, 4.0 / 3.0, 1.5, 1.5, 4.0 / 3.0],
        );
        for graph in [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]),
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
            AdjacencyGraph::with_test_counts(3, 0),
        ] {
            assert!(
                resource_allocation_output_scores(
                    &execute_resource_allocation(
                        &graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                )
                .into_iter()
                .all(|score| score == 0.0)
            );
        }
        assert!(
            execute_resource_allocation(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn resource_allocation_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 2), (1, 2)]);
        assert!(matches!(
            execute_resource_allocation(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_resource_allocation(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_resource_allocation(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            resource_allocation_discount(1),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::ResourceAllocation)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "resource_allocation");
    }

    fn execute_resource_allocation_with_pool(
        graph: &AdjacencyGraph,
        threads: usize,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        let control = AlgorithmControl::new(limits.with_compute_threads(threads), cancellation)
            .with_compute_pool(pool);
        registry.execute(
            Algorithm::Rank(RankAlgorithm::ResourceAllocation),
            graph,
            &control,
        )
    }

    fn dense_resource_allocation_graph(nodes: usize) -> AdjacencyGraph {
        let max_fanout = nodes.saturating_sub(1).max(1) / 2;
        let fanout = ((RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK as usize)
            / (nodes.max(1) * nodes.max(1)))
        .clamp(4, max_fanout.max(1));
        resource_allocation_ring_graph(nodes, fanout)
    }

    fn resource_allocation_ring_graph(nodes: usize, fanout: usize) -> AdjacencyGraph {
        let fanout = fanout.clamp(1, (nodes.saturating_sub(1) / 2).max(1));
        let edges = (0..nodes)
            .flat_map(|node| {
                (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(nodes as u64, &edges)
    }

    #[test]
    #[ignore = "manual crossover measurement; run with --ignored --nocapture"]
    fn measure_resource_allocation_parallel_crossover() {
        use std::time::Instant;

        for (nodes, fanout) in [
            (64_usize, 8_usize),
            (96, 12),
            (128, 16),
            (192, 16),
            (256, 16),
            (512, 32),
            (1024, 32),
        ] {
            let graph = resource_allocation_ring_graph(nodes, fanout);
            let control =
                AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
            let neighbors = simple_neighbors(&graph, &control, false).unwrap();
            let work = estimated_pairwise_source_work(&neighbors);
            let measurement_limits = AlgorithmLimits {
                iterations: 1_000_000,
                ..AlgorithmLimits::default()
            };
            let serial_ctl = AlgorithmControl::new(
                measurement_limits.with_compute_threads(1),
                AlgorithmCancellation::default(),
            )
            .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
            let parallel_ctl = AlgorithmControl::new(
                measurement_limits.with_compute_threads(4),
                AlgorithmCancellation::default(),
            )
            .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
            let mut serial_ns = u128::MAX;
            let mut parallel_ns = u128::MAX;
            for _ in 0..5 {
                let t0 = Instant::now();
                let serial = resource_allocation_scores(&graph, &serial_ctl).unwrap();
                serial_ns = serial_ns.min(t0.elapsed().as_nanos());
                let serial_bits = serial.iter().copied().map(f64::to_bits).collect::<Vec<_>>();

                let t1 = Instant::now();
                let parallel = resource_allocation_scores(&graph, &parallel_ctl).unwrap();
                parallel_ns = parallel_ns.min(t1.elapsed().as_nanos());
                let parallel_bits = parallel
                    .iter()
                    .copied()
                    .map(f64::to_bits)
                    .collect::<Vec<_>>();
                assert_eq!(parallel_bits, serial_bits);
            }
            println!(
                "nodes={nodes} fanout={fanout} work={work} serial_ns={serial_ns} parallel_ns={parallel_ns} ratio={}",
                parallel_ns as f64 / serial_ns as f64
            );
        }
    }

    fn resource_allocation_bits(output: &AlgorithmOutput) -> Vec<u64> {
        resource_allocation_output_scores(output)
            .into_iter()
            .map(f64::to_bits)
            .collect()
    }

    #[test]
    fn resource_allocation_path_selection_respects_crossover_and_one_thread() {
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_resource_allocation_path(
                &no_pool,
                64,
                RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK
            ),
            ResourceAllocationExecutionPath::Serial
        );
        let one = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        assert_eq!(
            select_resource_allocation_path(&one, 64, RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK),
            ResourceAllocationExecutionPath::Serial
        );
        let small = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            select_resource_allocation_path(
                &small,
                64,
                RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK - 1
            ),
            ResourceAllocationExecutionPath::Serial
        );
        assert_eq!(
            select_resource_allocation_path(
                &small,
                64,
                RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK
            ),
            ResourceAllocationExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn resource_allocation_parallel_preserves_boundary_bits() {
        let multigraph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        let complete =
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]);
        let empty = AdjacencyGraph::default();
        let single = AdjacencyGraph::with_test_edges(1, &[]);
        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
        for graph in [
            &multigraph,
            &directed,
            &undirected,
            &complete,
            &empty,
            &single,
            &disconnected,
        ] {
            let serial = execute_resource_allocation_with_pool(
                graph,
                1,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            for threads in [2_usize, 4, 8] {
                let parallel = execute_resource_allocation_with_pool(
                    graph,
                    threads,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap();
                assert_eq!(
                    resource_allocation_bits(&parallel),
                    resource_allocation_bits(&serial)
                );
                assert_eq!(parallel.rows(), serial.rows());
            }
        }
    }

    #[test]
    fn resource_allocation_thread_matrix_matches_one_thread_bits_and_ordering() {
        let graph = dense_resource_allocation_graph(128);
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let neighbors = simple_neighbors(&graph, &control, false).unwrap();
        assert!(
            estimated_pairwise_source_work(&neighbors)
                >= RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK
        );
        let serial = execute_resource_allocation_with_pool(
            &graph,
            1,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        let serial_rows = serial.rows();
        let serial_bits = resource_allocation_bits(&serial);
        for threads in [2_usize, 4, 8] {
            let parallel = execute_resource_allocation_with_pool(
                &graph,
                threads,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(parallel.schema, serial.schema);
            assert_eq!(parallel.rows(), serial_rows);
            assert_eq!(resource_allocation_bits(&parallel), serial_bits);
        }
    }

    #[test]
    fn resource_allocation_parallel_limits_and_cancellation_return_structured() {
        let graph = dense_resource_allocation_graph(128);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_resource_allocation_with_pool(
                &graph,
                4,
                AlgorithmLimits::default(),
                cancellation
            ),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            execute_resource_allocation_with_pool(
                &graph,
                4,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_resource_allocation_with_pool(
                &graph,
                4,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
    }

    #[test]
    fn resource_allocation_worker_panic_returns_structured_error() {
        let pool = crate::ComputePool::new(2).unwrap();
        let error = run_resource_allocation_on_pool(&pool, || -> Result<(), AlgorithmError> {
            panic!("synthetic resource-allocation worker failure")
        })
        .unwrap_err();
        assert_eq!(
            error,
            AlgorithmError::Execution {
                message: "resource-allocation worker panicked".into()
            }
        );
    }

    #[test]
    fn total_neighbors_aggregates_missing_directed_links_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let output = execute_total_neighbors(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            total_neighbor_output_scores(&output),
            [4.0, 4.0, 6.0, 8.0, 7.0]
        );
        assert_eq!(
            output,
            execute_total_neighbors(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn total_neighbors_obeys_undirected_and_boundary_contracts() {
        let undirected = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (2, 0),
                (0, 3),
                (3, 0),
                (1, 2),
                (2, 1),
                (1, 3),
                (3, 1),
                (2, 4),
                (4, 2),
                (3, 4),
                (4, 3),
            ],
        );
        assert_eq!(
            total_neighbor_output_scores(
                &execute_total_neighbors(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [4.0, 4.0, 3.0, 3.0, 4.0]
        );
        assert_eq!(
            total_neighbor_output_scores(
                &execute_total_neighbors(
                    &AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [4.0, 4.0, 4.0, 4.0]
        );
        assert_eq!(
            total_neighbor_output_scores(
                &execute_total_neighbors(
                    &AdjacencyGraph::with_test_edges(3, &[(0, 1)]),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [1.0, 1.0, 1.0]
        );
        for graph in [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]),
            AdjacencyGraph::with_test_counts(3, 0),
        ] {
            assert!(
                total_neighbor_output_scores(
                    &execute_total_neighbors(
                        &graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                )
                .into_iter()
                .all(|score| score == 0.0)
            );
        }
        assert!(
            execute_total_neighbors(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn total_neighbors_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1)]);
        assert!(matches!(
            execute_total_neighbors(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_total_neighbors(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_total_neighbors(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            exact_u64_as_f64((1_u64 << 53) + 1, "total-neighbors score"),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::TotalNeighbors)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "total_neighbors");
    }
}
