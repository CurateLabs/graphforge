//! Exact global triangle counting for `analyze(by = "triangle_count")`.
//!
//! Parallelism (#588) partitions canonical source-ordinal ranges across the
//! instance-owned private compute pool above a measured crossover. Each worker
//! counts with the same ordered simple-neighbor sets as the serial path; global
//! reduction merges chunk counts in range order with checked `UInt64` addition.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

/// Candidate `(source, middle, target)` probes below which counting stays serial.
///
/// Chosen from release-mode serial-vs-parallel timings of exact global triangle
/// count on this M4 agent host (4x Xeon vCPU, dense/simple fixture, 4 private
/// workers; see ignored `measure_triangle_count_parallel_crossover`):
/// - ~40k probes: parallel is still slower (pool install + chunk/reduction tax)
/// - ~85k probes: near parity
/// - ~160k probes: first clear win
///
/// `131_072` is the smallest power-of-two boundary between parity and the
/// measured win. Exact `UInt64` results are identical on either path.
pub const TRIANGLE_COUNT_PARALLEL_CROSSOVER_PROBES: u64 = 131_072;

/// Selected execution path for observability and crossover tests (#588).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TriangleCountExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

#[derive(Debug, PartialEq, Eq)]
struct TriangleChunk {
    count: u64,
    checkpoints: usize,
}

/// One stored edge entry in the selected public-identity projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TriangleEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

impl TriangleEdge {
    fn canonical(mut self) -> Self {
        if self.target < self.source {
            std::mem::swap(&mut self.source, &mut self.target);
        }
        self
    }
}

/// Count unordered triangles in the selected undirected simple projection.
///
/// Edge UUID collapses mirrored storage entries. Distinct parallel and
/// reciprocal edges collapse through the simple-neighbor sets.
pub(crate) fn triangle_count(
    nodes: &[[u8; 16]],
    edges: &[TriangleEdge],
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    control.checkpoint()?;
    control.check_output_rows(1)?;

    let mut work = 0_usize;
    let node_index = index_nodes(nodes, control, &mut work)?;
    let neighbors = simple_neighbors(edges, &node_index, control, &mut work)?;
    let estimated_probes = candidate_probe_count(&neighbors, control)?;

    match select_triangle_count_path(control, neighbors.len(), estimated_probes) {
        TriangleCountExecutionPath::Serial => count_triangles_serial(&neighbors, control),
        TriangleCountExecutionPath::Parallel { .. } => {
            count_triangles_parallel(&neighbors, control)
        }
    }
}

fn count_triangles_serial(
    neighbors: &[BTreeSet<usize>],
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    let mut work = 0_usize;
    let chunk = count_source_range(neighbors, 0, neighbors.len(), control, &mut work, true)?;
    Ok(chunk.count)
}

fn count_triangles_parallel(
    neighbors: &[BTreeSet<usize>],
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel triangle_count requires an instance-owned compute pool")
    })?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let chunk_results = run_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut work = 0_usize;
                count_source_range(neighbors, start, end, control, &mut work, false)
            })
            .collect::<Vec<Result<_, AlgorithmError>>>();
        first_chunk_error(results)
    })?;

    reduce_chunks(chunk_results, control)
}

fn count_source_range(
    neighbors: &[BTreeSet<usize>],
    start: usize,
    end: usize,
    control: &AlgorithmControl,
    work: &mut usize,
    consume_checkpoints: bool,
) -> Result<TriangleChunk, AlgorithmError> {
    let mut count = 0_u64;
    let mut checkpoints = 0_usize;

    for source in start..end {
        chunk_checkpoint(control, work, &mut checkpoints, consume_checkpoints)?;
        for &middle in neighbors[source].range(source.saturating_add(1)..) {
            chunk_checkpoint(control, work, &mut checkpoints, consume_checkpoints)?;
            for &target in neighbors[middle].range(middle.saturating_add(1)..) {
                chunk_checkpoint(control, work, &mut checkpoints, consume_checkpoints)?;
                if neighbors[source].contains(&target) {
                    count = increment(count)?;
                }
            }
        }
    }
    Ok(TriangleChunk { count, checkpoints })
}

fn index_nodes(
    nodes: &[[u8; 16]],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<BTreeMap<[u8; 16], usize>, AlgorithmError> {
    let mut ordered = nodes.to_vec();
    ordered.sort_unstable();
    let mut index = BTreeMap::new();
    for (position, uuid) in ordered.into_iter().enumerate() {
        checkpoint(control, work)?;
        if index.insert(uuid, position).is_some() {
            return Err(execution("triangle_count node UUIDs must be unique"));
        }
    }
    Ok(index)
}

fn simple_neighbors(
    edges: &[TriangleEdge],
    node_index: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<BTreeSet<usize>>, AlgorithmError> {
    let mut neighbors = vec![BTreeSet::new(); node_index.len()];
    let mut stored_edges = BTreeMap::new();
    for &raw in edges {
        checkpoint(control, work)?;
        let edge = raw.canonical();
        let Some(&source) = node_index.get(&edge.source) else {
            return Err(execution(
                "triangle_count edge endpoint is outside node selection",
            ));
        };
        let Some(&target) = node_index.get(&edge.target) else {
            return Err(execution(
                "triangle_count edge endpoint is outside node selection",
            ));
        };
        if let Some(previous) = stored_edges.insert(edge.edge, edge) {
            if previous != edge {
                return Err(execution(
                    "triangle_count edge UUID has inconsistent adjacency entries",
                ));
            }
            continue;
        }
        if source != target {
            neighbors[source].insert(target);
            neighbors[target].insert(source);
        }
    }
    Ok(neighbors)
}

fn increment(value: u64) -> Result<u64, AlgorithmError> {
    value
        .checked_add(1)
        .ok_or_else(|| execution("triangle_count exceeds supported range"))
}

fn checked_add(left: u64, right: u64) -> Result<u64, AlgorithmError> {
    left.checked_add(right)
        .ok_or_else(|| execution("triangle_count exceeds supported range"))
}

fn reduce_chunks(
    chunks: Vec<TriangleChunk>,
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    let mut total = 0_u64;
    for chunk in chunks {
        for _ in 0..chunk.checkpoints {
            control.checkpoint()?;
        }
        total = checked_add(total, chunk.count)?;
    }
    Ok(total)
}

fn first_chunk_error(
    results: Vec<Result<TriangleChunk, AlgorithmError>>,
) -> Result<Vec<TriangleChunk>, AlgorithmError> {
    let mut chunks = Vec::with_capacity(results.len());
    for result in results {
        chunks.push(result?);
    }
    Ok(chunks)
}

fn run_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("triangle_count worker panicked")),
    }
}

fn candidate_probe_count(
    neighbors: &[BTreeSet<usize>],
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    let mut probes = 0_u64;
    let mut work = 0_usize;
    let mut checkpoints = 0_usize;
    for (middle, adjacent) in neighbors.iter().enumerate() {
        chunk_checkpoint(control, &mut work, &mut checkpoints, true)?;
        let mut lower = 0_u64;
        let mut higher = 0_u64;
        for &neighbor in adjacent {
            chunk_checkpoint(control, &mut work, &mut checkpoints, true)?;
            if neighbor < middle {
                lower = lower.saturating_add(1);
            } else if neighbor > middle {
                higher = higher.saturating_add(1);
            }
        }
        probes = probes.saturating_add(lower.saturating_mul(higher));
    }
    Ok(probes)
}

/// Choose serial vs private-pool parallel execution for triangle counting.
pub(crate) fn select_triangle_count_path(
    control: &AlgorithmControl,
    sources: usize,
    estimated_probes: u64,
) -> TriangleCountExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || sources <= 1
        || estimated_probes < TRIANGLE_COUNT_PARALLEL_CROSSOVER_PROBES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return TriangleCountExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return TriangleCountExecutionPath::Serial;
    }
    TriangleCountExecutionPath::Parallel { threads, chunks }
}

fn source_chunks(sources: usize, threads: usize) -> Vec<(usize, usize)> {
    if sources == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, sources);
    let base = sources / workers;
    let rem = sources % workers;
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

fn chunk_checkpoint(
    control: &AlgorithmControl,
    work: &mut usize,
    checkpoints: &mut usize,
    consume_checkpoint: bool,
) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        if consume_checkpoint {
            control.checkpoint()?;
        } else {
            *checkpoints = checkpoints.saturating_add(1);
            control.check_cancelled()?;
        }
    } else {
        control.check_cancelled()?;
    }
    Ok(())
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    } else {
        control.check_cancelled()?;
    }
    Ok(())
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};
    use std::sync::Arc;

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn uuid_u128(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    fn edge(id: u8, source: u8, target: u8) -> TriangleEdge {
        TriangleEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn control_with_threads(threads: usize) -> AlgorithmControl {
        let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
        AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool)
    }

    fn complete_graph(size: usize) -> (Vec<[u8; 16]>, Vec<TriangleEdge>) {
        let nodes = (0..size)
            .map(|node| uuid_u128(u128::try_from(node).unwrap()))
            .collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut edge_id = 1_u128;
        for source in 0..size {
            for target in source.saturating_add(1)..size {
                edges.push(TriangleEdge {
                    edge: uuid_u128(edge_id),
                    source: nodes[source],
                    target: nodes[target],
                });
                edge_id = edge_id.saturating_add(1);
            }
        }
        (nodes, edges)
    }

    #[test]
    fn counts_disconnected_overlapping_triangles_and_ignores_isolates() {
        let nodes = (0..9).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(10, 0, 1),
            edge(11, 1, 2),
            edge(12, 2, 0),
            edge(13, 1, 3),
            edge(14, 2, 3),
            edge(15, 5, 6),
            edge(16, 6, 7),
            edge(17, 7, 5),
        ];

        assert_eq!(triangle_count(&nodes, &edges, &control()).unwrap(), 3);
        assert_eq!(triangle_count(&[], &[], &control()).unwrap(), 0);
    }

    #[test]
    fn collapses_mirrors_parallel_reciprocal_edges_and_self_loops() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        let edges = [
            edge(10, 0, 1),
            edge(10, 1, 0),
            edge(11, 0, 1),
            edge(12, 1, 0),
            edge(13, 1, 2),
            edge(14, 2, 1),
            edge(15, 2, 0),
            edge(16, 0, 0),
        ];

        assert_eq!(triangle_count(&nodes, &edges, &control()).unwrap(), 1);
    }

    #[test]
    fn result_is_independent_of_uuid_and_input_order() {
        let nodes = [uuid(200), uuid(3), uuid(99), uuid(17)];
        let edges = [
            edge(90, 200, 3),
            edge(4, 99, 200),
            edge(200, 3, 99),
            edge(2, 17, 3),
        ];
        let mut reversed_nodes = nodes;
        reversed_nodes.reverse();
        let mut reversed_edges = edges;
        reversed_edges.reverse();

        assert_eq!(triangle_count(&nodes, &edges, &control()).unwrap(), 1);
        assert_eq!(
            triangle_count(&reversed_nodes, &reversed_edges, &control()).unwrap(),
            1
        );
    }

    #[test]
    fn rejects_invalid_identity_topology_atomically() {
        assert!(matches!(
            triangle_count(&[uuid(0), uuid(0)], &[], &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            triangle_count(&[uuid(0)], &[edge(1, 0, 2)], &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            triangle_count(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control()
            ),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn uses_exact_checked_u64_and_shared_controls() {
        assert_eq!(increment(u64::MAX - 1).unwrap(), u64::MAX);
        assert!(matches!(
            increment(u64::MAX),
            Err(AlgorithmError::Execution { .. })
        ));

        let no_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            triangle_count(&[], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            triangle_count(&[], &[], &cancelled),
            Err(AlgorithmError::Cancelled)
        );

        let iteration_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            triangle_count(&[], &[], &iteration_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }

    #[test]
    fn path_selection_respects_crossover_pool_and_one_thread() {
        let without_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_triangle_count_path(
                &without_pool,
                128,
                TRIANGLE_COUNT_PARALLEL_CROSSOVER_PROBES
            ),
            TriangleCountExecutionPath::Serial
        );
        assert_eq!(
            select_triangle_count_path(
                &control_with_threads(1),
                128,
                TRIANGLE_COUNT_PARALLEL_CROSSOVER_PROBES
            ),
            TriangleCountExecutionPath::Serial
        );
        assert_eq!(
            select_triangle_count_path(
                &control_with_threads(4),
                128,
                TRIANGLE_COUNT_PARALLEL_CROSSOVER_PROBES - 1
            ),
            TriangleCountExecutionPath::Serial
        );
        assert_eq!(
            select_triangle_count_path(
                &control_with_threads(4),
                128,
                TRIANGLE_COUNT_PARALLEL_CROSSOVER_PROBES
            ),
            TriangleCountExecutionPath::Parallel {
                threads: 4,
                chunks: 4,
            }
        );
    }

    #[test]
    fn thread_matrix_matches_one_thread_oracle_above_crossover() {
        let (nodes, edges) = complete_graph(96);
        let oracle = triangle_count(&nodes, &edges, &control_with_threads(1)).unwrap();
        assert_eq!(oracle, 96 * 95 * 94 / 6);

        let node_index = index_nodes(&nodes, &control(), &mut 0).unwrap();
        let neighbors = simple_neighbors(&edges, &node_index, &control(), &mut 0).unwrap();
        assert!(
            candidate_probe_count(&neighbors, &control()).unwrap()
                >= TRIANGLE_COUNT_PARALLEL_CROSSOVER_PROBES
        );

        for threads in [2_usize, 4, 8] {
            let control = control_with_threads(threads);
            assert!(matches!(
                select_triangle_count_path(
                    &control,
                    nodes.len(),
                    candidate_probe_count(&neighbors, &control()).unwrap()
                ),
                TriangleCountExecutionPath::Parallel { threads: selected, .. } if selected == threads
            ));
            assert_eq!(triangle_count(&nodes, &edges, &control).unwrap(), oracle);
        }
    }

    #[test]
    fn parallel_controls_return_structured_errors_without_results() {
        let (nodes, edges) = complete_graph(96);

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            cancellation,
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            triangle_count(&nodes, &edges, &cancelled),
            Err(AlgorithmError::Cancelled)
        );

        let no_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            }
            .with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert!(matches!(
            triangle_count(&nodes, &edges, &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let iteration_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 3,
                ..AlgorithmLimits::default()
            }
            .with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert!(matches!(
            triangle_count(&nodes, &edges, &iteration_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }

    #[test]
    fn worker_panic_is_structured_execution_error() {
        let pool = crate::ComputePool::new(2).unwrap();
        assert!(matches!(
            run_on_pool(&pool, || -> Result<(), AlgorithmError> {
                panic!("synthetic triangle_count worker failure")
            }),
            Err(AlgorithmError::Execution { message }) if message == "triangle_count worker panicked"
        ));
    }

    #[test]
    #[ignore = "manual crossover measurement; run with --ignored --nocapture"]
    fn measure_triangle_count_parallel_crossover() {
        use std::time::Instant;

        for size in [64_usize, 80, 96, 128] {
            let (nodes, edges) = complete_graph(size);
            let serial = control_with_threads(1);
            let parallel = control_with_threads(4);

            let started = Instant::now();
            let serial_count = triangle_count(&nodes, &edges, &serial).unwrap();
            let serial_elapsed = started.elapsed();

            let started = Instant::now();
            let parallel_count = triangle_count(&nodes, &edges, &parallel).unwrap();
            let parallel_elapsed = started.elapsed();

            assert_eq!(serial_count, parallel_count);
            println!(
                "triangle_count size={size} probes={} serial={serial_elapsed:?} parallel={parallel_elapsed:?} count={serial_count}",
                size * (size - 1) * (size - 2) / 6
            );
        }
    }
}
