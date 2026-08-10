//! Dijkstra single-source and source-target execution remains serial for #541.
//! Its heap, best-path map, and target early exit are one canonical state
//! machine. The all-pairs variant has a separate #542 disposition.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::panic::{AssertUnwindSafe, catch_unwind};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

const CHECKPOINT_INTERVAL: usize = 4_096;
/// Estimated source-edge inspections below which APSP Dijkstra stays serial (#542).
///
/// The estimate is `selected_nodes * CSR adjacency entries`. The threshold keeps
/// small fixtures away from worker scheduling overhead while letting independent
/// source searches use the instance-owned private compute pool.
pub const DIJKSTRA_APSP_PARALLEL_CROSSOVER_WORK: u64 = 8_192;

/// Selected all-pairs Dijkstra execution path for crossover and parity tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DijkstraAllPairsExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DijkstraPath {
    pub source: u64,
    pub target: u64,
    pub cost: f64,
    pub nodes: Vec<u64>,
}

#[derive(Clone, Debug)]
struct HeapEntry {
    cost: f64,
    path: Vec<u64>,
    edge_id: u64,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cost.to_bits() == other.cost.to_bits()
            && self.path == other.path
            && self.edge_id == other.edge_id
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .cost
            .total_cmp(&self.cost)
            .then_with(|| other.path.cmp(&self.path))
            .then_with(|| other.edge_id.cmp(&self.edge_id))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) fn exact_dijkstra(
    graph: &AdjacencyGraph,
    source: u64,
    target: Option<u64>,
    control: &AlgorithmControl,
) -> Result<Vec<DijkstraPath>, AlgorithmError> {
    control.checkpoint()?;
    let mut work = 0_usize;
    validate_weights(graph, control, &mut work)?;
    if !graph.node_ids().contains(&source) {
        return Err(execution("dijkstra source is outside node selection"));
    }
    if target.is_some_and(|node| !graph.node_ids().contains(&node)) {
        return Err(execution("dijkstra target is outside node selection"));
    }

    let paths = dijkstra_from(graph, source, target, control, &mut work)?;
    control.check_output_rows(paths.len())?;
    Ok(paths)
}

pub(crate) fn exact_dijkstra_all_pairs(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<DijkstraPath>, AlgorithmError> {
    control.checkpoint()?;
    let mut work = 0_usize;
    validate_weights(graph, control, &mut work)?;
    match select_dijkstra_all_pairs_path(control, graph) {
        DijkstraAllPairsExecutionPath::Serial => {
            exact_dijkstra_all_pairs_serial(graph, control, &mut work)
        }
        DijkstraAllPairsExecutionPath::Parallel { .. } => {
            exact_dijkstra_all_pairs_parallel(graph, control)
        }
    }
}

fn exact_dijkstra_all_pairs_serial(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<DijkstraPath>, AlgorithmError> {
    let mut paths = Vec::new();
    for &source in graph.node_ids() {
        checkpoint(control, work)?;
        let source_paths = dijkstra_from(graph, source, None, control, work)?
            .into_iter()
            .filter(|path| path.target != source)
            .collect::<Vec<_>>();
        append_paths(&mut paths, source_paths, control)?;
    }
    Ok(paths)
}

fn exact_dijkstra_all_pairs_parallel(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<DijkstraPath>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel dijkstra_all_pairs requires an instance-owned compute pool")
    })?;
    let sources = graph.node_ids();
    let ranges = source_chunks(sources.len(), control.compute_threads());
    let chunk_results = run_on_pool(pool, || {
        let results = ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut work = 0_usize;
                let mut chunk_paths = Vec::new();
                for &source in &sources[start..end] {
                    checkpoint(control, &mut work)?;
                    let source_paths = dijkstra_from(graph, source, None, control, &mut work)?
                        .into_iter()
                        .filter(|path| path.target != source)
                        .collect::<Vec<_>>();
                    chunk_paths.extend(source_paths);
                }
                Ok(chunk_paths)
            })
            .collect::<Vec<Result<_, AlgorithmError>>>();
        first_chunk_result(results)
    })?;
    merge_chunks(chunk_results, control)
}

fn dijkstra_from(
    graph: &AdjacencyGraph,
    source: u64,
    target: Option<u64>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<DijkstraPath>, AlgorithmError> {
    let source_path = vec![source];
    let mut best = HashMap::from([(source, (0.0, source_path.clone(), 0_u64))]);
    let mut heap = BinaryHeap::from([HeapEntry {
        cost: 0.0,
        path: source_path,
        edge_id: 0,
    }]);

    while let Some(entry) = heap.pop() {
        let node = *entry.path.last().expect("heap paths are non-empty");
        let Some((known_cost, known_path, known_edge)) = best.get(&node) else {
            continue;
        };
        if entry.cost.total_cmp(known_cost) != Ordering::Equal
            || entry.path != *known_path
            || entry.edge_id != *known_edge
        {
            continue;
        }
        if target == Some(node) {
            break;
        }

        for edge in graph.neighbors(node) {
            checkpoint(control, work)?;
            if entry.path.contains(&edge.neighbor_id) {
                continue;
            }
            let cost = entry.cost + edge.weight;
            if !cost.is_finite() {
                return Err(execution("dijkstra accumulated cost is not finite"));
            }
            let mut path = entry.path.clone();
            path.push(edge.neighbor_id);
            let candidate = (cost, path, edge.edge_id);
            let improves = best.get(&edge.neighbor_id).is_none_or(|known| {
                candidate.0.total_cmp(&known.0) == Ordering::Less
                    || (candidate.0.total_cmp(&known.0) == Ordering::Equal
                        && (candidate.1.as_slice(), candidate.2) < (known.1.as_slice(), known.2))
            });
            if improves {
                best.insert(edge.neighbor_id, candidate.clone());
                heap.push(HeapEntry {
                    cost: candidate.0,
                    path: candidate.1,
                    edge_id: candidate.2,
                });
            }
        }
    }

    let mut targets = match target {
        Some(node) if best.contains_key(&node) => vec![node],
        Some(_) => Vec::new(),
        None => best.keys().copied().collect(),
    };
    targets.sort_unstable();
    Ok(targets
        .into_iter()
        .map(|node| {
            let (cost, path, _) = &best[&node];
            DijkstraPath {
                source,
                target: node,
                cost: *cost,
                nodes: path.clone(),
            }
        })
        .collect())
}

fn append_paths(
    paths: &mut Vec<DijkstraPath>,
    source_paths: Vec<DijkstraPath>,
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    control.check_cancelled()?;
    control.check_output_rows(paths.len().saturating_add(source_paths.len()))?;
    paths.extend(source_paths);
    Ok(())
}

fn merge_chunks(
    chunk_results: Vec<Vec<DijkstraPath>>,
    control: &AlgorithmControl,
) -> Result<Vec<DijkstraPath>, AlgorithmError> {
    let mut paths = Vec::new();
    for chunk in chunk_results {
        append_paths(&mut paths, chunk, control)?;
    }
    Ok(paths)
}

fn first_chunk_result(
    results: Vec<Result<Vec<DijkstraPath>, AlgorithmError>>,
) -> Result<Vec<Vec<DijkstraPath>>, AlgorithmError> {
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
        Err(_) => Err(execution("dijkstra_all_pairs worker panicked")),
    }
}

/// Choose serial vs private-pool parallel source execution for APSP Dijkstra.
pub(crate) fn select_dijkstra_all_pairs_path(
    control: &AlgorithmControl,
    graph: &AdjacencyGraph,
) -> DijkstraAllPairsExecutionPath {
    let sources = graph.node_ids().len();
    let threads = control.compute_threads();
    let estimated_work = (sources as u64).saturating_mul(graph.edge_entry_count());
    if threads <= 1 || sources <= 1 || estimated_work < DIJKSTRA_APSP_PARALLEL_CROSSOVER_WORK {
        return DijkstraAllPairsExecutionPath::Serial;
    }
    if control
        .compute_pool()
        .is_none_or(|pool| !pool.is_parallel())
    {
        return DijkstraAllPairsExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        return DijkstraAllPairsExecutionPath::Serial;
    }
    DijkstraAllPairsExecutionPath::Parallel { threads, chunks }
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

fn validate_weights(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    for &node in graph.node_ids() {
        for edge in graph.neighbors(node) {
            checkpoint(control, work)?;
            if !edge.weight.is_finite() || edge.weight < 0.0 {
                return Err(execution(
                    "dijkstra requires finite non-negative edge weights",
                ));
            }
        }
    }
    Ok(())
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work += 1;
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
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
    use crate::compute_pool::ComputePool;
    use std::sync::Arc;

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn control_with_threads(threads: usize) -> AlgorithmControl {
        let pool = Arc::new(ComputePool::new(threads).unwrap());
        AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool)
    }

    fn parallel_fixture(node_count: u64, weight: f64) -> AdjacencyGraph {
        let offsets = [1_u64, 5, 17, 31];
        let edges = (0..node_count)
            .flat_map(|source| {
                offsets
                    .into_iter()
                    .map(move |offset| (source, (source + offset) % node_count))
            })
            .collect::<Vec<_>>();
        let weights = edges
            .iter()
            .map(|(source, target)| {
                if weight == 1.0 {
                    1.0 + ((source + target) % 7) as f64 / 10.0
                } else {
                    weight
                }
            })
            .collect::<Vec<_>>();
        AdjacencyGraph::with_test_edges(node_count, &edges).with_test_edge_weights(&weights)
    }

    fn path_fingerprint(paths: &[DijkstraPath]) -> Vec<(u64, u64, u64, Vec<u64>)> {
        paths
            .iter()
            .map(|path| {
                (
                    path.source,
                    path.target,
                    path.cost.to_bits(),
                    path.nodes.clone(),
                )
            })
            .collect()
    }

    #[test]
    fn weighted_target_and_all_reachable_paths_are_exact_and_stable() {
        let graph =
            AdjacencyGraph::with_test_edges(6, &[(0, 2), (0, 1), (1, 3), (2, 3), (0, 3), (3, 4)])
                .with_test_edge_weights(&[1.0, 1.0, 2.0, 2.0, 9.0, 0.5]);
        let all = exact_dijkstra(&graph, 0, None, &control()).unwrap();
        assert_eq!(
            all,
            vec![
                DijkstraPath {
                    source: 0,
                    target: 0,
                    cost: 0.0,
                    nodes: vec![0]
                },
                DijkstraPath {
                    source: 0,
                    target: 1,
                    cost: 1.0,
                    nodes: vec![0, 1]
                },
                DijkstraPath {
                    source: 0,
                    target: 2,
                    cost: 1.0,
                    nodes: vec![0, 2]
                },
                DijkstraPath {
                    source: 0,
                    target: 3,
                    cost: 3.0,
                    nodes: vec![0, 1, 3]
                },
                DijkstraPath {
                    source: 0,
                    target: 4,
                    cost: 3.5,
                    nodes: vec![0, 1, 3, 4]
                },
            ]
        );
        assert_eq!(
            exact_dijkstra(&graph, 0, Some(4), &control()).unwrap(),
            vec![DijkstraPath {
                source: 0,
                target: 4,
                cost: 3.5,
                nodes: vec![0, 1, 3, 4]
            }]
        );
    }

    #[test]
    fn unweighted_default_cost_returns_the_stable_shortest_path() {
        let graph = AdjacencyGraph::with_test_edges(4, &[(0, 2), (2, 3), (0, 1), (1, 3)]);
        assert_eq!(
            exact_dijkstra(&graph, 0, Some(3), &control()).unwrap(),
            vec![DijkstraPath {
                source: 0,
                target: 3,
                cost: 2.0,
                nodes: vec![0, 1, 3],
            }]
        );
    }

    #[test]
    fn parallel_zero_self_and_boundary_cases_are_deterministic() {
        let graph =
            AdjacencyGraph::with_test_edges(5, &[(0, 0), (0, 1), (0, 1), (1, 3), (0, 2), (2, 3)])
                .with_test_edge_weights(&[0.0, 4.0, 1.0, 1.0, 1.0, 1.0]);
        assert_eq!(
            exact_dijkstra(&graph, 0, Some(3), &control()).unwrap(),
            vec![DijkstraPath {
                source: 0,
                target: 3,
                cost: 2.0,
                nodes: vec![0, 1, 3]
            }]
        );
        assert!(
            exact_dijkstra(&graph, 0, Some(4), &control())
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            exact_dijkstra(&graph, 0, Some(0), &control()).unwrap()[0].nodes,
            [0]
        );
        assert!(exact_dijkstra(&AdjacencyGraph::default(), 0, None, &control()).is_err());
    }

    #[test]
    fn all_pairs_selects_parallel_only_above_private_pool_crossover() {
        let small = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2), (2, 3)]);
        assert_eq!(
            select_dijkstra_all_pairs_path(&control_with_threads(4), &small),
            DijkstraAllPairsExecutionPath::Serial
        );
        let large = parallel_fixture(48, 1.0);
        assert_eq!(
            select_dijkstra_all_pairs_path(&control(), &large),
            DijkstraAllPairsExecutionPath::Serial
        );
        assert_eq!(
            select_dijkstra_all_pairs_path(&control_with_threads(1), &large),
            DijkstraAllPairsExecutionPath::Serial
        );
        assert_eq!(
            select_dijkstra_all_pairs_path(&control_with_threads(4), &large),
            DijkstraAllPairsExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn all_pairs_returns_exact_reachable_ordered_non_self_paths() {
        let graph =
            AdjacencyGraph::with_test_edges(5, &[(0, 2), (0, 1), (1, 3), (2, 3), (3, 0), (4, 4)])
                .with_test_edge_weights(&[1.0, 1.0, 2.0, 2.0, 4.0, 0.0]);
        let paths = exact_dijkstra_all_pairs(&graph, &control()).unwrap();

        assert_eq!(
            paths
                .iter()
                .map(|path| (path.source, path.target, path.cost, path.nodes.as_slice()))
                .collect::<Vec<_>>(),
            vec![
                (0, 1, 1.0, &[0, 1][..]),
                (0, 2, 1.0, &[0, 2][..]),
                (0, 3, 3.0, &[0, 1, 3][..]),
                (1, 0, 6.0, &[1, 3, 0][..]),
                (1, 2, 7.0, &[1, 3, 0, 2][..]),
                (1, 3, 2.0, &[1, 3][..]),
                (2, 0, 6.0, &[2, 3, 0][..]),
                (2, 1, 7.0, &[2, 3, 0, 1][..]),
                (2, 3, 2.0, &[2, 3][..]),
                (3, 0, 4.0, &[3, 0][..]),
                (3, 1, 5.0, &[3, 0, 1][..]),
                (3, 2, 5.0, &[3, 0, 2][..]),
            ]
        );
        assert_eq!(paths, exact_dijkstra_all_pairs(&graph, &control()).unwrap());
        assert!(paths.iter().all(|path| path.source != path.target));
    }

    #[test]
    fn all_pairs_parallel_source_chunks_match_serial_fingerprint() {
        let graph = parallel_fixture(48, 1.0);
        let serial = exact_dijkstra_all_pairs(&graph, &control_with_threads(1)).unwrap();
        assert_eq!(
            serial.len(),
            48 * 47,
            "fixture should be strongly connected and omit self-pairs"
        );
        let serial_fingerprint = path_fingerprint(&serial);
        for threads in [2, 4, 8] {
            let parallel = exact_dijkstra_all_pairs(&graph, &control_with_threads(threads))
                .unwrap_or_else(|error| panic!("threads={threads}: {error:?}"));
            assert_eq!(
                serial_fingerprint,
                path_fingerprint(&parallel),
                "threads={threads}: canonical source/target/cost/path fingerprint"
            );
        }
    }

    #[test]
    fn all_pairs_empty_invalid_and_global_limits_are_structured() {
        assert!(
            exact_dijkstra_all_pairs(&AdjacencyGraph::default(), &control())
                .unwrap()
                .is_empty()
        );
        let invalid = AdjacencyGraph::with_test_edges(2, &[(0, 1)]).with_test_edge_weights(&[-1.0]);
        assert!(matches!(
            exact_dijkstra_all_pairs(&invalid, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 5,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_dijkstra_all_pairs(&graph, &limited),
            Err(AlgorithmError::OutputLimit {
                observed: 6,
                limit: 5
            })
        ));
        let dense_edges = (0..65)
            .flat_map(|source| {
                (0..65)
                    .filter(move |&target| source != target)
                    .map(move |target| (source, target))
            })
            .collect::<Vec<_>>();
        let dense = AdjacencyGraph::with_test_edges(65, &dense_edges);
        let iteration_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_dijkstra_all_pairs(&dense, &iteration_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let repeated_edges = vec![(0, 1); 3_000];
        let validation_plus_traversal = AdjacencyGraph::with_test_edges(2, &repeated_edges);
        let phase_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_dijkstra_all_pairs(&validation_plus_traversal, &phase_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let isolated = AdjacencyGraph::with_test_edges(4_096, &[]);
        let source_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_dijkstra_all_pairs(&isolated, &source_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            exact_dijkstra_all_pairs(
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn all_pairs_parallel_limits_and_worker_errors_are_structured() {
        let graph = parallel_fixture(48, 1.0);
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 10,
                ..AlgorithmLimits::default().with_compute_threads(4)
            },
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(ComputePool::new(4).unwrap()));
        assert!(matches!(
            exact_dijkstra_all_pairs(&graph, &limited),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let overflow = parallel_fixture(48, f64::MAX);
        assert!(matches!(
            exact_dijkstra_all_pairs(&overflow, &control_with_threads(4)),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn invalid_weights_limits_and_cancellation_are_structured() {
        for weight in [-1.0, f64::NAN, f64::INFINITY] {
            let graph =
                AdjacencyGraph::with_test_edges(2, &[(0, 1)]).with_test_edge_weights(&[weight]);
            assert!(matches!(
                exact_dijkstra(&graph, 0, None, &control()),
                Err(AlgorithmError::Execution { .. })
            ));
        }
        let overflow = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)])
            .with_test_edge_weights(&[f64::MAX, f64::MAX]);
        assert!(matches!(
            exact_dijkstra(&overflow, 0, None, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_dijkstra(&graph, 0, None, &limited),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let zero_iterations = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_dijkstra(&graph, 0, None, &zero_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            exact_dijkstra(
                &graph,
                0,
                None,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
