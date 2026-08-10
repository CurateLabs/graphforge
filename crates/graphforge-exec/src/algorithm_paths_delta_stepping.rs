use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::panic::{AssertUnwindSafe, catch_unwind};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::{AdjacencyGraph, AlgorithmEdge};
use crate::algorithm_paths_dijkstra::DijkstraPath;

const DELTA: f64 = 1.0;
const CHECKPOINT_INTERVAL: usize = 4_096;
/// Direction-expanded edge scans below which delta-stepping proposal collection
/// stays serial (#539).
///
/// The parallel path partitions the current bucket's source set, not adjacency
/// rows globally, so single-source buckets remain serial. On this agent host the
/// private-pool path first won consistently once a relaxation wave scanned at
/// least about 8k edges; 8,192 keeps small public calls off the pool while large
/// bucket waves can use available compute threads.
pub(crate) const DELTA_STEPPING_PARALLEL_CROSSOVER_EDGE_SCANS: u64 = 8_192;

type BestPath = (f64, Vec<u64>, Vec<u64>);
type Buckets = BTreeMap<BucketIndex, BTreeSet<u64>>;
type Proposal = (u64, BestPath);

/// Selected execution path for observability and crossover tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeltaSteppingExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct BucketIndex(f64);

impl Eq for BucketIndex {}

impl Ord for BucketIndex {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialOrd for BucketIndex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Exact deterministic Delta-stepping with the canonical fixed bucket width.
pub(crate) fn exact_delta_stepping(
    graph: &AdjacencyGraph,
    source: u64,
    target: Option<u64>,
    control: &AlgorithmControl,
) -> Result<Vec<DijkstraPath>, AlgorithmError> {
    control.checkpoint()?;
    validate_endpoint(graph, source, "source")?;
    if let Some(target) = target {
        validate_endpoint(graph, target, "target")?;
    }

    let mut work = 0_usize;
    validate_weights(graph, control, &mut work)?;
    let mut best = HashMap::from([(source, (0.0, vec![source], Vec::new()))]);
    let mut buckets = Buckets::from([(BucketIndex(0.0), BTreeSet::from([source]))]);

    while let Some((index, mut requests)) = buckets.pop_first() {
        checkpoint(control, &mut work)?;
        requests.retain(|node| is_current_bucket(*node, index, &best));
        if requests.is_empty() {
            continue;
        }

        let mut settled = BTreeSet::new();
        while !requests.is_empty() {
            checkpoint(control, &mut work)?;
            settled.extend(requests.iter().copied());
            relax_edges(
                graph,
                &requests,
                true,
                &mut best,
                &mut buckets,
                control,
                &mut work,
            )?;
            requests = buckets.remove(&index).unwrap_or_default();
            requests.retain(|node| is_current_bucket(*node, index, &best));
        }
        relax_edges(
            graph,
            &settled,
            false,
            &mut best,
            &mut buckets,
            control,
            &mut work,
        )?;
    }

    let mut targets = match target {
        Some(node) if best.contains_key(&node) => vec![node],
        Some(_) => Vec::new(),
        None => best.keys().copied().collect(),
    };
    targets.sort_unstable();
    control.check_output_rows(targets.len())?;
    Ok(targets
        .into_iter()
        .map(|node| {
            let (cost, nodes, _) = &best[&node];
            DijkstraPath {
                source,
                target: node,
                cost: *cost,
                nodes: nodes.clone(),
            }
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn relax_edges(
    graph: &AdjacencyGraph,
    sources: &BTreeSet<u64>,
    light: bool,
    best: &mut HashMap<u64, BestPath>,
    buckets: &mut Buckets,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    let mut proposals = collect_relaxation_proposals(graph, sources, light, best, control, work)?;
    sort_proposals(&mut proposals);
    for (node, candidate) in proposals {
        if improves(&candidate, best.get(&node)) {
            let index = bucket_index(candidate.0);
            best.insert(node, candidate);
            buckets.entry(index).or_default().insert(node);
        }
    }
    Ok(())
}

fn collect_relaxation_proposals(
    graph: &AdjacencyGraph,
    sources: &BTreeSet<u64>,
    light: bool,
    best: &HashMap<u64, BestPath>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<Proposal>, AlgorithmError> {
    let edge_scans = relaxation_edge_scans(graph, sources);
    match select_delta_stepping_path(control, sources.len(), edge_scans) {
        DeltaSteppingExecutionPath::Serial => {
            collect_relaxation_proposals_serial(graph, sources, light, best, control, work)
        }
        DeltaSteppingExecutionPath::Parallel { .. } => {
            collect_relaxation_proposals_parallel(graph, sources, light, best, control)
        }
    }
}

fn collect_relaxation_proposals_serial(
    graph: &AdjacencyGraph,
    sources: &BTreeSet<u64>,
    light: bool,
    best: &HashMap<u64, BestPath>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<Proposal>, AlgorithmError> {
    let mut proposals = Vec::new();
    for &source in sources {
        let Some(current) = best.get(&source).cloned() else {
            continue;
        };
        for edge in graph.neighbors(source) {
            checkpoint(control, work)?;
            if (edge.weight <= DELTA) != light || current.1.contains(&edge.neighbor_id) {
                continue;
            }
            proposals.push((edge.neighbor_id, candidate(&current, edge)?));
        }
    }
    Ok(proposals)
}

fn collect_relaxation_proposals_parallel(
    graph: &AdjacencyGraph,
    sources: &BTreeSet<u64>,
    light: bool,
    best: &HashMap<u64, BestPath>,
    control: &AlgorithmControl,
) -> Result<Vec<Proposal>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel delta_stepping requires an instance-owned compute pool")
    })?;
    let source_ids = sources.iter().copied().collect::<Vec<_>>();
    let ranges = source_chunks(source_ids.len(), control.compute_threads());
    let chunk_results = run_delta_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut work = 0_usize;
                let mut proposals = Vec::new();
                for &source in &source_ids[start..end] {
                    let Some(current) = best.get(&source).cloned() else {
                        continue;
                    };
                    for edge in graph.neighbors(source) {
                        checkpoint(control, &mut work)?;
                        if (edge.weight <= DELTA) != light || current.1.contains(&edge.neighbor_id)
                        {
                            continue;
                        }
                        proposals.push((edge.neighbor_id, candidate(&current, edge)?));
                    }
                }
                Ok(proposals)
            })
            .collect::<Vec<Result<_, AlgorithmError>>>()
    })?;
    merge_chunk_proposals(chunk_results)
}

fn merge_chunk_proposals(
    chunk_results: Vec<Result<Vec<Proposal>, AlgorithmError>>,
) -> Result<Vec<Proposal>, AlgorithmError> {
    let mut proposals = Vec::new();
    for chunk in chunk_results {
        proposals.extend(chunk?);
    }
    Ok(proposals)
}

fn sort_proposals(proposals: &mut [Proposal]) {
    proposals.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| compare_paths(&left.1, &right.1))
    });
}

fn relaxation_edge_scans(graph: &AdjacencyGraph, sources: &BTreeSet<u64>) -> u64 {
    sources.iter().fold(0_u64, |acc, source| {
        acc.saturating_add(u64::try_from(graph.neighbors(*source).len()).unwrap_or(u64::MAX))
    })
}

/// Choose serial vs private-pool parallel proposal collection for one bucket wave.
pub(crate) fn select_delta_stepping_path(
    control: &AlgorithmControl,
    sources: usize,
    edge_scans: u64,
) -> DeltaSteppingExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || sources <= 1
        || edge_scans < DELTA_STEPPING_PARALLEL_CROSSOVER_EDGE_SCANS
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return DeltaSteppingExecutionPath::Serial;
    }
    let chunks = source_chunks(sources, threads).len();
    if chunks <= 1 {
        DeltaSteppingExecutionPath::Serial
    } else {
        DeltaSteppingExecutionPath::Parallel { threads, chunks }
    }
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

fn run_delta_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("delta_stepping worker panicked")),
    }
}

fn validate_endpoint(graph: &AdjacencyGraph, node: u64, role: &str) -> Result<(), AlgorithmError> {
    if graph.node_ids().contains(&node) {
        Ok(())
    } else {
        Err(execution(format!(
            "delta_stepping {role} is outside node selection"
        )))
    }
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
                    "delta_stepping requires finite non-negative edge weights",
                ));
            }
        }
    }
    Ok(())
}

fn candidate(current: &BestPath, edge: &AlgorithmEdge) -> Result<BestPath, AlgorithmError> {
    let cost = current.0 + edge.weight;
    if !cost.is_finite() {
        return Err(execution("delta_stepping accumulated cost is not finite"));
    }
    let mut path = current.1.clone();
    path.push(edge.neighbor_id);
    let mut edges = current.2.clone();
    edges.push(edge.edge_id);
    Ok((cost, path, edges))
}

fn compare_paths(left: &BestPath, right: &BestPath) -> Ordering {
    left.0
        .total_cmp(&right.0)
        .then_with(|| left.1.cmp(&right.1))
        .then_with(|| left.2.cmp(&right.2))
}

fn improves(candidate: &BestPath, known: Option<&BestPath>) -> bool {
    known.is_none_or(|known| compare_paths(candidate, known) == Ordering::Less)
}

fn bucket_index(cost: f64) -> BucketIndex {
    BucketIndex((cost / DELTA).floor())
}

fn is_current_bucket(node: u64, index: BucketIndex, best: &HashMap<u64, BestPath>) -> bool {
    best.get(&node)
        .is_some_and(|path| bucket_index(path.0) == index)
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
    use std::time::Instant;

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

    fn control_with_threads_and_limits(
        threads: usize,
        limits: AlgorithmLimits,
    ) -> AlgorithmControl {
        let pool = Arc::new(ComputePool::new(threads).unwrap());
        AlgorithmControl::new(
            limits.with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool)
    }

    fn parallel_wave_fixture(middles: u64, fanout: u64) -> AdjacencyGraph {
        let common = 1 + middles;
        let targets = (0..fanout)
            .map(|target| common + target)
            .collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut weights = Vec::new();
        for middle in 1..=middles {
            edges.push((0, middle));
            weights.push(0.5);
        }
        for middle in 1..=middles {
            edges.push((middle, common));
            weights.push(0.5);
            for &target in &targets {
                edges.push((middle, target));
                weights.push(0.5);
            }
        }
        AdjacencyGraph::with_test_directed_edges(common + fanout + 1, &edges)
            .with_test_edge_weights(&weights)
    }

    #[test]
    fn light_and_heavy_buckets_return_exact_target_and_reachable_paths() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            6,
            &[(0, 2), (0, 1), (1, 2), (1, 3), (2, 3), (3, 4)],
        )
        .with_test_edge_weights(&[5.0, 0.5, 0.5, 4.0, 1.0, 2.0]);
        let all = exact_delta_stepping(&graph, 0, None, &control()).unwrap();
        assert_eq!(
            all.iter()
                .map(|path| (path.target, path.cost, path.nodes.as_slice()))
                .collect::<Vec<_>>(),
            vec![
                (0, 0.0, &[0][..]),
                (1, 0.5, &[0, 1][..]),
                (2, 1.0, &[0, 1, 2][..]),
                (3, 2.0, &[0, 1, 2, 3][..]),
                (4, 4.0, &[0, 1, 2, 3, 4][..]),
            ]
        );
        assert_eq!(
            exact_delta_stepping(&graph, 0, Some(4), &control()).unwrap(),
            vec![all[4].clone()]
        );
    }

    #[test]
    fn ties_parallel_edges_self_loops_and_stale_buckets_are_stable() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            6,
            &[(0, 0), (0, 3), (0, 2), (0, 1), (0, 1), (1, 3), (2, 3)],
        )
        .with_test_edge_weights(&[0.0, 5.0, 1.0, 4.0, 1.0, 1.0, 1.0]);
        let expected = DijkstraPath {
            source: 0,
            target: 3,
            cost: 2.0,
            nodes: vec![0, 1, 3],
        };
        assert_eq!(
            exact_delta_stepping(&graph, 0, Some(3), &control()).unwrap(),
            vec![expected]
        );
        assert!(
            exact_delta_stepping(&graph, 0, Some(5), &control())
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            exact_delta_stepping(&graph, 0, Some(0), &control()).unwrap()[0].nodes,
            [0]
        );
    }

    #[test]
    fn full_edge_sequence_breaks_a_tie_before_the_final_edge() {
        let start = (0.0, vec![0], Vec::new());
        let later_parallel = AlgorithmEdge {
            edge_id: 1,
            edge_uuid: [1; 16],
            neighbor_id: 1,
            weight: 1.0,
        };
        let earlier_parallel = AlgorithmEdge {
            edge_id: 0,
            edge_uuid: [0; 16],
            ..later_parallel
        };
        let final_edge = AlgorithmEdge {
            edge_id: 9,
            edge_uuid: [9; 16],
            neighbor_id: 2,
            weight: 1.0,
        };
        let later = candidate(&candidate(&start, &later_parallel).unwrap(), &final_edge).unwrap();
        let earlier =
            candidate(&candidate(&start, &earlier_parallel).unwrap(), &final_edge).unwrap();

        assert_eq!(later.1, earlier.1);
        assert_eq!(later.2, [1, 9]);
        assert_eq!(earlier.2, [0, 9]);
        assert!(improves(&earlier, Some(&later)));
    }

    #[test]
    fn select_delta_stepping_path_respects_pool_and_crossover() {
        let serial = control();
        assert_eq!(
            select_delta_stepping_path(&serial, 64, DELTA_STEPPING_PARALLEL_CROSSOVER_EDGE_SCANS),
            DeltaSteppingExecutionPath::Serial
        );

        let one = control_with_threads(1);
        assert_eq!(
            select_delta_stepping_path(&one, 64, DELTA_STEPPING_PARALLEL_CROSSOVER_EDGE_SCANS),
            DeltaSteppingExecutionPath::Serial
        );

        let small = control_with_threads(4);
        assert_eq!(
            select_delta_stepping_path(
                &small,
                64,
                DELTA_STEPPING_PARALLEL_CROSSOVER_EDGE_SCANS - 1
            ),
            DeltaSteppingExecutionPath::Serial
        );
        assert_eq!(
            select_delta_stepping_path(&small, 1, DELTA_STEPPING_PARALLEL_CROSSOVER_EDGE_SCANS),
            DeltaSteppingExecutionPath::Serial
        );
        assert_eq!(
            select_delta_stepping_path(&small, 64, DELTA_STEPPING_PARALLEL_CROSSOVER_EDGE_SCANS),
            DeltaSteppingExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn parallel_proposals_match_one_thread_oracle_across_thread_counts() {
        let graph = parallel_wave_fixture(96, 96);
        let oracle = exact_delta_stepping(&graph, 0, None, &control_with_threads(1)).unwrap();

        for threads in [2, 4, 8] {
            let parallel =
                exact_delta_stepping(&graph, 0, None, &control_with_threads(threads)).unwrap();
            assert_eq!(parallel, oracle, "threads={threads}");
        }
    }

    #[test]
    fn parallel_limit_failure_is_structured_without_partial_results() {
        let graph = parallel_wave_fixture(160, 128);
        let limited = AlgorithmLimits {
            iterations: 8,
            ..AlgorithmLimits::default()
        };
        assert!(matches!(
            exact_delta_stepping(
                &graph,
                0,
                None,
                &control_with_threads_and_limits(4, limited)
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }

    #[test]
    fn weights_endpoints_limits_and_cancellation_are_structured() {
        let negative =
            AdjacencyGraph::with_test_edges(2, &[(0, 1)]).with_test_edge_weights(&[-1.0]);
        assert!(matches!(
            exact_delta_stepping(&negative, 0, None, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        let non_finite =
            AdjacencyGraph::with_test_edges(2, &[(0, 1)]).with_test_edge_weights(&[f64::INFINITY]);
        assert!(exact_delta_stepping(&non_finite, 0, None, &control()).is_err());
        let overflow = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)])
            .with_test_edge_weights(&[f64::MAX, f64::MAX]);
        assert!(matches!(
            exact_delta_stepping(&overflow, 0, None, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(exact_delta_stepping(&negative, 9, None, &control()).is_err());

        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_delta_stepping(&graph, 0, None, &limited),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let iteration_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_delta_stepping(&graph, 0, None, &iteration_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert!(matches!(
            exact_delta_stepping(&graph, 0, None, &cancelled),
            Err(AlgorithmError::Cancelled)
        ));
    }

    #[test]
    #[ignore = "manual crossover evidence; timing is hardware-specific"]
    fn measure_delta_stepping_parallel_crossover() {
        let graph = parallel_wave_fixture(192, 96);
        let oracle = exact_delta_stepping(&graph, 0, None, &control_with_threads(1)).unwrap();
        eprintln!(
            "delta_stepping fixture: nodes={} edge_entries={} rows={} crossover_edge_scans={}",
            graph.node_ids().len(),
            graph.edge_entry_count(),
            oracle.len(),
            DELTA_STEPPING_PARALLEL_CROSSOVER_EDGE_SCANS
        );
        for threads in [1, 2, 4, 8] {
            let control = control_with_threads(threads);
            let started = Instant::now();
            let paths = exact_delta_stepping(&graph, 0, None, &control).unwrap();
            let elapsed = started.elapsed();
            assert_eq!(paths, oracle);
            eprintln!("threads={threads} elapsed={elapsed:?}");
        }
    }
}
