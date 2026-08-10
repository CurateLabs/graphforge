//! Deterministic positive-length transitive closure over shared adjacency.

use std::collections::{HashSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

const TRANSITIVE_CLOSURE_PARALLEL_CROSSOVER_WORK: u64 = 65_536;
const TRANSITIVE_CLOSURE_CHECKPOINT_EDGES: usize = 4_096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransitiveClosureExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// One reachable ordered node pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ClosurePair {
    /// Execution-internal source surrogate.
    pub(crate) source: u64,
    /// Execution-internal reachable target surrogate.
    pub(crate) target: u64,
}

/// Compute every distinct positive-length reachable pair.
///
/// Sources and targets are ordered lexicographically by public UUID. The
/// caller owns direction and relationship filtering when it exports the shared
/// adjacency graph.
///
/// This deterministic per-source traversal is `O(V(V + E))` time and uses
/// `O(V)` traversal state per source plus result storage.
pub(crate) fn positive_transitive_closure(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<ClosurePair>, AlgorithmError> {
    control.check_cancelled()?;

    let mut sources = graph
        .node_ids()
        .iter()
        .map(|&node| {
            graph
                .node_uuid(node)
                .map(|uuid| (uuid, node))
                .ok_or_else(|| execution("transitive_closure source has no UUID identity"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    sources.sort_unstable();

    match select_transitive_closure_path(control, sources.len(), graph.edge_entry_count()) {
        TransitiveClosureExecutionPath::Serial => {
            positive_transitive_closure_serial(graph, &sources, control)
        }
        TransitiveClosureExecutionPath::Parallel { .. } => {
            positive_transitive_closure_parallel(graph, &sources, control)
        }
    }
}

fn positive_transitive_closure_serial(
    graph: &AdjacencyGraph,
    sources: &[([u8; 16], u64)],
    control: &AlgorithmControl,
) -> Result<Vec<ClosurePair>, AlgorithmError> {
    let mut rows = Vec::new();
    let mut traversed_entries = 0_usize;
    for &(_, source) in sources {
        control.check_cancelled()?;
        let targets = transitive_closure_targets(
            graph,
            source,
            control,
            |control, traversed_entries| {
                if traversed_entries.is_multiple_of(TRANSITIVE_CLOSURE_CHECKPOINT_EDGES) {
                    control.checkpoint()?;
                }
                Ok(())
            },
            &mut traversed_entries,
        )?;
        let next_len = rows
            .len()
            .checked_add(targets.len())
            .ok_or_else(|| execution("transitive_closure output size exceeds platform range"))?;
        control.check_output_rows(next_len)?;
        rows.extend(
            targets
                .into_iter()
                .map(|(_, target)| target)
                .map(|target| ClosurePair { source, target }),
        );
    }
    Ok(rows)
}

fn positive_transitive_closure_parallel(
    graph: &AdjacencyGraph,
    sources: &[([u8; 16], u64)],
    control: &AlgorithmControl,
) -> Result<Vec<ClosurePair>, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel transitive_closure requires an instance-owned compute pool")
    })?;
    let ranges = transitive_closure_source_chunks(sources.len(), control.compute_threads());
    let traversed_entries = AtomicUsize::new(0);
    let chunk_rows = run_transitive_closure_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut rows = Vec::new();
                let mut local_edges = 0_usize;
                for &(_, source) in &sources[start..end] {
                    let targets = transitive_closure_targets(
                        graph,
                        source,
                        control,
                        |control, local_edges| {
                            let observed = traversed_entries.fetch_add(1, Ordering::Relaxed);
                            if observed.is_multiple_of(TRANSITIVE_CLOSURE_CHECKPOINT_EDGES)
                                || local_edges.is_multiple_of(TRANSITIVE_CLOSURE_CHECKPOINT_EDGES)
                            {
                                control.check_cancelled()?;
                            }
                            Ok(())
                        },
                        &mut local_edges,
                    )?;
                    rows.extend(
                        targets
                            .into_iter()
                            .map(|(_, target)| ClosurePair { source, target }),
                    );
                }
                Ok(rows)
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;

    let total_rows = chunk_rows.iter().try_fold(0_usize, |total, rows| {
        total
            .checked_add(rows.len())
            .ok_or_else(|| execution("transitive_closure output size exceeds platform range"))
    })?;
    control.check_output_rows(total_rows)?;

    let mut rows = Vec::with_capacity(total_rows);
    for chunk in chunk_rows {
        rows.extend(chunk);
    }
    Ok(rows)
}

fn transitive_closure_targets<CHECKPOINT>(
    graph: &AdjacencyGraph,
    source: u64,
    control: &AlgorithmControl,
    mut checkpoint: CHECKPOINT,
    traversed_entries: &mut usize,
) -> Result<Vec<([u8; 16], u64)>, AlgorithmError>
where
    CHECKPOINT: FnMut(&AlgorithmControl, usize) -> Result<(), AlgorithmError>,
{
    control.check_cancelled()?;
    let mut reachable = HashSet::new();
    let mut queue = VecDeque::from([source]);

    while let Some(node) = queue.pop_front() {
        for edge in graph.neighbors(node) {
            checkpoint(control, *traversed_entries)?;
            *traversed_entries = traversed_entries.saturating_add(1);
            if reachable.insert(edge.neighbor_id) {
                queue.push_back(edge.neighbor_id);
            }
        }
    }

    let mut targets = reachable
        .into_iter()
        .map(|node| {
            graph
                .node_uuid(node)
                .map(|uuid| (uuid, node))
                .ok_or_else(|| execution("transitive_closure target has no UUID identity"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    targets.sort_unstable();
    Ok(targets)
}

fn select_transitive_closure_path(
    control: &AlgorithmControl,
    sources: usize,
    edge_entries: u64,
) -> TransitiveClosureExecutionPath {
    let threads = control.compute_threads();
    let estimated_work = (sources as u64).saturating_mul(edge_entries);
    if threads <= 1
        || sources <= 1
        || estimated_work < TRANSITIVE_CLOSURE_PARALLEL_CROSSOVER_WORK
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return TransitiveClosureExecutionPath::Serial;
    }
    let chunks = transitive_closure_source_chunks(sources, threads).len();
    if chunks <= 1 {
        TransitiveClosureExecutionPath::Serial
    } else {
        TransitiveClosureExecutionPath::Parallel { threads, chunks }
    }
}

fn transitive_closure_source_chunks(sources: usize, threads: usize) -> Vec<(usize, usize)> {
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

fn run_transitive_closure_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("transitive_closure worker panicked")),
    }
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

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn control_with_threads(threads: usize) -> AlgorithmControl {
        AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(threads).unwrap()))
    }

    fn pairs(rows: &[ClosurePair]) -> Vec<(u64, u64)> {
        rows.iter().map(|row| (row.source, row.target)).collect()
    }

    fn cyclic_graph(nodes: u64, fanout: u64) -> AdjacencyGraph {
        let mut edges = Vec::new();
        for source in 0..nodes {
            for hop in 1..=fanout {
                edges.push((source, (source + hop) % nodes));
            }
        }
        AdjacencyGraph::with_test_directed_edges(nodes, &edges)
    }

    #[test]
    fn directed_chain_cycle_self_loop_and_parallel_edges_are_positive_length() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(5, &[(0, 1), (0, 1), (1, 2), (2, 0), (3, 3)]);
        let rows = positive_transitive_closure(&graph, &control()).unwrap();
        assert_eq!(
            pairs(&rows),
            vec![
                (0, 0),
                (0, 1),
                (0, 2),
                (1, 0),
                (1, 1),
                (1, 2),
                (2, 0),
                (2, 1),
                (2, 2),
                (3, 3),
            ]
        );
    }

    #[test]
    fn symmetric_projection_reaches_both_directions_without_duplicates() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 1), (1, 2), (2, 1)]);
        let rows = positive_transitive_closure(&graph, &control()).unwrap();
        assert_eq!(
            pairs(&rows),
            vec![
                (0, 0),
                (0, 1),
                (0, 2),
                (1, 0),
                (1, 1),
                (1, 2),
                (2, 0),
                (2, 1),
                (2, 2),
            ]
        );
    }

    #[test]
    fn empty_disconnected_and_isolated_nodes_emit_only_reachable_pairs() {
        assert!(
            positive_transitive_closure(&AdjacencyGraph::default(), &control())
                .unwrap()
                .is_empty()
        );

        let graph = AdjacencyGraph::with_test_directed_edges(5, &[(0, 1), (2, 3)]);
        assert_eq!(
            pairs(&positive_transitive_closure(&graph, &control()).unwrap()),
            vec![(0, 1), (2, 3)]
        );
    }

    #[test]
    fn output_is_sorted_by_public_source_then_target_uuid() {
        let graph = AdjacencyGraph::with_test_directed_edges(4, &[(2, 3), (0, 3), (0, 1), (1, 2)]);
        let first = positive_transitive_closure(&graph, &control()).unwrap();
        let second = positive_transitive_closure(&graph, &control()).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            pairs(&first),
            vec![(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        );
    }

    #[test]
    fn output_limit_and_cancellation_abort_without_rows() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            positive_transitive_closure(&graph, &limited),
            Err(AlgorithmError::OutputLimit {
                observed: 2..,
                limit: 2
            })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            positive_transitive_closure(&graph, &cancelled),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn source_chunks_cover_canonical_ranges() {
        assert_eq!(
            transitive_closure_source_chunks(0, 4),
            Vec::<(usize, usize)>::new()
        );
        assert_eq!(transitive_closure_source_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(transitive_closure_source_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(
            transitive_closure_source_chunks(8, 4),
            vec![(0, 2), (2, 4), (4, 6), (6, 8)]
        );
        assert_eq!(
            transitive_closure_source_chunks(3, 8),
            vec![(0, 1), (1, 2), (2, 3)]
        );
    }

    #[test]
    fn path_selection_requires_private_pool_and_crossover() {
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_transitive_closure_path(&no_pool, 64, 1_024),
            TransitiveClosureExecutionPath::Serial
        );

        let one = control_with_threads(1);
        assert_eq!(
            select_transitive_closure_path(&one, 64, 1_024),
            TransitiveClosureExecutionPath::Serial
        );

        let parallel = control_with_threads(4);
        assert_eq!(
            select_transitive_closure_path(&parallel, 64, 1),
            TransitiveClosureExecutionPath::Serial
        );
        assert_eq!(
            select_transitive_closure_path(
                &parallel,
                64,
                TRANSITIVE_CLOSURE_PARALLEL_CROSSOVER_WORK / 64
            ),
            TransitiveClosureExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn thread_matrix_matches_one_thread_output_order_and_fingerprint() {
        let graph = cyclic_graph(128, 8);
        assert!(
            (graph.node_ids().len() as u64).saturating_mul(graph.edge_entry_count())
                >= TRANSITIVE_CLOSURE_PARALLEL_CROSSOVER_WORK
        );
        let serial = positive_transitive_closure(&graph, &control_with_threads(1)).unwrap();
        assert_eq!(serial.len(), 128 * 128);
        let serial_pairs = pairs(&serial);

        for threads in [2, 4, 8] {
            let parallel =
                positive_transitive_closure(&graph, &control_with_threads(threads)).unwrap();
            assert_eq!(parallel, serial);
            assert_eq!(pairs(&parallel), serial_pairs);
        }
    }

    #[test]
    fn parallel_path_honors_cancelled_control() {
        let graph = cyclic_graph(128, 8);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            cancellation,
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            positive_transitive_closure(&graph, &cancelled),
            Err(AlgorithmError::Cancelled)
        );
    }
}
