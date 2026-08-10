use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use rayon::prelude::*;

const CHECKPOINT_INTERVAL: usize = 4_096;
const DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS: usize = 16_384;
type NodeUuid = [u8; 16];
type DirectedPair = (NodeUuid, NodeUuid);
type UnorderedPair = (NodeUuid, NodeUuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DyadCensusExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// One directed stored edge entry in the selected public-identity projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DyadEdge {
    pub edge: NodeUuid,
    pub source: NodeUuid,
    pub target: NodeUuid,
}

/// Counts for the three canonical directed dyad categories.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DyadCounts {
    pub mutual: u64,
    pub asymmetric: u64,
    pub null: u64,
}

/// Classify every unordered pair of distinct selected nodes by edge presence.
pub(crate) fn dyad_census(
    nodes: &[NodeUuid],
    edges: &[DyadEdge],
    control: &AlgorithmControl,
) -> Result<DyadCounts, AlgorithmError> {
    control.checkpoint()?;
    control.check_output_rows(3)?;
    let mut work = 0_usize;
    let selected = index_nodes(nodes, control, &mut work)?;
    let directed_pairs = normalize_edges(edges, &selected, control, &mut work)?;
    let mut counts = classify_dyads(&directed_pairs, control, &mut work)?;

    let nodes = u64::try_from(selected.len())
        .map_err(|_| execution("dyad_census node count exceeds UInt64 range"))?;
    let total = nodes
        .checked_mul(nodes.saturating_sub(1))
        .and_then(|value| value.checked_div(2))
        .ok_or_else(|| execution("dyad_census pair count exceeds supported range"))?;
    let present = counts
        .mutual
        .checked_add(counts.asymmetric)
        .ok_or_else(|| execution("dyad_census category sum exceeds supported range"))?;
    counts.null = total
        .checked_sub(present)
        .ok_or_else(|| execution("dyad_census category sum exceeds pair count"))?;
    Ok(counts)
}

fn classify_dyads(
    directed_pairs: &BTreeSet<DirectedPair>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<DyadCounts, AlgorithmError> {
    let seen_pairs = match select_dyad_census_path(control, directed_pairs.len()) {
        DyadCensusExecutionPath::Serial => classify_dyads_serial(directed_pairs, control, work)?,
        DyadCensusExecutionPath::Parallel { .. } => {
            classify_dyads_parallel(directed_pairs, control)?
        }
    };
    count_seen_dyads(&seen_pairs, control, work)
}

fn classify_dyads_serial(
    directed_pairs: &BTreeSet<DirectedPair>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<BTreeMap<UnorderedPair, u8>, AlgorithmError> {
    let mut seen_pairs = BTreeMap::new();
    for &(source, target) in directed_pairs {
        checkpoint(control, work)?;
        record_directed_pair(&mut seen_pairs, source, target);
    }
    Ok(seen_pairs)
}

fn classify_dyads_parallel(
    directed_pairs: &BTreeSet<DirectedPair>,
    control: &AlgorithmControl,
) -> Result<BTreeMap<UnorderedPair, u8>, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel dyad_census requires an instance-owned compute pool"))?;
    let pairs = directed_pairs.iter().copied().collect::<Vec<_>>();
    let ranges = dyad_pair_chunks(pairs.len(), control.compute_threads());
    let chunk_maps = run_dyad_census_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut seen_pairs = BTreeMap::new();
                let mut work = 0_usize;
                for &(source, target) in &pairs[start..end] {
                    work = work.saturating_add(1);
                    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
                        control.check_cancelled()?;
                    }
                    record_directed_pair(&mut seen_pairs, source, target);
                }
                Ok(seen_pairs)
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;

    let mut seen_pairs = BTreeMap::new();
    for chunk in chunk_maps {
        for (pair, directions) in chunk {
            *seen_pairs.entry(pair).or_default() |= directions;
        }
    }
    Ok(seen_pairs)
}

fn record_directed_pair(
    seen_pairs: &mut BTreeMap<UnorderedPair, u8>,
    source: NodeUuid,
    target: NodeUuid,
) {
    let (pair, direction) = if source < target {
        ((source, target), 0b01)
    } else {
        ((target, source), 0b10)
    };
    *seen_pairs.entry(pair).or_default() |= direction;
}

fn count_seen_dyads(
    seen_pairs: &BTreeMap<UnorderedPair, u8>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<DyadCounts, AlgorithmError> {
    let mut counts = DyadCounts::default();
    for directions in seen_pairs.values() {
        checkpoint(control, work)?;
        if *directions == 0b11 {
            counts.mutual = increment(counts.mutual, "mutual")?;
        } else {
            counts.asymmetric = increment(counts.asymmetric, "asymmetric")?;
        }
    }
    Ok(counts)
}

fn select_dyad_census_path(
    control: &AlgorithmControl,
    directed_pairs: usize,
) -> DyadCensusExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || directed_pairs < DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return DyadCensusExecutionPath::Serial;
    }
    let chunks = dyad_pair_chunks(directed_pairs, threads).len();
    if chunks <= 1 {
        DyadCensusExecutionPath::Serial
    } else {
        DyadCensusExecutionPath::Parallel { threads, chunks }
    }
}

fn dyad_pair_chunks(pairs: usize, threads: usize) -> Vec<(usize, usize)> {
    if pairs == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, pairs);
    let base = pairs / workers;
    let rem = pairs % workers;
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

fn run_dyad_census_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, AlgorithmError> + Send,
) -> Result<R, AlgorithmError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(execution("dyad_census worker panicked")),
    }
}

fn index_nodes(
    nodes: &[[u8; 16]],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<BTreeSet<NodeUuid>, AlgorithmError> {
    let mut selected = BTreeSet::new();
    for &node in nodes {
        checkpoint(control, work)?;
        if !selected.insert(node) {
            return Err(execution("dyad_census node UUIDs must be unique"));
        }
    }
    Ok(selected)
}

fn normalize_edges(
    edges: &[DyadEdge],
    selected: &BTreeSet<NodeUuid>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<BTreeSet<DirectedPair>, AlgorithmError> {
    let mut stored = BTreeMap::new();
    let mut directed_pairs = BTreeSet::new();
    for &edge in edges {
        checkpoint(control, work)?;
        if !selected.contains(&edge.source) || !selected.contains(&edge.target) {
            return Err(execution(
                "dyad_census edge endpoint is outside node selection",
            ));
        }
        if let Some(previous) = stored.insert(edge.edge, (edge.source, edge.target))
            && previous != (edge.source, edge.target)
        {
            return Err(execution(
                "dyad_census edge UUID has inconsistent adjacency entries",
            ));
        }
        if edge.source != edge.target {
            directed_pairs.insert((edge.source, edge.target));
        }
    }
    Ok(directed_pairs)
}

fn increment(value: u64, category: &str) -> Result<u64, AlgorithmError> {
    value.checked_add(1).ok_or_else(|| {
        execution(format!(
            "dyad_census {category} count exceeds supported range"
        ))
    })
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

    fn wide_uuid(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    fn edge(id: u8, source: u8, target: u8) -> DyadEdge {
        DyadEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

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

    fn dense_directed_fixture(nodes: u128, fanout: u128) -> (Vec<NodeUuid>, Vec<DyadEdge>) {
        let selected = (0..nodes).map(wide_uuid).collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut edge_id = 0_u128;
        for source in 0..nodes {
            for hop in 1..=fanout {
                let target = (source + hop) % nodes;
                edges.push(DyadEdge {
                    edge: wide_uuid(1_000_000 + edge_id),
                    source: wide_uuid(source),
                    target: wide_uuid(target),
                });
                edge_id += 1;
            }
        }
        (selected, edges)
    }

    #[test]
    fn classifies_hand_verifiable_directed_fixture() {
        let nodes = (0..5).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(10, 0, 1),
            edge(11, 1, 0),
            edge(12, 0, 2),
            edge(13, 3, 2),
        ];
        assert_eq!(
            dyad_census(&nodes, &edges, &control()).unwrap(),
            DyadCounts {
                mutual: 1,
                asymmetric: 2,
                null: 7,
            }
        );
    }

    #[test]
    fn normalizes_parallel_duplicate_reciprocal_and_loop_entries() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        let edges = [
            edge(10, 0, 1),
            edge(10, 0, 1),
            edge(11, 0, 1),
            edge(12, 1, 0),
            edge(13, 2, 2),
        ];
        assert_eq!(
            dyad_census(&nodes, &edges, &control()).unwrap(),
            DyadCounts {
                mutual: 1,
                asymmetric: 0,
                null: 2,
            }
        );
    }

    #[test]
    fn empty_singleton_and_edgeless_selections_keep_three_category_counts() {
        for (nodes, expected_null) in [
            (vec![], 0),
            (vec![uuid(0)], 0),
            (vec![uuid(0), uuid(1), uuid(2), uuid(3)], 6),
        ] {
            assert_eq!(
                dyad_census(&nodes, &[], &control()).unwrap(),
                DyadCounts {
                    null: expected_null,
                    ..DyadCounts::default()
                }
            );
        }
    }

    #[test]
    fn rejects_invalid_identity_topology_atomically() {
        for result in [
            dyad_census(&[uuid(0), uuid(0)], &[], &control()),
            dyad_census(&[uuid(0)], &[edge(1, 0, 2)], &control()),
            dyad_census(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control(),
            ),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn shared_output_iteration_and_cancellation_controls_are_structured() {
        let no_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            dyad_census(&[], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let no_iterations = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            dyad_census(&[], &[], &no_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            dyad_census(
                &[],
                &[],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn pair_chunks_cover_canonical_ranges() {
        assert_eq!(dyad_pair_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(dyad_pair_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(dyad_pair_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(dyad_pair_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
        assert_eq!(dyad_pair_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    fn path_selection_requires_private_pool_and_crossover() {
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_dyad_census_path(&no_pool, DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS),
            DyadCensusExecutionPath::Serial
        );

        let one = control_with_threads(1);
        assert_eq!(
            select_dyad_census_path(&one, DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS),
            DyadCensusExecutionPath::Serial
        );

        let parallel = control_with_threads(4);
        assert_eq!(
            select_dyad_census_path(&parallel, DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS - 1),
            DyadCensusExecutionPath::Serial
        );
        assert_eq!(
            select_dyad_census_path(&parallel, DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS),
            DyadCensusExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn thread_matrix_matches_one_thread_counts() {
        let (nodes, edges) = dense_directed_fixture(192, 96);
        let serial = dyad_census(&nodes, &edges, &control_with_threads(1)).unwrap();
        assert_eq!(
            serial,
            DyadCounts {
                mutual: 96,
                asymmetric: 18_240,
                null: 0,
            }
        );
        for threads in [2, 4, 8] {
            assert_eq!(
                dyad_census(&nodes, &edges, &control_with_threads(threads)).unwrap(),
                serial
            );
        }
    }

    #[test]
    fn parallel_path_honors_cancelled_control() {
        let (nodes, edges) = dense_directed_fixture(192, 96);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let control = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            cancellation,
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        assert_eq!(
            dyad_census(&nodes, &edges, &control),
            Err(AlgorithmError::Cancelled)
        );
    }
}
