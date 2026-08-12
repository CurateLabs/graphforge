use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;
/// Present dyads below this count stay serial to avoid private-pool scheduling tax.
///
/// Dyad normalization remains serial and canonical. Above this crossover, only the
/// independent category tally over already-normalized present dyads is chunked.
pub(crate) const DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS: usize = 32_768;
type NodeUuid = [u8; 16];
type DirectedPair = (NodeUuid, NodeUuid);

/// Selected execution path for dyad census category counting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DyadCensusExecutionPath {
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
    let mut seen_pairs = BTreeMap::<([u8; 16], [u8; 16]), u8>::new();

    for (source, target) in directed_pairs {
        checkpoint(control, &mut work)?;
        let (pair, direction) = if source < target {
            ((source, target), 0b01)
        } else {
            ((target, source), 0b10)
        };
        *seen_pairs.entry(pair).or_default() |= direction;
    }

    let mut counts = match select_dyad_census_path(control, seen_pairs.len()) {
        DyadCensusExecutionPath::Serial => {
            count_dyads_serial(seen_pairs.values(), control, &mut work)?
        }
        DyadCensusExecutionPath::Parallel { .. } => {
            let directions = seen_pairs.values().copied().collect::<Vec<_>>();
            count_dyads_parallel(&directions, control)?
        }
    };
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

pub(crate) fn select_dyad_census_path(
    control: &AlgorithmControl,
    present_pairs: usize,
) -> DyadCensusExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || present_pairs < DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return DyadCensusExecutionPath::Serial;
    }
    DyadCensusExecutionPath::Parallel {
        threads,
        chunks: dyad_chunks(present_pairs, threads).len(),
    }
}

fn count_dyads_serial<'a>(
    directions: impl IntoIterator<Item = &'a u8>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<DyadCounts, AlgorithmError> {
    let mut counts = DyadCounts::default();
    for directions in directions {
        checkpoint(control, work)?;
        add_direction(&mut counts, *directions)?;
    }
    Ok(counts)
}

fn count_dyads_parallel(
    directions: &[u8],
    control: &AlgorithmControl,
) -> Result<DyadCounts, AlgorithmError> {
    let pool = control
        .compute_pool()
        .ok_or_else(|| execution("parallel dyad_census requires an instance-owned compute pool"))?;
    let ranges = dyad_chunks(directions.len(), control.compute_threads());
    let chunk_counts = run_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                control.check_cancelled()?;
                let mut work = 0_usize;
                let mut counts = DyadCounts::default();
                for &directions in &directions[start..end] {
                    checkpoint(control, &mut work)?;
                    add_direction(&mut counts, directions)?;
                }
                Ok(counts)
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()
    })?;
    let mut counts = DyadCounts::default();
    for chunk in chunk_counts {
        counts.mutual = add_count(counts.mutual, chunk.mutual, "mutual")?;
        counts.asymmetric = add_count(counts.asymmetric, chunk.asymmetric, "asymmetric")?;
    }
    Ok(counts)
}

fn dyad_chunks(len: usize, threads: usize) -> Vec<(usize, usize)> {
    if len == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, len);
    let base = len / workers;
    let rem = len % workers;
    let mut chunks = Vec::with_capacity(workers);
    let mut start = 0;
    for index in 0..workers {
        let chunk_len = base + usize::from(index < rem);
        let end = start + chunk_len;
        if start < end {
            chunks.push((start, end));
        }
        start = end;
    }
    chunks
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

fn add_direction(counts: &mut DyadCounts, directions: u8) -> Result<(), AlgorithmError> {
    if directions == 0b11 {
        counts.mutual = increment(counts.mutual, "mutual")?;
    } else {
        counts.asymmetric = increment(counts.asymmetric, "asymmetric")?;
    }
    Ok(())
}

fn increment(value: u64, category: &str) -> Result<u64, AlgorithmError> {
    add_count(value, 1, category)
}

fn add_count(left: u64, right: u64, category: &str) -> Result<u64, AlgorithmError> {
    left.checked_add(right).ok_or_else(|| {
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
    use crate::compute_pool::ComputePool;
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
        .with_compute_pool(Arc::new(ComputePool::new(threads).unwrap()))
    }

    fn dense_asymmetric_fixture(nodes: usize) -> (Vec<NodeUuid>, Vec<DyadEdge>) {
        let nodes = (0..nodes)
            .map(|node| wide_uuid(node as u128))
            .collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut edge_id = 1_u128;
        for source in 0..nodes.len() {
            for target in source + 1..nodes.len() {
                edges.push(DyadEdge {
                    edge: wide_uuid(edge_id),
                    source: nodes[source],
                    target: nodes[target],
                });
                edge_id += 1;
            }
        }
        (nodes, edges)
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
    fn path_selection_respects_crossover_and_private_pool() {
        let serial = control_with_threads(1);
        assert_eq!(
            select_dyad_census_path(&serial, DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS),
            DyadCensusExecutionPath::Serial
        );
        let below = control_with_threads(4);
        assert_eq!(
            select_dyad_census_path(&below, DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS - 1),
            DyadCensusExecutionPath::Serial
        );
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_dyad_census_path(&no_pool, DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS),
            DyadCensusExecutionPath::Serial
        );
        assert_eq!(
            select_dyad_census_path(
                &control_with_threads(4),
                DYAD_CENSUS_PARALLEL_CROSSOVER_PAIRS
            ),
            DyadCensusExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn thread_matrix_preserves_dyad_counts() {
        let (nodes, edges) = dense_asymmetric_fixture(300);
        let serial = dyad_census(&nodes, &edges, &control_with_threads(1)).unwrap();
        assert_eq!(
            serial,
            DyadCounts {
                mutual: 0,
                asymmetric: 44_850,
                null: 0
            }
        );
        for threads in [2_usize, 4, 8] {
            let control = control_with_threads(threads);
            assert!(matches!(
                select_dyad_census_path(&control, 44_850),
                DyadCensusExecutionPath::Parallel { .. }
            ));
            assert_eq!(dyad_census(&nodes, &edges, &control).unwrap(), serial);
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
}
