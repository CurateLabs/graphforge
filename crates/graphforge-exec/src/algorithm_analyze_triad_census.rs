use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;
/// Weak dyads below this count stay serial to avoid private-pool scheduling tax.
pub(crate) const TRIAD_CENSUS_PARALLEL_CROSSOVER_DYADS: usize = 4_096;

pub(crate) const TRIAD_NAMES: [&str; 16] = [
    "003", "012", "102", "021D", "021U", "021C", "111D", "111U", "030T", "030C", "201", "120D",
    "120U", "120C", "210", "300",
];

// Indexed by the six ordered-pair presence bits described in `triad_code`.
const TRIAD_INDEX: [usize; 64] = [
    0, 1, 1, 2, 1, 3, 5, 7, 1, 5, 4, 6, 2, 7, 6, 10, 1, 5, 3, 7, 4, 8, 8, 12, 5, 9, 8, 13, 6, 13,
    11, 14, 1, 4, 5, 6, 5, 8, 9, 13, 3, 8, 8, 11, 7, 12, 13, 14, 2, 6, 7, 10, 6, 11, 13, 14, 7, 13,
    12, 14, 10, 14, 14, 15,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TriadEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TriadCensusExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

/// Count the sixteen MAN triad classes in a directed simple projection.
pub(crate) fn triad_census(
    nodes: &[[u8; 16]],
    edges: &[TriadEdge],
    control: &AlgorithmControl,
) -> Result<[u64; 16], AlgorithmError> {
    control.checkpoint()?;
    control.check_output_rows(16)?;
    let mut work = 0;
    let index = index_nodes(nodes, control, &mut work)?;
    let successors = directed_neighbors(edges, &index, control, &mut work)?;
    let neighbors = weak_neighbors(&successors);
    let weak_dyads = count_ordered_weak_dyads(&neighbors);
    let mut counts = match select_triad_census_path(control, neighbors.len(), weak_dyads) {
        TriadCensusExecutionPath::Serial => count_source_range(
            0,
            neighbors.len(),
            &neighbors,
            &successors,
            control,
            &mut work,
        )?,
        TriadCensusExecutionPath::Parallel { .. } => {
            count_connected_triads_parallel(&neighbors, &successors, control)?
        }
    };

    let total = choose_three(neighbors.len())?;
    let connected = counts[1..]
        .iter()
        .try_fold(0_u64, |sum, &value| add(sum, value))?;
    counts[0] = total
        .checked_sub(connected)
        .ok_or_else(|| execution("triad_census invariant exceeds V choose 3"))?;
    if counts
        .iter()
        .try_fold(0_u64, |sum, &value| add(sum, value))?
        != total
    {
        return Err(execution(
            "triad_census invariant does not equal V choose 3",
        ));
    }
    Ok(counts)
}

pub(crate) fn select_triad_census_path(
    control: &AlgorithmControl,
    nodes: usize,
    weak_dyads: usize,
) -> TriadCensusExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes < 3
        || weak_dyads < TRIAD_CENSUS_PARALLEL_CROSSOVER_DYADS
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return TriadCensusExecutionPath::Serial;
    }
    TriadCensusExecutionPath::Parallel {
        threads,
        chunks: source_chunks(nodes, threads).len(),
    }
}

fn count_connected_triads_parallel(
    neighbors: &[BTreeSet<usize>],
    successors: &[BTreeSet<usize>],
    control: &AlgorithmControl,
) -> Result<[u64; 16], AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel triad_census requires an instance-owned compute pool")
    })?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let mut chunk_results = run_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut work = 0_usize;
                (
                    start,
                    count_source_range(start, end, neighbors, successors, control, &mut work),
                )
            })
            .collect::<Vec<_>>()
    })?;
    chunk_results.sort_unstable_by_key(|(start, _)| *start);
    let mut counts = [0_u64; 16];
    for (_, chunk) in chunk_results {
        merge_counts(&mut counts, chunk?)?;
    }
    Ok(counts)
}

fn count_source_range(
    start: usize,
    end: usize,
    neighbors: &[BTreeSet<usize>],
    successors: &[BTreeSet<usize>],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<[u64; 16], AlgorithmError> {
    let mut counts = [0_u64; 16];
    // Batagelj-Mrvar: visit only triads incident to a dyad, then derive 003.
    for v in start..end {
        checkpoint(control, work)?;
        for &u in neighbors[v].range(v.saturating_add(1)..) {
            checkpoint(control, work)?;
            let union = neighbors[v]
                .union(&neighbors[u])
                .copied()
                .filter(|&w| w != v && w != u)
                .collect::<BTreeSet<_>>();
            for &w in &union {
                checkpoint(control, work)?;
                if u < w || (v < w && w < u && !neighbors[w].contains(&v)) {
                    let class = TRIAD_INDEX[triad_code(v, u, w, successors)];
                    counts[class] = increment(counts[class])?;
                }
            }
            let dyadic = u64::try_from(neighbors.len() - union.len() - 2)
                .map_err(|_| execution("triad_census exceeds supported range"))?;
            let class = if successors[v].contains(&u) && successors[u].contains(&v) {
                2
            } else {
                1
            };
            counts[class] = add(counts[class], dyadic)?;
        }
    }
    Ok(counts)
}

fn count_ordered_weak_dyads(neighbors: &[BTreeSet<usize>]) -> usize {
    neighbors
        .iter()
        .enumerate()
        .map(|(source, adjacent)| adjacent.range(source.saturating_add(1)..).count())
        .fold(0_usize, usize::saturating_add)
}

fn source_chunks(len: usize, threads: usize) -> Vec<(usize, usize)> {
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
        Err(_) => Err(execution("triad_census worker panicked")),
    }
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
            return Err(execution("triad_census node UUIDs must be unique"));
        }
    }
    Ok(index)
}

fn directed_neighbors(
    edges: &[TriadEdge],
    index: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<BTreeSet<usize>>, AlgorithmError> {
    let mut successors = vec![BTreeSet::new(); index.len()];
    let mut stored = BTreeMap::new();
    for &edge in edges {
        checkpoint(control, work)?;
        let Some(&source) = index.get(&edge.source) else {
            return Err(execution(
                "triad_census edge endpoint is outside node selection",
            ));
        };
        let Some(&target) = index.get(&edge.target) else {
            return Err(execution(
                "triad_census edge endpoint is outside node selection",
            ));
        };
        if let Some(previous) = stored.insert(edge.edge, edge) {
            if previous != edge {
                return Err(execution(
                    "triad_census edge UUID has inconsistent adjacency entries",
                ));
            }
            continue;
        }
        if source != target {
            successors[source].insert(target);
        }
    }
    Ok(successors)
}

fn weak_neighbors(successors: &[BTreeSet<usize>]) -> Vec<BTreeSet<usize>> {
    let mut neighbors = successors.to_vec();
    for (source, targets) in successors.iter().enumerate() {
        for &target in targets {
            neighbors[target].insert(source);
        }
    }
    neighbors
}

fn triad_code(a: usize, b: usize, c: usize, successors: &[BTreeSet<usize>]) -> usize {
    [(a, b), (b, a), (a, c), (c, a), (b, c), (c, b)]
        .into_iter()
        .enumerate()
        .fold(0, |code, (bit, (source, target))| {
            code | (usize::from(successors[source].contains(&target)) << bit)
        })
}

fn choose_three(n: usize) -> Result<u64, AlgorithmError> {
    let n = u64::try_from(n).map_err(|_| execution("triad_census exceeds supported range"))?;
    if n < 3 {
        return Ok(0);
    }
    let mut factors = [n, n - 1, n - 2];
    for divisor in [2_u64, 3] {
        let factor = factors
            .iter_mut()
            .find(|factor| **factor % divisor == 0)
            .expect("three consecutive integers contain required divisor");
        *factor /= divisor;
    }
    factors.into_iter().try_fold(1_u64, |product, factor| {
        product
            .checked_mul(factor)
            .ok_or_else(|| execution("triad_census exceeds supported range"))
    })
}

fn increment(value: u64) -> Result<u64, AlgorithmError> {
    add(value, 1)
}

fn merge_counts(left: &mut [u64; 16], right: [u64; 16]) -> Result<(), AlgorithmError> {
    for (left, right) in left.iter_mut().zip(right) {
        *left = add(*left, right)?;
    }
    Ok(())
}

fn add(left: u64, right: u64) -> Result<u64, AlgorithmError> {
    left.checked_add(right)
        .ok_or_else(|| execution("triad_census exceeds supported range"))
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint().map(|_| ())
    } else {
        control.check_cancelled()
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
    use crate::compute_pool::ComputePool;
    use std::sync::Arc;

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn wide_uuid(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    fn edge(id: u8, source: u8, target: u8) -> TriadEdge {
        TriadEdge {
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

    fn complete_reciprocal_fixture(nodes: usize) -> (Vec<[u8; 16]>, Vec<TriadEdge>) {
        let nodes = (0..nodes)
            .map(|node| wide_uuid(node as u128))
            .collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut edge_id = 1_u128;
        for source in 0..nodes.len() {
            for target in 0..nodes.len() {
                if source == target {
                    continue;
                }
                edges.push(TriadEdge {
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
    fn classifies_every_ordered_pair_pattern_in_standard_order() {
        for code in 0_u8..64 {
            let edges = [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]
                .into_iter()
                .enumerate()
                .filter(|(bit, _)| code & (1 << bit) != 0)
                .map(|(bit, (source, target))| edge(10 + bit as u8, source, target))
                .collect::<Vec<_>>();
            let counts = triad_census(&[uuid(0), uuid(1), uuid(2)], &edges, &control()).unwrap();
            assert_eq!(counts[TRIAD_INDEX[usize::from(code)]], 1, "code {code}");
            assert_eq!(counts.iter().sum::<u64>(), 1);
        }
        assert_eq!(TRIAD_NAMES[0], "003");
        assert_eq!(TRIAD_NAMES[15], "300");
    }

    #[test]
    fn handles_trivial_edgeless_and_normalized_directed_graphs() {
        assert_eq!(triad_census(&[], &[], &control()).unwrap(), [0; 16]);
        assert_eq!(
            triad_census(&[uuid(0), uuid(1)], &[], &control()).unwrap(),
            [0; 16]
        );
        let edgeless =
            triad_census(&[uuid(0), uuid(1), uuid(2), uuid(3)], &[], &control()).unwrap();
        assert_eq!(edgeless[0], 4);
        let normalized = triad_census(
            &[uuid(0), uuid(1), uuid(2)],
            &[
                edge(10, 0, 1),
                edge(10, 0, 1),
                edge(11, 0, 1),
                edge(12, 1, 0),
                edge(13, 0, 0),
            ],
            &control(),
        )
        .unwrap();
        assert_eq!(normalized[2], 1);
    }

    #[test]
    fn path_selection_respects_crossover_and_private_pool() {
        let serial = control_with_threads(1);
        assert_eq!(
            select_triad_census_path(&serial, 96, TRIAD_CENSUS_PARALLEL_CROSSOVER_DYADS),
            TriadCensusExecutionPath::Serial
        );
        let below = control_with_threads(4);
        assert_eq!(
            select_triad_census_path(&below, 96, TRIAD_CENSUS_PARALLEL_CROSSOVER_DYADS - 1),
            TriadCensusExecutionPath::Serial
        );
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_triad_census_path(&no_pool, 96, TRIAD_CENSUS_PARALLEL_CROSSOVER_DYADS),
            TriadCensusExecutionPath::Serial
        );
        assert_eq!(
            select_triad_census_path(
                &control_with_threads(4),
                96,
                TRIAD_CENSUS_PARALLEL_CROSSOVER_DYADS
            ),
            TriadCensusExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn thread_matrix_preserves_complete_triad_counts() {
        let (nodes, edges) = complete_reciprocal_fixture(96);
        let serial = triad_census(&nodes, &edges, &control_with_threads(1)).unwrap();
        let mut expected = [0_u64; 16];
        expected[15] = 142_880;
        assert_eq!(serial, expected);
        for threads in [2_usize, 4, 8] {
            let control = control_with_threads(threads);
            assert!(matches!(
                select_triad_census_path(&control, nodes.len(), 4_560),
                TriadCensusExecutionPath::Parallel { .. }
            ));
            assert_eq!(triad_census(&nodes, &edges, &control).unwrap(), serial);
        }
    }

    #[test]
    fn rejects_bad_identity_and_obeys_shared_controls() {
        assert!(triad_census(&[uuid(0), uuid(0)], &[], &control()).is_err());
        assert!(triad_census(&[uuid(0)], &[edge(1, 0, 2)], &control()).is_err());
        assert!(
            triad_census(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control()
            )
            .is_err()
        );
        let no_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 15,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            triad_census(&[], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            triad_census(
                &[],
                &[],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
