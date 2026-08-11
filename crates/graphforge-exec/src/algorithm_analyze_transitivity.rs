use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use rayon::prelude::*;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;
/// Wedges below this count stay serial to avoid private-pool scheduling tax.
pub(crate) const TRANSITIVITY_PARALLEL_CROSSOVER_WEDGES: u64 = 32_768;

/// One stored edge entry in the selected UUID projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TransitivityEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransitivityExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

impl TransitivityEdge {
    fn canonical(mut self) -> Self {
        if self.target < self.source {
            std::mem::swap(&mut self.source, &mut self.target);
        }
        self
    }
}

/// Compute global transitivity over an undirected simple projection.
pub(crate) fn transitivity(
    nodes: &[[u8; 16]],
    edges: &[TransitivityEdge],
    control: &AlgorithmControl,
) -> Result<f64, AlgorithmError> {
    control.checkpoint()?;
    control.check_output_rows(1)?;
    let mut work = 0_usize;
    let index = index_nodes(nodes, control, &mut work)?;
    let neighbors = simple_neighbors(edges, &index, control, &mut work)?;
    let wedges = count_wedges(&neighbors, control, &mut work)?;
    if wedges == 0 {
        return Ok(0.0);
    }
    let triangles = match select_transitivity_path(control, neighbors.len(), wedges) {
        TransitivityExecutionPath::Serial => count_triangles(&neighbors, control, &mut work)?,
        TransitivityExecutionPath::Parallel { .. } => {
            count_triangles_parallel(&neighbors, control)?
        }
    };
    ratio(triangles, wedges)
}

pub(crate) fn select_transitivity_path(
    control: &AlgorithmControl,
    nodes: usize,
    wedges: u64,
) -> TransitivityExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1
        || nodes < 3
        || wedges < TRANSITIVITY_PARALLEL_CROSSOVER_WEDGES
        || control
            .compute_pool()
            .is_none_or(|pool| !pool.is_parallel())
    {
        return TransitivityExecutionPath::Serial;
    }
    TransitivityExecutionPath::Parallel {
        threads,
        chunks: source_chunks(nodes, threads).len(),
    }
}

fn ratio(triangles: u64, wedges: u64) -> Result<f64, AlgorithmError> {
    let closed = triangles
        .checked_mul(3)
        .ok_or_else(|| execution("transitivity closed-wedge count exceeds supported range"))?;
    Ok(u64_as_f64(closed) / u64_as_f64(wedges))
}

fn u64_as_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).expect("shifted u64 fits u32");
    let low = u32::try_from(value & u64::from(u32::MAX)).expect("masked u64 fits u32");
    f64::from(high).mul_add(4_294_967_296.0, f64::from(low))
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
            return Err(execution("transitivity node UUIDs must be unique"));
        }
    }
    Ok(index)
}

fn simple_neighbors(
    edges: &[TransitivityEdge],
    index: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<BTreeSet<usize>>, AlgorithmError> {
    let mut stored = BTreeMap::new();
    for &raw in edges {
        checkpoint(control, work)?;
        let edge = raw.canonical();
        if !index.contains_key(&edge.source) || !index.contains_key(&edge.target) {
            return Err(execution(
                "transitivity edge endpoint is outside node selection",
            ));
        }
        if let Some(previous) = stored.insert(edge.edge, edge)
            && previous != edge
        {
            return Err(execution(
                "transitivity edge UUID has inconsistent adjacency entries",
            ));
        }
    }
    let mut neighbors = vec![BTreeSet::new(); index.len()];
    for edge in stored.into_values() {
        checkpoint(control, work)?;
        let source = index[&edge.source];
        let target = index[&edge.target];
        if source != target {
            neighbors[source].insert(target);
            neighbors[target].insert(source);
        }
    }
    Ok(neighbors)
}

fn count_wedges(
    neighbors: &[BTreeSet<usize>],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<u64, AlgorithmError> {
    let mut wedges = 0_u64;
    for adjacent in neighbors {
        checkpoint(control, work)?;
        let degree = u64::try_from(adjacent.len())
            .map_err(|_| execution("transitivity degree exceeds UInt64 range"))?;
        let at_node = degree
            .checked_mul(degree.saturating_sub(1))
            .and_then(|value| value.checked_div(2))
            .ok_or_else(|| execution("transitivity wedge count exceeds supported range"))?;
        wedges = wedges
            .checked_add(at_node)
            .ok_or_else(|| execution("transitivity wedge count exceeds supported range"))?;
    }
    Ok(wedges)
}

fn count_triangles(
    neighbors: &[BTreeSet<usize>],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<u64, AlgorithmError> {
    count_triangles_range(0, neighbors.len(), neighbors, control, work)
}

fn count_triangles_parallel(
    neighbors: &[BTreeSet<usize>],
    control: &AlgorithmControl,
) -> Result<u64, AlgorithmError> {
    let pool = control.compute_pool().ok_or_else(|| {
        execution("parallel transitivity requires an instance-owned compute pool")
    })?;
    let ranges = source_chunks(neighbors.len(), control.compute_threads());
    let mut chunk_results = run_on_pool(pool, || {
        Ok(ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut work = 0_usize;
                (
                    start,
                    count_triangles_range(start, end, neighbors, control, &mut work),
                )
            })
            .collect::<Vec<_>>())
    })?;
    chunk_results.sort_unstable_by_key(|(start, _)| *start);
    let mut triangles = 0_u64;
    for (_, chunk) in chunk_results {
        triangles = triangles
            .checked_add(chunk?)
            .ok_or_else(|| execution("transitivity triangle count exceeds supported range"))?;
    }
    Ok(triangles)
}

fn count_triangles_range(
    start: usize,
    end: usize,
    neighbors: &[BTreeSet<usize>],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<u64, AlgorithmError> {
    let mut triangles = 0_u64;
    for source in start..end {
        checkpoint(control, work)?;
        for &middle in neighbors[source].range(source.saturating_add(1)..) {
            for &target in neighbors[middle].range(middle.saturating_add(1)..) {
                checkpoint(control, work)?;
                if neighbors[source].contains(&target) {
                    triangles = triangles.checked_add(1).ok_or_else(|| {
                        execution("transitivity triangle count exceeds supported range")
                    })?;
                }
            }
        }
    }
    Ok(triangles)
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
        Err(_) => Err(execution("transitivity worker panicked")),
    }
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

    fn edge(id: u8, source: u8, target: u8) -> TransitivityEdge {
        TransitivityEdge {
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

    fn complete_graph_fixture(nodes: usize) -> (Vec<[u8; 16]>, Vec<TransitivityEdge>) {
        let nodes = (0..nodes)
            .map(|node| wide_uuid(node as u128))
            .collect::<Vec<_>>();
        let mut edges = Vec::new();
        let mut edge_id = 1_u128;
        for source in 0..nodes.len() {
            for target in source + 1..nodes.len() {
                edges.push(TransitivityEdge {
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
    fn exact_global_ratio_sums_disconnected_components_and_isolates() {
        let nodes = (0..8).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(10, 0, 1),
            edge(11, 1, 2),
            edge(12, 2, 0),
            edge(13, 2, 3),
            edge(14, 4, 5),
            edge(15, 5, 6),
            edge(16, 6, 4),
        ];
        // Two triangles and one tail: 6 closed wedges / 8 total wedges.
        assert_eq!(transitivity(&nodes, &edges, &control()).unwrap(), 0.75);
        let mut reversed_nodes = nodes;
        reversed_nodes.reverse();
        let mut reversed_edges = edges;
        reversed_edges.reverse();
        assert_eq!(
            transitivity(&reversed_nodes, &reversed_edges, &control()).unwrap(),
            0.75
        );
    }

    #[test]
    fn mirrors_parallel_reciprocals_and_self_loops_use_simple_neighbors() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        let edges = [
            edge(10, 0, 1),
            edge(10, 1, 0),
            edge(11, 0, 1),
            edge(12, 1, 0),
            edge(13, 1, 2),
            edge(14, 2, 0),
            edge(15, 0, 0),
        ];
        assert_eq!(transitivity(&nodes, &edges, &control()).unwrap(), 1.0);
    }

    #[test]
    fn empty_edgeless_and_no_wedge_graphs_return_finite_zero() {
        for (nodes, edges) in [
            (vec![], vec![]),
            (vec![uuid(0), uuid(1)], vec![]),
            (vec![uuid(0), uuid(1)], vec![edge(1, 0, 1)]),
        ] {
            let value = transitivity(&nodes, &edges, &control()).unwrap();
            assert_eq!(value, 0.0);
            assert!(value.is_finite());
        }
    }

    #[test]
    fn path_selection_respects_crossover_and_private_pool() {
        let serial = control_with_threads(1);
        assert_eq!(
            select_transitivity_path(&serial, 128, TRANSITIVITY_PARALLEL_CROSSOVER_WEDGES),
            TransitivityExecutionPath::Serial
        );
        let below = control_with_threads(4);
        assert_eq!(
            select_transitivity_path(&below, 128, TRANSITIVITY_PARALLEL_CROSSOVER_WEDGES - 1),
            TransitivityExecutionPath::Serial
        );
        let no_pool = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            select_transitivity_path(&no_pool, 128, TRANSITIVITY_PARALLEL_CROSSOVER_WEDGES),
            TransitivityExecutionPath::Serial
        );
        assert_eq!(
            select_transitivity_path(
                &control_with_threads(4),
                128,
                TRANSITIVITY_PARALLEL_CROSSOVER_WEDGES
            ),
            TransitivityExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn thread_matrix_preserves_complete_graph_ratio_bits() {
        let (nodes, edges) = complete_graph_fixture(128);
        let serial = transitivity(&nodes, &edges, &control_with_threads(1)).unwrap();
        assert_eq!(serial, 1.0);
        for threads in [2_usize, 4, 8] {
            let control = control_with_threads(threads);
            assert!(matches!(
                select_transitivity_path(&control, nodes.len(), 1_024_128),
                TransitivityExecutionPath::Parallel { .. }
            ));
            assert_eq!(
                transitivity(&nodes, &edges, &control).unwrap().to_bits(),
                serial.to_bits()
            );
        }
    }

    #[test]
    fn invalid_identity_topology_is_atomic() {
        for result in [
            transitivity(&[uuid(0), uuid(0)], &[], &control()),
            transitivity(&[uuid(0)], &[edge(1, 0, 2)], &control()),
            transitivity(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control(),
            ),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn shared_limits_and_cancellation_are_structured() {
        assert!(matches!(
            ratio(u64::MAX, 1),
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
            transitivity(&[], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            transitivity(
                &[],
                &[],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
        let no_iterations = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            transitivity(&[], &[], &no_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }
}
