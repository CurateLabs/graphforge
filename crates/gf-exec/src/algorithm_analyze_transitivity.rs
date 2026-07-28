use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

/// One stored edge entry in the selected UUID projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TransitivityEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
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
    let triangles = count_triangles(&neighbors, control, &mut work)?;
    ratio(triangles, wedges)
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
    let mut triangles = 0_u64;
    for source in 0..neighbors.len() {
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

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
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
