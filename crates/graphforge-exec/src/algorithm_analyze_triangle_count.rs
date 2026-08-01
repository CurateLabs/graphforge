use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

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
    let mut count = 0_u64;

    for source in 0..neighbors.len() {
        checkpoint(control, &mut work)?;
        for &middle in neighbors[source].range(source.saturating_add(1)..) {
            checkpoint(control, &mut work)?;
            for &target in neighbors[middle].range(middle.saturating_add(1)..) {
                checkpoint(control, &mut work)?;
                if neighbors[source].contains(&target) {
                    count = increment(count)?;
                }
            }
        }
    }
    Ok(count)
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
}
