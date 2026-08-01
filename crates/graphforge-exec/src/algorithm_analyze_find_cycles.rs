use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

/// One stored edge entry in the selected UUID projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CycleEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

impl CycleEdge {
    fn canonical_undirected(mut self) -> Self {
        if self.target < self.source {
            std::mem::swap(&mut self.source, &mut self.target);
        }
        self
    }
}

#[derive(Clone, Copy)]
struct Frame {
    node: usize,
    next: usize,
}

struct IndexedNodes {
    ordered: Vec<[u8; 16]>,
    positions: BTreeMap<[u8; 16], usize>,
}

/// Enumerate deterministic canonical simple node cycles.
pub(crate) fn find_cycles(
    nodes: &[[u8; 16]],
    edges: &[CycleEdge],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<Vec<Vec<[u8; 16]>>, AlgorithmError> {
    control.checkpoint()?;
    let mut work = 0_usize;
    let nodes = index_nodes(nodes, control, &mut work)?;
    let neighbors = normalize_edges(edges, &nodes.positions, directed, control, &mut work)?;
    let mut cycles = BTreeSet::new();
    let mut visited = vec![false; nodes.ordered.len()];

    for start in 0..nodes.ordered.len() {
        checkpoint(control, &mut work)?;
        visited[start] = true;
        let mut path = vec![start];
        let mut stack = vec![Frame {
            node: start,
            next: 0,
        }];

        while let Some(frame) = stack.last() {
            checkpoint(control, &mut work)?;
            if frame.next == neighbors[frame.node].len() {
                let finished = stack.pop().expect("stack is non-empty").node;
                path.pop();
                visited[finished] = false;
                continue;
            }

            let frame = stack.last_mut().expect("stack is non-empty");
            let next = neighbors[frame.node][frame.next];
            frame.next = frame.next.saturating_add(1);
            if next == start {
                if path.len() == 1
                    || (directed && path.len() >= 2)
                    || (!directed && path.len() >= 3)
                {
                    let cycle = path
                        .iter()
                        .map(|&node| nodes.ordered[node])
                        .collect::<Vec<_>>();
                    let cycle = canonical_cycle(&cycle, directed);
                    if cycles.insert(cycle) {
                        control.check_output_rows(cycles.len())?;
                    }
                }
            } else if next >= start && !visited[next] {
                visited[next] = true;
                path.push(next);
                stack.push(Frame {
                    node: next,
                    next: 0,
                });
            }
        }
    }
    Ok(cycles.into_iter().collect())
}

fn index_nodes(
    nodes: &[[u8; 16]],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<IndexedNodes, AlgorithmError> {
    let mut ordered = nodes.to_vec();
    ordered.sort_unstable();
    let mut index = BTreeMap::new();
    for (position, &uuid) in ordered.iter().enumerate() {
        checkpoint(control, work)?;
        if index.insert(uuid, position).is_some() {
            return Err(execution("find_cycles node UUIDs must be unique"));
        }
    }
    Ok(IndexedNodes {
        ordered,
        positions: index,
    })
}

fn normalize_edges(
    edges: &[CycleEdge],
    node_index: &BTreeMap<[u8; 16], usize>,
    directed: bool,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<Vec<usize>>, AlgorithmError> {
    let mut stored = BTreeMap::new();
    for &raw in edges {
        checkpoint(control, work)?;
        let edge = if directed {
            raw
        } else {
            raw.canonical_undirected()
        };
        if !node_index.contains_key(&edge.source) || !node_index.contains_key(&edge.target) {
            return Err(execution(
                "find_cycles edge endpoint is outside node selection",
            ));
        }
        if let Some(previous) = stored.insert(edge.edge, edge)
            && previous != edge
        {
            return Err(execution(
                "find_cycles edge UUID has inconsistent adjacency entries",
            ));
        }
    }

    let mut neighbors = vec![BTreeSet::new(); node_index.len()];
    for edge in stored.into_values() {
        checkpoint(control, work)?;
        let source = node_index[&edge.source];
        let target = node_index[&edge.target];
        neighbors[source].insert(target);
        if !directed && source != target {
            neighbors[target].insert(source);
        }
    }
    Ok(neighbors
        .into_iter()
        .map(|adjacent| adjacent.into_iter().collect())
        .collect())
}

fn canonical_cycle(cycle: &[[u8; 16]], directed: bool) -> Vec<[u8; 16]> {
    let forward = smallest_rotation(cycle);
    if directed || cycle.len() < 2 {
        return forward;
    }
    let reversed = cycle.iter().rev().copied().collect::<Vec<_>>();
    forward.min(smallest_rotation(&reversed))
}

fn smallest_rotation(cycle: &[[u8; 16]]) -> Vec<[u8; 16]> {
    (0..cycle.len())
        .map(|offset| {
            cycle[offset..]
                .iter()
                .chain(&cycle[..offset])
                .copied()
                .collect::<Vec<_>>()
        })
        .min()
        .unwrap_or_default()
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

    fn edge(id: u8, source: u8, target: u8) -> CycleEdge {
        CycleEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn values(cycles: Vec<Vec<[u8; 16]>>) -> Vec<Vec<u8>> {
        cycles
            .into_iter()
            .map(|cycle| cycle.into_iter().map(|uuid| uuid[0]).collect())
            .collect()
    }

    #[test]
    fn directed_cycles_are_canonical_exact_and_disconnected() {
        let nodes = (0..8).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(10, 0, 1),
            edge(11, 1, 2),
            edge(12, 2, 0),
            edge(13, 1, 3),
            edge(14, 3, 1),
            edge(15, 4, 4),
            edge(16, 5, 6),
        ];
        assert_eq!(
            values(find_cycles(&nodes, &edges, true, &control()).unwrap()),
            [vec![0, 1, 2], vec![1, 3], vec![4]]
        );
        let mut reversed_nodes = nodes.clone();
        reversed_nodes.reverse();
        let mut reversed_edges = edges;
        reversed_edges.reverse();
        assert_eq!(
            find_cycles(&nodes, &edges, true, &control()).unwrap(),
            find_cycles(&reversed_nodes, &reversed_edges, true, &control()).unwrap()
        );
        assert!(find_cycles(&[], &[], true, &control()).unwrap().is_empty());
    }

    #[test]
    fn undirected_mirrors_parallel_reciprocals_and_loops_are_node_deduped() {
        let nodes = (0..5).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(10, 0, 1),
            edge(10, 1, 0),
            edge(11, 0, 1),
            edge(12, 1, 0),
            edge(13, 1, 2),
            edge(14, 2, 0),
            edge(15, 2, 3),
            edge(16, 3, 0),
            edge(17, 4, 4),
        ];
        assert_eq!(
            values(find_cycles(&nodes, &edges, false, &control()).unwrap()),
            [vec![0, 1, 2], vec![0, 1, 2, 3], vec![0, 2, 3], vec![4]]
        );
    }

    #[test]
    fn invalid_identity_topology_is_atomic() {
        for result in [
            find_cycles(&[uuid(0), uuid(0)], &[], true, &control()),
            find_cycles(&[uuid(0)], &[edge(1, 0, 2)], true, &control()),
            find_cycles(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                true,
                &control(),
            ),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn shared_limits_and_cancellation_are_structured() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        let edges = [edge(10, 0, 1), edge(11, 1, 2), edge(12, 2, 0)];
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            find_cycles(&nodes, &edges, true, &limited),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            find_cycles(
                &nodes,
                &edges,
                true,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
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
            find_cycles(&nodes, &edges, true, &no_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }
}
