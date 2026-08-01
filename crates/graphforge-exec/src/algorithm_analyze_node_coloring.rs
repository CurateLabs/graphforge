use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

/// One stored edge entry in the selected public-identity projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NodeColoringEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

impl NodeColoringEdge {
    fn canonical(mut self) -> Self {
        if self.target < self.source {
            std::mem::swap(&mut self.source, &mut self.target);
        }
        self
    }
}

/// One deterministic public node-color assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NodeColor {
    pub node: [u8; 16],
    pub color: u64,
}

/// Greedily color the selected undirected simple graph in ascending UUID order.
pub(crate) fn greedy_node_coloring(
    nodes: &[[u8; 16]],
    edges: &[NodeColoringEdge],
    control: &AlgorithmControl,
) -> Result<Vec<NodeColor>, AlgorithmError> {
    control.checkpoint()?;

    let mut work = 0_usize;
    let nodes = index_nodes(nodes, control, &mut work)?;
    control.check_output_rows(nodes.ordered.len())?;
    let neighbors = simple_neighbors(edges, &nodes.positions, control, &mut work)?;
    let mut colors = vec![None; nodes.ordered.len()];
    let mut output = Vec::with_capacity(nodes.ordered.len());

    for node in 0..nodes.ordered.len() {
        checkpoint(control, &mut work)?;
        let mut used = BTreeSet::new();
        for &neighbor in &neighbors[node] {
            checkpoint(control, &mut work)?;
            if let Some(color) = colors[neighbor] {
                used.insert(color);
            }
        }
        let mut color = 0_usize;
        while used.contains(&color) {
            checkpoint(control, &mut work)?;
            color = next_color(color)?;
        }
        colors[node] = Some(color);
        output.push(NodeColor {
            node: nodes.ordered[node],
            color: u64::try_from(color)
                .map_err(|_| execution("node_coloring color exceeds UInt64 range"))?,
        });
    }
    Ok(output)
}

struct IndexedNodes {
    ordered: Vec<[u8; 16]>,
    positions: BTreeMap<[u8; 16], usize>,
}

fn index_nodes(
    nodes: &[[u8; 16]],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<IndexedNodes, AlgorithmError> {
    let mut ordered = nodes.to_vec();
    ordered.sort_unstable();
    let mut positions = BTreeMap::new();
    for (position, &uuid) in ordered.iter().enumerate() {
        checkpoint(control, work)?;
        if positions.insert(uuid, position).is_some() {
            return Err(execution("node_coloring node UUIDs must be unique"));
        }
    }
    Ok(IndexedNodes { ordered, positions })
}

fn simple_neighbors(
    edges: &[NodeColoringEdge],
    node_index: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<BTreeSet<usize>>, AlgorithmError> {
    let mut stored = BTreeMap::new();
    for &raw in edges {
        checkpoint(control, work)?;
        let edge = raw.canonical();
        let Some(&source) = node_index.get(&edge.source) else {
            return Err(execution(
                "node_coloring edge endpoint is outside node selection",
            ));
        };
        let Some(&target) = node_index.get(&edge.target) else {
            return Err(execution(
                "node_coloring edge endpoint is outside node selection",
            ));
        };
        if source == target {
            return Err(execution(
                "node_coloring cannot color a graph containing a self-loop",
            ));
        }
        if let Some(previous) = stored.insert(edge.edge, edge)
            && previous != edge
        {
            return Err(execution(
                "node_coloring edge UUID has inconsistent adjacency entries",
            ));
        }
    }

    let mut neighbors = vec![BTreeSet::new(); node_index.len()];
    for edge in stored.into_values() {
        checkpoint(control, work)?;
        let source = node_index[&edge.source];
        let target = node_index[&edge.target];
        neighbors[source].insert(target);
        neighbors[target].insert(source);
    }
    Ok(neighbors)
}

fn next_color(color: usize) -> Result<usize, AlgorithmError> {
    color
        .checked_add(1)
        .ok_or_else(|| execution("node_coloring color exceeds platform range"))
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

    fn edge(id: u8, source: u8, target: u8) -> NodeColoringEdge {
        NodeColoringEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn values(colors: Vec<NodeColor>) -> Vec<(u8, u64)> {
        colors
            .into_iter()
            .map(|color| (color.node[0], color.color))
            .collect()
    }

    #[test]
    fn assigns_smallest_available_colors_in_uuid_order() {
        let nodes = [uuid(4), uuid(0), uuid(3), uuid(2), uuid(1), uuid(8)];
        let edges = [
            edge(10, 0, 1),
            edge(11, 1, 2),
            edge(12, 2, 0),
            edge(13, 1, 3),
            edge(14, 0, 4),
            edge(15, 1, 4),
        ];

        assert_eq!(
            values(greedy_node_coloring(&nodes, &edges, &control()).unwrap()),
            [(0, 0), (1, 1), (2, 2), (3, 0), (4, 2), (8, 0)]
        );

        let mut reversed_edges = edges;
        reversed_edges.reverse();
        assert_eq!(
            values(greedy_node_coloring(&nodes, &reversed_edges, &control()).unwrap()),
            [(0, 0), (1, 1), (2, 2), (3, 0), (4, 2), (8, 0)]
        );
    }

    #[test]
    fn covers_empty_disconnected_and_isolated_nodes() {
        assert!(
            greedy_node_coloring(&[], &[], &control())
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            values(
                greedy_node_coloring(
                    &[uuid(5), uuid(1), uuid(9), uuid(2)],
                    &[edge(1, 1, 2)],
                    &control()
                )
                .unwrap()
            ),
            [(1, 0), (2, 1), (5, 0), (9, 0)]
        );
    }

    #[test]
    fn collapses_mirrors_parallel_and_reciprocal_edges() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        let edges = [
            edge(10, 0, 1),
            edge(10, 1, 0),
            edge(11, 0, 1),
            edge(12, 1, 0),
            edge(13, 1, 2),
        ];
        assert_eq!(
            values(greedy_node_coloring(&nodes, &edges, &control()).unwrap()),
            [(0, 0), (1, 1), (2, 0)]
        );
    }

    #[test]
    fn rejects_self_loops_and_invalid_identity_atomically() {
        for result in [
            greedy_node_coloring(&[uuid(0)], &[edge(1, 0, 0)], &control()),
            greedy_node_coloring(&[uuid(0), uuid(0)], &[], &control()),
            greedy_node_coloring(&[uuid(0)], &[edge(1, 0, 2)], &control()),
            greedy_node_coloring(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control(),
            ),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn uses_checked_colors_and_shared_controls() {
        assert!(matches!(
            next_color(usize::MAX),
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
            greedy_node_coloring(&[uuid(0)], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            greedy_node_coloring(
                &[],
                &[],
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
            greedy_node_coloring(&[], &[], &no_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }
}
