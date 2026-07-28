use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

/// One stored edge entry in the selected public-identity projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EdgeColoringEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

impl EdgeColoringEdge {
    fn canonical(mut self) -> Self {
        if self.target < self.source {
            std::mem::swap(&mut self.source, &mut self.target);
        }
        self
    }
}

/// One deterministic public edge-color assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EdgeColor {
    pub edge: [u8; 16],
    pub color: u64,
}

/// Greedily color the selected undirected multigraph in ascending edge UUID order.
pub(crate) fn greedy_edge_coloring(
    nodes: &[[u8; 16]],
    edges: &[EdgeColoringEdge],
    control: &AlgorithmControl,
) -> Result<Vec<EdgeColor>, AlgorithmError> {
    control.checkpoint()?;

    let mut work = 0_usize;
    let nodes = index_nodes(nodes, control, &mut work)?;
    let edges = index_edges(edges, &nodes, control, &mut work)?;
    control.check_output_rows(edges.len())?;

    let mut incident_colors = vec![BTreeSet::new(); nodes.len()];
    let mut output = Vec::with_capacity(edges.len());
    for edge in edges.into_values() {
        checkpoint(control, &mut work)?;
        let source = nodes[&edge.source];
        let target = nodes[&edge.target];
        let mut color = 0_usize;
        while incident_colors[source].contains(&color) || incident_colors[target].contains(&color) {
            checkpoint(control, &mut work)?;
            color = next_color(color)?;
        }
        incident_colors[source].insert(color);
        incident_colors[target].insert(color);
        output.push(EdgeColor {
            edge: edge.edge,
            color: u64::try_from(color)
                .map_err(|_| execution("edge_coloring color exceeds UInt64 range"))?,
        });
    }
    Ok(output)
}

fn index_nodes(
    nodes: &[[u8; 16]],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<BTreeMap<[u8; 16], usize>, AlgorithmError> {
    let mut ordered = nodes.to_vec();
    ordered.sort_unstable();
    let mut positions = BTreeMap::new();
    for (position, uuid) in ordered.into_iter().enumerate() {
        checkpoint(control, work)?;
        if positions.insert(uuid, position).is_some() {
            return Err(execution("edge_coloring node UUIDs must be unique"));
        }
    }
    Ok(positions)
}

fn index_edges(
    edges: &[EdgeColoringEdge],
    nodes: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<BTreeMap<[u8; 16], EdgeColoringEdge>, AlgorithmError> {
    let mut stored = BTreeMap::new();
    for &raw in edges {
        checkpoint(control, work)?;
        let edge = raw.canonical();
        let Some(&source) = nodes.get(&edge.source) else {
            return Err(execution(
                "edge_coloring edge endpoint is outside node selection",
            ));
        };
        let Some(&target) = nodes.get(&edge.target) else {
            return Err(execution(
                "edge_coloring edge endpoint is outside node selection",
            ));
        };
        if source == target {
            return Err(execution(
                "edge_coloring cannot color a graph containing a self-loop",
            ));
        }
        if let Some(previous) = stored.insert(edge.edge, edge)
            && previous != edge
        {
            return Err(execution(
                "edge_coloring edge UUID has inconsistent adjacency entries",
            ));
        }
    }
    Ok(stored)
}

fn next_color(color: usize) -> Result<usize, AlgorithmError> {
    color
        .checked_add(1)
        .ok_or_else(|| execution("edge_coloring color exceeds platform range"))
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

    fn edge(id: u8, source: u8, target: u8) -> EdgeColoringEdge {
        EdgeColoringEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn values(colors: Vec<EdgeColor>) -> Vec<(u8, u64)> {
        colors
            .into_iter()
            .map(|color| (color.edge[0], color.color))
            .collect()
    }

    #[test]
    fn assigns_smallest_available_colors_in_edge_uuid_order() {
        let nodes = [uuid(4), uuid(0), uuid(3), uuid(2), uuid(1)];
        let edges = [
            edge(15, 1, 4),
            edge(10, 0, 1),
            edge(13, 1, 3),
            edge(12, 2, 0),
            edge(14, 0, 4),
            edge(11, 1, 2),
            edge(16, 0, 1),
        ];

        assert_eq!(
            values(greedy_edge_coloring(&nodes, &edges, &control()).unwrap()),
            [
                (10, 0),
                (11, 1),
                (12, 2),
                (13, 2),
                (14, 1),
                (15, 3),
                (16, 4),
            ]
        );
    }

    #[test]
    fn collapses_mirrors_but_keeps_parallel_and_reciprocal_edge_identities() {
        let nodes = [uuid(0), uuid(1)];
        let edges = [
            edge(10, 0, 1),
            edge(10, 1, 0),
            edge(11, 0, 1),
            edge(12, 1, 0),
        ];

        assert_eq!(
            values(greedy_edge_coloring(&nodes, &edges, &control()).unwrap()),
            [(10, 0), (11, 1), (12, 2)]
        );
    }

    #[test]
    fn covers_empty_disconnected_and_isolated_nodes() {
        assert!(
            greedy_edge_coloring(&[], &[], &control())
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            values(
                greedy_edge_coloring(
                    &[uuid(0), uuid(1), uuid(2), uuid(3), uuid(9)],
                    &[edge(2, 2, 3), edge(1, 0, 1)],
                    &control(),
                )
                .unwrap()
            ),
            [(1, 0), (2, 0)]
        );
    }

    #[test]
    fn rejects_self_loops_and_invalid_identity_atomically() {
        for result in [
            greedy_edge_coloring(&[uuid(0)], &[edge(1, 0, 0)], &control()),
            greedy_edge_coloring(&[uuid(0), uuid(0)], &[], &control()),
            greedy_edge_coloring(&[uuid(0)], &[edge(1, 0, 2)], &control()),
            greedy_edge_coloring(
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
            greedy_edge_coloring(&[uuid(0), uuid(1)], &[edge(1, 0, 1)], &no_output,),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            greedy_edge_coloring(
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
            greedy_edge_coloring(&[], &[], &no_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }
}
