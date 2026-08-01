use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_analyze_node_coloring::{NodeColor, NodeColoringEdge};
use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

/// Color a simple undirected graph in descending-degree, ascending-UUID order.
pub(crate) fn k1_coloring(
    nodes: &[[u8; 16]],
    edges: &[NodeColoringEdge],
    control: &AlgorithmControl,
) -> Result<Vec<NodeColor>, AlgorithmError> {
    control.checkpoint()?;
    control.check_graph_size(nodes.len(), u64::try_from(edges.len()).unwrap_or(u64::MAX))?;
    control.check_output_rows(nodes.len())?;

    let mut work = 0_usize;
    let mut ordered = nodes.to_vec();
    ordered.sort_unstable();
    let mut positions = BTreeMap::new();
    for (position, &node) in ordered.iter().enumerate() {
        checkpoint(control, &mut work)?;
        if positions.insert(node, position).is_some() {
            return Err(execution("k1_coloring node UUIDs must be unique"));
        }
    }

    let neighbors = simple_neighbors(edges, &positions, control, &mut work)?;
    let mut coloring_order = (0..ordered.len()).collect::<Vec<_>>();
    coloring_order.sort_unstable_by(|&left, &right| {
        neighbors[right]
            .len()
            .cmp(&neighbors[left].len())
            .then_with(|| ordered[left].cmp(&ordered[right]))
    });

    let mut colors = vec![None; ordered.len()];
    for node in coloring_order {
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
            color = color
                .checked_add(1)
                .ok_or_else(|| execution("k1_coloring color exceeds platform range"))?;
        }
        colors[node] = Some(color);
    }

    let mut canonical = BTreeMap::new();
    let mut output = Vec::with_capacity(ordered.len());
    for (position, raw_color) in colors.into_iter().enumerate() {
        checkpoint(control, &mut work)?;
        let raw_color = raw_color.ok_or_else(|| execution("k1_coloring left a node uncolored"))?;
        let color = if neighbors[position].is_empty() {
            0
        } else {
            let next = canonical.len();
            *canonical.entry(raw_color).or_insert(next)
        };
        output.push(NodeColor {
            node: ordered[position],
            color: u64::try_from(color)
                .map_err(|_| execution("k1_coloring color exceeds UInt64 range"))?,
        });
    }
    Ok(output)
}

fn simple_neighbors(
    edges: &[NodeColoringEdge],
    positions: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<BTreeSet<usize>>, AlgorithmError> {
    let mut stored = BTreeMap::new();
    for &raw in edges {
        checkpoint(control, work)?;
        let (source_uuid, target_uuid) = if raw.source <= raw.target {
            (raw.source, raw.target)
        } else {
            (raw.target, raw.source)
        };
        let Some(&source) = positions.get(&source_uuid) else {
            return Err(execution(
                "k1_coloring edge endpoint is outside node selection",
            ));
        };
        let Some(&target) = positions.get(&target_uuid) else {
            return Err(execution(
                "k1_coloring edge endpoint is outside node selection",
            ));
        };
        if source == target {
            return Err(execution(
                "k1_coloring cannot color a graph containing a self-loop",
            ));
        }
        let canonical = NodeColoringEdge {
            edge: raw.edge,
            source: source_uuid,
            target: target_uuid,
        };
        if let Some(previous) = stored.insert(raw.edge, canonical)
            && previous != canonical
        {
            return Err(execution(
                "k1_coloring edge UUID has inconsistent adjacency entries",
            ));
        }
    }

    let mut neighbors = vec![BTreeSet::new(); positions.len()];
    for edge in stored.into_values() {
        checkpoint(control, work)?;
        let source = positions[&edge.source];
        let target = positions[&edge.target];
        neighbors[source].insert(target);
        neighbors[target].insert(source);
    }
    Ok(neighbors)
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work
        .checked_add(1)
        .ok_or_else(|| execution("k1_coloring work counter overflow"))?;
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
    fn uses_degree_uuid_order_lowest_color_and_canonical_labels() {
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
            values(k1_coloring(&nodes, &edges, &control()).unwrap()),
            [(0, 0), (1, 1), (2, 2), (3, 0), (4, 2), (8, 0)]
        );

        let mut nodes_permuted = nodes;
        nodes_permuted.reverse();
        let mut edges_permuted = edges;
        edges_permuted.reverse();
        assert_eq!(
            values(k1_coloring(&nodes_permuted, &edges_permuted, &control()).unwrap()),
            [(0, 0), (1, 1), (2, 2), (3, 0), (4, 2), (8, 0)]
        );
    }

    #[test]
    fn every_mixed_graph_isolate_remains_zero_after_canonical_renumbering() {
        let nodes = [
            uuid(9),
            uuid(4),
            uuid(0),
            uuid(3),
            uuid(2),
            uuid(1),
            uuid(8),
        ];
        let edges = [
            edge(10, 0, 1),
            edge(11, 1, 2),
            edge(12, 2, 0),
            edge(13, 1, 3),
            edge(14, 0, 4),
            edge(15, 1, 4),
        ];
        assert_eq!(
            values(k1_coloring(&nodes, &edges, &control()).unwrap()),
            [(0, 0), (1, 1), (2, 2), (3, 0), (4, 2), (8, 0), (9, 0)]
        );

        let mut permuted_nodes = nodes;
        permuted_nodes.rotate_left(3);
        let mut permuted_edges = edges;
        permuted_edges.reverse();
        assert_eq!(
            values(k1_coloring(&permuted_nodes, &permuted_edges, &control()).unwrap()),
            [(0, 0), (1, 1), (2, 2), (3, 0), (4, 2), (8, 0), (9, 0)]
        );
    }

    #[test]
    fn resolves_degree_ties_by_uuid() {
        assert_eq!(
            values(
                k1_coloring(
                    &[uuid(3), uuid(2), uuid(1), uuid(0)],
                    &[edge(10, 0, 1), edge(11, 0, 2), edge(12, 1, 3)],
                    &control()
                )
                .unwrap()
            ),
            [(0, 0), (1, 1), (2, 1), (3, 0)]
        );
    }

    #[test]
    fn covers_empty_disconnected_and_isolated_nodes() {
        assert!(k1_coloring(&[], &[], &control()).unwrap().is_empty());
        assert_eq!(
            values(
                k1_coloring(
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
    fn collapses_parallel_reciprocal_edges_and_rejects_self_loops() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        assert_eq!(
            values(
                k1_coloring(
                    &nodes,
                    &[
                        edge(10, 0, 1),
                        edge(10, 1, 0),
                        edge(11, 0, 1),
                        edge(12, 1, 0),
                        edge(13, 1, 2),
                    ],
                    &control()
                )
                .unwrap()
            ),
            [(0, 0), (1, 1), (2, 0)]
        );
        assert!(matches!(
            k1_coloring(&[uuid(0)], &[edge(1, 0, 0)], &control()),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn rejects_invalid_identity_atomically() {
        for result in [
            k1_coloring(&[uuid(0), uuid(0)], &[], &control()),
            k1_coloring(&[uuid(0)], &[edge(1, 0, 2)], &control()),
            k1_coloring(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control(),
            ),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn honors_cancellation_and_resource_limits() {
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            k1_coloring(
                &[],
                &[],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );

        for (limits, expected) in [
            (
                AlgorithmLimits {
                    nodes: 0,
                    ..AlgorithmLimits::default()
                },
                "node",
            ),
            (
                AlgorithmLimits {
                    edges: 0,
                    ..AlgorithmLimits::default()
                },
                "edge",
            ),
            (
                AlgorithmLimits {
                    output_rows: 0,
                    ..AlgorithmLimits::default()
                },
                "output",
            ),
        ] {
            let result = k1_coloring(
                &[uuid(0), uuid(1)],
                &[edge(1, 0, 1)],
                &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
            );
            assert!(
                matches!(
                    (expected, result),
                    ("node", Err(AlgorithmError::NodeLimit { .. }))
                        | ("edge", Err(AlgorithmError::EdgeLimit { .. }))
                        | ("output", Err(AlgorithmError::OutputLimit { .. }))
                ),
                "{expected} limit must fail atomically"
            );
        }

        assert!(matches!(
            k1_coloring(
                &[],
                &[],
                &AlgorithmControl::new(
                    AlgorithmLimits {
                        iterations: 0,
                        ..AlgorithmLimits::default()
                    },
                    AlgorithmCancellation::default()
                )
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }
}
