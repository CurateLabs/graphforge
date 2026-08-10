//! Deterministic Gomory-Hu forests over undirected capacity multigraphs.

//! The Gomory-Hu tree path remains serial (#544). Each component-local
//! min-cut updates parent links that determine later source/sink pairs, so cut
//! calls are ordered state transitions rather than independent tasks.

use std::collections::VecDeque;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_paths_max_flow::CapacityEdge;
use crate::algorithm_paths_min_cut::minimum_cut_unshaped;

/// One synthetic edge in a canonical Gomory-Hu forest.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct GomoryHuEdge {
    pub source_uuid: [u8; 16],
    pub target_uuid: [u8; 16],
    pub cut_value: f64,
}

/// Compute the version-pinned classic Gomory-Hu parent-update forest.
pub(crate) fn gomory_hu_forest(
    nodes: &[[u8; 16]],
    edges: &[CapacityEdge],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<Vec<GomoryHuEdge>, AlgorithmError> {
    let (nodes, edges) = validate_projection(nodes, edges, directed, control)?;
    let components = connected_components(&nodes, &edges, control)?;
    let expected_rows = nodes.len().saturating_sub(components.len());
    control.check_output_rows(expected_rows)?;

    let mut forest = reserved_vec(expected_rows, "Gomory-Hu forest rows")?;
    for component in components {
        control.check_cancelled()?;
        if component.len() < 2 {
            continue;
        }
        let mut component_edges = reserved_vec(edges.len(), "Gomory-Hu component edges")?;
        component_edges.extend(edges.iter().copied().filter(|edge| {
            edge.source_uuid != edge.target_uuid
                && component.binary_search(&edge.source_uuid).is_ok()
                && component.binary_search(&edge.target_uuid).is_ok()
        }));
        forest.extend(component_forest(&component, &component_edges, control)?);
    }
    forest.sort_unstable_by(|left, right| {
        left.source_uuid
            .cmp(&right.source_uuid)
            .then_with(|| left.target_uuid.cmp(&right.target_uuid))
            .then_with(|| left.cut_value.total_cmp(&right.cut_value))
    });
    control.check_output_rows(forest.len())?;
    Ok(forest)
}

fn validate_projection(
    nodes: &[[u8; 16]],
    edges: &[CapacityEdge],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<(Vec<[u8; 16]>, Vec<CapacityEdge>), AlgorithmError> {
    control.checkpoint()?;
    if directed {
        return Err(execution("Gomory-Hu requires an undirected graph"));
    }

    let mut ordered_nodes = reserved_vec(nodes.len(), "Gomory-Hu nodes")?;
    ordered_nodes.extend_from_slice(nodes);
    let mut nodes = ordered_nodes;
    nodes.sort_unstable();
    if nodes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(execution("Gomory-Hu node UUIDs must be unique"));
    }
    let mut ordered_edges = reserved_vec(edges.len(), "Gomory-Hu edges")?;
    for &edge in edges {
        control.check_cancelled()?;
        if !edge.capacity.is_finite() || edge.capacity < 0.0 {
            return Err(execution(
                "Gomory-Hu requires finite nonnegative capacities",
            ));
        }
        if nodes.binary_search(&edge.source_uuid).is_err()
            || nodes.binary_search(&edge.target_uuid).is_err()
        {
            return Err(execution(
                "Gomory-Hu edge endpoint is outside node selection",
            ));
        }
        ordered_edges.push(edge);
    }
    ordered_edges.sort_unstable_by_key(|edge| edge.edge_uuid);
    if ordered_edges
        .windows(2)
        .any(|pair| pair[0].edge_uuid == pair[1].edge_uuid)
    {
        return Err(execution("Gomory-Hu edge UUIDs must be unique"));
    }
    let adjacency_entries = checked_adjacency_entries(edges.len())?;
    control.check_graph_size(nodes.len(), adjacency_entries)?;
    Ok((nodes, ordered_edges))
}

fn connected_components(
    nodes: &[[u8; 16]],
    edges: &[CapacityEdge],
    control: &AlgorithmControl,
) -> Result<Vec<Vec<[u8; 16]>>, AlgorithmError> {
    let mut degrees = reserved_vec(nodes.len(), "Gomory-Hu adjacency degrees")?;
    degrees.resize(nodes.len(), 0_usize);
    for edge in edges {
        control.check_cancelled()?;
        if edge.source_uuid == edge.target_uuid {
            continue;
        }
        let source = nodes
            .binary_search(&edge.source_uuid)
            .expect("validated endpoint");
        let target = nodes
            .binary_search(&edge.target_uuid)
            .expect("validated endpoint");
        degrees[source] = degrees[source]
            .checked_add(1)
            .ok_or_else(|| allocation("Gomory-Hu adjacency degree"))?;
        degrees[target] = degrees[target]
            .checked_add(1)
            .ok_or_else(|| allocation("Gomory-Hu adjacency degree"))?;
    }

    let mut adjacency = reserved_vec(nodes.len(), "Gomory-Hu adjacency")?;
    for degree in degrees {
        adjacency.push(reserved_vec(degree, "Gomory-Hu adjacency entries")?);
    }
    for edge in edges {
        if edge.source_uuid == edge.target_uuid {
            continue;
        }
        let source = nodes
            .binary_search(&edge.source_uuid)
            .expect("validated endpoint");
        let target = nodes
            .binary_search(&edge.target_uuid)
            .expect("validated endpoint");
        adjacency[source].push(target);
        adjacency[target].push(source);
    }
    for neighbors in &mut adjacency {
        neighbors.sort_unstable();
        neighbors.dedup();
    }

    let mut unseen = reserved_vec(nodes.len(), "Gomory-Hu component state")?;
    unseen.resize(nodes.len(), true);
    let mut components = reserved_vec(nodes.len(), "Gomory-Hu components")?;
    for root in 0..nodes.len() {
        if !unseen[root] {
            continue;
        }
        control.checkpoint()?;
        unseen[root] = false;
        let mut queue = VecDeque::new();
        try_reserve_queue(&mut queue, nodes.len(), "Gomory-Hu component queue")?;
        queue.push_back(root);
        let mut component = Vec::new();
        while let Some(node_index) = queue.pop_front() {
            control.check_cancelled()?;
            try_push(
                &mut component,
                nodes[node_index],
                "Gomory-Hu component nodes",
            )?;
            for &neighbor in &adjacency[node_index] {
                if unseen[neighbor] {
                    unseen[neighbor] = false;
                    queue.push_back(neighbor);
                }
            }
        }
        component.sort_unstable();
        components.push(component);
    }
    Ok(components)
}

fn component_forest(
    nodes: &[[u8; 16]],
    edges: &[CapacityEdge],
    control: &AlgorithmControl,
) -> Result<Vec<GomoryHuEdge>, AlgorithmError> {
    let mut parent = reserved_vec(nodes.len(), "Gomory-Hu parents")?;
    parent.resize(nodes.len(), 0_usize);
    let mut cut = reserved_vec(nodes.len(), "Gomory-Hu cut values")?;
    cut.resize(nodes.len(), 0.0);
    for source in 1..nodes.len() {
        control.check_cancelled()?;
        let target = parent[source];
        let solution =
            minimum_cut_unshaped(nodes, edges, nodes[source], nodes[target], false, control)?;
        let source_side = solution.source_side;

        for candidate in (source + 1)..nodes.len() {
            if parent[candidate] == target && source_side.binary_search(&nodes[candidate]).is_ok() {
                parent[candidate] = source;
            }
        }
        if target != 0 && source_side.binary_search(&nodes[parent[target]]).is_ok() {
            parent[source] = parent[target];
            parent[target] = source;
            cut[source] = cut[target];
            cut[target] = solution.value;
        } else {
            cut[source] = solution.value;
        }
    }

    let mut forest = reserved_vec(nodes.len().saturating_sub(1), "Gomory-Hu component rows")?;
    for node in 1..nodes.len() {
        let (source_uuid, target_uuid) = if nodes[node] < nodes[parent[node]] {
            (nodes[node], nodes[parent[node]])
        } else {
            (nodes[parent[node]], nodes[node])
        };
        forest.push(GomoryHuEdge {
            source_uuid,
            target_uuid,
            cut_value: cut[node],
        });
    }
    Ok(forest)
}

fn reserved_vec<T>(capacity: usize, context: &str) -> Result<Vec<T>, AlgorithmError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| allocation(context))?;
    Ok(values)
}

fn try_push<T>(values: &mut Vec<T>, value: T, context: &str) -> Result<(), AlgorithmError> {
    if values.len() == values.capacity() {
        values.try_reserve(1).map_err(|_| allocation(context))?;
    }
    values.push(value);
    Ok(())
}

fn try_reserve_queue<T>(
    queue: &mut VecDeque<T>,
    capacity: usize,
    context: &str,
) -> Result<(), AlgorithmError> {
    queue.try_reserve(capacity).map_err(|_| allocation(context))
}

fn checked_adjacency_entries(edges: usize) -> Result<u64, AlgorithmError> {
    u64::try_from(edges)
        .map_err(|_| execution("Gomory-Hu adjacency entry count overflow"))
        .and_then(checked_doubled_entries)
}

fn checked_doubled_entries(edges: u64) -> Result<u64, AlgorithmError> {
    edges
        .checked_mul(2)
        .ok_or_else(|| execution("Gomory-Hu adjacency entry count overflow"))
}

fn allocation(context: &str) -> AlgorithmError {
    execution(format!("{context} allocation exceeds available memory"))
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};
    use crate::algorithm_paths_min_cut::minimum_cut;

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn edge(id: u8, source: u8, target: u8, capacity: f64) -> CapacityEdge {
        CapacityEdge {
            edge_uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
            capacity,
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn tree_cut(forest: &[GomoryHuEdge], source: [u8; 16], target: [u8; 16]) -> f64 {
        let mut adjacency = BTreeMap::<_, Vec<_>>::new();
        for edge in forest {
            adjacency
                .entry(edge.source_uuid)
                .or_default()
                .push((edge.target_uuid, edge.cut_value));
            adjacency
                .entry(edge.target_uuid)
                .or_default()
                .push((edge.source_uuid, edge.cut_value));
        }
        let mut queue = VecDeque::from([(source, f64::INFINITY)]);
        let mut seen = BTreeSet::from([source]);
        while let Some((node, value)) = queue.pop_front() {
            if node == target {
                return value;
            }
            for &(neighbor, cut) in &adjacency[&node] {
                if seen.insert(neighbor) {
                    queue.push_back((neighbor, value.min(cut)));
                }
            }
        }
        panic!("tree endpoints must be connected");
    }

    #[test]
    fn forest_is_pairwise_cut_equivalent_and_deterministic() {
        let nodes = [uuid(4), uuid(1), uuid(3), uuid(2)];
        let edges = [
            edge(14, 3, 4, 4.0),
            edge(10, 1, 2, 3.0),
            edge(13, 2, 4, 2.0),
            edge(12, 2, 3, 1.0),
            edge(11, 1, 3, 2.0),
        ];
        let expected = gomory_hu_forest(&nodes, &edges, false, &control()).unwrap();
        assert_eq!(expected.len(), nodes.len() - 1);
        assert!(expected.windows(2).all(|pair| {
            (pair[0].source_uuid, pair[0].target_uuid) < (pair[1].source_uuid, pair[1].target_uuid)
        }));

        let ordered = [uuid(1), uuid(2), uuid(3), uuid(4)];
        for source in 0..ordered.len() {
            for target in (source + 1)..ordered.len() {
                let cut = minimum_cut(
                    &ordered,
                    &edges,
                    ordered[source],
                    ordered[target],
                    false,
                    &control(),
                )
                .unwrap();
                assert_eq!(
                    tree_cut(&expected, ordered[source], ordered[target]),
                    cut.value
                );
            }
        }

        let mut reversed_edges = edges;
        reversed_edges.reverse();
        assert_eq!(
            gomory_hu_forest(
                &[uuid(2), uuid(4), uuid(1), uuid(3)],
                &reversed_edges,
                false,
                &control(),
            )
            .unwrap(),
            expected
        );
    }

    #[test]
    fn tied_cuts_follow_canonical_parent_updates() {
        let forest = gomory_hu_forest(
            &[uuid(4), uuid(3), uuid(2), uuid(1)],
            &[
                edge(10, 1, 2, 1.0),
                edge(11, 2, 3, 1.0),
                edge(12, 3, 4, 1.0),
                edge(13, 4, 1, 1.0),
            ],
            false,
            &control(),
        )
        .unwrap();
        assert_eq!(
            forest,
            [
                GomoryHuEdge {
                    source_uuid: uuid(1),
                    target_uuid: uuid(2),
                    cut_value: 2.0,
                },
                GomoryHuEdge {
                    source_uuid: uuid(1),
                    target_uuid: uuid(3),
                    cut_value: 2.0,
                },
                GomoryHuEdge {
                    source_uuid: uuid(1),
                    target_uuid: uuid(4),
                    cut_value: 2.0,
                },
            ]
        );
    }

    #[test]
    fn disconnected_projection_returns_canonical_forest() {
        let forest = gomory_hu_forest(
            &[uuid(7), uuid(6), uuid(4), uuid(3), uuid(2), uuid(1)],
            &[
                edge(10, 1, 2, 1.0),
                edge(11, 2, 3, 2.0),
                edge(12, 4, 6, 4.0),
                edge(13, 6, 7, 3.0),
            ],
            false,
            &control(),
        )
        .unwrap();
        assert_eq!(forest.len(), 4);
        assert_eq!(
            forest
                .iter()
                .map(|edge| (edge.source_uuid, edge.target_uuid, edge.cut_value))
                .collect::<Vec<_>>(),
            [
                (uuid(1), uuid(2), 1.0),
                (uuid(2), uuid(3), 2.0),
                (uuid(4), uuid(6), 4.0),
                (uuid(6), uuid(7), 3.0),
            ]
        );
        assert!(
            gomory_hu_forest(&[], &[], false, &control())
                .unwrap()
                .is_empty()
        );
        assert!(
            gomory_hu_forest(&[uuid(1)], &[], false, &control())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn loops_parallel_reciprocal_and_zero_capacity_records_are_exact() {
        let forest = gomory_hu_forest(
            &[uuid(3), uuid(2), uuid(1)],
            &[
                edge(10, 1, 2, 1.0),
                edge(11, 1, 2, 2.0),
                edge(12, 2, 1, 3.0),
                edge(13, 2, 3, 4.0),
                edge(14, 1, 1, f64::MAX),
                edge(15, 1, 3, 0.0),
            ],
            false,
            &control(),
        )
        .unwrap();
        assert_eq!(tree_cut(&forest, uuid(1), uuid(2)), 6.0);
        assert_eq!(tree_cut(&forest, uuid(1), uuid(3)), 4.0);
        assert_eq!(tree_cut(&forest, uuid(2), uuid(3)), 4.0);
    }

    #[test]
    fn internal_cut_edges_do_not_consume_public_forest_row_budget() {
        let control = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        let forest = gomory_hu_forest(
            &[uuid(1), uuid(2)],
            &[
                edge(10, 1, 2, 1.0),
                edge(11, 1, 2, 2.0),
                edge(12, 2, 1, 3.0),
            ],
            false,
            &control,
        )
        .unwrap();
        assert_eq!(
            forest,
            [GomoryHuEdge {
                source_uuid: uuid(1),
                target_uuid: uuid(2),
                cut_value: 6.0,
            }]
        );
    }

    #[test]
    fn repeated_cuts_share_one_nonzero_iteration_budget() {
        let nodes = [uuid(1), uuid(2), uuid(3)];
        let edges = [edge(10, 1, 2, 1.0), edge(11, 2, 3, 1.0)];
        let bounded_control = |iterations| {
            AlgorithmControl::new(
                AlgorithmLimits {
                    iterations,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            )
        };
        assert!(
            minimum_cut(
                &nodes,
                &edges,
                uuid(2),
                uuid(1),
                false,
                &bounded_control(10)
            )
            .is_ok()
        );
        let limit = 12; // one cut plus projection and component checkpoints
        assert!(matches!(
            gomory_hu_forest(
                &nodes,
                &edges,
                false,
                &bounded_control(limit),
            ),
            Err(AlgorithmError::IterationLimit {
                observed,
                limit: reported,
            }) if observed > reported && reported == limit
        ));
    }

    #[test]
    fn impossible_transient_reservations_are_structured_errors() {
        assert!(matches!(
            reserved_vec::<u8>(usize::MAX, "test vector"),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut queue = VecDeque::<u8>::new();
        assert!(matches!(
            try_reserve_queue(&mut queue, usize::MAX, "test queue"),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            checked_doubled_entries(u64::MAX),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn malformed_projection_and_nonfinite_accumulation_are_atomic() {
        assert!(gomory_hu_forest(&[uuid(1), uuid(2)], &[], true, &control()).is_err());
        assert!(gomory_hu_forest(&[uuid(1), uuid(1)], &[], false, &control()).is_err());
        assert!(
            gomory_hu_forest(
                &[uuid(1), uuid(2)],
                &[edge(10, 1, 3, 1.0)],
                false,
                &control(),
            )
            .is_err()
        );
        assert!(
            gomory_hu_forest(
                &[uuid(1), uuid(2)],
                &[edge(10, 1, 2, f64::NAN)],
                false,
                &control(),
            )
            .is_err()
        );
        assert!(
            gomory_hu_forest(
                &[uuid(1), uuid(2)],
                &[edge(10, 1, 2, -1.0)],
                false,
                &control(),
            )
            .is_err()
        );
        assert!(
            gomory_hu_forest(
                &[uuid(1), uuid(2)],
                &[edge(10, 1, 2, f64::MAX), edge(11, 2, 1, f64::MAX),],
                false,
                &control(),
            )
            .is_err()
        );
    }

    #[test]
    fn cancellation_and_shared_limits_fail_without_partial_forest() {
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            gomory_hu_forest(
                &[uuid(1), uuid(2)],
                &[edge(10, 1, 2, 1.0)],
                false,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );

        for limits in [
            AlgorithmLimits {
                nodes: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                edges: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
        ] {
            assert!(
                gomory_hu_forest(
                    &[uuid(1), uuid(2)],
                    &[edge(10, 1, 2, 1.0)],
                    false,
                    &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
                )
                .is_err()
            );
        }
    }
}
