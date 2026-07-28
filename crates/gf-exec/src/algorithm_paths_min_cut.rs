use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_paths_max_flow::{CapacityEdge, maximum_flow};

/// One canonical cut edge, retaining its stored identity and orientation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CutEdge {
    pub edge_uuid: [u8; 16],
    pub source_uuid: [u8; 16],
    pub target_uuid: [u8; 16],
    pub capacity: f64,
}

/// One validated solution shared by the scalar and per-edge minimum-cut views.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MinCutSolution {
    pub source_side: Vec<[u8; 16]>,
    pub value: f64,
    pub cut_edges: Vec<CutEdge>,
}

struct CutProblem<'a> {
    nodes: &'a [[u8; 16]],
    edges: &'a [CapacityEdge],
    source: [u8; 16],
    sink: [u8; 16],
    directed: bool,
    control: &'a AlgorithmControl,
}

/// Compute the canonical source-target minimum cut.
///
/// A single residual-reachable partition is not sufficient for GraphForge's
/// tie contract: it is only the inclusion-minimal source side. This kernel
/// instead uses constrained minimum-cut values as a feasibility oracle. Forced
/// source/sink memberships are enforced by contracting nodes into the
/// corresponding endpoint, avoiding artificial infinite capacities. Greedy
/// set construction then returns the lexicographically smallest sorted
/// source-side UUID set among all cuts with the minimum value.
pub(crate) fn minimum_cut(
    nodes: &[[u8; 16]],
    edges: &[CapacityEdge],
    source: [u8; 16],
    sink: [u8; 16],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<MinCutSolution, AlgorithmError> {
    let solution = minimum_cut_unshaped(nodes, edges, source, sink, directed, control)?;
    control.check_output_rows(solution.cut_edges.len().max(1))?;
    Ok(solution)
}

/// Compute the shared canonical cut without charging public result-row limits.
///
/// Composite kernels consume this partition and scalar internally, then
/// account only for their own canonical public rows.
pub(crate) fn minimum_cut_unshaped(
    nodes: &[[u8; 16]],
    edges: &[CapacityEdge],
    source: [u8; 16],
    sink: [u8; 16],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<MinCutSolution, AlgorithmError> {
    let (ordered_nodes, ordered_edges) =
        validate_projection(nodes, edges, source, sink, directed, control)?;
    let minimum_value = maximum_flow(
        &ordered_nodes,
        &ordered_edges,
        source,
        sink,
        directed,
        control,
    )?
    .value;
    let problem = CutProblem {
        nodes: &ordered_nodes,
        edges: &ordered_edges,
        source,
        sink,
        directed,
        control,
    };

    let mut forced_source = reserved_vec(ordered_nodes.len(), "minimum-cut source partition")?;
    forced_source.push(source);
    let mut forced_sink = reserved_vec(ordered_nodes.len(), "minimum-cut sink partition")?;
    forced_sink.push(sink);
    for (position, &node) in ordered_nodes.iter().enumerate() {
        control.check_cancelled()?;
        if contains(&forced_source, node) || contains(&forced_sink, node) {
            continue;
        }

        // Excluding this node is lexicographically smallest only when the
        // source-side vector can end here. Otherwise including this UUID beats
        // every feasible completion whose next UUID is larger.
        let end_source = clone_reserved(&forced_source, "minimum-cut source candidate")?;
        let mut end_sink = clone_reserved(&forced_sink, "minimum-cut sink candidate")?;
        for &remaining in &ordered_nodes[position..] {
            insert_sorted(&mut end_sink, remaining, "minimum-cut sink candidate")?;
        }
        if problem
            .constrained_value(&end_source, &end_sink)?
            .is_some_and(|value| same_value(value, minimum_value))
        {
            forced_source = end_source;
            break;
        }

        let mut include = clone_reserved(&forced_source, "minimum-cut source candidate")?;
        insert_sorted(&mut include, node, "minimum-cut source candidate")?;
        if problem
            .constrained_value(&include, &forced_sink)?
            .is_some_and(|value| same_value(value, minimum_value))
        {
            forced_source = include;
        } else {
            insert_sorted(&mut forced_sink, node, "minimum-cut sink partition")?;
        }
    }

    let mut cut_edges = reserved_vec(ordered_edges.len(), "minimum-cut result edges")?;
    for edge in &ordered_edges {
        let source_inside = contains(&forced_source, edge.source_uuid);
        let target_inside = contains(&forced_source, edge.target_uuid);
        let crosses = if directed {
            source_inside && !target_inside
        } else {
            source_inside != target_inside
        };
        if crosses {
            cut_edges.push(CutEdge {
                edge_uuid: edge.edge_uuid,
                source_uuid: edge.source_uuid,
                target_uuid: edge.target_uuid,
                capacity: edge.capacity,
            });
        }
    }
    cut_edges.sort_unstable_by_key(|edge| edge.edge_uuid);
    let value = checked_capacity_sum(cut_edges.iter().map(|edge| edge.capacity))?;
    if !same_value(value, minimum_value) {
        return Err(execution(
            "canonical minimum-cut capacity disagrees with maximum-flow value",
        ));
    }
    Ok(MinCutSolution {
        source_side: forced_source,
        value,
        cut_edges,
    })
}

fn validate_projection(
    nodes: &[[u8; 16]],
    edges: &[CapacityEdge],
    source: [u8; 16],
    sink: [u8; 16],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<(Vec<[u8; 16]>, Vec<CapacityEdge>), AlgorithmError> {
    control.checkpoint()?;
    if source == sink {
        return Err(execution("minimum cut requires distinct endpoints"));
    }
    let mut ordered_nodes = reserved_vec(nodes.len(), "minimum-cut nodes")?;
    ordered_nodes.extend_from_slice(nodes);
    ordered_nodes.sort_unstable();
    if ordered_nodes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(execution("minimum-cut node UUIDs must be unique"));
    }
    if !contains(&ordered_nodes, source) {
        return Err(execution("minimum-cut source is outside node selection"));
    }
    if !contains(&ordered_nodes, sink) {
        return Err(execution("minimum-cut target is outside node selection"));
    }

    let mut ordered_edges = reserved_vec(edges.len(), "minimum-cut edges")?;
    for &edge in edges {
        control.check_cancelled()?;
        if !edge.capacity.is_finite() || edge.capacity < 0.0 {
            return Err(execution(
                "minimum cut requires finite nonnegative capacities",
            ));
        }
        if !contains(&ordered_nodes, edge.source_uuid)
            || !contains(&ordered_nodes, edge.target_uuid)
        {
            return Err(execution(
                "minimum-cut edge endpoint is outside node selection",
            ));
        }
        ordered_edges.push(edge);
    }
    ordered_edges.sort_unstable_by_key(|edge| edge.edge_uuid);
    if ordered_edges
        .windows(2)
        .any(|pair| pair[0].edge_uuid == pair[1].edge_uuid)
    {
        return Err(execution("minimum-cut edge UUIDs must be unique"));
    }
    let directions = if directed { 1_u64 } else { 2 };
    let adjacency_entries = checked_adjacency_entries(edges.len(), directions)?;
    control.check_graph_size(ordered_nodes.len(), adjacency_entries)?;
    Ok((ordered_nodes, ordered_edges))
}

impl CutProblem<'_> {
    /// Return `None` when the requested memberships conflict.
    fn constrained_value(
        &self,
        forced_source: &[[u8; 16]],
        forced_sink: &[[u8; 16]],
    ) -> Result<Option<f64>, AlgorithmError> {
        self.control.checkpoint()?;
        if forced_source
            .iter()
            .any(|&node| contains(forced_sink, node))
            || !contains(forced_source, self.source)
            || !contains(forced_sink, self.sink)
        {
            return Ok(None);
        }
        let representative = |node: [u8; 16]| {
            if contains(forced_source, node) {
                self.source
            } else if contains(forced_sink, node) {
                self.sink
            } else {
                node
            }
        };
        let mut contracted_nodes = reserved_vec(self.nodes.len(), "minimum-cut contracted nodes")?;
        contracted_nodes.extend(self.nodes.iter().copied().map(representative));
        contracted_nodes.sort_unstable();
        contracted_nodes.dedup();
        let mut contracted_edges = reserved_vec(self.edges.len(), "minimum-cut contracted edges")?;
        contracted_edges.extend(self.edges.iter().map(|edge| CapacityEdge {
            source_uuid: representative(edge.source_uuid),
            target_uuid: representative(edge.target_uuid),
            ..*edge
        }));
        maximum_flow(
            &contracted_nodes,
            &contracted_edges,
            self.source,
            self.sink,
            self.directed,
            self.control,
        )
        .map(|solution| Some(solution.value))
    }
}

fn contains(values: &[[u8; 16]], value: [u8; 16]) -> bool {
    values.binary_search(&value).is_ok()
}

fn insert_sorted(
    values: &mut Vec<[u8; 16]>,
    value: [u8; 16],
    context: &str,
) -> Result<(), AlgorithmError> {
    match values.binary_search(&value) {
        Ok(_) => Ok(()),
        Err(position) => {
            if values.len() == values.capacity() {
                values.try_reserve(1).map_err(|_| allocation(context))?;
            }
            values.insert(position, value);
            Ok(())
        }
    }
}

fn clone_reserved<T: Clone>(values: &[T], context: &str) -> Result<Vec<T>, AlgorithmError> {
    let mut cloned = reserved_vec(values.len(), context)?;
    cloned.extend_from_slice(values);
    Ok(cloned)
}

fn reserved_vec<T>(capacity: usize, context: &str) -> Result<Vec<T>, AlgorithmError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| allocation(context))?;
    Ok(values)
}

fn checked_adjacency_entries(edges: usize, directions: u64) -> Result<u64, AlgorithmError> {
    u64::try_from(edges)
        .map_err(|_| execution("minimum-cut adjacency entry count overflow"))
        .and_then(|edges| checked_adjacency_product(edges, directions))
}

fn checked_adjacency_product(edges: u64, directions: u64) -> Result<u64, AlgorithmError> {
    edges
        .checked_mul(directions)
        .ok_or_else(|| execution("minimum-cut adjacency entry count overflow"))
}

fn allocation(context: &str) -> AlgorithmError {
    execution(format!("{context} allocation exceeds available memory"))
}

fn same_value(left: f64, right: f64) -> bool {
    left.total_cmp(&right).is_eq()
}

fn checked_capacity_sum(capacities: impl IntoIterator<Item = f64>) -> Result<f64, AlgorithmError> {
    capacities.into_iter().try_fold(0.0, |sum, capacity| {
        let next = sum + capacity;
        if next.is_finite() {
            Ok(next)
        } else {
            Err(execution("minimum-cut total is not finite"))
        }
    })
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

    #[test]
    fn canonical_source_side_is_not_an_arbitrary_residual_partition() {
        // Both {04} and {01,04} have value 1. Lexicographic UUID-vector
        // ordering requires {01,04}, unlike ordinary residual reachability.
        let solution = minimum_cut(
            &[uuid(5), uuid(4), uuid(1)],
            &[edge(11, 4, 1, 1.0), edge(10, 1, 5, 1.0)],
            uuid(4),
            uuid(5),
            true,
            &control(),
        )
        .unwrap();
        assert_eq!(solution.source_side, [uuid(1), uuid(4)]);
        assert_eq!(solution.value, 1.0);
        assert_eq!(
            solution.cut_edges,
            [CutEdge {
                edge_uuid: uuid(10),
                source_uuid: uuid(1),
                target_uuid: uuid(5),
                capacity: 1.0,
            }]
        );
    }

    #[test]
    fn undirected_cut_retains_stored_orientation_and_canonical_edge_order() {
        let edges = [
            edge(14, 5, 1, 2.0),
            edge(12, 4, 1, 2.0),
            edge(13, 1, 1, 99.0),
        ];
        let solution = minimum_cut(
            &[uuid(5), uuid(1), uuid(4)],
            &edges,
            uuid(4),
            uuid(5),
            false,
            &control(),
        )
        .unwrap();
        assert_eq!(solution.source_side, [uuid(1), uuid(4)]);
        assert_eq!(solution.value, 2.0);
        assert_eq!(
            solution.cut_edges,
            [CutEdge {
                edge_uuid: uuid(14),
                source_uuid: uuid(5),
                target_uuid: uuid(1),
                capacity: 2.0,
            }]
        );
    }

    #[test]
    fn parallel_zero_capacity_and_loops_are_handled_atomically() {
        let edges = [
            edge(15, 4, 1, 2.0),
            edge(11, 4, 1, 1.0),
            edge(12, 1, 5, 3.0),
            edge(10, 4, 5, 0.0),
            edge(13, 4, 4, 100.0),
        ];
        let expected = minimum_cut(
            &[uuid(5), uuid(4), uuid(1)],
            &edges,
            uuid(4),
            uuid(5),
            true,
            &control(),
        )
        .unwrap();
        assert_eq!(expected.source_side, [uuid(1), uuid(4)]);
        assert_eq!(expected.value, 3.0);
        assert_eq!(
            expected
                .cut_edges
                .iter()
                .map(|edge| edge.edge_uuid)
                .collect::<Vec<_>>(),
            [uuid(10), uuid(12)]
        );

        let mut permuted = edges;
        permuted.reverse();
        assert_eq!(
            minimum_cut(
                &[uuid(1), uuid(5), uuid(4)],
                &permuted,
                uuid(4),
                uuid(5),
                true,
                &control(),
            )
            .unwrap(),
            expected
        );
    }

    #[test]
    fn unreachable_cut_uses_lexicographically_smallest_source_side() {
        let solution = minimum_cut(
            &[uuid(6), uuid(4), uuid(2), uuid(1)],
            &[],
            uuid(4),
            uuid(6),
            true,
            &control(),
        )
        .unwrap();
        assert_eq!(solution.source_side, [uuid(1), uuid(2), uuid(4)]);
        assert_eq!(solution.value, 0.0);
        assert!(solution.cut_edges.is_empty());
    }

    #[test]
    fn invalid_inputs_cancellation_and_limits_return_no_solution() {
        for edges in [
            vec![edge(1, 4, 5, f64::NAN)],
            vec![edge(1, 4, 5, f64::INFINITY)],
            vec![edge(1, 4, 5, -1.0)],
        ] {
            assert!(
                minimum_cut(
                    &[uuid(4), uuid(5)],
                    &edges,
                    uuid(4),
                    uuid(5),
                    true,
                    &control(),
                )
                .is_err()
            );
        }
        assert!(
            minimum_cut(&[uuid(4), uuid(5)], &[], uuid(4), uuid(4), true, &control(),).is_err()
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            minimum_cut(
                &[uuid(4), uuid(5)],
                &[],
                uuid(4),
                uuid(5),
                true,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
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
            minimum_cut(
                &[uuid(4), uuid(5)],
                &[],
                uuid(4),
                uuid(5),
                true,
                &iteration_limited,
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));

        let output_limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            minimum_cut(
                &[uuid(4), uuid(5)],
                &[],
                uuid(4),
                uuid(5),
                true,
                &output_limited,
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let node_limited = AlgorithmControl::new(
            AlgorithmLimits {
                nodes: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            minimum_cut(
                &[uuid(4), uuid(5)],
                &[],
                uuid(4),
                uuid(5),
                true,
                &node_limited,
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));

        let edge_limited = AlgorithmControl::new(
            AlgorithmLimits {
                edges: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            minimum_cut(
                &[uuid(4), uuid(5)],
                &[edge(1, 4, 5, 1.0)],
                uuid(4),
                uuid(5),
                true,
                &edge_limited,
            ),
            Err(AlgorithmError::EdgeLimit { .. })
        ));
    }

    #[test]
    fn unshaped_solution_matches_public_solution_without_output_charge() {
        let nodes = [uuid(5), uuid(4), uuid(1)];
        let edges = [edge(11, 4, 1, 1.0), edge(10, 1, 5, 1.0)];
        let expected = minimum_cut(&nodes, &edges, uuid(4), uuid(5), true, &control()).unwrap();
        let zero_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );

        assert_eq!(
            minimum_cut_unshaped(&nodes, &edges, uuid(4), uuid(5), true, &zero_output,).unwrap(),
            expected
        );
    }

    #[test]
    fn internal_allocation_and_adjacency_overflow_are_structured() {
        assert!(matches!(
            reserved_vec::<u8>(usize::MAX, "minimum-cut test"),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            checked_adjacency_product(u64::MAX, 2),
            Err(AlgorithmError::Execution { .. })
        ));
    }
}
