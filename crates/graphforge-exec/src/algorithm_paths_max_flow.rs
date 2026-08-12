//! Maximum-flow views intentionally remain serial (#545/#546). Edmonds-Karp
//! augmentation mutates one residual graph after each canonical BFS path, and
//! each update determines the next path, edge-flow assignment, and tie order.
//! Parallel augmentations would need shared residual coordination and could
//! change public flow fingerprints, so no private-pool crossover is claimed.

use std::collections::VecDeque;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CapacityEdge {
    pub edge_uuid: [u8; 16],
    pub source_uuid: [u8; 16],
    pub target_uuid: [u8; 16],
    pub capacity: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FlowSolution {
    pub value: f64,
    /// Net flow in each stored edge's declared direction, ordered by edge UUID.
    pub edge_flows: Vec<(CapacityEdge, f64)>,
}

#[derive(Clone, Copy, Debug)]
struct Arc {
    to: usize,
    reverse: usize,
    residual: f64,
    edge: usize,
    sign: f64,
}

struct ValidatedFlow {
    nodes: Vec<[u8; 16]>,
    edges: Vec<CapacityEdge>,
    source: usize,
    sink: usize,
}

/// Compute one canonical flow solution for both public maximum-flow views.
///
/// Edmonds-Karp chooses the shortest augmenting path. Canonically sorted nodes,
/// edges, and adjacency make that choice deterministic. In undirected mode each
/// stored edge receives independent capacity in both directions; its reported
/// assignment is the signed net flow in its stored direction.
pub(crate) fn maximum_flow(
    nodes: &[[u8; 16]],
    edges: &[CapacityEdge],
    source: [u8; 16],
    sink: [u8; 16],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<FlowSolution, AlgorithmError> {
    let validated = validate_projection(nodes, edges, source, sink, directed, control)?;
    let mut graph = residual_graph(&validated.nodes, &validated.edges, directed)?;
    let mut value = 0.0;
    let mut edge_flow = reserved_vec(validated.edges.len(), "maximum-flow edge state")?;
    edge_flow.resize(validated.edges.len(), 0.0);
    while let Some(path) = augmenting_path(&graph, validated.source, validated.sink, control)? {
        control.checkpoint()?;
        let amount = path
            .iter()
            .map(|&(from, arc)| graph[from][arc].residual)
            .fold(f64::INFINITY, f64::min);
        if !amount.is_finite() {
            return Err(execution("maximum-flow augmentation is not finite"));
        }
        for (from, arc_index) in path {
            let arc = graph[from][arc_index];
            graph[from][arc_index].residual -= amount;
            graph[arc.to][arc.reverse].residual += amount;
            edge_flow[arc.edge] += arc.sign * amount;
        }
        value += amount;
        if !value.is_finite() {
            return Err(execution("maximum-flow total is not finite"));
        }
    }
    control.check_cancelled()?;
    let mut edge_flows = reserved_vec(validated.edges.len(), "maximum-flow result edges")?;
    edge_flows.extend(validated.edges.into_iter().zip(edge_flow));
    Ok(FlowSolution { value, edge_flows })
}

fn validate_projection(
    nodes: &[[u8; 16]],
    edges: &[CapacityEdge],
    source: [u8; 16],
    sink: [u8; 16],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<ValidatedFlow, AlgorithmError> {
    control.checkpoint()?;
    if source == sink {
        return Err(execution("maximum flow requires distinct endpoints"));
    }
    let mut ordered_nodes = reserved_vec(nodes.len(), "maximum-flow nodes")?;
    ordered_nodes.extend_from_slice(nodes);
    ordered_nodes.sort_unstable();
    if ordered_nodes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(execution("maximum-flow node UUIDs must be unique"));
    }
    let Ok(source_index) = ordered_nodes.binary_search(&source) else {
        return Err(execution("maximum-flow source is outside node selection"));
    };
    let Ok(sink_index) = ordered_nodes.binary_search(&sink) else {
        return Err(execution("maximum-flow target is outside node selection"));
    };

    let mut ordered_edges = reserved_vec(edges.len(), "maximum-flow edges")?;
    for &edge in edges {
        control.check_cancelled()?;
        if !edge.capacity.is_finite() || edge.capacity < 0.0 {
            return Err(execution(
                "maximum flow requires finite nonnegative capacities",
            ));
        }
        if ordered_nodes.binary_search(&edge.source_uuid).is_err()
            || ordered_nodes.binary_search(&edge.target_uuid).is_err()
        {
            return Err(execution(
                "maximum-flow edge endpoint is outside node selection",
            ));
        }
        ordered_edges.push(edge);
    }
    ordered_edges.sort_unstable_by_key(|edge| edge.edge_uuid);
    if ordered_edges
        .windows(2)
        .any(|pair| pair[0].edge_uuid == pair[1].edge_uuid)
    {
        return Err(execution("maximum-flow edge UUIDs must be unique"));
    }
    let directions = if directed { 1_u64 } else { 2 };
    let adjacency_entries = checked_adjacency_entries(ordered_edges.len(), directions)?;
    control.check_graph_size(ordered_nodes.len(), adjacency_entries)?;

    Ok(ValidatedFlow {
        nodes: ordered_nodes,
        edges: ordered_edges,
        source: source_index,
        sink: sink_index,
    })
}

fn residual_graph(
    ordered_nodes: &[[u8; 16]],
    ordered_edges: &[CapacityEdge],
    directed: bool,
) -> Result<Vec<Vec<Arc>>, AlgorithmError> {
    let mut degrees = reserved_vec(ordered_nodes.len(), "maximum-flow adjacency degrees")?;
    degrees.resize(ordered_nodes.len(), 0_usize);
    for edge in ordered_edges {
        if edge.source_uuid == edge.target_uuid {
            continue;
        }
        let source = ordered_nodes
            .binary_search(&edge.source_uuid)
            .expect("validated endpoint");
        let target = ordered_nodes
            .binary_search(&edge.target_uuid)
            .expect("validated endpoint");
        add_degree(&mut degrees, source)?;
        add_degree(&mut degrees, target)?;
        if !directed {
            add_degree(&mut degrees, source)?;
            add_degree(&mut degrees, target)?;
        }
    }
    let mut graph = reserved_vec(ordered_nodes.len(), "maximum-flow adjacency")?;
    for degree in degrees {
        graph.push(reserved_vec(degree, "maximum-flow adjacency arcs")?);
    }
    for (edge_index, edge) in ordered_edges.iter().enumerate() {
        if edge.source_uuid == edge.target_uuid {
            continue;
        }
        add_capacity(
            &mut graph,
            ordered_nodes
                .binary_search(&edge.source_uuid)
                .expect("validated endpoint"),
            ordered_nodes
                .binary_search(&edge.target_uuid)
                .expect("validated endpoint"),
            edge.capacity,
            edge_index,
            1.0,
        )?;
        if !directed {
            add_capacity(
                &mut graph,
                ordered_nodes
                    .binary_search(&edge.target_uuid)
                    .expect("validated endpoint"),
                ordered_nodes
                    .binary_search(&edge.source_uuid)
                    .expect("validated endpoint"),
                edge.capacity,
                edge_index,
                -1.0,
            )?;
        }
    }

    Ok(graph)
}

fn add_capacity(
    graph: &mut [Vec<Arc>],
    from: usize,
    to: usize,
    capacity: f64,
    edge: usize,
    sign: f64,
) -> Result<(), AlgorithmError> {
    let (forward_reverse, reverse_reverse) = (graph[to].len(), graph[from].len());
    try_push(
        &mut graph[from],
        Arc {
            to,
            reverse: forward_reverse,
            residual: capacity,
            edge,
            sign,
        },
        "maximum-flow residual arcs",
    )?;
    try_push(
        &mut graph[to],
        Arc {
            to: from,
            reverse: reverse_reverse,
            residual: 0.0,
            edge,
            sign: -sign,
        },
        "maximum-flow residual arcs",
    )?;
    Ok(())
}

fn augmenting_path(
    graph: &[Vec<Arc>],
    source: usize,
    sink: usize,
    control: &AlgorithmControl,
) -> Result<Option<Vec<(usize, usize)>>, AlgorithmError> {
    let mut parent = reserved_vec(graph.len(), "maximum-flow BFS parents")?;
    parent.resize(graph.len(), None);
    let mut queue = VecDeque::new();
    queue
        .try_reserve(graph.len())
        .map_err(|_| allocation("maximum-flow BFS queue"))?;
    queue.push_back(source);
    while let Some(from) = queue.pop_front() {
        control.check_cancelled()?;
        for (arc_index, arc) in graph[from].iter().enumerate() {
            if arc.residual <= 0.0 || arc.to == source || parent[arc.to].is_some() {
                continue;
            }
            parent[arc.to] = Some((from, arc_index));
            if arc.to == sink {
                let mut path = reserved_vec(graph.len(), "maximum-flow augmenting path")?;
                let mut node = sink;
                while node != source {
                    let step = parent[node].expect("reachable nodes have parents");
                    path.push(step);
                    node = step.0;
                }
                path.reverse();
                return Ok(Some(path));
            }
            queue.push_back(arc.to);
        }
    }
    Ok(None)
}

fn add_degree(degrees: &mut [usize], node: usize) -> Result<(), AlgorithmError> {
    degrees[node] = degrees[node]
        .checked_add(1)
        .ok_or_else(|| execution("maximum-flow adjacency degree overflow"))?;
    Ok(())
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

fn checked_adjacency_entries(edges: usize, directions: u64) -> Result<u64, AlgorithmError> {
    u64::try_from(edges)
        .map_err(|_| execution("maximum-flow adjacency entry count overflow"))
        .and_then(|edges| checked_adjacency_product(edges, directions))
}

fn checked_adjacency_product(edges: u64, directions: u64) -> Result<u64, AlgorithmError> {
    edges
        .checked_mul(directions)
        .ok_or_else(|| execution("maximum-flow adjacency entry count overflow"))
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
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn edge(id: u8, source: u8, target: u8, capacity: f64) -> CapacityEdge {
        CapacityEdge {
            edge_uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
            capacity,
        }
    }

    fn simple(
        edges: &[CapacityEdge],
        source: u8,
        sink: u8,
        directed: bool,
        control: &AlgorithmControl,
    ) -> Result<FlowSolution, AlgorithmError> {
        maximum_flow(
            &[uuid(1), uuid(2)],
            edges,
            uuid(source),
            uuid(sink),
            directed,
            control,
        )
    }

    #[test]
    fn canonical_solution_handles_parallel_edges_and_conservation() {
        let edges = [
            edge(14, 2, 4, 3.0),
            edge(10, 1, 2, 3.0),
            edge(12, 1, 3, 2.0),
            edge(15, 3, 4, 3.0),
            edge(11, 1, 2, 1.0),
            edge(13, 2, 3, 1.0),
            edge(16, 2, 2, 99.0),
        ];
        let solution = maximum_flow(
            &[uuid(4), uuid(2), uuid(1), uuid(3)],
            &edges,
            uuid(1),
            uuid(4),
            true,
            &control(),
        )
        .unwrap();
        assert_eq!(solution.value, 6.0);
        assert_eq!(
            solution
                .edge_flows
                .iter()
                .map(|(edge, _)| edge.edge_uuid)
                .collect::<Vec<_>>(),
            (10..=16).map(uuid).collect::<Vec<_>>()
        );
        assert_eq!(solution.edge_flows.last().unwrap().1, 0.0);
        assert_eq!(
            solution.edge_flows[0..3]
                .iter()
                .map(|row| row.1)
                .sum::<f64>(),
            6.0
        );
        assert_eq!(
            solution.edge_flows[4..6]
                .iter()
                .map(|row| row.1)
                .sum::<f64>(),
            6.0
        );
        let inflow_2 = solution.edge_flows[0].1 + solution.edge_flows[1].1;
        let outflow_2 = solution.edge_flows[3].1 + solution.edge_flows[4].1;
        assert_eq!(inflow_2, outflow_2);
    }

    #[test]
    fn undirected_edges_supply_both_directions_and_unreachable_is_zero() {
        let reverse = simple(&[edge(10, 1, 2, 4.0)], 2, 1, false, &control()).unwrap();
        assert_eq!(reverse.value, 4.0);
        assert_eq!(reverse.edge_flows[0].1, -4.0);
        let unreachable = simple(&[], 1, 2, true, &control()).unwrap();
        assert_eq!(unreachable.value, 0.0);
        assert!(unreachable.edge_flows.is_empty());
    }

    #[test]
    fn invalid_inputs_cancellation_and_iteration_limits_are_atomic() {
        assert!(simple(&[edge(10, 1, 2, f64::NAN)], 1, 2, true, &control()).is_err());
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            simple(
                &[],
                1,
                2,
                true,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            )
            .unwrap_err(),
            AlgorithmError::Cancelled
        );
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            simple(&[edge(10, 1, 2, 1.0)], 1, 2, true, &limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));

        let node_limited = AlgorithmControl::new(
            AlgorithmLimits {
                nodes: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            simple(&[], 1, 2, true, &node_limited),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        let edge_limited = AlgorithmControl::new(
            AlgorithmLimits {
                edges: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            simple(&[edge(10, 1, 2, 1.0)], 1, 2, false, &edge_limited),
            Err(AlgorithmError::EdgeLimit { .. })
        ));
    }

    #[test]
    fn impossible_reservation_and_adjacency_overflow_are_structured() {
        assert!(matches!(
            reserved_vec::<u8>(usize::MAX, "maximum-flow test"),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            checked_adjacency_product(u64::MAX, 2),
            Err(AlgorithmError::Execution { .. })
        ));
    }
}
