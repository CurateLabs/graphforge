//! Min-cost maximum-flow views intentionally remain serial (#547/#548). Each
//! Bellman-Ford residual shortest path mutates capacities, costs, and flow
//! assignments consumed by the next augmentation, so private-pool work would
//! change residual visibility and public tie behavior.
//! The `min_cost_max_flow_edges` view (#548) projects per-edge flow/cost rows
//! from that final canonical residual state, not from independent edge tasks.

use std::collections::HashMap;
use std::hash::Hash;

use graphforge_core::algorithms::{Algorithm, PathAlgorithm};

use crate::algorithm_dispatch::{
    AlgorithmControl, AlgorithmError, AlgorithmOutput, AlgorithmValue,
};

/// One selected edge with graph-native capacity and cost.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CostCapacityEdge {
    pub edge_uuid: [u8; 16],
    pub source_uuid: [u8; 16],
    pub target_uuid: [u8; 16],
    pub capacity: f64,
    pub unit_cost: f64,
}

/// One validated solution shared by both public min-cost maximum-flow views.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MinCostFlowSolution {
    pub flow: f64,
    pub cost: f64,
    /// Signed stored-orientation flow, ordered by edge UUID.
    pub edge_flows: Vec<MinCostFlowEdge>,
}

/// One canonical per-edge row, ready for Arrow result shaping.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct MinCostFlowEdge {
    pub edge: CostCapacityEdge,
    pub flow: f64,
    pub flow_cost: f64,
}

#[derive(Clone, Copy, Debug)]
struct Arc {
    to: usize,
    reverse: usize,
    residual: f64,
    cost: f64,
    edge: usize,
    sign: f64,
}

/// Compute maximum flow first and minimum cost second.
///
/// Bellman-Ford shortest augmenting paths support signed costs without an
/// external graph dependency. Canonically ordered nodes, edge UUIDs, and
/// adjacency provide stable tie behavior. A reachable negative residual cycle
/// is rejected before it can make path choice undefined.
#[allow(
    clippy::too_many_lines,
    reason = "validation, residual construction, optimization, and atomic shaping remain one transaction"
)]
pub(crate) fn minimum_cost_maximum_flow(
    nodes: &[[u8; 16]],
    edges: &[CostCapacityEdge],
    source: [u8; 16],
    sink: [u8; 16],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<MinCostFlowSolution, AlgorithmError> {
    control.checkpoint()?;
    let adjacency_entries = min_cost_flow_adjacency_entries(edges, directed)?;
    control.check_graph_size(nodes.len(), adjacency_entries)?;
    if source == sink {
        return Err(execution(
            "minimum-cost maximum flow requires distinct endpoints",
        ));
    }

    let mut ordered_nodes = clone_slice(nodes, "node ordering")?;
    ordered_nodes.sort_unstable();
    if ordered_nodes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(execution(
            "minimum-cost maximum-flow node UUIDs must be unique",
        ));
    }
    let mut index = HashMap::new();
    reserve(&mut index, ordered_nodes.len(), "node index")?;
    for (position, &uuid) in ordered_nodes.iter().enumerate() {
        index.insert(uuid, position);
    }
    let Some(&source_index) = index.get(&source) else {
        return Err(execution(
            "minimum-cost maximum-flow source is outside node selection",
        ));
    };
    let Some(&sink_index) = index.get(&sink) else {
        return Err(execution(
            "minimum-cost maximum-flow target is outside node selection",
        ));
    };

    let mut ordered_edges = clone_slice(edges, "edge ordering")?;
    ordered_edges.sort_unstable_by_key(|edge| edge.edge_uuid);
    if ordered_edges
        .windows(2)
        .any(|pair| pair[0].edge_uuid == pair[1].edge_uuid)
    {
        return Err(execution(
            "minimum-cost maximum-flow edge UUIDs must be unique",
        ));
    }
    for &edge in &ordered_edges {
        control.check_cancelled()?;
        if !edge.capacity.is_finite() || edge.capacity < 0.0 {
            return Err(execution(
                "minimum-cost maximum flow requires finite nonnegative capacities",
            ));
        }
        if !edge.unit_cost.is_finite() {
            return Err(execution("minimum-cost maximum flow requires finite costs"));
        }
        if !index.contains_key(&edge.source_uuid) || !index.contains_key(&edge.target_uuid) {
            return Err(execution(
                "minimum-cost maximum-flow edge endpoint is outside node selection",
            ));
        }
    }

    let mut graph = vec_with(ordered_nodes.len(), Vec::<Arc>::new(), "adjacency index")?;
    for (edge_index, edge) in ordered_edges.iter().enumerate() {
        if edge.source_uuid == edge.target_uuid || edge.capacity == 0.0 {
            continue;
        }
        add_arc(
            &mut graph,
            index[&edge.source_uuid],
            index[&edge.target_uuid],
            edge.capacity,
            edge.unit_cost,
            edge_index,
            1.0,
        )?;
        if !directed {
            add_arc(
                &mut graph,
                index[&edge.target_uuid],
                index[&edge.source_uuid],
                edge.capacity,
                edge.unit_cost,
                edge_index,
                -1.0,
            )?;
        }
    }
    canonicalize_adjacency(&mut graph)?;

    let mut flow = 0.0;
    let mut cost = 0.0;
    let mut edge_flow = vec_with(ordered_edges.len(), 0.0, "edge-flow state")?;
    let mut edge_cost = vec_with(ordered_edges.len(), 0.0, "edge-cost state")?;
    while let Some(path) = shortest_path(&graph, source_index, sink_index, control)? {
        control.checkpoint()?;
        let amount = path
            .iter()
            .map(|&(from, arc)| graph[from][arc].residual)
            .fold(f64::INFINITY, f64::min);
        if !amount.is_finite() || amount <= 0.0 {
            return Err(execution(
                "minimum-cost maximum-flow augmentation is not finite",
            ));
        }
        for (from, arc_index) in path {
            let arc = graph[from][arc_index];
            graph[from][arc_index].residual -= amount;
            graph[arc.to][arc.reverse].residual += amount;
            edge_flow[arc.edge] = checked_add(
                edge_flow[arc.edge],
                arc.sign * amount,
                "minimum-cost maximum-flow edge accumulation is not finite",
            )?;
            cost = checked_add(
                cost,
                arc.cost * amount,
                "minimum-cost maximum-flow cost is not finite",
            )?;
            edge_cost[arc.edge] = checked_add(
                edge_cost[arc.edge],
                arc.cost * amount,
                "minimum-cost maximum-flow edge cost is not finite",
            )?;
        }
        flow = checked_add(
            flow,
            amount,
            "minimum-cost maximum-flow total is not finite",
        )?;
    }
    refine_lexicographic_ties(
        &mut graph,
        &mut edge_flow,
        &mut edge_cost,
        &mut cost,
        control,
    )?;

    control.check_cancelled()?;
    let mut edge_flows = Vec::new();
    reserve_vec(&mut edge_flows, ordered_edges.len(), "edge-flow result")?;
    for ((edge, flow), flow_cost) in ordered_edges.into_iter().zip(edge_flow).zip(edge_cost) {
        edge_flows.push(MinCostFlowEdge {
            edge,
            flow: normalize_zero(flow),
            flow_cost: normalize_zero(flow_cost),
        });
    }
    Ok(MinCostFlowSolution {
        flow: normalize_zero(flow),
        cost: normalize_zero(cost),
        edge_flows,
    })
}

/// Count direction-expanded residual entries after loop/zero-capacity filtering.
pub(crate) fn min_cost_flow_adjacency_entries(
    edges: &[CostCapacityEdge],
    directed: bool,
) -> Result<u64, AlgorithmError> {
    let active = edges
        .iter()
        .filter(|edge| edge.source_uuid != edge.target_uuid && edge.capacity != 0.0)
        .count();
    active
        .checked_mul(if directed { 2 } else { 4 })
        .and_then(|count| u64::try_from(count).ok())
        .ok_or_else(|| execution("minimum-cost maximum-flow graph size overflow"))
}

/// Shape either canonical public view from the same validated solution.
pub(crate) fn shape_min_cost_flow_output(
    solution: MinCostFlowSolution,
    source: [u8; 16],
    sink: [u8; 16],
    edges: bool,
    control: &AlgorithmControl,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let algorithm = Algorithm::Paths(if edges {
        PathAlgorithm::MinCostMaxFlowEdges
    } else {
        PathAlgorithm::MinCostMaxFlow
    });
    let output_rows = if edges { solution.edge_flows.len() } else { 1 };
    control.check_output_rows(output_rows)?;
    let mut output = control.output_sink(algorithm)?;
    if edges {
        for row in solution.edge_flows {
            let mut values = Vec::new();
            reserve_vec(&mut values, 6, "edge output values")?;
            values.extend([
                AlgorithmValue::Uuid(row.edge.edge_uuid),
                AlgorithmValue::Uuid(row.edge.source_uuid),
                AlgorithmValue::Uuid(row.edge.target_uuid),
                AlgorithmValue::Float64(row.flow),
                AlgorithmValue::Float64(row.edge.unit_cost),
                AlgorithmValue::Float64(row.flow_cost),
            ]);
            output.append_row(&values)?;
        }
    } else {
        let mut values = Vec::new();
        reserve_vec(&mut values, 4, "scalar output values")?;
        values.extend([
            AlgorithmValue::Uuid(source),
            AlgorithmValue::Uuid(sink),
            AlgorithmValue::Float64(solution.flow),
            AlgorithmValue::Float64(solution.cost),
        ]);
        output.append_row(&values)?;
    }
    output.finish()
}

fn refine_lexicographic_ties(
    graph: &mut [Vec<Arc>],
    edge_flow: &mut [f64],
    edge_cost: &mut [f64],
    total_cost: &mut f64,
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    for edge in 0..edge_flow.len() {
        loop {
            control.checkpoint()?;
            let mut refinement = None;
            'candidate: for from in 0..graph.len() {
                for (arc_index, arc) in graph[from].iter().enumerate() {
                    if arc.edge != edge || arc.sign >= 0.0 || arc.residual <= 0.0 {
                        continue;
                    }
                    if let Some(path) =
                        shortest_path_with_locked_edges(graph, arc.to, from, edge, control)?
                    {
                        let cycle_cost = checked_path_cost(arc.cost, &path, graph)?;
                        if cycle_cost == 0.0 {
                            refinement = Some((from, arc_index, path));
                            break 'candidate;
                        }
                    }
                }
            }
            let Some((from, arc_index, path)) = refinement else {
                break;
            };
            let amount = path
                .iter()
                .map(|&(node, index)| graph[node][index].residual)
                .chain(std::iter::once(graph[from][arc_index].residual))
                .fold(f64::INFINITY, f64::min);
            apply_cycle_arc(
                graph, from, arc_index, amount, edge_flow, edge_cost, total_cost,
            )?;
            for (node, index) in path {
                apply_cycle_arc(graph, node, index, amount, edge_flow, edge_cost, total_cost)?;
            }
        }
    }
    Ok(())
}

fn checked_path_cost(
    initial: f64,
    path: &[(usize, usize)],
    graph: &[Vec<Arc>],
) -> Result<f64, AlgorithmError> {
    let mut cost = initial;
    for &(node, index) in path {
        cost = checked_add(
            cost,
            graph[node][index].cost,
            "minimum-cost maximum-flow tie cycle cost is not finite",
        )?;
    }
    Ok(cost)
}

fn shortest_path_with_locked_edges(
    graph: &[Vec<Arc>],
    source: usize,
    sink: usize,
    first_unlocked_edge: usize,
    control: &AlgorithmControl,
) -> Result<Option<Vec<(usize, usize)>>, AlgorithmError> {
    let mut distance = vec_with(graph.len(), f64::INFINITY, "tie-distance state")?;
    let mut parent = vec_with(graph.len(), None, "tie-parent state")?;
    distance[source] = 0.0;
    for _ in 0..graph.len().saturating_sub(1) {
        control.check_cancelled()?;
        let mut changed = false;
        for from in 0..graph.len() {
            if !distance[from].is_finite() {
                continue;
            }
            for (arc_index, arc) in graph[from].iter().enumerate() {
                if arc.residual <= 0.0 || arc.edge < first_unlocked_edge {
                    continue;
                }
                let candidate = checked_add(
                    distance[from],
                    arc.cost,
                    "minimum-cost maximum-flow tie cost is not finite",
                )?;
                if candidate < distance[arc.to] {
                    distance[arc.to] = candidate;
                    parent[arc.to] = Some((from, arc_index));
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    if !distance[sink].is_finite() {
        return Ok(None);
    }
    let mut path = Vec::new();
    reserve_vec(&mut path, graph.len(), "tie-path state")?;
    let mut node = sink;
    let mut seen = vec_with(graph.len(), false, "tie-path membership")?;
    while node != source {
        if seen[node] {
            return Ok(None);
        }
        seen[node] = true;
        let Some(step) = parent[node] else {
            return Ok(None);
        };
        path.push(step);
        node = step.0;
    }
    path.reverse();
    Ok(Some(path))
}

fn apply_cycle_arc(
    graph: &mut [Vec<Arc>],
    from: usize,
    arc_index: usize,
    amount: f64,
    edge_flow: &mut [f64],
    edge_cost: &mut [f64],
    total_cost: &mut f64,
) -> Result<(), AlgorithmError> {
    let arc = graph[from][arc_index];
    graph[from][arc_index].residual -= amount;
    graph[arc.to][arc.reverse].residual += amount;
    edge_flow[arc.edge] = checked_add(
        edge_flow[arc.edge],
        arc.sign * amount,
        "minimum-cost maximum-flow tie accumulation is not finite",
    )?;
    edge_cost[arc.edge] = checked_add(
        edge_cost[arc.edge],
        arc.cost * amount,
        "minimum-cost maximum-flow tie cost is not finite",
    )?;
    *total_cost = checked_add(
        *total_cost,
        arc.cost * amount,
        "minimum-cost maximum-flow total tie cost is not finite",
    )?;
    Ok(())
}

fn add_arc(
    graph: &mut [Vec<Arc>],
    from: usize,
    to: usize,
    capacity: f64,
    cost: f64,
    edge: usize,
    sign: f64,
) -> Result<(), AlgorithmError> {
    let forward_reverse = graph[to].len();
    let reverse_reverse = graph[from].len();
    graph[from]
        .try_reserve(1)
        .map_err(|_| allocation("adjacency arc"))?;
    graph[to]
        .try_reserve(1)
        .map_err(|_| allocation("adjacency reverse arc"))?;
    graph[from].push(Arc {
        to,
        reverse: forward_reverse,
        residual: capacity,
        cost,
        edge,
        sign,
    });
    graph[to].push(Arc {
        to: from,
        reverse: reverse_reverse,
        residual: 0.0,
        cost: -cost,
        edge,
        sign: -sign,
    });
    Ok(())
}

fn canonicalize_adjacency(graph: &mut [Vec<Arc>]) -> Result<(), AlgorithmError> {
    // Reordering arcs requires rebuilding reverse indexes.
    let mut arcs = Vec::new();
    let arc_count = graph.iter().try_fold(0_usize, |total, adjacency| {
        total
            .checked_add(adjacency.len())
            .ok_or_else(|| execution("minimum-cost maximum-flow adjacency size overflow"))
    })?;
    reserve_vec(&mut arcs, arc_count, "canonical adjacency")?;
    for (from, adjacency) in graph.iter().enumerate() {
        for arc in adjacency {
            if arc.residual > 0.0 {
                arcs.push((from, arc.to, arc.residual, arc.cost, arc.edge, arc.sign));
            }
        }
    }
    for adjacency in graph.iter_mut() {
        adjacency.clear();
    }
    arcs.sort_unstable_by(|left, right| {
        (left.0, left.1, left.4, left.5.is_sign_negative()).cmp(&(
            right.0,
            right.1,
            right.4,
            right.5.is_sign_negative(),
        ))
    });
    for (from, to, capacity, cost, edge, sign) in arcs {
        add_arc(graph, from, to, capacity, cost, edge, sign)?;
    }
    Ok(())
}

fn shortest_path(
    graph: &[Vec<Arc>],
    source: usize,
    sink: usize,
    control: &AlgorithmControl,
) -> Result<Option<Vec<(usize, usize)>>, AlgorithmError> {
    let mut distance = vec_with(graph.len(), f64::INFINITY, "distance state")?;
    let mut parent = vec_with(graph.len(), None, "parent state")?;
    distance[source] = 0.0;

    for pass in 0..graph.len() {
        control.checkpoint()?;
        let mut changed = false;
        for from in 0..graph.len() {
            control.check_cancelled()?;
            if !distance[from].is_finite() {
                continue;
            }
            for (arc_index, arc) in graph[from].iter().enumerate() {
                if arc.residual <= 0.0 {
                    continue;
                }
                let candidate = checked_add(
                    distance[from],
                    arc.cost,
                    "minimum-cost maximum-flow path cost is not finite",
                )?;
                if candidate < distance[arc.to] {
                    if pass + 1 == graph.len() {
                        return Err(execution(
                            "minimum-cost maximum flow is undefined because a reachable negative-cost residual cycle exists",
                        ));
                    }
                    distance[arc.to] = candidate;
                    parent[arc.to] = Some((from, arc_index));
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    if !distance[sink].is_finite() {
        return Ok(None);
    }

    let mut path = Vec::new();
    reserve_vec(&mut path, graph.len(), "path state")?;
    let mut node = sink;
    let mut seen = vec_with(graph.len(), false, "path membership")?;
    while node != source {
        if seen[node] {
            return Err(execution(
                "minimum-cost maximum-flow predecessor cycle is undefined",
            ));
        }
        seen[node] = true;
        let Some(step) = parent[node] else {
            return Err(execution(
                "minimum-cost maximum-flow path reconstruction failed",
            ));
        };
        path.push(step);
        node = step.0;
    }
    path.reverse();
    Ok(Some(path))
}

fn checked_add(left: f64, right: f64, message: &'static str) -> Result<f64, AlgorithmError> {
    let value = left + right;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(execution(message))
    }
}

fn normalize_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

fn clone_slice<T: Clone>(values: &[T], name: &str) -> Result<Vec<T>, AlgorithmError> {
    let mut cloned = Vec::new();
    reserve_vec(&mut cloned, values.len(), name)?;
    cloned.extend_from_slice(values);
    Ok(cloned)
}

fn vec_with<T: Clone>(length: usize, value: T, name: &str) -> Result<Vec<T>, AlgorithmError> {
    let mut values = Vec::new();
    reserve_vec(&mut values, length, name)?;
    values.resize(length, value);
    Ok(values)
}

fn reserve_vec<T>(
    values: &mut Vec<T>,
    additional: usize,
    name: &str,
) -> Result<(), AlgorithmError> {
    values
        .try_reserve_exact(additional)
        .map_err(|_| allocation(name))
}

fn reserve<K: Eq + Hash, V>(
    values: &mut HashMap<K, V>,
    additional: usize,
    name: &str,
) -> Result<(), AlgorithmError> {
    values.try_reserve(additional).map_err(|_| allocation(name))
}

fn allocation(name: &str) -> AlgorithmError {
    execution(format!(
        "minimum-cost maximum-flow {name} allocation failed"
    ))
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

    fn edge(id: u8, source: u8, target: u8, capacity: f64, cost: f64) -> CostCapacityEdge {
        CostCapacityEdge {
            edge_uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
            capacity,
            unit_cost: cost,
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn solve(
        nodes: &[u8],
        edges: &[CostCapacityEdge],
        source: u8,
        sink: u8,
        directed: bool,
        control: &AlgorithmControl,
    ) -> Result<MinCostFlowSolution, AlgorithmError> {
        minimum_cost_maximum_flow(
            &nodes.iter().copied().map(uuid).collect::<Vec<_>>(),
            edges,
            uuid(source),
            uuid(sink),
            directed,
            control,
        )
    }

    #[test]
    fn maximizes_flow_before_minimizing_cost() {
        let solution = solve(
            &[1, 2, 3, 4],
            &[
                edge(12, 1, 3, 2.0, 1.0),
                edge(14, 3, 4, 2.0, 1.0),
                edge(10, 1, 2, 3.0, 9.0),
                edge(13, 2, 4, 3.0, 9.0),
            ],
            1,
            4,
            true,
            &control(),
        )
        .unwrap();
        assert_eq!(solution.flow, 5.0);
        assert_eq!(solution.cost, 58.0);
        assert_eq!(
            solution
                .edge_flows
                .iter()
                .map(|row| (row.edge.edge_uuid, row.flow))
                .collect::<Vec<_>>(),
            vec![
                (uuid(10), 3.0),
                (uuid(12), 2.0),
                (uuid(13), 3.0),
                (uuid(14), 2.0),
            ]
        );
    }

    #[test]
    fn negative_cost_parallel_edges_and_self_loops_are_stable() {
        let edges = [
            edge(13, 2, 3, 3.0, 2.0),
            edge(11, 1, 2, 1.0, -3.0),
            edge(10, 1, 2, 2.0, -3.0),
            edge(12, 2, 2, 100.0, -100.0),
        ];
        let first = solve(&[3, 1, 2], &edges, 1, 3, true, &control()).unwrap();
        let second = solve(&[2, 3, 1], &edges, 1, 3, true, &control()).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.flow, 3.0);
        assert_eq!(first.cost, -3.0);
        assert_eq!(first.edge_flows[2].flow, 0.0);
    }

    #[test]
    fn equal_cost_ties_choose_lexicographically_smallest_edge_flow() {
        let solution = solve(
            &[1, 2],
            &[edge(10, 1, 2, 1.0, 3.0), edge(11, 1, 2, 1.0, 3.0)],
            1,
            2,
            true,
            &control(),
        )
        .unwrap();
        assert_eq!(solution.flow, 2.0);
        assert_eq!(
            solution
                .edge_flows
                .iter()
                .map(|row| row.flow)
                .collect::<Vec<_>>(),
            vec![1.0, 1.0]
        );

        let limited = solve(
            &[1, 2, 3],
            &[
                edge(10, 1, 2, 1.0, 3.0),
                edge(11, 1, 2, 1.0, 3.0),
                edge(12, 2, 3, 1.0, 0.0),
            ],
            1,
            3,
            true,
            &control(),
        )
        .unwrap();
        assert_eq!(
            limited
                .edge_flows
                .iter()
                .map(|row| row.flow)
                .collect::<Vec<_>>(),
            vec![0.0, 1.0, 1.0]
        );
    }

    #[test]
    fn undirected_flow_is_signed_in_stored_orientation() {
        let solution = solve(
            &[1, 2],
            &[edge(10, 1, 2, 4.0, 2.5)],
            2,
            1,
            false,
            &control(),
        )
        .unwrap();
        assert_eq!(solution.flow, 4.0);
        assert_eq!(solution.cost, 10.0);
        assert_eq!(solution.edge_flows[0].flow, -4.0);
        assert_eq!(solution.edge_flows[0].flow_cost, 10.0);
    }

    #[test]
    fn unreachable_is_zero_with_complete_edge_shape() {
        let solution = solve(
            &[1, 2, 3],
            &[edge(10, 2, 3, 1.0, 4.0)],
            1,
            3,
            true,
            &control(),
        )
        .unwrap();
        assert_eq!(solution.flow, 0.0);
        assert_eq!(solution.cost, 0.0);
        assert_eq!(solution.edge_flows[0].flow, 0.0);
    }

    #[test]
    fn preserves_positive_fractional_capacity_below_old_epsilon() {
        let solution = solve(
            &[1, 2],
            &[edge(10, 1, 2, 1.0e-13, 2.0)],
            1,
            2,
            true,
            &control(),
        )
        .unwrap();
        assert_eq!(solution.flow, 1.0e-13);
        assert_eq!(solution.cost, 2.0e-13);
        assert_eq!(solution.edge_flows[0].flow, 1.0e-13);
    }

    #[test]
    fn scalar_and_edge_outputs_use_distinct_canonical_schemas() {
        let solution = solve(&[1, 2], &[edge(10, 1, 2, 2.0, 3.0)], 1, 2, true, &control()).unwrap();
        let scalar =
            shape_min_cost_flow_output(solution.clone(), uuid(1), uuid(2), false, &control())
                .unwrap();
        assert_eq!(
            scalar.schema,
            Algorithm::Paths(PathAlgorithm::MinCostMaxFlow).result_schema()
        );
        assert_eq!(scalar.rows().len(), 1);
        assert_eq!(scalar.rows()[0].len(), 4);

        let edges =
            shape_min_cost_flow_output(solution, uuid(1), uuid(2), true, &control()).unwrap();
        assert_eq!(
            edges.schema,
            Algorithm::Paths(PathAlgorithm::MinCostMaxFlowEdges).result_schema()
        );
        assert_eq!(edges.rows().len(), 1);
        assert_eq!(edges.rows()[0].len(), 6);
    }

    #[test]
    fn tie_cycle_cost_accumulation_is_checked() {
        let graph = vec![
            vec![Arc {
                to: 1,
                reverse: 0,
                residual: 1.0,
                cost: f64::MAX,
                edge: 0,
                sign: 1.0,
            }],
            vec![],
        ];
        let error = checked_path_cost(f64::MAX, &[(0, 0)], &graph).unwrap_err();
        assert!(error.to_string().contains("tie cycle cost is not finite"));
    }

    #[test]
    fn rejects_reachable_negative_residual_cycle() {
        let error = solve(
            &[1, 2, 3, 4],
            &[
                edge(10, 1, 2, 1.0, 0.0),
                edge(11, 2, 3, 1.0, -2.0),
                edge(12, 3, 2, 1.0, 1.0),
                edge(13, 3, 4, 1.0, 0.0),
            ],
            1,
            4,
            true,
            &control(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("negative-cost residual cycle"));
    }

    #[test]
    fn validation_cancellation_limits_and_overflow_are_atomic() {
        for invalid in [
            edge(10, 1, 2, -1.0, 1.0),
            edge(10, 1, 2, f64::NAN, 1.0),
            edge(10, 1, 2, 1.0, f64::INFINITY),
        ] {
            assert!(solve(&[1, 2], &[invalid], 1, 2, true, &control()).is_err());
        }
        assert!(solve(&[1, 2], &[], 1, 1, true, &control()).is_err());

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            solve(&[1, 2], &[], 1, 2, true, &cancelled).unwrap_err(),
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
            solve(&[1, 2], &[edge(10, 1, 2, 1.0, 0.0)], 1, 2, true, &limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));

        let overflow = solve(
            &[1, 2],
            &[edge(10, 1, 2, f64::MAX, 2.0)],
            1,
            2,
            true,
            &control(),
        )
        .unwrap_err();
        assert!(overflow.to_string().contains("cost is not finite"));
    }

    #[test]
    fn resource_preflight_and_cancellation_fail_atomically() {
        let nodes = [1, 2];
        let edges = [edge(10, 1, 2, 1.0, 0.0)];
        let limits = |nodes, edges, output_rows| {
            AlgorithmControl::new(
                AlgorithmLimits {
                    nodes,
                    edges,
                    output_rows,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            )
        };

        assert_eq!(
            solve(&nodes, &edges, 1, 2, true, &limits(1, 2, 1)),
            Err(AlgorithmError::NodeLimit {
                observed: 2,
                limit: 1,
            })
        );
        assert_eq!(
            solve(&nodes, &edges, 1, 2, true, &limits(2, 1, 1)),
            Err(AlgorithmError::EdgeLimit {
                observed: 2,
                limit: 1,
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            solve(
                &nodes,
                &edges,
                1,
                2,
                true,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn adjacency_limits_count_only_direction_expanded_active_edges() {
        let edges = [
            edge(10, 1, 2, 1.0, 0.0),
            edge(11, 1, 1, 9.0, 0.0),
            edge(12, 1, 2, 0.0, 0.0),
        ];
        let limited = |edge_limit| {
            AlgorithmControl::new(
                AlgorithmLimits {
                    edges: edge_limit,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            )
        };
        assert!(solve(&[1, 2], &edges, 1, 2, true, &limited(2)).is_ok());
        assert!(solve(&[1, 2], &edges, 1, 2, false, &limited(4)).is_ok());
        assert_eq!(
            solve(&[1, 2], &edges, 1, 2, false, &limited(3)),
            Err(AlgorithmError::EdgeLimit {
                observed: 4,
                limit: 3,
            })
        );
    }

    #[test]
    fn output_limits_are_view_specific_and_atomic() {
        let solution = solve(
            &[1, 2],
            &[edge(10, 1, 2, 1.0, 0.0), edge(11, 1, 2, 1.0, 0.0)],
            1,
            2,
            true,
            &control(),
        )
        .unwrap();
        let one_row = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(
            shape_min_cost_flow_output(solution.clone(), uuid(1), uuid(2), false, &one_row).is_ok()
        );
        assert_eq!(
            shape_min_cost_flow_output(solution, uuid(1), uuid(2), true, &one_row),
            Err(AlgorithmError::OutputLimit {
                observed: 2,
                limit: 1,
            })
        );
    }

    #[test]
    fn small_integer_network_matches_exhaustive_oracle() {
        let edges = [
            edge(10, 1, 2, 2.0, 2.0),
            edge(11, 1, 3, 2.0, 1.0),
            edge(12, 2, 3, 1.0, -2.0),
            edge(13, 2, 4, 2.0, 1.0),
            edge(14, 3, 4, 2.0, 3.0),
        ];
        let solution = solve(&[1, 2, 3, 4], &edges, 1, 4, true, &control()).unwrap();
        let mut best: Option<(i32, i32, Vec<i32>)> = None;
        for f0 in 0..=2 {
            for f1 in 0..=2 {
                for f2 in 0..=1 {
                    for f3 in 0..=2 {
                        for f4 in 0..=2 {
                            if f0 != f2 + f3 || f1 + f2 != f4 {
                                continue;
                            }
                            let value = f0 + f1;
                            let cost = 2 * f0 + f1 - 2 * f2 + f3 + 3 * f4;
                            let flows = vec![f0, f1, f2, f3, f4];
                            let candidate = (value, cost, flows);
                            if best.as_ref().is_none_or(|current| {
                                candidate.0 > current.0
                                    || (candidate.0 == current.0
                                        && (candidate.1 < current.1
                                            || (candidate.1 == current.1
                                                && candidate.2 < current.2)))
                            }) {
                                best = Some(candidate);
                            }
                        }
                    }
                }
            }
        }
        let best = best.unwrap();
        assert_eq!(solution.flow, f64::from(best.0));
        assert_eq!(solution.cost, f64::from(best.1));
        assert_eq!(
            solution
                .edge_flows
                .iter()
                .map(|row| row.flow)
                .collect::<Vec<_>>(),
            best.2.into_iter().map(f64::from).collect::<Vec<_>>()
        );
    }
}
