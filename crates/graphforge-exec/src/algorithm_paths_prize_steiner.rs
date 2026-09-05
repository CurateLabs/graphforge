//! Exact deterministic prize-collecting Steiner trees for undirected graphs.

//! Prize-collecting Steiner tree remains serial (#552). Exact subset evaluation
//! compares one global objective with canonical edge ties; parallel workers
//! would need shared best-candidate coordination and could alter fingerprints.

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

/// A graph-native numeric property resolved before kernel dispatch.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ResolvedNumber {
    Float64(f64),
}

impl ResolvedNumber {
    fn finite_nonnegative(self, name: &str) -> Result<f64, AlgorithmError> {
        let Self::Float64(value) = self;
        if !value.is_finite() || value < 0.0 {
            return Err(execution(format!("{name} must be finite and nonnegative")));
        }
        Ok(value)
    }
}

/// One selected graph node and its explicitly resolved prize property.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct NodePrize {
    pub node_uuid: [u8; 16],
    pub prize: ResolvedNumber,
}

/// One graph-native undirected edge and its resolved cost.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct PrizeSteinerInputEdge {
    pub edge_uuid: [u8; 16],
    pub source_uuid: [u8; 16],
    pub target_uuid: [u8; 16],
    pub cost: ResolvedNumber,
}

/// One canonical selected edge in the exact optimum.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct PrizeSteinerEdge {
    pub edge_uuid: [u8; 16],
    pub source_uuid: [u8; 16],
    pub target_uuid: [u8; 16],
    pub weight: f64,
}

#[derive(Clone, Copy, Debug)]
struct Edge {
    output: PrizeSteinerEdge,
    source: usize,
    target: usize,
}

#[derive(Clone, Debug)]
struct Candidate {
    objective: f64,
    edge_indices: Vec<usize>,
}

#[derive(Debug)]
enum Evaluation {
    Infeasible,
    Overflow,
    Candidate(Candidate),
}

#[derive(Debug, Default)]
struct SearchOutcome {
    best: Option<Candidate>,
    feasible: bool,
}

/// Compute one exact prize-collecting Steiner tree.
///
/// Every non-loop edge subset is considered after an exact state-space
/// preflight. Feasible subsets are connected trees containing every mandatory
/// terminal. Nonnegative costs make cycle removal objective-preserving or
/// improving, and the fewer-edge tie-break therefore makes this restriction
/// exact even when costs are zero.
pub(crate) fn prize_collecting_steiner_tree(
    nodes: &[[u8; 16]],
    prizes: &[NodePrize],
    edges: &[PrizeSteinerInputEdge],
    terminals: &[[u8; 16]],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<Vec<PrizeSteinerEdge>, AlgorithmError> {
    let (nodes, prizes, edges, terminals) =
        validate_projection(nodes, prizes, edges, terminals, directed, control)?;
    let state_count = subset_state_count(edges.len())?;
    control.check_states(state_count)?;

    let mut outcome = SearchOutcome::default();
    let mut selected = reserved_vec(edges.len(), "prize Steiner subset")?;
    enumerate(
        0,
        &nodes,
        &prizes,
        &edges,
        &terminals,
        &mut selected,
        &mut outcome,
        control,
    )?;

    let best = match outcome.best {
        Some(best) => best,
        None if outcome.feasible => {
            return Err(execution("prize Steiner objective is not finite"));
        }
        None => return Err(execution("prize Steiner terminals are unreachable")),
    };
    control.check_output_rows(best.edge_indices.len())?;
    let mut output = reserved_vec(best.edge_indices.len(), "prize Steiner result")?;
    for edge in best.edge_indices {
        output.push(edges[edge].output);
    }
    Ok(output)
}

type Validated = (Vec<[u8; 16]>, Vec<f64>, Vec<Edge>, Vec<usize>);

fn validate_projection(
    nodes: &[[u8; 16]],
    prize_mapping: &[NodePrize],
    edges: &[PrizeSteinerInputEdge],
    terminals: &[[u8; 16]],
    directed: bool,
    control: &AlgorithmControl,
) -> Result<Validated, AlgorithmError> {
    control.checkpoint()?;
    if directed {
        return Err(execution(
            "prize-collecting Steiner tree requires an undirected graph",
        ));
    }
    if terminals.is_empty() {
        return Err(execution(
            "prize-collecting Steiner tree requires at least one terminal",
        ));
    }

    let mut node_uuids = clone_slice(nodes, "prize Steiner nodes")?;
    node_uuids.sort_unstable();
    if node_uuids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(execution("prize Steiner node UUIDs must be unique"));
    }
    let mut ordered_prizes = clone_slice(prize_mapping, "prize Steiner prize mapping")?;
    ordered_prizes.sort_unstable_by_key(|mapping| mapping.node_uuid);
    if ordered_prizes
        .windows(2)
        .any(|pair| pair[0].node_uuid == pair[1].node_uuid)
    {
        return Err(execution("prize Steiner prize UUIDs must be unique"));
    }
    if ordered_prizes.len() != node_uuids.len()
        || ordered_prizes
            .iter()
            .zip(&node_uuids)
            .any(|(mapping, node)| mapping.node_uuid != *node)
    {
        return Err(execution(
            "prize Steiner requires exactly one prize for every selected node",
        ));
    }
    let mut prizes = reserved_vec(ordered_prizes.len(), "prize Steiner prizes")?;
    for mapping in ordered_prizes {
        control.check_cancelled()?;
        prizes.push(mapping.prize.finite_nonnegative("prize Steiner prize")?);
    }

    let mut terminal_uuids = clone_slice(terminals, "prize Steiner terminals")?;
    terminal_uuids.sort_unstable();
    terminal_uuids.dedup();
    let mut terminal_indices = reserved_vec(terminal_uuids.len(), "prize Steiner terminals")?;
    for terminal in terminal_uuids {
        control.check_cancelled()?;
        terminal_indices.push(
            node_uuids
                .binary_search(&terminal)
                .map_err(|_| execution("prize Steiner terminal is outside node selection"))?,
        );
    }

    let mut ordered_edges = clone_slice(edges, "prize Steiner edges")?;
    ordered_edges.sort_unstable_by_key(|edge| edge.edge_uuid);
    if ordered_edges
        .windows(2)
        .any(|pair| pair[0].edge_uuid == pair[1].edge_uuid)
    {
        return Err(execution("prize Steiner edge UUIDs must be unique"));
    }
    let adjacency_entries = ordered_edges
        .len()
        .checked_mul(2)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| execution("prize Steiner adjacency size overflow"))?;
    control.check_graph_size(node_uuids.len(), adjacency_entries)?;
    let mut validated_edges = reserved_vec(ordered_edges.len(), "prize Steiner edges")?;
    for edge in ordered_edges {
        control.check_cancelled()?;
        let source = node_uuids
            .binary_search(&edge.source_uuid)
            .map_err(|_| execution("prize Steiner edge endpoint is outside node selection"))?;
        let target = node_uuids
            .binary_search(&edge.target_uuid)
            .map_err(|_| execution("prize Steiner edge endpoint is outside node selection"))?;
        let weight = edge.cost.finite_nonnegative("prize Steiner edge cost")?;
        if source != target {
            let (source_uuid, target_uuid) = if edge.source_uuid <= edge.target_uuid {
                (edge.source_uuid, edge.target_uuid)
            } else {
                (edge.target_uuid, edge.source_uuid)
            };
            validated_edges.push(Edge {
                output: PrizeSteinerEdge {
                    edge_uuid: edge.edge_uuid,
                    source_uuid,
                    target_uuid,
                    weight,
                },
                source,
                target,
            });
        }
    }
    Ok((node_uuids, prizes, validated_edges, terminal_indices))
}

#[allow(
    clippy::too_many_arguments,
    reason = "recursive exact search carries one immutable problem and two mutable search values"
)]
fn enumerate(
    edge: usize,
    nodes: &[[u8; 16]],
    prizes: &[f64],
    edges: &[Edge],
    terminals: &[usize],
    selected: &mut Vec<usize>,
    outcome: &mut SearchOutcome,
    control: &AlgorithmControl,
) -> Result<(), AlgorithmError> {
    control.check_cancelled()?;
    if edge == edges.len() {
        control.consume_states(1)?;
        match evaluate(nodes, prizes, edges, terminals, selected)? {
            Evaluation::Infeasible => {}
            Evaluation::Overflow => outcome.feasible = true,
            Evaluation::Candidate(candidate) => {
                outcome.feasible = true;
                if better(&candidate, outcome.best.as_ref(), edges) {
                    outcome.best = Some(candidate);
                }
            }
        }
        return Ok(());
    }

    enumerate(
        edge + 1,
        nodes,
        prizes,
        edges,
        terminals,
        selected,
        outcome,
        control,
    )?;
    selected.push(edge);
    enumerate(
        edge + 1,
        nodes,
        prizes,
        edges,
        terminals,
        selected,
        outcome,
        control,
    )?;
    selected.pop();
    Ok(())
}

fn evaluate(
    nodes: &[[u8; 16]],
    prizes: &[f64],
    edges: &[Edge],
    terminals: &[usize],
    selected: &[usize],
) -> Result<Evaluation, AlgorithmError> {
    let mut parent = reserved_vec(nodes.len(), "prize Steiner connectivity")?;
    parent.extend(0..nodes.len());
    let mut present = reserved_vec(nodes.len(), "prize Steiner selected nodes")?;
    present.resize(nodes.len(), false);
    for &terminal in terminals {
        present[terminal] = true;
    }
    let mut objective = Some(0.0);
    for &edge_index in selected {
        let edge = edges[edge_index];
        present[edge.source] = true;
        present[edge.target] = true;
        if !union(&mut parent, edge.source, edge.target) {
            return Ok(Evaluation::Infeasible);
        }
        objective = objective.and_then(|sum| finite_add(sum, edge.output.weight));
    }
    let root = find(&mut parent, terminals[0]);
    for (node, included) in present.iter().copied().enumerate() {
        if included && find(&mut parent, node) != root {
            return Ok(Evaluation::Infeasible);
        }
    }

    for (node, &prize) in prizes.iter().enumerate() {
        if !present[node] && terminals.binary_search(&node).is_err() {
            objective = objective.and_then(|sum| finite_add(sum, prize));
        }
    }
    let Some(objective) = objective else {
        return Ok(Evaluation::Overflow);
    };
    Ok(Evaluation::Candidate(Candidate {
        objective,
        edge_indices: clone_slice(selected, "prize Steiner candidate")?,
    }))
}

fn better(candidate: &Candidate, current: Option<&Candidate>, edges: &[Edge]) -> bool {
    let Some(current) = current else {
        return true;
    };
    candidate
        .objective
        .total_cmp(&current.objective)
        .then_with(|| {
            candidate
                .edge_indices
                .len()
                .cmp(&current.edge_indices.len())
        })
        .then_with(|| {
            candidate
                .edge_indices
                .iter()
                .map(|&edge| edges[edge].output.edge_uuid)
                .cmp(
                    current
                        .edge_indices
                        .iter()
                        .map(|&edge| edges[edge].output.edge_uuid),
                )
        })
        .is_lt()
}

fn find(parent: &mut [usize], mut node: usize) -> usize {
    while parent[node] != node {
        parent[node] = parent[parent[node]];
        node = parent[node];
    }
    node
}

fn union(parent: &mut [usize], left: usize, right: usize) -> bool {
    let left = find(parent, left);
    let right = find(parent, right);
    if left == right {
        return false;
    }
    parent[right] = left;
    true
}

fn subset_state_count(edges: usize) -> Result<u64, AlgorithmError> {
    let exponent = u32::try_from(edges).map_err(|_| AlgorithmError::StateOverflow)?;
    1_u64
        .checked_shl(exponent)
        .ok_or(AlgorithmError::StateOverflow)
}

fn finite_add(left: f64, right: f64) -> Option<f64> {
    let value = left + right;
    if value.is_finite() { Some(value) } else { None }
}

fn clone_slice<T: Clone>(values: &[T], context: &str) -> Result<Vec<T>, AlgorithmError> {
    let mut cloned = reserved_vec(values.len(), context)?;
    cloned.extend_from_slice(values);
    Ok(cloned)
}

fn reserved_vec<T>(capacity: usize, context: &str) -> Result<Vec<T>, AlgorithmError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| execution(format!("{context} allocation exceeds available memory")))?;
    Ok(values)
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

    fn number(value: f64) -> ResolvedNumber {
        ResolvedNumber::Float64(value)
    }

    fn node(id: u8, prize: f64) -> NodePrize {
        NodePrize {
            node_uuid: uuid(id),
            prize: number(prize),
        }
    }

    fn edge(id: u8, source: u8, target: u8, cost: f64) -> PrizeSteinerInputEdge {
        PrizeSteinerInputEdge {
            edge_uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
            cost: number(cost),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn solve(
        nodes: &[NodePrize],
        edges: &[PrizeSteinerInputEdge],
        terminals: &[u8],
    ) -> Result<Vec<PrizeSteinerEdge>, AlgorithmError> {
        prize_collecting_steiner_tree(
            &nodes.iter().map(|node| node.node_uuid).collect::<Vec<_>>(),
            nodes,
            edges,
            &terminals.iter().copied().map(uuid).collect::<Vec<_>>(),
            false,
            &control(),
        )
    }

    #[test]
    fn chooses_profitable_candidates_and_accounts_for_omitted_prizes() {
        let nodes = [node(1, 0.0), node(2, 3.0), node(3, 1.0), node(4, 20.0)];
        let edges = [
            edge(12, 1, 2, 2.0),
            edge(13, 1, 3, 2.0),
            edge(24, 2, 4, 9.0),
        ];
        let result = solve(&nodes, &edges, &[1]).unwrap();
        assert_eq!(
            result.iter().map(|edge| edge.edge_uuid).collect::<Vec<_>>(),
            [uuid(12), uuid(24)]
        );
    }

    #[test]
    fn one_terminal_may_return_zero_rows_and_multiple_terminals_must_connect() {
        assert!(solve(&[node(1, 0.0)], &[], &[1]).unwrap().is_empty());
        let nodes = [node(1, 0.0), node(2, 0.0), node(3, 100.0)];
        let edges = [edge(12, 1, 2, 4.0), edge(13, 1, 3, 1.0)];
        assert_eq!(solve(&nodes, &edges, &[2, 1, 2]).unwrap().len(), 2);
        assert!(matches!(
            solve(&nodes, &[edge(13, 1, 3, 1.0)], &[1, 2]),
            Err(AlgorithmError::Execution { message }) if message.contains("unreachable")
        ));
    }

    #[test]
    fn tie_breaks_by_edge_count_then_canonical_edge_uuid() {
        let nodes = [node(1, 0.0), node(2, 0.0), node(3, 0.0)];
        let fewer = [
            edge(20, 1, 2, 0.0),
            edge(30, 1, 3, 0.0),
            edge(31, 3, 2, 0.0),
        ];
        assert_eq!(
            solve(&nodes, &fewer, &[1, 2]).unwrap()[0].edge_uuid,
            uuid(20)
        );

        let parallel = [edge(9, 1, 2, 1.0), edge(8, 1, 2, 1.0)];
        assert_eq!(
            solve(&nodes[..2], &parallel, &[1, 2]).unwrap()[0].edge_uuid,
            uuid(8)
        );
    }

    #[test]
    fn loops_are_excluded_and_parallel_edges_remain_distinct() {
        let nodes = [node(1, 0.0), node(2, 4.0)];
        let edges = [edge(1, 1, 1, 0.0), edge(2, 1, 2, 2.0), edge(3, 1, 2, 3.0)];
        assert_eq!(solve(&nodes, &edges, &[1]).unwrap()[0].edge_uuid, uuid(2));
    }

    #[test]
    fn validates_direction_uuid_topology_terminals_and_typed_values() {
        let nodes = [node(1, 0.0), node(2, 1.0)];
        let edges = [edge(1, 1, 2, 1.0)];
        assert!(
            prize_collecting_steiner_tree(
                &[uuid(1), uuid(2)],
                &nodes,
                &edges,
                &[uuid(1)],
                true,
                &control()
            )
            .is_err()
        );
        assert!(solve(&[node(1, 0.0), node(1, 1.0)], &[], &[1]).is_err());
        assert!(solve(&nodes, &[edge(1, 1, 9, 1.0)], &[1]).is_err());
        assert!(solve(&nodes, &[edge(1, 1, 2, 1.0), edge(1, 1, 2, 2.0)], &[1]).is_err());
        assert!(solve(&nodes, &edges, &[]).is_err());
        assert!(solve(&nodes, &edges, &[9]).is_err());
        assert!(
            prize_collecting_steiner_tree(
                &[uuid(1), uuid(2)],
                &nodes[..1],
                &edges,
                &[uuid(1)],
                false,
                &control(),
            )
            .is_err()
        );
        assert!(
            prize_collecting_steiner_tree(&[uuid(1)], &nodes, &[], &[uuid(1)], false, &control(),)
                .is_err()
        );
        for invalid in [
            ResolvedNumber::Float64(f64::NAN),
            ResolvedNumber::Float64(f64::INFINITY),
            ResolvedNumber::Float64(-1.0),
        ] {
            let mut invalid_nodes = nodes;
            invalid_nodes[1].prize = invalid;
            assert!(solve(&invalid_nodes, &edges, &[1]).is_err());
            let mut invalid_edges = edges;
            invalid_edges[0].cost = invalid;
            assert!(solve(&nodes, &invalid_edges, &[1]).is_err());
        }
        assert!(
            solve(
                &[NodePrize {
                    node_uuid: uuid(1),
                    prize: ResolvedNumber::Float64(18_446_744_073_709_551_616.0)
                }],
                &[],
                &[1]
            )
            .is_ok()
        );
    }

    #[test]
    fn candidate_overflow_is_salvaged_but_all_overflow_is_structured() {
        let nodes = [node(1, 0.0), node(2, f64::MAX), node(3, f64::MAX)];
        let salvaged = solve(&nodes, &[edge(12, 1, 2, 0.0)], &[1]).unwrap();
        assert_eq!(
            salvaged
                .iter()
                .map(|edge| edge.edge_uuid)
                .collect::<Vec<_>>(),
            [uuid(12)]
        );
        assert!(matches!(
            solve(&nodes, &[], &[1]),
            Err(AlgorithmError::Execution { message }) if message.contains("not finite")
        ));
        assert!(matches!(
            solve(&nodes, &[], &[1, 2]),
            Err(AlgorithmError::Execution { message }) if message.contains("unreachable")
        ));
    }

    #[test]
    fn cancellation_graph_output_and_state_limits_are_atomic() {
        let nodes = [node(1, 0.0), node(2, 2.0)];
        let edges = [edge(1, 1, 2, 1.0)];
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            prize_collecting_steiner_tree(
                &[uuid(1), uuid(2)],
                &nodes,
                &edges,
                &[uuid(1)],
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
                states: 1,
                ..AlgorithmLimits::default()
            },
        ] {
            assert!(
                prize_collecting_steiner_tree(
                    &[uuid(1), uuid(2)],
                    &nodes,
                    &edges,
                    &[uuid(1)],
                    false,
                    &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
                )
                .is_err()
            );
        }
        assert_eq!(subset_state_count(64), Err(AlgorithmError::StateOverflow));
    }

    #[test]
    fn replay_is_stable_and_failed_retry_has_no_partial_result() {
        let nodes = [node(1, 0.0), node(2, 4.0), node(3, 3.0)];
        let edges = [edge(8, 1, 2, 1.0), edge(7, 2, 3, 1.0)];
        let first = solve(&nodes, &edges, &[1]).unwrap();
        let second = solve(&nodes, &edges, &[1]).unwrap();
        assert_eq!(first, second);
        let failed = prize_collecting_steiner_tree(
            &[uuid(1), uuid(2), uuid(3)],
            &nodes,
            &edges,
            &[uuid(1)],
            false,
            &AlgorithmControl::new(
                AlgorithmLimits {
                    states: 3,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
        );
        assert!(matches!(failed, Err(AlgorithmError::StateLimit { .. })));
        assert_eq!(solve(&nodes, &edges, &[1]).unwrap(), first);

        let reversed = [edge(8, 2, 1, 1.0), edge(7, 3, 2, 1.0)];
        let reversed_result = solve(&nodes, &reversed, &[1]).unwrap();
        assert_eq!(reversed_result, first);
        assert!(
            reversed_result
                .iter()
                .all(|edge| edge.source_uuid <= edge.target_uuid)
        );
    }

    #[test]
    fn independent_vertex_and_edge_oracle_matches_small_graphs() {
        for prize_mask in 0_u8..8 {
            let nodes = (0..4)
                .map(|index| node(index + 1, f64::from((prize_mask >> index.min(2)) & 1) * 2.0))
                .collect::<Vec<_>>();
            let edges = [
                edge(10, 1, 2, 1.0),
                edge(11, 2, 3, 1.0),
                edge(12, 3, 4, 1.0),
                edge(13, 1, 4, 2.0),
                edge(14, 1, 2, 1.0),
            ];
            let actual = solve(&nodes, &edges, &[1, 3]).unwrap();
            let expected = oracle(&nodes, &edges, &[1, 3]);
            assert_eq!(
                actual.iter().map(|edge| edge.edge_uuid).collect::<Vec<_>>(),
                expected
            );
        }
    }

    fn oracle(
        nodes: &[NodePrize],
        edges: &[PrizeSteinerInputEdge],
        terminals: &[u8],
    ) -> Vec<[u8; 16]> {
        let terminal_mask = terminals
            .iter()
            .fold(0_u64, |mask, id| mask | (1 << (id - 1)));
        let mut winner: Option<(f64, Vec<[u8; 16]>)> = None;
        for vertex_mask in 0_u64..(1 << nodes.len()) {
            if vertex_mask & terminal_mask != terminal_mask {
                continue;
            }
            for edge_mask in 0_u64..(1 << edges.len()) {
                let chosen = edges
                    .iter()
                    .enumerate()
                    .filter(|(index, edge)| {
                        edge_mask & (1 << index) != 0
                            && vertex_mask & (1 << (edge.source_uuid[0] - 1)) != 0
                            && vertex_mask & (1 << (edge.target_uuid[0] - 1)) != 0
                    })
                    .map(|(_, edge)| edge)
                    .collect::<Vec<_>>();
                if chosen.len() + 1 != vertex_mask.count_ones() as usize
                    || !oracle_connected(vertex_mask, &chosen)
                {
                    continue;
                }
                let edge_cost = chosen
                    .iter()
                    .map(|edge| match edge.cost {
                        ResolvedNumber::Float64(value) => value,
                        _ => unreachable!(),
                    })
                    .sum::<f64>();
                let omitted = nodes
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| vertex_mask & (1 << index) == 0)
                    .map(|(_, node)| match node.prize {
                        ResolvedNumber::Float64(value) => value,
                        _ => unreachable!(),
                    })
                    .sum::<f64>();
                let mut ids = chosen.iter().map(|edge| edge.edge_uuid).collect::<Vec<_>>();
                ids.sort_unstable();
                let candidate = (edge_cost + omitted, ids);
                if winner.as_ref().is_none_or(|best| {
                    candidate
                        .0
                        .total_cmp(&best.0)
                        .then_with(|| candidate.1.len().cmp(&best.1.len()))
                        .then_with(|| candidate.1.cmp(&best.1))
                        .is_lt()
                }) {
                    winner = Some(candidate);
                }
            }
        }
        winner.unwrap().1
    }

    fn oracle_connected(vertices: u64, edges: &[&PrizeSteinerInputEdge]) -> bool {
        let root = vertices.trailing_zeros() as u8 + 1;
        let mut reached = 1_u64 << (root - 1);
        loop {
            let before = reached;
            for edge in edges {
                let source = 1_u64 << (edge.source_uuid[0] - 1);
                let target = 1_u64 << (edge.target_uuid[0] - 1);
                if reached & source != 0 {
                    reached |= target;
                }
                if reached & target != 0 {
                    reached |= source;
                }
            }
            if reached == before {
                return reached == vertices;
            }
        }
    }
}
