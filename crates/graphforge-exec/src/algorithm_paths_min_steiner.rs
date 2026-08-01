//! Exact minimum-weight Steiner tree kernel for graph-native undirected inputs.
//!
//! This module deliberately contains no dispatch registration or Arrow shaping.
//! It returns stored edge identities in the canonical order required by the
//! dedicated Steiner edge schema.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_weighted_undirected::WeightedEdge;

const MAX_SEARCH_DEPTH: usize = 4_096;

/// One exact Steiner tree in canonical stored-edge UUID order.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MinimumSteinerTree {
    pub edges: Vec<WeightedEdge>,
    pub total_weight: f64,
}

/// Compute exactly one minimum-weight tree connecting all mandatory terminals.
///
/// Equal-cost solutions prefer fewer edges and then the lexicographically
/// smallest sorted edge-UUID sequence. Search completes before the result is
/// returned, so cancellation and every resource failure remain atomic.
pub(crate) fn minimum_steiner_tree(
    nodes: &[[u8; 16]],
    edges: &[WeightedEdge],
    terminals: &[[u8; 16]],
    control: &AlgorithmControl,
) -> Result<MinimumSteinerTree, AlgorithmError> {
    let adjacency_entries = direction_expanded_non_loop_adjacency_entries(edges)?;
    control.check_graph_size(nodes.len(), adjacency_entries)?;
    control.checkpoint()?;

    let prepared = prepare_input(nodes, edges, terminals, control)?;
    if !terminals_reachable(
        prepared.node_count,
        &prepared.edges,
        &prepared.node_index,
        &prepared.terminals,
        control,
    )? {
        return Err(disconnected());
    }

    let mut search = Search {
        nodes: prepared.node_count,
        edges: &prepared.edges,
        node_index: &prepared.node_index,
        terminals: &prepared.terminals,
        control,
        selected: Vec::new(),
        best: None,
        saw_overflow: false,
    };
    search
        .selected
        .try_reserve_exact(prepared.edges.len())
        .map_err(|_| allocation("minimum Steiner search path"))?;
    search.visit(0, 0.0)?;
    let best = match search.best {
        Some(best) => best,
        None if search.saw_overflow => {
            return Err(execution("minimum Steiner tree total cost overflowed"));
        }
        None => return Err(disconnected()),
    };
    control.check_output_rows(best.edges.len())?;
    Ok(best)
}

fn direction_expanded_non_loop_adjacency_entries(
    edges: &[WeightedEdge],
) -> Result<u64, AlgorithmError> {
    let non_loop_edges = edges
        .iter()
        .filter(|edge| edge.source_uuid != edge.target_uuid)
        .count();
    let non_loop_edges = u64::try_from(non_loop_edges)
        .map_err(|_| execution("minimum Steiner adjacency entry count overflow"))?;
    checked_direction_expanded_adjacency_entries(non_loop_edges)
}

fn checked_direction_expanded_adjacency_entries(
    non_loop_edges: u64,
) -> Result<u64, AlgorithmError> {
    non_loop_edges
        .checked_mul(2)
        .ok_or_else(|| execution("minimum Steiner adjacency entry count overflow"))
}

struct PreparedInput {
    node_count: usize,
    edges: Vec<WeightedEdge>,
    node_index: HashMap<[u8; 16], usize>,
    terminals: Vec<usize>,
}

fn prepare_input(
    nodes: &[[u8; 16]],
    edges: &[WeightedEdge],
    terminals: &[[u8; 16]],
    control: &AlgorithmControl,
) -> Result<PreparedInput, AlgorithmError> {
    let mut ordered_nodes = clone_fallibly(nodes, "minimum Steiner node index")?;
    ordered_nodes.sort_unstable();
    if ordered_nodes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(execution("minimum Steiner node UUIDs must be unique"));
    }

    let mut ordered_terminals = clone_fallibly(terminals, "minimum Steiner terminals")?;
    ordered_terminals.sort_unstable();
    if ordered_terminals.len() < 2 || ordered_terminals.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(execution(
            "minimum Steiner tree requires at least two distinct terminals",
        ));
    }
    for terminal in &ordered_terminals {
        control.check_cancelled()?;
        if ordered_nodes.binary_search(terminal).is_err() {
            return Err(execution(
                "minimum Steiner terminal is outside the selected graph",
            ));
        }
    }

    let mut node_index = HashMap::new();
    node_index
        .try_reserve(ordered_nodes.len())
        .map_err(|_| allocation("minimum Steiner node map"))?;
    node_index.extend(
        ordered_nodes
            .iter()
            .copied()
            .enumerate()
            .map(|(index, uuid)| (uuid, index)),
    );
    let mut stored_uuids = HashSet::new();
    stored_uuids
        .try_reserve(edges.len())
        .map_err(|_| allocation("minimum Steiner stored edge identities"))?;
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(edges.len())
        .map_err(|_| allocation("minimum Steiner edge candidates"))?;
    for &raw in edges {
        control.check_cancelled()?;
        if !raw.weight.is_finite() || raw.weight < 0.0 {
            return Err(execution(
                "minimum Steiner tree requires finite nonnegative edge costs",
            ));
        }
        if !node_index.contains_key(&raw.source_uuid) || !node_index.contains_key(&raw.target_uuid)
        {
            return Err(execution(
                "minimum Steiner edge endpoint is outside the selected graph",
            ));
        }
        if !stored_uuids.insert(raw.edge_uuid) {
            return Err(execution(
                "minimum Steiner stored edge UUIDs must be distinct",
            ));
        }
        if raw.source_uuid != raw.target_uuid {
            let mut edge = raw;
            if edge.target_uuid < edge.source_uuid {
                std::mem::swap(&mut edge.source_uuid, &mut edge.target_uuid);
            }
            candidates.push(edge);
        }
    }
    candidates.sort_by_key(|edge| edge.edge_uuid);
    if candidates.len() > MAX_SEARCH_DEPTH {
        return Err(execution(format!(
            "minimum Steiner search depth limit exceeded: observed {}, limit {MAX_SEARCH_DEPTH}",
            candidates.len()
        )));
    }

    let mut terminal_indices = Vec::new();
    terminal_indices
        .try_reserve_exact(ordered_terminals.len())
        .map_err(|_| allocation("minimum Steiner terminal indices"))?;
    terminal_indices.extend(
        ordered_terminals
            .iter()
            .map(|terminal| node_index[terminal]),
    );
    Ok(PreparedInput {
        node_count: ordered_nodes.len(),
        edges: candidates,
        node_index,
        terminals: terminal_indices,
    })
}

struct Search<'a> {
    nodes: usize,
    edges: &'a [WeightedEdge],
    node_index: &'a HashMap<[u8; 16], usize>,
    terminals: &'a [usize],
    control: &'a AlgorithmControl,
    selected: Vec<WeightedEdge>,
    best: Option<MinimumSteinerTree>,
    saw_overflow: bool,
}

impl Search<'_> {
    fn visit(&mut self, next: usize, total_weight: f64) -> Result<(), AlgorithmError> {
        self.control.consume_states(1)?;
        self.control.checkpoint()?;

        let status = selected_status(self.nodes, &self.selected, self.node_index, self.terminals)?;
        if status.has_cycle {
            return Ok(());
        }
        if status.is_tree {
            self.consider(total_weight)?;
            return Ok(());
        }
        if next == self.edges.len() || self.cannot_improve(total_weight) {
            return Ok(());
        }

        self.visit(next + 1, total_weight)?;

        let edge = self.edges[next];
        let included_weight = total_weight + edge.weight;
        if !included_weight.is_finite() {
            self.saw_overflow = true;
            return Ok(());
        }
        self.selected.push(edge);
        let result = self.visit(next + 1, included_weight);
        self.selected.pop();
        result
    }

    fn cannot_improve(&self, total_weight: f64) -> bool {
        self.best.as_ref().is_some_and(|best| {
            total_weight > best.total_weight
                || (total_weight.total_cmp(&best.total_weight).is_eq()
                    && self.selected.len() >= best.edges.len())
        })
    }

    fn consider(&mut self, total_weight: f64) -> Result<(), AlgorithmError> {
        let mut edges = clone_fallibly(&self.selected, "minimum Steiner result")?;
        edges.sort_by_key(|edge| edge.edge_uuid);
        let candidate = MinimumSteinerTree {
            edges,
            total_weight,
        };
        if self
            .best
            .as_ref()
            .is_none_or(|best| compare_trees(&candidate, best).is_lt())
        {
            self.best = Some(candidate);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct SelectedStatus {
    has_cycle: bool,
    is_tree: bool,
}

fn selected_status(
    node_count: usize,
    edges: &[WeightedEdge],
    node_index: &HashMap<[u8; 16], usize>,
    terminals: &[usize],
) -> Result<SelectedStatus, AlgorithmError> {
    let mut parent = fallible_sequence(node_count, "minimum Steiner disjoint set")?;
    let mut used = Vec::new();
    used.try_reserve_exact(node_count)
        .map_err(|_| allocation("minimum Steiner used-node flags"))?;
    used.resize(node_count, false);
    for edge in edges {
        let source = node_index[&edge.source_uuid];
        let target = node_index[&edge.target_uuid];
        used[source] = true;
        used[target] = true;
        let source_root = find(&mut parent, source);
        let target_root = find(&mut parent, target);
        if source_root == target_root {
            return Ok(SelectedStatus {
                has_cycle: true,
                is_tree: false,
            });
        }
        parent[target_root] = source_root;
    }
    let root = find(&mut parent, terminals[0]);
    let terminals_connected = terminals
        .iter()
        .copied()
        .all(|terminal| find(&mut parent, terminal) == root);
    let all_edges_in_terminal_component = used
        .iter()
        .enumerate()
        .all(|(node, &is_used)| !is_used || find(&mut parent, node) == root);
    Ok(SelectedStatus {
        has_cycle: false,
        is_tree: terminals_connected && all_edges_in_terminal_component,
    })
}

fn terminals_reachable(
    node_count: usize,
    edges: &[WeightedEdge],
    node_index: &HashMap<[u8; 16], usize>,
    terminals: &[usize],
    control: &AlgorithmControl,
) -> Result<bool, AlgorithmError> {
    let mut parent = fallible_sequence(node_count, "minimum Steiner reachability")?;
    for edge in edges {
        control.check_cancelled()?;
        let source = find(&mut parent, node_index[&edge.source_uuid]);
        let target = find(&mut parent, node_index[&edge.target_uuid]);
        parent[target] = source;
    }
    let root = find(&mut parent, terminals[0]);
    Ok(terminals
        .iter()
        .copied()
        .all(|terminal| find(&mut parent, terminal) == root))
}

fn find(parent: &mut [usize], mut node: usize) -> usize {
    while parent[node] != node {
        parent[node] = parent[parent[node]];
        node = parent[node];
    }
    node
}

fn compare_trees(left: &MinimumSteinerTree, right: &MinimumSteinerTree) -> Ordering {
    left.total_weight
        .total_cmp(&right.total_weight)
        .then_with(|| left.edges.len().cmp(&right.edges.len()))
        .then_with(|| {
            left.edges
                .iter()
                .map(|edge| edge.edge_uuid)
                .cmp(right.edges.iter().map(|edge| edge.edge_uuid))
        })
}

fn clone_fallibly<T: Clone>(values: &[T], name: &str) -> Result<Vec<T>, AlgorithmError> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(values.len())
        .map_err(|_| allocation(name))?;
    cloned.extend_from_slice(values);
    Ok(cloned)
}

fn fallible_sequence(len: usize, name: &str) -> Result<Vec<usize>, AlgorithmError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| allocation(name))?;
    values.extend(0..len);
    Ok(values)
}

fn allocation(name: &str) -> AlgorithmError {
    execution(format!("{name} allocation exceeds available memory"))
}

fn disconnected() -> AlgorithmError {
    execution("minimum Steiner tree is undefined: mandatory terminals are disconnected")
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

    fn edge(id: u8, source: u8, target: u8, weight: f64) -> WeightedEdge {
        WeightedEdge {
            edge_uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
            weight,
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn edge_ids(tree: &MinimumSteinerTree) -> Vec<[u8; 16]> {
        tree.edges.iter().map(|edge| edge.edge_uuid).collect()
    }

    #[test]
    fn chooses_an_exact_steiner_node_and_canonical_edge_rows() {
        let tree = minimum_steiner_tree(
            &[uuid(1), uuid(2), uuid(3), uuid(4)],
            &[
                edge(11, 1, 2, 3.0),
                edge(12, 2, 3, 3.0),
                edge(13, 1, 3, 3.0),
                edge(4, 1, 4, 1.0),
                edge(5, 2, 4, 1.0),
                edge(6, 3, 4, 1.0),
            ],
            &[uuid(1), uuid(2), uuid(3)],
            &control(),
        )
        .unwrap();
        assert_eq!(tree.total_weight, 3.0);
        assert_eq!(edge_ids(&tree), [uuid(4), uuid(5), uuid(6)]);
        assert!(
            tree.edges
                .iter()
                .all(|edge| edge.source_uuid <= edge.target_uuid)
        );
    }

    #[test]
    fn refines_equal_cost_by_edge_count_then_edge_uuid() {
        let tree = minimum_steiner_tree(
            &[uuid(1), uuid(2), uuid(3)],
            &[
                edge(9, 1, 3, 2.0),
                edge(1, 1, 2, 1.0),
                edge(2, 2, 3, 1.0),
                edge(8, 1, 3, 2.0),
            ],
            &[uuid(1), uuid(3)],
            &control(),
        )
        .unwrap();
        assert_eq!(edge_ids(&tree), [uuid(8)]);

        let parallel = minimum_steiner_tree(
            &[uuid(1), uuid(2)],
            &[edge(7, 1, 2, 1.0), edge(6, 2, 1, 1.0)],
            &[uuid(1), uuid(2)],
            &control(),
        )
        .unwrap();
        assert_eq!(edge_ids(&parallel), [uuid(6)]);
    }

    #[test]
    fn agrees_with_an_independent_small_graph_oracle() {
        let nodes = [uuid(1), uuid(2), uuid(3), uuid(4)];
        let edges = [
            edge(1, 1, 2, 1.0),
            edge(2, 2, 3, 2.0),
            edge(3, 3, 4, 1.0),
            edge(4, 1, 4, 3.0),
            edge(5, 2, 4, 1.5),
            edge(6, 1, 3, 2.5),
        ];
        for terminal_values in [&[1, 3][..], &[1, 4], &[1, 3, 4], &[1, 2, 3, 4]] {
            let terminals = terminal_values
                .iter()
                .copied()
                .map(uuid)
                .collect::<Vec<_>>();
            let actual = minimum_steiner_tree(&nodes, &edges, &terminals, &control()).unwrap();
            let expected = oracle(&nodes, &edges, &terminals);
            assert_eq!(
                (actual.total_weight, edge_ids(&actual)),
                (expected.total_weight, edge_ids(&expected))
            );
        }
    }

    #[test]
    fn loops_are_ignored_and_disconnected_or_invalid_inputs_are_atomic() {
        let tree = minimum_steiner_tree(
            &[uuid(1), uuid(2)],
            &[edge(1, 1, 1, 0.0), edge(2, 1, 2, 4.0)],
            &[uuid(1), uuid(2)],
            &control(),
        )
        .unwrap();
        assert_eq!(edge_ids(&tree), [uuid(2)]);

        let invalid_cases = [
            minimum_steiner_tree(&[uuid(1), uuid(2)], &[], &[uuid(1), uuid(2)], &control()),
            minimum_steiner_tree(
                &[uuid(1), uuid(2)],
                &[edge(1, 1, 2, -1.0)],
                &[uuid(1), uuid(2)],
                &control(),
            ),
            minimum_steiner_tree(
                &[uuid(1), uuid(2)],
                &[edge(1, 1, 2, f64::NAN)],
                &[uuid(1), uuid(2)],
                &control(),
            ),
            minimum_steiner_tree(
                &[uuid(1), uuid(2)],
                &[edge(1, 1, 2, 1.0), edge(1, 1, 2, 1.0)],
                &[uuid(1), uuid(2)],
                &control(),
            ),
            minimum_steiner_tree(
                &[uuid(1), uuid(2)],
                &[edge(1, 1, 3, 1.0)],
                &[uuid(1), uuid(2)],
                &control(),
            ),
            minimum_steiner_tree(
                &[uuid(1), uuid(2)],
                &[edge(1, 1, 2, 1.0)],
                &[uuid(1), uuid(1)],
                &control(),
            ),
        ];
        assert!(invalid_cases.into_iter().all(|result| result.is_err()));
    }

    #[test]
    fn overflow_cancellation_and_limits_fail_atomically_then_retry() {
        assert_eq!(
            minimum_steiner_tree(
                &[uuid(1), uuid(2), uuid(3)],
                &[edge(1, 1, 2, f64::MAX), edge(2, 2, 3, f64::MAX)],
                &[uuid(1), uuid(3)],
                &control(),
            ),
            Err(execution("minimum Steiner tree total cost overflowed"))
        );
        let finite = minimum_steiner_tree(
            &[uuid(1), uuid(2), uuid(3)],
            &[
                edge(1, 1, 3, 2.0),
                edge(2, 1, 2, f64::MAX),
                edge(3, 2, 3, f64::MAX),
            ],
            &[uuid(1), uuid(3)],
            &control(),
        )
        .unwrap();
        assert_eq!(
            (finite.total_weight, edge_ids(&finite)),
            (2.0, vec![uuid(1)])
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            minimum_steiner_tree(
                &[uuid(1), uuid(2)],
                &[edge(1, 1, 2, 1.0)],
                &[uuid(1), uuid(2)],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            ),
            Err(AlgorithmError::Cancelled)
        );

        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                states: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            minimum_steiner_tree(
                &[uuid(1), uuid(2)],
                &[edge(1, 1, 2, 1.0)],
                &[uuid(1), uuid(2)],
                &limited,
            ),
            Err(AlgorithmError::StateLimit { .. })
        ));
        let retry = minimum_steiner_tree(
            &[uuid(1), uuid(2)],
            &[edge(1, 1, 2, 1.0)],
            &[uuid(1), uuid(2)],
            &control(),
        )
        .unwrap();
        assert_eq!(edge_ids(&retry), [uuid(1)]);

        let output_limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            minimum_steiner_tree(
                &[uuid(1), uuid(2)],
                &[edge(1, 1, 2, 1.0)],
                &[uuid(1), uuid(2)],
                &output_limited,
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        for limits in [
            AlgorithmLimits {
                nodes: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                edges: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
        ] {
            assert!(
                minimum_steiner_tree(
                    &[uuid(1), uuid(2)],
                    &[edge(1, 1, 2, 1.0)],
                    &[uuid(1), uuid(2)],
                    &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
                )
                .is_err()
            );
        }
    }

    #[test]
    fn edge_limit_counts_direction_expanded_non_loop_adjacency() {
        assert_eq!(
            checked_direction_expanded_adjacency_entries(u64::MAX),
            Err(execution("minimum Steiner adjacency entry count overflow"))
        );

        let one_stored_edge = [edge(1, 1, 2, 1.0)];
        let adjacency_limited = AlgorithmControl::new(
            AlgorithmLimits {
                edges: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            minimum_steiner_tree(
                &[uuid(1), uuid(2)],
                &one_stored_edge,
                &[uuid(1), uuid(2)],
                &adjacency_limited,
            ),
            Err(AlgorithmError::EdgeLimit {
                observed: 2,
                limit: 1,
            })
        );

        let direction_expanded_limit = AlgorithmControl::new(
            AlgorithmLimits {
                edges: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        let with_ignored_loop = minimum_steiner_tree(
            &[uuid(1), uuid(2)],
            &[edge(2, 1, 1, 0.0), one_stored_edge[0]],
            &[uuid(1), uuid(2)],
            &direction_expanded_limit,
        )
        .unwrap();
        assert_eq!(edge_ids(&with_ignored_loop), [uuid(1)]);
    }

    #[test]
    fn deterministic_replay_is_independent_of_input_order() {
        let nodes = [uuid(3), uuid(1), uuid(2)];
        let edges = [edge(3, 2, 3, 1.0), edge(2, 1, 3, 1.0), edge(1, 1, 2, 1.0)];
        let expected =
            minimum_steiner_tree(&nodes, &edges, &[uuid(3), uuid(1), uuid(2)], &control()).unwrap();
        let replay = minimum_steiner_tree(
            &[uuid(2), uuid(3), uuid(1)],
            &[edges[2], edges[0], edges[1]],
            &[uuid(2), uuid(1), uuid(3)],
            &control(),
        )
        .unwrap();
        assert_eq!(replay, expected);
    }

    fn oracle(
        nodes: &[[u8; 16]],
        edges: &[WeightedEdge],
        terminals: &[[u8; 16]],
    ) -> MinimumSteinerTree {
        let node_index = nodes
            .iter()
            .copied()
            .enumerate()
            .map(|(index, uuid)| (uuid, index))
            .collect::<HashMap<_, _>>();
        let mut best: Option<MinimumSteinerTree> = None;
        for mask in 0_u64..(1_u64 << edges.len()) {
            let mut selected = edges
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1 << index) != 0)
                .map(|(_, edge)| *edge)
                .collect::<Vec<_>>();
            let mut parent = (0..nodes.len()).collect::<Vec<_>>();
            let mut used = vec![false; nodes.len()];
            let mut acyclic = true;
            for edge in &selected {
                let source = node_index[&edge.source_uuid];
                let target = node_index[&edge.target_uuid];
                used[source] = true;
                used[target] = true;
                let source_root = oracle_find(&mut parent, source);
                let target_root = oracle_find(&mut parent, target);
                if source_root == target_root {
                    acyclic = false;
                    break;
                }
                parent[target_root] = source_root;
            }
            let root = oracle_find(&mut parent, node_index[&terminals[0]]);
            let covers_terminals = terminals
                .iter()
                .all(|terminal| oracle_find(&mut parent, node_index[terminal]) == root);
            let connected_tree = used
                .iter()
                .enumerate()
                .all(|(node, &present)| !present || oracle_find(&mut parent, node) == root);
            if !acyclic || !covers_terminals || !connected_tree {
                continue;
            }
            selected.sort_by_key(|edge| edge.edge_uuid);
            let candidate = MinimumSteinerTree {
                total_weight: selected.iter().map(|edge| edge.weight).sum(),
                edges: selected,
            };
            let is_better = best.as_ref().is_none_or(|current| {
                candidate
                    .total_weight
                    .total_cmp(&current.total_weight)
                    .then_with(|| candidate.edges.len().cmp(&current.edges.len()))
                    .then_with(|| {
                        candidate
                            .edges
                            .iter()
                            .map(|edge| edge.edge_uuid)
                            .cmp(current.edges.iter().map(|edge| edge.edge_uuid))
                    })
                    .is_lt()
            });
            if is_better {
                best = Some(candidate);
            }
        }
        best.unwrap()
    }

    fn oracle_find(parent: &mut [usize], mut node: usize) -> usize {
        while parent[node] != node {
            node = parent[node];
        }
        node
    }
}
