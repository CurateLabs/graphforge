//! Yen's k-shortest paths remain serial (#555). Each accepted path defines the
//! next spur candidates, and the global candidate map is drained in canonical
//! rank order, so iterations are not independent private-pool work.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

const CHECKPOINT_INTERVAL: usize = 4_096;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct YenPath {
    pub cost: f64,
    pub nodes: Vec<u64>,
    pub edge_ids: Vec<u64>,
}

#[derive(Clone, Debug)]
struct PathState(YenPath);

impl PartialEq for PathState {
    fn eq(&self, other: &Self) -> bool {
        compare_paths(&self.0, &other.0) == Ordering::Equal
    }
}

impl Eq for PathState {}

impl Ord for PathState {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_paths(&other.0, &self.0)
    }
}

impl PartialOrd for PathState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Return up to `k` exact, distinct, loopless paths in stable rank order.
pub(crate) fn exact_yens(
    graph: &AdjacencyGraph,
    source: u64,
    target: u64,
    k: usize,
    control: &AlgorithmControl,
) -> Result<Vec<YenPath>, AlgorithmError> {
    control.checkpoint()?;
    if k == 0 {
        return Err(execution("yens k must be at least 1"));
    }
    if !graph.node_ids().contains(&source) {
        return Err(execution("yens source is outside node selection"));
    }
    if !graph.node_ids().contains(&target) {
        return Err(execution("yens target is outside node selection"));
    }

    let mut work = 0_usize;
    validate_weights(graph, control, &mut work)?;
    if source == target {
        control.check_output_rows(1)?;
        return Ok(vec![YenPath {
            cost: 0.0,
            nodes: vec![source],
            edge_ids: Vec::new(),
        }]);
    }

    let Some(first) = constrained_shortest_path(
        graph,
        source,
        target,
        &HashSet::new(),
        &HashSet::new(),
        control,
        &mut work,
    )?
    else {
        return Ok(Vec::new());
    };
    control.check_output_rows(1)?;
    let mut accepted = vec![first];
    let mut candidates: HashMap<Vec<u64>, YenPath> = HashMap::new();

    while accepted.len() < k {
        let previous = accepted
            .last()
            .expect("Yen search starts with one accepted path")
            .clone();
        enqueue_spur_candidates(
            graph,
            target,
            &previous,
            &accepted,
            &mut candidates,
            control,
            &mut work,
        )?;

        let mut next_key: Option<&Vec<u64>> = None;
        for (nodes, candidate) in &candidates {
            checkpoint(control, &mut work)?;
            if next_key.is_none_or(|current| {
                compare_paths(candidate, &candidates[current]) == Ordering::Less
            }) {
                next_key = Some(nodes);
            }
        }
        let Some(next_key) = next_key.cloned() else {
            break;
        };
        control.check_output_rows(accepted.len().saturating_add(1))?;
        accepted.push(
            candidates
                .remove(&next_key)
                .expect("selected Yen candidate remains present"),
        );
    }
    control.check_cancelled()?;
    Ok(accepted)
}

#[allow(clippy::too_many_arguments, reason = "Yen spur state is explicit")]
fn enqueue_spur_candidates(
    graph: &AdjacencyGraph,
    target: u64,
    previous: &YenPath,
    accepted: &[YenPath],
    candidates: &mut HashMap<Vec<u64>, YenPath>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    let mut root_cost = 0.0;
    for spur_index in 0..previous.nodes.len() - 1 {
        checkpoint(control, work)?;
        let spur = previous.nodes[spur_index];
        let root_nodes = &previous.nodes[..=spur_index];
        let root_edges = &previous.edge_ids[..spur_index];
        let banned_nodes = root_nodes[..spur_index].iter().copied().collect();
        let mut banned_arcs = HashSet::new();
        for path in accepted {
            checkpoint(control, work)?;
            if path.nodes.len() > spur_index + 1 && path.nodes[..=spur_index] == *root_nodes {
                banned_arcs.insert((path.nodes[spur_index], path.nodes[spur_index + 1]));
            }
        }
        if let Some(spur_path) = constrained_shortest_path(
            graph,
            spur,
            target,
            &banned_nodes,
            &banned_arcs,
            control,
            work,
        )? {
            let candidate = join_root_and_spur(root_nodes, root_edges, root_cost, &spur_path)?;
            let mut already_accepted = false;
            for path in accepted {
                checkpoint(control, work)?;
                already_accepted |= path.nodes == candidate.nodes;
            }
            if !already_accepted {
                candidates
                    .entry(candidate.nodes.clone())
                    .and_modify(|current| {
                        if compare_paths(&candidate, current) == Ordering::Less {
                            *current = candidate.clone();
                        }
                    })
                    .or_insert(candidate);
            }
        }
        root_cost += edge_weight(
            graph,
            previous.nodes[spur_index],
            previous.nodes[spur_index + 1],
            previous.edge_ids[spur_index],
        )?;
        if !root_cost.is_finite() {
            return Err(execution("yens accumulated cost is not finite"));
        }
    }
    Ok(())
}

fn join_root_and_spur(
    root_nodes: &[u64],
    root_edges: &[u64],
    root_cost: f64,
    spur_path: &YenPath,
) -> Result<YenPath, AlgorithmError> {
    let cost = root_cost + spur_path.cost;
    if !cost.is_finite() {
        return Err(execution("yens accumulated cost is not finite"));
    }
    let mut nodes = root_nodes[..root_nodes.len() - 1].to_vec();
    nodes.extend_from_slice(&spur_path.nodes);
    let mut edge_ids = root_edges.to_vec();
    edge_ids.extend_from_slice(&spur_path.edge_ids);
    Ok(YenPath {
        cost,
        nodes,
        edge_ids,
    })
}

fn constrained_shortest_path(
    graph: &AdjacencyGraph,
    source: u64,
    target: u64,
    banned_nodes: &HashSet<u64>,
    banned_arcs: &HashSet<(u64, u64)>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Option<YenPath>, AlgorithmError> {
    if banned_nodes.contains(&source) || banned_nodes.contains(&target) {
        return Ok(None);
    }
    let mut heap = BinaryHeap::from([PathState(YenPath {
        cost: 0.0,
        nodes: vec![source],
        edge_ids: Vec::new(),
    })]);

    while let Some(PathState(path)) = heap.pop() {
        checkpoint(control, work)?;
        let node = *path.nodes.last().expect("candidate paths are non-empty");
        if node == target {
            return Ok(Some(path));
        }
        for edge in graph.neighbors(node) {
            checkpoint(control, work)?;
            if banned_nodes.contains(&edge.neighbor_id)
                || banned_arcs.contains(&(node, edge.neighbor_id))
                || path.nodes.contains(&edge.neighbor_id)
            {
                continue;
            }
            let cost = path.cost + edge.weight;
            if !cost.is_finite() {
                return Err(execution("yens accumulated cost is not finite"));
            }
            let mut nodes = path.nodes.clone();
            nodes.push(edge.neighbor_id);
            let mut edge_ids = path.edge_ids.clone();
            edge_ids.push(edge.edge_id);
            heap.push(PathState(YenPath {
                cost,
                nodes,
                edge_ids,
            }));
        }
    }
    Ok(None)
}

fn validate_weights(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    for &node in graph.node_ids() {
        for edge in graph.neighbors(node) {
            checkpoint(control, work)?;
            if !edge.weight.is_finite() || edge.weight < 0.0 {
                return Err(execution("yens requires finite non-negative edge weights"));
            }
        }
    }
    Ok(())
}

fn edge_weight(
    graph: &AdjacencyGraph,
    source: u64,
    target: u64,
    edge_id: u64,
) -> Result<f64, AlgorithmError> {
    graph
        .neighbors(source)
        .iter()
        .find(|edge| edge.neighbor_id == target && edge.edge_id == edge_id)
        .map(|edge| edge.weight)
        .ok_or_else(|| execution("yens accepted path references a missing edge"))
}

fn compare_paths(left: &YenPath, right: &YenPath) -> Ordering {
    left.cost
        .total_cmp(&right.cost)
        .then_with(|| left.nodes.cmp(&right.nodes))
        .then_with(|| left.edge_ids.cmp(&right.edge_ids))
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
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

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn signatures(paths: &[YenPath]) -> Vec<(f64, &[u64], &[u64])> {
        paths
            .iter()
            .map(|path| (path.cost, path.nodes.as_slice(), path.edge_ids.as_slice()))
            .collect()
    }

    #[test]
    fn exact_paths_are_ranked_by_cost_full_nodes_then_full_edges() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            6,
            &[(0, 2), (2, 4), (0, 1), (1, 4), (0, 3), (3, 4), (1, 2)],
        )
        .with_test_edge_weights(&[1.0, 2.0, 1.0, 2.0, 2.0, 2.0, 0.5]);

        let paths = exact_yens(&graph, 0, 4, 10, &control()).unwrap();
        assert_eq!(
            signatures(&paths),
            vec![
                (3.0, &[0, 1, 4][..], &[2, 3][..]),
                (3.0, &[0, 2, 4][..], &[0, 1][..]),
                (3.5, &[0, 1, 2, 4][..], &[2, 6, 1][..]),
                (4.0, &[0, 3, 4][..], &[4, 5][..]),
            ]
        );
        assert_eq!(paths, exact_yens(&graph, 0, 4, 10, &control()).unwrap());
        assert_eq!(exact_yens(&graph, 0, 4, 2, &control()).unwrap(), paths[..2]);
    }

    #[test]
    fn parallel_edges_choose_one_public_node_path_without_starving_others() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            4,
            &[(0, 1), (0, 1), (1, 3), (0, 2), (2, 3), (0, 0), (1, 0)],
        )
        .with_test_edge_weights(&[4.0, 1.0, 1.0, 1.0, 2.0, 0.0, 0.0]);
        let paths = exact_yens(&graph, 0, 3, 5, &control()).unwrap();

        assert_eq!(
            signatures(&paths),
            vec![
                (2.0, &[0, 1, 3][..], &[1, 2][..]),
                (3.0, &[0, 2, 3][..], &[3, 4][..]),
            ]
        );
        assert!(paths.iter().all(|path| {
            path.nodes.iter().copied().collect::<HashSet<_>>().len() == path.nodes.len()
        }));

        let equal_parallel = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1), (0, 1)])
            .with_test_edge_weights(&[1.0, 1.0]);
        assert_eq!(
            exact_yens(&equal_parallel, 0, 1, 2, &control()).unwrap(),
            vec![YenPath {
                cost: 1.0,
                nodes: vec![0, 1],
                edge_ids: vec![0],
            }]
        );
    }

    #[test]
    fn boundaries_weights_and_structured_failures_are_exact() {
        let disconnected = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1)]);
        assert!(
            exact_yens(&disconnected, 0, 2, 3, &control())
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            exact_yens(&disconnected, 1, 1, 3, &control()).unwrap(),
            vec![YenPath {
                cost: 0.0,
                nodes: vec![1],
                edge_ids: vec![],
            }]
        );
        assert!(matches!(
            exact_yens(&disconnected, 0, 2, 0, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            exact_yens(&disconnected, 9, 2, 1, &control()),
            Err(AlgorithmError::Execution { .. })
        ));

        for weight in [-1.0, f64::NAN, f64::INFINITY] {
            let invalid = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)])
                .with_test_edge_weights(&[weight]);
            assert!(matches!(
                exact_yens(&invalid, 0, 1, 1, &control()),
                Err(AlgorithmError::Execution { .. })
            ));
        }

        let overflowing = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)])
            .with_test_edge_weights(&[f64::MAX, f64::MAX]);
        assert!(matches!(
            exact_yens(&overflowing, 0, 2, 1, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn limits_and_cancellation_return_no_partial_paths() {
        let graph = AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (1, 3), (0, 2), (2, 3)]);
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert_eq!(
            exact_yens(&graph, 0, 3, 2, &limited).unwrap_err(),
            AlgorithmError::OutputLimit {
                observed: 2,
                limit: 1,
            }
        );

        let cancelled = AlgorithmCancellation::default();
        cancelled.cancel();
        assert_eq!(
            exact_yens(
                &graph,
                0,
                3,
                2,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancelled),
            )
            .unwrap_err(),
            AlgorithmError::Cancelled
        );
    }

    #[test]
    fn work_limit_covers_large_spur_searches() {
        let graph = AdjacencyGraph::with_test_counts(65, 4_096);
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_yens(&graph, 0, 64, 2, &limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }
}
