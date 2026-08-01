use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_analyze_dag_longest_path::DagLongestPath;
use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

/// One stored weighted directed edge in the selected UUID projection.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct WeightedDagEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
    pub weight: f64,
}

/// Compute the exact global maximum-weight path through a directed DAG.
///
/// Identical stored-edge entries collapse. Distinct parallel arcs retain only
/// their maximum weight. Every selected node is an eligible zero-cost path.
pub(crate) fn weighted_dag_longest_path(
    nodes: &[[u8; 16]],
    edges: &[WeightedDagEdge],
    control: &AlgorithmControl,
) -> Result<DagLongestPath, AlgorithmError> {
    control.checkpoint()?;
    control.check_output_rows(1)?;

    let mut work = 0_usize;
    let nodes = index_nodes(nodes, control, &mut work)?;
    let mut projection = normalize_edges(edges, &nodes.positions, control, &mut work)?;
    let mut ready = projection
        .indegrees
        .iter()
        .enumerate()
        .filter_map(|(node, &indegree)| (indegree == 0).then_some(node))
        .collect::<BTreeSet<_>>();
    let mut costs = vec![0.0_f64; nodes.ordered.len()];
    let mut paths = nodes
        .ordered
        .iter()
        .copied()
        .map(|uuid| vec![uuid])
        .collect::<Vec<_>>();
    let mut processed = 0_usize;

    while let Some(&source) = ready.first() {
        checkpoint(control, &mut work)?;
        ready.remove(&source);
        processed = processed.checked_add(1).ok_or_else(|| {
            execution("dag_longest_path_weighted node count exceeds platform range")
        })?;

        for (&target, &weight) in &projection.neighbors[source] {
            checkpoint(control, &mut work)?;
            let candidate_cost = costs[source] + weight;
            if !candidate_cost.is_finite() {
                return Err(execution(
                    "dag_longest_path_weighted accumulated cost must be finite",
                ));
            }
            let mut candidate_path = paths[source].clone();
            candidate_path.push(nodes.ordered[target]);
            let comparison = candidate_cost.total_cmp(&costs[target]);
            if comparison == Ordering::Greater
                || (comparison == Ordering::Equal && candidate_path < paths[target])
            {
                costs[target] = candidate_cost;
                paths[target] = candidate_path;
            }

            projection.indegrees[target] = projection.indegrees[target]
                .checked_sub(1)
                .ok_or_else(|| execution("dag_longest_path_weighted indegree underflow"))?;
            if projection.indegrees[target] == 0 {
                ready.insert(target);
            }
        }
    }

    if processed != nodes.ordered.len() {
        return Err(execution(
            "dag_longest_path_weighted requires a directed acyclic graph",
        ));
    }

    let mut best: Option<(f64, Vec<[u8; 16]>)> = None;
    for (cost, path) in costs.into_iter().zip(paths) {
        checkpoint(control, &mut work)?;
        if best.as_ref().is_none_or(|(best_cost, best_path)| {
            let comparison = cost.total_cmp(best_cost);
            comparison == Ordering::Greater || (comparison == Ordering::Equal && path < *best_path)
        }) {
            best = Some((cost, path));
        }
    }
    let Some((cost, path)) = best else {
        return Ok(DagLongestPath {
            cost: 0.0,
            path: Vec::new(),
        });
    };
    Ok(DagLongestPath { cost, path })
}

struct IndexedNodes {
    ordered: Vec<[u8; 16]>,
    positions: BTreeMap<[u8; 16], usize>,
}

struct WeightedProjection {
    neighbors: Vec<BTreeMap<usize, f64>>,
    indegrees: Vec<usize>,
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
            return Err(execution(
                "dag_longest_path_weighted node UUIDs must be unique",
            ));
        }
    }
    Ok(IndexedNodes { ordered, positions })
}

fn normalize_edges(
    edges: &[WeightedDagEdge],
    node_index: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<WeightedProjection, AlgorithmError> {
    let mut stored = BTreeMap::new();
    for &edge in edges {
        checkpoint(control, work)?;
        if !edge.weight.is_finite() {
            return Err(execution(
                "dag_longest_path_weighted edge weights must be finite",
            ));
        }
        if !node_index.contains_key(&edge.source) || !node_index.contains_key(&edge.target) {
            return Err(execution(
                "dag_longest_path_weighted edge endpoint is outside node selection",
            ));
        }
        if let Some(previous) = stored.insert(edge.edge, edge)
            && previous != edge
        {
            return Err(execution(
                "dag_longest_path_weighted edge UUID has inconsistent adjacency entries",
            ));
        }
    }

    let mut neighbors = vec![BTreeMap::new(); node_index.len()];
    let mut indegrees = vec![0_usize; node_index.len()];
    for edge in stored.into_values() {
        checkpoint(control, work)?;
        let source = node_index[&edge.source];
        let target = node_index[&edge.target];
        match neighbors[source].entry(target) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(edge.weight);
                indegrees[target] = indegrees[target].checked_add(1).ok_or_else(|| {
                    execution("dag_longest_path_weighted indegree exceeds platform range")
                })?;
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if edge.weight > *entry.get() {
                    entry.insert(edge.weight);
                }
            }
        }
    }
    Ok(WeightedProjection {
        neighbors,
        indegrees,
    })
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

    fn edge(id: u8, source: u8, target: u8, weight: f64) -> WeightedDagEdge {
        WeightedDagEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
            weight,
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn values(result: DagLongestPath) -> (f64, Vec<u8>) {
        (
            result.cost,
            result.path.into_iter().map(|uuid| uuid[0]).collect(),
        )
    }

    #[test]
    fn finds_global_signed_path_and_breaks_full_path_ties() {
        let nodes = (0..10).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(20, 0, 4, 5.0),
            edge(21, 4, 6, -1.0),
            edge(22, 1, 3, 2.0),
            edge(23, 3, 5, 3.0),
            edge(24, 5, 8, 1.0),
            edge(25, 1, 2, 2.0),
            edge(26, 2, 5, 3.0),
            edge(27, 7, 9, 5.0),
        ];
        assert_eq!(
            values(weighted_dag_longest_path(&nodes, &edges, &control()).unwrap()),
            (6.0, vec![1, 2, 5, 8])
        );

        let mut reversed_nodes = nodes;
        reversed_nodes.reverse();
        let mut reversed_edges = edges;
        reversed_edges.reverse();
        assert_eq!(
            values(
                weighted_dag_longest_path(&reversed_nodes, &reversed_edges, &control()).unwrap()
            ),
            (6.0, vec![1, 2, 5, 8])
        );
    }

    #[test]
    fn covers_empty_singletons_and_all_negative_edges() {
        assert_eq!(
            weighted_dag_longest_path(&[], &[], &control()).unwrap(),
            DagLongestPath {
                cost: 0.0,
                path: Vec::new()
            }
        );
        assert_eq!(
            values(weighted_dag_longest_path(&[uuid(9)], &[], &control()).unwrap()),
            (0.0, vec![9])
        );
        assert_eq!(
            values(
                weighted_dag_longest_path(&[uuid(9), uuid(2)], &[edge(1, 2, 9, -1.0)], &control())
                    .unwrap()
            ),
            (0.0, vec![2])
        );
    }

    #[test]
    fn collapses_stored_duplicates_and_keeps_maximum_parallel_weight() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        let edges = [
            edge(10, 0, 1, 2.0),
            edge(10, 0, 1, 2.0),
            edge(11, 0, 1, 5.0),
            edge(12, 0, 1, 5.0),
            edge(13, 1, 2, 1.0),
        ];
        assert_eq!(
            values(weighted_dag_longest_path(&nodes, &edges, &control()).unwrap()),
            (6.0, vec![0, 1, 2])
        );
    }

    #[test]
    fn rejects_cycles_invalid_identity_and_non_finite_weights() {
        for result in [
            weighted_dag_longest_path(&[uuid(0)], &[edge(1, 0, 0, 1.0)], &control()),
            weighted_dag_longest_path(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1, 1.0), edge(2, 1, 2, 1.0), edge(3, 2, 0, 1.0)],
                &control(),
            ),
            weighted_dag_longest_path(&[uuid(0), uuid(0)], &[], &control()),
            weighted_dag_longest_path(&[uuid(0)], &[edge(1, 0, 2, 1.0)], &control()),
            weighted_dag_longest_path(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1, 1.0), edge(1, 0, 2, 1.0)],
                &control(),
            ),
            weighted_dag_longest_path(
                &[uuid(0), uuid(1)],
                &[edge(1, 0, 1, 1.0), edge(1, 0, 1, 2.0)],
                &control(),
            ),
            weighted_dag_longest_path(&[uuid(0), uuid(1)], &[edge(1, 0, 1, f64::NAN)], &control()),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn rejects_non_finite_addition_and_honors_shared_controls() {
        assert!(matches!(
            weighted_dag_longest_path(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1, f64::MAX), edge(2, 1, 2, f64::MAX)],
                &control()
            ),
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
            weighted_dag_longest_path(&[], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            weighted_dag_longest_path(
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
            weighted_dag_longest_path(&[], &[], &no_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }
}
