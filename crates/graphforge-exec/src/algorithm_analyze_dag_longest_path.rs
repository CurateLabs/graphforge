use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;
const MAX_EXACT_FLOAT64_INTEGER: u64 = 1_u64 << 53;

/// One stored directed edge entry in the selected UUID projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DagLongestPathEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

/// Exact global longest path through a directed acyclic graph.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DagLongestPath {
    pub cost: f64,
    pub path: Vec<[u8; 16]>,
}

/// Compute the unweighted global longest directed path.
///
/// Stored-edge UUID duplicates collapse before distinct parallel arcs are
/// projected to one simple adjacency entry. Equal-hop candidates choose the
/// lexicographically smallest complete UUID path.
pub(crate) fn dag_longest_path(
    nodes: &[[u8; 16]],
    edges: &[DagLongestPathEdge],
    control: &AlgorithmControl,
) -> Result<DagLongestPath, AlgorithmError> {
    control.checkpoint()?;
    control.check_output_rows(1)?;

    let mut work = 0_usize;
    let nodes = index_nodes(nodes, control, &mut work)?;
    let (neighbors, mut indegrees) = normalize_edges(edges, &nodes.positions, control, &mut work)?;
    let mut ready = indegrees
        .iter()
        .enumerate()
        .filter_map(|(node, &indegree)| (indegree == 0).then_some(node))
        .collect::<BTreeSet<_>>();
    let mut paths = nodes
        .ordered
        .iter()
        .copied()
        .map(|uuid| vec![uuid])
        .collect::<Vec<_>>();
    let mut hops = vec![0_u64; nodes.ordered.len()];
    let mut processed = 0_usize;

    while let Some(&source) = ready.first() {
        checkpoint(control, &mut work)?;
        ready.remove(&source);
        processed = processed
            .checked_add(1)
            .ok_or_else(|| execution("dag_longest_path node count exceeds platform range"))?;

        for &target in &neighbors[source] {
            checkpoint(control, &mut work)?;
            let candidate_hops = hops[source]
                .checked_add(1)
                .ok_or_else(|| execution("dag_longest_path hop count exceeds supported range"))?;
            let mut candidate_path = paths[source].clone();
            candidate_path.push(nodes.ordered[target]);
            if candidate_hops > hops[target]
                || (candidate_hops == hops[target] && candidate_path < paths[target])
            {
                hops[target] = candidate_hops;
                paths[target] = candidate_path;
            }

            indegrees[target] = indegrees[target]
                .checked_sub(1)
                .ok_or_else(|| execution("dag_longest_path indegree underflow"))?;
            if indegrees[target] == 0 {
                ready.insert(target);
            }
        }
    }

    if processed != nodes.ordered.len() {
        return Err(execution(
            "dag_longest_path requires a directed acyclic graph",
        ));
    }

    let Some((best_hops, best_path)) =
        hops.into_iter()
            .zip(paths)
            .max_by(|(left_hops, left_path), (right_hops, right_path)| {
                left_hops
                    .cmp(right_hops)
                    .then_with(|| right_path.cmp(left_path))
            })
    else {
        return Ok(DagLongestPath {
            cost: 0.0,
            path: Vec::new(),
        });
    };
    if best_hops > MAX_EXACT_FLOAT64_INTEGER {
        return Err(execution(
            "dag_longest_path hop count cannot be represented exactly as Float64",
        ));
    }
    let cost = best_hops
        .to_string()
        .parse::<f64>()
        .map_err(|_| execution("dag_longest_path hop count cannot be converted to Float64"))?;
    Ok(DagLongestPath {
        cost,
        path: best_path,
    })
}

struct IndexedNodes {
    ordered: Vec<[u8; 16]>,
    positions: BTreeMap<[u8; 16], usize>,
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
            return Err(execution("dag_longest_path node UUIDs must be unique"));
        }
    }
    Ok(IndexedNodes { ordered, positions })
}

fn normalize_edges(
    edges: &[DagLongestPathEdge],
    node_index: &BTreeMap<[u8; 16], usize>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(Vec<BTreeSet<usize>>, Vec<usize>), AlgorithmError> {
    let mut stored = BTreeMap::new();
    for &edge in edges {
        checkpoint(control, work)?;
        if !node_index.contains_key(&edge.source) || !node_index.contains_key(&edge.target) {
            return Err(execution(
                "dag_longest_path edge endpoint is outside node selection",
            ));
        }
        if let Some(previous) = stored.insert(edge.edge, edge)
            && previous != edge
        {
            return Err(execution(
                "dag_longest_path edge UUID has inconsistent adjacency entries",
            ));
        }
    }

    let mut neighbors = vec![BTreeSet::new(); node_index.len()];
    let mut indegrees = vec![0_usize; node_index.len()];
    for edge in stored.into_values() {
        checkpoint(control, work)?;
        let source = node_index[&edge.source];
        let target = node_index[&edge.target];
        if neighbors[source].insert(target) {
            indegrees[target] = indegrees[target]
                .checked_add(1)
                .ok_or_else(|| execution("dag_longest_path indegree exceeds platform range"))?;
        }
    }
    Ok((neighbors, indegrees))
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

    fn edge(id: u8, source: u8, target: u8) -> DagLongestPathEdge {
        DagLongestPathEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
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
    fn finds_global_path_across_disconnected_components_and_breaks_full_path_ties() {
        let nodes = (0..10).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(20, 0, 4),
            edge(21, 4, 6),
            edge(22, 1, 3),
            edge(23, 3, 5),
            edge(24, 5, 8),
            edge(25, 1, 2),
            edge(26, 2, 5),
            edge(27, 9, 7),
        ];

        assert_eq!(
            values(dag_longest_path(&nodes, &edges, &control()).unwrap()),
            (3.0, vec![1, 2, 5, 8])
        );

        let mut reversed_nodes = nodes;
        reversed_nodes.reverse();
        let mut reversed_edges = edges;
        reversed_edges.reverse();
        assert_eq!(
            values(dag_longest_path(&reversed_nodes, &reversed_edges, &control()).unwrap()),
            (3.0, vec![1, 2, 5, 8])
        );
    }

    #[test]
    fn covers_empty_singleton_and_isolates() {
        assert_eq!(
            dag_longest_path(&[], &[], &control()).unwrap(),
            DagLongestPath {
                cost: 0.0,
                path: Vec::new()
            }
        );
        assert_eq!(
            values(dag_longest_path(&[uuid(9)], &[], &control()).unwrap()),
            (0.0, vec![9])
        );
        assert_eq!(
            values(dag_longest_path(&[uuid(9), uuid(2)], &[], &control()).unwrap()),
            (0.0, vec![2])
        );
    }

    #[test]
    fn collapses_stored_duplicates_and_distinct_parallel_arcs() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        let edges = [
            edge(10, 0, 1),
            edge(10, 0, 1),
            edge(11, 0, 1),
            edge(12, 1, 2),
        ];
        assert_eq!(
            values(dag_longest_path(&nodes, &edges, &control()).unwrap()),
            (2.0, vec![0, 1, 2])
        );
    }

    #[test]
    fn rejects_cycles_self_loops_and_invalid_identity_atomically() {
        for result in [
            dag_longest_path(&[uuid(0)], &[edge(1, 0, 0)], &control()),
            dag_longest_path(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(2, 1, 2), edge(3, 2, 0)],
                &control(),
            ),
            dag_longest_path(&[uuid(0), uuid(0)], &[], &control()),
            dag_longest_path(&[uuid(0)], &[edge(1, 0, 2)], &control()),
            dag_longest_path(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control(),
            ),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn shared_limits_and_cancellation_return_no_result() {
        let no_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            dag_longest_path(&[], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            dag_longest_path(
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
            dag_longest_path(&[], &[], &no_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));
    }
}
