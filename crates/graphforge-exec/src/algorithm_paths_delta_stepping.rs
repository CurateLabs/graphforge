use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::{AdjacencyGraph, AlgorithmEdge};
use crate::algorithm_paths_dijkstra::DijkstraPath;

const DELTA: f64 = 1.0;
const CHECKPOINT_INTERVAL: usize = 4_096;

type BestPath = (f64, Vec<u64>, Vec<u64>);
type Buckets = BTreeMap<BucketIndex, BTreeSet<u64>>;

#[derive(Clone, Copy, Debug, PartialEq)]
struct BucketIndex(f64);

impl Eq for BucketIndex {}

impl Ord for BucketIndex {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialOrd for BucketIndex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Exact deterministic Delta-stepping with the canonical fixed bucket width.
pub(crate) fn exact_delta_stepping(
    graph: &AdjacencyGraph,
    source: u64,
    target: Option<u64>,
    control: &AlgorithmControl,
) -> Result<Vec<DijkstraPath>, AlgorithmError> {
    control.checkpoint()?;
    validate_endpoint(graph, source, "source")?;
    if let Some(target) = target {
        validate_endpoint(graph, target, "target")?;
    }

    let mut work = 0_usize;
    validate_weights(graph, control, &mut work)?;
    let mut best = HashMap::from([(source, (0.0, vec![source], Vec::new()))]);
    let mut buckets = Buckets::from([(BucketIndex(0.0), BTreeSet::from([source]))]);

    while let Some((index, mut requests)) = buckets.pop_first() {
        checkpoint(control, &mut work)?;
        requests.retain(|node| is_current_bucket(*node, index, &best));
        if requests.is_empty() {
            continue;
        }

        let mut settled = BTreeSet::new();
        while !requests.is_empty() {
            checkpoint(control, &mut work)?;
            settled.extend(requests.iter().copied());
            relax_edges(
                graph,
                &requests,
                true,
                &mut best,
                &mut buckets,
                control,
                &mut work,
            )?;
            requests = buckets.remove(&index).unwrap_or_default();
            requests.retain(|node| is_current_bucket(*node, index, &best));
        }
        relax_edges(
            graph,
            &settled,
            false,
            &mut best,
            &mut buckets,
            control,
            &mut work,
        )?;
    }

    let mut targets = match target {
        Some(node) if best.contains_key(&node) => vec![node],
        Some(_) => Vec::new(),
        None => best.keys().copied().collect(),
    };
    targets.sort_unstable();
    control.check_output_rows(targets.len())?;
    Ok(targets
        .into_iter()
        .map(|node| {
            let (cost, nodes, _) = &best[&node];
            DijkstraPath {
                source,
                target: node,
                cost: *cost,
                nodes: nodes.clone(),
            }
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn relax_edges(
    graph: &AdjacencyGraph,
    sources: &BTreeSet<u64>,
    light: bool,
    best: &mut HashMap<u64, BestPath>,
    buckets: &mut Buckets,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    let mut proposals = Vec::new();
    for &source in sources {
        let Some(current) = best.get(&source).cloned() else {
            continue;
        };
        for edge in graph.neighbors(source) {
            checkpoint(control, work)?;
            if (edge.weight <= DELTA) != light || current.1.contains(&edge.neighbor_id) {
                continue;
            }
            proposals.push((edge.neighbor_id, candidate(&current, edge)?));
        }
    }
    proposals.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| compare_paths(&left.1, &right.1))
    });
    for (node, candidate) in proposals {
        if improves(&candidate, best.get(&node)) {
            let index = bucket_index(candidate.0);
            best.insert(node, candidate);
            buckets.entry(index).or_default().insert(node);
        }
    }
    Ok(())
}

fn validate_endpoint(graph: &AdjacencyGraph, node: u64, role: &str) -> Result<(), AlgorithmError> {
    if graph.node_ids().contains(&node) {
        Ok(())
    } else {
        Err(execution(format!(
            "delta_stepping {role} is outside node selection"
        )))
    }
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
                return Err(execution(
                    "delta_stepping requires finite non-negative edge weights",
                ));
            }
        }
    }
    Ok(())
}

fn candidate(current: &BestPath, edge: &AlgorithmEdge) -> Result<BestPath, AlgorithmError> {
    let cost = current.0 + edge.weight;
    if !cost.is_finite() {
        return Err(execution("delta_stepping accumulated cost is not finite"));
    }
    let mut path = current.1.clone();
    path.push(edge.neighbor_id);
    let mut edges = current.2.clone();
    edges.push(edge.edge_id);
    Ok((cost, path, edges))
}

fn compare_paths(left: &BestPath, right: &BestPath) -> Ordering {
    left.0
        .total_cmp(&right.0)
        .then_with(|| left.1.cmp(&right.1))
        .then_with(|| left.2.cmp(&right.2))
}

fn improves(candidate: &BestPath, known: Option<&BestPath>) -> bool {
    known.is_none_or(|known| compare_paths(candidate, known) == Ordering::Less)
}

fn bucket_index(cost: f64) -> BucketIndex {
    BucketIndex((cost / DELTA).floor())
}

fn is_current_bucket(node: u64, index: BucketIndex, best: &HashMap<u64, BestPath>) -> bool {
    best.get(&node)
        .is_some_and(|path| bucket_index(path.0) == index)
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work += 1;
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

    #[test]
    fn light_and_heavy_buckets_return_exact_target_and_reachable_paths() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            6,
            &[(0, 2), (0, 1), (1, 2), (1, 3), (2, 3), (3, 4)],
        )
        .with_test_edge_weights(&[5.0, 0.5, 0.5, 4.0, 1.0, 2.0]);
        let all = exact_delta_stepping(&graph, 0, None, &control()).unwrap();
        assert_eq!(
            all.iter()
                .map(|path| (path.target, path.cost, path.nodes.as_slice()))
                .collect::<Vec<_>>(),
            vec![
                (0, 0.0, &[0][..]),
                (1, 0.5, &[0, 1][..]),
                (2, 1.0, &[0, 1, 2][..]),
                (3, 2.0, &[0, 1, 2, 3][..]),
                (4, 4.0, &[0, 1, 2, 3, 4][..]),
            ]
        );
        assert_eq!(
            exact_delta_stepping(&graph, 0, Some(4), &control()).unwrap(),
            vec![all[4].clone()]
        );
    }

    #[test]
    fn ties_parallel_edges_self_loops_and_stale_buckets_are_stable() {
        let graph = AdjacencyGraph::with_test_directed_edges(
            6,
            &[(0, 0), (0, 3), (0, 2), (0, 1), (0, 1), (1, 3), (2, 3)],
        )
        .with_test_edge_weights(&[0.0, 5.0, 1.0, 4.0, 1.0, 1.0, 1.0]);
        let expected = DijkstraPath {
            source: 0,
            target: 3,
            cost: 2.0,
            nodes: vec![0, 1, 3],
        };
        assert_eq!(
            exact_delta_stepping(&graph, 0, Some(3), &control()).unwrap(),
            vec![expected]
        );
        assert!(
            exact_delta_stepping(&graph, 0, Some(5), &control())
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            exact_delta_stepping(&graph, 0, Some(0), &control()).unwrap()[0].nodes,
            [0]
        );
    }

    #[test]
    fn full_edge_sequence_breaks_a_tie_before_the_final_edge() {
        let start = (0.0, vec![0], Vec::new());
        let later_parallel = AlgorithmEdge {
            edge_id: 1,
            edge_uuid: [1; 16],
            neighbor_id: 1,
            weight: 1.0,
        };
        let earlier_parallel = AlgorithmEdge {
            edge_id: 0,
            edge_uuid: [0; 16],
            ..later_parallel
        };
        let final_edge = AlgorithmEdge {
            edge_id: 9,
            edge_uuid: [9; 16],
            neighbor_id: 2,
            weight: 1.0,
        };
        let later = candidate(&candidate(&start, &later_parallel).unwrap(), &final_edge).unwrap();
        let earlier =
            candidate(&candidate(&start, &earlier_parallel).unwrap(), &final_edge).unwrap();

        assert_eq!(later.1, earlier.1);
        assert_eq!(later.2, [1, 9]);
        assert_eq!(earlier.2, [0, 9]);
        assert!(improves(&earlier, Some(&later)));
    }

    #[test]
    fn weights_endpoints_limits_and_cancellation_are_structured() {
        let negative =
            AdjacencyGraph::with_test_edges(2, &[(0, 1)]).with_test_edge_weights(&[-1.0]);
        assert!(matches!(
            exact_delta_stepping(&negative, 0, None, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        let non_finite =
            AdjacencyGraph::with_test_edges(2, &[(0, 1)]).with_test_edge_weights(&[f64::INFINITY]);
        assert!(exact_delta_stepping(&non_finite, 0, None, &control()).is_err());
        let overflow = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)])
            .with_test_edge_weights(&[f64::MAX, f64::MAX]);
        assert!(matches!(
            exact_delta_stepping(&overflow, 0, None, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(exact_delta_stepping(&negative, 9, None, &control()).is_err());

        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_delta_stepping(&graph, 0, None, &limited),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let iteration_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            exact_delta_stepping(&graph, 0, None, &iteration_limited),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert!(matches!(
            exact_delta_stepping(&graph, 0, None, &cancelled),
            Err(AlgorithmError::Cancelled)
        ));
    }
}
