//! Spanning-forest analysis remains serial for minimum spanning trees (#582) and
//! maximum spanning trees (#580). Stable edge order and union-find acceptance
//! define public ties; each accepted edge mutates component state consumed by
//! later candidates, so there is no independent edge frontier for private-pool
//! execution without changing ties.

use std::cmp::Ordering;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_weighted_undirected::{
    WeightedEdge, compare_weighted_edges, normalize_weighted_undirected,
};

const CHECKPOINT_INTERVAL: usize = 4_096;

/// One public-identity edge candidate for an undirected spanning forest.
pub(crate) type SpanningEdge = WeightedEdge;

/// Compute a deterministic spanning forest with Kruskal's algorithm.
///
/// Mirrored undirected adjacency entries are collapsed by edge UUID. Parallel
/// edges with distinct UUIDs remain independent candidates.
pub(crate) fn spanning_forest(
    nodes: &[[u8; 16]],
    edges: &[SpanningEdge],
    maximize: bool,
    control: &AlgorithmControl,
) -> Result<Vec<SpanningEdge>, AlgorithmError> {
    control.checkpoint()?;
    let mut work = 0_usize;
    let mut graph = normalize_weighted_undirected(nodes, edges, control, &mut work)?;
    checkpointed_sort(&mut graph.edges, maximize, control, &mut work)?;

    let mut sets = DisjointSets::new(nodes.len());
    let mut forest = Vec::with_capacity(nodes.len().saturating_sub(1));
    for edge in graph.edges {
        checkpoint(control, &mut work)?;
        if edge.source_uuid == edge.target_uuid {
            continue;
        }
        let source = graph.node_index[&edge.source_uuid];
        let target = graph.node_index[&edge.target_uuid];
        if sets.union(source, target) {
            control.check_output_rows(forest.len().saturating_add(1))?;
            forest.push(edge);
        }
    }
    Ok(forest)
}

fn compare_edges(left: &SpanningEdge, right: &SpanningEdge, maximize: bool) -> Ordering {
    compare_weighted_edges(left, right, maximize)
}

fn checkpointed_sort(
    edges: &mut [SpanningEdge],
    maximize: bool,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    if edges.len() < 2 {
        return Ok(());
    }
    let mut scratch = edges.to_vec();
    let mut width = 1_usize;
    while width < edges.len() {
        control.check_cancelled()?;
        for start in (0..edges.len()).step_by(width.saturating_mul(2)) {
            let middle = start.saturating_add(width).min(edges.len());
            let end = start
                .saturating_add(width.saturating_mul(2))
                .min(edges.len());
            let (mut left, mut right) = (start, middle);
            for output in &mut scratch[start..end] {
                checkpoint(control, work)?;
                if right == end
                    || (left < middle
                        && compare_edges(&edges[left], &edges[right], maximize)
                            != Ordering::Greater)
                {
                    *output = edges[left];
                    left += 1;
                } else {
                    *output = edges[right];
                    right += 1;
                }
            }
        }
        edges.copy_from_slice(&scratch);
        width = width.saturating_mul(2);
    }
    Ok(())
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    }
    Ok(())
}

struct DisjointSets {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl DisjointSets {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
            rank: vec![0; len],
        }
    }

    fn find(&mut self, node: usize) -> usize {
        if self.parent[node] != node {
            self.parent[node] = self.find(self.parent[node]);
        }
        self.parent[node]
    }

    fn union(&mut self, left: usize, right: usize) -> bool {
        let mut left = self.find(left);
        let mut right = self.find(right);
        if left == right {
            return false;
        }
        if self.rank[left] < self.rank[right] {
            std::mem::swap(&mut left, &mut right);
        }
        self.parent[right] = left;
        if self.rank[left] == self.rank[right] {
            self.rank[left] = self.rank[left].saturating_add(1);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn edge(id: u8, source: u8, target: u8, weight: f64) -> SpanningEdge {
        SpanningEdge {
            edge_uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
            weight,
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    #[test]
    fn exact_forest_covers_disconnected_negative_mirrored_and_parallel_edges() {
        let nodes = (0..7).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(10, 0, 1, 4.0),
            edge(10, 1, 0, 4.0),
            edge(20, 0, 2, 3.0),
            edge(30, 1, 2, 1.0),
            edge(40, 1, 3, 2.0),
            edge(50, 2, 3, 4.0),
            edge(60, 4, 5, -2.0),
            edge(61, 4, 5, 3.0),
            edge(70, 3, 3, -10.0),
        ];
        assert_eq!(
            spanning_forest(&nodes, &edges, false, &control()).unwrap(),
            [
                edge(60, 4, 5, -2.0),
                edge(30, 1, 2, 1.0),
                edge(40, 1, 3, 2.0),
                edge(20, 0, 2, 3.0),
            ]
        );
    }

    #[test]
    fn unit_weight_whole_edge_ties_are_uuid_stable() {
        let nodes = (0..4).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(9, 1, 0, 1.0),
            edge(8, 0, 1, 1.0),
            edge(7, 0, 2, 1.0),
            edge(6, 1, 2, 1.0),
            edge(5, 2, 3, 1.0),
        ];
        assert_eq!(
            spanning_forest(&nodes, &edges, false, &control()).unwrap(),
            [edge(8, 0, 1, 1.0), edge(7, 0, 2, 1.0), edge(5, 2, 3, 1.0),]
        );
    }

    #[test]
    fn maximum_forest_prefers_heaviest_signed_parallel_edges_per_component() {
        let nodes = (0..8).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(10, 0, 1, 4.0),
            edge(10, 1, 0, 4.0),
            edge(11, 0, 1, 9.0),
            edge(12, 1, 0, 8.0),
            edge(20, 0, 2, 7.0),
            edge(30, 1, 2, 6.0),
            edge(40, 1, 3, -3.0),
            edge(50, 2, 3, -1.0),
            edge(60, 4, 5, -5.0),
            edge(61, 4, 5, -2.0),
            edge(70, 3, 3, f64::MAX),
        ];

        assert_eq!(
            spanning_forest(&nodes, &edges, true, &control()).unwrap(),
            [
                edge(11, 0, 1, 9.0),
                edge(20, 0, 2, 7.0),
                edge(50, 2, 3, -1.0),
                edge(61, 4, 5, -2.0),
            ]
        );
    }

    #[test]
    fn maximum_equal_weight_ties_use_canonical_endpoints_then_edge_uuid() {
        let nodes = (0..4).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(9, 1, 0, 5.0),
            edge(8, 0, 1, 5.0),
            edge(7, 0, 2, 5.0),
            edge(6, 1, 2, 5.0),
            edge(5, 2, 3, 5.0),
        ];

        assert_eq!(
            spanning_forest(&nodes, &edges, true, &control()).unwrap(),
            [edge(8, 0, 1, 5.0), edge(7, 0, 2, 5.0), edge(5, 2, 3, 5.0)]
        );
    }

    #[test]
    fn empty_and_isolated_graphs_return_empty_canonical_forests() {
        assert!(
            spanning_forest(&[], &[], false, &control())
                .unwrap()
                .is_empty()
        );
        assert!(
            spanning_forest(&[uuid(1), uuid(2)], &[], true, &control(),)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn malformed_candidates_are_rejected() {
        for result in [
            spanning_forest(&[uuid(1), uuid(1)], &[], true, &control()),
            spanning_forest(&[uuid(1), uuid(2)], &[edge(1, 1, 3, 1.0)], true, &control()),
            spanning_forest(
                &[uuid(1), uuid(2)],
                &[edge(1, 1, 2, f64::NAN)],
                true,
                &control(),
            ),
            spanning_forest(
                &[uuid(1), uuid(2), uuid(3)],
                &[edge(1, 1, 2, 1.0), edge(1, 1, 3, 1.0)],
                true,
                &control(),
            ),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn shared_output_iteration_and_cancellation_controls_are_enforced() {
        let nodes = [uuid(1), uuid(2)];
        let edges = [edge(1, 1, 2, 1.0)];
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            spanning_forest(&nodes, &edges, true, &limited),
            Err(AlgorithmError::OutputLimit { .. })
        ));

        let no_work = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            spanning_forest(&nodes, &edges, true, &no_work),
            Err(AlgorithmError::IterationLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            spanning_forest(&nodes, &edges, true, &cancelled),
            Err(AlgorithmError::Cancelled)
        );
    }

    #[test]
    fn seeded_forests_match_independent_prim_and_preserve_permutation_ties() {
        // Prim grows one component at a time using a plain edge scan, with no
        // shared sorting or union-find implementation from Kruskal.
        fn prim(nodes: &[[u8; 16]], edges: &[SpanningEdge], maximize: bool) -> (f64, usize) {
            let mut visited = vec![false; nodes.len()];
            let mut weight = 0.0;
            let mut count = 0;
            while visited.contains(&false) {
                let candidate = edges
                    .iter()
                    .filter_map(|edge| {
                        let source = nodes
                            .iter()
                            .position(|node| *node == edge.source_uuid)
                            .unwrap();
                        let target = nodes
                            .iter()
                            .position(|node| *node == edge.target_uuid)
                            .unwrap();
                        (visited[source] != visited[target]).then_some((
                            edge.weight,
                            source,
                            target,
                        ))
                    })
                    .min_by(|left, right| {
                        if maximize {
                            right.0.total_cmp(&left.0)
                        } else {
                            left.0.total_cmp(&right.0)
                        }
                    });
                if let Some((edge_weight, source, target)) = candidate {
                    visited[source] = true;
                    visited[target] = true;
                    weight += edge_weight;
                    count += 1;
                } else {
                    let root = visited.iter().position(|value| !value).unwrap();
                    visited[root] = true;
                }
            }
            (weight, count)
        }

        let mut state = 0x5eed_u64;
        let mut random = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            state >> 32
        };
        for sample in 0..128 {
            let nodes = (0..(sample % 12)).map(uuid).collect::<Vec<_>>();
            let mut edges = Vec::new();
            for source in 0..nodes.len() {
                for target in source..nodes.len() {
                    for _ in 0..random() % 3 {
                        let edge = SpanningEdge {
                            edge_uuid: (edges.len() as u128).to_be_bytes(),
                            source_uuid: nodes[source],
                            target_uuid: nodes[target],
                            weight: (random() % 15) as f64 - 7.0,
                        };
                        edges.push(edge);
                        // Preserve stored identity across mirrored adjacency.
                        edges.push(SpanningEdge {
                            source_uuid: edge.target_uuid,
                            target_uuid: edge.source_uuid,
                            ..edge
                        });
                    }
                }
            }
            for maximize in [false, true] {
                let expected = prim(&nodes, &edges, maximize);
                let forest = spanning_forest(&nodes, &edges, maximize, &control()).unwrap();
                assert_eq!(
                    (
                        forest.iter().map(|edge| edge.weight).sum::<f64>(),
                        forest.len()
                    ),
                    expected,
                    "sample={sample}, maximize={maximize}"
                );
                let mut permuted_nodes = nodes.clone();
                permuted_nodes.reverse();
                let mut permuted_edges = edges.clone();
                permuted_edges.reverse();
                assert_eq!(
                    spanning_forest(&permuted_nodes, &permuted_edges, maximize, &control())
                        .unwrap(),
                    forest,
                    "sample={sample}, maximize={maximize}"
                );
            }
        }
    }

    #[test]
    fn sorting_consumes_the_shared_iteration_budget() {
        let nodes = [uuid(1), uuid(2)];
        let edges = (0_u32..4_096)
            .map(|id| SpanningEdge {
                edge_uuid: u128::from(id).to_be_bytes(),
                source_uuid: nodes[0],
                target_uuid: nodes[1],
                weight: f64::from(id),
            })
            .collect::<Vec<_>>();
        let sorting_limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            spanning_forest(&nodes, &edges, true, &sorting_limited),
            Err(AlgorithmError::IterationLimit {
                observed: 3,
                limit: 2
            })
        ));
    }
}
