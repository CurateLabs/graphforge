use std::collections::{BTreeMap, VecDeque};

use crate::algorithm_analyze_bipartite::{BipartiteEdge, BipartiteProjection};
use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

/// Selected bipartite matching execution path for #556 disposition evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BipartiteMatchingExecutionPath {
    /// BFS layers and augmenting-path commits stay serial.
    SerialLayeredAugmentation,
}

/// Compute a deterministic maximum-cardinality matching.
///
/// The projection has already established left/right orientation and retained
/// parallel edges. Matching completes before limits are checked or rows return.
pub(crate) fn maximum_bipartite_matching(
    projection: &BipartiteProjection,
    control: &AlgorithmControl,
) -> Result<Vec<BipartiteEdge>, AlgorithmError> {
    match select_bipartite_matching_path(
        control,
        projection.left_nodes.len(),
        projection.right_nodes.len(),
        projection.edges.len(),
    ) {
        BipartiteMatchingExecutionPath::SerialLayeredAugmentation => {}
    }
    control.checkpoint()?;
    let left_index = projection
        .left_nodes
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect::<BTreeMap<_, _>>();
    let right_index = projection
        .right_nodes
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect::<BTreeMap<_, _>>();
    let mut adjacency = vec![Vec::new(); projection.left_nodes.len()];
    let mut edge_by_pair = BTreeMap::new();
    for edge in &projection.edges {
        control.check_cancelled()?;
        let (&left, &right) = (
            left_index
                .get(&edge.source)
                .ok_or_else(|| invalid_projection("edge source is not in the left partition"))?,
            right_index
                .get(&edge.target)
                .ok_or_else(|| invalid_projection("edge target is not in the right partition"))?,
        );
        adjacency[left].push(right);
        edge_by_pair
            .entry((left, right))
            .and_modify(|current: &mut BipartiteEdge| {
                if edge.edge < current.edge {
                    *current = *edge;
                }
            })
            .or_insert(*edge);
    }
    for neighbors in &mut adjacency {
        neighbors.sort_unstable();
        neighbors.dedup();
    }

    let mut left_match = vec![None; adjacency.len()];
    let mut right_match = vec![None; projection.right_nodes.len()];
    let mut distance = vec![usize::MAX; adjacency.len()];
    let mut dfs_stack = Vec::new();
    while find_layers(
        &adjacency,
        &left_match,
        &right_match,
        &mut distance,
        control,
    )? {
        for left in 0..adjacency.len() {
            control.check_cancelled()?;
            if left_match[left].is_none() {
                augment_iterative(
                    left,
                    &adjacency,
                    &mut left_match,
                    &mut right_match,
                    &mut distance,
                    &mut dfs_stack,
                    control,
                )?;
            }
        }
    }
    let mut result = left_match
        .iter()
        .enumerate()
        .filter_map(|(left, right)| right.map(|right| edge_by_pair[&(left, right)]))
        .collect::<Vec<_>>();
    result.sort_unstable_by_key(|edge| (edge.source, edge.target, edge.edge));
    control.check_output_rows(result.len())?;
    Ok(result)
}

pub(crate) fn select_bipartite_matching_path(
    _control: &AlgorithmControl,
    _left_nodes: usize,
    _right_nodes: usize,
    _edge_count: usize,
) -> BipartiteMatchingExecutionPath {
    BipartiteMatchingExecutionPath::SerialLayeredAugmentation
}

fn find_layers(
    adjacency: &[Vec<usize>],
    left_match: &[Option<usize>],
    right_match: &[Option<usize>],
    distance: &mut [usize],
    control: &AlgorithmControl,
) -> Result<bool, AlgorithmError> {
    control.checkpoint()?;
    let mut queue = VecDeque::new();
    for left in 0..adjacency.len() {
        if left_match[left].is_none() {
            distance[left] = 0;
            queue.push_back(left);
        } else {
            distance[left] = usize::MAX;
        }
    }
    let mut found = false;
    while let Some(left) = queue.pop_front() {
        control.check_cancelled()?;
        for &right in &adjacency[left] {
            if let Some(next) = right_match[right] {
                if distance[next] == usize::MAX {
                    distance[next] = distance[left].saturating_add(1);
                    queue.push_back(next);
                }
            } else {
                found = true;
            }
        }
    }
    Ok(found)
}
fn augment_iterative(
    root: usize,
    adjacency: &[Vec<usize>],
    left_match: &mut [Option<usize>],
    right_match: &mut [Option<usize>],
    distance: &mut [usize],
    stack: &mut Vec<(usize, usize, Option<usize>)>,
    control: &AlgorithmControl,
) -> Result<bool, AlgorithmError> {
    stack.clear();
    stack.push((root, 0, None));
    while !stack.is_empty() {
        control.check_cancelled()?;
        let frame = stack.len() - 1;
        let (left, next_neighbor, _) = stack[frame];
        let Some(&right) = adjacency[left].get(next_neighbor) else {
            distance[left] = usize::MAX;
            stack.pop();
            continue;
        };
        stack[frame].1 += 1;
        match right_match[right] {
            None => {
                let mut path_right = right;
                for &(path_left, _, incoming_right) in stack.iter().rev() {
                    left_match[path_left] = Some(path_right);
                    right_match[path_right] = Some(path_left);
                    if let Some(incoming_right) = incoming_right {
                        path_right = incoming_right;
                    }
                }
                return Ok(true);
            }
            Some(next) if distance[next] == distance[left].saturating_add(1) => {
                stack.push((next, 0, Some(right)));
            }
            Some(_) => {}
        }
    }
    Ok(false)
}
fn invalid_projection(message: &str) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};
    use crate::compute_pool::ComputePool;
    use std::sync::Arc;
    fn uuid(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }
    fn edge(id: u128, source: u128, target: u128) -> BipartiteEdge {
        BipartiteEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }
    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn control_with_threads(threads: usize) -> AlgorithmControl {
        AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(ComputePool::new(threads).unwrap()))
    }
    #[test]
    fn finds_maximum_cardinality_with_deterministic_ties() {
        let projection = BipartiteProjection {
            left_nodes: vec![uuid(1), uuid(2), uuid(3)],
            right_nodes: vec![uuid(4), uuid(5), uuid(6)],
            edges: vec![
                edge(1, 1, 4),
                edge(2, 1, 5),
                edge(3, 2, 4),
                edge(4, 3, 5),
                edge(5, 3, 6),
            ],
        };
        let expected = vec![edge(2, 1, 5), edge(3, 2, 4), edge(5, 3, 6)];
        assert_eq!(
            maximum_bipartite_matching(&projection, &control()).unwrap(),
            expected
        );
        assert_eq!(
            maximum_bipartite_matching(&projection, &control()).unwrap(),
            expected
        );
    }

    #[test]
    fn serial_disposition_holds_across_thread_budgets() {
        let projection = BipartiteProjection {
            left_nodes: vec![uuid(1), uuid(2), uuid(3), uuid(4)],
            right_nodes: vec![uuid(5), uuid(6), uuid(7), uuid(8)],
            edges: vec![
                edge(9, 1, 5),
                edge(8, 1, 6),
                edge(7, 2, 5),
                edge(6, 2, 7),
                edge(5, 3, 6),
                edge(4, 3, 8),
                edge(3, 4, 7),
            ],
        };
        let control = control_with_threads(8);
        assert_eq!(
            select_bipartite_matching_path(
                &control,
                projection.left_nodes.len(),
                projection.right_nodes.len(),
                projection.edges.len(),
            ),
            BipartiteMatchingExecutionPath::SerialLayeredAugmentation
        );
        let oracle = maximum_bipartite_matching(&projection, &control_with_threads(1)).unwrap();
        for threads in [2_usize, 4, 8] {
            assert_eq!(
                maximum_bipartite_matching(&projection, &control_with_threads(threads)).unwrap(),
                oracle
            );
        }
    }
    #[test]
    fn selects_lowest_parallel_edge_and_ignores_isolates() {
        let projection = BipartiteProjection {
            left_nodes: vec![uuid(1), uuid(9)],
            right_nodes: vec![uuid(2), uuid(8)],
            edges: vec![edge(7, 1, 2), edge(3, 1, 2)],
        };
        assert_eq!(
            maximum_bipartite_matching(&projection, &control()).unwrap(),
            [edge(3, 1, 2)]
        );
    }
    #[test]
    fn cancellation_and_output_limits_fail_without_rows() {
        let projection = BipartiteProjection {
            left_nodes: vec![uuid(1)],
            right_nodes: vec![uuid(2)],
            edges: vec![edge(1, 1, 2)],
        };
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            maximum_bipartite_matching(
                &projection,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            maximum_bipartite_matching(&projection, &limited),
            Err(AlgorithmError::OutputLimit {
                observed: 1,
                limit: 0
            })
        ));
    }
    #[test]
    fn reconstructs_a_deep_augmenting_path_without_recursion() {
        let len = 20_000_usize;
        let adjacency = (0..len)
            .map(|left| {
                if left == 0 {
                    vec![0]
                } else {
                    vec![left - 1, left]
                }
            })
            .collect::<Vec<_>>();
        let mut left_match = (0..len).map(|left| left.checked_sub(1)).collect::<Vec<_>>();
        let mut right_match = (0..len)
            .map(|right| (right + 1 < len).then_some(right + 1))
            .collect::<Vec<_>>();
        let mut distance = (0..len).collect::<Vec<_>>();
        let mut stack = Vec::new();
        assert!(
            augment_iterative(
                0,
                &adjacency,
                &mut left_match,
                &mut right_match,
                &mut distance,
                &mut stack,
                &control()
            )
            .unwrap()
        );
        assert_eq!(left_match, (0..len).map(Some).collect::<Vec<_>>());
    }
    #[test]
    fn handles_many_isolates_with_one_reused_dfs_stack() {
        let projection = BipartiteProjection {
            left_nodes: (0..50_000_u128).map(uuid).collect(),
            right_nodes: Vec::new(),
            edges: Vec::new(),
        };
        assert!(
            maximum_bipartite_matching(&projection, &control())
                .unwrap()
                .is_empty()
        );
    }
}
