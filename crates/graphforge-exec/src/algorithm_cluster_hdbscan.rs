//! Deterministic mutual-reachability tree for Rust-owned HDBSCAN clustering.
#![allow(
    dead_code,
    reason = "HDBSCAN hierarchy and dispatch land in the dependent algorithm leaves"
)]

use std::cmp::Ordering;

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

const MIN_SAMPLES: usize = 5;
const MIN_CLUSTER_SIZE: usize = 5;
const CHECKPOINT_INTERVAL: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct MstEdge {
    pub(crate) left: usize,
    pub(crate) right: usize,
    pub(crate) distance: f64,
}

#[derive(Debug, PartialEq)]
pub(crate) struct ReachabilityTree {
    pub(crate) core_distances: Vec<f64>,
    pub(crate) edges: Vec<MstEdge>,
}

impl ReachabilityTree {
    pub(crate) fn from_graph(
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<Self, AlgorithmError> {
        let vectors = graph
            .node_ids()
            .iter()
            .map(|&node_id| {
                graph
                    .node_vector(node_id)
                    .ok_or_else(|| AlgorithmError::Execution {
                        message: "validated HDBSCAN vector is missing".into(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_vectors(&vectors, control)
    }

    fn from_vectors(
        vectors: &[&[f64]],
        control: &AlgorithmControl,
    ) -> Result<Self, AlgorithmError> {
        let distances = pair_distances(vectors, control)?;
        let core_distances = core_distances(&distances)?;
        let edges = minimum_spanning_tree(&distances, &core_distances, control)?;
        Ok(Self {
            core_distances,
            edges,
        })
    }

    pub(crate) fn stable_labels(
        &self,
        node_count: usize,
        control: &AlgorithmControl,
    ) -> Result<Vec<i64>, AlgorithmError> {
        if node_count < MIN_CLUSTER_SIZE {
            return Ok(vec![-1; node_count]);
        }
        let mut work = WorkControl::new(control);
        let hierarchy = SingleLinkageHierarchy::from_tree(node_count, &self.edges, &mut work)?;
        let root = hierarchy.root.ok_or_else(hierarchy_error)?;
        let node_count_f64 = f64::from(u32::try_from(node_count).map_err(|_| hierarchy_error())?);
        let max_lambda = f64::MAX / (2.0 * node_count_f64);
        let mut candidates = vec![Candidate::new(root, 0.0, &hierarchy)];
        condense(&hierarchy, root, 0, max_lambda, &mut candidates, &mut work)?;

        let mut selected = Vec::new();
        for &child in &candidates[0].children {
            select_eom(child, &candidates, &mut selected, &mut work)?;
        }
        selected.sort_by_key(|&candidate| candidates[candidate].first);

        let mut labels = vec![-1; node_count];
        for (community, candidate) in selected.into_iter().enumerate() {
            let community = i64::try_from(community).map_err(|_| hierarchy_error())?;
            for point in hierarchy.leaves(candidates[candidate].node, &mut work)? {
                labels[point] = community;
            }
        }
        Ok(labels)
    }
}

#[derive(Clone, Copy, Debug)]
struct HierarchyNode {
    left: Option<usize>,
    right: Option<usize>,
    distance: f64,
    size: usize,
    first: usize,
}

#[derive(Debug)]
struct SingleLinkageHierarchy {
    nodes: Vec<HierarchyNode>,
    root: Option<usize>,
}

impl SingleLinkageHierarchy {
    fn from_tree(
        node_count: usize,
        edges: &[MstEdge],
        work: &mut WorkControl<'_>,
    ) -> Result<Self, AlgorithmError> {
        if node_count == 0 {
            return Ok(Self {
                nodes: Vec::new(),
                root: None,
            });
        }
        if edges.len() != node_count - 1 {
            return Err(hierarchy_error());
        }
        let mut nodes = (0..node_count)
            .map(|point| HierarchyNode {
                left: None,
                right: None,
                distance: 0.0,
                size: 1,
                first: point,
            })
            .collect::<Vec<_>>();
        let mut parent = (0..node_count).collect::<Vec<_>>();
        let mut component_node = (0..node_count).collect::<Vec<_>>();
        let mut ordered = edges.to_vec();
        ordered.sort_by(|left, right| {
            edge_order(
                left.distance,
                left.left,
                left.right,
                right.distance,
                right.left,
                right.right,
            )
        });

        for edge in ordered {
            work.tick()?;
            if edge.left >= node_count
                || edge.right >= node_count
                || !edge.distance.is_finite()
                || edge.distance < 0.0
            {
                return Err(hierarchy_error());
            }
            let left_root = find(&mut parent, edge.left);
            let right_root = find(&mut parent, edge.right);
            if left_root == right_root {
                return Err(hierarchy_error());
            }
            let mut left = component_node[left_root];
            let mut right = component_node[right_root];
            if nodes[left].first > nodes[right].first {
                std::mem::swap(&mut left, &mut right);
            }
            let merged = nodes.len();
            nodes.push(HierarchyNode {
                left: Some(left),
                right: Some(right),
                distance: edge.distance,
                size: nodes[left].size + nodes[right].size,
                first: nodes[left].first,
            });
            let representative = left_root.min(right_root);
            let absorbed = left_root.max(right_root);
            parent[absorbed] = representative;
            component_node[representative] = merged;
        }
        let root = component_node[find(&mut parent, 0)];
        if nodes[root].size != node_count {
            return Err(hierarchy_error());
        }
        Ok(Self {
            nodes,
            root: Some(root),
        })
    }

    fn leaves(
        &self,
        node: usize,
        work: &mut WorkControl<'_>,
    ) -> Result<Vec<usize>, AlgorithmError> {
        let mut leaves = Vec::with_capacity(self.nodes[node].size);
        let mut pending = vec![node];
        while let Some(next) = pending.pop() {
            work.tick()?;
            match (self.nodes[next].left, self.nodes[next].right) {
                (Some(left), Some(right)) => {
                    pending.push(right);
                    pending.push(left);
                }
                (None, None) => leaves.push(self.nodes[next].first),
                _ => return Err(hierarchy_error()),
            }
        }
        Ok(leaves)
    }
}

#[derive(Clone, Debug)]
struct Candidate {
    node: usize,
    first: usize,
    birth: f64,
    stability: f64,
    children: Vec<usize>,
}

impl Candidate {
    fn new(node: usize, birth: f64, hierarchy: &SingleLinkageHierarchy) -> Self {
        Self {
            node,
            first: hierarchy.nodes[node].first,
            birth,
            stability: 0.0,
            children: Vec::new(),
        }
    }
}

fn condense(
    hierarchy: &SingleLinkageHierarchy,
    node: usize,
    candidate: usize,
    max_lambda: f64,
    candidates: &mut Vec<Candidate>,
    work: &mut WorkControl<'_>,
) -> Result<(), AlgorithmError> {
    let mut pending = vec![(node, candidate)];
    while let Some((node, candidate)) = pending.pop() {
        work.tick()?;
        let current = hierarchy.nodes[node];
        let (Some(left), Some(right)) = (current.left, current.right) else {
            add_stability(candidate, 1, max_lambda, candidates)?;
            continue;
        };
        let split_lambda = lambda(current.distance, max_lambda)?;
        let left_large = hierarchy.nodes[left].size >= MIN_CLUSTER_SIZE;
        let right_large = hierarchy.nodes[right].size >= MIN_CLUSTER_SIZE;
        match (left_large, right_large) {
            (true, true) => {
                add_stability(candidate, current.size, split_lambda, candidates)?;
                let left_candidate = candidates.len();
                candidates.push(Candidate::new(left, split_lambda, hierarchy));
                let right_candidate = candidates.len();
                candidates.push(Candidate::new(right, split_lambda, hierarchy));
                candidates[candidate]
                    .children
                    .extend([left_candidate, right_candidate]);
                pending.push((right, right_candidate));
                pending.push((left, left_candidate));
            }
            (true, false) => {
                add_stability(
                    candidate,
                    hierarchy.nodes[right].size,
                    split_lambda,
                    candidates,
                )?;
                pending.push((left, candidate));
            }
            (false, true) => {
                add_stability(
                    candidate,
                    hierarchy.nodes[left].size,
                    split_lambda,
                    candidates,
                )?;
                pending.push((right, candidate));
            }
            (false, false) => {
                add_stability(candidate, current.size, split_lambda, candidates)?;
            }
        }
    }
    Ok(())
}

fn add_stability(
    candidate: usize,
    count: usize,
    exit: f64,
    candidates: &mut [Candidate],
) -> Result<(), AlgorithmError> {
    let delta = (exit - candidates[candidate].birth).max(0.0);
    let count = u32::try_from(count).map_err(|_| hierarchy_error())?;
    let stability = candidates[candidate].stability + f64::from(count) * delta;
    if !stability.is_finite() {
        return Err(numeric_error());
    }
    candidates[candidate].stability = stability;
    Ok(())
}

fn select_eom(
    candidate: usize,
    candidates: &[Candidate],
    selected: &mut Vec<usize>,
    work: &mut WorkControl<'_>,
) -> Result<f64, AlgorithmError> {
    let mut effective = vec![0.0; candidates.len()];
    let mut choose_self = vec![false; candidates.len()];
    let mut pending = vec![(candidate, false)];
    while let Some((current, expanded)) = pending.pop() {
        if !expanded {
            work.tick()?;
            pending.push((current, true));
            for &child in candidates[current].children.iter().rev() {
                pending.push((child, false));
            }
            continue;
        }
        let child_stability = candidates[current]
            .children
            .iter()
            .map(|&child| effective[child])
            .sum::<f64>();
        choose_self[current] = candidates[current].children.is_empty()
            || candidates[current].stability >= child_stability;
        effective[current] = if choose_self[current] {
            candidates[current].stability
        } else {
            child_stability
        };
    }
    let mut pending = vec![candidate];
    while let Some(current) = pending.pop() {
        if choose_self[current] {
            selected.push(current);
        } else {
            pending.extend(candidates[current].children.iter().rev());
        }
    }
    Ok(effective[candidate])
}

fn lambda(distance: f64, maximum: f64) -> Result<f64, AlgorithmError> {
    if !distance.is_finite() || distance < 0.0 {
        return Err(numeric_error());
    }
    Ok(distance.recip().min(maximum))
}

fn find(parent: &mut [usize], node: usize) -> usize {
    let mut root = node;
    while parent[root] != root {
        root = parent[root];
    }
    let mut current = node;
    while parent[current] != current {
        let next = parent[current];
        parent[current] = root;
        current = next;
    }
    root
}

struct WorkControl<'a> {
    control: &'a AlgorithmControl,
    steps: usize,
}

impl<'a> WorkControl<'a> {
    fn new(control: &'a AlgorithmControl) -> Self {
        Self { control, steps: 0 }
    }

    fn tick(&mut self) -> Result<(), AlgorithmError> {
        if self.steps.is_multiple_of(CHECKPOINT_INTERVAL) {
            self.control.checkpoint()?;
        }
        self.steps += 1;
        Ok(())
    }
}

fn hierarchy_error() -> AlgorithmError {
    AlgorithmError::Execution {
        message: "HDBSCAN reachability tree is not a valid hierarchy".into(),
    }
}

fn pair_distances(
    vectors: &[&[f64]],
    control: &AlgorithmControl,
) -> Result<Vec<Vec<f64>>, AlgorithmError> {
    let count = vectors.len();
    let mut distances = vec![vec![0.0; count]; count];
    for left in 0..count {
        control.checkpoint()?;
        for right in (left + 1)..count {
            let squared = vectors[left]
                .iter()
                .zip(vectors[right])
                .try_fold(0.0, |sum, (&a, &b)| {
                    let delta = a - b;
                    let next = sum + delta * delta;
                    next.is_finite().then_some(next)
                })
                .ok_or_else(numeric_error)?;
            let distance = squared.sqrt();
            if !distance.is_finite() {
                return Err(numeric_error());
            }
            distances[left][right] = distance;
            distances[right][left] = distance;
        }
    }
    Ok(distances)
}

fn core_distances(distances: &[Vec<f64>]) -> Result<Vec<f64>, AlgorithmError> {
    let sample_count = MIN_SAMPLES.min(distances.len());
    if sample_count == 0 {
        return Ok(Vec::new());
    }
    distances
        .iter()
        .map(|row| {
            let mut ordered = row.clone();
            ordered.sort_by(f64::total_cmp);
            let core = ordered[sample_count - 1];
            core.is_finite().then_some(core).ok_or_else(numeric_error)
        })
        .collect()
}

fn mutual_reachability(distances: &[Vec<f64>], core: &[f64], left: usize, right: usize) -> f64 {
    distances[left][right].max(core[left]).max(core[right])
}

fn minimum_spanning_tree(
    distances: &[Vec<f64>],
    core: &[f64],
    control: &AlgorithmControl,
) -> Result<Vec<MstEdge>, AlgorithmError> {
    let count = distances.len();
    if count < 2 {
        return Ok(Vec::new());
    }
    let mut visited = vec![false; count];
    let mut best = vec![f64::INFINITY; count];
    let mut parent = vec![0; count];
    visited[0] = true;
    for (node, distance) in best.iter_mut().enumerate().skip(1) {
        *distance = mutual_reachability(distances, core, 0, node);
    }

    let mut edges = Vec::with_capacity(count - 1);
    for _ in 1..count {
        control.checkpoint()?;
        let node = (0..count)
            .filter(|&candidate| !visited[candidate])
            .min_by(|&left, &right| {
                edge_order(
                    best[left],
                    parent[left],
                    left,
                    best[right],
                    parent[right],
                    right,
                )
            })
            .ok_or_else(|| AlgorithmError::Execution {
                message: "HDBSCAN reachability graph is disconnected".into(),
            })?;
        if !best[node].is_finite() {
            return Err(numeric_error());
        }
        edges.push(MstEdge {
            left: parent[node].min(node),
            right: parent[node].max(node),
            distance: best[node],
        });
        visited[node] = true;
        for candidate in 0..count {
            if visited[candidate] {
                continue;
            }
            let distance = mutual_reachability(distances, core, node, candidate);
            if edge_order(
                distance,
                node,
                candidate,
                best[candidate],
                parent[candidate],
                candidate,
            ) == Ordering::Less
            {
                best[candidate] = distance;
                parent[candidate] = node;
            }
        }
    }
    Ok(edges)
}

fn edge_order(
    left_distance: f64,
    left_source: usize,
    left_target: usize,
    right_distance: f64,
    right_source: usize,
    right_target: usize,
) -> Ordering {
    left_distance
        .total_cmp(&right_distance)
        .then_with(|| {
            left_source
                .min(left_target)
                .cmp(&right_source.min(right_target))
        })
        .then_with(|| {
            left_source
                .max(left_target)
                .cmp(&right_source.max(right_target))
        })
}

fn numeric_error() -> AlgorithmError {
    AlgorithmError::Execution {
        message: "HDBSCAN distance is NaN or infinite".into(),
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
    fn distances_core_reachability_and_mst_are_hand_verifiable() {
        let values = [vec![0.0], vec![1.0], vec![2.0], vec![10.0], vec![11.0]];
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let distances = pair_distances(&vectors, &control()).unwrap();
        assert_eq!(distances[0], [0.0, 1.0, 2.0, 10.0, 11.0]);
        let core = core_distances(&distances).unwrap();
        assert_eq!(core, [11.0, 10.0, 9.0, 10.0, 11.0]);
        assert_eq!(mutual_reachability(&distances, &core, 1, 2), 10.0);
        assert_eq!(
            minimum_spanning_tree(&distances, &core, &control()).unwrap(),
            [
                MstEdge {
                    left: 0,
                    right: 1,
                    distance: 11.0
                },
                MstEdge {
                    left: 1,
                    right: 2,
                    distance: 10.0
                },
                MstEdge {
                    left: 1,
                    right: 3,
                    distance: 10.0
                },
                MstEdge {
                    left: 0,
                    right: 4,
                    distance: 11.0
                },
            ]
        );
    }

    #[test]
    fn small_duplicate_and_equal_weight_inputs_are_stable() {
        for values in [
            vec![],
            vec![vec![3.0]],
            vec![vec![1.0], vec![1.0], vec![1.0]],
        ] {
            let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let first = ReachabilityTree::from_vectors(&vectors, &control()).unwrap();
            let second = ReachabilityTree::from_vectors(&vectors, &control()).unwrap();
            assert_eq!(first, second);
            assert!(first.core_distances.iter().all(|value| value.is_finite()));
        }
    }

    #[test]
    fn controls_and_non_finite_intermediates_are_structured() {
        let values = [vec![0.0], vec![1.0]];
        let vectors = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let limits = AlgorithmLimits {
            iterations: 1,
            ..AlgorithmLimits::default()
        };
        assert!(matches!(
            ReachabilityTree::from_vectors(
                &vectors,
                &AlgorithmControl::new(limits, AlgorithmCancellation::default())
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            ReachabilityTree::from_vectors(
                &vectors,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
        let huge = [vec![f64::MAX], vec![-f64::MAX]];
        let huge = huge.iter().map(Vec::as_slice).collect::<Vec<_>>();
        assert!(matches!(
            ReachabilityTree::from_vectors(&huge, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn hierarchy_merges_by_weight_then_canonical_endpoints() {
        let edges = [
            MstEdge {
                left: 2,
                right: 3,
                distance: 2.0,
            },
            MstEdge {
                left: 0,
                right: 1,
                distance: 1.0,
            },
            MstEdge {
                left: 1,
                right: 2,
                distance: 2.0,
            },
        ];
        let control = control();
        let mut work = WorkControl::new(&control);
        let hierarchy = SingleLinkageHierarchy::from_tree(4, &edges, &mut work).unwrap();
        assert_eq!(hierarchy.nodes.len(), 7);
        assert_eq!(hierarchy.nodes[4].distance, 1.0);
        assert_eq!(
            (hierarchy.nodes[4].left, hierarchy.nodes[4].right),
            (Some(0), Some(1))
        );
        assert_eq!(hierarchy.nodes[5].first, 0);
        assert_eq!(hierarchy.nodes[5].size, 3);
        assert_eq!(
            (hierarchy.nodes[5].left, hierarchy.nodes[5].right),
            (Some(4), Some(2))
        );
        let root = hierarchy.root.unwrap();
        assert_eq!(hierarchy.nodes[root].distance, 2.0);
        assert_eq!(hierarchy.nodes[root].size, 4);
        assert_eq!(hierarchy.leaves(root, &mut work).unwrap(), [0, 1, 2, 3]);
    }

    #[test]
    fn hierarchy_boundaries_controls_and_malformed_trees_are_structured() {
        let control = control();
        let mut work = WorkControl::new(&control);
        assert!(
            SingleLinkageHierarchy::from_tree(0, &[], &mut work)
                .unwrap()
                .root
                .is_none()
        );
        let single = SingleLinkageHierarchy::from_tree(1, &[], &mut work).unwrap();
        assert_eq!(single.leaves(single.root.unwrap(), &mut work).unwrap(), [0]);

        let chain = (1..130)
            .map(|point| MstEdge {
                left: point - 1,
                right: point,
                distance: 1.0,
            })
            .collect::<Vec<_>>();
        let limited = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            SingleLinkageHierarchy::from_tree(130, &chain, &mut WorkControl::new(&limited)),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancelled = AlgorithmCancellation::default();
        cancelled.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancelled);
        assert_eq!(
            SingleLinkageHierarchy::from_tree(130, &chain, &mut WorkControl::new(&cancelled))
                .unwrap_err(),
            AlgorithmError::Cancelled
        );
        let malformed = [
            (2, vec![]),
            (
                2,
                vec![MstEdge {
                    left: 0,
                    right: 2,
                    distance: 1.0,
                }],
            ),
            (
                2,
                vec![MstEdge {
                    left: 0,
                    right: 1,
                    distance: -1.0,
                }],
            ),
            (
                2,
                vec![MstEdge {
                    left: 0,
                    right: 1,
                    distance: f64::NAN,
                }],
            ),
            (
                3,
                vec![
                    MstEdge {
                        left: 0,
                        right: 1,
                        distance: 1.0,
                    };
                    2
                ],
            ),
        ];
        for (node_count, edges) in malformed {
            assert!(matches!(
                SingleLinkageHierarchy::from_tree(
                    node_count,
                    &edges,
                    &mut WorkControl::new(&control)
                ),
                Err(AlgorithmError::Execution { .. })
            ));
        }
    }

    #[test]
    fn eom_extracts_two_dense_groups_and_leaves_noise() {
        let mut edges = Vec::new();
        for start in [0, 5] {
            for point in (start + 1)..(start + 5) {
                edges.push(MstEdge {
                    left: point - 1,
                    right: point,
                    distance: 1.0,
                });
            }
        }
        edges.extend([
            MstEdge {
                left: 4,
                right: 5,
                distance: 10.0,
            },
            MstEdge {
                left: 9,
                right: 10,
                distance: 20.0,
            },
        ]);
        let tree = ReachabilityTree {
            core_distances: vec![1.0; 11],
            edges,
        };
        let control = control();
        let mut work = WorkControl::new(&control);
        let hierarchy = SingleLinkageHierarchy::from_tree(11, &tree.edges, &mut work).unwrap();
        let root = hierarchy.root.unwrap();
        let mut candidates = vec![Candidate::new(root, 0.0, &hierarchy)];
        condense(
            &hierarchy,
            root,
            0,
            f64::MAX / 22.0,
            &mut candidates,
            &mut work,
        )
        .unwrap();
        assert_eq!(candidates.len(), 3);
        assert_eq!(candidates[0].stability, 1.05);
        assert_eq!(candidates[1].birth, 0.1);
        assert_eq!(candidates[1].stability, 4.5);
        assert_eq!(
            tree.stable_labels(11, &control).unwrap(),
            [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, -1]
        );
    }

    #[test]
    fn root_is_never_selected_and_duplicate_state_stays_finite() {
        let small = ReachabilityTree {
            core_distances: vec![0.0; 4],
            edges: vec![],
        };
        assert_eq!(small.stable_labels(4, &control()).unwrap(), [-1; 4]);

        let duplicate = ReachabilityTree {
            core_distances: vec![0.0; 5],
            edges: (1..5)
                .map(|point| MstEdge {
                    left: point - 1,
                    right: point,
                    distance: 0.0,
                })
                .collect(),
        };
        assert_eq!(duplicate.stable_labels(5, &control()).unwrap(), [-1; 5]);
    }

    #[test]
    fn equal_stability_selects_the_parent_deterministically() {
        let hierarchy = SingleLinkageHierarchy {
            nodes: vec![HierarchyNode {
                left: None,
                right: None,
                distance: 0.0,
                size: 1,
                first: 0,
            }],
            root: Some(0),
        };
        let mut candidates = vec![Candidate::new(0, 0.0, &hierarchy); 4];
        candidates[1].stability = 4.0;
        candidates[1].children = vec![2, 3];
        candidates[2].stability = 2.0;
        candidates[3].stability = 2.0;
        let mut selected = Vec::new();
        let control = control();
        let mut work = WorkControl::new(&control);
        assert_eq!(
            select_eom(1, &candidates, &mut selected, &mut work).unwrap(),
            4.0
        );
        assert_eq!(selected, [1]);
    }

    #[test]
    fn extraction_controls_and_malformed_hierarchies_are_structured() {
        let chain = ReachabilityTree {
            core_distances: vec![1.0; 130],
            edges: (1..130)
                .map(|point| MstEdge {
                    left: point - 1,
                    right: point,
                    distance: 1.0,
                })
                .collect(),
        };
        let limits = AlgorithmLimits {
            iterations: 1,
            ..AlgorithmLimits::default()
        };
        assert!(matches!(
            chain.stable_labels(
                130,
                &AlgorithmControl::new(limits, AlgorithmCancellation::default())
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            chain.stable_labels(
                130,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
        let malformed = ReachabilityTree {
            core_distances: vec![1.0; 5],
            edges: vec![],
        };
        assert!(matches!(
            malformed.stable_labels(5, &control()),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn maximally_skewed_walks_use_bounded_call_stack() {
        const NODE_COUNT: usize = 4_096;
        let chain = ReachabilityTree {
            core_distances: vec![1.0; NODE_COUNT],
            edges: (1..NODE_COUNT)
                .map(|point| MstEdge {
                    left: point - 1,
                    right: point,
                    distance: 1.0,
                })
                .collect(),
        };
        let control = control();
        assert_eq!(
            chain.stable_labels(NODE_COUNT, &control).unwrap(),
            vec![-1; NODE_COUNT]
        );

        let hierarchy = SingleLinkageHierarchy {
            nodes: vec![HierarchyNode {
                left: None,
                right: None,
                distance: 0.0,
                size: 1,
                first: 0,
            }],
            root: Some(0),
        };
        let mut candidates = vec![Candidate::new(0, 0.0, &hierarchy); NODE_COUNT];
        for candidate in 0..(NODE_COUNT - 1) {
            candidates[candidate].children.push(candidate + 1);
        }
        let mut selected = Vec::new();
        let mut work = WorkControl::new(&control);
        assert_eq!(
            select_eom(0, &candidates, &mut selected, &mut work).unwrap(),
            0.0
        );
        assert_eq!(selected, [0]);
    }
}
