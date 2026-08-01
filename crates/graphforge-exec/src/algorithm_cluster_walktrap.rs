//! Deterministic transition-distance kernel for Rust-owned Walktrap clustering.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

pub(crate) const MAX_WALKTRAP_NODES: usize = 4_096;
const WALK_STEPS: usize = 4;
const SCORE_TOLERANCE: f64 = 1e-12;

#[derive(Debug)]
struct Community {
    members: Vec<usize>,
    centroid: Vec<f64>,
    representative: usize,
    volume: f64,
}

#[derive(Clone, Copy, Debug)]
struct MergeCandidate {
    cost: f64,
    cross_edges: f64,
}

#[derive(Debug)]
pub(crate) struct WalktrapGraph {
    adjacency: Vec<BTreeSet<usize>>,
    degree: Vec<f64>,
    distributions: Vec<Vec<f64>>,
}

impl WalktrapGraph {
    pub(crate) fn from_graph(
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<Self, AlgorithmError> {
        control.checkpoint()?;
        let node_count = graph.node_ids().len();
        if node_count > MAX_WALKTRAP_NODES {
            return Err(AlgorithmError::NodeLimit {
                observed: node_count as u64,
                limit: MAX_WALKTRAP_NODES as u64,
            });
        }
        let indices: HashMap<_, _> = graph
            .node_ids()
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let mut adjacency = vec![BTreeSet::new(); node_count];
        let mut work = 0_usize;
        for (source, &node) in graph.node_ids().iter().enumerate() {
            for edge in graph.neighbors(node) {
                checkpoint_chunk(control, &mut work)?;
                let target = indices
                    .get(&edge.neighbor_id)
                    .copied()
                    .ok_or_else(|| execution("adjacency references an unselected node"))?;
                if source != target {
                    adjacency[source].insert(target);
                    adjacency[target].insert(source);
                }
            }
        }
        let degree = adjacency
            .iter()
            .map(|neighbors| count(neighbors.len()))
            .collect::<Result<Vec<_>, _>>()?;
        let distributions = transition_distributions(&adjacency, &degree, control)?;
        Ok(Self {
            adjacency,
            degree,
            distributions,
        })
    }

    #[cfg(test)]
    fn are_adjacent(&self, left: &[usize], right: &[usize]) -> bool {
        left.iter().any(|&node| {
            right
                .iter()
                .any(|other| self.adjacency[node].contains(other))
        })
    }

    #[cfg(test)]
    fn distance(
        &self,
        left: &[usize],
        right: &[usize],
        control: &AlgorithmControl,
    ) -> Result<f64, AlgorithmError> {
        control.checkpoint()?;
        if left.is_empty() || right.is_empty() {
            return Err(execution("Walktrap community cannot be empty"));
        }
        let left_size = count(left.len())?;
        let right_size = count(right.len())?;
        let mut squared = 0.0;
        let mut work = 0_usize;
        for target in 0..self.distributions.len() {
            checkpoint_chunk(control, &mut work)?;
            let left_probability = left
                .iter()
                .map(|&node| self.probability(node, target))
                .sum::<Result<f64, _>>()?
                / left_size;
            let right_probability = right
                .iter()
                .map(|&node| self.probability(node, target))
                .sum::<Result<f64, _>>()?
                / right_size;
            squared +=
                (left_probability - right_probability).powi(2) / self.degree[target].max(1.0);
        }
        let distance = squared.sqrt();
        if distance.is_finite() {
            Ok(distance)
        } else {
            Err(execution("Walktrap distance is not finite"))
        }
    }

    #[cfg(test)]
    fn probability(&self, source: usize, target: usize) -> Result<f64, AlgorithmError> {
        self.distributions
            .get(source)
            .and_then(|row| row.get(target))
            .copied()
            .filter(|value| value.is_finite())
            .ok_or_else(|| execution("Walktrap transition probability is invalid"))
    }
}

pub(crate) fn walktrap_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    walktrap_communities_with_progress(graph, control, |_| {})
}

fn walktrap_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    mut progress: impl FnMut((usize, usize)),
) -> Result<Vec<usize>, AlgorithmError> {
    let walk = WalktrapGraph::from_graph(graph, control)?;
    let node_count = walk.adjacency.len();
    if node_count == 0 {
        return Ok(Vec::new());
    }
    let (total_volume, mut modularity) = singleton_modularity(&walk.degree);
    let mut communities: BTreeMap<_, _> = (0..node_count)
        .map(|node| {
            (
                node,
                Community {
                    members: vec![node],
                    centroid: walk.distributions[node].clone(),
                    representative: node,
                    volume: walk.degree[node],
                },
            )
        })
        .collect();
    let mut candidates = BTreeMap::new();
    let mut work = 0_usize;
    for source in 0..node_count {
        for &target in &walk.adjacency[source] {
            checkpoint_chunk(control, &mut work)?;
            if source < target {
                let cost = ward_cost(
                    &communities[&source],
                    &communities[&target],
                    &walk.degree,
                    node_count,
                )?;
                candidates.insert(
                    (source, target),
                    MergeCandidate {
                        cost,
                        cross_edges: 1.0,
                    },
                );
            }
        }
    }
    let mut best = assignment(&communities, node_count);
    let mut best_modularity = modularity;
    let mut next_id = node_count;
    while let Some(pair) = best_candidate(&candidates, &communities) {
        progress(pair);
        control.checkpoint()?;
        let selected = candidates[&pair];
        let left = communities
            .remove(&pair.0)
            .ok_or_else(|| execution("Walktrap merge references a missing community"))?;
        let right = communities
            .remove(&pair.1)
            .ok_or_else(|| execution("Walktrap merge references a missing community"))?;
        let left_volume = left.volume;
        let right_volume = right.volume;
        let mut neighboring = BTreeMap::<usize, f64>::new();
        for (&(first, second), candidate) in &candidates {
            let neighbor = if first == pair.0 || first == pair.1 {
                Some(second)
            } else if second == pair.0 || second == pair.1 {
                Some(first)
            } else {
                None
            };
            if let Some(neighbor) = neighbor.filter(|id| *id != pair.0 && *id != pair.1) {
                *neighboring.entry(neighbor).or_default() += candidate.cross_edges;
            }
        }
        candidates.retain(|&(first, second), _| {
            first != pair.0 && first != pair.1 && second != pair.0 && second != pair.1
        });
        let merged = merge_communities(left, right)?;
        if total_volume > 0.0 {
            modularity += 2.0 * selected.cross_edges / total_volume
                - 2.0 * left_volume * right_volume / total_volume.powi(2);
        }
        communities.insert(next_id, merged);
        for (neighbor, cross_edges) in neighboring {
            let cost = ward_cost(
                &communities[&next_id],
                &communities[&neighbor],
                &walk.degree,
                node_count,
            )?;
            candidates.insert(
                ordered_pair(next_id, neighbor),
                MergeCandidate { cost, cross_edges },
            );
        }
        if modularity > best_modularity + SCORE_TOLERANCE {
            best_modularity = modularity;
            best = assignment(&communities, node_count);
        }
        next_id += 1;
    }
    Ok(best)
}

fn singleton_modularity(degree: &[f64]) -> (f64, f64) {
    let total: f64 = degree.iter().sum();
    let score = if total == 0.0 {
        0.0
    } else {
        -degree
            .iter()
            .map(|value| (value / total).powi(2))
            .sum::<f64>()
    };
    (total, score)
}

fn merge_communities(left: Community, right: Community) -> Result<Community, AlgorithmError> {
    let left_size = count(left.members.len())?;
    let right_size = count(right.members.len())?;
    let total = left_size + right_size;
    let centroid = left
        .centroid
        .iter()
        .zip(&right.centroid)
        .map(|(left, right)| (left * left_size + right * right_size) / total)
        .collect();
    let mut members = left.members;
    members.extend(right.members);
    members.sort_unstable();
    Ok(Community {
        representative: members[0],
        centroid,
        volume: left.volume + right.volume,
        members,
    })
}

fn ward_cost(
    left: &Community,
    right: &Community,
    degree: &[f64],
    node_count: usize,
) -> Result<f64, AlgorithmError> {
    let squared_distance: f64 = left
        .centroid
        .iter()
        .zip(&right.centroid)
        .zip(degree)
        .map(|((&left, &right), &degree)| (left - right).powi(2) / degree.max(1.0))
        .sum();
    let left_size = count(left.members.len())?;
    let right_size = count(right.members.len())?;
    let cost =
        left_size * right_size / (left_size + right_size) * squared_distance / count(node_count)?;
    if cost.is_finite() {
        Ok(cost)
    } else {
        Err(execution("Walktrap merge cost is not finite"))
    }
}

fn best_candidate(
    candidates: &BTreeMap<(usize, usize), MergeCandidate>,
    communities: &BTreeMap<usize, Community>,
) -> Option<(usize, usize)> {
    candidates.keys().copied().fold(None, |best, pair| {
        let representatives = ordered_pair(
            communities[&pair.0].representative,
            communities[&pair.1].representative,
        );
        match best {
            None => Some(pair),
            Some(current) => {
                let current_representatives = ordered_pair(
                    communities[&current.0].representative,
                    communities[&current.1].representative,
                );
                let cost = candidates[&pair].cost;
                let current_cost = candidates[&current].cost;
                if cost < current_cost - SCORE_TOLERANCE
                    || ((cost - current_cost).abs() <= SCORE_TOLERANCE
                        && representatives < current_representatives)
                {
                    Some(pair)
                } else {
                    Some(current)
                }
            }
        }
    })
}

fn assignment(communities: &BTreeMap<usize, Community>, node_count: usize) -> Vec<usize> {
    let mut ordered: Vec<_> = communities.values().collect();
    ordered.sort_by_key(|community| community.representative);
    let mut assignment = vec![0; node_count];
    for (community_id, community) in ordered.into_iter().enumerate() {
        for &node in &community.members {
            assignment[node] = community_id;
        }
    }
    assignment
}

fn ordered_pair(left: usize, right: usize) -> (usize, usize) {
    if left < right {
        (left, right)
    } else {
        (right, left)
    }
}

fn transition_distributions(
    adjacency: &[BTreeSet<usize>],
    degree: &[f64],
    control: &AlgorithmControl,
) -> Result<Vec<Vec<f64>>, AlgorithmError> {
    let mut rows = Vec::with_capacity(adjacency.len());
    let mut work = 0_usize;
    for source in 0..adjacency.len() {
        control.checkpoint()?;
        let mut current = vec![0.0; adjacency.len()];
        current[source] = 1.0;
        for _ in 0..WALK_STEPS {
            let mut next = vec![0.0; adjacency.len()];
            for (node, neighbors) in adjacency.iter().enumerate() {
                checkpoint_chunk(control, &mut work)?;
                if neighbors.is_empty() {
                    next[node] += current[node];
                } else {
                    let share = current[node] / degree[node];
                    for &neighbor in neighbors {
                        checkpoint_chunk(control, &mut work)?;
                        next[neighbor] += share;
                    }
                }
            }
            current = next;
        }
        if current.iter().any(|value| !value.is_finite()) {
            return Err(execution("Walktrap transition probability is not finite"));
        }
        rows.push(current);
    }
    Ok(rows)
}

fn checkpoint_chunk(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work += 1;
    if *work == 1_024 {
        control.checkpoint()?;
        *work = 0;
    }
    Ok(())
}

fn count(value: usize) -> Result<f64, AlgorithmError> {
    u32::try_from(value)
        .map(f64::from)
        .map_err(|_| execution("Walktrap graph count exceeds numeric range"))
}

fn execution(message: &str) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn control(limits: AlgorithmLimits) -> AlgorithmControl {
        AlgorithmControl::new(limits, AlgorithmCancellation::default())
    }

    fn run(graph: &AdjacencyGraph) -> Vec<usize> {
        walktrap_communities(graph, &control(AlgorithmLimits::default())).unwrap()
    }

    #[test]
    fn four_step_distributions_and_distances_are_hand_verifiable() {
        let setup = control(AlgorithmLimits::default());
        let walk = WalktrapGraph::from_graph(
            &AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2)]),
            &setup,
        )
        .unwrap();
        assert_eq!(walk.distributions[0], [0.5, 0.0, 0.5, 0.0]);
        assert_eq!(walk.distributions[1], [0.0, 1.0, 0.0, 0.0]);
        assert_eq!(walk.distributions[3], [0.0, 0.0, 0.0, 1.0]);
        assert_eq!(walk.distance(&[0], &[2], &setup).unwrap(), 0.0);
        assert!((walk.distance(&[0, 2], &[1], &setup).unwrap() - 1.0).abs() < 1e-12);
        assert!(walk.are_adjacent(&[0], &[1]));
        assert!(!walk.are_adjacent(&[0], &[3]));
    }

    #[test]
    fn projection_normalizes_edges_and_enforces_boundaries() {
        let setup = control(AlgorithmLimits::default());
        let walk = WalktrapGraph::from_graph(
            &AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 0), (0, 1), (0, 0)]),
            &setup,
        )
        .unwrap();
        assert_eq!(
            walk.adjacency,
            [BTreeSet::from([1]), BTreeSet::from([0]), BTreeSet::new()]
        );
        assert!(
            WalktrapGraph::from_graph(&AdjacencyGraph::default(), &setup)
                .unwrap()
                .distributions
                .is_empty()
        );
        assert!(matches!(
            WalktrapGraph::from_graph(&AdjacencyGraph::with_test_edges(4_097, &[]), &setup),
            Err(AlgorithmError::NodeLimit {
                observed: 4_097,
                limit: 4_096
            })
        ));
    }

    #[test]
    fn controls_and_numeric_validation_are_structured() {
        let no_iterations = control(AlgorithmLimits {
            iterations: 0,
            ..AlgorithmLimits::default()
        });
        assert!(matches!(
            WalktrapGraph::from_graph(&AdjacencyGraph::default(), &no_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            WalktrapGraph::from_graph(
                &AdjacencyGraph::default(),
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        ));
        let setup = control(AlgorithmLimits::default());
        let mut walk =
            WalktrapGraph::from_graph(&AdjacencyGraph::with_test_edges(2, &[(0, 1)]), &setup)
                .unwrap();
        assert!(matches!(
            walk.distance(&[], &[1], &setup),
            Err(AlgorithmError::Execution { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            walk.distance(
                &[0],
                &[1],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
        walk.distributions[0][0] = f64::NAN;
        assert!(matches!(
            walk.distance(&[0], &[1], &setup),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn agglomeration_selects_the_stable_maximum_modularity_partition() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)],
        );
        let first = run(&graph);
        assert_eq!(first, [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(run(&graph), first);

        let tied = AdjacencyGraph::with_test_edges(5, &[(0, 1), (2, 3)]);
        let mut first_merge = None;
        let partition = walktrap_communities_with_progress(
            &tied,
            &control(AlgorithmLimits::default()),
            |pair| {
                if first_merge.is_none() {
                    first_merge = Some(pair);
                }
            },
        )
        .unwrap();
        assert_eq!(first_merge, Some((0, 1)));
        assert_eq!(partition, [0, 0, 1, 1, 2]);
    }

    #[test]
    fn agglomeration_observes_boundaries_limits_and_midflight_cancellation() {
        assert!(run(&AdjacencyGraph::default()).is_empty());
        assert_eq!(run(&AdjacencyGraph::with_test_edges(3, &[])), [0, 1, 2]);
        assert!(matches!(
            walktrap_communities(
                &AdjacencyGraph::with_test_edges(4, &[(0, 1), (2, 3)]),
                &control(AlgorithmLimits {
                    iterations: 5,
                    ..AlgorithmLimits::default()
                })
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        let cancel = cancellation.clone();
        assert_eq!(
            walktrap_communities_with_progress(
                &AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
                |_| cancel.cancel(),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
