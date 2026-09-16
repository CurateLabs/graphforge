//! Girvan newman execution.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BTreeMap, BUILTIN_REVIEW, ClusterAlgorithm, RustAlgorithm, VecDeque,
    WeightedAdjacency, canonicalize_partition, checkpoint_chunk, community_output, execution,
    normalized_communities, partition_modularity,
};

pub(super) struct GirvanNewman;

impl RustAlgorithm for GirvanNewman {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::GirvanNewman),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = girvan_newman_communities_with_progress(graph, control, || {})?;
        community_output(graph, &communities, ClusterAlgorithm::GirvanNewman, control)
    }
}

fn girvan_newman_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    mut progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    let (original, _) = normalized_communities(graph, control)?;
    let mut current = original.clone();
    let mut level = component_partition(&current, control)?;
    let mut best = level.clone();
    let mut best_score = partition_modularity(&original, &best, "Girvan-Newman", control)?;

    while current.iter().any(|neighbors| !neighbors.is_empty()) {
        control.checkpoint()?;
        progress();
        let scores = edge_betweenness(&current, control)?;
        let ((left, right), _) = scores
            .into_iter()
            .reduce(|best, candidate| {
                let better = candidate.1 > best.1 + 1e-12
                    || ((candidate.1 - best.1).abs() <= 1e-12 && candidate.0 < best.0);
                if better { candidate } else { best }
            })
            .ok_or_else(|| execution("non-empty graph has no removable edge"))?;
        current[left].remove(&right);
        current[right].remove(&left);
        let candidate = component_partition(&current, control)?;
        if candidate != level {
            let score = partition_modularity(&original, &candidate, "Girvan-Newman", control)?;
            if score > best_score + 1e-12 {
                best_score = score;
                best.clone_from(&candidate);
            }
            level = candidate;
        }
    }
    Ok(best)
}

fn edge_betweenness(
    graph: &WeightedAdjacency,
    control: &AlgorithmControl,
) -> Result<BTreeMap<(usize, usize), f64>, AlgorithmError> {
    let mut scores = BTreeMap::new();
    for (node, neighbors) in graph.iter().enumerate() {
        for &neighbor in neighbors.keys().filter(|&&neighbor| node < neighbor) {
            scores.insert((node, neighbor), 0.0);
        }
    }
    let mut work = 0_usize;
    for source in 0..graph.len() {
        checkpoint_chunk(control, &mut work)?;
        let mut stack = Vec::new();
        let mut predecessors = vec![Vec::new(); graph.len()];
        let mut paths = vec![0.0_f64; graph.len()];
        let mut distance = vec![usize::MAX; graph.len()];
        let mut queue = VecDeque::from([source]);
        paths[source] = 1.0;
        distance[source] = 0;
        while let Some(node) = queue.pop_front() {
            checkpoint_chunk(control, &mut work)?;
            stack.push(node);
            let next = distance[node]
                .checked_add(1)
                .ok_or_else(|| execution("shortest-path depth exceeds platform range"))?;
            for &neighbor in graph[node].keys() {
                checkpoint_chunk(control, &mut work)?;
                if distance[neighbor] == usize::MAX {
                    distance[neighbor] = next;
                    queue.push_back(neighbor);
                }
                if distance[neighbor] == next {
                    paths[neighbor] += paths[node];
                    if !paths[neighbor].is_finite() {
                        return Err(execution("shortest-path count is not finite"));
                    }
                    predecessors[neighbor].push(node);
                }
            }
        }
        let mut dependency = vec![0.0_f64; graph.len()];
        while let Some(node) = stack.pop() {
            for &predecessor in &predecessors[node] {
                checkpoint_chunk(control, &mut work)?;
                let contribution = paths[predecessor] / paths[node] * (1.0 + dependency[node]);
                let edge = if predecessor < node {
                    (predecessor, node)
                } else {
                    (node, predecessor)
                };
                let score = scores
                    .get_mut(&edge)
                    .ok_or_else(|| execution("shortest path references a missing edge"))?;
                *score += contribution;
                dependency[predecessor] += contribution;
                if !score.is_finite() || !dependency[predecessor].is_finite() {
                    return Err(execution("edge betweenness is not finite"));
                }
            }
        }
    }
    Ok(scores)
}

fn component_partition(
    graph: &WeightedAdjacency,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut partition = vec![usize::MAX; graph.len()];
    let mut work = 0_usize;
    for start in 0..graph.len() {
        checkpoint_chunk(control, &mut work)?;
        if partition[start] != usize::MAX {
            continue;
        }
        let community = start;
        partition[start] = community;
        let mut queue = VecDeque::from([start]);
        while let Some(node) = queue.pop_front() {
            for &neighbor in graph[node].keys() {
                checkpoint_chunk(control, &mut work)?;
                if partition[neighbor] == usize::MAX {
                    partition[neighbor] = community;
                    queue.push_back(neighbor);
                }
            }
        }
    }
    canonicalize_partition(&mut partition, control)?;
    Ok(partition)
}

#[cfg(test)]
mod tests;
