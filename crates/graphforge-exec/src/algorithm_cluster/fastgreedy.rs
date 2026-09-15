//! Fastgreedy execution.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BTreeMap, BTreeSet, BUILTIN_REVIEW, ClusterAlgorithm, RustAlgorithm,
    WeightedAdjacency, canonicalize_partition, checkpoint_chunk, community_output, execution,
    normalized_communities, partition_modularity,
};

pub(super) struct FastGreedy;

impl RustAlgorithm for FastGreedy {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::FastGreedy),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = fastgreedy_communities(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::FastGreedy, control)
    }
}

fn fastgreedy_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    fastgreedy_communities_with_progress(graph, control, || {})
}

pub(super) fn fastgreedy_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    let (weights, _) = normalized_communities(graph, control)?;
    fastgreedy_from_weights(&weights, control, progress)
}

fn fastgreedy_from_weights(
    weights: &WeightedAdjacency,
    control: &AlgorithmControl,
    progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    fastgreedy_from_weights_with_updates(weights, control, progress, |_, _, _| {})
}

fn fastgreedy_from_weights_with_updates(
    weights: &WeightedAdjacency,
    control: &AlgorithmControl,
    mut progress: impl FnMut(),
    mut observe_updates: impl FnMut(usize, usize, usize),
) -> Result<Vec<usize>, AlgorithmError> {
    let singleton: Vec<_> = (0..weights.len()).collect();
    let mut best_score = partition_modularity(weights, &singleton, "Fastgreedy", control)?;
    let mut adjacency = weights.clone();
    let mut degrees = Vec::with_capacity(adjacency.len());
    let mut total = 0.0;
    let mut work = 0_usize;
    for neighbors in &adjacency {
        let mut degree = 0.0;
        for &weight in neighbors.values() {
            checkpoint_chunk(control, &mut work)?;
            degree += weight;
        }
        degrees.push(degree);
        total += degree;
    }
    if !total.is_finite() {
        return Err(execution("Fastgreedy total edge weight is not finite"));
    }
    if total == 0.0 {
        return Ok(singleton);
    }

    let mut gains = BTreeMap::new();
    for (source, neighbors) in adjacency.iter().enumerate() {
        for (&target, &weight) in neighbors {
            checkpoint_chunk(control, &mut work)?;
            if target >= adjacency.len() {
                return Err(execution("Fastgreedy adjacency index is out of range"));
            }
            if source < target {
                gains.insert(
                    (source, target),
                    fastgreedy_gain(weight, degrees[source], degrees[target], total)?,
                );
            }
        }
    }

    let mut merge_history = Vec::new();
    let mut best_merge_count = 0_usize;
    let mut current_score = best_score;
    while !gains.is_empty() {
        let mut selected = None;
        let mut selected_gain = f64::NEG_INFINITY;
        for (&pair, &gain) in &gains {
            checkpoint_chunk(control, &mut work)?;
            if gain > selected_gain + 1e-12 {
                selected = Some(pair);
                selected_gain = gain;
            }
        }
        let Some((left, right)) = selected else {
            break;
        };
        progress();
        control.checkpoint()?;

        let mut affected: BTreeSet<_> = adjacency[left]
            .keys()
            .chain(adjacency[right].keys())
            .copied()
            .collect();
        affected.retain(|&community| community != left && community != right);
        let right_neighbors = std::mem::take(&mut adjacency[right]);
        gains.remove(&(left, right));
        for &neighbor in &affected {
            checkpoint_chunk(control, &mut work)?;
            gains.remove(&(left.min(neighbor), left.max(neighbor)));
            gains.remove(&(right.min(neighbor), right.max(neighbor)));
            let combined = adjacency[left].remove(&neighbor).unwrap_or(0.0)
                + right_neighbors.get(&neighbor).copied().unwrap_or(0.0);
            adjacency[neighbor].remove(&left);
            adjacency[neighbor].remove(&right);
            if combined != 0.0 {
                adjacency[left].insert(neighbor, combined);
                adjacency[neighbor].insert(left, combined);
            }
        }
        adjacency[left].remove(&right);
        degrees[left] += degrees[right];
        degrees[right] = 0.0;

        let mut updated = 0_usize;
        for &neighbor in &affected {
            if let Some(&weight) = adjacency[left].get(&neighbor) {
                gains.insert(
                    (left.min(neighbor), left.max(neighbor)),
                    fastgreedy_gain(weight, degrees[left], degrees[neighbor], total)?,
                );
                updated += 1;
            }
        }
        observe_updates(left, right, updated);
        merge_history.push((left, right));
        current_score += selected_gain;
        if !current_score.is_finite() {
            return Err(execution("Fastgreedy modularity is not finite"));
        }
        if current_score > best_score + 1e-12 {
            best_score = current_score;
            best_merge_count = merge_history.len();
        }
    }

    fastgreedy_partition(weights.len(), &merge_history[..best_merge_count], control)
}

fn fastgreedy_gain(
    edge_weight: f64,
    left_degree: f64,
    right_degree: f64,
    total: f64,
) -> Result<f64, AlgorithmError> {
    let gain = 2.0 * edge_weight / total - 2.0 * left_degree * right_degree / total.powi(2);
    if gain.is_finite() {
        Ok(gain)
    } else {
        Err(execution("Fastgreedy modularity gain is not finite"))
    }
}

fn fastgreedy_partition(
    node_count: usize,
    merges: &[(usize, usize)],
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut parent: Vec<_> = (0..node_count).collect();
    let mut work = 0_usize;
    for &(left, right) in merges {
        checkpoint_chunk(control, &mut work)?;
        parent[right] = left;
    }
    let mut partition = Vec::with_capacity(node_count);
    for node in 0..node_count {
        let mut representative = node;
        while parent[representative] != representative {
            checkpoint_chunk(control, &mut work)?;
            representative = parent[representative];
        }
        let mut current = node;
        while parent[current] != current {
            checkpoint_chunk(control, &mut work)?;
            let next = parent[current];
            parent[current] = representative;
            current = next;
        }
        partition.push(representative);
    }
    canonicalize_partition(&mut partition, control)?;
    Ok(partition)
}

#[cfg(test)]
mod tests;
