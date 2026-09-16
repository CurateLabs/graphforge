//! Leiden execution.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BTreeMap, BUILTIN_REVIEW, ClusterAlgorithm, LeidenExecutionPath,
    RustAlgorithm, canonicalize_partition, checkpoint_chunk, community_output, condense, execution,
    local_moves_from, next_random, normalized_communities, select_leiden_path,
};

pub(super) struct Leiden;

impl RustAlgorithm for Leiden {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::Leiden),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = leiden_communities(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::Leiden, control)
    }
}

fn leiden_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    match select_leiden_path(control, graph.node_ids().len(), graph.edge_entry_count()) {
        LeidenExecutionPath::SerialRefinement => {}
    }
    let (mut weights, mut members) = normalized_communities(graph, control)?;
    let mut seed = None;
    let mut random = 0x4c45_4944_454e_u64;
    loop {
        let coarse = local_moves_from(&weights, seed.as_deref(), "Leiden", control)?;
        let refined = refine_partition(&weights, &coarse, &mut random, control)?;
        let count = refined.iter().copied().max().map_or(0, |id| id + 1);
        if count == weights.len() {
            return expand_partition(&members, &coarse, graph.node_ids().len(), control);
        }

        let mut next_seed = vec![0; count];
        let mut work = 0;
        for node in 0..weights.len() {
            checkpoint_chunk(control, &mut work)?;
            next_seed[refined[node]] = coarse[node];
        }
        canonicalize_partition(&mut next_seed, control)?;
        (weights, members) = condense(&weights, &members, &refined, count, control)?;
        seed = Some(next_seed);
    }
}

fn refine_partition(
    weights: &[BTreeMap<usize, f64>],
    coarse: &[usize],
    random: &mut u64,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut refined: Vec<usize> = (0..weights.len()).collect();
    let mut degrees = vec![0.0; weights.len()];
    let mut total_weight = 0.0;
    let mut work = 0;
    for (node, neighbors) in weights.iter().enumerate() {
        for weight in neighbors.values() {
            checkpoint_chunk(control, &mut work)?;
            degrees[node] += weight;
        }
        total_weight += degrees[node];
    }
    if !total_weight.is_finite() {
        return Err(execution("Leiden total edge weight is not finite"));
    }
    if total_weight == 0.0 {
        return Ok(refined);
    }

    let mut coarse_totals = vec![0.0; weights.len()];
    for (node, &community) in coarse.iter().enumerate() {
        coarse_totals[community] += degrees[node];
    }
    let mut refined_totals = degrees.clone();
    let mut sizes = vec![1_usize; weights.len()];
    for node in 0..weights.len() {
        checkpoint_chunk(control, &mut work)?;
        let old = refined[node];
        if sizes[old] != 1 || degrees[node] == 0.0 {
            continue;
        }
        let parent = coarse[node];
        let mut parent_weight = 0.0;
        let mut candidates = BTreeMap::new();
        for (&neighbor, &weight) in &weights[node] {
            checkpoint_chunk(control, &mut work)?;
            if neighbor != node && coarse[neighbor] == parent {
                parent_weight += weight;
                if refined[neighbor] != old {
                    *candidates.entry(refined[neighbor]).or_insert(0.0) += weight;
                }
            }
        }
        let threshold = degrees[node] * (coarse_totals[parent] - degrees[node]) / total_weight;
        if parent_weight + 1e-12 < threshold {
            continue;
        }

        let mut gains = Vec::new();
        for (candidate, internal) in candidates {
            checkpoint_chunk(control, &mut work)?;
            let gain = internal - degrees[node] * refined_totals[candidate] / total_weight;
            if !gain.is_finite() {
                return Err(execution("Leiden refinement gain is not finite"));
            }
            if gain > 1e-12 {
                gains.push((candidate, gain));
            }
        }
        if gains.is_empty() {
            continue;
        }
        let selected = weighted_choice(&gains, random)?;
        refined[node] = selected;
        refined_totals[old] -= degrees[node];
        refined_totals[selected] += degrees[node];
        sizes[old] = 0;
        sizes[selected] += 1;
    }
    canonicalize_partition(&mut refined, control)?;
    Ok(refined)
}

fn weighted_choice(gains: &[(usize, f64)], random: &mut u64) -> Result<usize, AlgorithmError> {
    let max_gain = gains
        .iter()
        .map(|entry| entry.1)
        .reduce(f64::max)
        .expect("non-empty refinement gains");
    let scaled: Vec<_> = gains
        .iter()
        .map(|&(candidate, gain)| (candidate, ((gain - max_gain) / 0.01).exp()))
        .collect();
    let total: f64 = scaled.iter().map(|entry| entry.1).sum();
    if !total.is_finite() || total <= 0.0 {
        return Err(execution("Leiden refinement probability is not finite"));
    }
    let mut draw = next_unit(random) * total;
    for &(candidate, probability) in &scaled {
        if draw < probability {
            return Ok(candidate);
        }
        draw -= probability;
    }
    Ok(scaled.last().expect("non-empty probabilities").0)
}

fn next_unit(state: &mut u64) -> f64 {
    let value = next_random(state);
    let high = u32::try_from(value >> 32).expect("upper random bits fit UInt32");
    let low = u32::try_from((value >> 11) & 0x1f_ffff).expect("lower random bits fit UInt32");
    (f64::from(high) * 2_097_152.0 + f64::from(low)) / 9_007_199_254_740_992.0
}

fn expand_partition(
    members: &[Vec<usize>],
    assignment: &[usize],
    node_count: usize,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut result = vec![0; node_count];
    let mut work = 0;
    for (node, originals) in members.iter().enumerate() {
        for &original in originals {
            checkpoint_chunk(control, &mut work)?;
            result[original] = assignment[node];
        }
    }
    canonicalize_partition(&mut result, control)?;
    Ok(result)
}

#[cfg(test)]
mod tests;
