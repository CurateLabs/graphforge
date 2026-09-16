//! Label propagation execution.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BTreeMap, BUILTIN_REVIEW, ClusterAlgorithm, RustAlgorithm,
    canonicalize_partition, checkpoint_chunk, community_output, execution, normalized_communities,
    random_index, shuffle,
};

pub(super) struct LabelPropagation;

impl RustAlgorithm for LabelPropagation {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::LabelPropagation),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = label_propagation_communities(graph, control)?;
        community_output(
            graph,
            &communities,
            ClusterAlgorithm::LabelPropagation,
            control,
        )
    }
}

fn label_propagation_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    label_propagation_communities_with_progress(graph, control, || {})
}

fn label_propagation_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    mut progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    let (weights, _) = normalized_communities(graph, control)?;
    let mut labels: Vec<_> = (0..weights.len()).collect();
    let mut order: Vec<_> = (0..weights.len()).collect();
    let mut random = 0x004c_4142_454c_u64;
    let mut work = 0;

    loop {
        control.checkpoint()?;
        progress();
        shuffle(&mut order, &mut random, control, &mut work)?;
        for &node in &order {
            checkpoint_chunk(control, &mut work)?;
            let dominant = dominant_neighbor_labels(&weights, &labels, node, control, &mut work)?;
            if !dominant.is_empty() {
                labels[node] = dominant[random_index(&mut random, dominant.len())?];
            }
        }

        let mut stable = true;
        for node in 0..weights.len() {
            checkpoint_chunk(control, &mut work)?;
            let dominant = dominant_neighbor_labels(&weights, &labels, node, control, &mut work)?;
            if !dominant.is_empty() && !dominant.contains(&labels[node]) {
                stable = false;
                break;
            }
        }
        if stable {
            break;
        }
    }

    canonicalize_partition(&mut labels, control)?;
    Ok(labels)
}

fn dominant_neighbor_labels(
    weights: &[BTreeMap<usize, f64>],
    labels: &[usize],
    node: usize,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut counts = BTreeMap::new();
    for &neighbor in weights[node].keys() {
        checkpoint_chunk(control, work)?;
        let count = counts.entry(labels[neighbor]).or_insert(0_usize);
        *count = count
            .checked_add(1)
            .ok_or_else(|| execution("label frequency exceeds platform range"))?;
    }
    let maximum = counts.values().copied().max().unwrap_or(0);
    Ok(counts
        .into_iter()
        .filter_map(|(label, count)| (count == maximum).then_some(label))
        .collect())
}

#[cfg(test)]
mod tests;
