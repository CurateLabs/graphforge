//! Speaker listener execution.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BTreeMap, BUILTIN_REVIEW, ClusterAlgorithm, RustAlgorithm,
    canonicalize_partition, checkpoint_chunk, community_output, execution, normalized_communities,
    random_index, shuffle,
};

pub(super) struct SpeakerListener;

impl RustAlgorithm for SpeakerListener {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::SpeakerListener),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = speaker_listener_communities(graph, control)?;
        community_output(
            graph,
            &communities,
            ClusterAlgorithm::SpeakerListener,
            control,
        )
    }
}

fn speaker_listener_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    speaker_listener_communities_with_progress(graph, control, || {})
}

fn speaker_listener_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    mut progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    const SWEEPS: usize = 100;
    let (weights, _) = normalized_communities(graph, control)?;
    let mut memories: Vec<BTreeMap<usize, usize>> = (0..weights.len())
        .map(|label| BTreeMap::from([(label, 1)]))
        .collect();
    let mut lengths = vec![1_usize; weights.len()];
    let mut order: Vec<_> = (0..weights.len()).collect();
    let mut random = 0x0053_4c50_4101_u64;
    let mut work = 0_usize;

    for _ in 0..SWEEPS {
        control.checkpoint()?;
        progress();
        shuffle(&mut order, &mut random, control, &mut work)?;
        for &listener in &order {
            checkpoint_chunk(control, &mut work)?;
            let mut received = BTreeMap::new();
            for &speaker in weights[listener].keys() {
                checkpoint_chunk(control, &mut work)?;
                let label = sample_memory_label(&memories[speaker], lengths[speaker], &mut random)?;
                let count = received.entry(label).or_insert(0_usize);
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| execution("speaker label count exceeds platform range"))?;
            }
            let maximum = received.values().copied().max().unwrap_or(0);
            let dominant: Vec<_> = received
                .into_iter()
                .filter_map(|(label, count)| (count == maximum).then_some(label))
                .collect();
            if !dominant.is_empty() {
                let label = dominant[random_index(&mut random, dominant.len())?];
                let count = memories[listener].entry(label).or_insert(0);
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| execution("listener memory exceeds platform range"))?;
                lengths[listener] = lengths[listener]
                    .checked_add(1)
                    .ok_or_else(|| execution("listener memory exceeds platform range"))?;
            }
        }
    }

    let mut labels = Vec::with_capacity(memories.len());
    for (memory, length) in memories.iter().zip(lengths) {
        checkpoint_chunk(control, &mut work)?;
        let strongest = memory
            .iter()
            .filter(|(_, count)| count.checked_mul(20).is_some_and(|seen| seen >= length))
            .max_by_key(|(label, count)| (**count, std::cmp::Reverse(**label)))
            .or_else(|| {
                memory
                    .iter()
                    .max_by_key(|(label, count)| (**count, std::cmp::Reverse(**label)))
            })
            .map(|(&label, _)| label)
            .ok_or_else(|| execution("speaker-listener memory is empty"))?;
        labels.push(strongest);
    }
    canonicalize_partition(&mut labels, control)?;
    Ok(labels)
}

fn sample_memory_label(
    memory: &BTreeMap<usize, usize>,
    length: usize,
    random: &mut u64,
) -> Result<usize, AlgorithmError> {
    let mut selected = random_index(random, length)?;
    for (&label, &count) in memory {
        if selected < count {
            return Ok(label);
        }
        selected -= count;
    }
    Err(execution("speaker-listener memory length is inconsistent"))
}

#[cfg(test)]
mod tests;
