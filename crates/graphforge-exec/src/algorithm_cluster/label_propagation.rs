//! Label propagation execution.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BTreeMap, BUILTIN_REVIEW, ClusterAlgorithm, RustAlgorithm,
    canonicalize_partition, checkpoint_chunk, community_output, execution, random_index, shuffle,
};
use crate::algorithm_graph::OrdinalRows;

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
        if let Some(options) = control.synchronous_label_propagation() {
            let labels = synchronous_labels(graph, options, control)?;
            return super::label_output(
                graph,
                &labels,
                ClusterAlgorithm::LabelPropagation,
                control,
            );
        }
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
    let mut work = 0;
    // The distinct neighbors of each node in either direction, ascending.
    let out = OrdinalRows::distinct_out(graph, || checkpoint_chunk(control, &mut work))?;
    let neighbors = out.union(&out.transpose()?)?;
    drop(out);
    let mut labels: Vec<_> = (0..neighbors.len()).collect();
    let mut order: Vec<_> = (0..neighbors.len()).collect();
    let mut random = 0x004c_4142_454c_u64;

    loop {
        control.checkpoint()?;
        progress();
        shuffle(&mut order, &mut random, control, &mut work)?;
        for &node in &order {
            checkpoint_chunk(control, &mut work)?;
            let dominant = dominant_neighbor_labels(&neighbors, &labels, node, control, &mut work)?;
            if !dominant.is_empty() {
                labels[node] = dominant[random_index(&mut random, dominant.len())?];
            }
        }

        let mut stable = true;
        for node in 0..neighbors.len() {
            checkpoint_chunk(control, &mut work)?;
            let dominant = dominant_neighbor_labels(&neighbors, &labels, node, control, &mut work)?;
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
    neighbors: &OrdinalRows,
    labels: &[usize],
    node: usize,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut counts = BTreeMap::new();
    for &neighbor in neighbors.row(node) {
        checkpoint_chunk(control, work)?;
        let count = counts.entry(labels[neighbor as usize]).or_insert(0_usize);
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

/// Build distinct directional neighbor votes without collapsing reciprocal arcs.
fn synchronous_labels(
    graph: &AdjacencyGraph,
    options: &graphforge_core::SynchronousLabelPropagationOptions,
    control: &AlgorithmControl,
) -> Result<Vec<i64>, AlgorithmError> {
    let mut labels = (0..graph.node_count())
        .map(|index| {
            if options.initial_label_property.is_some() {
                graph
                    .initial_label_of_ordinal(index)
                    .ok_or_else(|| execution("selected node has no initial label"))
            } else {
                i64::try_from(index).map_err(|_| execution("initial label exceeds Int64 range"))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut work = 0;
    // Each node votes for its distinct out-neighbors; a directed graph also
    // lets each target vote back, without collapsing reciprocal arcs.
    let outbound = OrdinalRows::distinct_out(graph, || checkpoint_chunk(control, &mut work))?;
    let inbound = if graph.is_directed() {
        Some(outbound.transpose()?)
    } else {
        None
    };
    for _ in 0..options.iterations {
        control.checkpoint()?;
        let mut next = labels.clone();
        for (node, slot) in next.iter_mut().enumerate() {
            checkpoint_chunk(control, &mut work)?;
            let mut counts = BTreeMap::<i64, usize>::new();
            let voters = outbound
                .row(node)
                .iter()
                .chain(inbound.as_ref().map_or(&[][..], |rows| rows.row(node)));
            for &neighbor in voters {
                checkpoint_chunk(control, &mut work)?;
                let count = counts.entry(labels[neighbor as usize]).or_default();
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| execution("label frequency overflow"))?;
            }
            // Iteration in ascending label order preserves the first maximum.
            let mut maximum = 0;
            for (label, count) in counts {
                if count > maximum {
                    maximum = count;
                    *slot = label;
                }
            }
        }
        labels = next;
    }
    Ok(labels)
}
