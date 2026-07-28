//! Deterministic Node2Vec walk-corpus generation.

use std::collections::HashMap;

use gf_core::embedding_options::Node2VecOptions;

use crate::algorithm_embedding_control::{EmbeddingControl, EmbeddingResourceError};
use crate::algorithm_embedding_output::EmbeddingOutputRow;
use crate::algorithm_embedding_rng::{EmbeddingRng, EmbeddingRngField};
use crate::algorithm_graph::{AdjacencyGraph, AlgorithmEdge};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Node2VecCorpus {
    pub(crate) walks: Vec<Vec<u64>>,
    pub(crate) token_counts: HashMap<u64, u64>,
}

pub(crate) type Node2VecEmbeddingRow = EmbeddingOutputRow;

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub(crate) enum Node2VecWalkError {
    #[error("node2vec edge weight must be finite and non-negative")]
    InvalidWeight,
    #[error("node2vec transition mass must be finite")]
    InvalidTransitionMass,
    #[error("node2vec token count exceeds UInt64 range")]
    TokenCountOverflow,
    #[error("node2vec training produced a non-finite embedding")]
    NonFiniteEmbedding,
    #[error(transparent)]
    Resource(#[from] EmbeddingResourceError),
}

pub(crate) fn train_node2vec(
    graph: &AdjacencyGraph,
    options: &Node2VecOptions,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<Node2VecEmbeddingRow>, Node2VecWalkError> {
    let corpus = build_walk_corpus(graph, options, control)?;
    let nodes = canonical_nodes(graph);
    let node_index = nodes
        .iter()
        .enumerate()
        .map(|(index, &(_, node_id))| (node_id, index))
        .collect::<HashMap<_, _>>();
    let mut input = initialize_input(&nodes, options);
    let mut output = vec![vec![0.0_f32; options.dimensions]; nodes.len()];
    let learning_rate = f64_to_f32(options.learning_rate);

    for epoch in 0..options.epochs {
        let epoch = to_u64(epoch)?;
        for (walk_index, walk) in corpus.walks.iter().enumerate() {
            let walk_ordinal = to_u64(walk_index % options.walks_per_node)?;
            let start_uuid = graph
                .node_uuid(walk[0])
                .expect("walk start belongs to selected graph");
            for center_position in 0..walk.len() {
                let lower = center_position.saturating_sub(options.window_size);
                let upper = center_position
                    .saturating_add(options.window_size)
                    .saturating_add(1)
                    .min(walk.len());
                for context_position in lower..upper {
                    if center_position == context_position {
                        continue;
                    }
                    control.checkpoint(1)?;
                    let center_id = walk[center_position];
                    let context_id = walk[context_position];
                    let center_index = node_index[&center_id];
                    let context_index = node_index[&context_id];
                    let center = input[center_index].clone();
                    let mut delta = vec![0.0_f32; options.dimensions];

                    update_sample(
                        &center,
                        &mut output[context_index],
                        &mut delta,
                        1.0,
                        learning_rate,
                    );
                    control.checkpoint(1)?;

                    for negative_ordinal in 0..options.negative_samples {
                        let Some(negative_id) = sample_negative(
                            &nodes,
                            &corpus.token_counts,
                            context_id,
                            options.seed,
                            epoch,
                            start_uuid,
                            walk_ordinal,
                            to_u64(center_position)?,
                            to_u64(context_position)?,
                            to_u64(negative_ordinal)?,
                        )?
                        else {
                            break;
                        };
                        let negative_index = node_index[&negative_id];
                        update_sample(
                            &center,
                            &mut output[negative_index],
                            &mut delta,
                            0.0,
                            learning_rate,
                        );
                        control.checkpoint(1)?;
                    }
                    for (coordinate, delta) in delta.into_iter().enumerate() {
                        input[center_index][coordinate] += delta;
                    }
                }
            }
        }
    }

    control.before_publish()?;
    nodes
        .into_iter()
        .enumerate()
        .map(|(index, (node_uuid, _))| {
            let embedding = input[index].clone();
            if embedding.iter().all(|value| value.is_finite()) {
                Ok(Node2VecEmbeddingRow {
                    node_uuid,
                    embedding,
                })
            } else {
                Err(Node2VecWalkError::NonFiniteEmbedding)
            }
        })
        .collect()
}

fn initialize_input(nodes: &[([u8; 16], u64)], options: &Node2VecOptions) -> Vec<Vec<f32>> {
    let scale = 0.5_f64 / usize_to_f64(options.dimensions);
    nodes
        .iter()
        .map(|&(node_uuid, _)| {
            (0..options.dimensions)
                .map(|coordinate| {
                    let mut rng = EmbeddingRng::derive(
                        "node2vec",
                        "node2vec-init-input",
                        options.seed,
                        &[
                            EmbeddingRngField::Uuid(node_uuid),
                            EmbeddingRngField::U64(
                                u64::try_from(coordinate).expect("validated dimensions fit UInt64"),
                            ),
                        ],
                    );
                    f64_to_f32((rng.unit_f64() * 2.0 - 1.0) * scale)
                })
                .collect()
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn sample_negative(
    nodes: &[([u8; 16], u64)],
    token_counts: &HashMap<u64, u64>,
    context_id: u64,
    seed: u64,
    epoch: u64,
    start_uuid: [u8; 16],
    walk_ordinal: u64,
    center_position: u64,
    context_position: u64,
    negative_ordinal: u64,
) -> Result<Option<u64>, Node2VecWalkError> {
    let masses = nodes
        .iter()
        .map(|&(_, node_id)| {
            let count = token_counts.get(&node_id).copied().unwrap_or(0);
            let mass = if node_id == context_id {
                0.0
            } else {
                u64_to_f64(count).powf(0.75)
            };
            (node_id, mass)
        })
        .collect::<Vec<_>>();
    let total = masses.iter().map(|(_, mass)| mass).sum::<f64>();
    if total == 0.0 {
        return Ok(None);
    }
    if !total.is_finite() {
        return Err(Node2VecWalkError::InvalidTransitionMass);
    }
    let mut rng = EmbeddingRng::derive(
        "node2vec",
        "negative",
        seed,
        &[
            EmbeddingRngField::U64(epoch),
            EmbeddingRngField::Uuid(start_uuid),
            EmbeddingRngField::U64(walk_ordinal),
            EmbeddingRngField::U64(center_position),
            EmbeddingRngField::U64(context_position),
            EmbeddingRngField::U64(negative_ordinal),
        ],
    );
    let draw = rng.unit_f64() * total;
    let mut cumulative = 0.0;
    Ok(masses.into_iter().find_map(|(node_id, mass)| {
        cumulative += mass;
        (draw < cumulative).then_some(node_id)
    }))
}

fn update_sample(
    center: &[f32],
    output: &mut [f32],
    delta: &mut [f32],
    label: f32,
    learning_rate: f32,
) {
    let mut dot = 0.0_f32;
    for coordinate in 0..center.len() {
        dot += center[coordinate] * output[coordinate];
    }
    let sigmoid = 1.0 / (1.0 + (-dot.clamp(-15.0, 15.0)).exp());
    let gradient = learning_rate * (label - sigmoid);
    for coordinate in 0..center.len() {
        let old_output = output[coordinate];
        delta[coordinate] += gradient * old_output;
        output[coordinate] = old_output + gradient * center[coordinate];
    }
}

fn to_u64(value: usize) -> Result<u64, Node2VecWalkError> {
    u64::try_from(value).map_err(|_| Node2VecWalkError::TokenCountOverflow)
}

#[allow(clippy::cast_possible_truncation)]
fn f64_to_f32(value: f64) -> f32 {
    value as f32
}

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

#[allow(clippy::cast_precision_loss)]
fn u64_to_f64(value: u64) -> f64 {
    value as f64
}

pub(crate) fn build_walk_corpus(
    graph: &AdjacencyGraph,
    options: &Node2VecOptions,
    control: &EmbeddingControl<'_>,
) -> Result<Node2VecCorpus, Node2VecWalkError> {
    let starts = canonical_nodes(graph);
    let capacity = starts
        .len()
        .checked_mul(options.walks_per_node)
        .ok_or(Node2VecWalkError::TokenCountOverflow)?;
    let mut walks = Vec::with_capacity(capacity);
    let mut token_counts = HashMap::with_capacity(starts.len());

    for &(start_uuid, start_id) in &starts {
        for walk_ordinal in 0..options.walks_per_node {
            control.checkpoint(1)?;
            let walk = build_walk(
                graph,
                options,
                control,
                start_uuid,
                start_id,
                u64::try_from(walk_ordinal).map_err(|_| Node2VecWalkError::TokenCountOverflow)?,
            )?;
            for &node_id in &walk {
                let count = token_counts.entry(node_id).or_insert(0_u64);
                *count = count
                    .checked_add(1)
                    .ok_or(Node2VecWalkError::TokenCountOverflow)?;
            }
            walks.push(walk);
        }
    }

    Ok(Node2VecCorpus {
        walks,
        token_counts,
    })
}

fn build_walk(
    graph: &AdjacencyGraph,
    options: &Node2VecOptions,
    control: &EmbeddingControl<'_>,
    start_uuid: [u8; 16],
    start_id: u64,
    walk_ordinal: u64,
) -> Result<Vec<u64>, Node2VecWalkError> {
    let mut walk = Vec::with_capacity(options.walk_length.saturating_add(1));
    walk.push(start_id);

    for transition in 0..options.walk_length {
        control.checkpoint(1)?;
        let current = *walk.last().expect("a walk always contains its start");
        let previous = walk.get(walk.len().wrapping_sub(2)).copied();
        let candidates = canonical_candidates(graph, current);
        let masses = candidates
            .iter()
            .map(|edge| transition_mass(graph, previous, edge, options))
            .collect::<Result<Vec<_>, _>>()?;
        let total = masses.iter().try_fold(0.0_f64, |total, mass| {
            let next = total + mass;
            next.is_finite()
                .then_some(next)
                .ok_or(Node2VecWalkError::InvalidTransitionMass)
        })?;
        if total == 0.0 {
            break;
        }

        let transition =
            u64::try_from(transition).map_err(|_| Node2VecWalkError::TokenCountOverflow)?;
        let mut rng = EmbeddingRng::derive(
            "node2vec",
            "walk",
            options.seed,
            &[
                EmbeddingRngField::Uuid(start_uuid),
                EmbeddingRngField::U64(walk_ordinal),
                EmbeddingRngField::U64(transition),
            ],
        );
        let draw = rng.unit_f64() * total;
        let mut cumulative = 0.0;
        let selected = candidates
            .iter()
            .zip(masses)
            .find_map(|(edge, mass)| {
                cumulative += mass;
                (draw < cumulative).then_some(edge.neighbor_id)
            })
            .unwrap_or_else(|| {
                candidates
                    .last()
                    .expect("positive total requires a candidate")
                    .neighbor_id
            });
        walk.push(selected);
    }
    Ok(walk)
}

fn canonical_nodes(graph: &AdjacencyGraph) -> Vec<([u8; 16], u64)> {
    let mut nodes = graph
        .node_ids()
        .iter()
        .filter_map(|&node_id| graph.node_uuid(node_id).map(|uuid| (uuid, node_id)))
        .collect::<Vec<_>>();
    nodes.sort_unstable();
    nodes
}

fn canonical_candidates(graph: &AdjacencyGraph, node_id: u64) -> Vec<&AlgorithmEdge> {
    let mut candidates = graph.neighbors(node_id).iter().collect::<Vec<_>>();
    candidates.sort_unstable_by_key(|edge| {
        (
            graph
                .node_uuid(edge.neighbor_id)
                .expect("adjacency neighbor belongs to the selected graph"),
            edge.edge_uuid,
        )
    });
    candidates
}

fn transition_mass(
    graph: &AdjacencyGraph,
    previous: Option<u64>,
    edge: &AlgorithmEdge,
    options: &Node2VecOptions,
) -> Result<f64, Node2VecWalkError> {
    if !edge.weight.is_finite() || edge.weight < 0.0 {
        return Err(Node2VecWalkError::InvalidWeight);
    }
    let bias = match previous {
        None => 1.0,
        Some(previous) if edge.neighbor_id == previous => 1.0 / options.p,
        Some(previous) if adjacent_to_previous(graph, edge.neighbor_id, previous) => 1.0,
        Some(_) => 1.0 / options.q,
    };
    let mass = edge.weight * bias;
    mass.is_finite()
        .then_some(mass)
        .ok_or(Node2VecWalkError::InvalidTransitionMass)
}

fn adjacent_to_previous(graph: &AdjacencyGraph, candidate: u64, previous: u64) -> bool {
    graph
        .neighbors(candidate)
        .iter()
        .any(|edge| edge.neighbor_id == previous)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmControl, AlgorithmLimits};
    use crate::algorithm_embedding_control::EmbeddingResourceLimits;

    fn controls(
        cancellation: AlgorithmCancellation,
        work: u64,
    ) -> (AlgorithmControl, EmbeddingResourceLimits) {
        (
            AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            EmbeddingResourceLimits {
                work,
                ..EmbeddingResourceLimits::default()
            },
        )
    }

    fn corpus(
        graph: &AdjacencyGraph,
        options: &Node2VecOptions,
    ) -> Result<Node2VecCorpus, Node2VecWalkError> {
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
        build_walk_corpus(graph, options, &EmbeddingControl::new(&algorithm, limits))
    }

    #[test]
    fn replay_seed_and_uuid_order_are_deterministic() {
        let uuids = [
            3_u128.to_be_bytes(),
            1_u128.to_be_bytes(),
            2_u128.to_be_bytes(),
        ];
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(&uuids, &[(0, 1), (0, 2)]);
        let options = Node2VecOptions {
            walk_length: 1,
            walks_per_node: 8,
            seed: 7,
            ..Node2VecOptions::default()
        };
        let first = corpus(&graph, &options).unwrap();
        assert_eq!(first, corpus(&graph, &options).unwrap());
        assert_ne!(
            first,
            corpus(&graph, &Node2VecOptions { seed: 8, ..options }).unwrap()
        );
        assert_eq!(first.walks[0][0], 1);
        assert_eq!(first.walks[8][0], 2);
        assert_eq!(first.walks[16][0], 0);
    }

    #[test]
    fn second_order_bias_uses_directed_candidate_to_previous_adjacency() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (1, 0), (1, 2), (1, 3), (2, 0)]);
        let options = Node2VecOptions {
            p: 0.01,
            q: 100.0,
            ..Node2VecOptions::default()
        };
        let candidates = canonical_candidates(&graph, 1);
        let masses = candidates
            .iter()
            .map(|edge| {
                (
                    edge.neighbor_id,
                    transition_mass(&graph, Some(0), edge, &options).unwrap(),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(masses[&0], 100.0, "return candidate uses 1/p");
        assert_eq!(
            masses[&2], 1.0,
            "selected 2 -> 0 edge makes candidate distance one"
        );
        assert_eq!(masses[&3], 0.01, "distant candidate uses 1/q");
    }

    #[test]
    fn parallel_edges_self_loops_zero_mass_and_isolates_are_total() {
        let graph =
            AdjacencyGraph::with_test_undirected_multigraph(3, &[(7, 0, 1), (8, 0, 1), (9, 0, 0)])
                .with_test_edge_weights(&[0.0, 0.0, 0.0, 0.0, 0.0]);
        let candidates = canonical_candidates(&graph, 0);
        assert_eq!(candidates.len(), 3, "parallel edges remain distinct");
        assert_eq!(
            candidates
                .iter()
                .filter(|edge| edge.neighbor_id == 0)
                .count(),
            1,
            "self-loop appears once"
        );
        let result = corpus(
            &graph,
            &Node2VecOptions {
                walk_length: 4,
                walks_per_node: 2,
                ..Node2VecOptions::default()
            },
        )
        .unwrap();
        assert!(result.walks.iter().all(|walk| walk.len() == 1));
        assert_eq!(result.token_counts.values().copied().sum::<u64>(), 6);
    }

    #[test]
    fn invalid_weights_cancellation_and_work_limits_fail_atomically() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]).with_test_edge_weights(&[-1.0]);
        assert_eq!(
            corpus(&graph, &Node2VecOptions::default()),
            Err(Node2VecWalkError::InvalidWeight)
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let (algorithm, limits) = controls(cancellation, u64::MAX);
        assert!(matches!(
            build_walk_corpus(
                &graph,
                &Node2VecOptions::default(),
                &EmbeddingControl::new(&algorithm, limits)
            ),
            Err(Node2VecWalkError::Resource(
                EmbeddingResourceError::Algorithm(_)
            ))
        ));

        let graph = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), 1);
        assert!(matches!(
            build_walk_corpus(
                &graph,
                &Node2VecOptions {
                    walk_length: 1,
                    walks_per_node: 1,
                    ..Node2VecOptions::default()
                },
                &EmbeddingControl::new(&algorithm, limits)
            ),
            Err(Node2VecWalkError::Resource(
                EmbeddingResourceError::WorkLimit { .. }
            ))
        ));
    }

    #[test]
    fn sgns_output_is_exact_finite_uuid_ordered_and_replayable() {
        let uuids = [
            3_u128.to_be_bytes(),
            1_u128.to_be_bytes(),
            2_u128.to_be_bytes(),
        ];
        let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(&uuids, &[(0, 1), (1, 0)]);
        let options = Node2VecOptions {
            dimensions: 2,
            walk_length: 2,
            walks_per_node: 1,
            window_size: 1,
            negative_samples: 1,
            epochs: 1,
            learning_rate: 0.025,
            seed: 3,
            ..Node2VecOptions::default()
        };
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
        let first =
            train_node2vec(&graph, &options, &EmbeddingControl::new(&algorithm, limits)).unwrap();
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
        assert_eq!(
            first,
            train_node2vec(&graph, &options, &EmbeddingControl::new(&algorithm, limits)).unwrap()
        );
        assert_eq!(
            first.iter().map(|row| row.node_uuid).collect::<Vec<_>>(),
            [
                1_u128.to_be_bytes(),
                2_u128.to_be_bytes(),
                3_u128.to_be_bytes()
            ]
        );
        assert!(first.iter().all(|row| {
            row.embedding.len() == 2 && row.embedding.iter().all(|value| value.is_finite())
        }));
        assert_eq!(
            first
                .iter()
                .flat_map(|row| row.embedding.iter().map(|value| value.to_bits()))
                .collect::<Vec<_>>(),
            [
                1_033_343_337,
                1_042_572_670,
                1_031_203_965,
                1_027_727_270,
                3_161_040_333,
                3_171_706_725,
            ]
        );
    }

    #[test]
    fn sgns_seed_epochs_isolates_cancellation_and_work_are_observable() {
        let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 0)]);
        let options = Node2VecOptions {
            dimensions: 3,
            walk_length: 2,
            walks_per_node: 1,
            window_size: 1,
            negative_samples: 1,
            epochs: 1,
            seed: 4,
            ..Node2VecOptions::default()
        };
        let trained = corpus_training(&graph, &options).unwrap();
        assert!(trained[2].embedding.iter().any(|value| *value != 0.0));
        assert_ne!(
            trained,
            corpus_training(
                &graph,
                &Node2VecOptions {
                    seed: 5,
                    ..options.clone()
                }
            )
            .unwrap()
        );
        assert_ne!(
            trained,
            corpus_training(
                &graph,
                &Node2VecOptions {
                    epochs: 2,
                    ..options
                }
            )
            .unwrap()
        );

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let (algorithm, limits) = controls(cancellation, u64::MAX);
        assert!(matches!(
            train_node2vec(
                &graph,
                &Node2VecOptions::default(),
                &EmbeddingControl::new(&algorithm, limits)
            ),
            Err(Node2VecWalkError::Resource(
                EmbeddingResourceError::Algorithm(_)
            ))
        ));
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), 2);
        assert!(matches!(
            train_node2vec(
                &graph,
                &Node2VecOptions {
                    dimensions: 2,
                    walk_length: 1,
                    walks_per_node: 1,
                    window_size: 1,
                    negative_samples: 1,
                    epochs: 1,
                    ..Node2VecOptions::default()
                },
                &EmbeddingControl::new(&algorithm, limits)
            ),
            Err(Node2VecWalkError::Resource(
                EmbeddingResourceError::WorkLimit { .. }
            ))
        ));
    }

    #[test]
    fn negative_sampling_excludes_context_and_handles_no_remaining_mass() {
        let graph = AdjacencyGraph::with_test_directed_edges(2, &[]);
        let nodes = canonical_nodes(&graph);
        let only_context = HashMap::from([(0, 4)]);
        assert_eq!(
            sample_negative(
                &nodes,
                &only_context,
                0,
                0,
                0,
                0_u128.to_be_bytes(),
                0,
                0,
                1,
                0,
            )
            .unwrap(),
            None
        );
        let both = HashMap::from([(0, 4), (1, 1)]);
        assert_eq!(
            sample_negative(&nodes, &both, 0, 0, 0, 0_u128.to_be_bytes(), 0, 0, 1, 0).unwrap(),
            Some(1)
        );
    }

    fn corpus_training(
        graph: &AdjacencyGraph,
        options: &Node2VecOptions,
    ) -> Result<Vec<Node2VecEmbeddingRow>, Node2VecWalkError> {
        let (algorithm, limits) = controls(AlgorithmCancellation::default(), u64::MAX);
        train_node2vec(graph, options, &EmbeddingControl::new(&algorithm, limits))
    }
}
