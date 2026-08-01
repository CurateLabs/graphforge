//! Deterministic FastRP mathematical kernel.

use std::collections::{HashMap, HashSet};

use graphforge_core::embedding_options::FastRpOptions;

use crate::algorithm_embedding_control::{EmbeddingControl, EmbeddingResourceError};
use crate::algorithm_embedding_output::EmbeddingOutputRow;
use crate::algorithm_embedding_rng::{EmbeddingRng, EmbeddingRngField};
use crate::algorithm_graph::AdjacencyGraph;

pub(crate) type FastRpEmbeddingRow = EmbeddingOutputRow;

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub(crate) enum FastRpError {
    #[error("fastrp edge weight must be finite and non-negative")]
    InvalidWeight,
    #[error("fastrp normalization is undefined for an isolate when beta is negative")]
    NegativeBetaWithIsolate,
    #[error("fastrp total outgoing strength must be positive when beta is nonzero")]
    ZeroStrength,
    #[error("fastrp feature values must be finite and match the ordered feature properties")]
    InvalidFeatures,
    #[error("fastrp produced a non-finite embedding")]
    NonFiniteEmbedding,
    #[error("fastrp dimensions exceed UInt64 range")]
    DimensionOverflow,
    #[error(transparent)]
    Resource(#[from] EmbeddingResourceError),
}

#[derive(Clone, Debug, PartialEq)]
struct FastRpInput {
    directed: bool,
    nodes: Vec<FastRpNode>,
    edges: Vec<FastRpEdge>,
}

#[derive(Clone, Debug, PartialEq)]
struct FastRpNode {
    uuid: [u8; 16],
    features: Vec<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct FastRpEdge {
    uuid: [u8; 16],
    source: [u8; 16],
    target: [u8; 16],
    weight: f64,
}

pub(crate) fn train_fastrp(
    graph: &AdjacencyGraph,
    options: &FastRpOptions,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<FastRpEmbeddingRow>, FastRpError> {
    run_fastrp(FastRpInput::from_graph(graph), options, control)
}

impl FastRpInput {
    fn from_graph(graph: &AdjacencyGraph) -> Self {
        let mut nodes = graph
            .node_ids()
            .iter()
            .map(|&node_id| FastRpNode {
                uuid: graph
                    .node_uuid(node_id)
                    .expect("selected algorithm node has public UUID identity"),
                features: graph.node_vector(node_id).unwrap_or_default().to_vec(),
            })
            .collect::<Vec<_>>();
        nodes.sort_unstable_by_key(|node| node.uuid);

        let mut edges = Vec::new();
        let mut undirected_loops = HashSet::new();
        for &source_id in graph.node_ids() {
            let source = graph
                .node_uuid(source_id)
                .expect("selected algorithm node has public UUID identity");
            for edge in graph.neighbors(source_id) {
                let target = graph
                    .node_uuid(edge.neighbor_id)
                    .expect("selected algorithm neighbor has public UUID identity");
                if !graph.is_directed()
                    && source == target
                    && !undirected_loops.insert(edge.edge_uuid)
                {
                    continue;
                }
                edges.push(FastRpEdge {
                    uuid: edge.edge_uuid,
                    source,
                    target,
                    weight: edge.weight,
                });
            }
        }
        edges.sort_unstable_by_key(|edge| (edge.source, edge.uuid, edge.target));
        Self {
            directed: graph.is_directed(),
            nodes,
            edges,
        }
    }
}

fn run_fastrp(
    mut input: FastRpInput,
    options: &FastRpOptions,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<FastRpEmbeddingRow>, FastRpError> {
    input.nodes.sort_unstable_by_key(|node| node.uuid);
    input
        .edges
        .sort_unstable_by_key(|edge| (edge.source, edge.uuid, edge.target));
    let adjacency = build_adjacency(&input.nodes, input.edges, input.directed)?;

    let strengths = adjacency
        .iter()
        .map(|row| row.iter().map(|(_, weight)| weight).sum::<f64>())
        .collect::<Vec<_>>();
    if strengths.iter().any(|strength| !strength.is_finite()) {
        return Err(FastRpError::InvalidWeight);
    }
    let total_strength = strengths.iter().sum::<f64>();
    if !total_strength.is_finite() {
        return Err(FastRpError::InvalidWeight);
    }
    if options.normalization_strength < 0.0 && strengths.contains(&0.0) {
        return Err(FastRpError::NegativeBetaWithIsolate);
    }
    if options.normalization_strength != 0.0 && total_strength == 0.0 {
        return Err(FastRpError::ZeroStrength);
    }

    let dimensions =
        u64::try_from(options.dimensions).map_err(|_| FastRpError::DimensionOverflow)?;
    let mut current = initial_projection(
        &input.nodes,
        &strengths,
        total_strength,
        options,
        control,
        dimensions,
    )?;
    mix_features(&mut current, &input.nodes, options, control, dimensions)?;

    let mut accumulator = vec![vec![0.0; options.dimensions]; input.nodes.len()];
    accumulate(
        &mut accumulator,
        &current,
        options.iteration_weights[0],
        control,
    )?;
    for &iteration_weight in options.iteration_weights.iter().skip(1) {
        let mut next = vec![vec![0.0; options.dimensions]; input.nodes.len()];
        for (source, neighbors) in adjacency.iter().enumerate() {
            let adjacency_work = u64::try_from(neighbors.len())
                .ok()
                .and_then(|entries| entries.checked_mul(dimensions))
                .ok_or(EmbeddingResourceError::Overflow)?;
            control.checkpoint(adjacency_work)?;
            if strengths[source] == 0.0 {
                continue;
            }
            for &(target, weight) in neighbors {
                let probability = weight / strengths[source];
                for coordinate in 0..options.dimensions {
                    next[source][coordinate] += probability * current[target][coordinate];
                }
            }
        }
        current = next;
        accumulate(&mut accumulator, &current, iteration_weight, control)?;
    }

    control.before_publish()?;
    input
        .nodes
        .into_iter()
        .zip(accumulator)
        .map(|(node, embedding)| {
            control.checkpoint(0)?;
            let embedding = embedding
                .into_iter()
                .map(checked_f32)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FastRpEmbeddingRow {
                node_uuid: node.uuid,
                embedding,
            })
        })
        .collect()
}

fn build_adjacency(
    nodes: &[FastRpNode],
    edges: Vec<FastRpEdge>,
    directed: bool,
) -> Result<Vec<Vec<(usize, f64)>>, FastRpError> {
    let node_index = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.uuid, index))
        .collect::<HashMap<_, _>>();
    let mut adjacency = vec![Vec::new(); nodes.len()];
    let mut undirected_loops = HashSet::new();
    for edge in edges {
        if !edge.weight.is_finite() || edge.weight < 0.0 {
            return Err(FastRpError::InvalidWeight);
        }
        if !directed && edge.source == edge.target && !undirected_loops.insert(edge.uuid) {
            continue;
        }
        let source = node_index[&edge.source];
        let target = node_index[&edge.target];
        adjacency[source].push((target, edge.weight));
    }
    Ok(adjacency)
}

fn initial_projection(
    nodes: &[FastRpNode],
    strengths: &[f64],
    total_strength: f64,
    options: &FastRpOptions,
    control: &EmbeddingControl<'_>,
    dimensions: u64,
) -> Result<Vec<Vec<f64>>, FastRpError> {
    let node_q = usize_to_f64(nodes.len()).sqrt().max(1.0);
    nodes
        .iter()
        .enumerate()
        .map(|(node_index, node)| {
            control.checkpoint(dimensions)?;
            let level = if options.normalization_strength == 0.0 {
                1.0
            } else if strengths[node_index] == 0.0 {
                0.0
            } else {
                (strengths[node_index] / total_strength).powf(options.normalization_strength)
            };
            let mut row = vec![0.0; options.dimensions];
            for (coordinate, value) in row.iter_mut().enumerate() {
                *value = level
                    * sparse_projection(
                        "node-projection",
                        options.seed,
                        &[
                            EmbeddingRngField::Uuid(node.uuid),
                            EmbeddingRngField::U64(to_u64(coordinate)?),
                        ],
                        node_q,
                    );
            }
            unit_l2(&mut row);
            Ok(row)
        })
        .collect()
}

fn mix_features(
    current: &mut [Vec<f64>],
    nodes: &[FastRpNode],
    options: &FastRpOptions,
    control: &EmbeddingControl<'_>,
    dimensions: u64,
) -> Result<(), FastRpError> {
    if options.feature_properties.is_empty() {
        return Ok(());
    }
    let property_count = options.feature_properties.len();
    if nodes.iter().any(|node| {
        node.features.len() != property_count
            || node.features.iter().any(|value| !value.is_finite())
    }) {
        return Err(FastRpError::InvalidFeatures);
    }
    let feature_q = usize_to_f64(property_count).sqrt().max(1.0);
    let mut projection = vec![vec![0.0; options.dimensions]; property_count];
    for (property_ordinal, (property, row)) in options
        .feature_properties
        .iter()
        .zip(&mut projection)
        .enumerate()
    {
        control.checkpoint(dimensions)?;
        for (coordinate, value) in row.iter_mut().enumerate() {
            *value = sparse_projection(
                "feature-projection",
                options.seed,
                &[
                    EmbeddingRngField::Utf8(property),
                    EmbeddingRngField::U64(to_u64(property_ordinal)?),
                    EmbeddingRngField::U64(to_u64(coordinate)?),
                ],
                feature_q,
            );
        }
    }
    for (row, node) in current.iter_mut().zip(nodes) {
        let mut features = vec![0.0; options.dimensions];
        for (property_ordinal, value) in node.features.iter().enumerate() {
            for (coordinate, coordinate_value) in features.iter_mut().enumerate() {
                *coordinate_value += value * projection[property_ordinal][coordinate];
            }
        }
        unit_l2(&mut features);
        for (coordinate, value) in row.iter_mut().enumerate() {
            *value += options.feature_weight * features[coordinate];
        }
    }
    Ok(())
}

fn sparse_projection(phase: &str, seed: u64, fields: &[EmbeddingRngField<'_>], q: f64) -> f64 {
    let mut rng = EmbeddingRng::derive("fastrp", phase, seed, fields);
    let draw = rng.unit_f64();
    let half = 1.0 / (2.0 * q);
    if draw < half {
        q.sqrt()
    } else if draw >= 1.0 - half {
        -q.sqrt()
    } else {
        0.0
    }
}

fn accumulate(
    accumulator: &mut [Vec<f64>],
    matrix: &[Vec<f64>],
    weight: f64,
    control: &EmbeddingControl<'_>,
) -> Result<(), FastRpError> {
    for (output, row) in accumulator.iter_mut().zip(matrix) {
        control.checkpoint(to_u64(row.len())?)?;
        let mut normalized = row.clone();
        unit_l2(&mut normalized);
        for (coordinate, value) in normalized.into_iter().enumerate() {
            output[coordinate] += weight * value;
        }
    }
    Ok(())
}

fn unit_l2(row: &mut [f64]) {
    let norm = row.iter().map(|value| value * value).sum::<f64>().sqrt();
    if norm != 0.0 {
        for value in row {
            *value /= norm;
        }
    }
}

fn checked_f32(value: f64) -> Result<f32, FastRpError> {
    if !value.is_finite() {
        return Err(FastRpError::NonFiniteEmbedding);
    }
    #[allow(clippy::cast_possible_truncation)]
    let output = value as f32;
    output
        .is_finite()
        .then_some(output)
        .ok_or(FastRpError::NonFiniteEmbedding)
}

fn to_u64(value: usize) -> Result<u64, FastRpError> {
    u64::try_from(value).map_err(|_| FastRpError::DimensionOverflow)
}

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmControl, AlgorithmLimits};
    use crate::algorithm_embedding_control::EmbeddingResourceLimits;

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn node(value: u8) -> FastRpNode {
        FastRpNode {
            uuid: uuid(value),
            features: Vec::new(),
        }
    }

    fn edge(value: u8, source: u8, target: u8, weight: f64) -> FastRpEdge {
        FastRpEdge {
            uuid: uuid(value),
            source: uuid(source),
            target: uuid(target),
            weight,
        }
    }

    fn options(seed: u64) -> FastRpOptions {
        FastRpOptions {
            dimensions: 8,
            iteration_weights: vec![1.0, 1.0, 1.0],
            normalization_strength: 0.0,
            feature_weight: 0.0,
            feature_properties: Vec::new(),
            seed,
        }
    }

    fn with_control<T>(
        limits: EmbeddingResourceLimits,
        run: impl FnOnce(&EmbeddingControl<'_>) -> T,
    ) -> T {
        let algorithm =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let control = EmbeddingControl::new(&algorithm, limits);
        run(&control)
    }

    fn run(
        input: FastRpInput,
        options: &FastRpOptions,
    ) -> Result<Vec<FastRpEmbeddingRow>, FastRpError> {
        with_control(EmbeddingResourceLimits::default(), |control| {
            run_fastrp(input, options, control)
        })
    }

    #[test]
    fn fixed_seed_is_exact_and_canonical_node_and_edge_order_is_irrelevant() {
        let canonical = FastRpInput {
            directed: true,
            nodes: vec![node(1), node(2), node(3)],
            edges: vec![
                edge(10, 1, 2, 1.0),
                edge(11, 1, 3, 2.0),
                edge(12, 2, 3, 1.0),
            ],
        };
        let mut reordered = canonical.clone();
        reordered.nodes.reverse();
        reordered.edges.reverse();
        let expected = run(canonical, &options(7)).unwrap();
        assert_eq!(run(reordered, &options(7)).unwrap(), expected);
        assert_eq!(
            run(
                FastRpInput {
                    directed: true,
                    nodes: vec![node(1), node(2), node(3)],
                    edges: vec![
                        edge(10, 1, 2, 1.0),
                        edge(11, 1, 3, 2.0),
                        edge(12, 2, 3, 1.0),
                    ],
                },
                &options(7),
            )
            .unwrap(),
            expected
        );
        assert!(
            expected
                .iter()
                .flat_map(|row| &row.embedding)
                .any(|value| *value != 0.0)
        );
    }

    #[test]
    fn fixed_seed_vector_matches_the_typed_projection_golden() {
        let input = FastRpInput {
            directed: true,
            nodes: vec![node(0xff)],
            edges: Vec::new(),
        };
        let configured = FastRpOptions {
            dimensions: 8,
            iteration_weights: vec![1.0],
            normalization_strength: 0.0,
            feature_weight: 0.0,
            feature_properties: Vec::new(),
            seed: 42,
        };
        let rows = run(input, &configured).unwrap();
        assert_eq!(rows[0].node_uuid, uuid(0xff));
        let expected = [
            -0.353_553_38,
            -0.353_553_38,
            -0.353_553_38,
            -0.353_553_38,
            -0.353_553_38,
            0.353_553_38,
            0.353_553_38,
            -0.353_553_38,
        ];
        for (actual, expected) in rows[0].embedding.iter().zip(expected) {
            assert!((actual - expected).abs() <= 1.0e-7);
        }
        assert_eq!(
            sparse_projection(
                "node-projection",
                42,
                &[
                    EmbeddingRngField::Uuid(uuid(0xff)),
                    EmbeddingRngField::U64(7),
                ],
                1.0,
            ),
            -1.0
        );
    }

    #[test]
    fn propagation_consumes_normalized_h0_not_raw_degree_scaled_projection() {
        let input = FastRpInput {
            directed: true,
            nodes: vec![node(1), node(2), node(3)],
            edges: vec![
                edge(10, 1, 2, 1.0),
                edge(11, 1, 3, 1.0),
                edge(12, 2, 1, 1.0),
                edge(13, 3, 1, 3.0),
            ],
        };
        let mut beta_zero = options(19);
        beta_zero.iteration_weights = vec![0.0, 1.0];
        let mut beta_one = beta_zero.clone();
        beta_one.normalization_strength = 1.0;

        assert_eq!(
            run(input.clone(), &beta_zero).unwrap(),
            run(input, &beta_one).unwrap()
        );
    }

    #[test]
    fn seeds_have_separate_projection_substreams() {
        let input = FastRpInput {
            directed: true,
            nodes: vec![node(1), node(2)],
            edges: vec![edge(10, 1, 2, 1.0)],
        };
        assert_ne!(
            run(input.clone(), &options(1)).unwrap(),
            run(input, &options(2)).unwrap()
        );
    }

    #[test]
    fn directed_and_undirected_transitions_weight_parallel_edges_and_count_loop_once() {
        let directed = FastRpInput {
            directed: true,
            nodes: vec![node(1), node(2), node(3)],
            edges: vec![
                edge(10, 1, 2, 1.0),
                edge(11, 1, 2, 2.0),
                edge(12, 1, 3, 3.0),
                edge(13, 2, 2, 2.0),
            ],
        };
        let undirected = FastRpInput {
            directed: false,
            nodes: directed.nodes.clone(),
            edges: vec![
                edge(10, 1, 2, 1.0),
                edge(11, 1, 2, 2.0),
                edge(12, 1, 3, 3.0),
                edge(10, 2, 1, 1.0),
                edge(11, 2, 1, 2.0),
                edge(12, 3, 1, 3.0),
                edge(13, 2, 2, 2.0),
                edge(13, 2, 2, 2.0),
            ],
        };
        let mut loop_once = undirected.clone();
        loop_once.edges.pop();
        assert_eq!(
            run(undirected.clone(), &options(5)).unwrap(),
            run(loop_once, &options(5)).unwrap()
        );
        let weighted = run(directed.clone(), &options(5)).unwrap();
        let unweighted = run(
            FastRpInput {
                directed: true,
                nodes: directed.nodes,
                edges: directed
                    .edges
                    .into_iter()
                    .map(|mut edge| {
                        edge.weight = 1.0;
                        edge
                    })
                    .collect(),
            },
            &options(5),
        )
        .unwrap();
        assert_ne!(weighted, unweighted);
        assert_ne!(weighted, run(undirected, &options(5)).unwrap());
    }

    #[test]
    fn beta_signs_define_isolates_and_zero_strength() {
        let input = FastRpInput {
            directed: true,
            nodes: vec![node(1), node(2)],
            edges: Vec::new(),
        };
        let mut beta_zero = options(3);
        beta_zero.iteration_weights = vec![1.0, 1.0];
        let rows = run(input.clone(), &beta_zero).unwrap();
        assert!(
            rows.iter()
                .any(|row| row.embedding.iter().any(|value| *value != 0.0))
        );

        let mut positive = beta_zero.clone();
        positive.normalization_strength = 0.5;
        assert_eq!(
            run(input.clone(), &positive),
            Err(FastRpError::ZeroStrength)
        );

        let mut negative = beta_zero;
        negative.normalization_strength = -0.5;
        assert_eq!(
            run(input, &negative),
            Err(FastRpError::NegativeBetaWithIsolate)
        );
    }

    #[test]
    fn ordered_features_are_projected_and_validated() {
        let input = FastRpInput {
            directed: true,
            nodes: vec![
                FastRpNode {
                    uuid: uuid(1),
                    features: vec![1.0, 2.0],
                },
                FastRpNode {
                    uuid: uuid(2),
                    features: vec![3.0, 4.0],
                },
            ],
            edges: Vec::new(),
        };
        let mut configured = options(11);
        configured.feature_properties = vec!["a".into(), "b".into()];
        configured.feature_weight = 2.0;
        let ordered = run(input.clone(), &configured).unwrap();
        configured.feature_properties.reverse();
        assert_ne!(run(input.clone(), &configured).unwrap(), ordered);

        let mut invalid = input;
        invalid.nodes[0].features[0] = f64::NAN;
        assert_eq!(run(invalid, &configured), Err(FastRpError::InvalidFeatures));
    }

    #[test]
    fn finite_weights_and_output_are_enforced() {
        let input = FastRpInput {
            directed: true,
            nodes: vec![node(1), node(2)],
            edges: vec![edge(10, 1, 2, f64::INFINITY)],
        };
        assert_eq!(run(input, &options(0)), Err(FastRpError::InvalidWeight));

        let input = FastRpInput {
            directed: true,
            nodes: vec![node(1)],
            edges: Vec::new(),
        };
        let mut configured = options(0);
        configured.iteration_weights = vec![f64::MAX, f64::MAX];
        assert_eq!(
            run(input, &configured),
            Err(FastRpError::NonFiniteEmbedding)
        );
    }

    #[test]
    fn work_and_cancellation_checkpoints_abort_before_publish() {
        let input = FastRpInput {
            directed: true,
            nodes: vec![node(1), node(2)],
            edges: vec![edge(10, 1, 2, 1.0)],
        };
        let limited = EmbeddingResourceLimits {
            memory_bytes: u64::MAX,
            work: 1,
        };
        let error = with_control(limited, |control| {
            run_fastrp(input.clone(), &options(0), control)
        })
        .unwrap_err();
        assert!(matches!(error, FastRpError::Resource(_)));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let algorithm = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        let control = EmbeddingControl::new(&algorithm, EmbeddingResourceLimits::default());
        assert!(matches!(
            run_fastrp(input, &options(0), &control),
            Err(FastRpError::Resource(_))
        ));
    }
}
