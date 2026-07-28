//! Deterministic GraphSAGE-v1 projection and neighborhood-sampling kernel.

use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;

use gf_core::embedding_options::{GraphSageOptions, MAX_EMBEDDING_DIMENSIONS};

use crate::algorithm_embedding_control::{
    EmbeddingControl, EmbeddingResourceError, EmbeddingResourceEstimate, GraphSageResources,
    TopologyResources,
};
use crate::algorithm_embedding_output::EmbeddingOutputRow;
use crate::algorithm_embedding_rng::{EmbeddingRng, EmbeddingRngField};

const POSITIVE_WALKS_PER_NODE: usize = 50;
const POSITIVE_WALK_TRANSITIONS: usize = 5;
const ADAM_BETA1: f64 = 0.9;
const ADAM_BETA2: f64 = 0.999;
const ADAM_EPSILON: f64 = 1e-8;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GraphSageNode {
    pub(crate) uuid: [u8; 16],
    pub(crate) features: Vec<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GraphSageEdge {
    pub(crate) uuid: [u8; 16],
    pub(crate) source_uuid: [u8; 16],
    pub(crate) target_uuid: [u8; 16],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct IncidentCandidate {
    neighbor: usize,
    neighbor_uuid: [u8; 16],
    edge_uuid: [u8; 16],
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GraphSageProjection {
    nodes: Vec<GraphSageNode>,
    adjacency: Vec<Vec<IncidentCandidate>>,
    feature_width: usize,
}

impl GraphSageProjection {
    #[cfg(test)]
    pub(crate) fn nodes(&self) -> &[GraphSageNode] {
        &self.nodes
    }

    #[cfg(test)]
    pub(crate) fn feature_width(&self) -> usize {
        self.feature_width
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GraphSageRolePath(Box<[u8]>);

impl GraphSageRolePath {
    pub(crate) fn center() -> Self {
        Self(typed_field(0x01, b"center").into_boxed_slice())
    }

    pub(crate) fn positive() -> Self {
        Self(typed_field(0x01, b"positive").into_boxed_slice())
    }

    pub(crate) fn negative(ordinal: u64) -> Self {
        let mut bytes = typed_field(0x01, b"negative");
        bytes.extend(typed_field(0x02, &ordinal.to_be_bytes()));
        Self(bytes.into_boxed_slice())
    }

    pub(crate) fn child(&self, parent: [u8; 16], sampled_slot: u64) -> Self {
        let mut bytes = self.0.to_vec();
        bytes.extend(typed_field(0x03, &parent));
        bytes.extend(typed_field(0x02, &sampled_slot.to_be_bytes()));
        Self(bytes.into_boxed_slice())
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GraphSageSampleKey<'a> {
    pub(crate) seed: u64,
    pub(crate) epoch: u64,
    pub(crate) example: u64,
    pub(crate) role_path: &'a GraphSageRolePath,
    pub(crate) layer: u64,
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub(crate) enum GraphSageKernelError {
    #[error("graphsage training is undefined because the projection produces no positive pair")]
    UndefinedTraining,
    #[error("graphsage contains duplicate node UUID")]
    DuplicateNode,
    #[error("graphsage contains duplicate edge UUID")]
    DuplicateEdge,
    #[error("graphsage edge endpoint is not selected")]
    DanglingEndpoint,
    #[error("graphsage requires a non-empty numeric feature vector")]
    EmptyFeatures,
    #[error("graphsage feature vectors have inconsistent shape")]
    FeatureShape,
    #[error("graphsage features must be finite")]
    NonFiniteFeature,
    #[error("graphsage fanout must be positive")]
    InvalidFanout,
    #[error("graphsage dimensions, layers, epochs, and sample counts must be positive")]
    InvalidCount,
    #[error("graphsage dimensions exceed the version-one maximum")]
    DimensionsTooLarge,
    #[error("graphsage sample_sizes length must equal layers")]
    SampleShape,
    #[error("graphsage requires explicit feature_properties")]
    MissingFeatureProperties,
    #[error("graphsage learning_rate must be finite and positive")]
    InvalidLearningRate,
    #[error("graphsage computation produced a non-finite value")]
    NonFiniteComputation,
    #[error("graphsage projection size exceeds the platform index range")]
    IndexOverflow,
    #[error(transparent)]
    Resource(#[from] EmbeddingResourceError),
}

/// Validate public identity and features, then canonicalize an undirected graph.
pub(crate) fn validate_graphsage_projection(
    mut nodes: Vec<GraphSageNode>,
    edges: Vec<GraphSageEdge>,
) -> Result<GraphSageProjection, GraphSageKernelError> {
    if nodes.is_empty() {
        return Err(GraphSageKernelError::UndefinedTraining);
    }
    nodes.sort_unstable_by_key(|node| node.uuid);
    if nodes.windows(2).any(|pair| pair[0].uuid == pair[1].uuid) {
        return Err(GraphSageKernelError::DuplicateNode);
    }
    let feature_width = nodes[0].features.len();
    if feature_width == 0 {
        return Err(GraphSageKernelError::EmptyFeatures);
    }
    if nodes
        .iter()
        .any(|node| node.features.len() != feature_width)
    {
        return Err(GraphSageKernelError::FeatureShape);
    }
    if nodes
        .iter()
        .flat_map(|node| &node.features)
        .any(|value| !value.is_finite())
    {
        return Err(GraphSageKernelError::NonFiniteFeature);
    }

    let index_by_uuid = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.uuid, index))
        .collect::<HashMap<_, _>>();
    let mut edge_by_uuid = BTreeMap::new();
    for edge in edges {
        if edge_by_uuid.insert(edge.uuid, edge).is_some() {
            return Err(GraphSageKernelError::DuplicateEdge);
        }
    }
    let mut adjacency = vec![Vec::new(); nodes.len()];
    for edge in edge_by_uuid.into_values() {
        let source = index_by_uuid
            .get(&edge.source_uuid)
            .copied()
            .ok_or(GraphSageKernelError::DanglingEndpoint)?;
        let target = index_by_uuid
            .get(&edge.target_uuid)
            .copied()
            .ok_or(GraphSageKernelError::DanglingEndpoint)?;
        if source == target {
            continue;
        }
        adjacency[source].push(IncidentCandidate {
            neighbor: target,
            neighbor_uuid: edge.target_uuid,
            edge_uuid: edge.uuid,
        });
        adjacency[target].push(IncidentCandidate {
            neighbor: source,
            neighbor_uuid: edge.source_uuid,
            edge_uuid: edge.uuid,
        });
    }
    for candidates in &mut adjacency {
        candidates.sort_unstable_by_key(|candidate| (candidate.neighbor_uuid, candidate.edge_uuid));
    }
    Ok(GraphSageProjection {
        nodes,
        adjacency,
        feature_width,
    })
}

/// Sample canonical incident candidates using the exact GraphSAGE-v1 key rules.
pub(crate) fn sample_graphsage_neighbors(
    projection: &GraphSageProjection,
    node: usize,
    fanout: usize,
    key: GraphSageSampleKey<'_>,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<usize>, GraphSageKernelError> {
    if fanout == 0 {
        return Err(GraphSageKernelError::InvalidFanout);
    }
    let candidates = projection
        .adjacency
        .get(node)
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    control.checkpoint(1)?;
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let node_uuid = projection.nodes[node].uuid;
    if candidates.len() >= fanout {
        let mut ranked = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let mut rng = EmbeddingRng::derive(
                "graphsage",
                "neighbor-priority",
                key.seed,
                &[
                    EmbeddingRngField::U64(key.epoch),
                    EmbeddingRngField::U64(key.example),
                    EmbeddingRngField::Bytes(key.role_path.0.as_ref()),
                    EmbeddingRngField::U64(key.layer),
                    EmbeddingRngField::Uuid(node_uuid),
                    EmbeddingRngField::Uuid(candidate.neighbor_uuid),
                    EmbeddingRngField::Uuid(candidate.edge_uuid),
                ],
            );
            ranked.push((rng.next(), candidate));
        }
        ranked.sort_unstable_by_key(|(priority, candidate)| {
            (*priority, candidate.neighbor_uuid, candidate.edge_uuid)
        });
        Ok(ranked
            .into_iter()
            .take(fanout)
            .map(|(_, candidate)| candidate.neighbor)
            .collect())
    } else {
        let degree =
            u64::try_from(candidates.len()).map_err(|_| GraphSageKernelError::IndexOverflow)?;
        (0..fanout)
            .map(|slot| {
                let slot = u64::try_from(slot).map_err(|_| GraphSageKernelError::IndexOverflow)?;
                let mut rng = EmbeddingRng::derive(
                    "graphsage",
                    "neighbor-index",
                    key.seed,
                    &[
                        EmbeddingRngField::U64(key.epoch),
                        EmbeddingRngField::U64(key.example),
                        EmbeddingRngField::Bytes(key.role_path.0.as_ref()),
                        EmbeddingRngField::U64(key.layer),
                        EmbeddingRngField::Uuid(node_uuid),
                        EmbeddingRngField::U64(slot),
                    ],
                );
                let index = rng
                    .bounded(degree)
                    .expect("validated non-empty candidate list");
                let index =
                    usize::try_from(index).map_err(|_| GraphSageKernelError::IndexOverflow)?;
                Ok(candidates[index].neighbor)
            })
            .collect()
    }
}

/// Mean the sampled node representations; multiplicity is intentionally kept.
#[cfg(test)]
pub(crate) fn graphsage_mean(
    representations: &[Vec<f64>],
    sampled: &[usize],
) -> Result<Vec<f64>, GraphSageKernelError> {
    let width = representations.first().map_or(0, Vec::len);
    if width == 0 || representations.iter().any(|row| row.len() != width) {
        return Err(GraphSageKernelError::FeatureShape);
    }
    let mut mean = vec![0.0; width];
    if sampled.is_empty() {
        return Ok(mean);
    }
    for &node in sampled {
        let row = representations
            .get(node)
            .ok_or(GraphSageKernelError::IndexOverflow)?;
        for (total, value) in mean.iter_mut().zip(row) {
            *total += value;
        }
    }
    let count = usize_to_f64(sampled.len());
    for value in &mut mean {
        *value /= count;
    }
    Ok(mean)
}

pub(crate) type GraphSageEmbeddingRow = EmbeddingOutputRow;

#[derive(Clone, Debug)]
struct Matrix {
    rows: usize,
    columns: usize,
    values: Vec<f64>,
}

impl Matrix {
    fn zeros(rows: usize, columns: usize) -> Result<Self, GraphSageKernelError> {
        let cells = rows
            .checked_mul(columns)
            .ok_or(GraphSageKernelError::IndexOverflow)?;
        Ok(Self {
            rows,
            columns,
            values: vec![0.0; cells],
        })
    }

    fn row(&self, row: usize) -> &[f64] {
        let start = row * self.columns;
        &self.values[start..start + self.columns]
    }
}

#[derive(Clone, Debug)]
struct Parameters {
    weights: Vec<Matrix>,
    first_moments: Vec<Matrix>,
    second_moments: Vec<Matrix>,
    beta1_power: f64,
    beta2_power: f64,
}

#[derive(Clone, Debug)]
struct SampledTreeNode {
    graph_node: usize,
    role_path: GraphSageRolePath,
    children: Vec<usize>,
}

#[derive(Clone, Debug)]
struct SampledTree {
    nodes: Vec<SampledTreeNode>,
    layers: usize,
}

/// Train deterministic unsupervised GraphSAGE and return canonical UUID rows.
pub(crate) fn train_graphsage(
    projection: &GraphSageProjection,
    options: &GraphSageOptions,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<GraphSageEmbeddingRow>, GraphSageKernelError> {
    validate_graphsage_options(options)?;
    if projection.nodes.is_empty() {
        return Err(GraphSageKernelError::UndefinedTraining);
    }
    let layer_widths = layer_widths(options);
    preflight_graphsage(projection, options, &layer_widths, control)?;
    let mut parameters = initialize_parameters(projection.feature_width, &layer_widths, options)?;

    let mut pair_count = 0_usize;
    visit_positive_pairs(projection, options.seed, control, true, |_, _, _| {
        pair_count = pair_count
            .checked_add(1)
            .ok_or(GraphSageKernelError::IndexOverflow)?;
        Ok(())
    })?;
    if pair_count == 0 {
        return Err(GraphSageKernelError::UndefinedTraining);
    }
    let negative_distribution = negative_distribution(projection)?;

    for epoch in 0..options.epochs {
        control.iteration_checkpoint()?;
        let epoch_u64 = to_u64(epoch)?;
        visit_positive_pairs(
            projection,
            options.seed,
            control,
            false,
            |pair_ordinal, center, positive| {
                train_pair(
                    projection,
                    options,
                    epoch_u64,
                    pair_ordinal,
                    center,
                    positive,
                    &negative_distribution,
                    &mut parameters,
                    control,
                )
            },
        )?;
    }

    infer_full_neighborhood(projection, options, &parameters.weights, control)
}

fn validate_graphsage_options(options: &GraphSageOptions) -> Result<(), GraphSageKernelError> {
    if options.dimensions == 0
        || options.hidden_dimensions == 0
        || options.layers == 0
        || options.epochs == 0
        || options.negative_samples == 0
    {
        return Err(GraphSageKernelError::InvalidCount);
    }
    if options.dimensions > MAX_EMBEDDING_DIMENSIONS
        || options.hidden_dimensions > MAX_EMBEDDING_DIMENSIONS
    {
        return Err(GraphSageKernelError::DimensionsTooLarge);
    }
    if options.sample_sizes.len() != options.layers {
        return Err(GraphSageKernelError::SampleShape);
    }
    if options.sample_sizes.contains(&0) {
        return Err(GraphSageKernelError::InvalidFanout);
    }
    if options.feature_properties.is_empty() {
        return Err(GraphSageKernelError::MissingFeatureProperties);
    }
    if !options.learning_rate.is_finite() || options.learning_rate <= 0.0 {
        return Err(GraphSageKernelError::InvalidLearningRate);
    }
    Ok(())
}

fn layer_widths(options: &GraphSageOptions) -> Vec<usize> {
    (0..options.layers)
        .map(|layer| {
            if layer + 1 == options.layers {
                options.dimensions
            } else {
                options.hidden_dimensions
            }
        })
        .collect()
}

fn preflight_graphsage(
    projection: &GraphSageProjection,
    options: &GraphSageOptions,
    layer_widths: &[usize],
    control: &EmbeddingControl<'_>,
) -> Result<(), GraphSageKernelError> {
    control.preflight(graphsage_resource_estimate_for_shape(
        to_u64(projection.nodes.len())?,
        projection
            .adjacency
            .iter()
            .try_fold(0_u64, |total, entries| {
                total
                    .checked_add(to_u64(entries.len())?)
                    .ok_or(GraphSageKernelError::IndexOverflow)
            })?,
        to_u64(projection.feature_width)?,
        options,
        layer_widths,
        0,
    )?)?;
    Ok(())
}

/// Preflight dispatch while the source adjacency and feature matrix remain live.
pub(crate) fn preflight_graphsage_dispatch(
    nodes: u64,
    adjacency_entries: u64,
    feature_width: u64,
    retained_source_bytes: u64,
    options: &GraphSageOptions,
    control: &EmbeddingControl<'_>,
) -> Result<(), GraphSageKernelError> {
    validate_graphsage_options(options)?;
    let layer_widths = layer_widths(options);
    control.preflight(graphsage_resource_estimate_for_shape(
        nodes,
        adjacency_entries,
        feature_width,
        options,
        &layer_widths,
        retained_source_bytes,
    )?)?;
    Ok(())
}

#[cfg(test)]
fn graphsage_resource_estimate(
    projection: &GraphSageProjection,
    options: &GraphSageOptions,
    layer_widths: &[usize],
) -> Result<EmbeddingResourceEstimate, GraphSageKernelError> {
    graphsage_resource_estimate_for_shape(
        to_u64(projection.nodes.len())?,
        projection
            .adjacency
            .iter()
            .try_fold(0_u64, |total, entries| {
                total
                    .checked_add(to_u64(entries.len())?)
                    .ok_or(GraphSageKernelError::IndexOverflow)
            })?,
        to_u64(projection.feature_width)?,
        options,
        layer_widths,
        0,
    )
}

fn graphsage_resource_estimate_for_shape(
    nodes: u64,
    adjacency_entries: u64,
    feature_width: u64,
    options: &GraphSageOptions,
    layer_widths: &[usize],
    retained_source_bytes: u64,
) -> Result<EmbeddingResourceEstimate, GraphSageKernelError> {
    let sample_sizes = options
        .sample_sizes
        .iter()
        .copied()
        .map(to_u64)
        .collect::<Result<Vec<_>, _>>()?;
    let layer_widths = layer_widths
        .iter()
        .copied()
        .map(to_u64)
        .collect::<Result<Vec<_>, _>>()?;
    let sampled_nodes = checked_sampled_nodes(&sample_sizes)?;
    let tree_scratch = sampled_tree_peak_bytes(sampled_nodes, to_u64(options.layers)?)?;
    let widest_layer = layer_widths
        .iter()
        .copied()
        .chain(std::iter::once(feature_width))
        .max()
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    let inference_scratch = nodes
        .checked_mul(widest_layer)
        .and_then(|cells| cells.checked_mul(16))
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    let recursive_scratch = to_u64(options.layers)?
        .checked_add(1)
        .and_then(|levels| levels.checked_mul(widest_layer))
        .and_then(|cells| cells.checked_mul(32))
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    let scratch_bytes = tree_scratch
        .checked_add(inference_scratch)
        .and_then(|bytes| bytes.checked_add(recursive_scratch))
        .and_then(|bytes| bytes.checked_add(retained_source_bytes))
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    Ok(EmbeddingResourceEstimate::graphsage(GraphSageResources {
        topology: TopologyResources {
            nodes,
            adjacency_entries,
            bytes_per_node: 16,
            bytes_per_adjacency_entry: 32,
        },
        dimensions: to_u64(options.dimensions)?,
        feature_width,
        sample_sizes: &sample_sizes,
        layer_widths: &layer_widths,
        epochs: to_u64(options.epochs)?,
        negative_samples: to_u64(options.negative_samples)?,
        scratch_bytes,
    })?)
}

fn checked_sampled_nodes(sample_sizes: &[u64]) -> Result<u64, GraphSageKernelError> {
    let mut nodes = 1_u64;
    let mut product = 1_u64;
    for &fanout in sample_sizes {
        product = product
            .checked_mul(fanout)
            .ok_or(GraphSageKernelError::IndexOverflow)?;
        nodes = nodes
            .checked_add(product)
            .ok_or(GraphSageKernelError::IndexOverflow)?;
    }
    Ok(nodes)
}

/// Upper-bound two simultaneously live trees: center plus current target.
fn sampled_tree_peak_bytes(sampled_nodes: u64, layers: u64) -> Result<u64, GraphSageKernelError> {
    let node_struct = to_u64(size_of::<SampledTreeNode>())?;
    let index = to_u64(size_of::<usize>())?;
    let role_root = 9_u64
        .checked_add(8)
        .and_then(|bytes| bytes.checked_add(9 + 8))
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    let role_step = (9_u64 + 16)
        .checked_add(9 + 8)
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    let max_role = role_root
        .checked_add(
            layers
                .checked_mul(role_step)
                .ok_or(GraphSageKernelError::IndexOverflow)?,
        )
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    let node_storage = sampled_nodes
        .checked_mul(node_struct)
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    let role_storage = sampled_nodes
        .checked_mul(max_role)
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    let child_storage = sampled_nodes
        .saturating_sub(1)
        .checked_mul(index)
        .ok_or(GraphSageKernelError::IndexOverflow)?;
    node_storage
        .checked_add(role_storage)
        .and_then(|bytes| bytes.checked_add(child_storage))
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or(GraphSageKernelError::IndexOverflow)
}

fn initialize_parameters(
    feature_width: usize,
    layer_widths: &[usize],
    options: &GraphSageOptions,
) -> Result<Parameters, GraphSageKernelError> {
    let mut weights = Vec::with_capacity(layer_widths.len());
    let mut prior_width = feature_width;
    for (layer, &output_width) in layer_widths.iter().enumerate() {
        let input_width = prior_width
            .checked_mul(2)
            .ok_or(GraphSageKernelError::IndexOverflow)?;
        let fan_sum = input_width
            .checked_add(output_width)
            .ok_or(GraphSageKernelError::IndexOverflow)?;
        let bound = (6.0 / usize_to_f64(fan_sum)).sqrt();
        let mut matrix = Matrix::zeros(output_width, input_width)?;
        for output in 0..output_width {
            for input in 0..input_width {
                let mut rng = EmbeddingRng::derive(
                    "graphsage",
                    "graphsage-xavier",
                    options.seed,
                    &[
                        EmbeddingRngField::U64(to_u64(layer)?),
                        EmbeddingRngField::U64(to_u64(output)?),
                        EmbeddingRngField::U64(to_u64(input)?),
                    ],
                );
                matrix.values[output * input_width + input] = (rng.unit_f64() * 2.0 - 1.0) * bound;
            }
        }
        weights.push(matrix);
        prior_width = output_width;
    }
    let first_moments = weights
        .iter()
        .map(|matrix| Matrix::zeros(matrix.rows, matrix.columns))
        .collect::<Result<Vec<_>, _>>()?;
    let second_moments = weights
        .iter()
        .map(|matrix| Matrix::zeros(matrix.rows, matrix.columns))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Parameters {
        weights,
        first_moments,
        second_moments,
        beta1_power: 1.0,
        beta2_power: 1.0,
    })
}

fn visit_positive_pairs(
    projection: &GraphSageProjection,
    seed: u64,
    control: &EmbeddingControl<'_>,
    charge_transitions: bool,
    mut visit: impl FnMut(u64, usize, usize) -> Result<(), GraphSageKernelError>,
) -> Result<(), GraphSageKernelError> {
    let mut pair_ordinal = 0_u64;
    for start in 0..projection.nodes.len() {
        let start_uuid = projection.nodes[start].uuid;
        for walk in 0..POSITIVE_WALKS_PER_NODE {
            let mut current = start;
            for transition in 0..POSITIVE_WALK_TRANSITIONS {
                if charge_transitions {
                    control.checkpoint(1)?;
                }
                let candidates = &projection.adjacency[current];
                if candidates.is_empty() {
                    break;
                }
                let mut rng = EmbeddingRng::derive(
                    "graphsage",
                    "positive-walk",
                    seed,
                    &[
                        EmbeddingRngField::Uuid(start_uuid),
                        EmbeddingRngField::U64(to_u64(walk)?),
                        EmbeddingRngField::U64(to_u64(transition)?),
                    ],
                );
                let degree = to_u64(candidates.len())?;
                let selected = rng
                    .bounded(degree)
                    .expect("validated non-empty candidate list");
                let selected =
                    usize::try_from(selected).map_err(|_| GraphSageKernelError::IndexOverflow)?;
                current = candidates[selected].neighbor;
                visit(pair_ordinal, start, current)?;
                pair_ordinal = pair_ordinal
                    .checked_add(1)
                    .ok_or(GraphSageKernelError::IndexOverflow)?;
            }
        }
    }
    Ok(())
}

fn negative_distribution(
    projection: &GraphSageProjection,
) -> Result<Vec<(usize, f64)>, GraphSageKernelError> {
    let mut masses = projection
        .adjacency
        .iter()
        .enumerate()
        .filter(|(_, candidates)| !candidates.is_empty())
        .map(|(node, candidates)| (node, usize_to_f64(candidates.len()).powf(0.75)))
        .collect::<Vec<_>>();
    let total = masses.iter().map(|(_, mass)| mass).sum::<f64>();
    if !total.is_finite() || total <= 0.0 {
        return Err(GraphSageKernelError::UndefinedTraining);
    }
    for (_, mass) in &mut masses {
        *mass /= total;
    }
    Ok(masses)
}

#[allow(clippy::too_many_arguments)]
fn train_pair(
    projection: &GraphSageProjection,
    options: &GraphSageOptions,
    epoch: u64,
    pair_ordinal: u64,
    center: usize,
    positive: usize,
    negative_distribution: &[(usize, f64)],
    parameters: &mut Parameters,
    control: &EmbeddingControl<'_>,
) -> Result<(), GraphSageKernelError> {
    control.checkpoint(0)?;
    let center_tree = build_sampled_tree(
        projection,
        center,
        options,
        epoch,
        pair_ordinal,
        &GraphSageRolePath::center(),
        control,
    )?;
    let center_embedding = forward_tree(
        &center_tree,
        projection,
        &parameters.weights,
        options.layers,
    )?;
    let mut center_gradient = vec![0.0; options.dimensions];
    let mut gradients = parameters
        .weights
        .iter()
        .map(|matrix| Matrix::zeros(matrix.rows, matrix.columns))
        .collect::<Result<Vec<_>, _>>()?;

    accumulate_target(
        projection,
        options,
        epoch,
        pair_ordinal,
        positive,
        &GraphSageRolePath::positive(),
        true,
        &center_embedding,
        &mut center_gradient,
        &parameters.weights,
        &mut gradients,
        control,
    )?;

    for negative_ordinal in 0..options.negative_samples {
        let negative = sample_graphsage_negative(
            negative_distribution,
            options.seed,
            epoch,
            pair_ordinal,
            to_u64(negative_ordinal)?,
        );
        let role = GraphSageRolePath::negative(to_u64(negative_ordinal)?);
        accumulate_target(
            projection,
            options,
            epoch,
            pair_ordinal,
            negative,
            &role,
            false,
            &center_embedding,
            &mut center_gradient,
            &parameters.weights,
            &mut gradients,
            control,
        )?;
    }
    backward_tree(
        &center_tree,
        projection,
        &parameters.weights,
        options.layers,
        &center_gradient,
        &mut gradients,
    )?;
    apply_adam(parameters, &gradients, options.learning_rate, control)
}

#[allow(clippy::too_many_arguments)]
fn accumulate_target(
    projection: &GraphSageProjection,
    options: &GraphSageOptions,
    epoch: u64,
    pair_ordinal: u64,
    target: usize,
    role: &GraphSageRolePath,
    positive: bool,
    center_embedding: &[f64],
    center_gradient: &mut [f64],
    weights: &[Matrix],
    gradients: &mut [Matrix],
    control: &EmbeddingControl<'_>,
) -> Result<(), GraphSageKernelError> {
    let tree = build_sampled_tree(
        projection,
        target,
        options,
        epoch,
        pair_ordinal,
        role,
        control,
    )?;
    let target_embedding = forward_tree(&tree, projection, weights, options.layers)?;
    let score = dot(center_embedding, &target_embedding)?;
    let loss = if positive {
        stable_softplus(-score)
    } else {
        stable_softplus(score)
    };
    if !loss.is_finite() {
        return Err(GraphSageKernelError::NonFiniteComputation);
    }
    let label = if positive { 1.0 } else { 0.0 };
    let coefficient = sigmoid(score) - label;
    for coordinate in 0..center_embedding.len() {
        center_gradient[coordinate] += coefficient * target_embedding[coordinate];
    }
    let target_gradient = center_embedding
        .iter()
        .map(|value| coefficient * value)
        .collect::<Vec<_>>();
    backward_tree(
        &tree,
        projection,
        weights,
        options.layers,
        &target_gradient,
        gradients,
    )
}

fn sample_graphsage_negative(
    distribution: &[(usize, f64)],
    seed: u64,
    epoch: u64,
    pair_ordinal: u64,
    negative_ordinal: u64,
) -> usize {
    let mut rng = EmbeddingRng::derive(
        "graphsage",
        "graphsage-negative",
        seed,
        &[
            EmbeddingRngField::U64(epoch),
            EmbeddingRngField::U64(pair_ordinal),
            EmbeddingRngField::U64(negative_ordinal),
        ],
    );
    let draw = rng.unit_f64();
    let mut cumulative = 0.0;
    distribution
        .iter()
        .find_map(|&(node, mass)| {
            cumulative += mass;
            (draw < cumulative).then_some(node)
        })
        .unwrap_or_else(|| distribution.last().expect("non-empty distribution").0)
}

fn build_sampled_tree(
    projection: &GraphSageProjection,
    root: usize,
    options: &GraphSageOptions,
    epoch: u64,
    example: u64,
    role_path: &GraphSageRolePath,
    control: &EmbeddingControl<'_>,
) -> Result<SampledTree, GraphSageKernelError> {
    let sample_sizes = options
        .sample_sizes
        .iter()
        .copied()
        .map(to_u64)
        .collect::<Result<Vec<_>, _>>()?;
    let capacity = usize::try_from(checked_sampled_nodes(&sample_sizes)?)
        .map_err(|_| GraphSageKernelError::IndexOverflow)?;
    let mut tree = SampledTree {
        nodes: Vec::with_capacity(capacity),
        layers: options.layers,
    };
    append_sampled_tree_node(
        &mut tree, projection, root, options, epoch, example, role_path, 0, control,
    )?;
    Ok(tree)
}

#[allow(clippy::too_many_arguments)]
fn append_sampled_tree_node(
    tree: &mut SampledTree,
    projection: &GraphSageProjection,
    graph_node: usize,
    options: &GraphSageOptions,
    epoch: u64,
    example: u64,
    role_path: &GraphSageRolePath,
    depth: usize,
    control: &EmbeddingControl<'_>,
) -> Result<usize, GraphSageKernelError> {
    let position = tree.nodes.len();
    tree.nodes.push(SampledTreeNode {
        graph_node,
        role_path: role_path.clone(),
        children: Vec::new(),
    });
    if depth == options.layers {
        control.checkpoint(1)?;
        return Ok(position);
    }
    let sampled = sample_graphsage_neighbors(
        projection,
        graph_node,
        options.sample_sizes[depth],
        GraphSageSampleKey {
            seed: options.seed,
            epoch,
            example,
            role_path,
            layer: to_u64(depth)?,
        },
        control,
    )?;
    tree.nodes[position].children = Vec::with_capacity(sampled.len());
    for (slot, neighbor) in sampled.into_iter().enumerate() {
        let child_role = role_path.child(projection.nodes[graph_node].uuid, to_u64(slot)?);
        let child = append_sampled_tree_node(
            tree,
            projection,
            neighbor,
            options,
            epoch,
            example,
            &child_role,
            depth + 1,
            control,
        )?;
        tree.nodes[position].children.push(child);
    }
    let mut children = std::mem::take(&mut tree.nodes[position].children);
    children.sort_unstable_by(|&left, &right| {
        tree.nodes[left]
            .role_path
            .0
            .cmp(&tree.nodes[right].role_path.0)
    });
    tree.nodes[position].children = children;
    Ok(position)
}

fn forward_tree(
    tree: &SampledTree,
    projection: &GraphSageProjection,
    weights: &[Matrix],
    level: usize,
) -> Result<Vec<f64>, GraphSageKernelError> {
    debug_assert_eq!(tree.layers, weights.len());
    forward_state(tree, 0, projection, weights, level)
}

fn forward_state(
    tree: &SampledTree,
    position: usize,
    projection: &GraphSageProjection,
    weights: &[Matrix],
    level: usize,
) -> Result<Vec<f64>, GraphSageKernelError> {
    let node = &tree.nodes[position];
    if level == 0 {
        return Ok(projection.nodes[node.graph_node].features.clone());
    }
    let self_state = forward_state(tree, position, projection, weights, level - 1)?;
    let mut neighbor_mean = vec![0.0; self_state.len()];
    for &child in &node.children {
        let child_state = forward_state(tree, child, projection, weights, level - 1)?;
        for (total, value) in neighbor_mean.iter_mut().zip(child_state) {
            *total += value;
        }
    }
    if !node.children.is_empty() {
        let count = usize_to_f64(node.children.len());
        for value in &mut neighbor_mean {
            *value /= count;
        }
    }
    let mut input = self_state;
    input.extend(neighbor_mean);
    activate(&weights[level - 1], &input)
}

fn activate(matrix: &Matrix, input: &[f64]) -> Result<Vec<f64>, GraphSageKernelError> {
    if input.len() != matrix.columns {
        return Err(GraphSageKernelError::FeatureShape);
    }
    let mut output = Vec::with_capacity(matrix.rows);
    for row in 0..matrix.rows {
        let value = matrix
            .row(row)
            .iter()
            .zip(input)
            .try_fold(0.0, |total, (weight, input)| {
                let value = total + weight * input;
                value
                    .is_finite()
                    .then_some(value)
                    .ok_or(GraphSageKernelError::NonFiniteComputation)
            })?;
        output.push(value.max(0.0));
    }
    normalize(output)
}

fn normalize(mut values: Vec<f64>) -> Result<Vec<f64>, GraphSageKernelError> {
    let squared_norm = values.iter().try_fold(0.0, |total, value| {
        let next = total + value * value;
        next.is_finite()
            .then_some(next)
            .ok_or(GraphSageKernelError::NonFiniteComputation)
    })?;
    if squared_norm == 0.0 {
        return Ok(values);
    }
    let norm = squared_norm.sqrt();
    for value in &mut values {
        *value /= norm;
    }
    Ok(values)
}

fn backward_tree(
    tree: &SampledTree,
    projection: &GraphSageProjection,
    weights: &[Matrix],
    level: usize,
    output_gradient: &[f64],
    gradients: &mut [Matrix],
) -> Result<(), GraphSageKernelError> {
    backward_state(
        tree,
        0,
        projection,
        weights,
        level,
        output_gradient,
        gradients,
    )
}

#[allow(clippy::too_many_arguments)]
fn backward_state(
    tree: &SampledTree,
    position: usize,
    projection: &GraphSageProjection,
    weights: &[Matrix],
    level: usize,
    output_gradient: &[f64],
    gradients: &mut [Matrix],
) -> Result<(), GraphSageKernelError> {
    if level == 0 {
        return Ok(());
    }
    let node = &tree.nodes[position];
    let self_state = forward_state(tree, position, projection, weights, level - 1)?;
    let mut child_states = Vec::with_capacity(node.children.len());
    let mut neighbor_mean = vec![0.0; self_state.len()];
    for &child in &node.children {
        let state = forward_state(tree, child, projection, weights, level - 1)?;
        for (total, value) in neighbor_mean.iter_mut().zip(&state) {
            *total += value;
        }
        child_states.push(state);
    }
    if !child_states.is_empty() {
        let count = usize_to_f64(child_states.len());
        for value in &mut neighbor_mean {
            *value /= count;
        }
    }
    let mut input = self_state;
    input.extend(neighbor_mean);
    let matrix = &weights[level - 1];
    let pre_activation = matrix
        .values
        .chunks(matrix.columns)
        .map(|row| {
            row.iter()
                .zip(&input)
                .map(|(weight, value)| weight * value)
                .sum()
        })
        .collect::<Vec<f64>>();
    let activated = pre_activation
        .iter()
        .map(|value| value.max(0.0))
        .collect::<Vec<_>>();
    let normalized = normalize(activated.clone())?;
    let norm = activated
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    let mut pre_activation_gradient = vec![0.0; matrix.rows];
    if norm > 0.0 {
        let normalization_projection = normalized
            .iter()
            .zip(output_gradient)
            .map(|(state, gradient)| state * gradient)
            .sum::<f64>();
        for output in 0..matrix.rows {
            let normalized_gradient =
                (output_gradient[output] - normalized[output] * normalization_projection) / norm;
            if pre_activation[output] > 0.0 {
                pre_activation_gradient[output] = normalized_gradient;
            }
        }
    }
    let mut input_gradient = vec![0.0; matrix.columns];
    for (output, &output_derivative) in pre_activation_gradient.iter().enumerate() {
        for input_coordinate in 0..matrix.columns {
            let index = output * matrix.columns + input_coordinate;
            gradients[level - 1].values[index] += output_derivative * input[input_coordinate];
            input_gradient[input_coordinate] += matrix.values[index] * output_derivative;
        }
    }
    let prior_width = input_gradient.len() / 2;
    backward_state(
        tree,
        position,
        projection,
        weights,
        level - 1,
        &input_gradient[..prior_width],
        gradients,
    )?;
    if !node.children.is_empty() {
        let scale = 1.0 / usize_to_f64(node.children.len());
        let neighbor_gradient = input_gradient[prior_width..]
            .iter()
            .map(|value| value * scale)
            .collect::<Vec<_>>();
        for &child in &node.children {
            backward_state(
                tree,
                child,
                projection,
                weights,
                level - 1,
                &neighbor_gradient,
                gradients,
            )?;
        }
    }
    Ok(())
}

fn apply_adam(
    parameters: &mut Parameters,
    gradients: &[Matrix],
    learning_rate: f64,
    control: &EmbeddingControl<'_>,
) -> Result<(), GraphSageKernelError> {
    control.checkpoint(0)?;
    parameters.beta1_power *= ADAM_BETA1;
    parameters.beta2_power *= ADAM_BETA2;
    let beta1_power = parameters.beta1_power;
    let beta2_power = parameters.beta2_power;
    for (((weights, first_moments), second_moments), layer_gradients) in parameters
        .weights
        .iter_mut()
        .zip(&mut parameters.first_moments)
        .zip(&mut parameters.second_moments)
        .zip(gradients)
    {
        for (((weight, first_moment), second_moment), &gradient) in weights
            .values
            .iter_mut()
            .zip(&mut first_moments.values)
            .zip(&mut second_moments.values)
            .zip(&layer_gradients.values)
        {
            control.checkpoint(1)?;
            if !gradient.is_finite() {
                return Err(GraphSageKernelError::NonFiniteComputation);
            }
            let first = ADAM_BETA1 * *first_moment + (1.0 - ADAM_BETA1) * gradient;
            let second = ADAM_BETA2 * *second_moment + (1.0 - ADAM_BETA2) * gradient * gradient;
            let first_hat = first / (1.0 - beta1_power);
            let second_hat = second / (1.0 - beta2_power);
            let updated_weight =
                *weight - learning_rate * first_hat / (second_hat.sqrt() + ADAM_EPSILON);
            if !updated_weight.is_finite() {
                return Err(GraphSageKernelError::NonFiniteComputation);
            }
            *first_moment = first;
            *second_moment = second;
            *weight = updated_weight;
        }
    }
    Ok(())
}

fn infer_full_neighborhood(
    projection: &GraphSageProjection,
    options: &GraphSageOptions,
    weights: &[Matrix],
    control: &EmbeddingControl<'_>,
) -> Result<Vec<GraphSageEmbeddingRow>, GraphSageKernelError> {
    let mut states = projection
        .nodes
        .iter()
        .map(|node| node.features.clone())
        .collect::<Vec<_>>();
    for matrix in weights {
        let mut next = Vec::with_capacity(states.len());
        for (node, candidates) in projection.adjacency.iter().enumerate() {
            let mut mean = vec![0.0; states[node].len()];
            for candidate in candidates {
                for (coordinate, total) in mean.iter_mut().enumerate() {
                    control.checkpoint(1)?;
                    *total += states[candidate.neighbor][coordinate];
                }
            }
            if !candidates.is_empty() {
                let count = usize_to_f64(candidates.len());
                for value in &mut mean {
                    *value /= count;
                }
            }
            let mut input = states[node].clone();
            input.extend(mean);
            let mut output = vec![0.0; matrix.rows];
            for (row, value) in output.iter_mut().enumerate() {
                for (weight, input) in matrix.row(row).iter().zip(&input) {
                    control.checkpoint(1)?;
                    *value += weight * input;
                }
                *value = value.max(0.0);
            }
            output = normalize(output)?;
            control.checkpoint(to_u64(matrix.rows)?)?;
            next.push(output);
        }
        states = next;
    }
    control.before_publish()?;
    projection
        .nodes
        .iter()
        .zip(states)
        .map(|(node, embedding)| {
            let embedding = embedding
                .into_iter()
                .map(f64_to_f32)
                .collect::<Result<Vec<_>, _>>()?;
            debug_assert_eq!(embedding.len(), options.dimensions);
            Ok(EmbeddingOutputRow {
                node_uuid: node.uuid,
                embedding,
            })
        })
        .collect()
}

fn dot(left: &[f64], right: &[f64]) -> Result<f64, GraphSageKernelError> {
    if left.len() != right.len() {
        return Err(GraphSageKernelError::FeatureShape);
    }
    left.iter()
        .zip(right)
        .try_fold(0.0, |total, (left, right)| {
            let value = total + left * right;
            value
                .is_finite()
                .then_some(value)
                .ok_or(GraphSageKernelError::NonFiniteComputation)
        })
}

fn sigmoid(value: f64) -> f64 {
    if value >= 0.0 {
        1.0 / (1.0 + (-value).exp())
    } else {
        let exponential = value.exp();
        exponential / (1.0 + exponential)
    }
}

fn stable_softplus(value: f64) -> f64 {
    value.max(0.0) + (-value.abs()).exp().ln_1p()
}

fn to_u64(value: usize) -> Result<u64, GraphSageKernelError> {
    u64::try_from(value).map_err(|_| GraphSageKernelError::IndexOverflow)
}

#[allow(clippy::cast_possible_truncation)]
fn f64_to_f32(value: f64) -> Result<f32, GraphSageKernelError> {
    let value = value as f32;
    value
        .is_finite()
        .then_some(value)
        .ok_or(GraphSageKernelError::NonFiniteComputation)
}

fn typed_field(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9 + payload.len());
    bytes.push(tag);
    bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{
        AlgorithmCancellation, AlgorithmControl, AlgorithmError, AlgorithmLimits,
    };
    use crate::algorithm_embedding_control::EmbeddingResourceLimits;

    fn uuid(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    fn node(id: u128, features: &[f64]) -> GraphSageNode {
        GraphSageNode {
            uuid: uuid(id),
            features: features.to_vec(),
        }
    }

    fn edge(id: u128, source: u128, target: u128) -> GraphSageEdge {
        GraphSageEdge {
            uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
        }
    }

    fn control(
        cancellation: AlgorithmCancellation,
        work: u64,
    ) -> (AlgorithmControl, EmbeddingResourceLimits) {
        (
            AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            EmbeddingResourceLimits {
                memory_bytes: u64::MAX,
                work,
            },
        )
    }

    fn options(seed: u64) -> GraphSageOptions {
        GraphSageOptions {
            dimensions: 2,
            hidden_dimensions: 3,
            layers: 2,
            sample_sizes: vec![2, 3],
            epochs: 1,
            negative_samples: 1,
            learning_rate: 0.000_002,
            feature_properties: vec!["feature".into()],
            seed,
            ..GraphSageOptions::default()
        }
    }

    #[test]
    fn projection_is_uuid_ordered_and_validates_feature_shape_and_finiteness() {
        let projection = validate_graphsage_projection(
            vec![node(3, &[3.0, 30.0]), node(1, &[1.0, 10.0])],
            vec![],
        )
        .unwrap();
        assert_eq!(
            projection
                .nodes()
                .iter()
                .map(|node| node.uuid)
                .collect::<Vec<_>>(),
            vec![uuid(1), uuid(3)]
        );
        assert_eq!(projection.feature_width(), 2);
        assert_eq!(
            validate_graphsage_projection(vec![node(1, &[1.0]), node(2, &[2.0, 3.0])], vec![]),
            Err(GraphSageKernelError::FeatureShape)
        );
        assert_eq!(
            validate_graphsage_projection(vec![node(1, &[f64::NAN])], vec![]),
            Err(GraphSageKernelError::NonFiniteFeature)
        );
        assert_eq!(
            validate_graphsage_projection(vec![node(1, &[])], vec![]),
            Err(GraphSageKernelError::EmptyFeatures)
        );
    }

    #[test]
    fn samples_without_replacement_when_degree_meets_fanout() {
        let graph = validate_graphsage_projection(
            (1..=5).rev().map(|id| node(id, &[id as f64])).collect(),
            vec![
                edge(12, 1, 2),
                edge(13, 1, 3),
                edge(14, 1, 4),
                edge(15, 1, 5),
            ],
        )
        .unwrap();
        let (algorithm, limits) = control(AlgorithmCancellation::default(), 100);
        let control = EmbeddingControl::new(&algorithm, limits);
        let key = GraphSageSampleKey {
            seed: 7,
            epoch: 2,
            example: 9,
            role_path: &GraphSageRolePath::positive(),
            layer: 1,
        };
        let sampled = sample_graphsage_neighbors(&graph, 0, 3, key, &control).unwrap();
        assert_eq!(sampled, vec![4, 2, 3]);
        assert_eq!(
            sampled
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn replacement_preserves_parallel_edge_multiplicity_and_mean() {
        let graph = validate_graphsage_projection(
            vec![node(1, &[1.0]), node(2, &[2.0]), node(3, &[9.0])],
            vec![edge(11, 1, 2), edge(12, 1, 2), edge(13, 1, 3)],
        )
        .unwrap();
        assert_eq!(
            graph.adjacency[0]
                .iter()
                .map(|candidate| candidate.neighbor)
                .collect::<Vec<_>>(),
            vec![1, 1, 2]
        );
        let (algorithm, limits) = control(AlgorithmCancellation::default(), 100);
        let control = EmbeddingControl::new(&algorithm, limits);
        let sampled = sample_graphsage_neighbors(
            &graph,
            0,
            7,
            GraphSageSampleKey {
                seed: 4,
                epoch: 0,
                example: 3,
                role_path: &GraphSageRolePath::negative(1).child(uuid(9), 2),
                layer: 0,
            },
            &control,
        )
        .unwrap();
        assert_eq!(sampled, vec![1, 1, 2, 2, 2, 1, 2]);
        assert_eq!(
            graphsage_mean(
                &graph
                    .nodes()
                    .iter()
                    .map(|node| node.features.clone())
                    .collect::<Vec<_>>(),
                &sampled
            )
            .unwrap(),
            vec![6.0]
        );
    }

    #[test]
    fn loops_are_excluded_and_isolates_and_dead_ends_sample_empty() {
        let graph = validate_graphsage_projection(
            vec![node(1, &[1.0]), node(2, &[2.0])],
            vec![edge(10, 1, 1)],
        )
        .unwrap();
        let (algorithm, limits) = control(AlgorithmCancellation::default(), 10);
        let control = EmbeddingControl::new(&algorithm, limits);
        for node in 0..2 {
            assert_eq!(
                sample_graphsage_neighbors(
                    &graph,
                    node,
                    2,
                    GraphSageSampleKey {
                        seed: 0,
                        epoch: 0,
                        example: 0,
                        role_path: &GraphSageRolePath::center(),
                        layer: 0,
                    },
                    &control,
                )
                .unwrap(),
                Vec::<usize>::new()
            );
        }
        assert_eq!(
            graphsage_mean(&[vec![1.0, 2.0]], &[]).unwrap(),
            vec![0.0, 0.0]
        );
    }

    #[test]
    fn sampling_replays_exactly_and_separates_seed_and_role_paths() {
        let graph = validate_graphsage_projection(
            (1..=4).map(|id| node(id, &[id as f64])).collect(),
            vec![edge(11, 1, 2), edge(12, 1, 3), edge(13, 1, 4)],
        )
        .unwrap();
        let draw = |seed, role: &GraphSageRolePath| {
            let (algorithm, limits) = control(AlgorithmCancellation::default(), 100);
            let control = EmbeddingControl::new(&algorithm, limits);
            sample_graphsage_neighbors(
                &graph,
                0,
                8,
                GraphSageSampleKey {
                    seed,
                    epoch: 1,
                    example: 2,
                    role_path: role,
                    layer: 0,
                },
                &control,
            )
            .unwrap()
        };
        let first = draw(17, &GraphSageRolePath::center());
        assert_eq!(first, draw(17, &GraphSageRolePath::center()));
        assert_ne!(first, draw(18, &GraphSageRolePath::center()));
        assert_ne!(first, draw(17, &GraphSageRolePath::positive()));
    }

    #[test]
    fn checkpoints_enforce_work_and_cancellation_without_partial_samples() {
        let graph = validate_graphsage_projection(
            vec![node(1, &[1.0]), node(2, &[2.0])],
            vec![edge(1, 1, 2)],
        )
        .unwrap();
        let key = GraphSageSampleKey {
            seed: 0,
            epoch: 0,
            example: 0,
            role_path: &GraphSageRolePath::center(),
            layer: 0,
        };
        let (algorithm, limits) = control(AlgorithmCancellation::default(), 0);
        let bounded = EmbeddingControl::new(&algorithm, limits);
        assert!(matches!(
            sample_graphsage_neighbors(&graph, 0, 3, key, &bounded),
            Err(GraphSageKernelError::Resource(
                EmbeddingResourceError::WorkLimit {
                    observed: 1,
                    limit: 0
                }
            ))
        ));

        let cancellation = AlgorithmCancellation::default();
        let (algorithm, limits) = control(cancellation.clone(), 10);
        let cancelled = EmbeddingControl::new(&algorithm, limits);
        cancellation.cancel();
        assert_eq!(
            sample_graphsage_neighbors(&graph, 0, 1, key, &cancelled),
            Err(GraphSageKernelError::Resource(
                EmbeddingResourceError::Algorithm(AlgorithmError::Cancelled)
            ))
        );
    }

    #[test]
    fn sampled_tree_uses_root_nearest_fanouts_and_reuses_self_positions() {
        let graph = validate_graphsage_projection(
            vec![node(1, &[1.0]), node(2, &[2.0]), node(3, &[3.0])],
            vec![edge(11, 1, 2), edge(12, 2, 3)],
        )
        .unwrap();
        let options = options(5);
        let (algorithm, limits) = control(AlgorithmCancellation::default(), 100);
        let control = EmbeddingControl::new(&algorithm, limits);
        let tree = build_sampled_tree(
            &graph,
            0,
            &options,
            0,
            0,
            &GraphSageRolePath::center(),
            &control,
        )
        .unwrap();
        assert_eq!(tree.nodes.len(), 1 + 2 + 2 * 3);
        assert_eq!(tree.nodes[0].children.len(), 2);
        assert!(
            tree.nodes[0]
                .children
                .iter()
                .all(|&child| tree.nodes[child].children.len() == 3)
        );
        assert_eq!(
            tree.nodes
                .iter()
                .filter(|node| node.role_path == GraphSageRolePath::center())
                .count(),
            1,
            "the self state reuses the root position rather than sampling a self child"
        );
    }

    #[test]
    fn xavier_initialization_is_exact_and_seed_separated() {
        let first = initialize_parameters(2, &[3, 2], &options(7)).unwrap();
        let replay = initialize_parameters(2, &[3, 2], &options(7)).unwrap();
        let other = initialize_parameters(2, &[3, 2], &options(8)).unwrap();
        assert_eq!(first.weights[0].values, replay.weights[0].values);
        assert_ne!(first.weights[0].values, other.weights[0].values);
        assert_eq!(first.weights[0].values[0].to_bits(), 0xbfd6_027a_b81c_2f50);
    }

    #[test]
    fn positive_walks_replay_and_dead_ends_are_undefined() {
        let graph = validate_graphsage_projection(
            vec![node(1, &[1.0]), node(2, &[2.0])],
            vec![edge(1, 1, 2)],
        )
        .unwrap();
        let (algorithm, limits) = control(AlgorithmCancellation::default(), u64::MAX);
        let control = EmbeddingControl::new(&algorithm, limits);
        let collect = |charge| {
            let mut pairs = Vec::new();
            visit_positive_pairs(&graph, 9, &control, charge, |ordinal, start, target| {
                pairs.push((ordinal, start, target));
                Ok(())
            })
            .unwrap();
            pairs
        };
        assert_eq!(collect(true), collect(false));

        let isolate = validate_graphsage_projection(vec![node(1, &[1.0, 2.0])], vec![]).unwrap();
        assert_eq!(
            train_graphsage(&isolate, &options(0), &control),
            Err(GraphSageKernelError::UndefinedTraining)
        );
    }

    #[test]
    fn trained_output_is_uuid_ordered_finite_and_exactly_replayed() {
        let graph = validate_graphsage_projection(
            vec![node(2, &[0.0, 1.0]), node(1, &[1.0, 0.0])],
            vec![edge(1, 1, 2)],
        )
        .unwrap();
        let mut options = options(11);
        options.layers = 1;
        options.sample_sizes = vec![1];
        options.hidden_dimensions = 2;
        let run = || {
            let (algorithm, limits) = control(AlgorithmCancellation::default(), u64::MAX);
            let control = EmbeddingControl::new(&algorithm, limits);
            train_graphsage(&graph, &options, &control).unwrap()
        };
        let first = run();
        assert_eq!(first, run());
        assert_eq!(
            first.iter().map(|row| row.node_uuid).collect::<Vec<_>>(),
            vec![uuid(1), uuid(2)]
        );
        assert!(
            first
                .iter()
                .flat_map(|row| &row.embedding)
                .all(|value| value.is_finite())
        );
        assert_eq!(
            first
                .iter()
                .flat_map(|row| &row.embedding)
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            vec![1.0_f32.to_bits(), 0, 0, 0]
        );
    }

    #[test]
    fn direct_and_equivalent_resolved_uuid_feature_projections_are_identical() {
        let direct = validate_graphsage_projection(
            vec![
                node(1, &[1.0, 0.0]),
                node(2, &[0.0, 1.0]),
                node(3, &[0.5, 0.5]),
            ],
            vec![edge(11, 1, 2), edge(12, 2, 3)],
        )
        .unwrap();
        let resolved = validate_graphsage_projection(
            vec![
                node(3, &[0.5, 0.5]),
                node(1, &[1.0, 0.0]),
                node(2, &[0.0, 1.0]),
            ],
            vec![edge(12, 3, 2), edge(11, 2, 1)],
        )
        .unwrap();
        let mut options = options(17);
        options.layers = 1;
        options.sample_sizes = vec![1];
        options.hidden_dimensions = 2;
        let run = |projection| {
            let (algorithm, limits) = control(AlgorithmCancellation::default(), u64::MAX);
            let control = EmbeddingControl::new(&algorithm, limits);
            train_graphsage(projection, &options, &control).unwrap()
        };
        assert_eq!(run(&direct), run(&resolved));
    }

    #[test]
    fn complete_invocation_validation_and_controls_fail_atomically() {
        let graph = validate_graphsage_projection(
            vec![node(1, &[1.0]), node(2, &[2.0])],
            vec![edge(1, 1, 2)],
        )
        .unwrap();
        let mut invalid = options(0);
        invalid.sample_sizes = vec![1];
        assert_eq!(
            validate_graphsage_options(&invalid),
            Err(GraphSageKernelError::SampleShape)
        );
        invalid = options(0);
        invalid.feature_properties.clear();
        assert_eq!(
            validate_graphsage_options(&invalid),
            Err(GraphSageKernelError::MissingFeatureProperties)
        );
        invalid = options(0);
        invalid.learning_rate = f64::NAN;
        assert_eq!(
            validate_graphsage_options(&invalid),
            Err(GraphSageKernelError::InvalidLearningRate)
        );

        let (algorithm, limits) = control(AlgorithmCancellation::default(), 1);
        let bounded = EmbeddingControl::new(&algorithm, limits);
        assert!(matches!(
            train_graphsage(&graph, &options(0), &bounded),
            Err(GraphSageKernelError::Resource(
                EmbeddingResourceError::WorkLimit { .. }
            ))
        ));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let (algorithm, limits) = control(cancellation, u64::MAX);
        let cancelled = EmbeddingControl::new(&algorithm, limits);
        assert_eq!(
            train_graphsage(&graph, &options(0), &cancelled),
            Err(GraphSageKernelError::Resource(
                EmbeddingResourceError::Algorithm(AlgorithmError::Cancelled)
            ))
        );
    }

    #[test]
    fn stable_loss_and_negative_draws_are_exact_and_endpoint_total() {
        assert_eq!(stable_softplus(1_000.0), 1_000.0);
        assert_eq!(stable_softplus(-1_000.0), 0.0);
        let distribution = vec![(0, 0.5), (1, 0.5)];
        assert_eq!(
            (0..4)
                .map(|ordinal| sample_graphsage_negative(&distribution, 3, 1, 2, ordinal))
                .collect::<Vec<_>>(),
            vec![1, 0, 1, 1]
        );
    }

    #[test]
    fn two_tree_peak_is_fully_preflighted_at_the_memory_boundary() {
        let graph = validate_graphsage_projection(
            vec![node(1, &[1.0]), node(2, &[2.0]), node(3, &[3.0])],
            vec![edge(11, 1, 2), edge(12, 2, 3)],
        )
        .unwrap();
        let options = options(4);
        let widths = layer_widths(&options);
        let estimate = graphsage_resource_estimate(&graph, &options, &widths).unwrap();
        let sampled_nodes = 1 + 2 + 2 * 3;
        let max_role_bytes = 34 + 2 * 42;
        let exact_tree_peak = 2
            * (sampled_nodes * size_of::<SampledTreeNode>()
                + sampled_nodes * max_role_bytes
                + (sampled_nodes - 1) * size_of::<usize>());
        assert!(estimate.scratch_bytes >= u64::try_from(exact_tree_peak).unwrap());
        let observed = estimate.memory_bytes().unwrap();

        let algorithm =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let passing = EmbeddingControl::new(
            &algorithm,
            EmbeddingResourceLimits {
                memory_bytes: observed,
                work: u64::MAX,
            },
        );
        assert_eq!(
            preflight_graphsage(&graph, &options, &widths, &passing),
            Ok(())
        );
        let failing = EmbeddingControl::new(
            &algorithm,
            EmbeddingResourceLimits {
                memory_bytes: observed - 1,
                work: u64::MAX,
            },
        );
        assert_eq!(
            preflight_graphsage(&graph, &options, &widths, &failing),
            Err(GraphSageKernelError::Resource(
                EmbeddingResourceError::MemoryLimit {
                    observed,
                    limit: observed - 1
                }
            ))
        );
    }

    #[test]
    fn activation_relu_and_l2_are_exact() {
        let matrix = Matrix {
            rows: 2,
            columns: 4,
            values: vec![1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
        };
        assert_eq!(
            activate(&matrix, &[3.0, -4.0, 9.0, 9.0]).unwrap(),
            vec![1.0, 0.0]
        );
        assert_eq!(
            activate(&matrix, &[-3.0, -4.0, 9.0, 9.0]).unwrap(),
            vec![0.0, 0.0]
        );
        let diagonal = Matrix {
            rows: 2,
            columns: 4,
            values: vec![1.0, 0.0, 0.0, 0.0, 0.0, -1.0, 0.0, 0.0],
        };
        assert_eq!(
            activate(&diagonal, &[3.0, -4.0, 0.0, 0.0]).unwrap(),
            vec![0.6, 0.8]
        );
    }

    #[test]
    fn reverse_mode_matches_a_finite_difference_tiny_case() {
        let projection =
            validate_graphsage_projection(vec![node(1, &[0.6, -0.2])], vec![]).unwrap();
        let tree = SampledTree {
            nodes: vec![SampledTreeNode {
                graph_node: 0,
                role_path: GraphSageRolePath::center(),
                children: vec![],
            }],
            layers: 1,
        };
        let matrix = Matrix {
            rows: 2,
            columns: 4,
            values: vec![0.7, -0.3, 0.2, 0.4, 0.4, -0.2, 0.5, -0.6],
        };
        let output_gradient = [0.3, -0.7];
        let mut analytical = vec![Matrix::zeros(2, 4).unwrap()];
        backward_tree(
            &tree,
            &projection,
            std::slice::from_ref(&matrix),
            1,
            &output_gradient,
            &mut analytical,
        )
        .unwrap();

        let epsilon = 1e-6;
        for coordinate in 0..matrix.values.len() {
            let objective = |delta| {
                let mut perturbed = matrix.clone();
                perturbed.values[coordinate] += delta;
                dot(
                    &forward_tree(&tree, &projection, std::slice::from_ref(&perturbed), 1).unwrap(),
                    &output_gradient,
                )
                .unwrap()
            };
            let numerical = (objective(epsilon) - objective(-epsilon)) / (2.0 * epsilon);
            assert!(
                (analytical[0].values[coordinate] - numerical).abs() < 1e-8,
                "coordinate {coordinate}: analytical={}, numerical={numerical}",
                analytical[0].values[coordinate]
            );
        }
    }

    #[test]
    fn adam_one_and_multiple_updates_use_carried_powers_and_old_state() {
        let mut parameters = Parameters {
            weights: vec![Matrix {
                rows: 1,
                columns: 2,
                values: vec![1.0, -2.0],
            }],
            first_moments: vec![Matrix::zeros(1, 2).unwrap()],
            second_moments: vec![Matrix::zeros(1, 2).unwrap()],
            beta1_power: 1.0,
            beta2_power: 1.0,
        };
        let (algorithm, limits) = control(AlgorithmCancellation::default(), 10);
        let control = EmbeddingControl::new(&algorithm, limits);
        let first = vec![Matrix {
            rows: 1,
            columns: 2,
            values: vec![0.5, -0.25],
        }];
        apply_adam(&mut parameters, &first, 0.01, &control).unwrap();
        assert_eq!(parameters.beta1_power.to_bits(), 0.9_f64.to_bits());
        assert_eq!(parameters.beta2_power.to_bits(), 0.999_f64.to_bits());
        for (coordinate, old_weight) in [1.0, -2.0].into_iter().enumerate() {
            let gradient = first[0].values[coordinate];
            let first_moment = (1.0 - ADAM_BETA1) * gradient;
            let second_moment = (1.0 - ADAM_BETA2) * gradient * gradient;
            let expected = old_weight
                - 0.01 * (first_moment / (1.0 - ADAM_BETA1))
                    / ((second_moment / (1.0 - ADAM_BETA2)).sqrt() + ADAM_EPSILON);
            assert_eq!(parameters.weights[0].values[coordinate], expected);
        }

        let old_weights = parameters.weights[0].values.clone();
        let old_first = parameters.first_moments[0].values.clone();
        let old_second = parameters.second_moments[0].values.clone();
        let second = vec![Matrix {
            rows: 1,
            columns: 2,
            values: vec![0.25, 0.5],
        }];
        apply_adam(&mut parameters, &second, 0.01, &control).unwrap();
        assert_eq!(parameters.beta1_power.to_bits(), (0.9_f64 * 0.9).to_bits());
        assert_eq!(
            parameters.beta2_power.to_bits(),
            (0.999_f64 * 0.999).to_bits()
        );
        for coordinate in 0..2 {
            let gradient = second[0].values[coordinate];
            let first_moment = ADAM_BETA1 * old_first[coordinate] + (1.0 - ADAM_BETA1) * gradient;
            let second_moment =
                ADAM_BETA2 * old_second[coordinate] + (1.0 - ADAM_BETA2) * gradient * gradient;
            let expected = old_weights[coordinate]
                - 0.01 * (first_moment / (1.0 - 0.9_f64 * 0.9))
                    / ((second_moment / (1.0 - 0.999_f64 * 0.999)).sqrt() + ADAM_EPSILON);
            assert_eq!(parameters.weights[0].values[coordinate], expected);
        }
    }

    #[test]
    fn training_changes_parameters_and_full_neighborhood_output() {
        let graph = validate_graphsage_projection(
            vec![
                node(1, &[1.0, 0.2]),
                node(2, &[0.1, 1.0]),
                node(3, &[0.7, 0.4]),
            ],
            vec![edge(11, 1, 2), edge(12, 2, 3), edge(13, 1, 3)],
        )
        .unwrap();
        let mut options = options(19);
        options.layers = 1;
        options.sample_sizes = vec![2];
        options.learning_rate = 0.05;
        let widths = layer_widths(&options);
        let mut initial = initialize_parameters(graph.feature_width, &widths, &options).unwrap();
        initial.weights[0].values = vec![0.5, 0.2, 0.3, 0.1, 0.1, 0.4, 0.2, 0.6];
        let (algorithm, limits) = control(AlgorithmCancellation::default(), u64::MAX);
        let initial_control = EmbeddingControl::new(&algorithm, limits);
        let initial_output =
            infer_full_neighborhood(&graph, &options, &initial.weights, &initial_control).unwrap();

        let mut trained = initial.clone();
        let trained_control = EmbeddingControl::new(&algorithm, limits);
        train_pair(
            &graph,
            &options,
            0,
            0,
            0,
            1,
            &negative_distribution(&graph).unwrap(),
            &mut trained,
            &trained_control,
        )
        .unwrap();
        assert_ne!(trained.weights[0].values, initial.weights[0].values);
        let output_control = EmbeddingControl::new(&algorithm, limits);
        let trained_output =
            infer_full_neighborhood(&graph, &options, &trained.weights, &output_control).unwrap();
        assert_ne!(trained_output, initial_output);
    }
}
