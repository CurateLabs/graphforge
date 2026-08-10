//! Deterministic FastRP mathematical kernel.
//!
//! Row-owned FastRP work (#559) may use the instance-owned private compute pool
//! above a documented crossover. Each source row retains serial neighbor and
//! coordinate order; worker chunks merge by canonical node ordinal so embeddings
//! remain bit-for-bit identical to the one-thread path.

use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use graphforge_core::embedding_options::FastRpOptions;
use rayon::prelude::*;

use crate::algorithm_embedding_control::{EmbeddingControl, EmbeddingResourceError};
use crate::algorithm_embedding_output::EmbeddingOutputRow;
use crate::algorithm_embedding_rng::{EmbeddingRng, EmbeddingRngField};
use crate::algorithm_graph::AdjacencyGraph;

/// Estimated FastRP row/coordinate ops below which execution stays serial (#559).
///
/// Keeps small fixtures and micro-invocations off the worker pool. At or above
/// this boundary, row-owned projection, accumulation, and sparse matvec work
/// amortize scheduling while preserving each row's serial arithmetic order.
pub const FASTRP_PARALLEL_CROSSOVER_OPS: u64 = 65_536;

pub(crate) type FastRpEmbeddingRow = EmbeddingOutputRow;

/// Selected FastRP execution path for observability and crossover tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FastRpExecutionPath {
    Serial,
    Parallel { threads: usize, chunks: usize },
}

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
    #[error("fastrp worker panicked")]
    WorkerPanic,
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
    let work_units = estimated_fastrp_ops(
        input.nodes.len(),
        adjacency_entries(&adjacency),
        options.dimensions,
        options.iteration_weights.len(),
        options.feature_properties.len(),
    );
    let path = select_fastrp_path(control, input.nodes.len(), work_units);
    let mut current = initial_projection(
        &input.nodes,
        &strengths,
        total_strength,
        options,
        control,
        dimensions,
        path,
    )?;
    mix_features(
        &mut current,
        &input.nodes,
        options,
        control,
        dimensions,
        path,
    )?;

    let mut accumulator = vec![vec![0.0; options.dimensions]; input.nodes.len()];
    accumulate(
        &mut accumulator,
        &current,
        options.iteration_weights[0],
        control,
        path,
    )?;
    for &iteration_weight in options.iteration_weights.iter().skip(1) {
        let next = propagate(
            &adjacency,
            &strengths,
            &current,
            options.dimensions,
            dimensions,
            control,
            path,
        )?;
        current = next;
        accumulate(&mut accumulator, &current, iteration_weight, control, path)?;
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

/// Choose serial vs private-pool parallel FastRP execution.
pub(crate) fn select_fastrp_path(
    control: &EmbeddingControl<'_>,
    rows: usize,
    estimated_ops: u64,
) -> FastRpExecutionPath {
    let threads = control.compute_threads();
    if threads <= 1 || rows <= 1 || estimated_ops < FASTRP_PARALLEL_CROSSOVER_OPS {
        return FastRpExecutionPath::Serial;
    }
    if control
        .compute_pool()
        .is_none_or(|pool| !pool.is_parallel())
    {
        return FastRpExecutionPath::Serial;
    }
    let chunks = row_chunks(rows, threads).len();
    if chunks <= 1 {
        return FastRpExecutionPath::Serial;
    }
    FastRpExecutionPath::Parallel { threads, chunks }
}

fn estimated_fastrp_ops(
    nodes: usize,
    adjacency_entries: usize,
    dimensions: usize,
    iteration_weights: usize,
    properties: usize,
) -> u64 {
    let nodes = u64::try_from(nodes).unwrap_or(u64::MAX);
    let adjacency_entries = u64::try_from(adjacency_entries).unwrap_or(u64::MAX);
    let dimensions = u64::try_from(dimensions).unwrap_or(u64::MAX);
    let iteration_weights = u64::try_from(iteration_weights).unwrap_or(u64::MAX);
    let properties = u64::try_from(properties).unwrap_or(u64::MAX);
    let propagated_iterations = iteration_weights.saturating_sub(1);
    adjacency_entries
        .saturating_mul(propagated_iterations)
        .saturating_mul(dimensions)
        .saturating_add(nodes.saturating_mul(dimensions))
        .saturating_add(
            nodes
                .saturating_mul(iteration_weights)
                .saturating_mul(dimensions),
        )
        .saturating_add(properties.saturating_mul(dimensions))
        .saturating_add(nodes.saturating_mul(properties).saturating_mul(dimensions))
}

fn adjacency_entries(adjacency: &[Vec<(usize, f64)>]) -> usize {
    adjacency.iter().map(Vec::len).sum()
}

fn initial_projection(
    nodes: &[FastRpNode],
    strengths: &[f64],
    total_strength: f64,
    options: &FastRpOptions,
    control: &EmbeddingControl<'_>,
    dimensions: u64,
    path: FastRpExecutionPath,
) -> Result<Vec<Vec<f64>>, FastRpError> {
    match path {
        FastRpExecutionPath::Serial => initial_projection_serial(
            nodes,
            strengths,
            total_strength,
            options,
            control,
            dimensions,
        ),
        FastRpExecutionPath::Parallel { .. } => initial_projection_parallel(
            nodes,
            strengths,
            total_strength,
            options,
            control,
            dimensions,
        ),
    }
}

fn initial_projection_serial(
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
            initial_projection_row(
                node,
                node_index,
                strengths,
                total_strength,
                options,
                control,
                dimensions,
                node_q,
            )
        })
        .collect()
}

fn initial_projection_parallel(
    nodes: &[FastRpNode],
    strengths: &[f64],
    total_strength: f64,
    options: &FastRpOptions,
    control: &EmbeddingControl<'_>,
    dimensions: u64,
) -> Result<Vec<Vec<f64>>, FastRpError> {
    let pool = fastrp_pool(control)?;
    let node_q = usize_to_f64(nodes.len()).sqrt().max(1.0);
    let ranges = row_chunks(nodes.len(), control.compute_threads());
    let chunks = run_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut rows = Vec::with_capacity(end - start);
                for (node_index, node) in nodes.iter().enumerate().take(end).skip(start) {
                    rows.push(initial_projection_row(
                        node,
                        node_index,
                        strengths,
                        total_strength,
                        options,
                        control,
                        dimensions,
                        node_q,
                    )?);
                }
                Ok((start, rows))
            })
            .collect::<Result<Vec<_>, FastRpError>>()
    })?;
    Ok(merge_row_chunks(nodes.len(), chunks))
}

#[allow(clippy::too_many_arguments)]
fn initial_projection_row(
    node: &FastRpNode,
    node_index: usize,
    strengths: &[f64],
    total_strength: f64,
    options: &FastRpOptions,
    control: &EmbeddingControl<'_>,
    dimensions: u64,
    node_q: f64,
) -> Result<Vec<f64>, FastRpError> {
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
}

fn mix_features(
    current: &mut [Vec<f64>],
    nodes: &[FastRpNode],
    options: &FastRpOptions,
    control: &EmbeddingControl<'_>,
    dimensions: u64,
    path: FastRpExecutionPath,
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
    match path {
        FastRpExecutionPath::Serial => {
            for (row, node) in current.iter_mut().zip(nodes) {
                mix_feature_row(row, node, &projection, options);
            }
        }
        FastRpExecutionPath::Parallel { .. } => {
            let pool = fastrp_pool(control)?;
            let ranges = row_chunks(current.len(), control.compute_threads());
            let chunks = run_on_pool(pool, || {
                ranges
                    .par_iter()
                    .map(|&(start, end)| {
                        let mut rows = Vec::with_capacity(end - start);
                        for (row, node) in current.iter().zip(nodes).take(end).skip(start) {
                            let mut row = row.clone();
                            mix_feature_row(&mut row, node, &projection, options);
                            rows.push(row);
                        }
                        Ok((start, rows))
                    })
                    .collect::<Result<Vec<_>, FastRpError>>()
            })?;
            for (start, rows) in chunks {
                for (offset, row) in rows.into_iter().enumerate() {
                    current[start + offset] = row;
                }
            }
        }
    }
    Ok(())
}

fn mix_feature_row(
    row: &mut [f64],
    node: &FastRpNode,
    projection: &[Vec<f64>],
    options: &FastRpOptions,
) {
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

fn propagate(
    adjacency: &[Vec<(usize, f64)>],
    strengths: &[f64],
    current: &[Vec<f64>],
    dimensions: usize,
    dimensions_u64: u64,
    control: &EmbeddingControl<'_>,
    path: FastRpExecutionPath,
) -> Result<Vec<Vec<f64>>, FastRpError> {
    match path {
        FastRpExecutionPath::Serial => propagate_serial(
            adjacency,
            strengths,
            current,
            dimensions,
            dimensions_u64,
            control,
        ),
        FastRpExecutionPath::Parallel { .. } => propagate_parallel(
            adjacency,
            strengths,
            current,
            dimensions,
            dimensions_u64,
            control,
        ),
    }
}

fn propagate_serial(
    adjacency: &[Vec<(usize, f64)>],
    strengths: &[f64],
    current: &[Vec<f64>],
    dimensions: usize,
    dimensions_u64: u64,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<Vec<f64>>, FastRpError> {
    let mut next = Vec::with_capacity(adjacency.len());
    for (neighbors, &strength) in adjacency.iter().zip(strengths) {
        next.push(propagate_row(
            neighbors,
            strength,
            current,
            dimensions,
            dimensions_u64,
            control,
        )?);
    }
    Ok(next)
}

fn propagate_parallel(
    adjacency: &[Vec<(usize, f64)>],
    strengths: &[f64],
    current: &[Vec<f64>],
    dimensions: usize,
    dimensions_u64: u64,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<Vec<f64>>, FastRpError> {
    let pool = fastrp_pool(control)?;
    let ranges = row_chunks(adjacency.len(), control.compute_threads());
    let chunks = run_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut rows = Vec::with_capacity(end - start);
                for (source, neighbors) in adjacency.iter().enumerate().take(end).skip(start) {
                    rows.push(propagate_row(
                        neighbors,
                        strengths[source],
                        current,
                        dimensions,
                        dimensions_u64,
                        control,
                    )?);
                }
                Ok((start, rows))
            })
            .collect::<Result<Vec<_>, FastRpError>>()
    })?;
    Ok(merge_row_chunks(adjacency.len(), chunks))
}

fn propagate_row(
    neighbors: &[(usize, f64)],
    strength: f64,
    current: &[Vec<f64>],
    dimensions: usize,
    dimensions_u64: u64,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<f64>, FastRpError> {
    let adjacency_work = u64::try_from(neighbors.len())
        .ok()
        .and_then(|entries| entries.checked_mul(dimensions_u64))
        .ok_or(EmbeddingResourceError::Overflow)?;
    control.checkpoint(adjacency_work)?;
    let mut row = vec![0.0; dimensions];
    if strength == 0.0 {
        return Ok(row);
    }
    for &(target, weight) in neighbors {
        let probability = weight / strength;
        for (coordinate, value) in row.iter_mut().enumerate() {
            *value += probability * current[target][coordinate];
        }
    }
    Ok(row)
}

fn accumulate(
    accumulator: &mut [Vec<f64>],
    matrix: &[Vec<f64>],
    weight: f64,
    control: &EmbeddingControl<'_>,
    path: FastRpExecutionPath,
) -> Result<(), FastRpError> {
    match path {
        FastRpExecutionPath::Serial => accumulate_serial(accumulator, matrix, weight, control),
        FastRpExecutionPath::Parallel { .. } => {
            accumulate_parallel(accumulator, matrix, weight, control)
        }
    }
}

fn accumulate_serial(
    accumulator: &mut [Vec<f64>],
    matrix: &[Vec<f64>],
    weight: f64,
    control: &EmbeddingControl<'_>,
) -> Result<(), FastRpError> {
    for (output, row) in accumulator.iter_mut().zip(matrix) {
        let normalized = normalized_accumulation_row(row, control)?;
        for (coordinate, value) in normalized.into_iter().enumerate() {
            output[coordinate] += weight * value;
        }
    }
    Ok(())
}

fn accumulate_parallel(
    accumulator: &mut [Vec<f64>],
    matrix: &[Vec<f64>],
    weight: f64,
    control: &EmbeddingControl<'_>,
) -> Result<(), FastRpError> {
    let pool = fastrp_pool(control)?;
    let ranges = row_chunks(matrix.len(), control.compute_threads());
    let chunks = run_on_pool(pool, || {
        ranges
            .par_iter()
            .map(|&(start, end)| {
                let mut rows = Vec::with_capacity(end - start);
                for row in &matrix[start..end] {
                    rows.push(normalized_accumulation_row(row, control)?);
                }
                Ok((start, rows))
            })
            .collect::<Result<Vec<_>, FastRpError>>()
    })?;
    for (start, rows) in chunks {
        for (offset, row) in rows.into_iter().enumerate() {
            let output = &mut accumulator[start + offset];
            for (coordinate, value) in row.into_iter().enumerate() {
                output[coordinate] += weight * value;
            }
        }
    }
    Ok(())
}

fn normalized_accumulation_row(
    row: &[f64],
    control: &EmbeddingControl<'_>,
) -> Result<Vec<f64>, FastRpError> {
    control.checkpoint(to_u64(row.len())?)?;
    let mut normalized = row.to_vec();
    unit_l2(&mut normalized);
    Ok(normalized)
}

fn row_chunks(rows: usize, threads: usize) -> Vec<(usize, usize)> {
    if rows == 0 {
        return Vec::new();
    }
    let workers = threads.clamp(1, rows);
    let base = rows / workers;
    let rem = rows % workers;
    let mut ranges = Vec::with_capacity(workers);
    let mut start = 0;
    for index in 0..workers {
        let len = base + usize::from(index < rem);
        let end = start + len;
        if start < end {
            ranges.push((start, end));
        }
        start = end;
    }
    ranges
}

fn merge_row_chunks(rows: usize, chunks: Vec<(usize, Vec<Vec<f64>>)>) -> Vec<Vec<f64>> {
    let mut output = vec![Vec::new(); rows];
    for (start, chunk_rows) in chunks {
        for (offset, row) in chunk_rows.into_iter().enumerate() {
            output[start + offset] = row;
        }
    }
    output
}

fn fastrp_pool(control: &EmbeddingControl<'_>) -> Result<&crate::ComputePool, FastRpError> {
    control.compute_pool().ok_or_else(|| {
        FastRpError::Resource(EmbeddingResourceError::Algorithm(
            crate::algorithm_dispatch::AlgorithmError::Execution {
                message: "parallel FastRP requires an instance-owned compute pool".into(),
            },
        ))
    })
}

fn run_on_pool<R>(
    pool: &crate::ComputePool,
    op: impl FnOnce() -> Result<R, FastRpError> + Send,
) -> Result<R, FastRpError>
where
    R: Send,
{
    match catch_unwind(AssertUnwindSafe(|| pool.install(op))) {
        Ok(result) => result,
        Err(_) => Err(FastRpError::WorkerPanic),
    }
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
    use crate::compute_pool::ComputePool;
    use std::sync::Arc;
    use std::time::Instant;

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

    fn uuid_from_usize(value: usize) -> [u8; 16] {
        let mut uuid = [0_u8; 16];
        uuid[8..].copy_from_slice(
            &u64::try_from(value)
                .expect("test UUID ordinal fits in u64")
                .to_be_bytes(),
        );
        uuid
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

    fn parallel_options() -> FastRpOptions {
        FastRpOptions {
            dimensions: 16,
            iteration_weights: vec![0.5, 1.0, 0.25, 0.75],
            normalization_strength: 0.5,
            feature_weight: 0.35,
            feature_properties: vec!["x".into(), "y".into()],
            seed: 17,
        }
    }

    fn parallel_input(nodes: usize, degree: usize, features: bool) -> FastRpInput {
        let node_rows = (0..nodes)
            .map(|index| FastRpNode {
                uuid: uuid_from_usize(index),
                features: if features {
                    vec![
                        ((index * 17) % 23) as f64 / 23.0,
                        ((index * 29 + 3) % 31) as f64 / 31.0,
                    ]
                } else {
                    Vec::new()
                },
            })
            .collect::<Vec<_>>();
        let mut edges = Vec::with_capacity(nodes.saturating_mul(degree));
        let mut edge_ordinal = 0_usize;
        for source in 0..nodes {
            for hop in 1..=degree {
                let target = (source + hop) % nodes;
                edges.push(FastRpEdge {
                    uuid: uuid_from_usize(nodes + edge_ordinal),
                    source: uuid_from_usize(source),
                    target: uuid_from_usize(target),
                    weight: 0.5 + ((source + hop) % 11) as f64 / 7.0,
                });
                edge_ordinal += 1;
            }
        }
        FastRpInput {
            directed: true,
            nodes: node_rows,
            edges,
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

    fn control_with_threads(threads: usize) -> AlgorithmControl {
        let pool = Arc::new(ComputePool::new(threads).unwrap());
        AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(threads),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool)
    }

    fn embedding_control<'a>(algorithm: &'a AlgorithmControl) -> EmbeddingControl<'a> {
        EmbeddingControl::new(algorithm, EmbeddingResourceLimits::default())
    }

    fn run_with_threads(
        input: FastRpInput,
        options: &FastRpOptions,
        threads: usize,
    ) -> Result<Vec<FastRpEmbeddingRow>, FastRpError> {
        let algorithm = control_with_threads(threads);
        run_fastrp(input, options, &embedding_control(&algorithm))
    }

    fn fingerprint(rows: &[FastRpEmbeddingRow]) -> Vec<([u8; 16], Vec<u32>)> {
        rows.iter()
            .map(|row| {
                (
                    row.node_uuid,
                    row.embedding.iter().map(|value| value.to_bits()).collect(),
                )
            })
            .collect()
    }

    fn peak_rss_kib() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        status.lines().find_map(|line| {
            let value = line.strip_prefix("VmHWM:")?.trim();
            value.split_whitespace().next()?.parse().ok()
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

    #[test]
    fn small_work_and_one_thread_select_serial_fastrp_path() {
        let small_algorithm = control_with_threads(4);
        let small = embedding_control(&small_algorithm);
        assert_eq!(
            select_fastrp_path(&small, 64, FASTRP_PARALLEL_CROSSOVER_OPS - 1),
            FastRpExecutionPath::Serial
        );

        let one_algorithm = control_with_threads(1);
        let one = embedding_control(&one_algorithm);
        assert_eq!(
            select_fastrp_path(&one, 64, FASTRP_PARALLEL_CROSSOVER_OPS),
            FastRpExecutionPath::Serial
        );

        let large_algorithm = control_with_threads(4);
        let large = embedding_control(&large_algorithm);
        assert_eq!(
            select_fastrp_path(&large, 64, FASTRP_PARALLEL_CROSSOVER_OPS),
            FastRpExecutionPath::Parallel {
                threads: 4,
                chunks: 4
            }
        );
    }

    #[test]
    fn row_chunks_cover_canonical_ranges() {
        assert_eq!(row_chunks(0, 4), Vec::<(usize, usize)>::new());
        assert_eq!(row_chunks(5, 1), vec![(0, 5)]);
        assert_eq!(row_chunks(5, 2), vec![(0, 3), (3, 5)]);
        assert_eq!(row_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
        assert_eq!(row_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
    }

    #[test]
    fn thread_matrix_preserves_parallel_fastrp_fingerprints() {
        let input = parallel_input(96, 24, true);
        let options = parallel_options();
        let estimated = estimated_fastrp_ops(
            input.nodes.len(),
            input.edges.len(),
            options.dimensions,
            options.iteration_weights.len(),
            options.feature_properties.len(),
        );
        assert!(estimated >= FASTRP_PARALLEL_CROSSOVER_OPS);

        let serial = run_with_threads(input.clone(), &options, 1).unwrap();
        let serial_fingerprint = fingerprint(&serial);
        for threads in [2_usize, 4, 8] {
            let algorithm = control_with_threads(threads);
            let control = embedding_control(&algorithm);
            assert!(matches!(
                select_fastrp_path(&control, input.nodes.len(), estimated),
                FastRpExecutionPath::Parallel { .. }
            ));
            let parallel = run_fastrp(input.clone(), &options, &control).unwrap();
            assert_eq!(parallel, serial);
            assert_eq!(fingerprint(&parallel), serial_fingerprint);
        }
    }

    #[test]
    fn parallel_cancellation_work_limits_and_worker_panic_are_structured() {
        let input = parallel_input(96, 24, false);
        let mut options = parallel_options();
        options.feature_properties.clear();
        options.feature_weight = 0.0;

        let pool = Arc::new(ComputePool::new(4).unwrap());
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            cancellation,
        )
        .with_compute_pool(pool.clone());
        assert!(matches!(
            run_fastrp(input.clone(), &options, &embedding_control(&cancelled)),
            Err(FastRpError::Resource(EmbeddingResourceError::Algorithm(_)))
        ));

        let limited = AlgorithmControl::new(
            AlgorithmLimits::default().with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(pool);
        let control = EmbeddingControl::new(
            &limited,
            EmbeddingResourceLimits {
                work: 3,
                ..EmbeddingResourceLimits::default()
            },
        );
        assert!(matches!(
            run_fastrp(input, &options, &control),
            Err(FastRpError::Resource(
                EmbeddingResourceError::WorkLimit { .. }
            ))
        ));

        let pool = ComputePool::new(2).unwrap();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let worker_result = run_on_pool(&pool, || -> Result<(), FastRpError> {
            panic!("test FastRP worker panic");
        });
        std::panic::set_hook(previous_hook);
        assert_eq!(worker_result, Err(FastRpError::WorkerPanic));
    }

    #[test]
    #[ignore = "manual crossover measurement; run with --ignored --nocapture"]
    fn measure_fastrp_parallel_crossover() {
        for (nodes, degree, dimensions) in [(64, 16, 8), (96, 24, 8), (96, 24, 16), (160, 32, 16)] {
            let input = parallel_input(nodes, degree, true);
            let mut options = parallel_options();
            options.dimensions = dimensions;
            let estimated = estimated_fastrp_ops(
                input.nodes.len(),
                input.edges.len(),
                options.dimensions,
                options.iteration_weights.len(),
                options.feature_properties.len(),
            );

            let serial_start = Instant::now();
            let serial = run_with_threads(input.clone(), &options, 1).unwrap();
            let serial_elapsed = serial_start.elapsed();

            let parallel_algorithm = control_with_threads(4);
            let parallel_control = embedding_control(&parallel_algorithm);
            let path = select_fastrp_path(&parallel_control, input.nodes.len(), estimated);
            let parallel_start = Instant::now();
            let parallel = run_fastrp(input.clone(), &options, &parallel_control).unwrap();
            let parallel_elapsed = parallel_start.elapsed();
            assert_eq!(fingerprint(&parallel), fingerprint(&serial));

            println!(
                "fastrp_measure nodes={nodes} edges={} dimensions={dimensions} ops={estimated} path={path:?} threads=4 serial_ms={} parallel_ms={} peak_rss_kib={:?} fingerprint_head={:?}",
                input.edges.len(),
                serial_elapsed.as_millis(),
                parallel_elapsed.as_millis(),
                peak_rss_kib(),
                fingerprint(&serial).first()
            );
        }
    }
}
