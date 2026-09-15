//! Embedding adapters and projection.

use super::{
    AdjacencyGraph, AdjacencyProvider, AdjacencySelection, AlgorithmCancellation, AlgorithmControl,
    AlgorithmLimits, AnalyzeAlgorithm, Array, BTreeMap, Digest, Direction, EmbeddingAnalyzeOptions,
    EmbeddingControl, EmbeddingExecution, EmbeddingInvocationDescriptor, EmbeddingInvocationLimits,
    EmbeddingOptions, EmbeddingProjectionSelector, EmbeddingResourceEstimate,
    EmbeddingResourceLimits, EmbeddingRngContract, EntityTypeSelection, FastRpResources,
    FixedSizeBinaryArray, GfError, GraphSageEdge, GraphSageNode, GraphSageProjection,
    HashGnnResources, HashGnnTypeTokens, HashSet, Int64Array, IrLiteral, Node2VecResources,
    NormalizedEmbeddingOptions, OntologyMode, Path, RNG_DERIVATION, RNG_VERSION, RecordBatch,
    SCHEMA_VERSION, Sha256, StringArray, TopologyResources, export_adjacency, hashgnn_embeddings,
    load_node_feature_properties, load_node_scalar_features, normalize_embedding_options,
    preflight_graphsage_dispatch, shape_embedding_output, train_fastrp, train_graphsage,
    train_node2vec, validate_graphsage_projection,
};

/// Execute one typed embedding analysis through its Rust-owned kernel.
///
/// # Errors
/// Returns structured validation, projection, resource, kernel, or Arrow-shaping
/// failures. Embedding values without an activated native kernel remain
/// unavailable.
pub fn embedding_algorithm(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    invocation: &EmbeddingAnalyzeOptions,
) -> Result<RecordBatch, GfError> {
    embedding_algorithm_execution(provider, dir, mode, label, None, invocation)
        .map(|execution| execution.result)
}

/// Execute an activated embedding and return its neutral deterministic invocation descriptor.
///
/// `label_name` records the normalized public selector corresponding to the
/// already-resolved `label` ID. It does not participate in graph resolution.
pub fn embedding_algorithm_execution(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    label_name: Option<&str>,
    invocation: &EmbeddingAnalyzeOptions,
) -> Result<EmbeddingExecution, GfError> {
    embedding_algorithm_execution_with_compute(
        provider,
        dir,
        mode,
        label,
        label_name,
        invocation,
        AlgorithmLimits::default(),
        None,
    )
}

/// Execute an embedding with shaping/compute limits and an optional private pool (#344).
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors embedding_algorithm_execution plus instance compute handles"
)]
pub fn embedding_algorithm_execution_with_compute(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    label_name: Option<&str>,
    invocation: &EmbeddingAnalyzeOptions,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<EmbeddingExecution, GfError> {
    let prepared =
        prepare_embedding_projection(provider, dir, mode, label, invocation, limits, compute)?;
    let invocation = &prepared.invocation;
    embedding_algorithm_execution_with_controls(
        &prepared.graph,
        invocation,
        EmbeddingProjectionSelector {
            label: label_name.map(str::to_owned),
            via: invocation.via.clone(),
            directed: invocation.directed,
            weight: invocation.weight.clone(),
        },
        &prepared.algorithm_control,
        prepared.resource_limits,
        prepared.hashgnn_type_tokens.as_ref(),
    )
}

struct PreparedEmbeddingProjection {
    invocation: NormalizedEmbeddingOptions,
    graph: AdjacencyGraph,
    hashgnn_type_tokens: Option<HashGnnTypeTokens>,
    algorithm_control: AlgorithmControl,
    resource_limits: EmbeddingResourceLimits,
}

fn prepare_embedding_projection(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    invocation: &EmbeddingAnalyzeOptions,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<PreparedEmbeddingProjection, GfError> {
    let invocation = normalize_embedding_options(invocation)?;
    let mut graph = export_adjacency(
        provider,
        dir,
        mode,
        AdjacencySelection {
            label,
            via: invocation.via.as_deref().unwrap_or("*"),
            direction: if invocation.directed {
                Direction::Out
            } else {
                Direction::Undirected
            },
            weight: invocation.weight.as_deref(),
        },
    )?;
    if let EmbeddingOptions::FastRandomProjection(options) = &invocation.options {
        load_node_scalar_features(&mut graph, dir, &options.feature_properties)?;
    }
    if let EmbeddingOptions::GraphSage(options) = &invocation.options {
        load_node_feature_properties(&mut graph, dir, &options.feature_properties)?;
    }
    let hashgnn_type_tokens = match &invocation.options {
        EmbeddingOptions::HashGnn(options) if options.heterogeneous => {
            Some(load_hashgnn_type_tokens(
                &graph,
                dir,
                options
                    .node_type_property
                    .as_deref()
                    .expect("normalized heterogeneous HashGNN has a node type property"),
                options
                    .relationship_type_property
                    .as_deref()
                    .expect("normalized heterogeneous HashGNN has a relationship type property"),
            )?)
        }
        _ => None,
    };
    let mut algorithm_control = AlgorithmControl::new(limits, AlgorithmCancellation::default());
    if let Some(pool) = compute {
        algorithm_control = algorithm_control.with_compute_pool(pool);
    }
    let resource_limits = EmbeddingResourceLimits::default();
    if let EmbeddingOptions::HashGnn(options) = &invocation.options {
        let topology = TopologyResources {
            nodes: usize_to_u64(graph.node_ids().len())?,
            adjacency_entries: graph.edge_entry_count(),
            bytes_per_node: 16,
            bytes_per_adjacency_entry: 32,
        };
        preflight_hashgnn(
            options,
            topology,
            hashgnn_type_tokens
                .as_ref()
                .map(hashgnn_type_token_bytes)
                .transpose()?
                .unwrap_or(0),
            &EmbeddingControl::new(&algorithm_control, resource_limits),
        )?;
    }
    Ok(PreparedEmbeddingProjection {
        invocation,
        graph,
        hashgnn_type_tokens,
        algorithm_control,
        resource_limits,
    })
}

/// Prepare the complete neutral embedding descriptor without running a kernel.
///
/// # Errors
/// Returns the same normalization, projection, property, and resource failures
/// that would occur before [`embedding_algorithm_execution`] starts a kernel.
pub fn prepare_embedding_invocation_descriptor(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    label_name: Option<&str>,
    invocation: &EmbeddingAnalyzeOptions,
) -> Result<EmbeddingInvocationDescriptor, GfError> {
    prepare_embedding_invocation_descriptor_with_compute(
        provider,
        dir,
        mode,
        label,
        label_name,
        invocation,
        AlgorithmLimits::default(),
        None,
    )
}

/// Prepare an embedding descriptor with the instance compute budget recorded (#344).
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors prepare_embedding_invocation_descriptor plus compute handles"
)]
pub fn prepare_embedding_invocation_descriptor_with_compute(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: EntityTypeSelection,
    label_name: Option<&str>,
    invocation: &EmbeddingAnalyzeOptions,
    limits: AlgorithmLimits,
    compute: Option<crate::SharedComputePool>,
) -> Result<EmbeddingInvocationDescriptor, GfError> {
    let prepared =
        prepare_embedding_projection(provider, dir, mode, label, invocation, limits, compute)?;
    let invocation = &prepared.invocation;
    let limits = prepared.algorithm_control.configured_limits();
    Ok(EmbeddingInvocationDescriptor {
        catalog_value: match &invocation.options {
            EmbeddingOptions::Node2Vec(_) => "node2vec",
            EmbeddingOptions::GraphSage(_) => "graphsage",
            EmbeddingOptions::FastRandomProjection(_) => "fast_random_projection",
            EmbeddingOptions::HashGnn(_) => "hashgnn",
        },
        algorithm_version: invocation.algorithm_version,
        selector: EmbeddingProjectionSelector {
            label: label_name.map(str::to_owned),
            via: invocation.via.clone(),
            directed: invocation.directed,
            weight: invocation.weight.clone(),
        },
        options: invocation.options.clone(),
        rng: EmbeddingRngContract {
            version: RNG_VERSION,
            derivation: RNG_DERIVATION,
            seed: invocation.seed(),
        },
        limits: EmbeddingInvocationLimits {
            nodes: limits.nodes,
            adjacency_entries: limits.edges,
            output_rows: limits.output_rows,
            iterations: limits.iterations,
            states: limits.states,
            memory_bytes: prepared.resource_limits.memory_bytes,
            work: prepared.resource_limits.work,
        },
        projection_fingerprint: embedding_descriptor_projection_fingerprint(
            &prepared.graph,
            prepared.hashgnn_type_tokens.as_ref(),
        )?,
        result_schema_version: SCHEMA_VERSION,
    })
}

fn embedding_descriptor_projection_fingerprint(
    graph: &AdjacencyGraph,
    type_tokens: Option<&HashGnnTypeTokens>,
) -> Result<[u8; 32], GfError> {
    let mut digest = Sha256::new();
    digest.update(b"graphforge_embedding_descriptor_projection_v1");
    digest.update(graph.descriptor_projection_fingerprint()?.as_bytes());
    if let Some(tokens) = type_tokens {
        digest.update(b"nodes");
        for (uuid, token) in &tokens.nodes {
            digest.update(uuid);
            digest.update(
                u64::try_from(token.len())
                    .map_err(|_| GfError::Execution("HashGNN token is too long".into()))?
                    .to_be_bytes(),
            );
            digest.update(token.as_bytes());
        }
        digest.update(b"relationships");
        for (uuid, token) in &tokens.relationships {
            digest.update(uuid);
            digest.update(
                u64::try_from(token.len())
                    .map_err(|_| GfError::Execution("HashGNN token is too long".into()))?
                    .to_be_bytes(),
            );
            digest.update(token.as_bytes());
        }
    }
    Ok(digest.finalize().into())
}

#[cfg(test)]
pub(crate) fn embedding_algorithm_with_controls(
    graph: &AdjacencyGraph,
    invocation: &NormalizedEmbeddingOptions,
    algorithm_control: &AlgorithmControl,
    resource_limits: EmbeddingResourceLimits,
) -> Result<RecordBatch, GfError> {
    embedding_algorithm_execution_with_controls(
        graph,
        invocation,
        EmbeddingProjectionSelector {
            label: None,
            via: invocation.via.clone(),
            directed: invocation.directed,
            weight: invocation.weight.clone(),
        },
        algorithm_control,
        resource_limits,
        None,
    )
    .map(|execution| execution.result)
}

fn embedding_algorithm_execution_with_controls(
    graph: &AdjacencyGraph,
    invocation: &NormalizedEmbeddingOptions,
    selector: EmbeddingProjectionSelector,
    algorithm_control: &AlgorithmControl,
    resource_limits: EmbeddingResourceLimits,
    hashgnn_type_tokens: Option<&HashGnnTypeTokens>,
) -> Result<EmbeddingExecution, GfError> {
    algorithm_control
        .check_graph_size(graph.node_ids().len(), graph.edge_entry_count())
        .map_err(GfError::from)?;
    algorithm_control
        .check_output_rows(graph.node_ids().len())
        .map_err(GfError::from)?;
    let control = EmbeddingControl::new(algorithm_control, resource_limits);
    let topology = TopologyResources {
        nodes: usize_to_u64(graph.node_ids().len())?,
        adjacency_entries: graph.edge_entry_count(),
        bytes_per_node: 16,
        bytes_per_adjacency_entry: 32,
    };
    let (algorithm, catalog_value, rows) = match &invocation.options {
        EmbeddingOptions::Node2Vec(options) => {
            let estimate = EmbeddingResourceEstimate::node2vec(Node2VecResources {
                topology,
                dimensions: usize_to_u64(options.dimensions)?,
                walks_per_node: usize_to_u64(options.walks_per_node)?,
                walk_length: usize_to_u64(options.walk_length)?,
                window_size: usize_to_u64(options.window_size)?,
                negative_samples: usize_to_u64(options.negative_samples)?,
                epochs: usize_to_u64(options.epochs)?,
                scratch_bytes: 0,
            })
            .map_err(|error| GfError::Execution(error.to_string()))?;
            control
                .preflight(estimate)
                .map_err(|error| GfError::Execution(error.to_string()))?;
            let rows = train_node2vec(graph, options, &control)
                .map_err(|error| GfError::Execution(error.to_string()))?;
            (AnalyzeAlgorithm::Node2Vec, "node2vec", rows)
        }
        EmbeddingOptions::FastRandomProjection(options) => {
            let estimate = EmbeddingResourceEstimate::fastrp(FastRpResources {
                topology,
                dimensions: usize_to_u64(options.dimensions)?,
                iteration_weights: usize_to_u64(options.iteration_weights.len())?,
                properties: usize_to_u64(options.feature_properties.len())?,
                scratch_bytes: 0,
            })
            .map_err(|error| GfError::Execution(error.to_string()))?;
            control
                .preflight(estimate)
                .map_err(|error| GfError::Execution(error.to_string()))?;
            let rows = train_fastrp(graph, options, &control)
                .map_err(|error| GfError::Execution(error.to_string()))?;
            (
                AnalyzeAlgorithm::FastRandomProjection,
                "fast_random_projection",
                rows,
            )
        }
        EmbeddingOptions::GraphSage(options) => {
            let rows = execute_graphsage(graph, options, topology, &control)?;
            (AnalyzeAlgorithm::GraphSage, "graphsage", rows)
        }
        EmbeddingOptions::HashGnn(options) => {
            let rows = execute_hashgnn(graph, options, topology, hashgnn_type_tokens, &control)?;
            (AnalyzeAlgorithm::HashGnn, "hashgnn", rows)
        }
    };
    let result = shape_embedding_output(algorithm, invocation, &rows, &control)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    let limits = algorithm_control.configured_limits();
    Ok(EmbeddingExecution {
        descriptor: EmbeddingInvocationDescriptor {
            catalog_value,
            algorithm_version: invocation.algorithm_version,
            selector,
            options: invocation.options.clone(),
            rng: EmbeddingRngContract {
                version: RNG_VERSION,
                derivation: RNG_DERIVATION,
                seed: invocation.seed(),
            },
            limits: EmbeddingInvocationLimits {
                nodes: limits.nodes,
                adjacency_entries: limits.edges,
                output_rows: limits.output_rows,
                iterations: limits.iterations,
                states: limits.states,
                memory_bytes: resource_limits.memory_bytes,
                work: resource_limits.work,
            },
            projection_fingerprint: embedding_descriptor_projection_fingerprint(
                graph,
                hashgnn_type_tokens,
            )?,
            result_schema_version: SCHEMA_VERSION,
        },
        result,
    })
}

fn usize_to_u64(value: usize) -> Result<u64, GfError> {
    u64::try_from(value).map_err(|_| {
        GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
    })
}

fn execute_hashgnn(
    graph: &AdjacencyGraph,
    options: &graphforge_core::embedding_options::HashGnnOptions,
    topology: TopologyResources,
    type_tokens: Option<&HashGnnTypeTokens>,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<crate::algorithm_embedding_output::EmbeddingOutputRow>, GfError> {
    let scratch_bytes = type_tokens
        .map(hashgnn_type_token_bytes)
        .transpose()?
        .unwrap_or(0);
    preflight_hashgnn(options, topology, scratch_bytes, control)?;
    hashgnn_embeddings(graph, options, type_tokens, control)
        .map_err(|error| GfError::Execution(error.to_string()))
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "normalized HashGNN dimensions are at most 8192 and density is finite in (0, 1]"
)]
fn preflight_hashgnn(
    options: &graphforge_core::embedding_options::HashGnnOptions,
    topology: TopologyResources,
    scratch_bytes: u64,
    control: &EmbeddingControl<'_>,
) -> Result<(), GfError> {
    let dimensions = usize_to_u64(options.dimensions)?;
    let active_bits = (options.embedding_density * options.dimensions as f64)
        .ceil()
        .max(1.0) as u64;
    let estimate = EmbeddingResourceEstimate::hashgnn(HashGnnResources {
        topology,
        dimensions,
        iterations: usize_to_u64(options.iterations)?,
        active_bits,
        scratch_bytes,
    })
    .map_err(|error| GfError::Execution(error.to_string()))?;
    control
        .preflight(estimate)
        .map_err(|error| GfError::Execution(error.to_string()))
}

fn hashgnn_type_token_bytes(tokens: &HashGnnTypeTokens) -> Result<u64, GfError> {
    tokens
        .nodes
        .values()
        .chain(tokens.relationships.values())
        .try_fold(0_u64, |total, token| {
            let token_bytes = usize_to_u64(token.len())?;
            total
                .checked_add(16)
                .and_then(|value| value.checked_add(token_bytes))
                .ok_or_else(|| {
                    GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
                })
        })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HashGnnTypeKind {
    String,
    Integer,
}

fn load_hashgnn_type_tokens(
    graph: &AdjacencyGraph,
    dir: &Path,
    node_property: &str,
    relationship_property: &str,
) -> Result<HashGnnTypeTokens, GfError> {
    let nodes = load_hashgnn_node_types(graph, dir, node_property)?;
    let relationships = load_hashgnn_relationship_types(graph, dir, relationship_property)?;
    Ok(HashGnnTypeTokens {
        nodes,
        relationships,
    })
}

fn load_hashgnn_node_types(
    graph: &AdjacencyGraph,
    dir: &Path,
    property: &str,
) -> Result<BTreeMap<[u8; 16], String>, GfError> {
    let selected = graph.node_uuids().collect::<HashSet<_>>();
    let mut values = BTreeMap::new();
    let mut kind = None;
    for stem in graph.property_routes(dir, false) {
        for (uuid, row) in graph
            .node_property_rows(dir, &stem)
            .map_err(|error| GfError::Storage(error.to_string()))?
        {
            if !selected.contains(&uuid) {
                continue;
            }
            let Some(value) = row.get(property) else {
                continue;
            };
            let (value_kind, token) = hashgnn_node_type_token(property, &uuid, value)?;
            validate_hashgnn_type_kind("node", property, &mut kind, value_kind)?;
            insert_hashgnn_type_value(&mut values, uuid, token, "node", property)?;
        }
    }
    for uuid in selected {
        if !values.contains_key(&uuid) {
            return Err(GfError::Validation(format!(
                "node {uuid:?} is missing HashGNN type property {property:?}"
            )));
        }
    }
    Ok(values)
}

fn hashgnn_node_type_token(
    property: &str,
    uuid: &[u8; 16],
    value: &IrLiteral,
) -> Result<(HashGnnTypeKind, String), GfError> {
    match value {
        IrLiteral::Str(value) => Ok((
            HashGnnTypeKind::String,
            format!("string:{}:{value}", value.len()),
        )),
        IrLiteral::Int(value) => Ok((HashGnnTypeKind::Integer, format!("integer:{value}"))),
        _ => Err(GfError::Validation(format!(
            "node {uuid:?} HashGNN type property {property:?} must be a non-null scalar string or integer"
        ))),
    }
}

fn load_hashgnn_relationship_types(
    graph: &AdjacencyGraph,
    dir: &Path,
    property: &str,
) -> Result<BTreeMap<[u8; 16], String>, GfError> {
    let selected = graph
        .node_ids()
        .iter()
        .flat_map(|&node_id| graph.neighbors(node_id))
        .map(|edge| edge.edge_uuid)
        .collect::<HashSet<_>>();
    let mut values = BTreeMap::new();
    let mut kind = None;
    for stem in graph.property_routes(dir, true) {
        for batch in graph
            .edge_property_batches(dir, &stem)
            .map_err(|error| GfError::Storage(error.to_string()))?
        {
            let Some(uuids) = batch
                .column_by_name("edge_uuid")
                .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
            else {
                return Err(GfError::Execution(
                    "HashGNN edge property batch is missing edge_uuid identity".into(),
                ));
            };
            let Some(column) = batch.column_by_name(property) else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if uuids.is_null(row) || column.is_null(row) {
                    continue;
                }
                let uuid: [u8; 16] = uuids.value(row).try_into().map_err(|_| {
                    GfError::Execution(
                        "HashGNN edge property UUID does not contain 16 bytes".into(),
                    )
                })?;
                if !selected.contains(&uuid) {
                    continue;
                }
                let (value_kind, token) =
                    hashgnn_edge_type_token(property, &uuid, column.as_ref(), row)?;
                validate_hashgnn_type_kind("relationship", property, &mut kind, value_kind)?;
                insert_hashgnn_type_value(&mut values, uuid, token, "relationship", property)?;
            }
        }
    }
    for uuid in selected {
        if !values.contains_key(&uuid) {
            return Err(GfError::Validation(format!(
                "relationship {uuid:?} is missing HashGNN type property {property:?}"
            )));
        }
    }
    Ok(values)
}

fn hashgnn_edge_type_token(
    property: &str,
    uuid: &[u8; 16],
    array: &dyn Array,
    row: usize,
) -> Result<(HashGnnTypeKind, String), GfError> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        let value = values.value(row);
        Ok((
            HashGnnTypeKind::String,
            format!("string:{}:{value}", value.len()),
        ))
    } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        Ok((
            HashGnnTypeKind::Integer,
            format!("integer:{}", values.value(row)),
        ))
    } else {
        Err(GfError::Validation(format!(
            "relationship {uuid:?} HashGNN type property {property:?} must be a non-null scalar string or integer"
        )))
    }
}

fn validate_hashgnn_type_kind(
    entity: &str,
    property: &str,
    expected: &mut Option<HashGnnTypeKind>,
    actual: HashGnnTypeKind,
) -> Result<(), GfError> {
    if expected.is_some_and(|value| value != actual) {
        return Err(GfError::Validation(format!(
            "{entity} HashGNN type property {property:?} mixes string and integer values"
        )));
    }
    *expected = Some(actual);
    Ok(())
}

fn insert_hashgnn_type_value(
    values: &mut BTreeMap<[u8; 16], String>,
    uuid: [u8; 16],
    token: String,
    entity: &str,
    property: &str,
) -> Result<(), GfError> {
    match values.entry(uuid) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(token);
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &token => {
            return Err(GfError::Validation(format!(
                "{entity} {uuid:?} has conflicting HashGNN type property {property:?}"
            )));
        }
        std::collections::btree_map::Entry::Occupied(_) => {}
    }
    Ok(())
}

fn execute_graphsage(
    graph: &AdjacencyGraph,
    options: &graphforge_core::embedding_options::GraphSageOptions,
    topology: TopologyResources,
    control: &EmbeddingControl<'_>,
) -> Result<Vec<crate::algorithm_embedding_output::EmbeddingOutputRow>, GfError> {
    if graph.is_empty() {
        return Ok(Vec::new());
    }
    let (feature_width, retained_source_bytes) = graphsage_source_resources(graph)?;
    preflight_graphsage_dispatch(
        topology.nodes,
        topology.adjacency_entries,
        feature_width,
        retained_source_bytes,
        options,
        control,
    )
    .map_err(|error| GfError::Execution(error.to_string()))?;
    let projection = graphsage_projection(graph)?;
    train_graphsage(&projection, options, control)
        .map_err(|error| GfError::Execution(error.to_string()))
}

pub(super) fn graphsage_projection(graph: &AdjacencyGraph) -> Result<GraphSageProjection, GfError> {
    let nodes = graph
        .node_ids()
        .iter()
        .map(|&node_id| {
            let uuid = graph.node_uuid(node_id).ok_or_else(|| {
                GfError::Execution("graphsage selected node has no UUID identity".into())
            })?;
            let features = graph.node_vector(node_id).ok_or_else(|| {
                GfError::Validation(format!(
                    "graphsage selected node {uuid:?} has no resolved feature vector"
                ))
            })?;
            Ok(GraphSageNode {
                uuid,
                features: features.to_vec(),
            })
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    let mut seen_edges = HashSet::new();
    let mut edges = Vec::new();
    for &source_id in graph.node_ids() {
        let source_uuid = graph.node_uuid(source_id).ok_or_else(|| {
            GfError::Execution("graphsage selected node has no UUID identity".into())
        })?;
        for edge in graph.neighbors(source_id) {
            if !seen_edges.insert(edge.edge_uuid) {
                continue;
            }
            let target_uuid = graph.node_uuid(edge.neighbor_id).ok_or_else(|| {
                GfError::Execution("graphsage selected neighbor has no UUID identity".into())
            })?;
            edges.push(GraphSageEdge {
                uuid: edge.edge_uuid,
                source_uuid,
                target_uuid,
            });
        }
    }
    validate_graphsage_projection(nodes, edges)
        .map_err(|error| GfError::Execution(error.to_string()))
}

fn graphsage_source_resources(graph: &AdjacencyGraph) -> Result<(u64, u64), GfError> {
    let first_id = *graph
        .node_ids()
        .first()
        .ok_or_else(|| GfError::Execution("graphsage source projection is empty".into()))?;
    let first = graph.node_vector(first_id).ok_or_else(|| {
        GfError::Validation("graphsage selected node has no resolved feature vector".into())
    })?;
    if first.is_empty() {
        return Err(GfError::Validation(
            "graphsage requires a non-empty numeric feature vector".into(),
        ));
    }
    for &node_id in graph.node_ids() {
        let vector = graph.node_vector(node_id).ok_or_else(|| {
            GfError::Validation("graphsage selected node has no resolved feature vector".into())
        })?;
        if vector.len() != first.len() {
            return Err(GfError::Validation(
                "graphsage feature vectors have inconsistent shape".into(),
            ));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(GfError::Validation(
                "graphsage features must be finite".into(),
            ));
        }
    }
    let nodes = usize_to_u64(graph.node_ids().len())?;
    let width = usize_to_u64(first.len())?;
    let topology_bytes = nodes
        .checked_mul(16)
        .and_then(|bytes| {
            graph
                .edge_entry_count()
                .checked_mul(32)
                .and_then(|adjacency| bytes.checked_add(adjacency))
        })
        .ok_or_else(|| {
            GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
        })?;
    let feature_bytes = nodes
        .checked_mul(width)
        .and_then(|cells| cells.checked_mul(8))
        .ok_or_else(|| {
            GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
        })?;
    let projection_staging_bytes = graph
        .edge_entry_count()
        .checked_mul(usize_to_u64(std::mem::size_of::<GraphSageEdge>())?)
        .ok_or_else(|| {
            GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
        })?;
    let retained_source_bytes = topology_bytes
        .checked_add(feature_bytes)
        .and_then(|bytes| bytes.checked_add(projection_staging_bytes))
        .ok_or_else(|| {
            GfError::Execution("embedding resource accounting exceeds UInt64 range".into())
        })?;
    Ok((width, retained_source_bytes))
}

#[cfg(test)]
mod tests;
