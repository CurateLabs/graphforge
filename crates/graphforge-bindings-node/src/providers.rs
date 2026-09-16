//! Providers bindings and native task ownership.

use crate::AlgorithmEmbeddingDistance;
use crate::AlgorithmEmbeddingNormalization;
use crate::AlgorithmEmbeddingPublicationRequest;
use crate::AnalyzeAlgorithm;
use crate::BTreeMap;
use crate::Buffer;
use crate::CallerEmbeddingBatchRequest;
use crate::CallerEmbeddingBatchRow;
use crate::CallerEmbeddingDistance;
use crate::CallerEmbeddingNormalization;
use crate::ClassInstance;
use crate::Duration;
use crate::Either3;
use crate::EmbeddingAnalyzeOptions;
use crate::EmbeddingOptions;
use crate::EmbeddingRefreshFailureClass;
use crate::EmbeddingRefreshInspection;
use crate::EmbeddingRefreshOutcomeStatus;
use crate::EmbeddingRefreshProjectPolicy;
use crate::EmbeddingRefreshSpacePolicy;
use crate::EmbeddingRefreshWorkerState;
use crate::EmbeddingSpaceFreshnessInspection;
use crate::EmbeddingSpaceFreshnessState;
use crate::EmbeddingSpaceInfo;
use crate::EmbeddingSpaceProducer;
use crate::EmbeddingSpaceReadDecision;
use crate::EmbeddingTokenCountClass;
use crate::Env;
use crate::FastRpOptions;
use crate::FindDiagnostic;
use crate::FindExecutionOptions;
use crate::FindOptions;
use crate::FindRerankOptions;
use crate::Function;
use crate::GfError;
use crate::GraphForge;
use crate::GraphSageAggregator;
use crate::GraphSageOptions;
use crate::HashGnnOptions;
use crate::HashMap;
use crate::JsObjectValue;
use crate::Node2VecOptions;
use crate::NodeError;
use crate::NodeHandle;
use crate::NodeSelector;
use crate::Object;
use crate::OpenRouterProviderSession;
use crate::OpenRouterProviderSessionConfig;
use crate::OpenRouterWireLimits;
use crate::ProviderBatchLimits;
use crate::ProviderCapabilities;
use crate::ProviderCapability;
use crate::ProviderEmbeddingDistance;
use crate::ProviderEmbeddingNormalization;
use crate::ProviderEmbeddingPlanInspection;
use crate::ProviderEmbeddingPlanRequest;
use crate::ProviderExecutionLimits;
use crate::ProviderRequestLimits;
use crate::RerankAdvisoryPolicy;
use crate::RerankFailurePolicy;
use crate::Result;
use crate::SearchIndexOptions;
use crate::TextIndexInspection;
use crate::TokenCountClass;
use crate::ipc_to_record_batch;
use crate::json_to_prop_value;
use crate::napi;
use crate::record_batch_to_ipc;
use crate::to_napi_err;

fn embedding_space_to_json(space: EmbeddingSpaceInfo) -> serde_json::Value {
    let producer = match space.producer {
        EmbeddingSpaceProducer::Algorithm {
            algorithm,
            algorithm_version,
        } => serde_json::json!({
            "kind": "algorithm",
            "algorithm": algorithm,
            "algorithmVersion": algorithm_version,
        }),
        EmbeddingSpaceProducer::Local {
            implementation,
            model,
            revision,
            contract_version,
        } => serde_json::json!({
            "kind": "local",
            "implementation": implementation,
            "model": model,
            "revision": revision,
            "contractVersion": contract_version,
        }),
        EmbeddingSpaceProducer::Callback {
            callback_contract,
            contract_version,
        } => serde_json::json!({
            "kind": "callback",
            "callbackContract": callback_contract,
            "contractVersion": contract_version,
        }),
        EmbeddingSpaceProducer::Remote {
            provider,
            model,
            revision,
            response_contract_version,
        } => serde_json::json!({
            "kind": "remote",
            "provider": provider,
            "model": model,
            "revision": revision,
            "responseContractVersion": response_contract_version,
        }),
        EmbeddingSpaceProducer::CallerSupplied { contract_version } => serde_json::json!({
            "kind": "callerSupplied",
            "contractVersion": contract_version,
        }),
    };
    let tokenizer = space.tokenizer.map(|tokenizer| {
        serde_json::json!({
            "identifier": tokenizer.identifier,
            "version": tokenizer.version,
            "countClass": match tokenizer.count_class {
                EmbeddingTokenCountClass::ExactLocal => "exactLocal",
                EmbeddingTokenCountClass::ProviderReported => "providerReported",
                EmbeddingTokenCountClass::Approximate => "approximate",
            },
            "maxInputTokens": tokenizer.max_input_tokens,
            "normalization": tokenizer.normalization,
        })
    });
    let chunking = space.chunking.map(|chunking| {
        serde_json::json!({
            "chunkSizeTokens": chunking.chunk_size_tokens,
            "overlapTokens": chunking.overlap_tokens,
            "aggregation": chunking.aggregation,
            "truncationPolicy": chunking.truncation_policy,
        })
    });
    let active = space.active.map(|active| {
        serde_json::json!({
            "generationId": active.generation_id,
            "vectorCount": active.vector_count,
            "sourceGraphGeneration": active.source_graph_generation,
            "sourceFingerprint": active.source_fingerprint,
            "generatedAtMicros": active.generated_at_micros,
            "committedAtMicros": active.committed_at_micros,
        })
    });
    serde_json::json!({
        "compatibilityId": space.compatibility_id,
        "aliases": space.aliases,
        "defaultAlias": space.default_alias,
        "dimensions": space.dimensions,
        "producer": producer,
        "tokenizer": tokenizer,
        "chunking": chunking,
        "active": active,
    })
}

fn refresh_project_policy_to_json(policy: EmbeddingRefreshProjectPolicy) -> serde_json::Value {
    serde_json::json!({
        "proactive": policy.proactive,
        "debounceMillis": policy.debounce.as_millis(),
        "maxConcurrentJobs": policy.max_concurrent_jobs,
    })
}

fn refresh_space_policy_to_json(policy: EmbeddingRefreshSpacePolicy) -> serde_json::Value {
    serde_json::json!({
        "proactive": policy.proactive,
        "debounceMillis": policy.debounce.map(|duration| duration.as_millis()),
    })
}

fn refresh_freshness_to_json(freshness: EmbeddingSpaceFreshnessInspection) -> serde_json::Value {
    let decision = match freshness.decision {
        EmbeddingSpaceReadDecision::ServeFresh => serde_json::json!({ "kind": "serve_fresh" }),
        EmbeddingSpaceReadDecision::ServeStale { reason } => {
            serde_json::json!({ "kind": "serve_stale", "reason": reason })
        }
        EmbeddingSpaceReadDecision::RefreshRequired { reason } => {
            serde_json::json!({ "kind": "refresh_required", "reason": reason })
        }
        EmbeddingSpaceReadDecision::ServeForcedStale { diagnostic } => {
            serde_json::json!({ "kind": "serve_forced_stale", "diagnostic": diagnostic })
        }
    };
    serde_json::json!({
        "compatibilityId": freshness.compatibility_id,
        "generationId": freshness.generation_id,
        "state": match freshness.state {
            EmbeddingSpaceFreshnessState::Fresh => "fresh",
            EmbeddingSpaceFreshnessState::Stale => "stale",
            EmbeddingSpaceFreshnessState::SubstantiallyStale => "substantially_stale",
        },
        "reason": freshness.reason,
        "decision": decision,
    })
}

fn refresh_failure_token(failure: EmbeddingRefreshFailureClass) -> &'static str {
    match failure {
        EmbeddingRefreshFailureClass::Provider => "provider",
        EmbeddingRefreshFailureClass::Validation => "validation",
        EmbeddingRefreshFailureClass::ResourceExhausted => "resource_exhausted",
        EmbeddingRefreshFailureClass::Storage => "storage",
        EmbeddingRefreshFailureClass::ConcurrentMutation => "concurrent_mutation",
        EmbeddingRefreshFailureClass::Incompatible => "incompatible",
        EmbeddingRefreshFailureClass::Corrupt => "corrupt",
        EmbeddingRefreshFailureClass::Unavailable => "unavailable",
    }
}

fn refresh_inspection_to_json(inspection: EmbeddingRefreshInspection) -> serde_json::Value {
    let last_outcome = inspection.last_outcome.map(|outcome| {
        let (status, failure_class) = match outcome.status {
            EmbeddingRefreshOutcomeStatus::Succeeded => ("succeeded", None),
            EmbeddingRefreshOutcomeStatus::Cancelled => ("cancelled", None),
            EmbeddingRefreshOutcomeStatus::Failed(failure) => {
                ("failed", Some(refresh_failure_token(failure)))
            }
        };
        let mut value = serde_json::json!({
            "status": status,
            "failureClass": failure_class,
            "graphGeneration": outcome.graph_generation,
            "sourceFingerprint": outcome.source_fingerprint.to_hex(),
            "completedAtMicros": outcome.completed_at_micros,
        });
        if failure_class.is_none() {
            value
                .as_object_mut()
                .expect("outcome object")
                .remove("failureClass");
        }
        value
    });
    serde_json::json!({
        "compatibilityId": inspection.compatibility_id,
        "projectPolicy": refresh_project_policy_to_json(inspection.project_policy),
        "spacePolicy": inspection.space_policy.map(refresh_space_policy_to_json),
        "resolvedPolicy": {
            "proactive": inspection.resolved_policy.proactive,
            "debounceMillis": inspection.resolved_policy.debounce.as_millis(),
            "maxConcurrentJobs": inspection.resolved_policy.max_concurrent_jobs,
        },
        "lastOutcome": last_outcome,
        "freshness": inspection.freshness.map(refresh_freshness_to_json),
        "worker": {
            "state": match inspection.worker.state {
                EmbeddingRefreshWorkerState::Running => "running",
                EmbeddingRefreshWorkerState::Shutdown => "shutdown",
            },
            "queuedLineages": inspection.worker.queued_lineages,
            "inFlightLineages": inspection.worker.in_flight_lineages,
            "selectedLineageQueued": inspection.worker.selected_lineage_queued,
            "selectedLineageInFlight": inspection.worker.selected_lineage_in_flight,
            "coalescedNotices": inspection.worker.coalesced_notices,
            "succeeded": inspection.worker.succeeded,
            "failed": inspection.worker.failed,
            "cancelled": inspection.worker.cancelled,
        },
    })
}

fn text_index_inspection_to_json(inspection: TextIndexInspection) -> serde_json::Value {
    serde_json::json!({
        "projectGenerationUuid": inspection.project_generation_uuid.to_string(),
        "properties": inspection.properties,
        "sourceGeneration": inspection.source_generation,
        "sourceFingerprint": inspection.source_fingerprint,
        "artifactGeneration": inspection.artifact_generation,
        "artifactSourceGeneration": inspection.artifact_source_generation,
        "artifactSourceFingerprint": inspection.artifact_source_fingerprint,
        "state": inspection.state.as_str(),
        "reason": inspection.reason.map(graphforge_api::TextIndexFreshnessReason::as_str),
    })
}

pub(super) fn adjacency_inspection_to_json(
    inspection: graphforge_api::AdjacencyInspection,
) -> serde_json::Value {
    serde_json::json!({
        "projectGenerationUuid": inspection.project_generation_uuid.to_string(),
        "sourceTopologyGeneration": inspection.source_topology_generation,
        "sourceTopologyFingerprint": inspection.source_topology_fingerprint,
        "artifactSourceGeneration": inspection.artifact_source_generation,
        "artifactEffectiveGeneration": inspection.artifact_effective_generation,
        "artifactFingerprint": inspection.artifact_fingerprint,
        "state": inspection.state.as_str(),
        "reason": inspection.reason.map(graphforge_api::AdjacencyFreshnessReason::as_str),
    })
}

pub(super) type EmbeddingInput = HashMap<String, serde_json::Value>;

pub(super) fn embedding_error(message: impl Into<String>) -> NodeError {
    to_napi_err(&GfError::Validation(message.into()))
}

fn embedding_usize(
    input: &mut EmbeddingInput,
    algorithm: &str,
    name: &str,
    default: usize,
) -> Result<usize> {
    let Some(value) = input.remove(name) else {
        return Ok(default);
    };
    let value = value.as_u64().ok_or_else(|| {
        embedding_error(format!("{algorithm} {name} must be nonnegative integer"))
    })?;
    usize::try_from(value)
        .map_err(|_| embedding_error(format!("{algorithm} {name} exceeds platform range")))
}

fn embedding_seed(input: &mut EmbeddingInput, algorithm: &str, default: u64) -> Result<u64> {
    let Some(value) = input.remove("seed") else {
        return Ok(default);
    };
    value
        .as_u64()
        .ok_or_else(|| embedding_error(format!("{algorithm} seed must fit unsigned 64-bit")))
}

fn embedding_f64(
    input: &mut EmbeddingInput,
    algorithm: &str,
    name: &str,
    default: f64,
) -> Result<f64> {
    let Some(value) = input.remove(name) else {
        return Ok(default);
    };
    value
        .as_f64()
        .ok_or_else(|| embedding_error(format!("{algorithm} {name} must be numeric")))
}

fn embedding_bool(
    input: &mut EmbeddingInput,
    algorithm: &str,
    name: &str,
    default: bool,
) -> Result<bool> {
    let Some(value) = input.remove(name) else {
        return Ok(default);
    };
    value
        .as_bool()
        .ok_or_else(|| embedding_error(format!("{algorithm} {name} must be boolean")))
}

fn embedding_strings(
    input: &mut EmbeddingInput,
    algorithm: &str,
    name: &str,
    default: Vec<String>,
) -> Result<Vec<String>> {
    let Some(value) = input.remove(name) else {
        return Ok(default);
    };
    let values = value.as_array().ok_or_else(|| {
        embedding_error(format!(
            "{algorithm} {name} must be an ordered list of property names"
        ))
    })?;
    values
        .iter()
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                embedding_error(format!(
                    "{algorithm} {name} must contain only property names"
                ))
            })
        })
        .collect()
}

fn embedding_usizes(
    input: &mut EmbeddingInput,
    algorithm: &str,
    name: &str,
    default: Vec<usize>,
) -> Result<Vec<usize>> {
    let Some(value) = input.remove(name) else {
        return Ok(default);
    };
    let values = value.as_array().ok_or_else(|| {
        embedding_error(format!(
            "{algorithm} {name} must be an ordered integer list"
        ))
    })?;
    values
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| {
                    embedding_error(format!(
                        "{algorithm} {name} must contain nonnegative integers"
                    ))
                })
        })
        .collect()
}

fn embedding_f64s(
    input: &mut EmbeddingInput,
    algorithm: &str,
    name: &str,
    default: Vec<f64>,
) -> Result<Vec<f64>> {
    let Some(value) = input.remove(name) else {
        return Ok(default);
    };
    let values = value
        .as_array()
        .ok_or_else(|| embedding_error(format!("{algorithm} {name} must be a numeric list")))?;
    values
        .iter()
        .map(|value| {
            value
                .as_f64()
                .ok_or_else(|| embedding_error(format!("{algorithm} {name} must be numeric")))
        })
        .collect()
}

fn embedding_optional_string(
    input: &mut EmbeddingInput,
    algorithm: &str,
    name: &str,
    default: Option<String>,
) -> Result<Option<String>> {
    let Some(value) = input.remove(name) else {
        return Ok(default);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|value| Some(value.to_owned()))
        .ok_or_else(|| {
            embedding_error(format!(
                "{algorithm} {name} must be a property name or null"
            ))
        })
}

fn finish_embedding_input(
    algorithm: &str,
    input: EmbeddingInput,
    options: EmbeddingOptions,
) -> Result<EmbeddingOptions> {
    if let Some(name) = input.keys().min() {
        return Err(embedding_error(format!(
            "unknown {algorithm} option {name:?}"
        )));
    }
    Ok(options)
}

fn node2vec_options(mut input: EmbeddingInput) -> Result<EmbeddingOptions> {
    let defaults = Node2VecOptions::default();
    let options = Node2VecOptions {
        dimensions: embedding_usize(&mut input, "node2vec", "dimensions", defaults.dimensions)?,
        walk_length: embedding_usize(&mut input, "node2vec", "walk_length", defaults.walk_length)?,
        walks_per_node: embedding_usize(
            &mut input,
            "node2vec",
            "walks_per_node",
            defaults.walks_per_node,
        )?,
        p: embedding_f64(&mut input, "node2vec", "p", defaults.p)?,
        q: embedding_f64(&mut input, "node2vec", "q", defaults.q)?,
        window_size: embedding_usize(&mut input, "node2vec", "window_size", defaults.window_size)?,
        negative_samples: embedding_usize(
            &mut input,
            "node2vec",
            "negative_samples",
            defaults.negative_samples,
        )?,
        epochs: embedding_usize(&mut input, "node2vec", "epochs", defaults.epochs)?,
        learning_rate: embedding_f64(
            &mut input,
            "node2vec",
            "learning_rate",
            defaults.learning_rate,
        )?,
        seed: embedding_seed(&mut input, "node2vec", defaults.seed)?,
    };
    finish_embedding_input("node2vec", input, EmbeddingOptions::Node2Vec(options))
}

fn graphsage_options(mut input: EmbeddingInput) -> Result<EmbeddingOptions> {
    let defaults = GraphSageOptions::default();
    let aggregator = input.remove("aggregator").map_or_else(
        || Ok("mean".to_owned()),
        |value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| embedding_error("graphsage aggregator must be \"mean\""))
        },
    )?;
    if aggregator != "mean" {
        return Err(embedding_error("graphsage aggregator must be \"mean\""));
    }
    let options = GraphSageOptions {
        dimensions: embedding_usize(&mut input, "graphsage", "dimensions", defaults.dimensions)?,
        hidden_dimensions: embedding_usize(
            &mut input,
            "graphsage",
            "hidden_dimensions",
            defaults.hidden_dimensions,
        )?,
        layers: embedding_usize(&mut input, "graphsage", "layers", defaults.layers)?,
        sample_sizes: embedding_usizes(
            &mut input,
            "graphsage",
            "sample_sizes",
            defaults.sample_sizes,
        )?,
        aggregator: GraphSageAggregator::Mean,
        epochs: embedding_usize(&mut input, "graphsage", "epochs", defaults.epochs)?,
        negative_samples: embedding_usize(
            &mut input,
            "graphsage",
            "negative_samples",
            defaults.negative_samples,
        )?,
        learning_rate: embedding_f64(
            &mut input,
            "graphsage",
            "learning_rate",
            defaults.learning_rate,
        )?,
        feature_properties: embedding_strings(
            &mut input,
            "graphsage",
            "feature_properties",
            defaults.feature_properties,
        )?,
        seed: embedding_seed(&mut input, "graphsage", defaults.seed)?,
    };
    finish_embedding_input("graphsage", input, EmbeddingOptions::GraphSage(options))
}

fn fastrp_options(mut input: EmbeddingInput) -> Result<EmbeddingOptions> {
    let defaults = FastRpOptions::default();
    let options = FastRpOptions {
        dimensions: embedding_usize(
            &mut input,
            "fast_random_projection",
            "dimensions",
            defaults.dimensions,
        )?,
        iteration_weights: embedding_f64s(
            &mut input,
            "fast_random_projection",
            "iteration_weights",
            defaults.iteration_weights,
        )?,
        normalization_strength: embedding_f64(
            &mut input,
            "fast_random_projection",
            "normalization_strength",
            defaults.normalization_strength,
        )?,
        feature_weight: embedding_f64(
            &mut input,
            "fast_random_projection",
            "feature_weight",
            defaults.feature_weight,
        )?,
        feature_properties: embedding_strings(
            &mut input,
            "fast_random_projection",
            "feature_properties",
            defaults.feature_properties,
        )?,
        seed: embedding_seed(&mut input, "fast_random_projection", defaults.seed)?,
    };
    finish_embedding_input(
        "fast_random_projection",
        input,
        EmbeddingOptions::FastRandomProjection(options),
    )
}

fn hashgnn_options(mut input: EmbeddingInput) -> Result<EmbeddingOptions> {
    let defaults = HashGnnOptions::default();
    let options = HashGnnOptions {
        dimensions: embedding_usize(&mut input, "hashgnn", "dimensions", defaults.dimensions)?,
        iterations: embedding_usize(&mut input, "hashgnn", "iterations", defaults.iterations)?,
        embedding_density: embedding_f64(
            &mut input,
            "hashgnn",
            "embedding_density",
            defaults.embedding_density,
        )?,
        heterogeneous: embedding_bool(
            &mut input,
            "hashgnn",
            "heterogeneous",
            defaults.heterogeneous,
        )?,
        node_type_property: embedding_optional_string(
            &mut input,
            "hashgnn",
            "node_type_property",
            defaults.node_type_property,
        )?,
        relationship_type_property: embedding_optional_string(
            &mut input,
            "hashgnn",
            "relationship_type_property",
            defaults.relationship_type_property,
        )?,
        seed: embedding_seed(&mut input, "hashgnn", defaults.seed)?,
    };
    finish_embedding_input("hashgnn", input, EmbeddingOptions::HashGnn(options))
}

pub(super) fn embedding_options_from_input(
    by: AnalyzeAlgorithm,
    via: Option<String>,
    directed: bool,
    weight: Option<String>,
    input: EmbeddingInput,
) -> Result<EmbeddingAnalyzeOptions> {
    let options = match by {
        AnalyzeAlgorithm::Node2Vec => node2vec_options(input)?,
        AnalyzeAlgorithm::GraphSage => graphsage_options(input)?,
        AnalyzeAlgorithm::FastRandomProjection => fastrp_options(input)?,
        AnalyzeAlgorithm::HashGnn => hashgnn_options(input)?,
        _ => {
            return Err(embedding_error(format!(
                "{by} is not an embedding algorithm"
            )));
        }
    };
    Ok(EmbeddingAnalyzeOptions {
        by,
        via,
        directed,
        weight,
        options,
    })
}

pub(super) type NodeSelectorInput<'env> =
    Either3<String, ClassInstance<'env, NodeHandle>, HashMap<String, serde_json::Value>>;

/// Thin Node representation of one typed search-index request.
#[napi(object)]
pub struct SearchIndexInput<'env> {
    /// Explicit properties, or `null` for deterministic string-property discovery.
    pub properties: Option<Option<Vec<String>>>,
    /// Replace even an exactly matching fresh text index.
    pub rebuild: Option<bool>,
    /// UUID, graph-owned handle, or exact property selector for a vector upsert.
    #[napi(ts_type = "string | NodeHandle | { label: string; property: string; value: any }")]
    pub node: Option<NodeSelectorInput<'env>>,
    /// Caller-supplied vector values.
    pub vector: Option<Vec<f64>>,
    /// Caller-defined vector space.
    pub space: Option<String>,
}

/// Opt-in OpenRouter configuration shared by provider indexing and find.
#[napi(object)]
pub struct OpenRouterProviderConfigInput {
    /// Explicit HTTPS origin, or loopback HTTP for local deterministic tests.
    pub origin: String,
    /// Exact provider model identifier.
    pub model: String,
    /// Immutable model revision, defaulting to `unavailable`.
    pub revision: Option<String>,
    /// Versioned response contract, defaulting to `v1`.
    pub response_contract_version: Option<String>,
    /// Explicit capabilities, defaulting to all supported provider operations.
    pub capabilities: Option<Vec<String>>,
    /// Conservative model input bound, defaulting to one million tokens.
    pub max_input_tokens: Option<u32>,
    /// Per-call transport deadline in milliseconds.
    pub transport_timeout_millis: Option<u32>,
    /// Conservative caller-owned cost estimate per counted token.
    pub estimated_cost_microunits_per_token: Option<u32>,
}

/// Explicit property projection for provider embedding inspection/publication.
#[napi(object)]
pub struct ProviderEmbeddingPlanInput {
    /// User-visible embedding-space name.
    pub name: String,
    /// Required graph label.
    pub label: String,
    /// Explicit outbound string properties.
    pub properties: Vec<String>,
    /// Fixed provider response width.
    pub dimensions: u32,
    /// `none` or `l2` storage normalization.
    pub normalization: Option<String>,
    /// Permit an occupied alias to be explicitly replaced.
    pub replace: Option<bool>,
}

/// Explicit bounded reranking options.
#[napi(object)]
pub struct ProviderRerankInput {
    /// Explicit rerank query.
    pub query: String,
    /// Explicit outbound candidate properties.
    pub properties: Vec<String>,
    /// Bounded canonical candidate depth.
    pub candidate_depth: u32,
    /// `error` or the explicit `canonical_unreranked` fallback.
    pub failure_policy: Option<String>,
}

/// One complete caller embedding row at the Node boundary.
#[napi(object)]
pub struct CallerEmbeddingRowInput<'env> {
    /// UUID, graph-owned handle, or exact property selector.
    #[napi(ts_type = "string | NodeHandle | { label: string; property: string; value: any }")]
    pub node: NodeSelectorInput<'env>,
    /// Finite Float32-compatible vector coordinates.
    pub vector: Vec<f64>,
}

/// Complete caller embedding publication options.
#[napi(object)]
pub struct CallerEmbeddingPublicationInput<'env> {
    /// Complete selected UUID/vector projection.
    pub rows: Vec<CallerEmbeddingRowInput<'env>>,
    /// Fixed width retained for empty projections.
    pub dimensions: u32,
    /// Non-empty versioned graph projection identity.
    pub source_projection: HashMap<String, String>,
    /// Stable caller batch contract version.
    pub contract_version: Option<String>,
    /// `none` or `l2`.
    pub normalization: Option<String>,
    /// Permit explicit alias rebinding.
    pub replace: Option<bool>,
}

/// Canonical algorithm embedding publication options.
#[napi(object)]
pub struct AlgorithmEmbeddingPublicationInput {
    /// Explicit eligible algorithm embedding algorithm.
    pub algorithm: String,
    /// Frozen algorithm contract version.
    pub algorithm_version: String,
    /// Fixed width retained for empty projections.
    pub dimensions: u32,
    /// Normalized algorithm hyperparameters.
    pub hyperparameters: Option<HashMap<String, serde_json::Value>>,
    /// Non-empty versioned algorithm input recipe.
    pub input_recipe: HashMap<String, serde_json::Value>,
    /// Non-empty graph projection identity.
    pub source_projection: HashMap<String, serde_json::Value>,
    /// `none` or `l2`.
    pub normalization: Option<String>,
    /// Permit explicit alias rebinding.
    pub replace: Option<bool>,
}

/// Coerce public Node selector shapes without resolving graph state.
pub(super) fn node_selector_from_input(input: NodeSelectorInput<'_>) -> Result<NodeSelector> {
    match input {
        Either3::A(uuid) => NodeSelector::uuid(&uuid).map_err(|error| to_napi_err(&error)),
        Either3::B(handle) => Ok(NodeSelector::Handle(handle.inner.clone())),
        Either3::C(mut selector) => {
            if selector.len() != 3 {
                return Err(to_napi_err(&GfError::Validation(
                    "property selector must contain exactly label, property, and value".into(),
                )));
            }
            let string_field = |selector: &mut HashMap<String, serde_json::Value>, name| {
                selector
                    .remove(name)
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .ok_or_else(|| {
                        to_napi_err(&GfError::Validation(format!(
                            "property selector requires string {name}"
                        )))
                    })
            };
            let label = string_field(&mut selector, "label")?;
            let property = string_field(&mut selector, "property")?;
            let value = selector.remove("value").ok_or_else(|| {
                to_napi_err(&GfError::Validation(
                    "property selector requires value".into(),
                ))
            })?;
            Ok(NodeSelector::Match {
                label,
                property,
                value: json_to_prop_value(&value)?,
            })
        }
    }
}

/// Convert JS numbers to the facade's native vector width without silent range loss.
fn vector_from_input(values: Option<Vec<f64>>) -> Result<Option<Vec<f32>>> {
    let Some(values) = values else {
        return Ok(None);
    };
    let mut vector = Vec::new();
    vector.try_reserve_exact(values.len()).map_err(|_| {
        to_napi_err(&GfError::Execution(
            "search vector allocation exceeds available memory".into(),
        ))
    })?;
    for value in values {
        if value.is_finite() && value.abs() > f64::from(f32::MAX) {
            return Err(to_napi_err(&GfError::Validation(
                "search vector value exceeds the finite f32 range".into(),
            )));
        }
        #[allow(
            clippy::cast_possible_truncation,
            reason = "finite range is checked above; f32 is the Rust search contract"
        )]
        let converted = value as f32;
        if value.is_finite() && value != 0.0 && converted == 0.0 {
            return Err(to_napi_err(&GfError::Validation(
                "search vector value is smaller than the finite f32 range".into(),
            )));
        }
        vector.push(converted);
    }
    Ok(Some(vector))
}

pub(super) struct ConfiguredProviderBinding {
    session: OpenRouterProviderSession,
    request_limits: ProviderRequestLimits,
    execution_limits: ProviderExecutionLimits,
}

fn node_provider_capabilities(values: Option<Vec<String>>) -> Result<ProviderCapabilities> {
    let values = values.unwrap_or_else(|| {
        vec![
            "document_embeddings".to_owned(),
            "query_embeddings".to_owned(),
            "candidate_reranking".to_owned(),
        ]
    });
    let values = values
        .into_iter()
        .map(|value| match value.as_str() {
            "document_embeddings" => Ok(ProviderCapability::DocumentEmbeddings),
            "query_embeddings" => Ok(ProviderCapability::QueryEmbeddings),
            "candidate_reranking" => Ok(ProviderCapability::CandidateReranking),
            _ => Err(to_napi_err(&GfError::Validation(format!(
                "unknown provider capability {value:?}"
            )))),
        })
        .collect::<Result<Vec<_>>>()?;
    ProviderCapabilities::new(values)
        .map_err(|error| to_napi_err(&GfError::Validation(error.to_string())))
}

fn node_provider_plan_request(
    configured: &ConfiguredProviderBinding,
    input: ProviderEmbeddingPlanInput,
) -> Result<ProviderEmbeddingPlanRequest> {
    let normalization = match input.normalization.as_deref().unwrap_or("none") {
        "none" => ProviderEmbeddingNormalization::None,
        "l2" => ProviderEmbeddingNormalization::L2,
        other => {
            return Err(to_napi_err(&GfError::Validation(format!(
                "unknown provider embedding normalization {other:?}"
            ))));
        }
    };
    Ok(ProviderEmbeddingPlanRequest {
        display_name: input.name,
        label: input.label,
        properties: input.properties,
        contract: configured.session.contract().clone(),
        dimensions: input.dimensions,
        normalization,
        distance: ProviderEmbeddingDistance::Cosine,
        request_limits: configured.request_limits,
        batch_limits: ProviderBatchLimits::default(),
        execution_limits: configured.execution_limits,
        replace_alias: input.replace.unwrap_or(false),
    })
}

fn provider_plan_to_json(inspection: ProviderEmbeddingPlanInspection) -> serde_json::Value {
    let token_count_class = match inspection.token_count_class {
        TokenCountClass::ExactLocal => "exact_local",
        TokenCountClass::ProviderReported => "provider_reported",
        TokenCountClass::Approximate => "approximate",
    };
    let normalization = match inspection.normalization {
        ProviderEmbeddingNormalization::None => "none",
        ProviderEmbeddingNormalization::L2 => "l2",
    };
    serde_json::json!({
        "displayName": inspection.display_name,
        "compatibilityId": inspection.compatibility_id,
        "sourceFingerprint": inspection.source_fingerprint,
        "graphGeneration": inspection.graph_generation,
        "label": inspection.label,
        "properties": inspection.properties,
        "provider": inspection.provider,
        "model": inspection.model,
        "revision": inspection.revision,
        "responseContractVersion": inspection.response_contract_version,
        "tokenizerIdentifier": inspection.tokenizer_identifier,
        "tokenizerVersion": inspection.tokenizer_version,
        "tokenCountClass": token_count_class,
        "modelInputTokens": inspection.model_input_tokens,
        "tokenizerNormalization": inspection.tokenizer_normalization,
        "chunking": inspection.chunking.map(|chunking| serde_json::json!({
            "chunkSizeTokens": chunking.chunk_size_tokens,
            "overlapTokens": chunking.overlap_tokens,
            "aggregation": chunking.aggregation,
            "truncationPolicy": chunking.truncation_policy,
        })),
        "dimensions": inspection.dimensions,
        "normalization": normalization,
        "distance": "cosine",
        "selectedNodes": inspection.selected_nodes,
        "inputBytes": inspection.input_bytes,
        "inputTokens": inspection.input_tokens,
        "batches": inspection.batches.into_iter().map(|batch| serde_json::json!({
            "items": batch.items,
            "inputBytes": batch.input_bytes,
            "inputTokens": batch.input_tokens,
        })).collect::<Vec<_>>(),
        "requestLimits": {
            "items": inspection.request_limits.items,
            "inputBytes": inspection.request_limits.input_bytes,
            "inputTokens": inspection.request_limits.input_tokens,
            "outputValues": inspection.request_limits.output_values,
            "providerCalls": inspection.request_limits.provider_calls,
        },
        "batchLimits": {
            "items": inspection.batch_limits.items,
            "inputBytes": inspection.batch_limits.input_bytes,
            "inputTokens": inspection.batch_limits.input_tokens,
        },
        "executionLimits": {
            "providerCalls": inspection.execution_limits.provider_calls,
            "retries": inspection.execution_limits.retries,
            "inputTokenExposure": inspection.execution_limits.input_token_exposure,
            "estimatedCostMicrounits": inspection.execution_limits.estimated_cost_microunits,
            "timeoutMillis": inspection.execution_limits.timeout.as_millis(),
            "minimumCallIntervalMillis": inspection.execution_limits.minimum_call_interval.as_millis(),
            "retryBackoffMillis": inspection.execution_limits.retry_backoff.as_millis(),
            "maximumRetryBackoffMillis": inspection.execution_limits.maximum_retry_backoff.as_millis(),
        },
    })
}

fn node_rerank_options(
    input: ProviderRerankInput,
    configured: &ConfiguredProviderBinding,
) -> Result<FindRerankOptions> {
    let failure_policy = match input.failure_policy.as_deref().unwrap_or("error") {
        "error" => RerankFailurePolicy::Error,
        "canonical_unreranked" => RerankFailurePolicy::CanonicalUnreranked,
        other => {
            return Err(to_napi_err(&GfError::Validation(format!(
                "unknown rerank failure policy {other:?}"
            ))));
        }
    };
    Ok(FindRerankOptions {
        query: input.query,
        properties: input.properties,
        candidate_depth: input.candidate_depth as usize,
        contract: configured.session.contract().clone(),
        request_limits: configured.request_limits,
        execution_limits: configured.execution_limits,
        failure_policy,
    })
}

fn node_runtime_error(error: napi::Error) -> NodeError {
    napi::Error::new("ExecutionError".to_owned(), error.to_string())
}

fn emit_node_warnings(env: Env, diagnostics: &[FindDiagnostic]) -> Result<()> {
    for diagnostic in diagnostics {
        let message = match diagnostic {
            FindDiagnostic::ForcedStale { diagnostic } => diagnostic.clone(),
            FindDiagnostic::RerankSuggested { provider, model } => format!(
                "configured reranker {provider}/{model} was omitted; explicit reranking may improve top-result quality"
            ),
        };
        let global = env.get_global().map_err(node_runtime_error)?;
        let process: Object = global
            .get_named_property("process")
            .map_err(node_runtime_error)?;
        let emit: Function<String, ()> = process
            .get_named_property("emitWarning")
            .map_err(node_runtime_error)?;
        emit.apply(process, message).map_err(node_runtime_error)?;
    }
    Ok(())
}

#[napi]
impl GraphForge {
    /// Configure one opt-in OpenRouter session shared by provider indexing and find.
    #[napi]
    pub fn configure_openrouter(
        &mut self,
        credential: String,
        input: OpenRouterProviderConfigInput,
    ) -> Result<()> {
        self.ensure_open()?;
        let request_limits = ProviderRequestLimits::default();
        let execution_limits = ProviderExecutionLimits::default();
        let config = OpenRouterProviderSessionConfig {
            origin: input.origin,
            model: input.model,
            revision: input.revision.unwrap_or_else(|| "unavailable".to_owned()),
            response_contract_version: input
                .response_contract_version
                .unwrap_or_else(|| "v1".to_owned()),
            capabilities: node_provider_capabilities(input.capabilities)?,
            max_input_tokens: u64::from(input.max_input_tokens.unwrap_or(1_000_000)),
            chunking: None,
            wire_limits: OpenRouterWireLimits::default(),
            request_limits,
            execution_limits,
            transport_timeout: Duration::from_millis(u64::from(
                input.transport_timeout_millis.unwrap_or(30_000),
            )),
            estimated_cost_microunits_per_token: u64::from(
                input.estimated_cost_microunits_per_token.unwrap_or(1),
            ),
        };
        let session = OpenRouterProviderSession::new(config, credential)
            .map_err(|error| to_napi_err(&error))?;
        self.provider = Some(ConfiguredProviderBinding {
            session,
            request_limits,
            execution_limits,
        });
        Ok(())
    }

    /// Inspect one content-free provider property-embedding plan without network work.
    #[napi]
    pub fn inspect_provider_embedding_plan(
        &self,
        input: ProviderEmbeddingPlanInput,
    ) -> Result<serde_json::Value> {
        self.ensure_open()?;
        let configured = self.provider.as_ref().ok_or_else(|| {
            to_napi_err(&GfError::Validation(
                "OpenRouter is not configured".to_owned(),
            ))
        })?;
        let request = node_provider_plan_request(configured, input)?;
        let graph = self.open_guard()?;
        configured
            .session
            .inspect_embedding_plan(&graph, &request)
            .map(provider_plan_to_json)
            .map_err(|error| to_napi_err(&GfError::Execution(error.to_string())))
    }

    /// Confirm, execute, and atomically publish one provider embedding generation.
    #[napi]
    pub fn publish_provider_embeddings(
        &self,
        input: ProviderEmbeddingPlanInput,
    ) -> Result<serde_json::Value> {
        self.ensure_open()?;
        let configured = self.provider.as_ref().ok_or_else(|| {
            to_napi_err(&GfError::Validation(
                "OpenRouter is not configured".to_owned(),
            ))
        })?;
        let request = node_provider_plan_request(configured, input)?;
        let graph = self.open_guard()?;
        configured
            .session
            .publish_embeddings(&graph, &request)
            .map(embedding_space_to_json)
            .map_err(|error| to_napi_err(&GfError::Execution(error.to_string())))
    }

    /// Text + vector hybrid search. Returns an Arrow IPC `Buffer`.
    #[allow(clippy::too_many_arguments)] // explicit cross-language v0.5 find contract
    #[napi]
    pub fn find(
        &self,
        env: Env,
        query: Option<String>,
        label: Option<String>,
        vector: Option<Vec<f64>>,
        similar_to: Option<NodeSelectorInput<'_>>,
        semantic_query: Option<String>,
        limit: Option<u32>,
        space: Option<String>,
        force_stale: Option<bool>,
        rerank: Option<ProviderRerankInput>,
        suppress_rerank_advisory: Option<bool>,
    ) -> Result<Buffer> {
        let options = FindOptions {
            query,
            label,
            vector: vector_from_input(vector)?,
            similar_to: similar_to.map(node_selector_from_input).transpose()?,
            semantic_query,
            limit: limit.unwrap_or(10) as usize,
            space,
            force_stale: force_stale.unwrap_or(false),
        };
        let rerank = match (rerank, self.provider.as_ref()) {
            (Some(value), Some(configured)) => Some(node_rerank_options(value, configured)?),
            (Some(_), None) => {
                return Err(to_napi_err(&GfError::Validation(
                    "rerank requires a configured OpenRouter session".to_owned(),
                )));
            }
            (None, _) => None,
        };
        let omitted_reranker = self.provider.as_ref().and_then(|configured| {
            (rerank.is_none()
                && configured
                    .session
                    .contract()
                    .capabilities()
                    .supports(ProviderCapability::CandidateReranking))
            .then(|| configured.session.contract().clone())
        });
        let execution = FindExecutionOptions {
            find: options,
            rerank,
            omitted_reranker,
            advisory_policy: if suppress_rerank_advisory.unwrap_or(false) {
                RerankAdvisoryPolicy::Suppress
            } else {
                RerankAdvisoryPolicy::Emit
            },
        };
        let graph = self.open_guard()?;
        let result = match self.provider.as_ref() {
            Some(configured) => configured.session.find(&graph, execution),
            None => graph.find_with_diagnostics(execution, None),
        }
        .map_err(|error| to_napi_err(&error))?;
        let (batch, diagnostics, _) = result.into_parts();
        drop(graph);
        emit_node_warnings(env, &diagnostics)?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Atomically publish one complete caller-supplied UUID/vector generation.
    #[napi]
    pub fn publish_caller_embeddings(
        &self,
        name: String,
        input: CallerEmbeddingPublicationInput<'_>,
    ) -> Result<String> {
        let normalization = match input.normalization.as_deref().unwrap_or("none") {
            "none" => CallerEmbeddingNormalization::None,
            "l2" => CallerEmbeddingNormalization::L2,
            other => {
                return Err(to_napi_err(&GfError::Validation(format!(
                    "unknown caller embedding normalization {other:?}"
                ))));
            }
        };
        let rows = input
            .rows
            .into_iter()
            .map(|row| {
                Ok(CallerEmbeddingBatchRow {
                    node: node_selector_from_input(row.node)?,
                    vector: vector_from_input(Some(row.vector))?
                        .expect("caller row always supplies a vector"),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let graph = self.open_guard()?;
        graph
            .publish_caller_embeddings(CallerEmbeddingBatchRequest {
                display_name: name,
                contract_version: input
                    .contract_version
                    .unwrap_or_else(|| "graphforge_binding_caller_v1".to_owned()),
                dimensions: input.dimensions,
                normalization,
                distance: CallerEmbeddingDistance::Cosine,
                source_projection_recipe: input
                    .source_projection
                    .into_iter()
                    .collect::<BTreeMap<_, _>>(),
                rows,
                replace_alias: input.replace.unwrap_or(false),
            })
            .map(|space| space.compatibility_id)
            .map_err(|error| to_napi_err(&error))
    }

    /// Atomically publish one complete canonical algorithm Arrow IPC result.
    #[napi]
    pub fn publish_algorithm_embeddings(
        &self,
        name: String,
        result: Buffer,
        input: AlgorithmEmbeddingPublicationInput,
    ) -> Result<String> {
        let normalization = match input.normalization.as_deref().unwrap_or("none") {
            "none" => AlgorithmEmbeddingNormalization::None,
            "l2" => AlgorithmEmbeddingNormalization::L2,
            other => {
                return Err(to_napi_err(&GfError::Validation(format!(
                    "unknown algorithm embedding normalization {other:?}"
                ))));
            }
        };
        let algorithm = input
            .algorithm
            .parse::<AnalyzeAlgorithm>()
            .map_err(|error| to_napi_err(&error))?;
        let graph = self.open_guard()?;
        graph
            .publish_algorithm_embeddings(AlgorithmEmbeddingPublicationRequest {
                display_name: name,
                algorithm,
                algorithm_version: input.algorithm_version,
                dimensions: input.dimensions,
                normalization,
                distance: AlgorithmEmbeddingDistance::Cosine,
                hyperparameters: input
                    .hyperparameters
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
                input_recipe: input.input_recipe.into_iter().collect(),
                source_projection_recipe: input.source_projection.into_iter().collect(),
                result: ipc_to_record_batch(&result)?,
                replace_alias: input.replace.unwrap_or(false),
            })
            .map(|space| space.compatibility_id)
            .map_err(|error| to_napi_err(&error))
    }

    /// List verified embedding-space lineages in deterministic Rust order.
    #[napi]
    pub fn embedding_spaces(&self) -> Result<Vec<serde_json::Value>> {
        let graph = self.open_guard()?;
        graph
            .embedding_spaces()
            .map(|spaces| spaces.into_iter().map(embedding_space_to_json).collect())
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect one explicit embedding alias or the configured default.
    #[napi]
    pub fn embedding_space(&self, name: Option<String>) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        graph
            .embedding_space(name.as_deref())
            .map(embedding_space_to_json)
            .map_err(|error| to_napi_err(&error))
    }

    /// Bind one alias to a verified compatibility lineage.
    #[napi]
    pub fn bind_embedding_space_alias(
        &self,
        name: String,
        compatibility_id: String,
        replace: Option<bool>,
    ) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        graph
            .bind_embedding_space_alias(&name, &compatibility_id, replace.unwrap_or(false))
            .map(embedding_space_to_json)
            .map_err(|error| to_napi_err(&error))
    }

    /// Remove one alias without deleting primary vector generations.
    #[napi]
    pub fn remove_embedding_space_alias(&self, name: String) -> Result<bool> {
        let graph = self.open_guard()?;
        graph
            .remove_embedding_space_alias(&name)
            .map_err(|error| to_napi_err(&error))
    }

    /// Select or clear the durable default embedding alias.
    #[napi]
    pub fn set_default_embedding_space(
        &self,
        name: Option<String>,
    ) -> Result<Option<serde_json::Value>> {
        let graph = self.open_guard()?;
        graph
            .set_default_embedding_space(name.as_deref())
            .map(|space| space.map(embedding_space_to_json))
            .map_err(|error| to_napi_err(&error))
    }

    /// Delete one complete embedding compatibility lineage by alias or default.
    #[napi]
    pub fn delete_embedding_space(&self, name: Option<String>) -> Result<bool> {
        let graph = self.open_guard()?;
        graph
            .delete_embedding_space(name.as_deref())
            .map_err(|error| to_napi_err(&error))
    }

    /// Build or update one typed search index, or use the legacy adjacency call.
    #[napi]
    pub fn index(
        &self,
        label: String,
        input: Option<SearchIndexInput<'_>>,
    ) -> Result<Option<serde_json::Value>> {
        let graph = self.open_guard()?;
        if input.is_none() && label == "adjacency" {
            graph.index(&label).map_err(|error| to_napi_err(&error))?;
            return Ok(None);
        }
        let options = match input {
            Some(input) => {
                let node = input.node.map(node_selector_from_input).transpose()?;
                let vector = vector_from_input(input.vector)?;
                SearchIndexOptions::from_binding_fields(
                    input.properties,
                    input.rebuild,
                    node,
                    vector,
                    input.space,
                )
            }
            None => SearchIndexOptions::from_binding_fields(None, None, None, None, None),
        }
        .map_err(|error| to_napi_err(&error))?;
        graph
            .index_search(&label, options)
            .map(|receipt| receipt.map(text_index_inspection_to_json))
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect one graph-native text index without building it.
    #[napi]
    pub fn inspect_text_index(
        &self,
        label: String,
        properties: Option<Vec<String>>,
    ) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        graph
            .inspect_text_index(&label, properties.as_deref())
            .map(text_index_inspection_to_json)
            .map_err(|error| to_napi_err(&error))
    }

    /// Explicitly build the graph's derived adjacency index.
    #[napi]
    pub fn index_adjacency(&self) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        graph
            .index_adjacency()
            .map(adjacency_inspection_to_json)
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect the graph's derived adjacency index without rebuilding it.
    #[napi]
    pub fn inspect_adjacency(&self) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        graph
            .inspect_adjacency()
            .map(adjacency_inspection_to_json)
            .map_err(|error| to_napi_err(&error))
    }

    /// Rebuild adjacency and return the canonical receipt.
    #[napi]
    pub fn rebuild_adjacency(&self) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        graph
            .rebuild_adjacency(None)
            .map(adjacency_inspection_to_json)
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect one active embedding generation's Rust-owned freshness decision.
    #[napi]
    pub fn inspect_embedding_space_freshness(
        &self,
        name: Option<String>,
        force_stale: Option<bool>,
    ) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        graph
            .inspect_embedding_space_freshness(name.as_deref(), force_stale.unwrap_or(false))
            .map(refresh_freshness_to_json)
            .map_err(|error| to_napi_err(&error))
    }

    /// Read the durable project-wide embedding refresh defaults.
    #[napi]
    pub fn embedding_refresh_project_policy(&self) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        graph
            .embedding_refresh_project_policy()
            .map(refresh_project_policy_to_json)
            .map_err(|error| to_napi_err(&error))
    }

    /// Replace the durable project-wide embedding refresh defaults.
    #[napi]
    pub fn set_embedding_refresh_project_policy(
        &self,
        proactive: bool,
        debounce_millis: u32,
        max_concurrent_jobs: u32,
    ) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        graph
            .set_embedding_refresh_project_policy(EmbeddingRefreshProjectPolicy {
                proactive,
                debounce: Duration::from_millis(u64::from(debounce_millis)),
                max_concurrent_jobs: max_concurrent_jobs as usize,
            })
            .map(refresh_project_policy_to_json)
            .map_err(|error| to_napi_err(&error))
    }

    /// Set or explicitly clear one lineage's durable refresh override.
    #[napi]
    pub fn set_embedding_refresh_space_policy(
        &self,
        name: Option<String>,
        proactive: Option<bool>,
        debounce_millis: Option<u32>,
        clear: Option<bool>,
    ) -> Result<serde_json::Value> {
        let clear = clear.unwrap_or(false);
        let policy = if clear {
            if proactive.is_some() || debounce_millis.is_some() {
                return Err(to_napi_err(&GfError::Validation(
                    "clearing an embedding refresh space policy cannot include overrides"
                        .to_owned(),
                )));
            }
            None
        } else {
            if proactive.is_none() && debounce_millis.is_none() {
                return Err(to_napi_err(&GfError::Validation(
                    "embedding refresh space policy requires an override or clear=true".to_owned(),
                )));
            }
            Some(EmbeddingRefreshSpacePolicy {
                proactive,
                debounce: debounce_millis.map(|millis| Duration::from_millis(u64::from(millis))),
            })
        };
        let graph = self.open_guard()?;
        graph
            .set_embedding_refresh_space_policy(name.as_deref(), policy)
            .map(refresh_inspection_to_json)
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect durable refresh state and this process's worker counters.
    #[napi]
    pub fn inspect_embedding_refresh(&self, name: Option<String>) -> Result<serde_json::Value> {
        let graph = self.open_guard()?;
        graph
            .inspect_embedding_refresh(name.as_deref())
            .map(refresh_inspection_to_json)
            .map_err(|error| to_napi_err(&error))
    }
}

#[cfg(test)]
mod tests;
