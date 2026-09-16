//! Providers Python binding methods and conversions.

use super::{
    GraphForge, PyCancellationToken, PyNodeHandle, algorithm_result, json_map, py_to_prop_value,
    pyarrow_table_to_batch, string_map, to_pyerr,
};
use graphforge_api::AlgorithmEmbeddingDistance;
use graphforge_api::AlgorithmEmbeddingNormalization;
use graphforge_api::AlgorithmEmbeddingPublicationRequest;
use graphforge_api::CallerEmbeddingBatchRequest;
use graphforge_api::CallerEmbeddingBatchRow;
use graphforge_api::CallerEmbeddingDistance;
use graphforge_api::CallerEmbeddingNormalization;
use graphforge_api::EmbeddingAnalyzeOptions;
use graphforge_api::EmbeddingOptions;
use graphforge_api::EmbeddingRefreshFailureClass;
use graphforge_api::EmbeddingRefreshInspection;
use graphforge_api::EmbeddingRefreshOutcomeStatus;
use graphforge_api::EmbeddingRefreshProjectPolicy;
use graphforge_api::EmbeddingRefreshSpacePolicy;
use graphforge_api::EmbeddingRefreshWorkerState;
use graphforge_api::EmbeddingSpaceFreshnessInspection;
use graphforge_api::EmbeddingSpaceFreshnessState;
use graphforge_api::EmbeddingSpaceInfo;
use graphforge_api::EmbeddingSpaceProducer;
use graphforge_api::EmbeddingSpaceReadDecision;
use graphforge_api::EmbeddingTokenCountClass;
use graphforge_api::FastRpOptions;
use graphforge_api::FindDiagnostic;
use graphforge_api::FindExecutionOptions;
use graphforge_api::FindRerankOptions;
use graphforge_api::GfError;
use graphforge_api::GraphSageAggregator;
use graphforge_api::GraphSageOptions;
use graphforge_api::HashGnnOptions;
use graphforge_api::Node2VecOptions;
use graphforge_api::NodeSelector;
use graphforge_api::OpenRouterProviderSession;
use graphforge_api::OpenRouterProviderSessionConfig;
use graphforge_api::OpenRouterWireLimits;
use graphforge_api::ProviderBatchLimits;
use graphforge_api::ProviderCapabilities;
use graphforge_api::ProviderCapability;
use graphforge_api::ProviderEmbeddingDistance;
use graphforge_api::ProviderEmbeddingNormalization;
use graphforge_api::ProviderEmbeddingPlanInspection;
use graphforge_api::ProviderEmbeddingPlanRequest;
use graphforge_api::ProviderExecutionLimits;
use graphforge_api::ProviderRequestLimits;
use graphforge_api::RerankAdvisoryPolicy;
use graphforge_api::RerankFailurePolicy;
use graphforge_api::SearchIndexOptions;
use graphforge_api::TextIndexInspection;
use graphforge_api::TokenCountClass;
use pyo3::exceptions::PyRuntimeWarning;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use std::ffi::CString;
use std::time::Duration;

/// Coerce public Python selector shapes without resolving graph state.
pub(super) fn py_to_node_selector(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<NodeSelector> {
    if let Ok(handle) = value.extract::<PyRef<'_, PyNodeHandle>>() {
        return Ok(NodeSelector::Handle(handle.inner.clone()));
    }
    if let Ok(uuid) = value.extract::<String>() {
        return NodeSelector::uuid(&uuid).map_err(|error| to_pyerr(py, &error));
    }
    if let Ok(selector) = value.cast::<PyDict>() {
        if selector.len() != 3 {
            return Err(PyTypeError::new_err(
                "property selector must contain exactly label, property, and value",
            ));
        }
        let label = selector
            .get_item("label")?
            .ok_or_else(|| PyTypeError::new_err("property selector requires label"))?
            .extract::<String>()?;
        let property = selector
            .get_item("property")?
            .ok_or_else(|| PyTypeError::new_err("property selector requires property"))?
            .extract::<String>()?;
        let value = selector
            .get_item("value")?
            .ok_or_else(|| PyTypeError::new_err("property selector requires value"))?;
        return Ok(NodeSelector::Match {
            label,
            property,
            value: py_to_prop_value(&value)?,
        });
    }
    Err(PyTypeError::new_err(
        "node selector must be a UUID string, NodeHandle, or label/property/value dict",
    ))
}

/// Coerce Python keyword representations, then delegate all variant semantics
/// to the shared Rust search-index option boundary.
fn search_index_options_from_kwargs(
    py: Python<'_>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<SearchIndexOptions> {
    let mut properties = None;
    let mut rebuild = None;
    let mut node = None;
    let mut vector = None;
    let mut space = None;

    if let Some(kwargs) = kwargs {
        for (name, value) in kwargs {
            match name.extract::<String>()?.as_str() {
                "properties" => {
                    properties = Some(if value.is_none() {
                        None
                    } else {
                        Some(value.extract::<Vec<String>>()?)
                    });
                }
                "rebuild" => rebuild = Some(value.extract::<bool>()?),
                "node" => {
                    node =
                        Some(py_to_node_selector(py, &value).map_err(|error| {
                            to_pyerr(py, &GfError::Validation(error.to_string()))
                        })?);
                }
                "vector" => vector = Some(value.extract::<Vec<f32>>()?),
                "space" => space = Some(value.extract::<String>()?),
                unknown => {
                    return Err(to_pyerr(
                        py,
                        &GfError::Validation(format!("unknown search index keyword {unknown:?}")),
                    ));
                }
            }
        }
    }

    SearchIndexOptions::from_binding_fields(properties, rebuild, node, vector, space)
        .map_err(|error| to_pyerr(py, &error))
}

fn caller_embedding_rows(
    py: Python<'_>,
    rows: &Bound<'_, PyList>,
) -> PyResult<Vec<CallerEmbeddingBatchRow>> {
    rows.iter()
        .map(|row| {
            let row = row
                .cast::<PyDict>()
                .map_err(|_| PyTypeError::new_err("caller embedding rows must be dictionaries"))?;
            if row.len() != 2 {
                return Err(PyTypeError::new_err(
                    "caller embedding row must contain exactly node and vector",
                ));
            }
            let node = row
                .get_item("node")?
                .ok_or_else(|| PyTypeError::new_err("caller embedding row requires node"))?;
            let vector = row
                .get_item("vector")?
                .ok_or_else(|| PyTypeError::new_err("caller embedding row requires vector"))?
                .extract::<Vec<f32>>()?;
            Ok(CallerEmbeddingBatchRow {
                node: py_to_node_selector(py, &node)?,
                vector,
            })
        })
        .collect()
}

fn embedding_space_to_python(py: Python<'_>, space: EmbeddingSpaceInfo) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("compatibility_id", space.compatibility_id)?;
    value.set_item("aliases", space.aliases)?;
    value.set_item("default_alias", space.default_alias)?;
    value.set_item("dimensions", space.dimensions)?;

    let producer = PyDict::new(py);
    match space.producer {
        EmbeddingSpaceProducer::Algorithm {
            algorithm,
            algorithm_version,
        } => {
            producer.set_item("kind", "algorithm")?;
            producer.set_item("algorithm", algorithm)?;
            producer.set_item("algorithm_version", algorithm_version)?;
        }
        EmbeddingSpaceProducer::Local {
            implementation,
            model,
            revision,
            contract_version,
        } => {
            producer.set_item("kind", "local")?;
            producer.set_item("implementation", implementation)?;
            producer.set_item("model", model)?;
            producer.set_item("revision", revision)?;
            producer.set_item("contract_version", contract_version)?;
        }
        EmbeddingSpaceProducer::Callback {
            callback_contract,
            contract_version,
        } => {
            producer.set_item("kind", "callback")?;
            producer.set_item("callback_contract", callback_contract)?;
            producer.set_item("contract_version", contract_version)?;
        }
        EmbeddingSpaceProducer::Remote {
            provider,
            model,
            revision,
            response_contract_version,
        } => {
            producer.set_item("kind", "remote")?;
            producer.set_item("provider", provider)?;
            producer.set_item("model", model)?;
            producer.set_item("revision", revision)?;
            producer.set_item("response_contract_version", response_contract_version)?;
        }
        EmbeddingSpaceProducer::CallerSupplied { contract_version } => {
            producer.set_item("kind", "caller_supplied")?;
            producer.set_item("contract_version", contract_version)?;
        }
    }
    value.set_item("producer", producer)?;

    let tokenizer = space.tokenizer.map(|tokenizer| {
        let value = PyDict::new(py);
        value.set_item("identifier", tokenizer.identifier)?;
        value.set_item("version", tokenizer.version)?;
        value.set_item(
            "count_class",
            match tokenizer.count_class {
                EmbeddingTokenCountClass::ExactLocal => "exact_local",
                EmbeddingTokenCountClass::ProviderReported => "provider_reported",
                EmbeddingTokenCountClass::Approximate => "approximate",
            },
        )?;
        value.set_item("max_input_tokens", tokenizer.max_input_tokens)?;
        value.set_item("normalization", tokenizer.normalization)?;
        Ok::<_, PyErr>(value)
    });
    value.set_item("tokenizer", tokenizer.transpose()?)?;

    let chunking = space.chunking.map(|chunking| {
        let value = PyDict::new(py);
        value.set_item("chunk_size_tokens", chunking.chunk_size_tokens)?;
        value.set_item("overlap_tokens", chunking.overlap_tokens)?;
        value.set_item("aggregation", chunking.aggregation)?;
        value.set_item("truncation_policy", chunking.truncation_policy)?;
        Ok::<_, PyErr>(value)
    });
    value.set_item("chunking", chunking.transpose()?)?;

    let active = space.active.map(|active| {
        let value = PyDict::new(py);
        value.set_item("generation_id", active.generation_id)?;
        value.set_item("vector_count", active.vector_count)?;
        value.set_item("source_graph_generation", active.source_graph_generation)?;
        value.set_item("source_fingerprint", active.source_fingerprint)?;
        value.set_item("generated_at_micros", active.generated_at_micros)?;
        value.set_item("committed_at_micros", active.committed_at_micros)?;
        Ok::<_, PyErr>(value)
    });
    value.set_item("active", active.transpose()?)?;
    Ok(value.into_any().unbind())
}

fn refresh_project_policy_to_python(
    py: Python<'_>,
    policy: EmbeddingRefreshProjectPolicy,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("proactive", policy.proactive)?;
    value.set_item("debounce_millis", policy.debounce.as_millis())?;
    value.set_item("max_concurrent_jobs", policy.max_concurrent_jobs)?;
    Ok(value.into_any().unbind())
}

fn refresh_space_policy_to_python(
    py: Python<'_>,
    policy: EmbeddingRefreshSpacePolicy,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("proactive", policy.proactive)?;
    value.set_item(
        "debounce_millis",
        policy.debounce.map(|duration| duration.as_millis()),
    )?;
    Ok(value.into_any().unbind())
}

fn refresh_freshness_to_python(
    py: Python<'_>,
    freshness: EmbeddingSpaceFreshnessInspection,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("compatibility_id", freshness.compatibility_id)?;
    value.set_item("generation_id", freshness.generation_id)?;
    value.set_item(
        "state",
        match freshness.state {
            EmbeddingSpaceFreshnessState::Fresh => "fresh",
            EmbeddingSpaceFreshnessState::Stale => "stale",
            EmbeddingSpaceFreshnessState::SubstantiallyStale => "substantially_stale",
        },
    )?;
    value.set_item("reason", freshness.reason)?;
    let decision = PyDict::new(py);
    match freshness.decision {
        EmbeddingSpaceReadDecision::ServeFresh => {
            decision.set_item("kind", "serve_fresh")?;
        }
        EmbeddingSpaceReadDecision::ServeStale { reason } => {
            decision.set_item("kind", "serve_stale")?;
            decision.set_item("reason", reason)?;
        }
        EmbeddingSpaceReadDecision::RefreshRequired { reason } => {
            decision.set_item("kind", "refresh_required")?;
            decision.set_item("reason", reason)?;
        }
        EmbeddingSpaceReadDecision::ServeForcedStale { diagnostic } => {
            decision.set_item("kind", "serve_forced_stale")?;
            decision.set_item("diagnostic", diagnostic)?;
        }
    }
    value.set_item("decision", decision)?;
    Ok(value.into_any().unbind())
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

fn refresh_inspection_to_python(
    py: Python<'_>,
    inspection: EmbeddingRefreshInspection,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("compatibility_id", inspection.compatibility_id)?;
    value.set_item(
        "project_policy",
        refresh_project_policy_to_python(py, inspection.project_policy)?,
    )?;
    value.set_item(
        "space_policy",
        inspection
            .space_policy
            .map(|policy| refresh_space_policy_to_python(py, policy))
            .transpose()?,
    )?;
    let resolved = PyDict::new(py);
    resolved.set_item("proactive", inspection.resolved_policy.proactive)?;
    resolved.set_item(
        "debounce_millis",
        inspection.resolved_policy.debounce.as_millis(),
    )?;
    resolved.set_item(
        "max_concurrent_jobs",
        inspection.resolved_policy.max_concurrent_jobs,
    )?;
    value.set_item("resolved_policy", resolved)?;
    let outcome = inspection.last_outcome.map(|outcome| {
        let value = PyDict::new(py);
        match outcome.status {
            EmbeddingRefreshOutcomeStatus::Succeeded => value.set_item("status", "succeeded")?,
            EmbeddingRefreshOutcomeStatus::Cancelled => value.set_item("status", "cancelled")?,
            EmbeddingRefreshOutcomeStatus::Failed(failure) => {
                value.set_item("status", "failed")?;
                value.set_item("failure_class", refresh_failure_token(failure))?;
            }
        }
        value.set_item("graph_generation", outcome.graph_generation)?;
        value.set_item("source_fingerprint", outcome.source_fingerprint.to_hex())?;
        value.set_item("completed_at_micros", outcome.completed_at_micros)?;
        Ok::<_, PyErr>(value)
    });
    value.set_item("last_outcome", outcome.transpose()?)?;
    value.set_item(
        "freshness",
        inspection
            .freshness
            .map(|freshness| refresh_freshness_to_python(py, freshness))
            .transpose()?,
    )?;
    let worker = PyDict::new(py);
    worker.set_item(
        "state",
        match inspection.worker.state {
            EmbeddingRefreshWorkerState::Running => "running",
            EmbeddingRefreshWorkerState::Shutdown => "shutdown",
        },
    )?;
    worker.set_item("queued_lineages", inspection.worker.queued_lineages)?;
    worker.set_item("in_flight_lineages", inspection.worker.in_flight_lineages)?;
    worker.set_item(
        "selected_lineage_queued",
        inspection.worker.selected_lineage_queued,
    )?;
    worker.set_item(
        "selected_lineage_in_flight",
        inspection.worker.selected_lineage_in_flight,
    )?;
    worker.set_item("coalesced_notices", inspection.worker.coalesced_notices)?;
    worker.set_item("succeeded", inspection.worker.succeeded)?;
    worker.set_item("failed", inspection.worker.failed)?;
    worker.set_item("cancelled", inspection.worker.cancelled)?;
    value.set_item("worker", worker)?;
    Ok(value.into_any().unbind())
}

fn text_index_inspection_to_python(
    py: Python<'_>,
    inspection: TextIndexInspection,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item(
        "project_generation_uuid",
        inspection.project_generation_uuid.to_string(),
    )?;
    value.set_item("properties", inspection.properties)?;
    value.set_item("source_generation", inspection.source_generation)?;
    value.set_item("source_fingerprint", inspection.source_fingerprint)?;
    value.set_item("artifact_generation", inspection.artifact_generation)?;
    value.set_item(
        "artifact_source_generation",
        inspection.artifact_source_generation,
    )?;
    value.set_item(
        "artifact_source_fingerprint",
        inspection.artifact_source_fingerprint,
    )?;
    value.set_item("state", inspection.state.as_str())?;
    value.set_item(
        "reason",
        inspection
            .reason
            .map(graphforge_api::TextIndexFreshnessReason::as_str),
    )?;
    Ok(value.into_any().unbind())
}

pub(super) fn adjacency_inspection_to_python(
    py: Python<'_>,
    inspection: graphforge_api::AdjacencyInspection,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item(
        "artifact_effective_generation",
        inspection.artifact_effective_generation,
    )?;
    value.set_item("artifact_fingerprint", inspection.artifact_fingerprint)?;
    value.set_item(
        "artifact_source_generation",
        inspection.artifact_source_generation,
    )?;
    value.set_item(
        "project_generation_uuid",
        inspection.project_generation_uuid.to_string(),
    )?;
    value.set_item(
        "reason",
        inspection
            .reason
            .map(graphforge_api::AdjacencyFreshnessReason::as_str),
    )?;
    value.set_item(
        "source_topology_fingerprint",
        inspection.source_topology_fingerprint,
    )?;
    value.set_item(
        "source_topology_generation",
        inspection.source_topology_generation,
    )?;
    value.set_item("state", inspection.state.as_str())?;
    Ok(value.into_any().unbind())
}

pub(super) fn embedding_validation(py: Python<'_>, message: impl Into<String>) -> PyErr {
    to_pyerr(py, &GfError::Validation(message.into()))
}

fn embedding_count(
    py: Python<'_>,
    algorithm: &str,
    name: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<usize> {
    let value = value
        .extract::<i128>()
        .map_err(|_| embedding_validation(py, format!("{algorithm} {name} must be an integer")))?;
    usize::try_from(value)
        .map_err(|_| embedding_validation(py, format!("{algorithm} {name} must be nonnegative")))
}

fn embedding_counts(
    py: Python<'_>,
    algorithm: &str,
    name: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<Vec<usize>> {
    value
        .extract::<Vec<i128>>()
        .map_err(|_| {
            embedding_validation(py, format!("{algorithm} {name} must be integer values"))
        })?
        .into_iter()
        .map(|value| {
            usize::try_from(value).map_err(|_| {
                embedding_validation(py, format!("{algorithm} {name} values must be nonnegative"))
            })
        })
        .collect()
}

fn embedding_seed(py: Python<'_>, algorithm: &str, value: &Bound<'_, PyAny>) -> PyResult<u64> {
    let value = value
        .extract::<i128>()
        .map_err(|_| embedding_validation(py, format!("{algorithm} seed must be an integer")))?;
    u64::try_from(value)
        .map_err(|_| embedding_validation(py, format!("{algorithm} seed must fit unsigned 64-bit")))
}

fn embedding_strings(
    py: Python<'_>,
    algorithm: &str,
    name: &str,
    value: &Bound<'_, PyAny>,
) -> PyResult<Vec<String>> {
    value.extract::<Vec<String>>().map_err(|_| {
        embedding_validation(
            py,
            format!("{algorithm} {name} must be an ordered list of property names"),
        )
    })
}

fn node2vec_options(
    py: Python<'_>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Node2VecOptions> {
    let mut options = Node2VecOptions::default();
    if let Some(kwargs) = kwargs {
        for (name, value) in kwargs {
            let name = name.extract::<String>()?;
            match name.as_str() {
                "dimensions" => {
                    options.dimensions = embedding_count(py, "node2vec", &name, &value)?;
                }
                "walk_length" => {
                    options.walk_length = embedding_count(py, "node2vec", &name, &value)?;
                }
                "walks_per_node" => {
                    options.walks_per_node = embedding_count(py, "node2vec", &name, &value)?;
                }
                "p" => options.p = value.extract()?,
                "q" => options.q = value.extract()?,
                "window_size" => {
                    options.window_size = embedding_count(py, "node2vec", &name, &value)?;
                }
                "negative_samples" => {
                    options.negative_samples = embedding_count(py, "node2vec", &name, &value)?;
                }
                "epochs" => options.epochs = embedding_count(py, "node2vec", &name, &value)?,
                "learning_rate" => options.learning_rate = value.extract()?,
                "seed" => options.seed = embedding_seed(py, "node2vec", &value)?,
                _ => {
                    return Err(embedding_validation(
                        py,
                        format!("unknown node2vec option {name:?}"),
                    ));
                }
            }
        }
    }
    Ok(options)
}

fn graphsage_options(
    py: Python<'_>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<GraphSageOptions> {
    let mut options = GraphSageOptions::default();
    if let Some(kwargs) = kwargs {
        for (name, value) in kwargs {
            let name = name.extract::<String>()?;
            match name.as_str() {
                "dimensions" => {
                    options.dimensions = embedding_count(py, "graphsage", &name, &value)?;
                }
                "hidden_dimensions" => {
                    options.hidden_dimensions = embedding_count(py, "graphsage", &name, &value)?;
                }
                "layers" => options.layers = embedding_count(py, "graphsage", &name, &value)?,
                "sample_sizes" => {
                    options.sample_sizes = embedding_counts(py, "graphsage", &name, &value)?;
                }
                "aggregator" => {
                    let aggregator = value.extract::<String>()?;
                    if aggregator != "mean" {
                        return Err(embedding_validation(
                            py,
                            "graphsage aggregator must be \"mean\"",
                        ));
                    }
                    options.aggregator = GraphSageAggregator::Mean;
                }
                "epochs" => options.epochs = embedding_count(py, "graphsage", &name, &value)?,
                "negative_samples" => {
                    options.negative_samples = embedding_count(py, "graphsage", &name, &value)?;
                }
                "learning_rate" => options.learning_rate = value.extract()?,
                "feature_properties" => {
                    options.feature_properties = embedding_strings(py, "graphsage", &name, &value)?;
                }
                "seed" => options.seed = embedding_seed(py, "graphsage", &value)?,
                _ => {
                    return Err(embedding_validation(
                        py,
                        format!("unknown graphsage option {name:?}"),
                    ));
                }
            }
        }
    }
    Ok(options)
}

fn fastrp_options(py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<FastRpOptions> {
    let mut options = FastRpOptions::default();
    if let Some(kwargs) = kwargs {
        for (name, value) in kwargs {
            let name = name.extract::<String>()?;
            match name.as_str() {
                "dimensions" => {
                    options.dimensions =
                        embedding_count(py, "fast_random_projection", &name, &value)?;
                }
                "iteration_weights" => options.iteration_weights = value.extract()?,
                "normalization_strength" => options.normalization_strength = value.extract()?,
                "feature_weight" => options.feature_weight = value.extract()?,
                "feature_properties" => {
                    options.feature_properties =
                        embedding_strings(py, "fast_random_projection", &name, &value)?;
                }
                "seed" => {
                    options.seed = embedding_seed(py, "fast_random_projection", &value)?;
                }
                _ => {
                    return Err(embedding_validation(
                        py,
                        format!("unknown fast_random_projection option {name:?}"),
                    ));
                }
            }
        }
    }
    Ok(options)
}

fn hashgnn_options(py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<HashGnnOptions> {
    let mut options = HashGnnOptions::default();
    if let Some(kwargs) = kwargs {
        for (name, value) in kwargs {
            let name = name.extract::<String>()?;
            match name.as_str() {
                "dimensions" => {
                    options.dimensions = embedding_count(py, "hashgnn", &name, &value)?;
                }
                "iterations" => {
                    options.iterations = embedding_count(py, "hashgnn", &name, &value)?;
                }
                "embedding_density" => options.embedding_density = value.extract()?,
                "heterogeneous" => options.heterogeneous = value.extract()?,
                "node_type_property" => options.node_type_property = value.extract()?,
                "relationship_type_property" => {
                    options.relationship_type_property = value.extract()?;
                }
                "seed" => options.seed = embedding_seed(py, "hashgnn", &value)?,
                _ => {
                    return Err(embedding_validation(
                        py,
                        format!("unknown hashgnn option {name:?}"),
                    ));
                }
            }
        }
    }
    Ok(options)
}

pub(super) fn embedding_options_from_kwargs(
    py: Python<'_>,
    by: graphforge_api::AnalyzeAlgorithm,
    via: Option<&str>,
    directed: bool,
    weight: Option<&str>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<EmbeddingAnalyzeOptions> {
    let options = match by {
        graphforge_api::AnalyzeAlgorithm::Node2Vec => {
            EmbeddingOptions::Node2Vec(node2vec_options(py, kwargs)?)
        }
        graphforge_api::AnalyzeAlgorithm::GraphSage => {
            EmbeddingOptions::GraphSage(graphsage_options(py, kwargs)?)
        }
        graphforge_api::AnalyzeAlgorithm::FastRandomProjection => {
            EmbeddingOptions::FastRandomProjection(fastrp_options(py, kwargs)?)
        }
        graphforge_api::AnalyzeAlgorithm::HashGnn => {
            EmbeddingOptions::HashGnn(hashgnn_options(py, kwargs)?)
        }
        _ => {
            return Err(embedding_validation(
                py,
                format!("{by} is not an embedding algorithm"),
            ));
        }
    };
    Ok(EmbeddingAnalyzeOptions {
        by,
        via: via.map(str::to_owned),
        directed,
        weight: weight.map(str::to_owned),
        options,
    })
}

pub(super) struct ConfiguredProviderBinding {
    session: OpenRouterProviderSession,
    request_limits: ProviderRequestLimits,
    execution_limits: ProviderExecutionLimits,
}

fn provider_capabilities(values: Option<Vec<String>>) -> Result<ProviderCapabilities, GfError> {
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
            _ => Err(GfError::Validation(format!(
                "unknown provider capability {value:?}"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    ProviderCapabilities::new(values).map_err(Into::into)
}

fn provider_plan_request(
    configured: &ConfiguredProviderBinding,
    name: &str,
    label: &str,
    properties: Vec<String>,
    dimensions: u32,
    normalization: &str,
    replace: bool,
) -> Result<ProviderEmbeddingPlanRequest, GfError> {
    let normalization = match normalization {
        "none" => ProviderEmbeddingNormalization::None,
        "l2" => ProviderEmbeddingNormalization::L2,
        other => {
            return Err(GfError::Validation(format!(
                "unknown provider embedding normalization {other:?}"
            )));
        }
    };
    Ok(ProviderEmbeddingPlanRequest {
        display_name: name.to_owned(),
        label: label.to_owned(),
        properties,
        contract: configured.session.contract().clone(),
        dimensions,
        normalization,
        distance: ProviderEmbeddingDistance::Cosine,
        request_limits: configured.request_limits,
        batch_limits: ProviderBatchLimits::default(),
        execution_limits: configured.execution_limits,
        replace_alias: replace,
    })
}

fn provider_execution_limits_to_python(
    py: Python<'_>,
    limits: ProviderExecutionLimits,
) -> PyResult<Bound<'_, PyDict>> {
    let value = PyDict::new(py);
    value.set_item("provider_calls", limits.provider_calls)?;
    value.set_item("retries", limits.retries)?;
    value.set_item("input_token_exposure", limits.input_token_exposure)?;
    value.set_item(
        "estimated_cost_microunits",
        limits.estimated_cost_microunits,
    )?;
    value.set_item("timeout_millis", limits.timeout.as_millis())?;
    value.set_item(
        "minimum_call_interval_millis",
        limits.minimum_call_interval.as_millis(),
    )?;
    value.set_item("retry_backoff_millis", limits.retry_backoff.as_millis())?;
    value.set_item(
        "maximum_retry_backoff_millis",
        limits.maximum_retry_backoff.as_millis(),
    )?;
    Ok(value)
}

fn provider_plan_to_python(
    py: Python<'_>,
    inspection: ProviderEmbeddingPlanInspection,
) -> PyResult<Py<PyAny>> {
    let value = PyDict::new(py);
    value.set_item("display_name", inspection.display_name)?;
    value.set_item("compatibility_id", inspection.compatibility_id)?;
    value.set_item("source_fingerprint", inspection.source_fingerprint)?;
    value.set_item("graph_generation", inspection.graph_generation)?;
    value.set_item("label", inspection.label)?;
    value.set_item("properties", inspection.properties)?;
    value.set_item("provider", inspection.provider)?;
    value.set_item("model", inspection.model)?;
    value.set_item("revision", inspection.revision)?;
    value.set_item(
        "response_contract_version",
        inspection.response_contract_version,
    )?;
    value.set_item("tokenizer_identifier", inspection.tokenizer_identifier)?;
    value.set_item("tokenizer_version", inspection.tokenizer_version)?;
    value.set_item(
        "token_count_class",
        match inspection.token_count_class {
            TokenCountClass::ExactLocal => "exact_local",
            TokenCountClass::ProviderReported => "provider_reported",
            TokenCountClass::Approximate => "approximate",
        },
    )?;
    value.set_item("model_input_tokens", inspection.model_input_tokens)?;
    value.set_item(
        "tokenizer_normalization",
        inspection.tokenizer_normalization,
    )?;
    let chunking = inspection.chunking.map(|chunking| {
        let value = PyDict::new(py);
        value.set_item("chunk_size_tokens", chunking.chunk_size_tokens)?;
        value.set_item("overlap_tokens", chunking.overlap_tokens)?;
        value.set_item("aggregation", chunking.aggregation)?;
        value.set_item("truncation_policy", chunking.truncation_policy)?;
        Ok::<_, PyErr>(value)
    });
    value.set_item("chunking", chunking.transpose()?)?;
    value.set_item("dimensions", inspection.dimensions)?;
    value.set_item(
        "normalization",
        match inspection.normalization {
            ProviderEmbeddingNormalization::None => "none",
            ProviderEmbeddingNormalization::L2 => "l2",
        },
    )?;
    value.set_item("distance", "cosine")?;
    value.set_item("selected_nodes", inspection.selected_nodes)?;
    value.set_item("input_bytes", inspection.input_bytes)?;
    value.set_item("input_tokens", inspection.input_tokens)?;
    let batches = inspection
        .batches
        .into_iter()
        .map(|batch| {
            let value = PyDict::new(py);
            value.set_item("items", batch.items)?;
            value.set_item("input_bytes", batch.input_bytes)?;
            value.set_item("input_tokens", batch.input_tokens)?;
            Ok::<_, PyErr>(value)
        })
        .collect::<PyResult<Vec<_>>>()?;
    value.set_item("batches", batches)?;
    let request_limits = PyDict::new(py);
    request_limits.set_item("items", inspection.request_limits.items)?;
    request_limits.set_item("input_bytes", inspection.request_limits.input_bytes)?;
    request_limits.set_item("input_tokens", inspection.request_limits.input_tokens)?;
    request_limits.set_item("output_values", inspection.request_limits.output_values)?;
    request_limits.set_item("provider_calls", inspection.request_limits.provider_calls)?;
    value.set_item("request_limits", request_limits)?;
    let batch_limits = PyDict::new(py);
    batch_limits.set_item("items", inspection.batch_limits.items)?;
    batch_limits.set_item("input_bytes", inspection.batch_limits.input_bytes)?;
    batch_limits.set_item("input_tokens", inspection.batch_limits.input_tokens)?;
    value.set_item("batch_limits", batch_limits)?;
    value.set_item(
        "execution_limits",
        provider_execution_limits_to_python(py, inspection.execution_limits)?,
    )?;
    Ok(value.into_any().unbind())
}

fn emit_find_warnings(py: Python<'_>, diagnostics: &[FindDiagnostic]) -> PyResult<()> {
    for diagnostic in diagnostics {
        let message = match diagnostic {
            FindDiagnostic::ForcedStale { diagnostic } => diagnostic.clone(),
            FindDiagnostic::RerankSuggested { provider, model } => format!(
                "configured reranker {provider}/{model} was omitted; explicit reranking may improve top-result quality"
            ),
        };
        let message = CString::new(message)
            .map_err(|_| PyTypeError::new_err("warning text contained a NUL byte"))?;
        PyErr::warn(py, &py.get_type::<PyRuntimeWarning>(), &message, 1)?;
    }
    Ok(())
}

fn py_rerank_options(
    py: Python<'_>,
    value: &Bound<'_, PyDict>,
    configured: &ConfiguredProviderBinding,
) -> PyResult<FindRerankOptions> {
    const ALLOWED: &[&str] = &["query", "properties", "candidate_depth", "failure_policy"];
    for (key, _) in value.iter() {
        let key = key.extract::<String>()?;
        if !ALLOWED.contains(&key.as_str()) {
            return Err(to_pyerr(
                py,
                &GfError::Validation(format!("unknown rerank option {key:?}")),
            ));
        }
    }
    let required = |key: &str| -> PyResult<Bound<'_, PyAny>> {
        value.get_item(key)?.ok_or_else(|| {
            to_pyerr(
                py,
                &GfError::Validation(format!("rerank option {key:?} is required")),
            )
        })
    };
    let failure_policy = match value
        .get_item("failure_policy")?
        .map(|value| value.extract::<String>())
        .transpose()?
        .as_deref()
        .unwrap_or("error")
    {
        "error" => RerankFailurePolicy::Error,
        "canonical_unreranked" => RerankFailurePolicy::CanonicalUnreranked,
        other => {
            return Err(to_pyerr(
                py,
                &GfError::Validation(format!("unknown rerank failure policy {other:?}")),
            ));
        }
    };
    Ok(FindRerankOptions {
        query: required("query")?.extract()?,
        properties: required("properties")?.extract()?,
        candidate_depth: required("candidate_depth")?.extract()?,
        contract: configured.session.contract().clone(),
        request_limits: configured.request_limits,
        execution_limits: configured.execution_limits,
        failure_policy,
    })
}

#[pymethods]
impl GraphForge {
    /// Configure one opt-in OpenRouter session shared by provider indexing and find.
    #[pyo3(signature = (credential, *, origin, model, revision="unavailable", response_contract_version="v1", capabilities=None, max_input_tokens=1_000_000, transport_timeout_millis=30_000, estimated_cost_microunits_per_token=1))]
    #[allow(clippy::too_many_arguments)]
    fn configure_openrouter(
        &mut self,
        py: Python<'_>,
        credential: String,
        origin: String,
        model: String,
        revision: &str,
        response_contract_version: &str,
        capabilities: Option<Vec<String>>,
        max_input_tokens: u64,
        transport_timeout_millis: u64,
        estimated_cost_microunits_per_token: u64,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let request_limits = ProviderRequestLimits::default();
        let execution_limits = ProviderExecutionLimits::default();
        let config = OpenRouterProviderSessionConfig {
            origin,
            model,
            revision: revision.to_owned(),
            response_contract_version: response_contract_version.to_owned(),
            capabilities: provider_capabilities(capabilities)
                .map_err(|error| to_pyerr(py, &error))?,
            max_input_tokens,
            chunking: None,
            wire_limits: OpenRouterWireLimits::default(),
            request_limits,
            execution_limits,
            transport_timeout: Duration::from_millis(transport_timeout_millis),
            estimated_cost_microunits_per_token,
        };
        let session = py
            .detach(|| OpenRouterProviderSession::new(config, credential))
            .map_err(|error| to_pyerr(py, &error))?;
        self.provider = Some(ConfiguredProviderBinding {
            session,
            request_limits,
            execution_limits,
        });
        Ok(())
    }

    /// Inspect one content-free provider property-embedding plan without network work.
    #[pyo3(signature = (name, label, properties, *, dimensions, normalization="none", replace=false))]
    #[allow(clippy::too_many_arguments)]
    fn inspect_provider_embedding_plan(
        &self,
        py: Python<'_>,
        name: &str,
        label: &str,
        properties: Vec<String>,
        dimensions: u32,
        normalization: &str,
        replace: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let configured = self.provider.as_ref().ok_or_else(|| {
            to_pyerr(
                py,
                &GfError::Validation("OpenRouter is not configured".to_owned()),
            )
        })?;
        let request = provider_plan_request(
            configured,
            name,
            label,
            properties,
            dimensions,
            normalization,
            replace,
        )
        .map_err(|error| to_pyerr(py, &error))?;
        let inspection = py
            .detach(|| {
                configured
                    .session
                    .inspect_embedding_plan(&self.inner, &request)
            })
            .map_err(|error| to_pyerr(py, &GfError::Execution(error.to_string())))?;
        provider_plan_to_python(py, inspection)
    }

    /// Confirm, execute, and atomically publish one provider embedding generation.
    #[pyo3(signature = (name, label, properties, *, dimensions, normalization="none", replace=false))]
    #[allow(clippy::too_many_arguments)]
    fn publish_provider_embeddings(
        &self,
        py: Python<'_>,
        name: &str,
        label: &str,
        properties: Vec<String>,
        dimensions: u32,
        normalization: &str,
        replace: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let configured = self.provider.as_ref().ok_or_else(|| {
            to_pyerr(
                py,
                &GfError::Validation("OpenRouter is not configured".to_owned()),
            )
        })?;
        let request = provider_plan_request(
            configured,
            name,
            label,
            properties,
            dimensions,
            normalization,
            replace,
        )
        .map_err(|error| to_pyerr(py, &error))?;
        let space = py
            .detach(|| configured.session.publish_embeddings(&self.inner, &request))
            .map_err(|error| to_pyerr(py, &GfError::Execution(error.to_string())))?;
        embedding_space_to_python(py, space)
    }

    /// Text + vector hybrid search. Returns a `pyarrow.Table`.
    #[pyo3(signature = (query=None, *, label=None, vector=None, similar_to=None, semantic_query=None, limit=10, space=None, force_stale=false, rerank=None, suppress_rerank_advisory=false))]
    #[allow(clippy::too_many_arguments)]
    fn find(
        &self,
        py: Python<'_>,
        query: Option<&str>,
        label: Option<&str>,
        vector: Option<Vec<f32>>,
        similar_to: Option<&Bound<'_, PyAny>>,
        semantic_query: Option<&str>,
        limit: usize,
        space: Option<&str>,
        force_stale: bool,
        rerank: Option<&Bound<'_, PyDict>>,
        suppress_rerank_advisory: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let similar_to = similar_to
            .map(|value| py_to_node_selector(py, value))
            .transpose()?;
        let opts = graphforge_api::FindOptions {
            query: query.map(str::to_owned),
            label: label.map(str::to_owned),
            vector,
            similar_to,
            semantic_query: semantic_query.map(str::to_owned),
            limit,
            space: space.map(str::to_owned),
            force_stale,
        };
        let rerank = match (rerank, self.provider.as_ref()) {
            (Some(value), Some(configured)) => Some(py_rerank_options(py, value, configured)?),
            (Some(_), None) => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(
                        "rerank requires a configured OpenRouter session".to_owned(),
                    ),
                ));
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
            find: opts,
            rerank,
            omitted_reranker,
            advisory_policy: if suppress_rerank_advisory {
                RerankAdvisoryPolicy::Suppress
            } else {
                RerankAdvisoryPolicy::Emit
            },
        };
        let result = match self.provider.as_ref() {
            Some(configured) => py.detach(|| configured.session.find(&self.inner, execution)),
            None => py.detach(|| self.inner.find_with_diagnostics(execution, None)),
        }
        .map_err(|error| to_pyerr(py, &error))?;
        let (batch, diagnostics, _) = result.into_parts();
        emit_find_warnings(py, &diagnostics)?;
        algorithm_result(py, Ok(batch))
    }

    /// Atomically publish one complete caller-supplied UUID/vector generation.
    #[pyo3(signature = (name, rows, *, dimensions, source_projection, contract_version="graphforge_binding_caller_v1", normalization="none", replace=false))]
    #[allow(clippy::too_many_arguments)]
    fn publish_caller_embeddings(
        &self,
        py: Python<'_>,
        name: &str,
        rows: &Bound<'_, PyList>,
        dimensions: u32,
        source_projection: &Bound<'_, PyDict>,
        contract_version: &str,
        normalization: &str,
        replace: bool,
    ) -> PyResult<String> {
        self.ensure_open()?;
        let normalization = match normalization {
            "none" => CallerEmbeddingNormalization::None,
            "l2" => CallerEmbeddingNormalization::L2,
            other => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(format!(
                        "unknown caller embedding normalization {other:?}"
                    )),
                ));
            }
        };
        let request = CallerEmbeddingBatchRequest {
            display_name: name.to_owned(),
            contract_version: contract_version.to_owned(),
            dimensions,
            normalization,
            distance: CallerEmbeddingDistance::Cosine,
            source_projection_recipe: string_map(source_projection)?,
            rows: caller_embedding_rows(py, rows)?,
            replace_alias: replace,
        };
        let published = py
            .detach(|| self.inner.publish_caller_embeddings(request))
            .map_err(|error| to_pyerr(py, &error))?;
        Ok(published.compatibility_id)
    }

    /// Atomically publish one complete canonical algorithm embedding Arrow result.
    #[pyo3(signature = (name, result, *, algorithm, algorithm_version, dimensions, input_recipe, source_projection, hyperparameters=None, normalization="none", replace=false))]
    #[allow(clippy::too_many_arguments)]
    fn publish_algorithm_embeddings(
        &self,
        py: Python<'_>,
        name: &str,
        result: &Bound<'_, PyAny>,
        algorithm: &str,
        algorithm_version: &str,
        dimensions: u32,
        input_recipe: &Bound<'_, PyDict>,
        source_projection: &Bound<'_, PyDict>,
        hyperparameters: Option<&Bound<'_, PyDict>>,
        normalization: &str,
        replace: bool,
    ) -> PyResult<String> {
        self.ensure_open()?;
        let normalization = match normalization {
            "none" => AlgorithmEmbeddingNormalization::None,
            "l2" => AlgorithmEmbeddingNormalization::L2,
            other => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(format!(
                        "unknown algorithm embedding normalization {other:?}"
                    )),
                ));
            }
        };
        let algorithm = algorithm.parse().map_err(|error| to_pyerr(py, &error))?;
        let request = AlgorithmEmbeddingPublicationRequest {
            display_name: name.to_owned(),
            algorithm,
            algorithm_version: algorithm_version.to_owned(),
            dimensions,
            normalization,
            distance: AlgorithmEmbeddingDistance::Cosine,
            hyperparameters: json_map(hyperparameters)?,
            input_recipe: json_map(Some(input_recipe))?,
            source_projection_recipe: json_map(Some(source_projection))?,
            result: pyarrow_table_to_batch(result)?,
            replace_alias: replace,
        };
        let published = py
            .detach(|| self.inner.publish_algorithm_embeddings(request))
            .map_err(|error| to_pyerr(py, &error))?;
        Ok(published.compatibility_id)
    }

    /// List verified embedding-space lineages in deterministic Rust order.
    fn embedding_spaces(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.ensure_open()?;
        py.detach(|| self.inner.embedding_spaces())
            .map_err(|error| to_pyerr(py, &error))?
            .into_iter()
            .map(|space| embedding_space_to_python(py, space))
            .collect()
    }

    /// Inspect one embedding-space alias, or the configured default.
    #[pyo3(signature = (name=None))]
    fn embedding_space(&self, py: Python<'_>, name: Option<&str>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let name = name.map(str::to_owned);
        let space = py
            .detach(|| self.inner.embedding_space(name.as_deref()))
            .map_err(|error| to_pyerr(py, &error))?;
        embedding_space_to_python(py, space)
    }

    /// Bind one alias to an existing verified compatibility lineage.
    #[pyo3(signature = (name, compatibility_id, *, replace=false))]
    fn bind_embedding_space_alias(
        &self,
        py: Python<'_>,
        name: &str,
        compatibility_id: &str,
        replace: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let name = name.to_owned();
        let compatibility_id = compatibility_id.to_owned();
        let space = py
            .detach(|| {
                self.inner
                    .bind_embedding_space_alias(&name, &compatibility_id, replace)
            })
            .map_err(|error| to_pyerr(py, &error))?;
        embedding_space_to_python(py, space)
    }

    /// Remove one alias without deleting primary vector data.
    fn remove_embedding_space_alias(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        self.ensure_open()?;
        let name = name.to_owned();
        py.detach(|| self.inner.remove_embedding_space_alias(&name))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Delete one named/default compatibility lineage and every targeting alias.
    #[pyo3(signature = (name=None))]
    fn delete_embedding_space(&self, py: Python<'_>, name: Option<&str>) -> PyResult<bool> {
        self.ensure_open()?;
        let name = name.map(str::to_owned);
        py.detach(|| self.inner.delete_embedding_space(name.as_deref()))
            .map_err(|error| to_pyerr(py, &error))
    }

    /// Select one existing alias as default, or clear the default with `None`.
    #[pyo3(signature = (name=None))]
    fn set_default_embedding_space(
        &self,
        py: Python<'_>,
        name: Option<&str>,
    ) -> PyResult<Option<Py<PyAny>>> {
        self.ensure_open()?;
        let name = name.map(str::to_owned);
        py.detach(|| self.inner.set_default_embedding_space(name.as_deref()))
            .map_err(|error| to_pyerr(py, &error))?
            .map(|space| embedding_space_to_python(py, space))
            .transpose()
    }

    /// Inspect one active embedding generation's Rust-owned freshness decision.
    #[pyo3(signature = (name=None, *, force_stale=false))]
    fn inspect_embedding_space_freshness(
        &self,
        py: Python<'_>,
        name: Option<&str>,
        force_stale: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let name = name.map(str::to_owned);
        let freshness = py
            .detach(|| {
                self.inner
                    .inspect_embedding_space_freshness(name.as_deref(), force_stale)
            })
            .map_err(|error| to_pyerr(py, &error))?;
        refresh_freshness_to_python(py, freshness)
    }

    /// Read the durable project-wide embedding refresh defaults.
    fn embedding_refresh_project_policy(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let policy = py
            .detach(|| self.inner.embedding_refresh_project_policy())
            .map_err(|error| to_pyerr(py, &error))?;
        refresh_project_policy_to_python(py, policy)
    }

    /// Replace the durable project-wide embedding refresh defaults.
    #[pyo3(signature = (*, proactive, debounce_millis, max_concurrent_jobs))]
    fn set_embedding_refresh_project_policy(
        &self,
        py: Python<'_>,
        proactive: bool,
        debounce_millis: u64,
        max_concurrent_jobs: usize,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let policy = py
            .detach(|| {
                self.inner
                    .set_embedding_refresh_project_policy(EmbeddingRefreshProjectPolicy {
                        proactive,
                        debounce: Duration::from_millis(debounce_millis),
                        max_concurrent_jobs,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        refresh_project_policy_to_python(py, policy)
    }

    /// Set or explicitly clear one lineage's durable refresh override.
    #[pyo3(signature = (name=None, *, proactive=None, debounce_millis=None, clear=false))]
    fn set_embedding_refresh_space_policy(
        &self,
        py: Python<'_>,
        name: Option<&str>,
        proactive: Option<bool>,
        debounce_millis: Option<u64>,
        clear: bool,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let policy = if clear {
            if proactive.is_some() || debounce_millis.is_some() {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(
                        "clearing an embedding refresh space policy cannot include overrides"
                            .to_owned(),
                    ),
                ));
            }
            None
        } else {
            if proactive.is_none() && debounce_millis.is_none() {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation(
                        "embedding refresh space policy requires an override or clear=True"
                            .to_owned(),
                    ),
                ));
            }
            Some(EmbeddingRefreshSpacePolicy {
                proactive,
                debounce: debounce_millis.map(Duration::from_millis),
            })
        };
        let name = name.map(str::to_owned);
        let inspection = py
            .detach(|| {
                self.inner
                    .set_embedding_refresh_space_policy(name.as_deref(), policy)
            })
            .map_err(|error| to_pyerr(py, &error))?;
        refresh_inspection_to_python(py, inspection)
    }

    /// Inspect durable refresh state and this process's worker counters.
    #[pyo3(signature = (name=None))]
    fn inspect_embedding_refresh(&self, py: Python<'_>, name: Option<&str>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let name = name.map(str::to_owned);
        let inspection = py
            .detach(|| self.inner.inspect_embedding_refresh(name.as_deref()))
            .map_err(|error| to_pyerr(py, &error))?;
        refresh_inspection_to_python(py, inspection)
    }

    /// Build, reuse, or replace one graph-native text/vector search index.
    ///
    /// The legacy no-keyword `index("adjacency")` call remains compatible;
    /// new code should use `index_adjacency()` for the unambiguous operation.
    #[pyo3(signature = (label, **kwargs))]
    fn index(
        &self,
        py: Python<'_>,
        label: &str,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let label = label.to_owned();
        if kwargs.is_none_or(pyo3::types::PyDictMethods::is_empty) && label == "adjacency" {
            py.detach(|| self.inner.index(&label))
                .map_err(|error| to_pyerr(py, &error))?;
            return Ok(py.None());
        }
        let options = search_index_options_from_kwargs(py, kwargs)?;
        let receipt = py
            .detach(|| self.inner.index_search(&label, options))
            .map_err(|error| to_pyerr(py, &error))?;
        receipt.map_or_else(
            || Ok(py.None()),
            |value| text_index_inspection_to_python(py, value),
        )
    }

    /// Inspect a graph-native text index without building it.
    #[pyo3(signature = (label, *, properties=None))]
    #[allow(clippy::needless_pass_by_value)]
    fn inspect_text_index(
        &self,
        py: Python<'_>,
        label: &str,
        properties: Option<Vec<String>>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let label = label.to_owned();
        let inspection = py
            .detach(|| self.inner.inspect_text_index(&label, properties.as_deref()))
            .map_err(|error| to_pyerr(py, &error))?;
        text_index_inspection_to_python(py, inspection)
    }

    /// Explicitly build the derived CSR adjacency index.
    fn index_adjacency(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let inspection = py
            .detach(|| self.inner.index_adjacency())
            .map_err(|error| to_pyerr(py, &error))?;
        adjacency_inspection_to_python(py, inspection)
    }

    /// Inspect the derived adjacency index without rebuilding it.
    fn inspect_adjacency(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let inspection = py
            .detach(|| self.inner.inspect_adjacency())
            .map_err(|error| to_pyerr(py, &error))?;
        adjacency_inspection_to_python(py, inspection)
    }

    /// Rebuild adjacency with an optional shared cancellation token.
    #[pyo3(signature = (*, cancellation=None))]
    fn rebuild_adjacency(
        &self,
        py: Python<'_>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let inspection = py
            .detach(|| self.inner.rebuild_adjacency(cancellation))
            .map_err(|error| to_pyerr(py, &error))?;
        adjacency_inspection_to_python(py, inspection)
    }
}
