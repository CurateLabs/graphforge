//! GraphForge Node.js bindings via napi-rs.
// napi-derive macros expand to unsafe FFI code; unsafe is permitted here but audited.
#![warn(unsafe_code)]
// napi-derive deserializes JS arguments into owned Rust values, so `#[napi]`
// methods take their args by value even when only borrowed in the body.
#![allow(clippy::needless_pass_by_value)]

use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use arrow::compute::concat_batches;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use gf_api::{
    AnalyzeAlgorithm, AnalyzeOptions, AssertionGraphRefInput, AssertionGraphRole,
    AttachResolvedRunRequest, BeliefProjectionPolicyV1, BeliefSubjectV1, BulkEdgePublicationError,
    BulkNodePublicationError, CallerEmbeddingBatchRequest, CallerEmbeddingBatchRow,
    CallerEmbeddingDistance, CallerEmbeddingNormalization, CapabilityId, EmbeddingAnalyzeOptions,
    EmbeddingOptions, EmbeddingRefreshFailureClass, EmbeddingRefreshInspection,
    EmbeddingRefreshOutcomeStatus, EmbeddingRefreshProjectPolicy, EmbeddingRefreshSpacePolicy,
    EmbeddingRefreshWorkerState, EmbeddingSpaceFreshnessInspection, EmbeddingSpaceFreshnessState,
    EmbeddingSpaceInfo, EmbeddingSpaceProducer, EmbeddingSpaceReadDecision,
    EmbeddingTokenCountClass, ExecutionResult, FastRpOptions, FindDiagnostic, FindExecutionOptions,
    FindOptions, FindRerankOptions, GfError, GraphForgeOptions, GraphObjectKind,
    GraphSageAggregator, GraphSageOptions, HashGnnOptions, InvocationDescriptor, InvocationError,
    IrLiteral, M18EmbeddingDistance, M18EmbeddingNormalization, M18EmbeddingPublicationRequest,
    Node2VecOptions, NodeSelector, OpenRouterProviderSession, OpenRouterProviderSessionConfig,
    OpenRouterWireLimits, OperationId, PathsOptions, ProjectWriteMode, PropValue,
    ProviderBatchLimits, ProviderCapabilities, ProviderCapability, ProviderEmbeddingDistance,
    ProviderEmbeddingNormalization, ProviderEmbeddingPlanInspection, ProviderEmbeddingPlanRequest,
    ProviderExecutionLimits, ProviderRequestLimits, RerankAdvisoryPolicy, RerankFailurePolicy,
    ResolveBeliefProjectionRequest, ResolveBeliefSubjectRequest, ResolvedAttachmentOutcome,
    ResolvedBeliefProjection, ResolvedBeliefSubject, ResolvedRecordedAlgorithmRequest,
    SearchIndexOptions, SimilarOptions, StatuslessPolicyV1, SupersessionBranchPolicyV1,
    TextIndexInspection, TokenCountClass, WriteContext, algorithm_descriptor_contracts,
    validate_embedding_options,
};
use napi::bindgen_prelude::{
    AbortSignal, AsyncTask, BigInt, Buffer, ClassInstance, Either3, Function, JsObjectValue, Object,
};
use napi::{Env, Task};
use napi_derive::napi;

mod composite;
mod error;
use composite::CompositeTransactionInput;
use error::{NodeError, to_napi_err};

/// Result alias whose error surfaces a typed JS `error.code` (see [`error`]).
pub(crate) type Result<T> = std::result::Result<T, NodeError>;

pub(crate) fn napi_validation(message: &'static str) -> NodeError {
    to_napi_err(&GfError::Validation(message.into()))
}

fn project_write_mode(value: &str) -> Result<ProjectWriteMode> {
    match value {
        "single_writer" => Ok(ProjectWriteMode::SingleWriter),
        "queued_writer" => Ok(ProjectWriteMode::QueuedWriter),
        "optimistic_multi_writer" => Ok(ProjectWriteMode::OptimisticMultiWriter),
        _ => Err(napi_validation(
            "writeMode must be single_writer, queued_writer, or optimistic_multi_writer",
        )),
    }
}

#[napi(object)]
/// Embedded project-write construction options.
pub struct GraphForgeOptionsInput {
    /// Write coordination policy name.
    pub write_mode: Option<String>,
    /// Maximum number of queued same-instance writers.
    pub write_queue_capacity: Option<i32>,
    /// Maximum optimistic rebase attempts after initial staging.
    pub max_rebase_attempts: Option<i32>,
}

#[napi(object)]
/// One portable runtime-catalog observation.
pub struct RuntimeCatalogEntryOutput {
    /// Stable observation category.
    pub kind: String,
    /// Observed label, relation type, or property name.
    pub name: String,
    /// Owning entity label for a property.
    pub owner: Option<String>,
    /// Number of observations recorded by the catalog.
    pub observation_count: BigInt,
}

#[napi(object)]
/// Frozen, deterministically ordered runtime-catalog contract.
pub struct RuntimeCatalogSnapshotOutput {
    /// Frozen contract version.
    pub contract_version: u32,
    /// Entries in deterministic kind, owner, and name order.
    pub entries: Vec<RuntimeCatalogEntryOutput>,
}

#[napi(object)]
/// Conservative non-authoritative ontology suggestion.
pub struct OntologySuggestionOutput {
    /// Always true: the result is not authoritative until explicitly adopted.
    pub draft: bool,
    /// Canonically ordered Rust-owned ontology document.
    pub document: serde_json::Value,
    /// SHA-256 of the canonical JSON document bytes.
    pub fingerprint_sha256: String,
    /// Relations omitted because the catalog lacks endpoint evidence.
    pub omitted_relation_types: Vec<String>,
}

#[napi(object)]
/// One semantic ontology validation diagnostic.
pub struct OntologyValidationDiagnosticOutput {
    /// Stable semantic diagnostic category.
    pub kind: String,
    /// Human-readable ontology field location.
    pub location: String,
    /// Human-readable diagnostic detail.
    pub message: String,
}

#[napi(object)]
/// Complete non-mutating ontology validation result.
pub struct OntologyValidationReportOutput {
    /// Whether the document passed semantic validation.
    pub valid: bool,
    /// Complete diagnostics in Rust validator order.
    pub diagnostics: Vec<OntologyValidationDiagnosticOutput>,
}

#[napi(object)]
/// Generation-managed authoritative ontology record.
pub struct WorkspaceOntologyOutput {
    /// Frozen workspace-record contract version.
    pub contract_version: u32,
    /// Explicit persisted mode: none, advisory, or strict.
    pub mode: String,
    /// Original adopted source syntax.
    pub source_format: Option<String>,
    /// SHA-256 of the canonical adopted document.
    pub canonical_ontology_sha256: Option<String>,
    /// Canonical adopted document, absent in none mode.
    pub canonical_ontology: Option<serde_json::Value>,
}

fn to_napi_invocation_err(error: &InvocationError) -> NodeError {
    match error {
        InvocationError::Graph(error) => to_napi_err(error),
        _ => napi::Error::new(error.code().to_owned(), error.to_string()),
    }
}

fn bulk_node_publication_error(error: BulkNodePublicationError) -> NodeError {
    match error {
        BulkNodePublicationError::Validation(error) => {
            to_napi_err(&GfError::Validation(error.to_string()))
        }
        BulkNodePublicationError::Publication(error) => to_napi_err(&error),
    }
}

fn bulk_edge_publication_error(error: BulkEdgePublicationError) -> NodeError {
    match error {
        BulkEdgePublicationError::Validation(error) => {
            to_napi_err(&GfError::Validation(error.to_string()))
        }
        BulkEdgePublicationError::Publication(error) => to_napi_err(&error),
    }
}

/// Serialize an execution result to an Arrow IPC **stream** Buffer. The stream
/// preamble carries the schema (incl. the `graphforge.*` metadata); a zero-row
/// result still emits a valid schema-only stream. JS decodes it with
/// apache-arrow `tableFromIPC`.
fn result_to_ipc(result: &ExecutionResult) -> std::result::Result<Vec<u8>, GfError> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, result.schema.as_ref())
            .map_err(|e| GfError::Execution(e.to_string()))?;
        for batch in &result.batches {
            writer
                .write(batch)
                .map_err(|e| GfError::Execution(e.to_string()))?;
        }
        writer
            .finish()
            .map_err(|e| GfError::Execution(e.to_string()))?;
    }
    Ok(buf)
}

/// Serialize one native analyst-verb batch without changing its schema.
pub(crate) fn record_batch_to_ipc(
    batch: &arrow::record_batch::RecordBatch,
) -> std::result::Result<Vec<u8>, GfError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|error| GfError::Execution(error.to_string()))?;
        writer
            .write(batch)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| GfError::Execution(error.to_string()))?;
    }
    Ok(buf)
}

/// Decode and coalesce one Arrow IPC stream without changing its schema metadata.
fn ipc_to_record_batch(data: &Buffer) -> Result<arrow::record_batch::RecordBatch> {
    let mut reader = StreamReader::try_new(Cursor::new(data.as_ref()), None).map_err(|error| {
        to_napi_err(&GfError::Validation(format!(
            "invalid Arrow IPC stream: {error}"
        )))
    })?;
    let schema = reader.schema();
    let batches = reader
        .by_ref()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            to_napi_err(&GfError::Validation(format!(
                "invalid Arrow IPC record batch: {error}"
            )))
        })?;
    concat_batches(&schema, &batches).map_err(|error| {
        to_napi_err(&GfError::Validation(format!(
            "cannot coalesce Arrow IPC batches: {error}"
        )))
    })
}

/// Convert a JSON query-parameter value to the matching [`IrLiteral`].
/// `serde_json::Number` doesn't distinguish int/float, so check `is_i64` first
/// (mirrors the Python binding's bool-before-int, int-before-float ordering).
fn json_to_ir_literal(value: &serde_json::Value) -> Result<IrLiteral> {
    use serde_json::Value;
    Ok(match value {
        Value::Null => IrLiteral::Null,
        Value::Bool(b) => IrLiteral::Bool(*b),
        Value::Number(n) if n.is_i64() => IrLiteral::Int(n.as_i64().unwrap()),
        Value::Number(n) => {
            IrLiteral::Float(n.as_f64().ok_or_else(|| {
                to_napi_err(&GfError::Validation("non-finite numeric param".into()))
            })?)
        }
        Value::String(s) => IrLiteral::Str(s.clone()),
        Value::Array(items) => IrLiteral::List(
            items
                .iter()
                .map(json_to_ir_literal)
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Object(entries) if entries.contains_key("$uuid") => {
            if entries.len() != 1 {
                return Err(to_napi_err(&GfError::Validation(
                    "UUID parameter tag must contain only $uuid".into(),
                )));
            }
            let text = entries["$uuid"].as_str().ok_or_else(|| {
                to_napi_err(&GfError::Validation(
                    "UUID parameter $uuid value must be a string".into(),
                ))
            })?;
            let uuid = uuid::Uuid::parse_str(text).map_err(|_| {
                to_napi_err(&GfError::Validation(
                    "UUID parameter must be canonical hyphenated UUID text".into(),
                ))
            })?;
            if uuid.hyphenated().to_string() != text {
                return Err(to_napi_err(&GfError::Validation(
                    "UUID parameter must be canonical hyphenated UUID text".into(),
                )));
            }
            IrLiteral::Uuid(*uuid.as_bytes())
        }
        Value::Object(entries) => IrLiteral::Map(
            entries
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_to_ir_literal(value)?)))
                .collect::<Result<Vec<_>>>()?,
        ),
    })
}

/// Build the `$param` map from an optional JS object (empty when omitted).
fn params_from_map(
    params: Option<HashMap<String, serde_json::Value>>,
) -> Result<HashMap<String, IrLiteral>> {
    let mut out = HashMap::new();
    if let Some(map) = params {
        for (k, v) in &map {
            out.insert(k.clone(), json_to_ir_literal(v)?);
        }
    }
    Ok(out)
}

fn ontology_mode(value: &str) -> Result<gf_api::OntologyMode> {
    match value {
        "advisory" => Ok(gf_api::OntologyMode::Advisory),
        "strict" => Ok(gf_api::OntologyMode::Strict),
        _ => Err(to_napi_err(&GfError::Validation(
            "ontology mode must be advisory or strict".into(),
        ))),
    }
}

fn ontology_export_format(value: &str) -> Result<gf_api::OntologyExportFormat> {
    match value {
        "yaml" | "yml" => Ok(gf_api::OntologyExportFormat::Yaml),
        "json" => Ok(gf_api::OntologyExportFormat::Json),
        _ => Err(to_napi_err(&GfError::Validation(
            "ontology export format must be yaml or json".into(),
        ))),
    }
}

fn embedding_space_to_json(space: EmbeddingSpaceInfo) -> serde_json::Value {
    let producer = match space.producer {
        EmbeddingSpaceProducer::M18 {
            algorithm,
            algorithm_version,
        } => serde_json::json!({
            "kind": "m18",
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
        "reason": inspection.reason.map(gf_api::TextIndexFreshnessReason::as_str),
    })
}

fn adjacency_inspection_to_json(inspection: gf_api::AdjacencyInspection) -> serde_json::Value {
    serde_json::json!({
        "projectGenerationUuid": inspection.project_generation_uuid.to_string(),
        "sourceTopologyGeneration": inspection.source_topology_generation,
        "sourceTopologyFingerprint": inspection.source_topology_fingerprint,
        "artifactSourceGeneration": inspection.artifact_source_generation,
        "artifactEffectiveGeneration": inspection.artifact_effective_generation,
        "artifactFingerprint": inspection.artifact_fingerprint,
        "state": inspection.state.as_str(),
        "reason": inspection.reason.map(gf_api::AdjacencyFreshnessReason::as_str),
    })
}

/// Convert one JSON construction value into the shared Rust property model.
fn json_to_prop_value(value: &serde_json::Value) -> Result<PropValue> {
    use serde_json::Value;
    Ok(match value {
        Value::Null => PropValue::Null,
        Value::Bool(value) => PropValue::Bool(*value),
        Value::Number(value) if value.is_i64() => PropValue::Int(value.as_i64().unwrap()),
        Value::Number(value) if value.is_u64() => {
            return Err(to_napi_err(&GfError::Validation(
                "integer node property exceeds signed 64-bit range".into(),
            )));
        }
        Value::Number(value) => {
            PropValue::Float(value.as_f64().ok_or_else(|| {
                to_napi_err(&GfError::Validation("non-finite node property".into()))
            })?)
        }
        Value::String(value) => PropValue::Str(value.clone()),
        Value::Array(values) => PropValue::List(
            values
                .iter()
                .map(json_to_prop_value)
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Object(_) => {
            return Err(to_napi_err(&GfError::Validation(
                "unsupported node property type (expected null/boolean/number/string/array)".into(),
            )));
        }
    })
}

pub(crate) fn props_from_map(
    props: Option<HashMap<String, serde_json::Value>>,
) -> Result<HashMap<String, PropValue>> {
    props
        .unwrap_or_default()
        .into_iter()
        .map(|(name, value)| Ok((name, json_to_prop_value(&value)?)))
        .collect()
}

type EmbeddingInput = HashMap<String, serde_json::Value>;

fn embedding_error(message: impl Into<String>) -> NodeError {
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

fn embedding_options_from_input(
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

type NodeSelectorInput<'env> =
    Either3<String, ClassInstance<'env, NodeHandle>, HashMap<String, serde_json::Value>>;

/// Thin Node request for atomic capability initialization.
#[napi(object)]
pub struct EnableCapabilityInput {
    /// Required idempotency UUID.
    pub operation_uuid: String,
    /// Registered lowercase capability ID.
    pub capability_id: String,
    /// Requested capability contract version.
    pub capability_version: u32,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for durable checkpoint creation.
#[napi(object)]
pub struct CheckpointInput {
    /// Canonical checkpoint name.
    pub name: String,
    /// Optional bounded description.
    pub description: Option<String>,
    /// Required idempotency UUID.
    pub idempotency_key: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for deterministic checkpoint listing.
#[napi(object, object_to_js = false)]
pub struct ListCheckpointsInput {
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque cursor returned in Arrow schema metadata.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for durable checkpoint deletion.
#[napi(object)]
pub struct DeleteCheckpointInput {
    /// Exact active checkpoint name.
    pub name: String,
    /// Required idempotency UUID.
    pub idempotency_key: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for complete-workspace checkpoint restoration.
#[napi(object)]
pub struct RevertCheckpointInput {
    /// Exact active checkpoint name.
    pub name: String,
    /// Required non-empty restoration reason.
    pub reason: String,
    /// Required idempotency UUID.
    pub idempotency_key: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for deterministic checkpoint diffing.
#[napi(object, object_to_js = false)]
pub struct DiffCheckpointsInput {
    /// Checkpoint name or the reserved selector `current`.
    pub from: String,
    /// Checkpoint name or the reserved selector `current`.
    pub to: String,
    /// `summary`, `graph`, `ontology`, `configuration`, `capabilities`,
    /// `provenance`, `knowledge`, `epistemic`, or `all`.
    pub scope: String,
    /// `summary` or `records`.
    pub detail: String,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque cursor returned in Arrow schema metadata.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node provenance-history page and filter request.
#[napi(object, object_to_js = false)]
pub struct ProvenanceHistoryInput {
    /// Optional referenced graph/knowledge UUID.
    pub subject_uuid: Option<String>,
    /// Optional operation/idempotency UUID.
    pub operation_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque cursor returned in Arrow schema metadata.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// One thin assertion-to-graph reference.
#[napi(object)]
pub struct AssertionGraphRefInputJs {
    /// Canonical graph UUID.
    pub graph_uuid: String,
    /// `node` or `edge`.
    pub graph_kind: String,
    /// `subject`, `object`, or `context`.
    pub role: String,
    /// Contiguous position within the role.
    pub ordinal: u32,
}

/// Thin Node request for atomic assertion creation.
#[napi(object)]
pub struct CreateAssertionInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 assertion identity.
    pub assertion_uuid: String,
    /// Exact claim text.
    pub claim: String,
    /// Ordered graph UUID references.
    pub graph_refs: Vec<AssertionGraphRefInputJs>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin assertion-list filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListAssertionsInput {
    /// Optional referenced graph UUID.
    pub graph_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin page request for one assertion's graph references.
#[napi(object, object_to_js = false)]
pub struct AssertionGraphRefsInput {
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for atomic confidence assessment.
#[napi(object)]
pub struct AssessConfidenceInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 confidence identity.
    pub confidence_uuid: String,
    /// Existing assertion UUID.
    pub assertion_uuid: String,
    /// `explicit` or `conservative_min`.
    pub policy: String,
    /// Required only by `explicit`.
    pub value: Option<f64>,
    /// Requested immutable inputs for `conservative_min`.
    pub input_confidence_uuids: Option<Vec<String>>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin confidence-list filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListConfidenceAssessmentsInput {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin page request for one confidence input snapshot.
#[napi(object, object_to_js = false)]
pub struct ConfidenceInputsInput {
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for one immutable evidence link.
#[napi(object)]
pub struct AttachEvidenceInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 evidence identity.
    pub evidence_uuid: String,
    /// Existing assertion UUID.
    pub assertion_uuid: String,
    /// Caller-managed source UUID.
    pub source_uuid: String,
    /// `document`, `observation`, `graph_node`, or `graph_edge`.
    pub source_kind: String,
    /// `supports`, `contradicts`, or `context`.
    pub role: String,
    /// Optional finite metadata weight in `[0, 1]`.
    pub weight: Option<f64>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// One evidence row in an atomic assertion bundle.
#[napi(object)]
pub struct EvidenceInputJs {
    /// Caller-supplied UUIDv7 evidence identity.
    pub evidence_uuid: String,
    /// Caller-managed source UUID.
    pub source_uuid: String,
    /// Closed evidence source kind.
    pub source_kind: String,
    /// Closed evidence role.
    pub role: String,
    /// Optional finite metadata weight.
    pub weight: Option<f64>,
}

/// Thin Node request for atomic assertion-plus-evidence creation.
#[napi(object)]
pub struct CreateAssertionWithEvidenceInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 assertion identity.
    pub assertion_uuid: String,
    /// Exact claim text.
    pub claim: String,
    /// Ordered graph UUID references.
    pub graph_refs: Vec<AssertionGraphRefInputJs>,
    /// Non-empty evidence bundle.
    pub evidence: Vec<EvidenceInputJs>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin evidence-list filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListEvidenceLinksInput {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<String>,
    /// Optional source UUID filter.
    pub source_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for one immutable M21 reasoning record.
#[napi(object)]
pub struct RecordReasoningInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 reasoning identity.
    pub reasoning_uuid: String,
    /// Existing assertion UUID.
    pub assertion_uuid: String,
    /// Closed reasoning kind.
    pub kind: String,
    /// Closed content media type.
    pub content_format: String,
    /// Exact UTF-8 content bytes.
    pub content: Buffer,
    /// Existing provenance event UUID.
    pub provenance_uuid: String,
    /// Optional prior reasoning record amended by this record.
    pub supersedes_reasoning_uuid: Option<String>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin reasoning-history filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListReasoningInput {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for one explicit assertion-status event.
#[napi(object)]
pub struct RecordAssertionStatusInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 event identity.
    pub status_event_uuid: String,
    /// Existing assertion UUID.
    pub assertion_uuid: String,
    /// Closed explicit status.
    pub status: String,
    /// Existing producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional immutable confidence UUID.
    pub confidence_uuid: Option<String>,
    /// Optional immutable reasoning UUID.
    pub reasoning_uuid: Option<String>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin assertion-status history filter.
#[napi(object, object_to_js = false)]
pub struct ListAssertionStatusInput {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for one immutable assertion-validity event.
#[napi(object)]
pub struct RecordAssertionValidityInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 event identity.
    pub validity_event_uuid: String,
    /// Existing assertion UUID.
    pub assertion_uuid: String,
    /// Inclusive lower valid-time bound in Unix microseconds.
    pub valid_from_micros: Option<i64>,
    /// Exclusive upper valid-time bound in Unix microseconds.
    pub valid_to_micros: Option<i64>,
    /// Optional existing reasoning UUID.
    pub reasoning_uuid: Option<String>,
    /// Existing producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin assertion-validity history filter.
#[napi(object, object_to_js = false)]
pub struct ListAssertionValidityInput {
    /// Optional assertion UUID filter.
    pub assertion_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for valid-time evaluation after a transaction-time cutoff.
#[napi(object)]
pub struct ApplyValidTimeInput {
    /// Mandatory transaction-time cutoff in Unix microseconds.
    pub transaction_cutoff_micros: i64,
    /// Valid time to evaluate in Unix microseconds.
    pub valid_time_micros: i64,
}

/// Thin Node request for one atomic assertion supersession.
#[napi(object)]
pub struct SupersedeAssertionInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 relation identity.
    pub supersession_uuid: String,
    /// Existing assertion that becomes superseded.
    pub prior_assertion_uuid: String,
    /// Existing replacement assertion.
    pub replacement_assertion_uuid: String,
    /// Caller-supplied UUIDv7 paired status-event identity.
    pub status_event_uuid: String,
    /// Existing reasoning record for the prior assertion.
    pub reasoning_uuid: String,
    /// Existing producing provenance event.
    pub provenance_uuid: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Thin branch-preserving supersession-history filter.
#[napi(object, object_to_js = false)]
pub struct ListAssertionSupersessionsInput {
    /// Optional prior-assertion UUID filter.
    pub prior_assertion_uuid: Option<String>,
    /// Optional replacement-assertion UUID filter.
    pub replacement_assertion_uuid: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin Node request for one immutable hypothesis group.
#[napi(object)]
pub struct CreateHypothesisGroupInput {
    /// Idempotency UUID.
    pub operation_uuid: String,
    /// Group UUID.
    pub group_uuid: String,
    /// Canonical question key.
    pub question_key: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional actor UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for one hypothesis-membership event.
#[napi(object)]
pub struct RecordHypothesisMembershipInput {
    /// Idempotency UUID.
    pub operation_uuid: String,
    /// Event UUID.
    pub membership_event_uuid: String,
    /// Group UUID.
    pub group_uuid: String,
    /// Assertion UUID.
    pub assertion_uuid: String,
    /// `added` or `removed`.
    pub action: String,
    /// Reasoning UUID.
    pub reasoning_uuid: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional actor UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for one explicit hypothesis selection or clear.
#[napi(object)]
pub struct RecordHypothesisSelectionInput {
    /// Idempotency UUID.
    pub operation_uuid: String,
    /// Event UUID.
    pub selection_event_uuid: String,
    /// Group UUID.
    pub group_uuid: String,
    /// Selected assertion, or absent to clear.
    pub selected_assertion_uuid: Option<String>,
    /// Reasoning UUID.
    pub reasoning_uuid: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional actor UUID.
    pub actor_uuid: Option<String>,
}

/// Thin Node request for atomic selected-member removal.
#[napi(object)]
pub struct RemoveHypothesisMemberInput {
    /// Idempotency UUID.
    pub operation_uuid: String,
    /// Removal event UUID.
    pub membership_event_uuid: String,
    /// Paired selection event UUID.
    pub selection_event_uuid: String,
    /// Group UUID.
    pub group_uuid: String,
    /// Removed assertion UUID.
    pub assertion_uuid: String,
    /// Replacement selection, or absent to clear.
    pub selected_assertion_uuid: Option<String>,
    /// Reasoning UUID.
    pub reasoning_uuid: String,
    /// Producing provenance UUID.
    pub provenance_uuid: String,
    /// Optional actor UUID.
    pub actor_uuid: Option<String>,
}

/// Thin hypothesis-group history filter.
#[napi(object, object_to_js = false)]
pub struct ListHypothesisGroupsInput {
    /// Optional exact question key.
    pub question_key: Option<String>,
    /// Page size.
    pub limit: Option<u32>,
    /// Opaque page cursor.
    pub after: Option<String>,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Thin hypothesis-membership history filter.
#[napi(object, object_to_js = false)]
pub struct ListHypothesisMembershipInput {
    /// Optional group UUID.
    pub group_uuid: Option<String>,
    /// Optional assertion UUID.
    pub assertion_uuid: Option<String>,
    /// Page size.
    pub limit: Option<u32>,
    /// Opaque page cursor.
    pub after: Option<String>,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Thin hypothesis-selection history filter.
#[napi(object, object_to_js = false)]
pub struct ListHypothesisSelectionInput {
    /// Optional group UUID.
    pub group_uuid: Option<String>,
    /// Page size.
    pub limit: Option<u32>,
    /// Opaque page cursor.
    pub after: Option<String>,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Thin atomic assertion-plus-first-status request.
#[napi(object)]
pub struct CreateAssertionWithStatusInput {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 assertion identity.
    pub assertion_uuid: String,
    /// Exact claim text.
    pub claim: String,
    /// Ordered graph UUID references.
    pub graph_refs: Vec<AssertionGraphRefInputJs>,
    /// Caller-supplied UUIDv7 first-status identity.
    pub status_event_uuid: String,
    /// Explicit first status.
    pub status: String,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
}

/// Complete version-1 belief-resolution policy. Every field is mandatory.
#[napi(object)]
pub struct BeliefProjectionPolicyInput {
    /// Statuses eligible for projection.
    pub included_statuses: Vec<String>,
    /// Explicit statusless behavior.
    pub statusless: String,
    /// Explicit supersession-branch behavior.
    pub supersession_branches: String,
    /// Explicit hypothesis-selection behavior.
    pub hypotheses: String,
}

/// Resolve an immutable graph-only projection at one M21 cutoff.
#[napi(object, object_to_js = false)]
pub struct ResolveBeliefProjectionInput {
    /// Mandatory transaction-time cutoff.
    pub transaction_cutoff_micros: i64,
    /// Optional valid-time intersection.
    pub valid_time_micros: Option<i64>,
    /// Complete version-1 policy.
    pub policy: BeliefProjectionPolicyInput,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Version-1 policy input whose omissions receive GraphForge validation codes.
#[napi(object)]
pub struct BeliefSubjectPolicyInput {
    /// Statuses eligible for projection.
    pub included_statuses: Option<Vec<String>>,
    /// Explicit statusless behavior.
    pub statusless: Option<String>,
    /// Explicit supersession-branch behavior.
    pub supersession_branches: Option<String>,
    /// Explicit hypothesis-selection behavior.
    pub hypotheses: Option<String>,
}

/// Resolve one explicitly addressed belief subject and its graph projection.
#[napi(object, object_to_js = false)]
pub struct ResolveBeliefSubjectInput {
    /// Exactly one of this assertion UUID or `hypothesisQuestionKey` is required.
    pub assertion_uuid: Option<String>,
    /// Exactly one of this question key or `assertionUuid` is required.
    pub hypothesis_question_key: Option<String>,
    /// Mandatory transaction-time cutoff.
    pub transaction_cutoff_micros: i64,
    /// Optional valid-time intersection.
    pub valid_time_micros: Option<i64>,
    /// Complete version-1 policy.
    pub policy: Option<BeliefSubjectPolicyInput>,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Same-generation opaque graph projection and canonical subject evidence.
#[napi(js_name = "ResolvedBeliefSubjectOutput")]
pub struct ResolvedBeliefSubjectOutput {
    projection: Arc<ResolvedBeliefProjection>,
    evidence: Vec<u8>,
}

/// Thin Node request for one recorded M18 invocation.
#[napi(object, object_to_js = false)]
pub struct RecordedAlgorithmInput<'env> {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 run identity.
    pub run_uuid: String,
    /// Opaque Rust-owned neutral descriptor.
    #[napi(ts_type = "InvocationDescriptor")]
    pub descriptor: ClassInstance<'env, InvocationDescriptorHandle>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// First-time recorded dispatch output.
#[napi(object)]
pub struct RecordedAlgorithmOutput {
    /// Durable run UUID.
    pub run_uuid: String,
    /// Canonical Arrow IPC result.
    pub result: Buffer,
}

/// Execute an M20 descriptor against a resolved projection and attach M21 context.
#[napi(object, object_to_js = false)]
pub struct ResolvedRecordedAlgorithmInput<'env> {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Caller-supplied UUIDv7 run identity.
    pub run_uuid: String,
    /// Caller-supplied UUIDv7 attachment identity.
    pub attachment_uuid: String,
    /// Opaque Rust-owned neutral descriptor.
    #[napi(ts_type = "InvocationDescriptor")]
    pub descriptor: ClassInstance<'env, InvocationDescriptorHandle>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
    /// Optional abort signal.
    pub signal: Option<AbortSignal>,
}

/// Recorded result plus the separately observable attachment outcome.
#[napi(object)]
pub struct ResolvedRecordedAlgorithmOutput {
    /// Durable M20 run UUID.
    pub run_uuid: String,
    /// Canonical M20 result as Arrow IPC.
    pub result: Buffer,
    /// Stable M21 attachment UUID.
    pub attachment_uuid: String,
    /// `attached` or `attachment_failed`.
    pub attachment_state: String,
    /// Attached row as Arrow IPC when publication succeeded.
    pub attachment: Option<Buffer>,
    /// Stable public failure code when publication failed.
    pub attachment_error_code: Option<String>,
}

/// Retry only the M21 attachment for an already completed M20 run.
#[napi(object, object_to_js = false)]
pub struct AttachResolvedRunInput<'env> {
    /// Required operation/idempotency UUID.
    pub operation_uuid: String,
    /// Stable attachment retry UUID.
    pub attachment_uuid: String,
    /// Existing completed M20 run UUID.
    pub run_uuid: String,
    /// Exact descriptor used by the completed run.
    #[napi(ts_type = "InvocationDescriptor")]
    pub descriptor: ClassInstance<'env, InvocationDescriptorHandle>,
    /// Optional analyst or agent UUID.
    pub actor_uuid: Option<String>,
    /// Optional abort signal. Settles JavaScript scheduling early but cannot
    /// cancel or roll back an attachment publication that has already started.
    pub signal: Option<AbortSignal>,
}

/// Thin algorithm-run list filter and page request.
#[napi(object, object_to_js = false)]
pub struct ListAlgorithmRunsInput {
    /// Optional exact `verb.name` algorithm ID.
    pub algorithm: Option<String>,
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

/// Thin algorithm-run event page request.
#[napi(object, object_to_js = false)]
pub struct AlgorithmRunEventsInput {
    /// Page size, default 100.
    pub limit: Option<u32>,
    /// Opaque generation-pinned cursor.
    pub after: Option<String>,
    /// Optional standard AbortSignal.
    pub signal: Option<AbortSignal>,
}

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

/// Canonical M18 embedding publication options.
#[napi(object)]
pub struct M18EmbeddingPublicationInput {
    /// Explicit eligible M18 embedding algorithm.
    pub algorithm: String,
    /// Frozen algorithm contract version.
    pub algorithm_version: String,
    /// Fixed width retained for empty projections.
    pub dimensions: u32,
    /// Normalized algorithm hyperparameters.
    pub hyperparameters: Option<HashMap<String, serde_json::Value>>,
    /// Non-empty versioned M18 input recipe.
    pub input_recipe: HashMap<String, serde_json::Value>,
    /// Non-empty graph projection identity.
    pub source_projection: HashMap<String, serde_json::Value>,
    /// `none` or `l2`.
    pub normalization: Option<String>,
    /// Permit explicit alias rebinding.
    pub replace: Option<bool>,
}

/// Coerce public Node selector shapes without resolving graph state.
fn node_selector_from_input(input: NodeSelectorInput<'_>) -> Result<NodeSelector> {
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

fn parse_terminal_uuids(values: &[String]) -> Result<Vec<[u8; 16]>> {
    let mut terminals = Vec::new();
    terminals.try_reserve_exact(values.len()).map_err(|_| {
        to_napi_err(&GfError::Execution(
            "Steiner terminal allocation exceeds available memory".into(),
        ))
    })?;
    for value in values {
        if value.len() != 36 {
            return Err(to_napi_err(&GfError::Validation(format!(
                "invalid Steiner terminal UUID {value:?}"
            ))));
        }
        let NodeSelector::Uuid(uuid) =
            NodeSelector::uuid(value).map_err(|error| to_napi_err(&error))?
        else {
            unreachable!("UUID parser always constructs a UUID selector")
        };
        if uuid.hyphenated().to_string() != *value {
            return Err(to_napi_err(&GfError::Validation(format!(
                "invalid Steiner terminal UUID {value:?}"
            ))));
        }
        terminals.push(*uuid.as_bytes());
    }
    Ok(terminals)
}

pub(crate) fn canonical_operation_id(value: &str) -> Result<OperationId> {
    if value.len() != 36 {
        return Err(to_napi_err(&GfError::Validation(format!(
            "invalid UUID {value:?}"
        ))));
    }
    let NodeSelector::Uuid(uuid) =
        NodeSelector::uuid(value).map_err(|error| to_napi_err(&error))?
    else {
        unreachable!("UUID parser always constructs a UUID selector")
    };
    if uuid.hyphenated().to_string() != value {
        return Err(to_napi_err(&GfError::Validation(format!(
            "invalid UUID {value:?}"
        ))));
    }
    Ok(OperationId(uuid))
}

pub(crate) fn optional_uuid(value: Option<&str>) -> Result<Option<uuid::Uuid>> {
    value
        .map(canonical_operation_id)
        .transpose()
        .map(|value| value.map(|id| id.0))
}

fn node_page(
    limit: Option<u32>,
    after: Option<&str>,
    signal: Option<AbortSignal>,
) -> Result<gf_api::PageRequest> {
    let after = after
        .map(gf_api::PageToken::parse)
        .transpose()
        .map_err(|error| to_napi_err(&error))?;
    let cancellation = gf_api::CancellationToken::new();
    if let Some(signal) = &signal {
        let cancellation = cancellation.clone();
        signal.on_abort(move || cancellation.cancel());
    }
    Ok(gf_api::PageRequest {
        limit: limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
        after,
        cancellation: Some(cancellation),
    })
}

fn checkpoint_selector(value: String) -> gf_api::CheckpointSelector {
    if value == "current" {
        gf_api::CheckpointSelector::Current
    } else {
        gf_api::CheckpointSelector::Named(value)
    }
}

fn checkpoint_diff_scope(value: &str) -> Result<gf_api::CheckpointDiffScope> {
    match value {
        "summary" => Ok(gf_api::CheckpointDiffScope::Summary),
        "graph" => Ok(gf_api::CheckpointDiffScope::Graph),
        "ontology" => Ok(gf_api::CheckpointDiffScope::Ontology),
        "configuration" => Ok(gf_api::CheckpointDiffScope::Configuration),
        "capabilities" => Ok(gf_api::CheckpointDiffScope::Capabilities),
        "provenance" => Ok(gf_api::CheckpointDiffScope::Provenance),
        "knowledge" => Ok(gf_api::CheckpointDiffScope::Knowledge),
        "epistemic" => Ok(gf_api::CheckpointDiffScope::Epistemic),
        "all" => Ok(gf_api::CheckpointDiffScope::All),
        _ => Err(napi_validation("unknown checkpoint diff scope")),
    }
}

fn checkpoint_diff_detail(value: &str) -> Result<gf_api::CheckpointDiffDetail> {
    match value {
        "summary" => Ok(gf_api::CheckpointDiffDetail::Summary),
        "records" => Ok(gf_api::CheckpointDiffDetail::Records),
        _ => Err(napi_validation("unknown checkpoint diff detail")),
    }
}

pub(crate) fn assertion_status(value: &str) -> Result<gf_api::AssertionStatus> {
    match value {
        "hypothesis" => Ok(gf_api::AssertionStatus::Hypothesis),
        "supported" => Ok(gf_api::AssertionStatus::Supported),
        "refuted" => Ok(gf_api::AssertionStatus::Refuted),
        "disputed" => Ok(gf_api::AssertionStatus::Disputed),
        "retracted" => Ok(gf_api::AssertionStatus::Retracted),
        "superseded" => Ok(gf_api::AssertionStatus::Superseded),
        _ => Err(napi_validation("unknown assertion status")),
    }
}

fn parse_algorithm_id(value: &str) -> Result<gf_api::Algorithm> {
    let (verb, name) = value
        .split_once('.')
        .ok_or_else(|| napi_validation("algorithm must be verb.name"))?;
    let verb = match verb {
        "rank" => gf_api::AlgorithmVerb::Rank,
        "cluster" => gf_api::AlgorithmVerb::Cluster,
        "paths" => gf_api::AlgorithmVerb::Paths,
        "analyze" => gf_api::AlgorithmVerb::Analyze,
        "similar" => gf_api::AlgorithmVerb::Similar,
        _ => return Err(napi_validation("unknown algorithm verb")),
    };
    gf_api::Algorithm::parse(verb, name).map_err(|_| napi_validation("unknown algorithm ID"))
}

fn parse_capability_id(value: &str) -> Result<CapabilityId> {
    match value {
        "graph" => Ok(CapabilityId::Graph),
        "provenance" => Ok(CapabilityId::Provenance),
        "knowledge" => Ok(CapabilityId::Knowledge),
        "epistemic" => Ok(CapabilityId::Epistemic),
        "valid_time" => Ok(CapabilityId::ValidTime),
        _ => Err(to_napi_err(&GfError::Validation(format!(
            "unknown capability {value:?}"
        )))),
    }
}

fn assertion_graph_ref(value: AssertionGraphRefInputJs) -> Result<AssertionGraphRefInput> {
    let graph_uuid = canonical_operation_id(&value.graph_uuid)?.0;
    let graph_kind = match value.graph_kind.as_str() {
        "node" => GraphObjectKind::Node,
        "edge" => GraphObjectKind::Edge,
        _ => {
            return Err(to_napi_err(&GfError::Validation(
                "graphKind must be 'node' or 'edge'".into(),
            )));
        }
    };
    let role = match value.role.as_str() {
        "subject" => AssertionGraphRole::Subject,
        "object" => AssertionGraphRole::Object,
        "context" => AssertionGraphRole::Context,
        _ => {
            return Err(to_napi_err(&GfError::Validation(
                "role must be 'subject', 'object', or 'context'".into(),
            )));
        }
    };
    Ok(AssertionGraphRefInput {
        graph_uuid,
        graph_kind,
        role,
        ordinal: value.ordinal,
    })
}

fn evidence_input(value: EvidenceInputJs) -> Result<gf_api::EvidenceInput> {
    let source_kind = match value.source_kind.as_str() {
        "document" => gf_api::EvidenceSourceKind::Document,
        "observation" => gf_api::EvidenceSourceKind::Observation,
        "graph_node" => gf_api::EvidenceSourceKind::GraphNode,
        "graph_edge" => gf_api::EvidenceSourceKind::GraphEdge,
        _ => return Err(napi_validation("unknown evidence source kind")),
    };
    let role = match value.role.as_str() {
        "supports" => gf_api::EvidenceRole::Supports,
        "contradicts" => gf_api::EvidenceRole::Contradicts,
        "context" => gf_api::EvidenceRole::Context,
        _ => return Err(napi_validation("unknown evidence role")),
    };
    Ok(gf_api::EvidenceInput {
        evidence_uuid: canonical_operation_id(&value.evidence_uuid)?.0,
        source_uuid: canonical_operation_id(&value.source_uuid)?.0,
        source_kind,
        role,
        weight: value.weight,
    })
}

/// Returns the crate version.
#[napi]
#[must_use]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Captured output from one Rust-owned CLI invocation.
#[napi(object)]
pub struct CliExecutionOutput {
    /// Process-compatible exit status.
    pub exit_code: i32,
    /// Exact standard-output bytes, including binary Arrow IPC results.
    pub stdout: Buffer,
    /// Exact standard-error bytes.
    pub stderr: Buffer,
}

/// Parse and execute the native GraphForge CLI without terminating Node.js.
#[napi(js_name = "runCli")]
#[must_use]
pub fn run_cli(args: Vec<String>) -> CliExecutionOutput {
    let execution = gf_cli::execute(std::iter::once("gf".to_owned()).chain(args));
    CliExecutionOutput {
        exit_code: execution.exit_code,
        stdout: execution.stdout.into(),
        stderr: execution.stderr.into(),
    }
}

static WRITER_HOLD_PROBE: LazyLock<Mutex<Option<gf_api::concurrency_test_support::HeldWriter>>> =
    LazyLock::new(|| Mutex::new(None));

/// Stage and retain the project writer lock for concurrency acceptance tests.
#[napi(js_name = "testAcquireWriterHold")]
pub fn test_acquire_writer_hold(path: String) -> Result<()> {
    {
        let state = WRITER_HOLD_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.is_some() {
            return Err(to_napi_err(&GfError::Validation(
                "writer-hold probe already active".into(),
            )));
        }
    }
    let held = gf_api::concurrency_test_support::hold_writer(&path)
        .map_err(|error| to_napi_err(&error))?;
    let mut state = WRITER_HOLD_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.is_some() {
        return Err(to_napi_err(&GfError::Validation(
            "writer-hold probe already active".into(),
        )));
    }
    *state = Some(held);
    Ok(())
}

/// Drop the staged writer hold created by [`test_acquire_writer_hold`].
#[napi(js_name = "testReleaseWriterHold")]
pub fn test_release_writer_hold() -> Result<()> {
    let mut state = WRITER_HOLD_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.take().is_none() {
        return Err(to_napi_err(&GfError::Validation(
            "writer-hold probe is not active".into(),
        )));
    }
    Ok(())
}

/// UUID-backed node handle returned by [`GraphForge::add_node`].
#[napi]
pub struct NodeHandle {
    inner: gf_api::NodeHandle,
}

#[napi]
impl NodeHandle {
    /// Stable public UUID identity.
    #[napi(getter)]
    #[must_use]
    pub fn uuid(&self) -> String {
        self.inner.uuid.to_string()
    }

    /// Primary label metadata (not an identity surrogate).
    #[napi(getter)]
    #[must_use]
    pub fn label(&self) -> String {
        self.inner.label.clone()
    }

    /// Human-readable UUID-only handle representation.
    #[napi(js_name = "toString")]
    #[must_use]
    pub fn as_string(&self) -> String {
        self.inner.to_string()
    }
}

/// UUID-backed edge handle returned by [`GraphForge::add_edge`].
#[napi]
pub struct EdgeHandle {
    inner: gf_api::EdgeHandle,
}

#[napi]
impl EdgeHandle {
    /// Stable public UUID identity.
    #[napi(getter)]
    #[must_use]
    pub fn uuid(&self) -> String {
        self.inner.uuid.to_string()
    }

    /// Relationship-type metadata (not an identity surrogate).
    #[napi(getter)]
    #[must_use]
    pub fn rel_type(&self) -> String {
        self.inner.rel_type.clone()
    }

    /// Human-readable UUID-only handle representation.
    #[napi(js_name = "toString")]
    #[must_use]
    pub fn as_string(&self) -> String {
        self.inner.to_string()
    }
}

struct ConfiguredProviderBinding {
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

/// Opaque Rust-owned neutral M18 invocation descriptor.
#[napi(js_name = "InvocationDescriptor")]
pub struct InvocationDescriptorHandle {
    inner: InvocationDescriptor,
}

/// Thin Node projection of one live Rust M18 descriptor contract.
#[napi(object)]
pub struct AlgorithmDescriptorContractJs {
    /// Owning analyst verb.
    pub verb: String,
    /// Canonical algorithm catalog value.
    pub algorithm: String,
    /// Mathematical/dispatch contract version.
    pub algorithm_version: u32,
    /// Result schema version.
    pub result_schema_version: u32,
}

#[napi]
impl InvocationDescriptorHandle {
    /// Canonical language-neutral descriptor bytes.
    #[napi(getter)]
    #[must_use]
    pub fn canonical_bytes(&self) -> Buffer {
        Buffer::from(self.inner.canonical_bytes().to_vec())
    }

    /// Full descriptor fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn fingerprint(&self) -> String {
        hex_bytes(self.inner.fingerprint())
    }

    /// Exact logical projection fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn projection_fingerprint(&self) -> String {
        hex_bytes(self.inner.projection_fingerprint())
    }

    /// Owning analyst verb.
    #[napi(getter)]
    #[must_use]
    pub fn verb(&self) -> String {
        self.inner.algorithm().verb().as_str().to_owned()
    }

    /// Canonical algorithm catalog value.
    #[napi(getter)]
    #[must_use]
    pub fn algorithm(&self) -> String {
        self.inner.algorithm().as_str().to_owned()
    }
}

fn belief_projection_policy(
    input: BeliefProjectionPolicyInput,
) -> Result<BeliefProjectionPolicyV1> {
    let included_statuses = input
        .included_statuses
        .iter()
        .map(|value| assertion_status(value))
        .collect::<Result<Vec<gf_api::AssertionStatus>>>()?;
    let statusless = match input.statusless.as_str() {
        "reject" => StatuslessPolicyV1::Reject,
        "exclude" => StatuslessPolicyV1::Exclude,
        "include" => StatuslessPolicyV1::Include,
        _ => {
            return Err(napi_validation(
                "statusless must be reject, exclude, or include",
            ));
        }
    };
    let supersession_branches = match input.supersession_branches.as_str() {
        "reject" => SupersessionBranchPolicyV1::Reject,
        "include_all_leaves" => SupersessionBranchPolicyV1::IncludeAllLeaves,
        _ => {
            return Err(napi_validation(
                "supersessionBranches must be reject or include_all_leaves",
            ));
        }
    };
    let hypotheses = match input.hypotheses.as_str() {
        "require_selected" => gf_api::HypothesisSelectionPolicyV1::RequireSelected,
        "exclude_unselected_group" => gf_api::HypothesisSelectionPolicyV1::ExcludeUnselectedGroup,
        "include_all_current_members" => {
            gf_api::HypothesisSelectionPolicyV1::IncludeAllCurrentMembers
        }
        _ => {
            return Err(napi_validation(
                "hypotheses must be require_selected, exclude_unselected_group, or include_all_current_members",
            ));
        }
    };
    Ok(BeliefProjectionPolicyV1 {
        included_statuses,
        statusless,
        supersession_branches,
        hypotheses,
    })
}

fn belief_subject_policy(
    input: Option<BeliefSubjectPolicyInput>,
) -> Result<BeliefProjectionPolicyV1> {
    let input = input.ok_or_else(|| napi_validation("policy is required"))?;
    belief_projection_policy(BeliefProjectionPolicyInput {
        included_statuses: input
            .included_statuses
            .ok_or_else(|| napi_validation("policy.includedStatuses is required"))?,
        statusless: input
            .statusless
            .ok_or_else(|| napi_validation("policy.statusless is required"))?,
        supersession_branches: input
            .supersession_branches
            .ok_or_else(|| napi_validation("policy.supersessionBranches is required"))?,
        hypotheses: input
            .hypotheses
            .ok_or_else(|| napi_validation("policy.hypotheses is required"))?,
    })
}

/// Opaque Rust-owned graph projection resolved from explicit M21 policy.
#[napi(js_name = "ResolvedBeliefProjection")]
pub struct ResolvedBeliefProjectionHandle {
    inner: Arc<ResolvedBeliefProjection>,
}

#[napi]
impl ResolvedBeliefProjectionHandle {
    /// Source project generation pinned during resolution.
    #[napi(getter)]
    #[must_use]
    pub fn source_generation_uuid(&self) -> String {
        self.inner.source_generation_uuid().to_string()
    }

    /// Universal graph-content fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn graph_content_fingerprint(&self) -> String {
        hex_bytes(&self.inner.graph_content_fingerprint())
    }

    /// Canonical versioned policy bytes.
    #[napi(getter)]
    #[must_use]
    pub fn policy_bytes(&self) -> Buffer {
        Buffer::from(self.inner.policy_bytes().to_vec())
    }

    /// Policy fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn policy_fingerprint(&self) -> String {
        hex_bytes(&self.inner.policy_fingerprint())
    }

    /// Transaction snapshot fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn snapshot_fingerprint(&self) -> String {
        hex_bytes(&self.inner.snapshot_fingerprint())
    }

    /// Transaction cutoff used by resolution.
    #[napi(getter)]
    #[must_use]
    pub fn transaction_cutoff_micros(&self) -> i64 {
        self.inner.transaction_cutoff_micros()
    }

    /// Optional valid-time intersection.
    #[napi(getter)]
    #[must_use]
    pub fn valid_time_micros(&self) -> Option<i64> {
        self.inner.valid_time_micros()
    }

    /// Optional valid-time result fingerprint as lowercase hex.
    #[napi(getter)]
    #[must_use]
    pub fn valid_time_fingerprint(&self) -> Option<String> {
        self.inner
            .valid_time_fingerprint()
            .map(|value| hex_bytes(&value))
    }

    /// Sorted M21 source-record UUIDs used by resolution.
    #[napi(getter)]
    #[must_use]
    pub fn source_record_uuids(&self) -> Vec<String> {
        self.inner
            .source_record_uuids()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// Prepare rank without executing it.
    #[napi]
    pub fn prepare_rank_invocation(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
    ) -> Result<InvocationDescriptorHandle> {
        let options = gf_api::RankOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            write_property: None,
        };
        self.inner
            .prepare_rank_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare clustering without executing it.
    #[napi]
    pub fn prepare_cluster_invocation(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        vector_property: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let options = gf_api::ClusterOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            vector_property,
            via,
            directed: directed.unwrap_or(false),
            write_property: None,
        };
        self.inner
            .prepare_cluster_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare paths without executing it.
    #[napi(
        ts_args_type = "source: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, target: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, by: string, via?: string | null, directed?: boolean | null, k?: number | null, weight?: string | null, heuristic?: string | null, walkLength?: number | null, seed?: bigint | null, terminalUuids?: string[] | null, prizeProperty?: string | null, capacityProperty?: string | null, costProperty?: string | null"
    )]
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_paths_invocation(
        &self,
        source: Option<NodeSelectorInput<'_>>,
        target: Option<NodeSelectorInput<'_>>,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        k: Option<u32>,
        weight: Option<String>,
        heuristic: Option<String>,
        walk_length: Option<u32>,
        seed: Option<BigInt>,
        terminal_uuids: Option<Vec<String>>,
        prize_property: Option<String>,
        capacity_property: Option<String>,
        cost_property: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let seed = seed.map(parse_seed).transpose()?;
        let options = PathsOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            k: k.unwrap_or(1) as usize,
            weight,
            capacity_property,
            cost_property,
            heuristic,
            walk_length: walk_length.map(|value| value as usize),
            seed,
            terminal_uuids: parse_terminal_uuids(terminal_uuids.as_deref().unwrap_or_default())?,
            prize_property,
        };
        let source = source.map(node_selector_from_input).transpose()?;
        let target = target.map(node_selector_from_input).transpose()?;
        self.inner
            .prepare_paths_invocation(source.as_ref(), target.as_ref(), &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare analysis without executing it.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_analyze_invocation(
        &self,
        by: String,
        label: Option<String>,
        via: Option<String>,
        directed: Option<bool>,
        weight: Option<String>,
        partition_property: Option<String>,
        k: Option<u32>,
    ) -> Result<InvocationDescriptorHandle> {
        let options = AnalyzeOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            weight,
            k: k.map(|value| value as usize),
            partition_property,
        };
        self.inner
            .prepare_analyze_invocation(label.as_deref(), &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare similarity without executing it.
    #[napi]
    pub fn prepare_similar_invocation(
        &self,
        label: String,
        by: String,
        k: Option<u32>,
        vector_property: Option<String>,
        via: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let options = SimilarOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            k: k.unwrap_or(10) as usize,
            vector_property,
            via,
        };
        self.inner
            .prepare_similar_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }
}

#[napi]
impl ResolvedBeliefSubjectOutput {
    /// Opaque Rust-owned graph projection.
    #[napi(getter)]
    #[must_use]
    pub fn projection(&self) -> ResolvedBeliefProjectionHandle {
        ResolvedBeliefProjectionHandle {
            inner: Arc::clone(&self.projection),
        }
    }

    /// Canonical subject-evidence Arrow IPC stream.
    #[napi(getter)]
    #[must_use]
    pub fn evidence(&self) -> Buffer {
        Buffer::from(self.evidence.clone())
    }
}

fn parse_seed(value: BigInt) -> Result<u64> {
    let (negative, seed, lossless) = value.get_u64();
    if !negative && lossless {
        Ok(seed)
    } else {
        Err(napi_validation("seed must be an unsigned 64-bit integer"))
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}

/// Immutable, lease-pinned view of one named checkpoint.
///
/// The class deliberately exposes only Rust-owned read operations; it has no
/// binding-side mutation or history implementation.
#[napi]
pub struct CheckpointView {
    inner: Arc<RwLock<gf_api::CheckpointView>>,
}

#[napi]
impl CheckpointView {
    /// Stable checkpoint UUID.
    #[napi(getter)]
    pub fn checkpoint_uuid(&self) -> Result<String> {
        let view = self
            .inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("CheckpointView lock poisoned".into())))?;
        Ok(view.checkpoint_uuid().to_string())
    }

    /// Pinned generation UUID.
    #[napi(getter)]
    pub fn generation_uuid(&self) -> Result<String> {
        let view = self
            .inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("CheckpointView lock poisoned".into())))?;
        Ok(view.generation_uuid().to_string())
    }

    /// Execute read-only Cypher against the pinned generation as Arrow IPC.
    #[napi]
    pub fn execute(&self, cypher: String) -> Result<Buffer> {
        let view = self
            .inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("CheckpointView lock poisoned".into())))?;
        let result = view.execute(&cypher).map_err(|error| to_napi_err(&error))?;
        result_to_ipc(&result)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect the pinned capability manifest as Arrow IPC.
    #[napi]
    pub fn project_capabilities(&self) -> Result<Buffer> {
        let view = self
            .inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("CheckpointView lock poisoned".into())))?;
        let result = view
            .project_capabilities()
            .map_err(|error| to_napi_err(&error))?;
        result_to_ipc(&result)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect adjacency freshness from the pinned generation.
    #[napi]
    pub fn inspect_adjacency(&self) -> Result<serde_json::Value> {
        let view = self
            .inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("CheckpointView lock poisoned".into())))?;
        view.inspect_adjacency()
            .map(adjacency_inspection_to_json)
            .map_err(|error| to_napi_err(&error))
    }
}

/// The GraphForge engine — a Node handle over the native Rust core
/// ([`gf_api::GraphForge`]).
///
/// Construct in-memory (`new GraphForge()`) or over a Parquet project directory
/// (`new GraphForge(path)`); the constructor returns a `GraphForge` instance.
/// Cypher execution, analyst verbs, explicit indexing, and text/vector/hybrid
/// search delegate to Rust and return their tabular results as Arrow IPC.
#[napi]
pub struct GraphForge {
    inner: OwnedEngine,
    provider: Option<ConfiguredProviderBinding>,
    closed: Arc<AtomicBool>,
}

/// The binding's owned reference to the native engine.
///
/// Worker tasks and deferred plans clone the inner `Arc`, but the JavaScript
/// wrapper must relinquish its own reference synchronously on `close()`. This
/// matters on Windows, where retaining the native engine also retains open
/// project-directory handles until JavaScript garbage collection runs.
struct OwnedEngine(Option<Arc<RwLock<gf_api::GraphForge>>>);

impl OwnedEngine {
    fn new(engine: gf_api::GraphForge) -> Self {
        Self(Some(Arc::new(RwLock::new(engine))))
    }

    fn close(&mut self) {
        self.0.take();
    }
}

impl Deref for OwnedEngine {
    type Target = Arc<RwLock<gf_api::GraphForge>>;

    fn deref(&self) -> &Self::Target {
        self.0
            .as_ref()
            .expect("lifecycle gate must reject access after close")
    }
}

impl GraphForge {
    /// Lifecycle gate mirroring the v0.5 contract: operations after `close()`
    /// raise `LifecycleError`.
    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(to_napi_err(&GfError::Lifecycle(
                "operation on a closed GraphForge instance".into(),
            )));
        }
        Ok(())
    }

    /// Check the lifecycle gate, then acquire shared engine access.
    fn open_guard(&self) -> Result<RwLockReadGuard<'_, gf_api::GraphForge>> {
        self.ensure_open()?;
        self.inner
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))
    }

    /// Check the lifecycle gate, then acquire exclusive engine access.
    fn open_write_guard(&self) -> Result<RwLockWriteGuard<'_, gf_api::GraphForge>> {
        self.ensure_open()?;
        self.inner
            .write()
            .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))
    }
}

#[napi]
impl GraphForge {
    /// Open an in-memory (`path` omitted) or Parquet-backed (`path` = dir) instance.
    #[napi(constructor)]
    pub fn new(path: Option<String>, options: Option<GraphForgeOptionsInput>) -> Result<Self> {
        let defaults = GraphForgeOptions::default();
        let options = options.unwrap_or(GraphForgeOptionsInput {
            write_mode: None,
            write_queue_capacity: None,
            max_rebase_attempts: None,
        });
        let options = GraphForgeOptions {
            write_mode: match options.write_mode {
                Some(value) => project_write_mode(&value)?,
                None => defaults.write_mode,
            },
            write_queue_capacity: match options.write_queue_capacity {
                Some(value) if (1..=65_536).contains(&value) => {
                    usize::try_from(value).map_err(|_| {
                        napi_validation("writeQueueCapacity must be between 1 and 65536")
                    })?
                }
                Some(_) => {
                    return Err(napi_validation(
                        "writeQueueCapacity must be between 1 and 65536",
                    ));
                }
                None => defaults.write_queue_capacity,
            },
            max_rebase_attempts: match options.max_rebase_attempts {
                Some(value) if (0..=32).contains(&value) => u32::try_from(value)
                    .map_err(|_| napi_validation("maxRebaseAttempts must be between 0 and 32"))?,
                Some(_) => {
                    return Err(napi_validation(
                        "maxRebaseAttempts must be between 0 and 32",
                    ));
                }
                None => defaults.max_rebase_attempts,
            },
        };
        let inner = gf_api::GraphForge::new_with_options(path.as_deref(), options)
            .map_err(|e| to_napi_err(&e))?;
        Ok(Self {
            inner: OwnedEngine::new(inner),
            provider: None,
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Inspect the committed project capability manifest as Arrow IPC.
    #[napi]
    pub fn project_capabilities(&self) -> Result<AsyncTask<ProjectCapabilitiesTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(ProjectCapabilitiesTask {
            engine: Arc::clone(&self.inner),
        }))
    }

    /// Create a durable named checkpoint and return its Arrow receipt.
    #[napi]
    pub fn checkpoint(&self, request: CheckpointInput) -> Result<AsyncTask<CheckpointTask>> {
        self.ensure_open()?;
        let actor_uuid = optional_uuid(request.actor_uuid.as_deref())?;
        Ok(AsyncTask::new(CheckpointTask {
            engine: Arc::clone(&self.inner),
            operation: CheckpointOperation::Create(gf_api::CheckpointRequest {
                name: request.name,
                description: request.description,
                idempotency_key: canonical_operation_id(&request.idempotency_key)?,
                actor_uuid,
            }),
        }))
    }

    /// List active checkpoints in canonical order as Arrow IPC.
    #[napi]
    pub fn list_checkpoints(
        &self,
        request: Option<ListCheckpointsInput>,
    ) -> Result<AsyncTask<CheckpointTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListCheckpointsInput {
            limit: None,
            after: None,
            signal: None,
        });
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(CheckpointTask {
            engine: Arc::clone(&self.inner),
            operation: CheckpointOperation::List(gf_api::ListCheckpointsRequest { page }),
        }))
    }

    /// Open an immutable view pinned to one named checkpoint.
    #[napi]
    pub fn open_checkpoint(&self, name: String) -> Result<CheckpointView> {
        let graph = self.open_guard()?;
        graph
            .open_checkpoint(&name)
            .map(|inner| CheckpointView {
                inner: Arc::new(RwLock::new(inner)),
            })
            .map_err(|error| to_napi_err(&error))
    }

    /// Delete an active checkpoint reference and return its Arrow receipt.
    #[napi]
    pub fn delete_checkpoint(
        &self,
        request: DeleteCheckpointInput,
    ) -> Result<AsyncTask<CheckpointTask>> {
        self.ensure_open()?;
        let actor_uuid = optional_uuid(request.actor_uuid.as_deref())?;
        Ok(AsyncTask::new(CheckpointTask {
            engine: Arc::clone(&self.inner),
            operation: CheckpointOperation::Delete(gf_api::DeleteCheckpointRequest {
                name: request.name,
                idempotency_key: canonical_operation_id(&request.idempotency_key)?,
                actor_uuid,
            }),
        }))
    }

    /// Compare two checkpoint/current endpoints through the Rust diff engine.
    #[napi]
    pub fn diff_checkpoints(
        &self,
        request: DiffCheckpointsInput,
    ) -> Result<AsyncTask<CheckpointTask>> {
        self.ensure_open()?;
        let scope = checkpoint_diff_scope(&request.scope)?;
        let detail = checkpoint_diff_detail(&request.detail)?;
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(CheckpointTask {
            engine: Arc::clone(&self.inner),
            operation: CheckpointOperation::Diff(gf_api::DiffCheckpointsRequest {
                from: checkpoint_selector(request.from),
                to: checkpoint_selector(request.to),
                scope,
                detail,
                page,
            }),
        }))
    }

    /// Restore a checkpoint as a new committed generation and return its receipt.
    #[napi]
    pub fn revert_to_checkpoint(
        &self,
        request: RevertCheckpointInput,
    ) -> Result<AsyncTask<CheckpointTask>> {
        self.ensure_open()?;
        let actor_uuid = optional_uuid(request.actor_uuid.as_deref())?;
        Ok(AsyncTask::new(CheckpointTask {
            engine: Arc::clone(&self.inner),
            operation: CheckpointOperation::Revert(gf_api::RevertCheckpointRequest {
                name: request.name,
                reason: request.reason,
                idempotency_key: canonical_operation_id(&request.idempotency_key)?,
                actor_uuid,
            }),
        }))
    }

    /// Atomically enable one registered project capability.
    #[napi]
    pub fn enable_capability(
        &self,
        request: EnableCapabilityInput,
    ) -> Result<AsyncTask<EnableCapabilityTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let capability_id = parse_capability_id(&request.capability_id)?;
        Ok(AsyncTask::new(EnableCapabilityTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                capability_id,
                capability_version: request.capability_version,
            },
        }))
    }

    /// Return one exact provenance event as an Arrow IPC stream.
    #[napi]
    pub fn provenance_event(
        &self,
        provenance_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<ProvenanceEventTask>> {
        self.ensure_open()?;
        let provenance_uuid = canonical_operation_id(&provenance_uuid)?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ProvenanceEventTask {
            engine: Arc::clone(&self.inner),
            provenance_uuid,
            cancellation,
        }))
    }

    /// Return one deterministic provenance-history page as Arrow IPC.
    #[napi]
    pub fn list_provenance_history(
        &self,
        request: Option<ProvenanceHistoryInput>,
    ) -> Result<AsyncTask<ProvenanceHistoryTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ProvenanceHistoryInput {
            subject_uuid: None,
            operation_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let subject_uuid = request
            .subject_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let operation_uuid = request
            .operation_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?;
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ProvenanceHistoryTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::ProvenanceHistoryRequest {
                subject_uuid,
                operation_uuid,
                page: gf_api::PageRequest {
                    limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Atomically create one immutable assertion.
    #[napi]
    pub fn create_assertion(
        &self,
        request: CreateAssertionInput,
    ) -> Result<AsyncTask<CreateAssertionTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let graph_refs = request
            .graph_refs
            .into_iter()
            .map(assertion_graph_ref)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(CreateAssertionTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::CreateAssertionRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                assertion_uuid,
                claim: request.claim,
                graph_refs,
            },
        }))
    }

    /// Atomically create one assertion and a non-empty evidence bundle.
    #[napi]
    pub fn create_assertion_with_evidence(
        &self,
        request: CreateAssertionWithEvidenceInput,
    ) -> Result<AsyncTask<CreateAssertionWithEvidenceTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let graph_refs = request
            .graph_refs
            .into_iter()
            .map(assertion_graph_ref)
            .collect::<Result<Vec<_>>>()?;
        let evidence = request
            .evidence
            .into_iter()
            .map(evidence_input)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(CreateAssertionWithEvidenceTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::CreateAssertionWithEvidenceRequest {
                assertion: gf_api::CreateAssertionRequest {
                    context: WriteContext {
                        operation_uuid,
                        actor_uuid,
                    },
                    assertion_uuid,
                    claim: request.claim,
                    graph_refs,
                },
                evidence,
            },
        }))
    }

    /// Atomically create one assertion and its first explicit status.
    #[napi]
    pub fn create_assertion_with_status(
        &self,
        request: CreateAssertionWithStatusInput,
    ) -> Result<AsyncTask<CreateAssertionWithStatusTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let status_event_uuid = canonical_operation_id(&request.status_event_uuid)?.0;
        let status = assertion_status(&request.status)?;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let graph_refs = request
            .graph_refs
            .into_iter()
            .map(assertion_graph_ref)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(CreateAssertionWithStatusTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::CreateAssertionWithStatusRequest {
                assertion: gf_api::CreateAssertionRequest {
                    context: WriteContext {
                        operation_uuid,
                        actor_uuid,
                    },
                    assertion_uuid,
                    claim: request.claim,
                    graph_refs,
                },
                first_status: gf_api::FirstAssertionStatusInput {
                    status_event_uuid,
                    status,
                },
            },
        }))
    }

    /// Return one exact immutable assertion as Arrow IPC.
    #[napi]
    pub fn assertion(
        &self,
        assertion_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<AssertionTask>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(&assertion_uuid)?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(AssertionTask {
            engine: Arc::clone(&self.inner),
            assertion_uuid,
            cancellation,
        }))
    }

    /// Return one deterministic assertion page as Arrow IPC.
    #[napi]
    pub fn list_assertions(
        &self,
        request: Option<ListAssertionsInput>,
    ) -> Result<AsyncTask<ListAssertionsTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListAssertionsInput {
            graph_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let graph_uuid = request
            .graph_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListAssertionsTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::ListAssertionsRequest {
                graph_uuid,
                page: gf_api::PageRequest {
                    limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Return one assertion's graph references as Arrow IPC.
    #[napi]
    pub fn assertion_graph_refs(
        &self,
        assertion_uuid: String,
        request: Option<AssertionGraphRefsInput>,
    ) -> Result<AsyncTask<AssertionGraphRefsTask>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(&assertion_uuid)?;
        let request = request.unwrap_or(AssertionGraphRefsInput {
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(AssertionGraphRefsTask {
            engine: Arc::clone(&self.inner),
            assertion_uuid,
            page: gf_api::PageRequest {
                limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                after,
                cancellation: Some(cancellation),
            },
        }))
    }

    /// Atomically record one immutable confidence assessment.
    #[napi]
    pub fn assess_confidence(
        &self,
        request: AssessConfidenceInput,
    ) -> Result<AsyncTask<AssessConfidenceTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let confidence_uuid = canonical_operation_id(&request.confidence_uuid)?.0;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let policy = match request.policy.as_str() {
            "explicit" => gf_api::ConfidencePolicyRequest::Explicit {
                value: request
                    .value
                    .ok_or_else(|| napi_validation("explicit requires value"))?,
            },
            "conservative_min" => {
                if request.value.is_some() {
                    return Err(napi_validation(
                        "conservative_min does not accept explicit value",
                    ));
                }
                let input_confidence_uuids = request
                    .input_confidence_uuids
                    .unwrap_or_default()
                    .iter()
                    .map(|value| canonical_operation_id(value).map(|id| id.0))
                    .collect::<Result<Vec<_>>>()?;
                gf_api::ConfidencePolicyRequest::ConservativeMin {
                    input_confidence_uuids,
                }
            }
            _ => return Err(napi_validation("unknown confidence policy")),
        };
        Ok(AsyncTask::new(AssessConfidenceTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::AssessConfidenceRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                confidence_uuid,
                assertion_uuid,
                policy,
            },
        }))
    }

    /// Return one exact confidence assessment as Arrow IPC.
    #[napi]
    pub fn confidence_assessment(
        &self,
        confidence_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<ConfidenceAssessmentTask>> {
        self.ensure_open()?;
        let confidence_uuid = canonical_operation_id(&confidence_uuid)?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ConfidenceAssessmentTask {
            engine: Arc::clone(&self.inner),
            confidence_uuid,
            cancellation,
        }))
    }

    /// Return one deterministic confidence-assessment page as Arrow IPC.
    #[napi]
    pub fn list_confidence_assessments(
        &self,
        request: Option<ListConfidenceAssessmentsInput>,
    ) -> Result<AsyncTask<ListConfidenceAssessmentsTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListConfidenceAssessmentsInput {
            assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let assertion_uuid = request
            .assertion_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListConfidenceAssessmentsTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::ListConfidenceAssessmentsRequest {
                assertion_uuid,
                page: gf_api::PageRequest {
                    limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Return one assessment's immutable input snapshot as Arrow IPC.
    #[napi]
    pub fn confidence_inputs(
        &self,
        confidence_uuid: String,
        request: Option<ConfidenceInputsInput>,
    ) -> Result<AsyncTask<ConfidenceInputsTask>> {
        self.ensure_open()?;
        let confidence_uuid = canonical_operation_id(&confidence_uuid)?;
        let request = request.unwrap_or(ConfidenceInputsInput {
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ConfidenceInputsTask {
            engine: Arc::clone(&self.inner),
            confidence_uuid,
            page: gf_api::PageRequest {
                limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                after,
                cancellation: Some(cancellation),
            },
        }))
    }

    /// Atomically attach one immutable evidence link.
    #[napi]
    pub fn attach_evidence(
        &self,
        request: AttachEvidenceInput,
    ) -> Result<AsyncTask<AttachEvidenceTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let evidence_uuid = canonical_operation_id(&request.evidence_uuid)?.0;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let source_uuid = canonical_operation_id(&request.source_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let source_kind = match request.source_kind.as_str() {
            "document" => gf_api::EvidenceSourceKind::Document,
            "observation" => gf_api::EvidenceSourceKind::Observation,
            "graph_node" => gf_api::EvidenceSourceKind::GraphNode,
            "graph_edge" => gf_api::EvidenceSourceKind::GraphEdge,
            _ => return Err(napi_validation("unknown evidence source kind")),
        };
        let role = match request.role.as_str() {
            "supports" => gf_api::EvidenceRole::Supports,
            "contradicts" => gf_api::EvidenceRole::Contradicts,
            "context" => gf_api::EvidenceRole::Context,
            _ => return Err(napi_validation("unknown evidence role")),
        };
        Ok(AsyncTask::new(AttachEvidenceTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::AttachEvidenceRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                evidence_uuid,
                assertion_uuid,
                source_uuid,
                source_kind,
                role,
                weight: request.weight,
            },
        }))
    }

    /// Return one exact immutable evidence link as Arrow IPC.
    #[napi]
    pub fn evidence_link(
        &self,
        evidence_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<EvidenceLinkTask>> {
        self.ensure_open()?;
        let evidence_uuid = canonical_operation_id(&evidence_uuid)?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(EvidenceLinkTask {
            engine: Arc::clone(&self.inner),
            evidence_uuid,
            cancellation,
        }))
    }

    /// Return one deterministic evidence-link page as Arrow IPC.
    #[napi]
    pub fn list_evidence_links(
        &self,
        request: Option<ListEvidenceLinksInput>,
    ) -> Result<AsyncTask<ListEvidenceLinksTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListEvidenceLinksInput {
            assertion_uuid: None,
            source_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let assertion_uuid = request
            .assertion_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let source_uuid = request
            .source_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|id| id.0);
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListEvidenceLinksTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::ListEvidenceLinksRequest {
                assertion_uuid,
                source_uuid,
                page: gf_api::PageRequest {
                    limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Atomically append one immutable M21 reasoning record.
    #[napi]
    pub fn record_reasoning(
        &self,
        request: RecordReasoningInput,
    ) -> Result<AsyncTask<RecordReasoningTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let reasoning_uuid = canonical_operation_id(&request.reasoning_uuid)?.0;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let provenance_uuid = canonical_operation_id(&request.provenance_uuid)?.0;
        let supersedes_reasoning_uuid = request
            .supersedes_reasoning_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let kind = match request.kind.as_str() {
            "evidence_interpretation" => gf_api::ReasoningKind::EvidenceInterpretation,
            "logical_inference" => gf_api::ReasoningKind::LogicalInference,
            "methodological_note" => gf_api::ReasoningKind::MethodologicalNote,
            "decision_rationale" => gf_api::ReasoningKind::DecisionRationale,
            _ => return Err(napi_validation("unknown reasoning kind")),
        };
        let content_format = match request.content_format.as_str() {
            "text/plain" => gf_api::ReasoningContentFormat::TextPlain,
            "text/markdown" => gf_api::ReasoningContentFormat::TextMarkdown,
            "application/json" => gf_api::ReasoningContentFormat::ApplicationJson,
            _ => return Err(napi_validation("unknown reasoning content format")),
        };
        Ok(AsyncTask::new(RecordReasoningTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::RecordReasoningRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                reasoning_uuid,
                assertion_uuid,
                kind,
                content_format,
                content: request.content.to_vec(),
                supersedes_reasoning_uuid,
                provenance_uuid,
            },
        }))
    }

    /// Return one exact immutable reasoning record as Arrow IPC.
    #[napi]
    pub fn reasoning(
        &self,
        reasoning_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<ReasoningTask>> {
        self.ensure_open()?;
        let reasoning_uuid = canonical_operation_id(&reasoning_uuid)?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ReasoningTask {
            engine: Arc::clone(&self.inner),
            reasoning_uuid,
            cancellation,
        }))
    }

    /// Return deterministic immutable reasoning history as Arrow IPC.
    #[napi]
    pub fn list_reasoning(
        &self,
        request: Option<ListReasoningInput>,
    ) -> Result<AsyncTask<ListReasoningTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListReasoningInput {
            assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let assertion_uuid = request
            .assertion_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListReasoningTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::ListReasoningRequest {
                assertion_uuid,
                page: gf_api::PageRequest {
                    limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Append one explicit assertion-status event.
    #[napi]
    pub fn record_assertion_status(
        &self,
        request: RecordAssertionStatusInput,
    ) -> Result<AsyncTask<RecordAssertionStatusTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let status_event_uuid = canonical_operation_id(&request.status_event_uuid)?.0;
        let assertion_uuid = canonical_operation_id(&request.assertion_uuid)?.0;
        let provenance_uuid = canonical_operation_id(&request.provenance_uuid)?.0;
        let confidence_uuid = request
            .confidence_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let reasoning_uuid = request
            .reasoning_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        Ok(AsyncTask::new(RecordAssertionStatusTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::RecordAssertionStatusRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                status_event_uuid,
                assertion_uuid,
                status: assertion_status(&request.status)?,
                confidence_uuid,
                reasoning_uuid,
                provenance_uuid,
            },
        }))
    }

    /// Return the current explicit status, or an empty Arrow table when statusless.
    #[napi]
    pub fn assertion_status(
        &self,
        assertion_uuid: String,
    ) -> Result<AsyncTask<AssertionStatusTask>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(&assertion_uuid)?;
        Ok(AsyncTask::new(AssertionStatusTask {
            engine: Arc::clone(&self.inner),
            assertion_uuid,
        }))
    }

    /// Return deterministic immutable assertion-status history as Arrow IPC.
    #[napi]
    pub fn list_assertion_status(
        &self,
        request: Option<ListAssertionStatusInput>,
    ) -> Result<AsyncTask<ListAssertionStatusTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListAssertionStatusInput {
            assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let assertion_uuid = request
            .assertion_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListAssertionStatusTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::ListAssertionStatusRequest {
                assertion_uuid,
                page: gf_api::PageRequest {
                    limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Append one immutable assertion-validity event.
    #[napi]
    pub fn record_assertion_validity(
        &self,
        request: RecordAssertionValidityInput,
    ) -> Result<AsyncTask<RecordAssertionValidityTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(RecordAssertionValidityTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::RecordAssertionValidityRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                },
                validity_event_uuid: canonical_operation_id(&request.validity_event_uuid)?.0,
                assertion_uuid: canonical_operation_id(&request.assertion_uuid)?.0,
                valid_from_micros: request.valid_from_micros,
                valid_to_micros: request.valid_to_micros,
                reasoning_uuid: optional_uuid(request.reasoning_uuid.as_deref())?,
                provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
            },
        }))
    }

    /// Return deterministic immutable assertion-validity history as Arrow IPC.
    #[napi]
    pub fn list_assertion_validity(
        &self,
        request: Option<ListAssertionValidityInput>,
    ) -> Result<AsyncTask<ListAssertionValidityTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListAssertionValidityInput {
            assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(ListAssertionValidityTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::ListAssertionValidityRequest {
                assertion_uuid: optional_uuid(request.assertion_uuid.as_deref())?,
                page,
            },
        }))
    }

    /// Apply valid time after resolving the mandatory transaction-time cutoff.
    #[napi]
    pub fn apply_valid_time(
        &self,
        request: ApplyValidTimeInput,
    ) -> Result<AsyncTask<ApplyValidTimeTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(ApplyValidTimeTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::ApplyValidTimeRequest {
                transaction_cutoff_micros: request.transaction_cutoff_micros,
                valid_time_micros: request.valid_time_micros,
            },
        }))
    }

    /// Atomically append one assertion supersession and paired terminal status.
    #[napi]
    pub fn supersede_assertion(
        &self,
        request: SupersedeAssertionInput,
    ) -> Result<AsyncTask<SupersedeAssertionTask>> {
        self.ensure_open()?;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        Ok(AsyncTask::new(SupersedeAssertionTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::SupersedeAssertionRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid,
                },
                supersession_uuid: canonical_operation_id(&request.supersession_uuid)?.0,
                prior_assertion_uuid: canonical_operation_id(&request.prior_assertion_uuid)?.0,
                replacement_assertion_uuid: canonical_operation_id(
                    &request.replacement_assertion_uuid,
                )?
                .0,
                status_event_uuid: canonical_operation_id(&request.status_event_uuid)?.0,
                reasoning_uuid: canonical_operation_id(&request.reasoning_uuid)?.0,
                provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
            },
        }))
    }

    /// Return deterministic branch-preserving supersession history as Arrow IPC.
    #[napi]
    pub fn list_assertion_supersessions(
        &self,
        request: Option<ListAssertionSupersessionsInput>,
    ) -> Result<AsyncTask<ListAssertionSupersessionsTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListAssertionSupersessionsInput {
            prior_assertion_uuid: None,
            replacement_assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let parse_optional = |value: Option<&str>| {
            value
                .map(canonical_operation_id)
                .transpose()
                .map(|value| value.map(|id| id.0))
        };
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ListAssertionSupersessionsTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::ListAssertionSupersessionsRequest {
                prior_assertion_uuid: parse_optional(request.prior_assertion_uuid.as_deref())?,
                replacement_assertion_uuid: parse_optional(
                    request.replacement_assertion_uuid.as_deref(),
                )?,
                page: gf_api::PageRequest {
                    limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Create one immutable hypothesis group.
    #[napi]
    pub fn create_hypothesis_group(
        &self,
        request: CreateHypothesisGroupInput,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::Create(gf_api::CreateHypothesisGroupRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                },
                group_uuid: canonical_operation_id(&request.group_uuid)?.0,
                question_key: request.question_key,
                provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
            }),
        }))
    }

    /// Append one explicit hypothesis-membership event.
    #[napi]
    pub fn record_hypothesis_membership(
        &self,
        request: RecordHypothesisMembershipInput,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        let action = match request.action.as_str() {
            "added" => gf_api::HypothesisMembershipAction::Added,
            "removed" => gf_api::HypothesisMembershipAction::Removed,
            _ => return Err(napi_validation("action must be 'added' or 'removed'")),
        };
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::Membership(gf_api::RecordHypothesisMembershipRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                },
                membership_event_uuid: canonical_operation_id(&request.membership_event_uuid)?.0,
                group_uuid: canonical_operation_id(&request.group_uuid)?.0,
                assertion_uuid: canonical_operation_id(&request.assertion_uuid)?.0,
                action,
                reasoning_uuid: canonical_operation_id(&request.reasoning_uuid)?.0,
                provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
            }),
        }))
    }

    /// Append one explicit hypothesis selection or clear.
    #[napi]
    pub fn record_hypothesis_selection(
        &self,
        request: RecordHypothesisSelectionInput,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::Selection(gf_api::RecordHypothesisSelectionRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                },
                selection_event_uuid: canonical_operation_id(&request.selection_event_uuid)?.0,
                group_uuid: canonical_operation_id(&request.group_uuid)?.0,
                selected_assertion_uuid: optional_uuid(request.selected_assertion_uuid.as_deref())?,
                reasoning_uuid: canonical_operation_id(&request.reasoning_uuid)?.0,
                provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
            }),
        }))
    }

    /// Atomically remove one member and explicitly change or clear selection.
    #[napi]
    pub fn remove_hypothesis_member(
        &self,
        request: RemoveHypothesisMemberInput,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::Remove(gf_api::RemoveHypothesisMemberRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id(&request.operation_uuid)?,
                    actor_uuid: optional_uuid(request.actor_uuid.as_deref())?,
                },
                membership_event_uuid: canonical_operation_id(&request.membership_event_uuid)?.0,
                selection_event_uuid: canonical_operation_id(&request.selection_event_uuid)?.0,
                group_uuid: canonical_operation_id(&request.group_uuid)?.0,
                assertion_uuid: canonical_operation_id(&request.assertion_uuid)?.0,
                selected_assertion_uuid: optional_uuid(request.selected_assertion_uuid.as_deref())?,
                reasoning_uuid: canonical_operation_id(&request.reasoning_uuid)?.0,
                provenance_uuid: canonical_operation_id(&request.provenance_uuid)?.0,
            }),
        }))
    }

    /// Return deterministic hypothesis-group history.
    #[napi]
    pub fn list_hypothesis_groups(
        &self,
        request: Option<ListHypothesisGroupsInput>,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListHypothesisGroupsInput {
            question_key: None,
            limit: None,
            after: None,
            signal: None,
        });
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::ListGroups(gf_api::ListHypothesisGroupsRequest {
                question_key: request.question_key,
                page,
            }),
        }))
    }

    /// Return deterministic hypothesis-membership history.
    #[napi]
    pub fn list_hypothesis_membership(
        &self,
        request: Option<ListHypothesisMembershipInput>,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListHypothesisMembershipInput {
            group_uuid: None,
            assertion_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::ListMembership(
                gf_api::ListHypothesisMembershipRequest {
                    group_uuid: optional_uuid(request.group_uuid.as_deref())?,
                    assertion_uuid: optional_uuid(request.assertion_uuid.as_deref())?,
                    page,
                },
            ),
        }))
    }

    /// Return deterministic hypothesis-selection history.
    #[napi]
    pub fn list_hypothesis_selection(
        &self,
        request: Option<ListHypothesisSelectionInput>,
    ) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListHypothesisSelectionInput {
            group_uuid: None,
            limit: None,
            after: None,
            signal: None,
        });
        let page = node_page(request.limit, request.after.as_deref(), request.signal)?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::ListSelection(gf_api::ListHypothesisSelectionRequest {
                group_uuid: optional_uuid(request.group_uuid.as_deref())?,
                page,
            }),
        }))
    }

    /// Return current hypothesis members.
    #[napi]
    pub fn hypothesis_members(&self, group_uuid: String) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::Members(canonical_operation_id(&group_uuid)?),
        }))
    }

    /// Return the current explicit hypothesis selection.
    #[napi]
    pub fn hypothesis_selection(&self, group_uuid: String) -> Result<AsyncTask<HypothesisTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(HypothesisTask {
            engine: Arc::clone(&self.inner),
            operation: HypothesisOperation::CurrentSelection(canonical_operation_id(&group_uuid)?),
        }))
    }

    /// Reconstruct one deterministic M21 transaction-time snapshot as Arrow IPC.
    #[napi]
    pub fn epistemic_snapshot(
        &self,
        transaction_cutoff: i64,
    ) -> Result<AsyncTask<EpistemicSnapshotTask>> {
        self.ensure_open()?;
        Ok(AsyncTask::new(EpistemicSnapshotTask {
            engine: Arc::clone(&self.inner),
            transaction_cutoff,
        }))
    }

    /// Run a Cypher query and return the result as an Arrow IPC stream `Buffer`
    /// (decode with apache-arrow `tableFromIPC`).
    ///
    /// `params` binds `$name` placeholders (values: JSON null/boolean/number/
    /// string/array/object, plus exact `{ "$uuid": "..." }` identity tags).
    /// Writes (`CREATE`/`SET`/`DELETE`/…) execute and return a summary.
    #[napi]
    pub fn execute(
        &self,
        cypher: String,
        params: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<Buffer> {
        let has_params = params.is_some();
        let p = params_from_map(params)?;
        let g = self.open_guard()?;
        let result = if has_params {
            g.execute_with_params(&cypher, &p)
        } else {
            g.execute(&cypher)
        }
        .map_err(|e| to_napi_err(&e))?;
        let bytes = result_to_ipc(&result).map_err(|e| to_napi_err(&e))?;
        Ok(Buffer::from(bytes))
    }

    /// Human-readable explanation of the compiler pipeline for `cypher`
    /// (`AST` → `GraphIR` → `LogicalPlan` → `PhysicalPlan`).
    #[napi]
    pub fn explain(&self, cypher: String) -> Result<String> {
        let g = self.open_guard()?;
        g.explain(&cypher).map_err(|e| to_napi_err(&e))
    }

    /// Load and apply an ontology from `path` (YAML/JSON by extension).
    #[napi]
    pub fn load_ontology(&self, path: String) -> Result<()> {
        let mut g = self.open_write_guard()?;
        g.load_ontology(&path).map_err(|e| to_napi_err(&e))
    }

    /// Return the stable, deterministically ordered runtime-catalog contract.
    #[napi]
    pub fn inspect_runtime_catalog(&self) -> Result<RuntimeCatalogSnapshotOutput> {
        let graph = self.open_guard()?;
        let snapshot = graph
            .inspect_runtime_catalog()
            .map_err(|error| to_napi_err(&error))?;
        Ok(RuntimeCatalogSnapshotOutput {
            contract_version: snapshot.contract_version,
            entries: snapshot
                .entries
                .into_iter()
                .map(|entry| RuntimeCatalogEntryOutput {
                    kind: match entry.kind {
                        gf_api::CatalogEntryKind::EntityType => "entity_type",
                        gf_api::CatalogEntryKind::RelationType => "relation_type",
                        gf_api::CatalogEntryKind::Property => "property",
                    }
                    .into(),
                    name: entry.name,
                    owner: entry.owner,
                    observation_count: entry.observation_count.into(),
                })
                .collect(),
        })
    }

    /// Suggest a conservative, explicitly non-authoritative ontology draft.
    #[napi]
    pub fn suggest_ontology(
        &self,
        ontology_id: String,
        version: String,
    ) -> Result<OntologySuggestionOutput> {
        let graph = self.open_guard()?;
        let suggestion = graph
            .suggest_ontology(gf_api::OntologySuggestionOptions {
                ontology_id,
                version,
            })
            .map_err(|error| to_napi_err(&error))?;
        Ok(OntologySuggestionOutput {
            draft: suggestion.draft,
            document: serde_json::to_value(suggestion.document).map_err(|error| {
                to_napi_err(&GfError::Validation(format!(
                    "encode ontology suggestion: {error}"
                )))
            })?,
            fingerprint_sha256: suggestion.fingerprint_sha256,
            omitted_relation_types: suggestion.omitted_relation_types,
        })
    }

    /// Validate an ontology document without changing live or durable state.
    #[napi]
    pub fn validate_ontology(
        &self,
        document: serde_json::Value,
    ) -> Result<OntologyValidationReportOutput> {
        let document: gf_api::OntologyDoc = serde_json::from_value(document).map_err(|error| {
            to_napi_err(&GfError::Validation(format!(
                "invalid ontology document: {error}"
            )))
        })?;
        let graph = self.open_guard()?;
        let report = graph.validate_ontology(&document);
        let diagnostics = report
            .diagnostics
            .into_iter()
            .map(|diagnostic| OntologyValidationDiagnosticOutput {
                kind: diagnostic.kind.to_string(),
                location: diagnostic.location,
                message: diagnostic.message,
            })
            .collect::<Vec<_>>();
        Ok(OntologyValidationReportOutput {
            valid: report.valid,
            diagnostics,
        })
    }

    /// Atomically export an explicit ontology source as YAML or JSON.
    #[napi]
    pub fn export_ontology(
        &self,
        source: String,
        destination: String,
        format: String,
        document: Option<serde_json::Value>,
    ) -> Result<()> {
        let source = match source.as_str() {
            "loaded" => gf_api::OntologyExportSource::Loaded,
            "adopted" => gf_api::OntologyExportSource::Adopted,
            "suggested" => {
                let document = document.ok_or_else(|| {
                    to_napi_err(&GfError::Validation(
                        "document is required for suggested ontology export".into(),
                    ))
                })?;
                gf_api::OntologyExportSource::Suggested(serde_json::from_value(document).map_err(
                    |error| {
                        to_napi_err(&GfError::Validation(format!(
                            "invalid ontology document: {error}"
                        )))
                    },
                )?)
            }
            _ => {
                return Err(to_napi_err(&GfError::Validation(
                    "ontology export source must be suggested, loaded, or adopted".into(),
                )));
            }
        };
        let format = ontology_export_format(&format)?;
        let graph = self.open_guard()?;
        graph
            .export_ontology(source, std::path::Path::new(&destination), format)
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect the generation-managed authoritative ontology record.
    #[napi]
    pub fn workspace_ontology(&self) -> Result<WorkspaceOntologyOutput> {
        let graph = self.open_guard()?;
        let record = graph
            .workspace_ontology()
            .map_err(|error| to_napi_err(&error))?;
        Ok(WorkspaceOntologyOutput {
            contract_version: record.contract_version,
            mode: match record.mode {
                gf_api::WorkspaceOntologyMode::None => "none",
                gf_api::WorkspaceOntologyMode::Advisory => "advisory",
                gf_api::WorkspaceOntologyMode::Strict => "strict",
            }
            .into(),
            source_format: record.source_format.map(|format| {
                match format {
                    gf_api::WorkspaceOntologySourceFormat::Yaml => "yaml",
                    gf_api::WorkspaceOntologySourceFormat::Json => "json",
                }
                .into()
            }),
            canonical_ontology_sha256: record.canonical_ontology_sha256,
            canonical_ontology: record.canonical_ontology,
        })
    }

    /// Adopt an ontology as durable project authority.
    #[napi]
    pub fn adopt_ontology(
        &self,
        path: String,
        mode: String,
        operation_uuid: String,
        actor_uuid: Option<String>,
    ) -> Result<()> {
        let request = gf_api::AdoptOntologyRequest {
            context: WriteContext {
                operation_uuid: canonical_operation_id(&operation_uuid)?,
                actor_uuid: optional_uuid(actor_uuid.as_deref())?,
            },
            path: path.into(),
            mode: ontology_mode(&mode)?,
        };
        let mut graph = self.open_write_guard()?;
        graph
            .adopt_ontology(request)
            .map_err(|error| to_napi_err(&error))
    }

    /// Publish explicit durable ontology absence.
    #[napi]
    pub fn clear_ontology(&self, operation_uuid: String, actor_uuid: Option<String>) -> Result<()> {
        let request = gf_api::ClearOntologyRequest {
            context: WriteContext {
                operation_uuid: canonical_operation_id(&operation_uuid)?,
                actor_uuid: optional_uuid(actor_uuid.as_deref())?,
            },
        };
        let mut graph = self.open_write_guard()?;
        graph
            .clear_ontology(request)
            .map_err(|error| to_napi_err(&error))
    }

    // ----- Analyst verbs.

    /// Rank nodes through the Rust registry. Returns an Arrow IPC `Buffer`.
    #[napi]
    pub fn rank(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        write_property: Option<String>,
    ) -> Result<Buffer> {
        let g = self.open_guard()?;
        let options = gf_api::RankOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            write_property,
        };
        let batch = g
            .rank(&label, options)
            .map_err(|error| to_napi_err(&error))?;
        Ok(Buffer::from(
            record_batch_to_ipc(&batch).map_err(|error| to_napi_err(&error))?,
        ))
    }

    /// Prepare a Rust-owned neutral rank invocation without executing it.
    #[napi]
    pub fn prepare_rank_invocation(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
    ) -> Result<InvocationDescriptorHandle> {
        let graph = self.open_guard()?;
        let options = gf_api::RankOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            write_property: None,
        };
        graph
            .prepare_rank_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare clustering without executing it.
    #[napi]
    pub fn prepare_cluster_invocation(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        vector_property: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let graph = self.open_guard()?;
        let options = gf_api::ClusterOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            vector_property,
            via,
            directed: directed.unwrap_or(false),
            write_property: None,
        };
        graph
            .prepare_cluster_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare paths without executing it.
    #[napi(
        ts_args_type = "source: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, target: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, by: string, via?: string | null, directed?: boolean | null, k?: number | null, weight?: string | null, heuristic?: string | null, walkLength?: number | null, seed?: bigint | null, terminalUuids?: string[] | null, prizeProperty?: string | null, capacityProperty?: string | null, costProperty?: string | null"
    )]
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_paths_invocation(
        &self,
        source: Option<NodeSelectorInput<'_>>,
        target: Option<NodeSelectorInput<'_>>,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        k: Option<u32>,
        weight: Option<String>,
        heuristic: Option<String>,
        walk_length: Option<u32>,
        seed: Option<BigInt>,
        terminal_uuids: Option<Vec<String>>,
        prize_property: Option<String>,
        capacity_property: Option<String>,
        cost_property: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let graph = self.open_guard()?;
        let seed = seed.map(parse_seed).transpose()?;
        let options = PathsOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            k: k.unwrap_or(1) as usize,
            weight,
            capacity_property,
            cost_property,
            heuristic,
            walk_length: walk_length.map(|value| value as usize),
            seed,
            terminal_uuids: parse_terminal_uuids(terminal_uuids.as_deref().unwrap_or_default())?,
            prize_property,
        };
        let source = source.map(node_selector_from_input).transpose()?;
        let target = target.map(node_selector_from_input).transpose()?;
        graph
            .prepare_paths_invocation(source.as_ref(), target.as_ref(), &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare analysis without executing it.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_analyze_invocation(
        &self,
        by: String,
        label: Option<String>,
        via: Option<String>,
        directed: Option<bool>,
        weight: Option<String>,
        partition_property: Option<String>,
        k: Option<u32>,
    ) -> Result<InvocationDescriptorHandle> {
        let graph = self.open_guard()?;
        let options = AnalyzeOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            weight,
            k: k.map(|value| value as usize),
            partition_property,
        };
        graph
            .prepare_analyze_invocation(label.as_deref(), &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Prepare similarity without executing it.
    #[napi]
    pub fn prepare_similar_invocation(
        &self,
        label: String,
        by: String,
        k: Option<u32>,
        vector_property: Option<String>,
        via: Option<String>,
    ) -> Result<InvocationDescriptorHandle> {
        let graph = self.open_guard()?;
        let options = SimilarOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            k: k.unwrap_or(10) as usize,
            vector_property,
            via,
        };
        graph
            .prepare_similar_invocation(&label, &options)
            .map(|inner| InvocationDescriptorHandle { inner })
            .map_err(|error| to_napi_invocation_err(&error))
    }

    /// Return every live M18 descriptor contract in deterministic catalog order.
    ///
    /// This projects the Rust-owned registry without opening knowledge or
    /// epistemic storage. Callers still prepare and invoke descriptors through
    /// the neutral facade below.
    #[napi]
    pub fn algorithm_descriptor_contracts(&self) -> Result<Vec<AlgorithmDescriptorContractJs>> {
        self.ensure_open()?;
        Ok(algorithm_descriptor_contracts()
            .into_iter()
            .map(|contract| AlgorithmDescriptorContractJs {
                algorithm: contract.algorithm.as_str().to_owned(),
                algorithm_version: contract.algorithm_version,
                result_schema_version: contract.result_schema_version,
                verb: contract.algorithm.verb().as_str().to_owned(),
            })
            .collect())
    }

    /// Dispatch an opaque descriptor through its Rust-owned analyst verb.
    #[napi]
    pub fn invoke_descriptor(
        &self,
        descriptor: ClassInstance<'_, InvocationDescriptorHandle>,
    ) -> Result<Buffer> {
        let graph = self.open_guard()?;
        let batch = graph
            .invoke_descriptor(&descriptor.inner)
            .map_err(|error| to_napi_invocation_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Decode canonical descriptor bytes in Rust and dispatch them.
    #[napi]
    pub fn invoke_descriptor_bytes(&self, descriptor: Buffer) -> Result<Buffer> {
        let graph = self.open_guard()?;
        let batch = graph
            .invoke_descriptor_bytes(descriptor.as_ref())
            .map_err(|error| to_napi_invocation_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Resolve an immutable graph-only projection from explicit M21 policy.
    #[napi(ts_return_type = "Promise<ResolvedBeliefProjection>")]
    pub fn resolve_belief_projection(
        &self,
        request: ResolveBeliefProjectionInput,
    ) -> Result<AsyncTask<ResolveBeliefProjectionTask>> {
        self.ensure_open()?;
        let signal = request.signal;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ResolveBeliefProjectionTask {
            engine: Arc::clone(&self.inner),
            request: ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: request.transaction_cutoff_micros,
                valid_time_micros: request.valid_time_micros,
                policy: belief_projection_policy(request.policy)?,
            },
            cancellation,
        }))
    }

    /// Resolve one explicit belief subject and its projection from one generation.
    #[napi(
        ts_args_type = "request: ({ assertionUuid: string; hypothesisQuestionKey?: never } | { assertionUuid?: never; hypothesisQuestionKey: string }) & { transactionCutoffMicros: number; validTimeMicros?: number; policy: Required<BeliefSubjectPolicyInput>; signal?: AbortSignal }",
        ts_return_type = "Promise<ResolvedBeliefSubjectOutput>"
    )]
    pub fn resolve_belief_subject(
        &self,
        request: ResolveBeliefSubjectInput,
    ) -> Result<AsyncTask<ResolveBeliefSubjectTask>> {
        self.ensure_open()?;
        let subject = match (
            request.assertion_uuid.as_deref(),
            request.hypothesis_question_key,
        ) {
            (Some(assertion_uuid), None) => {
                BeliefSubjectV1::Assertion(canonical_operation_id(assertion_uuid)?.0)
            }
            (None, Some(question_key)) => BeliefSubjectV1::HypothesisQuestionKey(question_key),
            _ => {
                return Err(napi_validation(
                    "exactly one of assertionUuid or hypothesisQuestionKey is required",
                ));
            }
        };
        let signal = request.signal;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ResolveBeliefSubjectTask {
            engine: Arc::clone(&self.inner),
            request: ResolveBeliefSubjectRequest {
                subject,
                projection: ResolveBeliefProjectionRequest {
                    transaction_cutoff_micros: request.transaction_cutoff_micros,
                    valid_time_micros: request.valid_time_micros,
                    policy: belief_subject_policy(request.policy)?,
                },
            },
            cancellation,
        }))
    }

    /// Execute one recorded descriptor on a resolved projection, then attach it.
    #[napi(ts_return_type = "Promise<ResolvedRecordedAlgorithmOutput>")]
    pub fn invoke_resolved_recorded(
        &self,
        projection: ClassInstance<'_, ResolvedBeliefProjectionHandle>,
        request: ResolvedRecordedAlgorithmInput<'_>,
    ) -> Result<AsyncTask<ResolvedRecordedAlgorithmTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let run_uuid = canonical_operation_id(&request.run_uuid)?.0;
        let attachment_uuid = canonical_operation_id(&request.attachment_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let signal = request.signal;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(ResolvedRecordedAlgorithmTask {
            engine: Arc::clone(&self.inner),
            projection: Arc::clone(&projection.inner),
            request: ResolvedRecordedAlgorithmRequest {
                recorded: gf_api::RecordedAlgorithmRequest {
                    context: WriteContext {
                        operation_uuid,
                        actor_uuid,
                    },
                    run_uuid,
                    descriptor: request.descriptor.inner.clone(),
                    cancellation: Some(cancellation),
                },
                attachment_uuid,
            },
        }))
    }

    /// Retry only the attachment for an already-completed resolved run.
    ///
    /// Cancellation is cooperative before publication starts; an already-started
    /// durable M21 publication still runs to its atomic outcome.
    #[napi(ts_return_type = "Promise<Buffer>")]
    pub fn attach_resolved_run(
        &self,
        projection: ClassInstance<'_, ResolvedBeliefProjectionHandle>,
        request: AttachResolvedRunInput<'_>,
    ) -> Result<AsyncTask<AttachResolvedRunTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let attachment_uuid = canonical_operation_id(&request.attachment_uuid)?.0;
        let run_uuid = canonical_operation_id(&request.run_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let signal = request.signal;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(AttachResolvedRunTask {
            engine: Arc::clone(&self.inner),
            projection: Arc::clone(&projection.inner),
            request: AttachResolvedRunRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                attachment_uuid,
                run_uuid,
                descriptor: request.descriptor.inner.clone(),
            },
            cancellation,
        }))
    }

    /// Durably record a lifecycle around the unchanged descriptor dispatch.
    #[napi(ts_return_type = "Promise<RecordedAlgorithmOutput>")]
    pub fn invoke_recorded(
        &self,
        request: RecordedAlgorithmInput<'_>,
    ) -> Result<AsyncTask<RecordedAlgorithmTask>> {
        self.ensure_open()?;
        let operation_uuid = canonical_operation_id(&request.operation_uuid)?;
        let run_uuid = canonical_operation_id(&request.run_uuid)?.0;
        let actor_uuid = request
            .actor_uuid
            .as_deref()
            .map(canonical_operation_id)
            .transpose()?
            .map(|value| value.0);
        let signal = request.signal;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(RecordedAlgorithmTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::RecordedAlgorithmRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid,
                },
                run_uuid,
                descriptor: request.descriptor.inner.clone(),
                cancellation: Some(cancellation),
            },
        }))
    }

    /// Return one immutable algorithm-run identity as Arrow IPC.
    #[napi]
    pub fn algorithm_run(
        &self,
        run_uuid: String,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<AlgorithmRunTask>> {
        self.ensure_open()?;
        let run_uuid = canonical_operation_id(&run_uuid)?;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        Ok(AsyncTask::new(AlgorithmRunTask {
            engine: Arc::clone(&self.inner),
            run_uuid,
            cancellation,
        }))
    }

    /// Return one deterministic generation-bound run page as Arrow IPC.
    #[napi]
    pub fn list_algorithm_runs(
        &self,
        request: Option<ListAlgorithmRunsInput>,
    ) -> Result<AsyncTask<ListAlgorithmRunsTask>> {
        self.ensure_open()?;
        let request = request.unwrap_or(ListAlgorithmRunsInput {
            algorithm: None,
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        let algorithm = request
            .algorithm
            .as_deref()
            .map(parse_algorithm_id)
            .transpose()?;
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        Ok(AsyncTask::new(ListAlgorithmRunsTask {
            engine: Arc::clone(&self.inner),
            request: gf_api::ListAlgorithmRunsRequest {
                algorithm,
                page: gf_api::PageRequest {
                    limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                    after,
                    cancellation: Some(cancellation),
                },
            },
        }))
    }

    /// Return one deterministic generation-bound lifecycle page as Arrow IPC.
    #[napi]
    pub fn algorithm_run_events(
        &self,
        run_uuid: String,
        request: Option<AlgorithmRunEventsInput>,
    ) -> Result<AsyncTask<AlgorithmRunEventsTask>> {
        self.ensure_open()?;
        let run_uuid = canonical_operation_id(&run_uuid)?;
        let request = request.unwrap_or(AlgorithmRunEventsInput {
            limit: None,
            after: None,
            signal: None,
        });
        let signal = request.signal;
        let cancellation = gf_api::CancellationToken::new();
        if let Some(signal) = &signal {
            let cancellation = cancellation.clone();
            signal.on_abort(move || cancellation.cancel());
        }
        let after = request
            .after
            .as_deref()
            .map(gf_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_napi_err(&error))?;
        Ok(AsyncTask::new(AlgorithmRunEventsTask {
            engine: Arc::clone(&self.inner),
            run_uuid,
            page: gf_api::PageRequest {
                limit: request.limit.unwrap_or(gf_api::DEFAULT_PAGE_LIMIT),
                after,
                cancellation: Some(cancellation),
            },
        }))
    }

    /// Detect communities/components. Returns an Arrow IPC `Buffer`.
    #[napi]
    pub fn cluster(
        &self,
        label: String,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        write_property: Option<String>,
        vector_property: Option<String>,
    ) -> Result<Buffer> {
        let g = self.open_guard()?;
        let options = gf_api::ClusterOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            vector_property,
            via,
            directed: directed.unwrap_or(false),
            write_property,
        };
        let batch = g
            .cluster(&label, options)
            .map_err(|error| to_napi_err(&error))?;
        Ok(Buffer::from(
            record_batch_to_ipc(&batch).map_err(|error| to_napi_err(&error))?,
        ))
    }

    /// Path-finding / flow between typed node selectors. Returns Arrow IPC.
    #[napi(
        ts_args_type = "source: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, target: string | NodeHandle | { label: string; property: string; value: any } | null | undefined, by: string, via?: string | null, directed?: boolean | null, k?: number | null, weight?: string | null, heuristic?: string | null, walkLength?: number | null, seed?: bigint | null, terminalUuids?: string[] | null, prizeProperty?: string | null, capacityProperty?: string | null, costProperty?: string | null"
    )]
    #[allow(clippy::too_many_arguments)] // kwarg-rich v0.5 paths() signature
    pub fn paths(
        &self,
        source: Option<NodeSelectorInput<'_>>,
        target: Option<NodeSelectorInput<'_>>,
        by: String,
        via: Option<String>,
        directed: Option<bool>,
        k: Option<u32>,
        weight: Option<String>,
        heuristic: Option<String>,
        walk_length: Option<u32>,
        seed: Option<BigInt>,
        terminal_uuids: Option<Vec<String>>,
        prize_property: Option<String>,
        capacity_property: Option<String>,
        cost_property: Option<String>,
    ) -> Result<Buffer> {
        self.ensure_open()?;
        let seed = seed
            .map(|value| {
                let (negative, seed, lossless) = value.get_u64();
                if !negative && lossless {
                    Ok(seed)
                } else {
                    Err(to_napi_err(&GfError::Validation(
                        "seed must be an unsigned 64-bit integer".into(),
                    )))
                }
            })
            .transpose()?;
        let options = PathsOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            via,
            directed: directed.unwrap_or(true),
            k: k.unwrap_or(1) as usize,
            weight,
            capacity_property,
            cost_property,
            heuristic,
            walk_length: walk_length.map(|value| value as usize),
            seed,
            terminal_uuids: parse_terminal_uuids(terminal_uuids.as_deref().unwrap_or_default())?,
            prize_property,
        };
        let source = source.map(node_selector_from_input).transpose()?;
        let target = target.map(node_selector_from_input).transpose()?;
        let graph = self.open_guard()?;
        let batch = graph
            .paths(source.as_ref(), target.as_ref(), options)
            .map_err(|error| to_napi_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Graph-level structural metric. Returns an Arrow IPC `Buffer`.
    #[allow(clippy::too_many_arguments)] // positional v0.5 analyze() signature
    #[napi]
    pub fn analyze(
        &self,
        by: String,
        label: Option<String>,
        via: Option<String>,
        directed: Option<bool>,
        weight: Option<String>,
        partition_property: Option<String>,
        k: Option<u32>,
        embedding_options: Option<EmbeddingInput>,
    ) -> Result<Buffer> {
        self.ensure_open()?;
        let algorithm = by.parse().map_err(|error| to_napi_err(&error))?;
        if matches!(
            algorithm,
            AnalyzeAlgorithm::Node2Vec
                | AnalyzeAlgorithm::GraphSage
                | AnalyzeAlgorithm::FastRandomProjection
                | AnalyzeAlgorithm::HashGnn
        ) {
            if partition_property.is_some() || k.is_some() {
                return Err(embedding_error(
                    "embedding algorithms do not accept partition_property or k",
                ));
            }
            let directed = directed.unwrap_or(!matches!(algorithm, AnalyzeAlgorithm::GraphSage));
            let options = embedding_options_from_input(
                algorithm,
                via,
                directed,
                weight,
                embedding_options.unwrap_or_default(),
            )?;
            validate_embedding_options(&options).map_err(|error| to_napi_err(&error))?;
            let graph = self.open_guard()?;
            let batch = graph
                .analyze_embedding(label.as_deref(), &options)
                .map_err(|error| to_napi_err(&error))?;
            return record_batch_to_ipc(&batch)
                .map(Buffer::from)
                .map_err(|error| to_napi_err(&error));
        }
        if embedding_options.is_some() {
            return Err(embedding_error(format!(
                "{by} does not accept embedding options"
            )));
        }
        // Keep binding construction extension-safe as AnalyzeOptions gains fields.
        #[allow(clippy::needless_update)]
        let options = AnalyzeOptions {
            by: algorithm,
            via,
            directed: directed.unwrap_or(true),
            weight,
            k: k.map(|value| value as usize),
            partition_property,
            ..AnalyzeOptions::default()
        };
        let graph = self.open_guard()?;
        let batch = graph
            .analyze(label.as_deref(), options)
            .map_err(|error| to_napi_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Pairwise node similarity. Returns an Arrow IPC `Buffer`.
    #[napi]
    pub fn similar(
        &self,
        label: String,
        by: String,
        k: Option<u32>,
        vector_property: Option<String>,
        via: Option<String>,
    ) -> Result<Buffer> {
        let options = SimilarOptions {
            by: by.parse().map_err(|error| to_napi_err(&error))?,
            k: k.unwrap_or(10) as usize,
            vector_property,
            via,
        };
        let graph = self.open_guard()?;
        let batch = graph
            .similar(&label, options)
            .map_err(|error| to_napi_err(&error))?;
        record_batch_to_ipc(&batch)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

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

    /// Atomically publish one complete canonical M18 Arrow IPC result.
    #[napi]
    pub fn publish_m18_embeddings(
        &self,
        name: String,
        result: Buffer,
        input: M18EmbeddingPublicationInput,
    ) -> Result<String> {
        let normalization = match input.normalization.as_deref().unwrap_or("none") {
            "none" => M18EmbeddingNormalization::None,
            "l2" => M18EmbeddingNormalization::L2,
            other => {
                return Err(to_napi_err(&GfError::Validation(format!(
                    "unknown M18 embedding normalization {other:?}"
                ))));
            }
        };
        let algorithm = input
            .algorithm
            .parse::<AnalyzeAlgorithm>()
            .map_err(|error| to_napi_err(&error))?;
        let graph = self.open_guard()?;
        graph
            .publish_m18_embeddings(M18EmbeddingPublicationRequest {
                display_name: name,
                algorithm,
                algorithm_version: input.algorithm_version,
                dimensions: input.dimensions,
                normalization,
                distance: M18EmbeddingDistance::Cosine,
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

    // ----- Construction (write API) — not yet implemented (raise NotImplementedError).

    /// Add a node through the Rust facade and return its graph-owned UUID handle.
    #[napi]
    pub fn add_node(
        &self,
        label: String,
        props: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<NodeHandle> {
        self.ensure_open()?;
        let props = props_from_map(props)?;
        let graph = self.open_guard()?;
        graph
            .add_node(&label, &props)
            .map(|inner| NodeHandle { inner })
            .map_err(|error| to_napi_err(&error))
    }

    /// Add a directed edge and return its graph UUID handle.
    #[napi]
    pub fn add_edge(
        &self,
        src: ClassInstance<'_, NodeHandle>,
        rel_type: String,
        dst: ClassInstance<'_, NodeHandle>,
        props: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<EdgeHandle> {
        let props = props_from_map(props)?;
        let graph = self.open_guard()?;
        graph
            .add_edge(&src.inner, &rel_type, &dst.inner, &props)
            .map(|inner| EdgeHandle { inner })
            .map_err(|error| to_napi_err(&error))
    }

    /// Publish one atomic bulk node batch (Arrow IPC) through the Rust contract.
    #[napi]
    pub fn publish_bulk_nodes(&self, operation_uuid: String, data: Buffer) -> Result<Buffer> {
        let operation_uuid = canonical_operation_id(&operation_uuid)?;
        let batch = ipc_to_record_batch(&data)?;
        let graph = self.open_guard()?;
        let receipt = graph
            .publish_bulk_nodes(operation_uuid, &[batch])
            .map_err(bulk_node_publication_error)?;
        record_batch_to_ipc(&receipt)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Publish one atomic bulk edge batch (Arrow IPC) through the Rust contract.
    #[napi]
    pub fn publish_bulk_edges(&self, operation_uuid: String, data: Buffer) -> Result<Buffer> {
        let operation_uuid = canonical_operation_id(&operation_uuid)?;
        let batch = ipc_to_record_batch(&data)?;
        let graph = self.open_guard()?;
        let receipt = graph
            .publish_bulk_edges(operation_uuid, &[batch])
            .map_err(bulk_edge_publication_error)?;
        record_batch_to_ipc(&receipt)
            .map(Buffer::from)
            .map_err(|error| to_napi_err(&error))
    }

    /// Publish one composite graph + knowledge transaction through Rust.
    ///
    /// Returns the canonical singleton Arrow IPC receipt. Node performs only
    /// request conversion; validation, staging, publication, recovery, and
    /// idempotency remain Rust-owned.
    #[napi]
    pub fn publish_composite_transaction(
        &self,
        request: CompositeTransactionInput,
    ) -> Result<Buffer> {
        self.ensure_open()?;
        let graph = self.open_guard()?;
        composite::publish_composite_transaction(&graph, request)
    }

    /// Bulk-add nodes (Arrow IPC / records).
    #[napi]
    #[allow(unused_variables)]
    pub fn add_nodes(&self, label: String, data: Buffer) -> Result<()> {
        let g = self.open_guard()?;
        g.add_nodes().map_err(|e| to_napi_err(&e))
    }

    /// Bulk-add edges (Arrow IPC / records).
    #[napi]
    #[allow(unused_variables)]
    pub fn add_edges(&self, rel_type: String, data: Buffer) -> Result<()> {
        let g = self.open_guard()?;
        g.add_edges().map_err(|e| to_napi_err(&e))
    }

    // ----- Transactions — not yet implemented (raise NotImplementedError).

    /// Begin a transaction.
    #[napi]
    pub fn begin(&self) -> Result<()> {
        let g = self.open_guard()?;
        g.begin().map_err(|e| to_napi_err(&e))
    }

    /// Commit the current transaction.
    #[napi]
    pub fn commit(&self) -> Result<()> {
        let g = self.open_guard()?;
        g.commit().map_err(|e| to_napi_err(&e))
    }

    /// Roll back the current transaction.
    #[napi]
    pub fn rollback(&self) -> Result<()> {
        let g = self.open_guard()?;
        g.rollback().map_err(|e| to_napi_err(&e))
    }

    /// Remove all nodes and edges (in-memory instances only).
    #[napi]
    pub fn clear(&self) -> Result<()> {
        let g = self.open_guard()?;
        g.clear().map_err(|e| to_napi_err(&e))
    }

    // ----- Introspection.

    /// Schema summary as an Arrow IPC `Buffer` (label/property/type).
    #[napi]
    pub fn schema(&self) -> Result<Buffer> {
        self.ensure_open()?;
        Err(to_napi_err(&GfError::NotImplemented("schema")))
    }

    /// The node labels present in the graph.
    #[napi]
    pub fn labels(&self) -> Result<Vec<String>> {
        let g = self.open_guard()?;
        g.labels().map_err(|e| to_napi_err(&e))
    }

    /// The relationship types present in the graph.
    #[napi]
    pub fn relationship_types(&self) -> Result<Vec<String>> {
        let g = self.open_guard()?;
        g.relationship_types().map_err(|e| to_napi_err(&e))
    }

    /// Count nodes (optionally for one `label`).
    #[napi]
    pub fn node_count(&self, label: Option<String>) -> Result<i64> {
        let g = self.open_guard()?;
        let n = g
            .node_count(label.as_deref().unwrap_or(""))
            .map_err(|e| to_napi_err(&e))?;
        Ok(i64::try_from(n).unwrap_or(i64::MAX))
    }

    /// Close the instance; subsequent operations raise `LifecycleError`. Idempotent.
    #[napi]
    pub fn close(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.inner.close();
    }

    /// The storage path, or `null` for an in-memory instance.
    #[napi(getter)]
    pub fn path(&self) -> Result<Option<String>> {
        let g = self.open_guard()?;
        Ok(g.path().map(|p| p.display().to_string()))
    }

    /// The effective ontology mode: `"exploratory"` | `"advisory"` | `"strict"`.
    #[napi(getter)]
    pub fn ontology_mode(&self) -> Result<String> {
        let g = self.open_guard()?;
        Ok(format!("{:?}", g.ontology_mode()).to_lowercase())
    }

    /// Prepare a deferred query, returning a [`PlanHandle`] for `explain()`
    /// (sync) and the async `collectIpc()` / `sinkParquet()` sinks.
    #[napi]
    pub fn plan(
        &self,
        cypher: String,
        params: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<PlanHandle> {
        self.ensure_open()?;
        let p = params_from_map(params)?;
        Ok(PlanHandle {
            engine: Arc::clone(&self.inner),
            closed: Arc::clone(&self.closed),
            cypher,
            params: p,
        })
    }
}

enum CheckpointOperation {
    Create(gf_api::CheckpointRequest),
    List(gf_api::ListCheckpointsRequest),
    Delete(gf_api::DeleteCheckpointRequest),
    Diff(gf_api::DiffCheckpointsRequest),
    Revert(gf_api::RevertCheckpointRequest),
}

/// Worker task for Rust-owned checkpoint operations.
pub struct CheckpointTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    operation: CheckpointOperation,
}

impl Task for CheckpointTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let result = match &self.operation {
                CheckpointOperation::Create(request) => self
                    .engine
                    .read()
                    .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                    .checkpoint(request.clone())?,
                CheckpointOperation::List(request) => self
                    .engine
                    .read()
                    .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                    .list_checkpoints(request.clone())?,
                CheckpointOperation::Delete(request) => self
                    .engine
                    .read()
                    .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                    .delete_checkpoint(request.clone())?,
                CheckpointOperation::Diff(request) => self
                    .engine
                    .read()
                    .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                    .diff_checkpoints(request.clone())?,
                CheckpointOperation::Revert(request) => self
                    .engine
                    .write()
                    .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?
                    .revert_to_checkpoint(request.clone())?,
            };
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for manifest-only capability inspection.
pub struct ProjectCapabilitiesTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
}

impl Task for ProjectCapabilitiesTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result = graph.project_capabilities()?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for atomic capability initialization.
pub struct EnableCapabilityTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::EnableCapabilityRequest,
}

impl Task for EnableCapabilityTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result = graph.enable_capability(self.request.clone())?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact provenance event.
pub struct ProvenanceEventTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    provenance_uuid: OperationId,
    cancellation: gf_api::CancellationToken,
}

impl Task for ProvenanceEventTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result =
                graph.provenance_event(self.provenance_uuid.0, Some(self.cancellation.clone()))?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one deterministic provenance-history page.
pub struct ProvenanceHistoryTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::ProvenanceHistoryRequest,
}

impl Task for ProvenanceHistoryTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result = graph.list_provenance_history(self.request.clone())?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic assertion publication.
pub struct CreateAssertionTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::CreateAssertionRequest,
}

impl Task for CreateAssertionTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.create_assertion(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic assertion-plus-evidence publication.
pub struct CreateAssertionWithEvidenceTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::CreateAssertionWithEvidenceRequest,
}

impl Task for CreateAssertionWithEvidenceTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.create_assertion_with_evidence(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact assertion.
pub struct AssertionTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    assertion_uuid: OperationId,
    cancellation: gf_api::CancellationToken,
}

impl Task for AssertionTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.assertion(self.assertion_uuid.0, Some(self.cancellation.clone()))?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion page.
pub struct ListAssertionsTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::ListAssertionsRequest,
}

impl Task for ListAssertionsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_assertions(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion graph-reference page.
pub struct AssertionGraphRefsTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    assertion_uuid: OperationId,
    page: gf_api::PageRequest,
}

impl Task for AssertionGraphRefsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.assertion_graph_refs(self.assertion_uuid.0, self.page.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic confidence publication.
pub struct AssessConfidenceTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::AssessConfidenceRequest,
}

impl Task for AssessConfidenceTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.assess_confidence(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact confidence assessment.
pub struct ConfidenceAssessmentTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    confidence_uuid: OperationId,
    cancellation: gf_api::CancellationToken,
}

impl Task for ConfidenceAssessmentTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(
                &graph.confidence_assessment(
                    self.confidence_uuid.0,
                    Some(self.cancellation.clone()),
                )?,
            )
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one confidence-assessment page.
pub struct ListConfidenceAssessmentsTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::ListConfidenceAssessmentsRequest,
}

impl Task for ListConfidenceAssessmentsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_confidence_assessments(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one confidence input page.
pub struct ConfidenceInputsTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    confidence_uuid: OperationId,
    page: gf_api::PageRequest,
}

impl Task for ConfidenceInputsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.confidence_inputs(self.confidence_uuid.0, self.page.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic evidence publication.
pub struct AttachEvidenceTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::AttachEvidenceRequest,
}

impl Task for AttachEvidenceTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.attach_evidence(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact evidence link.
pub struct EvidenceLinkTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    evidence_uuid: OperationId,
    cancellation: gf_api::CancellationToken,
}

impl Task for EvidenceLinkTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(
                &graph.evidence_link(self.evidence_uuid.0, Some(self.cancellation.clone()))?,
            )
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one evidence-link page.
pub struct ListEvidenceLinksTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::ListEvidenceLinksRequest,
}

impl Task for ListEvidenceLinksTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_evidence_links(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic reasoning publication.
pub struct RecordReasoningTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::RecordReasoningRequest,
}

impl Task for RecordReasoningTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.record_reasoning(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact reasoning record.
pub struct ReasoningTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    reasoning_uuid: OperationId,
    cancellation: gf_api::CancellationToken,
}

impl Task for ReasoningTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.reasoning(self.reasoning_uuid.0, Some(self.cancellation.clone()))?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one reasoning-history page.
pub struct ListReasoningTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::ListReasoningRequest,
}

impl Task for ListReasoningTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_reasoning(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic assertion-plus-first-status publication.
pub struct CreateAssertionWithStatusTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::CreateAssertionWithStatusRequest,
}

impl Task for CreateAssertionWithStatusTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.create_assertion_with_status(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion-status append.
pub struct RecordAssertionStatusTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::RecordAssertionStatusRequest,
}

impl Task for RecordAssertionStatusTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.record_assertion_status(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion's current explicit status.
pub struct AssertionStatusTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    assertion_uuid: OperationId,
}

impl Task for AssertionStatusTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.assertion_status(self.assertion_uuid.0)?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion-status history page.
pub struct ListAssertionStatusTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::ListAssertionStatusRequest,
}

impl Task for ListAssertionStatusTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_assertion_status(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion-validity append.
pub struct RecordAssertionValidityTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::RecordAssertionValidityRequest,
}

impl Task for RecordAssertionValidityTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.record_assertion_validity(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one assertion-validity history page.
pub struct ListAssertionValidityTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::ListAssertionValidityRequest,
}

impl Task for ListAssertionValidityTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_assertion_validity(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one valid-time projection.
pub struct ApplyValidTimeTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::ApplyValidTimeRequest,
}

impl Task for ApplyValidTimeTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.apply_valid_time(self.request)?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one atomic assertion supersession.
pub struct SupersedeAssertionTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::SupersedeAssertionRequest,
}

impl Task for SupersedeAssertionTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.supersede_assertion(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for a deterministic supersession-history page.
pub struct ListAssertionSupersessionsTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::ListAssertionSupersessionsRequest,
}

impl Task for ListAssertionSupersessionsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_assertion_supersessions(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

enum HypothesisOperation {
    Create(gf_api::CreateHypothesisGroupRequest),
    Membership(gf_api::RecordHypothesisMembershipRequest),
    Selection(gf_api::RecordHypothesisSelectionRequest),
    Remove(gf_api::RemoveHypothesisMemberRequest),
    ListGroups(gf_api::ListHypothesisGroupsRequest),
    ListMembership(gf_api::ListHypothesisMembershipRequest),
    ListSelection(gf_api::ListHypothesisSelectionRequest),
    Members(OperationId),
    CurrentSelection(OperationId),
}

/// Worker task for hypothesis mutation and projection operations.
pub struct HypothesisTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    operation: HypothesisOperation,
}

impl Task for HypothesisTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result = match &self.operation {
                HypothesisOperation::Create(request) => {
                    graph.create_hypothesis_group(request.clone())
                }
                HypothesisOperation::Membership(request) => {
                    graph.record_hypothesis_membership(request)
                }
                HypothesisOperation::Selection(request) => {
                    graph.record_hypothesis_selection(request)
                }
                HypothesisOperation::Remove(request) => graph.remove_hypothesis_member(request),
                HypothesisOperation::ListGroups(request) => graph.list_hypothesis_groups(request),
                HypothesisOperation::ListMembership(request) => {
                    graph.list_hypothesis_membership(request)
                }
                HypothesisOperation::ListSelection(request) => {
                    graph.list_hypothesis_selection(request)
                }
                HypothesisOperation::Members(group_uuid) => graph.hypothesis_members(group_uuid.0),
                HypothesisOperation::CurrentSelection(group_uuid) => {
                    graph.hypothesis_selection(group_uuid.0)
                }
            }?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one deterministic M21 transaction-time snapshot.
pub struct EpistemicSnapshotTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    transaction_cutoff: i64,
}

impl Task for EpistemicSnapshotTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.epistemic_snapshot(self.transaction_cutoff)?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one resolved M21 projection.
pub struct ResolveBeliefProjectionTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: ResolveBeliefProjectionRequest,
    cancellation: gf_api::CancellationToken,
}

/// Worker task for one same-generation subject evidence and graph projection.
pub struct ResolveBeliefSubjectTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: ResolveBeliefSubjectRequest,
    cancellation: gf_api::CancellationToken,
}

impl Task for ResolveBeliefSubjectTask {
    type Output = std::result::Result<ResolvedBeliefSubject, GfError>;
    type JsValue = ResolvedBeliefSubjectOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            if self.cancellation.is_cancelled() {
                return Err(cancelled_error());
            }
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            graph.resolve_belief_subject(&self.request)
        })())
    }

    fn resolve(
        &mut self,
        env: Env,
        output: Self::Output,
    ) -> napi::Result<ResolvedBeliefSubjectOutput> {
        output
            .and_then(|resolved| {
                Ok(ResolvedBeliefSubjectOutput {
                    projection: Arc::new(resolved.projection),
                    evidence: result_to_ipc(&resolved.evidence)?,
                })
            })
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

impl Task for ResolveBeliefProjectionTask {
    type Output = std::result::Result<ResolvedBeliefProjection, GfError>;
    type JsValue = ResolvedBeliefProjectionHandle;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            if self.cancellation.is_cancelled() {
                return Err(cancelled_error());
            }
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            graph.resolve_belief_projection(self.request.clone())
        })())
    }

    fn resolve(
        &mut self,
        env: Env,
        output: Self::Output,
    ) -> napi::Result<ResolvedBeliefProjectionHandle> {
        output
            .map(|inner| ResolvedBeliefProjectionHandle {
                inner: Arc::new(inner),
            })
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Rust-side transport used to materialize the public napi output on the main thread.
pub struct ResolvedRecordedOutputData {
    run_uuid: String,
    result: Vec<u8>,
    attachment_uuid: String,
    attachment_state: String,
    attachment: Option<Vec<u8>>,
    attachment_error_code: Option<String>,
}

/// Worker task for one resolved recorded dispatch and its attachment outcome.
pub struct ResolvedRecordedAlgorithmTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    projection: Arc<ResolvedBeliefProjection>,
    request: ResolvedRecordedAlgorithmRequest,
}

impl Task for ResolvedRecordedAlgorithmTask {
    type Output = std::result::Result<ResolvedRecordedOutputData, GfError>;
    type JsValue = ResolvedRecordedAlgorithmOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let attachment_uuid = self.request.attachment_uuid.to_string();
            let resolved =
                graph.invoke_resolved_recorded(&self.projection, self.request.clone())?;
            let (attachment_state, attachment, attachment_error_code) = match resolved.attachment {
                ResolvedAttachmentOutcome::Attached(result) => {
                    ("attached".to_owned(), Some(result_to_ipc(&result)?), None)
                }
                ResolvedAttachmentOutcome::Failed { error_code, .. } => {
                    ("attachment_failed".to_owned(), None, Some(error_code))
                }
            };
            Ok(ResolvedRecordedOutputData {
                run_uuid: resolved.recorded.run_uuid.to_string(),
                result: result_to_ipc(&resolved.recorded.result)?,
                attachment_uuid,
                attachment_state,
                attachment,
                attachment_error_code,
            })
        })())
    }

    fn resolve(
        &mut self,
        env: Env,
        output: Self::Output,
    ) -> napi::Result<ResolvedRecordedAlgorithmOutput> {
        output
            .map(|value| ResolvedRecordedAlgorithmOutput {
                run_uuid: value.run_uuid,
                result: Buffer::from(value.result),
                attachment_uuid: value.attachment_uuid,
                attachment_state: value.attachment_state,
                attachment: value.attachment.map(Buffer::from),
                attachment_error_code: value.attachment_error_code,
            })
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact attachment retry without algorithm redispatch.
pub struct AttachResolvedRunTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    projection: Arc<ResolvedBeliefProjection>,
    request: AttachResolvedRunRequest,
    cancellation: gf_api::CancellationToken,
}

impl Task for AttachResolvedRunTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            if self.cancellation.is_cancelled() {
                return Err(cancelled_error());
            }
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.attach_resolved_run(&self.projection, self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one recorded algorithm dispatch.
pub struct RecordedAlgorithmTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::RecordedAlgorithmRequest,
}

impl Task for RecordedAlgorithmTask {
    type Output = std::result::Result<(String, Vec<u8>), GfError>;
    type JsValue = RecordedAlgorithmOutput;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let recorded = graph.invoke_recorded(self.request.clone())?;
            Ok((
                recorded.run_uuid.to_string(),
                result_to_ipc(&recorded.result)?,
            ))
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<RecordedAlgorithmOutput> {
        output
            .map(|(run_uuid, result)| RecordedAlgorithmOutput {
                run_uuid,
                result: Buffer::from(result),
            })
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one exact algorithm-run identity.
pub struct AlgorithmRunTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    run_uuid: OperationId,
    cancellation: gf_api::CancellationToken,
}

impl Task for AlgorithmRunTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.algorithm_run(self.run_uuid.0, Some(self.cancellation.clone()))?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one algorithm-run identity page.
pub struct ListAlgorithmRunsTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    request: gf_api::ListAlgorithmRunsRequest,
}

impl Task for ListAlgorithmRunsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.list_algorithm_runs(self.request.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// Worker task for one algorithm-run lifecycle page.
pub struct AlgorithmRunEventsTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    run_uuid: OperationId,
    page: gf_api::PageRequest,
}

impl Task for AlgorithmRunEventsTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            result_to_ipc(&graph.algorithm_run_events(self.run_uuid.0, self.page.clone())?)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

fn to_napi_deferred_err(env: Env, error: &GfError) -> napi::Error {
    let value = napi::JsError::from(to_napi_err(error)).into_unknown(env);
    napi::Error::from(value)
}

fn cancelled_error() -> GfError {
    GfError::Api {
        code: gf_api::ApiErrorCode::Cancelled,
        message: "operation was cancelled".into(),
    }
}

/// A deferred query over a [`GraphForge`], produced by `GraphForge.plan(...)`.
///
/// Shares the parent engine (so it stays usable after the parent is dropped).
/// `explain()` is synchronous; `collectIpc()`/`sinkParquet()` run on a libuv
/// worker thread (napi `AsyncTask`) and return Promises — avoiding a `block_on`
/// inside napi's own runtime. Async rejections preserve GraphForge's structured
/// fault-domain code through the shared deferred-error bridge.
#[napi]
pub struct PlanHandle {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    closed: Arc<AtomicBool>,
    cypher: String,
    params: HashMap<String, IrLiteral>,
}

#[napi]
impl PlanHandle {
    /// Run the query and resolve to an Arrow IPC stream `Buffer`.
    #[napi]
    #[must_use]
    pub fn collect_ipc(&self) -> AsyncTask<CollectIpcTask> {
        AsyncTask::new(CollectIpcTask {
            engine: Arc::clone(&self.engine),
            closed: Arc::clone(&self.closed),
            cypher: self.cypher.clone(),
            params: self.params.clone(),
        })
    }

    /// Explain the compiler pipeline for the deferred query (synchronous).
    #[napi]
    pub fn explain(&self) -> Result<String> {
        if self.closed.load(Ordering::Acquire) {
            return Err(to_napi_err(&GfError::Lifecycle(
                "operation on a closed GraphForge instance".into(),
            )));
        }
        let g = self
            .engine
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))?;
        g.explain(&self.cypher).map_err(|e| to_napi_err(&e))
    }

    /// Run the query and write the result to a Parquet file at `path`.
    #[napi]
    #[must_use]
    pub fn sink_parquet(&self, path: String) -> AsyncTask<SinkParquetTask> {
        AsyncTask::new(SinkParquetTask {
            engine: Arc::clone(&self.engine),
            closed: Arc::clone(&self.closed),
            cypher: self.cypher.clone(),
            params: self.params.clone(),
            path,
        })
    }
}

/// `AsyncTask` backing [`PlanHandle::collect_ipc`].
pub struct CollectIpcTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    closed: Arc<AtomicBool>,
    cypher: String,
    params: HashMap<String, IrLiteral>,
}

impl Task for CollectIpcTask {
    type Output = std::result::Result<Vec<u8>, GfError>;
    type JsValue = Buffer;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            if self.closed.load(Ordering::Acquire) {
                return Err(GfError::Lifecycle(
                    "operation on a closed GraphForge instance".into(),
                ));
            }
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            let result = graph.execute_with_params(&self.cypher, &self.params)?;
            result_to_ipc(&result)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Buffer> {
        output
            .map(Buffer::from)
            .map_err(|error| to_napi_deferred_err(env, &error))
    }
}

/// `AsyncTask` backing [`PlanHandle::sink_parquet`].
pub struct SinkParquetTask {
    engine: Arc<RwLock<gf_api::GraphForge>>,
    closed: Arc<AtomicBool>,
    cypher: String,
    params: HashMap<String, IrLiteral>,
    path: String,
}

impl Task for SinkParquetTask {
    type Output = std::result::Result<(), GfError>;
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Ok((|| {
            if self.closed.load(Ordering::Acquire) {
                return Err(GfError::Lifecycle(
                    "operation on a closed GraphForge instance".into(),
                ));
            }
            let graph = self
                .engine
                .read()
                .map_err(|_| GfError::Execution("GraphForge lock poisoned".into()))?;
            graph.execute_to_parquet_with_params(&self.cypher, &self.params, &self.path)
        })())
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<()> {
        output.map_err(|error| to_napi_deferred_err(env, &error))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;

    use super::*;

    #[test]
    fn query_uuid_tag_is_exact_and_strings_stay_strings() {
        let text = "550e8400-e29b-41d4-a716-446655440000";
        assert!(matches!(
            json_to_ir_literal(&serde_json::json!({"$uuid": text})).unwrap(),
            IrLiteral::Uuid(_)
        ));
        assert_eq!(
            json_to_ir_literal(&serde_json::json!(text)).unwrap(),
            IrLiteral::Str(text.to_owned())
        );
        for (invalid, reason) in [
            (
                serde_json::json!({"$uuid": "not-a-uuid"}),
                "UUID parameter must be canonical hyphenated UUID text",
            ),
            (
                serde_json::json!({"$uuid": text.to_uppercase()}),
                "UUID parameter must be canonical hyphenated UUID text",
            ),
            (
                serde_json::json!({"$uuid": text, "extra": true}),
                "UUID parameter tag must contain only $uuid",
            ),
            (
                serde_json::json!({"$uuid": 7}),
                "UUID parameter $uuid value must be a string",
            ),
        ] {
            let error = json_to_ir_literal(&invalid).unwrap_err();
            assert_eq!(error.status, "GF_VALIDATION", "wrong code for {invalid}");
            assert_eq!(error.reason, reason, "wrong message for {invalid}");
        }
    }

    #[test]
    fn node_engine_lock_allows_shared_reads_and_blocks_exclusive_replacement() {
        let graph = Arc::new(GraphForge::new(None, None).unwrap());

        let read_guard = graph.open_guard().unwrap();
        let reader = Arc::clone(&graph);
        let (read_tx, read_rx) = mpsc::channel();
        let read_thread = thread::spawn(move || {
            read_tx.send(reader.path().is_ok()).unwrap();
        });
        assert!(read_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        drop(read_guard);
        read_thread.join().unwrap();

        let write_guard = graph.open_write_guard().unwrap();
        let blocked_reader = Arc::clone(&graph);
        let (blocked_tx, blocked_rx) = mpsc::channel();
        let blocked_thread = thread::spawn(move || {
            blocked_tx.send(blocked_reader.path().is_ok()).unwrap();
        });
        assert_eq!(
            blocked_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        );
        drop(write_guard);
        assert!(blocked_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        blocked_thread.join().unwrap();
    }

    #[test]
    fn rejects_unsigned_property_values_above_i64() {
        let error = json_to_prop_value(&serde_json::json!(u64::MAX)).unwrap_err();
        assert_eq!(error.status, "ValidationError");
    }

    #[test]
    fn search_vectors_reject_f32_range_loss_and_preserve_backend_validation() {
        assert_eq!(
            vector_from_input(Some(vec![1.0, -0.5])).unwrap(),
            Some(vec![1.0, -0.5])
        );
        assert_eq!(
            vector_from_input(Some(vec![f64::MAX])).unwrap_err().status,
            "ValidationError"
        );
        assert_eq!(
            vector_from_input(Some(vec![f64::from_bits(1)]))
                .unwrap_err()
                .status,
            "ValidationError"
        );

        let non_finite = vector_from_input(Some(vec![f64::NAN, f64::INFINITY]))
            .unwrap()
            .unwrap();
        assert!(non_finite[0].is_nan());
        assert!(non_finite[1].is_infinite());
    }

    #[test]
    fn steiner_terminals_are_checked_and_preserve_input_order() {
        let first = "018f0f4e-7b8c-7000-8000-000000000002".to_owned();
        let second = "018f0f4e-7b8c-7000-8000-000000000001".to_owned();
        let terminals = parse_terminal_uuids(&[first.clone(), second.clone()]).unwrap();

        let NodeSelector::Uuid(first_uuid) = NodeSelector::uuid(&first).unwrap() else {
            unreachable!()
        };
        let NodeSelector::Uuid(second_uuid) = NodeSelector::uuid(&second).unwrap() else {
            unreachable!()
        };
        assert_eq!(
            terminals,
            vec![*first_uuid.as_bytes(), *second_uuid.as_bytes()]
        );

        assert_eq!(
            parse_terminal_uuids(&["not-a-uuid".to_owned()])
                .unwrap_err()
                .status,
            "ValidationError"
        );
        assert_eq!(
            parse_terminal_uuids(&[first.to_uppercase()])
                .unwrap_err()
                .status,
            "ValidationError"
        );
    }

    #[test]
    fn source_free_minimum_steiner_reaches_active_rust_handler() {
        let graph = GraphForge::new(None, None).unwrap();
        let first = graph.add_node("Person".into(), None).unwrap();
        let second = graph.add_node("Person".into(), None).unwrap();

        let result = graph.paths(
            None,
            None,
            "min_steiner_tree".into(),
            None,
            Some(false),
            Some(1),
            None,
            None,
            None,
            None,
            Some(vec![first.uuid(), second.uuid()]),
            None,
            None,
            None,
        );
        let Err(error) = result else {
            panic!("disconnected minimum Steiner input must fail")
        };

        assert_eq!(error.status, "ExecutionError");
        assert!(
            error.reason.contains(
                "minimum Steiner tree is undefined: mandatory terminals are disconnected"
            )
        );
    }

    #[test]
    fn capability_tasks_return_rust_owned_arrow_ipc() {
        let graph = GraphForge::new(None, None).unwrap();
        let mut inspect = ProjectCapabilitiesTask {
            engine: Arc::clone(&graph.inner),
        };
        let initial = inspect.compute().unwrap().unwrap();
        let initial = ipc_to_record_batch(&Buffer::from(initial)).unwrap();
        assert_eq!(initial.num_rows(), 2);

        let mut enable = EnableCapabilityTask {
            engine: Arc::clone(&graph.inner),
            request: gf_api::EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: canonical_operation_id("018f0f4e-7b8c-7000-8000-000000000001")
                        .unwrap(),
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Knowledge,
                capability_version: 1,
            },
        };
        let enabled = enable.compute().unwrap().unwrap();
        let enabled = ipc_to_record_batch(&Buffer::from(enabled)).unwrap();
        assert_eq!(enabled.num_rows(), 3);
        assert_eq!(
            enabled
                .column_by_name("capability_id")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(1),
            "knowledge"
        );
    }
}
