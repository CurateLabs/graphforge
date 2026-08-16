//! Thin Node wrappers for Rust-owned transactions and maintenance (#755).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use graphforge_api::{
    CancellationToken, GfError, GraphDeltaCompactionLimits, GraphDeltaCompactionPolicy,
    GraphDeltaCompactionReport, GraphDeltaCompactionRequest, GraphDeltaCompactionStatus,
    GraphDeltaJournalLimits, GraphTransaction as ApiTransaction, IrLiteral, OperationId,
    ProjectCleanupReport, ProjectOpenRecoveryEvidence, ProjectReachabilityReport,
    ProjectRetentionLimits, ProjectRetentionPolicy, ProjectWriteMode, PropValue, TransactionPhase,
    WriteContext,
};
use napi::bindgen_prelude::BigInt;
use napi_derive::napi;
use uuid::Uuid;

use crate::error::to_napi_err;
use crate::{Result, napi_validation};

fn phase_name(phase: TransactionPhase) -> &'static str {
    match phase {
        TransactionPhase::Open => "open",
        TransactionPhase::Validated => "validated",
        TransactionPhase::Committed => "committed",
        TransactionPhase::RolledBack => "rolled_back",
        _ => "unknown",
    }
}

fn write_mode_name(mode: ProjectWriteMode) -> &'static str {
    match mode {
        ProjectWriteMode::SingleWriter => "single_writer",
        ProjectWriteMode::QueuedWriter => "queued_writer",
        ProjectWriteMode::OptimisticMultiWriter => "optimistic_multi_writer",
        _ => "unknown",
    }
}

fn fingerprint_hex(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|_| {
        to_napi_err(&GfError::Validation(format!(
            "{field} must be a canonical UUID string"
        )))
    })
}

fn canonical_operation_id(value: &str) -> Result<OperationId> {
    crate::canonical_operation_id(value)
}

fn u64_bigint(value: u64) -> BigInt {
    BigInt::from(value)
}

/// Safe transaction status snapshot.
#[napi(object)]
pub struct TransactionStatusOutput {
    pub operation_uuid: String,
    pub base_generation_uuid: String,
    pub phase: String,
    pub write_mode: String,
    pub staged_entry_count: u32,
    pub committed: bool,
}

/// Safe recovery-on-open evidence.
#[napi(object)]
pub struct ProjectOpenRecoveryOutput {
    pub kind: String,
    pub selected_generation_uuid: String,
    pub selected_generation_class: String,
    pub work_detected: bool,
    pub repaired_journals: BigInt,
    pub aborted_journals: BigInt,
    pub removed_generations: BigInt,
    pub preserved_unknown_entries: BigInt,
    pub deferred: Option<String>,
    pub elapsed_ms: BigInt,
}

/// One classified cleanup entry.
#[napi(object)]
pub struct ProjectCleanupEntryOutput {
    pub generation_uuid: Option<String>,
    pub location: String,
    pub disposition: String,
    pub bytes: BigInt,
}

/// Retention/GC report with lossless 64-bit counters.
#[napi(object)]
pub struct ProjectCleanupReportOutput {
    pub dry_run: bool,
    pub selected_generation_uuid: String,
    pub retained_ancestors: u32,
    pub reachable_count: BigInt,
    pub candidates: BigInt,
    pub removed: BigInt,
    pub skipped_live: BigInt,
    pub quarantined: BigInt,
    pub unknown: BigInt,
    pub remaining_bytes: BigInt,
    pub bytes_scanned: BigInt,
    pub entries_scanned: BigInt,
    pub work_units: BigInt,
    pub bounded: bool,
    pub elapsed_ms: BigInt,
    pub entries: Vec<ProjectCleanupEntryOutput>,
}

/// Reachability inspection report.
#[napi(object)]
pub struct ProjectReachabilityReportOutput {
    pub selected_generation_uuid: String,
    pub retained_ancestors_policy: u32,
    pub reachable: Vec<String>,
    pub checkpoint_roots: Vec<String>,
    pub retained_ancestors: Vec<String>,
    pub entries_scanned: BigInt,
    pub bytes_scanned: BigInt,
    pub elapsed_ms: BigInt,
}

/// Delta compaction progress/evidence report.
#[napi(object)]
pub struct GraphDeltaCompactionReportOutput {
    pub dry_run: bool,
    pub input_generation_uuid: String,
    pub output_generation_uuid: Option<String>,
    pub input_runs: BigInt,
    pub compacted_runs: BigInt,
    pub retained_suffix_runs: BigInt,
    pub input_rows: BigInt,
    pub output_rows: BigInt,
    pub input_bytes: BigInt,
    pub output_bytes: BigInt,
    pub spill_bytes: BigInt,
    pub peak_memory_bytes: BigInt,
    pub elapsed_ms: BigInt,
    pub state_fingerprint: String,
    pub cleanup: Option<ProjectCleanupReportOutput>,
}

/// Compaction status under optional policy triggers.
#[napi(object)]
pub struct GraphDeltaCompactionStatusOutput {
    pub generation_uuid: String,
    pub run_count: BigInt,
    pub run_bytes: BigInt,
    pub estimated_replay_memory_bytes: BigInt,
    pub state_fingerprint: String,
    pub should_compact: bool,
    pub trigger_reasons: Vec<String>,
}

/// Retention policy/limit overrides.
#[napi(object)]
pub struct ProjectRetentionInput {
    pub retained_ancestors: Option<u32>,
    pub max_entries: Option<u32>,
    pub max_bytes_scanned: Option<BigInt>,
    pub max_work_units: Option<u32>,
    pub cleanup_batch: Option<u32>,
}

/// Compaction request overrides.
#[napi(object)]
pub struct GraphDeltaCompactionInput {
    pub transaction_uuid: String,
    pub generation_uuid: String,
    pub through_run_sequence: Option<BigInt>,
    pub cleanup_after_commit: Option<bool>,
    pub retained_ancestors: Option<u32>,
}

/// Compaction status policy overrides.
#[napi(object)]
#[allow(clippy::struct_field_names)] // mirrors GraphDeltaCompactionPolicy field names
pub struct GraphDeltaCompactionStatusInput {
    pub compact_when_runs: Option<BigInt>,
    pub compact_when_run_bytes: Option<BigInt>,
    pub compact_when_replay_memory_bytes: Option<BigInt>,
}

fn bigint_u64(value: BigInt, field: &str) -> Result<u64> {
    let (negative, parsed, lossless) = value.get_u64();
    if negative || !lossless {
        return Err(to_napi_err(&GfError::Validation(format!(
            "{field} must be an exact unsigned 64-bit integer"
        ))));
    }
    Ok(parsed)
}

fn retention_from_input(
    input: Option<ProjectRetentionInput>,
) -> Result<(ProjectRetentionPolicy, ProjectRetentionLimits)> {
    let defaults = ProjectRetentionLimits::default();
    let policy_default = ProjectRetentionPolicy::default();
    let Some(input) = input else {
        return Ok((policy_default, defaults));
    };
    Ok((
        ProjectRetentionPolicy {
            retained_ancestors: input
                .retained_ancestors
                .map(usize::try_from)
                .transpose()
                .map_err(|_| napi_validation("retainedAncestors exceeds usize"))?
                .unwrap_or(policy_default.retained_ancestors),
        },
        ProjectRetentionLimits {
            max_entries: input
                .max_entries
                .map(usize::try_from)
                .transpose()
                .map_err(|_| napi_validation("maxEntries exceeds usize"))?
                .unwrap_or(defaults.max_entries),
            max_bytes_scanned: match input.max_bytes_scanned {
                Some(value) => bigint_u64(value, "maxBytesScanned")?,
                None => defaults.max_bytes_scanned,
            },
            max_work_units: input
                .max_work_units
                .map(usize::try_from)
                .transpose()
                .map_err(|_| napi_validation("maxWorkUnits exceeds usize"))?
                .unwrap_or(defaults.max_work_units),
            cleanup_batch: input
                .cleanup_batch
                .map(usize::try_from)
                .transpose()
                .map_err(|_| napi_validation("cleanupBatch exceeds usize"))?
                .unwrap_or(defaults.cleanup_batch),
        },
    ))
}

pub(crate) fn recovery_output(evidence: &ProjectOpenRecoveryEvidence) -> ProjectOpenRecoveryOutput {
    ProjectOpenRecoveryOutput {
        kind: match evidence.kind {
            graphforge_api::ProjectOpenRecoveryKind::ProjectOpen => "project_open".into(),
            graphforge_api::ProjectOpenRecoveryKind::Initialization => "initialization".into(),
            graphforge_api::ProjectOpenRecoveryKind::CheckpointView => "checkpoint_view".into(),
        },
        selected_generation_uuid: evidence.selected_generation_uuid.to_string(),
        selected_generation_class: evidence.selected_generation_class.as_str().into(),
        work_detected: evidence.work_detected,
        repaired_journals: u64_bigint(evidence.repaired_journals),
        aborted_journals: u64_bigint(evidence.aborted_journals),
        removed_generations: u64_bigint(evidence.removed_generations),
        preserved_unknown_entries: u64_bigint(evidence.preserved_unknown_entries),
        deferred: evidence.deferred.map(|value| value.as_str().into()),
        elapsed_ms: u64_bigint(evidence.elapsed_ms),
    }
}

fn cleanup_output(report: &ProjectCleanupReport) -> ProjectCleanupReportOutput {
    ProjectCleanupReportOutput {
        dry_run: report.dry_run,
        selected_generation_uuid: report.selected_generation_uuid.to_string(),
        retained_ancestors: u32::try_from(report.policy.retained_ancestors).unwrap_or(u32::MAX),
        reachable_count: u64_bigint(report.reachable_count),
        candidates: u64_bigint(report.candidates),
        removed: u64_bigint(report.removed),
        skipped_live: u64_bigint(report.skipped_live),
        quarantined: u64_bigint(report.quarantined),
        unknown: u64_bigint(report.unknown),
        remaining_bytes: u64_bigint(report.remaining_bytes),
        bytes_scanned: u64_bigint(report.bytes_scanned),
        entries_scanned: u64_bigint(report.entries_scanned),
        work_units: u64_bigint(report.work_units),
        bounded: report.bounded,
        elapsed_ms: u64_bigint(report.elapsed_ms),
        entries: report
            .entries
            .iter()
            .map(|entry| ProjectCleanupEntryOutput {
                generation_uuid: entry.generation_uuid.map(|uuid| uuid.to_string()),
                location: entry.location.as_str().into(),
                disposition: entry.disposition.as_str().into(),
                bytes: u64_bigint(entry.bytes),
            })
            .collect(),
    }
}

fn reachability_output(report: &ProjectReachabilityReport) -> ProjectReachabilityReportOutput {
    ProjectReachabilityReportOutput {
        selected_generation_uuid: report.selected_generation_uuid.to_string(),
        retained_ancestors_policy: u32::try_from(report.policy.retained_ancestors)
            .unwrap_or(u32::MAX),
        reachable: report.reachable.iter().map(ToString::to_string).collect(),
        checkpoint_roots: report
            .checkpoint_roots
            .iter()
            .map(ToString::to_string)
            .collect(),
        retained_ancestors: report
            .retained_ancestors
            .iter()
            .map(ToString::to_string)
            .collect(),
        entries_scanned: u64_bigint(report.entries_scanned),
        bytes_scanned: u64_bigint(report.bytes_scanned),
        elapsed_ms: u64_bigint(report.elapsed_ms),
    }
}

fn compaction_report_output(
    report: &GraphDeltaCompactionReport,
) -> GraphDeltaCompactionReportOutput {
    GraphDeltaCompactionReportOutput {
        dry_run: report.dry_run,
        input_generation_uuid: report.input_generation_uuid.to_string(),
        output_generation_uuid: report.output_generation_uuid.map(|uuid| uuid.to_string()),
        input_runs: u64_bigint(report.input_runs),
        compacted_runs: u64_bigint(report.compacted_runs),
        retained_suffix_runs: u64_bigint(report.retained_suffix_runs),
        input_rows: u64_bigint(report.input_rows),
        output_rows: u64_bigint(report.output_rows),
        input_bytes: u64_bigint(report.input_bytes),
        output_bytes: u64_bigint(report.output_bytes),
        spill_bytes: u64_bigint(report.spill_bytes),
        peak_memory_bytes: u64_bigint(report.peak_memory_bytes),
        elapsed_ms: u64_bigint(report.elapsed_ms),
        state_fingerprint: fingerprint_hex(&report.state_fingerprint),
        cleanup: report.cleanup.as_ref().map(cleanup_output),
    }
}

fn compaction_status_output(
    status: &GraphDeltaCompactionStatus,
) -> GraphDeltaCompactionStatusOutput {
    GraphDeltaCompactionStatusOutput {
        generation_uuid: status.generation_uuid.to_string(),
        run_count: u64_bigint(status.run_count),
        run_bytes: u64_bigint(status.run_bytes),
        estimated_replay_memory_bytes: u64_bigint(status.estimated_replay_memory_bytes),
        state_fingerprint: fingerprint_hex(&status.state_fingerprint),
        should_compact: status.should_compact,
        trigger_reasons: status.trigger_reasons.clone(),
    }
}

fn compaction_request(input: GraphDeltaCompactionInput) -> Result<GraphDeltaCompactionRequest> {
    Ok(GraphDeltaCompactionRequest {
        transaction_uuid: parse_uuid(&input.transaction_uuid, "transactionUuid")?,
        generation_uuid: parse_uuid(&input.generation_uuid, "generationUuid")?,
        through_run_sequence: input
            .through_run_sequence
            .map(|value| bigint_u64(value, "throughRunSequence"))
            .transpose()?,
        limits: GraphDeltaCompactionLimits::default(),
        cleanup_after_commit: input.cleanup_after_commit.unwrap_or(false),
        cleanup_policy: ProjectRetentionPolicy {
            retained_ancestors: input
                .retained_ancestors
                .map(usize::try_from)
                .transpose()
                .map_err(|_| napi_validation("retainedAncestors exceeds usize"))?
                .unwrap_or_else(|| ProjectRetentionPolicy::default().retained_ancestors),
        },
        cleanup_limits: ProjectRetentionLimits::default(),
    })
}

/// Explicit multi-mutation transaction handle. Finalization rolls back without commit.
#[napi]
pub struct GraphTransaction {
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    inner: Mutex<Option<ApiTransaction>>,
}

#[napi]
impl GraphTransaction {
    pub(crate) fn new(
        engine: Arc<RwLock<graphforge_api::GraphForge>>,
        inner: ApiTransaction,
    ) -> Self {
        Self {
            engine,
            inner: Mutex::new(Some(inner)),
        }
    }

    fn with_tx<R>(
        &self,
        f: impl FnOnce(&ApiTransaction) -> std::result::Result<R, GfError>,
    ) -> Result<R> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| to_napi_err(&GfError::Validation("transaction lock poisoned".into())))?;
        let tx = guard.as_ref().ok_or_else(|| {
            to_napi_err(&GfError::Validation(
                "transaction handle already released".into(),
            ))
        })?;
        f(tx).map_err(|error| to_napi_err(&error))
    }

    #[napi]
    pub fn status(&self) -> Result<TransactionStatusOutput> {
        let status = self.with_tx(ApiTransaction::status)?;
        Ok(TransactionStatusOutput {
            operation_uuid: status.operation_uuid.to_string(),
            base_generation_uuid: status.base_generation_uuid.to_string(),
            phase: phase_name(status.phase).into(),
            write_mode: write_mode_name(status.write_mode).into(),
            staged_entry_count: u32::try_from(status.staged_entry_count).unwrap_or(u32::MAX),
            committed: status.committed,
        })
    }

    #[napi]
    pub fn stage_cypher(
        &self,
        query: String,
        params: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<()> {
        let mut converted = HashMap::new();
        if let Some(params) = params {
            for (key, value) in params {
                converted.insert(key, json_to_ir_literal(value)?);
            }
        }
        self.with_tx(|tx| tx.stage_cypher(query, converted))
    }

    #[napi]
    pub fn stage_add_node(
        &self,
        node_uuid: String,
        label: String,
        properties: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<()> {
        let node_uuid = parse_uuid(&node_uuid, "nodeUuid")?;
        let properties = json_props(properties)?;
        self.with_tx(|tx| tx.stage_add_node(node_uuid, label, properties))
    }

    #[napi]
    pub fn stage_add_edge(
        &self,
        edge_uuid: String,
        rel_type: String,
        source_uuid: String,
        target_uuid: String,
        properties: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<()> {
        let edge_uuid = parse_uuid(&edge_uuid, "edgeUuid")?;
        let source_uuid = parse_uuid(&source_uuid, "sourceUuid")?;
        let target_uuid = parse_uuid(&target_uuid, "targetUuid")?;
        let properties = json_props(properties)?;
        self.with_tx(|tx| {
            tx.stage_add_edge(edge_uuid, rel_type, source_uuid, target_uuid, properties)
        })
    }

    #[napi]
    pub fn validate(&self) -> Result<()> {
        let graph = self
            .engine
            .read()
            .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))?;
        self.with_tx(|tx| tx.validate(&graph))
    }

    #[napi]
    pub fn commit(&self) -> Result<String> {
        let graph = self
            .engine
            .write()
            .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))?;
        let receipt = self.with_tx(|tx| tx.commit(&graph))?;
        Ok(receipt.generation_uuid.to_string())
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        self.with_tx(ApiTransaction::rollback)
    }
}

impl Drop for GraphTransaction {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.take();
        }
    }
}

fn json_to_ir_literal(value: serde_json::Value) -> Result<IrLiteral> {
    match value {
        serde_json::Value::Null => Ok(IrLiteral::Null),
        serde_json::Value::Bool(value) => Ok(IrLiteral::Bool(value)),
        serde_json::Value::Number(value) => {
            if let Some(int) = value.as_i64() {
                Ok(IrLiteral::Int(int))
            } else if let Some(float) = value.as_f64() {
                Ok(IrLiteral::Float(float))
            } else {
                Err(napi_validation("unsupported numeric Cypher parameter"))
            }
        }
        serde_json::Value::String(value) => Ok(IrLiteral::Str(value)),
        serde_json::Value::Array(values) => Ok(IrLiteral::List(
            values
                .into_iter()
                .map(json_to_ir_literal)
                .collect::<Result<Vec<_>>>()?,
        )),
        serde_json::Value::Object(map) => Ok(IrLiteral::Map(
            map.into_iter()
                .map(|(key, value)| Ok((key, json_to_ir_literal(value)?)))
                .collect::<Result<Vec<_>>>()?,
        )),
    }
}

fn json_to_prop_value(value: serde_json::Value) -> Result<PropValue> {
    match value {
        serde_json::Value::Null => Ok(PropValue::Null),
        serde_json::Value::Bool(value) => Ok(PropValue::Bool(value)),
        serde_json::Value::Number(value) => {
            if let Some(int) = value.as_i64() {
                Ok(PropValue::Int(int))
            } else if let Some(float) = value.as_f64() {
                Ok(PropValue::Float(float))
            } else {
                Err(napi_validation("unsupported numeric property value"))
            }
        }
        serde_json::Value::String(value) => Ok(PropValue::Str(value)),
        _ => Err(napi_validation(
            "transaction property values must be null/bool/number/string",
        )),
    }
}

fn json_props(
    properties: Option<HashMap<String, serde_json::Value>>,
) -> Result<HashMap<String, PropValue>> {
    let mut out = HashMap::new();
    if let Some(properties) = properties {
        for (key, value) in properties {
            out.insert(key, json_to_prop_value(value)?);
        }
    }
    Ok(out)
}

pub(crate) fn begin_transaction(
    engine: Arc<RwLock<graphforge_api::GraphForge>>,
    operation_uuid: String,
    actor_uuid: Option<String>,
) -> Result<GraphTransaction> {
    let operation_uuid = canonical_operation_id(&operation_uuid)?;
    let actor_uuid = actor_uuid
        .as_deref()
        .map(canonical_operation_id)
        .transpose()?
        .map(|id| id.0);
    let context = WriteContext {
        operation_uuid,
        actor_uuid,
    };
    let graph = engine
        .read()
        .map_err(|_| to_napi_err(&GfError::Execution("GraphForge lock poisoned".into())))?;
    let inner = graph
        .begin_transaction(context)
        .map_err(|error| to_napi_err(&error))?;
    drop(graph);
    Ok(GraphTransaction::new(engine, inner))
}

pub(crate) fn inspect_reachability(
    graph: &graphforge_api::GraphForge,
    input: Option<ProjectRetentionInput>,
) -> Result<ProjectReachabilityReportOutput> {
    let (policy, limits) = retention_from_input(input)?;
    let report = graph
        .inspect_project_reachability(policy, limits)
        .map_err(|error| to_napi_err(&error))?;
    Ok(reachability_output(&report))
}

pub(crate) fn preview_cleanup(
    graph: &graphforge_api::GraphForge,
    input: Option<ProjectRetentionInput>,
) -> Result<ProjectCleanupReportOutput> {
    let (policy, limits) = retention_from_input(input)?;
    let report = graph
        .preview_project_cleanup(policy, limits)
        .map_err(|error| to_napi_err(&error))?;
    Ok(cleanup_output(&report))
}

pub(crate) fn execute_cleanup(
    graph: &graphforge_api::GraphForge,
    input: Option<ProjectRetentionInput>,
) -> Result<ProjectCleanupReportOutput> {
    let (policy, limits) = retention_from_input(input)?;
    let report = graph
        .execute_project_cleanup(policy, limits)
        .map_err(|error| to_napi_err(&error))?;
    Ok(cleanup_output(&report))
}

pub(crate) fn compaction_status(
    graph: &graphforge_api::GraphForge,
    input: Option<GraphDeltaCompactionStatusInput>,
) -> Result<GraphDeltaCompactionStatusOutput> {
    let policy = match input {
        Some(input) => GraphDeltaCompactionPolicy {
            compact_when_runs: input
                .compact_when_runs
                .map(|value| bigint_u64(value, "compactWhenRuns"))
                .transpose()?,
            compact_when_run_bytes: input
                .compact_when_run_bytes
                .map(|value| bigint_u64(value, "compactWhenRunBytes"))
                .transpose()?,
            compact_when_replay_memory_bytes: input
                .compact_when_replay_memory_bytes
                .map(|value| bigint_u64(value, "compactWhenReplayMemoryBytes"))
                .transpose()?,
        },
        None => GraphDeltaCompactionPolicy::default(),
    };
    let status = graph
        .graph_delta_compaction_status(policy, GraphDeltaJournalLimits::default())
        .map_err(|error| to_napi_err(&error))?;
    Ok(compaction_status_output(&status))
}

pub(crate) fn preview_compaction(
    graph: &graphforge_api::GraphForge,
    input: GraphDeltaCompactionInput,
    cancellation: Option<&CancellationToken>,
) -> Result<GraphDeltaCompactionReportOutput> {
    let request = compaction_request(input)?;
    let report = graph
        .preview_graph_delta_compaction(&request, cancellation)
        .map_err(|error| to_napi_err(&error))?;
    Ok(compaction_report_output(&report))
}

pub(crate) fn compact(
    graph: &graphforge_api::GraphForge,
    input: GraphDeltaCompactionInput,
    cancellation: Option<&CancellationToken>,
) -> Result<GraphDeltaCompactionReportOutput> {
    let request = compaction_request(input)?;
    let report = graph
        .compact_graph_delta(&request, cancellation)
        .map_err(|error| to_napi_err(&error))?;
    Ok(compaction_report_output(&report))
}
