//! CLI surfaces for transactions, recovery evidence, and maintenance (#755).

use std::collections::HashMap;
use std::io::Write;

use clap::{Args, Subcommand};
use graphforge_api::{
    GraphDeltaCompactionLimits, GraphDeltaCompactionPolicy, GraphDeltaCompactionRequest,
    GraphDeltaJournalLimits, GraphForge, OperationId, ProjectRetentionLimits,
    ProjectRetentionPolicy, PropValue, WriteContext,
};
use uuid::Uuid;

use crate::canonical_uuid;

fn write_json_value(
    value: &serde_json::Value,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    serde_json::to_writer(&mut *output, value)
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    writeln!(output).map_err(|error| graphforge_api::GfError::Execution(error.to_string()))
}

#[derive(Subcommand)]
pub(crate) enum TransactionCommand {
    /// Stage supported mutations and commit one generation.
    Commit(TransactionArgs),
    /// Stage supported mutations then explicitly roll back.
    Rollback(TransactionArgs),
}

#[derive(Args)]
pub(crate) struct TransactionArgs {
    /// Caller-owned UUIDv7 operation identity.
    #[arg(long)]
    operation_uuid: String,
    /// Optional actor UUID.
    #[arg(long)]
    actor_uuid: Option<String>,
    /// Write Cypher statement to stage (repeatable).
    #[arg(long = "cypher")]
    cypher: Vec<String>,
    /// Stage a node as `uuid:Label` (optional trailing `:{"k":"v"}` JSON props).
    #[arg(long = "add-node")]
    add_node: Vec<String>,
}

#[derive(Subcommand)]
pub(crate) enum MaintenanceCommand {
    /// Inspect verified generation reachability.
    Reachability(RetentionArgs),
    /// Preview retention/GC candidates without deletion.
    CleanupPreview(RetentionArgs),
    /// Execute retention/GC (requires --yes).
    CleanupExecute(CleanupExecuteArgs),
    /// Report whether CURRENT should compact under policy triggers.
    CompactionStatus(CompactionStatusArgs),
    /// Preview delta compaction without publishing CURRENT.
    CompactionPreview(CompactionArgs),
    /// Compact a contiguous verified delta prefix (requires --yes).
    CompactionRun(CompactionRunArgs),
}

#[derive(Args)]
pub(crate) struct RetentionArgs {
    #[arg(long)]
    retained_ancestors: Option<usize>,
    #[arg(long)]
    max_entries: Option<usize>,
    #[arg(long)]
    max_bytes_scanned: Option<u64>,
    #[arg(long)]
    max_work_units: Option<usize>,
    #[arg(long)]
    cleanup_batch: Option<usize>,
}

#[derive(Args)]
pub(crate) struct CleanupExecuteArgs {
    #[command(flatten)]
    retention: RetentionArgs,
    /// Required explicit confirmation for destructive cleanup.
    #[arg(long)]
    yes: bool,
}

#[derive(Args)]
#[allow(clippy::struct_field_names)] // clap long names mirror GraphDeltaCompactionPolicy fields
pub(crate) struct CompactionStatusArgs {
    #[arg(long)]
    compact_when_runs: Option<u64>,
    #[arg(long)]
    compact_when_run_bytes: Option<u64>,
    #[arg(long)]
    compact_when_replay_memory_bytes: Option<u64>,
}

#[derive(Args)]
pub(crate) struct CompactionArgs {
    #[arg(long)]
    transaction_uuid: String,
    #[arg(long)]
    generation_uuid: String,
    #[arg(long)]
    through_run_sequence: Option<u64>,
    #[arg(long, default_value_t = false)]
    cleanup_after_commit: bool,
    #[arg(long)]
    retained_ancestors: Option<usize>,
}

#[derive(Args)]
pub(crate) struct CompactionRunArgs {
    #[command(flatten)]
    compaction: CompactionArgs,
    /// Required explicit confirmation for CURRENT publication.
    #[arg(long)]
    yes: bool,
}

fn retention_policy(args: &RetentionArgs) -> ProjectRetentionPolicy {
    ProjectRetentionPolicy {
        retained_ancestors: args
            .retained_ancestors
            .unwrap_or_else(|| ProjectRetentionPolicy::default().retained_ancestors),
    }
}

fn retention_limits(args: &RetentionArgs) -> ProjectRetentionLimits {
    let defaults = ProjectRetentionLimits::default();
    ProjectRetentionLimits {
        max_entries: args.max_entries.unwrap_or(defaults.max_entries),
        max_bytes_scanned: args.max_bytes_scanned.unwrap_or(defaults.max_bytes_scanned),
        max_work_units: args.max_work_units.unwrap_or(defaults.max_work_units),
        cleanup_batch: args.cleanup_batch.unwrap_or(defaults.cleanup_batch),
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

fn parse_add_node(
    spec: &str,
) -> Result<(Uuid, String, HashMap<String, PropValue>), graphforge_api::GfError> {
    let mut parts = spec.splitn(3, ':');
    let uuid = parts.next().ok_or_else(|| {
        graphforge_api::GfError::Validation("--add-node requires uuid:Label".into())
    })?;
    let label = parts.next().ok_or_else(|| {
        graphforge_api::GfError::Validation("--add-node requires uuid:Label".into())
    })?;
    let props = match parts.next() {
        Some(raw) if !raw.is_empty() => {
            let value: serde_json::Value = serde_json::from_str(raw).map_err(|error| {
                graphforge_api::GfError::Validation(format!("invalid --add-node JSON: {error}"))
            })?;
            let serde_json::Value::Object(map) = value else {
                return Err(graphforge_api::GfError::Validation(
                    "--add-node properties must be a JSON object".into(),
                ));
            };
            let mut out = HashMap::new();
            for (key, item) in map {
                out.insert(
                    key,
                    match item {
                        serde_json::Value::Null => PropValue::Null,
                        serde_json::Value::Bool(v) => PropValue::Bool(v),
                        serde_json::Value::Number(v) if v.as_i64().is_some() => {
                            PropValue::Int(v.as_i64().unwrap())
                        }
                        serde_json::Value::Number(v) if v.as_f64().is_some() => {
                            PropValue::Float(v.as_f64().unwrap())
                        }
                        serde_json::Value::String(v) => PropValue::Str(v),
                        _ => {
                            return Err(graphforge_api::GfError::Validation(
                                "--add-node property values must be null/bool/number/string".into(),
                            ));
                        }
                    },
                );
            }
            out
        }
        _ => HashMap::new(),
    };
    Ok((canonical_uuid(uuid)?, label.to_owned(), props))
}

fn cleanup_json(report: &graphforge_api::ProjectCleanupReport) -> serde_json::Value {
    serde_json::json!({
        "dry_run": report.dry_run,
        "selected_generation_uuid": report.selected_generation_uuid.to_string(),
        "retained_ancestors": report.policy.retained_ancestors,
        "reachable_count": report.reachable_count,
        "candidates": report.candidates,
        "removed": report.removed,
        "skipped_live": report.skipped_live,
        "quarantined": report.quarantined,
        "unknown": report.unknown,
        "remaining_bytes": report.remaining_bytes,
        "bytes_scanned": report.bytes_scanned,
        "entries_scanned": report.entries_scanned,
        "work_units": report.work_units,
        "bounded": report.bounded,
        "elapsed_ms": report.elapsed_ms,
        "entries": report.entries.iter().map(|entry| serde_json::json!({
            "generation_uuid": entry.generation_uuid.map(|uuid| uuid.to_string()),
            "location": entry.location.as_str(),
            "disposition": entry.disposition.as_str(),
            "bytes": entry.bytes,
        })).collect::<Vec<_>>(),
    })
}

fn reachability_json(report: &graphforge_api::ProjectReachabilityReport) -> serde_json::Value {
    serde_json::json!({
        "selected_generation_uuid": report.selected_generation_uuid.to_string(),
        "retained_ancestors_policy": report.policy.retained_ancestors,
        "reachable": report.reachable.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "checkpoint_roots": report.checkpoint_roots.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "retained_ancestors": report.retained_ancestors.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "entries_scanned": report.entries_scanned,
        "bytes_scanned": report.bytes_scanned,
        "elapsed_ms": report.elapsed_ms,
    })
}

fn compaction_report_json(
    report: &graphforge_api::GraphDeltaCompactionReport,
) -> serde_json::Value {
    serde_json::json!({
        "dry_run": report.dry_run,
        "input_generation_uuid": report.input_generation_uuid.to_string(),
        "output_generation_uuid": report.output_generation_uuid.map(|uuid| uuid.to_string()),
        "input_runs": report.input_runs,
        "compacted_runs": report.compacted_runs,
        "retained_suffix_runs": report.retained_suffix_runs,
        "input_rows": report.input_rows,
        "output_rows": report.output_rows,
        "input_bytes": report.input_bytes,
        "output_bytes": report.output_bytes,
        "spill_bytes": report.spill_bytes,
        "peak_memory_bytes": report.peak_memory_bytes,
        "elapsed_ms": report.elapsed_ms,
        "state_fingerprint": fingerprint_hex(&report.state_fingerprint),
        "cleanup": report.cleanup.as_ref().map(cleanup_json),
    })
}

fn compaction_request(
    args: &CompactionArgs,
) -> Result<GraphDeltaCompactionRequest, graphforge_api::GfError> {
    Ok(GraphDeltaCompactionRequest {
        transaction_uuid: canonical_uuid(&args.transaction_uuid)?,
        generation_uuid: canonical_uuid(&args.generation_uuid)?,
        through_run_sequence: args.through_run_sequence,
        limits: GraphDeltaCompactionLimits::default(),
        cleanup_after_commit: args.cleanup_after_commit,
        cleanup_policy: ProjectRetentionPolicy {
            retained_ancestors: args
                .retained_ancestors
                .unwrap_or_else(|| ProjectRetentionPolicy::default().retained_ancestors),
        },
        cleanup_limits: ProjectRetentionLimits::default(),
    })
}

pub(crate) fn run_recovery(
    graph: &GraphForge,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let evidence = graph.project_open_recovery();
    let value = serde_json::json!({
        "kind": match evidence.kind {
            graphforge_api::ProjectOpenRecoveryKind::ProjectOpen => "project_open",
            graphforge_api::ProjectOpenRecoveryKind::Initialization => "initialization",
            graphforge_api::ProjectOpenRecoveryKind::CheckpointView => "checkpoint_view",
        },
        "selected_generation_uuid": evidence.selected_generation_uuid.to_string(),
        "selected_generation_class": evidence.selected_generation_class.as_str(),
        "work_detected": evidence.work_detected,
        "repaired_journals": evidence.repaired_journals,
        "aborted_journals": evidence.aborted_journals,
        "removed_generations": evidence.removed_generations,
        "preserved_unknown_entries": evidence.preserved_unknown_entries,
        "deferred": evidence.deferred.map(graphforge_api::ProjectRecoveryDeferral::as_str),
        "elapsed_ms": evidence.elapsed_ms,
    });
    write_machine(&value, json, output)
}

pub(crate) fn run_transaction(
    graph: &GraphForge,
    command: TransactionCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let (args, commit) = match command {
        TransactionCommand::Commit(args) => (args, true),
        TransactionCommand::Rollback(args) => (args, false),
    };
    if args.cypher.is_empty() && args.add_node.is_empty() {
        return Err(graphforge_api::GfError::Validation(
            "transaction requires at least one --cypher or --add-node".into(),
        ));
    }
    let context = WriteContext {
        operation_uuid: OperationId(canonical_uuid(&args.operation_uuid)?),
        actor_uuid: args.actor_uuid.as_deref().map(canonical_uuid).transpose()?,
    };
    let tx = graph.begin_transaction(context)?;
    for query in &args.cypher {
        tx.stage_cypher(query.clone(), HashMap::new())?;
    }
    for spec in &args.add_node {
        let (node_uuid, label, properties) = parse_add_node(spec)?;
        tx.stage_add_node(node_uuid, label, properties)?;
    }
    let value = if commit {
        let receipt = tx.commit(graph)?;
        serde_json::json!({
            "phase": "committed",
            "generation_uuid": receipt.generation_uuid.to_string(),
            "operation_uuid": args.operation_uuid,
        })
    } else {
        tx.rollback()?;
        serde_json::json!({
            "phase": "rolled_back",
            "operation_uuid": args.operation_uuid,
        })
    };
    write_machine(&value, json, output)
}

pub(crate) fn run_maintenance(
    graph: &GraphForge,
    command: MaintenanceCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let value = match command {
        MaintenanceCommand::Reachability(args) => {
            let report = graph
                .inspect_project_reachability(retention_policy(&args), retention_limits(&args))?;
            reachability_json(&report)
        }
        MaintenanceCommand::CleanupPreview(args) => {
            let report =
                graph.preview_project_cleanup(retention_policy(&args), retention_limits(&args))?;
            cleanup_json(&report)
        }
        MaintenanceCommand::CleanupExecute(args) => {
            if !args.yes {
                return Err(graphforge_api::GfError::Validation(
                    "cleanup execute requires --yes".into(),
                ));
            }
            let report = graph.execute_project_cleanup(
                retention_policy(&args.retention),
                retention_limits(&args.retention),
            )?;
            cleanup_json(&report)
        }
        MaintenanceCommand::CompactionStatus(args) => {
            let status = graph.graph_delta_compaction_status(
                GraphDeltaCompactionPolicy {
                    compact_when_runs: args.compact_when_runs,
                    compact_when_run_bytes: args.compact_when_run_bytes,
                    compact_when_replay_memory_bytes: args.compact_when_replay_memory_bytes,
                },
                GraphDeltaJournalLimits::default(),
            )?;
            serde_json::json!({
                "generation_uuid": status.generation_uuid.to_string(),
                "run_count": status.run_count,
                "run_bytes": status.run_bytes,
                "estimated_replay_memory_bytes": status.estimated_replay_memory_bytes,
                "state_fingerprint": fingerprint_hex(&status.state_fingerprint),
                "should_compact": status.should_compact,
                "trigger_reasons": status.trigger_reasons,
            })
        }
        MaintenanceCommand::CompactionPreview(args) => {
            let report = graph.preview_graph_delta_compaction(&compaction_request(&args)?, None)?;
            compaction_report_json(&report)
        }
        MaintenanceCommand::CompactionRun(args) => {
            if !args.yes {
                return Err(graphforge_api::GfError::Validation(
                    "compaction run requires --yes".into(),
                ));
            }
            let report = graph.compact_graph_delta(&compaction_request(&args.compaction)?, None)?;
            compaction_report_json(&report)
        }
    };
    write_machine(&value, json, output)
}

fn write_machine(
    value: &serde_json::Value,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if json {
        write_json_value(value, output)
    } else {
        writeln!(output, "{value}").map_err(|error| {
            graphforge_api::GfError::Storage(format!("write maintenance output: {error}"))
        })?;
        Ok(())
    }
}
