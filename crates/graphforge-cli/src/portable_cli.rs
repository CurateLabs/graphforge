//! CLI surfaces for portable-v2, OCI promotion, streaming query sinks, and staged ingest (#744).

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use graphforge_api::{
    BulkInputKind, GraphForge, ImportSessionLimits, OperationId, PortableSelection,
    PortableV2Error, PortableV2ExportRequest, PortableV2ImportRequest, PortableV2Limits,
    PortableV2Mode, PortableV2OciAuthenticityPolicy, PortableV2OciPublishFacadeRequest,
    PortableV2OciPullFacadeRequest, PortableV2Output, PortableV2SelectionPreviewRequest,
    PortableV2SelectionProfile, PortableV2SelectionRequest, PortableVerifyRequest,
    ResultSinkOptions, publish_portable_v2_oci, pull_portable_v2_oci, verify_portable_v2,
};
use serde_json::Value;
use uuid::Uuid;

use crate::canonical_uuid;

fn map_portable(error: PortableV2Error) -> crate::CliRuntimeError {
    graphforge_api::MultiOntologyError::from(error).into()
}

fn write_json(
    value: &impl serde::Serialize,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    serde_json::to_writer(&mut *output, value)
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    writeln!(output).map_err(|error| graphforge_api::GfError::Execution(error.to_string()))
}

fn selection_flag(
    current: bool,
    checkpoint: Option<String>,
) -> Result<PortableSelection, graphforge_api::GfError> {
    match (current, checkpoint) {
        (true, None) => Ok(PortableSelection::Current),
        (false, Some(name)) => Ok(PortableSelection::Checkpoint(name)),
        _ => Err(graphforge_api::GfError::Validation(
            "exactly one of --current or --checkpoint is required".into(),
        )),
    }
}

#[derive(Subcommand)]
pub(crate) enum PortableCommand {
    /// Preview a content-free portable-v2 component selection.
    Preview(PortablePreviewArgs),
    /// Export an expanded or bundled portable-v2 package.
    Export(PortableV2ExportArgs),
    /// Inspect or fully verify a portable-v2 package.
    Verify(PortableVerifyArgs),
    /// Import a complete portable-v2 package into a new/empty project.
    Import(PortableV2ImportArgs),
    /// Publish a verified package through an OCI Distribution registry.
    PublishOci(PortablePublishOciArgs),
    /// Pull and verify a digest-pinned package from an OCI registry.
    PullOci(PortablePullOciArgs),
    /// Inspect or explicitly adopt durable non-authoritative ontology staging.
    Staging {
        #[command(subcommand)]
        command: PortableStagingCommand,
    },
}

#[derive(Subcommand)]
pub(crate) enum PortableStagingCommand {
    /// Inspect path-free semantic staging identity.
    Inspect,
    /// Explicitly adopt staged authority with exact optimistic identities.
    Adopt(crate::ontology_cli::AuthorityArgs),
}

#[derive(Clone, Copy, ValueEnum)]
enum PortableProfile {
    Complete,
    OntologyOnly,
    DataComponents,
    Artifacts,
    Settings,
}

impl From<PortableProfile> for PortableV2SelectionProfile {
    fn from(value: PortableProfile) -> Self {
        match value {
            PortableProfile::Complete => Self::Complete,
            PortableProfile::OntologyOnly => Self::OntologyOnly,
            PortableProfile::DataComponents => Self::DataComponents,
            PortableProfile::Artifacts => Self::Artifacts,
            PortableProfile::Settings => Self::Settings,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum PortableFormat {
    Expanded,
    Bundle,
}

impl From<PortableFormat> for PortableV2Output {
    fn from(value: PortableFormat) -> Self {
        match value {
            PortableFormat::Expanded => Self::Expanded,
            PortableFormat::Bundle => Self::Bundle,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum VerifyMode {
    Inspect,
    Full,
}

impl From<VerifyMode> for PortableV2Mode {
    fn from(value: VerifyMode) -> Self {
        match value {
            VerifyMode::Inspect => Self::StructureOnly,
            VerifyMode::Full => Self::Full,
        }
    }
}

#[derive(Args)]
pub(crate) struct PortablePreviewArgs {
    #[arg(
        long,
        required_unless_present = "checkpoint",
        conflicts_with = "checkpoint"
    )]
    current: bool,
    #[arg(long, required_unless_present = "current", conflicts_with = "current")]
    checkpoint: Option<String>,
    #[arg(long, value_enum, default_value_t = PortableProfile::Complete)]
    profile: PortableProfile,
    #[arg(long)]
    strict: bool,
}

#[derive(Args)]
pub(crate) struct PortableV2ExportArgs {
    #[arg(
        long,
        required_unless_present = "checkpoint",
        conflicts_with = "checkpoint"
    )]
    current: bool,
    #[arg(long, required_unless_present = "current", conflicts_with = "current")]
    checkpoint: Option<String>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, value_enum, default_value_t = PortableFormat::Bundle)]
    format: PortableFormat,
    #[arg(long, value_enum, default_value_t = PortableProfile::Complete)]
    profile: PortableProfile,
}

#[derive(Args)]
pub(crate) struct PortableVerifyArgs {
    #[arg(long)]
    input: PathBuf,
    #[arg(long, value_enum, default_value_t = VerifyMode::Full)]
    mode: VerifyMode,
}

#[derive(Args)]
pub(crate) struct PortableV2ImportArgs {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    idempotency_key: String,
}

#[derive(Args)]
pub(crate) struct PortablePublishOciArgs {
    #[arg(long)]
    package: PathBuf,
    #[arg(long)]
    registry: String,
    #[arg(long)]
    repository: String,
    #[arg(long)]
    tag: Option<String>,
    #[arg(long)]
    insecure_http: bool,
}

#[derive(Args)]
pub(crate) struct PortablePullOciArgs {
    #[arg(long)]
    registry: String,
    #[arg(long)]
    repository: String,
    #[arg(long)]
    reference: String,
    #[arg(long)]
    expected_digest: Option<String>,
    #[arg(long)]
    destination: PathBuf,
    #[arg(long)]
    insecure_http: bool,
}

/// Environment variable carrying the OCI registry credential.
const OCI_CREDENTIAL_ENV: &str = "GRAPHFORGE_OCI_CREDENTIAL";

/// Map a raw environment value to a credential, treating an empty or
/// whitespace-only value as no credential at all.
///
/// A variable that is exported but unset yields `Some("")`, which would
/// otherwise travel to the registry as a malformed empty bearer token and turn
/// an anonymous pull into an authentication failure.
fn oci_credential(raw: Option<String>) -> Option<String> {
    raw.filter(|value| !value.trim().is_empty())
}

#[allow(
    clippy::too_many_lines,
    reason = "CLI dispatch keeps preview/export in one portable command table"
)]
pub(crate) fn run_portable(
    graph: &mut GraphForge,
    project_root: &std::path::Path,
    command: PortableCommand,
    json: bool,
    output: &mut dyn Write,
    allocation: Option<&graphforge_api::StorageAllocationDiagnostics>,
) -> Result<(), crate::CliRuntimeError> {
    match command {
        PortableCommand::Preview(args) => {
            let plan = graph
                .preview_portable_v2_selection(&PortableV2SelectionPreviewRequest {
                    selection: selection_flag(args.current, args.checkpoint)?,
                    request: PortableV2SelectionRequest {
                        profile: args.profile.into(),
                        strict: args.strict,
                    },
                    limits: PortableV2Limits::default(),
                })
                .map_err(map_portable)?;
            if json {
                write_json(&plan, output)?;
            } else {
                writeln!(
                    output,
                    "selection class={} fingerprint={} estimated_bytes={}",
                    plan.package_class, plan.selection_fingerprint, plan.estimated_payload_bytes
                )
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
            }
        }
        PortableCommand::Export(args) => {
            let result = graph
                .export_portable_v2(
                    &PortableV2ExportRequest {
                        selection: selection_flag(args.current, args.checkpoint)?,
                        output_path: args.output,
                        representation: args.format.into(),
                        profile: args.profile.into(),
                        subset: None,
                        limits: PortableV2Limits::default(),
                    },
                    None,
                    |progress| {
                        if !json {
                            let _ = writeln!(
                                output,
                                "export progress entries={}/{} bytes={}/{}",
                                progress.entries_completed,
                                progress.entries_total,
                                progress.bytes_completed,
                                progress.bytes_total
                            );
                        }
                    },
                )
                .map_err(map_portable)?;
            if json {
                let mut export_receipt = portable_export_receipt(&result);
                export_receipt["application_io"] = serde_json::to_value(
                    crate::storage_attribution_cli::lifecycle_application_io()?,
                )
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
                write_json(&export_receipt, output)?;
            } else {
                writeln!(
                    output,
                    "exported {} package_digest={} transport_digest={}",
                    result.representation, result.package_digest, result.transport_digest
                )
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
            }
        }
        PortableCommand::Staging { command } => match command {
            PortableStagingCommand::Inspect => {
                crate::ontology_cli::run_portable_staging_inspect(graph, json, output)?;
            }
            PortableStagingCommand::Adopt(args) => {
                crate::ontology_cli::run_portable_staging_adopt(graph, &args, json, output)?;
            }
        },
        command => {
            return run_portable_without_graph(project_root, command, json, output, allocation);
        }
    }
    Ok(())
}

fn portable_export_receipt(result: &graphforge_api::PortableV2ExportFacadeResult) -> Value {
    serde_json::to_value(result.receipt())
        .expect("portable receipt contains serializable scalar values")
}

/// Project-free portable operations that must not hold a live `GraphForge` lock.
#[allow(
    clippy::too_many_lines,
    reason = "CLI dispatch keeps verify/import/OCI in one project-free portable table"
)]
pub(crate) fn run_portable_without_graph(
    project_root: &std::path::Path,
    command: PortableCommand,
    json: bool,
    output: &mut dyn Write,
    allocation: Option<&graphforge_api::StorageAllocationDiagnostics>,
) -> Result<(), crate::CliRuntimeError> {
    match command {
        PortableCommand::Preview(_)
        | PortableCommand::Export(_)
        | PortableCommand::Staging { .. } => Err(graphforge_api::GfError::Validation(
            "preview/export require an open project handle".into(),
        )
        .into()),
        PortableCommand::Verify(args) => {
            let report = verify_portable_v2(
                &PortableVerifyRequest {
                    input: args.input,
                    mode: args.mode.into(),
                    limits: PortableV2Limits::default(),
                },
                None,
            )
            .map_err(map_portable)?;
            if json {
                write_json(&report, output)?;
            } else {
                writeln!(
                    output,
                    "verified package_digest={} integrity={:?} compatibility={:?}",
                    report.package_digest, report.integrity, report.compatibility
                )
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
            }
            Ok(())
        }
        PortableCommand::Import(args) => {
            let request = PortableV2ImportRequest {
                input: args.input,
                operation_id: OperationId(canonical_uuid(&args.idempotency_key)?),
                limits: PortableV2Limits::default(),
            };
            let result = match allocation {
                Some(allocation) => allocation.import(project_root, &request, None),
                None => GraphForge::import_portable_v2(project_root, &request, None),
            }
            .map_err(map_portable)?;
            let transient_peak_allocated_bytes = import_transient_peak(&result)?;
            if json {
                write_json(
                    &serde_json::json!({
                        "contract": "graphforge-portable-import/2",
                        "application_io": crate::storage_attribution_cli::lifecycle_application_io()?,
                        "package_digest": result.package_digest,
                        "transport_digest": result.transport_digest,
                        "generation_uuid": result.generation_uuid,
                        "idempotent_replay": result.idempotent_replay,
                        "transient_peak_allocated_bytes": transient_peak_allocated_bytes,
                    }),
                    output,
                )?;
            } else {
                writeln!(
                    output,
                    "imported generation {} package_digest={}",
                    result.generation_uuid, result.package_digest
                )
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
            }
            Ok(())
        }
        PortableCommand::PublishOci(args) => {
            let reference = publish_portable_v2_oci(
                &PortableV2OciPublishFacadeRequest {
                    package_path: args.package,
                    registry: args.registry,
                    repository: args.repository,
                    tag: args.tag,
                    limits: PortableV2Limits::default(),
                    authenticity: PortableV2OciAuthenticityPolicy::default(),
                    signature: None,
                    insecure_http: args.insecure_http,
                    credential: oci_credential(std::env::var(OCI_CREDENTIAL_ENV).ok()),
                },
                None,
            )
            .map_err(map_portable)?;
            if json {
                write_json(
                    &serde_json::json!({
                        "contract": "graphforge-portable-oci-publish/2",
                        "registry": reference.registry,
                        "repository": reference.repository,
                        "oci_manifest_digest": reference.oci_manifest_digest,
                        "package_digest": reference.package_digest,
                        "tag": reference.tag,
                    }),
                    output,
                )?;
            } else {
                writeln!(
                    output,
                    "published oci_manifest_digest={}",
                    reference.oci_manifest_digest
                )
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
            }
            Ok(())
        }
        PortableCommand::PullOci(args) => {
            let receipt = pull_portable_v2_oci(
                &PortableV2OciPullFacadeRequest {
                    registry: args.registry,
                    repository: args.repository,
                    reference: args.reference,
                    expected_oci_digest: args.expected_digest,
                    destination: args.destination,
                    limits: PortableV2Limits::default(),
                    authenticity: PortableV2OciAuthenticityPolicy::default(),
                    insecure_http: args.insecure_http,
                    credential: oci_credential(std::env::var(OCI_CREDENTIAL_ENV).ok()),
                },
                None,
            )
            .map_err(map_portable)?;
            if json {
                write_json(
                    &serde_json::json!({
                        "contract": "graphforge-portable-oci-pull/2",
                        "oci_manifest_digest": receipt.reference.oci_manifest_digest,
                        "package_digest": receipt.reference.package_digest,
                    }),
                    output,
                )?;
            } else {
                writeln!(
                    output,
                    "pulled package_digest={} oci_manifest_digest={}",
                    receipt.reference.package_digest, receipt.reference.oci_manifest_digest
                )
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
            }
            Ok(())
        }
    }
}

fn import_transient_peak(
    result: &graphforge_api::PortableV2ImportResult,
) -> Result<u64, graphforge_api::GfError> {
    result.transient_peak_allocated_bytes()
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum QuerySinkFormat {
    Parquet,
    ArrowIpc,
}

#[derive(Args)]
pub(crate) struct QueryArgs {
    /// Cypher statement to execute. Repeat `--cypher`/`--output` pairs to run
    /// several statements against one open project; each statement emits its
    /// own receipt line, in order.
    #[arg(long, required = true)]
    cypher: Vec<String>,
    /// Streaming sink destination for the `--cypher` at the same position.
    #[arg(long, required = true)]
    output: Vec<PathBuf>,
    #[arg(long, value_enum, default_value_t = QuerySinkFormat::Parquet)]
    format: QuerySinkFormat,
    #[arg(long)]
    max_batch_rows: Option<usize>,
    #[arg(long)]
    max_row_group_rows: Option<usize>,
}

pub(crate) fn run_query(
    graph: &GraphForge,
    args: &QueryArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if args.cypher.len() != args.output.len() {
        return Err(graphforge_api::GfError::Validation(
            "query requires exactly one --output per --cypher".into(),
        ));
    }
    let mut destinations = std::collections::BTreeSet::new();
    let mut paths = Vec::with_capacity(args.output.len());
    for path in &args.output {
        let path = path.to_str().ok_or_else(|| {
            graphforge_api::GfError::Validation("query --output must be valid UTF-8".into())
        })?;
        if !destinations.insert(path) {
            return Err(graphforge_api::GfError::Validation(
                "query --output destinations must be distinct".into(),
            ));
        }
        paths.push(path);
    }
    let options = ResultSinkOptions {
        max_batch_rows: args.max_batch_rows.unwrap_or(65_536),
        max_row_group_rows: args.max_row_group_rows.unwrap_or(65_536),
    };
    let params = std::collections::HashMap::new();
    let format = match args.format {
        QuerySinkFormat::Parquet => graphforge_api::ResultSinkFormat::Parquet,
        QuerySinkFormat::ArrowIpc => graphforge_api::ResultSinkFormat::ArrowIpc,
    };
    // Each receipt attributes only the I/O performed since the previous
    // receipt, so the first statement carries the project open and the
    // receipts of one process sum to that process's whole attribution.
    let mut previous_io: Option<graphforge_api::LifecyclePhaseAttribution> = None;
    for (cypher, path) in args.cypher.iter().zip(paths) {
        let receipt = graph
            .execute_to_result_sink_with_evidence(cypher, &params, path, format, &options, None)?;
        if json {
            let snapshot = graphforge_api::lifecycle_io_snapshot();
            let application_io = match &previous_io {
                Some(earlier) => snapshot.since(earlier)?,
                None => snapshot.clone(),
            };
            application_io.validate_for_qualification()?;
            previous_io = Some(snapshot);
            write_json(
                &serde_json::json!({
                    "contract": "graphforge-result-sink/2",
                    "application_io": application_io,
                    "destination": receipt.sink.destination,
                    "format": format!("{:?}", receipt.sink.format),
                    "rows": receipt.sink.progress.rows,
                    "batches": receipt.sink.progress.batches,
                    "bytes": receipt.sink.progress.bytes,
                    "complete": receipt.sink.progress.complete,
                    "result_sha256": receipt.result_sha256,
                    "scalar_u64": receipt.scalar_u64,
                    "query_evidence": receipt.evidence,
                }),
                output,
            )?;
        } else {
            writeln!(
                output,
                "wrote {} rows={} bytes={}",
                receipt.sink.destination.display(),
                receipt.sink.progress.rows,
                receipt.sink.progress.bytes
            )
            .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
        }
    }
    Ok(())
}

#[derive(Subcommand)]
pub(crate) enum ImportSessionCommand {
    /// Open a new staged import session.
    Begin(ImportSessionBeginArgs),
    /// Resume an existing session by UUID.
    Resume(ImportSessionResumeArgs),
    /// Read durable progress, including a terminal construction receipt.
    Status(ImportSessionIdArgs),
    /// Register a Parquet source path into the session.
    RegisterParquet(ImportSessionRegisterArgs),
    /// Checkpoint session progress.
    Checkpoint(ImportSessionIdArgs),
    /// Validate staged sources.
    Validate(ImportSessionIdArgs),
    /// Commit the session into the project.
    Commit(ImportSessionIdArgs),
    /// Abort the session.
    Abort(ImportSessionIdArgs),
    /// Cleanup stale sessions older than the given age.
    Cleanup(ImportSessionCleanupArgs),
}

#[derive(Args)]
pub(crate) struct ImportSessionBeginArgs {
    #[arg(long)]
    operation_uuid: String,
}

#[derive(Args)]
pub(crate) struct ImportSessionResumeArgs {
    #[arg(long)]
    session_uuid: String,
}

#[derive(Args)]
pub(crate) struct ImportSessionRegisterArgs {
    #[arg(long)]
    session_uuid: String,
    #[arg(long)]
    path: PathBuf,
    #[arg(long, value_enum)]
    kind: ImportSourceKindArg,
}

#[derive(Args)]
pub(crate) struct ImportSessionIdArgs {
    #[arg(long)]
    session_uuid: String,
}

#[derive(Args)]
pub(crate) struct ImportSessionCleanupArgs {
    #[arg(long, default_value_t = 86_400)]
    max_age_secs: u64,
}

#[derive(Clone, Copy, ValueEnum)]
enum ImportSourceKindArg {
    Nodes,
    Edges,
}

pub(crate) fn run_import_session(
    graph: &GraphForge,
    command: ImportSessionCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    match command {
        ImportSessionCommand::Begin(args) => {
            let session = graph.begin_import_session(
                OperationId(canonical_uuid(&args.operation_uuid)?),
                ImportSessionLimits::default(),
            )?;
            write_session_receipt(session.session_uuid(), "begun", json, output)
        }
        ImportSessionCommand::Resume(args) => {
            let session = graph.resume_import_session(canonical_uuid(&args.session_uuid)?)?;
            write_session_receipt(session.session_uuid(), "resumed", json, output)
        }
        ImportSessionCommand::Status(args) => {
            let session_uuid = canonical_uuid(&args.session_uuid)?;
            let (phase, progress) = graph.import_session_status(session_uuid)?;
            write_progress(
                session_uuid,
                &format!("{phase:?}").to_ascii_lowercase(),
                &progress,
                None,
                json,
                output,
            )
        }
        ImportSessionCommand::RegisterParquet(args) => {
            let mut session = graph.resume_import_session(canonical_uuid(&args.session_uuid)?)?;
            let kind = match args.kind {
                ImportSourceKindArg::Nodes => BulkInputKind::Node,
                ImportSourceKindArg::Edges => BulkInputKind::Edge,
            };
            session.register_parquet(kind, &args.path)?;
            write_session_receipt(session.session_uuid(), "registered", json, output)
        }
        ImportSessionCommand::Checkpoint(args) => {
            let mut session = graph.resume_import_session(canonical_uuid(&args.session_uuid)?)?;
            let progress = session.checkpoint()?;
            write_progress(
                session.session_uuid(),
                "checkpointed",
                &progress,
                None,
                json,
                output,
            )
        }
        ImportSessionCommand::Validate(args) => {
            let mut session = graph.resume_import_session(canonical_uuid(&args.session_uuid)?)?;
            let progress = session.validate(graph)?;
            write_progress(
                session.session_uuid(),
                "validated",
                &progress,
                Some(session.operation_timings()),
                json,
                output,
            )
        }
        ImportSessionCommand::Commit(args) => {
            let mut session = graph.resume_import_session(canonical_uuid(&args.session_uuid)?)?;
            let generation = session.commit(graph, None)?;
            write_import_commit(&session, generation, json, output)
        }
        ImportSessionCommand::Abort(args) => {
            let session = graph.resume_import_session(canonical_uuid(&args.session_uuid)?)?;
            let session_uuid = session.session_uuid();
            let progress = session.abort(graph)?;
            write_progress(session_uuid, "aborted", &progress, None, json, output)
        }
        ImportSessionCommand::Cleanup(args) => {
            let removed =
                graph.cleanup_stale_import_sessions(Duration::from_secs(args.max_age_secs))?;
            if json {
                write_json(
                    &serde_json::json!({
                        "contract": "graphforge-import-session-cleanup/1",
                        "removed": removed,
                    }),
                    output,
                )
            } else {
                writeln!(output, "removed {removed} stale import sessions")
                    .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))
            }
        }
    }
}

fn write_import_commit(
    session: &graphforge_api::GraphImportSession,
    generation: Uuid,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let (_, progress) = session.status();
    if json {
        write_json(
            &serde_json::json!({
                "contract": "graphforge-import-session/1",
                "outcome": "committed",
                "session_uuid": session.session_uuid(),
                "generation_uuid": generation,
                "construction": progress.construction,
                "operation_timings": session.operation_timings(),
            }),
            output,
        )
    } else {
        writeln!(
            output,
            "committed session {} generation {generation}",
            session.session_uuid()
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))
    }
}

fn write_session_receipt(
    session_uuid: Uuid,
    outcome: &str,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if json {
        write_json(
            &serde_json::json!({
                "contract": "graphforge-import-session/1",
                "outcome": outcome,
                "session_uuid": session_uuid,
            }),
            output,
        )
    } else {
        writeln!(output, "{outcome} session {session_uuid}")
            .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))
    }
}

fn write_progress(
    session_uuid: Uuid,
    outcome: &str,
    progress: &graphforge_api::ImportProgress,
    timings: Option<graphforge_api::ImportOperationTimings>,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if json {
        let mut receipt = serde_json::json!({
            "contract": "graphforge-import-session/1",
            "outcome": outcome,
            "session_uuid": session_uuid,
            "rows_accepted": progress.rows_accepted,
            "rows_rejected": progress.rows_rejected,
            "bytes_accepted": progress.bytes_accepted,
            "construction": progress.construction,
        });
        if let Some(timings) = timings {
            receipt["operation_timings"] = serde_json::json!(timings);
        }
        write_json(&receipt, output)
    } else {
        writeln!(
            output,
            "{outcome} session {session_uuid} rows_accepted={} bytes_accepted={}",
            progress.rows_accepted, progress.bytes_accepted
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))
    }
}

#[cfg(test)]
mod lifecycle_storage_tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn portable_export_receipt_exposes_closed_writer_allocation() {
        let result = graphforge_api::PortableV2ExportFacadeResult {
            contract: "graphforge-portable-export/2",
            source: "current",
            checkpoint: None,
            generation_uuid: Uuid::nil(),
            package_digest: format!("sha256:{}", "0".repeat(64)),
            transport_digest: format!("sha256:{}", "1".repeat(64)),
            entry_count: 2,
            payload_bytes: 100,
            representation: "bundle",
            selection_fingerprint: format!("sha256:{}", "2".repeat(64)),
            output: PathBuf::from("/secret/package.gfpb"),
            allocation_identity_allocated_bytes: BTreeMap::from([
                ("first-native-identity".to_owned(), 4_096),
                ("second-native-identity".to_owned(), 8_192),
            ]),
            allocation_logical_bytes: 10_000,
            allocation_physical_objects: 2,
        };

        let receipt = portable_export_receipt(&result);

        assert_eq!(receipt["allocation_logical_bytes"], 10_000);
        assert_eq!(receipt["allocation_allocated_bytes"], 12_288);
        assert_eq!(receipt["allocation_physical_objects"], 2);
        let encoded = serde_json::to_string(&receipt).unwrap();
        for forbidden in ["/secret", "native-identity", "output"] {
            assert!(!encoded.contains(forbidden));
        }
    }

    fn import_result() -> graphforge_api::PortableV2ImportResult {
        graphforge_api::PortableV2ImportResult {
            package_digest: format!("sha256:{}", "0".repeat(64)),
            transport_digest: Some(format!("sha256:{}", "1".repeat(64))),
            generation_uuid: Uuid::nil(),
            idempotent_replay: false,
            materialized_identity_allocated_bytes: BTreeMap::from([
                ("shared".to_owned(), 4),
                ("stage".to_owned(), 6),
            ]),
            published_identity_allocated_bytes: BTreeMap::from([
                ("project".to_owned(), 10),
                ("shared".to_owned(), 4),
            ]),
            materialized_cleanup_removed_identity_allocated_bytes: BTreeMap::from([
                ("shared".to_owned(), 4),
                ("stage".to_owned(), 6),
            ]),
            materialized_cleanup_parent_sync_confirmed: true,
        }
    }

    #[test]
    fn portable_import_peak_deduplicates_shared_native_identity() {
        assert_eq!(import_transient_peak(&import_result()).unwrap(), 20);
    }

    #[test]
    fn portable_import_peak_rejects_cleanup_contradiction() {
        let mut result = import_result();
        result
            .materialized_cleanup_removed_identity_allocated_bytes
            .remove("stage");
        assert!(import_transient_peak(&result).is_err());
        let mut result = import_result();
        result.materialized_cleanup_parent_sync_confirmed = false;
        assert!(import_transient_peak(&result).is_err());
    }
}

#[cfg(test)]
mod oci_credential_tests {
    use super::{OCI_CREDENTIAL_ENV, oci_credential};

    #[test]
    fn absent_empty_and_whitespace_values_yield_no_credential() {
        for raw in [
            None,
            Some(String::new()),
            Some("   ".to_owned()),
            Some("\t\n ".to_owned()),
        ] {
            assert_eq!(oci_credential(raw), None);
        }
    }

    #[test]
    fn present_values_reach_the_registry_unchanged() {
        for raw in ["plain-token", "user:secret", " padded-token "] {
            assert_eq!(oci_credential(Some(raw.to_owned())), Some(raw.to_owned()));
        }
    }

    #[test]
    fn the_credential_variable_keeps_its_documented_name() {
        assert_eq!(OCI_CREDENTIAL_ENV, "GRAPHFORGE_OCI_CREDENTIAL");
    }
}
