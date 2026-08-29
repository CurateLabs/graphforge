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

fn oci_credential() -> Option<String> {
    std::env::var("GRAPHFORGE_OCI_CREDENTIAL").ok()
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
                write_json(
                    &serde_json::json!({
                        "contract": result.contract,
                        "source": result.source,
                        "checkpoint": result.checkpoint,
                        "generation_uuid": result.generation_uuid,
                        "package_digest": result.package_digest,
                        "transport_digest": result.transport_digest,
                        "entry_count": result.entry_count,
                        "payload_bytes": result.payload_bytes,
                        "representation": result.representation,
                        "selection_fingerprint": result.selection_fingerprint,
                    }),
                    output,
                )?;
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
            return run_portable_without_graph(project_root, command, json, output);
        }
    }
    Ok(())
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
            let result = GraphForge::import_portable_v2(
                project_root,
                &PortableV2ImportRequest {
                    input: args.input,
                    operation_id: OperationId(canonical_uuid(&args.idempotency_key)?),
                    limits: PortableV2Limits::default(),
                },
                None,
            )
            .map_err(map_portable)?;
            if json {
                write_json(
                    &serde_json::json!({
                        "contract": "graphforge-portable-import/2",
                        "package_digest": result.package_digest,
                        "transport_digest": result.transport_digest,
                        "generation_uuid": result.generation_uuid,
                        "idempotent_replay": result.idempotent_replay,
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
                    credential: oci_credential(),
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
                    credential: oci_credential(),
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

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum QuerySinkFormat {
    Parquet,
    ArrowIpc,
}

#[derive(Args)]
pub(crate) struct QueryArgs {
    /// Cypher query to execute.
    #[arg(long)]
    cypher: String,
    /// Streaming sink destination.
    #[arg(long)]
    output: PathBuf,
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
    let options = ResultSinkOptions {
        max_batch_rows: args.max_batch_rows.unwrap_or(65_536),
        max_row_group_rows: args.max_row_group_rows.unwrap_or(65_536),
    };
    let path = args.output.to_str().ok_or_else(|| {
        graphforge_api::GfError::Validation("query --output must be valid UTF-8".into())
    })?;
    let params = std::collections::HashMap::new();
    let receipt = match args.format {
        QuerySinkFormat::Parquet => graph.execute_to_parquet_stream_with_params(
            &args.cypher,
            &params,
            path,
            &options,
            None,
        )?,
        QuerySinkFormat::ArrowIpc => graph.execute_to_arrow_ipc_stream_with_params(
            &args.cypher,
            &params,
            path,
            &options,
            None,
        )?,
    };
    if json {
        write_json(
            &serde_json::json!({
                "contract": "graphforge-result-sink/1",
                "destination": receipt.destination,
                "format": format!("{:?}", receipt.format),
                "rows": receipt.progress.rows,
                "batches": receipt.progress.batches,
                "bytes": receipt.progress.bytes,
                "complete": receipt.progress.complete,
            }),
            output,
        )?;
    } else {
        writeln!(
            output,
            "wrote {} rows={} bytes={}",
            receipt.destination.display(),
            receipt.progress.rows,
            receipt.progress.bytes
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
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
                json,
                output,
            )
        }
        ImportSessionCommand::Validate(args) => {
            let mut session = graph.resume_import_session(canonical_uuid(&args.session_uuid)?)?;
            let progress = session.validate(graph)?;
            write_progress(session.session_uuid(), "validated", &progress, json, output)
        }
        ImportSessionCommand::Commit(args) => {
            let mut session = graph.resume_import_session(canonical_uuid(&args.session_uuid)?)?;
            let generation = session.commit(graph, None)?;
            let (_, progress) = session.status();
            if json {
                write_json(
                    &serde_json::json!({
                        "contract": "graphforge-import-session/1",
                        "outcome": "committed",
                        "session_uuid": session.session_uuid(),
                        "generation_uuid": generation,
                        "construction": progress.construction,
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
        ImportSessionCommand::Abort(args) => {
            let session = graph.resume_import_session(canonical_uuid(&args.session_uuid)?)?;
            let session_uuid = session.session_uuid();
            let progress = session.abort(graph)?;
            write_progress(session_uuid, "aborted", &progress, json, output)
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
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if json {
        write_json(
            &serde_json::json!({
                "contract": "graphforge-import-session/1",
                "outcome": outcome,
                "session_uuid": session_uuid,
                "rows_accepted": progress.rows_accepted,
                "rows_rejected": progress.rows_rejected,
                "bytes_accepted": progress.bytes_accepted,
                "construction": progress.construction,
            }),
            output,
        )
    } else {
        writeln!(
            output,
            "{outcome} session {session_uuid} rows_accepted={} bytes_accepted={}",
            progress.rows_accepted, progress.bytes_accepted
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))
    }
}
