//! Reusable GraphForge command-line interface.
#![forbid(unsafe_code)]

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use arrow::ipc::writer::StreamWriter;
use clap::{Args, Parser, Subcommand, ValueEnum, error::ErrorKind};
use gf_api::{
    CheckpointDiffDetail, CheckpointDiffScope, CheckpointRequest, CheckpointSelector,
    DeleteCheckpointRequest, DiffCheckpointsRequest, ExecutionResult, GraphForge,
    ListCheckpointsRequest, OperationId, PageRequest, PageToken, PortableExportRequest,
    PortableExportResult, PortableImportRequest, PortableImportResult, PortableSelection,
    RepositoryContext, RevertCheckpointRequest,
};
use uuid::Uuid;

/// GraphForge command-line interface.
#[derive(Parser)]
#[command(name = "graphforge", version, about = "GraphForge CLI")]
struct Cli {
    /// Print version info and exit.
    #[arg(long)]
    info: bool,

    /// Persistent GraphForge project directory.
    #[arg(long, global = true)]
    project: Option<PathBuf>,

    /// Code repository root; defaults to discovery from the current directory.
    #[arg(long, global = true)]
    project_dir: Option<PathBuf>,

    /// Emit the stable JSON result for repository lifecycle commands.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize repository-local GraphForge definitions and state.
    Init,
    /// Validate declared definitions and source digests without ingesting data.
    Sync,
    /// Remove only repository-local runtime state.
    Remove(RemoveArgs),
    /// Validate or resolve repository configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Restore the complete workspace from a checkpoint.
    Revert(RevertArgs),
    /// Export one immutable generation as a portable envelope.
    Export(ExportArgs),
    /// Import a portable envelope into a new or empty project.
    Import(ImportArgs),
    /// Manage immutable named workspace checkpoints.
    Checkpoint {
        #[command(subcommand)]
        command: CheckpointCommand,
    },
}

#[derive(Args)]
struct RemoveArgs {
    /// Required explicit non-interactive confirmation.
    #[arg(long)]
    yes: bool,
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Parse and validate graphforge.yaml.
    Validate,
    /// Emit deterministic, secret-free resolved configuration.
    Resolve,
}

#[derive(Subcommand)]
enum CheckpointCommand {
    /// Create a checkpoint of the current complete workspace.
    Create(CreateArgs),
    /// List active checkpoints in canonical order.
    List(PageArgs),
    /// Execute one read-only Cypher query against a checkpoint.
    Open(OpenArgs),
    /// Delete an active checkpoint reference.
    Delete(DeleteArgs),
    /// Diff two checkpoint/current endpoints.
    Diff(DiffArgs),
    /// Restore the complete workspace from a checkpoint.
    Revert(RevertArgs),
}

#[derive(Args)]
struct CreateArgs {
    name: String,
    #[arg(long)]
    description: Option<String>,
    #[arg(long)]
    idempotency_key: String,
    #[arg(long)]
    actor_uuid: Option<String>,
}

#[derive(Args)]
struct PageArgs {
    #[arg(long, default_value_t = 100)]
    limit: u32,
    #[arg(long)]
    after: Option<String>,
}

#[derive(Args)]
struct OpenArgs {
    name: String,
    /// Read-only Cypher after `--`.
    #[arg(last = true, required = true, num_args = 1..)]
    query: Vec<String>,
}

#[derive(Args)]
struct DeleteArgs {
    name: String,
    #[arg(long)]
    idempotency_key: String,
    #[arg(long)]
    actor_uuid: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
enum ScopeArg {
    Summary,
    Graph,
    Ontology,
    Configuration,
    Capabilities,
    Provenance,
    Knowledge,
    Epistemic,
    All,
}

#[derive(Clone, Copy, ValueEnum)]
enum DetailArg {
    Summary,
    Records,
}

#[derive(Args)]
struct DiffArgs {
    /// Named checkpoint used as the earlier endpoint.
    #[arg(long, conflicts_with = "from_current")]
    from: Option<String>,
    /// Use the current generation as the earlier endpoint.
    #[arg(long, conflicts_with = "from")]
    from_current: bool,
    /// Named checkpoint used as the later endpoint.
    #[arg(long, conflicts_with = "to_current")]
    to: Option<String>,
    /// Use the current generation as the later endpoint.
    #[arg(long, conflicts_with = "to")]
    to_current: bool,
    #[arg(long, value_enum)]
    scope: ScopeArg,
    #[arg(long, value_enum)]
    detail: DetailArg,
    #[command(flatten)]
    page: PageArgs,
}

#[derive(Args)]
struct RevertArgs {
    name: String,
    /// Required bounded audit reason.
    #[arg(long)]
    reason: String,
    /// Required canonical UUID used for idempotent replay.
    #[arg(long)]
    idempotency_key: String,
    #[arg(long)]
    actor_uuid: Option<String>,
}

#[derive(Args)]
struct ExportArgs {
    /// Export the generation committed as CURRENT when the command starts.
    #[arg(
        long,
        required_unless_present = "checkpoint",
        conflicts_with = "checkpoint"
    )]
    current: bool,
    /// Export the generation pinned by this active checkpoint.
    #[arg(long, required_unless_present = "current", conflicts_with = "current")]
    checkpoint: Option<String>,
    /// Portable envelope destination.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Args)]
struct ImportArgs {
    /// Portable envelope source.
    #[arg(long)]
    input: PathBuf,
    /// Required canonical UUID used for idempotent replay.
    #[arg(long)]
    idempotency_key: String,
}

fn canonical_uuid(value: &str) -> Result<Uuid, gf_api::GfError> {
    let parsed = Uuid::parse_str(value)
        .map_err(|_| gf_api::GfError::Validation("expected canonical UUID".into()))?;
    if parsed.hyphenated().to_string() != value {
        return Err(gf_api::GfError::Validation(
            "expected canonical lowercase hyphenated UUID".into(),
        ));
    }
    Ok(parsed)
}

fn actor(value: Option<&str>) -> Result<Option<Uuid>, gf_api::GfError> {
    value.map(canonical_uuid).transpose()
}

fn page(args: &PageArgs) -> Result<PageRequest, gf_api::GfError> {
    Ok(PageRequest {
        limit: args.limit,
        after: args.after.as_deref().map(PageToken::parse).transpose()?,
        cancellation: None,
    })
}

fn selector(
    value: Option<String>,
    current: bool,
    endpoint: &str,
) -> Result<CheckpointSelector, gf_api::GfError> {
    match (value, current) {
        (Some(name), false) => Ok(CheckpointSelector::Named(name)),
        (None, true) => Ok(CheckpointSelector::Current),
        _ => Err(gf_api::GfError::Validation(format!(
            "exactly one of --{endpoint} or --{endpoint}-current is required"
        ))),
    }
}

fn write_result(result: &ExecutionResult, output: &mut dyn Write) -> Result<(), gf_api::GfError> {
    {
        let mut writer = StreamWriter::try_new(&mut *output, result.schema.as_ref())
            .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
        for batch in &result.batches {
            writer
                .write(batch)
                .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
        }
        writer
            .finish()
            .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
    }
    output
        .flush()
        .map_err(|error| gf_api::GfError::Execution(error.to_string()))
}

fn write_export_result(
    result: &PortableExportResult,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), gf_api::GfError> {
    if json {
        writeln!(
            output,
            "{}",
            serde_json::to_string(result)
                .map_err(|error| gf_api::GfError::Execution(error.to_string()))?
        )
        .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
    } else {
        writeln!(
            output,
            "exported {} generation {} to {} (sha256 {}, {} bytes, {} participants)",
            result.source,
            result.generation_uuid,
            result.output.display(),
            result.envelope_sha256,
            result.byte_length,
            result.participant_count
        )
        .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
    }
    Ok(())
}

fn write_import_result(
    result: &PortableImportResult,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), gf_api::GfError> {
    if json {
        writeln!(
            output,
            "{}",
            serde_json::to_string(result)
                .map_err(|error| gf_api::GfError::Execution(error.to_string()))?
        )
        .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
    } else {
        let outcome = if result.idempotent_replay {
            "replayed"
        } else {
            "imported"
        };
        writeln!(
            output,
            "{outcome} source generation {} as {} (sha256 {})",
            result.source_generation_uuid, result.generation_uuid, result.envelope_sha256
        )
        .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
    }
    Ok(())
}

fn run_import(
    args: ImportArgs,
    path: &Path,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), gf_api::GfError> {
    let result = GraphForge::import_portable(
        path,
        &PortableImportRequest {
            input: args.input,
            operation_id: OperationId(canonical_uuid(&args.idempotency_key)?),
        },
    )?;
    write_import_result(&result, json, output)
}

fn run_export(
    graph: &GraphForge,
    args: ExportArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), gf_api::GfError> {
    let selection = match args.checkpoint {
        Some(name) => PortableSelection::Checkpoint(name),
        None if args.current => PortableSelection::Current,
        None => unreachable!("clap requires exactly one export selector"),
    };
    let result = graph.export_portable(PortableExportRequest {
        selection,
        output: args.output,
    })?;
    write_export_result(&result, json, output)
}

fn run_repository(
    command: Command,
    project_dir: Option<PathBuf>,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), gf_api::GfError> {
    let start = project_dir.unwrap_or(
        std::env::current_dir().map_err(|error| gf_api::GfError::Storage(error.to_string()))?,
    );
    let repository = RepositoryContext::discover(start)?;
    let always_json = matches!(
        &command,
        Command::Config {
            command: ConfigCommand::Resolve
        }
    );
    let value = match command {
        Command::Init => serde_json::to_value(repository.init()?),
        Command::Sync => serde_json::to_value(repository.sync()?),
        Command::Remove(args) => serde_json::to_value(repository.remove(args.yes)?),
        Command::Config {
            command: ConfigCommand::Validate,
        } => {
            repository.load_config()?;
            Ok(serde_json::json!({"valid": true}))
        }
        Command::Config {
            command: ConfigCommand::Resolve,
        } => Ok(repository.resolve_config()?),
        _ => unreachable!(),
    }
    .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
    if json || always_json {
        writeln!(
            output,
            "{}",
            serde_json::to_string(&value)
                .map_err(|error| gf_api::GfError::Execution(error.to_string()))?
        )
        .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
    } else {
        writeln!(output, "ok").map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
    }
    Ok(())
}

fn run(cli: Cli, output: &mut dyn Write) -> Result<(), gf_api::GfError> {
    if cli.info {
        writeln!(output, "graphforge {}", env!("CARGO_PKG_VERSION"))
            .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
        return Ok(());
    }
    let Some(command) = cli.command else {
        writeln!(output, "GraphForge — use --help for options")
            .map_err(|error| gf_api::GfError::Execution(error.to_string()))?;
        return Ok(());
    };
    if matches!(
        &command,
        Command::Init | Command::Sync | Command::Remove(_) | Command::Config { .. }
    ) {
        return run_repository(command, cli.project_dir, cli.json, output);
    }
    let path = match cli.project {
        Some(path) => path,
        None => {
            RepositoryContext::discover(
                cli.project_dir.unwrap_or(
                    std::env::current_dir()
                        .map_err(|error| gf_api::GfError::Storage(error.to_string()))?,
                ),
            )?
            .state_path
        }
    };
    let command = match command {
        Command::Import(args) => return run_import(args, &path, cli.json, output),
        command => command,
    };
    let path = path
        .to_str()
        .ok_or_else(|| gf_api::GfError::Validation("--project must be valid UTF-8".into()))?;
    let mut graph = GraphForge::new(Some(path))?;
    let command = match command {
        Command::Export(args) => return run_export(&graph, args, cli.json, output),
        command => command,
    };
    let result = match command {
        Command::Revert(args) => graph.revert_to_checkpoint(RevertCheckpointRequest {
            name: args.name,
            reason: args.reason,
            idempotency_key: OperationId(canonical_uuid(&args.idempotency_key)?),
            actor_uuid: actor(args.actor_uuid.as_deref())?,
        })?,
        Command::Checkpoint { command } => match command {
            CheckpointCommand::Create(args) => graph.checkpoint(CheckpointRequest {
                name: args.name,
                description: args.description,
                idempotency_key: OperationId(canonical_uuid(&args.idempotency_key)?),
                actor_uuid: actor(args.actor_uuid.as_deref())?,
            })?,
            CheckpointCommand::List(args) => {
                graph.list_checkpoints(ListCheckpointsRequest { page: page(&args)? })?
            }
            CheckpointCommand::Open(args) => {
                let view = graph.open_checkpoint(&args.name)?;
                view.execute(&args.query.join(" "))?
            }
            CheckpointCommand::Delete(args) => {
                graph.delete_checkpoint(DeleteCheckpointRequest {
                    name: args.name,
                    idempotency_key: OperationId(canonical_uuid(&args.idempotency_key)?),
                    actor_uuid: actor(args.actor_uuid.as_deref())?,
                })?
            }
            CheckpointCommand::Diff(args) => graph.diff_checkpoints(DiffCheckpointsRequest {
                from: selector(args.from, args.from_current, "from")?,
                to: selector(args.to, args.to_current, "to")?,
                scope: match args.scope {
                    ScopeArg::Summary => CheckpointDiffScope::Summary,
                    ScopeArg::Graph => CheckpointDiffScope::Graph,
                    ScopeArg::Ontology => CheckpointDiffScope::Ontology,
                    ScopeArg::Configuration => CheckpointDiffScope::Configuration,
                    ScopeArg::Capabilities => CheckpointDiffScope::Capabilities,
                    ScopeArg::Provenance => CheckpointDiffScope::Provenance,
                    ScopeArg::Knowledge => CheckpointDiffScope::Knowledge,
                    ScopeArg::Epistemic => CheckpointDiffScope::Epistemic,
                    ScopeArg::All => CheckpointDiffScope::All,
                },
                detail: match args.detail {
                    DetailArg::Summary => CheckpointDiffDetail::Summary,
                    DetailArg::Records => CheckpointDiffDetail::Records,
                },
                page: page(&args.page)?,
            })?,
            CheckpointCommand::Revert(args) => {
                graph.revert_to_checkpoint(RevertCheckpointRequest {
                    name: args.name,
                    reason: args.reason,
                    idempotency_key: OperationId(canonical_uuid(&args.idempotency_key)?),
                    actor_uuid: actor(args.actor_uuid.as_deref())?,
                })?
            }
        },
        _ => unreachable!(),
    };
    write_result(&result, output)
}

/// Captured result of one Rust-owned CLI invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliExecution {
    /// Process-compatible exit status.
    pub exit_code: i32,
    /// Exact standard-output bytes, including Arrow IPC results.
    pub stdout: Vec<u8>,
    /// Exact standard-error bytes.
    pub stderr: Vec<u8>,
}

/// Parse and execute the GraphForge CLI without terminating the host process.
pub fn execute<I, T>(args: I) -> CliExecution
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut args: Vec<std::ffi::OsString> = args.into_iter().map(Into::into).collect();
    if args.is_empty() {
        args.push("graphforge".into());
    } else {
        args[0] = "graphforge".into();
    }
    let json = args.iter().any(|arg| arg == "--json");
    let cli = match Cli::try_parse_from(&args) {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = error.exit_code();
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) {
                stdout = error.to_string().into_bytes();
            } else if json {
                writeln!(
                    stderr,
                    "{}",
                    serde_json::json!({"error":{"code":"GF_VALIDATION","message":error.to_string()}})
                )
                .expect("writing to Vec cannot fail");
            } else {
                stderr = error.to_string().into_bytes();
            }
            return CliExecution {
                exit_code,
                stdout,
                stderr,
            };
        }
    };
    let json = cli.json;
    let exit_code = if let Err(error) = run(cli, &mut stdout) {
        write_error(&error, json, &mut stderr).expect("writing to Vec cannot fail");
        error_exit_code(&error)
    } else {
        0
    };
    CliExecution {
        exit_code,
        stdout,
        stderr,
    }
}

fn write_error(error: &gf_api::GfError, json: bool, output: &mut dyn Write) -> io::Result<()> {
    if json {
        writeln!(
            output,
            "{}",
            serde_json::json!({"error":{"code":error.code(),"message":error.to_string()}})
        )
    } else {
        writeln!(output, "{}: {error}", error.code())
    }
}

fn error_exit_code(error: &gf_api::GfError) -> i32 {
    match error {
        gf_api::GfError::Validation(_) => 2,
        gf_api::GfError::Storage(_) => 3,
        _ => 1,
    }
}

/// Stream process output directly and terminate with the native CLI exit status.
pub fn run_process() {
    let cli = Cli::parse();
    let json = cli.json;
    let stdout = io::stdout();
    let mut output = stdout.lock();
    if let Err(error) = run(cli, &mut output) {
        let _ = output.flush();
        let stderr = io::stderr();
        write_error(&error, json, &mut stderr.lock()).expect("write CLI stderr");
        std::process::exit(error_exit_code(&error));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reusable_execution_normalizes_identity_and_captures_structured_errors() {
        let version = execute(["arbitrary-launcher", "--version"]);
        assert_eq!(version.exit_code, 0);
        assert_eq!(
            String::from_utf8(version.stdout).expect("UTF-8 version"),
            format!("graphforge {}\n", env!("CARGO_PKG_VERSION"))
        );
        assert!(version.stderr.is_empty());

        let invalid = execute(["another-launcher", "--json", "unknown-command"]);
        assert_eq!(invalid.exit_code, 2);
        assert!(invalid.stdout.is_empty());
        let error: serde_json::Value =
            serde_json::from_slice(&invalid.stderr).expect("structured CLI error");
        assert_eq!(error["error"]["code"], "GF_VALIDATION");
        assert!(
            error["error"]["message"]
                .as_str()
                .expect("error message")
                .contains("Usage: graphforge")
        );
    }

    #[test]
    fn revert_requires_reason_and_idempotency_key() {
        let missing_reason = Cli::try_parse_from([
            "gf",
            "--project",
            "/tmp/project",
            "checkpoint",
            "revert",
            "before-change",
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000001",
        ]);
        assert!(missing_reason.is_err());

        let missing_key = Cli::try_parse_from([
            "gf",
            "--project",
            "/tmp/project",
            "checkpoint",
            "revert",
            "before-change",
            "--reason",
            "undo invalid import",
        ]);
        assert!(missing_key.is_err());
    }

    #[test]
    fn create_and_delete_require_idempotency_keys() {
        for command in ["create", "delete"] {
            assert!(
                Cli::try_parse_from([
                    "gf",
                    "--project",
                    "/tmp/project",
                    "checkpoint",
                    command,
                    "before-change",
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn diff_disambiguates_current_from_a_checkpoint_named_current() {
        assert!(matches!(
            selector(Some("current".into()), false, "from").unwrap(),
            CheckpointSelector::Named(name) if name == "current"
        ));
        assert!(matches!(
            selector(None, true, "to").unwrap(),
            CheckpointSelector::Current
        ));
    }

    #[test]
    fn canonical_uuid_rejects_noncanonical_text() {
        assert!(canonical_uuid("not-an-id").is_err());
        assert!(canonical_uuid("00000000-0000-0000-0000-00000000000A").is_err());
        assert!(canonical_uuid("00000000-0000-0000-0000-000000000001").is_ok());
    }

    #[test]
    fn open_accepts_only_a_name_and_trailing_read_command() {
        let cli = Cli::try_parse_from([
            "gf",
            "--project",
            "/tmp/project",
            "checkpoint",
            "open",
            "before-change",
            "--",
            "MATCH",
            "(n)",
            "RETURN",
            "n",
        ])
        .expect("valid checkpoint read command");
        let Some(Command::Checkpoint {
            command: CheckpointCommand::Open(args),
        }) = cli.command
        else {
            panic!("expected checkpoint open command");
        };
        assert_eq!(args.name, "before-change");
        assert_eq!(args.query.join(" "), "MATCH (n) RETURN n");
    }

    #[test]
    fn repository_commands_use_project_dir_and_explicit_remove_confirmation() {
        assert!(Cli::try_parse_from(["gf", "--project-dir", "/tmp/repo", "init"]).is_ok());
        assert!(
            Cli::try_parse_from(["gf", "--project-dir", "/tmp/repo", "config", "resolve"]).is_ok()
        );
        let cli = Cli::try_parse_from(["gf", "--project-dir", "/tmp/repo", "remove"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Remove(RemoveArgs { yes: false }))
        ));
    }

    #[test]
    fn export_requires_exactly_one_generation_selector() {
        let base = ["gf", "--project", "/tmp/project", "export"];
        assert!(
            Cli::try_parse_from(base.into_iter().chain(["--output", "/tmp/out.gfportable"]))
                .is_err()
        );
        assert!(
            Cli::try_parse_from(base.into_iter().chain([
                "--current",
                "--checkpoint",
                "before-change",
                "--output",
                "/tmp/out.gfportable",
            ]))
            .is_err()
        );
        assert!(
            Cli::try_parse_from(base.into_iter().chain([
                "--current",
                "--output",
                "/tmp/out.gfportable",
            ]))
            .is_ok()
        );
        assert!(
            Cli::try_parse_from(base.into_iter().chain([
                "--checkpoint",
                "before-change",
                "--output",
                "/tmp/out.gfportable",
            ]))
            .is_ok()
        );
    }
}
