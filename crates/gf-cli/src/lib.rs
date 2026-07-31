//! Reusable GraphForge command-line interface.
#![forbid(unsafe_code)]

use std::io::{self, Read, Write};
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

include!(concat!(env!("OUT_DIR"), "/project_skills.rs"));

const MAX_SKILL_MANIFEST_BYTES: u64 = 256 * 1024;
const MAX_SKILL_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_SKILL_BUNDLE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_SKILL_PATH_BYTES: usize = 1024;

fn project_skill_bundle() -> gf_api::SkillBundle<'static> {
    gf_api::SkillBundle {
        manifest: PROJECT_SKILL_MANIFEST,
        files: PROJECT_SKILL_FILES,
    }
}

struct OwnedSkillFile {
    path: String,
    bytes: Vec<u8>,
}

struct OwnedSkillBundle {
    manifest: Vec<u8>,
    files: Vec<OwnedSkillFile>,
}

fn read_bounded_file(
    path: &Path,
    maximum: u64,
    message: &'static str,
) -> Result<Vec<u8>, gf_api::GfError> {
    let file =
        std::fs::File::open(path).map_err(|error| gf_api::GfError::Storage(error.to_string()))?;
    let metadata = file
        .metadata()
        .map_err(|error| gf_api::GfError::Storage(error.to_string()))?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(gf_api::GfError::Validation(message.into()));
    }
    let capacity =
        usize::try_from(metadata.len()).map_err(|_| gf_api::GfError::Validation(message.into()))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| gf_api::GfError::Storage(error.to_string()))?;
    if bytes.len() as u64 > maximum {
        return Err(gf_api::GfError::Validation(message.into()));
    }
    Ok(bytes)
}

fn load_skill_file(
    root: &Path,
    entry: &serde_json::Value,
) -> Result<(String, PathBuf, u64), gf_api::GfError> {
    let path = entry
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| gf_api::GfError::Validation("project skill file path is required".into()))?;
    if path.is_empty() || path.len() > MAX_SKILL_PATH_BYTES {
        return Err(gf_api::GfError::Validation(
            "project skill file path exceeds byte bound".into(),
        ));
    }
    let relative = Path::new(path);
    if relative.is_absolute()
        || relative
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(gf_api::GfError::Validation(
            "project skill file path is not contained".into(),
        ));
    }
    let mut candidate = root.to_path_buf();
    for component in relative.components() {
        candidate.push(component);
        let component_metadata = std::fs::symlink_metadata(&candidate)
            .map_err(|error| gf_api::GfError::Storage(error.to_string()))?;
        if component_metadata.file_type().is_symlink() {
            return Err(gf_api::GfError::Validation(
                "packaged project skill paths must not contain symlinks".into(),
            ));
        }
    }
    let file_metadata = std::fs::symlink_metadata(&candidate)
        .map_err(|error| gf_api::GfError::Storage(error.to_string()))?;
    if file_metadata.file_type().is_symlink() || !file_metadata.is_file() {
        return Err(gf_api::GfError::Validation(
            "packaged project skill must be a real file".into(),
        ));
    }
    if file_metadata.len() > MAX_SKILL_FILE_BYTES {
        return Err(gf_api::GfError::Validation(
            "packaged project skill exceeds per-file byte bound".into(),
        ));
    }
    let canonical = candidate
        .canonicalize()
        .map_err(|error| gf_api::GfError::Storage(error.to_string()))?;
    if !canonical.starts_with(root) {
        return Err(gf_api::GfError::Validation(
            "project skill file path escaped its bundle".into(),
        ));
    }
    Ok((path.to_owned(), canonical, file_metadata.len()))
}

fn load_skill_bundle(root: &Path) -> Result<OwnedSkillBundle, gf_api::GfError> {
    let metadata = std::fs::symlink_metadata(root)
        .map_err(|error| gf_api::GfError::Storage(error.to_string()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(gf_api::GfError::Validation(
            "packaged skill bundle root must be a real directory".into(),
        ));
    }
    let root = root
        .canonicalize()
        .map_err(|error| gf_api::GfError::Storage(error.to_string()))?;
    let manifest_path = root.join("manifest.json");
    let manifest_metadata = std::fs::symlink_metadata(&manifest_path)
        .map_err(|error| gf_api::GfError::Storage(error.to_string()))?;
    if manifest_metadata.file_type().is_symlink() || !manifest_metadata.is_file() {
        return Err(gf_api::GfError::Validation(
            "packaged skill manifest must be a real file".into(),
        ));
    }
    if manifest_metadata.len() > MAX_SKILL_MANIFEST_BYTES {
        return Err(gf_api::GfError::Validation(
            "packaged skill manifest exceeds byte bound".into(),
        ));
    }
    let manifest = read_bounded_file(
        &manifest_path,
        MAX_SKILL_MANIFEST_BYTES,
        "packaged skill manifest exceeds byte bound",
    )?;
    let value: serde_json::Value = serde_json::from_slice(&manifest).map_err(|error| {
        gf_api::GfError::Validation(format!("invalid project skill manifest: {error}"))
    })?;
    let entries = value
        .get("files")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            gf_api::GfError::Validation("project skill manifest files are required".into())
        })?;
    if entries.len() > 256 {
        return Err(gf_api::GfError::Validation(
            "project skill bundle exceeds file bound".into(),
        ));
    }
    let mut prepared = Vec::with_capacity(entries.len());
    let mut total_bytes = 0_u64;
    for entry in entries {
        let file = load_skill_file(&root, entry)?;
        total_bytes = total_bytes.checked_add(file.2).ok_or_else(|| {
            gf_api::GfError::Validation(
                "packaged project skill bundle exceeds total byte bound".into(),
            )
        })?;
        if total_bytes > MAX_SKILL_BUNDLE_BYTES {
            return Err(gf_api::GfError::Validation(
                "packaged project skill bundle exceeds total byte bound".into(),
            ));
        }
        prepared.push(file);
    }
    let mut files = Vec::with_capacity(prepared.len());
    for (path, canonical, _) in prepared {
        files.push(OwnedSkillFile {
            path,
            bytes: read_bounded_file(
                &canonical,
                MAX_SKILL_FILE_BYTES,
                "packaged project skill exceeds per-file byte bound",
            )?,
        });
    }
    Ok(OwnedSkillBundle { manifest, files })
}

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

    /// Internal packaged project-skill asset root used by distribution wrappers.
    #[arg(long, global = true, hide = true)]
    skills_bundle_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize repository-local GraphForge definitions and state.
    Init(InitArgs),
    /// Validate declared definitions and source digests without ingesting data.
    Sync,
    /// Remove only repository-local runtime state.
    Remove(RemoveArgs),
    /// Validate or resolve repository configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Manage project-local GraphForge agent skills.
    Skills {
        #[command(subcommand)]
        command: SkillsCommand,
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
struct InitArgs {
    /// Do not install missing project-local GraphForge agent skills.
    #[arg(long)]
    no_skills: bool,
}

#[derive(Subcommand)]
enum SkillsCommand {
    /// Install the packaged project-local skills.
    Install(SkillMutationArgs),
    /// Inspect managed skill provenance and user edits.
    Status,
    /// Update managed skills to the packaged bundle.
    Update(SkillMutationArgs),
    /// Remove only managed project-local skills.
    Remove(SkillMutationArgs),
}

#[derive(Args)]
struct SkillMutationArgs {
    /// Explicitly resolve conflicts by replacing edited managed files.
    #[arg(long)]
    force: bool,
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
    skill_bundle: &gf_api::SkillBundle<'_>,
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
        Command::Init(args) => {
            let init = repository.init_without_skills()?;
            let skills = if args.no_skills {
                None
            } else {
                Some(repository.skills_install(skill_bundle, false)?)
            };
            serde_json::to_value(serde_json::json!({
                "root": init.root,
                "created_config": init.created_config,
                "ignore_changed": init.ignore_changed,
                "state": init.state,
                "skills": skills
            }))
        }
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
        Command::Skills {
            command: SkillsCommand::Install(args),
        } => serde_json::to_value(repository.skills_install(skill_bundle, args.force)?),
        Command::Skills {
            command: SkillsCommand::Status,
        } => serde_json::to_value(repository.skills_status(skill_bundle)?),
        Command::Skills {
            command: SkillsCommand::Update(args),
        } => serde_json::to_value(repository.skills_update(skill_bundle, args.force)?),
        Command::Skills {
            command: SkillsCommand::Remove(args),
        } => serde_json::to_value(repository.skills_remove(args.force)?),
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

fn run_repository_with_bundle(
    command: Command,
    project_dir: Option<PathBuf>,
    json: bool,
    bundle_dir: Option<&Path>,
    output: &mut dyn Write,
) -> Result<(), gf_api::GfError> {
    let needs_packaged_bundle = matches!(&command, Command::Init(args) if !args.no_skills)
        || matches!(
            &command,
            Command::Skills {
                command: SkillsCommand::Install(_)
                    | SkillsCommand::Status
                    | SkillsCommand::Update(_)
            }
        );
    if !needs_packaged_bundle {
        return run_repository(command, project_dir, json, &project_skill_bundle(), output);
    }
    let owned_bundle = bundle_dir.map(load_skill_bundle).transpose()?;
    let owned_files = owned_bundle.as_ref().map(|bundle| {
        bundle
            .files
            .iter()
            .map(|file| gf_api::SkillBundleFile {
                path: &file.path,
                bytes: &file.bytes,
            })
            .collect::<Vec<_>>()
    });
    let embedded = project_skill_bundle();
    let skill_bundle =
        owned_bundle
            .as_ref()
            .zip(owned_files.as_ref())
            .map_or(embedded, |(bundle, files)| gf_api::SkillBundle {
                manifest: &bundle.manifest,
                files,
            });
    run_repository(command, project_dir, json, &skill_bundle, output)
}

fn resolve_project_path(
    project: Option<PathBuf>,
    project_dir: Option<PathBuf>,
) -> Result<PathBuf, gf_api::GfError> {
    match project {
        Some(path) => Ok(path),
        None => Ok(RepositoryContext::discover(project_dir.unwrap_or(
            std::env::current_dir().map_err(|error| gf_api::GfError::Storage(error.to_string()))?,
        ))?
        .state_path),
    }
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
        Command::Init(_)
            | Command::Sync
            | Command::Remove(_)
            | Command::Config { .. }
            | Command::Skills { .. }
    ) {
        return run_repository_with_bundle(
            command,
            cli.project_dir,
            cli.json,
            cli.skills_bundle_dir.as_deref(),
            output,
        );
    }
    let path = resolve_project_path(cli.project, cli.project_dir)?;
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
    use std::fs;
    use tempfile::tempdir;

    fn write_loader_manifest(root: &Path, paths: &[String]) {
        fs::write(
            root.join("manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "files": paths.iter().map(|path| serde_json::json!({"path": path})).collect::<Vec<_>>()
            }))
            .unwrap(),
        )
        .unwrap();
    }

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

    #[cfg(unix)]
    #[test]
    fn packaged_skill_loader_rejects_a_manifest_symlink() {
        let root = tempdir().unwrap();
        fs::write(root.path().join("real-manifest.json"), br#"{"files":[]}"#).unwrap();
        std::os::unix::fs::symlink(
            root.path().join("real-manifest.json"),
            root.path().join("manifest.json"),
        )
        .unwrap();
        let error = load_skill_bundle(root.path()).err().unwrap();
        assert!(
            error
                .to_string()
                .contains("packaged skill manifest must be a real file")
        );
    }

    #[test]
    fn packaged_skill_loader_rejects_non_file_and_oversized_manifests() {
        let root = tempdir().unwrap();
        fs::create_dir(root.path().join("manifest.json")).unwrap();
        assert!(
            load_skill_bundle(root.path())
                .err()
                .unwrap()
                .to_string()
                .contains("packaged skill manifest must be a real file")
        );

        let root = tempdir().unwrap();
        let manifest = fs::File::create(root.path().join("manifest.json")).unwrap();
        manifest.set_len(MAX_SKILL_MANIFEST_BYTES + 1).unwrap();
        assert!(
            load_skill_bundle(root.path())
                .err()
                .unwrap()
                .to_string()
                .contains("packaged skill manifest exceeds byte bound")
        );
    }

    #[test]
    fn packaged_skill_loader_bounds_paths_files_and_total_payload() {
        let root = tempdir().unwrap();
        write_loader_manifest(root.path(), &["x".repeat(MAX_SKILL_PATH_BYTES + 1)]);
        assert!(
            load_skill_bundle(root.path())
                .err()
                .unwrap()
                .to_string()
                .contains("project skill file path exceeds byte bound")
        );

        let root = tempdir().unwrap();
        let path = "graphforge-bootstrap/large.md".to_owned();
        fs::create_dir(root.path().join("graphforge-bootstrap")).unwrap();
        fs::File::create(root.path().join(&path))
            .unwrap()
            .set_len(MAX_SKILL_FILE_BYTES + 1)
            .unwrap();
        write_loader_manifest(root.path(), std::slice::from_ref(&path));
        assert!(
            load_skill_bundle(root.path())
                .err()
                .unwrap()
                .to_string()
                .contains("packaged project skill exceeds per-file byte bound")
        );

        let root = tempdir().unwrap();
        fs::create_dir(root.path().join("graphforge-bootstrap")).unwrap();
        let paths = (0..3)
            .map(|index| format!("graphforge-bootstrap/{index}.md"))
            .collect::<Vec<_>>();
        for path in &paths {
            fs::File::create(root.path().join(path))
                .unwrap()
                .set_len(3 * 1024 * 1024)
                .unwrap();
        }
        write_loader_manifest(root.path(), &paths);
        assert!(
            load_skill_bundle(root.path())
                .err()
                .unwrap()
                .to_string()
                .contains("packaged project skill bundle exceeds total byte bound")
        );
    }

    #[test]
    fn no_skills_init_does_not_load_the_distribution_bundle() {
        let project = tempdir().unwrap();
        let missing_bundle = project.path().join("missing-bundle");
        let result = execute([
            "graphforge".to_owned(),
            "--project-dir".to_owned(),
            project.path().to_string_lossy().into_owned(),
            "--skills-bundle-dir".to_owned(),
            missing_bundle.to_string_lossy().into_owned(),
            "init".to_owned(),
            "--no-skills".to_owned(),
        ]);
        assert_eq!(
            result.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(!project.path().join(".agents").exists());
        let validate = execute([
            "graphforge".to_owned(),
            "--project-dir".to_owned(),
            project.path().to_string_lossy().into_owned(),
            "--skills-bundle-dir".to_owned(),
            missing_bundle.to_string_lossy().into_owned(),
            "config".to_owned(),
            "validate".to_owned(),
        ]);
        assert_eq!(
            validate.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&validate.stderr)
        );

        let remove = execute([
            "graphforge".to_owned(),
            "--project-dir".to_owned(),
            project.path().to_string_lossy().into_owned(),
            "--skills-bundle-dir".to_owned(),
            missing_bundle.to_string_lossy().into_owned(),
            "skills".to_owned(),
            "remove".to_owned(),
        ]);
        assert_eq!(
            remove.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&remove.stderr)
        );
    }
}
