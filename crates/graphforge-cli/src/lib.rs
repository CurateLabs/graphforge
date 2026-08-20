//! Reusable GraphForge command-line interface.
#![forbid(unsafe_code)]

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use arrow::datatypes::DataType;
use arrow::ipc::writer::StreamWriter;
use clap::{Args, Parser, Subcommand, ValueEnum, error::ErrorKind};
use graphforge_api::{
    CheckpointDiffDetail, CheckpointDiffScope, CheckpointRequest, CheckpointSelector,
    DeleteCheckpointRequest, DiffCheckpointsRequest, ExecutionResult, GraphForge,
    ListCheckpointsRequest, OperationId, PageRequest, PageToken, PortableExportRequest,
    PortableExportResult, PortableImportRequest, PortableImportResult, PortableSelection,
    PreviewRevertCheckpointRequest, RepositoryContext, RepositorySyncRequest, RepositorySyncStatus,
    RevertCheckpointPreview, RevertCheckpointRequest, ShowCheckpointRequest,
};
use uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/project_skills.rs"));

mod maintenance_cli;
mod portable_cli;

const MAX_SKILL_MANIFEST_BYTES: u64 = 256 * 1024;
const MAX_SKILL_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_SKILL_BUNDLE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_SKILL_BUNDLE_FILES: usize = 256;
const MAX_SKILL_PATH_BYTES: usize = 1024;
const OUT_OF_SYNC_EXIT_CODE: i32 = 4;

fn project_skill_bundle() -> graphforge_api::SkillBundle<'static> {
    graphforge_api::SkillBundle {
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
) -> Result<Vec<u8>, graphforge_api::GfError> {
    let file = std::fs::File::open(path)
        .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?;
    let metadata = file
        .metadata()
        .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(graphforge_api::GfError::Validation(message.into()));
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| graphforge_api::GfError::Validation(message.into()))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?;
    if bytes.len() as u64 > maximum {
        return Err(graphforge_api::GfError::Validation(message.into()));
    }
    Ok(bytes)
}

fn load_skill_file(
    root: &Path,
    entry: &serde_json::Value,
) -> Result<(String, PathBuf, u64), graphforge_api::GfError> {
    let path = entry
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            graphforge_api::GfError::Validation("project skill file path is required".into())
        })?;
    if path.is_empty() || path.len() > MAX_SKILL_PATH_BYTES {
        return Err(graphforge_api::GfError::Validation(
            "project skill file path exceeds byte bound".into(),
        ));
    }
    let relative = Path::new(path);
    if relative.is_absolute()
        || relative
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(graphforge_api::GfError::Validation(
            "project skill file path is not contained".into(),
        ));
    }
    let mut candidate = root.to_path_buf();
    for component in relative.components() {
        candidate.push(component);
        let component_metadata = std::fs::symlink_metadata(&candidate)
            .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?;
        if component_metadata.file_type().is_symlink() {
            return Err(graphforge_api::GfError::Validation(
                "packaged project skill paths must not contain symlinks".into(),
            ));
        }
    }
    let file_metadata = std::fs::symlink_metadata(&candidate)
        .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?;
    if file_metadata.file_type().is_symlink() || !file_metadata.is_file() {
        return Err(graphforge_api::GfError::Validation(
            "packaged project skill must be a real file".into(),
        ));
    }
    if file_metadata.len() > MAX_SKILL_FILE_BYTES {
        return Err(graphforge_api::GfError::Validation(
            "packaged project skill exceeds per-file byte bound".into(),
        ));
    }
    let canonical = candidate
        .canonicalize()
        .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?;
    if !canonical.starts_with(root) {
        return Err(graphforge_api::GfError::Validation(
            "project skill file path escaped its bundle".into(),
        ));
    }
    Ok((path.to_owned(), canonical, file_metadata.len()))
}

fn load_skill_bundle(root: &Path) -> Result<OwnedSkillBundle, graphforge_api::GfError> {
    let metadata = std::fs::symlink_metadata(root)
        .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(graphforge_api::GfError::Validation(
            "packaged skill bundle root must be a real directory".into(),
        ));
    }
    let root = root
        .canonicalize()
        .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?;
    let manifest_path = root.join("manifest.json");
    let manifest_metadata = std::fs::symlink_metadata(&manifest_path)
        .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?;
    if manifest_metadata.file_type().is_symlink() || !manifest_metadata.is_file() {
        return Err(graphforge_api::GfError::Validation(
            "packaged skill manifest must be a real file".into(),
        ));
    }
    if manifest_metadata.len() > MAX_SKILL_MANIFEST_BYTES {
        return Err(graphforge_api::GfError::Validation(
            "packaged skill manifest exceeds byte bound".into(),
        ));
    }
    let manifest = read_bounded_file(
        &manifest_path,
        MAX_SKILL_MANIFEST_BYTES,
        "packaged skill manifest exceeds byte bound",
    )?;
    let value: serde_json::Value = serde_json::from_slice(&manifest).map_err(|error| {
        graphforge_api::GfError::Validation(format!("invalid project skill manifest: {error}"))
    })?;
    let entries = value
        .get("files")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            graphforge_api::GfError::Validation("project skill manifest files are required".into())
        })?;
    if entries.len() > MAX_SKILL_BUNDLE_FILES {
        return Err(graphforge_api::GfError::Validation(
            "project skill bundle exceeds file bound".into(),
        ));
    }
    let mut prepared = Vec::with_capacity(entries.len());
    let mut total_bytes = 0_u64;
    for entry in entries {
        let file = load_skill_file(&root, entry)?;
        total_bytes = total_bytes.checked_add(file.2).ok_or_else(|| {
            graphforge_api::GfError::Validation(
                "packaged project skill bundle exceeds total byte bound".into(),
            )
        })?;
        if total_bytes > MAX_SKILL_BUNDLE_BYTES {
            return Err(graphforge_api::GfError::Validation(
                "packaged project skill bundle exceeds total byte bound".into(),
            ));
        }
        prepared.push(file);
    }
    let mut files = Vec::with_capacity(prepared.len());
    let mut read_bytes = 0_u64;
    for (path, canonical, _) in prepared {
        let bytes = read_bounded_file(
            &canonical,
            MAX_SKILL_FILE_BYTES,
            "packaged project skill exceeds per-file byte bound",
        )?;
        read_bytes = read_bytes
            .checked_add(bytes.len() as u64)
            .filter(|total| *total <= MAX_SKILL_BUNDLE_BYTES)
            .ok_or_else(|| {
                graphforge_api::GfError::Validation(
                    "packaged project skill bundle exceeds total byte bound".into(),
                )
            })?;
        files.push(OwnedSkillFile { path, bytes });
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

    /// Emit the stable machine-readable JSON result when supported.
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
    /// Compare or reconcile declared definitions and source digests without ingesting data.
    Sync(SyncArgs),
    /// Remove only repository-local runtime state.
    Remove(RemoveArgs),
    /// Validate or resolve repository configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Validate provider-neutral infrastructure intent without provisioning.
    Infra {
        #[command(subcommand)]
        command: InfraCommand,
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
    /// Portable-v2 preview, export, verify, import, and OCI promotion.
    Portable {
        #[command(subcommand)]
        command: portable_cli::PortableCommand,
    },
    /// Stream a Cypher result to Parquet or Arrow IPC without full materialization.
    Query(portable_cli::QueryArgs),
    /// Staged Arrow/Parquet graph-import sessions.
    ImportSession {
        #[command(subcommand)]
        command: portable_cli::ImportSessionCommand,
    },
    /// Manage immutable named workspace checkpoints.
    Checkpoint {
        #[command(subcommand)]
        command: CheckpointCommand,
    },
    /// Explicit multi-mutation transactions (Rust-owned lifecycle).
    Transaction {
        #[command(subcommand)]
        command: maintenance_cli::TransactionCommand,
    },
    /// Retention/GC and graph-delta compaction maintenance.
    Maintenance {
        #[command(subcommand)]
        command: maintenance_cli::MaintenanceCommand,
    },
    /// Emit safe recovery-on-open evidence for the project.
    Recovery,
}

#[derive(Args)]
struct InitArgs {
    /// Do not install missing project-local GraphForge agent skills.
    #[arg(long)]
    no_skills: bool,
}

#[derive(Args)]
struct SyncArgs {
    /// Compare declared repository state without publishing a generation.
    #[arg(long, conflicts_with_all = ["idempotency_key", "actor_uuid"])]
    check: bool,
    /// Caller-owned operation identity, required only when applying drift.
    #[arg(long)]
    idempotency_key: Option<String>,
    /// Optional caller-owned actor identity for a published snapshot.
    #[arg(long, requires = "idempotency_key")]
    actor_uuid: Option<String>,
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
enum InfraCommand {
    /// Validate one named target without network access or mutation.
    Validate(InfraValidateArgs),
}

#[derive(Args)]
struct InfraValidateArgs {
    /// Stable target identifier from graphforge.yaml.
    #[arg(long)]
    target: String,
}

#[derive(Subcommand)]
enum CheckpointCommand {
    /// Create a checkpoint of the current complete workspace.
    Create(CreateArgs),
    /// List active checkpoints in canonical order.
    List(PageArgs),
    /// Show authoritative metadata for one active checkpoint.
    Show(ShowArgs),
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
struct ShowArgs {
    name: String,
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
    #[arg(long, required_unless_present = "preview")]
    reason: Option<String>,
    /// Required canonical UUID used for idempotent replay.
    #[arg(long, required_unless_present = "preview")]
    idempotency_key: Option<String>,
    #[arg(long)]
    actor_uuid: Option<String>,
    /// Explicitly authorize this destructive operation in automation.
    #[arg(long, conflicts_with = "preview")]
    yes: bool,
    /// Inspect the checkpoint and current generation without mutation.
    #[arg(long)]
    preview: bool,
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

pub(crate) fn canonical_uuid(value: &str) -> Result<Uuid, graphforge_api::GfError> {
    let parsed = Uuid::parse_str(value)
        .map_err(|_| graphforge_api::GfError::Validation("expected canonical UUID".into()))?;
    if parsed.hyphenated().to_string() != value {
        return Err(graphforge_api::GfError::Validation(
            "expected canonical lowercase hyphenated UUID".into(),
        ));
    }
    Ok(parsed)
}

fn actor(value: Option<&str>) -> Result<Option<Uuid>, graphforge_api::GfError> {
    value.map(canonical_uuid).transpose()
}

fn page(args: &PageArgs) -> Result<PageRequest, graphforge_api::GfError> {
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
) -> Result<CheckpointSelector, graphforge_api::GfError> {
    match (value, current) {
        (Some(name), false) => Ok(CheckpointSelector::Named(name)),
        (None, true) => Ok(CheckpointSelector::Current),
        _ => Err(graphforge_api::GfError::Validation(format!(
            "exactly one of --{endpoint} or --{endpoint}-current is required"
        ))),
    }
}

fn write_result(
    result: &ExecutionResult,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    {
        let mut writer = StreamWriter::try_new(&mut *output, result.schema.as_ref())
            .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
        for batch in &result.batches {
            writer
                .write(batch)
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
        }
        writer
            .finish()
            .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    }
    output
        .flush()
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))
}

#[derive(serde::Serialize)]
struct JsonResultColumn<'a> {
    name: &'a str,
    data_type: String,
    nullable: bool,
}

#[derive(serde::Serialize)]
struct JsonExecutionResult<'a> {
    contract: &'static str,
    columns: Vec<JsonResultColumn<'a>>,
    metadata: std::collections::BTreeMap<&'a str, &'a str>,
    rows: Vec<Vec<serde_json::Value>>,
}

fn canonical_json_value(
    value: serde_json::Value,
    data_type: &DataType,
) -> Result<serde_json::Value, graphforge_api::GfError> {
    match (value, data_type) {
        (serde_json::Value::String(hex), DataType::FixedSizeBinary(16)) => {
            if hex.len() != 32 {
                return Err(graphforge_api::GfError::Execution(
                    "invalid 16-byte JSON result value".into(),
                ));
            }
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|index| u8::from_str_radix(&hex[index..index + 2], 16))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
            Ok(serde_json::Value::String(
                Uuid::from_slice(&bytes)
                    .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?
                    .hyphenated()
                    .to_string(),
            ))
        }
        (value, _) => Ok(value),
    }
}

fn write_json_result(
    result: &ExecutionResult,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let mut encoded = arrow::json::ArrayWriter::new(Vec::new());
    let batches = result.batches.iter().collect::<Vec<_>>();
    encoded
        .write_batches(&batches)
        .and_then(|()| encoded.finish())
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    let objects: Vec<serde_json::Map<String, serde_json::Value>> =
        serde_json::from_slice(&encoded.into_inner())
            .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    let fields = result.schema.fields();
    let mut rows = Vec::with_capacity(objects.len());
    for mut object in objects {
        let mut row = Vec::with_capacity(fields.len());
        for field in fields {
            let value = object
                .remove(field.name())
                .unwrap_or(serde_json::Value::Null);
            row.push(canonical_json_value(value, field.data_type())?);
        }
        rows.push(row);
    }
    let value = JsonExecutionResult {
        contract: "graphforge-cli-result/1",
        columns: fields
            .iter()
            .map(|field| JsonResultColumn {
                name: field.name(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
            })
            .collect(),
        metadata: result
            .schema
            .metadata()
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect(),
        rows,
    };
    serde_json::to_writer(&mut *output, &value)
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    writeln!(output).map_err(|error| graphforge_api::GfError::Execution(error.to_string()))
}

fn write_execution_result(
    result: &ExecutionResult,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if json {
        write_json_result(result, output)
    } else {
        write_result(result, output)
    }
}

fn write_export_result(
    result: &PortableExportResult,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if json {
        writeln!(
            output,
            "{}",
            serde_json::to_string(result)
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
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
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    }
    Ok(())
}

fn write_import_result(
    result: &PortableImportResult,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if json {
        writeln!(
            output,
            "{}",
            serde_json::to_string(result)
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
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
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    }
    Ok(())
}

fn run_import(
    args: ImportArgs,
    path: &Path,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
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
) -> Result<(), graphforge_api::GfError> {
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

#[derive(serde::Serialize)]
struct JsonRevertPreview<'a> {
    contract: &'static str,
    checkpoint_uuid: Uuid,
    source_generation_uuid: Uuid,
    source_manifest_sha256: &'a str,
    current_generation_uuid: Uuid,
}

fn write_revert_preview(
    preview: &RevertCheckpointPreview,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    if json {
        serde_json::to_writer(
            &mut *output,
            &JsonRevertPreview {
                contract: "graphforge-revert-preview/1",
                checkpoint_uuid: preview.checkpoint_uuid,
                source_generation_uuid: preview.source_generation_uuid,
                source_manifest_sha256: &preview.source_manifest_sha256,
                current_generation_uuid: preview.current_generation_uuid,
            },
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
        writeln!(output).map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    } else {
        writeln!(
            output,
            "checkpoint {} pins generation {} (sha256 {}); current generation is {}",
            preview.checkpoint_uuid,
            preview.source_generation_uuid,
            preview.source_manifest_sha256,
            preview.current_generation_uuid
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    }
    Ok(())
}

fn run_revert_preview(
    path: &Path,
    name: String,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let preview =
        GraphForge::preview_revert_to_checkpoint(path, PreviewRevertCheckpointRequest { name })?;
    write_revert_preview(&preview, json, output)
}

fn run_revert(
    graph: &mut GraphForge,
    args: RevertArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    debug_assert!(args.yes && !args.preview);
    let result = graph.revert_to_checkpoint(RevertCheckpointRequest {
        name: args.name,
        reason: args
            .reason
            .expect("clap requires --reason unless --preview is present"),
        idempotency_key: OperationId(canonical_uuid(
            args.idempotency_key
                .as_deref()
                .expect("clap requires --idempotency-key unless --preview is present"),
        )?),
        actor_uuid: actor(args.actor_uuid.as_deref())?,
    })?;
    write_execution_result(&result, json, output)
}

fn revert_args(command: &Command) -> Option<&RevertArgs> {
    match command {
        Command::Revert(args)
        | Command::Checkpoint {
            command: CheckpointCommand::Revert(args),
        } => Some(args),
        _ => None,
    }
}

fn handle_revert_before_open(
    command: &Command,
    path: &Path,
    json: bool,
    output: &mut dyn Write,
) -> Result<bool, graphforge_api::GfError> {
    let Some(args) = revert_args(command) else {
        return Ok(false);
    };
    if args.preview {
        run_revert_preview(path, args.name.clone(), json, output)?;
        return Ok(true);
    }
    if !args.yes {
        return Err(graphforge_api::GfError::Validation(
            "revert requires explicit confirmation with --yes".into(),
        ));
    }
    Ok(false)
}

fn run_repository(
    command: Command,
    project_dir: Option<PathBuf>,
    json: bool,
    skill_bundle: &graphforge_api::SkillBundle<'_>,
    output: &mut dyn Write,
) -> Result<i32, graphforge_api::GfError> {
    let start = project_dir.unwrap_or(
        std::env::current_dir()
            .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?,
    );
    let repository = RepositoryContext::discover(start)?;
    let always_json = matches!(
        &command,
        Command::Config {
            command: ConfigCommand::Resolve
        }
    );
    let mut exit_code = 0;
    let mut plain_output = "ok";
    let value = match command {
        Command::Init(args) => {
            let init = repository.init_without_skills()?;
            let skills = if args.no_skills {
                None
            } else {
                Some(repository.skills_install(skill_bundle, false)?)
            };
            serde_json::to_value(serde_json::json!({
                "root": ".",
                "created_config": init.created_config,
                "ignore_changed": init.ignore_changed,
                "state": ".graphforge/state",
                "skills": skills
            }))
        }
        Command::Sync(args) => {
            let result = repository.sync(RepositorySyncRequest {
                check: args.check,
                operation_uuid: args
                    .idempotency_key
                    .as_deref()
                    .map(canonical_uuid)
                    .transpose()?,
                actor_uuid: actor(args.actor_uuid.as_deref())?,
            })?;
            if args.check && result.status == RepositorySyncStatus::Drift {
                exit_code = OUT_OF_SYNC_EXIT_CODE;
            }
            plain_output = match result.status {
                RepositorySyncStatus::InSync => "in_sync",
                RepositorySyncStatus::Drift => "drift",
                RepositorySyncStatus::Published => "published",
            };
            serde_json::to_value(result)
        }
        Command::Remove(args) => {
            let receipt = repository.remove(args.yes)?;
            serde_json::to_value(serde_json::json!({
                "target": ".graphforge/state",
                "removed": receipt.removed
            }))
        }
        Command::Config {
            command: ConfigCommand::Validate,
        } => {
            repository.load_config()?;
            Ok(serde_json::json!({"valid": true}))
        }
        Command::Config {
            command: ConfigCommand::Resolve,
        } => Ok(repository.resolve_config()?),
        Command::Infra {
            command: InfraCommand::Validate(args),
        } => {
            plain_output = "valid";
            serde_json::to_value(repository.validate_infra_target(&args.target)?)
        }
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
    .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    if json || always_json {
        writeln!(
            output,
            "{}",
            serde_json::to_string(&value)
                .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?
        )
        .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    } else {
        writeln!(output, "{plain_output}")
            .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    }
    Ok(exit_code)
}

fn run_repository_with_bundle(
    command: Command,
    project_dir: Option<PathBuf>,
    json: bool,
    bundle_dir: Option<&Path>,
    output: &mut dyn Write,
) -> Result<i32, graphforge_api::GfError> {
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
            .map(|file| graphforge_api::SkillBundleFile {
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
            .map_or(embedded, |(bundle, files)| graphforge_api::SkillBundle {
                manifest: &bundle.manifest,
                files,
            });
    run_repository(command, project_dir, json, &skill_bundle, output)
}

fn resolve_project_path(
    project: Option<PathBuf>,
    project_dir: Option<PathBuf>,
) -> Result<PathBuf, graphforge_api::GfError> {
    match project {
        Some(path) => Ok(path),
        None => Ok(RepositoryContext::discover(
            project_dir.unwrap_or(
                std::env::current_dir()
                    .map_err(|error| graphforge_api::GfError::Storage(error.to_string()))?,
            ),
        )?
        .state_path),
    }
}

fn is_repository_command(command: &Command) -> bool {
    matches!(
        command,
        Command::Init(_)
            | Command::Sync(_)
            | Command::Remove(_)
            | Command::Config { .. }
            | Command::Infra { .. }
            | Command::Skills { .. }
    )
}

#[allow(clippy::too_many_lines)] // CLI command dispatch table
fn run(cli: Cli, output: &mut dyn Write) -> Result<i32, graphforge_api::GfError> {
    if cli.info {
        writeln!(output, "graphforge {}", env!("CARGO_PKG_VERSION"))
            .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
        return Ok(0);
    }
    let Some(command) = cli.command else {
        writeln!(output, "GraphForge — use --help for options")
            .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
        return Ok(0);
    };
    if is_repository_command(&command) {
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
        Command::Import(args) => return run_import(args, &path, cli.json, output).map(|()| 0),
        command => command,
    };
    if handle_revert_before_open(&command, &path, cli.json, output)? {
        return Ok(0);
    }
    // Project-free portable ops must run before GraphForge::new so import/OCI
    // do not contend with the live project-root writer lock.
    if let Command::Portable { command } = command {
        return match command {
            portable_cli::PortableCommand::Verify(_)
            | portable_cli::PortableCommand::Import(_)
            | portable_cli::PortableCommand::PublishOci(_)
            | portable_cli::PortableCommand::PullOci(_) => {
                portable_cli::run_portable_without_graph(&path, command, cli.json, output)
                    .map(|()| 0)
            }
            command => {
                let path_text = path.to_str().ok_or_else(|| {
                    graphforge_api::GfError::Validation("--project must be valid UTF-8".into())
                })?;
                let graph = GraphForge::new(Some(path_text))?;
                portable_cli::run_portable(&graph, &path, command, cli.json, output).map(|()| 0)
            }
        };
    }
    let path_text = path.to_str().ok_or_else(|| {
        graphforge_api::GfError::Validation("--project must be valid UTF-8".into())
    })?;
    let mut graph = GraphForge::new(Some(path_text))?;
    let command = match command {
        Command::Export(args) => return run_export(&graph, args, cli.json, output).map(|()| 0),
        Command::Query(args) => {
            return portable_cli::run_query(&graph, &args, cli.json, output).map(|()| 0);
        }
        Command::ImportSession { command } => {
            return portable_cli::run_import_session(&graph, command, cli.json, output).map(|()| 0);
        }
        Command::Recovery => {
            return maintenance_cli::run_recovery(&graph, cli.json, output).map(|()| 0);
        }
        Command::Transaction { command } => {
            return maintenance_cli::run_transaction(&graph, command, cli.json, output).map(|()| 0);
        }
        Command::Maintenance { command } => {
            return maintenance_cli::run_maintenance(&graph, command, cli.json, output).map(|()| 0);
        }
        command => command,
    };
    let command = match command {
        Command::Revert(args)
        | Command::Checkpoint {
            command: CheckpointCommand::Revert(args),
        } => return run_revert(&mut graph, args, cli.json, output).map(|()| 0),
        command => command,
    };
    let result = match command {
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
            CheckpointCommand::Show(args) => {
                graph.show_checkpoint(ShowCheckpointRequest { name: args.name })?
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
            CheckpointCommand::Revert(_) => unreachable!("revert handled before result dispatch"),
        },
        _ => unreachable!(),
    };
    write_execution_result(&result, cli.json, output)?;
    Ok(0)
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
                write_json_error(
                    &mut stderr,
                    "GF_VALIDATION",
                    &error.to_string(),
                    JsonErrorDetails {
                        source: "argument_parser",
                        kind: clap_error_kind(error.kind()),
                    },
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
    let exit_code = match run(cli, &mut stdout) {
        Ok(exit_code) => exit_code,
        Err(error) => {
            write_error(&error, json, &mut stderr).expect("writing to Vec cannot fail");
            error_exit_code(&error)
        }
    };
    CliExecution {
        exit_code,
        stdout,
        stderr,
    }
}

fn write_error(
    error: &graphforge_api::GfError,
    json: bool,
    output: &mut dyn Write,
) -> io::Result<()> {
    if json {
        write_json_error(
            output,
            error.code(),
            &error.to_string(),
            JsonErrorDetails {
                source: "runtime",
                kind: runtime_error_kind(error),
            },
        )
    } else {
        writeln!(output, "{}: {error}", error.code())
    }
}

#[derive(Clone, Copy)]
struct JsonErrorDetails {
    source: &'static str,
    kind: &'static str,
}

fn write_json_error(
    output: &mut dyn Write,
    code: &'static str,
    message: &str,
    details: JsonErrorDetails,
) -> io::Result<()> {
    let code = serde_json::to_string(code).map_err(io::Error::other)?;
    let message = serde_json::to_string(message).map_err(io::Error::other)?;
    let source = serde_json::to_string(details.source).map_err(io::Error::other)?;
    let kind = serde_json::to_string(details.kind).map_err(io::Error::other)?;
    writeln!(
        output,
        "{{\"error\":{{\"code\":{code},\"message\":{message},\"details\":{{\"source\":{source},\"kind\":{kind}}}}}}}"
    )
}

const fn clap_error_kind(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::InvalidValue => "invalid_value",
        ErrorKind::UnknownArgument => "unknown_argument",
        ErrorKind::InvalidSubcommand => "invalid_subcommand",
        ErrorKind::NoEquals => "missing_equals",
        ErrorKind::ValueValidation => "value_validation",
        ErrorKind::TooManyValues => "too_many_values",
        ErrorKind::TooFewValues => "too_few_values",
        ErrorKind::WrongNumberOfValues => "wrong_number_of_values",
        ErrorKind::ArgumentConflict => "argument_conflict",
        ErrorKind::MissingRequiredArgument => "missing_required_argument",
        ErrorKind::MissingSubcommand => "missing_subcommand",
        ErrorKind::DisplayHelp => "display_help",
        ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => "display_help_on_missing_input",
        ErrorKind::DisplayVersion => "display_version",
        ErrorKind::Io => "io",
        ErrorKind::Format => "format",
        _ => "other",
    }
}

const fn runtime_error_kind(error: &graphforge_api::GfError) -> &'static str {
    match error {
        graphforge_api::GfError::NotImplemented(_) => "not_implemented",
        graphforge_api::GfError::Parse { .. } => "parse",
        graphforge_api::GfError::Bind { .. } => "bind",
        graphforge_api::GfError::Plan(_) => "plan",
        graphforge_api::GfError::Execution(_) => "execution",
        graphforge_api::GfError::Provider { .. } => "provider",
        graphforge_api::GfError::Storage(_) => "storage",
        graphforge_api::GfError::Project { .. } => "project",
        graphforge_api::GfError::Api { .. } => "api",
        graphforge_api::GfError::Lifecycle(_) => "lifecycle",
        graphforge_api::GfError::Validation(_) => "validation",
        graphforge_api::GfError::Ontology(_) => "ontology",
    }
}

fn error_exit_code(error: &graphforge_api::GfError) -> i32 {
    match error {
        graphforge_api::GfError::Validation(_) => 2,
        graphforge_api::GfError::Storage(_) => 3,
        _ => 1,
    }
}

/// Stream process output directly and terminate with the native CLI exit status.
pub fn run_process() {
    let cli = Cli::parse();
    let json = cli.json;
    let stdout = io::stdout();
    let mut output = stdout.lock();
    match run(cli, &mut output) {
        Ok(exit_code) if exit_code != 0 => {
            let _ = output.flush();
            std::process::exit(exit_code);
        }
        Ok(_) => {}
        Err(error) => {
            let _ = output.flush();
            let stderr = io::stderr();
            write_error(&error, json, &mut stderr.lock()).expect("write CLI stderr");
            std::process::exit(error_exit_code(&error));
        }
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
    fn bounded_file_and_skill_entry_validation_fail_before_payload_use() {
        let root = tempdir().unwrap();
        let file = root.path().join("skill.md");
        fs::write(&file, b"abcd").unwrap();
        assert_eq!(read_bounded_file(&file, 4, "bounded").unwrap(), b"abcd");
        assert!(matches!(
            read_bounded_file(&file, 3, "bounded"),
            Err(graphforge_api::GfError::Validation(message)) if message == "bounded"
        ));
        assert!(matches!(
            read_bounded_file(root.path(), 100, "regular"),
            Err(graphforge_api::GfError::Validation(message)) if message == "regular"
        ));
        assert!(matches!(
            read_bounded_file(&root.path().join("absent"), 100, "missing"),
            Err(graphforge_api::GfError::Storage(_))
        ));

        assert!(matches!(
            load_skill_file(root.path(), &serde_json::json!({})),
            Err(graphforge_api::GfError::Validation(message))
                if message == "project skill file path is required"
        ));
        for path in ["", "../escape", "/absolute"] {
            assert!(matches!(
                load_skill_file(root.path(), &serde_json::json!({"path": path})),
                Err(graphforge_api::GfError::Validation(_))
            ));
        }
        assert!(matches!(
            load_skill_file(
                root.path(),
                &serde_json::json!({"path": "missing/skill.md"})
            ),
            Err(graphforge_api::GfError::Storage(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn wave11_skill_bundle_rejects_linked_components_nonfiles_and_entry_overflow() {
        use std::os::unix::fs::symlink;

        let external = tempdir().unwrap();
        fs::write(external.path().join("SKILL.md"), b"external").unwrap();
        let root = tempdir().unwrap();
        symlink(external.path(), root.path().join("linked")).unwrap();
        assert!(matches!(
            load_skill_file(
                root.path(),
                &serde_json::json!({"path": "linked/SKILL.md"})
            ),
            Err(graphforge_api::GfError::Validation(message))
                if message.contains("must not contain symlinks")
        ));
        assert_eq!(
            fs::read(external.path().join("SKILL.md")).unwrap(),
            b"external"
        );

        let root = tempdir().unwrap();
        fs::create_dir(root.path().join("directory.md")).unwrap();
        assert!(matches!(
            load_skill_file(
                root.path(),
                &serde_json::json!({"path": "directory.md"})
            ),
            Err(graphforge_api::GfError::Validation(message))
                if message.contains("must be a real file")
        ));

        let root = tempdir().unwrap();
        let paths = (0..=MAX_SKILL_BUNDLE_FILES)
            .map(|index| format!("skill-{index}.md"))
            .collect::<Vec<_>>();
        write_loader_manifest(root.path(), &paths);
        assert!(matches!(
            load_skill_bundle(root.path()),
            Err(graphforge_api::GfError::Validation(message))
                if message.contains("exceeds file bound")
        ));

        let root = tempdir().unwrap();
        fs::create_dir(root.path().join("manifest.json")).unwrap();
        assert!(matches!(
            load_skill_bundle(root.path()),
            Err(graphforge_api::GfError::Validation(message))
                if message.contains("manifest must be a real file")
        ));

        let root = tempdir().unwrap();
        fs::File::create(root.path().join("manifest.json"))
            .unwrap()
            .set_len(MAX_SKILL_MANIFEST_BYTES + 1)
            .unwrap();
        assert!(matches!(
            load_skill_bundle(root.path()),
            Err(graphforge_api::GfError::Validation(message))
                if message.contains("manifest exceeds byte bound")
        ));
    }

    #[test]
    fn wave11_portable_and_preview_output_contracts_cover_plain_json_and_write_failure() {
        struct FailWrite;
        impl Write for FailWrite {
            fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("closed output"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let source = Uuid::from_u128(1);
        let generation = Uuid::from_u128(2);
        for replay in [false, true] {
            let result = PortableImportResult {
                contract: "graphforge-portable-import/1",
                source_generation_uuid: source,
                generation_uuid: generation,
                envelope_sha256: "00".repeat(32),
                idempotent_replay: replay,
            };
            let mut plain = Vec::new();
            write_import_result(&result, false, &mut plain).unwrap();
            let text = String::from_utf8(plain).unwrap();
            assert!(text.starts_with(if replay { "replayed" } else { "imported" }));
            let mut json = Vec::new();
            write_import_result(&result, true, &mut json).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&json).unwrap()["idempotent_replay"],
                replay
            );
            assert!(matches!(
                write_import_result(&result, replay, &mut FailWrite),
                Err(graphforge_api::GfError::Execution(_))
            ));
        }

        let preview = RevertCheckpointPreview {
            checkpoint_uuid: Uuid::from_u128(3),
            source_generation_uuid: source,
            source_manifest_sha256: "11".repeat(32),
            current_generation_uuid: generation,
        };
        for json in [false, true] {
            let mut output = Vec::new();
            write_revert_preview(&preview, json, &mut output).unwrap();
            assert!(!output.is_empty());
            assert!(matches!(
                write_revert_preview(&preview, json, &mut FailWrite),
                Err(graphforge_api::GfError::Execution(_))
            ));
        }
    }

    #[test]
    fn wave11_reusable_execution_separates_plain_parse_and_runtime_failures() {
        let parse = execute(["launcher", "unknown-command"]);
        assert_eq!(parse.exit_code, 2);
        assert!(parse.stdout.is_empty());
        assert!(
            String::from_utf8(parse.stderr)
                .unwrap()
                .contains("unrecognized subcommand")
        );

        let root = tempdir().unwrap();
        let runtime = execute([
            "launcher",
            "--project",
            root.path().to_str().unwrap(),
            "checkpoint",
            "create",
            "snapshot",
            "--idempotency-key",
            "not-a-uuid",
        ]);
        assert_ne!(runtime.exit_code, 0);
        assert!(runtime.stdout.is_empty());
        assert!(
            String::from_utf8(runtime.stderr)
                .unwrap()
                .starts_with("GF_")
        );
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
        assert_eq!(
            error["error"]["details"],
            serde_json::json!({
                "source": "argument_parser",
                "kind": "invalid_subcommand"
            })
        );
        assert!(
            String::from_utf8(invalid.stderr.clone())
                .unwrap()
                .starts_with("{\"error\":{\"code\":\"GF_VALIDATION\",\"message\":")
        );
        assert!(
            error["error"]["message"]
                .as_str()
                .expect("error message")
                .contains("Usage: graphforge")
        );
    }

    #[test]
    fn runtime_json_errors_include_stable_safe_details_without_changing_message() {
        let cases = [
            (
                graphforge_api::GfError::Validation("invalid repository input".into()),
                "GF_VALIDATION",
                "validation",
                2,
            ),
            (
                graphforge_api::GfError::Storage("unavailable".into()),
                "GF_IO",
                "storage",
                3,
            ),
        ];
        for (error, code, kind, exit_code) in cases {
            let message = error.to_string();
            let mut output = Vec::new();
            write_error(&error, true, &mut output).unwrap();
            let serialized = String::from_utf8(output.clone()).unwrap();
            assert!(
                serialized.starts_with(&format!("{{\"error\":{{\"code\":\"{code}\",\"message\":"))
            );
            assert!(
                serialized.find("\"message\":").unwrap() < serialized.find("\"details\":").unwrap()
            );
            let value: serde_json::Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(value["error"]["code"], code);
            assert_eq!(value["error"]["message"], message);
            assert_eq!(
                value["error"]["details"],
                serde_json::json!({"source": "runtime", "kind": kind})
            );
            assert_eq!(error_exit_code(&error), exit_code);
        }
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

        let preview = Cli::try_parse_from([
            "gf",
            "--project",
            "/tmp/project",
            "revert",
            "before-change",
            "--preview",
        ])
        .expect("preview does not require mutation identity");
        assert!(matches!(
            preview.command,
            Some(Command::Revert(RevertArgs { preview: true, .. }))
        ));
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
    fn show_accepts_exactly_one_checkpoint_name() {
        let cli = Cli::try_parse_from([
            "gf",
            "--project",
            "/tmp/project",
            "checkpoint",
            "show",
            "before-change",
        ])
        .expect("valid checkpoint show command");
        assert!(matches!(
            cli.command,
            Some(Command::Checkpoint {
                command: CheckpointCommand::Show(ShowArgs { name })
            }) if name == "before-change"
        ));
    }

    #[test]
    fn repository_commands_use_project_dir_and_explicit_remove_confirmation() {
        assert!(Cli::try_parse_from(["gf", "--project-dir", "/tmp/repo", "init"]).is_ok());
        assert!(
            Cli::try_parse_from(["gf", "--project-dir", "/tmp/repo", "config", "resolve"]).is_ok()
        );
        assert!(
            Cli::try_parse_from(["gf", "--project-dir", "/tmp/repo", "sync", "--check",]).is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "gf",
                "--project-dir",
                "/tmp/repo",
                "sync",
                "--check",
                "--idempotency-key",
                "41414141-4141-4141-4141-414141414141",
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "gf",
                "--project-dir",
                "/tmp/repo",
                "sync",
                "--actor-uuid",
                "42424242-4242-4242-4242-424242424242",
            ])
            .is_err()
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
    fn packaged_skill_loader_accepts_exact_manifest_and_rejects_malformed_shapes() {
        let root = tempdir().unwrap();
        fs::create_dir(root.path().join("graphforge-bootstrap")).unwrap();
        fs::write(root.path().join("graphforge-bootstrap/SKILL.md"), b"skill").unwrap();
        write_loader_manifest(root.path(), &["graphforge-bootstrap/SKILL.md".into()]);
        let loaded = load_skill_bundle(root.path()).unwrap();
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.files[0].path, "graphforge-bootstrap/SKILL.md");
        assert_eq!(loaded.files[0].bytes, b"skill");

        let root = tempdir().unwrap();
        fs::write(root.path().join("manifest.json"), b"not json").unwrap();
        assert!(matches!(
            load_skill_bundle(root.path()),
            Err(graphforge_api::GfError::Validation(_))
        ));

        let root = tempdir().unwrap();
        fs::write(root.path().join("manifest.json"), br#"{}"#).unwrap();
        assert!(matches!(
            load_skill_bundle(root.path()),
            Err(graphforge_api::GfError::Validation(_))
        ));

        let root = tempdir().unwrap();
        fs::write(root.path().join("manifest.json"), br#"{"files":[{}]}"#).unwrap();
        assert!(matches!(
            load_skill_bundle(root.path()),
            Err(graphforge_api::GfError::Validation(_))
        ));

        let root = tempdir().unwrap();
        fs::write(
            root.path().join("manifest.json"),
            br#"{"files":[{"path":"../escape"}]}"#,
        )
        .unwrap();
        assert!(matches!(
            load_skill_bundle(root.path()),
            Err(graphforge_api::GfError::Validation(_))
        ));

        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(matches!(
            load_skill_bundle(file.path()),
            Err(graphforge_api::GfError::Validation(_))
        ));
        assert!(matches!(
            read_bounded_file(root.path(), 10, "not a file"),
            Err(graphforge_api::GfError::Validation(_))
        ));
    }

    #[test]
    fn reusable_execution_covers_default_info_and_runtime_error_vocabulary() {
        let default = execute(std::iter::empty::<&str>());
        assert_eq!(default.exit_code, 0);
        assert_eq!(
            default.stdout,
            "GraphForge — use --help for options\n".as_bytes()
        );

        let info = execute(["graphforge", "--info"]);
        assert_eq!(info.exit_code, 0);
        assert!(
            String::from_utf8(info.stdout)
                .unwrap()
                .starts_with("graphforge ")
        );

        let cases = [
            (
                graphforge_api::GfError::NotImplemented("test"),
                "not_implemented",
            ),
            (graphforge_api::GfError::Plan("test".into()), "plan"),
            (
                graphforge_api::GfError::Execution("test".into()),
                "execution",
            ),
            (
                graphforge_api::GfError::Provider {
                    class: "transport".into(),
                    provider: "provider".into(),
                    model: "model".into(),
                },
                "provider",
            ),
            (
                graphforge_api::GfError::Lifecycle("test".into()),
                "lifecycle",
            ),
            (graphforge_api::GfError::Ontology("test".into()), "ontology"),
        ];
        for (error, expected) in cases {
            assert_eq!(runtime_error_kind(&error), expected);
            assert_eq!(error_exit_code(&error), 1);
            let mut output = Vec::new();
            write_error(&error, false, &mut output).unwrap();
            assert!(!output.is_empty());
        }
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

    #[test]
    fn repository_commands_execute_through_json_and_plain_dispatch() {
        let project = tempdir().unwrap();
        let root = project.path().to_string_lossy().into_owned();
        let init = execute([
            "graphforge".to_owned(),
            "--json".to_owned(),
            "--project-dir".to_owned(),
            root.clone(),
            "init".to_owned(),
            "--no-skills".to_owned(),
        ]);
        assert_eq!(
            init.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&init.stderr)
        );
        let init_json: serde_json::Value = serde_json::from_slice(&init.stdout).unwrap();
        assert_eq!(init_json["root"], ".");

        for tail in [
            vec!["config", "resolve"],
            vec!["config", "validate"],
            vec!["sync", "--check"],
        ] {
            let mut args = vec![
                "graphforge".to_owned(),
                "--json".to_owned(),
                "--project-dir".to_owned(),
                root.clone(),
            ];
            args.extend(tail.into_iter().map(str::to_owned));
            let result = execute(args);
            assert!(
                result.exit_code == 0 || result.exit_code == OUT_OF_SYNC_EXIT_CODE,
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            assert!(serde_json::from_slice::<serde_json::Value>(&result.stdout).is_ok());
        }

        let remove = execute([
            "graphforge".to_owned(),
            "--project-dir".to_owned(),
            root,
            "remove".to_owned(),
            "--yes".to_owned(),
        ]);
        assert_eq!(
            remove.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&remove.stderr)
        );
        assert_eq!(remove.stdout, b"ok\n");
    }

    #[test]
    fn checkpoint_commands_execute_end_to_end_through_the_reusable_cli() {
        let project = tempdir().unwrap();
        let path = project.path().join("state");
        fs::create_dir(&path).unwrap();
        let path = path.to_string_lossy().into_owned();
        let invoke = |tail: &[&str]| {
            let mut args = vec![
                "graphforge".to_owned(),
                "--json".to_owned(),
                "--project".to_owned(),
                path.clone(),
            ];
            args.extend(tail.iter().map(|value| (*value).to_owned()));
            execute(args)
        };
        let create_id = Uuid::now_v7().to_string();
        let create = invoke(&[
            "checkpoint",
            "create",
            "baseline",
            "--description",
            "before change",
            "--idempotency-key",
            &create_id,
        ]);
        assert_eq!(
            create.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&create.stderr)
        );

        for command in [
            vec!["checkpoint", "list", "--limit", "10"],
            vec!["checkpoint", "show", "baseline"],
            vec![
                "checkpoint",
                "open",
                "baseline",
                "--",
                "RETURN",
                "1",
                "AS",
                "value",
            ],
            vec![
                "checkpoint",
                "diff",
                "--from",
                "baseline",
                "--to-current",
                "--scope",
                "summary",
                "--detail",
                "summary",
            ],
            vec!["checkpoint", "revert", "baseline", "--preview"],
        ] {
            let result = invoke(&command);
            assert_eq!(
                result.exit_code,
                0,
                "command={command:?}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            assert!(!result.stdout.is_empty());
        }

        for scope in [
            "graph",
            "ontology",
            "configuration",
            "capabilities",
            "provenance",
            "knowledge",
            "epistemic",
            "all",
        ] {
            let result = invoke(&[
                "checkpoint",
                "diff",
                "--from",
                "baseline",
                "--to-current",
                "--scope",
                scope,
                "--detail",
                "records",
            ]);
            assert_eq!(
                result.exit_code,
                0,
                "scope={scope}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
        }

        let revert_id = Uuid::now_v7().to_string();
        let revert = invoke(&[
            "checkpoint",
            "revert",
            "baseline",
            "--reason",
            "coverage contract",
            "--idempotency-key",
            &revert_id,
            "--yes",
        ]);
        assert_eq!(
            revert.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&revert.stderr)
        );

        let envelope = project.path().join("baseline.gfportable");
        let envelope_text = envelope.to_string_lossy().into_owned();
        let export = invoke(&[
            "export",
            "--checkpoint",
            "baseline",
            "--output",
            &envelope_text,
        ]);
        assert_eq!(
            export.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&export.stderr)
        );
        assert!(envelope.is_file());

        let imported_path = project.path().join("imported");
        fs::create_dir(&imported_path).unwrap();
        let imported = imported_path.to_string_lossy().into_owned();
        let import_id = Uuid::now_v7().to_string();
        let import = execute([
            "graphforge".to_owned(),
            "--json".to_owned(),
            "--project".to_owned(),
            imported,
            "import".to_owned(),
            "--input".to_owned(),
            envelope_text,
            "--idempotency-key".to_owned(),
            import_id,
        ]);
        assert_eq!(
            import.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&import.stderr)
        );

        let delete_id = Uuid::now_v7().to_string();
        let delete = invoke(&[
            "checkpoint",
            "delete",
            "baseline",
            "--idempotency-key",
            &delete_id,
        ]);
        assert_eq!(
            delete.exit_code,
            0,
            "{}",
            String::from_utf8_lossy(&delete.stderr)
        );
    }

    #[test]
    fn clap_error_vocabulary_is_total_and_stable() {
        let cases = [
            (ErrorKind::InvalidValue, "invalid_value"),
            (ErrorKind::UnknownArgument, "unknown_argument"),
            (ErrorKind::InvalidSubcommand, "invalid_subcommand"),
            (ErrorKind::NoEquals, "missing_equals"),
            (ErrorKind::ValueValidation, "value_validation"),
            (ErrorKind::TooManyValues, "too_many_values"),
            (ErrorKind::TooFewValues, "too_few_values"),
            (ErrorKind::WrongNumberOfValues, "wrong_number_of_values"),
            (ErrorKind::ArgumentConflict, "argument_conflict"),
            (
                ErrorKind::MissingRequiredArgument,
                "missing_required_argument",
            ),
            (ErrorKind::MissingSubcommand, "missing_subcommand"),
            (ErrorKind::DisplayHelp, "display_help"),
            (
                ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand,
                "display_help_on_missing_input",
            ),
            (ErrorKind::DisplayVersion, "display_version"),
            (ErrorKind::Io, "io"),
            (ErrorKind::Format, "format"),
        ];
        for (kind, token) in cases {
            assert_eq!(clap_error_kind(kind), token);
        }
    }

    #[test]
    fn canonical_json_uuid_conversion_is_exact_and_rejects_malformed_hex() {
        let ty = DataType::FixedSizeBinary(16);
        assert_eq!(
            canonical_json_value(
                serde_json::Value::String("000102030405060708090a0b0c0d0e0f".into()),
                &ty,
            )
            .unwrap(),
            serde_json::Value::String("00010203-0405-0607-0809-0a0b0c0d0e0f".into())
        );
        for value in ["00", "zz0102030405060708090a0b0c0d0e0f"] {
            assert!(matches!(
                canonical_json_value(serde_json::Value::String(value.into()), &ty),
                Err(graphforge_api::GfError::Execution(_))
            ));
        }
        let ordinary = serde_json::json!({"nested": [true, 1, null]});
        assert_eq!(
            canonical_json_value(ordinary.clone(), &DataType::Utf8).unwrap(),
            ordinary
        );
    }

    #[test]
    fn selector_requires_exactly_one_source_and_preserves_named_current() {
        assert!(selector(None, false, "from").is_err());
        assert!(selector(Some("named".into()), true, "from").is_err());
        assert!(matches!(
            selector(Some("current".into()), false, "from").unwrap(),
            CheckpointSelector::Named(value) if value == "current"
        ));
        assert!(matches!(
            selector(None, true, "from").unwrap(),
            CheckpointSelector::Current
        ));
    }

    #[test]
    fn arrow_result_export_preserves_geoarrow_fields_values_and_nulls() {
        use std::collections::HashMap;
        use std::io::Cursor;

        use arrow::array::{
            Array, FixedSizeListArray, Float64Array, LargeListArray, ListArray, StructArray,
        };
        use arrow::ipc::reader::StreamReader;
        use graphforge_api::{PropValue, SpatialValue};

        fn flatten_value(array: &dyn Array, row: usize, output: &mut Vec<f64>) {
            if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
                output.push(values.value(row));
            } else if let Some(values) = array.as_any().downcast_ref::<StructArray>() {
                for column in values.columns() {
                    flatten_value(column.as_ref(), row, output);
                }
            } else if let Some(values) = array.as_any().downcast_ref::<ListArray>() {
                let values = values.value(row);
                for index in 0..values.len() {
                    flatten_value(values.as_ref(), index, output);
                }
            } else if let Some(values) = array.as_any().downcast_ref::<LargeListArray>() {
                let values = values.value(row);
                for index in 0..values.len() {
                    flatten_value(values.as_ref(), index, output);
                }
            } else if let Some(values) = array.as_any().downcast_ref::<FixedSizeListArray>() {
                let values = values.value(row);
                for index in 0..values.len() {
                    flatten_value(values.as_ref(), index, output);
                }
            } else {
                panic!(
                    "unexpected GeoArrow coordinate array: {:?}",
                    array.data_type()
                );
            }
        }

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/contracts/geoarrow-interchange-v1.json"
        ))
        .unwrap();
        let cases = fixture["cases"].as_array().unwrap();
        let properties = cases
            .iter()
            .map(|case| {
                let name = case["name"].as_str().unwrap().to_owned();
                let spatial: SpatialValue = serde_json::from_value(serde_json::json!({
                    "spatial_type": {
                        "geometry": case["geometry"],
                        "crs": case["crs"],
                    },
                    "coordinates": case["coordinates"],
                    "extension_name": case.get("preservedOnly").and_then(|value| value.as_bool()).unwrap_or(false).then(|| case["extensionName"].clone()),
                    "extension_metadata": case.get("preservedOnly").and_then(|value| value.as_bool()).unwrap_or(false).then(|| case["extensionMetadata"].clone()),
                }))
                .unwrap();
                (name, PropValue::Spatial(spatial))
            })
            .collect::<HashMap<_, _>>();
        let graph = GraphForge::new(None).unwrap();
        graph.add_node("Geometry", &properties).unwrap();
        graph.add_node("Geometry", &HashMap::new()).unwrap();
        let projection = cases
            .iter()
            .map(|case| {
                let name = case["name"].as_str().unwrap();
                format!("n.{name} AS {name}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let result = graph
            .execute(&format!("MATCH (n:Geometry) RETURN {projection}"))
            .unwrap();
        let mut ipc = Vec::new();
        write_result(&result, &mut ipc).unwrap();
        let batches = StreamReader::try_new(Cursor::new(ipc), None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let expected_batches = fixture["rows"]["batchSizes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_u64().unwrap() as usize)
            .collect::<Vec<_>>();
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.num_rows())
                .collect::<Vec<_>>(),
            expected_batches
        );
        assert_eq!(
            batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let field = batches[0].schema().field_with_name(name).unwrap().clone();
            assert_eq!(
                field.metadata()["ARROW:extension:name"],
                case["extensionName"]
            );
            assert_eq!(
                field.metadata()["ARROW:extension:metadata"],
                case["extensionMetadata"]
            );
            let column = batches[0].column_by_name(name).unwrap();
            let mut coordinates = Vec::new();
            flatten_value(
                column.as_ref(),
                fixture["rows"]["populated"].as_u64().unwrap() as usize,
                &mut coordinates,
            );
            let expected = case["flat"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_f64().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(coordinates, expected);
            assert!(column.is_null(fixture["rows"]["null"].as_u64().unwrap() as usize));
        }
    }
}
