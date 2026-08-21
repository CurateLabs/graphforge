//! Thin CLI projection of the Rust-owned composable multi-ontology facade (#842).

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand, ValueEnum};
use graphforge_api::{
    ActivationMode, ActivationProfileChangeRequest, ActivationRecord, BridgeAdoptionRequest,
    BridgeDeleteRequest, BridgeDocument, BridgeExportFormat, BridgeImportFormatHint,
    BridgeSelector, BridgeSetId, BridgeUpdateRequest, CancellationToken, CompositionChangeRequest,
    CompositionDataDisposition, GraphForge, ImportFormatHint, ModuleAdoptionRequest,
    ModuleDeleteRequest, ModuleSelector, ModuleUpdateRequest, OntologyAuthorityExpectation,
    OntologyDoc, OntologyModuleId, OperationId, PortableV2Limits, ResolutionExplainRequest,
    SymbolKind, WorkspaceOntologyComposition, WriteContext,
};
use serde::Deserialize;
use serde_json::json;

use crate::{CliRuntimeError, canonical_uuid};

const MAX_INPUT_BYTES: u64 = 16 * 1024 * 1024;

#[cfg(test)]
const CLI_SURFACE_PATHS: &[&str] = &[
    "ontology/module/list",
    "ontology/module/get",
    "ontology/module/inspect",
    "ontology/module/validate",
    "ontology/module/create",
    "ontology/module/import",
    "ontology/module/adopt",
    "ontology/module/preview-update",
    "ontology/module/update",
    "ontology/module/preview-delete",
    "ontology/module/delete",
    "ontology/module/export",
    "ontology/bridge/list",
    "ontology/bridge/get",
    "ontology/bridge/inspect",
    "ontology/bridge/validate",
    "ontology/bridge/create",
    "ontology/bridge/import",
    "ontology/bridge/adopt",
    "ontology/bridge/preview-update",
    "ontology/bridge/update",
    "ontology/bridge/preview-delete",
    "ontology/bridge/delete",
    "ontology/bridge/export",
    "ontology/activation/inspect",
    "ontology/activation/change",
    "ontology/composition/validate",
    "ontology/composition/preflight",
    "ontology/composition/explain-resolution",
    "portable/verify/inspect",
    "portable/verify/full",
    "portable/export",
    "portable/import",
    "portable/staging/inspect",
    "portable/staging/adopt",
];

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum OntologyCommand {
    /// Manage independently identified ontology modules.
    Module {
        #[command(subcommand)]
        command: ModuleCommand,
    },
    /// Manage provenance-bearing semantic bridge sets.
    Bridge {
        #[command(subcommand)]
        command: BridgeCommand,
    },
    /// Inspect or replace the complete activation profile.
    Activation {
        #[command(subcommand)]
        command: ActivationCommand,
    },
    /// Validate, preflight, or explain composed authority.
    Composition {
        #[command(subcommand)]
        command: CompositionCommand,
    },
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ModuleCommand {
    List,
    Get(ModuleSelectorArgs),
    Inspect(ModuleSelectorArgs),
    Validate(DocumentArgs),
    Create(ModuleCandidateArgs),
    Import(ModuleCandidateArgs),
    Adopt(ModuleMutationArgs),
    PreviewUpdate(ModulePreviewUpdateArgs),
    Update(ModuleUpdateArgs),
    PreviewDelete(ModuleSelectorArgs),
    Delete(ModuleDeleteArgs),
    Export(ModuleExportArgs),
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum BridgeCommand {
    List,
    Get(BridgeSelectorArgs),
    Inspect(BridgeSelectorArgs),
    Validate(DocumentArgs),
    Create(BridgeCandidateArgs),
    Import(BridgeCandidateArgs),
    Adopt(BridgeMutationArgs),
    PreviewUpdate(BridgePreviewUpdateArgs),
    Update(BridgeUpdateArgs),
    PreviewDelete(BridgeSelectorArgs),
    Delete(BridgeDeleteArgs),
    Export(BridgeExportArgs),
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ActivationCommand {
    Inspect,
    Change(ActivationChangeArgs),
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum CompositionCommand {
    Validate(DocumentArgs),
    Preflight(CompositionPreflightArgs),
    ExplainResolution(ResolutionArgs),
}

#[derive(Args)]
pub(crate) struct DocumentArgs {
    /// Bounded YAML or JSON document input.
    #[arg(long)]
    input: PathBuf,
}

#[derive(Args, Clone)]
pub(crate) struct ModuleSelectorArgs {
    /// Globally unique ontology URI.
    #[arg(long)]
    ontology_id: String,
    /// Exact opaque authored version; requires --canonical-digest.
    #[arg(long, requires = "canonical_digest")]
    authored_version: Option<String>,
    /// Exact lowercase SHA-256 digest; requires --authored-version.
    #[arg(long, requires = "authored_version")]
    canonical_digest: Option<String>,
}

#[derive(Args, Clone)]
pub(crate) struct BridgeSelectorArgs {
    /// Globally unique bridge URI.
    #[arg(long)]
    bridge_id: String,
    /// Exact opaque authored version; requires --canonical-digest.
    #[arg(long, requires = "canonical_digest")]
    authored_version: Option<String>,
    /// Exact lowercase SHA-256 digest; requires --authored-version.
    #[arg(long, requires = "authored_version")]
    canonical_digest: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
enum InputFormat {
    Auto,
    Json,
    Yaml,
}

impl From<InputFormat> for ImportFormatHint {
    fn from(value: InputFormat) -> Self {
        match value {
            InputFormat::Auto => Self::Auto,
            InputFormat::Json => Self::Json,
            InputFormat::Yaml => Self::Yaml,
        }
    }
}

impl From<InputFormat> for BridgeImportFormatHint {
    fn from(value: InputFormat) -> Self {
        match value {
            InputFormat::Auto => Self::Auto,
            InputFormat::Json => Self::Json,
            InputFormat::Yaml => Self::Yaml,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum ModeArg {
    Exploratory,
    Advisory,
    Strict,
}

impl From<ModeArg> for ActivationMode {
    fn from(value: ModeArg) -> Self {
        match value {
            ModeArg::Exploratory => Self::Exploratory,
            ModeArg::Advisory => Self::Advisory,
            ModeArg::Strict => Self::Strict,
        }
    }
}

#[derive(Args)]
pub(crate) struct ModuleCandidateArgs {
    #[command(flatten)]
    document: DocumentArgs,
    #[arg(long, value_enum, default_value_t = InputFormat::Auto)]
    format: InputFormat,
    /// Exact dependency as `ontology_uri@authored_version#canonical_digest`.
    #[arg(long = "dependency")]
    dependencies: Vec<String>,
    /// Optional module-specific activation override.
    #[arg(long, value_enum)]
    mode: Option<ModeArg>,
}

#[derive(Args)]
pub(crate) struct BridgeCandidateArgs {
    #[command(flatten)]
    document: DocumentArgs,
    #[arg(long, value_enum, default_value_t = InputFormat::Auto)]
    format: InputFormat,
}

#[derive(Args, Clone)]
pub(crate) struct AuthorityArgs {
    /// Caller-owned idempotency UUID.
    #[arg(long)]
    operation_uuid: String,
    /// Exact project generation observed before mutation.
    #[arg(long)]
    expected_generation: String,
    /// Exact current composition fingerprint; omit only when authority is absent.
    #[arg(long)]
    expected_composition_fingerprint: Option<String>,
    /// Optional actor UUID.
    #[arg(long)]
    actor_uuid: Option<String>,
    /// Deterministically request cancellation before any mutation (automation/testing boundary).
    #[arg(long, hide = true)]
    cancel_before_start: bool,
}

#[derive(Args)]
pub(crate) struct ModuleMutationArgs {
    #[command(flatten)]
    candidate: ModuleCandidateArgs,
    #[command(flatten)]
    authority: AuthorityArgs,
}

#[derive(Args)]
pub(crate) struct ModulePreviewUpdateArgs {
    #[command(flatten)]
    selector: ModuleSelectorArgs,
    #[command(flatten)]
    candidate: ModuleCandidateArgs,
}

#[derive(Args)]
pub(crate) struct ModuleUpdateArgs {
    #[command(flatten)]
    preview: ModulePreviewUpdateArgs,
    #[command(flatten)]
    authority: AuthorityArgs,
}

#[derive(Args)]
pub(crate) struct ModuleDeleteArgs {
    #[command(flatten)]
    selector: ModuleSelectorArgs,
    #[command(flatten)]
    authority: AuthorityArgs,
}

#[derive(Args)]
pub(crate) struct BridgeMutationArgs {
    #[command(flatten)]
    candidate: BridgeCandidateArgs,
    #[command(flatten)]
    authority: AuthorityArgs,
}

#[derive(Args)]
pub(crate) struct BridgePreviewUpdateArgs {
    #[command(flatten)]
    selector: BridgeSelectorArgs,
    #[command(flatten)]
    candidate: BridgeCandidateArgs,
}

#[derive(Args)]
pub(crate) struct BridgeUpdateArgs {
    #[command(flatten)]
    preview: BridgePreviewUpdateArgs,
    #[command(flatten)]
    authority: AuthorityArgs,
}

#[derive(Args)]
pub(crate) struct BridgeDeleteArgs {
    #[command(flatten)]
    selector: BridgeSelectorArgs,
    #[command(flatten)]
    authority: AuthorityArgs,
}

#[derive(Clone, Copy, ValueEnum)]
enum ExportFormatArg {
    Json,
    Yaml,
}

#[derive(Args)]
pub(crate) struct ModuleExportArgs {
    #[command(flatten)]
    selector: ModuleSelectorArgs,
    #[arg(long, value_enum, default_value_t = ExportFormatArg::Json)]
    format: ExportFormatArg,
}

#[derive(Args)]
pub(crate) struct BridgeExportArgs {
    #[command(flatten)]
    selector: BridgeSelectorArgs,
    #[arg(long, value_enum, default_value_t = ExportFormatArg::Json)]
    format: ExportFormatArg,
}

#[derive(Deserialize)]
struct ActivationProfileInput {
    profile_default: ActivationMode,
    activation: Vec<ActivationRecord>,
}

#[derive(Args)]
pub(crate) struct ActivationChangeArgs {
    /// JSON object with `profile_default` and the complete `activation` array.
    #[arg(long)]
    input: PathBuf,
    #[command(flatten)]
    authority: AuthorityArgs,
}

#[derive(Args)]
pub(crate) struct CompositionPreflightArgs {
    #[command(flatten)]
    document: DocumentArgs,
    #[command(flatten)]
    authority: AuthorityArgs,
}

#[derive(Clone, Copy, ValueEnum)]
enum SymbolKindArg {
    Entity,
    Relation,
    Property,
    Constraint,
    Migration,
}

impl From<SymbolKindArg> for SymbolKind {
    fn from(value: SymbolKindArg) -> Self {
        match value {
            SymbolKindArg::Entity => Self::Entity,
            SymbolKindArg::Relation => Self::Relation,
            SymbolKindArg::Property => Self::Property,
            SymbolKindArg::Constraint => Self::Constraint,
            SymbolKindArg::Migration => Self::Migration,
        }
    }
}

#[derive(Args)]
pub(crate) struct ResolutionArgs {
    #[arg(long, value_enum)]
    kind: SymbolKindArg,
    #[arg(long)]
    local_id: String,
    #[arg(long)]
    ontology_id: Option<String>,
    #[arg(long, requires = "ontology_id", requires = "canonical_digest")]
    authored_version: Option<String>,
    #[arg(long, requires = "ontology_id", requires = "authored_version")]
    canonical_digest: Option<String>,
    #[arg(long, default_value_t = 64)]
    max_candidates: usize,
}

pub(crate) fn run_ontology(
    graph: &mut GraphForge,
    command: OntologyCommand,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), CliRuntimeError> {
    match command {
        OntologyCommand::Module { command } => run_module(graph, command, json_output, output),
        OntologyCommand::Bridge { command } => run_bridge(graph, command, json_output, output),
        OntologyCommand::Activation { command } => {
            run_activation(graph, command, json_output, output)
        }
        OntologyCommand::Composition { command } => {
            run_composition(graph, command, json_output, output)
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run_module(
    graph: &mut GraphForge,
    command: ModuleCommand,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), CliRuntimeError> {
    match command {
        ModuleCommand::List => emit(&graph.ontology_modules()?, json_output, output),
        ModuleCommand::Get(args) => emit(
            &graph.inspect_ontology_module(&exact_module_selector(&args)?)?,
            json_output,
            output,
        ),
        ModuleCommand::Inspect(args) => emit(
            &graph.inspect_ontology_module(&module_selector(&args)?)?,
            json_output,
            output,
        ),
        ModuleCommand::Validate(args) => {
            let document: OntologyDoc = read_authored(&args.input)?;
            emit(
                &graph.validate_ontology_module(&document)?,
                json_output,
                output,
            )
        }
        ModuleCommand::Create(args) => {
            let candidate = module_candidate_from_args(graph, &args, true)?;
            emit(&candidate, json_output, output)
        }
        ModuleCommand::Import(args) => {
            let candidate = module_candidate_from_args(graph, &args, false)?;
            emit(&candidate, json_output, output)
        }
        ModuleCommand::Adopt(args) => {
            let candidate = module_candidate_from_args(graph, &args.candidate, true)?;
            let (authority, token) = authority(&args.authority)?;
            let receipt = graph.adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority,
                    candidate,
                },
                Some(&token),
            )?;
            emit(&receipt, json_output, output)
        }
        ModuleCommand::PreviewUpdate(args) => {
            let candidate = module_candidate_from_args(graph, &args.candidate, true)?;
            let preview = graph.preview_update_ontology_module(
                &module_selector(&args.selector)?,
                &candidate.document,
                &candidate.dependencies,
            )?;
            emit(&preview, json_output, output)
        }
        ModuleCommand::Update(args) => {
            let candidate = module_candidate_from_args(graph, &args.preview.candidate, true)?;
            let (authority, token) = authority(&args.authority)?;
            let receipt = graph.update_ontology_module(
                &ModuleUpdateRequest {
                    authority,
                    selector: module_selector(&args.preview.selector)?,
                    document: candidate.document,
                    dependencies: candidate.dependencies,
                    enforcement: candidate.enforcement,
                },
                Some(&token),
            )?;
            emit(&receipt, json_output, output)
        }
        ModuleCommand::PreviewDelete(args) => emit(
            &graph.preview_delete_ontology_module(&module_selector(&args)?)?,
            json_output,
            output,
        ),
        ModuleCommand::Delete(args) => {
            let (authority, token) = authority(&args.authority)?;
            let receipt = graph.delete_ontology_module(
                &ModuleDeleteRequest {
                    authority,
                    selector: module_selector(&args.selector)?,
                },
                Some(&token),
            )?;
            emit(&receipt, json_output, output)
        }
        ModuleCommand::Export(args) => {
            let format = match args.format {
                ExportFormatArg::Json => graphforge_api::ExportFormat::Json,
                ExportFormatArg::Yaml => graphforge_api::ExportFormat::Yaml,
            };
            let document =
                graph.export_ontology_module(&module_selector(&args.selector)?, format)?;
            emit(
                &json!({
                    "contract":"graphforge-ontology-module-export/1",
                    "format": export_token(args.format),
                    "document": document
                }),
                json_output,
                output,
            )
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run_bridge(
    graph: &mut GraphForge,
    command: BridgeCommand,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), CliRuntimeError> {
    match command {
        BridgeCommand::List => emit(&graph.ontology_bridges()?, json_output, output),
        BridgeCommand::Get(args) => emit(
            &graph.inspect_ontology_bridge(&exact_bridge_selector(&args)?)?,
            json_output,
            output,
        ),
        BridgeCommand::Inspect(args) => emit(
            &graph.inspect_ontology_bridge(&bridge_selector(&args)?)?,
            json_output,
            output,
        ),
        BridgeCommand::Validate(args) => {
            let document: BridgeDocument = read_authored(&args.input)?;
            emit(
                &graph.validate_ontology_bridge(&document)?,
                json_output,
                output,
            )
        }
        BridgeCommand::Create(args) => emit(
            &bridge_candidate_from_args(graph, &args, true)?,
            json_output,
            output,
        ),
        BridgeCommand::Import(args) => emit(
            &bridge_candidate_from_args(graph, &args, false)?,
            json_output,
            output,
        ),
        BridgeCommand::Adopt(args) => {
            let candidate = bridge_candidate_from_args(graph, &args.candidate, true)?;
            let (authority, token) = authority(&args.authority)?;
            let receipt = graph.adopt_ontology_bridge(
                &BridgeAdoptionRequest {
                    authority,
                    candidate,
                },
                Some(&token),
            )?;
            emit(&receipt, json_output, output)
        }
        BridgeCommand::PreviewUpdate(args) => {
            let candidate = bridge_candidate_from_args(graph, &args.candidate, true)?;
            emit(
                &graph.preview_update_ontology_bridge(
                    &bridge_selector(&args.selector)?,
                    &candidate.document,
                )?,
                json_output,
                output,
            )
        }
        BridgeCommand::Update(args) => {
            let candidate = bridge_candidate_from_args(graph, &args.preview.candidate, true)?;
            let (authority, token) = authority(&args.authority)?;
            let receipt = graph.update_ontology_bridge(
                &BridgeUpdateRequest {
                    authority,
                    selector: bridge_selector(&args.preview.selector)?,
                    document: candidate.document,
                },
                Some(&token),
            )?;
            emit(&receipt, json_output, output)
        }
        BridgeCommand::PreviewDelete(args) => emit(
            &graph.preview_delete_ontology_bridge(&bridge_selector(&args)?)?,
            json_output,
            output,
        ),
        BridgeCommand::Delete(args) => {
            let (authority, token) = authority(&args.authority)?;
            let receipt = graph.delete_ontology_bridge(
                &BridgeDeleteRequest {
                    authority,
                    selector: bridge_selector(&args.selector)?,
                },
                Some(&token),
            )?;
            emit(&receipt, json_output, output)
        }
        BridgeCommand::Export(args) => {
            let format = match args.format {
                ExportFormatArg::Json => BridgeExportFormat::Json,
                ExportFormatArg::Yaml => BridgeExportFormat::Yaml,
            };
            let document =
                graph.export_ontology_bridge(&bridge_selector(&args.selector)?, format)?;
            emit(
                &json!({
                    "contract":"graphforge-ontology-bridge-export/1",
                    "format": export_token(args.format),
                    "document": document
                }),
                json_output,
                output,
            )
        }
    }
}

fn run_activation(
    graph: &mut GraphForge,
    command: ActivationCommand,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), CliRuntimeError> {
    match command {
        ActivationCommand::Inspect => {
            let (profile_default, activation) = graph.ontology_activation_profile()?;
            emit(
                &json!({"profile_default":profile_default,"activation":activation}),
                json_output,
                output,
            )
        }
        ActivationCommand::Change(args) => {
            let input: ActivationProfileInput = read_json(&args.input)?;
            let (authority, token) = authority(&args.authority)?;
            let receipt = graph.change_ontology_activation_profile(
                &ActivationProfileChangeRequest {
                    authority,
                    profile_default: input.profile_default,
                    activation: input.activation,
                },
                Some(&token),
            )?;
            emit(&receipt, json_output, output)
        }
    }
}

fn run_composition(
    graph: &mut GraphForge,
    command: CompositionCommand,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), CliRuntimeError> {
    match command {
        CompositionCommand::Validate(args) => emit(
            &graph.validate_ontology_composition(&read_json(&args.input)?)?,
            json_output,
            output,
        ),
        CompositionCommand::Preflight(args) => {
            let candidate: WorkspaceOntologyComposition = read_json(&args.document.input)?;
            let (authority, token) = authority(&args.authority)?;
            let request = CompositionChangeRequest {
                context: authority.context,
                expected_project_generation_uuid: authority.expected_project_generation_uuid,
                expected_composition_fingerprint: authority.expected_composition_fingerprint,
                candidate,
                data_disposition: CompositionDataDisposition::RequireConforming,
            };
            emit(
                &graph.preflight_ontology_composition(&request, Some(&token))?,
                json_output,
                output,
            )
        }
        CompositionCommand::ExplainResolution(args) => {
            let module = resolution_module(&args)?;
            emit(
                &graph.explain_ontology_resolution(&ResolutionExplainRequest {
                    module,
                    kind: args.kind.into(),
                    local_id: args.local_id,
                    max_candidates: args.max_candidates,
                })?,
                json_output,
                output,
            )
        }
    }
}

pub(crate) fn run_portable_staging_inspect(
    graph: &GraphForge,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), CliRuntimeError> {
    let staged = graph.portable_ontology_staging(PortableV2Limits::default())?;
    let value = staged.map(|candidate| {
        json!({
            "contract":"graphforge-portable-ontology-staging/1",
            "package_digest":candidate.package_digest,
            "portable_composition_digest":candidate.portable_composition_digest,
            "composition_fingerprint":candidate.composition.composition_fingerprint,
            "composition":candidate.composition,
        })
    });
    emit(&value, json_output, output)
}

pub(crate) fn run_portable_staging_adopt(
    graph: &mut GraphForge,
    authority_args: &AuthorityArgs,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), CliRuntimeError> {
    let (authority, token) = authority(authority_args)?;
    let receipt = graph.adopt_portable_ontology_staging(
        &authority,
        PortableV2Limits::default(),
        Some(&token),
    )?;
    emit(&receipt, json_output, output)
}

fn module_candidate_from_args(
    graph: &GraphForge,
    args: &ModuleCandidateArgs,
    create: bool,
) -> Result<graphforge_api::ModuleCandidate, CliRuntimeError> {
    module_candidate(
        graph,
        &args.document.input,
        args.format,
        &args.dependencies,
        args.mode,
        create,
    )
}

fn module_candidate(
    graph: &GraphForge,
    input: &Path,
    format: InputFormat,
    dependencies: &[String],
    mode: Option<ModeArg>,
    create: bool,
) -> Result<graphforge_api::ModuleCandidate, CliRuntimeError> {
    let text = read_bounded(input)?;
    let dependencies = dependencies
        .iter()
        .map(|value| parse_module_id(value))
        .collect::<Result<Vec<_>, _>>()?;
    let imported = graph.import_ontology_module(&text, format.into(), dependencies.clone())?;
    if create {
        Ok(graph.create_ontology_module(imported.document, dependencies, mode.map(Into::into))?)
    } else {
        let mut imported = imported;
        imported.enforcement = mode.map(Into::into);
        Ok(imported)
    }
}

fn bridge_candidate_from_args(
    graph: &GraphForge,
    args: &BridgeCandidateArgs,
    create: bool,
) -> Result<graphforge_api::BridgeCandidate, CliRuntimeError> {
    bridge_candidate(graph, &args.document.input, args.format, create)
}

fn bridge_candidate(
    graph: &GraphForge,
    input: &Path,
    format: InputFormat,
    create: bool,
) -> Result<graphforge_api::BridgeCandidate, CliRuntimeError> {
    let text = read_bounded(input)?;
    let imported = graph.import_ontology_bridge(&text, format.into())?;
    if create {
        Ok(graph.create_ontology_bridge(imported.document)?)
    } else {
        Ok(imported)
    }
}

fn module_selector(args: &ModuleSelectorArgs) -> Result<ModuleSelector, graphforge_api::GfError> {
    match (&args.authored_version, &args.canonical_digest) {
        (Some(authored_version), Some(canonical_digest)) => {
            Ok(ModuleSelector::Exact(OntologyModuleId {
                ontology_id: args.ontology_id.clone(),
                authored_version: authored_version.clone(),
                canonical_digest: canonical_digest.clone(),
            }))
        }
        (None, None) => Ok(ModuleSelector::OntologyId(args.ontology_id.clone())),
        _ => Err(graphforge_api::GfError::Validation(
            "exact module selection requires authored version and canonical digest".into(),
        )),
    }
}

fn exact_module_selector(
    args: &ModuleSelectorArgs,
) -> Result<ModuleSelector, graphforge_api::GfError> {
    match module_selector(args)? {
        exact @ ModuleSelector::Exact(_) => Ok(exact),
        ModuleSelector::OntologyId(_) => Err(graphforge_api::GfError::Validation(
            "module get requires an exact identity".into(),
        )),
    }
}

fn bridge_selector(args: &BridgeSelectorArgs) -> Result<BridgeSelector, graphforge_api::GfError> {
    match (&args.authored_version, &args.canonical_digest) {
        (Some(authored_version), Some(canonical_digest)) => {
            Ok(BridgeSelector::Exact(BridgeSetId {
                bridge_id: args.bridge_id.clone(),
                authored_version: authored_version.clone(),
                canonical_digest: canonical_digest.clone(),
            }))
        }
        (None, None) => Ok(BridgeSelector::BridgeId(args.bridge_id.clone())),
        _ => Err(graphforge_api::GfError::Validation(
            "exact bridge selection requires authored version and canonical digest".into(),
        )),
    }
}

fn exact_bridge_selector(
    args: &BridgeSelectorArgs,
) -> Result<BridgeSelector, graphforge_api::GfError> {
    match bridge_selector(args)? {
        exact @ BridgeSelector::Exact(_) => Ok(exact),
        BridgeSelector::BridgeId(_) => Err(graphforge_api::GfError::Validation(
            "bridge get requires an exact identity".into(),
        )),
    }
}

fn resolution_module(
    args: &ResolutionArgs,
) -> Result<Option<OntologyModuleId>, graphforge_api::GfError> {
    match (
        &args.ontology_id,
        &args.authored_version,
        &args.canonical_digest,
    ) {
        (None, None, None) => Ok(None),
        (Some(ontology_id), Some(authored_version), Some(canonical_digest)) => {
            Ok(Some(OntologyModuleId {
                ontology_id: ontology_id.clone(),
                authored_version: authored_version.clone(),
                canonical_digest: canonical_digest.clone(),
            }))
        }
        _ => Err(graphforge_api::GfError::Validation(
            "qualified resolution requires the complete exact module identity".into(),
        )),
    }
}

fn authority(
    args: &AuthorityArgs,
) -> Result<(OntologyAuthorityExpectation, CancellationToken), graphforge_api::GfError> {
    let token = CancellationToken::new();
    if args.cancel_before_start {
        token.cancel();
    }
    Ok((
        OntologyAuthorityExpectation {
            context: WriteContext {
                operation_uuid: OperationId(canonical_uuid(&args.operation_uuid)?),
                actor_uuid: args.actor_uuid.as_deref().map(canonical_uuid).transpose()?,
            },
            expected_project_generation_uuid: canonical_uuid(&args.expected_generation)?,
            expected_composition_fingerprint: args.expected_composition_fingerprint.clone(),
        },
        token,
    ))
}

fn parse_module_id(value: &str) -> Result<OntologyModuleId, graphforge_api::GfError> {
    let (identity, canonical_digest) = value.rsplit_once('#').ok_or_else(|| {
        graphforge_api::GfError::Validation("module dependency lacks canonical digest".into())
    })?;
    let (ontology_id, authored_version) = identity.rsplit_once('@').ok_or_else(|| {
        graphforge_api::GfError::Validation("module dependency lacks authored version".into())
    })?;
    Ok(OntologyModuleId {
        ontology_id: ontology_id.into(),
        authored_version: authored_version.into(),
        canonical_digest: canonical_digest.into(),
    })
}

fn read_bounded(path: &Path) -> Result<String, graphforge_api::GfError> {
    let metadata = fs::metadata(path)
        .map_err(|_| graphforge_api::GfError::Validation("ontology input is unavailable".into()))?;
    if metadata.len() > MAX_INPUT_BYTES {
        return Err(graphforge_api::GfError::Validation(
            "ontology input exceeds the 16 MiB limit".into(),
        ));
    }
    fs::read_to_string(path)
        .map_err(|_| graphforge_api::GfError::Validation("ontology input is invalid".into()))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, graphforge_api::GfError> {
    serde_json::from_str(&read_bounded(path)?)
        .map_err(|_| graphforge_api::GfError::Validation("ontology JSON is malformed".into()))
}

fn read_authored<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, graphforge_api::GfError> {
    let text = read_bounded(path)?;
    serde_json::from_str(&text)
        .or_else(|_| serde_yaml::from_str(&text))
        .map_err(|_| graphforge_api::GfError::Validation("ontology document is malformed".into()))
}

fn emit(
    value: &impl serde::Serialize,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), CliRuntimeError> {
    if json_output {
        serde_json::to_writer(&mut *output, value)
    } else {
        serde_json::to_writer_pretty(&mut *output, value)
    }
    .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
    writeln!(output).map_err(|error| graphforge_api::GfError::Execution(error.to_string()).into())
}

const fn export_token(format: ExportFormatArg) -> &'static str {
    match format {
        ExportFormatArg::Json => "json",
        ExportFormatArg::Yaml => "yaml",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn exact_selectors_require_complete_identity() {
        let selector = ModuleSelectorArgs {
            ontology_id: "https://example.test/ontology".into(),
            authored_version: Some("v1".into()),
            canonical_digest: Some("a".repeat(64)),
        };
        assert!(matches!(
            module_selector(&selector),
            Ok(ModuleSelector::Exact(_))
        ));
        let incomplete = ModuleSelectorArgs {
            canonical_digest: None,
            ..selector
        };
        assert!(module_selector(&incomplete).is_err());
        let unqualified = ModuleSelectorArgs {
            ontology_id: "https://example.test/ontology".into(),
            authored_version: None,
            canonical_digest: None,
        };
        assert!(exact_module_selector(&unqualified).is_err());
    }

    #[test]
    fn authority_cancellation_boundary_is_deterministic() {
        let args = AuthorityArgs {
            operation_uuid: Uuid::from_u128(1).hyphenated().to_string(),
            expected_generation: Uuid::from_u128(2).hyphenated().to_string(),
            expected_composition_fingerprint: None,
            actor_uuid: None,
            cancel_before_start: true,
        };
        let (_, token) = authority(&args).unwrap();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cli_surface_path_inventory_is_closed() {
        assert_eq!(CLI_SURFACE_PATHS.len(), 35);
        let mut sorted = CLI_SURFACE_PATHS.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), CLI_SURFACE_PATHS.len());
    }
}
