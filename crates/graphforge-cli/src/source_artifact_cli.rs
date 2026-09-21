//! Thin CLI projection for Source and Artifact lifecycle (#1349).

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use graphforge_api::{
    ArtifactKind, ArtifactPayloadRequest, CapabilityId, DerivationInput, DerivationSubjectKind,
    EnableCapabilityRequest, GraphForge, LineageDirection, ListArtifactsRequest,
    ListSourcesRequest, OperationId, PageRequest, PageToken, RegisterArtifactRequest,
    RegisterSourceRequest, ReplacementImpactRequest, ResearchLineageRequest,
    RetentionDependencyClosureRequest, SetPreferredArtifactRequest, SourceKind, WriteContext,
};

use crate::{canonical_uuid, write_execution_result};

// ---------------------------------------------------------------------------
// Top-level subcommand tree
// ---------------------------------------------------------------------------

#[derive(Subcommand)]
pub(crate) enum SourceArtifactCommand {
    /// Atomically register one immutable research Source.
    RegisterSource(RegisterSourceArgs),
    /// Atomically register one immutable research Artifact for a Source.
    RegisterArtifact(RegisterArtifactArgs),
    /// Set the preferred Artifact for one Source.
    SetPreferredArtifact(SetPreferredArtifactArgs),
    /// Traverse bounded derivation lineage for one research subject.
    ResearchLineage(ResearchLineageArgs),
    /// Inspect replacement-impact candidates for a proposed preference change.
    ReplacementImpact(ReplacementImpactArgs),
    /// Enumerate the retention-dependency closure for one scope UUID.
    RetentionDependencyClosure(RetentionDependencyClosureArgs),
    /// Inspect the current committed capability manifest.
    ProjectCapabilities,
    /// Enable one registered project capability (prerequisite for Source/Artifact operations).
    EnableCapability(EnableCapabilityArgs),
    /// Show one Source by UUID.
    Source(SourceShowArgs),
    /// List Sources with optional pagination.
    ListSources(ListSourcesArgs),
    /// Show one Artifact by UUID.
    Artifact(ArtifactShowArgs),
    /// List Artifacts with optional Source filter and pagination.
    ListArtifacts(ListArtifactsArgs),
}

// ---------------------------------------------------------------------------
// Argument structs
// ---------------------------------------------------------------------------

#[derive(Args)]
pub(crate) struct RegisterSourceArgs {
    /// Idempotent operation UUID.
    #[arg(long)]
    operation_uuid: String,
    /// Optional actor UUID.
    #[arg(long)]
    actor_uuid: Option<String>,
    /// Caller-supplied UUIDv7 Source identity.
    #[arg(long)]
    source_uuid: String,
    /// Bounded human-readable label.
    #[arg(long)]
    label: String,
    /// Closed source kind (manuscript|edition|pdf|epub|web|photograph|recording|database_export|other).
    #[arg(long)]
    source_kind: String,
    /// Optional stable external identity URI.
    #[arg(long)]
    identity_uri: Option<String>,
}

#[derive(Args)]
pub(crate) struct RegisterArtifactArgs {
    /// Idempotent operation UUID.
    #[arg(long)]
    operation_uuid: String,
    /// Optional actor UUID.
    #[arg(long)]
    actor_uuid: Option<String>,
    /// Caller-supplied UUIDv7 Artifact identity.
    #[arg(long)]
    artifact_uuid: String,
    /// Parent Source identity.
    #[arg(long)]
    source_uuid: String,
    /// Closed artifact kind (raw_scan|processed_scan|ocr_text|normalized_text|passage_extract|other).
    #[arg(long)]
    artifact_kind: String,
    /// MIME-like media type label.
    #[arg(long)]
    media_type: String,
    /// Local file path whose bytes are stored as the artifact payload.
    #[arg(long, conflicts_with_all = ["payload_uri", "payload_absent"])]
    payload_file: Option<PathBuf>,
    /// External reference URI (historical; never fetched).
    #[arg(long, conflicts_with_all = ["payload_file", "payload_absent"])]
    payload_uri: Option<String>,
    /// Hex-encoded SHA-256 fingerprint for the external reference (32 bytes = 64 hex chars).
    #[arg(long, requires = "payload_uri")]
    payload_fingerprint: Option<String>,
    /// Explicitly absent payload bytes.
    #[arg(long, conflicts_with_all = ["payload_file", "payload_uri"])]
    payload_absent: bool,
    /// Ordered derivation input as `<uuid>:<kind>` (repeatable).
    #[arg(long = "derivation-input")]
    derivation_inputs: Vec<String>,
    /// Optional algorithm-run UUID.
    #[arg(long)]
    run_uuid: Option<String>,
}

#[derive(Args)]
pub(crate) struct SetPreferredArtifactArgs {
    /// Idempotent operation UUID.
    #[arg(long)]
    operation_uuid: String,
    /// Optional actor UUID.
    #[arg(long)]
    actor_uuid: Option<String>,
    /// Caller-supplied UUIDv7 preference-event identity.
    #[arg(long)]
    preference_event_uuid: String,
    /// Parent Source identity.
    #[arg(long)]
    source_uuid: String,
    /// Newly preferred Artifact identity.
    #[arg(long)]
    artifact_uuid: String,
    /// Bounded human-readable reason.
    #[arg(long)]
    reason: String,
}

#[derive(Args)]
pub(crate) struct ResearchLineageArgs {
    /// Subject UUID.
    #[arg(long)]
    subject_uuid: String,
    /// Closed subject kind (source|artifact|node|edge|assertion|evidence_link|algorithm_run).
    #[arg(long)]
    subject_kind: String,
    /// Traversal direction (backward|forward).
    #[arg(long)]
    direction: String,
    /// Maximum hop depth (inclusive, must be positive).
    #[arg(long, default_value_t = 4)]
    max_depth: u32,
    #[command(flatten)]
    page: PageArgs,
}

#[derive(Args)]
pub(crate) struct ReplacementImpactArgs {
    /// Parent Source identity.
    #[arg(long)]
    source_uuid: String,
    /// Proposed preferred Artifact identity.
    #[arg(long)]
    artifact_uuid: String,
}

#[derive(Args)]
pub(crate) struct RetentionDependencyClosureArgs {
    /// Selection or retention root UUID.
    #[arg(long)]
    scope_uuid: String,
    #[command(flatten)]
    page: PageArgs,
}

#[derive(Args)]
pub(crate) struct EnableCapabilityArgs {
    /// Idempotent operation UUID.
    #[arg(long)]
    operation_uuid: String,
    /// Optional actor UUID.
    #[arg(long)]
    actor_uuid: Option<String>,
    /// Registered capability (graph|provenance|knowledge|epistemic|valid_time).
    #[arg(long)]
    capability_id: String,
    /// Requested capability contract version.
    #[arg(long, default_value_t = 1)]
    capability_version: u32,
}

#[derive(Args)]
pub(crate) struct SourceShowArgs {
    /// Source UUID to retrieve.
    #[arg(long)]
    source_uuid: String,
}

#[derive(Args)]
pub(crate) struct ListSourcesArgs {
    #[command(flatten)]
    page: PageArgs,
}

#[derive(Args)]
pub(crate) struct ArtifactShowArgs {
    /// Artifact UUID to retrieve.
    #[arg(long)]
    artifact_uuid: String,
}

#[derive(Args)]
pub(crate) struct ListArtifactsArgs {
    /// Optional Source UUID filter.
    #[arg(long)]
    source_uuid: Option<String>,
    #[command(flatten)]
    page: PageArgs,
}

#[derive(Args)]
pub(crate) struct PageArgs {
    #[arg(long, default_value_t = 100)]
    limit: u32,
    #[arg(long)]
    after: Option<String>,
}

// ---------------------------------------------------------------------------
// Parsers
// ---------------------------------------------------------------------------

fn parse_source_kind(value: &str) -> Result<SourceKind, graphforge_api::GfError> {
    match value {
        "manuscript" => Ok(SourceKind::Manuscript),
        "edition" => Ok(SourceKind::Edition),
        "pdf" => Ok(SourceKind::Pdf),
        "epub" => Ok(SourceKind::Epub),
        "web" => Ok(SourceKind::Web),
        "photograph" => Ok(SourceKind::Photograph),
        "recording" => Ok(SourceKind::Recording),
        "database_export" => Ok(SourceKind::DatabaseExport),
        "other" => Ok(SourceKind::Other),
        _ => Err(graphforge_api::GfError::Validation(format!(
            "unknown source kind {value:?}; expected one of: manuscript, edition, pdf, epub, web, \
             photograph, recording, database_export, other"
        ))),
    }
}

fn parse_artifact_kind(value: &str) -> Result<ArtifactKind, graphforge_api::GfError> {
    match value {
        "raw_scan" => Ok(ArtifactKind::RawScan),
        "processed_scan" => Ok(ArtifactKind::ProcessedScan),
        "ocr_text" => Ok(ArtifactKind::OcrText),
        "normalized_text" => Ok(ArtifactKind::NormalizedText),
        "passage_extract" => Ok(ArtifactKind::PassageExtract),
        "other" => Ok(ArtifactKind::Other),
        _ => Err(graphforge_api::GfError::Validation(format!(
            "unknown artifact kind {value:?}; expected one of: raw_scan, processed_scan, \
             ocr_text, normalized_text, passage_extract, other"
        ))),
    }
}

fn parse_derivation_subject_kind(
    value: &str,
) -> Result<DerivationSubjectKind, graphforge_api::GfError> {
    match value {
        "source" => Ok(DerivationSubjectKind::Source),
        "artifact" => Ok(DerivationSubjectKind::Artifact),
        "node" => Ok(DerivationSubjectKind::Node),
        "edge" => Ok(DerivationSubjectKind::Edge),
        "assertion" => Ok(DerivationSubjectKind::Assertion),
        "evidence_link" => Ok(DerivationSubjectKind::EvidenceLink),
        "algorithm_run" => Ok(DerivationSubjectKind::AlgorithmRun),
        _ => Err(graphforge_api::GfError::Validation(format!(
            "unknown subject kind {value:?}; expected one of: source, artifact, node, edge, \
             assertion, evidence_link, algorithm_run"
        ))),
    }
}

fn parse_lineage_direction(value: &str) -> Result<LineageDirection, graphforge_api::GfError> {
    match value {
        "backward" => Ok(LineageDirection::Backward),
        "forward" => Ok(LineageDirection::Forward),
        _ => Err(graphforge_api::GfError::Validation(format!(
            "unknown direction {value:?}; expected backward or forward"
        ))),
    }
}

fn parse_capability_id(value: &str) -> Result<CapabilityId, graphforge_api::GfError> {
    match value {
        "graph" => Ok(CapabilityId::Graph),
        "provenance" => Ok(CapabilityId::Provenance),
        "knowledge" => Ok(CapabilityId::Knowledge),
        "epistemic" => Ok(CapabilityId::Epistemic),
        "valid_time" => Ok(CapabilityId::ValidTime),
        _ => Err(graphforge_api::GfError::Validation(format!(
            "unknown capability id {value:?}; expected one of: graph, provenance, knowledge, \
             epistemic, valid_time"
        ))),
    }
}

/// Parse a `<uuid>:<kind>` derivation input string.
fn parse_derivation_input(value: &str) -> Result<DerivationInput, graphforge_api::GfError> {
    let (uuid_part, kind_part) = value.rsplit_once(':').ok_or_else(|| {
        graphforge_api::GfError::Validation(format!(
            "derivation input must be <uuid>:<kind>, got {value:?}"
        ))
    })?;
    Ok(DerivationInput {
        input_uuid: canonical_uuid(uuid_part)?,
        input_kind: parse_derivation_subject_kind(kind_part)?,
    })
}

fn page(args: &PageArgs) -> Result<PageRequest, graphforge_api::GfError> {
    Ok(PageRequest {
        limit: args.limit,
        after: args.after.as_deref().map(PageToken::parse).transpose()?,
        cancellation: None,
    })
}

fn write_context(
    operation_uuid: &str,
    actor_uuid: Option<&str>,
) -> Result<WriteContext, graphforge_api::GfError> {
    Ok(WriteContext {
        operation_uuid: OperationId(canonical_uuid(operation_uuid)?),
        actor_uuid: actor_uuid.map(canonical_uuid).transpose()?,
    })
}

fn parse_hex_fingerprint(hex: &str) -> Result<[u8; 32], graphforge_api::GfError> {
    if hex.len() != 64 {
        return Err(graphforge_api::GfError::Validation(
            "fingerprint must be exactly 64 hex characters (32 bytes)".into(),
        ));
    }
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).map_err(|_| {
            graphforge_api::GfError::Validation("fingerprint is not valid hex".into())
        })?;
    }
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub(crate) fn run_source_artifact(
    graph: &mut GraphForge,
    command: SourceArtifactCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    match command {
        SourceArtifactCommand::RegisterSource(args) => {
            run_register_source(graph, args, json, output)
        }
        SourceArtifactCommand::RegisterArtifact(args) => {
            run_register_artifact(graph, args, json, output)
        }
        SourceArtifactCommand::SetPreferredArtifact(args) => {
            run_set_preferred(graph, args, json, output)
        }
        SourceArtifactCommand::ResearchLineage(args) => run_lineage(graph, args, json, output),
        SourceArtifactCommand::ReplacementImpact(args) => run_impact(graph, args, json, output),
        SourceArtifactCommand::RetentionDependencyClosure(args) => {
            run_closure(graph, args, json, output)
        }
        SourceArtifactCommand::ProjectCapabilities => {
            write_execution_result(&graph.project_capabilities()?, json, output)
        }
        SourceArtifactCommand::EnableCapability(args) => {
            run_enable_capability(graph, args, json, output)
        }
        SourceArtifactCommand::Source(args) => write_execution_result(
            &graph.source(canonical_uuid(&args.source_uuid)?)?,
            json,
            output,
        ),
        SourceArtifactCommand::ListSources(args) => write_execution_result(
            &graph.list_sources(ListSourcesRequest {
                page: page(&args.page)?,
            })?,
            json,
            output,
        ),
        SourceArtifactCommand::Artifact(args) => write_execution_result(
            &graph.artifact(canonical_uuid(&args.artifact_uuid)?)?,
            json,
            output,
        ),
        SourceArtifactCommand::ListArtifacts(args) => write_execution_result(
            &graph.list_artifacts(ListArtifactsRequest {
                source_uuid: args
                    .source_uuid
                    .as_deref()
                    .map(canonical_uuid)
                    .transpose()?,
                page: page(&args.page)?,
            })?,
            json,
            output,
        ),
    }
}

fn run_register_source(
    graph: &mut GraphForge,
    args: RegisterSourceArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let result = graph.register_source(RegisterSourceRequest {
        context: write_context(&args.operation_uuid, args.actor_uuid.as_deref())?,
        source_uuid: canonical_uuid(&args.source_uuid)?,
        label: args.label,
        source_kind: parse_source_kind(&args.source_kind)?,
        identity_uri: args.identity_uri,
    })?;
    write_execution_result(&result, json, output)
}

fn run_register_artifact(
    graph: &mut GraphForge,
    args: RegisterArtifactArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let payload = if let Some(path) = &args.payload_file {
        let bytes = fs::read(path).map_err(|error| {
            graphforge_api::GfError::Validation(format!("payload file is unreadable: {error}"))
        })?;
        ArtifactPayloadRequest::LocalBytes(bytes)
    } else if let Some(uri) = args.payload_uri.clone() {
        let fingerprint = args
            .payload_fingerprint
            .as_deref()
            .map(parse_hex_fingerprint)
            .transpose()?;
        ArtifactPayloadRequest::ExternalReference { uri, fingerprint }
    } else {
        ArtifactPayloadRequest::Absent
    };
    let derivation_inputs = args
        .derivation_inputs
        .iter()
        .map(|s| parse_derivation_input(s))
        .collect::<Result<Vec<_>, _>>()?;
    let run_uuid = args.run_uuid.as_deref().map(canonical_uuid).transpose()?;
    let result = graph.register_artifact(RegisterArtifactRequest {
        context: write_context(&args.operation_uuid, args.actor_uuid.as_deref())?,
        artifact_uuid: canonical_uuid(&args.artifact_uuid)?,
        source_uuid: canonical_uuid(&args.source_uuid)?,
        artifact_kind: parse_artifact_kind(&args.artifact_kind)?,
        media_type: args.media_type,
        payload,
        derivation_inputs,
        run_uuid,
    })?;
    write_execution_result(&result, json, output)
}

fn run_set_preferred(
    graph: &mut GraphForge,
    args: SetPreferredArtifactArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let result = graph.set_preferred_artifact(SetPreferredArtifactRequest {
        context: write_context(&args.operation_uuid, args.actor_uuid.as_deref())?,
        preference_event_uuid: canonical_uuid(&args.preference_event_uuid)?,
        source_uuid: canonical_uuid(&args.source_uuid)?,
        artifact_uuid: canonical_uuid(&args.artifact_uuid)?,
        reason: args.reason,
    })?;
    write_execution_result(&result, json, output)
}

#[allow(clippy::needless_pass_by_value)]
fn run_lineage(
    graph: &mut GraphForge,
    args: ResearchLineageArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let result = graph.research_lineage(ResearchLineageRequest {
        subject_uuid: canonical_uuid(&args.subject_uuid)?,
        subject_kind: parse_derivation_subject_kind(&args.subject_kind)?,
        direction: parse_lineage_direction(&args.direction)?,
        max_depth: args.max_depth,
        page: page(&args.page)?,
    })?;
    write_execution_result(&result, json, output)
}

#[allow(clippy::needless_pass_by_value)]
fn run_impact(
    graph: &mut GraphForge,
    args: ReplacementImpactArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let result = graph.replacement_impact(ReplacementImpactRequest {
        source_uuid: canonical_uuid(&args.source_uuid)?,
        artifact_uuid: canonical_uuid(&args.artifact_uuid)?,
    })?;
    write_execution_result(&result, json, output)
}

#[allow(clippy::needless_pass_by_value)]
fn run_closure(
    graph: &mut GraphForge,
    args: RetentionDependencyClosureArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let result = graph.retention_dependency_closure(RetentionDependencyClosureRequest {
        scope_uuid: canonical_uuid(&args.scope_uuid)?,
        page: page(&args.page)?,
    })?;
    write_execution_result(&result, json, output)
}

#[allow(clippy::needless_pass_by_value)]
fn run_enable_capability(
    graph: &mut GraphForge,
    args: EnableCapabilityArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    let result = graph.enable_capability(EnableCapabilityRequest {
        context: write_context(&args.operation_uuid, args.actor_uuid.as_deref())?,
        capability_id: parse_capability_id(&args.capability_id)?,
        capability_version: args.capability_version,
    })?;
    write_execution_result(&result, json, output)
}
