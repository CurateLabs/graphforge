//! Thin CLI projection for research Project metadata and local discovery (#1348).

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use graphforge_api::{
    DiscoverResearchProjectsRequest, GraphForge, OperationId, ResearchProjectDiscoveryLimits,
    ResearchProjectDiscoveryQuery, UpdateResearchMetadataRequest, WorkspaceResearchMetadata,
    WriteContext,
};
use serde_json::json;

use crate::{canonical_uuid, write_execution_result};

#[derive(Subcommand)]
pub(crate) enum ResearchCommand {
    /// Inspect or update research metadata for the open Project.
    Metadata {
        #[command(subcommand)]
        command: ResearchMetadataCommand,
    },
    /// Discover caller-supplied local Projects without opening graph payloads.
    Discover(ResearchDiscoverArgs),
}

#[derive(Subcommand)]
pub(crate) enum ResearchMetadataCommand {
    /// Show authoritative metadata and identity for the open Project.
    Show,
    /// Replace authoritative metadata from one canonical JSON document.
    Update(ResearchMetadataUpdateArgs),
}

#[derive(Args)]
pub(crate) struct ResearchMetadataUpdateArgs {
    /// Canonical JSON metadata document.
    #[arg(long)]
    file: PathBuf,
    /// Idempotent operation UUID.
    #[arg(long)]
    operation_uuid: String,
    /// Optional actor UUID.
    #[arg(long)]
    actor_uuid: Option<String>,
}

#[derive(Args)]
pub(crate) struct ResearchDiscoverArgs {
    /// Local durable Project roots to inspect.
    #[arg(long = "root", required = true)]
    roots: Vec<PathBuf>,
    /// Optional case-insensitive free-text filter.
    #[arg(long)]
    free_text: Option<String>,
    /// Require every listed language label.
    #[arg(long)]
    language: Vec<String>,
    /// Require every listed subject label.
    #[arg(long)]
    subject: Vec<String>,
    /// Require every listed ontology label.
    #[arg(long)]
    ontology: Vec<String>,
    /// Require every listed source-type label.
    #[arg(long = "source-type")]
    source_types: Vec<String>,
    /// Optional temporal label substring.
    #[arg(long)]
    temporal_label: Option<String>,
    /// Maximum summaries returned.
    #[arg(long, default_value_t = 1_024)]
    max_projects: usize,
    /// Maximum candidate roots inspected.
    #[arg(long, default_value_t = 1_024)]
    max_candidates: usize,
}

pub(crate) fn run_research(
    graph: &mut GraphForge,
    command: ResearchCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), graphforge_api::GfError> {
    match command {
        ResearchCommand::Metadata { command } => match command {
            ResearchMetadataCommand::Show => {
                let summary = graph.research_project_summary()?;
                let payload = json!({
                    "project_path": summary.project_path.display().to_string(),
                    "identity": {
                        "volume_serial": summary.identity.volume_serial,
                        "file_id_hex": summary.identity.file_id_hex,
                        "generation_uuid": summary.identity.generation_uuid.hyphenated().to_string(),
                    },
                    "metadata": summary.metadata,
                });
                let text = if json {
                    serde_json::to_string_pretty(&payload)
                        .map_err(|error| graphforge_api::GfError::Validation(error.to_string()))?
                } else {
                    serde_json::to_string(&payload)
                        .map_err(|error| graphforge_api::GfError::Validation(error.to_string()))?
                };
                writeln!(output, "{text}")
                    .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
            }
            ResearchMetadataCommand::Update(args) => {
                let bytes = fs::read(&args.file).map_err(|error| {
                    graphforge_api::GfError::Validation(format!(
                        "metadata file is unreadable: {error}"
                    ))
                })?;
                let metadata = WorkspaceResearchMetadata::from_canonical_json(&bytes)?;
                graph.update_research_metadata(UpdateResearchMetadataRequest {
                    context: WriteContext {
                        operation_uuid: OperationId(canonical_uuid(&args.operation_uuid)?),
                        actor_uuid: args.actor_uuid.as_deref().map(canonical_uuid).transpose()?,
                    },
                    metadata,
                })?;
                let text = if json {
                    r#"{"status":"updated"}"#
                } else {
                    "updated"
                };
                writeln!(output, "{text}")
                    .map_err(|error| graphforge_api::GfError::Execution(error.to_string()))?;
            }
        },
        ResearchCommand::Discover(args) => {
            let result =
                GraphForge::discover_research_projects(&DiscoverResearchProjectsRequest {
                    project_roots: args.roots,
                    query: ResearchProjectDiscoveryQuery {
                        free_text: args.free_text,
                        languages: args.language,
                        subjects: args.subject,
                        ontologies: args.ontology,
                        source_types: args.source_types,
                        temporal_label: args.temporal_label,
                    },
                    limits: ResearchProjectDiscoveryLimits {
                        max_projects: args.max_projects,
                        max_candidates: args.max_candidates,
                    },
                })?;
            write_execution_result(&result, json, output)?;
        }
    }
    Ok(())
}
