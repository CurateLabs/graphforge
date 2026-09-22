//! Thin CLI projection of native immutable Version operations.
use crate::{canonical_uuid, write_execution_result};
use clap::Subcommand;
use graphforge_api::{CancellationToken, GfError, GraphForge};
use std::io::{Read, Write};
use std::path::PathBuf;

#[derive(Subcommand)]
pub(crate) enum ResearchVersionCommand {
    /// Freeze a complete-Project capture from a JSON PrepareResearchVersionRequest.
    Prepare {
        #[arg(long)]
        file: PathBuf,
    },
    /// Commit an exact prepared operation, including retention and explicit restore.
    Commit {
        #[arg(long)]
        file: PathBuf,
    },
    /// List immutable identities and citation labels.
    List,
    /// Inspect immutable citation and content commitments.
    Show {
        #[arg(long)]
        version: String,
    },
    /// Read frozen ontology metadata.
    Ontology {
        #[arg(long)]
        version: String,
    },
    /// Read frozen Project research metadata.
    Metadata {
        #[arg(long)]
        version: String,
    },
    /// Inspect current heads, dependency roots and durable receipts.
    Retention,
    /// Run read-only Cypher against exact historical research.
    Query {
        #[arg(long)]
        version: String,
        #[arg(long)]
        query: String,
    },
    /// Read frozen Artifact identities, source links and availability.
    Artifact {
        #[arg(long)]
        version: String,
        #[arg(long)]
        artifact: String,
    },
    /// Read exact retained local Artifact bytes as Arrow data.
    ArtifactPayload {
        #[arg(long)]
        version: String,
        #[arg(long)]
        artifact: String,
    },
}

pub(crate) fn run(
    graph: &mut GraphForge,
    command: ResearchVersionCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let value = match command {
        ResearchVersionCommand::Prepare { file } => {
            serde_json::to_value(graph.prepare_research_version(read_request(file)?)?)
        }
        ResearchVersionCommand::Commit { file } => {
            serde_json::to_value(graph.commit_research_version_operation(
                read_request(file)?,
                &CancellationToken::new(),
            )?)
        }
        ResearchVersionCommand::Show { version } => {
            serde_json::to_value(graph.research_version(canonical_uuid(&version)?)?)
        }
        ResearchVersionCommand::Ontology { version } => serde_json::to_value(
            graph
                .open_research_version(canonical_uuid(&version)?)?
                .workspace_ontology()?,
        ),
        ResearchVersionCommand::Metadata { version } => serde_json::to_value(
            graph
                .open_research_version(canonical_uuid(&version)?)?
                .research_project_metadata()?,
        ),
        ResearchVersionCommand::Retention => {
            serde_json::to_value(graph.research_version_retention()?)
        }
        ResearchVersionCommand::List => {
            return write_execution_result(&graph.list_research_versions()?, json, output);
        }
        ResearchVersionCommand::Query { version, query } => {
            return write_execution_result(
                &graph
                    .open_research_version(canonical_uuid(&version)?)?
                    .execute(&query)?,
                json,
                output,
            );
        }
        ResearchVersionCommand::Artifact { version, artifact } => {
            return write_execution_result(
                &graph
                    .open_research_version(canonical_uuid(&version)?)?
                    .artifact(canonical_uuid(&artifact)?)?,
                json,
                output,
            );
        }
        ResearchVersionCommand::ArtifactPayload { version, artifact } => {
            return write_execution_result(
                &graph
                    .open_research_version(canonical_uuid(&version)?)?
                    .artifact_payload(canonical_uuid(&artifact)?)?,
                json,
                output,
            );
        }
    }
    .map_err(|error| GfError::Validation(error.to_string()))?;
    serde_json::to_writer(&mut *output, &value)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    writeln!(output).map_err(|error| GfError::Execution(error.to_string()))
}

fn read_request<T: serde::de::DeserializeOwned>(path: PathBuf) -> Result<T, GfError> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|error| GfError::Validation(error.to_string()))?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| GfError::Validation(error.to_string()))?;
    if bytes.len() > 1024 * 1024 {
        return Err(GfError::Validation("Version request exceeds 1 MiB".into()));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| GfError::Validation("invalid research Version JSON contract".into()))
}
