//! Project-owned saved query metadata and explicit Arrow execution.
use clap::Subcommand;
use graphforge_api::{GfError, GraphForge, SavedQuery, SavedQuerySource};
use serde_json::Value;
use std::{collections::HashMap, io::Write, path::PathBuf};

#[derive(Subcommand)]
pub(crate) enum SavedQueryCommand {
    /// Save a JSON query definition without executing it.
    Save {
        #[arg(long)]
        file: PathBuf,
    },
    /// List saved definitions at the current head or an exact Version.
    List {
        #[arg(long)]
        version_uuid: Option<String>,
    },
    /// Inspect one definition without executing it.
    Show {
        query_uuid: String,
        #[arg(long)]
        version_uuid: Option<String>,
    },
    /// Replace an existing definition, preserving its UUID.
    Update {
        #[arg(long)]
        file: PathBuf,
    },
    /// Delete a saved definition.
    Delete { query_uuid: String },
    /// Execute a saved query with explicit JSON parameters; emit Arrow IPC or --json.
    Run {
        query_uuid: String,
        #[arg(long)]
        params: Option<PathBuf>,
        #[arg(long)]
        version_uuid: Option<String>,
    },
}
fn source(version: Option<String>) -> Result<SavedQuerySource, GfError> {
    version.map_or(Ok(SavedQuerySource::Current), |text| {
        crate::canonical_uuid(&text).map(|version_uuid| SavedQuerySource::Version { version_uuid })
    })
}
fn json(output: &mut dyn Write, value: impl serde::Serialize) -> Result<(), GfError> {
    serde_json::to_writer(&mut *output, &value).map_err(|e| GfError::Execution(e.to_string()))?;
    writeln!(output).map_err(|e| GfError::Execution(e.to_string()))
}
fn load<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Result<T, GfError> {
    let bytes = crate::read_bounded_file(path, 1024 * 1024, "saved query JSON exceeds byte bound")?;
    serde_json::from_slice(&bytes)
        .map_err(|_| GfError::Validation("invalid saved query JSON contract".into()))
}
pub(crate) fn run(
    graph: &mut GraphForge,
    command: SavedQueryCommand,
    json_output: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    match command {
        SavedQueryCommand::Save { file } => {
            let definition: SavedQuery = load(&file)?;
            json(output, graph.create_saved_query(definition)?)
        }
        SavedQueryCommand::Update { file } => {
            let definition: SavedQuery = load(&file)?;
            json(output, graph.update_saved_query(definition)?)
        }
        SavedQueryCommand::Delete { query_uuid } => {
            graph.delete_saved_query(crate::canonical_uuid(&query_uuid)?)?;
            json(output, serde_json::json!({"deleted": query_uuid}))
        }
        SavedQueryCommand::Show {
            query_uuid,
            version_uuid,
        } => json(
            output,
            graph.saved_query_at(crate::canonical_uuid(&query_uuid)?, &source(version_uuid)?)?,
        ),
        SavedQueryCommand::List { version_uuid } => {
            json(output, graph.saved_queries_at(&source(version_uuid)?)?)
        }
        SavedQueryCommand::Run {
            query_uuid,
            params,
            version_uuid,
        } => {
            let params: HashMap<String, Value> = params
                .map(|path| load(&path))
                .transpose()?
                .unwrap_or_default();
            let result = graph.execute_saved_query_json(
                crate::canonical_uuid(&query_uuid)?,
                &params,
                &source(version_uuid)?,
                None,
            )?;
            crate::write_execution_result(&result, json_output, output)
        }
    }
}
