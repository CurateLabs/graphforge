//! Thin native upstream review and publication commands.
use clap::{Args, Subcommand};
use graphforge_api::{CancellationToken, GfError, GraphForge};
use std::{
    io::{Read, Write},
    path::PathBuf,
};
#[derive(Args)]
pub(crate) struct Request {
    /// Exact native request JSON file.
    #[arg(long)]
    file: PathBuf,
}
#[derive(Subcommand)]
pub(crate) enum UpstreamCommand {
    /// Review exact base/local/upstream state without advancing the Branch.
    Preview(Request),
    /// Publish only reviewed native fields and explicit resolutions.
    Update(Request),
    /// Inspect permanent review decisions and source Version citations.
    History(Request),
}
fn request<T: serde::de::DeserializeOwned>(args: Request) -> Result<T, GfError> {
    let file = std::fs::File::open(args.file)
        .map_err(|_| GfError::Validation("cannot open upstream research request".into()))?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GfError::Validation("cannot read upstream research request".into()))?;
    if bytes.len() > 1024 * 1024 {
        return Err(GfError::Api {
            code: graphforge_api::ApiErrorCode::ResourceLimit,
            message: "upstream request exceeds 1 MiB".into(),
        });
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| GfError::Validation("invalid upstream research JSON contract".into()))
}
pub(crate) fn run(
    graph: &mut GraphForge,
    command: UpstreamCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let cancel = CancellationToken::new();
    let result = match command {
        UpstreamCommand::Preview(args) => {
            graph.preview_research_upstream(&request(args)?, &cancel)?
        }
        UpstreamCommand::History(args) => {
            graph.research_upstream_history(&request(args)?, &cancel)?
        }
        UpstreamCommand::Update(args) => {
            let receipt = graph.update_research_branch(&request(args)?, &cancel)?;
            serde_json::to_writer(&mut *output, &receipt)
                .map_err(|error| GfError::Execution(error.to_string()))?;
            return writeln!(output).map_err(|error| GfError::Execution(error.to_string()));
        }
    };
    crate::write_execution_result(&result, json, output)
}
