//! Thin JSON requests and Arrow/control output for native contextual research.
use clap::{Args, Subcommand};
use graphforge_api::{CancellationToken, GfError, GraphForge};
use std::{
    io::{Read, Write},
    path::PathBuf,
};
#[derive(Args)]
pub(crate) struct Request {
    /// Exact native JSON request file.
    #[arg(long)]
    file: PathBuf,
}
fn request<T: serde::de::DeserializeOwned>(args: Request) -> Result<T, GfError> {
    let file = std::fs::File::open(args.file)
        .map_err(|_| GfError::Validation("cannot open Proposal request".into()))?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GfError::Validation("cannot read Proposal request".into()))?;
    if bytes.len() > 1024 * 1024 {
        return Err(GfError::Api {
            code: graphforge_api::ApiErrorCode::ResourceLimit,
            message: "Proposal request exceeds byte limit".into(),
        });
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| GfError::Validation("invalid Proposal JSON contract".into()))
}
#[derive(Subcommand)]
pub(crate) enum ProposalCommand {
    /// Freeze selected Branch contributions.
    Submit(Request),
    /// Inspect dependencies and conflicts as Arrow.
    Preview(Request),
    /// Publish item decisions and accepted content atomically.
    Review(Request),
    /// Release the obsolete frozen payload root.
    Release(Request),
    /// Inspect native proposal history as Arrow.
    History(Request),
}
pub(crate) fn run(
    graph: &mut GraphForge,
    command: ProposalCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let token = CancellationToken::new();
    let receipt = match command {
        ProposalCommand::Submit(args) => graph.submit_research_proposal(&request(args)?, &token)?,
        ProposalCommand::Review(args) => graph.review_research_proposal(&request(args)?, &token)?,
        ProposalCommand::Release(args) => {
            graph.release_research_proposal(&request(args)?, &token)?
        }
        ProposalCommand::Preview(args) => {
            return crate::write_execution_result(
                &graph.preview_research_proposal(&request(args)?, &token)?,
                json,
                output,
            );
        }
        ProposalCommand::History(args) => {
            return crate::write_execution_result(
                &graph.research_proposal_history(&request(args)?, &token)?,
                json,
                output,
            );
        }
    };
    serde_json::to_writer(&mut *output, &receipt).map_err(|e| GfError::Execution(e.to_string()))?;
    writeln!(output).map_err(|e| GfError::Execution(e.to_string()))
}
