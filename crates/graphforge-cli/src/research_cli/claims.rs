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
#[derive(Subcommand)]
pub(crate) enum ClaimCommand {
    /// Atomically create a classified native assertion.
    Create(Request),
    /// Append an explicit relation without canonical promotion.
    Relate(Request),
    /// Publish one Branch-local create/challenge/revision/suppression change.
    ChangeBranch(Request),
    /// Record explicit contextual integration/promotion/revocation decisions.
    Decide(Request),
    /// Inspect the explicitly scoped active knowledge view.
    Inspect(Request),
    /// Inspect retained native claim owner histories.
    History(Request),
    /// Inspect contextual canonical decision history.
    Decisions(Request),
    /// Inspect current explicit canonical choices.
    Canonical(Request),
}
fn request<T: serde::de::DeserializeOwned>(args: Request) -> Result<T, GfError> {
    let file = std::fs::File::open(args.file)
        .map_err(|_| GfError::Validation("cannot open research claim request".into()))?;
    let mut bytes = Vec::new();
    file.take(2 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GfError::Validation("cannot read research claim request".into()))?;
    if bytes.len() > 2 * 1024 * 1024 {
        return Err(GfError::Api {
            code: graphforge_api::ApiErrorCode::ResourceLimit,
            message: "research claim request exceeds byte limit".into(),
        });
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| GfError::Validation("invalid contextual research JSON contract".into()))
}
pub(crate) fn run(
    graph: &mut GraphForge,
    command: ClaimCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let token = CancellationToken::new();
    let result = match command {
        ClaimCommand::Create(args) => graph.create_research_claim(&request(args)?, &token)?,
        ClaimCommand::Relate(args) => graph.relate_research_claims(&request(args)?, &token)?,
        ClaimCommand::Decide(args) => graph.record_research_decisions(&request(args)?, &token)?,
        ClaimCommand::Inspect(args) => graph.inspect_research_claims(&request(args)?)?,
        ClaimCommand::History(args) => graph.research_claim_history(&request(args)?)?,
        ClaimCommand::Decisions(args) => {
            let q: graphforge_api::ResearchAuthorityQuery = request(args)?;
            graph.research_decision_history(&q.context, q.community_uuid)?
        }
        ClaimCommand::Canonical(args) => {
            let q: graphforge_api::ResearchAuthorityQuery = request(args)?;
            graph.research_canonical_choices(&q.context, q.community_uuid)?
        }
        ClaimCommand::ChangeBranch(args) => {
            let receipt = graph.change_research_branch_claim(&request(args)?, &token)?;
            serde_json::to_writer(&mut *output, &receipt)
                .map_err(|e| GfError::Execution(e.to_string()))?;
            return writeln!(output).map_err(|e| GfError::Execution(e.to_string()));
        }
    };
    crate::write_execution_result(&result, json, output)
}
