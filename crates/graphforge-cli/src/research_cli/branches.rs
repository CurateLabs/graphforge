//! Thin CLI transport for native Branch controls and Arrow inspection.
use clap::{Args, Subcommand};
use graphforge_api::{CancellationToken, GfError, GraphForge};
use std::{
    io::{Read, Write},
    path::PathBuf,
};
#[derive(Args)]
pub(crate) struct Request {
    /// Native JSON request, including exact CURRENT and stable operation UUID.
    #[arg(long)]
    file: PathBuf,
}
#[derive(Args)]
pub(crate) struct Identity {
    #[arg(long)]
    branch_uuid: String,
}
#[derive(Subcommand)]
pub(crate) enum BranchCommand {
    /// Execute the native Create request.
    Create(Request),
    /// Execute the native Execute request.
    Execute(Request),
    /// Execute the native Restore request.
    Restore(Request),
    /// Execute the native Ontology request.
    Ontology(Request),
    /// Execute the native Reference request.
    Reference(Request),
    /// Execute the native Bring request.
    Bring(Request),
    /// Execute the native SuppressAssertion request.
    SuppressAssertion(Request),
    /// Inspect genealogy and current Version identity.
    Info(Identity),
    /// Inspect immutable exact membership at creation.
    Selection(Identity),
    /// Inspect object/field origin, baseline, contributions and local status.
    Fields(Identity),
    /// Inspect citations that do not expand active research.
    References(Identity),
    /// Read-only Cypher against the effective Branch graph.
    Query {
        #[command(flatten)]
        identity: Identity,
        #[arg(long)]
        query: String,
    },
}
fn request<T: serde::de::DeserializeOwned>(path: PathBuf) -> Result<T, GfError> {
    let file = std::fs::File::open(path)
        .map_err(|_| GfError::Validation("cannot open Branch request file".into()))?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GfError::Validation("cannot read Branch request file".into()))?;
    if bytes.len() > 1024 * 1024 {
        return Err(GfError::Api {
            code: graphforge_api::ApiErrorCode::ResourceLimit,
            message: "Branch request exceeds byte bound".into(),
        });
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| GfError::Validation("invalid Branch JSON contract".into()))
}
pub(crate) fn run(
    graph: &mut GraphForge,
    command: BranchCommand,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let token = CancellationToken::new();
    let receipt = match command {
        BranchCommand::Create(args) => {
            graph.create_research_branch(&request(args.file)?, &token)?
        }
        BranchCommand::Execute(args) => {
            graph.execute_research_branch(&request(args.file)?, &token)?
        }
        BranchCommand::Restore(args) => {
            graph.restore_research_branch(&request(args.file)?, &token)?
        }
        BranchCommand::Ontology(args) => {
            graph.change_research_branch_ontology(&request(args.file)?, &token)?
        }
        BranchCommand::Reference(args) => {
            graph.reference_research_branch(&request(args.file)?, &token)?
        }
        BranchCommand::Bring(args) => graph.bring_research_branch(&request(args.file)?, &token)?,
        BranchCommand::SuppressAssertion(args) => {
            graph.suppress_research_branch_assertion(&request(args.file)?, &token)?
        }
        BranchCommand::Info(args) => {
            let view = graph.open_research_branch(crate::canonical_uuid(&args.branch_uuid)?)?;
            return write_json(
                output,
                &serde_json::json!({"record": view.record(), "version_uuid": view.version_uuid()}),
            );
        }
        BranchCommand::Selection(args) => {
            let result =
                graph.research_branch_selection(crate::canonical_uuid(&args.branch_uuid)?)?;
            return crate::write_execution_result(&result, json, output);
        }
        BranchCommand::Fields(args) => {
            let result = graph
                .open_research_branch(crate::canonical_uuid(&args.branch_uuid)?)?
                .fields()?;
            return crate::write_execution_result(&result, json, output);
        }
        BranchCommand::References(args) => {
            let result = graph
                .open_research_branch(crate::canonical_uuid(&args.branch_uuid)?)?
                .references()?;
            return crate::write_execution_result(&result, json, output);
        }
        BranchCommand::Query { identity, query } => {
            let result = graph
                .open_research_branch(crate::canonical_uuid(&identity.branch_uuid)?)?
                .graph()
                .execute(&query)?;
            return crate::write_execution_result(&result, json, output);
        }
    };
    write_json(output, &receipt)
}
fn write_json(output: &mut dyn Write, value: &impl serde::Serialize) -> Result<(), GfError> {
    serde_json::to_writer(&mut *output, value).map_err(|e| GfError::Execution(e.to_string()))?;
    writeln!(output).map_err(|e| GfError::Execution(e.to_string()))
}
