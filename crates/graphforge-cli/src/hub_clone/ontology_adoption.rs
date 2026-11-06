//! High-level commands reuse exact module discovery and native atomic adoption.
use super::{
    HttpTransport, ModuleFetchArgs, RETRY_POLICY, RetryPolicy, Transport, acquire_staging,
    ensure_destination_absent, install_destination, module_fetch, release_staging, storage,
    validation,
};
use clap::{Args, Subcommand};
use graphforge_api::{
    BranchSource, CancellationToken, CreateResearchBranchRequest, GfError, GraphForge,
    OntologyAdoptionReceipt, OperationId, SelectResearchBranchRequest, WriteContext,
};
use std::io::Write;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Args)]
pub(crate) struct ExactOntologyArgs {
    /// Exact advertised module ID (bare permanent ID or ontology URI).
    #[arg(long)]
    ontology_id: String,
    /// Exact opaque authored version.
    #[arg(long)]
    version: String,
    /// Exact canonical SHA-256 content digest, optionally prefixed with sha256:.
    #[arg(long)]
    digest: String,
    /// HTTPS Hub base URL; defaults to https://graphforge.sh.
    #[arg(long)]
    hub: Option<String>,
    /// Request cancellation before network or project mutation.
    #[arg(long, hide = true)]
    cancel_before_start: bool,
}

#[derive(Args)]
pub(crate) struct OntologyAdoptArgs {
    /// Public source owner/repository or permanent module reference.
    repository: String,
    #[command(flatten)]
    exact: ExactOntologyArgs,
}

#[derive(Subcommand)]
pub(crate) enum ProjectCommand {
    /// Create a publishable research Project from one exact ontology and its dependencies.
    New(ProjectNewArgs),
}

#[derive(Args)]
pub(crate) struct ProjectNewArgs {
    /// New destination directory; an unrelated existing destination is never replaced.
    directory: PathBuf,
    /// Public source owner/repository or permanent module reference.
    #[arg(long)]
    from_ontology: String,
    #[command(flatten)]
    exact: ExactOntologyArgs,
}

pub(crate) fn dispatch(
    command: crate::Command,
    project: Option<PathBuf>,
    project_dir: Option<PathBuf>,
    json: bool,
    output: &mut dyn Write,
) -> Result<ControlFlow<(), crate::Command>, GfError> {
    dispatch_with(
        &HttpTransport::new(),
        command,
        project,
        project_dir,
        json,
        output,
        &RETRY_POLICY,
    )
}

#[allow(clippy::too_many_arguments)]
fn dispatch_with(
    transport: &dyn Transport,
    command: crate::Command,
    project: Option<PathBuf>,
    project_dir: Option<PathBuf>,
    json: bool,
    output: &mut dyn Write,
    retry: &RetryPolicy,
) -> Result<ControlFlow<(), crate::Command>, GfError> {
    let (repository, exact, destination, new_project) = match command {
        crate::Command::Ontology {
            command: crate::ontology_cli::OntologyCommand::Adopt(args),
        } => (
            args.repository,
            args.exact,
            crate::resolve_project_path(project, project_dir)?,
            false,
        ),
        crate::Command::Project {
            command: ProjectCommand::New(args),
        } => (args.from_ontology, args.exact, args.directory, true),
        command => return Ok(ControlFlow::Continue(command)),
    };
    let token = CancellationToken::new();
    if exact.cancel_before_start {
        token.cancel();
    }
    check_cancelled(&token)?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !parent.is_dir() {
        return Err(validation(
            "hub.destination_conflict",
            "destination parent does not exist",
        ));
    }
    if new_project {
        ensure_destination_absent(&destination)?;
    } else if !destination.join("CURRENT").is_file() {
        return Err(validation(
            "hub.destination_conflict",
            "ontology adopt requires an existing durable Project",
        ));
    }
    let staging = acquire_staging(&parent.join(format!(".ontology-adoption-{}", Uuid::now_v7())))?;
    let result = (|| {
        let fetch = ModuleFetchArgs {
            repository,
            ontology_id: Some(exact.ontology_id),
            version: Some(exact.version),
            digest: Some(exact.digest),
            hub: exact.hub,
            output: staging.root.join("ontology"),
        };
        let (resolved, _moved) = module_fetch::resolve_module_with(transport, &fetch, retry)?;
        check_cancelled(&token)?;
        let path = if new_project {
            &staging.project
        } else {
            &destination
        };
        let mut graph = GraphForge::new(Some(path.to_str().ok_or_else(|| {
            validation("hub.destination_conflict", "Project path must be UTF-8")
        })?))?;
        let mut receipt = graph.adopt_resolved_ontology_module(
            &resolved,
            WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            Some(&token),
        )?;
        if new_project {
            initialize_branch(&mut graph, &receipt, &token)?;
            receipt.project_generation_uuid =
                graph.committed_generation_identity()?.generation_uuid;
            drop(graph);
            check_cancelled(&token)?;
            install_destination(&staging, &destination)?;
        }
        write_result(&receipt, &destination, new_project, json, output)
    })();
    release_staging(staging);
    result?;
    Ok(ControlFlow::Break(()))
}

fn initialize_branch(
    graph: &mut GraphForge,
    adoption: &OntologyAdoptionReceipt,
    token: &CancellationToken,
) -> Result<(), GfError> {
    let branch = Uuid::now_v7();
    let context = Uuid::now_v7();
    graph.create_research_branch(
        &CreateResearchBranchRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: graph.committed_generation_identity()?.generation_uuid,
            branch_uuid: branch,
            version_uuid: Uuid::now_v7(),
            source: BranchSource::Current {
                origin_version_uuid: Uuid::now_v7(),
                context_uuid: context,
            },
            creator_uuid: Uuid::now_v7(),
            created_at: i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| GfError::Execution("system clock precedes UTC epoch".into()))?
                    .as_micros(),
            )
            .map_err(|_| GfError::Execution("system clock exceeds timestamp range".into()))?,
            label: "main".into(),
            author: None,
            committer: None,
            description: Some(format!(
                "Adopt ontology {} from {} ({})",
                adoption.module.display_ref(),
                adoption.repository,
                adoption.package_digest
            )),
        },
        token,
    )?;
    graph.select_research_branch(
        &SelectResearchBranchRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: graph.committed_generation_identity()?.generation_uuid,
            branch_uuid: branch,
        },
        token,
    )?;
    Ok(())
}

fn write_result(
    receipt: &OntologyAdoptionReceipt,
    destination: &Path,
    new_project: bool,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    if json {
        serde_json::to_writer(&mut *output, &serde_json::json!({
            "contract": if new_project {"graphforge-project-from-ontology/1"} else {"graphforge-ontology-adopt/1"},
            "project": destination.display().to_string(), "adoption": receipt,
        })).map_err(|error| GfError::Execution(error.to_string()))?;
        writeln!(output).map_err(storage)?;
    } else {
        writeln!(
            output,
            "{} ontology {} from {} in {}",
            if new_project {
                "Started Project with"
            } else {
                "Adopted"
            },
            receipt.module.display_ref(),
            receipt.repository,
            destination.display()
        )
        .map_err(storage)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;

fn check_cancelled(token: &CancellationToken) -> Result<(), GfError> {
    if token.is_cancelled() {
        Err(GfError::Api {
            code: graphforge_api::ApiErrorCode::Cancelled,
            message: "ontology adoption cancelled".into(),
        })
    } else {
        Ok(())
    }
}
