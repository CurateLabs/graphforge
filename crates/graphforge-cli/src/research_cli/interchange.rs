//! Native research references, portable export, and explicit independent Fork.
use clap::{Args, Subcommand};
use graphforge_api::{CancellationToken, GfError, GraphForge};
use std::{
    io::{Read, Write},
    path::PathBuf,
};
#[derive(Args)]
pub(crate) struct Request {
    /// Bounded native request JSON document.
    #[arg(long)]
    file: PathBuf,
}
#[derive(Subcommand)]
pub(crate) enum InterchangeCommand {
    /// Resolve a live Branch or immutable Version citation.
    Reference(Request),
    /// Export a complete Version or explicit selected projection.
    Export(Request),
    /// Create a separately governed Project from selected native research.
    Fork(Request),
}
fn request<T: serde::de::DeserializeOwned>(args: Request) -> Result<T, GfError> {
    let file = std::fs::File::open(args.file)
        .map_err(|_| GfError::Validation("cannot open research request".into()))?;
    let mut bytes = Vec::new();
    file.take(64 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GfError::Validation("cannot read research request".into()))?;
    if bytes.len() > 64 * 1024 * 1024 {
        return Err(GfError::Validation(
            "research request exceeds 64 MiB".into(),
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| GfError::Validation("invalid research interchange JSON contract".into()))
}
pub(crate) fn run(
    graph: &GraphForge,
    command: InterchangeCommand,
    output: &mut dyn Write,
) -> Result<(), crate::CliRuntimeError> {
    let cancel = CancellationToken::new();
    let result = match command {
        InterchangeCommand::Reference(args) => {
            serde_json::to_value(graph.research_reference(&request(args)?, &cancel)?)
        }
        InterchangeCommand::Export(args) => serde_json::to_value(
            graph
                .export_research(&request(args)?, &cancel)
                .map_err(graphforge_api::MultiOntologyError::from)?,
        ),
        InterchangeCommand::Fork(args) => serde_json::to_value(
            graph
                .fork_research(&request(args)?, &cancel)
                .map_err(graphforge_api::MultiOntologyError::from)?,
        ),
    }
    .map_err(|error| GfError::Execution(error.to_string()))?;
    serde_json::to_writer(&mut *output, &result)
        .map_err(|error| GfError::Execution(error.to_string()))?;
    writeln!(output).map_err(|error| GfError::Execution(error.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use graphforge_api::{
        BranchSource, CreateResearchBranchRequest, OperationId, PortableV2ImportRequest,
        PortableVerifyRequest, ResearchReferenceTarget,
    };
    use serde_json::{Value, json};
    use std::path::Path;
    use uuid::Uuid;

    #[test]
    fn cli_research_interchange_preserves_citations_fork_replay_and_invalid_request_authority() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("source");
        let mut graph = GraphForge::new(root.to_str()).unwrap();
        graph.execute("CREATE (:Item {score:7})").unwrap();
        let version = Uuid::now_v7();
        graph
            .create_research_branch(
                &CreateResearchBranchRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: graph
                        .research_project_summary()
                        .unwrap()
                        .identity
                        .generation_uuid,
                    branch_uuid: Uuid::now_v7(),
                    version_uuid: version,
                    source: BranchSource::Current {
                        origin_version_uuid: Uuid::now_v7(),
                        context_uuid: Uuid::now_v7(),
                    },
                    creator_uuid: Uuid::now_v7(),
                    created_at: 1,
                    label: "CLI research".into(),
                },
                &CancellationToken::new(),
            )
            .unwrap();
        let mut metadata = graph.research_project_metadata().unwrap();
        metadata.title = Some("Independent CLI Fork".into());
        metadata.access.access_policy = Some("Independent local review".into());
        drop(graph);
        let before = std::fs::read(root.join("CURRENT")).unwrap();
        let file = directory.path().join("request.json");
        let reference = success(
            &root,
            &file,
            "reference",
            &json!({"kind":"version", "version_uuid":version}),
        );
        let package = directory.path().join("package");
        let exported = success(
            &root,
            &file,
            "export",
            &json!({"version_uuid":version, "output":package, "bundled":false, "projection":null}),
        );
        let report = graphforge_api::verify_portable_v2(
            &PortableVerifyRequest {
                input: package.clone(),
                mode: graphforge_api::PortableV2Mode::Full,
                limits: Default::default(),
            },
            None,
        )
        .unwrap();
        assert_eq!(exported["package_digest"], report.package_digest);
        let imported_root = directory.path().join("imported");
        GraphForge::import_portable_v2(
            &imported_root,
            &PortableV2ImportRequest {
                input: package,
                operation_id: OperationId(Uuid::now_v7()),
                limits: Default::default(),
            },
            None,
        )
        .unwrap();
        let imported = GraphForge::new(imported_root.to_str()).unwrap();
        let imported_reference = imported
            .research_reference(
                &ResearchReferenceTarget::Version {
                    version_uuid: version,
                },
                &CancellationToken::new(),
            )
            .unwrap();
        assert_eq!(
            serde_json::to_value(imported_reference.version).unwrap(),
            reference["version"]
        );
        let target = directory.path().join("fork");
        let request = json!({"operation_uuid":Uuid::now_v7(), "project_uuid":Uuid::now_v7(), "version_uuid":version,
            "projection":null,"target":target,"actor_uuid":Uuid::now_v7(),"governance":"Independent review",
            "adopt_selected_ontology":true,"metadata":metadata});
        let first = success(&root, &file, "fork", &request);
        let replay = success(&root, &file, "fork", &request);
        assert_eq!(first["generation_uuid"], replay["generation_uuid"]);
        assert_eq!(replay["idempotent_replay"], true);
        let fork = GraphForge::new(target.to_str()).unwrap();
        assert_eq!(fork.research_project_metadata().unwrap(), metadata);
        let citation = fork
            .research_reference(
                &ResearchReferenceTarget::Version {
                    version_uuid: version,
                },
                &CancellationToken::new(),
            )
            .unwrap();
        assert_eq!(
            serde_json::to_value(citation.project_uuid).unwrap(),
            request["project_uuid"]
        );
        assert_eq!(
            serde_json::to_value(citation.version).unwrap(),
            reference["version"]
        );
        drop(fork);
        let target_before = std::fs::read(target.join("CURRENT")).unwrap();
        let mut changed = request.clone();
        changed["governance"] = json!("Different intent");
        let (ok, output) = execute(&root, &file, "fork", &changed);
        assert!(!ok);
        assert!(String::from_utf8_lossy(&output).contains("GF_IDEMPOTENCY_CONFLICT"));
        assert_eq!(
            std::fs::read(target.join("CURRENT")).unwrap(),
            target_before
        );
        for command in ["reference", "export", "fork"] {
            let (ok, output) = execute(
                &root,
                &file,
                command,
                &json!({"PRIVATE_INTERCHANGE_SENTINEL":"private"}),
            );
            assert!(!ok);
            assert!(!String::from_utf8_lossy(&output).contains("PRIVATE_INTERCHANGE_SENTINEL"));
        }
        assert_eq!(std::fs::read(root.join("CURRENT")).unwrap(), before);
    }

    fn success(root: &Path, file: &Path, command: &str, request: &Value) -> Value {
        let (ok, output) = execute(root, file, command, request);
        assert!(ok, "{}", String::from_utf8_lossy(&output));
        serde_json::from_slice(&output).unwrap()
    }
    fn execute(root: &Path, file: &Path, command: &str, request: &Value) -> (bool, Vec<u8>) {
        std::fs::write(file, serde_json::to_vec(request).unwrap()).unwrap();
        let cli = crate::Cli::try_parse_from([
            "gf",
            "--project",
            root.to_str().unwrap(),
            "--json",
            "research",
            "interchange",
            command,
            "--file",
            file.to_str().unwrap(),
        ])
        .unwrap();
        let mut output = Vec::new();
        match crate::run(cli, &mut output) {
            Ok(code) => (code == 0, output),
            Err(error) => {
                crate::write_runtime_error(&error, true, &mut output).unwrap();
                (false, output)
            }
        }
    }
}
