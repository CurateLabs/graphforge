//! Confirmation and native durable execution through the same-build CLI.

use graphforge_api::{
    BranchSource, CancellationToken, CreateResearchBranchRequest, GraphForge, ItemIdRegeneration,
    ModuleAdoptionRequest, OntologyAuthorityExpectation, OperationId, WriteContext,
};
use graphforge_discovery::{ItemId, ItemKind};
use graphforge_hub_publish::IdTakenItem;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use uuid::Uuid;

const OLD: &str = "ont_aaaaaaaaaaaaaaaa";
const OLD_URI: &str = "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa";

struct Project {
    _root: tempfile::TempDir,
    path: PathBuf,
    branch: Uuid,
    base: Uuid,
}

impl Project {
    fn new(receipt: Option<bool>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("project");
        let mut graph = GraphForge::new(path.to_str()).unwrap();
        let candidate = graph
            .create_ontology_module(
                serde_json::from_value(serde_json::json!({
                    "ontology_id": OLD_URI,
                    "version": "1",
                    "entity_types": [{"name": "Thing"}]
                }))
                .unwrap(),
                Vec::new(),
                None,
            )
            .unwrap();
        let state = graph.ontology_authority_state().unwrap();
        graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: OntologyAuthorityExpectation {
                        context: WriteContext {
                            operation_uuid: OperationId(Uuid::now_v7()),
                            actor_uuid: None,
                        },
                        expected_project_generation_uuid: state.project_generation_uuid,
                        expected_composition_fingerprint: state.composition_fingerprint,
                    },
                    candidate,
                },
                None,
            )
            .unwrap();
        let branch = Uuid::now_v7();
        let base = Uuid::now_v7();
        graph
            .create_research_branch(
                &CreateResearchBranchRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: graph
                        .committed_generation_identity()
                        .unwrap()
                        .generation_uuid,
                    branch_uuid: branch,
                    version_uuid: base,
                    source: BranchSource::Current {
                        origin_version_uuid: Uuid::now_v7(),
                        context_uuid: Uuid::now_v7(),
                    },
                    creator_uuid: Uuid::now_v7(),
                    created_at: 1,
                    label: "main".into(),
                    description: None,
                    author: None,
                    committer: None,
                },
                &CancellationToken::new(),
            )
            .unwrap();
        if let Some(as_owner) = receipt {
            // Seed the actual native receipt API, never a handwritten sidecar.
            graph
                .record_publish_id_taken(
                    base,
                    &[IdTakenItem {
                        kind: ItemKind::OntologyModule,
                        id: OLD.into(),
                        owner: Some("organization".into()),
                        publish_as_owner: as_owner.then_some(true),
                    }],
                )
                .unwrap();
        }
        Self {
            _root: root,
            path,
            branch,
            base,
        }
    }

    fn open(&self) -> GraphForge {
        GraphForge::new(self.path.to_str()).unwrap()
    }

    fn command(&self, id: &str, operation: Uuid, yes: bool) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_gf"));
        command
            .arg("--project")
            .arg(&self.path)
            .args([
                "--json",
                "ontology",
                "regenerate-id",
                id,
                "--operation-uuid",
            ])
            .arg(operation.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if yes {
            command.arg("--yes");
        }
        command
    }

    fn snapshot(&self) -> serde_json::Value {
        let graph = self.open();
        serde_json::json!({
            "generation": graph.committed_generation_identity().unwrap().generation_uuid,
            "modules": graph.ontology_modules().unwrap(),
            "receipt": graph.id_taken_receipt(OLD).unwrap(),
            "base": graph.research_version(self.base).unwrap(),
            "head": graph.open_research_branch(self.branch).unwrap().version_uuid(),
        })
    }

    fn assert_unchanged(&self, before: &serde_json::Value) {
        assert_eq!(&self.snapshot(), before);
    }

    fn assert_regenerated(
        &self,
        output: &Output,
        before: &serde_json::Value,
    ) -> ItemIdRegeneration {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: ItemIdRegeneration = serde_json::from_slice(&output.stdout).unwrap();
        let id = ItemId::parse_reference(&result.id).unwrap();
        assert_eq!(id.kind(), ItemKind::OntologyModule);
        assert_ne!(result.id, OLD);
        let graph = self.open();
        assert_eq!(
            graph.id_taken_receipt(OLD).unwrap().unwrap().regenerated,
            Some(result.clone())
        );
        assert_eq!(
            graph
                .open_research_branch(self.branch)
                .unwrap()
                .version_uuid(),
            result.version_uuid
        );
        assert_eq!(
            graph.research_version(result.version_uuid).unwrap().parents,
            vec![self.base]
        );
        assert_eq!(
            serde_json::to_value(graph.research_version(self.base).unwrap()).unwrap(),
            before["base"]
        );
        let modules = graph.ontology_modules().unwrap();
        assert!(
            modules
                .iter()
                .any(|module| module.id.ontology_id == id.to_uri())
        );
        assert!(
            modules
                .iter()
                .all(|module| module.id.ontology_id != OLD_URI)
        );
        result
    }
}

#[test]
fn yes_flag_invokes_native_regeneration_and_replays_after_reopen() {
    let project = Project::new(Some(false));
    let before = project.snapshot();
    let operation = Uuid::now_v7();
    let output = project.command(OLD, operation, true).output().unwrap();
    let result = project.assert_regenerated(&output, &before);
    let generation = project
        .open()
        .committed_generation_identity()
        .unwrap()
        .generation_uuid;
    let replay = project.command(OLD, operation, true).output().unwrap();
    assert!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<ItemIdRegeneration>(&replay.stdout).unwrap(),
        result
    );
    assert_eq!(
        project
            .open()
            .committed_generation_identity()
            .unwrap()
            .generation_uuid,
        generation
    );
    assert!(!String::from_utf8_lossy(&output.stderr).contains("[y/N]"));
}

#[test]
fn native_receipt_guards_are_preserved_by_the_cli() {
    for (receipt, id, hint) in [
        (None, OLD, "receipt"),
        (Some(true), OLD, "organization"),
        (Some(false), "https://example.org/ontology", "GraphForge"),
    ] {
        let project = Project::new(receipt);
        let before = project.snapshot();
        let output = project.command(id, Uuid::now_v7(), true).output().unwrap();
        assert!(!output.status.success());
        let error = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(error.contains(hint), "{error}");
        project.assert_unchanged(&before);
    }
}

#[test]
fn noninteractive_answers_and_eof_never_confirm_without_yes_flag() {
    let project = Project::new(Some(false));
    let before = project.snapshot();
    for answer in [b"yes\n".as_slice(), b"no\n", b"invalid\n", b"\n", b""] {
        let mut child = project
            .command(OLD, Uuid::now_v7(), false)
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        if let Err(error) = input.write_all(answer) {
            // The CLI may refuse non-terminal stdin before the writer runs.
            assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        }
        drop(input);
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("--yes"), "{error}");
        project.assert_unchanged(&before);
    }
}

#[cfg(unix)]
fn terminal_answer(mut command: Command, answer: &[u8]) -> Output {
    use std::os::fd::FromRawFd as _;

    let mut master = -1;
    let mut slave = -1;
    // SAFETY: openpty initializes both descriptors; null options request the
    // platform's default terminal settings and no name output.
    assert_eq!(
        unsafe {
            libc::openpty(
                &raw mut master,
                &raw mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        },
        0
    );
    // SAFETY: each fresh descriptor is transferred to exactly one owned File.
    let mut master = unsafe { std::fs::File::from_raw_fd(master) };
    // SAFETY: the slave descriptor is distinct from the master and still owned here.
    let slave = unsafe { std::fs::File::from_raw_fd(slave) };
    let child = command.stdin(Stdio::from(slave)).spawn().unwrap();
    master.write_all(answer).unwrap();
    child.wait_with_output().unwrap()
}

#[cfg(unix)]
#[test]
fn terminal_requires_an_explicit_affirmative_before_native_mutation() {
    let project = Project::new(Some(false));
    let before = project.snapshot();
    // Ctrl-D on an empty canonical terminal line produces EOF.
    for answer in [b"no\n".as_slice(), b"invalid\n", b"\n", b"\x04"] {
        let output = terminal_answer(project.command(OLD, Uuid::now_v7(), false), answer);
        assert_eq!(output.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&output.stderr).contains("[y/N]"));
        project.assert_unchanged(&before);
    }
    let output = terminal_answer(project.command(OLD_URI, Uuid::now_v7(), false), b"yes\n");
    assert!(String::from_utf8_lossy(&output.stderr).contains("[y/N]"));
    project.assert_regenerated(&output, &before);
}
