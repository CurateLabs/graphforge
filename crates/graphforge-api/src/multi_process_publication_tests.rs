use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{Array, StringArray};
use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectStageOutcome,
};
use uuid::Uuid;

use crate::{GfError, GraphForge};

const DEADLINE: Duration = Duration::from_secs(10);
const HELPER: &str = "multi_process_publication_tests::multi_process_writer_helper";
const CHILD_MODE: &str = "GF_TEST_MULTIPROCESS_MODE";
const CHILD_ROOT: &str = "GF_TEST_MULTIPROCESS_ROOT";
const READ_NAMES: &str = "MATCH (n:Person) RETURN n.name AS name ORDER BY name";

fn request(root: &Path, transaction_uuid: Uuid, generation_uuid: Uuid) -> ProjectGenerationRequest {
    let selected =
        graphforge_storage::resolve_project_generation(root).expect("phase=request resolve");
    let capabilities = selected
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let participants = selected
        .participant_snapshots()
        .expect("phase=request participant snapshots")
        .into_iter()
        .map(|entry| ProjectParticipant {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
            record_family_id: entry.record_family_id,
            record_version: entry.record_version,
            encoding: match entry.encoding.as_str() {
                "arrow" => ProjectParticipantEncoding::Arrow,
                "json" => ProjectParticipantEncoding::Json,
                "parquet" => ProjectParticipantEncoding::Parquet,
                other => panic!("phase=request unsupported encoding={other}"),
            },
            schema_fingerprint: entry.schema_fingerprint,
            row_count: entry.row_count,
            bytes: entry.bytes,
        })
        .collect();
    ProjectGenerationRequest {
        transaction_uuid,
        generation_uuid,
        capabilities,
        participants,
    }
}

fn names(graph: &GraphForge) -> Result<Vec<String>, GfError> {
    let result = graph.execute(READ_NAMES)?;
    let mut names = Vec::new();
    for batch in result.batches {
        let values = batch
            .column_by_name("name")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| GfError::Execution("ordered name result is malformed".into()))?;
        names.extend((0..values.len()).map(|row| values.value(row).to_owned()));
    }
    Ok(names)
}

fn seed(root: &Path) {
    let graph = GraphForge::new(root.to_str()).expect("phase=seed open");
    graph
        .execute("CREATE (:Person {name:'Alpha'})")
        .expect("phase=seed publish Alpha");
}

fn inventory(root: &Path, directory: &str) -> BTreeSet<String> {
    std::fs::read_dir(root.join(directory))
        .unwrap_or_else(|error| panic!("phase=inventory directory={directory} error={error}"))
        .map(|entry| {
            entry
                .expect("phase=inventory read entry")
                .file_name()
                .into_string()
                .expect("phase=inventory machine name is UTF-8")
        })
        .collect()
}

fn assert_complete_generation_directories(root: &Path) {
    let generations = root.join("generations");
    for generation in inventory(root, "generations") {
        let path = generations.join(&generation);
        assert!(path.is_dir(), "generation={generation} is not a directory");
        assert!(
            path.join("manifest.json").is_file(),
            "generation={generation} lacks a durable manifest"
        );
        assert!(
            path.join("participants").is_dir(),
            "generation={generation} lacks participants"
        );
    }
}

struct ChildHarness {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    reader: Option<thread::JoinHandle<()>>,
    reaped: bool,
}

impl ChildHarness {
    fn spawn(root: &Path, mode: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("phase=child current exe"))
            .args(["--exact", HELPER, "--nocapture"])
            .env(CHILD_MODE, mode)
            .env(CHILD_ROOT, root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|error| panic!("phase=child mode={mode} spawn error={error}"));
        let stdin = child.stdin.take().expect("phase=child piped stdin");
        let stdout = child.stdout.take().expect("phase=child piped stdout");
        let (sender, lines) = mpsc::sync_channel(16);
        let reader = thread::Builder::new()
            .name(format!("gf-multiprocess-{mode}-stdout"))
            .spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    if sender.send(line).is_err() {
                        break;
                    }
                }
            })
            .expect("phase=child spawn stdout reader");
        Self {
            child,
            stdin: Some(stdin),
            lines,
            reader: Some(reader),
            reaped: false,
        }
    }

    fn marker(&mut self, expected: &str, phase: &str) {
        self.marker_before(expected, phase, Instant::now() + DEADLINE);
    }

    fn marker_before(&mut self, expected: &str, phase: &str, deadline: Instant) {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = self.lines.recv_timeout(remaining).unwrap_or_else(|error| {
                let status = self.child.try_wait().expect("phase=child try_wait");
                panic!(
                    "phase={phase} expected={expected} timeout={DEADLINE:?} status={status:?} \
                     channel={error}"
                )
            });
            if line == format!("GF_CHILD {expected}") {
                return;
            }
        }
    }

    fn command(&mut self, command: &str) {
        let stdin = self.stdin.as_mut().expect("phase=child stdin available");
        writeln!(stdin, "{command}").expect("phase=child write command");
        stdin.flush().expect("phase=child flush command");
    }

    fn kill(&mut self) {
        self.child.kill().expect("phase=child kill");
    }

    fn wait(mut self, phase: &str) -> std::process::ExitStatus {
        drop(self.stdin.take());
        let deadline = Instant::now() + DEADLINE;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("phase=child try_wait") {
                break status;
            }
            assert!(Instant::now() < deadline, "phase={phase} child timeout");
            thread::yield_now();
        };
        self.reader
            .take()
            .expect("phase=child stdout reader available")
            .join()
            .unwrap_or_else(|_| panic!("phase={phase} stdout reader panicked"));
        self.reaped = true;
        status
    }
}

impl Drop for ChildHarness {
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        drop(self.stdin.take());
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

#[test]
fn multi_process_writer_helper() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(CHILD_ROOT).expect("child project root"));
    match mode.as_str() {
        "stage" => {
            let staged = match graphforge_storage::stage_project_generation(
                &root,
                &request(
                    &root,
                    Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000002544").unwrap(),
                    Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000002545").unwrap(),
                ),
            )
            .expect("child stage generation")
            {
                ProjectStageOutcome::Staged(staged) => staged,
                ProjectStageOutcome::AlreadyPublished(_) => panic!("child stage replayed"),
            };
            println!("GF_CHILD STAGED");
            std::io::stdout().flush().expect("child flush staged");
            let mut command = String::new();
            std::io::stdin()
                .read_line(&mut command)
                .expect("child read release command");
            assert_eq!(command.trim(), "RELEASE");
            drop(staged);
        }
        "publish" => {
            println!("GF_CHILD READY");
            std::io::stdout().flush().expect("child flush ready");
            let mut command = String::new();
            std::io::stdin()
                .read_line(&mut command)
                .expect("child read publish command");
            assert_eq!(command.trim(), "PUBLISH");
            let graph = GraphForge::new(root.to_str()).expect("child open writer");
            graph
                .execute("CREATE (:Person {name:'Beta'})")
                .expect("child publish Beta");
            println!("GF_CHILD PUBLISHED");
            std::io::stdout().flush().expect("child flush published");
        }
        "contend" => {
            println!("GF_CHILD READY");
            std::io::stdout()
                .flush()
                .expect("child flush contender ready");
            let mut command = String::new();
            std::io::stdin()
                .read_line(&mut command)
                .expect("child read contender command");
            assert_eq!(command.trim(), "CONTEND");
            let contender = request(
                &root,
                Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000002546").unwrap(),
                Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000002547").unwrap(),
            );
            let error = match graphforge_storage::stage_project_generation(&root, &contender) {
                Ok(_) => panic!("child contender unexpectedly acquired live writer lock"),
                Err(error) => error,
            };
            println!("GF_CHILD RESULT {}", error.code());
            std::io::stdout()
                .flush()
                .expect("child flush contender result");
        }
        other => panic!("unknown child mode={other}"),
    }
}

#[test]
fn killed_staged_child_releases_lock_and_recovers_without_partial_generation() {
    let project = tempfile::tempdir().expect("phase=killed tempdir");
    seed(project.path());
    let initial = graphforge_storage::resolve_project_generation(project.path())
        .expect("phase=killed resolve initial");
    initial
        .validate_complete_participant_inventory()
        .expect("phase=killed initial complete");
    let current_uuid = initial.generation_uuid();
    let generations_before = inventory(project.path(), "generations");
    let mut child = ChildHarness::spawn(project.path(), "stage");
    child.marker("STAGED", "killed-staged");

    let generations_staged = inventory(project.path(), "generations");
    let transactions_staged = inventory(project.path(), "transactions");
    assert_eq!(generations_staged.len(), generations_before.len() + 1);
    let contender_deadline = Instant::now() + DEADLINE;
    let mut contender = ChildHarness::spawn(project.path(), "contend");
    contender.marker_before("READY", "contender-ready", contender_deadline);
    contender.command("CONTEND");
    contender.marker_before(
        "RESULT GF_WRITER_BUSY",
        "contender-result",
        contender_deadline,
    );
    assert!(contender.wait("contender-exit").success());
    assert_eq!(inventory(project.path(), "generations"), generations_staged);
    assert_eq!(
        inventory(project.path(), "transactions"),
        transactions_staged
    );
    assert_eq!(
        graphforge_storage::resolve_project_generation(project.path())
            .expect("phase=contender resolve current")
            .generation_uuid(),
        current_uuid
    );

    child.kill();
    let status = child.wait("killed-staged");
    assert!(!status.success(), "killed staged child exited successfully");
    let recovered = graphforge_storage::recover_project_transactions(project.path())
        .expect("phase=killed recover transactions");
    assert_eq!(recovered.selected_generation_uuid, current_uuid);
    assert_eq!(recovered.aborted_journals, 1);
    assert_eq!(recovered.removed_generations, 1);
    let killed_journal = project
        .path()
        .join("transactions/018f0f4e-7b8c-7000-8000-000000002544.json");
    let aborted_journal_bytes =
        std::fs::read(&killed_journal).expect("phase=killed read durable aborted journal");
    let aborted_journal: serde_json::Value = serde_json::from_slice(&aborted_journal_bytes)
        .expect("phase=killed parse durable aborted journal");
    assert_eq!(aborted_journal["phase"], "ABORTED");
    assert_eq!(
        inventory(project.path(), "transactions"),
        transactions_staged
    );
    assert_eq!(inventory(project.path(), "generations"), generations_before);
    let reopened = graphforge_storage::resolve_project_generation(project.path())
        .expect("phase=killed reopen current");
    reopened
        .validate_complete_participant_inventory()
        .expect("phase=killed reopened complete");
    assert_eq!(reopened.generation_uuid(), current_uuid);
    assert_complete_generation_directories(project.path());

    let recovered_again = graphforge_storage::recover_project_transactions(project.path())
        .expect("phase=killed recover transactions idempotently");
    assert_eq!(recovered_again.selected_generation_uuid, current_uuid);
    assert_eq!(recovered_again.aborted_journals, 0);
    assert_eq!(recovered_again.removed_generations, 0);
    assert_eq!(
        std::fs::read(&killed_journal).expect("phase=killed reread idempotent aborted journal"),
        aborted_journal_bytes
    );
    assert_eq!(
        inventory(project.path(), "transactions"),
        transactions_staged
    );

    let probe = request(
        project.path(),
        Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000002548").unwrap(),
        Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000002549").unwrap(),
    );
    let staged = graphforge_storage::stage_project_generation(project.path(), &probe)
        .expect("phase=killed lock release probe");
    assert!(matches!(&staged, ProjectStageOutcome::Staged(_)));
    drop(staged);
    graphforge_storage::recover_project_transactions(project.path())
        .expect("phase=killed cleanup lock probe");
    assert_eq!(inventory(project.path(), "generations"), generations_before);
}

#[test]
fn published_child_is_visible_only_to_fresh_current_reader() {
    let project = tempfile::tempdir().expect("phase=published tempdir");
    seed(project.path());
    let pinned_generation = graphforge_storage::resolve_project_generation(project.path())
        .expect("phase=published pin initial generation");
    pinned_generation
        .validate_complete_participant_inventory()
        .expect("phase=published pinned complete");
    let initial_uuid = pinned_generation.generation_uuid();
    let generations_before = inventory(project.path(), "generations");
    let pinned_reader =
        GraphForge::new(project.path().to_str()).expect("phase=published open pinned reader");
    assert_eq!(
        names(&pinned_reader).expect("phase=pinned before"),
        ["Alpha"]
    );

    let mut child = ChildHarness::spawn(project.path(), "publish");
    child.marker("READY", "published-ready");
    child.command("PUBLISH");
    child.marker("PUBLISHED", "published-commit");
    assert!(child.wait("published-exit").success());

    assert_eq!(
        names(&pinned_reader).expect("phase=pinned after child commit"),
        ["Alpha"],
        "pinned reader followed CURRENT instead of its opened generation"
    );
    let current = graphforge_storage::resolve_project_generation(project.path())
        .expect("phase=published resolve fresh current");
    current
        .validate_complete_participant_inventory()
        .expect("phase=published current complete");
    assert_ne!(current.generation_uuid(), initial_uuid);
    let fresh_reader =
        GraphForge::new(project.path().to_str()).expect("phase=published open fresh reader");
    assert_eq!(
        names(&fresh_reader).expect("phase=fresh current read"),
        ["Alpha", "Beta"]
    );
    let generations_after = inventory(project.path(), "generations");
    assert_eq!(generations_after.len(), generations_before.len() + 1);
    assert!(generations_after.is_superset(&generations_before));
    pinned_generation
        .validate_complete_participant_inventory()
        .expect("phase=published pinned inventory remains complete");
    assert_complete_generation_directories(project.path());
}
