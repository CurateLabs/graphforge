//! A killed reader's graph workspace is reclaimed by the next open of the same
//! project, and a live reader's never is (#1992).
//!
//! Each open of a durable project hydrates a private workspace inside the
//! project. A process that is killed (a cgroup OOM, a timeout's SIGKILL, a
//! crash) cannot remove it, and its disk stays allocated until something
//! reclaims it. The children here are this test binary re-executed to open the
//! project and wait, so the kill is a real SIGKILL of a real facade owner.
//!
//! A kill is `TerminateProcess` on Windows, which releases locks the same way;
//! the Windows lane covers the protocol in `workspace_hydration::tests::reclaim`.

#![cfg(unix)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use graphforge_api::GraphForge;

const CHILD_TEST: &str = "workspace_holder_child";
const PROJECT_ENV: &str = "GF_WORKSPACE_RECLAIM_PROJECT";
const SIGNALS_ENV: &str = "GF_WORKSPACE_RECLAIM_SIGNALS";
const ID_ENV: &str = "GF_WORKSPACE_RECLAIM_ID";
const WAIT: Duration = Duration::from_secs(120);

/// Child role: open the project, say so, and hold the workspace until told to
/// stop. Bounded, so a parent that dies cannot leave it running.
#[test]
fn workspace_holder_child() {
    let (Some(project), Some(signals), Some(id)) = (
        std::env::var_os(PROJECT_ENV),
        std::env::var_os(SIGNALS_ENV),
        std::env::var(ID_ENV).ok(),
    ) else {
        return; // Run as an ordinary test: nothing to hold.
    };
    let signals = PathBuf::from(signals);
    let graph = GraphForge::new(project.to_str()).expect("holder opens the project");
    graph
        .execute("MATCH (n:Person) RETURN count(n) AS total")
        .expect("holder reads through its workspace");
    std::fs::write(signals.join(format!("ready-{id}")), b"ready").expect("announce readiness");
    let stop = signals.join(format!("stop-{id}"));
    let deadline = Instant::now() + WAIT;
    while !stop.exists() {
        assert!(Instant::now() < deadline, "holder {id} was never released");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn spawn_holder(project: &Path, signals: &Path, id: &str) -> Child {
    Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env(PROJECT_ENV, project)
        .env(SIGNALS_ENV, signals)
        .env(ID_ENV, id)
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn holder")
}

fn wait_until_ready(child: &mut Child, signals: &Path, id: &str) {
    let ready = signals.join(format!("ready-{id}"));
    let deadline = Instant::now() + WAIT;
    while !ready.exists() {
        if let Some(status) = child.try_wait().expect("poll holder") {
            panic!("holder {id} exited before it was ready: {status}");
        }
        assert!(Instant::now() < deadline, "holder {id} never became ready");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn names_with_prefix(project: &Path, prefix: &str) -> BTreeSet<String> {
    std::fs::read_dir(project)
        .expect("project directory")
        .map(|entry| entry.expect("entry").file_name().into_string().unwrap())
        .filter(|name| name.starts_with(prefix))
        .collect()
}

fn workspaces(project: &Path) -> BTreeSet<String> {
    names_with_prefix(project, "graphforge-graph-workspace-")
}

fn locks(project: &Path) -> BTreeSet<String> {
    names_with_prefix(project, ".gf-workspace-lock-")
}

/// The workspace a holder added, found as the one name that was not there.
fn added(before: &BTreeSet<String>, after: &BTreeSet<String>) -> String {
    let added: Vec<_> = after.difference(before).collect();
    assert_eq!(added.len(), 1, "expected one new workspace: {added:?}");
    added[0].clone()
}

fn tree(root: &Path) -> BTreeSet<(PathBuf, u64)> {
    let mut files = BTreeSet::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).expect("workspace directory") {
            let entry = entry.expect("entry");
            let metadata = entry.metadata().expect("metadata");
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                files.insert((
                    entry.path().strip_prefix(root).unwrap().to_path_buf(),
                    metadata.len(),
                ));
            }
        }
    }
    files
}

fn lock_name(workspace: &str) -> String {
    format!(
        ".gf-workspace-lock-{}",
        workspace
            .strip_prefix("graphforge-graph-workspace-")
            .unwrap()
    )
}

#[test]
fn a_killed_reader_is_reclaimed_and_a_live_reader_is_not() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let signals = root.path().join("signals");
    std::fs::create_dir(&signals).unwrap();

    {
        let graph = GraphForge::new(project.to_str()).unwrap();
        graph
            .execute("CREATE (:Person {name: 'Ada'}), (:Person {name: 'Grace'})")
            .unwrap();
    }
    assert!(
        workspaces(&project).is_empty(),
        "closing removes workspaces"
    );
    assert!(locks(&project).is_empty(), "closing removes locks");

    let mut doomed = spawn_holder(&project, &signals, "doomed");
    wait_until_ready(&mut doomed, &signals, "doomed");
    let doomed_workspace = added(&BTreeSet::new(), &workspaces(&project));
    let mut survivor = spawn_holder(&project, &signals, "survivor");
    wait_until_ready(&mut survivor, &signals, "survivor");
    let both = workspaces(&project);
    let survivor_workspace = added(&BTreeSet::from([doomed_workspace.clone()]), &both);
    let survivor_files = tree(&project.join(&survivor_workspace));
    assert!(!survivor_files.is_empty(), "the survivor hydrated payloads");

    doomed.kill().expect("SIGKILL the doomed holder");
    doomed.wait().expect("reap the doomed holder");
    assert!(
        project.join(&doomed_workspace).is_dir(),
        "the kill leaks the workspace: nothing ran to remove it"
    );
    assert!(project.join(lock_name(&doomed_workspace)).is_file());

    let graph = GraphForge::new(project.to_str()).expect("the parent opens the project");
    graph
        .execute("MATCH (n:Person) RETURN count(n) AS total")
        .unwrap();

    let after = workspaces(&project);
    assert!(
        !after.contains(&doomed_workspace),
        "the dead owner's workspace is reclaimed by the next open: {after:?}"
    );
    assert!(
        !project.join(lock_name(&doomed_workspace)).exists(),
        "and so is its lock"
    );
    assert!(
        after.contains(&survivor_workspace),
        "a live owner's workspace is untouched: {after:?}"
    );
    assert_eq!(
        tree(&project.join(&survivor_workspace)),
        survivor_files,
        "down to its last file"
    );
    assert!(project.join(lock_name(&survivor_workspace)).is_file());
    assert_eq!(after.len(), 2, "the survivor's and the parent's: {after:?}");
    assert_eq!(locks(&project).len(), 2);

    // A clean exit removes the survivor's own workspace and lock.
    std::fs::write(signals.join("stop-survivor"), b"stop").unwrap();
    let status = survivor.wait().expect("the survivor exits");
    assert!(status.success(), "{status}");
    assert!(!project.join(&survivor_workspace).exists());
    assert!(!project.join(lock_name(&survivor_workspace)).exists());
    drop(graph);
    assert!(workspaces(&project).is_empty());
    assert!(locks(&project).is_empty());
}
