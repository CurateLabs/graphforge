//! Crash reclamation of private graph workspaces (#1992), without a process
//! kill: the post-crash state is a lock file nobody holds beside a directory.
//! The SIGKILL variant through the facade is `tests/graph_workspace_reclaim.rs`.

use super::*;
use fs4::{FileExt, TryLockError};
use std::collections::BTreeSet;
use std::fs::File;

const ABANDONED: &str = "0123456789ab";
const LIVE: &str = "ba9876543210";
const LEGACY: &str = "graphforge-graph-workspace-legacy0";

fn directory(container: &Path, token: &str) -> PathBuf {
    container.join(format!("graphforge-graph-workspace-{token}"))
}

fn lock_file(container: &Path, token: &str) -> PathBuf {
    container.join(format!(".gf-workspace-lock-{token}"))
}

/// A workspace and lock as a killed owner leaves them: nothing holds the lock.
fn abandoned_workspace(container: &Path, token: &str) {
    let directory = directory(container, token);
    std::fs::create_dir_all(directory.join("topology")).unwrap();
    std::fs::write(directory.join("topology").join("nodes.parquet"), b"payload").unwrap();
    std::fs::write(lock_file(container, token), b"").unwrap();
}

/// A workspace whose owner is alive: it holds the lock for as long as it lives.
fn live_workspace(container: &Path, token: &str) -> File {
    abandoned_workspace(container, token);
    let lock = File::open(lock_file(container, token)).unwrap();
    assert!(
        FileExt::try_lock(&lock).is_ok(),
        "the fixture owner takes its lock"
    );
    lock
}

fn entries(container: &Path) -> BTreeSet<String> {
    std::fs::read_dir(container)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect()
}

fn is_locked_elsewhere(path: &Path) -> bool {
    let probe = File::open(path).unwrap();
    matches!(FileExt::try_lock(&probe), Err(TryLockError::WouldBlock))
}

#[test]
fn creating_a_workspace_reclaims_exactly_the_workspaces_whose_owner_is_gone() {
    let container = tempfile::tempdir().unwrap();
    let container = container.path();
    abandoned_workspace(container, ABANDONED);
    let live_lock = live_workspace(container, LIVE);
    // Built before locks existed, or not a workspace token at all: no signal
    // says the owner is gone, so none of these is reclaimed.
    std::fs::create_dir(container.join(LEGACY)).unwrap();
    std::fs::write(container.join(LEGACY).join("keep"), b"legacy").unwrap();
    abandoned_workspace(container, "not-a-token");
    std::fs::write(container.join(".gf-workspace-lock-unrelated"), b"").unwrap();
    let before = entries(container);

    let owned = PrivateWorkspace::create(container).unwrap();

    let after = entries(container);
    let removed: BTreeSet<String> = before.difference(&after).cloned().collect();
    assert_eq!(
        removed,
        BTreeSet::from([
            format!("graphforge-graph-workspace-{ABANDONED}"),
            format!(".gf-workspace-lock-{ABANDONED}"),
        ]),
        "only the dead owner's workspace and lock are reclaimed"
    );
    let added: Vec<_> = after.difference(&before).cloned().collect();
    assert_eq!(added.len(), 2, "{added:?}: the new workspace and its lock");
    assert!(
        directory(container, LIVE)
            .join("topology")
            .join("nodes.parquet")
            .is_file(),
        "a live owner's workspace is untouched"
    );
    assert!(container.join(LEGACY).join("keep").is_file());
    assert!(is_locked_elsewhere(&lock_file(container, LIVE)));
    drop(owned);
    drop(live_lock);

    // Once its owner is gone, the next open reclaims what was live.
    let next = PrivateWorkspace::create(container).unwrap();
    assert!(!directory(container, LIVE).exists());
    assert!(!lock_file(container, LIVE).exists());
    drop(next);
    assert_eq!(
        entries(container),
        BTreeSet::from([
            LEGACY.to_owned(),
            "graphforge-graph-workspace-not-a-token".to_owned(),
            ".gf-workspace-lock-not-a-token".to_owned(),
            ".gf-workspace-lock-unrelated".to_owned(),
        ]),
        "dropping an owner removes its directory and then its lock"
    );
}

#[test]
fn a_workspace_is_locked_from_creation_until_its_owner_drops() {
    let container = tempfile::tempdir().unwrap();
    let container = container.path();
    let first = PrivateWorkspace::create(container).unwrap();
    let second = PrivateWorkspace::create(container).unwrap();
    let locks: Vec<PathBuf> = entries(container)
        .into_iter()
        .filter(|name| name.starts_with(".gf-workspace-lock-"))
        .map(|name| container.join(name))
        .collect();
    assert_eq!(locks.len(), 2);
    for lock in &locks {
        assert!(is_locked_elsewhere(lock), "{}", lock.display());
    }
    // A third open reclaims nothing: both owners are alive.
    let third = PrivateWorkspace::create(container).unwrap();
    assert_eq!(
        entries(container)
            .iter()
            .filter(|name| name.starts_with("graphforge-graph-workspace-"))
            .count(),
        3
    );
    drop((first, second, third));
    assert!(entries(container).is_empty());
}
