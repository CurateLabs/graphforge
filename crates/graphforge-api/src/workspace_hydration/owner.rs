//! Ownership and crash reclamation for private graph workspaces (#1992).
//!
//! Every open of a durable project hydrates a private workspace,
//! `graphforge-graph-workspace-<token>`, directly inside the project's
//! container root. Several processes may open one project at once, so each
//! workspace has an owner that holds an exclusive advisory lock on a sibling
//! file for as long as the workspace exists:
//!
//! ```text
//! <container>/
//!     .gf-workspace-lock-3f2c9a41d07e        held by the live owner of the workspace below
//!     graphforge-graph-workspace-3f2c9a41d07e/
//! ```
//!
//! The lock lives beside the workspace, never inside it: a workspace is a
//! graph tree whose every file is inventoried, so an extra file would be an
//! undeclared payload. This is the protocol of `graphforge_storage::query_spill`
//! (#1595). An owner creates its lock under a temporary name, locks it, and
//! only then renames it to its final name, so every visible lock file is
//! already held if its owner is alive. It creates the workspace directory after
//! the rename and, when dropped, removes the directory before the lock file.
//!
//! A killed process cannot clean up, so creating a workspace first reclaims
//! every workspace whose lock is free: the operating system releases a
//! process's locks when it exits, so a lock that can be taken has no owner.
//! A live owner's lock cannot be taken, so its workspace is never touched.
//!
//! Workspaces created by builds before this protocol have no lock file. There
//! is no signal that separates a live one from an abandoned one, so they are
//! left alone and are never reclaimed here. A workspace whose directory cannot
//! be fully removed keeps its lock file, so a later open retries it.

use fs4::{FileExt, TryLockError};
use graphforge_core::GfError;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

const WORKSPACE_PREFIX: &str = "graphforge-graph-workspace-";
const LOCK_PREFIX: &str = ".gf-workspace-lock-";
const TEMPORARY_SUFFIX: &str = ".new";
/// Hex digits of a workspace token. A token another owner already holds is
/// refused when its lock is claimed, and a fresh token is drawn.
const TOKEN_LEN: usize = 12;
const CLAIM_ATTEMPTS: usize = 16;

/// What keeps a facade's graph workspace alive and removes it afterwards.
#[derive(Debug)]
pub(crate) enum WorkspaceOwner {
    /// A private tree in the project container, locked for its lifetime.
    Private(PrivateWorkspace),
    /// A scratch guard for a pinned alias, which owns no tree of its own.
    Guard(tempfile::TempDir),
}

impl WorkspaceOwner {
    pub(crate) fn path(&self) -> &Path {
        match self {
            Self::Private(workspace) => &workspace.directory,
            Self::Guard(guard) => guard.path(),
        }
    }
}

/// One private workspace directory and the lock that marks its owner alive.
#[derive(Debug)]
pub(crate) struct PrivateWorkspace {
    directory: PathBuf,
    lock_path: PathBuf,
    // Held open, and so locked, until the owner is dropped.
    _lock: File,
}

fn storage(action: &str, error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("failed to {action} graph workspace: {error}"))
}

fn try_lock_exclusive(file: &File) -> io::Result<bool> {
    match FileExt::try_lock(file) {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

/// Whether `token` is a workspace token: lowercase hex of the exact length.
fn is_token(token: &str) -> bool {
    token.len() == TOKEN_LEN
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn workspace_directory(container: &Path, token: &str) -> PathBuf {
    container.join(format!("{WORKSPACE_PREFIX}{token}"))
}

impl PrivateWorkspace {
    /// Reclaim abandoned workspaces in `container`, then create and lock a new
    /// one.
    pub(crate) fn create(container: &Path) -> Result<Self, GfError> {
        reclaim_abandoned(container);
        for _ in 0..CLAIM_ATTEMPTS {
            let token = uuid::Uuid::new_v4().simple().to_string()[..TOKEN_LEN].to_owned();
            let Some((lock_path, lock)) = claim_lock(container, &token)? else {
                continue;
            };
            let directory = workspace_directory(container, &token);
            match create_private_directory(&directory) {
                Ok(()) => {
                    return Ok(Self {
                        directory,
                        lock_path,
                        _lock: lock,
                    });
                }
                Err(error) => {
                    let _ = std::fs::remove_file(&lock_path);
                    if error.kind() != io::ErrorKind::AlreadyExists {
                        return Err(storage("create", error));
                    }
                }
            }
        }
        Err(storage("claim", "no unused workspace token"))
    }
}

impl Drop for PrivateWorkspace {
    fn drop(&mut self) {
        // The directory first: while the lock file exists and is held, no
        // other process reclaims it. A directory that cannot be removed keeps
        // its lock file, which becomes free when `_lock` closes after this, so
        // the next open reclaims the remainder.
        if remove_workspace(&self.directory).is_ok() {
            let _ = std::fs::remove_file(&self.lock_path);
        }
    }
}

#[cfg(unix)]
fn create_private_directory(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new().mode(0o700).create(path)
}

#[cfg(not(unix))]
fn create_private_directory(path: &Path) -> io::Result<()> {
    std::fs::create_dir(path)
}

/// Create, lock and publish one lock file. `None` when the token is taken, or
/// when another process reclaimed the temporary file before it was locked or
/// published.
fn claim_lock(container: &Path, token: &str) -> Result<Option<(PathBuf, File)>, GfError> {
    let temporary = container.join(format!("{LOCK_PREFIX}{token}{TEMPORARY_SUFFIX}"));
    let lock_path = container.join(format!("{LOCK_PREFIX}{token}"));
    let lock = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
    {
        Ok(lock) => lock,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => return Ok(None),
        Err(error) => return Err(storage("create the lock of", error)),
    };
    if !try_lock_exclusive(&lock).map_err(|error| storage("lock", error))? {
        return Ok(None);
    }
    // The temporary must still be the file this handle locked.
    let identity =
        graphforge_filesystem::file_identity(&lock).map_err(|error| storage("identify", error))?;
    match graphforge_filesystem::path_identity(&temporary) {
        Ok(named) if named == identity => {}
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage("identify", error)),
    }
    // A published lock must not replace an existing one.
    if lock_path.exists() {
        let _ = std::fs::remove_file(&temporary);
        return Ok(None);
    }
    match std::fs::rename(&temporary, &lock_path) {
        Ok(()) => Ok(Some((lock_path, lock))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage("publish the lock of", error)),
    }
}

/// Remove every workspace in `container` whose owner is gone. Best effort:
/// an entry that cannot be examined or removed is skipped, and never stops the
/// caller creating its own workspace. Lock files that do not match this
/// grammar, and directories without a lock file, are left alone.
fn reclaim_abandoned(container: &Path) {
    let Ok(entries) = std::fs::read_dir(container) else {
        return;
    };
    for entry in entries.flatten() {
        // Not followed: a link is never an owner's lock.
        if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(rest) = name.strip_prefix(LOCK_PREFIX) else {
            continue;
        };
        let (token, temporary) = rest
            .strip_suffix(TEMPORARY_SUFFIX)
            .map_or((rest, false), |token| (token, true));
        if !is_token(token) {
            continue;
        }
        let lock_path = entry.path();
        let Ok(lock) = File::open(&lock_path) else {
            continue; // Reclaimed or released meanwhile.
        };
        if !matches!(try_lock_exclusive(&lock), Ok(true)) {
            continue; // Its owner is alive.
        }
        // A temporary lock never fronts a workspace: its owner creates the
        // directory only after publishing the lock.
        if !temporary && remove_workspace(&workspace_directory(container, token)).is_err() {
            continue; // Keep the lock file so a later open retries.
        }
        let _ = std::fs::remove_file(&lock_path);
    }
}

fn remove_workspace(directory: &Path) -> io::Result<()> {
    match std::fs::remove_dir_all(directory) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}
