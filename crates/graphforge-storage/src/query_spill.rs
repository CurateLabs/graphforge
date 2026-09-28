//! Project-owned scratch space for query spill (#1595).
//!
//! A durable project's queries spill into `<project>/.graphforge-query-spill/`
//! instead of the operating system's temporary directory, which is RAM-backed
//! on some hosts and outside every budget. Several processes may open one
//! project at once, so each open instance owns one subdirectory, named by a
//! random token, and holds an exclusive lock on a sibling `<token>.lock` file
//! for as long as it lives:
//!
//! ```text
//! .graphforge-query-spill/
//!     3f2c…e9.lock   held by the live instance that owns 3f2c…e9/
//!     3f2c…e9/       DataFusion's spill files for that instance's queries
//! ```
//!
//! An owner creates its lock file under a temporary name (`<token>.lock.new`),
//! locks it, and only then renames it to `<token>.lock`, so every visible
//! `<token>.lock` is already held by its owner if the owner is alive. It
//! creates its subdirectory after the rename and, when dropped, removes the
//! subdirectory before the lock.
//!
//! A crashed process cannot clean up, so acquiring a new directory first
//! reclaims every entry whose lock is free: its owner is gone, because the
//! operating system releases a process's locks when it exits. A subdirectory
//! is also stale when its lock file is absent at the moment it is examined,
//! because an owner's lock exists for as long as its subdirectory does.
//! Reclaiming other owners' leftovers is housekeeping: an entry that cannot be
//! removed is skipped, and never stops this instance acquiring its own
//! scratch. Entries that do not match this grammar are left alone.
//!
//! Scratch is transient: it is never recovery authority and holds nothing a
//! query needs after it finishes.

use crate::file_lock::try_lock_exclusive;
use graphforge_core::GfError;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

/// The scratch root under a project directory.
pub const QUERY_SPILL_DIR: &str = ".graphforge-query-spill";

/// Default cap on one query's spill bytes in the project scratch directory.
pub const DEFAULT_QUERY_SPILL_MAX_BYTES: u64 = 8 << 30;

/// One instance's scratch subdirectory, held for the instance's lifetime.
#[derive(Debug)]
pub struct QuerySpillDirectory {
    directory: PathBuf,
    lock_path: PathBuf,
    // Held open, and so locked, until drop.
    _lock: File,
}

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("query spill directory: {error}"))
}

/// Whether `name` is an owner token: 32 lowercase hex digits.
fn is_token(name: &str) -> bool {
    name.len() == 32
        && name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl QuerySpillDirectory {
    /// Reclaim abandoned scratch under `project`, then create and lock this
    /// instance's own subdirectory.
    ///
    /// # Errors
    /// Refuses a scratch root that is not a real directory, and returns any
    /// filesystem error from reclaiming or creating scratch.
    pub fn acquire(project: &Path) -> Result<Self, GfError> {
        let root = project.join(QUERY_SPILL_DIR);
        match std::fs::symlink_metadata(&root) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(storage("the scratch root is not a directory")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::create_dir(&root) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        if !std::fs::symlink_metadata(&root).map_err(storage)?.is_dir() {
                            return Err(storage("the scratch root is not a directory"));
                        }
                    }
                    Err(error) => return Err(storage(error)),
                }
            }
            Err(error) => return Err(storage(error)),
        }
        reclaim_abandoned(&root);
        // A reclaimer can remove a temporary lock file between its creation
        // and its lock; the rename then fails and a fresh token is tried.
        let mut attempt = 0;
        let (token, lock_path, lock) = loop {
            attempt += 1;
            match claim_lock(&root)? {
                Some(claimed) => break claimed,
                None if attempt < 8 => {}
                None => return Err(storage("could not claim a scratch lock")),
            }
        };
        let directory = root.join(&token);
        if let Err(error) = std::fs::create_dir(&directory) {
            let _ = std::fs::remove_file(&lock_path);
            return Err(storage(error));
        }
        Ok(Self {
            directory,
            lock_path,
            _lock: lock,
        })
    }

    /// The directory this instance's queries spill into.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.directory
    }
}

impl Drop for QuerySpillDirectory {
    fn drop(&mut self) {
        // The subdirectory first: while the lock file exists and is held, no
        // other process reclaims it.
        let _ = std::fs::remove_dir_all(&self.directory);
        let _ = std::fs::remove_file(&self.lock_path);
    }
}

/// Create, lock and publish one `<token>.lock`. `None` when another process
/// reclaimed the temporary file before it was locked or published.
fn claim_lock(root: &Path) -> Result<Option<(String, PathBuf, File)>, GfError> {
    let token = uuid::Uuid::new_v4().simple().to_string();
    let temporary = root.join(format!("{token}.lock.new"));
    let lock_path = root.join(format!("{token}.lock"));
    let lock = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(storage)?;
    #[cfg(test)]
    tests::between_create_and_lock(root, &temporary);
    if !try_lock_exclusive(&lock).map_err(storage)? {
        return Ok(None);
    }
    // The temporary must still be the file this handle locked.
    let identity = graphforge_filesystem::file_identity(&lock).map_err(storage)?;
    match graphforge_filesystem::path_identity(&temporary) {
        Ok(named) if named == identity => {}
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    }
    match std::fs::rename(&temporary, &lock_path) {
        Ok(()) => Ok(Some((token, lock_path, lock))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage(error)),
    }
}

/// Remove every scratch entry whose owner is gone. Best effort: an entry that
/// cannot be examined or removed is skipped.
fn reclaim_abandoned(root: &Path) {
    let _ = reclaim_abandoned_entries(root);
}

fn reclaim_abandoned_entries(root: &Path) -> Result<(), GfError> {
    let mut locks = Vec::new();
    let mut directories = Vec::new();
    for entry in std::fs::read_dir(root).map_err(storage)? {
        let entry = entry.map_err(storage)?;
        // Not followed: a link is never an owner's entry.
        let kind = entry.file_type().map_err(storage)?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if kind.is_file() {
            let lock = name
                .strip_suffix(".lock")
                .or_else(|| name.strip_suffix(".lock.new"))
                .filter(|token| is_token(token));
            if lock.is_some() {
                locks.push(name);
            }
        } else if kind.is_dir() && is_token(&name) {
            directories.push(name);
        }
    }
    for name in &locks {
        let lock_path = root.join(name);
        let Ok(lock) = File::open(&lock_path) else {
            continue; // Reclaimed, released or unreadable meanwhile.
        };
        if !matches!(try_lock_exclusive(&lock), Ok(true)) {
            continue; // Its owner is alive.
        }
        if let Some(token) = name.strip_suffix(".lock") {
            let _ = remove_directory(&root.join(token));
        }
        let _ = remove_file(&lock_path);
    }
    for token in &directories {
        // Examined now, not from the listing: an owner's lock exists for as
        // long as its subdirectory does.
        match std::fs::symlink_metadata(root.join(format!("{token}.lock"))) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let _ = remove_directory(&root.join(token));
            }
            _ => {}
        }
    }
    Ok(())
}

fn remove_directory(path: &Path) -> Result<(), GfError> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage(error)),
    }
}

fn remove_file(path: &Path) -> Result<(), GfError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage(error)),
    }
}

#[cfg(test)]
mod tests;
