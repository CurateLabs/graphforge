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
//! An instance removes its own subdirectory and lock when it is dropped. A
//! crashed process cannot, so acquiring a new directory first reclaims every
//! entry whose lock is free: its owner is gone, because the operating system
//! releases a process's locks when it exits. A subdirectory with no lock file
//! is also stale, since an owner creates and locks its lock file before its
//! subdirectory. Entries that do not match this grammar are left alone.
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
        reclaim_abandoned(&root)?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        let lock_path = root.join(format!("{token}.lock"));
        let lock = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .map_err(storage)?;
        if !try_lock_exclusive(&lock).map_err(storage)? {
            return Err(storage("a new scratch lock is already held"));
        }
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

/// Remove every scratch entry whose owner is gone.
fn reclaim_abandoned(root: &Path) -> Result<(), GfError> {
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
            if let Some(token) = name.strip_suffix(".lock").filter(|token| is_token(token)) {
                locks.push(token.to_owned());
            }
        } else if kind.is_dir() && is_token(&name) {
            directories.push(name);
        }
    }
    for token in &locks {
        let lock_path = root.join(format!("{token}.lock"));
        let lock = match File::open(&lock_path) {
            Ok(lock) => lock,
            // Another process reclaimed or released it meanwhile.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(storage(error)),
        };
        if !try_lock_exclusive(&lock).map_err(storage)? {
            continue; // Its owner is alive.
        }
        remove_directory(&root.join(token))?;
        remove_file(&lock_path)?;
    }
    for token in directories.iter().filter(|token| !locks.contains(token)) {
        remove_directory(&root.join(token))?;
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
