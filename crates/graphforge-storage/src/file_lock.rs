//! Compatibility wrappers for fs4 1.x file locks.
//!
//! fs4 1.0 flattened `fs_std::FileExt` to the crate root and changed try-lock
//! to return `Result<(), TryLockError>` instead of `Result<bool>`. These
//! helpers preserve the previous bool-shaped call sites used across project
//! publication / recovery.

use std::fs::File;
use std::io;

use fs4::{FileExt, TryLockError};

pub(crate) fn try_lock_exclusive(file: &File) -> io::Result<bool> {
    match FileExt::try_lock(file) {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

pub(crate) fn try_lock_shared(file: &File) -> io::Result<bool> {
    match FileExt::try_lock_shared(file) {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

pub(crate) fn lock_exclusive(file: &File) -> io::Result<()> {
    FileExt::lock(file)
}

pub(crate) fn lock_shared(file: &File) -> io::Result<()> {
    FileExt::lock_shared(file)
}

pub(crate) fn unlock(file: &File) -> io::Result<()> {
    FileExt::unlock(file)
}
