//! Inodes whose producer has already run their file barrier.
//!
//! An encoded object is hashed and made durable by the writer that produces
//! it. Installing the same inode into the content-addressed store only has to
//! name it durably (ADR 0058 decision 3: one file barrier per published
//! object). The producer's witness lives only as long as the producing
//! process, so the registry is process-wide; an installer in another process
//! finds nothing here and runs the barrier itself.
//!
//! An entry vouches for one inode at the length and modification time its
//! producer sealed, the same exact-descriptor evidence a `FileSeal` carries.
//! Taking it removes it, so a barrier is skipped once per seal.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::sync::Mutex;
use std::time::SystemTime;

type Key = (u64, [u8; 16]);

/// More entries than any build has objects; past it, producers are simply not
/// recorded and their installs run their own barrier.
const CAPACITY: usize = 1 << 20;

static SEALED: Mutex<Option<HashMap<Key, (u64, Option<SystemTime>)>>> = Mutex::new(None);

fn key(file: &File) -> io::Result<Key> {
    let identity = graphforge_filesystem::file_identity(file)?;
    Ok((identity.volume_serial, identity.file_id))
}

/// Record that `file` was just made durable by its producer.
pub(super) fn record(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    let key = key(file)?;
    let mut sealed = SEALED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let sealed = sealed.get_or_insert_with(HashMap::new);
    if sealed.len() < CAPACITY || sealed.contains_key(&key) {
        sealed.insert(key, (metadata.len(), metadata.modified().ok()));
    }
    Ok(())
}

/// Whether `file` is, unchanged, the inode its producer made durable. A `true`
/// answer is given once per recorded barrier.
///
/// # Errors
/// Returns an error if the descriptor cannot be inspected.
pub fn take(file: &File) -> io::Result<bool> {
    let key = key(file)?;
    let metadata = file.metadata()?;
    let mut sealed = SEALED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(map) = sealed.as_mut() else {
        return Ok(false);
    };
    Ok(map.remove(&key).is_some_and(|(length, modified)| {
        metadata.len() == length && metadata.modified().ok() == modified
    }))
}
