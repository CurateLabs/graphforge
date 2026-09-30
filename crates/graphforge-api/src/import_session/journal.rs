//! Import-only progress records. Batch records are buffered by the filesystem;
//! framed bytes bound the flush cadence, and checkpoints/phase transitions
//! always cross durability barriers without rewriting the manifest per batch.
//! Construction's authenticated chunk receipts remain the row authority when
//! a crash loses the unflushed progress tail. This is not a durability API.

use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use graphforge_core::hash_observation::ControlSha256 as Sha256;
use graphforge_filesystem::{ObservedSync as _, StableDirectory};
use graphforge_storage::concurrency_attribution::RegionScope;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use uuid::Uuid;

use super::{GfError, ImportProgress, MANIFEST, SessionManifest, SourceRecord, storage};

const NAME: &str = "progress.journal";
const MAGIC: &[u8; 8] = b"GFIMPJ01";
const HEADER: usize = 24;
const DIGEST: usize = 32;
const MAX_RECORD: u64 = 16 * 1024 * 1024;
const SYNC_BYTES: u64 = 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    session_uuid: Uuid,
    sequence: u64,
    source_index: usize,
    source: SourceRecord,
    progress: ImportProgress,
}

pub(super) struct Journal {
    file: File,
    path: PathBuf,
    failed_write: bool,
    pending_bytes: u64,
}

impl Journal {
    pub(super) fn ensure_writable(&self) -> Result<(), GfError> {
        if self.failed_write {
            return Err(storage(
                "resume import journal after a failed write or sync",
            ));
        }
        Ok(())
    }

    pub(super) fn poison(&mut self) {
        self.failed_write = true;
    }

    pub(super) fn open(
        root: &Path,
        manifest: &SessionManifest,
        allocation: Option<&graphforge_storage::StorageAllocationOperation>,
    ) -> Result<Self, GfError> {
        let directory = StableDirectory::open(root).map_err(storage)?;
        let valid_len = scan(root, &mut manifest.clone())?;
        let mut file = match directory.open_child_file(OsStr::new(NAME)) {
            Ok(_) => directory
                .open_or_create_child_file(OsStr::new(NAME))
                .map_err(storage)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let temporary = format!("journal-{}.tmp", Uuid::new_v4());
                let mut guard = directory
                    .create_unpublished_replaceable_child(OsStr::new(&temporary))
                    .map_err(storage)?;
                let file = guard.take_file().map_err(storage)?;
                {
                    let _region = RegionScope::named("journal_sync");
                    file.observed_sync_all().map_err(storage)?;
                }
                let _region = RegionScope::named("journal_namespace_publication");
                guard.install_child(OsStr::new(NAME)).map_err(storage)?;
                guard.sync_parent().map_err(storage)?;
                guard.commit().map_err(storage)?;
                // Also make creation of the session and import-sessions names
                // durable before the first manifest becomes a checkpoint.
                if let Some(parent) = root.parent() {
                    StableDirectory::open(parent)
                        .map_err(storage)?
                        .sync()
                        .map_err(storage)?;
                    if let Some(container) = parent.parent() {
                        StableDirectory::open(container)
                            .map_err(storage)?
                            .sync()
                            .map_err(storage)?;
                    }
                }
                file
            }
            Err(error) => return Err(storage(error)),
        };
        // A crash may leave a partial final append. Never append a valid frame
        // behind that tail: subsequent recovery would stop at the old tear.
        let truncated = file.metadata().map_err(storage)?.len() != valid_len;
        if truncated {
            file.set_len(valid_len).map_err(storage)?;
        }
        file.seek(SeekFrom::End(0)).map_err(storage)?;
        let path = root.join(NAME);
        if let Some(allocation) = allocation {
            allocation.replace_file_at(&path, &file)?;
        }
        let mut journal = Self {
            file,
            path,
            failed_write: false,
            pending_bytes: 0,
        };
        // Complete recovered frames may never have crossed a barrier, and a
        // torn tail may have been truncated. Establish one durable writer
        // prefix before resetting the byte cadence on this fresh handle.
        if truncated || valid_len != 0 {
            journal.sync(allocation)?;
        }
        Ok(journal)
    }

    pub(super) fn append(
        &mut self,
        manifest: &mut SessionManifest,
        source_index: usize,
        allocation: Option<&graphforge_storage::StorageAllocationOperation>,
    ) -> Result<(), GfError> {
        let region = RegionScope::named("journal_append");
        self.ensure_writable()?;
        let sequence = manifest
            .journal_sequence
            .checked_add(1)
            .ok_or_else(|| storage("import journal sequence exhausted"))?;
        let record = Record {
            session_uuid: manifest.session_uuid,
            sequence,
            source_index,
            source: manifest.sources[source_index].clone(),
            progress: manifest.progress.clone(),
        };
        let bytes = serde_json::to_vec(&record).map_err(storage)?;
        let length = u64::try_from(bytes.len()).map_err(storage)?;
        if length > MAX_RECORD {
            return Err(storage(
                "import progress record exceeds its bounded envelope",
            ));
        }
        let mut frame = Vec::with_capacity(HEADER + bytes.len() + DIGEST);
        frame.extend_from_slice(MAGIC);
        frame.extend_from_slice(&length.to_le_bytes());
        frame.extend_from_slice(&sequence.to_le_bytes());
        frame.extend_from_slice(&bytes);
        let digest = Sha256::digest(&frame);
        frame.extend_from_slice(&digest);
        let pending_bytes = self
            .pending_bytes
            .checked_add(u64::try_from(frame.len()).map_err(storage)?)
            .ok_or_else(|| storage("import journal unflushed byte count overflow"))?;
        self.failed_write = true;
        self.file.write_all(&frame).map_err(storage)?;
        manifest.journal_sequence = sequence;
        self.pending_bytes = pending_bytes;
        if let Some(allocation) = allocation {
            allocation.replace_file_at(&self.path, &self.file)?;
        }
        self.failed_write = false;
        // The append and barrier scopes are disjoint for honest attribution.
        drop(region);
        if self.pending_bytes >= SYNC_BYTES {
            self.sync(allocation)?;
        }
        Ok(())
    }

    pub(super) fn sync(
        &mut self,
        allocation: Option<&graphforge_storage::StorageAllocationOperation>,
    ) -> Result<(), GfError> {
        let _region = RegionScope::named("journal_sync");
        self.ensure_writable()?;
        // Any failed barrier leaves durability uncertain. Only replay on a
        // fresh handle can admit the actual complete/torn tail before retry.
        self.failed_write = true;
        #[cfg(test)]
        failure("sync")?;
        self.file.observed_sync_all().map_err(storage)?;
        self.pending_bytes = 0;
        if let Some(allocation) = allocation {
            allocation.replace_file_at(&self.path, &self.file)?;
        }
        self.failed_write = false;
        Ok(())
    }
}

pub(super) fn replay(root: &Path, manifest: &mut SessionManifest) -> Result<(), GfError> {
    scan(root, manifest).map(|_| ())
}

fn scan(root: &Path, manifest: &mut SessionManifest) -> Result<u64, GfError> {
    let directory = StableDirectory::open(root).map_err(storage)?;
    let file = match directory.open_child_file(OsStr::new(NAME)) {
        Ok(file) => file,
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                && manifest.journal_sequence == 0
                && !root.join(MANIFEST).exists() =>
        {
            return Ok(0);
        }
        Err(error) => return Err(storage(error)),
    };
    let mut reader = BufReader::new(file);
    let total = reader.get_ref().metadata().map_err(storage)?.len();
    let mut offset = 0_u64;
    let mut previous = 0_u64;
    let checkpoint_sequence = manifest.journal_sequence;
    while total.saturating_sub(offset) >= HEADER as u64 {
        let mut header = [0_u8; HEADER];
        reader.read_exact(&mut header).map_err(storage)?;
        if &header[..8] != MAGIC {
            return Err(storage("incompatible import progress journal frame"));
        }
        let length = u64::from_le_bytes(header[8..16].try_into().map_err(storage)?);
        let sequence = u64::from_le_bytes(header[16..].try_into().map_err(storage)?);
        if length > MAX_RECORD {
            return Err(storage(
                "import progress frame exceeds its bounded envelope",
            ));
        }
        let frame_length = HEADER as u64 + length + DIGEST as u64;
        if total - offset < frame_length {
            break;
        }
        let mut bytes = vec![0_u8; usize::try_from(length).map_err(storage)?];
        reader.read_exact(&mut bytes).map_err(storage)?;
        let mut digest = [0_u8; DIGEST];
        reader.read_exact(&mut digest).map_err(storage)?;
        let mut hash = Sha256::new();
        hash.update(header);
        hash.update(&bytes);
        if hash.finalize()[..] != digest[..] {
            // A torn final frame beyond the durable checkpoint can be lost:
            // deterministic construction receipts will authenticate its replay.
            // Corruption of checkpointed records or an interior frame refuses.
            if previous >= checkpoint_sequence && offset + frame_length == total {
                break;
            }
            return Err(storage("import progress journal checksum mismatch"));
        }
        let record: Record = serde_json::from_slice(&bytes).map_err(storage)?;
        if record.session_uuid != manifest.session_uuid
            || record.sequence != previous + 1
            || record.sequence != sequence
        {
            return Err(storage(
                "import progress journal identity or sequence mismatch",
            ));
        }
        previous = record.sequence;
        if record.sequence > manifest.journal_sequence {
            if record.sequence != manifest.journal_sequence + 1 {
                return Err(storage("import progress journal checkpoint gap"));
            }
            let source = manifest
                .sources
                .get_mut(record.source_index)
                .ok_or_else(|| storage("import journal source absent from checkpoint"))?;
            if record.source.sequence != source.sequence
                || record.source.kind != source.kind
                || record.source.name != source.name
                || record.source.bytes != source.bytes
                || record.source.rows != source.rows
            {
                return Err(storage("import journal changes registered source identity"));
            }
            *source = record.source;
            manifest.progress = record.progress;
            manifest.journal_sequence = record.sequence;
        }
        offset += frame_length;
    }
    if previous < manifest.journal_sequence {
        return Err(storage(
            "import checkpoint is missing synchronized journal records",
        ));
    }
    Ok(offset)
}

pub(super) fn write_checkpoint(
    root: &Path,
    manifest: &SessionManifest,
    allocation: Option<&graphforge_storage::StorageAllocationOperation>,
    publication_started: &mut bool,
) -> Result<(), GfError> {
    let _region = RegionScope::named("manifest_persistence");
    let directory = StableDirectory::open(root).map_err(storage)?;
    let temporary = format!("manifest-{}.tmp", Uuid::new_v4());
    let temporary_path = root.join(&temporary);
    let mut guard = directory
        .create_unpublished_replaceable_child(OsStr::new(&temporary))
        .map_err(storage)?;
    let mut file = guard.take_file().map_err(storage)?;
    serde_json::to_writer(&mut file, manifest).map_err(storage)?;
    #[cfg(test)]
    failure("checkpoint_before_sync")?;
    file.observed_sync_all().map_err(storage)?;
    if let Some(allocation) = allocation {
        allocation.replace_file_at(&temporary_path, &file)?;
    }
    let identity = graphforge_filesystem::file_identity(&file).map_err(storage)?;
    // Replacement can report an error after the rename. From this point the
    // checkpoint may already reference its sources, even without a barrier.
    *publication_started = true;
    directory
        .replace_child(OsStr::new(&temporary), identity, OsStr::new(MANIFEST))
        .map_err(storage)?;
    #[cfg(test)]
    failure("checkpoint_after_replace")?;
    // The replacement is a complete checkpoint even if the parent barrier
    // fails: deleting it would also destroy the prior checkpoint. The guard
    // therefore owns only the preparation name. Cleanup removes that name if
    // a pre-publication failure left it behind, and syncs the retained parent
    // after successful replacement without unlinking the installed manifest.
    guard.cleanup().map_err(storage)?;
    if let Some(allocation) = allocation {
        allocation.remove_file_at(&root.join(MANIFEST))?;
        allocation.replace_file_at(&root.join(MANIFEST), &file)?;
        allocation.remove_file_at(&temporary_path)?;
    }
    Ok(())
}

/// Remove only an unreferenced source through its retained namespace authority.
pub(super) fn cleanup_source(
    destination: &Path,
    allocation: Option<&graphforge_storage::StorageAllocationOperation>,
) -> Result<(), GfError> {
    let _region = RegionScope::named("source_cleanup");
    let parent = destination
        .parent()
        .ok_or_else(|| storage("import source has no parent"))?;
    let directory = StableDirectory::open(parent).map_err(storage)?;
    let name = destination
        .file_name()
        .ok_or_else(|| storage("import source has no name"))?;
    let file = match directory.open_child_file(name) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let identity = graphforge_filesystem::file_identity(&file).map_err(storage)?;
    #[cfg(test)]
    failure("source_cleanup_before_unlink")?;
    directory
        .unlink_child_if_identity(name, identity)
        .map_err(storage)?;
    directory.sync().map_err(storage)?;
    if let Some(allocation) = allocation {
        allocation.remove_file_at(destination)?;
    }
    Ok(())
}

/// The manifest must never acknowledge a source whose rename is not durable.
/// Source registration has one barrier per source, independent of its batches.
pub(super) fn publish_source(
    temporary: &Path,
    destination: &Path,
    allocation: Option<&graphforge_storage::StorageAllocationOperation>,
) -> Result<(), GfError> {
    let _region = RegionScope::named("source_publication");
    let parent = temporary
        .parent()
        .ok_or_else(|| storage("import source temporary has no parent"))?;
    if destination.parent() != Some(parent) {
        return Err(storage("import source publication crosses directories"));
    }
    let directory = StableDirectory::open(parent).map_err(storage)?;
    let temporary_name = temporary
        .file_name()
        .ok_or_else(|| storage("import temporary has no name"))?;
    let destination_name = destination
        .file_name()
        .ok_or_else(|| storage("import source has no name"))?;
    let file = directory.open_child_file(temporary_name).map_err(storage)?;
    if let Some(allocation) = allocation {
        allocation.replace_file_at(temporary, &file)?;
    }
    directory
        .replace_child(
            temporary_name,
            graphforge_filesystem::file_identity(&file).map_err(storage)?,
            destination_name,
        )
        .map_err(storage)?;
    #[cfg(test)]
    failure("source_after_replace")?;
    directory.sync().map_err(storage)?;
    if let Some(allocation) = allocation {
        allocation.remove_file_at(destination)?;
        allocation.replace_file_at(destination, &file)?;
        allocation.remove_file_at(temporary)?;
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static FAILURES: std::cell::RefCell<Vec<&'static str>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(super) fn inject(point: &'static str) {
    FAILURES.with(|failures| failures.borrow_mut().push(point));
}

#[cfg(test)]
pub(super) fn failure(point: &str) -> Result<(), GfError> {
    FAILURES.with(|failures| {
        let mut failures = failures.borrow_mut();
        if let Some(index) = failures.iter().position(|candidate| *candidate == point) {
            failures.remove(index);
            return Err(storage(format!(
                "injected import journal {point} interruption"
            )));
        }
        Ok(())
    })?;
    Ok(())
}

#[cfg(test)]
mod tests;
