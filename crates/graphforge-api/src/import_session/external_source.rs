//! Registered Parquet sources, read where they are (#1898).
//!
//! Registration records what identifies the file: its canonical path, native
//! file identity, size, modification time and Parquet footer. Nothing is copied
//! into the session. Every read re-establishes that identity before and during
//! the pass, and the build's own read pass computes the whole-file SHA-256, so a
//! source that is deleted, replaced, resized, rewritten or touched is refused
//! rather than read.

use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use graphforge_core::{ApiErrorCode, GfError};
use graphforge_filesystem::{FileIdentity, StableDirectory};
use graphforge_storage::concurrency_attribution::{ObservedSha256 as Sha256, RegionScope};
use serde::{Deserialize, Serialize};
use sha2::Digest;

use super::{storage, validation};

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
/// Trailer: little-endian footer length, then the closing magic.
const PARQUET_TRAILER: u64 = 8;
const FOOTER_READ_LIMIT: u64 = 1 << 30;
/// Out-of-order bytes held while waiting for the gap before them. A source whose
/// decode order exceeds this re-reads the dropped ranges once at the end.
const PENDING_LIMIT_BYTES: usize = 64 << 20;
const GAP_READ_BYTES: u64 = 1 << 20;

/// What registration recorded about a Parquet file that stays where it is.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct ExternalSource {
    /// Canonical absolute path at registration.
    pub(super) path: PathBuf,
    volume_serial: u64,
    /// Native file identity, hex. The inode on Unix.
    file_id: String,
    pub(super) size: u64,
    mtime_secs: i64,
    mtime_nanos: u32,
    footer_len: u64,
    pub(super) footer_sha256: String,
}

/// How a source differs from what registration recorded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SourceChange {
    Missing,
    Replaced,
    Resized,
    Modified,
    DigestChanged,
}

impl SourceChange {
    const fn description(self) -> &'static str {
        match self {
            Self::Missing => "is missing",
            Self::Replaced => "was replaced (its file identity changed)",
            Self::Resized => "was resized",
            Self::Modified => "was modified",
            Self::DigestChanged => "content digest changed between reads",
        }
    }
}

/// The typed refusal for a source that no longer matches its registration:
/// `GF_NOT_FOUND` when it is gone, `GF_IDENTITY_CONFLICT` when the name now
/// refers to different content.
pub(super) fn source_changed(path: &Path, change: SourceChange, detail: &str) -> GfError {
    GfError::Api {
        code: if change == SourceChange::Missing {
            ApiErrorCode::NotFound
        } else {
            ApiErrorCode::IdentityConflict
        },
        message: format!(
            "import source {} {}{}{detail}",
            path.display(),
            change.description(),
            if detail.is_empty() { "" } else { ": " },
        ),
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

fn modified(metadata: &fs::Metadata) -> Result<(i64, u32), GfError> {
    let since = metadata
        .modified()
        .map_err(storage)?
        .duration_since(UNIX_EPOCH)
        .map_err(|_| validation("import source modification time precedes the Unix epoch"))?;
    Ok((
        i64::try_from(since.as_secs()).map_err(storage)?,
        since.subsec_nanos(),
    ))
}

fn read_exact_at(file: &File, offset: u64, bytes: &mut [u8]) -> std::io::Result<()> {
    let mut handle = file.try_clone()?;
    handle.seek(SeekFrom::Start(offset))?;
    handle.read_exact(bytes)
}

/// Length and SHA-256 of the Parquet footer, refusing a file that is not
/// plain (unencrypted) Parquet.
fn footer_identity(file: &File, size: u64) -> Result<(u64, String), GfError> {
    if size < 2 * PARQUET_MAGIC.len() as u64 + 4 {
        return Err(validation(
            "Parquet source is too short to be a Parquet file",
        ));
    }
    let mut head = [0_u8; 4];
    read_exact_at(file, 0, &mut head).map_err(storage)?;
    let mut trailer = [0_u8; PARQUET_TRAILER as usize];
    read_exact_at(file, size - PARQUET_TRAILER, &mut trailer).map_err(storage)?;
    if &head != PARQUET_MAGIC || &trailer[4..] != PARQUET_MAGIC {
        return Err(validation(
            "Parquet source lacks the PAR1 magic (not a plain Parquet file)",
        ));
    }
    let footer_len = u64::from(u32::from_le_bytes(
        trailer[..4].try_into().expect("four bytes"),
    ));
    if footer_len > FOOTER_READ_LIMIT || footer_len + PARQUET_TRAILER + 4 > size {
        return Err(validation("Parquet source footer length is out of range"));
    }
    let mut footer = vec![0_u8; usize::try_from(footer_len).map_err(storage)?];
    read_exact_at(file, size - PARQUET_TRAILER - footer_len, &mut footer).map_err(storage)?;
    Ok((footer_len, hex(&Sha256::digest(&footer))))
}

impl ExternalSource {
    /// Record the identity of `source`, which registration has already checked is
    /// a regular, non-symlink file under a path without `..`.
    pub(super) fn capture(source: &Path) -> Result<Self, GfError> {
        let path = fs::canonicalize(source).map_err(storage)?;
        let file = open_named(&path)?;
        let metadata = file.metadata().map_err(storage)?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage)?;
        let (mtime_secs, mtime_nanos) = modified(&metadata)?;
        let (footer_len, footer_sha256) = footer_identity(&file, metadata.len())?;
        Ok(Self {
            path,
            volume_serial: identity.volume_serial,
            file_id: hex(&identity.file_id),
            size: metadata.len(),
            mtime_secs,
            mtime_nanos,
            footer_len,
            footer_sha256,
        })
    }

    fn identity_matches(&self, identity: &FileIdentity) -> bool {
        identity.volume_serial == self.volume_serial && hex(&identity.file_id) == self.file_id
    }

    /// Open the source for one read pass after re-establishing its identity.
    pub(super) fn open(&self) -> Result<File, GfError> {
        let file = match open_named(&self.path) {
            Ok(file) => file,
            Err(error) => return Err(self.classify_open_failure(error)),
        };
        self.check(&file)?;
        let (footer_len, footer_sha256) = footer_identity(&file, self.size)?;
        if footer_len != self.footer_len || footer_sha256 != self.footer_sha256 {
            return Err(source_changed(
                &self.path,
                SourceChange::Modified,
                "Parquet footer differs from registration",
            ));
        }
        Ok(file)
    }

    fn classify_open_failure(&self, error: GfError) -> GfError {
        match fs::symlink_metadata(&self.path) {
            Err(io) if io.kind() == std::io::ErrorKind::NotFound => {
                source_changed(&self.path, SourceChange::Missing, "")
            }
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                source_changed(
                    &self.path,
                    SourceChange::Replaced,
                    "it is no longer a regular non-symlink file",
                )
            }
            _ => error,
        }
    }

    /// Refuse unless `file`, and the name it was opened by, are still what
    /// registration recorded. Cheap enough to run for every batch.
    pub(super) fn check(&self, file: &File) -> Result<(), GfError> {
        let named = match graphforge_filesystem::path_identity(&self.path) {
            Ok(identity) => identity,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(source_changed(&self.path, SourceChange::Missing, ""));
            }
            Err(error) => return Err(storage(error)),
        };
        let held = graphforge_filesystem::file_identity(file).map_err(storage)?;
        if !self.identity_matches(&named) || !self.identity_matches(&held) {
            return Err(source_changed(&self.path, SourceChange::Replaced, ""));
        }
        let metadata = file.metadata().map_err(storage)?;
        if metadata.len() != self.size {
            return Err(source_changed(
                &self.path,
                SourceChange::Resized,
                &format!("registered {} bytes, found {}", self.size, metadata.len()),
            ));
        }
        if modified(&metadata)? != (self.mtime_secs, self.mtime_nanos) {
            return Err(source_changed(
                &self.path,
                SourceChange::Modified,
                "modification time differs from registration",
            ));
        }
        Ok(())
    }
}

/// Open a canonical path as a regular file without following links.
fn open_named(path: &Path) -> Result<File, GfError> {
    let parent = path
        .parent()
        .ok_or_else(|| validation("Parquet source must have a parent directory"))?;
    let name = path
        .file_name()
        .ok_or_else(|| validation("Parquet source must have a file name"))?;
    StableDirectory::open(parent)
        .and_then(|directory| directory.open_child_file(name))
        .map_err(storage)
}

/// Whole-file SHA-256 assembled from the bytes a decode pass reads.
///
/// A Parquet decode reads the footer first and then each column chunk, mostly in
/// file order. Bytes that arrive in order are hashed as they are read; bytes that
/// arrive early wait (bounded) for the gap before them; whatever the decode never
/// asked for, such as the leading magic, is read from the file at the end. The
/// digest therefore costs no read pass of its own beyond those gaps.
#[derive(Clone)]
pub(super) struct SourceDigest(Arc<Mutex<DigestState>>);

struct DigestState {
    hasher: Sha256,
    hashed: u64,
    length: u64,
    pending: std::collections::BTreeMap<u64, Vec<u8>>,
    pending_bytes: usize,
    pending_limit: usize,
    overrun: bool,
}

impl DigestState {
    fn observe(&mut self, offset: u64, bytes: &[u8]) {
        let end = offset.saturating_add(bytes.len() as u64);
        if end > self.length {
            self.overrun = true;
            return;
        }
        if end <= self.hashed {
            return;
        }
        if offset <= self.hashed {
            let skip = usize::try_from(self.hashed - offset).unwrap_or(usize::MAX);
            self.hasher.update(&bytes[skip..]);
            self.hashed = end;
            self.drain();
        } else {
            self.hold(offset, bytes);
        }
    }

    /// Hold a range that arrived ahead of the hashed prefix. Over the bound, the
    /// ranges farthest from the prefix go first, since the nearest are the ones
    /// the prefix reaches next; whatever is dropped is read again by `finish`.
    fn hold(&mut self, offset: u64, bytes: &[u8]) {
        if let Some(held) = self.pending.get(&offset) {
            if held.len() >= bytes.len() {
                return;
            }
            self.pending_bytes -= held.len();
            self.pending.remove(&offset);
        }
        while self.pending_bytes + bytes.len() > self.pending_limit {
            match self.pending.last_key_value() {
                Some((&farthest, _)) if farthest > offset => {
                    let (_, dropped) = self.pending.pop_last().expect("last entry exists");
                    self.pending_bytes -= dropped.len();
                }
                _ => return,
            }
        }
        self.pending_bytes += bytes.len();
        self.pending.insert(offset, bytes.to_vec());
    }

    /// Hash every held range that now touches the hashed prefix.
    fn drain(&mut self) {
        while let Some((&start, _)) = self.pending.first_key_value() {
            if start > self.hashed {
                break;
            }
            let (start, bytes) = self.pending.pop_first().expect("first entry exists");
            self.pending_bytes -= bytes.len();
            let end = start + bytes.len() as u64;
            if end > self.hashed {
                let skip = usize::try_from(self.hashed - start).unwrap_or(usize::MAX);
                self.hasher.update(&bytes[skip..]);
                self.hashed = end;
            }
        }
    }
}

impl SourceDigest {
    pub(super) fn new(length: u64) -> Self {
        Self::with_pending_limit(length, PENDING_LIMIT_BYTES)
    }

    fn with_pending_limit(length: u64, pending_limit: usize) -> Self {
        Self(Arc::new(Mutex::new(DigestState {
            hasher: Sha256::new(),
            hashed: 0,
            length,
            pending: std::collections::BTreeMap::new(),
            pending_bytes: 0,
            pending_limit,
            overrun: false,
        })))
    }

    fn state(&self) -> std::sync::MutexGuard<'_, DigestState> {
        self.0.lock().expect("source digest lock poisoned")
    }

    pub(super) fn observe(&self, offset: u64, bytes: &[u8]) {
        self.state().observe(offset, bytes);
    }

    /// Hash what the decode did not read and return the digest as lowercase hex.
    pub(super) fn finish(&self, source: &ExternalSource, file: &File) -> Result<String, GfError> {
        let _region = RegionScope::named("source_read");
        let mut state = self.state();
        if state.overrun {
            return Err(source_changed(
                &source.path,
                SourceChange::Resized,
                "a read extended past the registered size",
            ));
        }
        let mut buffer = Vec::new();
        let mut reread = 0_u64;
        loop {
            state.drain();
            if state.hashed >= state.length {
                break;
            }
            let until = state
                .pending
                .first_key_value()
                .map_or(state.length, |(start, _)| *start);
            let want = (until - state.hashed).min(GAP_READ_BYTES);
            buffer.resize(usize::try_from(want).map_err(storage)?, 0);
            read_exact_at(file, state.hashed, &mut buffer).map_err(|error| {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    source_changed(
                        &source.path,
                        SourceChange::Resized,
                        "the file ended before its registered size",
                    )
                } else {
                    storage(error)
                }
            })?;
            state.hasher.update(&buffer);
            state.hashed += want;
            reread += want;
        }
        RegionScope::record_work("bytes", reread);
        let hasher = std::mem::replace(&mut state.hasher, Sha256::new());
        Ok(hex(&hasher.finalize()))
    }
}

/// A Parquet file whose every read is reported to a [`SourceDigest`], for
/// readers that decode row groups in parallel from separate handles.
pub(super) struct DigestingFile {
    file: File,
    size: u64,
    digest: SourceDigest,
}

impl DigestingFile {
    pub(super) const fn new(file: File, size: u64, digest: SourceDigest) -> Self {
        Self { file, size, digest }
    }
}

impl parquet::file::reader::Length for DigestingFile {
    fn len(&self) -> u64 {
        self.size
    }
}

impl parquet::file::reader::ChunkReader for DigestingFile {
    type T = std::io::BufReader<DigestingReader<File>>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        Ok(std::io::BufReader::new(DigestingReader::new(
            file,
            start,
            self.digest.clone(),
        )))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        let mut bytes = vec![0_u8; length];
        read_exact_at(&self.file, start, &mut bytes)?;
        self.digest.observe(start, &bytes);
        Ok(bytes::Bytes::from(bytes))
    }
}

/// Reports every byte range read through it to a [`SourceDigest`].
pub(super) struct DigestingReader<R> {
    inner: R,
    position: u64,
    digest: SourceDigest,
}

impl<R: Read> DigestingReader<R> {
    /// `position` is the file offset `inner` is positioned at.
    pub(super) const fn new(inner: R, position: u64, digest: SourceDigest) -> Self {
        Self {
            inner,
            position,
            digest,
        }
    }
}

impl<R: Read> Read for DigestingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.digest.observe(self.position, &buffer[..read]);
        self.position += read as u64;
        Ok(read)
    }
}

// Test-only seam between steps of a read pass, so a test can change the source
// at an exact point: `("opened", 0)` once the source is open, `("batch", n)`
// before batch `n` is checked.
#[cfg(test)]
thread_local! {
    static PASS_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(&'static str, u64)>>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
pub(super) fn set_pass_hook(hook: impl FnMut(&'static str, u64) + 'static) {
    PASS_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

#[cfg(test)]
pub(super) fn clear_pass_hook() {
    PASS_HOOK.with(|slot| *slot.borrow_mut() = None);
}

#[cfg(test)]
pub(super) fn pass_hook(stage: &'static str, index: u64) {
    PASS_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(stage, index);
        }
    });
}

#[cfg(test)]
mod tests;
