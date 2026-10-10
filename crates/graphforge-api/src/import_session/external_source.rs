//! Registered Parquet sources, read where they are (#1898).
//!
//! Registration records what identifies the file: its canonical path, native
//! file identity, size, modification time and Parquet footer. Nothing is copied
//! into the session. Every read re-establishes that identity before and during
//! the pass, and the whole-file SHA-256 is folded from the bytes the build's own
//! read pass decodes, so a source that is deleted, replaced, resized or touched
//! is refused rather than read. The pin (device, inode, size, modification time)
//! is the change detector; the digest is provenance: the content read under it.
//! A rewrite that preserves the whole pin is not detected.

use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};

use graphforge_core::{ApiErrorCode, GfError};
use graphforge_filesystem::{FileIdentity, StableDirectory};
use graphforge_storage::concurrency_attribution::{ObservedSha256 as Sha256, RegionScope};
use serde::{Deserialize, Serialize};
use sha2::Digest;

use super::{storage, validation};

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
/// Trailer: little-endian footer length, then the closing magic.
const PARQUET_TRAILER: u64 = 8;
const PARQUET_TRAILER_LEN: usize = 8;
const FOOTER_READ_LIMIT: u64 = 1 << 30;
/// A footer is read and hashed in pieces of this size.
const FOOTER_PIECE_BYTES: u64 = 1 << 20;
/// Out-of-order bytes held while waiting for the gap before them, unless the
/// source states a larger lead (see [`pending_limit`]). A decode order that
/// exceeds the bound re-reads the dropped ranges once at the end.
const PENDING_FLOOR_BYTES: u64 = 64 << 20;
const PENDING_CEILING_BYTES: u64 = 1 << 30;
const GAP_READ_BYTES: u64 = 1 << 20;
/// Conservative per-entry allowance for the Vec/credit/key and BTreeMap node
/// storage. The shared budget charges this in addition to each run's byte
/// capacity, so many tiny ranges cannot evade the aggregate ceiling.
const PENDING_RUN_OVERHEAD_BYTES: usize = 1_024;
/// Buffer of the reader a Parquet decode gets for a page header. The decode then
/// reads the page body again with `get_bytes`, so a larger buffer would read the
/// start of every page twice.
pub(super) const PAGE_HEADER_BUFFER_BYTES: usize = 1 << 10;

/// How many bytes ahead of the hashed prefix a source's decode is expected to be.
///
/// Tasks are claimed in file order, so in the usual case the tasks in flight are
/// the lowest unfinished ones and each reads at most its own span of the file: the
/// lead is about the workers times the largest task. A straggler can exceed that;
/// the bound is then enforced by dropping ranges, not by the claim order.
pub(super) fn pending_limit(workers: usize, largest_task_bytes: u64) -> usize {
    let lead = largest_task_bytes.saturating_mul(workers as u64);
    usize::try_from(lead.clamp(PENDING_FLOOR_BYTES, PENDING_CEILING_BYTES)).unwrap_or(usize::MAX)
}

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

pub(super) fn hex(bytes: &[u8]) -> String {
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
/// plain (unencrypted) Parquet. Every byte read is also offered to `digest`.
fn footer_identity(
    path: &Path,
    file: &File,
    size: u64,
    digest: Option<&SourceDigest>,
) -> Result<(u64, String), GfError> {
    // A file that ends early has been resized, whatever the read says.
    let read = |offset: u64, bytes: &mut [u8]| {
        read_exact_at(file, offset, bytes).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                source_changed(
                    path,
                    SourceChange::Resized,
                    "the file ended before its registered size",
                )
            } else {
                storage(error)
            }
        })
    };
    if size < 2 * PARQUET_MAGIC.len() as u64 + 4 {
        return Err(validation(
            "Parquet source is too short to be a Parquet file",
        ));
    }
    let observe = |offset: u64, bytes: &[u8]| {
        if let Some(digest) = digest {
            digest.observe(offset, bytes);
        }
    };
    let mut head = [0_u8; 4];
    read(0, &mut head)?;
    observe(0, &head);
    let mut trailer = [0_u8; PARQUET_TRAILER_LEN];
    read(size - PARQUET_TRAILER, &mut trailer)?;
    observe(size - PARQUET_TRAILER, &trailer);
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
    // The footer is hashed in pieces: its length is the file's to claim, and
    // nothing here needs the whole of it at once.
    let footer_start = size - PARQUET_TRAILER - footer_len;
    let mut hasher = Sha256::new();
    let mut piece =
        vec![0_u8; usize::try_from(footer_len.min(FOOTER_PIECE_BYTES)).map_err(storage)?];
    let mut done = 0_u64;
    while done < footer_len {
        let step = usize::try_from((footer_len - done).min(FOOTER_PIECE_BYTES)).map_err(storage)?;
        read(footer_start + done, &mut piece[..step])?;
        observe(footer_start + done, &piece[..step]);
        hasher.update(&piece[..step]);
        done += step as u64;
    }
    Ok((footer_len, hex(&hasher.finalize())))
}

impl ExternalSource {
    /// Record the identity of `source`, which must be a regular file reached
    /// without `..` and not a symlink.
    ///
    /// The file is opened once, without following links, and everything recorded
    /// comes from that handle: there is no earlier check of the name for a swap
    /// to slip between. Only the directory is canonicalized, so a link in the
    /// directory path resolves as usual while a link as the file itself is
    /// refused.
    pub(super) fn capture(source: &Path) -> Result<Self, GfError> {
        let directory = source
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let name = source
            .file_name()
            .ok_or_else(|| validation("Parquet source must have a file name"))?;
        let path = fs::canonicalize(directory).map_err(storage)?.join(name);
        let file = open_named(&path).map_err(|error| match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                validation("Parquet source must be a regular non-symlink file")
            }
            _ => error,
        })?;
        let metadata = file.metadata().map_err(storage)?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage)?;
        let (mtime_secs, mtime_nanos) = modified(&metadata)?;
        let (footer_len, footer_sha256) = footer_identity(&path, &file, metadata.len(), None)?;
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

    /// Bytes of the Parquet footer registration recorded.
    pub(super) fn footer_bytes(&self) -> u64 {
        self.footer_len
    }

    fn identity_matches(&self, identity: &FileIdentity) -> bool {
        identity.volume_serial == self.volume_serial && hex(&identity.file_id) == self.file_id
    }

    /// Open the source for one read pass after re-establishing its identity and
    /// its footer, offering the footer bytes it reads to `digest`.
    pub(super) fn open_observed(&self, digest: &SourceDigest) -> Result<File, GfError> {
        let file = self.reopen()?;
        self.verify_footer(&file, digest)?;
        Ok(file)
    }

    /// Read the footer through `file` and require it to be the registered one. A
    /// footer that is unreadable or malformed because the file changed after it
    /// was opened is that change, not a format error.
    pub(super) fn verify_footer(&self, file: &File, digest: &SourceDigest) -> Result<(), GfError> {
        let (footer_len, footer_sha256) =
            footer_identity(&self.path, file, self.size, Some(digest))
                .map_err(|error| self.reclassify(file, error))?;
        if footer_len != self.footer_len || footer_sha256 != self.footer_sha256 {
            return Err(source_changed(
                &self.path,
                SourceChange::Modified,
                "Parquet footer differs from registration",
            ));
        }
        Ok(())
    }

    /// Open the source for a task of a pass that already verified its footer:
    /// the file's identity, size and modification time are checked again.
    pub(super) fn reopen(&self) -> Result<File, GfError> {
        let file = match open_named(&self.path) {
            Ok(file) => file,
            Err(error) => return Err(self.classify_open_failure(error)),
        };
        self.check(&file)?;
        Ok(file)
    }

    /// The typed change a failed read was caused by, if the file no longer
    /// matches its registration; otherwise `error` unchanged.
    pub(super) fn reclassify(&self, file: &File, error: GfError) -> GfError {
        self.check(file).err().unwrap_or(error)
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
/// A Parquet decode reads the footer first and then each column chunk. Bytes that
/// arrive in order are hashed as they are read; bytes that arrive early wait
/// (bounded) for the gap before them; whatever the decode never asked for, such
/// as the page index, is read from the file at the end. The digest therefore
/// costs no read pass of its own beyond those gaps.
///
/// What it covers, precisely: the SHA-256 of the file as read under the identity
/// pin. Bytes the decode read are hashed as it read them (the first read of a byte
/// wins; a repeat is ignored). Bytes the decode did not read, or whose held range
/// the bound dropped, are read from the file when the digest completes. So the
/// digest equals the decoded bytes only while nothing was dropped and the file was
/// not rewritten under the pin; `reread_bytes` reports how much came from the
/// completion read.
///
/// Workers claim a source's tasks in file order, which keeps the lead over the
/// hashed prefix small in the usual case (see [`pending_limit`]). It does not
/// bound it: one slow task holds the prefix back while the others advance, and
/// ranges beyond the bound are dropped and read again at the end rather than held.
#[derive(Clone)]
pub(super) struct SourceDigest(Arc<Mutex<DigestState>>);

/// One nonblocking byte allowance shared by all source digests in a build.
///
/// Local per-source limits still choose which ranges to keep. This budget puts
/// one ceiling on their sum, so registering more sources cannot multiply the
/// pending-memory bound.
#[derive(Clone)]
pub(super) struct PendingDigestBudget(Arc<PendingDigestBudgetState>);

struct PendingDigestBudgetState {
    capacity: usize,
    used: AtomicUsize,
}

impl PendingDigestBudget {
    pub(super) fn new(capacity_bytes: usize) -> Self {
        Self(Arc::new(PendingDigestBudgetState {
            capacity: capacity_bytes,
            used: AtomicUsize::new(0),
        }))
    }

    pub(super) fn capacity_bytes(&self) -> usize {
        self.0.capacity
    }

    #[cfg(test)]
    pub(super) fn used_bytes(&self) -> usize {
        self.0.used.load(Ordering::Acquire)
    }

    fn try_acquire(&self, bytes: usize) -> Option<PendingDigestCredit> {
        let mut used = self.0.used.load(Ordering::Acquire);
        loop {
            let next = used.checked_add(bytes)?;
            if next > self.0.capacity {
                return None;
            }
            match self
                .0
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Some(PendingDigestCredit {
                        budget: self.0.clone(),
                        bytes,
                    });
                }
                Err(actual) => used = actual,
            }
        }
    }
}

/// Credit lives exactly as long as the held bytes in its pending run.
struct PendingDigestCredit {
    budget: Arc<PendingDigestBudgetState>,
    bytes: usize,
}

impl Drop for PendingDigestCredit {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct PendingRun {
    bytes: Vec<u8>,
    credit: PendingCreditRequest,
}

enum PendingCreditRequest {
    Untracked,
    Tracked { _credit: PendingDigestCredit },
}

struct DigestState {
    hasher: Sha256,
    hashed: u64,
    length: u64,
    /// Held runs keyed by their first byte. Runs may overlap; `drain` skips
    /// what is already hashed.
    pending: std::collections::BTreeMap<u64, PendingRun>,
    pending_bytes: usize,
    pending_limit: usize,
    pending_budget: Option<PendingDigestBudget>,
    overrun: bool,
    /// Bytes offered to the digest, counting every read of a byte.
    observed: u64,
    /// Bytes `finish` read again because the decode had not.
    reread: u64,
}

impl DigestState {
    fn observe(&mut self, offset: u64, bytes: &[u8]) {
        self.observed = self.observed.saturating_add(bytes.len() as u64);
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

    /// Drop the held runs farthest from the hashed prefix, beyond `keep_below`,
    /// until `need` more bytes fit. Whatever is dropped is read again by
    /// `finish`; the nearest runs are the ones the prefix reaches next.
    fn make_room(&mut self, keep_below: u64, need: usize) -> bool {
        while self.pending_bytes + need > self.pending_limit {
            match self.pending.last_key_value() {
                Some((&farthest, _)) if farthest > keep_below => {
                    let (_, dropped) = self.pending.pop_last().expect("last entry exists");
                    self.pending_bytes -= dropped.bytes.len();
                }
                _ => return false,
            }
        }
        true
    }

    /// Hold a range that arrived ahead of the hashed prefix. A range that
    /// continues the run before it extends that run, so a streamed column chunk
    /// is one entry however many small reads delivered it.
    fn hold(&mut self, offset: u64, bytes: &[u8]) {
        let continued = self
            .pending
            .range(..=offset)
            .next_back()
            .map(|(&start, run)| (start, start + run.bytes.len() as u64));
        if let Some((start, end)) = continued
            && end >= offset
        {
            let skip = usize::try_from(end - offset).unwrap_or(usize::MAX);
            if skip >= bytes.len() {
                return;
            }
            let extra = &bytes[skip..];
            let Some(new_len) = self.pending[&start].bytes.len().checked_add(extra.len()) else {
                return;
            };
            // Replacing a Vec temporarily keeps both allocations alive. Charge
            // the complete new run, including its node allowance, while the
            // old run's full credit remains held.
            let Some(new_credit) = self.reserve_run_credit(new_len) else {
                return;
            };
            if !self.make_room(start, extra.len()) {
                return;
            }
            let run = self
                .pending
                .get_mut(&start)
                .expect("the run was found above");
            let mut enlarged = Vec::new();
            if enlarged.try_reserve_exact(new_len).is_err() {
                return;
            }
            enlarged.extend_from_slice(&run.bytes);
            enlarged.extend_from_slice(extra);
            let old_bytes = std::mem::replace(&mut run.bytes, enlarged);
            let old_credit = std::mem::replace(&mut run.credit, new_credit);
            // Release the old allocation before its credit, keeping accounting
            // conservative throughout the replacement peak.
            drop(old_bytes);
            drop(old_credit);
            self.pending_bytes += extra.len();
            return;
        }
        let Some(credit) = self.reserve_run_credit(bytes.len()) else {
            return;
        };
        if self.make_room(offset, bytes.len()) {
            let mut held = Vec::new();
            if held.try_reserve_exact(bytes.len()).is_ok() {
                held.extend_from_slice(bytes);
                self.pending_bytes += bytes.len();
                self.pending.insert(
                    offset,
                    PendingRun {
                        bytes: held,
                        credit,
                    },
                );
            }
        }
    }

    /// An untracked request preserves the planning digest's local-only bound;
    /// `None` means shared pressure refused the run.
    fn reserve_run_credit(&self, bytes: usize) -> Option<PendingCreditRequest> {
        match &self.pending_budget {
            None => Some(PendingCreditRequest::Untracked),
            Some(budget) => {
                let charge = bytes.checked_add(PENDING_RUN_OVERHEAD_BYTES)?;
                budget
                    .try_acquire(charge)
                    .map(|credit| PendingCreditRequest::Tracked { _credit: credit })
            }
        }
    }

    /// Hash every held range that now touches the hashed prefix.
    fn drain(&mut self) {
        while let Some((&start, _)) = self.pending.first_key_value() {
            if start > self.hashed {
                break;
            }
            let (start, run) = self.pending.pop_first().expect("first entry exists");
            self.pending_bytes -= run.bytes.len();
            let end = start + run.bytes.len() as u64;
            if end > self.hashed {
                let skip = usize::try_from(self.hashed - start).unwrap_or(usize::MAX);
                self.hasher.update(&run.bytes[skip..]);
                self.hashed = end;
            }
        }
    }
}

impl SourceDigest {
    pub(super) fn new(length: u64) -> Self {
        Self::with_pending_limit(
            length,
            usize::try_from(PENDING_FLOOR_BYTES).unwrap_or(usize::MAX),
        )
    }

    pub(super) fn with_pending_limit(length: u64, pending_limit: usize) -> Self {
        Self(Arc::new(Mutex::new(DigestState {
            hasher: Sha256::new(),
            hashed: 0,
            length,
            pending: std::collections::BTreeMap::new(),
            pending_bytes: 0,
            pending_limit,
            pending_budget: None,
            overrun: false,
            observed: 0,
            reread: 0,
        })))
    }

    /// Replace the bound on held bytes. Called once, before the first task reads.
    pub(super) fn set_pending_limit(&self, limit: usize) {
        self.state().pending_limit = limit;
    }

    /// Attach the build-level aggregate bound before this source is read. The
    /// caller shares one budget across all sources in the build.
    pub(super) fn attach_pending_budget(&self, budget: PendingDigestBudget) {
        let mut state = self.state();
        assert!(
            state.pending.is_empty(),
            "digest budget must attach before reads"
        );
        state.pending_budget = Some(budget);
    }

    fn state(&self) -> std::sync::MutexGuard<'_, DigestState> {
        self.0.lock().expect("source digest lock poisoned")
    }

    /// Bytes `finish` had to read again, the cost of the digest beyond the
    /// decode's own reads. Zero until `finish` has run.
    #[cfg(test)]
    pub(super) fn reread_bytes(&self) -> u64 {
        self.state().reread
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
        state.reread = reread;
        // `observed` is every byte the pass read, counting repeats; `reread` is
        // what only the digest needed. Their difference from the file size is the
        // decode's own repeats.
        RegionScope::record_work("observed_bytes", state.observed);
        RegionScope::record_work("reread_bytes", reread);
        let hasher = std::mem::replace(&mut state.hasher, Sha256::new());
        Ok(hex(&hasher.finalize()))
    }
}

/// Reports every byte range read through it to a [`SourceDigest`], if it has one.
pub(super) struct DigestingReader<R> {
    inner: R,
    position: u64,
    digest: Option<SourceDigest>,
}

impl<R: Read> DigestingReader<R> {
    /// `position` is the file offset `inner` is positioned at.
    pub(super) const fn new(inner: R, position: u64, digest: Option<SourceDigest>) -> Self {
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
        if let Some(digest) = &self.digest {
            digest.observe(self.position, &buffer[..read]);
        }
        self.position += read as u64;
        Ok(read)
    }
}

/// An in-place Parquet file as the bulk builder's tasks read it: every range the
/// decoder asks for is offered to the source's digest as it is read, so hashing
/// needs no second pass over the bytes.
pub(super) struct ObservedFile {
    file: File,
    length: u64,
    digest: SourceDigest,
    /// Serializes every seek and read of the shared file description, so
    /// streams opened side by side hold stable virtual cursors.
    lock: Arc<Mutex<()>>,
}

impl ObservedFile {
    pub(super) fn new(file: File, digest: SourceDigest) -> Result<Self, GfError> {
        let length = file.metadata().map_err(storage)?.len();
        Ok(Self {
            file,
            length,
            digest,
            lock: Arc::new(Mutex::new(())),
        })
    }
}

impl Length for ObservedFile {
    fn len(&self) -> u64 {
        self.length
    }
}

/// One open read stream over an [`ObservedFile`]: its own virtual cursor,
/// positioned against the shared file description under the observer's lock.
/// Cloned handles share one OS cursor, so every seek and read is serialized;
/// streams held open side by side then never read each other's offsets.
pub(super) struct PositionedRead {
    file: File,
    position: u64,
    lock: Arc<Mutex<()>>,
}

impl Read for PositionedRead {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.file.seek(SeekFrom::Start(self.position))?;
        let read = self.file.read(buffer)?;
        self.position += read as u64;
        Ok(read)
    }
}

impl ChunkReader for ObservedFile {
    type T = std::io::BufReader<DigestingReader<PositionedRead>>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let file = self.file.try_clone()?;
        Ok(std::io::BufReader::with_capacity(
            PAGE_HEADER_BUFFER_BYTES,
            DigestingReader::new(
                PositionedRead {
                    file,
                    position: start,
                    lock: Arc::clone(&self.lock),
                },
                start,
                Some(self.digest.clone()),
            ),
        ))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        // A length the file cannot hold is the file's claim, not a read: refuse
        // it before allocating for it.
        if start
            .checked_add(length as u64)
            .is_none_or(|end| end > self.length)
        {
            return Err(parquet::errors::ParquetError::EOF(
                "a read extends beyond the end of the source".into(),
            ));
        }
        let mut bytes = vec![0_u8; length];
        {
            // The clone shares the streams' file description: seek and read
            // under their lock, so neither side reads the other's offset.
            let _guard = self
                .lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            read_exact_at(&self.file, start, &mut bytes)?;
        }
        self.digest.observe(start, &bytes);
        Ok(Bytes::from(bytes))
    }
}

// Test-only seam between steps of a read pass, so a test can change the source
// at an exact point: `("opened", n)` once the source is open for task `n`,
// `("batch", n)` before batch `n` is checked. Hooks are keyed by the source's
// canonical path, and a pass can run on any thread.
#[cfg(test)]
type PassHook = Box<dyn FnMut(&'static str, u64) + Send>;

#[cfg(test)]
static PASS_HOOKS: Mutex<Vec<(PathBuf, PassHook)>> = Mutex::new(Vec::new());

#[cfg(test)]
pub(super) fn set_pass_hook(path: &Path, hook: impl FnMut(&'static str, u64) + Send + 'static) {
    let path = fs::canonicalize(path).unwrap();
    let mut hooks = PASS_HOOKS.lock().unwrap();
    hooks.retain(|(held, _)| *held != path);
    hooks.push((path, Box::new(hook)));
}

#[cfg(test)]
pub(super) fn clear_pass_hook(path: &Path) {
    let path = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    PASS_HOOKS.lock().unwrap().retain(|(held, _)| *held != path);
}

#[cfg(test)]
pub(super) fn pass_hook(path: &Path, stage: &'static str, index: u64) {
    // A hook may change the source, so it runs outside the registry lock.
    let taken = {
        let mut hooks = PASS_HOOKS.lock().unwrap();
        hooks
            .iter()
            .position(|(held, _)| held == path)
            .map(|position| hooks.remove(position))
    };
    if let Some((held, mut hook)) = taken {
        hook(stage, index);
        PASS_HOOKS.lock().unwrap().push((held, hook));
    }
}

#[cfg(test)]
mod tests;
