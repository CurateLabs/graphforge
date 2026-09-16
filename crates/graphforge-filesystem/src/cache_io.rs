//! Bounded file-cache readers, durable writers, and release evidence.

use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

/// Default maximum dirty/file-cache window retained by one durable writer.
pub const DEFAULT_CACHE_RELEASE_WINDOW_BYTES: u64 = 64 * 1024 * 1024;

/// Derive one non-zero per-stream window from the shared 64 MiB operation budget.
///
/// # Errors
/// Returns an error for zero streams, an unrepresentable stream count, or when
/// the stream count is too large to receive even one byte.
pub fn cache_release_window_for_streams(active_streams: usize) -> io::Result<NonZeroU64> {
    let active_streams = u64::try_from(active_streams)
        .map_err(|_| io::Error::other("cache-release stream count overflow"))?;
    if active_streams == 0 {
        return Err(io::Error::other("cache-release stream count is zero"));
    }
    let window = DEFAULT_CACHE_RELEASE_WINDOW_BYTES
        .checked_div(active_streams)
        .and_then(NonZeroU64::new)
        .ok_or_else(|| io::Error::other("cache-release operation budget is exhausted"))?;
    let aggregate = window
        .get()
        .checked_mul(active_streams)
        .ok_or_else(|| io::Error::other("cache-release aggregate window overflow"))?;
    if aggregate > DEFAULT_CACHE_RELEASE_WINDOW_BYTES {
        return Err(io::Error::other(
            "cache-release aggregate window exceeds operation budget",
        ));
    }
    Ok(window)
}

/// Validate the aggregate configured windows of the streams actually opened
/// by one operation.
///
/// # Errors
/// Returns an error on arithmetic overflow, an empty stream set, or an
/// aggregate above the shared 64 MiB operation budget.
pub fn validate_cache_release_operation_windows(windows: &[NonZeroU64]) -> io::Result<u64> {
    if windows.is_empty() {
        return Err(io::Error::other(
            "cache-release operation has no active streams",
        ));
    }
    let aggregate = windows.iter().try_fold(0_u64, |sum, window| {
        sum.checked_add(window.get())
            .ok_or_else(|| io::Error::other("cache-release aggregate window overflow"))
    })?;
    if aggregate > DEFAULT_CACHE_RELEASE_WINDOW_BYTES {
        return Err(io::Error::other(
            "cache-release aggregate window exceeds operation budget",
        ));
    }
    Ok(aggregate)
}

/// Result of one file-level page-cache release request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileCacheReleaseOutcome {
    /// The operating system accepted the release request.
    Released,
    /// This target has no supported file-level cache-release primitive.
    Unsupported,
}

/// Content-free evidence emitted by a durable cache-bounded writer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FileCacheReleaseEvidence {
    /// Synchronization barriers completed by the writer.
    pub sync_operations: u64,
    /// File-level cache-release requests accepted by the operating system.
    pub release_operations: u64,
    /// File-level cache-release requests not supported on this target.
    pub unsupported_operations: u64,
    /// Bytes covered by accepted release requests.
    pub released_bytes: u64,
    /// Largest byte window synchronized before a release request.
    pub peak_window_bytes: u64,
}

/// Shared evidence and deferred-error channel for cache-releasing readers.
#[derive(Clone, Debug, Default)]
pub struct FileCacheReleaseTracker {
    state: Arc<Mutex<FileCacheReleaseTrackerState>>,
}

#[derive(Debug, Default)]
struct FileCacheReleaseTrackerState {
    evidence: FileCacheReleaseEvidence,
    deferred_error: Option<(io::ErrorKind, String)>,
}

impl FileCacheReleaseTracker {
    /// Return aggregate evidence from every reader attached to this tracker.
    #[must_use]
    pub fn evidence(&self) -> FileCacheReleaseEvidence {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .evidence
    }

    /// Surface an advisory failure captured while a reader was being dropped.
    ///
    /// # Errors
    /// Returns the first deferred cache-release error.
    pub fn check_error(&self) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.deferred_error.take() {
            Some((kind, message)) => Err(io::Error::new(kind, message)),
            None => Ok(()),
        }
    }

    fn account(&self, outcome: FileCacheReleaseOutcome, bytes: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.evidence.peak_window_bytes = state.evidence.peak_window_bytes.max(bytes);
        match outcome {
            FileCacheReleaseOutcome::Released => {
                state.evidence.release_operations =
                    state.evidence.release_operations.saturating_add(1);
                state.evidence.released_bytes = state.evidence.released_bytes.saturating_add(bytes);
            }
            FileCacheReleaseOutcome::Unsupported => {
                state.evidence.unsupported_operations =
                    state.evidence.unsupported_operations.saturating_add(1);
            }
        }
    }

    fn defer(&self, error: &io::Error) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.deferred_error.is_none() {
            state.deferred_error = Some((error.kind(), error.to_string()));
        }
    }
}

/// Owned sequential reader that releases completed, already-consumed windows.
#[derive(Debug)]
pub struct FileCacheReleasingReader {
    file: File,
    window_bytes: NonZeroU64,
    pending_offset: u64,
    pending_bytes: u64,
    tracker: FileCacheReleaseTracker,
    finished: bool,
}

impl FileCacheReleasingReader {
    /// Wrap `file` at its current descriptor offset with the default window.
    ///
    /// # Errors
    /// Returns an error when the current descriptor offset cannot be observed.
    pub fn new(file: File) -> io::Result<Self> {
        Self::with_window_bytes(
            file,
            NonZeroU64::new(DEFAULT_CACHE_RELEASE_WINDOW_BYTES)
                .expect("default cache-release window is non-zero"),
            FileCacheReleaseTracker::default(),
        )
    }

    /// Wrap `file` at its current descriptor offset and attach shared evidence.
    ///
    /// # Errors
    /// Returns an error when the current descriptor offset cannot be observed.
    pub fn with_tracker(file: File, tracker: FileCacheReleaseTracker) -> io::Result<Self> {
        Self::with_window_bytes(
            file,
            NonZeroU64::new(DEFAULT_CACHE_RELEASE_WINDOW_BYTES)
                .expect("default cache-release window is non-zero"),
            tracker,
        )
    }

    /// Wrap `file` with an explicit non-zero window and evidence tracker.
    ///
    /// # Errors
    /// Returns an error when the current descriptor offset cannot be observed.
    pub fn with_window_bytes(
        mut file: File,
        window_bytes: NonZeroU64,
        tracker: FileCacheReleaseTracker,
    ) -> io::Result<Self> {
        let pending_offset = file.stream_position()?;
        Ok(Self {
            file,
            window_bytes,
            pending_offset,
            pending_bytes: 0,
            tracker,
            finished: false,
        })
    }

    /// Release the final consumed partial window and surface deferred failures.
    ///
    /// # Errors
    /// Returns an error when a supported release request fails.
    pub fn finish(&mut self) -> io::Result<FileCacheReleaseEvidence> {
        self.release_pending()?;
        self.finished = true;
        self.tracker.check_error()?;
        Ok(self.tracker.evidence())
    }

    /// Borrow the shared aggregate evidence tracker.
    #[must_use]
    pub fn tracker(&self) -> FileCacheReleaseTracker {
        self.tracker.clone()
    }

    /// Return this reader's configured release window.
    #[must_use]
    pub const fn window_bytes(&self) -> NonZeroU64 {
        self.window_bytes
    }

    /// Borrow the underlying file.
    #[must_use]
    pub const fn file(&self) -> &File {
        &self.file
    }

    fn release_pending(&mut self) -> io::Result<()> {
        if self.pending_bytes == 0 {
            return Ok(());
        }
        let bytes = self.pending_bytes;
        let end = self
            .pending_offset
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("cache-release reader offset overflow"))?;
        let outcome = release_file_cache(&self.file, self.pending_offset, bytes)?;
        self.tracker.account(outcome, bytes);
        self.pending_offset = end;
        self.pending_bytes = 0;
        Ok(())
    }
}

impl Read for FileCacheReleasingReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() || self.finished {
            return Ok(0);
        }
        if file_cache_release_supported() && self.pending_bytes == self.window_bytes.get() {
            self.release_pending()?;
        }
        let limit = if file_cache_release_supported() {
            usize::try_from(self.window_bytes.get() - self.pending_bytes)
                .unwrap_or(usize::MAX)
                .min(buffer.len())
        } else {
            buffer.len()
        };
        self.pending_offset
            .checked_add(self.pending_bytes)
            .and_then(|offset| offset.checked_add(u64::try_from(limit).ok()?))
            .ok_or_else(|| io::Error::other("cache-release reader offset overflow"))?;
        let read = self.file.read(&mut buffer[..limit])?;
        self.pending_bytes = self
            .pending_bytes
            .checked_add(u64::try_from(read).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("cache-release reader byte count overflow"))?;
        if read == 0 {
            self.release_pending()?;
            self.finished = true;
        }
        Ok(read)
    }
}

impl Seek for FileCacheReleasingReader {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> {
        self.release_pending()?;
        let offset = self.file.seek(position)?;
        self.pending_offset = offset;
        self.finished = false;
        Ok(offset)
    }
}

impl Drop for FileCacheReleasingReader {
    fn drop(&mut self) {
        if !self.finished
            && let Err(error) = self.release_pending()
        {
            self.tracker.defer(&error);
        }
    }
}

/// File writer that synchronizes and releases completed page-cache windows.
#[derive(Debug)]
pub struct DurableFileCacheWriter {
    file: File,
    window_bytes: NonZeroU64,
    pending_offset: u64,
    pending_bytes: u64,
    evidence: FileCacheReleaseEvidence,
}

impl DurableFileCacheWriter {
    /// Wrap `file` with the default 64 MiB durable cache window.
    ///
    /// # Errors
    /// Returns an error when the current descriptor offset cannot be observed.
    pub fn new(file: File) -> io::Result<Self> {
        Self::with_window_bytes(
            file,
            NonZeroU64::new(DEFAULT_CACHE_RELEASE_WINDOW_BYTES)
                .expect("default cache-release window is non-zero"),
        )
    }

    /// Wrap `file` with an explicit non-zero durable cache window.
    ///
    /// # Errors
    /// Returns an error when the current descriptor offset cannot be observed.
    pub fn with_window_bytes(file: File, window_bytes: NonZeroU64) -> io::Result<Self> {
        Self::with_window_bytes_checked(file, window_bytes, || Ok(()))
    }

    /// Wrap `file` and run one caller-supplied setup check after observing the
    /// real descriptor offset but before constructing the writer.
    ///
    /// This exists so higher layers can deterministically exercise constructor
    /// failure without bypassing the actual descriptor setup path.
    ///
    /// # Errors
    /// Returns an error when offset observation or `setup_check` fails.
    pub fn with_window_bytes_checked(
        mut file: File,
        window_bytes: NonZeroU64,
        setup_check: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<Self> {
        let pending_offset = file.stream_position()?;
        setup_check()?;
        Ok(Self {
            file,
            window_bytes,
            pending_offset,
            pending_bytes: 0,
            evidence: FileCacheReleaseEvidence::default(),
        })
    }

    /// Complete the final durability barrier and release its remaining cache window.
    ///
    /// # Errors
    /// Returns an error if synchronization or a supported cache-release request fails.
    pub fn sync_all_and_release(&mut self) -> io::Result<()> {
        self.synchronize_pending(true)
    }

    /// Borrow the underlying file.
    #[must_use]
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Return content-free synchronization and cache-release evidence.
    #[must_use]
    pub const fn evidence(&self) -> FileCacheReleaseEvidence {
        self.evidence
    }

    /// Return this writer's configured synchronization/release window.
    #[must_use]
    pub const fn window_bytes(&self) -> NonZeroU64 {
        self.window_bytes
    }

    /// Consume the writer and return its underlying file.
    #[must_use]
    pub fn into_file(self) -> File {
        self.file
    }

    fn synchronize_pending(&mut self, final_barrier: bool) -> io::Result<()> {
        if self.pending_bytes == 0 && !final_barrier {
            return Ok(());
        }
        if self.pending_bytes == 0 {
            synchronize_file(&self.file)?;
            self.evidence.sync_operations = self
                .evidence
                .sync_operations
                .checked_add(1)
                .ok_or_else(|| io::Error::other("cache-release sync count overflow"))?;
            return Ok(());
        }
        let bytes = self.pending_bytes;
        let end = self
            .pending_offset
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("cache-release writer offset overflow"))?;
        let outcome = synchronize_before_release(
            || synchronize_file(&self.file),
            || release_file_cache(&self.file, self.pending_offset, bytes),
        )?;
        self.evidence.sync_operations = self
            .evidence
            .sync_operations
            .checked_add(1)
            .ok_or_else(|| io::Error::other("cache-release sync count overflow"))?;
        self.evidence.peak_window_bytes = self.evidence.peak_window_bytes.max(bytes);
        match outcome {
            FileCacheReleaseOutcome::Released => {
                self.evidence.release_operations = self
                    .evidence
                    .release_operations
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("cache-release operation count overflow"))?;
                self.evidence.released_bytes = self
                    .evidence
                    .released_bytes
                    .checked_add(bytes)
                    .ok_or_else(|| io::Error::other("cache-release byte count overflow"))?;
            }
            FileCacheReleaseOutcome::Unsupported => {
                self.evidence.unsupported_operations = self
                    .evidence
                    .unsupported_operations
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("unsupported cache-release count overflow"))?;
            }
        }
        self.pending_offset = end;
        self.pending_bytes = 0;
        Ok(())
    }
}

fn synchronize_before_release<T>(
    synchronize: impl FnOnce() -> io::Result<()>,
    release: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    synchronize()?;
    release()
}

impl Write for DurableFileCacheWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if file_cache_release_supported() && self.pending_bytes == self.window_bytes.get() {
            // Synchronize before consuming any bytes from this call. A failure
            // therefore obeys `Write::write`: callers may safely retry.
            self.synchronize_pending(false)?;
        }
        let limit = if file_cache_release_supported() {
            let remaining = self.window_bytes.get() - self.pending_bytes;
            buffer
                .len()
                .min(usize::try_from(remaining).unwrap_or(usize::MAX))
        } else {
            buffer.len()
        };
        self.pending_offset
            .checked_add(self.pending_bytes)
            .and_then(|offset| offset.checked_add(u64::try_from(limit).ok()?))
            .ok_or_else(|| io::Error::other("cache-release writer offset overflow"))?;
        let written = self.file.write(&buffer[..limit])?;
        self.pending_bytes = self
            .pending_bytes
            .checked_add(u64::try_from(written).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("cache-release writer byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Release one clean file range from the operating-system page cache.
///
/// Callers writing the file must synchronize the range before calling this
/// primitive. Prefer [`DurableFileCacheWriter`] for write paths because it
/// structurally enforces synchronization before release.
///
/// # Errors
/// Returns an error when a supported operating-system release request fails.
pub fn release_file_cache(
    file: &File,
    offset: u64,
    bytes: u64,
) -> io::Result<FileCacheReleaseOutcome> {
    #[cfg(test)]
    CACHE_RELEASE_FAILURE.with(|failure| {
        if failure.replace(false) {
            return Err(io::Error::other("injected cache-release failure"));
        }
        Ok(())
    })?;
    if bytes == 0 {
        return Ok(if file_cache_release_supported() {
            FileCacheReleaseOutcome::Released
        } else {
            FileCacheReleaseOutcome::Unsupported
        });
    }
    release_file_cache_inner(file, offset, bytes)
}

fn synchronize_file(file: &File) -> io::Result<()> {
    #[cfg(test)]
    CACHE_SYNC_FAILURE.with(|failure| {
        if failure.replace(false) {
            return Err(io::Error::other("injected cache-sync failure"));
        }
        Ok(())
    })?;
    file.sync_all()
}

#[cfg(test)]
thread_local! {
    static CACHE_RELEASE_FAILURE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static CACHE_SYNC_FAILURE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

#[cfg(test)]
fn inject_cache_release_failure() {
    CACHE_RELEASE_FAILURE.with(|failure| failure.set(true));
}

#[cfg(test)]
fn inject_cache_sync_failure() {
    CACHE_SYNC_FAILURE.with(|failure| failure.set(true));
}

#[cfg(target_os = "linux")]
const fn file_cache_release_supported() -> bool {
    true
}

#[cfg(not(target_os = "linux"))]
const fn file_cache_release_supported() -> bool {
    false
}

#[cfg(target_os = "linux")]
fn release_file_cache_inner(
    file: &File,
    offset: u64,
    bytes: u64,
) -> io::Result<FileCacheReleaseOutcome> {
    rustix::fs::fadvise(
        file,
        offset,
        NonZeroU64::new(bytes),
        rustix::fs::Advice::DontNeed,
    )
    .map_err(io::Error::from)?;
    Ok(FileCacheReleaseOutcome::Released)
}

#[cfg(not(target_os = "linux"))]
fn release_file_cache_inner(
    _file: &File,
    _offset: u64,
    _bytes: u64,
) -> io::Result<FileCacheReleaseOutcome> {
    Ok(FileCacheReleaseOutcome::Unsupported)
}

#[cfg(test)]
mod tests;
