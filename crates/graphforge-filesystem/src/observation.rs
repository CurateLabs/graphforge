//! Process-wide counters for an explicit diagnostic capture. Never reset them.
use std::fs::File;
use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use crate::FileIdentity;

static CAPTURES: AtomicUsize = AtomicUsize::new(0);
static FSYNC_CALLS: AtomicU64 = AtomicU64::new(0);
static FSYNC_NS: AtomicU64 = AtomicU64::new(0);
static FSYNC_DIRECTORY_CALLS: AtomicU64 = AtomicU64::new(0);

/// Keeps instrumented process counters active, including on worker threads.
#[derive(Debug)]
pub struct Observation(());
impl Observation {
    /// Start observing without resetting counters used by another capture.
    #[must_use]
    pub fn start() -> Self {
        CAPTURES.fetch_add(1, Ordering::Relaxed);
        Self(())
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        CAPTURES.fetch_sub(1, Ordering::Relaxed);
    }
}
static AUDITS: AtomicUsize = AtomicUsize::new(0);
static AUDIT_LOG: Mutex<Vec<(FileIdentity, bool)>> = Mutex::new(Vec::new());

/// Records which file or directory each barrier attempt reached, for as long
/// as it lives: an exact inventory of barriers per inode. The counters above say
/// how many barriers ran; this says on what. Barriers on every thread of the
/// process are recorded, so it is meant for single-purpose processes and tests
/// that serialize their measurements. The log grows by one entry per barrier.
#[derive(Debug)]
pub struct BarrierAudit {
    start: usize,
}
impl BarrierAudit {
    /// Begin recording barrier attempts.
    #[must_use]
    pub fn start() -> Self {
        AUDITS.fetch_add(1, Ordering::SeqCst);
        let start = AUDIT_LOG
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        Self { start }
    }

    /// The barriers attempted since [`BarrierAudit::start`], in order: the
    /// identity reached and whether it is a directory. Identities of objects
    /// that were later unlinked may be reused by the filesystem.
    #[must_use]
    pub fn finish(self) -> Vec<(FileIdentity, bool)> {
        AUDIT_LOG
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)[self.start..]
            .to_vec()
    }
}
impl Drop for BarrierAudit {
    fn drop(&mut self) {
        if AUDITS.fetch_sub(1, Ordering::SeqCst) == 1 {
            AUDIT_LOG
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
    }
}

/// Whether an explicit capture is active anywhere in this process.
#[must_use]
pub fn active() -> bool {
    CAPTURES.load(Ordering::Relaxed) != 0 || AUDITS.load(Ordering::Relaxed) != 0
}
/// Cumulative successful and failed barrier attempts, and their elapsed time.
#[must_use]
pub fn fsync_totals() -> (u64, u64) {
    (
        FSYNC_CALLS.load(Ordering::Relaxed),
        FSYNC_NS.load(Ordering::Relaxed),
    )
}

/// Cumulative barrier attempts on directories, a subset of the calls
/// [`fsync_totals`] counts. The rest are barriers on files.
#[must_use]
pub fn fsync_directory_calls() -> u64 {
    FSYNC_DIRECTORY_CALLS.load(Ordering::Relaxed)
}

/// File barriers with the same I/O behavior and optional diagnostic accounting.
pub trait ObservedSync {
    /// Invoke `File::sync_all`, accounting for the attempt when capturing.
    fn observed_sync_all(&self) -> io::Result<()>;
    /// Invoke `File::sync_data`, accounting for the attempt when capturing.
    fn observed_sync_data(&self) -> io::Result<()>;
}
impl ObservedSync for File {
    fn observed_sync_all(&self) -> io::Result<()> {
        observe(self, || self.sync_all())
    }
    fn observed_sync_data(&self) -> io::Result<()> {
        observe(self, || self.sync_data())
    }
}
fn observe(file: &File, operation: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
    if !active() {
        return operation();
    }
    let directory = file.metadata().is_ok_and(|metadata| metadata.is_dir());
    if directory {
        FSYNC_DIRECTORY_CALLS.fetch_add(1, Ordering::Relaxed);
    }
    if AUDITS.load(Ordering::SeqCst) != 0
        && let Ok(identity) = crate::file_identity(file)
    {
        AUDIT_LOG
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((identity, directory));
    }
    let started = Instant::now();
    let result = operation();
    FSYNC_NS.fetch_add(
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    FSYNC_CALLS.fetch_add(1, Ordering::Relaxed);
    result
}
