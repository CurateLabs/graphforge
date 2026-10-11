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
/// Mirrors `AuditState::holders` so the barrier path can skip the lock when no
/// audit runs. Only ever written under the lock.
static AUDITS: AtomicUsize = AtomicUsize::new(0);
static AUDIT: Mutex<AuditState> = Mutex::new(AuditState {
    holders: 0,
    log: Vec::new(),
});

struct AuditState {
    /// Audits alive. The log is kept, and grows, only while there are some.
    holders: usize,
    log: Vec<BarrierRecord>,
}

fn audit() -> std::sync::MutexGuard<'static, AuditState> {
    AUDIT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One barrier attempt: the inode it reached, whether that is a directory, and
/// when that inode was created. A filesystem reuses the inode number of a file
/// that was deleted, so the number alone does not say which file a barrier
/// reached; the creation time, with the number, does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BarrierRecord {
    /// The inode the barrier reached.
    pub identity: FileIdentity,
    /// Whether it is a directory.
    pub directory: bool,
    /// The inode's creation (birth) time, when the filesystem reports one.
    pub birth: Option<std::time::SystemTime>,
}

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
        let mut state = audit();
        state.holders += 1;
        AUDITS.store(state.holders, Ordering::SeqCst);
        Self {
            start: state.log.len(),
        }
    }

    /// The barriers attempted since [`BarrierAudit::start`], in order.
    #[must_use]
    pub fn finish(self) -> Vec<BarrierRecord> {
        let state = audit();
        // The log only shrinks when no audit is alive, so `start` is in range.
        state.log[self.start.min(state.log.len())..].to_vec()
    }
}
impl Drop for BarrierAudit {
    fn drop(&mut self) {
        let mut state = audit();
        state.holders -= 1;
        AUDITS.store(state.holders, Ordering::SeqCst);
        if state.holders == 0 {
            state.log.clear();
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
    let metadata = file.metadata().ok();
    let directory = metadata.as_ref().is_some_and(std::fs::Metadata::is_dir);
    if directory {
        FSYNC_DIRECTORY_CALLS.fetch_add(1, Ordering::Relaxed);
    }
    if AUDITS.load(Ordering::SeqCst) != 0
        && let Ok(identity) = crate::file_identity(file)
    {
        let mut state = audit();
        if state.holders != 0 {
            state.log.push(BarrierRecord {
                identity,
                directory,
                birth: metadata.and_then(|metadata| metadata.created().ok()),
            });
        }
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
