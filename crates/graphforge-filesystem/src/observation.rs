//! Process-wide counters for an explicit diagnostic capture. Never reset them.
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

static CAPTURES: AtomicUsize = AtomicUsize::new(0);
static FSYNC_CALLS: AtomicU64 = AtomicU64::new(0);
static FSYNC_NS: AtomicU64 = AtomicU64::new(0);

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
/// Whether an explicit capture is active anywhere in this process.
#[must_use]
pub fn active() -> bool {
    CAPTURES.load(Ordering::Relaxed) != 0
}
/// Cumulative successful and failed barrier attempts, and their elapsed time.
#[must_use]
pub fn fsync_totals() -> (u64, u64) {
    (
        FSYNC_CALLS.load(Ordering::Relaxed),
        FSYNC_NS.load(Ordering::Relaxed),
    )
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
        observe(|| self.sync_all())
    }
    fn observed_sync_data(&self) -> io::Result<()> {
        observe(|| self.sync_data())
    }
}
fn observe(operation: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
    if !active() {
        return operation();
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
