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
    log: Vec<BarrierEntry>,
}

/// What the log keeps for one barrier attempt: the record, or why none could be
/// made. A barrier that cannot be attributed is not dropped from the audit.
#[derive(Clone, Debug)]
enum BarrierEntry {
    Record(BarrierRecord),
    Unattributed(String),
}

fn audit() -> std::sync::MutexGuard<'static, AuditState> {
    AUDIT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One barrier attempt: the inode it reached and whether that is a directory.
/// A filesystem reuses the inode number of a deleted file, and its creation time
/// repeats within a coarse timer tick, so neither says which file a barrier
/// reached. The inode's generation number, which the filesystem changes every
/// time it reuses the inode, does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BarrierRecord {
    /// The inode the barrier reached.
    pub identity: FileIdentity,
    /// The generation number of that inode ([`crate::inode_generation`]).
    pub generation: u32,
    /// Whether it is a directory.
    pub directory: bool,
}

/// Everything an audit recorded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BarrierLog {
    /// The barriers attempted, in order.
    pub records: Vec<BarrierRecord>,
    /// One message for each barrier that reached a file the audit could not
    /// name (an identity or generation number it could not read). A correct
    /// audit has none, and a test asserts so.
    pub unattributed: Vec<String>,
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
    pub fn finish(self) -> BarrierLog {
        let state = audit();
        let mut log = BarrierLog::default();
        // The log only shrinks when no audit is alive, so `start` is in range.
        for entry in &state.log[self.start.min(state.log.len())..] {
            match entry {
                BarrierEntry::Record(record) => log.records.push(*record),
                BarrierEntry::Unattributed(reason) => log.unattributed.push(reason.clone()),
            }
        }
        log
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
    let metadata = file.metadata();
    let directory = metadata.as_ref().is_ok_and(std::fs::Metadata::is_dir);
    if directory {
        FSYNC_DIRECTORY_CALLS.fetch_add(1, Ordering::Relaxed);
    }
    if AUDITS.load(Ordering::SeqCst) != 0 {
        let entry = match (
            &metadata,
            crate::file_identity(file),
            crate::inode_generation(file),
        ) {
            (Ok(_), Ok(identity), Ok(generation)) => BarrierEntry::Record(BarrierRecord {
                identity,
                generation,
                directory,
            }),
            (metadata, identity, generation) => BarrierEntry::Unattributed(format!(
                "metadata: {:?}; identity: {:?}; generation: {:?}",
                metadata.as_ref().err(),
                identity.err(),
                generation.err()
            )),
        };
        let mut state = audit();
        if state.holders != 0 {
            state.log.push(entry);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The audit is process-wide, and `cargo test` runs a crate's tests on
    /// threads of one process: the tests that start audits take turns, as each
    /// asserts on the state when no other audit is alive.
    static TURNS: Mutex<()> = Mutex::new(());

    fn take_turn() -> std::sync::MutexGuard<'static, ()> {
        TURNS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn barrier_on(file: &File) {
        file.observed_sync_all().unwrap();
    }

    /// How many barriers of `identity` the log holds. Where the platform names
    /// inodes (Linux) these are the records of that inode; elsewhere no barrier
    /// can be attributed, so the log's entries, all unattributed, are counted:
    /// the lifecycle properties below do not depend on attribution, and a test
    /// there is the only user of the audit in its process.
    fn mine(log: &BarrierLog, identity: FileIdentity) -> usize {
        if cfg!(target_os = "linux") {
            assert!(log.unattributed.is_empty(), "{:?}", log.unattributed);
            log.records
                .iter()
                .filter(|record| record.identity == identity)
                .count()
        } else {
            // Never a weaker name: without a generation number nothing is
            // recorded as attributed.
            assert!(log.records.is_empty(), "{:?}", log.records);
            log.unattributed.len()
        }
    }

    /// The log holds at least `expected` barriers of the file: exactly where
    /// barriers are attributed, and the barriers of any other thread's audit
    /// too where they are not.
    fn assert_mine(log: &BarrierLog, identity: FileIdentity, expected: usize, what: &str) {
        let held = mine(log, identity);
        if cfg!(target_os = "linux") {
            assert_eq!(held, expected, "{what}");
        } else {
            assert!(held >= expected, "{what}: {held} < {expected}");
        }
    }

    /// Audits nest and overlap: one ending clears nothing another still needs,
    /// and the log is only cleared with the last, under the same lock that
    /// starts them. Nothing here depends on attributing a barrier to a file.
    #[test]
    fn overlapping_audits_keep_their_logs_until_the_last_ends() {
        let _turn = take_turn();
        let directory = tempfile::tempdir().unwrap();
        let file = File::create(directory.path().join("file")).unwrap();
        let identity = crate::file_identity(&file).unwrap();
        let first = BarrierAudit::start();
        barrier_on(&file);
        let second = BarrierAudit::start();
        barrier_on(&file);
        // The first audit, ending, leaves the second's records intact.
        assert_mine(&first.finish(), identity, 2, "first");
        barrier_on(&file);
        let second = second.finish();
        assert_mine(&second, identity, 2, "second");
        if cfg!(target_os = "linux") {
            assert!(
                second
                    .records
                    .iter()
                    .filter(|record| record.identity == identity)
                    .all(|record| !record.directory)
            );
        }
        // Nothing is recorded, or kept, once no audit is alive.
        barrier_on(&file);
        assert!(audit().log.is_empty());
        // A later audit starts from an empty log.
        assert_mine(&BarrierAudit::start().finish(), identity, 0, "later");
    }

    /// Many threads start, use and end audits that overlap one another, some
    /// ending by `finish` and some by drop. None may panic, and each audit's
    /// slice holds the barriers its own thread made while it lived (exactly
    /// them, where barriers are attributed to files).
    ///
    /// Start and drop used to adjust the live-audit count before taking the lock
    /// that guards the log, so a start could read a log length that the last
    /// drop then cleared, and `finish` sliced past the end.
    #[test]
    fn audits_started_and_ended_concurrently_keep_their_own_slices() {
        const THREADS: usize = 16;
        const ROUNDS: usize = 60_000;
        let _turn = take_turn();
        let directory = tempfile::tempdir().unwrap();
        std::thread::scope(|scope| {
            for thread in 0..THREADS {
                let directory = directory.path();
                scope.spawn(move || {
                    let file = File::create(directory.join(format!("file-{thread}"))).unwrap();
                    let identity = crate::file_identity(&file).unwrap();
                    for round in 0..ROUNDS {
                        let audit = BarrierAudit::start();
                        let barriers = 1 + (round % 3);
                        for _ in 0..barriers {
                            barrier_on(&file);
                        }
                        if round % 4 == 3 {
                            drop(audit);
                        } else {
                            assert_mine(&audit.finish(), identity, barriers, "own slice");
                        }
                    }
                });
            }
        });
        assert!(audit().log.is_empty());
        assert_eq!(audit().holders, 0);
    }

    /// A barrier that reaches a file the audit cannot name is reported, not
    /// dropped: procfs has no inode generation, and syncing it is refused.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_barrier_the_audit_cannot_name_is_reported() {
        let _turn = take_turn();
        let file = File::open("/proc/self/status").unwrap();
        let audit = BarrierAudit::start();
        // The sync itself fails on procfs; the attempt is what is recorded.
        let _ = file.observed_sync_all();
        let log = audit.finish();
        assert!(log.records.is_empty(), "{:?}", log.records);
        assert_eq!(log.unattributed.len(), 1, "{:?}", log.unattributed);
        assert!(log.unattributed[0].contains("generation"));
    }
}
