//! Explicit, operation-owned lifecycle I/O attribution.
//!
//! No collector, phase table or atomic counters are allocated or updated by
//! ordinary operations. [`CaptureScope::install`] requests measurement;
//! [`snapshot`] returns `None` without a valid capture. Captured observations
//! retain the closed phase inventory and distinguish measured zero from absence.
//!
//! Captures nest without charging inner work to outer operations. A clonable
//! [`CaptureContext`] carries the requested collector and phase across worker
//! boundaries; installation guards remain thread-bound. Deferred Parquet readers
//! retain the context active when the reader is opened. Process-wide CPU/hash
//! measurements in `concurrency_attribution` have separate semantics.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::storage_attribution::{PhaseIoTotals, StorageIoPhase};
use graphforge_core::GfError;
use serde::{Deserialize, Serialize};

const PHASE_COUNT: usize = StorageIoPhase::LIFECYCLE.len();

#[derive(Debug, Default)]
struct PhaseCounters {
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
    read_calls: AtomicU64,
    write_calls: AtomicU64,
    object_count: AtomicU64,
    block_count: AtomicU64,
    fsync_calls: AtomicU64,
}
impl PhaseCounters {
    fn totals(&self) -> PhaseIoTotals {
        PhaseIoTotals {
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            read_calls: self.read_calls.load(Ordering::Relaxed),
            write_calls: self.write_calls.load(Ordering::Relaxed),
            object_count: self.object_count.load(Ordering::Relaxed),
            block_count: self.block_count.load(Ordering::Relaxed),
            fsync_calls: self.fsync_calls.load(Ordering::Relaxed),
        }
    }
    fn reset(&self) {
        self.read_bytes.store(0, Ordering::Relaxed);
        self.write_bytes.store(0, Ordering::Relaxed);
        self.read_calls.store(0, Ordering::Relaxed);
        self.write_calls.store(0, Ordering::Relaxed);
        self.object_count.store(0, Ordering::Relaxed);
        self.block_count.store(0, Ordering::Relaxed);
        self.fsync_calls.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct CaptureState {
    rows: [PhaseCounters; PHASE_COUNT],
    invalid: AtomicBool,
    io_stats: crate::io_stats::Counters,
}
thread_local! {
    static ACTIVE_PHASE: Cell<Option<StorageIoPhase>> = const { Cell::new(None) };
    static CAPTURE: RefCell<Option<Arc<CaptureState>>> = const { RefCell::new(None) };
}

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static OBSERVER_WORK: Cell<(u64, u64, u64)> = const { Cell::new((0, 0, 0)) };
}

/// Test-only cumulative collector allocations, recording callbacks and phase maps.
/// Probes sit on the actual observer paths and do not activate measurement.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
#[must_use]
pub fn observer_work() -> (u64, u64, u64) {
    OBSERVER_WORK.with(Cell::get)
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn observe_work(kind: usize) {
    OBSERVER_WORK.with(|work| {
        let (mut allocations, mut recordings, mut maps) = work.get();
        match kind {
            0 => allocations += 1,
            1 => recordings += 1,
            _ => maps += 1,
        }
        work.set((allocations, recordings, maps));
    });
}

/// Requested collector and phase carried into one worker or deferred reader.
/// A disabled context contains no allocated collector.
#[derive(Clone, Debug, Default)]
pub struct CaptureContext {
    state: Option<Arc<CaptureState>>,
    phase: Option<StorageIoPhase>,
}
impl CaptureContext {
    /// Capture the current operation's optional observer at a job boundary.
    #[must_use]
    pub fn current() -> Self {
        Self {
            state: CAPTURE.with(|slot| slot.borrow().clone()),
            phase: ACTIVE_PHASE.with(Cell::get),
        }
    }
    /// Attach this job's observer, restoring any previous context on drop.
    #[must_use]
    pub fn attach(&self) -> CaptureScope {
        let previous = Self::current();
        CAPTURE.with(|slot| slot.borrow_mut().clone_from(&self.state));
        ACTIVE_PHASE.with(|phase| phase.set(self.phase));
        CaptureScope {
            state: self.state.clone(),
            previous,
            _not_send: std::marker::PhantomData,
        }
    }
    fn record(&self, default: StorageIoPhase, apply: impl FnOnce(&PhaseCounters)) {
        if let Some(state) = &self.state
            && !state.invalid.load(Ordering::Relaxed)
        {
            #[cfg(any(test, feature = "test-support"))]
            observe_work(1);
            apply(&state.rows[self.phase.unwrap_or(default).lifecycle_index()]);
        }
    }
}

/// Explicit measurement boundary. Guards are thread-bound and restore nested
/// captures instead of discarding an outer operation's observer.
#[derive(Debug)]
pub struct CaptureScope {
    state: Option<Arc<CaptureState>>,
    previous: CaptureContext,
    _not_send: std::marker::PhantomData<*const ()>,
}
impl CaptureScope {
    /// Start a fresh requested capture on this thread.
    #[must_use]
    pub fn install() -> Self {
        #[cfg(any(test, feature = "test-support"))]
        observe_work(0);
        CaptureContext {
            state: Some(Arc::new(CaptureState {
                rows: std::array::from_fn(|_| PhaseCounters::default()),
                invalid: AtomicBool::new(false),
                io_stats: crate::io_stats::Counters::default(),
            })),
            phase: None,
        }
        .attach()
    }
}
impl Drop for CaptureScope {
    fn drop(&mut self) {
        CAPTURE.with(|slot| {
            let mut active = slot.borrow_mut();
            let ordered = match (active.as_ref(), self.state.as_ref()) {
                (Some(active), Some(state)) => Arc::ptr_eq(active, state),
                (None, None) => true,
                _ => false,
            };
            if ordered {
                active.clone_from(&self.previous.state);
                ACTIVE_PHASE.with(|phase| phase.set(self.previous.phase));
            } else {
                if let Some(state) = &self.state {
                    state.invalid.store(true, Ordering::Relaxed);
                }
                if let Some(state) = active.as_ref() {
                    state.invalid.store(true, Ordering::Relaxed);
                }
                *active = None;
                ACTIVE_PHASE.with(|phase| phase.set(None));
            }
        });
    }
}

pub(crate) fn with_io_stats<T>(apply: impl FnOnce(&crate::io_stats::Counters) -> T) -> Option<T> {
    CAPTURE.with(|slot| {
        let captured = slot.borrow();
        let state = captured.as_ref()?;
        (!state.invalid.load(Ordering::Relaxed)).then(|| {
            #[cfg(any(test, feature = "test-support"))]
            observe_work(1);
            apply(&state.io_stats)
        })
    })
}

/// Whether lifecycle measurement was explicitly requested on this thread.
#[must_use]
pub fn is_active() -> bool {
    CAPTURE.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(|state| !state.invalid.load(Ordering::Relaxed))
    })
}

/// Override a captured operation's phase; inactive scopes do not mutate TLS.
#[derive(Debug)]
pub struct PhaseScope {
    previous: Option<StorageIoPhase>,
    active: bool,
    _not_send: std::marker::PhantomData<*const ()>,
}
impl PhaseScope {
    /// Attribute requested I/O to `phase` until the guard drops.
    #[must_use]
    pub fn enter(phase: StorageIoPhase) -> Self {
        let active = is_active();
        let previous = if active {
            ACTIVE_PHASE.with(|value| value.replace(Some(phase)))
        } else {
            None
        };
        Self {
            previous,
            active,
            _not_send: std::marker::PhantomData,
        }
    }
}
impl Drop for PhaseScope {
    fn drop(&mut self) {
        if self.active {
            ACTIVE_PHASE.with(|phase| phase.set(self.previous));
        }
    }
}
/// Requested override, otherwise the primitive's own phase.
#[must_use]
pub fn effective_phase(default: StorageIoPhase) -> StorageIoPhase {
    ACTIVE_PHASE.with(Cell::get).unwrap_or(default)
}
fn record(default: StorageIoPhase, apply: impl FnOnce(&PhaseCounters)) {
    // No Arc clone or phase lookup on the inactive production path.
    CAPTURE.with(|slot| {
        if let Some(state) = slot.borrow().as_ref()
            && !state.invalid.load(Ordering::Relaxed)
        {
            #[cfg(any(test, feature = "test-support"))]
            observe_work(1);
            apply(&state.rows[effective_phase(default).lifecycle_index()]);
        }
    });
}
/// Record nonempty application reads in an explicitly requested capture.
pub fn record_read(default: StorageIoPhase, bytes: u64, calls: u64) {
    if bytes != 0 && calls != 0 {
        record(default, |row| {
            row.read_bytes.fetch_add(bytes, Ordering::Relaxed);
            row.read_calls.fetch_add(calls, Ordering::Relaxed);
        });
    }
}
/// Record nonempty application write submissions in a requested capture.
pub fn record_write(default: StorageIoPhase, bytes: u64, calls: u64) {
    if bytes != 0 && calls != 0 {
        record(default, |row| {
            row.write_bytes.fetch_add(bytes, Ordering::Relaxed);
            row.write_calls.fetch_add(calls, Ordering::Relaxed);
        });
    }
}
/// Record completed durability barriers when requested.
pub fn record_fsync(default: StorageIoPhase, calls: u64) {
    if calls != 0 {
        record(default, |row| {
            row.fsync_calls.fetch_add(calls, Ordering::Relaxed);
        });
    }
}
/// Record immutable objects when requested.
pub fn record_objects(default: StorageIoPhase, objects: u64) {
    if objects != 0 {
        record(default, |row| {
            row.object_count.fetch_add(objects, Ordering::Relaxed);
        });
    }
}
/// Record blocks when requested.
pub fn record_blocks(default: StorageIoPhase, blocks: u64) {
    if blocks != 0 {
        record(default, |row| {
            row.block_count.fetch_add(blocks, Ordering::Relaxed);
        });
    }
}
/// Requested, valid snapshot; `None` allocates no phase map and means unavailable.
#[must_use]
pub fn snapshot() -> Option<LifecyclePhaseAttribution> {
    CAPTURE.with(|slot| {
        let capture = slot.borrow();
        let state = capture.as_ref()?;
        if state.invalid.load(Ordering::Relaxed) {
            return None;
        }
        #[cfg(any(test, feature = "test-support"))]
        observe_work(2);
        let mut totals = PhaseIoTotals::default();
        let phases = StorageIoPhase::LIFECYCLE
            .into_iter()
            .map(|phase| {
                let value = state.rows[phase.lifecycle_index()].totals();
                accumulate(&mut totals, &value);
                (phase, value)
            })
            .collect();
        Some(LifecyclePhaseAttribution { phases, totals })
    })
}
/// Difference only requested observations; inactive operations remain unavailable.
pub fn snapshot_since(
    earlier: Option<&LifecyclePhaseAttribution>,
) -> Result<Option<LifecyclePhaseAttribution>, GfError> {
    match (snapshot(), earlier) {
        (Some(later), Some(before)) => later.since(before).map(Some),
        _ => Ok(None),
    }
}
/// Reset the current explicitly requested collector. Does nothing when inactive.
#[doc(hidden)]
pub fn reset() {
    CAPTURE.with(|slot| {
        if let Some(state) = slot.borrow().as_ref() {
            for row in &state.rows {
                row.reset();
            }
        }
    });
}

/// Closed, reconciled phase attribution for one lifecycle region.
///
/// Serializes to the same `{phases, totals}` document
/// [`ConstructionPhaseAttribution`](crate::ConstructionPhaseAttribution) emits,
/// with one extra row: `read_path_scan`. Construction never records into that
/// row: the publish-side, import-side and explicit (`index("adjacency")`)
/// adjacency builds are scoped to the encoding row (#1449). A nonzero
/// `read_path_scan` therefore means committed read-path work, including a
/// query process's lazy adjacency rebuild with its writes and barriers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecyclePhaseAttribution {
    /// Every lifecycle phase exactly once, including zero observations.
    pub phases: BTreeMap<StorageIoPhase, PhaseIoTotals>,
    /// Exact sum of all phase rows.
    pub totals: PhaseIoTotals,
}

impl LifecyclePhaseAttribution {
    /// Attribution accumulated since `earlier`, which must be an earlier
    /// snapshot of the same process.
    ///
    /// # Errors
    /// Rejects a later snapshot whose counters went backwards, which can only
    /// happen if the counters were reset between the two captures.
    pub fn since(&self, earlier: &Self) -> Result<Self, GfError> {
        let mut phases = BTreeMap::new();
        let mut totals = PhaseIoTotals::default();
        for phase in StorageIoPhase::LIFECYCLE {
            let later = self.phases.get(&phase).cloned().unwrap_or_default();
            let before = earlier.phases.get(&phase).cloned().unwrap_or_default();
            let value = subtract(&later, &before)?;
            accumulate(&mut totals, &value);
            phases.insert(phase, value);
        }
        Ok(Self { phases, totals })
    }

    /// Reject missing phases or totals that do not equal the phase sum.
    ///
    /// # Errors
    /// Rejects an incomplete inventory or a total that does not reconcile.
    pub fn validate_reconciliation(&self) -> Result<(), GfError> {
        if StorageIoPhase::LIFECYCLE
            .iter()
            .any(|phase| !self.phases.contains_key(phase))
        {
            return Err(validation("lifecycle phase attribution is missing a phase"));
        }
        if self.phases.len() != PHASE_COUNT {
            return Err(validation(
                "lifecycle phase attribution carries an unknown phase",
            ));
        }
        let mut total = PhaseIoTotals::default();
        for phase in StorageIoPhase::LIFECYCLE {
            accumulate(&mut total, &self.phases[&phase]);
        }
        if total != self.totals {
            return Err(validation(
                "lifecycle phase attribution totals do not reconcile",
            ));
        }
        Ok(())
    }

    /// Validate qualification semantics as well as arithmetic reconciliation.
    /// A phase that truthfully performed no I/O stays an explicit zero row,
    /// while byte and call counters are paired so a synthetic byte-only or
    /// call-only row cannot be presented as observed application I/O.
    ///
    /// # Errors
    /// Rejects an unreconciled inventory or an unpaired byte/call row.
    pub fn validate_for_qualification(&self) -> Result<(), GfError> {
        self.validate_reconciliation()?;
        for phase in StorageIoPhase::LIFECYCLE {
            let totals = &self.phases[&phase];
            if (totals.read_bytes == 0) != (totals.read_calls == 0) {
                return Err(validation("lifecycle phase read bytes and calls disagree"));
            }
            if (totals.write_bytes == 0) != (totals.write_calls == 0) {
                return Err(validation("lifecycle phase write bytes and calls disagree"));
            }
        }
        Ok(())
    }
}

/// A [`std::fs::File`] that attributes every byte Parquet actually reads
/// through it to [`StorageIoPhase::ReadPathScan`], or to the active
/// [`PhaseScope`] when open-path code decodes committed data.
///
/// It delegates to the `ChunkReader` implementation Parquet already uses for a
/// plain file, so read placement, buffering and error behaviour are unchanged.
#[derive(Debug)]
pub struct ReadPathFile {
    file: std::fs::File,
    capture: CaptureContext,
}

impl ReadPathFile {
    /// Wrap an already-opened committed data file.
    #[must_use]
    pub fn new(file: std::fs::File) -> Self {
        Self {
            file,
            capture: CaptureContext::current(),
        }
    }
}

impl parquet::file::reader::Length for ReadPathFile {
    fn len(&self) -> u64 {
        parquet::file::reader::Length::len(&self.file)
    }
}

impl parquet::file::reader::ChunkReader for ReadPathFile {
    type T = ReadPathRead<<std::fs::File as parquet::file::reader::ChunkReader>::T>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(ReadPathRead {
            inner: parquet::file::reader::ChunkReader::get_read(&self.file, start)?,
            capture: self.capture.clone(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        let bytes = parquet::file::reader::ChunkReader::get_bytes(&self.file, start, length)?;
        if !bytes.is_empty() {
            self.capture.record(StorageIoPhase::ReadPathScan, |row| {
                row.read_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                row.read_calls.fetch_add(1, Ordering::Relaxed);
            });
        }
        Ok(bytes)
    }
}

/// Counting adapter for the reader [`ReadPathFile`] hands to Parquet.
///
/// It records one call per `read` on the wrapped reader. Placed beneath a
/// [`std::io::BufReader`], that is one call per buffer refill — the read
/// syscalls actually issued — rather than one per record the caller decodes.
#[derive(Debug)]
pub struct ReadPathRead<R> {
    inner: R,
    capture: CaptureContext,
}

impl<R> ReadPathRead<R> {
    /// Count reads issued against `inner`.
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            capture: CaptureContext::current(),
        }
    }
}

impl<R: std::io::Read> std::io::Read for ReadPathRead<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        if read != 0 {
            self.capture.record(StorageIoPhase::ReadPathScan, |row| {
                row.read_bytes.fetch_add(read as u64, Ordering::Relaxed);
                row.read_calls.fetch_add(1, Ordering::Relaxed);
            });
        }
        Ok(read)
    }
}

fn accumulate(target: &mut PhaseIoTotals, value: &PhaseIoTotals) {
    target.read_bytes = target.read_bytes.saturating_add(value.read_bytes);
    target.write_bytes = target.write_bytes.saturating_add(value.write_bytes);
    target.read_calls = target.read_calls.saturating_add(value.read_calls);
    target.write_calls = target.write_calls.saturating_add(value.write_calls);
    target.object_count = target.object_count.saturating_add(value.object_count);
    target.block_count = target.block_count.saturating_add(value.block_count);
    target.fsync_calls = target.fsync_calls.saturating_add(value.fsync_calls);
}

fn subtract(later: &PhaseIoTotals, earlier: &PhaseIoTotals) -> Result<PhaseIoTotals, GfError> {
    let field = |later: u64, earlier: u64| {
        later
            .checked_sub(earlier)
            .ok_or_else(|| validation("lifecycle phase counters moved backwards"))
    };
    Ok(PhaseIoTotals {
        read_bytes: field(later.read_bytes, earlier.read_bytes)?,
        write_bytes: field(later.write_bytes, earlier.write_bytes)?,
        read_calls: field(later.read_calls, earlier.read_calls)?,
        write_calls: field(later.write_calls, earlier.write_calls)?,
        object_count: field(later.object_count, earlier.object_count)?,
        block_count: field(later.block_count, earlier.block_count)?,
        fsync_calls: field(later.fsync_calls, earlier.fsync_calls)?,
    })
}

fn validation(message: &str) -> GfError {
    GfError::Validation(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_io_has_no_optional_observer_work() {
        let before = observer_work();
        let _phase = PhaseScope::enter(StorageIoPhase::SealAuthentication);
        record_read(StorageIoPhase::ReadPathScan, 512, 1);
        record_write(StorageIoPhase::CasInstallReadWrite, 512, 1);
        record_fsync(StorageIoPhase::FsyncSynchronization, 1);
        reset();
        crate::io_stats::reset();
        // Spill readers also retain a disabled context when opened without
        // collection, even though their reads pass through the adapter.
        let mut reader = ReadPathRead::new(std::io::Cursor::new([1_u8; 8]));
        let mut bytes = [0_u8; 8];
        std::io::Read::read_exact(&mut reader, &mut bytes).unwrap();
        assert_eq!(bytes, [1_u8; 8]);
        assert!(snapshot().is_none());
        assert!(crate::io_stats::snapshot().is_none());
        assert!(!is_active());
        assert_eq!(observer_work(), before);
        assert!(CaptureContext::current().state.is_none());
    }

    #[test]
    fn nested_capture_and_unwind_restore_the_outer_operation() {
        let _outer = CaptureScope::install();
        let _phase = PhaseScope::enter(StorageIoPhase::SealAuthentication);
        record_read(StorageIoPhase::ReadPathScan, 7, 1);
        let failed = std::panic::catch_unwind(|| {
            let _inner = CaptureScope::install();
            record_read(StorageIoPhase::ReadPathScan, 99, 1);
            assert_eq!(snapshot().unwrap().totals.read_bytes, 99);
            panic!("cancel operation");
        });
        assert!(failed.is_err());
        record_read(StorageIoPhase::ReadPathScan, 5, 1);
        let outer = snapshot().unwrap();
        assert_eq!(outer.totals.read_bytes, 12);
        assert_eq!(
            outer.phases[&StorageIoPhase::SealAuthentication].read_bytes,
            12
        );
    }

    #[test]
    fn misordered_capture_guards_refuse_observation() {
        let outer = CaptureScope::install();
        let inner = CaptureScope::install();
        drop(outer);
        assert!(snapshot().is_none());
        drop(inner);
        assert!(snapshot().is_none());
        assert!(crate::io_stats::snapshot().is_none());
        let _fresh = CaptureScope::install();
        assert_eq!(snapshot().unwrap().totals.read_bytes, 0);
    }

    #[test]
    fn reused_worker_attaches_each_jobs_context_and_clears_it() {
        let _first = CaptureScope::install();
        let first = CaptureContext::current();
        let first_snapshot;
        {
            let _second = CaptureScope::install();
            let second = CaptureContext::current();
            let worker = std::thread::spawn(move || {
                for (context, bytes) in [(first, 11), (second, 29)] {
                    let _attached = context.attach();
                    record_read(StorageIoPhase::ReadPathScan, bytes, 1);
                }
                assert!(snapshot().is_none());
                record_read(StorageIoPhase::ReadPathScan, 999, 1);
            });
            worker.join().unwrap();
            assert_eq!(snapshot().unwrap().totals.read_bytes, 29);
            first_snapshot = 11;
        }
        assert_eq!(snapshot().unwrap().totals.read_bytes, first_snapshot);
    }

    #[test]
    fn concurrent_deferred_file_reads_keep_the_opening_operations_context() {
        use parquet::file::reader::ChunkReader;
        use std::io::{Read, Write};
        let mut named = tempfile::NamedTempFile::new().unwrap();
        named.write_all(b"operation-owned read bytes").unwrap();
        let file = std::fs::File::open(named.path()).unwrap();
        let _first = CaptureScope::install();
        let _phase = PhaseScope::enter(StorageIoPhase::HydrationVerification);
        let first = ReadPathFile::new(file.try_clone().unwrap())
            .get_read(0)
            .unwrap();
        {
            let _second = CaptureScope::install();
            let second = ReadPathFile::new(std::fs::File::open(named.path()).unwrap());
            std::thread::scope(|scope| {
                let first_worker = scope.spawn(move || {
                    let mut reader = first;
                    let mut bytes = [0; 9];
                    reader.read_exact(&mut bytes).unwrap();
                    assert_eq!(&bytes, b"operation");
                    assert!(snapshot().is_none());
                });
                let second_worker = scope.spawn(move || {
                    assert_eq!(second.get_bytes(10, 5).unwrap().as_ref(), b"owned");
                    assert!(snapshot().is_none());
                });
                first_worker.join().unwrap();
                second_worker.join().unwrap();
            });
            assert_eq!(snapshot().unwrap().totals.read_bytes, 5);
        }
        let taken = snapshot().unwrap();
        assert_eq!(taken.totals.read_bytes, 9);
        assert_eq!(
            taken.phases[&StorageIoPhase::HydrationVerification].read_bytes,
            9
        );
    }

    #[test]
    fn a_capture_is_isolated_from_every_other_thread() {
        // The defect this closes: another thread's instrumented I/O landing in
        // a measured region. Without the capture, the spawned thread's 999
        // bytes reach the same global counter the region reads, and the final
        // assertion sees 40 + 999.
        let _capture = CaptureScope::install();
        let before = snapshot().expect("requested capture");
        record_read(StorageIoPhase::ReadPathScan, 40, 1);

        std::thread::spawn(|| {
            record_read(StorageIoPhase::ReadPathScan, 999, 7);
            record_write(StorageIoPhase::CasInstallReadWrite, 999, 7);
            record_fsync(StorageIoPhase::FsyncSynchronization, 99);
        })
        .join()
        .expect("recording thread");

        let region = snapshot()
            .expect("requested capture")
            .since(&before)
            .expect("region attribution");
        assert_eq!(region.phases[&StorageIoPhase::ReadPathScan].read_bytes, 40);
        assert_eq!(region.totals.read_bytes, 40);
        assert_eq!(region.totals.write_bytes, 0);
        assert_eq!(region.totals.fsync_calls, 0);
    }

    #[test]
    fn a_capture_is_removed_on_drop_and_a_fresh_one_starts_clean() {
        {
            let _capture = CaptureScope::install();
            record_read(StorageIoPhase::ReadPathScan, 1_234, 5);
            assert_eq!(
                snapshot().expect("requested capture").totals.read_bytes,
                1_234
            );
        }
        // A second capture must not inherit the first one's rows. Asserting
        // against the process-global counters here instead would reintroduce
        // exactly the cross-test race this change exists to remove, so the
        // scoping is proven from inside captures only.
        let _capture = CaptureScope::install();
        assert_eq!(snapshot().expect("requested capture").totals.read_bytes, 0);
    }

    #[test]
    fn lifecycle_inventory_is_the_construction_inventory_plus_the_read_path() {
        assert_eq!(
            StorageIoPhase::LIFECYCLE.len(),
            StorageIoPhase::ALL.len() + 1
        );
        for phase in StorageIoPhase::ALL {
            assert!(StorageIoPhase::LIFECYCLE.contains(&phase));
        }
        assert!(!StorageIoPhase::ALL.contains(&StorageIoPhase::ReadPathScan));
        for (index, phase) in StorageIoPhase::LIFECYCLE.into_iter().enumerate() {
            assert_eq!(phase.lifecycle_index(), index);
        }
    }

    #[test]
    fn snapshot_difference_attributes_only_the_measured_region() {
        let _capture = CaptureScope::install();
        record_read(StorageIoPhase::HydrationVerification, 100, 2);
        let before = snapshot().expect("requested capture");
        record_read(StorageIoPhase::ReadPathScan, 40, 1);
        record_write(StorageIoPhase::CasInstallReadWrite, 8, 1);
        record_fsync(StorageIoPhase::FsyncSynchronization, 3);
        let region = snapshot()
            .expect("requested capture")
            .since(&before)
            .expect("region attribution");
        region
            .validate_for_qualification()
            .expect("region reconciles");
        assert_eq!(
            region.phases[&StorageIoPhase::HydrationVerification].read_bytes,
            0
        );
        assert_eq!(region.phases[&StorageIoPhase::ReadPathScan].read_bytes, 40);
        assert_eq!(
            region.phases[&StorageIoPhase::CasInstallReadWrite].write_bytes,
            8
        );
        assert_eq!(region.totals.fsync_calls, 3);
        assert_eq!(region.totals.read_bytes, 40);
    }

    #[test]
    fn an_active_scope_overrides_the_primitive_default() {
        let _capture = CaptureScope::install();
        {
            let _scope = PhaseScope::enter(StorageIoPhase::RecoveryReauthentication);
            record_read(StorageIoPhase::PublicationPreauthentication, 64, 1);
            {
                let _inner = PhaseScope::enter(StorageIoPhase::SealAuthentication);
                record_read(StorageIoPhase::PublicationPreauthentication, 16, 1);
            }
            record_read(StorageIoPhase::PublicationPreauthentication, 8, 1);
        }
        record_read(StorageIoPhase::PublicationPreauthentication, 4, 1);
        let taken = snapshot().expect("requested capture");
        assert_eq!(
            taken.phases[&StorageIoPhase::RecoveryReauthentication].read_bytes,
            72
        );
        assert_eq!(
            taken.phases[&StorageIoPhase::SealAuthentication].read_bytes,
            16
        );
        assert_eq!(
            taken.phases[&StorageIoPhase::PublicationPreauthentication].read_bytes,
            4
        );
    }

    #[test]
    fn reconciliation_rejects_a_missing_phase_and_a_wrong_total() {
        let _capture = CaptureScope::install();
        record_read(StorageIoPhase::ReadPathScan, 10, 1);
        let mut taken = snapshot().expect("requested capture");
        taken
            .validate_reconciliation()
            .expect("baseline reconciles");
        let mut missing = taken.clone();
        missing.phases.remove(&StorageIoPhase::ReadPathScan);
        assert!(missing.validate_reconciliation().is_err());
        taken.totals.read_bytes += 1;
        assert!(taken.validate_reconciliation().is_err());
    }

    #[test]
    fn qualification_rejects_an_unpaired_byte_only_row() {
        let _capture = CaptureScope::install();
        let mut taken = snapshot().expect("requested capture");
        let row = taken
            .phases
            .get_mut(&StorageIoPhase::ReadPathScan)
            .expect("read path row");
        row.read_bytes = 9;
        taken.totals.read_bytes = 9;
        assert!(taken.validate_reconciliation().is_ok());
        assert!(taken.validate_for_qualification().is_err());
    }
}
