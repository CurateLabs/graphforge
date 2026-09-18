//! Process-global per-phase application I/O attribution for the whole project
//! lifecycle.
//!
//! # Why
//! [`ConstructionPhaseAttribution`](crate::ConstructionPhaseAttribution) already
//! attributes construction I/O to the closed [`StorageIoPhase`] inventory, and
//! that is how ingest can say which share of its reads is authentication. Every
//! other lifecycle phase — project open, reopen, recount, query, export and
//! clean import — had no per-phase attribution at all (#1389). This module adds
//! it without a parallel accounting scheme: the same phase inventory, the same
//! [`PhaseIoTotals`] counters, and the same `{phases, totals}` shape, so the
//! analysis written against construction attribution runs unchanged.
//!
//! # Semantics
//! Counters are process-global and aggregate across threads, exactly like
//! [`crate::io_stats`]. Difference two [`snapshot`]s to attribute work to a
//! region; [`LifecyclePhaseAttribution::since`] does that subtraction. A region
//! that spans concurrent unrelated work cannot be attributed, which is why the
//! ladder captures one snapshot per single-purpose process invocation.
//!
//! Recording is deliberately separate from the construction counters. Nothing
//! here feeds [`ConstructionPhaseAttribution`], so construction attribution
//! output is byte-identical to before for an unchanged workload.
//!
//! # Phase selection
//! Each instrumented primitive names the phase it naturally belongs to. A
//! [`PhaseScope`] on the current thread overrides that default for work whose
//! owning phase is only known to the caller — the recovery-on-open pass being
//! the clear case, since it reuses the ordinary generation readers.
//!
//! # Cost
//! One thread-local read plus two relaxed atomic adds per instrumented
//! operation, against operations that already read a file and usually hash it.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use graphforge_core::GfError;
use serde::{Deserialize, Serialize};

use crate::storage_attribution::{PhaseIoTotals, StorageIoPhase};

const PHASE_COUNT: usize = StorageIoPhase::LIFECYCLE.len();

/// Relaxed counters for one lifecycle phase.
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
    const fn new() -> Self {
        Self {
            read_bytes: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            read_calls: AtomicU64::new(0),
            write_calls: AtomicU64::new(0),
            object_count: AtomicU64::new(0),
            block_count: AtomicU64::new(0),
            fsync_calls: AtomicU64::new(0),
        }
    }

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

#[allow(
    clippy::declare_interior_mutable_const,
    reason = "a const item is the only way to build a static array of atomics"
)]
const ZERO_COUNTERS: PhaseCounters = PhaseCounters::new();

static PHASES: [PhaseCounters; PHASE_COUNT] = [ZERO_COUNTERS; PHASE_COUNT];

thread_local! {
    static ACTIVE_PHASE: Cell<Option<StorageIoPhase>> = const { Cell::new(None) };
}

/// Override the phase attributed to instrumented storage I/O on this thread for
/// as long as the guard is held. Scopes nest; the innermost wins.
#[derive(Debug)]
pub struct PhaseScope {
    previous: Option<StorageIoPhase>,
}

impl PhaseScope {
    /// Attribute instrumented storage I/O on this thread to `phase`.
    #[must_use]
    pub fn enter(phase: StorageIoPhase) -> Self {
        let previous = ACTIVE_PHASE.with(|active| active.replace(Some(phase)));
        Self { previous }
    }
}

impl Drop for PhaseScope {
    fn drop(&mut self) {
        ACTIVE_PHASE.with(|active| active.set(self.previous));
    }
}

/// The phase an instrumented primitive should record under: the active
/// [`PhaseScope`] when one is held, otherwise the primitive's own `default`.
#[must_use]
pub fn effective_phase(default: StorageIoPhase) -> StorageIoPhase {
    ACTIVE_PHASE.with(Cell::get).unwrap_or(default)
}

/// Record application reads returned to the caller.
///
/// Empty reads contribute nothing, matching the construction counters, so a
/// phase row can never carry calls without bytes.
pub fn record_read(default: StorageIoPhase, bytes: u64, calls: u64) {
    if bytes == 0 || calls == 0 {
        return;
    }
    let counters = &PHASES[effective_phase(default).lifecycle_index()];
    counters.read_bytes.fetch_add(bytes, Ordering::Relaxed);
    counters.read_calls.fetch_add(calls, Ordering::Relaxed);
}

/// Record application writes submitted by the caller.
///
/// Empty writes contribute nothing, matching the construction counters.
pub fn record_write(default: StorageIoPhase, bytes: u64, calls: u64) {
    if bytes == 0 || calls == 0 {
        return;
    }
    let counters = &PHASES[effective_phase(default).lifecycle_index()];
    counters.write_bytes.fetch_add(bytes, Ordering::Relaxed);
    counters.write_calls.fetch_add(calls, Ordering::Relaxed);
}

/// Record completed durability barriers.
pub fn record_fsync(default: StorageIoPhase, calls: u64) {
    if calls == 0 {
        return;
    }
    PHASES[effective_phase(default).lifecycle_index()]
        .fsync_calls
        .fetch_add(calls, Ordering::Relaxed);
}

/// Record immutable objects handled by a phase.
pub fn record_objects(default: StorageIoPhase, objects: u64) {
    if objects == 0 {
        return;
    }
    PHASES[effective_phase(default).lifecycle_index()]
        .object_count
        .fetch_add(objects, Ordering::Relaxed);
}

/// Record fixed-size authenticated or buffered blocks handled by a phase.
pub fn record_blocks(default: StorageIoPhase, blocks: u64) {
    if blocks == 0 {
        return;
    }
    PHASES[effective_phase(default).lifecycle_index()]
        .block_count
        .fetch_add(blocks, Ordering::Relaxed);
}

/// Point-in-time copy of every lifecycle phase counter.
#[must_use]
pub fn snapshot() -> LifecyclePhaseAttribution {
    let mut phases = BTreeMap::new();
    let mut totals = PhaseIoTotals::default();
    for phase in StorageIoPhase::LIFECYCLE {
        let value = PHASES[phase.lifecycle_index()].totals();
        accumulate(&mut totals, &value);
        phases.insert(phase, value);
    }
    LifecyclePhaseAttribution { phases, totals }
}

/// Zero every counter. Tests that assert on a region reset immediately before
/// it and keep that region single-threaded.
#[doc(hidden)]
pub fn reset() {
    for counters in &PHASES {
        counters.reset();
    }
}

/// Closed, reconciled phase attribution for one lifecycle region.
///
/// Serializes to the same `{phases, totals}` document
/// [`ConstructionPhaseAttribution`](crate::ConstructionPhaseAttribution) emits,
/// with one extra row: `read_path_scan`, which construction never performs.
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
}

impl ReadPathFile {
    /// Wrap an already-opened committed data file.
    #[must_use]
    pub const fn new(file: std::fs::File) -> Self {
        Self { file }
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
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        let bytes = parquet::file::reader::ChunkReader::get_bytes(&self.file, start, length)?;
        record_read(StorageIoPhase::ReadPathScan, bytes.len() as u64, 1);
        Ok(bytes)
    }
}

/// Counting adapter for the reader [`ReadPathFile`] hands to Parquet.
#[derive(Debug)]
pub struct ReadPathRead<R> {
    inner: R,
}

impl<R: std::io::Read> std::io::Read for ReadPathRead<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        record_read(StorageIoPhase::ReadPathScan, read as u64, 1);
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

    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn guard() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        let _guard = guard();
        reset();
        record_read(StorageIoPhase::HydrationVerification, 100, 2);
        let before = snapshot();
        record_read(StorageIoPhase::ReadPathScan, 40, 1);
        record_write(StorageIoPhase::CasInstallReadWrite, 8, 1);
        record_fsync(StorageIoPhase::FsyncSynchronization, 3);
        let region = snapshot().since(&before).expect("region attribution");
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
        reset();
    }

    #[test]
    fn an_active_scope_overrides_the_primitive_default() {
        let _guard = guard();
        reset();
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
        let taken = snapshot();
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
        reset();
    }

    #[test]
    fn reconciliation_rejects_a_missing_phase_and_a_wrong_total() {
        let _guard = guard();
        reset();
        record_read(StorageIoPhase::ReadPathScan, 10, 1);
        let mut taken = snapshot();
        taken
            .validate_reconciliation()
            .expect("baseline reconciles");
        let mut missing = taken.clone();
        missing.phases.remove(&StorageIoPhase::ReadPathScan);
        assert!(missing.validate_reconciliation().is_err());
        taken.totals.read_bytes += 1;
        assert!(taken.validate_reconciliation().is_err());
        reset();
    }

    #[test]
    fn qualification_rejects_an_unpaired_byte_only_row() {
        let _guard = guard();
        reset();
        let mut taken = snapshot();
        let row = taken
            .phases
            .get_mut(&StorageIoPhase::ReadPathScan)
            .expect("read path row");
        row.read_bytes = 9;
        taken.totals.read_bytes = 9;
        assert!(taken.validate_reconciliation().is_ok());
        assert!(taken.validate_for_qualification().is_err());
        reset();
    }
}
