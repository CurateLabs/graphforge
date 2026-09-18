//! Worker-local partition loads with coordinator-owned publication (#1456,
//! work item 1).
//!
//! Every parallelism attempt on the construction path (#1429, #1448) died on
//! the same shape: `&mut GraphConstructionEvidence` threaded through every
//! stage, and a whole-struct clone / mutate / write-back across an fsync in
//! `finish_optional`. Concurrent lanes lose updates even under a mutex.
//!
//! This module separates the two halves that were conflated:
//!
//! * **Workers return their own results and local counters.** A load never
//!   sees the shared evidence. It returns the sorted partition plus a
//!   [`PartitionLoadCounters`], whose merge into the evidence is explicit
//!   and total ([`PartitionLoadCounters::merge_into`]): plain sums add,
//!   per-partition maxima take `max`.
//! * **One coordinator owns the checkpoint, the allocation ledger and
//!   publication.** [`consume_in_partition_order`] runs the loads on a bounded
//!   pool and hands each result to the calling thread in canonical partition
//!   index order, so scheduling cannot change output. The ordered critical
//!   section is the coordinator's consume step; decoding, sorting and the bulk
//!   read happen outside it. No worker touches the allocation ledger, so the
//!   exact coexistence peak stays what it was.
//!
//! Two figures that are not the same figure:
//!
//! * The **observed** allocation peak is evidence. It is exact and depends on
//!   interleaving, which is why the ledger append stays ordered on the
//!   coordinator (two workers each installing and removing 100 bytes peak at
//!   100 or 200 depending on overlap; per-worker maxima are 100 and 100 either
//!   way and cannot reconstruct which).
//! * A **reservation bound** is what an admission gate needs. It is
//!   order-independent by construction. For materialized partitions it is
//!   [`materialized_records_bound`]: `min(workers, partitions)` times the
//!   largest partition, which the scheduler's window guarantees is never
//!   exceeded.
//!
//! Cancellation needs no `Send` bound on the caller's `FnMut() -> bool`: the
//! callback stays on the coordinator, which polls it in the consume step, and
//! the pool observes a stop flag. Workers poll the flag between records so an
//! abandoned load exits promptly.

use super::{GraphConstructionEvidence, account_cache_release, account_sequential_read, storage};
use graphforge_core::GfError;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

/// Concurrent partition jobs per fixed-width family finish: one partition
/// loading and sorting while the coordinator writes the previous one.
///
/// This is a scheduling decision, not a recorded format parameter: the
/// durable partition count and splitters are unchanged by it, and the
/// schedule-independence tests hold the evidence and output bytes equal
/// across worker counts. It bounds the materialized partitions, see
/// [`consume_in_partition_order`].
pub(super) const PARTITION_LOAD_WORKERS: NonZeroUsize = NonZeroUsize::new(2).unwrap();

/// The load-worker count to use, honouring a measurement override.
///
/// `finish_optional` is **44.2% of shaping at 0.60 effective cores** on a
/// 16-thread host (#1464), and this constant is why: the window is the worker
/// count, so two workers cap that region at two cores' worth however many the
/// machine has. Raising it also raises resident partitions, since the bound is
/// `min(workers, partitions) x global max` (#1459), which is the trade-off that
/// has to be measured rather than assumed.
///
/// `GRAPHFORGE_PARTITION_LOAD_WORKERS` exists to take that measurement. It is
/// deliberately not a product setting: the durable partition layout must stay a
/// pure function of recorded data (R1), and this is an execution choice that
/// changes no output. Absent or unparseable, the default above applies.
pub(super) fn partition_load_workers() -> NonZeroUsize {
    std::env::var("GRAPHFORGE_PARTITION_LOAD_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .and_then(NonZeroUsize::new)
        .unwrap_or(PARTITION_LOAD_WORKERS)
}

/// How often a load polls the stop flag, in records.
const STOP_POLL_RECORDS: usize = 4096;

/// Evidence a worker accumulates while loading one partition, kept apart from
/// the shared [`GraphConstructionEvidence`] until the coordinator merges it.
///
/// Every field here is either a plain sum or a per-partition maximum, so the
/// merge is commutative: the same set of loads produces the same evidence in
/// every consume order. The allocation ledger is deliberately absent; a load
/// installs and removes nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PartitionLoadCounters {
    /// Spill length on disk, credited to `merge_read_blocks` as whole blocks.
    pub(super) spill_bytes: u64,
    /// Records decoded from the spill; also this partition's size for the
    /// `peak_partition_records` maximum.
    pub(super) records: u64,
    /// Payload bytes actually read from the descriptor.
    pub(super) read_bytes: u64,
    /// Non-empty read submissions actually completed.
    pub(super) read_operations: u64,
    /// Page-cache release boundaries observed while reading.
    pub(super) cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
}

impl PartitionLoadCounters {
    /// Fold this load into the shared evidence. Total: every counter a load
    /// accumulates lands here, and nothing else is touched.
    ///
    /// Fields written: `merge_read_blocks`, `merge_read_records`,
    /// `merge_read_bytes`, `merge_read_operations`, `cache_release_operations`,
    /// `cache_release_unsupported_operations`, `cache_released_bytes`,
    /// `peak_cache_release_window_bytes` (max) and `peak_partition_records`
    /// (max). The test `merge_is_total_and_touches_nothing_else` pins that list.
    pub(super) fn merge_into(
        self,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        account_sequential_read(self.spill_bytes, evidence)?;
        evidence.merge_read_records = evidence
            .merge_read_records
            .checked_add(self.records)
            .ok_or_else(|| storage("merge read record count overflows"))?;
        if (self.read_bytes == 0) != (self.read_operations == 0) {
            return Err(storage("fixed-run read bytes and submissions disagree"));
        }
        evidence.merge_read_bytes = evidence
            .merge_read_bytes
            .checked_add(self.read_bytes)
            .ok_or_else(|| storage("merge read byte count overflows"))?;
        evidence.merge_read_operations = evidence
            .merge_read_operations
            .checked_add(self.read_operations)
            .ok_or_else(|| storage("merge read operation count overflows"))?;
        account_cache_release(self.cache_release, evidence)?;
        evidence.peak_partition_records = evidence.peak_partition_records.max(self.records);
        Ok(())
    }
}

/// Reservation bound on simultaneously materialized fixed-width records under
/// [`consume_in_partition_order`]: never more than `min(workers, partitions)`
/// partitions exist at once, and none is larger than the largest.
///
/// This is the order-independent figure an admission gate may reserve
/// against. It is a bound, not an observation: the observed maximum is at
/// most this in every schedule, and equal to it only when the largest
/// partitions happen to coincide. `None` on overflow.
///
/// Test-only for now: no production budget governs materialized partition
/// records (`max_run_records` bounds staged runs, not partitions), so there
/// is no admission gate to hand this to yet. It is defined here, next to the
/// window that guarantees it, so the gate that arrives computes this figure
/// rather than a smaller one.
#[cfg(test)]
pub(super) fn materialized_records_bound(
    workers: NonZeroUsize,
    partitions: usize,
    peak_partition_records: u64,
) -> Option<u64> {
    let slots = u64::try_from(workers.get().min(partitions)).ok()?;
    slots.checked_mul(peak_partition_records)
}

struct State<T> {
    /// Next partition index to hand to a worker. Dispatch is in index order.
    next: usize,
    /// Partitions the coordinator has consumed and dropped.
    released: usize,
    /// Loaded partitions awaiting the coordinator, keyed by index.
    ready: BTreeMap<usize, Result<T, GfError>>,
    /// Workers that have not exited. The coordinator refuses to wait on a
    /// partition no worker can deliver.
    live_workers: usize,
}

struct Shared<T> {
    state: Mutex<State<T>>,
    changed: Condvar,
    /// Set once by the coordinator when it stops consuming, for any reason.
    stop: AtomicBool,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait<'a>(&'a self, guard: MutexGuard<'a, State<T>>) -> MutexGuard<'a, State<T>> {
        self.changed
            .wait(guard)
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Decrements the live-worker count however the worker exits.
struct LiveWorker<'a, T>(&'a Shared<T>);

impl<T> Drop for LiveWorker<'_, T> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.live_workers -= 1;
        drop(state);
        self.0.changed.notify_all();
    }
}

fn worker<T, L>(shared: &Shared<T>, partitions: usize, window: usize, load: &L)
where
    L: Fn(usize, &AtomicBool) -> Result<T, GfError>,
{
    let _live = LiveWorker(shared);
    loop {
        let index = {
            let mut state = shared.lock();
            loop {
                if shared.stop.load(Ordering::Acquire) || state.next >= partitions {
                    return;
                }
                // Backpressure: running loads plus loaded-but-unconsumed
                // results plus the partition being consumed never exceed the
                // window. A slow first partition therefore holds the pool at
                // `window` materialized partitions, not `partitions - 1`
                // (#1448 measured exactly that defect).
                if state.next < state.released + window {
                    break;
                }
                state = shared.wait(state);
            }
            let index = state.next;
            state.next += 1;
            index
        };
        let result = load(index, &shared.stop);
        let mut state = shared.lock();
        state.ready.insert(index, result);
        drop(state);
        shared.changed.notify_all();
    }
}

/// Load `partitions` on `workers` threads and consume every result on the
/// calling thread in partition index order.
///
/// `load(index, stop)` runs on a worker; it may poll `stop` and abandon the
/// load once it is set. `consume(index, value)` runs on the calling thread and
/// may therefore borrow non-`Send` state: the shared evidence, the output
/// writer and the caller's cancellation callback. `value` is dropped when
/// `consume` returns and only then is its slot released to the pool.
///
/// Bound: at any instant at most `workers` partitions are materialized,
/// counting loads in flight, loaded results not yet consumed, and the one
/// being consumed. This is the `threads` bound #1448's reorder buffer claimed
/// and did not deliver.
///
/// The first error, in partition order for loads or immediately for
/// `consume`, stops dispatch, sets `stop`, and is returned after every worker
/// has exited. A load that fails after an earlier partition already failed is
/// never observed.
pub(super) fn consume_in_partition_order<T, L, C>(
    partitions: usize,
    workers: NonZeroUsize,
    load: L,
    mut consume: C,
) -> Result<(), GfError>
where
    T: Send,
    L: Fn(usize, &AtomicBool) -> Result<T, GfError> + Sync,
    C: FnMut(usize, T) -> Result<(), GfError>,
{
    if partitions == 0 {
        return Ok(());
    }
    let window = workers.get();
    let shared = Shared {
        state: Mutex::new(State {
            next: 0,
            released: 0,
            ready: BTreeMap::new(),
            live_workers: window,
        }),
        changed: Condvar::new(),
        stop: AtomicBool::new(false),
    };
    std::thread::scope(|scope| {
        for _ in 0..window {
            scope.spawn(|| worker(&shared, partitions, window, &load));
        }
        let outcome = (|| -> Result<(), GfError> {
            for index in 0..partitions {
                let loaded = {
                    let mut state = shared.lock();
                    loop {
                        if let Some(loaded) = state.ready.remove(&index) {
                            break loaded;
                        }
                        if state.live_workers == 0 {
                            return Err(storage(
                                "partition loaders exited before every partition was delivered",
                            ));
                        }
                        state = shared.wait(state);
                    }
                };
                let consumed = consume(index, loaded?);
                let mut state = shared.lock();
                state.released += 1;
                drop(state);
                shared.changed.notify_all();
                consumed?;
            }
            Ok(())
        })();
        shared.stop.store(true, Ordering::Release);
        shared.changed.notify_all();
        outcome
    })
}

/// Poll the stop flag once every [`STOP_POLL_RECORDS`] records of a load.
pub(super) fn abandon_if_stopped(records: usize, stop: &AtomicBool) -> Result<(), GfError> {
    if records.is_multiple_of(STOP_POLL_RECORDS) && stop.load(Ordering::Acquire) {
        return Err(storage("partition load abandoned after coordinator stop"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
