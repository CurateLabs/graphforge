//! The query memory pool: DataFusion's greedy pool, except that a grouped
//! aggregate that has spilled is guaranteed the memory its spills need, and a
//! floor of memory to merge them in (#1985).
//!
//! DataFusion's grouped aggregate spills in two steps that a greedy pool can
//! starve, and each fails a query that should spill, depending on how its
//! partitions and the exchange feeding them happen to interleave.
//!
//! # The sort of a spill
//!
//! A spill emits the whole state, frees the reservation that held it, and only
//! then reserves memory to sort the emitted batch before writing it out. The
//! emitted batch lives outside the pool in between, and the sort reservation is
//! a fresh request for the memory just released (plus a slice of one batch
//! more). On a greedy pool another partition can take the released memory
//! first, and the request fails with `Failed to reserve memory for sort during
//! spill`.
//!
//! The pool therefore remembers, for each such aggregate, that it has just
//! released its state in a bulk shrink, and grants its next request that the
//! pool cannot serve (up to the size of the pool) whatever else has been
//! reserved since. The bytes already exist: the emitted batch the pool stopped
//! counting a moment earlier holds the whole state, including the groups of the
//! last input batch the released reservation never counted, and the sort only
//! reorders it. A refusal could only fail the query, while granting it lets the
//! reserved total pass the budget by at most the batches being sorted, after
//! which every other request is refused until they are released.
//!
//! The grant is a loan. The request it serves is lent to the consumer, and the
//! grant is renewed once the consumer has released that many bytes. A spill
//! does exactly that: it releases the sort memory when the sorted batch is
//! written, and next restarts its empty state, a request a sibling or a
//! repartition can refuse in the gap, and which fails the query the same way
//! because there is no state left to spill. The loan is repaid by the release
//! that precedes the request, so the restart is served too. A consumer holds at
//! most one loan, and loses it when it unregisters.
//!
//! # The merge of the spill files
//!
//! When an aggregate that has spilled finishes its input, it merges its spill
//! files. A merge pass is admitted in one selection: the aggregate asks for the
//! state it holds while merging and for the narrowest pass (two streams, one
//! batch each) as one request right after its final spill, and the selection
//! then only grows above that, two batches per stream at a time. A narrower
//! retry returns to what the pass held when it began instead of freeing it, and
//! every later pass opens the same way. A loan serves one request beyond the
//! free memory, so one request is what a loan can guarantee: the final spill's
//! bulk shrink arms it, and that request is the one it serves. The loan is armed
//! again by every full release of a piece of a pass, so a spiller has a fresh
//! one for the request that opens each pass.
//!
//! The pool is full at that point when an exchange is holding the input of a
//! busy partition. DataFusion's repartition spills only when the pool refuses a
//! batch, so on a greedy pool it keeps everything up to the limit, and an idle
//! aggregate beside it, one that has already spilled and waits for its input to
//! end, cannot reserve a megabyte to merge (a skewed key distribution makes one
//! partition busy and the other idle).
//!
//! The pool therefore reserves a floor for every aggregate that has spilled,
//! from its first spill until it unregisters (an aggregate that spills and
//! merges is told by its name, `GroupedHashAggregateStream[..]`; a partial
//! aggregate emits early instead of spilling, never merges, and is named
//! `GroupedHashAggregateStream(partial)[..]`, so it gets no floor and no loan): the other consumers that can
//! spill are admitted only while the reserved total plus the unfilled floors of
//! the others stays within the pool, and a request that keeps the aggregate
//! within its own floor is always admitted. An exchange or a sibling that is
//! refused spills by writing, as it already does, instead of taking the memory
//! the merge will need. The floor is
//! [`SPILLER_FLOOR_BYTES`] (the reserve DataFusion's sort pre-reserves for the
//! same purpose, `sort_spill_reservation_bytes`), lowered so that all floors
//! together stay within half the pool.
//!
//! A consumer that cannot spill is not kept from the floors. A refusal fails
//! its query, whatever it asks for: the ungrouped aggregate that counts the
//! groups reserves a few bytes when the first of them arrives, and with the
//! floors kept from it that request failed while the pool had megabytes free.
//! It sees the limit of the pool alone, as on DataFusion's greedy pool, and the
//! loans do not count against it. A spilling aggregate that has not spilled and
//! holds nothing is served the same way for the request that admits its state:
//! there is nothing for it to spill, so a refusal would fail its query too, and
//! the floors apply from its next request on. The loans do not count against
//! such a consumer either: a loan has the reserved total past the limit
//! for the length of a sort, and an ungrouped aggregate that asked for no bytes
//! at all was refused in that window (a request for no bytes is always served).
//! If it takes memory a floor or a loan was reserved from, the aggregate that
//! owns it still has it, so the pool then passes its limit by that much.
//!
//! The floor is a minimum, not an exact bound. The narrowest pass needs two
//! streams of one batch each and the copies' working set, and a batch holds the
//! groups of one emit: an aggregate whose groups are far larger than a quarter of the floor, such as one
//! `collect(DISTINCT ...)` of tens of megabytes in one group, merges with the
//! free memory and the loan, as it did before the floor, and fails when the pool
//! has neither.
//!
//! # The floor kept for the readers of spilled data
//!
//! A consumer that spills recovers from a refusal by writing, and a consumer
//! that reads back what was spilled cannot: the replay of a batch an exchange
//! spilled, the decoder of a spill file and the encoder of a spill each ask the
//! pool as a consumer that cannot spill, and a refusal fails the query. Left
//! alone, the consumers that can spill fill the pool to its last byte, and the
//! first reader to come after them is refused (an aggregate that had grown to
//! within a batch of the limit left an exchange's reader 95 KiB for a 398 KiB
//! batch, #2173).
//!
//! The consumers that can spill are therefore admitted only while a floor for
//! the readers remains unreserved, on top of the unfilled floors of the
//! spilled aggregates. A consumer that cannot spill sees the whole pool, and
//! takes that floor. The readers' floor is a spiller's floor
//! ([`SPILLER_FLOOR_BYTES`]) within an eighth of the pool, never below half a
//! MiB, whatever the number of spillers, and the spillers' floors share the
//! rest of the half: counted as one more spiller's floor it was half of a
//! 16 MiB pool before any aggregate had spilled, and refused the sort's merge
//! reservation where the greedy pool served it; kept beside spiller floors of
//! a full share each, the floors together were three quarters of a 2 MiB pool
//! and refused a partial aggregate's first 5 KiB.
//!
//! # The headroom of the sorts
//!
//! A sort buffers its input until the pool refuses it, so left alone it holds
//! the whole pool, and a consumer that cannot spill and grows with the data
//! (the path state of a variable-length expansion, a hash join's build side) is
//! refused by the next request and fails the query. A query that plans several
//! sort-merge joins runs several sorts at once, each reserving its merge memory
//! up front, which makes this the usual way a sort-merge plan fails in a small
//! pool. A sort can always spill instead, so the pool keeps it short of the
//! limit by the unfilled producer share: three quarters of the pool. One
//! quarter remains for sort buffers ([`SORT_POOL_SHARE_DENOMINATOR`]); their
//! inputs and merge reservations draw on the producer share.
//! Memory already held by consumers that cannot spill counts toward this
//! headroom; reserving it again could refuse an empty sort's first batch.
//! An empty sort also needs its first batch before it can spill: that request
//! sees the real pool limit and the aggregate floors, without sort headroom.
//! Once the reservation holds a batch, the headroom applies again so its next
//! refusal triggers a spill instead of a fatal first-batch error.
//! The headroom is a heuristic, not a reservation: it bounds what the sorts
//! take, and no more than that.
//!
//! Everything else is first-come within the pool limit, as in DataFusion's
//! greedy pool: sorts reserve their merge memory up front, and only grouped
//! aggregates and sorts, identified by name, are affected. Other consumers that
//! can spill (exchanges, joins) get no floor; they are neither capped nor
//! guaranteed here.

use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation, human_readable_size,
};

/// DataFusion's name prefix for the memory consumer of a grouped hash
/// aggregate that spills and merges, the only spillable consumer that
/// reserves memory to spill. The prefix includes the bracket so that a
/// partial aggregate, which emits early instead of spilling and is named
/// `GroupedHashAggregateStream(partial)[..]`, is not matched: it never merges,
/// so it gets no floor and no loan.
pub(crate) const AGGREGATE_CONSUMER_PREFIX: &str = "GroupedHashAggregateStream[";

/// DataFusion's name prefix for the memory consumer of one partition of a sort.
/// The prefix includes the bracket so that the merge reservation of the sort,
/// `ExternalSorterMerge`, which cannot spill, is not matched.
const SORT_CONSUMER_PREFIX: &str = "ExternalSorter[";

/// Target fraction for buffered sorts while the nonspilling producer share
/// is unfilled. Empty sorts admit their first batch against the real cap;
/// later input can trigger a spill while preserving producer headroom.
const SORT_POOL_SHARE_DENOMINATOR: usize = 4;

/// The most the pool reserves for one aggregate that has spilled, the size of
/// the reserve DataFusion's sort sets aside to merge its spills
/// (`sort_spill_reservation_bytes`).
const SPILLER_FLOOR_BYTES: usize = 10 * 1024 * 1024;

/// The least the pool keeps for the readers and writers of spilled data in a
/// small pool: an eighth of a 2 MiB pool, 256 KiB, was taken by the readers
/// and encoders already there and refused an encoder 36 KiB; half a MiB kept
/// that pool served in 300 runs.
const READERS_FLOOR_MINIMUM_BYTES: usize = 512 * 1024;

/// The consumer a sort registers for the merge of its spills: it holds the
/// reserve the sort keeps for that merge (`sort_spill_reservation_bytes`),
/// which the merge's readers draw on first, and the merge pass's selection.
const SORT_MERGE_CONSUMER_PREFIX: &str = "ExternalSorterMerge[";

/// A greedy memory pool of `pool_size` bytes with the floors and the grant
/// described in the module documentation.
#[derive(Debug)]
pub(crate) struct QueryMemoryPool {
    pool_size: usize,
    state: Mutex<State>,
    /// The most bytes the pool has had reserved since it was created.
    /// Shared so the owner of a pool the runtime has taken over (and
    /// wrapped) can still read it.
    peak: Arc<AtomicUsize>,
}

#[derive(Debug, Default)]
struct State {
    /// Bytes reserved by every consumer.
    reserved: usize,
    /// Bytes already held by consumers that cannot spill, filling sort headroom.
    non_spilling_reserved: usize,
    /// The grouped aggregate consumers that have spilled, by consumer id.
    spillers: HashMap<usize, Spiller>,
}

/// A grouped aggregate that has spilled.
#[derive(Debug, Clone, Copy)]
struct Spiller {
    /// Its right to a request the pool cannot serve.
    grant: Grant,
    /// Bytes the consumer holds. Reservations split off a consumer share its
    /// registration without telling the pool, so the pool sums the changes
    /// of the consumer, not of a reservation.
    held: usize,
}

/// A spilling aggregate's right to a request the pool cannot serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Grant {
    /// The consumer released its state in a bulk shrink, or repaid its loan,
    /// and has not made a request the pool refused since.
    Armed,
    /// A request was served beyond the free memory; this many bytes of it are
    /// not released yet.
    Lent(usize),
}

impl QueryMemoryPool {
    /// A pool of `pool_size` bytes.
    pub(crate) fn new(pool_size: usize) -> Self {
        Self {
            pool_size,
            state: Mutex::new(State::default()),
            peak: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The pool's high-water mark of reserved bytes. The pool raises it with
    /// every reservation it serves, loans and unconditional growth included;
    /// repeated and concurrent reads share it without resetting it.
    pub(crate) fn peak_gauge(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.peak)
    }

    /// Adds `additional` to the reserved total and raises the high-water mark.
    fn reserve(&self, state: &mut State, reservation: &MemoryReservation, additional: usize) {
        state.reserved += additional;
        if !reservation.consumer().can_spill() {
            state.non_spilling_reserved += additional;
        }
        self.peak.fetch_max(state.reserved, Ordering::Relaxed);
    }

    fn locked(&self) -> MutexGuard<'_, State> {
        // The counters stay consistent if a holder panicked: every update is
        // one addition or subtraction.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn is_spilling_aggregate(consumer: &MemoryConsumer) -> bool {
        consumer.can_spill() && consumer.name().starts_with(AGGREGATE_CONSUMER_PREFIX)
    }

    fn is_sort(consumer: &MemoryConsumer) -> bool {
        consumer.can_spill() && consumer.name().starts_with(SORT_CONSUMER_PREFIX)
    }

    fn is_sort_merge(consumer: &MemoryConsumer) -> bool {
        consumer.can_spill() && consumer.name().starts_with(SORT_MERGE_CONSUMER_PREFIX)
    }

    /// Keep buffered sorts short of the producer share. An empty reservation
    /// cannot spill, so its first bounded input must see the real pool limit.
    fn headroom(&self, reservation: &MemoryReservation, state: &State) -> usize {
        if Self::is_sort(reservation.consumer()) && reservation.size() > 0 {
            self.pool_size
                .saturating_sub(self.pool_size / SORT_POOL_SHARE_DENOMINATOR)
                .saturating_sub(state.non_spilling_reserved)
        } else {
            0
        }
    }

    /// The bytes the pool reserves for each of `spillers` aggregates: the
    /// floors together, the readers' among them, never pass half the pool, so
    /// the other consumers keep the rest.
    fn floor(&self, spillers: usize) -> usize {
        SPILLER_FLOOR_BYTES
            .min((self.pool_size / 2).saturating_sub(self.readers_floor()) / spillers.max(1))
    }

    /// The bytes the pool keeps for the readers and writers of spilled data:
    /// a spiller's floor, within an eighth of the pool and never below
    /// [`READERS_FLOOR_MINIMUM_BYTES`], whatever the number of spillers. What
    /// they hold is data-dependent (a decoded batch each, under 1 MiB in every
    /// refusal seen; a spill's encoder, 2.6 MB and 9.4 MB in the two seen), so
    /// the floor is the one size the pool already keeps for a merge, and the
    /// cap is what a small pool can spare: a quarter of a 16 MiB pool refused
    /// the sort's merge reservation its 2 MiB with 10.4 MB held, and half the
    /// pool, the share one more spiller's floor took before any aggregate had
    /// spilled, refused it with 8.4 MB held, where the greedy pool served it.
    ///
    /// A pool smaller than twice [`READERS_FLOOR_MINIMUM_BYTES`] cannot spare
    /// the minimum: the floor is then half the pool and the spillers' floors
    /// are nothing, so the floors together still stay within half the pool.
    fn readers_floor(&self) -> usize {
        SPILLER_FLOOR_BYTES
            .min((self.pool_size / 8).max(READERS_FLOOR_MINIMUM_BYTES))
            .min(self.pool_size / 2)
    }

    /// The bytes lent to the spillers, which the reserved total counts beyond
    /// the pool.
    fn lent(state: &State) -> usize {
        state
            .spillers
            .values()
            .map(|spiller| match spiller.grant {
                Grant::Lent(lent) => lent,
                Grant::Armed => 0,
            })
            .sum()
    }

    /// The bytes reserved for the floors of the spillers other than `consumer`
    /// that they do not hold yet.
    fn unfilled_floors_except(&self, state: &State, consumer: usize) -> usize {
        let floor = self.floor(state.spillers.len());
        state
            .spillers
            .iter()
            .filter(|(id, _)| **id != consumer)
            .map(|(_, spiller)| floor.saturating_sub(spiller.held))
            .sum()
    }
}

impl MemoryPool for QueryMemoryPool {
    fn name(&self) -> &str {
        "query"
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.locked().spillers.remove(&consumer.id());
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.locked();
        self.reserve(&mut state, reservation, additional);
        if let Some(spiller) = state.spillers.get_mut(&reservation.consumer().id()) {
            spiller.held += additional;
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.locked();
        state.reserved = state.reserved.saturating_sub(shrink);
        if !reservation.consumer().can_spill() {
            state.non_spilling_reserved = state.non_spilling_reserved.saturating_sub(shrink);
        }
        let id = reservation.consumer().id();
        if let Some(spiller) = state.spillers.get_mut(&id) {
            spiller.held = spiller.held.saturating_sub(shrink);
        }
        if shrink == 0 {
            return;
        }
        // The reservation is already reduced: a bulk shrink released at least
        // as much as it kept.
        let bulk = shrink >= reservation.size();
        match state.spillers.get_mut(&id) {
            Some(spiller) if bulk => spiller.grant = Grant::Armed,
            Some(spiller) => {
                if let Grant::Lent(lent) = spiller.grant {
                    spiller.grant = match lent.saturating_sub(shrink) {
                        0 => Grant::Armed,
                        lent => Grant::Lent(lent),
                    };
                }
            }
            None if bulk && Self::is_spilling_aggregate(reservation.consumer()) => {
                state.spillers.insert(
                    id,
                    Spiller {
                        grant: Grant::Armed,
                        held: reservation.size(),
                    },
                );
            }
            None => {}
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        // Nothing to reserve: nothing to refuse, and nothing to lend. A request
        // for no bytes must not use up a grant or fail a query because a loan
        // has the pool over its limit.
        if additional == 0 {
            return Ok(());
        }
        let mut state = self.locked();
        let id = reservation.consumer().id();
        // An aggregate that has not spilled and holds nothing has nothing to
        // spill: its state was never admitted, so a refusal fails its query as
        // it fails a consumer that cannot spill (the spill releases nothing,
        // and the restart of its empty state is refused the same way). Its
        // first request sees the limit of the pool alone, as that consumer's
        // requests do; the floors apply from its next request on. An aggregate
        // that has spilled holds a floor and a loan for its restart instead.
        // A sort holding nothing is in the same position (its first batch
        // cannot spill, and the sort fails on its refusal), and holds no
        // loan: its first batch sees the limit of the pool alone, as its
        // headroom does.
        let first_state = reservation.size() == 0
            && ((Self::is_spilling_aggregate(reservation.consumer())
                && !state.spillers.contains_key(&id))
                || Self::is_sort(reservation.consumer()));
        // The reserve a sort keeps for the merge of its spills is the readers'
        // reserve itself: the merge's readers draw on it first, and a refusal
        // fails the sort (the sort asks it again at every spill). Within the
        // readers' floor it is served at the limit of the pool; the merge's
        // selection, which grows past the floor and backs off at a refusal,
        // sees the floors as every consumer that can spill does.
        let readers_reserve = Self::is_sort_merge(reservation.consumer())
            && reservation.size().saturating_add(additional) <= self.readers_floor();
        // A consumer that can spill recovers from a refusal by spilling, so
        // the floors are kept from it. One that cannot spill fails its query
        // on a refusal, so it is refused only at the limit of the pool, which
        // the loans do not count against: they are bytes the pool stopped
        // counting and took back, and they are repaid within a spill.
        let (reserved, others) =
            if reservation.consumer().can_spill() && !first_state && !readers_reserve {
                (
                    state.reserved,
                    self.unfilled_floors_except(&state, id)
                        .saturating_add(self.headroom(reservation, &state))
                        // The readers' floor stays with the readers of spilled data.
                        .saturating_add(self.readers_floor()),
                )
            } else {
                (state.reserved.saturating_sub(Self::lent(&state)), 0)
            };
        // A request that keeps a spiller within its floor is served, whatever
        // else the pool holds: the floor is reserved for it. Any other request
        // is served while it leaves the floors of the other spillers unfilled
        // memory to be filled. Either way a spill's small requests (the emit
        // and the reset of the state) leave the grant in place for the one
        // that sorts the emitted batch.
        let within_floor = state.spillers.get(&id).is_some_and(|spiller| {
            spiller.held.saturating_add(additional) <= self.floor(state.spillers.len())
        });
        let fits = reserved.saturating_add(additional).saturating_add(others) <= self.pool_size;
        if !(within_floor || fits) {
            let lend = additional <= self.pool_size
                && state
                    .spillers
                    .get(&id)
                    .is_some_and(|spiller| spiller.grant == Grant::Armed);
            if !lend {
                let available = self
                    .pool_size
                    .saturating_sub(reserved)
                    .saturating_sub(others);
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {}(used: {}, pool_size: {})",
                    human_readable_size(additional),
                    reservation.consumer().name(),
                    human_readable_size(reservation.size()),
                    human_readable_size(available),
                    self.name(),
                    human_readable_size(reserved),
                    human_readable_size(self.pool_size),
                )));
            }
            if let Some(spiller) = state.spillers.get_mut(&id) {
                spiller.grant = Grant::Lent(additional);
            }
        }
        self.reserve(&mut state, reservation, additional);
        if let Some(spiller) = state.spillers.get_mut(&id) {
            spiller.held += additional;
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.locked().reserved
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size)
    }
}

impl Display for QueryMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}(used: {}, pool_size: {})",
            self.name(),
            human_readable_size(self.locked().reserved),
            human_readable_size(self.pool_size)
        )
    }
}

#[cfg(test)]
mod merge_pass_tests;
#[cfg(test)]
mod tests;
