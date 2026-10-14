//! Per-operator sharing of hydrated path nodes (#1984).
//!
//! `HydratedPathNodes` is invoked once per partition batch. Each invocation
//! used to resolve its node uuids against the stored rows from scratch, so a
//! node reached by several batches, and the fixed cost of every storage read,
//! was paid once per batch. [`SharedNodes`] lives on one bound UDF, so it is
//! per operator and per execution: it starts empty, dies with the plan, and is
//! charged to the query memory pool for as long as it lives.
//!
//! Concurrent partitions all start with an empty set, so reads are single
//! flight and combining: the first caller that misses becomes the leader and
//! hydrates the union of every uuid the waiting callers registered, then all
//! of them are served from the stored block. A node's hydrated row depends
//! only on its uuid, never on the other uuids of a call, so an answer
//! assembled from a shared block equals the answer of a per-batch call.
use super::{
    Invocation, allocation_bytes, hash_table_bytes, hydrate_path_node_children, unique_path_uuids,
};
use crate::chunked_reservation::ChunkedReservation;
use arrow::array::{Array, ArrayRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

type Uuid = [u8; 16];
/// The child columns of one hydrated union, one entry per column.
type Block = Arc<[ArrayRef]>;
/// `(block, row)` addresses, one per requested node.
type Positions = Vec<(usize, usize)>;
/// `(block, row)` of a stored node; blocks are only ever appended.
type Slot = (u32, u32);

/// Share of the pool limit the stored set may hold.
const POOL_SHARE: usize = 8;
/// Cap when the pool reports no finite limit.
const UNBOUNDED_POOL_CAP: usize = 64 << 20;
/// A waiter re-checks cancellation and graph health this often.
const WAIT_SLICE: Duration = Duration::from_millis(50);

pub(super) struct SharedNodes {
    state: Mutex<State>,
    ready: Condvar,
    cap: usize,
}

struct State {
    index: HashMap<Uuid, Slot>,
    blocks: Vec<Block>,
    /// Uuids waiting callers need, folded into the next leader's read.
    pending: HashSet<Uuid>,
    in_flight: bool,
    /// The set reached its cap or the pool refused it: callers that miss
    /// hydrate their own batch, exactly as without sharing.
    exhausted: bool,
    reservation: ChunkedReservation,
    array_bytes: usize,
    index_bytes: usize,
}

impl std::fmt::Debug for SharedNodes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedNodes").finish_non_exhaustive()
    }
}

/// What a call does after consulting the set.
enum Turn {
    /// Every uuid is stored; positions address `blocks`.
    Hit {
        blocks: Vec<Block>,
        positions: Positions,
    },
    /// The set cannot take more; hydrate the whole batch directly.
    Bypass,
    /// This call reads `union`; `own` of its uuids were missing.
    Lead { union: Vec<Uuid>, own: usize },
}

/// Clears `in_flight` and wakes waiters when a leader finishes, errors or
/// panics.
struct LeaderGuard<'a>(&'a SharedNodes);
impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .in_flight = false;
        self.0.ready.notify_all();
    }
}

/// Bytes reserved on the invocation for one phase of a call, returned on drop.
struct Scratch<'a, 'b> {
    h: &'a Invocation<'b>,
    bytes: usize,
}
impl Scratch<'_, '_> {
    fn take(&mut self, bytes: usize) -> Result<()> {
        self.h.reserve(bytes)?;
        self.bytes += bytes;
        Ok(())
    }
    fn release(&mut self) {
        if self.bytes > 0 {
            self.h
                .reservation
                .lock()
                .expect("hydration reservation poisoned")
                .shrink(self.bytes);
            self.bytes = 0;
        }
    }
}
impl Drop for Scratch<'_, '_> {
    fn drop(&mut self) {
        self.release();
    }
}

impl SharedNodes {
    pub(super) fn new(pool: &Arc<dyn MemoryPool>) -> Self {
        let cap = match pool.memory_limit() {
            MemoryLimit::Finite(limit) => limit / POOL_SHARE,
            MemoryLimit::Infinite | MemoryLimit::Unknown => UNBOUNDED_POOL_CAP,
        };
        Self {
            state: Mutex::new(State {
                index: HashMap::new(),
                blocks: Vec::new(),
                pending: HashSet::new(),
                in_flight: false,
                exhausted: false,
                reservation: ChunkedReservation::register("path_node_sharing", pool),
                array_bytes: 0,
                index_bytes: 0,
            }),
            ready: Condvar::new(),
            cap,
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("shared path nodes poisoned")
    }

    /// The hydrated children of `flat`, one entry per flattened position,
    /// reading each distinct uuid from storage at most once per operator.
    pub(super) fn hydrate(
        &self,
        h: &Invocation<'_>,
        flat: &[Uuid],
        batch_size: usize,
    ) -> Result<Vec<ArrayRef>> {
        if flat.is_empty() || h.fields.len() <= 1 {
            return hydrate_path_node_children(h, flat, batch_size);
        }
        let mut scratch = Scratch { h, bytes: 0 };
        scratch.take(hash_table_bytes::<Uuid>(flat.len())?)?;
        scratch.take(allocation_bytes::<Uuid>(flat.len())?)?;
        scratch.take(allocation_bytes::<(usize, usize)>(flat.len())?)?;
        let unique = unique_path_uuids(flat);
        match self.next_turn(h, flat, &unique)? {
            Turn::Hit { blocks, positions } => {
                h.resource.record("shared_hits", unique.len() as u64);
                assemble(&blocks, &positions)
            }
            Turn::Bypass => {
                h.resource.record("shared_bypass", 1);
                drop(unique);
                scratch.release();
                hydrate_path_node_children(h, flat, batch_size)
            }
            Turn::Lead { union, own } => {
                let leader = LeaderGuard(self);
                h.resource
                    .record("shared_hits", (unique.len() - own) as u64);
                drop(unique);
                scratch.release();
                let children = hydrate_path_node_children(h, &union, batch_size)?;
                scratch.take(allocation_bytes::<(usize, usize)>(flat.len())?)?;
                let block: Block = Arc::from(children);
                let (blocks, positions) = self.publish(flat, &union, &block)?;
                drop(leader);
                assemble(&blocks, &positions)
            }
        }
    }

    /// Wait for the set to answer, or elect this call to read for everyone.
    fn next_turn(&self, h: &Invocation<'_>, flat: &[Uuid], unique: &[Uuid]) -> Result<Turn> {
        let mut state = self.lock();
        loop {
            let missing: Vec<Uuid> = unique
                .iter()
                .filter(|uuid| !state.index.contains_key(*uuid))
                .copied()
                .collect();
            if missing.is_empty() {
                let positions = flat
                    .iter()
                    .map(|uuid| {
                        let (block, row) = state.index[uuid];
                        (block as usize, row as usize)
                    })
                    .collect();
                return Ok(Turn::Hit {
                    blocks: state.blocks.clone(),
                    positions,
                });
            }
            if state.exhausted {
                return Ok(Turn::Bypass);
            }
            if state.in_flight {
                state.pending.extend(missing);
                state = self
                    .ready
                    .wait_timeout(state, WAIT_SLICE)
                    .expect("shared path nodes poisoned")
                    .0;
                h.resource.check()?;
                continue;
            }
            state.in_flight = true;
            let own = missing.len();
            let mut union: Vec<Uuid> = state.pending.drain().collect();
            union.extend(missing);
            union.sort_unstable();
            union.dedup();
            union.retain(|uuid| !state.index.contains_key(uuid));
            return Ok(Turn::Lead { union, own });
        }
    }

    /// Store the leader's block when the cap and the pool allow it, and
    /// address `flat` in the blocks that answer it.
    fn publish(
        &self,
        flat: &[Uuid],
        union: &[Uuid],
        block: &Block,
    ) -> Result<(Vec<Block>, Positions)> {
        let mut state = self.lock();
        let stored = state.store(block, union, self.cap);
        let mut blocks = state.blocks.clone();
        let local = if stored {
            None
        } else {
            blocks.push(Arc::clone(block));
            Some(blocks.len() - 1)
        };
        let positions = flat
            .iter()
            .map(|uuid| match state.index.get(uuid) {
                Some(&(block, row)) => Ok((block as usize, row as usize)),
                None => match (local, union.binary_search(uuid)) {
                    (Some(local), Ok(row)) => Ok((local, row)),
                    _ => Err(DataFusionError::Internal(
                        "cypher_path_nodes: hydrated block does not hold a requested node".into(),
                    )),
                },
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((blocks, positions))
    }
}

impl State {
    /// Charge and keep `block`; `false` marks the set exhausted instead.
    fn store(&mut self, block: &Block, union: &[Uuid], cap: usize) -> bool {
        if self.exhausted {
            return false;
        }
        let array_bytes: usize = block.iter().map(Array::get_array_memory_size).sum();
        let kept = match (
            hash_table_bytes::<(Uuid, Slot)>(self.index.len() + union.len()),
            u32::try_from(self.blocks.len()),
            u32::try_from(union.len()),
        ) {
            (Ok(index_bytes), Ok(id), Ok(rows)) => Some((index_bytes, id, rows)),
            _ => None,
        };
        let Some((index_bytes, id, rows)) = kept else {
            return self.exhaust();
        };
        if self.array_bytes + array_bytes + index_bytes > cap {
            return self.exhaust();
        }
        // The previous table is live while the grown one is built.
        let stale = self.index_bytes;
        if self
            .reservation
            .try_grow(array_bytes + index_bytes + stale)
            .is_err()
        {
            return self.exhaust();
        }
        self.blocks.push(Arc::clone(block));
        self.index.reserve(union.len());
        for (row, uuid) in (0..rows).zip(union) {
            self.index.insert(*uuid, (id, row));
        }
        self.reservation.shrink(stale * 2);
        self.array_bytes += array_bytes;
        self.index_bytes = index_bytes;
        true
    }

    fn exhaust(&mut self) -> bool {
        self.exhausted = true;
        self.pending.clear();
        false
    }
}

/// Gather `positions` out of `blocks`, one output array per child column.
fn assemble(blocks: &[Block], positions: &[(usize, usize)]) -> Result<Vec<ArrayRef>> {
    let columns = blocks.first().map_or(0, |block| block.len());
    (0..columns)
        .map(|column| {
            let values: Vec<&dyn Array> = blocks.iter().map(|b| b[column].as_ref()).collect();
            arrow::compute::interleave(&values, positions).map_err(DataFusionError::from)
        })
        .collect()
}
