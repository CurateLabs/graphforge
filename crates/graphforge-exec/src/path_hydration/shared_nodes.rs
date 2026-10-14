//! Per-operator sharing of hydrated path nodes (#1984).
//!
//! `HydratedPathNodes` is invoked once per partition batch. Each invocation
//! used to resolve its node uuids against the stored rows from scratch, so a
//! node reached by several batches, and the fixed cost of every storage read,
//! was paid once per batch. [`SharedNodes`] lives on one bound UDF, so it is
//! per operator and per execution: it starts empty, dies with the plan, and is
//! charged to the query memory pool for as long as it lives.
//!
//! Concurrent partitions all start with an empty set, so each uuid is claimed
//! by the first call that misses it. A call reads its own claims, publishes
//! them to the set, and only then waits for the uuids other calls claimed:
//! disjoint reads run in parallel exactly as without sharing, and a node two
//! calls both need is read once. A node's hydrated row depends only on its
//! uuid, never on the other uuids of a call, so an answer assembled from
//! shared blocks equals the answer of a per-batch call.
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
    /// Uuids some call is reading right now.
    claimed: HashSet<Uuid>,
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
    /// This call claimed `mine`, sorted, and reads them.
    Read { mine: Vec<Uuid> },
}

/// A block this call read that the set could not keep, with its sorted uuids.
struct Local {
    block: Block,
    uuids: Vec<Uuid>,
}

/// Uuids claimed for reading; unclaims them and wakes waiters when the read
/// finishes, errors or panics.
struct Claim<'a> {
    shared: &'a SharedNodes,
    uuids: Vec<Uuid>,
}
impl Drop for Claim<'_> {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for uuid in &self.uuids {
            state.claimed.remove(uuid);
        }
        drop(state);
        self.shared.ready.notify_all();
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
                claimed: HashSet::new(),
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
        let mut local: Option<Local> = None;
        let mut read = 0;
        loop {
            match self.next_turn(h, flat, &unique, local.as_ref())? {
                Turn::Hit { blocks, positions } => {
                    h.resource
                        .record("shared_hits", (unique.len() - read) as u64);
                    return assemble(&blocks, &positions);
                }
                Turn::Bypass => {
                    h.resource.record("shared_bypass", 1);
                    drop((unique, local));
                    scratch.release();
                    return hydrate_path_node_children(h, flat, batch_size);
                }
                Turn::Read { mine } => {
                    read += mine.len();
                    let claim = Claim {
                        shared: self,
                        uuids: mine,
                    };
                    let mut claim_scratch = Scratch { h, bytes: 0 };
                    claim_scratch.take(hash_table_bytes::<Uuid>(claim.uuids.len())?)?;
                    let children = hydrate_path_node_children(h, &claim.uuids, batch_size)?;
                    let block: Block = Arc::from(children);
                    local = self.publish(&claim, &block);
                    if let Some(kept) = &local {
                        scratch.take(allocation_bytes::<Uuid>(kept.uuids.len())?)?;
                    }
                }
            }
        }
    }

    /// Claim what no call is reading yet, or wait for the calls that are.
    fn next_turn(
        &self,
        h: &Invocation<'_>,
        flat: &[Uuid],
        unique: &[Uuid],
        local: Option<&Local>,
    ) -> Result<Turn> {
        let mut state = self.lock();
        loop {
            let missing: Vec<Uuid> = unique
                .iter()
                .filter(|uuid| {
                    !state.index.contains_key(*uuid)
                        && local.is_none_or(|l| l.uuids.binary_search(uuid).is_err())
                })
                .copied()
                .collect();
            if missing.is_empty() {
                let mut blocks = state.blocks.clone();
                let own = local.map(|l| {
                    blocks.push(Arc::clone(&l.block));
                    blocks.len() - 1
                });
                let positions = flat
                    .iter()
                    .map(|uuid| match state.index.get(uuid) {
                        Some(&(block, row)) => Ok((block as usize, row as usize)),
                        None => local_position(own, local, uuid),
                    })
                    .collect::<Result<Positions>>()?;
                return Ok(Turn::Hit { blocks, positions });
            }
            if state.exhausted {
                return Ok(Turn::Bypass);
            }
            let mut mine: Vec<Uuid> = missing
                .into_iter()
                .filter(|uuid| state.claimed.insert(*uuid))
                .collect();
            if !mine.is_empty() {
                mine.sort_unstable();
                return Ok(Turn::Read { mine });
            }
            state = self
                .ready
                .wait_timeout(state, WAIT_SLICE)
                .expect("shared path nodes poisoned")
                .0;
            h.resource.check()?;
        }
    }

    /// Store the block a call read when the cap and the pool allow it;
    /// otherwise hand it back for the call to answer from.
    fn publish(&self, claim: &Claim<'_>, block: &Block) -> Option<Local> {
        let stored = self.lock().store(block, &claim.uuids, self.cap);
        (!stored).then(|| Local {
            block: Arc::clone(block),
            uuids: claim.uuids.clone(),
        })
    }
}

/// `(block, row)` of `uuid` in the block this call kept for itself.
fn local_position(
    own: Option<usize>,
    local: Option<&Local>,
    uuid: &Uuid,
) -> Result<(usize, usize)> {
    match (own, local.map(|l| l.uuids.binary_search(uuid))) {
        (Some(block), Some(Ok(row))) => Ok((block, row)),
        _ => Err(DataFusionError::Internal(
            "cypher_path_nodes: hydrated block does not hold a requested node".into(),
        )),
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
