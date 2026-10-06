//! Endpoint resolution by a node-surrogate index (ADR 0057, #1387).
//!
//! A new node's surrogate is the base node tail plus its 1-based rank among
//! new nodes in UUID order (`assign_surrogates`). For an initial build the
//! sorted array of new node UUIDs is therefore the whole UUID-to-surrogate map:
//! index `i` holds the node whose surrogate is `base + i + 1`. Edge details
//! already carry both endpoint UUIDs, so an initial build within
//! `max_node_index_bytes` resolves endpoints by probing this index instead of
//! routing, sorting and merge-joining a separate endpoint family.
//!
//! Probes are pure reads of an immutable array. They run on lanes leased from
//! the instance's construction CPU admission (ADR 0047), so the lane count is
//! whatever the instance grants and never affects the resolved surrogates.

use std::num::NonZeroUsize;
use std::sync::Arc;

use graphforge_core::GfError;

use super::cpu_admission::ConstructionCpuAdmission;
use super::storage;

/// Index bytes per new node: one 16-byte UUID.
pub(crate) const NODE_INDEX_RECORD_BYTES: u64 = 16;

/// Fewest probes worth a lane of their own: below this, spawning the lane
/// costs more than the binary searches it would take over.
const MIN_PROBES_PER_LANE: usize = 4_096;

/// Accumulates node identities in UUID order and checks that their
/// surrogates are the dense ranks the index relies on.
#[derive(Default)]
pub(crate) struct NodeIndexBuilder {
    uuids: Vec<[u8; 16]>,
    base: Option<u64>,
}

impl NodeIndexBuilder {
    /// Append the next node identity in UUID order.
    ///
    /// # Errors
    /// Refuses a UUID out of order or a surrogate that is not the next dense
    /// rank: the index would then resolve some endpoint to the wrong node.
    pub(crate) fn push(&mut self, uuid: [u8; 16], surrogate: u64) -> Result<(), GfError> {
        let base = match self.base {
            Some(base) => base,
            None => *self.base.insert(
                surrogate
                    .checked_sub(1)
                    .ok_or_else(|| storage("node surrogates are not dense UUID ranks"))?,
            ),
        };
        let expected = u64::try_from(self.uuids.len())
            .ok()
            .and_then(|rank| base.checked_add(rank))
            .and_then(|rank| rank.checked_add(1));
        if expected != Some(surrogate) || self.uuids.last().is_some_and(|last| *last >= uuid) {
            return Err(storage("node surrogates are not dense UUID ranks"));
        }
        self.uuids.push(uuid);
        Ok(())
    }

    pub(crate) fn finish(self) -> NodeIndex {
        NodeIndex {
            uuids: self.uuids,
            base: self.base.unwrap_or(0),
        }
    }
}

/// Sorted new-node UUIDs and the surrogate their ranks start after.
pub(crate) struct NodeIndex {
    uuids: Vec<[u8; 16]>,
    base: u64,
}

impl NodeIndex {
    /// Resolve every UUID in `uuids` into `surrogates`, probing on up to one
    /// lane per [`MIN_PROBES_PER_LANE`] probes: the `held_lanes` the caller
    /// already leased and has idle, plus whatever the admission grants now.
    /// Without either, the calling thread probes alone.
    ///
    /// # Errors
    /// `endpoint UUID lacks node surrogate` when a UUID is not a new node.
    pub(crate) fn resolve(
        &self,
        uuids: &[[u8; 16]],
        surrogates: &mut [u64],
        held_lanes: usize,
        admission: Option<&Arc<ConstructionCpuAdmission>>,
    ) -> Result<(), GfError> {
        debug_assert_eq!(uuids.len(), surrogates.len());
        let useful = uuids.len() / MIN_PROBES_PER_LANE;
        let held = held_lanes.min(useful);
        let lease =
            admission
                .zip(NonZeroUsize::new(useful - held))
                .and_then(|(admission, wanted)| {
                    admission.try_acquire(wanted.min(
                        NonZeroUsize::new(admission.limit()).expect("positive admission limit"),
                    ))
                });
        let lanes = held + lease.as_ref().map_or(0, |lease| lease.lanes().get());
        let resolved = if lanes <= 1 {
            self.resolve_serial(uuids, surrogates)
        } else {
            let chunk = uuids.len().div_ceil(lanes);
            std::thread::scope(|scope| {
                let lanes: Vec<_> = uuids
                    .chunks(chunk)
                    .zip(surrogates.chunks_mut(chunk))
                    .map(|(uuids, surrogates)| {
                        scope.spawn(move || self.resolve_serial(uuids, surrogates))
                    })
                    .collect();
                lanes.into_iter().try_for_each(|lane| {
                    lane.join()
                        .map_err(|_| storage("endpoint resolution lane panicked"))?
                })
            })
        };
        drop(lease);
        resolved
    }

    /// Probe in key order. Endpoints arrive in edge order, which scatters them
    /// across the index, so independent binary searches miss cache at nearly
    /// every step. Sorted, consecutive probes land near each other: each one
    /// gallops forward from the previous rank and stays in cache.
    fn resolve_serial(&self, uuids: &[[u8; 16]], surrogates: &mut [u64]) -> Result<(), GfError> {
        let mut probes: Vec<([u8; 16], u32)> = uuids
            .iter()
            .enumerate()
            .map(|(position, uuid)| (*uuid, position as u32))
            .collect();
        probes.sort_unstable();
        let mut low = 0;
        for (uuid, position) in probes {
            let rank = self
                .rank_from(low, &uuid)
                .ok_or_else(|| storage("endpoint UUID lacks node surrogate"))?;
            surrogates[position as usize] = self.base + rank as u64 + 1;
            low = rank;
        }
        Ok(())
    }

    /// The rank of `uuid`, searching only at or after `low`: an exponential
    /// search for the bracket, then a binary search inside it.
    fn rank_from(&self, low: usize, uuid: &[u8; 16]) -> Option<usize> {
        let tail = self.uuids.get(low..)?;
        let mut bound = 1;
        while bound < tail.len() && tail[bound] < *uuid {
            bound *= 2;
        }
        let start = bound / 2;
        let end = (bound + 1).min(tail.len());
        tail.get(start..end)?
            .binary_search(uuid)
            .ok()
            .map(|at| low + start + at)
    }
}

#[cfg(test)]
mod tests;
