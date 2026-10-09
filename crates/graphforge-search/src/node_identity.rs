//! Topology node rows checked against the generation's node identity authority.
//!
//! A membership projection reads every node row. Each row's `(node_id,
//! node_uuid)` must be the authority's own pairing, and with the strictly
//! ascending `node_id`s the caller enforces, the rows must be exactly the
//! authority's live nodes, each once. Which authority proves it:
//!
//! * **Ordinal (v4)**, the facade's generation-pinned handle. The ordinal block
//!   of each row's `node_id` must hold its `node_uuid` (a tombstoned or
//!   undeclared ordinal resolves to nothing and disagrees), every declared
//!   ordinal the rows skip must be tombstoned, and the UUIDs must be distinct:
//!   ascending with the ordinals when the authenticated manifest says UUIDs
//!   follow ordinal order, otherwise checked against the UUIDs already seen.
//!   Only the ordinal and tombstone blocks those lookups read are read, each
//!   authenticated by its own checksum, so the cost follows the nodes and never
//!   the edges.
//! * **UUID membership (v3)**, for a caller without a session authority. The
//!   index is opened and fully authenticated (every node and edge record), each
//!   row's UUID must resolve to its `node_id`, and the row count must equal the
//!   authenticated live-node count.
//! * **Neither** (pre-index legacy graphs): the UUIDs must be distinct.

use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::Mutex;

use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};
use graphforge_storage::SearchArtifactError;
use graphforge_storage::ordinal_identity_v4::{V4OrdinalIdentityError, V4OrdinalIdentityHandle};

/// A facade's generation-pinned ordinal identity authority, the same handle its
/// queries resolve destination identities through.
pub type SessionOrdinalIdentity = Mutex<V4OrdinalIdentityHandle>;

/// One projection's identity check over the node rows it visits in order.
pub(crate) struct NodeIdentityCheck<'a> {
    authority: Authority<'a>,
}

enum Authority<'a> {
    Ordinal(Box<OrdinalCheck<'a>>),
    Unindexed(BTreeSet<[u8; 16]>),
}

struct OrdinalCheck<'a> {
    handle: &'a SessionOrdinalIdentity,
    /// Declared ordinals, inclusive, ascending.
    ranges: Vec<RangeInclusive<u64>>,
    /// Largest lookup the handle accepts.
    chunk: usize,
    /// Lowest ordinal neither a row nor a skipped-ordinal check has covered.
    next: u64,
    distinct: Distinct,
    /// A declared ordinal the rows skipped still resolves to a live UUID.
    omitted_live: bool,
}

enum Distinct {
    /// UUIDs ascend with ordinals, so each row's UUID must exceed the last.
    Ascending(Option<[u8; 16]>),
    /// No order claim: every UUID must be new.
    Seen(BTreeSet<[u8; 16]>),
}

impl<'a> NodeIdentityCheck<'a> {
    /// Select the authority for `project_dir`. A session ordinal authority is
    /// preferred; without one, rows are checked for repeats only.
    pub(crate) fn open(
        project_dir: &Path,
        ordinal: Option<&'a SessionOrdinalIdentity>,
    ) -> Result<Self, SearchArtifactError> {
        let authority = if let Some(handle) = ordinal {
            Authority::Ordinal(Box::new(OrdinalCheck::open(project_dir, handle)?))
        } else {
            Authority::Unindexed(BTreeSet::new())
        };
        Ok(Self { authority })
    }

    /// Resolve one batch of rows; `true` where the authority holds the row's
    /// own `(node_id, node_uuid)` pairing.
    pub(crate) fn resolve_batch<C>(
        &mut self,
        uuids: &FixedSizeBinaryArray,
        surrogates: &UInt64Array,
        checkpoint: &mut C,
    ) -> Result<Vec<bool>, SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        let mut batch_uuids = Vec::with_capacity(uuids.len());
        for row in 0..uuids.len() {
            let bytes: [u8; 16] = uuids
                .value(row)
                .try_into()
                .map_err(|_| source("topology node_uuid is not 16 bytes"))?;
            batch_uuids.push(uuid::Uuid::from_bytes(bytes));
        }
        if surrogates.len() != batch_uuids.len() {
            return Err(source("topology node_id and node_uuid lengths differ"));
        }
        let agrees = match &mut self.authority {
            Authority::Ordinal(check) => {
                check.resolve_batch(&batch_uuids, surrogates, checkpoint)?
            }
            Authority::Unindexed(_) => vec![true; batch_uuids.len()],
        };
        // Callers zip the verdicts with the rows; one per row, or none pass.
        if agrees.len() != batch_uuids.len() {
            return Err(source("node identity verdicts do not cover the batch"));
        }
        Ok(agrees)
    }

    /// Whether `node_uuid` is distinct from every row before it. The caller has
    /// already checked that this row's `node_id` exceeds the previous one.
    pub(crate) fn distinct(&mut self, node_uuid: [u8; 16]) -> bool {
        match &mut self.authority {
            Authority::Unindexed(seen) => seen.insert(node_uuid),
            Authority::Ordinal(check) => match &mut check.distinct {
                Distinct::Ascending(last) => {
                    let ascends = last.is_none_or(|prior| node_uuid > prior);
                    *last = Some(node_uuid);
                    ascends
                }
                Distinct::Seen(seen) => seen.insert(node_uuid),
            },
        }
    }

    /// Prove after the last row that no live node was left out.
    pub(crate) fn finish<C>(self, checkpoint: &mut C) -> Result<(), SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        match self.authority {
            Authority::Unindexed(_) => Ok(()),
            Authority::Ordinal(mut check) => {
                if let Some(last) = check.ranges.last().map(|range| *range.end()) {
                    check.check_skipped(last.saturating_add(1), checkpoint)?;
                }
                if check.omitted_live {
                    return Err(source(
                        "topology rows omit a live node of the authenticated ordinal identity",
                    ));
                }
                Ok(())
            }
        }
    }
}

impl<'a> OrdinalCheck<'a> {
    fn open(
        project_dir: &Path,
        handle: &'a SessionOrdinalIdentity,
    ) -> Result<Self, SearchArtifactError> {
        let mut guard = lock(handle)?;
        let generation = graphforge_storage::read_topology_generation(project_dir)
            .map_err(|error| source(error.to_string()))?;
        if guard.topology_generation() != generation {
            return Err(source(format!(
                "stale ordinal identity generation {} (graph generation {generation})",
                guard.topology_generation()
            )));
        }
        let distinct = if guard.uuid_order_matches_ordinals().map_err(ordinal)? {
            Distinct::Ascending(None)
        } else {
            Distinct::Seen(BTreeSet::new())
        };
        Ok(Self {
            ranges: guard.ordinal_ranges(),
            chunk: guard.max_requested_ids().max(1),
            handle,
            next: 0,
            distinct,
            omitted_live: false,
        })
    }

    fn resolve_batch<C>(
        &mut self,
        uuids: &[uuid::Uuid],
        surrogates: &UInt64Array,
        checkpoint: &mut C,
    ) -> Result<Vec<bool>, SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        let ids = (0..surrogates.len())
            .map(|row| surrogates.value(row))
            .collect::<Vec<_>>();
        for &id in &ids {
            // Rows out of order are refused by the caller; only a forward step
            // skips ordinals.
            if id >= self.next {
                self.check_skipped(id, checkpoint)?;
                self.next = id.saturating_add(1);
            }
        }
        let mut agrees = Vec::with_capacity(ids.len());
        for (ids, uuids) in ids.chunks(self.chunk).zip(uuids.chunks(self.chunk)) {
            checkpoint()?;
            let lookup = lock(self.handle)?
                .lookup_node_uuids_pinned(ids)
                .map_err(ordinal)?;
            agrees.extend(
                lookup
                    .values
                    .iter()
                    .zip(uuids)
                    .map(|(found, expected)| *found == Some(*expected)),
            );
        }
        Ok(agrees)
    }

    /// Every declared ordinal in `self.next..end` had no row, so each must be
    /// tombstoned: a lookup must resolve none of them.
    fn check_skipped<C>(&mut self, end: u64, checkpoint: &mut C) -> Result<(), SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        if self.omitted_live || end <= self.next {
            return Ok(());
        }
        let mut pending = Vec::new();
        for range in &self.ranges {
            let first = (*range.start()).max(self.next);
            let last = (*range.end()).min(end - 1);
            for id in first..=last {
                pending.push(id);
                if pending.len() == self.chunk {
                    checkpoint()?;
                    if self.resolves_any(&pending)? {
                        self.omitted_live = true;
                        return Ok(());
                    }
                    pending.clear();
                }
            }
        }
        if !pending.is_empty() {
            checkpoint()?;
            self.omitted_live = self.resolves_any(&pending)?;
        }
        Ok(())
    }

    fn resolves_any(&self, ids: &[u64]) -> Result<bool, SearchArtifactError> {
        Ok(lock(self.handle)?
            .lookup_node_uuids_pinned(ids)
            .map_err(ordinal)?
            .values
            .iter()
            .any(Option::is_some))
    }
}

fn lock(
    handle: &SessionOrdinalIdentity,
) -> Result<std::sync::MutexGuard<'_, V4OrdinalIdentityHandle>, SearchArtifactError> {
    handle
        .lock()
        .map_err(|_| source("ordinal identity handle is poisoned"))
}

#[allow(clippy::needless_pass_by_value)] // used as a `map_err` adapter
fn ordinal(error: V4OrdinalIdentityError) -> SearchArtifactError {
    source(error.to_string())
}

fn source(reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::SourceSnapshot {
        reason: reason.into(),
    }
}
