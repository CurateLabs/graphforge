//! Compact, allocation-bounded membership sets for nominated graph UUIDs.

use std::collections::BTreeSet;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

pub(crate) trait UuidMembership: Sync {
    fn len(&self) -> usize;
    fn contains_uuid(&self, uuid: &[u8; 16]) -> bool;
    fn is_empty(&self) -> bool;
    fn min_uuid(&self) -> Option<[u8; 16]>;
    fn max_uuid(&self) -> Option<[u8; 16]>;

    fn may_contain_in_range(&self, min: &[u8; 16], max: &[u8; 16]) -> bool {
        self.any_match_in_range(min, max, &|_| true)
    }

    fn any_match_in_range(
        &self,
        min: &[u8; 16],
        max: &[u8; 16],
        matches: &dyn Fn(&[u8; 16]) -> bool,
    ) -> bool;

    /// Whether every UUID in `min..=max` is a member. A read that knows this
    /// for a whole row group has no row to filter out of it. The default is
    /// the safe answer: unknown.
    fn contains_every_in_range(&self, _min: &[u8; 16], _max: &[u8; 16]) -> bool {
        false
    }
}

impl UuidMembership for BTreeSet<[u8; 16]> {
    fn len(&self) -> usize {
        self.len()
    }

    fn contains_uuid(&self, uuid: &[u8; 16]) -> bool {
        self.contains(uuid)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn min_uuid(&self) -> Option<[u8; 16]> {
        self.first().copied()
    }

    fn max_uuid(&self) -> Option<[u8; 16]> {
        self.last().copied()
    }

    fn any_match_in_range(
        &self,
        min: &[u8; 16],
        max: &[u8; 16],
        matches: &dyn Fn(&[u8; 16]) -> bool,
    ) -> bool {
        self.range(*min..=*max).any(matches)
    }
}

/// A strictly ascending run of UUIDs, borrowed from the caller. It is exact and
/// allocation-free: membership is a binary search and a range probe is a
/// partition point, so a caller that already owns its sorted targets charges
/// them once, as the slice.
pub(crate) struct SortedUuids<'a>(pub(crate) &'a [[u8; 16]]);

impl SortedUuids<'_> {
    /// Whether the slice is strictly ascending, which membership relies on.
    pub(crate) fn is_strictly_ascending(&self) -> bool {
        self.0.windows(2).all(|pair| pair[0] < pair[1])
    }
}

impl UuidMembership for SortedUuids<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn contains_uuid(&self, uuid: &[u8; 16]) -> bool {
        self.0.binary_search(uuid).is_ok()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn min_uuid(&self) -> Option<[u8; 16]> {
        self.0.first().copied()
    }

    fn max_uuid(&self) -> Option<[u8; 16]> {
        self.0.last().copied()
    }

    fn any_match_in_range(
        &self,
        min: &[u8; 16],
        max: &[u8; 16],
        matches: &dyn Fn(&[u8; 16]) -> bool,
    ) -> bool {
        let start = self.0.partition_point(|uuid| uuid < min);
        self.0[start..]
            .iter()
            .take_while(|uuid| *uuid <= max)
            .any(matches)
    }
}

/// A read-time intersection of nomination sets and an optional equality
/// candidate set. It avoids materializing a second UUID-sized collection.
pub(crate) struct UuidFilter<'a> {
    pub(crate) nominations: &'a [&'a dyn UuidMembership],
    pub(crate) candidates: Option<&'a BTreeSet<[u8; 16]>>,
}

impl UuidFilter<'_> {
    pub(crate) fn contains(&self, uuid: &[u8; 16]) -> bool {
        self.nominations.iter().all(|set| set.contains_uuid(uuid))
            && self
                .candidates
                .is_none_or(|candidates| candidates.contains(uuid))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.nominations.iter().any(|set| set.is_empty())
            || self.candidates.is_some_and(BTreeSet::is_empty)
    }
}

impl UuidMembership for UuidFilter<'_> {
    fn len(&self) -> usize {
        self.nominations
            .iter()
            .map(|set| set.len())
            .min()
            .or_else(|| self.candidates.map(BTreeSet::len))
            .unwrap_or(0)
    }

    fn contains_uuid(&self, uuid: &[u8; 16]) -> bool {
        self.contains(uuid)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn min_uuid(&self) -> Option<[u8; 16]> {
        let lower = self
            .nominations
            .iter()
            .filter_map(|set| set.min_uuid())
            .max();
        self.candidates
            .and_then(|set| set.first().copied())
            .into_iter()
            .chain(lower)
            .max()
    }

    fn max_uuid(&self) -> Option<[u8; 16]> {
        let upper = self
            .nominations
            .iter()
            .filter_map(|set| set.max_uuid())
            .min();
        self.candidates
            .and_then(|set| set.last().copied())
            .into_iter()
            .chain(upper)
            .min()
    }

    fn may_contain_in_range(&self, min: &[u8; 16], max: &[u8; 16]) -> bool {
        self.any_match_in_range(min, max, &|_| true)
    }

    fn any_match_in_range(
        &self,
        min: &[u8; 16],
        max: &[u8; 16],
        matches: &dyn Fn(&[u8; 16]) -> bool,
    ) -> bool {
        let smallest_nomination = self.nominations.iter().min_by_key(|set| set.len());
        match (smallest_nomination, self.candidates) {
            (Some(nomination), Some(candidates)) if candidates.len() < nomination.len() => {
                candidates
                    .range(*min..=*max)
                    .any(|uuid| self.contains(uuid) && matches(uuid))
            }
            (Some(nomination), _) => nomination
                .any_match_in_range(min, max, &|uuid| self.contains(uuid) && matches(uuid)),
            (None, Some(candidates)) => candidates.range(*min..=*max).any(matches),
            (None, None) => false,
        }
    }
}

/// The half-open UUID interval `low..high`, unbounded above when `high` is
/// `None`, optionally intersected with another membership.
///
/// A partitioned scan gives each partition one interval, so a key's base,
/// mutation and tombstone rows all fall in the same partition and the
/// newest-wins merge stays correct inside it. Reads consult the interval
/// through the same membership calls that prune fragments, row groups and
/// pages, so a partition opens only what intersects its interval.
pub(crate) struct UuidRange<'a> {
    low: [u8; 16],
    high: Option<[u8; 16]>,
    inner: Option<&'a dyn UuidMembership>,
}

impl<'a> UuidRange<'a> {
    pub(crate) fn new(
        low: [u8; 16],
        high: Option<[u8; 16]>,
        inner: Option<&'a dyn UuidMembership>,
    ) -> Self {
        Self { low, high, inner }
    }

    /// The largest UUID inside the interval, `None` for an empty interval.
    fn top(&self) -> Option<[u8; 16]> {
        match self.high {
            None => Some([u8::MAX; 16]),
            Some(high) => u128::from_be_bytes(high)
                .checked_sub(1)
                .map(u128::to_be_bytes)
                .filter(|top| *top >= self.low),
        }
    }

    /// The smallest and largest UUID a member can be, `None` when none can.
    fn bounds(&self) -> Option<([u8; 16], [u8; 16])> {
        let top = self.top()?;
        match self.inner {
            None => Some((self.low, top)),
            Some(inner) => self.clip(&inner.min_uuid()?, &inner.max_uuid()?),
        }
    }

    fn in_interval(&self, uuid: &[u8; 16]) -> bool {
        *uuid >= self.low && self.high.is_none_or(|high| *uuid < high)
    }

    /// `min..=max` clipped to the interval, `None` when nothing remains.
    fn clip(&self, min: &[u8; 16], max: &[u8; 16]) -> Option<([u8; 16], [u8; 16])> {
        let low = (*min).max(self.low);
        let high = (*max).min(self.top()?);
        (low <= high).then_some((low, high))
    }
}

impl UuidMembership for UuidRange<'_> {
    /// An upper bound: the inner set's size, or unbounded without one.
    fn len(&self) -> usize {
        self.inner.map_or(usize::MAX, |inner| inner.len())
    }

    fn contains_uuid(&self, uuid: &[u8; 16]) -> bool {
        self.in_interval(uuid) && self.inner.is_none_or(|inner| inner.contains_uuid(uuid))
    }

    fn is_empty(&self) -> bool {
        self.top().is_none() || self.inner.is_some_and(|inner| inner.is_empty())
    }

    fn min_uuid(&self) -> Option<[u8; 16]> {
        self.bounds().map(|(min, _)| min)
    }

    fn max_uuid(&self) -> Option<[u8; 16]> {
        self.bounds().map(|(_, max)| max)
    }

    fn may_contain_in_range(&self, min: &[u8; 16], max: &[u8; 16]) -> bool {
        let Some((low, high)) = self.clip(min, max) else {
            return false;
        };
        self.inner
            .is_none_or(|inner| inner.may_contain_in_range(&low, &high))
    }

    /// Exact with an inner membership. Without one the interval cannot be
    /// enumerated, so a non-empty overlap is reported as a match: pruning
    /// only ever skips work, and every row read is still filtered by
    /// `contains_uuid`.
    fn any_match_in_range(
        &self,
        min: &[u8; 16],
        max: &[u8; 16],
        matches: &dyn Fn(&[u8; 16]) -> bool,
    ) -> bool {
        let Some((low, high)) = self.clip(min, max) else {
            return false;
        };
        self.inner
            .is_none_or(|inner| inner.any_match_in_range(&low, &high, matches))
    }

    fn contains_every_in_range(&self, min: &[u8; 16], max: &[u8; 16]) -> bool {
        self.inner.is_none()
            && min <= max
            && *min >= self.low
            && self.top().is_some_and(|top| *max <= top)
    }
}

/// Open-addressed set with a maximum load factor of one half. Slot and
/// occupancy storage sizes are deterministic from `capacity`; callers charge
/// those bytes before allocation and while old storage remains live on growth.
#[derive(Debug, Default)]
pub(crate) struct CompactUuidSet {
    /// The hash table is split into separately allocated chunks so completed
    /// nominations can drop unused tail chunks before allocating sorted output.
    slots: Vec<Vec<[u8; 16]>>,
    capacity: usize,
    occupied: Vec<u64>,
    len: usize,
    hash_builder: RandomState,
    sorted: bool,
}

impl CompactUuidSet {
    pub(crate) const INITIAL_CAPACITY: usize = 16;
    const SLOT_CHUNK_CAPACITY: usize = 4_096;

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn storage_bytes_for_capacity(capacity: usize) -> Option<usize> {
        let slot_bytes = capacity.checked_mul(std::mem::size_of::<[u8; 16]>())?;
        let chunk_count = capacity.div_ceil(Self::SLOT_CHUNK_CAPACITY);
        let directory_bytes = chunk_count.checked_mul(std::mem::size_of::<Vec<[u8; 16]>>())?;
        let words = capacity.checked_add(63)?.checked_div(64)?;
        let bitmap_bytes = words.checked_mul(std::mem::size_of::<u64>())?;
        slot_bytes
            .checked_add(directory_bytes)?
            .checked_add(bitmap_bytes)
    }

    pub(crate) fn storage_bytes(&self) -> usize {
        let directory_bytes = self
            .slots
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<[u8; 16]>>());
        let slot_bytes = self.slots.iter().fold(directory_bytes, |bytes, chunk| {
            bytes.saturating_add(
                chunk
                    .capacity()
                    .saturating_mul(std::mem::size_of::<[u8; 16]>()),
            )
        });
        let bitmap_bytes = self
            .occupied
            .capacity()
            .saturating_mul(std::mem::size_of::<u64>());
        slot_bytes.saturating_add(bitmap_bytes)
    }

    pub(crate) fn contains(&self, uuid: &[u8; 16]) -> bool {
        if self.len == 0 {
            return false;
        }
        if self.sorted {
            return self.slots[0].binary_search(uuid).is_ok();
        }
        let mask = self.capacity() - 1;
        let mut slot = self.slot_for(uuid, mask);
        loop {
            if !self.is_occupied(slot) {
                return false;
            }
            if self.slot_at(slot) == *uuid {
                return true;
            }
            slot = (slot + 1) & mask;
        }
    }

    /// Insert an already budgeted key. Returns true only for a new UUID.
    pub(crate) fn insert_without_growing(&mut self, uuid: [u8; 16]) -> bool {
        let mask = self.capacity() - 1;
        let mut slot = self.slot_for(&uuid, mask);
        loop {
            if !self.is_occupied(slot) {
                self.set_slot(slot, uuid);
                self.set_occupied(slot);
                self.len += 1;
                return true;
            }
            if self.slot_at(slot) == uuid {
                return false;
            }
            slot = (slot + 1) & mask;
        }
    }

    pub(crate) fn needs_growth_for(&self, uuid: &[u8; 16]) -> bool {
        !self.contains(uuid) && self.len.saturating_add(1) > self.capacity() / 2
    }

    pub(crate) fn allocate(capacity: usize) -> Result<Self, String> {
        let chunk_count = capacity.div_ceil(Self::SLOT_CHUNK_CAPACITY);
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(chunk_count)
            .map_err(|error| error.to_string())?;
        if slots.capacity() != chunk_count {
            return Err(format!(
                "UUID slot directory capacity {} differs from requested {chunk_count}",
                slots.capacity()
            ));
        }
        let mut remaining = capacity;
        while remaining > 0 {
            let chunk_capacity = remaining.min(Self::SLOT_CHUNK_CAPACITY);
            let mut chunk = Vec::new();
            // Reject any allocator capacity beyond the amount precharged by
            // the caller.
            chunk
                .try_reserve_exact(chunk_capacity)
                .map_err(|error| error.to_string())?;
            if chunk.capacity() != chunk_capacity {
                return Err(format!(
                    "UUID slot chunk capacity {} differs from requested {chunk_capacity}",
                    chunk.capacity()
                ));
            }
            chunk.resize(chunk_capacity, [0; 16]);
            slots.push(chunk);
            remaining -= chunk_capacity;
        }

        let bitmap_len = capacity.div_ceil(64);
        let mut occupied = Vec::new();
        occupied
            .try_reserve_exact(bitmap_len)
            .map_err(|error| error.to_string())?;
        if occupied.capacity() != bitmap_len {
            return Err(format!(
                "UUID bitmap allocation capacity {} differs from requested {bitmap_len}",
                occupied.capacity()
            ));
        }
        occupied.resize(bitmap_len, 0);

        Ok(Self {
            slots,
            capacity,
            occupied,
            len: 0,
            hash_builder: RandomState::new(),
            sorted: false,
        })
    }

    pub(crate) fn capacity_for_next_insert(&self) -> Result<usize, ()> {
        if self.capacity() == 0 {
            Ok(Self::INITIAL_CAPACITY)
        } else {
            self.capacity().checked_mul(2).ok_or(())
        }
    }

    pub(crate) fn reinsert_all(&mut self, old: &Self) {
        for slot in 0..old.capacity {
            if old.is_occupied(slot) {
                self.insert_without_growing(old.slot_at(slot));
            }
        }
    }

    /// Compacts occupied keys into the table prefix and physically drops its
    /// unused trailing chunks and bitmap. The caller releases the returned
    /// reservation only after this method has dropped those allocations.
    pub(crate) fn compact_occupied_prefix(&mut self) -> usize {
        debug_assert!(!self.sorted);
        let old_bytes = self.storage_bytes();
        let mut write = 0;
        for read in 0..self.capacity {
            if self.is_occupied(read) {
                if write != read {
                    let uuid = self.slot_at(read);
                    self.set_slot(write, uuid);
                }
                write += 1;
            }
        }
        debug_assert_eq!(write, self.len);
        let keep_chunks = self.len.div_ceil(Self::SLOT_CHUNK_CAPACITY);
        self.slots.truncate(keep_chunks);
        self.capacity = self.slots.iter().map(Vec::len).sum();
        let bitmap = std::mem::take(&mut self.occupied);
        drop(bitmap);
        old_bytes.saturating_sub(self.storage_bytes())
    }

    /// Allocates a compact contiguous sorted copy of the occupied prefix.
    /// Callers precharge `sorted_storage_bytes()` before invoking this.
    pub(crate) fn allocate_sorted_prefix(&self) -> Result<Vec<[u8; 16]>, String> {
        debug_assert!(!self.sorted);
        let mut sorted = Vec::new();
        sorted
            .try_reserve_exact(self.len)
            .map_err(|error| error.to_string())?;
        if sorted.capacity() != self.len {
            return Err(format!(
                "UUID sorted allocation capacity {} differs from requested {}",
                sorted.capacity(),
                self.len
            ));
        }
        sorted.extend((0..self.len).map(|index| self.slot_at(index)));
        sorted.sort_unstable();
        Ok(sorted)
    }

    pub(crate) fn sorted_storage_bytes(&self) -> Option<usize> {
        self.len.checked_mul(std::mem::size_of::<[u8; 16]>())
    }

    /// Installs the precharged sorted vector, then returns bytes whose backing
    /// allocations have already been dropped.
    pub(crate) fn install_sorted(&mut self, sorted: Vec<[u8; 16]>) -> usize {
        debug_assert_eq!(sorted.len(), self.len);
        debug_assert_eq!(sorted.capacity(), self.len);
        let released = if self.len == 0 {
            let old_bytes = self.storage_bytes();
            let old_directory = std::mem::take(&mut self.slots);
            drop(old_directory);
            old_bytes
        } else {
            let old_slot_bytes = self.slots.iter().fold(0_usize, |bytes, chunk| {
                bytes.saturating_add(
                    chunk
                        .capacity()
                        .saturating_mul(std::mem::size_of::<[u8; 16]>()),
                )
            });
            self.slots.truncate(1);
            let old_chunk = std::mem::replace(&mut self.slots[0], sorted);
            drop(old_chunk);
            old_slot_bytes
        };
        self.capacity = self.len;
        self.sorted = true;
        released
    }

    fn slot_at(&self, index: usize) -> [u8; 16] {
        let chunk = index / Self::SLOT_CHUNK_CAPACITY;
        let offset = index % Self::SLOT_CHUNK_CAPACITY;
        self.slots[chunk][offset]
    }

    fn set_slot(&mut self, index: usize, uuid: [u8; 16]) {
        let chunk = index / Self::SLOT_CHUNK_CAPACITY;
        let offset = index % Self::SLOT_CHUNK_CAPACITY;
        self.slots[chunk][offset] = uuid;
    }

    fn is_occupied(&self, slot: usize) -> bool {
        self.occupied[slot / 64] & (1_u64 << (slot % 64)) != 0
    }

    fn set_occupied(&mut self, slot: usize) {
        self.occupied[slot / 64] |= 1_u64 << (slot % 64);
    }

    fn hash(&self, uuid: &[u8; 16]) -> u64 {
        self.hash_builder.hash_one(uuid)
    }

    fn slot_for(&self, uuid: &[u8; 16], mask: usize) -> usize {
        let mask = u64::try_from(mask).expect("UUID set capacity fits in u64");
        usize::try_from(self.hash(uuid) & mask).expect("masked UUID hash fits in usize")
    }
}

impl UuidMembership for CompactUuidSet {
    fn len(&self) -> usize {
        self.len
    }

    fn contains_uuid(&self, uuid: &[u8; 16]) -> bool {
        self.contains(uuid)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn min_uuid(&self) -> Option<[u8; 16]> {
        self.sorted
            .then(|| self.slots.first()?.first().copied())
            .flatten()
    }

    fn max_uuid(&self) -> Option<[u8; 16]> {
        self.sorted
            .then(|| self.slots.first()?.last().copied())
            .flatten()
    }

    fn any_match_in_range(
        &self,
        min: &[u8; 16],
        max: &[u8; 16],
        matches: &dyn Fn(&[u8; 16]) -> bool,
    ) -> bool {
        if !self.sorted {
            return self.min_uuid().is_none_or(|set_min| set_min <= *max)
                && self.max_uuid().is_none_or(|set_max| set_max >= *min);
        }
        let Some(slots) = self.slots.first() else {
            return false;
        };
        let first = slots.partition_point(|uuid| uuid < min);
        slots[first..]
            .iter()
            .take_while(|uuid| *uuid <= max)
            .any(matches)
    }
}

#[cfg(test)]
mod tests {
    use super::CompactUuidSet;
    use super::SortedUuids;
    use super::UuidFilter;
    use super::UuidMembership;
    use super::UuidRange;

    fn uuid(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    #[test]
    fn sorted_slice_answers_membership_and_closed_ranges_exactly() {
        let values = [uuid(2), uuid(4), uuid(8)];
        let set = SortedUuids(&values);
        assert!(set.is_strictly_ascending());
        assert_eq!((set.len(), set.is_empty()), (3, false));
        assert_eq!(
            (set.min_uuid(), set.max_uuid()),
            (Some(uuid(2)), Some(uuid(8)))
        );
        for value in 0..=9 {
            assert_eq!(set.contains_uuid(&uuid(value)), [2, 4, 8].contains(&value));
        }
        // A closed range holds a member exactly when one lies within it.
        assert!(set.may_contain_in_range(&uuid(0), &uuid(2)));
        assert!(set.may_contain_in_range(&uuid(8), &uuid(9)));
        assert!(set.may_contain_in_range(&uuid(3), &uuid(4)));
        assert!(!set.may_contain_in_range(&uuid(5), &uuid(7)));
        assert!(!set.may_contain_in_range(&uuid(9), &uuid(10)));
        assert!(!set.may_contain_in_range(&uuid(0), &uuid(1)));
        assert!(!set.any_match_in_range(&uuid(2), &uuid(8), &|member| member == &uuid(5)));
        assert!(set.any_match_in_range(&uuid(2), &uuid(8), &|member| member == &uuid(4)));

        let empty = SortedUuids(&[]);
        assert!(empty.is_empty() && empty.is_strictly_ascending());
        assert!(!empty.may_contain_in_range(&uuid(0), &uuid(u128::MAX)));
        assert_eq!(empty.min_uuid(), None);

        for unordered in [[uuid(4), uuid(2)], [uuid(2), uuid(2)]] {
            assert!(!SortedUuids(&unordered).is_strictly_ascending());
        }
    }

    #[test]
    fn compact_set_handles_zero_duplicates_growth_and_exact_ranges() {
        let mut set = CompactUuidSet::allocate(16).unwrap();
        assert!(set.insert_without_growing(uuid(0)));
        assert!(!set.insert_without_growing(uuid(0)));
        for value in 1..=8 {
            if set.needs_growth_for(&uuid(value)) {
                let mut grown =
                    CompactUuidSet::allocate(set.capacity_for_next_insert().unwrap()).unwrap();
                grown.reinsert_all(&set);
                set = grown;
            }
            assert!(set.insert_without_growing(uuid(value)));
        }
        assert_eq!(set.capacity(), 32);
        for value in 0..=8 {
            assert!(set.contains(&uuid(value)));
        }
        assert!(!set.contains(&uuid(9)));

        let bitmap_bytes = set.compact_occupied_prefix();
        assert_eq!(bitmap_bytes, 8);
        let sorted = set.allocate_sorted_prefix().unwrap();
        let released = set.install_sorted(sorted);
        assert!(released > 0);
        assert!(set.contains(&uuid(0)));
        assert_eq!(set.min_uuid(), Some(uuid(0)));
        assert_eq!(set.max_uuid(), Some(uuid(8)));
        assert!(set.may_contain_in_range(&uuid(4), &uuid(5)));
        assert!(!set.may_contain_in_range(&uuid(9), &uuid(10)));

        let mut gaps = CompactUuidSet::allocate(16).unwrap();
        assert!(gaps.insert_without_growing(uuid(0)));
        assert!(gaps.insert_without_growing(uuid(8)));
        gaps.compact_occupied_prefix();
        let sorted = gaps.allocate_sorted_prefix().unwrap();
        gaps.install_sorted(sorted);
        assert!(!gaps.may_contain_in_range(&uuid(4), &uuid(5)));

        let mut left = CompactUuidSet::allocate(16).unwrap();
        let mut right = CompactUuidSet::allocate(16).unwrap();
        for value in [1, 3] {
            left.insert_without_growing(uuid(value));
        }
        for value in [2, 4] {
            right.insert_without_growing(uuid(value));
        }
        for set in [&mut left, &mut right] {
            set.compact_occupied_prefix();
            let sorted = set.allocate_sorted_prefix().unwrap();
            set.install_sorted(sorted);
        }
        let nominations: [&dyn UuidMembership; 2] = [&left, &right];
        let filter = UuidFilter {
            nominations: &nominations,
            candidates: None,
        };
        assert!(!filter.may_contain_in_range(&uuid(1), &uuid(4)));
    }

    #[test]
    fn compact_set_probes_across_capacity_wraparound() {
        let mut set = CompactUuidSet::allocate(16).unwrap();
        let mut values = Vec::new();
        for value in 0..10_000_u128 {
            let candidate = uuid(value);
            if set.hash(&candidate) as usize & 15 == 15 {
                values.push(candidate);
                if values.len() == 8 {
                    break;
                }
            }
        }
        assert_eq!(values.len(), 8);
        for &value in &values {
            assert!(set.insert_without_growing(value));
        }
        for &value in &values {
            assert!(set.contains(&value));
        }
    }

    #[test]
    fn range_alone_is_a_half_open_interval_that_clips_range_probes() {
        let range = UuidRange::new(uuid(10), Some(uuid(20)), None);
        assert!(!range.is_empty());
        assert_eq!(
            (range.min_uuid(), range.max_uuid()),
            (Some(uuid(10)), Some(uuid(19)))
        );
        for value in [0, 9, 10, 15, 19, 20, 21, u128::MAX] {
            assert_eq!(range.contains_uuid(&uuid(value)), (10..20).contains(&value));
        }
        // A probe touching the interval, even at one end, may hold a member;
        // one wholly outside it cannot.
        assert!(range.may_contain_in_range(&uuid(0), &uuid(10)));
        assert!(range.may_contain_in_range(&uuid(19), &uuid(40)));
        assert!(!range.may_contain_in_range(&uuid(0), &uuid(9)));
        assert!(!range.may_contain_in_range(&uuid(20), &uuid(40)));
        assert!(range.any_match_in_range(&uuid(12), &uuid(14), &|_| false));
        assert!(!range.any_match_in_range(&uuid(20), &uuid(30), &|_| true));
        // Only a probe inside the interval is wholly covered.
        assert!(range.contains_every_in_range(&uuid(10), &uuid(19)));
        assert!(!range.contains_every_in_range(&uuid(9), &uuid(19)));
        assert!(!range.contains_every_in_range(&uuid(10), &uuid(20)));
    }

    #[test]
    fn range_without_an_upper_bound_reaches_the_largest_uuid() {
        let range = UuidRange::new(uuid(10), None, None);
        assert_eq!(range.max_uuid(), Some([u8::MAX; 16]));
        assert!(range.contains_uuid(&[u8::MAX; 16]));
        assert!(!range.contains_uuid(&uuid(9)));
        assert!(range.contains_every_in_range(&uuid(10), &[u8::MAX; 16]));
        assert!(!range.may_contain_in_range(&uuid(0), &uuid(9)));
    }

    #[test]
    fn empty_ranges_contain_nothing() {
        for range in [
            UuidRange::new(uuid(10), Some(uuid(10)), None),
            UuidRange::new(uuid(10), Some(uuid(3)), None),
            UuidRange::new(uuid(0), Some(uuid(0)), None),
        ] {
            assert!(range.is_empty());
            assert_eq!((range.min_uuid(), range.max_uuid()), (None, None));
            assert!(!range.contains_uuid(&uuid(10)));
            assert!(!range.may_contain_in_range(&uuid(0), &[u8::MAX; 16]));
            assert!(!range.contains_every_in_range(&uuid(10), &uuid(10)));
        }
    }

    #[test]
    fn range_over_a_membership_is_the_intersection() {
        let values = [uuid(2), uuid(12), uuid(15), uuid(30)];
        let inner = SortedUuids(&values);
        let range = UuidRange::new(uuid(10), Some(uuid(20)), Some(&inner));
        assert!(!range.is_empty());
        for value in 0..40 {
            assert_eq!(
                range.contains_uuid(&uuid(value)),
                [12, 15].contains(&value),
                "{value}"
            );
        }
        assert_eq!(
            (range.min_uuid(), range.max_uuid()),
            (Some(uuid(10)), Some(uuid(19)))
        );
        // The inner set decides a probe, clipped to the interval.
        assert!(range.may_contain_in_range(&uuid(0), &uuid(12)));
        assert!(!range.may_contain_in_range(&uuid(0), &uuid(11)));
        assert!(!range.may_contain_in_range(&uuid(16), &uuid(40)));
        assert!(!range.may_contain_in_range(&uuid(25), &uuid(35)));
        assert!(range.any_match_in_range(&uuid(0), &uuid(40), &|value| *value == uuid(15)));
        assert!(!range.any_match_in_range(&uuid(0), &uuid(40), &|value| *value == uuid(30)));
        // A membership never claims a whole span.
        assert!(!range.contains_every_in_range(&uuid(12), &uuid(12)));
        // An inner set with nothing inside the interval has no bounds.
        let outside = UuidRange::new(uuid(100), Some(uuid(200)), Some(&inner));
        assert!(!outside.may_contain_in_range(&uuid(0), &[u8::MAX; 16]));
        assert_eq!((outside.min_uuid(), outside.max_uuid()), (None, None));
    }
}
