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

    #[cfg(test)]
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
    use super::UuidFilter;
    use super::UuidMembership;

    fn uuid(value: u128) -> [u8; 16] {
        value.to_be_bytes()
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
}
