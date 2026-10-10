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
    slots: Vec<[u8; 16]>,
    occupied: Vec<u64>,
    len: usize,
    hash_builder: RandomState,
    sorted: bool,
}

impl CompactUuidSet {
    pub(crate) const INITIAL_CAPACITY: usize = 16;

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn storage_bytes_for_capacity(capacity: usize) -> Option<usize> {
        let slot_bytes = capacity.checked_mul(std::mem::size_of::<[u8; 16]>())?;
        let words = capacity.checked_add(63)?.checked_div(64)?;
        let bitmap_bytes = words.checked_mul(std::mem::size_of::<u64>())?;
        slot_bytes.checked_add(bitmap_bytes)
    }

    pub(crate) fn storage_bytes(&self) -> usize {
        self.slots
            .capacity()
            .checked_mul(std::mem::size_of::<[u8; 16]>())
            .and_then(|slots| {
                self.occupied
                    .capacity()
                    .checked_mul(std::mem::size_of::<u64>())
                    .and_then(|bitmap| slots.checked_add(bitmap))
            })
            .unwrap_or(usize::MAX)
    }

    pub(crate) fn contains(&self, uuid: &[u8; 16]) -> bool {
        if self.len == 0 {
            return false;
        }
        if self.sorted {
            return self.slots[..self.len].binary_search(uuid).is_ok();
        }
        let mask = self.capacity() - 1;
        let mut slot = self.slot_for(uuid, mask);
        loop {
            if !self.is_occupied(slot) {
                return false;
            }
            if self.slots[slot] == *uuid {
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
                self.slots[slot] = uuid;
                self.set_occupied(slot);
                self.len += 1;
                return true;
            }
            if self.slots[slot] == uuid {
                return false;
            }
            slot = (slot + 1) & mask;
        }
    }

    pub(crate) fn needs_growth_for(&self, uuid: &[u8; 16]) -> bool {
        !self.contains(uuid) && self.len.saturating_add(1) > self.capacity() / 2
    }

    pub(crate) fn allocate(capacity: usize) -> Result<Self, String> {
        let mut slots = Vec::new();
        // The pinned standard library's try_reserve_exact preserves the
        // requested capacity; reject any allocator result that exceeds the
        // bytes already charged by the caller.
        slots
            .try_reserve_exact(capacity)
            .map_err(|error| error.to_string())?;
        if slots.capacity() != capacity {
            return Err(format!(
                "UUID slot allocation capacity {} differs from requested {capacity}",
                slots.capacity()
            ));
        }
        slots.resize(capacity, [0; 16]);

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
        for (slot, uuid) in old.slots.iter().enumerate() {
            if old.is_occupied(slot) {
                self.insert_without_growing(*uuid);
            }
        }
    }

    /// Converts the hash table to sorted search storage in place. Sorting is
    /// allocation-free; only the occupancy bitmap is released.
    pub(crate) fn finalize(&mut self) -> usize {
        debug_assert!(!self.sorted);
        let mut write = 0;
        for read in 0..self.slots.len() {
            if self.is_occupied(read) {
                if write != read {
                    self.slots.copy_within(read..=read, write);
                }
                write += 1;
            }
        }
        debug_assert_eq!(write, self.len);
        self.slots[..self.len].sort_unstable();
        self.sorted = true;
        let released = self.occupied.capacity() * std::mem::size_of::<u64>();
        let bitmap = std::mem::take(&mut self.occupied);
        drop(bitmap);
        released
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
            .then(|| self.slots.get(..self.len)?.first().copied())
            .flatten()
    }

    fn max_uuid(&self) -> Option<[u8; 16]> {
        self.sorted
            .then(|| self.slots.get(..self.len)?.last().copied())
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
        let first = self.slots[..self.len].partition_point(|uuid| uuid < min);
        self.slots[first..self.len]
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

        let bitmap_bytes = set.finalize();
        assert_eq!(bitmap_bytes, 8);
        assert!(set.contains(&uuid(0)));
        assert_eq!(set.min_uuid(), Some(uuid(0)));
        assert_eq!(set.max_uuid(), Some(uuid(8)));
        assert!(set.may_contain_in_range(&uuid(4), &uuid(5)));
        assert!(!set.may_contain_in_range(&uuid(9), &uuid(10)));

        let mut gaps = CompactUuidSet::allocate(16).unwrap();
        assert!(gaps.insert_without_growing(uuid(0)));
        assert!(gaps.insert_without_growing(uuid(8)));
        gaps.finalize();
        assert!(!gaps.may_contain_in_range(&uuid(4), &uuid(5)));

        let mut left = CompactUuidSet::allocate(16).unwrap();
        let mut right = CompactUuidSet::allocate(16).unwrap();
        for value in [1, 3] {
            left.insert_without_growing(uuid(value));
        }
        for value in [2, 4] {
            right.insert_without_growing(uuid(value));
        }
        left.finalize();
        right.finalize();
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
