//! Checked native allocation requests for the pinned Arrow/Parquet reader.
//!
//! Rust 1.96 Vec growth and Arrow 60.2 MutableBuffer growth have different
//! floors and rounding. Keep the replaced allocation live beside its successor
//! until the native allocator completes the replacement. These payload bounds
//! do not account for allocator bookkeeping, fixed reader state, or RSS.

use std::alloc::Layout;
use std::collections::BTreeMap;
use std::mem::size_of;

use graphforge_core::GfError;

use super::limit;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Request {
    pub(super) retained_bytes: u64,
    pub(super) peak_bytes: u64,
}

fn overflow() -> GfError {
    limit("Parquet native allocation exceeds the addressable workspace")
}

fn payload(elements: usize, width: usize) -> Result<usize, GfError> {
    let bytes = elements.checked_mul(width).ok_or_else(overflow)?;
    if bytes > isize::MAX as usize {
        return Err(overflow());
    }
    Ok(bytes)
}

fn request(old: usize, new: usize) -> Result<Request, GfError> {
    let peak = if old == new {
        new
    } else {
        old.checked_add(new).ok_or_else(overflow)?
    };
    Ok(Request {
        retained_bytes: u64::try_from(new).map_err(|_| overflow())?,
        peak_bytes: u64::try_from(peak).map_err(|_| overflow())?,
    })
}

fn vector_floor(width: usize) -> usize {
    if width == 1 {
        8
    } else if width <= 1_024 {
        4
    } else {
        1
    }
}

/// An exact Vec reserve/resize request transition, including the old buffer.
/// `required` is the checked total length after the operation, not additional.
pub(super) fn vector(
    capacity: usize,
    required: usize,
    width: usize,
    exact: bool,
) -> Result<Request, GfError> {
    if width == 0 {
        return Err(limit(
            "Parquet native vectors require a nonzero element width",
        ));
    }
    let old = payload(capacity, width)?;
    if required <= capacity {
        return request(old, old);
    }
    let next = if exact {
        required
    } else {
        capacity
            .checked_mul(2)
            .ok_or_else(overflow)?
            .max(required)
            .max(vector_floor(width))
    };
    request(old, payload(next, width)?)
}

pub(super) fn round64(bytes: usize) -> Result<usize, GfError> {
    let rounded = bytes.checked_add(63).ok_or_else(overflow)? & !63;
    payload(rounded, 1)
}

/// Arrow MutableBuffer's request transition. An imported Vec buffer can have
/// an unaligned old capacity; only the newly required allocation is rounded.
#[cfg(test)]
pub(super) fn mutable(capacity: usize, required: usize) -> Result<Request, GfError> {
    payload(capacity, 1)?;
    if required <= capacity {
        return request(capacity, capacity);
    }
    let next = capacity
        .checked_mul(2)
        .ok_or_else(overflow)?
        .max(round64(required)?);
    request(capacity, payload(next, 1)?)
}

/// Bound every Vec reserve sequence whose required element counts stay at or
/// below `maximum`. Before a growth, the old capacity is at most maximum-1;
/// applying the exact transition gives this envelope without a copy factor.
pub(super) fn vector_envelope(
    initial: usize,
    maximum: usize,
    width: usize,
) -> Result<Request, GfError> {
    if width == 0 {
        return Err(limit(
            "Parquet native vectors require a nonzero element width",
        ));
    }
    if maximum <= initial {
        return vector(initial, initial, width, false);
    }
    let preceding = maximum - 1;
    let next = preceding
        .checked_mul(2)
        .ok_or_else(overflow)?
        .max(maximum)
        .max(vector_floor(width));
    request(payload(preceding, width)?, payload(next, width)?)
}

/// The analogous envelope for buffers constructed and grown by Arrow, whose
/// capacities are multiples of 64. Imported native Vec capacities use the
/// exact `mutable` transition instead of this aligned-history summary.
pub(super) fn mutable_envelope(initial: usize, maximum: usize) -> Result<Request, GfError> {
    let initial = round64(initial)?;
    if maximum <= initial {
        return request(initial, initial);
    }
    let preceding = ((maximum - 1) / 64).checked_mul(64).ok_or_else(overflow)?;
    let next = preceding
        .checked_mul(2)
        .ok_or_else(overflow)?
        .max(round64(maximum)?);
    request(preceding, payload(next, 1)?)
}

#[cfg(test)]
#[path = "parquet_alloc/tests.rs"]
mod tests;

/// Keys per std B-tree node (`alloc::collections::btree::node::CAPACITY`,
/// Rust 1.96); every node but the root holds at least `MIN_LEN` of them.
const BTREE_CAPACITY: usize = 11;
const BTREE_MIN_LEN: usize = 5;
/// Rust 1.96 stable sort: slices up to `MAX_LEN_ALWAYS_INSERTION_SORT`
/// elements are insertion-sorted in place; longer ones use driftsort, whose
/// scratch of `max(len - len / 2, min(len, 8 MB / size), 48)` elements lives
/// in a 4096-byte stack buffer when it fits and on the heap otherwise.
const STABLE_SORT_INSERTION_LEN: usize = 20;
const STABLE_SORT_MIN_SCRATCH: usize = 48;
const STABLE_SORT_STACK_BYTES: usize = 4096;

/// Heap scratch of one stable sort of `len` elements of `width` bytes, bounded
/// above by `max(len, 48)` elements.
fn stable_sort_heap_scratch(len: usize, width: usize) -> Result<usize, GfError> {
    if len <= STABLE_SORT_INSERTION_LEN {
        return Ok(0);
    }
    let scratch = payload(len.max(STABLE_SORT_MIN_SCRATCH), width)?;
    Ok(if scratch <= STABLE_SORT_STACK_BYTES {
        0
    } else {
        scratch
    })
}

/// An upper bound on one std B-tree internal node of `(K, V)` entries: the
/// leaf header, `CAPACITY` keys and values, and `CAPACITY + 1` edges, laid out
/// in declaration order. Rust may reorder the private fields only to remove
/// padding, so the declared-order layout is never smaller.
fn btree_node_bytes<K, V>() -> Result<usize, GfError> {
    let layout = Layout::new::<usize>();
    let mut layout = layout;
    for part in [
        Layout::new::<u16>(),
        Layout::new::<u16>(),
        Layout::array::<K>(BTREE_CAPACITY).map_err(|_| overflow())?,
        Layout::array::<V>(BTREE_CAPACITY).map_err(|_| overflow())?,
        Layout::array::<usize>(BTREE_CAPACITY + 1).map_err(|_| overflow())?,
    ] {
        layout = layout.extend(part).map_err(|_| overflow())?.0;
    }
    Ok(layout.pad_to_align().size())
}

/// Native requests of Arrow 60.2's `Metadata::from(HashMap<String, String>)`
/// (arrow-schema `metadata.rs`), which every `with_metadata` and
/// `Schema::new_with_metadata` call makes after the caller built the
/// `HashMap` (`hash_map_peak`, its own growth envelope).
///
/// `into_iter().collect::<BTreeMap<_, _>>()` collects the entries into an
/// exact `Vec` while the iterator still owns the hash table, stable-sorts it
/// (see [`stable_sort_heap_scratch`]), bulk-builds the
/// tree beside the `Vec`, and wraps it in an `Arc`. The peak sums every stage;
/// only the tree nodes and the `Arc` are retained, the table and `Vec` are
/// freed. Every non-root node holds at least `MIN_LEN` entries, so
/// `entries / MIN_LEN + 1` nodes bound the tree.
pub(super) fn string_metadata_map(entries: usize, hash_map_peak: u64) -> Result<Request, GfError> {
    if entries == 0 {
        // An empty map converts to `Metadata(None)` without allocating.
        return Ok(Request {
            retained_bytes: 0,
            peak_bytes: hash_map_peak,
        });
    }
    let entry = size_of::<(String, String)>();
    let collected = payload(entries, entry)?;
    let scratch = stable_sort_heap_scratch(entries, entry)?;
    let nodes = payload(
        entries / BTREE_MIN_LEN + 1,
        btree_node_bytes::<String, String>()?,
    )?;
    let arc = Layout::new::<[usize; 2]>()
        .extend(Layout::new::<BTreeMap<String, String>>())
        .map_err(|_| overflow())?
        .0
        .pad_to_align()
        .size();
    let retained = nodes.checked_add(arc).ok_or_else(overflow)?;
    let transient = collected
        .checked_add(scratch.max(nodes))
        .and_then(|bytes| bytes.checked_add(arc))
        .ok_or_else(overflow)?;
    Ok(Request {
        retained_bytes: u64::try_from(retained).map_err(|_| overflow())?,
        peak_bytes: hash_map_peak
            .checked_add(u64::try_from(transient).map_err(|_| overflow())?)
            .ok_or_else(overflow)?,
    })
}
