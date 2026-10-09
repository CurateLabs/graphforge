//! Checked native allocation requests for the pinned Arrow/Parquet reader.
//!
//! Rust 1.96 Vec growth and Arrow 58.4 MutableBuffer growth have different
//! floors and rounding. Keep the replaced allocation live beside its successor
//! until the native allocator completes the replacement. These payload bounds
//! do not account for allocator bookkeeping, fixed reader state, or RSS.

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
