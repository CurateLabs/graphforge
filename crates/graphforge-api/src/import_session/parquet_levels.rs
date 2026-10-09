//! Allocation-free borrowing cursors for Parquet definition and repetition
//! level streams and dictionary index streams (#1918).
//!
//! A page can claim far more level events than any batch holds, so these
//! cursors never allocate proportionally to events, runs, or body bytes: they
//! keep scalar run state and fill caller-owned fixed blocks of at most
//! [`MAX_BLOCK_EVENTS`] events. Every consumed value is range-checked against
//! the caller-supplied allowed maximum before it is returned, and every run
//! header is parsed with checked arithmetic before any byte is decoded, so a
//! hostile stream cannot drive the pinned library decoder's narrowing
//! conversions or assertions.
//!
//! The cursors borrow already-validated sections. [`split_v1`] carves V1
//! data page bodies into repetition, definition, and value sections using the
//! same grammar the pinned reader applies (`parse_v1_level`); V2 callers
//! supply header lengths checked against both body geometries themselves.

use graphforge_core::GfError;
use parquet::basic::Encoding;
use parquet::util::bit_util::num_required_bits;

use super::storage;

/// Largest event count one [`LevelSource::next_block`] or
/// [`IndexSource::next_block`] call may fill.
pub(super) const MAX_BLOCK_EVENTS: usize = 1024;

/// Values per bit-packed group in the hybrid encoding (pinned
/// `BIT_PACK_GROUP_SIZE`).
const BIT_PACK_GROUP: u64 = 8;

fn insufficient() -> GfError {
    storage("Parquet level stream ends before the required events")
}

fn truncated() -> GfError {
    storage("Parquet level stream payload is truncated")
}

fn overflow() -> GfError {
    storage("Parquet level stream run header overflows its bounded width")
}

fn out_of_range() -> GfError {
    storage("Parquet level or dictionary index exceeds the allowed maximum")
}

fn invalid_descriptor() -> GfError {
    storage("Parquet level descriptor has a negative maximum")
}

fn block_contract() -> GfError {
    storage("Parquet level block exceeds the 1024 event bound")
}

/// Uniform interface for the level cursors: scalar iteration with bounded
/// state, plus fixed-block fills for the schema shape accountant.
pub(super) trait LevelSource {
    /// Event count the caller declared for this section.
    fn expected(&self) -> usize;
    /// Events already consumed and validated.
    fn emitted(&self) -> usize;
    /// Next validated level, or `None` once `expected` events are supplied.
    fn next_level(&mut self) -> Result<Option<i16>, GfError>;

    /// Events still required.
    fn remaining(&self) -> usize {
        self.expected() - self.emitted()
    }

    /// Fill `block` with up to [`MAX_BLOCK_EVENTS`] validated levels and
    /// return how many were written. Stops at section end, which before
    /// `expected` events is an error surfaced by [`Self::next_level`].
    fn next_block(&mut self, block: &mut [i16]) -> Result<usize, GfError> {
        if block.len() > MAX_BLOCK_EVENTS {
            return Err(block_contract());
        }
        let mut filled = 0;
        while filled < block.len() {
            let Some(level) = self.next_level()? else {
                break;
            };
            block[filled] = level;
            filled += 1;
        }
        Ok(filled)
    }
}

/// Uniform interface for the dictionary index cursor.
pub(super) trait IndexSource {
    fn expected(&self) -> usize;
    fn emitted(&self) -> usize;
    fn next_index(&mut self) -> Result<Option<u32>, GfError>;

    fn remaining(&self) -> usize {
        self.expected() - self.emitted()
    }

    fn next_block(&mut self, block: &mut [u32]) -> Result<usize, GfError> {
        if block.len() > MAX_BLOCK_EVENTS {
            return Err(block_contract());
        }
        let mut filled = 0;
        while filled < block.len() {
            let Some(index) = self.next_index()? else {
                break;
            };
            block[filled] = index;
            filled += 1;
        }
        Ok(filled)
    }
}

/// Supplies implicit zero events for a descriptor whose maximum level is
/// zero: the page carries no section for it and the pinned reader never
/// decodes one.
pub(super) struct ImplicitZero {
    expected: usize,
    emitted: usize,
}

impl ImplicitZero {
    pub(super) fn new(expected: usize) -> Self {
        Self {
            expected,
            emitted: 0,
        }
    }
}

impl LevelSource for ImplicitZero {
    fn expected(&self) -> usize {
        self.expected
    }

    fn emitted(&self) -> usize {
        self.emitted
    }

    fn next_level(&mut self) -> Result<Option<i16>, GfError> {
        if self.emitted == self.expected {
            return Ok(None);
        }
        self.emitted += 1;
        Ok(Some(0))
    }
}

/// V1 `BIT_PACKED` definition/repetition levels. The section has no length
/// prefix and holds exactly `ceil(num_values * width / 8)` bytes, but final
/// padding bits are not logical events and are never validated.
///
/// Bit order matches the pinned `BitReader`: values are packed LSB-first,
/// contiguously across bytes.
pub(super) struct PackedLevels<'a> {
    data: &'a [u8],
    width: u8,
    max_level: i16,
    expected: usize,
    emitted: usize,
    bit_offset: usize,
}

impl<'a> PackedLevels<'a> {
    pub(super) fn new(data: &'a [u8], max_level: i16, expected: usize) -> Result<Self, GfError> {
        if max_level < 0 {
            return Err(invalid_descriptor());
        }
        Ok(Self {
            data,
            width: num_required_bits(u64::try_from(max_level).map_err(|_| invalid_descriptor())?),
            max_level,
            expected,
            emitted: 0,
            bit_offset: 0,
        })
    }

    /// Bytes of `data` actually consumed by emitted events; trailing padding
    /// is not attributed.
    pub(super) fn consumed_bytes(&self) -> usize {
        self.bit_offset.div_ceil(8)
    }
}

impl LevelSource for PackedLevels<'_> {
    fn expected(&self) -> usize {
        self.expected
    }

    fn emitted(&self) -> usize {
        self.emitted
    }

    fn next_level(&mut self) -> Result<Option<i16>, GfError> {
        if self.emitted == self.expected {
            return Ok(None);
        }
        let bit_end = self
            .bit_offset
            .checked_add(usize::from(self.width))
            .ok_or_else(truncated)?;
        if bit_end > self.data.len() * 8 {
            return Err(truncated());
        }
        let mut value = 0_u64;
        for index in 0..usize::from(self.width) {
            let at = self.bit_offset + index;
            value |= u64::from((self.data[at >> 3] >> (at & 7)) & 1) << index;
        }
        self.bit_offset = bit_end;
        self.emitted += 1;
        if value > u64::try_from(self.max_level).map_err(|_| invalid_descriptor())? {
            return Err(out_of_range());
        }
        let level = i16::try_from(value).map_err(|_| out_of_range())?;
        Ok(Some(level))
    }
}

/// One decoded run of the hybrid RLE/bit-packed grammar.
#[derive(Clone, Copy)]
enum Run {
    Idle,
    Rle { remaining: u64, value: u64 },
    Packed { remaining: u64 },
}

/// Shared engine for hybrid RLE/bit-packed streams. State is scalar:
/// `(byte_offset, run_remaining, run_kind, repeated_value,
/// packed_bit_offset)`.
struct Hybrid<'a> {
    data: &'a [u8],
    offset: usize,
    width: u8,
    /// Inclusive maximum one decoded value may take.
    limit: u64,
    expected: usize,
    emitted: usize,
    run: Run,
    packed_bit_offset: usize,
}

impl<'a> Hybrid<'a> {
    fn new(data: &'a [u8], width: u8, limit: u64, expected: usize) -> Self {
        Self {
            data,
            offset: 0,
            width,
            limit,
            expected,
            emitted: 0,
            run: Run::Idle,
            packed_bit_offset: 0,
        }
    }

    fn consumed_bytes(&self) -> usize {
        self.offset.max(self.packed_bit_offset.div_ceil(8))
    }

    fn remaining(&self) -> usize {
        self.expected - self.emitted
    }

    /// Parses one run header with checked arithmetic before any payload byte
    /// is decoded. The pinned `RleDecoder::reload` narrows run counts to
    /// `u32` and multiplies bit-packed groups by eight before those
    /// conversions; both steps are checked here instead so a hostile header
    /// cannot wrap or assert downstream. A zero indicator is the fastparquet
    /// end-of-stream terminator the pinned decoder accepts; it is only
    /// reachable while events are still required, which is insufficient
    /// data — after the required events `next` stops reading and never
    /// reloads.
    fn reload(&mut self) -> Result<(), GfError> {
        let indicator = self.read_vlq()?;
        if indicator == 0 {
            return Err(insufficient());
        }
        if indicator & 1 == 1 {
            let groups = indicator >> 1;
            let count = groups.checked_mul(BIT_PACK_GROUP).ok_or_else(overflow)?;
            let count = u32::try_from(count).map_err(|_| overflow())?;
            // Only the logical prefix this run will actually supply needs
            // payload bytes: writers may truncate the final group, and events
            // past `expected` are ignored padding that need not be valid.
            let decoded = usize::try_from(count)
                .map_err(|_| overflow())?
                .min(self.remaining());
            let bits = decoded
                .checked_mul(usize::from(self.width))
                .ok_or_else(overflow)?;
            let bit_end = self
                .packed_bit_offset
                .checked_add(bits)
                .ok_or_else(overflow)?;
            if bit_end > self.data.len() * 8 {
                return Err(truncated());
            }
            self.run = Run::Packed {
                remaining: u64::from(count),
            };
        } else {
            let count = u32::try_from(indicator >> 1).map_err(|_| overflow())?;
            let value = self.read_rle_value()?;
            self.run = Run::Rle {
                remaining: u64::from(count),
                value,
            };
        }
        Ok(())
    }

    /// Bounded VLQ matching the pinned reader's `MAX_VLQ_BYTE_LEN` of ten
    /// bytes, with explicit overflow instead of its shift assertion.
    fn read_vlq(&mut self) -> Result<u64, GfError> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = *self.data.get(self.offset).ok_or_else(truncated)?;
            self.offset += 1;
            if shift == 63 && byte > 1 {
                return Err(overflow());
            }
            value |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(overflow())
    }

    fn read_rle_value(&mut self) -> Result<u64, GfError> {
        let width_bytes = usize::from(self.width).div_ceil(8);
        let end = self.offset.checked_add(width_bytes).ok_or_else(truncated)?;
        let bytes = self.data.get(self.offset..end).ok_or_else(truncated)?;
        let mut value = 0_u64;
        for (index, byte) in bytes.iter().enumerate() {
            value |= u64::from(*byte) << (index * 8);
        }
        self.offset = end;
        if value > self.limit {
            return Err(out_of_range());
        }
        Ok(value)
    }

    fn read_packed_value(&mut self) -> Result<u64, GfError> {
        let bit_end = self
            .packed_bit_offset
            .checked_add(usize::from(self.width))
            .ok_or_else(truncated)?;
        if bit_end > self.data.len() * 8 {
            return Err(truncated());
        }
        let mut value = 0_u64;
        for index in 0..usize::from(self.width) {
            let at = self.packed_bit_offset + index;
            value |= u64::from((self.data[at >> 3] >> (at & 7)) & 1) << index;
        }
        self.packed_bit_offset = bit_end;
        if value > self.limit {
            return Err(out_of_range());
        }
        Ok(value)
    }

    fn next_value(&mut self) -> Result<Option<u64>, GfError> {
        if self.emitted == self.expected {
            return Ok(None);
        }
        loop {
            match self.run {
                Run::Rle { remaining, value } if remaining > 0 => {
                    self.run = Run::Rle {
                        remaining: remaining - 1,
                        value,
                    };
                    self.emitted += 1;
                    return Ok(Some(value));
                }
                Run::Packed { remaining } if remaining > 0 => {
                    let value = self.read_packed_value()?;
                    self.run = Run::Packed {
                        remaining: remaining - 1,
                    };
                    self.emitted += 1;
                    return Ok(Some(value));
                }
                _ => self.reload()?,
            }
        }
    }
}

/// Hybrid RLE/bit-packed definition/repetition levels, as used by V1 `RLE`
/// (after its four-byte length prefix is stripped by [`split_v1`]) and by V2
/// pages.
pub(super) struct HybridLevels<'a> {
    engine: Hybrid<'a>,
}

impl<'a> HybridLevels<'a> {
    /// `data` is the borrowed hybrid section without any length prefix.
    pub(super) fn new(data: &'a [u8], max_level: i16, expected: usize) -> Result<Self, GfError> {
        if max_level < 0 {
            return Err(invalid_descriptor());
        }
        Ok(Self {
            engine: Hybrid::new(
                data,
                num_required_bits(u64::try_from(max_level).map_err(|_| invalid_descriptor())?),
                u64::try_from(max_level).map_err(|_| invalid_descriptor())?,
                expected,
            ),
        })
    }

    /// Bytes consumed by the logical prefix, including run headers. The
    /// declared section span, not this prefix count, locates the value suffix:
    /// unused final packed padding may remain in the admitted section.
    pub(super) fn consumed_bytes(&self) -> usize {
        self.engine.consumed_bytes()
    }
}

impl LevelSource for HybridLevels<'_> {
    fn expected(&self) -> usize {
        self.engine.expected
    }

    fn emitted(&self) -> usize {
        self.engine.emitted
    }

    fn next_level(&mut self) -> Result<Option<i16>, GfError> {
        match self.engine.next_value()? {
            None => Ok(None),
            Some(value) => Ok(Some(i16::try_from(value).map_err(|_| out_of_range())?)),
        }
    }
}

/// `RLE_DICTIONARY`/`PLAIN_DICTIONARY` indices. The stream starts with the
/// one-bit-width prefix byte the actual consumer requires, followed by the
/// hybrid grammar with no V1-style length prefix.
pub(super) struct DictionaryIndices<'a> {
    engine: Hybrid<'a>,
}

impl<'a> DictionaryIndices<'a> {
    /// `dictionary_count` is the allowed maximum: every consumed index is
    /// validated against it. `expected_nonnull` is the exact number of
    /// indices the page must supply.
    pub(super) fn new(
        stream: &'a [u8],
        dictionary_count: usize,
        expected_nonnull: usize,
    ) -> Result<Self, GfError> {
        let (&width, rest) = stream
            .split_first()
            .ok_or_else(|| storage("Parquet dictionary index stream is missing the width byte"))?;
        // Refused before any shift can use it; matches the pinned consumer's
        // own `> 32` rejection.
        if width > 32 {
            return Err(storage("Parquet dictionary index width exceeds 32 bits"));
        }
        if dictionary_count == 0 && expected_nonnull != 0 {
            return Err(storage(
                "Parquet dictionary is empty but the page requires indices",
            ));
        }
        let limit = if dictionary_count == 0 {
            0
        } else {
            // Indices read from a width of at most 32 bits cannot exceed
            // u32::MAX, so a larger dictionary caps the inclusive limit there.
            u64::from(u32::try_from(dictionary_count - 1).unwrap_or(u32::MAX))
        };
        Ok(Self {
            engine: Hybrid::new(rest, width, limit, expected_nonnull),
        })
    }

    /// Bytes of the stream consumed so far, including the width byte.
    pub(super) fn consumed_bytes(&self) -> usize {
        self.engine.consumed_bytes() + 1
    }
}

impl IndexSource for DictionaryIndices<'_> {
    fn expected(&self) -> usize {
        self.engine.expected
    }

    fn emitted(&self) -> usize {
        self.engine.emitted
    }

    fn next_index(&mut self) -> Result<Option<u32>, GfError> {
        match self.engine.next_value()? {
            None => Ok(None),
            Some(value) => Ok(Some(u32::try_from(value).map_err(|_| out_of_range())?)),
        }
    }
}

/// V1 data page body split into level sections and the value suffix. Sections
/// for zero-maximum descriptors are absent: the descriptor supplies implicit
/// zero events and the value suffix starts where the preceding section ended.
#[derive(Debug)]
pub(super) struct V1Sections<'a> {
    pub(super) repetition: Option<&'a [u8]>,
    pub(super) definition: Option<&'a [u8]>,
    pub(super) values: &'a [u8],
}

/// Splits a V1 data page body the way the pinned `parse_v1_level` does,
/// validating every length before borrowing. `RLE` sections carry a signed
/// four-byte length; `BIT_PACKED` sections are exactly
/// `ceil(num_values * width / 8)` bytes with no length prefix.
pub(super) fn split_v1<'a>(
    body: &'a [u8],
    max_repetition: i16,
    max_definition: i16,
    num_values: u32,
    repetition_encoding: Encoding,
    definition_encoding: Encoding,
) -> Result<V1Sections<'a>, GfError> {
    if max_repetition < 0 || max_definition < 0 {
        return Err(invalid_descriptor());
    }
    let mut offset = 0;
    let repetition = if max_repetition > 0 {
        let (end, section) = v1_section(
            body,
            offset,
            max_repetition,
            num_values,
            repetition_encoding,
        )?;
        offset = end;
        Some(section)
    } else {
        None
    };
    let definition = if max_definition > 0 {
        let (end, section) = v1_section(
            body,
            offset,
            max_definition,
            num_values,
            definition_encoding,
        )?;
        offset = end;
        Some(section)
    } else {
        None
    };
    let values = body.get(offset..).ok_or_else(truncated)?;
    Ok(V1Sections {
        repetition,
        definition,
        values,
    })
}

fn v1_section<'a>(
    body: &'a [u8],
    offset: usize,
    max_level: i16,
    num_values: u32,
    encoding: Encoding,
) -> Result<(usize, &'a [u8]), GfError> {
    if max_level < 0 {
        return Err(invalid_descriptor());
    }
    match encoding {
        Encoding::RLE => {
            let prefix_end = offset.checked_add(4).ok_or_else(truncated)?;
            let prefix = body.get(offset..prefix_end).ok_or_else(|| {
                storage("Parquet V1 RLE level section is missing its length prefix")
            })?;
            let mut raw = [0_u8; 4];
            raw.copy_from_slice(prefix);
            let raw = i32::from_le_bytes(raw);
            // The length is signed; negative values are out of range, and the
            // claimed span must fit the owned body before it is borrowed.
            let length = usize::try_from(raw)
                .map_err(|_| storage("Parquet V1 RLE level length is negative"))?;
            let end = prefix_end
                .checked_add(length)
                .ok_or_else(|| storage("Parquet V1 RLE level length is out of range"))?;
            let section = body
                .get(prefix_end..end)
                .ok_or_else(|| storage("Parquet V1 RLE level section exceeds the page body"))?;
            Ok((end, section))
        }
        #[allow(deprecated)]
        Encoding::BIT_PACKED => {
            let width = usize::from(num_required_bits(
                u64::try_from(max_level).map_err(|_| invalid_descriptor())?,
            ));
            let bits = usize::try_from(num_values)
                .map_err(|_| storage("Parquet page event count is out of range"))?
                .checked_mul(width)
                .ok_or_else(|| storage("Parquet V1 BIT_PACKED level length is out of range"))?;
            let end = offset
                .checked_add(bits.div_ceil(8))
                .ok_or_else(|| storage("Parquet V1 BIT_PACKED level length is out of range"))?;
            let section = body.get(offset..end).ok_or_else(|| {
                storage("Parquet V1 BIT_PACKED level section exceeds the page body")
            })?;
            Ok((end, section))
        }
        _ => Err(storage("unsupported V1 level encoding")),
    }
}

#[cfg(test)]
mod tests;
