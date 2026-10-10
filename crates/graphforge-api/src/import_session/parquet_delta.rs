//! Allocation-free validation of counts and geometry in delta page bodies.
//!
//! Delta length/prefix decoders allocate from body counts, independently of the
//! page header. Validate those counts and every length before calling a decoder.

use graphforge_core::GfError;
use parquet::basic::Encoding;

use crate::CancellationToken;

use super::{cancelled, storage};

#[derive(Clone, Copy, Debug)]
pub(super) struct DeltaFacts {
    pub(super) values: usize,
    #[cfg(test)]
    pub(super) auxiliary_bytes: u64,
    #[cfg(test)]
    pub(super) largest_value: u64,
}

fn invalid() -> GfError {
    storage("Parquet delta body has invalid counts, lengths or block geometry")
}

fn unsigned(input: &[u8], offset: &mut usize) -> Result<u64, GfError> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *input.get(*offset).ok_or_else(invalid)?;
        *offset += 1;
        if shift == 63 && byte > 1 {
            return Err(invalid());
        }
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(value);
        }
    }
    Err(invalid())
}

fn signed(input: &[u8], offset: &mut usize) -> Result<i64, GfError> {
    let value = unsigned(input, offset)?;
    Ok(i64::try_from(value >> 1).map_err(|_| invalid())? ^ -i64::from(value & 1 != 0))
}

/// The integer stream retains references to miniblock widths, never a Vec of
/// widths or values. Exhaustion includes the padding in the last used miniblock.
struct Integers<'a> {
    input: &'a [u8],
    offset: usize,
    count: usize,
    remaining: usize,
    minis: usize,
    per_mini: usize,
    widths: &'a [u8],
    mini: usize,
    used: usize,
    start: usize,
    end: usize,
    minimum: i64,
    previous: i64,
    first: bool,
    width: u8,
}

impl<'a> Integers<'a> {
    fn new(input: &'a [u8], maximum: usize, width: u8) -> Result<Self, GfError> {
        let mut offset = 0;
        let block = usize::try_from(unsigned(input, &mut offset)?).map_err(|_| invalid())?;
        let minis = usize::try_from(unsigned(input, &mut offset)?).map_err(|_| invalid())?;
        let count = usize::try_from(unsigned(input, &mut offset)?).map_err(|_| invalid())?;
        // This check precedes every operation on the claimed count, including
        // traversing encoded miniblocks and any library decoder allocation.
        if count > maximum || block == 0 || block % 128 != 0 || minis == 0 || block % minis != 0 {
            return Err(invalid());
        }
        let per_mini = block / minis;
        if per_mini == 0 || per_mini % 32 != 0 {
            return Err(invalid());
        }
        let previous = signed(input, &mut offset)?;
        if width == 32 && i32::try_from(previous).is_err() {
            return Err(invalid());
        }
        Ok(Self {
            input,
            offset,
            count,
            remaining: count,
            minis,
            per_mini,
            widths: &[],
            mini: 0,
            used: 0,
            start: offset,
            end: offset,
            minimum: 0,
            previous,
            first: true,
            width,
        })
    }

    fn next(&mut self) -> Result<Option<i64>, GfError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        if self.first {
            self.first = false;
            self.remaining -= 1;
            return Ok(Some(self.previous));
        }
        if self.widths.is_empty() || self.used == self.per_mini {
            if !self.widths.is_empty() && self.mini + 1 < self.minis {
                self.mini += 1;
                self.start = self.end;
            } else {
                self.offset = self.end;
                self.minimum = signed(self.input, &mut self.offset)?;
                if self.width == 32 && i32::try_from(self.minimum).is_err() {
                    return Err(invalid());
                }
                let end = self.offset.checked_add(self.minis).ok_or_else(invalid)?;
                self.widths = self.input.get(self.offset..end).ok_or_else(invalid)?;
                self.offset = end;
                self.start = end;
                self.mini = 0;
            }
            self.used = 0;
            let bits = usize::from(self.widths[self.mini]);
            if bits > usize::from(self.width) {
                return Err(invalid());
            }
            let bytes = bits.checked_mul(self.per_mini).ok_or_else(invalid)? / 8;
            self.end = self.start.checked_add(bytes).ok_or_else(invalid)?;
            if self.end > self.input.len() {
                return Err(invalid());
            }
        }
        let bits = usize::from(self.widths[self.mini]);
        let bit = self.used.checked_mul(bits).ok_or_else(invalid)?;
        let mut delta = 0_u64;
        for index in 0..bits {
            let at = bit + index;
            delta |= u64::from((self.input[self.start + at / 8] >> (at % 8)) & 1) << index;
        }
        let delta = i64::from_ne_bytes(delta.to_ne_bytes());
        let value = self.previous.wrapping_add(self.minimum).wrapping_add(delta);
        self.previous = if self.width == 32 {
            let bytes = value.to_le_bytes();
            i64::from(i32::from_le_bytes(
                bytes[..4].try_into().expect("four bytes"),
            ))
        } else {
            value
        };
        self.used += 1;
        self.remaining -= 1;
        Ok(Some(self.previous))
    }
}

/// Borrowing lengths for the sizing pass. Decoding one repeated logical row
/// never creates a row-sized length vector or a reconstructed byte value.
pub(super) struct Lengths<'a> {
    prefixes: Option<Integers<'a>>,
    suffixes: Integers<'a>,
    payload_bytes: u64,
    consumed_bytes: u64,
    previous_length: u64,
    cancellation: Option<&'a CancellationToken>,
}

fn check_cancel(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    Ok(())
}

fn stream_end(
    integers: &mut Integers<'_>,
    cancellation: Option<&CancellationToken>,
) -> Result<usize, GfError> {
    check_cancel(cancellation)?;
    let mut events = 0;
    while integers.next()?.is_some() {
        events += 1;
        if events == 1024 {
            check_cancel(cancellation)?;
            events = 0;
        }
    }
    Ok(integers.offset.max(integers.end))
}

impl<'a> Lengths<'a> {
    pub(super) fn new(
        encoding: Encoding,
        input: &'a [u8],
        maximum: usize,
        cancellation: Option<&'a CancellationToken>,
    ) -> Result<Self, GfError> {
        check_cancel(cancellation)?;
        let (prefixes, suffix_input) = match encoding {
            Encoding::DELTA_LENGTH_BYTE_ARRAY => (None, input),
            Encoding::DELTA_BYTE_ARRAY => {
                let mut prefixes = Integers::new(input, maximum, 32)?;
                let end = stream_end(&mut prefixes, cancellation)?;
                (Some(Integers::new(input, maximum, 32)?), &input[end..])
            }
            _ => return Err(invalid()),
        };
        let mut suffixes = Integers::new(suffix_input, maximum, 32)?;
        if prefixes.as_ref().is_some_and(|p| p.count != suffixes.count) {
            return Err(invalid());
        }
        let end = stream_end(&mut suffixes, cancellation)?;
        Ok(Self {
            prefixes,
            suffixes: Integers::new(suffix_input, maximum, 32)?,
            payload_bytes: u64::try_from(suffix_input.len() - end).map_err(|_| invalid())?,
            consumed_bytes: 0,
            previous_length: 0,
            cancellation,
        })
    }

    pub(super) fn count(&self) -> usize {
        self.suffixes.count
    }

    /// At most 1024 events per call. A caller's smaller fixed block remains
    /// useful; a larger slice cannot turn a logical row into one unbounded step.
    pub(super) fn next_block(&mut self, output: &mut [u64]) -> Result<usize, GfError> {
        check_cancel(self.cancellation)?;
        let mut written = 0;
        for item in output.iter_mut().take(1024) {
            let Some(suffix) = self.suffixes.next()? else {
                break;
            };
            let suffix = u64::try_from(suffix).map_err(|_| invalid())?;
            let prefix = match &mut self.prefixes {
                Some(prefixes) => {
                    u64::try_from(prefixes.next()?.ok_or_else(invalid)?).map_err(|_| invalid())?
                }
                None => 0,
            };
            if prefix > self.previous_length {
                return Err(invalid());
            }
            self.consumed_bytes = self
                .consumed_bytes
                .checked_add(suffix)
                .ok_or_else(invalid)?;
            if self.consumed_bytes > self.payload_bytes {
                return Err(invalid());
            }
            self.previous_length = prefix.checked_add(suffix).ok_or_else(invalid)?;
            *item = self.previous_length;
            written += 1;
        }
        // Pinned Arrow consumes only the declared values. Any unused tail is
        // still owned and charged as page bytes, but is not another value.
        Ok(written)
    }
}

/// Check a complete delta payload without allocating according to body data.
/// `maximum` is the already admitted page value count; nulls can make the body
/// count smaller. Non-delta encodings return None and keep their own validators.
// Encoding variants share count validation and bounded replay invariants.
#[allow(clippy::too_many_lines)]
pub(super) fn validate(
    encoding: Encoding,
    input: &[u8],
    maximum: usize,
    integer_width: u8,
    cancellation: Option<&CancellationToken>,
) -> Result<Option<DeltaFacts>, GfError> {
    check_cancel(cancellation)?;
    match encoding {
        Encoding::DELTA_BINARY_PACKED => {
            let mut values = Integers::new(input, maximum, integer_width)?;
            let count = values.count;
            #[cfg(test)]
            let auxiliary_bytes = values.minis as u64;
            stream_end(&mut values, cancellation)?;
            Ok(Some(DeltaFacts {
                values: count,
                #[cfg(test)]
                auxiliary_bytes,
                #[cfg(test)]
                largest_value: u64::from(integer_width) / 8,
            }))
        }
        Encoding::DELTA_LENGTH_BYTE_ARRAY => {
            let mut lengths = Integers::new(input, maximum, 32)?;
            let count = lengths.count;
            #[cfg(test)]
            let minis = lengths.minis;
            let mut total = 0_u64;
            #[cfg(test)]
            let mut largest = 0_u64;
            let mut events = 0;
            while let Some(length) = lengths.next()? {
                events += 1;
                if events == 1024 {
                    check_cancel(cancellation)?;
                    events = 0;
                }
                let length = u64::try_from(length).map_err(|_| invalid())?;
                total = total.checked_add(length).ok_or_else(invalid)?;
                #[cfg(test)]
                {
                    largest = largest.max(length);
                }
            }
            let end = stream_end(&mut lengths, cancellation)?;
            if total > (input.len() - end) as u64 {
                return Err(invalid());
            }
            Ok(Some(DeltaFacts {
                values: count,
                #[cfg(test)]
                auxiliary_bytes: (count as u64)
                    .saturating_mul(4)
                    .saturating_add(minis as u64),
                #[cfg(test)]
                largest_value: largest,
            }))
        }
        Encoding::DELTA_BYTE_ARRAY => {
            let mut prefixes = Integers::new(input, maximum, 32)?;
            let count = prefixes.count;
            #[cfg(test)]
            let prefix_minis = prefixes.minis;
            let prefix_end = stream_end(&mut prefixes, cancellation)?;
            let suffix_input = input.get(prefix_end..).ok_or_else(invalid)?;
            let mut suffixes = Integers::new(suffix_input, maximum, 32)?;
            if suffixes.count != count {
                return Err(invalid());
            }
            #[cfg(test)]
            let suffix_minis = suffixes.minis;
            let suffix_end = stream_end(&mut suffixes, cancellation)?;
            let payload = suffix_input.len() - suffix_end;
            let mut prefixes = Integers::new(input, maximum, 32)?;
            let mut suffixes = Integers::new(suffix_input, maximum, 32)?;
            let mut previous = 0_u64;
            #[cfg(test)]
            let mut largest = 0_u64;
            let mut total = 0_u64;
            for index in 0..count {
                if index % 1024 == 0 {
                    check_cancel(cancellation)?;
                }
                let prefix =
                    u64::try_from(prefixes.next()?.ok_or_else(invalid)?).map_err(|_| invalid())?;
                let suffix =
                    u64::try_from(suffixes.next()?.ok_or_else(invalid)?).map_err(|_| invalid())?;
                if prefix > previous {
                    return Err(invalid());
                }
                total = total.checked_add(suffix).ok_or_else(invalid)?;
                previous = prefix.checked_add(suffix).ok_or_else(invalid)?;
                #[cfg(test)]
                {
                    largest = largest.max(previous);
                }
            }
            if total > payload as u64 {
                return Err(invalid());
            }
            Ok(Some(DeltaFacts {
                values: count,
                #[cfg(test)]
                auxiliary_bytes: (count as u64)
                    .saturating_mul(8)
                    .saturating_add(prefix_minis as u64)
                    .saturating_add(suffix_minis as u64)
                    .saturating_add(largest.saturating_mul(2)),
                #[cfg(test)]
                largest_value: largest,
            }))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests;
