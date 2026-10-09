//! Borrowing value-length facts for plain, delta, and dictionary-encoded
//! Parquet pages (#1918).
//!
//! Planning and owned-page preflight need per-value lengths, never payload
//! copies: these cursors borrow one already-owned page slice (or admitted
//! dictionary facts) and keep scalar offset/count/emitted state, filling
//! caller-owned fixed blocks of at most [`MAX_BLOCK_EVENTS`] lengths. Delta
//! bodies reuse `super::parquet_delta::Lengths`, and their decoded body count
//! must equal the required nonnull values exactly before any dangerous
//! third-party setter runs. Dictionary facts carry exactly one `u64` per
//! dictionary entry — never per data row — and that single vector is charged
//! against an explicit capacity argument and reserved only after an
//! allocation-free pass validates the body geometry, so a malformed body is
//! refused before any allocation regardless of the admitted count.
//!
//! Scope: this module covers only these borrowing cursors and that one
//! indispensable dictionary vector. Reader-owned compressed and decompressed
//! page bytes, the native dictionary payload/offset copies the runtime decoder
//! builds, and the shared-ledger/PageReader/footer integration remain separate
//! credits owned by later slices; complete source safety is not established
//! here.

use std::mem::size_of;

use graphforge_core::GfError;
use parquet::basic::Encoding;

use crate::CancellationToken;

use super::parquet_delta::Lengths;
use super::parquet_levels::{DictionaryIndices, IndexSource, MAX_BLOCK_EVENTS};
use super::{cancelled, limit, storage};

fn check_cancel(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    Ok(())
}

fn truncated() -> GfError {
    storage("Parquet value stream ends before the required bytes")
}

fn insufficient() -> GfError {
    storage("Parquet value stream ends before the required values")
}

fn count_mismatch() -> GfError {
    storage("Parquet delta body count does not equal the required nonnull values")
}

/// Uniform interface for the value-length cursors: scalar iteration with
/// bounded state, plus fixed-block fills for the shape accountant.
pub(super) trait LengthSource {
    /// Nonnull values the caller declared for this section.
    fn expected(&self) -> usize;
    /// Values already consumed and validated.
    fn emitted(&self) -> usize;
    /// Next validated length, or `None` once `expected` values are supplied.
    fn next_length(&mut self) -> Result<Option<u64>, GfError>;

    /// Values still required.
    fn remaining(&self) -> usize {
        self.expected() - self.emitted()
    }

    /// Fill `block` with up to [`MAX_BLOCK_EVENTS`] validated lengths and
    /// return how many were written. A larger slice is capped the same way,
    /// and the unfilled second half stays untouched for the caller's sentinel
    /// checks.
    fn next_block(&mut self, block: &mut [u64]) -> Result<usize, GfError>;
}

/// Shared block-fill loop. At most [`MAX_BLOCK_EVENTS`] lengths leave the
/// cursor per call; the caller keeps cancellation ownership per block.
fn drain_into(source: &mut impl LengthSource, block: &mut [u64]) -> Result<usize, GfError> {
    let mut filled = 0;
    for slot in block.iter_mut().take(MAX_BLOCK_EVENTS) {
        let Some(length) = source.next_length()? else {
            break;
        };
        *slot = length;
        filled += 1;
    }
    Ok(filled)
}

/// `PLAIN` `BYTE_ARRAY` lengths over one owned page slice. Each required
/// nonnull value consumes a signed four-byte length and its checked in-bounds
/// payload; the payload itself is never copied and no count-sized vector is
/// created. Unused body tails after the required prefix stay admitted.
pub(super) struct PlainByteLengths<'a> {
    data: &'a [u8],
    offset: usize,
    expected: usize,
    emitted: usize,
    cancellation: Option<&'a CancellationToken>,
}

impl<'a> PlainByteLengths<'a> {
    fn new(
        data: &'a [u8],
        expected_nonnull: usize,
        cancellation: Option<&'a CancellationToken>,
    ) -> Self {
        Self {
            data,
            offset: 0,
            expected: expected_nonnull,
            emitted: 0,
            cancellation,
        }
    }
}

impl LengthSource for PlainByteLengths<'_> {
    fn expected(&self) -> usize {
        self.expected
    }

    fn emitted(&self) -> usize {
        self.emitted
    }

    fn next_length(&mut self) -> Result<Option<u64>, GfError> {
        if self.emitted == self.expected {
            return Ok(None);
        }
        // Scalar steps stay cancellable like the block path: the check fires
        // once a value is actually consumed, consistent with the delegated
        // delta cursor.
        check_cancel(self.cancellation)?;
        let length_end = self.offset.checked_add(4).ok_or_else(truncated)?;
        let raw = self
            .data
            .get(self.offset..length_end)
            .ok_or_else(truncated)?;
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(raw);
        let length = usize::try_from(i32::from_le_bytes(bytes))
            .map_err(|_| storage("Parquet plain byte-array length is negative"))?;
        let payload_end = length_end.checked_add(length).ok_or_else(truncated)?;
        if payload_end > self.data.len() {
            return Err(truncated());
        }
        // The payload stays borrowed in place: only its length advances the
        // cursor, so a tiny header claiming a huge value refuses without any
        // length-sized allocation.
        self.offset = payload_end;
        self.emitted += 1;
        Ok(Some(u64::try_from(length).map_err(|_| {
            storage("Parquet plain byte-array length is out of range")
        })?))
    }

    fn next_block(&mut self, block: &mut [u64]) -> Result<usize, GfError> {
        check_cancel(self.cancellation)?;
        drain_into(self, block)
    }
}

/// Delta lengths delegated to the already-integrated
/// [`Lengths`](super::parquet_delta::Lengths) cursor, with the exact body-count
/// agreement the dangerous third-party setters rely on.
pub(super) struct DeltaByteLengths<'a> {
    lengths: Lengths<'a>,
    expected: usize,
    emitted: usize,
}

impl<'a> DeltaByteLengths<'a> {
    fn new(
        encoding: Encoding,
        input: &'a [u8],
        expected_nonnull: usize,
        cancellation: Option<&'a CancellationToken>,
    ) -> Result<Self, GfError> {
        // `Lengths` refuses a decoded count above the admitted maximum; the
        // agreement check below refuses a complete but smaller stream. Only
        // the exact nonnull count passes both.
        let lengths = Lengths::new(encoding, input, expected_nonnull, cancellation)?;
        if lengths.count() != expected_nonnull {
            return Err(count_mismatch());
        }
        Ok(Self {
            lengths,
            expected: expected_nonnull,
            emitted: 0,
        })
    }
}

impl LengthSource for DeltaByteLengths<'_> {
    fn expected(&self) -> usize {
        self.expected
    }

    fn emitted(&self) -> usize {
        self.emitted
    }

    fn next_length(&mut self) -> Result<Option<u64>, GfError> {
        if self.emitted == self.expected {
            return Ok(None);
        }
        let mut slot = [0_u64; 1];
        if self.lengths.next_block(&mut slot)? == 0 {
            return Err(insufficient());
        }
        self.emitted += 1;
        Ok(Some(slot[0]))
    }

    fn next_block(&mut self, block: &mut [u64]) -> Result<usize, GfError> {
        // The delegated cursor enforces cancellation and its own 1024 cap.
        let written = self.lengths.next_block(block)?;
        self.emitted += written;
        Ok(written)
    }
}

/// Borrowing length source for a data page's value section: `PLAIN`
/// `BYTE_ARRAY` or one of the two delta byte-array encodings. Construction
/// validates every dangerous count before a decoder can be initialized.
pub(super) enum ValueLengths<'a> {
    Plain(PlainByteLengths<'a>),
    Delta(DeltaByteLengths<'a>),
}

impl<'a> ValueLengths<'a> {
    /// `expected_nonnull` is the decoded nonnull value count, never the page's
    /// level-event count: null placeholders carry no length.
    pub(super) fn new(
        encoding: Encoding,
        body: &'a [u8],
        expected_nonnull: usize,
        cancellation: Option<&'a CancellationToken>,
    ) -> Result<Self, GfError> {
        check_cancel(cancellation)?;
        match encoding {
            Encoding::PLAIN => Ok(Self::Plain(PlainByteLengths::new(
                body,
                expected_nonnull,
                cancellation,
            ))),
            Encoding::DELTA_LENGTH_BYTE_ARRAY | Encoding::DELTA_BYTE_ARRAY => Ok(Self::Delta(
                DeltaByteLengths::new(encoding, body, expected_nonnull, cancellation)?,
            )),
            _ => Err(storage("unsupported Parquet value length encoding")),
        }
    }
}

impl LengthSource for ValueLengths<'_> {
    fn expected(&self) -> usize {
        match self {
            Self::Plain(plain) => plain.expected(),
            Self::Delta(delta) => delta.expected(),
        }
    }

    fn emitted(&self) -> usize {
        match self {
            Self::Plain(plain) => plain.emitted(),
            Self::Delta(delta) => delta.emitted(),
        }
    }

    fn next_length(&mut self) -> Result<Option<u64>, GfError> {
        match self {
            Self::Plain(plain) => plain.next_length(),
            Self::Delta(delta) => delta.next_length(),
        }
    }

    fn next_block(&mut self, block: &mut [u64]) -> Result<usize, GfError> {
        match self {
            Self::Plain(plain) => plain.next_block(block),
            Self::Delta(delta) => delta.next_block(block),
        }
    }
}

/// Aggregate plain byte-array facts for planning: the checked payload total
/// and largest length of the required prefix, walked without copying payload
/// or storing per-row lengths. The consumed value count is the caller's
/// `expected_nonnull` itself.
pub(super) struct PlainValueFacts {
    pub(super) payload_bytes: u64,
    pub(super) largest_length: u64,
}

pub(super) fn plain_value_facts(
    body: &[u8],
    expected_nonnull: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<PlainValueFacts, GfError> {
    check_cancel(cancellation)?;
    let mut cursor = PlainByteLengths::new(body, expected_nonnull, cancellation);
    let mut payload_bytes = 0_u64;
    let mut largest_length = 0_u64;
    let mut block = [0_u64; MAX_BLOCK_EVENTS];
    loop {
        let count = cursor.next_block(&mut block)?;
        for &length in &block[..count] {
            payload_bytes = payload_bytes
                .checked_add(length)
                .ok_or_else(|| storage("Parquet plain payload total overflows"))?;
            largest_length = largest_length.max(length);
        }
        if count == 0 {
            break;
        }
    }
    Ok(PlainValueFacts {
        payload_bytes,
        largest_length,
    })
}

/// Allocation-free preflight for one admitted `PLAIN` `BYTE_ARRAY` dictionary
/// body: a complete checked walk of the already-owned bytes that validates
/// every entry length and aggregates the plain facts while storing no
/// per-entry state and requesting no allocation. [`DictionaryByteFacts::new`]
/// runs this before it reserves anything, so a malformed body is refused as a
/// typed storage error before any allocation; a structural
/// `entry_count * 4 <= body.len()` lower bound would not validate the
/// individual entry lengths and cannot replace this walk. Cancellation is
/// honored per bounded block, zero entries are accepted, and unused tails
/// stay unexamined.
fn dictionary_body_preflight(
    body: &[u8],
    entry_count: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<PlainValueFacts, GfError> {
    plain_value_facts(body, entry_count, cancellation)
}

/// Admitted byte-length facts for one already-owned `PLAIN` `BYTE_ARRAY`
/// dictionary body. The lengths vector is this helper's entire live heap
/// allocation: exactly one `u64` per dictionary entry, never per data row.
/// The owned dictionary page bytes themselves and the native payload/offset
/// copies the runtime dictionary decoder later builds are separate credits
/// charged by their owners.
pub(super) struct DictionaryByteFacts {
    lengths: Vec<u64>,
    payload_bytes: u64,
    largest_length: u64,
}

impl DictionaryByteFacts {
    /// Every entry's length and payload bounds are validated here, before any
    /// consumer or dictionary setter can act on the facts. A zero-entry
    /// dictionary is accepted; a claimed entry count whose vector would
    /// exceed `credit` is refused before any allocation is requested, and the
    /// complete body geometry is checked by the allocation-free
    /// [`dictionary_body_preflight`] before the vector is reserved, so a
    /// malformed body is refused as a typed storage error before any
    /// allocation no matter how large an admitted count claims. Unused body
    /// tails after the entries stay admitted.
    pub(super) fn new(
        body: &[u8],
        entry_count: usize,
        credit: usize,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Self, GfError> {
        check_cancel(cancellation)?;
        // Charge the required count against the admitted capacity before
        // anything else. An oversized claim never reaches the allocator.
        let required = entry_count.checked_mul(size_of::<u64>()).ok_or_else(|| {
            limit("Parquet dictionary length vector overflows the addressable workspace")
        })?;
        if required > credit {
            return Err(limit(
                "Parquet dictionary length vector exceeds its admitted capacity",
            ));
        }
        // The complete allocation-free checked body pass runs BEFORE
        // reserve/resize: a tiny malformed body plus a huge admitted count is
        // refused here before any allocation is requested, however large the
        // admitted credit is.
        let facts = dictionary_body_preflight(body, entry_count, cancellation)?;
        let mut lengths: Vec<u64> = Vec::new();
        lengths
            .try_reserve_exact(entry_count)
            .map_err(|_| limit("Cannot allocate admitted Parquet dictionary length vector"))?;
        if lengths
            .capacity()
            .checked_mul(size_of::<u64>())
            .is_none_or(|bytes| bytes > credit)
        {
            return Err(limit(
                "Parquet dictionary length vector capacity exceeds its admitted capacity",
            ));
        }
        lengths.resize(entry_count, 0);
        // Replay the immutable same owned bytes to fill the admitted fixed
        // vector; the preflight already validated this exact prefix.
        let mut cursor = PlainByteLengths::new(body, entry_count, cancellation);
        let mut block = [0_u64; MAX_BLOCK_EVENTS];
        let mut at = 0;
        loop {
            let count = cursor.next_block(&mut block)?;
            for &length in &block[..count] {
                lengths[at] = length;
                at += 1;
            }
            if count == 0 {
                break;
            }
        }
        debug_assert_eq!(at, entry_count);
        Ok(Self {
            lengths,
            payload_bytes: facts.payload_bytes,
            largest_length: facts.largest_length,
        })
    }

    pub(super) fn entries(&self) -> usize {
        self.lengths.len()
    }

    pub(super) fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub(super) fn largest_length(&self) -> u64 {
        self.largest_length
    }

    /// Allocated capacity retained by the dictionary facts vector.
    pub(super) fn inventory_bytes(&self) -> Result<u64, GfError> {
        u64::try_from(self.lengths.capacity())
            .map_err(|_| limit("Parquet dictionary capacity is out of range"))?
            .checked_mul(
                u64::try_from(size_of::<u64>())
                    .map_err(|_| limit("Parquet dictionary element size is out of range"))?,
            )
            .ok_or_else(|| limit("Parquet dictionary capacity overflows"))
    }

    /// Actual referenced payload length for one admitted entry. Consumers get
    /// the referenced length itself, never a dictionary maximum.
    pub(super) fn length(&self, index: usize) -> Option<u64> {
        self.lengths.get(index).copied()
    }
}

/// Dictionary-expanded lengths: borrows admitted [`DictionaryByteFacts`] and
/// the same owned index bytes, consuming exactly `expected_nonnull` indices
/// through the existing [`DictionaryIndices`] cursor and looking up each
/// referenced entry's actual length. All state is scalar plus the borrowed
/// facts: no row- or event-sized vector exists.
pub(super) struct DictionaryExpanded<'a, 'f> {
    facts: Option<&'f DictionaryByteFacts>,
    indices: Option<DictionaryIndices<'a>>,
    expected: usize,
    emitted: usize,
    cancellation: Option<&'a CancellationToken>,
}

impl<'a, 'f> DictionaryExpanded<'a, 'f> {
    /// A missing dictionary or a zero-entry dictionary is refused while any
    /// nonnull value is still required. With zero required values the cursor
    /// ends immediately, so an empty index stream stays valid.
    pub(super) fn new(
        facts: Option<&'f DictionaryByteFacts>,
        stream: &'a [u8],
        expected_nonnull: usize,
        cancellation: Option<&'a CancellationToken>,
    ) -> Result<Self, GfError> {
        check_cancel(cancellation)?;
        if facts.is_none() && expected_nonnull != 0 {
            return Err(storage(
                "Parquet dictionary page is missing but the page requires nonnull values",
            ));
        }
        let indices = match facts {
            Some(facts) if expected_nonnull != 0 => {
                if facts.entries() == 0 {
                    return Err(storage(
                        "Parquet dictionary has no entries but the page requires nonnull values",
                    ));
                }
                // The existing cursor range-checks every index against the
                // admitted entry count before it is returned.
                Some(DictionaryIndices::new(
                    stream,
                    facts.entries(),
                    expected_nonnull,
                )?)
            }
            _ => None,
        };
        Ok(Self {
            facts,
            indices,
            expected: expected_nonnull,
            emitted: 0,
            cancellation,
        })
    }
}

impl LengthSource for DictionaryExpanded<'_, '_> {
    fn expected(&self) -> usize {
        self.expected
    }

    fn emitted(&self) -> usize {
        self.emitted
    }

    fn next_length(&mut self) -> Result<Option<u64>, GfError> {
        if self.emitted == self.expected {
            return Ok(None);
        }
        // Scalar steps stay cancellable like the block path: the check fires
        // once a value is actually consumed, consistent with the delegated
        // delta cursor.
        check_cancel(self.cancellation)?;
        let index = self
            .indices
            .as_mut()
            .ok_or_else(insufficient)?
            .next_index()?
            .ok_or_else(insufficient)?;
        let index = usize::try_from(index)
            .map_err(|_| storage("Parquet dictionary index is out of range"))?;
        let length = self
            .facts
            .and_then(|facts| facts.length(index))
            .ok_or_else(|| storage("Parquet dictionary index falls outside the admitted facts"))?;
        self.emitted += 1;
        Ok(Some(length))
    }

    fn next_block(&mut self, block: &mut [u64]) -> Result<usize, GfError> {
        check_cancel(self.cancellation)?;
        drain_into(self, block)
    }
}

#[cfg(test)]
mod tests;
