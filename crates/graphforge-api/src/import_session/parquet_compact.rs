//! Allocation-free compact-protocol cursor over an already-owned Parquet
//! footer (#1918).
//!
//! The grammar mirrors the pinned parquet 58.4 borrowing decoder
//! (`ThriftSliceInputProtocol` and the compact field, list, boolean, and skip
//! rules it implements) and adds the guards the native decoder does not have:
//! checked varint and integer widths, list-capacity admission before a caller
//! allocates, cooperative cancellation at entry and at every bounded field and
//! element cadence, and a constant-time fold for skipped boolean lists whose
//! zero-byte elements the native decoder still loops over one by one.
//!
//! The cursor borrows the footer bytes; it never allocates proportionally to
//! declared counts, depths, or body bytes, and it never demands that the input
//! be fully consumed: bytes after a root struct's STOP stay untouched, exactly
//! as the native parser leaves them. A boolean struct field carries no
//! payload, so its value rides in [`Kind::BoolTrue`] / [`Kind::BoolFalse`],
//! mirroring the native `FieldIdentifier::bool_val`. Declared UTF-8 strings
//! are admitted as a borrowed [`BorrowedUtf8`] range, validated in bounded
//! windows — never re-scanned or copied as a whole.
//!
//! Every public reading entry observes cancellation before it consumes a
//! byte or chooses any other error, including [`CompactSlice::allocating_list`]'s
//! zero-width admission error.
//!
//! Guard contract: [`CompactSlice::allocating_list`] only admits a declared
//! capacity against the caller's admitted budget and the bytes that remain.
//! It does not validate element grammar and does not claim a complete footer
//! or metadata envelope — callers must still walk every actual element and
//! account their owned allocations for the whole phase separately. It must not
//! be used for unknown boolean lists: native skips their elements without
//! reading any bytes, so [`CompactSlice::skip`] folds those lists instead.
//!
//! Documented divergences, all reachable only from hostile writers: the
//! value-reading [`CompactSlice::read_vlq`] caps the varint at the 10 bytes a
//! `u64` can occupy instead of letting the native decoder's shifts wrap into
//! lost bits; [`CompactSlice::read_i16`], [`CompactSlice::read_i32`], and full
//! field ids refuse a decoded value that does not fit the declared width,
//! where the native int16/int32 readers accept it with a truncating cast — so
//! a malformed varint that fits `u64` but not the target width is a typed
//! `integer_width` refusal here and silently different bits natively; and
//! list counts above `i32::MAX` are rejected where the native decoder's
//! wrapping shifts could smuggle a small count out of a huge overlong
//! encoding.

use graphforge_core::GfError;

use super::{cancelled, limit, storage};
use crate::CancellationToken;

/// Native default skip depth (pinned parquet 58.4 `DEFAULT_SKIP_DEPTH`).
const SKIP_DEPTH: i8 = 64;

/// Element cadence at which skipping re-checks cancellation.
const SKIP_CANCEL_CADENCE: usize = 1024;

/// Bytes of UTF-8 validated per bounded window before cancellation is
/// re-checked. Far longer than any incomplete UTF-8 prefix (at most three
/// bytes), which is what keeps every window step advancing.
const UTF8_WINDOW: usize = 1024;

fn unexpected_end() -> GfError {
    storage("Parquet footer compact stream ends before the declared bytes")
}

fn malformed() -> GfError {
    storage("Parquet footer compact wire is malformed")
}

fn varint_overflow() -> GfError {
    storage("Parquet footer compact varint exceeds 64 bits")
}

fn integer_width() -> GfError {
    storage("Parquet footer compact integer exceeds its declared width")
}

#[cfg(test)]
fn invalid_bool() -> GfError {
    storage("Parquet footer compact boolean value is not 0, 1 or 2")
}

fn invalid_utf8() -> GfError {
    storage("Parquet footer compact string is not UTF-8")
}

fn delta_overflow() -> GfError {
    storage("Parquet footer compact field id delta overflows i16")
}

fn list_count_overflow() -> GfError {
    storage("Parquet footer compact list count exceeds i32")
}

fn depth_exceeded() -> GfError {
    storage("Parquet footer compact skip exceeds the native depth of 64")
}

fn unsupported_skip() -> GfError {
    storage("Parquet footer compact field type cannot be skipped")
}

fn width_zero() -> GfError {
    limit("Parquet footer list element width must be positive")
}

fn capacity_overflow() -> GfError {
    limit("Parquet footer list capacity is not representable")
}

/// Compact wire discriminant for struct fields and list elements (pinned
/// parquet 58.4 `FieldType`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Kind {
    Stop = 0,
    BoolTrue = 1,
    BoolFalse = 2,
    Byte = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    Double = 7,
    Binary = 8,
    List = 9,
    Set = 10,
    Map = 11,
    Struct = 12,
}

impl TryFrom<u8> for Kind {
    type Error = GfError;

    fn try_from(value: u8) -> Result<Self, GfError> {
        match value {
            0 => Ok(Self::Stop),
            1 => Ok(Self::BoolTrue),
            2 => Ok(Self::BoolFalse),
            3 => Ok(Self::Byte),
            4 => Ok(Self::I16),
            5 => Ok(Self::I32),
            6 => Ok(Self::I64),
            7 => Ok(Self::Double),
            8 => Ok(Self::Binary),
            9 => Ok(Self::List),
            10 => Ok(Self::Set),
            11 => Ok(Self::Map),
            12 => Ok(Self::Struct),
            _ => Err(malformed()),
        }
    }
}

/// One decoded struct field. A boolean field's value is its kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Field {
    /// Compact field id, delta-checked or fully read against `i16`.
    pub(super) id: i16,
    /// Wire discriminant; boolean values ride in the kind.
    pub(super) kind: Kind,
}

/// One decoded list header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct List {
    /// Declared element count, at most `i32::MAX` like the native decoder.
    pub(super) count: usize,
    /// Element discriminant. Both boolean list tags normalize to
    /// [`Kind::BoolTrue`], and an all-zero header byte reports an empty list
    /// of [`Kind::Byte`], matching `read_list_begin`.
    pub(super) element: Kind,
}

/// An admitted borrowed UTF-8 range, validated in bounded windows.
///
/// Deliberately not a [`str`]: admitting a footer string must not demand a
/// whole-range re-validation pass or a copy, and the root footer facts that
/// will consume these ranges need raw bytes, byte offsets, and ASCII key
/// comparisons — none of which require string semantics. The only accessors
/// are the borrowed [`Self::as_bytes`] and [`Self::len`]; there is no
/// accessor that validates or copies the whole range as a string, and no
/// unsafe.
pub(super) struct BorrowedUtf8<'a> {
    bytes: &'a [u8],
}

impl BorrowedUtf8<'_> {
    /// The admitted UTF-8 bytes.
    #[cfg(test)]
    pub(super) fn as_bytes(&self) -> &[u8] {
        self.bytes
    }

    /// The admitted length in bytes.
    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }
}

/// Borrowing cursor over the unconsumed tail of an already-owned Parquet
/// footer. Holds only the borrowed bytes, an optional borrowed cancellation
/// token, and the consumed offset; construction allocates nothing.
pub(super) struct CompactSlice<'a> {
    remaining: &'a [u8],
    cancellation: Option<&'a CancellationToken>,
    position: usize,
}

/// Minimum body bytes one list element of `kind` can occupy on the wire, or
/// `None` when the native decoder cannot skip the kind at all.
fn minimum_body_bytes(kind: Kind) -> Option<u64> {
    match kind {
        Kind::Double => Some(8),
        Kind::Byte
        | Kind::I16
        | Kind::I32
        | Kind::I64
        | Kind::Binary
        | Kind::List
        | Kind::Struct => Some(1),
        Kind::BoolTrue | Kind::BoolFalse | Kind::Stop => Some(0),
        Kind::Set | Kind::Map => None,
    }
}

impl<'a> CompactSlice<'a> {
    /// Position a cursor over the unconsumed tail of an owned footer.
    pub(super) fn new(remaining: &'a [u8], cancellation: Option<&'a CancellationToken>) -> Self {
        Self {
            remaining,
            cancellation,
            position: 0,
        }
    }

    /// Bytes not yet consumed.
    pub(super) fn remaining(&self) -> &'a [u8] {
        self.remaining
    }

    /// Bytes consumed since construction.
    pub(super) fn position(&self) -> usize {
        self.position
    }

    fn cancelled(&self) -> Result<(), GfError> {
        if self
            .cancellation
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(cancelled());
        }
        Ok(())
    }

    /// Consume `count` bytes the caller already sized against the stream.
    fn take(&mut self, count: usize) -> Result<(), GfError> {
        if self.remaining.len() < count {
            return Err(unexpected_end());
        }
        self.remaining = &self.remaining[count..];
        self.position += count;
        Ok(())
    }

    /// Read one wire byte.
    pub(super) fn read_byte(&mut self) -> Result<u8, GfError> {
        self.cancelled()?;
        let Some((&byte, rest)) = self.remaining.split_first() else {
            return Err(unexpected_end());
        };
        self.remaining = rest;
        self.position += 1;
        Ok(byte)
    }

    /// Read a ULEB128 varint. Capped at the 10 bytes a `u64` can occupy, with
    /// a final payload of at most 1: anything else is rejected as overflow
    /// instead of wrapping the way the native decoder's shifted bits would.
    pub(super) fn read_vlq(&mut self) -> Result<u64, GfError> {
        self.cancelled()?;
        let mut value = 0_u64;
        for index in 0..10_u32 {
            let byte = self.read_byte()?;
            if index == 9 && byte > 1 {
                return Err(varint_overflow());
            }
            value |= u64::from(byte & 0x7F) << (index * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(varint_overflow())
    }

    /// Read a zig-zag encoded signed 64-bit value.
    #[allow(clippy::cast_possible_wrap)] // zigzag decoding
    pub(super) fn read_zig_zag(&mut self) -> Result<i64, GfError> {
        self.cancelled()?;
        let value = self.read_vlq()?;
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    /// Read a zig-zag varint that must fit `i16`, as full field ids do.
    pub(super) fn read_i16(&mut self) -> Result<i16, GfError> {
        self.cancelled()?;
        let value = self.read_zig_zag()?;
        i16::try_from(value).map_err(|_| integer_width())
    }

    /// Read a zig-zag varint that must fit `i32`.
    pub(super) fn read_i32(&mut self) -> Result<i32, GfError> {
        self.cancelled()?;
        let value = self.read_zig_zag()?;
        i32::try_from(value).map_err(|_| integer_width())
    }

    /// Read a boolean list-element value. The native decoder accepts 1 as
    /// true and both 0 and 2 as false.
    #[cfg(test)]
    pub(super) fn read_bool_value(&mut self) -> Result<bool, GfError> {
        self.cancelled()?;
        match self.read_byte()? {
            0x01 => Ok(true),
            0x00 | 0x02 => Ok(false),
            _ => Err(invalid_bool()),
        }
    }

    /// Borrow a declared-length binary range; nothing is copied.
    pub(super) fn read_bytes(&mut self) -> Result<&'a [u8], GfError> {
        self.cancelled()?;
        let length = self.read_vlq()?;
        let length = usize::try_from(length).map_err(|_| unexpected_end())?;
        let range = self.remaining.get(..length).ok_or_else(unexpected_end)?;
        self.remaining = &self.remaining[length..];
        self.position += length;
        Ok(range)
    }

    /// Admit a declared-length UTF-8 range without copying it.
    ///
    /// The borrowed bytes are validated in bounded [`UTF8_WINDOW`] windows
    /// with `std::str::from_utf8`, so a hostile footer cannot hold the cursor
    /// in one unbounded scan: cancellation is re-checked before every window
    /// and once after the last, and nothing is allocated proportionally to
    /// the declared length. A window edge landing inside a multibyte
    /// sequence reports an incomplete prefix (`error_len` is `None`); with
    /// input left the window advances over `valid_up_to` only, carrying the
    /// at-most-three prefix bytes into the next window so sequences are
    /// never split — and because a whole [`UTF8_WINDOW`] window can never
    /// consist only of that prefix, `valid_up_to` is positive there and every
    /// such step advances. A real invalid byte, or a sequence truncated by
    /// the end of the declared range, refuses the value.
    ///
    /// The admitted range is returned as [`BorrowedUtf8`], not [`str`]: the
    /// cursor lends admitted bytes for byte, offset, and ASCII key
    /// comparison, with no accessor that re-validates or copies the whole
    /// range as a string.
    pub(super) fn read_utf8(&mut self) -> Result<BorrowedUtf8<'a>, GfError> {
        self.cancelled()?;
        let bytes = self.read_bytes()?;
        let mut start = 0_usize;
        while start < bytes.len() {
            self.cancelled()?;
            let end = (start + UTF8_WINDOW).min(bytes.len());
            match std::str::from_utf8(&bytes[start..end]) {
                Ok(_) => start = end,
                Err(error) if error.error_len().is_some() => return Err(invalid_utf8()),
                Err(error) => {
                    if end == bytes.len() {
                        // The range itself ends inside the sequence.
                        return Err(invalid_utf8());
                    }
                    debug_assert!(error.valid_up_to() > 0);
                    start += error.valid_up_to();
                }
            }
        }
        self.cancelled()?;
        Ok(BorrowedUtf8 { bytes })
    }

    /// Read the next struct field, or `None` at the struct's STOP. `previous`
    /// is the last field id in the struct (0 before the first field). A delta
    /// addition and any explicit full id are checked against `i16`; a STOP
    /// ends the struct regardless of its delta nibble and consumes exactly one
    /// byte.
    pub(super) fn read_field(&mut self, previous: i16) -> Result<Option<Field>, GfError> {
        self.cancelled()?;
        let header = self.read_byte()?;
        let kind = Kind::try_from(header & 0x0F)?;
        if kind == Kind::Stop {
            return Ok(None);
        }
        let delta = (header & 0xF0) >> 4;
        let id = if delta != 0 {
            previous
                .checked_add(i16::from(delta))
                .ok_or_else(delta_overflow)?
        } else {
            self.read_i16()?
        };
        Ok(Some(Field { id, kind }))
    }

    /// Read a list header. Element tags 1 and 2 (the legacy and spec boolean
    /// tags) normalize to [`Kind::BoolTrue`], an all-zero header byte is an
    /// empty list of [`Kind::Byte`], and a nibble-encoded count of 15 reads a
    /// varint that must fit `i32`, all matching the native decoder. The
    /// advertised element kind is returned unchecked: the native
    /// `read_thrift_vec` dispatches on the expected consumer type and
    /// validates nothing about the tag beyond the header itself.
    pub(super) fn read_list(&mut self) -> Result<List, GfError> {
        self.cancelled()?;
        let header = self.read_byte()?;
        if header == 0 {
            return Ok(List {
                count: 0,
                element: Kind::Byte,
            });
        }
        let element = match header & 0x0F {
            1 | 2 => Kind::BoolTrue,
            nibble @ 3..=12 => Kind::try_from(nibble)?,
            _ => return Err(malformed()),
        };
        let nibble = (header & 0xF0) >> 4;
        let count = if nibble == 15 {
            let declared = i32::try_from(self.read_vlq()?).map_err(|_| list_count_overflow())?;
            usize::try_from(declared).map_err(|_| list_count_overflow())?
        } else {
            usize::from(nibble)
        };
        Ok(List { count, element })
    }

    /// Skip a value of the given kind with the native default depth of 64.
    pub(super) fn skip(&mut self, kind: Kind) -> Result<(), GfError> {
        self.skip_till_depth(kind, SKIP_DEPTH)
    }

    /// Skip a varint without decoding it. The native skip has no width
    /// requirement and no length cap, so this only follows continuation bits
    /// until the stream ends, re-checking cancellation at a fixed byte
    /// cadence.
    fn skip_vlq(&mut self) -> Result<(), GfError> {
        let mut batch = 0_usize;
        loop {
            if batch == SKIP_CANCEL_CADENCE {
                self.cancelled()?;
                batch = 0;
            }
            let byte = self.read_byte()?;
            batch += 1;
            if byte & 0x80 == 0 {
                return Ok(());
            }
        }
    }

    fn skip_till_depth(&mut self, kind: Kind, depth: i8) -> Result<(), GfError> {
        self.cancelled()?;
        if depth == 0 {
            return Err(depth_exceeded());
        }
        match kind {
            Kind::BoolTrue | Kind::BoolFalse => Ok(()),
            Kind::Byte => self.read_byte().map(|_| ()),
            Kind::I16 | Kind::I32 | Kind::I64 => self.skip_vlq(),
            Kind::Double => self.take(8),
            Kind::Binary => {
                let length = self.read_vlq()?;
                let length = usize::try_from(length).map_err(|_| unexpected_end())?;
                self.take(length)
            }
            Kind::Struct => {
                let mut previous = 0_i16;
                loop {
                    let Some(field) = self.read_field(previous)? else {
                        return Ok(());
                    };
                    self.skip_till_depth(field.kind, depth - 1)?;
                    previous = field.id;
                }
            }
            Kind::List => {
                let list = self.read_list()?;
                if list.count == 0 {
                    return Ok(());
                }
                // Native converts the element tag before it calls any child,
                // so an unskippable element kind fails first at any depth.
                let Some(minimum) = minimum_body_bytes(list.element) else {
                    return Err(unsupported_skip());
                };
                if depth == 1 {
                    // Native calls every element at depth 0, which fails
                    // before a single body byte is consumed.
                    return Err(depth_exceeded());
                }
                let element = list.element;
                if element == Kind::BoolTrue {
                    // Skipped boolean elements carry no payload bytes: fold
                    // the declared count in constant time instead of looping
                    // over it, and leave the body untouched.
                    self.cancelled()?;
                    return Ok(());
                }
                let count = u64::try_from(list.count).map_err(|_| unexpected_end())?;
                let needed = count.checked_mul(minimum).ok_or_else(unexpected_end)?;
                if needed > self.remaining.len() as u64 {
                    return Err(unexpected_end());
                }
                for index in 0..list.count {
                    if index % SKIP_CANCEL_CADENCE == 0 {
                        self.cancelled()?;
                    }
                    self.skip_till_depth(element, depth - 1)?;
                }
                Ok(())
            }
            Kind::Stop | Kind::Set | Kind::Map => Err(unsupported_skip()),
        }
    }

    /// Admit a known allocating list before the caller allocates for it.
    ///
    /// Observes cancellation on entry — before the zero-width admission
    /// error or any byte is consumed — and then rejects, with typed
    /// resource-limit errors, a zero `element_width`, a
    /// `count * element_width` that is not representable as `usize`/`isize`,
    /// or a request above `available_request_bytes` — all before the caller
    /// may reserve. The declared count must also fit the remaining bytes,
    /// because every known allocating element (structs, integers, key/value
    /// entries, column orders, histograms) needs at least one body byte.
    ///
    /// This does not validate element grammar and does not claim a complete
    /// footer or metadata envelope: callers must walk every actual element and
    /// account their owned allocations for the whole phase separately. It must
    /// not be used for unknown boolean lists — native skips their zero-byte
    /// elements without reading them, so [`CompactSlice::skip`] folds those
    /// lists instead.
    ///
    /// The advertised element kind is returned unchecked, matching the native
    /// `read_thrift_vec`: it dispatches on the expected consumer type and
    /// checks nothing about the element tag beyond the list header.
    pub(super) fn allocating_list(
        &mut self,
        element_width: usize,
        available_request_bytes: u64,
    ) -> Result<List, GfError> {
        self.cancelled()?;
        if element_width == 0 {
            return Err(width_zero());
        }
        let list = self.read_list()?;
        if list.count == 0 {
            return Ok(list);
        }
        let needed = list
            .count
            .checked_mul(element_width)
            .ok_or_else(capacity_overflow)?;
        if needed > isize::MAX as usize {
            return Err(capacity_overflow());
        }
        if needed as u64 > available_request_bytes {
            return Err(limit(
                "Parquet footer list capacity exceeds the admitted footer budget",
            ));
        }
        if list.count > self.remaining.len() {
            return Err(unexpected_end());
        }
        Ok(list)
    }
}

#[cfg(test)]
mod tests;
