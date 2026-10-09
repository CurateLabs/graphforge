//! Wire-grammar tests for the compact footer cursor (#1918).
//!
//! Fixtures encode literal native-compatible compact protocol bytes with a
//! small test-only writer. Assertions pin `GfError` variants (not messages),
//! consumption offsets, and the sentinel bytes a folded skip must leave
//! untouched.

use super::{CompactSlice, Kind};
use crate::CancellationToken;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};

/// Native-compatible compact wire writer for fixtures.
#[allow(clippy::cast_possible_truncation)] // fixture bytes are value-ranged before each cast
struct Wire(Vec<u8>);

impl Wire {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn byte(mut self, value: u8) -> Self {
        self.0.push(value);
        self
    }

    fn repeated(mut self, value: u8, count: usize) -> Self {
        for _ in 0..count {
            self.0.push(value);
        }
        self
    }

    fn vlq(mut self, mut value: u64) -> Self {
        while value >= 0x80 {
            self.0.push(((value & 0x7F) as u8) | 0x80);
            value >>= 7;
        }
        self.0.push(value as u8);
        self
    }

    fn zigzag(self, value: i64) -> Self {
        let encoded = value.wrapping_shl(1) ^ (value >> 63);
        self.vlq(u64::from_ne_bytes(encoded.to_ne_bytes()))
    }

    /// Field header, mirroring the native writer's delta/full-id choice.
    fn field(mut self, kind: Kind, id: i16, previous: i16) -> Self {
        let delta = id.wrapping_sub(previous);
        if (1..=0x0F).contains(&delta) {
            let delta = u8::try_from(delta).expect("delta fits u8");
            self.0.push((delta << 4) | kind as u8);
        } else {
            self.0.push(kind as u8);
            self = self.zigzag(i64::from(id));
        }
        self
    }

    fn stop(mut self) -> Self {
        self.0.push(0);
        self
    }

    fn binary(mut self, bytes: &[u8]) -> Self {
        self = self.vlq(bytes.len() as u64);
        self.0.extend_from_slice(bytes);
        self
    }

    /// List header with the given raw element tag and count.
    fn list(mut self, element: u8, count: usize) -> Self {
        if count < 15 {
            let count = u8::try_from(count).expect("count fits u8");
            self.0.push((count << 4) | element);
        } else {
            self.0.push(0xF0 | element);
            self = self.vlq(u64::try_from(count).expect("count fits u64"));
        }
        self
    }

    /// `depth` nested single-field structs, each holding one more struct.
    fn nested(self, depth: usize) -> Self {
        let with_headers = (0..depth).fold(self, |wire, _| wire.field(Kind::Struct, 1, 0));
        (0..depth).fold(with_headers, |wire, _| wire.stop())
    }

    fn slice(&self) -> &[u8] {
        &self.0
    }
}

fn cursor_of(wire: &Wire) -> CompactSlice<'_> {
    CompactSlice::new(wire.slice(), None)
}

fn assert_malformed(error: GfError) {
    assert!(
        matches!(error, GfError::Storage(_)),
        "expected a malformed-input storage error, got {error:?}"
    );
}

fn assert_limit(error: GfError) {
    assert!(
        matches!(
            error,
            GfError::Project {
                code: ProjectErrorCode::ResourceLimit,
                ..
            }
        ),
        "expected a typed resource-limit error, got {error:?}"
    );
}

fn assert_cancelled(error: GfError) {
    assert!(
        matches!(
            error,
            GfError::Api {
                code: ApiErrorCode::Cancelled,
                ..
            }
        ),
        "expected a typed cancellation error, got {error:?}"
    );
}

#[test]
fn allocating_list_rejects_a_claimed_body_that_cannot_exist() {
    let wire = Wire::new().list(12, 4_096);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.allocating_list(8, u64::MAX).unwrap_err());
    assert_eq!(cursor.position(), 3); // header consumed, body never walked
}

#[test]
fn allocating_list_enforces_the_admitted_budget_before_allocation() {
    let wire = Wire::new().list(12, 64).repeated(0, 64);
    let mut over_budget = cursor_of(&wire);
    assert_limit(over_budget.allocating_list(256, 8_192).unwrap_err());

    let mut admitted = cursor_of(&wire);
    let list = admitted.allocating_list(256, 16_384).unwrap();
    assert_eq!(list.count, 64);
    assert_eq!(list.element, Kind::Struct);
    assert_eq!(admitted.position(), 2); // header only; the caller owns the body walk
    assert_eq!(admitted.remaining().len(), 64);
}

#[test]
fn allocating_list_rejects_zero_width_and_unrepresentable_capacities() {
    let wire = Wire::new().list(12, 2);
    let mut cursor = cursor_of(&wire);
    assert_limit(cursor.allocating_list(0, 0).unwrap_err());

    let mut cursor = cursor_of(&wire);
    assert_limit(cursor.allocating_list(usize::MAX, u64::MAX).unwrap_err());

    let huge = Wire::new().list(12, 1 << 23);
    let mut cursor = cursor_of(&huge);
    assert_limit(cursor.allocating_list(1 << 40, u64::MAX).unwrap_err()); // 2^63 > isize::MAX
}

#[test]
fn allocating_list_accepts_an_empty_list_at_a_zero_budget() {
    let wire = Wire::new().byte(0x00);
    let mut cursor = cursor_of(&wire);
    let list = cursor.allocating_list(8, 0).unwrap();
    assert_eq!((list.count, list.element), (0, Kind::Byte));
    assert_eq!(cursor.position(), 1);
}

#[test]
fn extended_list_counts_above_i32_max_are_malformed() {
    let above_i32_max = usize::try_from(i64::from(i32::MAX) + 1).expect("fits usize");
    let wire = Wire::new().list(8, above_i32_max);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_list().unwrap_err());
}

#[test]
fn unknown_bool_lists_fold_without_touching_the_sentinel() {
    let i32_max = usize::try_from(i64::from(i32::MAX)).expect("fits usize");
    let wire = Wire::new().list(1, i32_max).byte(0xAA).zigzag(7);
    let mut cursor = cursor_of(&wire);
    let list = cursor.read_list().unwrap();
    assert_eq!(list.element, Kind::BoolTrue);
    assert_eq!(list.count, i32_max);

    let mut skipper = cursor_of(&wire);
    skipper.skip(Kind::List).unwrap();
    assert_eq!(skipper.remaining(), [0xAA_u8, 0x0E].as_slice());
}

#[test]
fn bool_values_accept_only_native_tags() {
    for (payload, expected) in [(0x01, true), (0x00, false), (0x02, false)] {
        let wire = Wire::new().byte(payload);
        let mut cursor = cursor_of(&wire);
        assert_eq!(cursor.read_bool_value().unwrap(), expected);
    }
    let wire = Wire::new().byte(0x03);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_bool_value().unwrap_err());
}

#[test]
fn empty_and_legacy_bool_list_headers_match_native() {
    let empty = Wire::new().byte(0x00);
    let mut cursor = cursor_of(&empty);
    let list = cursor.read_list().unwrap();
    assert_eq!((list.count, list.element), (0, Kind::Byte));

    for header in [0x01, 0x21] {
        let wire = Wire::new().byte(header);
        let mut cursor = cursor_of(&wire);
        let list = cursor.read_list().unwrap();
        assert_eq!(list.element, Kind::BoolTrue);
    }
}

#[test]
fn struct_fields_use_delta_full_ids_and_stop() {
    let wire = Wire::new()
        .field(Kind::I32, 2, 0)
        .zigzag(-5)
        .field(Kind::Binary, -7, 2)
        .binary(b"ok")
        .stop()
        .byte(0xFF);
    let mut cursor = cursor_of(&wire);
    let field = cursor.read_field(0).unwrap().expect("first field");
    assert_eq!((field.id, field.kind), (2, Kind::I32));
    assert_eq!(cursor.read_i32().unwrap(), -5);

    let field = cursor.read_field(2).unwrap().expect("full-id field");
    assert_eq!((field.id, field.kind), (-7, Kind::Binary));
    assert_eq!(cursor.read_string().unwrap(), "ok");

    assert!(cursor.read_field(-7).unwrap().is_none());
    assert_eq!(cursor.remaining(), [0xFF_u8].as_slice());
}

#[test]
fn stop_ends_a_struct_regardless_of_its_delta_nibble() {
    let wire = Wire::new().byte(0x50);
    let mut cursor = cursor_of(&wire);
    assert!(cursor.read_field(0).unwrap().is_none());
    assert_eq!(cursor.position(), 1);
}

#[test]
fn field_ids_reject_delta_and_full_id_overflow() {
    let wire = Wire::new().byte(0x84); // delta 8 from a near-max id overflows i16
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_field(i16::MAX - 7).unwrap_err());

    let wire = Wire::new().byte(0x05).zigzag(i64::from(i16::MAX) + 1);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_field(0).unwrap_err());
}

#[test]
fn scalars_read_with_native_widths() {
    let wire = Wire::new().byte(0xFF);
    let mut cursor = cursor_of(&wire);
    assert_eq!(cursor.read_byte().unwrap(), 0xFF);

    let wire = Wire::new().zigzag(-1);
    let mut cursor = cursor_of(&wire);
    assert_eq!(cursor.read_i16().unwrap(), -1);

    let wire = Wire::new().zigzag(i64::from(i32::MAX));
    let mut cursor = cursor_of(&wire);
    assert_eq!(cursor.read_i32().unwrap(), i32::MAX);

    let wire = Wire::new().zigzag(i64::from(i32::MAX) + 1);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_i32().unwrap_err());

    let wire = Wire::new().zigzag(i64::from(i16::MAX) + 1);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_i16().unwrap_err());

    let wire = Wire::new().zigzag(i64::MIN);
    let mut cursor = cursor_of(&wire);
    assert_eq!(cursor.read_zig_zag().unwrap(), i64::MIN);
}

#[test]
fn binary_reads_borrow_bounded_utf8_ranges() {
    let wire = Wire::new().binary("héllo".as_bytes());
    let mut cursor = cursor_of(&wire);
    assert_eq!(cursor.read_string().unwrap(), "héllo");

    let wire = Wire::new().binary(&[0xFF, 0xFE]);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_string().unwrap_err());

    let wire = Wire::new().vlq(10).repeated(0x01, 3);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_bytes().unwrap_err());

    let wire = Wire::new().vlq(u64::MAX);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_bytes().unwrap_err());
}

#[test]
fn varints_reject_truncation_and_overflow_but_accept_safe_overlong_prefixes() {
    let wire = Wire::new().byte(0x80);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_vlq().unwrap_err());

    let wire = Wire::new().byte(0x80).byte(0x80);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_vlq().unwrap_err());

    let wire = Wire::new().repeated(0x80, 9).byte(0x02);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_vlq().unwrap_err()); // the 10th byte cannot carry bit 64

    let wire = Wire::new().repeated(0x80, 9).byte(0x01);
    let mut cursor = cursor_of(&wire);
    assert_eq!(cursor.read_vlq().unwrap(), 1_u64 << 63);

    let wire = Wire::new().repeated(0x80, 10).byte(0x00);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.read_vlq().unwrap_err()); // no u64 needs more than 10 bytes

    let mut cursor = CompactSlice::new(&[0x81, 0x00], None);
    assert_eq!(cursor.read_vlq().unwrap(), 1); // safe overlong prefix, native accepts
}

#[test]
fn skip_depth_matches_the_native_64_level_limit() {
    let shallow = Wire::new().nested(64);
    let mut cursor = cursor_of(&shallow);
    cursor.skip(Kind::Struct).unwrap();
    assert_eq!(cursor.position(), shallow.slice().len());

    let deep = Wire::new().nested(65);
    let mut cursor = cursor_of(&deep);
    assert_malformed(cursor.skip(Kind::Struct).unwrap_err());
}

#[test]
fn skip_consumes_native_payload_widths() {
    let wire = Wire::new().byte(0xAA);
    let mut cursor = cursor_of(&wire);
    cursor.skip(Kind::BoolTrue).unwrap();
    assert_eq!(cursor.remaining(), [0xAA_u8].as_slice()); // booleans carry no bytes

    let wire = Wire::new().byte(0x7F).byte(0xAA);
    let mut cursor = cursor_of(&wire);
    cursor.skip(Kind::Byte).unwrap();
    assert_eq!(cursor.remaining(), [0xAA_u8].as_slice());

    let wire = Wire::new().repeated(0x80, 12).byte(0x00).byte(0xAA);
    let mut cursor = cursor_of(&wire);
    cursor.skip(Kind::I64).unwrap(); // skipped varints keep the native uncapped grammar
    assert_eq!(cursor.remaining(), [0xAA_u8].as_slice());

    let wire = Wire::new().repeated(0x00, 8).byte(0xAA);
    let mut cursor = cursor_of(&wire);
    cursor.skip(Kind::Double).unwrap();
    assert_eq!(cursor.remaining(), [0xAA_u8].as_slice());

    let wire = Wire::new().vlq(3).repeated(0x01, 3).byte(0xAA);
    let mut cursor = cursor_of(&wire);
    cursor.skip(Kind::Binary).unwrap();
    assert_eq!(cursor.remaining(), [0xAA_u8].as_slice());
}

#[test]
fn skip_rejects_stop_set_and_map_like_native() {
    for kind in [Kind::Stop, Kind::Set, Kind::Map] {
        let mut cursor = CompactSlice::new(&[], None);
        assert_malformed(cursor.skip(kind).unwrap_err());
    }

    let wire = Wire::new().list(10, 1).stop();
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.skip(Kind::List).unwrap_err());
    assert_eq!(cursor.position(), 1); // the header parsed; the element tag failed first
}

#[test]
fn non_bool_list_skips_prove_their_body_before_looping() {
    let wire = Wire::new().list(7, 5).repeated(0x00, 3);
    let mut cursor = cursor_of(&wire);
    assert_malformed(cursor.skip(Kind::List).unwrap_err());
    assert_eq!(cursor.position(), 1);
}

#[test]
fn skip_walks_unknown_structs_and_reads_the_next_known_field() {
    let wire = Wire::new()
        .field(Kind::Struct, 1, 0)
        .field(Kind::Binary, 2, 1)
        .binary(b"meta")
        .field(Kind::List, 3, 2)
        .list(5, 2)
        .zigzag(1)
        .zigzag(2)
        .field(Kind::I64, 4, 3)
        .zigzag(77)
        .stop()
        .field(Kind::I32, 6, 1)
        .zigzag(42);
    let mut cursor = cursor_of(&wire);
    let field = cursor.read_field(0).unwrap().expect("struct field");
    assert_eq!(field.kind, Kind::Struct);
    cursor.skip(Kind::Struct).unwrap();

    let field = cursor
        .read_field(1)
        .unwrap()
        .expect("known field after skip");
    assert_eq!((field.id, field.kind), (6, Kind::I32));
    assert_eq!(cursor.read_i32().unwrap(), 42);
    assert_eq!(cursor.position(), wire.slice().len());
}

#[test]
fn cancellation_is_typed_and_observed_at_entries() {
    let token = CancellationToken::new();
    token.cancel();

    let mut cursor = CompactSlice::new(&[], Some(&token));
    assert_cancelled(cursor.skip(Kind::BoolTrue).unwrap_err());

    let empty_list = Wire::new().byte(0x00);
    let mut cursor = CompactSlice::new(empty_list.slice(), Some(&token));
    assert_cancelled(cursor.skip(Kind::List).unwrap_err());

    let long_binary = Wire::new().vlq(4_096).repeated(0x00, 8);
    let mut cursor = CompactSlice::new(long_binary.slice(), Some(&token));
    assert_cancelled(cursor.skip(Kind::Binary).unwrap_err());

    let mut cursor = CompactSlice::new(&[], Some(&token));
    assert_cancelled(cursor.read_field(0).unwrap_err());
    assert_cancelled(cursor.read_list().unwrap_err());
    assert_cancelled(cursor.allocating_list(8, 8).unwrap_err());

    let uncancelled = CancellationToken::new();
    let mut cursor = CompactSlice::new(&[], Some(&uncancelled));
    cursor.skip(Kind::BoolTrue).unwrap();
}
