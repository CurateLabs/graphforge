use super::super::parquet_compact::CompactSlice;
use super::{FooterCountFacts, payload, preflight, sorting_column};
use crate::CancellationToken;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};

#[derive(Default)]
struct Wire(Vec<u8>);

#[allow(clippy::cast_possible_truncation)] // compact varints intentionally emit their low bits
impl Wire {
    fn byte(&mut self, byte: u8) {
        self.0.push(byte);
    }

    fn vlq(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.byte(((value & 0x7f) as u8) | 0x80);
            value >>= 7;
        }
        self.byte(value as u8);
    }

    fn signed(&mut self, value: i64) {
        let encoded = value.wrapping_shl(1) ^ (value >> 63);
        self.vlq(u64::from_ne_bytes(encoded.to_ne_bytes()));
    }

    fn text(&mut self, value: &[u8]) {
        self.vlq(u64::try_from(value.len()).unwrap());
        self.0.extend_from_slice(value);
    }
}

fn empty_footer() -> Vec<u8> {
    let mut wire = Wire::default();
    wire.byte(0x15); // FileMetaData.version: i32
    wire.signed(1);
    wire.byte(0x19); // schema: list<struct>, field 2
    wire.byte(0x2c); // two schema elements
    wire.byte(0x48); // root SchemaElement.name, field 4
    wire.text(b"schema");
    wire.byte(0x15); // num_children, field 5
    wire.signed(1);
    wire.byte(0);
    wire.byte(0x15); // leaf physical type INT32, field 1
    wire.signed(1);
    wire.byte(0x25); // leaf repetition REQUIRED, field 3
    wire.signed(0);
    wire.byte(0x18); // leaf name, field 4
    wire.text(b"x");
    wire.byte(0);
    wire.byte(0x16); // num_rows: i64, field 3
    wire.signed(0);
    wire.byte(0x19); // row_groups: list<struct>, field 4
    wire.byte(0);
    wire.byte(0);
    wire.0
}

fn assert_resource_limit(error: GfError) {
    assert!(matches!(
        error,
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
}

fn assert_cancelled(error: GfError) {
    assert!(matches!(
        error,
        GfError::Api {
            code: ApiErrorCode::Cancelled,
            ..
        }
    ));
}

fn assert_storage_error(error: GfError) {
    assert!(matches!(error, GfError::Storage(_)));
}

#[test]
fn ordinary_empty_native_footer_returns_zero_facts() {
    let facts = preflight(&empty_footer(), 1 << 20, None).unwrap();
    assert_eq!(facts.schema_elements, 2);
    assert_eq!(facts.row_groups, 0);
    assert_eq!(facts.owned_payload_bytes, 0);
    assert_eq!(facts.owned_payloads, 0);
    assert!(facts.vector_bytes >= facts.schema_elements * 1);
}

#[test]
fn native_sorting_column_embedded_booleans_are_consumed_without_payload_bytes() {
    let body = [0x15, 0x00, 0x11, 0x12, 0x00];
    sorting_column(&mut CompactSlice::new(&body, None)).unwrap();

    // Struct bool fields require the native embedded boolean tags.
    let malformed = [0x15, 0x00, 0x15, 0x00, 0x12, 0x00];
    assert_storage_error(sorting_column(&mut CompactSlice::new(&malformed, None)).unwrap_err());
}

#[test]
fn huge_schema_and_row_group_list_claims_refuse_before_the_body() {
    let mut schema = Wire::default();
    schema.byte(0x15);
    schema.signed(1);
    schema.byte(0x19);
    schema.byte(0xfc);
    schema.vlq(u64::try_from(i32::MAX).unwrap());
    schema.byte(0);
    assert_resource_limit(preflight(&schema.0, 64, None).unwrap_err());

    let mut row_groups = empty_footer();
    row_groups.truncate(row_groups.len() - 2); // retain row_groups field header
    row_groups.push(0xfc);
    let mut encoded_count = Wire::default();
    encoded_count.vlq(u64::try_from(i32::MAX).unwrap());
    row_groups.extend(encoded_count.0);
    row_groups.push(0);
    assert_resource_limit(preflight(&row_groups, 1_000_000, None).unwrap_err());

    let mut key_values = empty_footer();
    key_values.pop();
    key_values.push(0x19); // key_value_metadata, field 5
    key_values.push(0xfc);
    let mut encoded_count = Wire::default();
    encoded_count.vlq(u64::try_from(i32::MAX).unwrap());
    key_values.extend(encoded_count.0);
    key_values.push(0);
    assert_resource_limit(preflight(&key_values, 1_000_000, None).unwrap_err());
}

#[test]
fn impossible_root_child_count_is_refused_before_native_schema_conversion() {
    let mut wire = empty_footer();
    let name_start = wire
        .windows(b"schema".len())
        .position(|window| window == b"schema")
        .unwrap();
    let old_value_start = name_start + b"schema".len() + 1; // field 5 header then zigzag(1)
    let mut encoded = Wire::default();
    encoded.signed(i64::from(i32::MAX));
    wire.splice(old_value_start..old_value_start + 1, encoded.0);
    assert_storage_error(preflight(&wire, 1 << 20, None).unwrap_err());
}

#[test]
fn duplicate_known_key_value_fields_are_accounted_as_coexisting_requests() {
    let mut wire = empty_footer();
    // Replace root STOP with two field-5 key_value_metadata lists.
    wire.pop();
    for occurrence in 0..2 {
        if occurrence == 0 {
            wire.push(0x19);
        } else {
            wire.push(0x09);
            wire.push(10);
        }
        // field 5, list
        wire.push(0x1c); // one struct
        wire.push(0x18); // KeyValue.key, binary
        wire.push(1);
        wire.push(b'k');
        wire.push(0);
    }
    wire.push(0);
    let facts = preflight(&wire, 1 << 20, None).unwrap();
    let baseline = preflight(&empty_footer(), 1 << 20, None).unwrap();
    assert_eq!(
        facts.vector_bytes - baseline.vector_bytes,
        u64::try_from(2 * std::mem::size_of::<parquet::file::metadata::KeyValue>()).unwrap()
    );
    assert_eq!(facts.owned_payload_bytes, 2);
    assert_eq!(facts.owned_payloads, 2);
}

#[test]
fn skipped_boolean_list_is_constant_time_and_does_not_add_allocations() {
    let mut wire = empty_footer();
    wire.pop();
    wire.push(0xd9); // unknown field 17, list
    wire.push(0xf1); // huge bool list, zero-byte elements
    wire.push(0xff);
    wire.push(0xff);
    wire.push(0xff);
    wire.push(0xff);
    wire.push(0x07);
    wire.push(0);
    let facts = preflight(&wire, 1 << 20, None).unwrap();
    let baseline = preflight(&empty_footer(), 1 << 20, None).unwrap();
    assert_eq!(facts, baseline);
}

#[test]
fn cancellation_is_observed_before_footer_parsing() {
    let token = CancellationToken::new();
    token.cancel();
    assert_cancelled(preflight(&[], 0, Some(&token)).unwrap_err());
}

#[test]
fn checked_totals_refuse_when_the_budget_cannot_hold_the_next_list() {
    assert_resource_limit(preflight(&empty_footer(), 0, None).unwrap_err());
}

#[test]
fn facts_remain_plain_scalar_values() {
    let facts = FooterCountFacts::default();
    assert_eq!(facts.vector_bytes, 0);
    assert_eq!(facts.vector_peak_bytes, 0);
    assert_eq!(facts.owned_payload_bytes, 0);
}

#[test]
fn short_binary_statistics_charge_the_bytes_shared_control_box() {
    let mut facts = FooterCountFacts::default();
    payload(&mut facts, 1, true, 1 << 20).unwrap();
    assert_eq!(facts.owned_payload_bytes, 8);
    assert_eq!(facts.bytes_control_bytes, 24);
}

#[test]
fn each_owned_allocation_guard_rejects_a_request_over_the_budget() {
    let mut vector = FooterCountFacts::default();
    assert_resource_limit(vector.account_vector(1, 0).unwrap_err());

    let mut payload = FooterCountFacts::default();
    assert_resource_limit(payload.account_payload(9, 8).unwrap_err());

    let mut fixed = FooterCountFacts::default();
    assert_resource_limit(fixed.account_fixed(2, 1).unwrap_err());

    let mut control = FooterCountFacts::default();
    assert_resource_limit(control.account_bytes_control(2, 1).unwrap_err());
}

#[cfg(target_pointer_width = "64")]
#[test]
fn checked_scalar_totals_refuse_overflow() {
    let mut facts = FooterCountFacts::default();
    facts.account_vector(usize::MAX, u64::MAX).unwrap();
    assert_resource_limit(facts.account_vector(1, u64::MAX).unwrap_err());
}
