use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};
use arrow::ipc::convert::{IpcSchemaEncoder, fb_to_schema};
use arrow::ipc::root_as_schema;
use graphforge_core::{GfError, ProjectErrorCode};

use super::preflight;
use crate::CancellationToken;

fn encoded(schema: &Schema) -> Vec<u8> {
    let mut encoder = IpcSchemaEncoder::new();
    encoder.schema_to_fb(schema).finished_data().to_vec()
}

fn encoded_with_dictionaries(schema: &Schema) -> Vec<u8> {
    let mut tracker = arrow::ipc::writer::DictionaryTracker::new(false);
    IpcSchemaEncoder::new()
        .with_dictionary_tracker(&mut tracker)
        .schema_to_fb(schema)
        .finished_data()
        .to_vec()
}

fn read_u16(bytes: &[u8], offset: usize) -> usize {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap()) as usize
}

fn read_u32(bytes: &[u8], offset: usize) -> usize {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize
}

fn field_slot(bytes: &[u8], table: usize, vtable_slot: usize) -> Option<usize> {
    let displacement = i32::from_le_bytes(bytes[table..table + 4].try_into().ok()?);
    let vtable = if displacement >= 0 {
        table.checked_sub(displacement as usize)?
    } else {
        table.checked_add(displacement.unsigned_abs() as usize)?
    };
    let vtable_length = read_u16(bytes, vtable);
    if vtable_slot + 2 > vtable_length {
        return None;
    }
    let offset = read_u16(bytes, vtable + vtable_slot);
    (offset != 0).then_some(table + offset)
}

#[test]
fn ordinary_nested_schema_matches_native_conversion_and_admits() {
    let child = Field::new(
        "leaf",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        true,
    );
    let nested = Field::new("record", DataType::Struct(vec![child].into()), true);
    let list = Field::new(
        "items",
        DataType::List(Field::new("item", DataType::Int32, true).into()),
        true,
    );
    let mut metadata = HashMap::new();
    metadata.insert("origin".to_owned(), "ipc".to_owned());
    let expected = Schema::new_with_metadata(vec![nested, list], metadata);
    let bytes = encoded(&expected);
    let borrowed = root_as_schema(&bytes).unwrap();
    let envelope = preflight(borrowed, u64::MAX, None).unwrap();
    let actual = fb_to_schema(root_as_schema(&bytes).unwrap());

    assert_eq!(actual, expected);
    assert_eq!(envelope.field_occurrences, 4);
    assert_eq!(envelope.metadata_occurrences, 1);
    assert_eq!(envelope.field_name_copy_bytes, 19);
    assert_eq!(envelope.metadata_copy_bytes, 9);
    assert!(envelope.peak_request_bytes >= envelope.retained_request_bytes);
    assert!(
        preflight(
            root_as_schema(&bytes).unwrap(),
            envelope.peak_request_bytes,
            None
        )
        .is_ok()
    );
    assert!(
        preflight(
            root_as_schema(&bytes).unwrap(),
            envelope.peak_request_bytes - 1,
            None
        )
        .is_err()
    );
}

#[test]
fn empty_schema_metadata_and_timezone_accounting_are_exact() {
    let schema = Schema::new(vec![Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    )]);
    let bytes = encoded(&schema);
    let envelope = preflight(root_as_schema(&bytes).unwrap(), u64::MAX, None).unwrap();
    assert_eq!(envelope.timezone_copy_bytes, 0);
    assert_eq!(envelope.field_name_copy_bytes, 4);
    assert_eq!(fb_to_schema(root_as_schema(&bytes).unwrap()), schema);
}

#[test]
fn dictionary_and_union_occurrences_match_native_conversion() {
    let dictionary = Field::new(
        "dict",
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        true,
    );
    let union = Field::new(
        "union",
        DataType::Union(
            UnionFields::try_new(
                [2, 9],
                vec![
                    Field::new("left", DataType::Int32, true),
                    Field::new("right", DataType::Utf8, true),
                ],
            )
            .unwrap(),
            UnionMode::Dense,
        ),
        true,
    );
    let schema = Schema::new(vec![dictionary, union]);
    let bytes = encoded_with_dictionaries(&schema);
    let envelope = preflight(root_as_schema(&bytes).unwrap(), u64::MAX, None).unwrap();
    assert_eq!(envelope.field_occurrences, 4);
    assert_eq!(fb_to_schema(root_as_schema(&bytes).unwrap()), schema);
}

#[test]
fn cancellation_is_observed_before_schema_walk() {
    let bytes = encoded(&Schema::empty());
    let token = CancellationToken::new();
    token.cancel();
    assert!(preflight(root_as_schema(&bytes).unwrap(), u64::MAX, Some(&token)).is_err());
}

#[test]
fn unsupported_integer_width_is_refused_before_native_panic() {
    let schema = Schema::new(vec![Field::new("number", DataType::Int32, true)]);
    let mut bytes = encoded(&schema);
    let schema_table = read_u32(&bytes, 0);
    let fields_field = field_slot(&bytes, schema_table, 6).unwrap();
    let fields_vector = fields_field + read_u32(&bytes, fields_field);
    let vector_slot = fields_vector + 4;
    let field_table = vector_slot + read_u32(&bytes, vector_slot);
    let type_field = field_slot(&bytes, field_table, 10).unwrap();
    let int_table = type_field + read_u32(&bytes, type_field);
    let width_field = field_slot(&bytes, int_table, 4).unwrap();
    bytes[width_field..width_field + 4].copy_from_slice(&7_i32.to_le_bytes());

    let verified = root_as_schema(&bytes).expect("unsupported width remains verifier-valid");
    assert!(preflight(verified, u64::MAX, None).is_err());
}

#[test]
fn nesting_limit_is_applied_to_converted_fields_only() {
    fn schema_at_depth(depth: usize) -> Schema {
        let mut field = Field::new("leaf", DataType::Int8, true);
        for _ in 0..depth {
            field = Field::new("nested", DataType::Struct(vec![field].into()), true);
        }
        Schema::new(vec![field])
    }

    // The IPC verifier also caps nested tables; this valid depth verifies that
    // ordinary nesting remains below the admission walk's fixed bound.
    // The schema root and each nested Field plus its Struct union payload are
    // verifier tables; 30 wrappers stay below FlatBuffers' default depth 64.
    let accepted = encoded(&schema_at_depth(30));
    assert!(preflight(root_as_schema(&accepted).unwrap(), u64::MAX, None).is_ok());
}

#[test]
fn aliased_field_budget_refuses_before_later_malformed_field() {
    let schema = Schema::new(vec![
        Field::new("repeated-name", DataType::Utf8, true),
        Field::new("alias-target", DataType::Utf8, true),
        Field::new("invalid-later", DataType::Int32, true),
    ]);
    let mut bytes = encoded(&schema);
    let schema_table = read_u32(&bytes, 0);
    let fields_field = field_slot(&bytes, schema_table, 6).unwrap();
    let fields_vector = fields_field + read_u32(&bytes, fields_field);
    let first_slot = fields_vector + 4;
    let first_table = first_slot + read_u32(&bytes, first_slot);
    let second_slot = first_slot + 4;
    let alias_offset = first_table.checked_sub(second_slot).unwrap();
    bytes[second_slot..second_slot + 4]
        .copy_from_slice(&u32::try_from(alias_offset).unwrap().to_le_bytes());

    let third_slot = first_slot + 8;
    let third_table = third_slot + read_u32(&bytes, third_slot);
    let type_field = field_slot(&bytes, third_table, 10).unwrap();
    let int_table = type_field + read_u32(&bytes, type_field);
    let width_field = field_slot(&bytes, int_table, 4).unwrap();
    bytes[width_field..width_field + 4].copy_from_slice(&7_i32.to_le_bytes());

    let borrowed = root_as_schema(&bytes).expect("aliased and malformed-semantic tables verify");
    assert!(matches!(
        preflight(root_as_schema(&bytes).unwrap(), u64::MAX, None),
        Err(GfError::Storage(_))
    ));

    // Derive a threshold after the first occurrence but before the second
    // alias. This ensures the budget refusal occurs before the malformed third
    // field can be visited and reported as a storage/schema error.
    let fields = borrowed.fields().unwrap();
    let mut requests = super::Requests::new(u64::MAX);
    super::vector_growth::<Field>(&mut requests, fields.len()).unwrap();
    super::walk_field(
        &mut requests,
        fields.get(0),
        0,
        true,
        borrowed.endianness(),
        None,
    )
    .unwrap();
    let first_peak = requests.envelope.peak_request_bytes;
    super::walk_field(
        &mut requests,
        fields.get(1),
        0,
        true,
        borrowed.endianness(),
        None,
    )
    .unwrap();
    let second_peak = requests.envelope.peak_request_bytes;
    assert!(second_peak > first_peak);
    let capacity = first_peak + (second_peak - first_peak - 1) / 2;
    assert!(capacity >= first_peak && capacity < second_peak);

    assert!(matches!(
        preflight(root_as_schema(&bytes).unwrap(), capacity, None),
        Err(GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        })
    ));
}
