use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, GenericStringBuilder, Int64Array, ListBuilder, StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;

use super::{exact, has_slack};

fn grown_strings() -> ArrayRef {
    // Appending one value at a time grows the values buffer by doubling.
    let mut builder = GenericStringBuilder::<i32>::new();
    for row in 0..1_000 {
        builder.append_value("v".repeat(100 + row % 7));
    }
    Arc::new(builder.finish())
}

#[test]
fn a_grown_byte_array_is_copied_to_exactly_its_values() {
    let grown = grown_strings();
    assert!(
        has_slack(&grown.to_data()),
        "the builder left slack to remove"
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from((0..1_000).collect::<Vec<i64>>())),
            Arc::clone(&grown),
        ],
    )
    .unwrap();
    let exact = exact(batch.clone()).unwrap();
    assert_eq!(exact.column(1).as_ref(), grown.as_ref());
    assert!(!has_slack(&exact.column(1).to_data()));
    // A column without slack is shared, not copied.
    assert!(Arc::ptr_eq(exact.column(0), batch.column(0)));
}

#[test]
fn nested_children_are_copied_exactly_too() {
    let mut lists = ListBuilder::new(StringBuilder::new());
    for row in 0..300 {
        for child in 0..(row % 5) {
            lists.values().append_value(format!("{row}-{child}"));
        }
        lists.append(row % 11 != 0);
    }
    let lists: ArrayRef = Arc::new(lists.finish());
    assert!(has_slack(&lists.to_data()));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "items",
            lists.data_type().clone(),
            true,
        )])),
        vec![Arc::clone(&lists)],
    )
    .unwrap();
    let exact = exact(batch).unwrap();
    assert_eq!(exact.column(0).as_ref(), lists.as_ref());
    assert!(!has_slack(&exact.column(0).to_data()));
}

#[test]
fn a_struct_column_is_rebuilt_from_exact_children() {
    let text = grown_strings();
    let numbers: ArrayRef = Arc::new(Int64Array::from((0..1_000).collect::<Vec<i64>>()));
    let fields = Fields::from(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new("n", DataType::Int64, false),
    ]);
    let structs: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![Arc::clone(&text), numbers],
        None,
    ));
    assert!(has_slack(&structs.to_data()));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "pair",
            DataType::Struct(fields),
            false,
        )])),
        vec![Arc::clone(&structs)],
    )
    .unwrap();
    let exact = exact(batch).unwrap();
    assert_eq!(exact.column(0).as_ref(), structs.as_ref());
    assert!(!has_slack(&exact.column(0).to_data()));
}
