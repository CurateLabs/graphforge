use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use graphforge_core::{ApiErrorCode, GfError};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

use crate::CancellationToken;

use super::{NodeKind, SchemaShape};
use crate::import_session::inventory_budget::InventoryBudget;

fn metadata(schema: Schema) -> (tempfile::NamedTempFile, ArrowReaderMetadata) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let writer = ArrowWriter::try_new(file.reopen().unwrap(), Arc::new(schema), None).unwrap();
    writer.close().unwrap();
    let reader_file = file.reopen().unwrap();
    let metadata = ArrowReaderMetadata::load(&reader_file, ArrowReaderOptions::new()).unwrap();
    (file, metadata)
}

fn walk_children(shape: &SchemaShape, first: Option<usize>) -> Vec<usize> {
    let mut result = Vec::new();
    let mut cursor = first;
    while let Some(index) = cursor {
        result.push(index);
        cursor = shape.nodes[index].next_sibling;
    }
    result
}

#[test]
fn maps_nested_list_struct_and_temporal_fields_to_descriptor_levels() {
    let list_field = Arc::new(Field::new("element", DataType::Int32, true));
    let fields = vec![
        Field::new("items", DataType::List(list_field), true),
        Field::new(
            "nested",
            DataType::List(Arc::new(Field::new(
                "inner",
                DataType::List(Arc::new(Field::new("value", DataType::Int32, false))),
                true,
            ))),
            false,
        ),
        Field::new(
            "record",
            DataType::Struct(
                vec![
                    Field::new(
                        "tags",
                        DataType::LargeList(Arc::new(Field::new("tag", DataType::Utf8, false))),
                        false,
                    ),
                    Field::new("count", DataType::Int64, false),
                ]
                .into(),
            ),
            true,
        ),
        Field::new(
            "fixed",
            DataType::FixedSizeList(
                Arc::new(Field::new("coordinate", DataType::Float64, false)),
                2,
            ),
            false,
        ),
        Field::new(
            "when",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ];
    let (_file, metadata) = metadata(Schema::new(fields));
    let mut budget = InventoryBudget::new(1 << 20);
    let shape = SchemaShape::build(&metadata, &mut budget, None).unwrap();

    assert_eq!(shape.leaves.len(), 6);
    assert_eq!(shape.leaves[0].max_definition, 3);
    assert_eq!(shape.leaves[0].max_repetition, 1);
    assert_eq!(shape.leaves[1].max_repetition, 2);
    assert_eq!(shape.leaves[2].max_definition, 2);
    assert_eq!(shape.leaves[3].max_definition, 1);

    let roots = walk_children(&shape, shape.root_children);
    assert_eq!(roots.len(), 5);
    assert_eq!(shape.nodes[roots[0]].kind, NodeKind::List);
    assert_eq!(shape.nodes[roots[1]].kind, NodeKind::List);
    assert_eq!(shape.nodes[roots[3]].kind, NodeKind::FixedSizeList);

    let record = roots[2];
    let record_children = walk_children(&shape, shape.nodes[record].first_child);
    assert_eq!(record_children.len(), 2);
    let tags = record_children[0];
    assert_eq!(shape.nodes[tags].kind, NodeKind::LargeList);
    assert_eq!(shape.nodes[record].owner_leaf, Some(2));

    let retained = budget.live_bytes();
    assert!(retained > 0);
    assert_eq!(shape.inventory_bytes().unwrap(), retained);
    shape.release(&mut budget);
    assert_eq!(budget.live_bytes(), 0);
}

#[test]
fn cancellation_and_admission_failure_release_only_new_inventory() {
    let (_file, metadata) = metadata(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut budget = InventoryBudget::new(1 << 20);
    budget.admit(11, "preexisting fixture charge").unwrap();
    let error = SchemaShape::build(&metadata, &mut budget, Some(&cancelled)).unwrap_err();
    assert!(matches!(
        error,
        GfError::Api {
            code: ApiErrorCode::Cancelled,
            ..
        }
    ));
    assert_eq!(budget.live_bytes(), 11);

    let mut too_small = InventoryBudget::new(11);
    too_small.admit(7, "preexisting fixture charge").unwrap();
    assert!(SchemaShape::build(&metadata, &mut too_small, None).is_err());
    assert_eq!(too_small.live_bytes(), 7);
}
