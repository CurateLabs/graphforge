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

fn assert_owner_links(shape: &SchemaShape) {
    for (node_index, node) in shape.nodes.iter().enumerate() {
        if let Some(child) = node.first_child {
            assert_eq!(node.owner_leaf, shape.nodes[child].owner_leaf);
        }
        let Some(owner) = node.owner_leaf else {
            continue;
        };
        assert!(owner < shape.leaves.len());
        let mut cursor = Some(shape.leaves[owner].source_node);
        let mut descends_from_owner = false;
        while let Some(index) = cursor {
            if index == node_index {
                descends_from_owner = true;
                break;
            }
            cursor = shape.nodes[index].parent;
        }
        assert!(
            descends_from_owner,
            "node {node_index} owner {owner} is not a descendant"
        );
    }
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
    assert_owner_links(&shape);

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

/// Infer through the actual public metadata loader, without ARROW:schema hints.
fn physical_metadata(schema: &str) -> (tempfile::NamedTempFile, ArrowReaderMetadata) {
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    let file = tempfile::NamedTempFile::new().unwrap();
    let writer = SerializedFileWriter::new(
        file.reopen().unwrap(),
        Arc::new(parse_message_type(schema).unwrap()),
        Arc::new(WriterProperties::builder().build()),
    )
    .unwrap();
    writer.close().unwrap();
    let metadata =
        ArrowReaderMetadata::load(&file.reopen().unwrap(), ArrowReaderOptions::new()).unwrap();
    (file, metadata)
}

#[test]
fn omitted_map_retains_physical_ordinals_and_visible_owner_indices() {
    // All three cases have a real descriptor for the discarded Map's key.
    // The struct and collapsed-list cases exercise inference beneath containers.
    for schema in [
        "message schema {
            required int32 before;
            optional group hidden (MAP) {
                repeated group key_value {
                    required binary key (UTF8);
                    optional group value {}
                }
            }
            optional group empty {}
            optional int64 after;
        }",
        "message schema {
            optional group record {
                required int32 before;
                optional group hidden (MAP) {
                    repeated group key_value {
                        required binary key (UTF8);
                        optional group value {}
                    }
                }
                optional group empty {}
                optional int64 after;
            }
        }",
        "message schema {
            optional group records (LIST) {
                repeated group list {
                    optional group element {
                        required int32 before;
                        optional group hidden (MAP) {
                            repeated group key_value {
                                required binary key (UTF8);
                                optional group value {}
                            }
                        }
                        optional int64 after;
                    }
                }
            }
        }",
    ] {
        let (_file, metadata) = physical_metadata(schema);
        assert_eq!(metadata.parquet_schema().columns().len(), 3);
        let mut budget = InventoryBudget::new(1 << 20);
        let shape = SchemaShape::build(&metadata, &mut budget, None).unwrap();
        assert_eq!(
            shape
                .leaves
                .iter()
                .map(|leaf| leaf.column_index)
                .collect::<Vec<_>>(),
            [0, 2],
        );
        assert_eq!(shape.leaves.len(), 2);
        for (owner, leaf) in shape.leaves.iter().enumerate() {
            let node = &shape.nodes[leaf.source_node];
            assert_eq!(node.owner_leaf, Some(owner));
            assert_eq!(node.column_index, Some(leaf.column_index));
            let descriptor = &metadata.parquet_schema().columns()[leaf.column_index];
            assert_eq!(leaf.max_definition, descriptor.max_def_level());
            assert_eq!(leaf.max_repetition, descriptor.max_rep_level());
            assert_eq!(leaf.physical_type, descriptor.physical_type());
        }
        assert!(!shape.nodes.iter().any(|node| node.field.name() == "hidden"));
        assert!(!shape.nodes.iter().any(|node| node.field.name() == "empty"));
        assert_owner_links(&shape);
        assert_eq!(shape.inventory_bytes().unwrap(), budget.live_bytes());
        shape.release(&mut budget);
        assert_eq!(budget.live_bytes(), 0);
    }
}

#[test]
fn one_key_map_uses_the_resolved_legacy_list_structure() {
    for (entry, key_repetition, element_is_struct) in [
        ("key_value", "required", false),
        ("array", "required", true),
        ("keys_tuple", "required", true),
        ("key_value", "repeated", false),
    ] {
        let schema = format!(
            "message schema {{
                optional group keys (MAP) {{
                    repeated group {entry} {{ {key_repetition} binary key (UTF8); }}
                }}
            }}"
        );
        let (_file, metadata) = physical_metadata(&schema);
        assert!(matches!(
            metadata.schema().field(0).data_type(),
            DataType::List(_)
        ));
        let mut budget = InventoryBudget::new(1 << 20);
        let shape = SchemaShape::build(&metadata, &mut budget, None).unwrap();
        let root = shape.root_children.unwrap();
        assert_eq!(shape.nodes[root].kind, NodeKind::List);
        let element = shape.nodes[root].first_child.unwrap();
        if element_is_struct {
            assert_eq!(shape.nodes[element].kind, NodeKind::Struct);
        } else if key_repetition == "repeated" {
            assert_eq!(shape.nodes[element].kind, NodeKind::List);
        } else {
            assert_eq!(shape.nodes[element].kind, NodeKind::Primitive);
        }
        assert_eq!(shape.leaves.len(), 1);
        assert_eq!(shape.leaves[0].column_index, 0);
        assert_owner_links(&shape);
        shape.release(&mut budget);
        assert_eq!(budget.live_bytes(), 0);
    }
}

#[test]
fn preserved_list_struct_bypasses_the_repeated_group_map_annotation() {
    let (_file, metadata) = physical_metadata(
        "message schema {
            optional group records (LIST) {
                repeated group array (MAP_KEY_VALUE) {
                    required int32 before;
                    optional int64 after;
                }
            }
        }",
    );
    let mut budget = InventoryBudget::new(1 << 20);
    let shape = SchemaShape::build(&metadata, &mut budget, None).unwrap();
    let root = shape.root_children.unwrap();
    assert_eq!(shape.nodes[root].kind, NodeKind::List);
    let item = shape.nodes[root].first_child.unwrap();
    assert_eq!(shape.nodes[item].kind, NodeKind::Struct);
    assert_eq!(shape.leaves.len(), 2);
    assert_owner_links(&shape);
    shape.release(&mut budget);
    assert_eq!(budget.live_bytes(), 0);
}
