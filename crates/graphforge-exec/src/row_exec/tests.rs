use super::*;
use arrow::array::RecordBatch;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use std::sync::Arc;

#[test]
fn optional_join_preserves_left_rows_matches_duplicate_keys_and_null_shapes_misses() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;

    let outer_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, true),
        Field::new("outer", DataType::Utf8, false),
    ]));
    let inner_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, true),
        Field::new("inner", DataType::Int64, false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, true),
        Field::new("outer", DataType::Utf8, false),
        Field::new("inner", DataType::Int64, true),
    ]));
    let outer = RecordBatch::try_new(
        outer_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![Some(1), Some(2), None])),
            Arc::new(StringArray::from(vec!["one", "two", "null"])),
        ],
    )
    .unwrap();
    let inner = RecordBatch::try_new(
        inner_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![Some(1), Some(1), None])),
            Arc::new(Int64Array::from(vec![10, 11, 99])),
        ],
    )
    .unwrap();
    let cfg = OptionalConfig {
        join_keys: vec![(0, 0)],
        inner_keep_idx: vec![1],
        out_schema,
        outer_schema,
        inner_schema,
    };

    let joined = optional_join(&cfg, &[outer], &[inner]).unwrap();
    assert_eq!(
        joined.num_rows(),
        4,
        "duplicate matches must duplicate the left row"
    );
    let keys = joined
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(
        keys.iter().collect::<Vec<_>>(),
        vec![Some(1), Some(1), Some(2), None]
    );
    let values = joined
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(10), Some(11), None, None]
    );
}

#[test]
fn optional_join_handles_cartesian_empty_uuid_and_invalid_key_contracts() {
    use arrow::array::{FixedSizeBinaryArray, Int64Array, StringArray};
    use arrow::datatypes::Field;

    let outer_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let inner_schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let outer = RecordBatch::try_new(
        outer_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let cfg = OptionalConfig {
        join_keys: vec![],
        inner_keep_idx: vec![0],
        out_schema: out_schema.clone(),
        outer_schema: outer_schema.clone(),
        inner_schema: inner_schema.clone(),
    };
    let empty = optional_join(&cfg, &[outer.clone()], &[]).unwrap();
    assert_eq!(empty.num_rows(), 2);
    assert!(empty.column(1).is_null(0) && empty.column(1).is_null(1));

    let inner = RecordBatch::try_new(
        inner_schema,
        vec![Arc::new(StringArray::from(vec!["a", "b"]))],
    )
    .unwrap();
    let product = optional_join(&cfg, &[outer], &[inner]).unwrap();
    assert_eq!(product.num_rows(), 4);

    let uuid_schema = Arc::new(Schema::new(vec![Field::new(
        "uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]));
    let uuids = FixedSizeBinaryArray::try_from_iter([&[1_u8; 16][..]].into_iter()).unwrap();
    let uuid_batch = RecordBatch::try_new(uuid_schema.clone(), vec![Arc::new(uuids)]).unwrap();
    let uuid_cfg = OptionalConfig {
        join_keys: vec![(0, 0)],
        inner_keep_idx: vec![],
        out_schema: uuid_schema.clone(),
        outer_schema: uuid_schema.clone(),
        inner_schema: uuid_schema,
    };
    assert_eq!(
        optional_join(&uuid_cfg, &[uuid_batch.clone()], &[uuid_batch])
            .unwrap()
            .num_rows(),
        1
    );

    let bad_key_cfg = OptionalConfig {
        join_keys: vec![(0, 0)],
        inner_keep_idx: vec![],
        out_schema: outer_schema.clone(),
        outer_schema: outer_schema.clone(),
        inner_schema: outer_schema.clone(),
    };
    let ints =
        RecordBatch::try_new(outer_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    assert!(
        optional_join(&bad_key_cfg, &[ints.clone()], &[ints])
            .unwrap_err()
            .to_string()
            .contains("unsupported type")
    );

    let bad_keep_cfg = OptionalConfig {
        inner_keep_idx: vec![9],
        ..cfg
    };
    let outer = RecordBatch::try_new(
        bad_keep_cfg.outer_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let inner = RecordBatch::try_new(
        bad_keep_cfg.inner_schema.clone(),
        vec![Arc::new(StringArray::from(vec!["value"]))],
    )
    .unwrap();
    assert!(
        optional_join(&bad_keep_cfg, &[outer], &[inner])
            .unwrap_err()
            .to_string()
            .contains("inner_keep_idx 9 out of range")
    );
}

#[test]
fn wave11_unwind_explode_preserves_order_and_enforces_list_contract() {
    use arrow::array::{Int64Array, ListArray, StringArray};
    use arrow::datatypes::Field;
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::col;

    let list_field = Arc::new(Field::new("item", DataType::Int64, true));
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("items", DataType::List(list_field), true),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("items", input_schema.field(1).data_type().clone(), true),
        Field::new("item", DataType::Int64, true),
    ]));
    let lists = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>([
        Some(vec![Some(3), Some(4)]),
        None,
        Some(vec![]),
        Some(vec![Some(8)]),
    ]);
    let batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(lists),
        ],
    )
    .unwrap();
    let cfg = UnwindConfig {
        list_expr: col("items"),
        input_schema: input_schema.clone(),
        input_dfschema: Arc::new(DFSchema::try_from(input_schema.as_ref().clone()).unwrap()),
        out_schema,
    };
    let exploded = unwind_explode(&cfg, &[batch]).unwrap();
    assert_eq!(exploded.num_rows(), 3);
    assert_eq!(
        exploded
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[3, 4, 8]
    );

    let scalar_schema = Arc::new(Schema::new(vec![Field::new(
        "items",
        DataType::Int64,
        false,
    )]));
    let scalar_cfg = UnwindConfig {
        list_expr: col("items"),
        input_schema: scalar_schema.clone(),
        input_dfschema: Arc::new(DFSchema::try_from(scalar_schema.as_ref().clone()).unwrap()),
        out_schema: Arc::new(Schema::new(vec![
            Field::new("items", DataType::Int64, false),
            Field::new("item", DataType::Int64, true),
        ])),
    };
    let scalar =
        RecordBatch::try_new(scalar_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    assert!(
        unwind_explode(&scalar_cfg, &[scalar])
            .unwrap_err()
            .to_string()
            .contains("must evaluate to a list")
    );

    let entity_cfg = UnwindConfig {
        list_expr: col("items"),
        input_schema: input_schema.clone(),
        input_dfschema: Arc::new(DFSchema::try_from(input_schema.as_ref().clone()).unwrap()),
        out_schema: Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("items", input_schema.field(1).data_type().clone(), true),
            Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("node_id", DataType::UInt64, true),
        ])),
    };
    let primitive_lists =
        ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>([Some(vec![Some(1)])]);
    let entity_input = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(primitive_lists),
        ],
    )
    .unwrap();
    assert!(
        unwind_explode(&entity_cfg, &[entity_input])
            .unwrap_err()
            .to_string()
            .contains("requires a struct element")
    );
}
