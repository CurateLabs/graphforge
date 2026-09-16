use super::super::date_scalar;
use super::super::tests::{invoke_test_udf, invoke_test_udf_with_return_type, large_list_scalar};
use super::super::value_semantics::{cypher_order, cypher_order_key, cypher_value_eq};
use super::*;
use datafusion::arrow::array::ListArray;

#[test]
fn cypher_list_plus_concats_decoded_list_element() {
    use datafusion::arrow::array::{Array, ArrayRef, ListArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::scalar::ScalarValue as S;
    use std::sync::Arc;

    let list = |items: Vec<S>| S::List(S::new_list(&items, &DataType::Int64, true));
    let left_items = vec![
        list(vec![S::Int64(Some(1))]),
        list(vec![S::Int64(Some(2)), S::Int64(Some(3))]),
        list(vec![S::Int64(Some(4)), S::Int64(Some(5))]),
    ];
    let left = S::List(S::new_list(&left_items, &left_items[0].data_type(), true))
        .to_array()
        .unwrap();

    let right_list = list(vec![S::Int64(Some(8)), S::Int64(Some(9))]);
    let right: ArrayRef = Arc::new(build_het_struct(&[right_list], 1).unwrap());
    let udf = CypherListPlus::new();
    let arg_types = vec![left.data_type().clone(), right.data_type().clone()];
    let return_type = udf.return_type(&arg_types).unwrap();
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(left), ColumnarValue::Array(right)],
        arg_fields: vec![
            Arc::new(Field::new(
                "l",
                DataType::new_list(DataType::Int64, true),
                true,
            )),
            Arc::new(Field::new("r", DataType::Struct(het_fields(1)), true)),
        ],
        number_rows: 1,
        return_field: Arc::new(Field::new("out", return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let out = match udf.invoke_with_args(args).unwrap() {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => s.to_array().unwrap(),
    };
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(!out.is_null(0));
    let values = out.value(0);
    assert_eq!(values.len(), 5);
    assert_eq!(
        decode_het(&ScalarValue::try_from_array(&values, 3).unwrap()),
        Some(S::Int64(Some(8)))
    );
    assert_eq!(
        decode_het(&ScalarValue::try_from_array(&values, 4).unwrap()),
        Some(S::Int64(Some(9)))
    );
}

#[test]
fn tagged_list_element_plus_preserves_dynamic_concat_and_null_rows() {
    use datafusion::arrow::array::{Array, ArrayRef, ListArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::Field;
    use datafusion::scalar::ScalarValue as S;
    use std::sync::Arc;

    let values: ArrayRef = Arc::new(
        build_het_struct(
            &[S::Int64(Some(1)), S::Boolean(Some(true)), S::Int64(Some(2))],
            1,
        )
        .unwrap(),
    );
    let item = Arc::new(Field::new("item", values.data_type().clone(), true));
    let left: ArrayRef = Arc::new(ListArray::new(
        item.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 3, 3, 3])),
        values,
        Some(NullBuffer::from(vec![true, true, true, false])),
    ));
    let nested = S::List(S::new_list(
        &[S::Int64(Some(8)), S::Int64(Some(7))],
        &DataType::Int64,
        true,
    ));
    let right: ArrayRef = Arc::new(
        build_het_struct(&[S::Int64(Some(9)), nested, S::Null, S::Int64(Some(1))], 1).unwrap(),
    );
    let return_type = DataType::List(item);
    let output = invoke_tagged_list_element_plus(&left, &right, &return_type)
        .unwrap()
        .expect("tagged fast path");
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    let decode_row = |row: usize| {
        let values = output.value(row);
        (0..values.len())
            .map(|index| decode_het(&ScalarValue::try_from_array(&values, index).unwrap()).unwrap())
            .collect::<Vec<_>>()
    };

    assert_eq!(
        decode_row(0),
        vec![S::Int64(Some(1)), S::Boolean(Some(true)), S::Int64(Some(9))]
    );
    assert_eq!(
        decode_row(1),
        vec![S::Int64(Some(2)), S::Int64(Some(8)), S::Int64(Some(7))]
    );
    assert_eq!(decode_row(2), vec![S::Null]);
    assert!(output.is_null(3));
}

#[test]
fn het_list_map_roundtrip_and_order() {
    use datafusion::scalar::ScalarValue as S;
    // A plain map is recognised; a typed temporal struct is NOT (#1005).
    let map = const_map_scalar(&[
        ("a".to_owned(), S::Int64(Some(2))),
        ("b".to_owned(), S::Boolean(Some(true))),
    ])
    .expect("map scalar");
    let S::Struct(m) = &map else {
        panic!("map is a struct")
    };
    assert!(is_plain_map_struct(m));
    let S::Struct(d) = date_scalar(Some(0)) else {
        panic!("date is a struct")
    };
    assert!(!is_plain_map_struct(&d));

    // A mixed het list `[1, {a: 2, b: true}]` encodes to a tagged struct whose
    // map element (tag 5) decodes back to a structurally-equal map.
    let scalars = vec![S::Int64(Some(1)), map.clone()];
    assert_eq!(het_depth(&scalars[0]), Some(0));
    assert_eq!(het_depth(&scalars[1]), Some(1));
    let depth = scalars.iter().filter_map(het_depth).max().unwrap();
    let elem = build_het_struct(&scalars, depth).expect("build tagged struct");
    let e0 = ScalarValue::try_from_array(&elem, 0).unwrap();
    assert_eq!(decode_het(&e0), Some(S::Int64(Some(1))));
    let e1 = ScalarValue::try_from_array(&elem, 1).unwrap();
    let decoded = decode_het(&e1).expect("decode map element");
    assert_eq!(cypher_value_eq(&decoded, &map), Some(true));

    // Orderability (ADR 0011 slice 5): maps rank above numbers; two maps order
    // by their (sorted) entries.
    let m1 = const_map_scalar(&[("a".to_owned(), S::Int64(Some(1)))]).unwrap();
    let m2 = const_map_scalar(&[("a".to_owned(), S::Int64(Some(2)))]).unwrap();
    assert_eq!(cypher_order(&m1, &m2), std::cmp::Ordering::Less);
    assert_eq!(
        cypher_order(&S::Int64(Some(99)), &m1),
        std::cmp::Ordering::Less
    );
}

#[test]
fn empty_map_scalar_has_one_row_and_no_fields() {
    use datafusion::arrow::array::Array;
    use datafusion::scalar::ScalarValue as S;

    let map = const_map_scalar(&[]).expect("empty map scalar");
    let S::Struct(values) = map else {
        panic!("empty map is a struct")
    };
    assert_eq!(values.len(), 1);
    assert_eq!(values.num_columns(), 0);
    assert!(is_plain_map_struct(&values));

    let scalars = vec![S::Int64(Some(1)), S::Struct(values.clone())];
    let encoded = build_het_struct(&scalars, 1).expect("encode empty map");
    let tagged = S::try_from_array(&encoded, 1).expect("tagged empty map");
    let S::Struct(decoded) = decode_het(&tagged).expect("decode empty map") else {
        panic!("decoded empty map is a struct")
    };
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded.num_columns(), 0);
}

#[test]
fn dictionary_scalar_is_normalized_for_heterogeneous_encoding() {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::scalar::ScalarValue as S;

    let dictionary = S::Dictionary(
        Box::new(DataType::Int32),
        Box::new(S::Utf8(Some("value".to_owned()))),
    );
    let scalars = vec![S::Int64(Some(1)), dictionary];
    assert_eq!(het_depth(&scalars[1]), Some(0));

    let encoded = build_het_struct(&scalars, 0).expect("encode dictionary scalar");
    let tagged = S::try_from_array(&encoded, 1).expect("tagged dictionary scalar");
    assert_eq!(decode_het(&tagged), Some(S::Utf8(Some("value".to_owned()))));
}

#[test]
fn dynamic_heterogeneous_list_preserves_graph_value_payloads() {
    use datafusion::arrow::array::{Int64Array, StructArray};
    use datafusion::arrow::datatypes::{Field, Fields};
    use datafusion::config::ConfigOptions;

    let node_array = StructArray::new(
        Fields::from(vec![Field::new("node_uuid", DataType::Int64, false)]),
        vec![Arc::new(Int64Array::from(vec![7]))],
        None,
    );
    let node = ScalarValue::Struct(Arc::new(node_array));
    let number = ScalarValue::Int64(Some(42));
    let arg_types = vec![node.data_type(), number.data_type()];
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(node.clone()),
            ColumnarValue::Scalar(number.clone()),
        ],
        arg_fields: arg_types
            .iter()
            .enumerate()
            .map(|(i, ty)| Arc::new(Field::new(format!("arg_{i}"), ty.clone(), true)))
            .collect(),
        number_rows: 1,
        return_field: Arc::new(Field::new("out", dynamic_het_type(&arg_types), false)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let out = match CypherDynamicHetList::new()
        .invoke_with_args(args)
        .expect("dynamic heterogeneous list")
    {
        ColumnarValue::Array(array) => array,
        ColumnarValue::Scalar(value) => value.to_array().expect("scalar list"),
    };
    let list = out.as_any().downcast_ref::<ListArray>().expect("List");
    let values = list.value(0);
    let first = ScalarValue::try_from_array(&values, 0).expect("node element");
    let second = ScalarValue::try_from_array(&values, 1).expect("number element");
    assert_eq!(decode_het(&first), Some(node));
    assert_eq!(decode_het(&second), Some(number));
    assert!(cypher_order_key(&first).starts_with("20:node"));
    assert!(cypher_order_key(&second).starts_with("80:num"));
}

#[test]
fn cypher_list_plus_runtime_covers_each_operand_shape() {
    use datafusion::arrow::array::{Array, ListArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    let list = |values: &[i64]| {
        ScalarValue::List(ScalarValue::new_list(
            &values
                .iter()
                .copied()
                .map(|value| ScalarValue::Int64(Some(value)))
                .collect::<Vec<_>>(),
            &DataType::Int64,
            true,
        ))
    };
    let invoke = |left: ScalarValue, right: ScalarValue| {
        let udf = CypherListPlus::new();
        let types = [left.data_type(), right.data_type()];
        let return_type = udf.return_type(&types).unwrap();
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)],
            arg_fields: types
                .iter()
                .enumerate()
                .map(|(index, data_type)| {
                    Arc::new(Field::new(format!("arg_{index}"), data_type.clone(), true))
                })
                .collect(),
            number_rows: 1,
            return_field: Arc::new(Field::new("out", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    };
    let values = |result: ColumnarValue| {
        let array = match result {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
        };
        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.value(0);
        (0..values.len())
            .map(|row| {
                let value = ScalarValue::try_from_array(&values, row).unwrap();
                decode_het(&value).unwrap_or(value)
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(
        values(invoke(list(&[1, 2]), list(&[3, 4])).unwrap()),
        [1, 2, 3, 4]
            .map(|value| ScalarValue::Int64(Some(value)))
            .to_vec()
    );
    assert_eq!(
        values(invoke(list(&[1, 2]), ScalarValue::Int64(Some(3))).unwrap()),
        [1, 2, 3]
            .map(|value| ScalarValue::Int64(Some(value)))
            .to_vec()
    );
    assert_eq!(
        values(invoke(ScalarValue::Int64(Some(1)), list(&[2, 3])).unwrap()),
        [1, 2, 3]
            .map(|value| ScalarValue::Int64(Some(value)))
            .to_vec()
    );
    let error = invoke(ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Execution error: list + requires at least one list operand"
    );
}

#[test]
fn cypher_list_plus_executes_large_list_operands_and_null_rows() {
    use datafusion::arrow::array::{Array, ArrayRef, Int64Array, LargeListArray, ListArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::Field;

    let large = |values: &[i64], valid: bool| {
        let values: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        ScalarValue::LargeList(Arc::new(LargeListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![
                0_i64,
                i64::try_from(values.len()).unwrap(),
            ])),
            values,
            Some(NullBuffer::from(vec![valid])),
        )))
    };
    let values = |array: ArrayRef| {
        let list = array.as_any().downcast_ref::<ListArray>().expect("List");
        if list.is_null(0) {
            return None;
        }
        let values = list.value(0);
        Some(
            (0..values.len())
                .map(|row| {
                    let value = ScalarValue::try_from_array(&values, row).unwrap();
                    decode_het(&value).unwrap_or(value)
                })
                .collect::<Vec<_>>(),
        )
    };

    assert_eq!(
        values(
            invoke_test_udf(
                &CypherListPlus::new(),
                vec![large(&[1, 2], true), ScalarValue::Int64(Some(3))],
            )
            .unwrap()
        ),
        Some(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(3)),
        ])
    );
    assert_eq!(
        values(
            invoke_test_udf(
                &CypherListPlus::new(),
                vec![ScalarValue::Int64(Some(0)), large(&[1, 2], true)],
            )
            .unwrap()
        ),
        Some(vec![
            ScalarValue::Int64(Some(0)),
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(2)),
        ])
    );
    assert_eq!(
        values(
            invoke_test_udf(
                &CypherListPlus::new(),
                vec![large(&[1], false), large(&[2], true)],
            )
            .unwrap()
        ),
        None
    );
}

#[test]
fn exact_zero_dynamic_heterogeneous_list_builds_row_aligned_variants() {
    use datafusion::arrow::array::{Array, ListArray, StructArray};

    let output = invoke_test_udf(
        &CypherDynamicHetList::new(),
        vec![
            ScalarValue::Int64(Some(7)),
            ScalarValue::Utf8(Some("seven".into())),
            ScalarValue::Boolean(None),
        ],
    )
    .unwrap();
    let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(lists.len(), 1);
    assert_eq!(lists.value_length(0), 3);
    let values = lists.value(0);
    let variants = values.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(variants.len(), 3);
    assert!(!variants.is_null(0));
    assert!(!variants.is_null(1));
    assert!(variants.is_null(2));
}

#[test]
fn exact_zero_list_plus_handles_each_operand_shape_and_null_propagation() {
    use datafusion::arrow::array::{Array, ArrayRef, Int64Array, ListArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};

    let list = |values: &[i64]| {
        ScalarValue::List(ScalarValue::new_list(
            &values
                .iter()
                .copied()
                .map(|value| ScalarValue::Int64(Some(value)))
                .collect::<Vec<_>>(),
            &DataType::Int64,
            true,
        ))
    };
    for (left, right, expected) in [
        (list(&[1, 2]), list(&[3, 4]), vec![1, 2, 3, 4]),
        (list(&[1, 2]), ScalarValue::Int64(Some(3)), vec![1, 2, 3]),
        (ScalarValue::Int64(Some(1)), list(&[2, 3]), vec![1, 2, 3]),
    ] {
        let output = invoke_test_udf(&CypherListPlus::new(), vec![left, right]).unwrap();
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        let values = lists.value(0);
        assert_eq!(
            (0..values.len())
                .map(|row| { unwrap_het(ScalarValue::try_from_array(&values, row).unwrap()) })
                .collect::<Vec<_>>(),
            expected
                .into_iter()
                .map(|value| ScalarValue::Int64(Some(value)))
                .collect::<Vec<_>>()
        );
    }

    let null_list = ScalarValue::List(Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 0])),
        Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
        Some(NullBuffer::from(vec![false])),
    )));
    let output = invoke_test_udf(
        &CypherListPlus::new(),
        vec![null_list, ScalarValue::Int64(Some(1))],
    )
    .unwrap();
    assert!(output.is_null(0));

    assert!(
        invoke_test_udf(
            &CypherListPlus::new(),
            vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))],
        )
        .unwrap_err()
        .to_string()
        .contains("at least one list operand")
    );
}

#[test]
fn exact_zero_udf_return_shape_guards_report_contract_errors() {
    let too_wide = (0..128)
        .map(|value| ScalarValue::Int64(Some(value)))
        .collect::<Vec<_>>();
    assert!(
        invoke_test_udf(&CypherDynamicHetList::new(), too_wide)
            .unwrap_err()
            .to_string()
            .contains("exceeds 127 elements")
    );
    assert!(
        invoke_test_udf_with_return_type(
            &CypherDynamicHetList::new(),
            vec![ScalarValue::Int64(Some(1))],
            DataType::Int64,
        )
        .unwrap_err()
        .to_string()
        .contains("non-list return type")
    );
    assert!(
        invoke_test_udf_with_return_type(
            &CypherDynamicHetList::new(),
            vec![ScalarValue::Int64(Some(1))],
            DataType::new_list(DataType::Int64, true),
        )
        .unwrap_err()
        .to_string()
        .contains("non-struct element type")
    );
    assert!(
        invoke_test_udf_with_return_type(
            &CypherListPlus::new(),
            vec![
                ScalarValue::List(ScalarValue::new_list(
                    &[ScalarValue::Int64(Some(1))],
                    &DataType::Int64,
                    true,
                )),
                ScalarValue::Int64(Some(2)),
            ],
            DataType::Int64,
        )
        .unwrap_err()
        .to_string()
        .contains("return type is not a list")
    );
}

#[test]
fn exact_zero_tagged_append_rejects_incompatible_arrow_shapes() {
    use datafusion::arrow::array::{ArrayRef, Int64Array};

    let scalar: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    assert!(
        invoke_tagged_list_element_plus(&scalar, &scalar, &DataType::Int64)
            .unwrap()
            .is_none()
    );
    let list = ScalarValue::List(ScalarValue::new_list(
        &[ScalarValue::Int64(Some(1))],
        &DataType::Int64,
        true,
    ))
    .to_array_of_size(1)
    .unwrap();
    assert!(
        invoke_tagged_list_element_plus(&list, &scalar, &DataType::Int64)
            .unwrap()
            .is_none()
    );

    let map = const_map_scalar(&[("value".into(), ScalarValue::Int64(Some(1)))])
        .unwrap()
        .to_array_of_size(1)
        .unwrap();
    assert!(
        invoke_tagged_list_element_plus(&list, &map, &DataType::Int64)
            .unwrap()
            .is_none()
    );
    assert!(
        invoke_tagged_list_element_plus(
            &list,
            &map,
            &DataType::new_list(map.data_type().clone(), true),
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn exact_zero_heterogeneous_depth_and_builder_type_matrix_is_total() {
    use datafusion::arrow::datatypes::{Field, Fields};

    let primitives = [
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
    ];
    for data_type in primitives {
        assert_eq!(het_depth_for_data_type(&data_type), Some(0));
    }
    assert_eq!(
        het_depth_for_data_type(&DataType::new_list(
            DataType::new_list(DataType::Int64, true),
            true,
        )),
        Some(2)
    );
    assert_eq!(het_depth_for_data_type(&DataType::Binary), None);

    let map_type = DataType::Struct(Fields::from(vec![Field::new(
        "value",
        DataType::new_list(DataType::Int64, true),
        true,
    )]));
    assert_eq!(het_depth_for_data_type(&map_type), Some(2));
    assert!(build_het_struct(&[ScalarValue::Binary(Some(vec![1]))], 0).is_none());

    let nested_list = ScalarValue::List(ScalarValue::new_list(
        &[ScalarValue::Int64(Some(1))],
        &DataType::Int64,
        true,
    ));
    assert!(build_het_struct(std::slice::from_ref(&nested_list), 0).is_none());
    assert!(build_het_struct(&[const_map_scalar(&[]).unwrap()], 0).is_none());
    let built = build_het_struct(
        &[
            ScalarValue::Int64(Some(1)),
            ScalarValue::Float64(Some(2.0)),
            ScalarValue::LargeUtf8(Some("three".into())),
            ScalarValue::Boolean(Some(true)),
            nested_list,
            const_map_scalar(&[("k".into(), ScalarValue::Int64(Some(4)))]).unwrap(),
            ScalarValue::Null,
        ],
        1,
    )
    .unwrap();
    assert_eq!(built.len(), 7);
    assert!(built.is_null(6));
}

#[test]
fn exact_zero_map_union_rejects_non_maps_and_conflicting_key_types() {
    assert!(all_map_union_list(&[ScalarValue::Int64(Some(1))]).is_none());
    let int_map = const_map_scalar(&[("key".into(), ScalarValue::Int64(Some(1)))]).unwrap();
    let text_map =
        const_map_scalar(&[("key".into(), ScalarValue::Utf8(Some("one".into())))]).unwrap();
    assert!(all_map_union_list(&[int_map, text_map]).is_none());

    let left = const_map_scalar(&[("left".into(), ScalarValue::Int64(Some(1)))]).unwrap();
    let right = const_map_scalar(&[("right".into(), ScalarValue::Utf8(Some("r".into())))]).unwrap();
    let union = all_map_union_list(&[left, ScalarValue::Null, right]).unwrap();
    let DfExpr::Literal(ScalarValue::List(values), None) = union else {
        panic!("map union must const-fold to a list")
    };
    assert_eq!(values.value(0).len(), 3);
    assert!(values.value(0).is_null(1));
}

#[test]
fn exact_zero_heterogeneous_promotion_preserves_struct_and_list_validity() {
    use datafusion::arrow::array::{Array, Float64Array, ListArray, StructArray};
    use datafusion::arrow::datatypes::{Field, Fields};

    let source_map = const_map_scalar(&[("present".into(), ScalarValue::Int64(Some(7)))])
        .unwrap()
        .to_array_of_size(1)
        .unwrap();
    assert!(Arc::ptr_eq(
        &source_map,
        &promote_het_array(&source_map, source_map.data_type()).unwrap()
    ));
    let target = DataType::Struct(Fields::from(vec![
        Field::new("present", DataType::Float64, true),
        Field::new("missing", DataType::Utf8, true),
    ]));
    let promoted = promote_het_array(&source_map, &target).unwrap();
    let promoted = promoted.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(promoted.num_columns(), 2);
    assert_eq!(
        promoted
            .column_by_name("present")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        7.0
    );
    assert!(promoted.column_by_name("missing").unwrap().is_null(0));

    let source_list = ScalarValue::List(ScalarValue::new_list(
        &[ScalarValue::Int64(Some(1)), ScalarValue::Int64(None)],
        &DataType::Int64,
        true,
    ))
    .to_array_of_size(1)
    .unwrap();
    let target_list = DataType::new_list(DataType::Float64, true);
    let promoted = promote_het_array(&source_list, &target_list).unwrap();
    let promoted = promoted.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(promoted.value_length(0), 2);
    assert!(promoted.value(0).is_null(1));
}

#[test]
fn large_list_elements_and_variant_order_are_preserved() {
    use datafusion::arrow::datatypes::{Field, Fields};
    assert_eq!(
        scalar_list_elements(&large_list_scalar(vec![1, 2], true)).unwrap(),
        Some(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(2))
        ])
    );
    assert_eq!(
        scalar_list_elements(&large_list_scalar(vec![], false)).unwrap(),
        None
    );

    let legacy = DataType::Struct(Fields::from(vec![
        Field::new(graphforge_value::heterogeneous::TAG, DataType::Int8, false),
        Field::new(graphforge_value::heterogeneous::INT, DataType::Int64, true),
        Field::new(
            graphforge_value::heterogeneous::FLOAT,
            DataType::Float64,
            true,
        ),
        Field::new(graphforge_value::heterogeneous::STR, DataType::Utf8, true),
        Field::new(
            graphforge_value::heterogeneous::BOOL,
            DataType::Boolean,
            true,
        ),
    ]));
    let return_type = list_plus_return_type(&[
        DataType::new_list(legacy, true),
        DataType::new_list(DataType::Int64, true),
    ]);
    let DataType::List(item) = return_type else {
        panic!("list return")
    };
    let DataType::Struct(variants) = item.data_type() else {
        panic!("variant struct")
    };
    assert!(
        variants
            .iter()
            .any(|field| field.data_type() == &DataType::Int64)
    );
    assert!(
        variants
            .iter()
            .any(|field| field.data_type() == &DataType::Utf8)
    );
}
