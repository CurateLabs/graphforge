use super::super::tests::{invoke_test_udf, large_list_scalar};
use super::super::{build_het_struct, cypher_float_string, het_depth};
use super::*;
use datafusion::arrow::array::Array;

// -----------------------------------------------------------------------
// cypher_size UDF — runtime type dispatch (#743)
// -----------------------------------------------------------------------

#[test]
fn cypher_size_counts_list_elements() {
    use datafusion::arrow::array::{Int64Array, ListArray};
    use datafusion::arrow::datatypes::Int32Type;

    // Two list rows: [10,20,30] and [].
    let arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(10), Some(20), Some(30)]),
        Some(vec![]),
    ]);
    let out = invoke_cypher_size(std::sync::Arc::new(arr));
    let counts = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(counts.value(0), 3);
    assert_eq!(counts.value(1), 0);
}

#[test]
fn cypher_size_counts_string_chars() {
    use datafusion::arrow::array::{Array, Int64Array, StringArray};

    let arr = StringArray::from(vec![Some("abc"), Some(""), None]);
    let out = invoke_cypher_size(std::sync::Arc::new(arr));
    let counts = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(counts.value(0), 3);
    assert_eq!(counts.value(1), 0);
    assert!(counts.is_null(2), "null string → null size");
}

#[test]
fn cypher_size_counts_het_tagged_elements() {
    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::scalar::ScalarValue as S;

    // The four element shapes a quantifier loop variable can take over
    // `[[1, 2, 3], 'ab', true, null]`: a list payload counts its elements,
    // a string its bytes, and a non-list/string or null element is null.
    let scalars = vec![
        S::List(S::new_list(
            &[S::Int64(Some(1)), S::Int64(Some(2)), S::Int64(Some(3))],
            &DataType::Int64,
            true,
        )),
        S::Utf8(Some("ab".to_owned())),
        S::Boolean(Some(true)),
        S::Null,
    ];
    let depth = scalars.iter().filter_map(het_depth).max().unwrap();
    let elems = build_het_struct(&scalars, depth).expect("build tagged struct");
    let out = invoke_cypher_size(std::sync::Arc::new(elems));
    let counts = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(counts.value(0), 3, "tag-4 list element → element count");
    assert_eq!(counts.value(1), 2, "tag-2 string element → char count");
    assert!(counts.is_null(2), "non-list/string element → null");
    assert!(counts.is_null(3), "null element → null");
}

/// Invoke the `cypher_size` UDF over a single-column array and return the
/// result array.
fn invoke_cypher_size(
    array: datafusion::arrow::array::ArrayRef,
) -> datafusion::arrow::array::ArrayRef {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    let n = array.len();
    let field = Arc::new(Field::new("x", array.data_type().clone(), true));
    let ret = Arc::new(Field::new("size", DataType::Int64, true));
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(array)],
        arg_fields: vec![field],
        number_rows: n,
        return_field: ret,
        config_options: Arc::new(ConfigOptions::default()),
    };
    match CypherSize::new().invoke_with_args(args).unwrap() {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
    }
}

#[test]
fn cypher_reverse_runtime_dispatches_strings_lists_and_type_errors() {
    use datafusion::arrow::array::{Array, LargeStringArray, ListArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    let invoke = |value: ScalarValue| {
        let data_type = value.data_type();
        CypherReverse::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(value)],
            arg_fields: vec![Arc::new(Field::new("value", data_type.clone(), true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("out", data_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    };

    let text = invoke(ScalarValue::LargeUtf8(Some("Áda".into()))).unwrap();
    let text = match text {
        ColumnarValue::Array(array) => array,
        ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
    };
    let text = text.as_any().downcast_ref::<LargeStringArray>().unwrap();
    assert_eq!(text.value(0), "ad́A");

    use datafusion::arrow::array::StringArray;
    let utf8 = invoke(ScalarValue::Utf8(Some("Graph".into()))).unwrap();
    let utf8 = match utf8 {
        ColumnarValue::Array(array) => array,
        ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
    };
    let utf8 = utf8.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(utf8.value(0), "hparG");

    let list = ScalarValue::List(ScalarValue::new_list(
        &[
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(3)),
        ],
        &DataType::Int64,
        true,
    ));
    let reversed = invoke(list).unwrap();
    let reversed = match reversed {
        ColumnarValue::Array(array) => array,
        ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
    };
    let reversed = reversed.as_any().downcast_ref::<ListArray>().unwrap();
    let values = reversed.value(0);
    assert_eq!(
        (0..values.len())
            .map(|row| ScalarValue::try_from_array(&values, row).unwrap())
            .collect::<Vec<_>>(),
        [
            ScalarValue::Int64(Some(3)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(1)),
        ]
    );

    let error = invoke(ScalarValue::Int64(Some(7))).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Error during planning: reverse() expects a string or list, got Int64"
    );
}

#[test]
fn scalar_udf_runtime_strings_and_ranges() {
    use datafusion::arrow::array::{Array, BooleanArray, ListArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    fn invoke<U: ScalarUDFImpl>(
        udf: &U,
        values: Vec<ScalarValue>,
        return_type: DataType,
    ) -> datafusion::error::Result<ColumnarValue> {
        let fields = values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                Arc::new(Field::new(format!("arg_{index}"), value.data_type(), true))
            })
            .collect();
        udf.invoke_with_args(ScalarFunctionArgs {
            args: values.into_iter().map(ColumnarValue::Scalar).collect(),
            arg_fields: fields,
            number_rows: 1,
            return_field: Arc::new(Field::new("out", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    for (kind, expected) in [
        (StringPredicate::Starts, true),
        (StringPredicate::Ends, false),
        (StringPredicate::Contains, true),
    ] {
        let output = invoke(
            &CypherStringPredicate::new(kind),
            vec![
                ScalarValue::LargeUtf8(Some("GraphForge".into())),
                ScalarValue::Utf8(Some("Graph".into())),
            ],
            DataType::Boolean,
        )
        .unwrap();
        let output = match output {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
        };
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0),
            expected
        );
    }

    let range_type = DataType::new_list(DataType::Int64, true);
    for (start, end, step, expected) in [(1, 5, 2, vec![1, 3, 5]), (5, 1, -2, vec![5, 3, 1])] {
        let output = invoke(
            &CypherRange::new(),
            vec![
                ScalarValue::Int64(Some(start)),
                ScalarValue::Int64(Some(end)),
                ScalarValue::Int64(Some(step)),
            ],
            range_type.clone(),
        )
        .unwrap();
        let output = match output {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
        };
        let list = output
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        assert_eq!(
            (0..list.len())
                .map(|row| ScalarValue::try_from_array(&list, row).unwrap())
                .collect::<Vec<_>>(),
            expected
                .into_iter()
                .map(|value| ScalarValue::Int64(Some(value)))
                .collect::<Vec<_>>()
        );
    }
    for (step, fragment) in [(0, "must not be zero"), (2, "overflowed i64")] {
        let start = if step == 0 { 1 } else { i64::MAX - 1 };
        let end = if step == 0 { 2 } else { i64::MAX };
        let error = invoke(
            &CypherRange::new(),
            vec![
                ScalarValue::Int64(Some(start)),
                ScalarValue::Int64(Some(end)),
                ScalarValue::Int64(Some(step)),
            ],
            range_type.clone(),
        )
        .unwrap_err();
        assert!(error.to_string().contains(fragment));
    }
}

#[test]
fn canonical_float_strings_and_scalar_range_arguments_cover_boundaries() {
    for (value, expected) in [
        (0.0, "0.0"),
        (-0.0, "0.0"),
        (f64::NAN, "NaN"),
        (f64::INFINITY, "Infinity"),
        (f64::NEG_INFINITY, "-Infinity"),
        (1.0, "1.0"),
        (1.5, "1.5"),
        (1e20, "100000000000000000000.0"),
    ] {
        assert_eq!(cypher_float_string(value), expected);
    }

    for (value, expected) in [
        (ScalarValue::Int8(Some(-1)), -1),
        (ScalarValue::Int16(Some(-2)), -2),
        (ScalarValue::Int32(Some(-3)), -3),
        (ScalarValue::Int64(Some(-4)), -4),
        (ScalarValue::UInt8(Some(1)), 1),
        (ScalarValue::UInt16(Some(2)), 2),
        (ScalarValue::UInt32(Some(3)), 3),
        (ScalarValue::UInt64(Some(4)), 4),
    ] {
        assert_eq!(scalar_as_i64_arg(&value, "bound").unwrap(), expected);
    }
    assert!(
        scalar_as_i64_arg(&ScalarValue::UInt64(Some(u64::MAX)), "bound")
            .unwrap_err()
            .to_string()
            .contains("exceeds i64::MAX")
    );
    assert!(
        scalar_as_i64_arg(&ScalarValue::Utf8(Some("1".into())), "bound")
            .unwrap_err()
            .to_string()
            .contains("must be an integer")
    );
}

#[test]
fn large_list_size_and_large_utf8_reverse_preserve_values_and_nulls() {
    let size = invoke_test_udf(
        &CypherSize::new(),
        vec![large_list_scalar(vec![1, 2, 3], true)],
    )
    .unwrap();
    assert_eq!(
        ScalarValue::try_from_array(&size, 0).unwrap(),
        ScalarValue::Int64(Some(3))
    );
    let null_size =
        invoke_test_udf(&CypherSize::new(), vec![large_list_scalar(vec![], false)]).unwrap();
    assert!(null_size.is_null(0));

    let reversed = invoke_test_udf(
        &CypherReverse::new(),
        vec![ScalarValue::LargeUtf8(Some("a😀b".into()))],
    )
    .unwrap();
    assert_eq!(
        ScalarValue::try_from_array(&reversed, 0).unwrap(),
        ScalarValue::LargeUtf8(Some("b😀a".into()))
    );
}
