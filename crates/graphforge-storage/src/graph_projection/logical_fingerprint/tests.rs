use super::super::tests::TS;
use super::*;
use arrow::array::{
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};

#[test]
fn canonical_graph_encoding_normalizes_floats_timezones_and_nested_types() {
    assert_eq!(normalize_f32(f32::NAN), 0x7fc0_0000);
    assert_eq!(normalize_f32(-0.0), 0);
    assert_eq!(normalize_f64(f64::NAN), 0x7ff8_0000_0000_0000);
    assert_eq!(normalize_f64(-0.0), 0);
    assert_eq!(time_unit_tag(TimeUnit::Second), 0);
    assert_eq!(time_unit_tag(TimeUnit::Millisecond), 1);
    assert_eq!(time_unit_tag(TimeUnit::Microsecond), 2);
    assert_eq!(time_unit_tag(TimeUnit::Nanosecond), 3);
    for timezone in [
        None,
        Some("UTC"),
        Some("Etc/UTC"),
        Some("Z"),
        Some("+00:00"),
    ] {
        assert!(validate_timezone(timezone).is_ok());
    }
    assert_eq!(
        validate_timezone(Some("America/Denver"))
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );

    let supported = [
        DataType::Null,
        DataType::Boolean,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::FixedSizeBinary(16),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::UInt64, false)), 2),
        DataType::Struct(vec![Field::new("name", DataType::Utf8, false)].into()),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ];
    for data_type in supported {
        let mut writer = CanonicalWriter::new();
        encode_type(&mut writer, &data_type).unwrap();
        assert!(!writer.finish().is_empty());
    }
    let mut writer = CanonicalWriter::new();
    assert_eq!(
        encode_type(&mut writer, &DataType::Date32)
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );

    let micros: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![TS]));
    assert_eq!(
        timestamp_value(&micros, TimeUnit::Microsecond, 0).unwrap(),
        TS
    );
    let time: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![123_i64]));
    assert_eq!(time64_value(&time, TimeUnit::Microsecond, 0).unwrap(), 123);
    assert_eq!(
        time64_value(&time, TimeUnit::Second, 0).unwrap_err().code(),
        "GF_VALIDATION"
    );
}

#[test]
fn canonical_value_encoding_traverses_every_supported_nested_arrow_shape() {
    use arrow::array::{
        FixedSizeListArray, Int32Array, LargeListArray, StringDictionaryBuilder, StructArray,
    };
    use arrow::datatypes::Int32Type;

    let large: ArrayRef = Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), None, Some(2)]),
    ]));
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
        [Some(vec![Some(3), Some(4)])],
        2,
    ));
    let struct_fields: arrow::datatypes::Fields =
        vec![Field::new("value", DataType::Int32, false)].into();
    let structure: ArrayRef = Arc::new(StructArray::new(
        struct_fields.clone(),
        vec![Arc::new(Int32Array::from(vec![5]))],
        None,
    ));
    let mut dictionary_builder = StringDictionaryBuilder::<Int32Type>::new();
    dictionary_builder.append("six").unwrap();
    let dictionary: ArrayRef = Arc::new(dictionary_builder.finish());

    for (data_type, array) in [
        (large.data_type().clone(), large),
        (fixed.data_type().clone(), fixed),
        (DataType::Struct(struct_fields), structure),
        (dictionary.data_type().clone(), dictionary),
    ] {
        let mut writer = CanonicalWriter::new();
        encode_present_value(&mut writer, &data_type, &array, 0).unwrap();
        assert!(!writer.finish().is_empty());
    }
}

#[test]
fn canonical_value_encoding_rejects_type_mismatch_and_nonnullable_null() {
    let floats: ArrayRef = Arc::new(Float32Array::from(vec![Some(-0.0), Some(f32::NAN)]));
    let doubles: ArrayRef = Arc::new(Float64Array::from(vec![Some(-0.0), Some(f64::NAN)]));
    let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("value"), None]));
    let mut writer = CanonicalWriter::new();
    encode_present_value(&mut writer, &DataType::Float32, &floats, 0).unwrap();
    encode_present_value(&mut writer, &DataType::Float32, &floats, 1).unwrap();
    encode_present_value(&mut writer, &DataType::Float64, &doubles, 0).unwrap();
    encode_present_value(&mut writer, &DataType::Float64, &doubles, 1).unwrap();
    encode_value(&mut writer, &DataType::Utf8, &strings, 0, false).unwrap();
    encode_value(&mut writer, &DataType::Utf8, &strings, 1, true).unwrap();
    assert!(!writer.finish().is_empty());

    let mut writer = CanonicalWriter::new();
    assert_eq!(
        encode_value(&mut writer, &DataType::Utf8, &strings, 1, false)
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );
    let mut writer = CanonicalWriter::new();
    assert_eq!(
        encode_present_value(&mut writer, &DataType::UInt64, &strings, 0)
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );

    let nulls: ArrayRef = Arc::new(NullArray::new(2));
    let mut writer = CanonicalWriter::new();
    encode_value(&mut writer, &DataType::Null, &nulls, 0, true).unwrap();
    encode_value(&mut writer, &DataType::Null, &nulls, 1, true).unwrap();
    assert_eq!(writer.finish(), vec![0, 0]);
    let mut writer = CanonicalWriter::new();
    assert_eq!(
        encode_value(&mut writer, &DataType::Null, &nulls, 0, false)
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );
}

#[test]
fn canonical_value_encoding_covers_every_scalar_and_time_representation() {
    let values: Vec<(DataType, ArrayRef)> = vec![
        (DataType::Boolean, Arc::new(BooleanArray::from(vec![true]))),
        (DataType::Int32, Arc::new(Int32Array::from(vec![-7]))),
        (DataType::Int64, Arc::new(Int64Array::from(vec![-9]))),
        (DataType::UInt32, Arc::new(UInt32Array::from(vec![7]))),
        (DataType::UInt64, Arc::new(UInt64Array::from(vec![9]))),
        (DataType::Utf8, Arc::new(StringArray::from(vec!["small"]))),
        (
            DataType::LargeUtf8,
            Arc::new(LargeStringArray::from(vec!["large"])),
        ),
        (
            DataType::Binary,
            Arc::new(BinaryArray::from_vec(vec![b"small".as_slice()])),
        ),
        (
            DataType::LargeBinary,
            Arc::new(LargeBinaryArray::from_vec(vec![b"large".as_slice()])),
        ),
        (
            DataType::Timestamp(TimeUnit::Second, None),
            Arc::new(TimestampSecondArray::from(vec![1_i64])),
        ),
        (
            DataType::Timestamp(TimeUnit::Millisecond, Some("Z".into())),
            Arc::new(TimestampMillisecondArray::from(vec![2_i64]).with_timezone("Z")),
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Arc::new(TimestampMicrosecondArray::from(vec![3_i64]).with_timezone("UTC")),
        ),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some("Etc/UTC".into())),
            Arc::new(TimestampNanosecondArray::from(vec![4_i64]).with_timezone("Etc/UTC")),
        ),
        (
            DataType::Time64(TimeUnit::Microsecond),
            Arc::new(Time64MicrosecondArray::from(vec![5_i64])),
        ),
        (
            DataType::Time64(TimeUnit::Nanosecond),
            Arc::new(Time64NanosecondArray::from(vec![6_i64])),
        ),
    ];
    let mut encodings = Vec::new();
    for (data_type, array) in values {
        let mut writer = CanonicalWriter::new();
        encode_present_value(&mut writer, &data_type, &array, 0).unwrap();
        let encoded = writer.finish();
        assert!(!encoded.is_empty(), "{data_type} must emit canonical bytes");
        encodings.push(encoded);
    }
    assert_eq!(encodings.len(), 15);

    let seconds: ArrayRef = Arc::new(TimestampSecondArray::from(vec![11_i64]));
    let millis: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![12_i64]));
    let nanos: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![13_i64]));
    assert_eq!(timestamp_value(&seconds, TimeUnit::Second, 0).unwrap(), 11);
    assert_eq!(
        timestamp_value(&millis, TimeUnit::Millisecond, 0).unwrap(),
        12
    );
    assert_eq!(
        timestamp_value(&nanos, TimeUnit::Nanosecond, 0).unwrap(),
        13
    );
    let time_nanos: ArrayRef = Arc::new(Time64NanosecondArray::from(vec![14_i64]));
    assert_eq!(
        time64_value(&time_nanos, TimeUnit::Nanosecond, 0).unwrap(),
        14
    );
}

#[test]
fn wave10_projection_private_bounds_are_exact() {
    assert!(exact_u32(usize::MAX, "field count").is_err());
    assert!(validate_timezone(Some("America/Denver")).is_err());

    let values: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1]));
    assert!(time64_value(&values, TimeUnit::Second, 0).is_err());
    assert!(downcast::<arrow::array::StringArray>(&values).is_err());
}

#[test]
fn canonical_numeric_and_dictionary_normalization_are_exact() {
    assert_ne!(normalize_f32(1.25), 0);
    assert_ne!(normalize_f64(1.25), 0);
    assert_eq!(
        dictionary_value_type(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        )),
        &DataType::Utf8
    );
}
