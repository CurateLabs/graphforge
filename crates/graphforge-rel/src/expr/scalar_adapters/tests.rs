//! Direct scalar conversion adapter contracts.

use super::{
    CYPHER_TO_STRING, CypherConversion, CypherConversionKind, ir_literal_to_scalar,
    render_temporal, scalar_to_ir_literal, to_cypher_boolean, to_cypher_float, to_cypher_integer,
    to_cypher_string, trunc_float_to_i64,
};
use crate::expr::{build_het_struct, het_fields, scalar_as_f64, scalar_as_i128};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr::ScalarUDFImpl;
use datafusion::scalar::ScalarValue;
use graphforge_core::LoweringError;
use graphforge_ir::expr::IrLiteral;

#[test]
fn cypher_conversions_decode_tagged_values() {
    use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::scalar::ScalarValue as S;
    use std::sync::Arc;

    let values: ArrayRef = Arc::new(
        build_het_struct(
            &[
                S::Int64(Some(2)),
                S::Float64(Some(2.9)),
                S::Utf8(Some("foo".to_owned())),
            ],
            0,
        )
        .unwrap(),
    );
    let invoke = |kind: CypherConversionKind| {
        let udf = CypherConversion::new(kind);
        let return_type = udf.return_type(&[values.data_type().clone()]).unwrap();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(&values))],
            arg_fields: vec![Arc::new(Field::new("v", values.data_type().clone(), true))],
            number_rows: values.len(),
            return_field: Arc::new(Field::new("out", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(values.len()).unwrap(),
        }
    };

    let ints = invoke(CypherConversionKind::Integer);
    let ints = ints.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ints.value(0), 2);
    assert_eq!(ints.value(1), 2);
    assert!(ints.is_null(2));

    let floats = invoke(CypherConversionKind::Float);
    let floats = floats.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(floats.value(0), 2.0);
    assert_eq!(floats.value(1), 2.9);
    assert!(floats.is_null(2));

    let strings = match CYPHER_TO_STRING
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(values)],
            arg_fields: vec![Arc::new(Field::new(
                "v",
                DataType::Struct(het_fields(0)),
                true,
            ))],
            number_rows: 3,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
    {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => s.to_array_of_size(3).unwrap(),
    };
    let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "2");
    assert_eq!(strings.value(1), "2.9");
    assert_eq!(strings.value(2), "foo");
}

#[test]
fn scalar_to_ir_literal_round_trips_each_kind() {
    // Each IrLiteral → ScalarValue → IrLiteral is identity for the canonical
    // widths `ir_literal_to_scalar` emits.
    for lit in [
        IrLiteral::Bool(true),
        IrLiteral::Int(42),
        IrLiteral::Float(1.5),
        IrLiteral::Str("hi".into()),
        IrLiteral::Duration {
            months: 14,
            days: 3,
            seconds: 5,
            nanos: 1000,
        },
        IrLiteral::DateTime(1_700_000_000_000_000),
    ] {
        let scalar = ir_literal_to_scalar(&lit);
        assert_eq!(scalar_to_ir_literal(&scalar).unwrap(), lit);
    }
}

#[test]
fn scalar_to_ir_literal_rejects_graph_identity_as_a_property_value() {
    let scalar = ir_literal_to_scalar(&IrLiteral::Uuid([0x42; 16]));
    assert!(matches!(
        scalar_to_ir_literal(&scalar),
        Err(LoweringError::InvalidType(message))
            if message == "UUID values cannot be stored as graph properties"
    ));
}

#[test]
fn scalar_to_ir_literal_null_variants_map_to_null() {
    assert_eq!(
        scalar_to_ir_literal(&ScalarValue::Null).unwrap(),
        IrLiteral::Null
    );
    assert_eq!(
        scalar_to_ir_literal(&ScalarValue::Int64(None)).unwrap(),
        IrLiteral::Null
    );
}

#[test]
fn scalar_to_ir_literal_widens_smaller_ints() {
    assert_eq!(
        scalar_to_ir_literal(&ScalarValue::Int32(Some(7))).unwrap(),
        IrLiteral::Int(7)
    );
    assert_eq!(
        scalar_to_ir_literal(&ScalarValue::UInt8(Some(255))).unwrap(),
        IrLiteral::Int(255)
    );
}

#[test]
fn scalar_to_ir_literal_normalizes_all_native_widths_and_rejects_overflow() {
    let cases = [
        (ScalarValue::Int8(Some(-8)), IrLiteral::Int(-8)),
        (ScalarValue::Int16(Some(-16)), IrLiteral::Int(-16)),
        (ScalarValue::UInt16(Some(16)), IrLiteral::Int(16)),
        (ScalarValue::UInt32(Some(32)), IrLiteral::Int(32)),
        (ScalarValue::UInt64(Some(64)), IrLiteral::Int(64)),
        (ScalarValue::Float32(Some(1.25)), IrLiteral::Float(1.25)),
        (
            ScalarValue::LargeUtf8(Some("large".into())),
            IrLiteral::Str("large".into()),
        ),
        (
            ScalarValue::Utf8View(Some("view".into())),
            IrLiteral::Str("view".into()),
        ),
        (
            ScalarValue::TimestampSecond(Some(2), None),
            IrLiteral::DateTime(2_000_000),
        ),
        (
            ScalarValue::TimestampMillisecond(Some(3), None),
            IrLiteral::DateTime(3_000),
        ),
        (
            ScalarValue::TimestampNanosecond(Some(4_000), None),
            IrLiteral::DateTime(4),
        ),
        (ScalarValue::Time64Nanosecond(Some(5)), IrLiteral::Time(5)),
    ];
    for (scalar, expected) in cases {
        assert_eq!(scalar_to_ir_literal(&scalar).unwrap(), expected);
    }
    assert!(matches!(
        scalar_to_ir_literal(&ScalarValue::UInt64(Some(u64::MAX))),
        Err(LoweringError::UnsupportedExpr(message)) if message.contains("exceeds the i64 range")
    ));
    assert!(matches!(
        scalar_to_ir_literal(&ScalarValue::Binary(Some(vec![1, 2]))),
        Err(LoweringError::InvalidType(message)) if message.contains("invalid property type")
    ));
}

#[test]
fn scalar_to_ir_literal_round_trips_a_list() {
    // A homogeneous list now stores (#1006): scalar List → IrLiteral::List
    // and back, element-wise — including a list of typed temporals.
    for lit in [
        IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Int(2)]),
        IrLiteral::List(vec![IrLiteral::Date(5428), IrLiteral::Date(5429)]),
    ] {
        let scalar = ir_literal_to_scalar(&lit);
        assert_eq!(scalar_to_ir_literal(&scalar).unwrap(), lit);
    }
}

#[test]
fn scalar_conversion_helpers_exhaust_every_numeric_width_null_and_error_contract() {
    let integers = [
        (ScalarValue::Int8(Some(-8)), -8_i64),
        (ScalarValue::Int16(Some(-16)), -16),
        (ScalarValue::Int32(Some(-32)), -32),
        (ScalarValue::Int64(Some(-64)), -64),
        (ScalarValue::UInt8(Some(8)), 8),
        (ScalarValue::UInt16(Some(16)), 16),
        (ScalarValue::UInt32(Some(32)), 32),
        (ScalarValue::UInt64(Some(64)), 64),
    ];
    for (value, expected) in &integers {
        assert_eq!(scalar_as_i128(value), Some(i128::from(*expected)));
        assert_eq!(scalar_as_f64(value), Some(*expected as f64));
        assert_eq!(to_cypher_integer(value).unwrap(), Some(*expected));
        assert_eq!(to_cypher_float(value).unwrap(), Some(*expected as f64));
        assert_eq!(to_cypher_string(value).unwrap(), Some(expected.to_string()));
    }

    for (value, integer, float, text) in [
        (
            ScalarValue::Float32(Some(12.75)),
            Some(12),
            Some(12.75),
            Some("12.75".to_owned()),
        ),
        (
            ScalarValue::Float64(Some(-12.75)),
            Some(-12),
            Some(-12.75),
            Some("-12.75".to_owned()),
        ),
        (
            ScalarValue::Utf8(Some("42.9".into())),
            Some(42),
            Some(42.9),
            Some("42.9".to_owned()),
        ),
        (
            ScalarValue::LargeUtf8(Some("-3".into())),
            Some(-3),
            Some(-3.0),
            Some("-3".to_owned()),
        ),
    ] {
        assert_eq!(to_cypher_integer(&value).unwrap(), integer);
        assert_eq!(to_cypher_float(&value).unwrap(), float);
        assert_eq!(to_cypher_string(&value).unwrap(), text);
    }

    for null in [
        ScalarValue::Null,
        ScalarValue::Int64(None),
        ScalarValue::Float64(None),
        ScalarValue::Utf8(None),
        ScalarValue::Boolean(None),
    ] {
        assert_eq!(to_cypher_integer(&null).unwrap(), None);
        assert_eq!(to_cypher_float(&null).unwrap(), None);
        assert_eq!(to_cypher_boolean(&null).unwrap(), None);
        assert_eq!(to_cypher_string(&null).unwrap(), None);
    }

    for invalid_float in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, f64::MAX] {
        assert_eq!(trunc_float_to_i64(invalid_float), None);
    }
    assert_eq!(trunc_float_to_i64(-9.99), Some(-9));
    for invalid_text in ["", "not-a-number", "NaN", "inf"] {
        let value = ScalarValue::Utf8(Some(invalid_text.into()));
        assert_eq!(to_cypher_integer(&value).unwrap(), None);
        assert_eq!(to_cypher_float(&value).unwrap(), None);
    }
    assert_eq!(
        to_cypher_boolean(&ScalarValue::Boolean(Some(true))).unwrap(),
        Some(true)
    );
    assert_eq!(
        to_cypher_boolean(&ScalarValue::Utf8(Some("true".into()))).unwrap(),
        Some(true)
    );
    assert_eq!(
        to_cypher_boolean(&ScalarValue::LargeUtf8(Some("false".into()))).unwrap(),
        Some(false)
    );
    assert_eq!(
        to_cypher_boolean(&ScalarValue::Utf8(Some("TRUE".into()))).unwrap(),
        None
    );

    for invalid in [
        ScalarValue::Boolean(Some(true)),
        ScalarValue::Binary(Some(vec![1])),
    ] {
        assert!(to_cypher_integer(&invalid).is_err());
        assert!(to_cypher_float(&invalid).is_err());
    }
    assert!(to_cypher_boolean(&ScalarValue::Int64(Some(1))).is_err());
    assert!(to_cypher_string(&ScalarValue::Binary(Some(vec![1]))).is_err());
    assert!(to_cypher_integer(&ScalarValue::UInt64(Some(u64::MAX))).is_err());
}

#[test]
fn temporal_literal_render_dispatch_and_ir_scalar_round_trip_matrix() {
    for (name, input) in [
        ("date", "2024-02-29"),
        ("localtime", "12:34:56"),
        ("time", "12:34:56+01:00"),
        ("localdatetime", "2024-02-29T12:34:56"),
        ("datetime", "2024-02-29T12:34:56Z"),
        ("duration", "P1M2DT3S"),
    ] {
        assert!(render_temporal(name, input).is_some(), "{name}({input})");
    }
    assert_eq!(render_temporal("unknown", "2024-01-01"), None);
    assert_eq!(render_temporal("date", "not-a-date"), None);

    let literals = [
        IrLiteral::Null,
        IrLiteral::Bool(true),
        IrLiteral::Int(-7),
        IrLiteral::Float(1.25),
        IrLiteral::Str("value".into()),
        IrLiteral::Duration {
            months: 1,
            days: 2,
            seconds: 3,
            nanos: 4,
        },
        IrLiteral::DateTime(123),
        IrLiteral::Date(20_000),
        IrLiteral::LocalDateTime {
            days: 20_000,
            nanos: 123,
        },
        IrLiteral::Time(456),
        IrLiteral::ZonedTime {
            nanos: 789,
            offset: 3_600,
        },
        IrLiteral::ZonedDateTime {
            days: 20_000,
            nanos: 999,
            offset: -3_600,
            zone: Some("America/Denver".into()),
        },
        IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Null]),
        IrLiteral::Map(vec![("answer".into(), IrLiteral::Int(42))]),
    ];
    for literal in literals {
        let scalar = ir_literal_to_scalar(&literal);
        if !matches!(literal, IrLiteral::Map(_)) {
            assert_eq!(scalar_to_ir_literal(&scalar).unwrap(), literal);
        }
    }

    for (scalar, expected) in [
        (
            ScalarValue::DurationSecond(Some(-2)),
            IrLiteral::Duration {
                months: 0,
                days: 0,
                seconds: -2,
                nanos: 0,
            },
        ),
        (
            ScalarValue::DurationMillisecond(Some(-1)),
            IrLiteral::Duration {
                months: 0,
                days: 0,
                seconds: -1,
                nanos: 999_000_000,
            },
        ),
        (
            ScalarValue::DurationMicrosecond(Some(-1)),
            IrLiteral::Duration {
                months: 0,
                days: 0,
                seconds: -1,
                nanos: 999_999_000,
            },
        ),
        (
            ScalarValue::DurationNanosecond(Some(-1)),
            IrLiteral::Duration {
                months: 0,
                days: 0,
                seconds: -1,
                nanos: 999_999_999,
            },
        ),
    ] {
        assert_eq!(scalar_to_ir_literal(&scalar).unwrap(), expected);
    }
}
