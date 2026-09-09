//! Direct temporal adapter contracts.

use super::{
    CypherDateProject, CypherDateTimeProject, CypherDateTimeTruncate, CypherDateTruncate,
    CypherDurationAdd, CypherDurationBetween, CypherDurationParse, CypherDurationScale,
    CypherLocalDateTimeProject, CypherLocalDateTimeTruncate, CypherLocalTimeProject,
    CypherLocalTimeTruncate, CypherTemporalArith, CypherTimeProject, CypherTimeTruncate,
    build_date_struct, build_datetime_struct, build_duration_struct, build_localdatetime_struct,
    build_time_struct, cast_argument_arrays, date_scalar, date_struct_value, datetime_scalar,
    datetime_struct_parts, dur_secs_nanos, duration_scalar, duration_struct_parts,
    duration_value_to_ir, is_temporal_clock_fn, localdatetime_scalar, localdatetime_struct_parts,
    optional_i64_at, temporal_accessor_valid, temporal_null_scalar, time_scalar, time_struct_parts,
};
use crate::expr::CypherToString;
use crate::expr::tests::invoke_test_udf;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ScalarUDFImpl;
use datafusion::scalar::ScalarValue;
use graphforge_ir::expr::IrLiteral;
use std::sync::Arc;

#[test]
fn temporal_clock_fn_recognition() {
    // Every instant type × clock accessor is recognised (#920).
    for base in ["date", "localtime", "time", "localdatetime", "datetime"] {
        for clock in ["transaction", "statement", "realtime"] {
            assert!(is_temporal_clock_fn(&format!("{base}.{clock}")));
        }
    }
    // Case-insensitive (Cypher function names are).
    assert!(is_temporal_clock_fn("Date.Realtime"));
    assert!(is_temporal_clock_fn("DATETIME.TRANSACTION"));
    // Non-clock and non-temporal names are not.
    assert!(!is_temporal_clock_fn("datetime.truncate"));
    assert!(!is_temporal_clock_fn("duration.realtime")); // duration has no clock
    assert!(!is_temporal_clock_fn("date"));
    assert!(!is_temporal_clock_fn("foo.realtime"));

    // A null temporal arg lowers to a TYPED null, not generic Null.
    assert_eq!(
        temporal_null_scalar("date").data_type(),
        date_scalar(None).data_type()
    );
    assert_eq!(
        temporal_null_scalar("localtime"),
        ScalarValue::Time64Nanosecond(None)
    );
    assert_eq!(
        temporal_null_scalar("datetime.realtime").data_type(),
        datetime_scalar(None).data_type()
    );
    assert_eq!(temporal_null_scalar("unknown"), ScalarValue::Null);
}

#[test]
fn temporal_struct_builders_and_extractors_preserve_values_and_nulls() {
    use crate::temporal::DurationValue;
    use datafusion::arrow::array::{ArrayRef, Int32Array, StructArray};
    use datafusion::arrow::datatypes::{Field, Fields};

    let dates = build_date_struct(&[Some(19_723), None]);
    assert_eq!(date_struct_value(&dates, 0), Some(19_723));
    assert_eq!(date_struct_value(&dates, 1), None);

    let local = build_localdatetime_struct(&[(Some((19_723, 45_000))), None]);
    assert_eq!(
        localdatetime_struct_parts(&local, 0),
        Some((19_723, 45_000))
    );
    assert_eq!(localdatetime_struct_parts(&local, 1), None);

    let duration = DurationValue {
        months: 14,
        days: -2,
        seconds: 90,
        nanos: 123,
    };
    let durations = build_duration_struct(&[Some(duration), None]);
    assert_eq!(duration_struct_parts(&durations, 0), Some(duration));
    assert_eq!(duration_struct_parts(&durations, 1), None);
    assert_eq!(
        dur_secs_nanos(-3, 7),
        DurationValue {
            months: 0,
            days: 0,
            seconds: -3,
            nanos: 7,
        }
    );
    assert_eq!(
        duration_value_to_ir(duration),
        IrLiteral::Duration {
            months: 14,
            days: -2,
            seconds: 90,
            nanos: 123,
        }
    );

    let times = build_time_struct(&[Some((86_399_000_000_000, -25_200)), None]);
    assert_eq!(
        time_struct_parts(&times, 0),
        Some((86_399_000_000_000, -25_200))
    );
    assert_eq!(time_struct_parts(&times, 1), None);

    let datetimes = build_datetime_struct(&[
        Some((19_723, 45_000, 3_600, Some("Europe/Paris".into()))),
        Some((19_724, 46_000, 0, None)),
        None,
    ]);
    assert_eq!(
        datetime_struct_parts(&datetimes, 0),
        Some((19_723, 45_000, 3_600, Some("Europe/Paris".into())))
    );
    assert_eq!(
        datetime_struct_parts(&datetimes, 1),
        Some((19_724, 46_000, 0, None))
    );
    assert_eq!(datetime_struct_parts(&datetimes, 2), None);

    // Extractors reject a structurally wrong child type without fabricating
    // a value. This exercises the defensive downcast path with valid Arrow.
    let wrong_fields = Fields::from(vec![Field::new("epoch_day", DataType::Int32, true)]);
    let wrong_children: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![Some(1)]))];
    let wrong_date = StructArray::new(wrong_fields, wrong_children, None);
    assert_eq!(date_struct_value(&wrong_date, 0), None);

    let overrides: ArrayRef = Arc::new(datafusion::arrow::array::Int64Array::from(vec![
        Some(8),
        None,
    ]));
    assert_eq!(optional_i64_at(&overrides, 0), Some(8));
    assert_eq!(optional_i64_at(&overrides, 1), None);
    let wrong_override: ArrayRef = Arc::new(Int32Array::from(vec![Some(8)]));
    assert_eq!(optional_i64_at(&wrong_override, 0), None);
}

#[test]
fn temporal_project_and_truncate_udfs_execute_all_typed_families() {
    use datafusion::arrow::array::Array;

    let null_ints = |count| vec![ScalarValue::Int64(None); count];
    let assert_value = |array: datafusion::arrow::array::ArrayRef| {
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
    };

    let mut args = vec![ScalarValue::Utf8(Some("2024-02-29".into()))];
    args.extend(null_ints(8));
    assert_value(invoke_test_udf(&CypherDateProject::new(), args).unwrap());
    let mut null_args = vec![ScalarValue::Utf8(None)];
    null_args.extend(null_ints(8));
    let null_date = invoke_test_udf(&CypherDateProject::new(), null_args).unwrap();
    assert!(null_date.is_null(0));
    let mut typed_args = vec![date_scalar(Some(19_782))];
    typed_args.extend(null_ints(8));
    assert_value(invoke_test_udf(&CypherDateProject::new(), typed_args).unwrap());

    let mut args = vec![ScalarValue::Utf8(Some("12:34:56.123".into()))];
    args.extend(null_ints(6));
    assert_value(invoke_test_udf(&CypherLocalTimeProject::new(), args).unwrap());
    let mut typed_args = vec![ScalarValue::Time64Nanosecond(Some(45_296_123_000_000))];
    typed_args.extend(null_ints(6));
    assert_value(invoke_test_udf(&CypherLocalTimeProject::new(), typed_args).unwrap());

    let mut args = vec![
        ScalarValue::Utf8(Some("12:34:56.123".into())),
        ScalarValue::Utf8(Some("second".into())),
    ];
    args.extend(null_ints(6));
    assert_value(invoke_test_udf(&CypherLocalTimeTruncate::new(), args).unwrap());

    let mut args = vec![
        ScalarValue::Utf8(Some("2024-02-29".into())),
        ScalarValue::Utf8(Some("12:34:56.123".into())),
    ];
    args.extend(null_ints(14));
    assert_value(invoke_test_udf(&CypherLocalDateTimeProject::new(), args).unwrap());
    let mut typed_args = vec![
        date_scalar(Some(19_782)),
        ScalarValue::Time64Nanosecond(Some(45_296_123_000_000)),
    ];
    typed_args.extend(null_ints(14));
    assert_value(invoke_test_udf(&CypherLocalDateTimeProject::new(), typed_args).unwrap());

    let mut args = vec![
        ScalarValue::Utf8(Some("2024-02-29T12:34:56.123".into())),
        ScalarValue::Utf8(Some("day".into())),
    ];
    args.extend(null_ints(14));
    assert_value(invoke_test_udf(&CypherLocalDateTimeTruncate::new(), args).unwrap());

    let mut args = vec![ScalarValue::Utf8(Some("12:34:56+01:00".into()))];
    args.extend(null_ints(6));
    args.push(ScalarValue::Utf8(None));
    assert_value(invoke_test_udf(&CypherTimeProject::new(), args).unwrap());
    let mut typed_args = vec![time_scalar(Some((45_296_000_000_000, 3_600)))];
    typed_args.extend(null_ints(6));
    typed_args.push(ScalarValue::Utf8(None));
    assert_value(invoke_test_udf(&CypherTimeProject::new(), typed_args).unwrap());

    let mut args = vec![
        ScalarValue::Utf8(Some("12:34:56+01:00".into())),
        ScalarValue::Utf8(Some("minute".into())),
    ];
    args.extend(null_ints(6));
    args.push(ScalarValue::Utf8(None));
    assert_value(invoke_test_udf(&CypherTimeTruncate::new(), args).unwrap());

    let mut args = vec![
        ScalarValue::Utf8(Some("2024-02-29".into())),
        ScalarValue::Utf8(Some("12:34:56+01:00".into())),
    ];
    args.extend(null_ints(14));
    args.push(ScalarValue::Utf8(None));
    assert_value(invoke_test_udf(&CypherDateTimeProject::new(), args).unwrap());
    let mut typed_args = vec![
        date_scalar(Some(19_782)),
        time_scalar(Some((45_296_000_000_000, 3_600))),
    ];
    typed_args.extend(null_ints(14));
    typed_args.push(ScalarValue::Utf8(None));
    assert_value(invoke_test_udf(&CypherDateTimeProject::new(), typed_args).unwrap());

    let mut args = vec![
        ScalarValue::Utf8(Some("2024-02-29T12:34:56+01:00".into())),
        ScalarValue::Utf8(Some("hour".into())),
    ];
    args.extend(null_ints(14));
    args.push(ScalarValue::Utf8(None));
    assert_value(invoke_test_udf(&CypherDateTimeTruncate::new(), args).unwrap());

    let mut args = vec![
        ScalarValue::Utf8(Some("2024-02-29".into())),
        ScalarValue::Utf8(Some("month".into())),
    ];
    args.extend(null_ints(8));
    assert_value(invoke_test_udf(&CypherDateTruncate::new(), args).unwrap());
}

#[test]
fn shared_temporal_cast_helper_preserves_arrow_values_nulls_and_error() {
    use datafusion::arrow::array::{Array, ArrayRef, Int32Array, Int64Array};

    let integers: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
    let casted = cast_argument_arrays(&[integers], &DataType::Int64).unwrap();
    let casted = casted[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(casted.value(0), 7);
    assert!(casted.is_null(1));

    let invalid = ScalarValue::List(ScalarValue::new_list(
        &[ScalarValue::Int64(Some(1))],
        &DataType::Int64,
        true,
    ))
    .to_array_of_size(1)
    .unwrap();
    let direct_error = datafusion::arrow::compute::cast(&invalid, &DataType::Int64)
        .map_err(datafusion::error::DataFusionError::from)
        .unwrap_err()
        .to_string();
    let helper_error = cast_argument_arrays(&[invalid], &DataType::Int64)
        .unwrap_err()
        .to_string();
    assert_eq!(helper_error, direct_error);
}

#[test]
fn temporal_accessor_type_matrix_distinguishes_values_from_properties() {
    // Date and duration dispatch through their dedicated lowering paths.
    assert!(!temporal_accessor_valid(&DataType::Date32, "year"));
    assert!(!temporal_accessor_valid(&DataType::Date32, "timezone"));
    assert!(temporal_accessor_valid(
        &ScalarValue::Time64Nanosecond(None).data_type(),
        "nanosecond"
    ));
    assert!(!temporal_accessor_valid(
        &duration_scalar(None).data_type(),
        "monthsOfYear"
    ));
    assert!(temporal_accessor_valid(
        &localdatetime_scalar(None).data_type(),
        "year"
    ));
    assert!(temporal_accessor_valid(
        &datetime_scalar(None).data_type(),
        "offsetSeconds"
    ));
    assert!(!temporal_accessor_valid(&DataType::Utf8, "year"));
    assert!(!temporal_accessor_valid(&DataType::Int64, "day"));
}

#[test]
fn duration_and_temporal_runtime_udfs_cover_each_value_family_and_nulls() {
    let d1 = crate::temporal::DurationValue {
        months: 1,
        days: 2,
        seconds: 3,
        nanos: 750_000_000,
    };
    let d2 = crate::temporal::DurationValue {
        months: 2,
        days: 3,
        seconds: 4,
        nanos: 500_000_000,
    };
    let d1 = duration_scalar(Some(d1));
    let d2 = duration_scalar(Some(d2));

    let parsed = invoke_test_udf(
        &CypherDurationParse::new(),
        vec![ScalarValue::Utf8(Some("P1M2DT3.5S".into()))],
    )
    .unwrap();
    assert!(duration_struct_parts(parsed.as_any().downcast_ref().unwrap(), 0).is_some());
    let invalid = invoke_test_udf(
        &CypherDurationParse::new(),
        vec![ScalarValue::Utf8(Some("invalid".into()))],
    )
    .unwrap();
    assert!(duration_struct_parts(invalid.as_any().downcast_ref().unwrap(), 0).is_none());

    for sign in [1, -1] {
        let out = invoke_test_udf(
            &CypherDurationAdd::new(),
            vec![d1.clone(), d2.clone(), ScalarValue::Int64(Some(sign))],
        )
        .unwrap();
        assert!(duration_struct_parts(out.as_any().downcast_ref().unwrap(), 0).is_some());
    }
    for (factor, divide) in [(2.0, false), (2.0, true)] {
        let out = invoke_test_udf(
            &CypherDurationScale::new(),
            vec![
                d1.clone(),
                ScalarValue::Float64(Some(factor)),
                ScalarValue::Boolean(Some(divide)),
            ],
        )
        .unwrap();
        assert!(duration_struct_parts(out.as_any().downcast_ref().unwrap(), 0).is_some());
    }

    let temporal_values = [
        date_scalar(Some(20_000)),
        ScalarValue::Time64Nanosecond(Some(10)),
        time_scalar(Some((10, 3_600))),
        localdatetime_scalar(Some((20_000, 10))),
        datetime_scalar(Some((20_000, 10, 0, Some("UTC".into())))),
    ];
    for temporal in temporal_values {
        for sign in [1, -1] {
            let out = invoke_test_udf(
                &CypherTemporalArith::new(),
                vec![temporal.clone(), d1.clone(), ScalarValue::Int64(Some(sign))],
            )
            .unwrap();
            assert_eq!(out.data_type(), &temporal.data_type());
            assert!(!out.is_null(0));
        }
    }
    assert!(
        invoke_test_udf(
            &CypherTemporalArith::new(),
            vec![ScalarValue::Int64(Some(1)), d1, ScalarValue::Int64(Some(1))],
        )
        .unwrap_err()
        .to_string()
        .contains("not a temporal value")
    );
}

#[test]
fn exact_zero_temporal_udf_metadata_contracts_are_total() {
    fn check<U: ScalarUDFImpl + 'static>(udf: U) {
        assert!((&udf as &dyn ScalarUDFImpl).is::<U>());
        assert!(!udf.name().is_empty());
        let _ = udf.signature();
        assert!(udf.return_type(&[DataType::Null]).is_ok());
    }

    check(CypherDurationBetween::new());
    check(CypherTemporalArith::new());
    check(CypherDurationParse::new());
    check(CypherDurationAdd::new());
    check(CypherDurationScale::new());
    check(CypherDateProject::new());
    check(CypherLocalTimeProject::new());
    check(CypherLocalTimeTruncate::new());
    check(CypherLocalDateTimeProject::new());
    check(CypherLocalDateTimeTruncate::new());
    check(CypherTimeProject::new());
    check(CypherTimeTruncate::new());
    check(CypherDateTimeProject::new());
    check(CypherDateTimeTruncate::new());
    check(CypherToString::new());
    check(CypherDateTruncate::new());
}
