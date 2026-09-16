use super::*;
use crate::IrLiteral;
use crate::PropValue;
use graphforge_api::TemporalValue;

#[test]
fn query_uuid_tag_is_exact_and_strings_stay_strings() {
    let text = "550e8400-e29b-41d4-a716-446655440000";
    assert!(matches!(
        json_to_ir_literal(&serde_json::json!({"$uuid": text})).unwrap(),
        IrLiteral::Uuid(_)
    ));
    assert_eq!(
        json_to_ir_literal(&serde_json::json!(text)).unwrap(),
        IrLiteral::Str(text.to_owned())
    );
    for (invalid, reason) in [
        (
            serde_json::json!({"$uuid": "not-a-uuid"}),
            "UUID parameter must be canonical hyphenated UUID text",
        ),
        (
            serde_json::json!({"$uuid": text.to_uppercase()}),
            "UUID parameter must be canonical hyphenated UUID text",
        ),
        (
            serde_json::json!({"$uuid": text, "extra": true}),
            "UUID parameter tag must contain only $uuid",
        ),
        (
            serde_json::json!({"$uuid": 7}),
            "UUID parameter $uuid value must be a string",
        ),
    ] {
        let error = json_to_ir_literal(&invalid).unwrap_err();
        assert_eq!(error.status, "GF_VALIDATION", "wrong code for {invalid}");
        assert_eq!(error.reason, reason, "wrong message for {invalid}");
    }
}

#[test]
fn rejects_unsigned_property_values_above_i64() {
    let error = json_to_prop_value(&serde_json::json!(u64::MAX)).unwrap_err();
    assert_eq!(error.status, "ValidationError");
}

#[test]
fn temporal_json_preserves_calendar_offset_and_zone_components() {
    let value = json_to_prop_value(&serde_json::json!({
        "type": "zoned_date_time",
        "epoch_days": 19_932,
        "nanos": 5_400_000_000_000_i64,
        "offset_seconds": -21_600,
        "zone": "America/Denver"
    }))
    .unwrap();
    assert_eq!(
        value,
        PropValue::Temporal(TemporalValue::ZonedDateTime {
            epoch_days: 19_932,
            nanos: 5_400_000_000_000,
            offset_seconds: -21_600,
            zone: Some("America/Denver".into()),
        })
    );
    assert!(json_to_prop_value(&serde_json::json!({"nested": true})).is_err());
}
