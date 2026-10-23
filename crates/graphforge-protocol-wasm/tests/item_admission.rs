//! Verify the same native outcome survives the shared protocol error projection.
use graphforge_protocol_wasm::engine;
use serde_json::{Value, json};

#[test]
fn admission_corpus_preserves_native_error_items_and_decisions() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../docs/reference/hub-publish/v1/item-admission-conformance.json"
    ))
    .unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let bytes = serde_json::to_vec(&case["input"]).unwrap();
        let result = engine::plan_item_admission(&bytes);
        if let Some(plan) = case["expected"].get("plan") {
            assert_eq!(
                serde_json::to_value(result.unwrap()).unwrap(),
                *plan,
                "{}",
                case["name"]
            );
        } else {
            let error = result.unwrap_err();
            assert_eq!(
                serde_json::to_value(error).unwrap(),
                case["expected"]["error"],
                "{}",
                case["name"]
            );
        }
    }
    let invalid = engine::plan_item_admission(b"{}").unwrap_err();
    assert!(
        serde_json::to_value(invalid)
            .unwrap()
            .get("items")
            .is_none()
    );
    assert_eq!(
        engine::item_admission_requirements(b"{}").unwrap_err().code,
        "invalid_input"
    );
    assert_eq!(
        serde_json::to_value(engine::has_item_id_shape("bad")).unwrap(),
        json!(false)
    );
}

#[test]
fn every_hub_error_conversion_uses_the_native_body_message_and_items() {
    use graphforge_hub_publish::{HubPublishError, HubPublishErrorCode};
    for error in [
        HubPublishError::new(HubPublishErrorCode::IdTaken, "static internal detail"),
        HubPublishError::new(HubPublishErrorCode::InvalidInput, "fixed input guidance"),
    ] {
        let expected = serde_json::to_value(error.body()).unwrap();
        let actual = serde_json::to_value(engine::ProtocolError::from(error)).unwrap();
        assert_eq!(actual, expected);
    }
}
