//! Native adapter parity with the source-transition authority, not host execution.
use graphforge_protocol_wasm::engine;
use serde_json::Value;

#[test]
fn source_transition_adapter_replays_native_oracle() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/reference/hub-publish/v1/item-source-conformance.json");
    let corpus: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let outcome =
            engine::plan_item_source_transition(case["json"].as_str().unwrap().as_bytes());
        if let Some(plan) = case["expected"].get("plan") {
            assert_eq!(
                serde_json::to_value(outcome.unwrap()).unwrap(),
                *plan,
                "{}",
                case["name"]
            );
        } else {
            assert_eq!(
                outcome.unwrap_err().code,
                case["expected"]["code"],
                "{}",
                case["name"]
            );
        }
    }
}
