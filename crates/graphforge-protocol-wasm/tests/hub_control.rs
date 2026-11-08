//! Native adapter replays the same bounded documents used by actual WASM.
use graphforge_discovery::hub::{HubControlRequest, HubControlResponse, HubManagementPlan};
use graphforge_protocol_wasm::engine;
use serde_json::Value;

#[test]
fn control_adapters_preserve_native_decisions_and_commitment_vector() {
    let corpus: Value = serde_json::from_slice(include_bytes!(
        "../../../docs/contracts/hub-control/v1/conformance.json"
    ))
    .unwrap();
    let input = serde_json::to_vec(&corpus["plan_input"]).unwrap();
    let plan = HubManagementPlan::prepare_json(&input).unwrap();
    assert_eq!(serde_json::to_value(&plan).unwrap(), corpus["plan"]);
    assert_eq!(plan, engine::prepare_hub_management_plan(&input).unwrap());
    for case in corpus["cases"].as_array().unwrap() {
        let request = case["request"].as_str().unwrap().as_bytes();
        let native = HubControlRequest::from_json(request);
        let adapter = engine::validate_hub_control_request(request);
        assert_eq!(native.is_ok(), adapter.is_ok(), "{}", case["name"]);
        let accepted = if let Some(response) = case["response"].as_str() {
            let native = native
                .and_then(|request| HubControlResponse::from_json(response.as_bytes(), &request));
            let adapter = engine::validate_hub_control_response(request, response.as_bytes());
            assert_eq!(native.is_ok(), adapter.is_ok(), "{}", case["name"]);
            if let (Ok(native), Ok(adapter)) = (&native, &adapter) {
                assert_eq!(native, adapter);
            }
            native.is_ok()
        } else {
            native.is_ok()
        };
        assert_eq!(
            accepted,
            case["accept"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
    }
}
