//! Bounded retained-control recognition and native protocol parity.
use graphforge_hub_publish::{assess_retained_manifest, sha256_digest};
use graphforge_protocol_wasm::engine;
use serde_json::Value;

#[test]
fn retained_engine_projects_the_native_assessment_and_exact_digest() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../docs/reference/discovery/v1/retired-manifest-conformance.json"
    ))
    .unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let bytes = case["json"].as_str().unwrap().as_bytes();
        match assess_retained_manifest(bytes) {
            Ok((assessment, canonical)) => {
                let inspection = engine::assess_retained_manifest(bytes).unwrap();
                assert_eq!(
                    serde_json::to_value(inspection.assessment).unwrap(),
                    serde_json::to_value(assessment).unwrap()
                );
                assert_eq!(inspection.digest, sha256_digest(&canonical));
                assert_eq!(inspection.canonical_json.as_bytes(), canonical);
                assert!(engine::item_admission_requirements(bytes).is_err());
                assert!(
                    engine::validate_manifest(
                        bytes,
                        graphforge_discovery::DiscoveryLimits::default()
                    )
                    .is_err()
                );
            }
            Err(error) => {
                let projected = engine::assess_retained_manifest(bytes).unwrap_err();
                assert_eq!(
                    serde_json::to_value(projected).unwrap(),
                    serde_json::to_value(engine::ProtocolError::from(error)).unwrap()
                );
            }
        }
    }
}
