//! Pre-v1 identifier renames keep no compatibility: a discovery document that
//! requires a capability by its retired name is refused, not translated.

use graphforge_discovery::{
    DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, DiscoveryVersionSubject,
};
use serde_json::Value;

const CONFORMANCE: &str = include_str!("../../../docs/reference/discovery/v1/conformance.json");

fn valid_manifest() -> Value {
    let corpus: Value = serde_json::from_str(CONFORMANCE).unwrap();
    let case = &corpus["cases"][0];
    assert_eq!(case["document"], "manifest");
    assert_eq!(case["expected"]["result"], "valid");
    serde_json::from_str(case["json"].as_str().unwrap()).unwrap()
}

#[test]
fn retired_portable_capability_name_is_refused_as_unsupported_future() {
    let current = valid_manifest();
    let current_name = current["requirements"][0]["capability"].as_str().unwrap();
    assert_eq!(current_name, "portable");
    DiscoveryManifest::from_json(
        &serde_json::to_vec(&current).unwrap(),
        DiscoveryLimits::default(),
    )
    .expect("the current capability name is the positive control");

    let mut retired = current.clone();
    retired["requirements"][0]["capability"] = format!("{current_name}-{}", "v2").into();
    let error = DiscoveryManifest::from_json(
        &serde_json::to_vec(&retired).unwrap(),
        DiscoveryLimits::default(),
    )
    .unwrap_err();
    assert_eq!(error.code, DiscoveryErrorCode::UnsupportedFuture);
    assert_eq!(error.field, Some("requirements"));
    let version = error.version.expect("version details");
    assert_eq!(version.subject, DiscoveryVersionSubject::Capability);
    assert_eq!(version.supported_major, None);
}
