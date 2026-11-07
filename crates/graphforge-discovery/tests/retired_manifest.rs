//! Bounded retained-control recognition and native protocol parity.
use graphforge_discovery::{DiscoveryLimits, RetiredManifest};
use serde_json::{Value, json};

fn historical() -> Vec<u8> {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../docs/reference/discovery/v1/retired-manifest-conformance.json"
    ))
    .unwrap();
    corpus["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "manifest-with-research-lineage")
        .unwrap()["json"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec()
}

#[test]
fn historical_reader_reuses_trust_boundary_limits() {
    let bytes = historical();
    let mut value: Value = serde_json::from_slice(&bytes).unwrap();
    value["ontology"] =
        json!({"composition_digest":value["immutable_version"],"modules":[],"bridge_sets":[]});
    let default = DiscoveryLimits::default();
    for limits in [
        DiscoveryLimits {
            max_response_bytes: bytes.len() - 1,
            ..default
        },
        DiscoveryLimits {
            max_string_bytes: 1,
            ..default
        },
        DiscoveryLimits {
            max_objects: 0,
            ..default
        },
        DiscoveryLimits {
            max_locations_per_object: 0,
            ..default
        },
        DiscoveryLimits {
            max_cumulative_object_bytes: 0,
            ..default
        },
        DiscoveryLimits {
            max_lineage_bytes: 0,
            ..default
        },
    ] {
        assert!(
            RetiredManifest::from_json(&bytes, limits).is_err(),
            "{limits:?}"
        );
    }
    assert!(
        !RetiredManifest::from_json(&serde_json::to_vec(&value).unwrap(), default)
            .unwrap()
            .advertises_inventory()
    );
    value["ontology"]["composition_digest"] = json!("invalid");
    assert!(RetiredManifest::from_json(&serde_json::to_vec(&value).unwrap(), default).is_err());
}
