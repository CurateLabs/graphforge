//! Retained coverage is an absence-of-advertised-claims proof, not admission.
use graphforge_discovery::{DiscoveryLimits, DiscoveryManifest};
use graphforge_hub_publish::{assess_retained_manifest, sha256_digest};
use serde_json::{Value, json};

const RETIRED: &str =
    include_str!("../../../docs/reference/discovery/v1/retired-manifest-conformance.json");
const CURRENT: &[&str] = &[
    include_str!("../../../docs/reference/discovery/v1/conformance.json"),
    include_str!("../../../docs/reference/discovery/v1/module-metadata-conformance.json"),
    include_str!("../../../docs/reference/discovery/v1/module-definition-counts-conformance.json"),
];

#[test]
fn historical_controls_preserve_original_canonical_bytes_and_identity_without_authority() {
    let corpus: Value = serde_json::from_str(RETIRED).unwrap();
    assert_eq!(
        corpus["source_commit"],
        "9cbb410719d1d463bdb9c9f3b6d67b39bd27e391"
    );
    let mut claim_free = 0;
    for case in corpus["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let bytes = case["json"].as_str().unwrap().as_bytes();
        assert_eq!(
            sha256_digest(bytes),
            format!("sha256:{}", case["json_sha256"].as_str().unwrap()),
            "{name}: authentic input hash"
        );
        // Strict discovery never gains the historical exception.
        assert!(
            DiscoveryManifest::from_json(bytes, DiscoveryLimits::default()).is_err(),
            "{name}"
        );
        let result = assess_retained_manifest(bytes);
        if case["expected"]["revision"] == "retired_no_claims" {
            let (assessment, canonical) =
                result.unwrap_or_else(|error| panic!("{name}: {error:?}"));
            let original: Value = serde_json::from_slice(bytes).unwrap();
            assert_eq!(
                serde_json::to_value(assessment).unwrap(),
                json!({
                    "revision": "retired_no_claims", "repository": original["repository"],
                    "immutable_version": original["immutable_version"],
                }),
                "{name}"
            );
            assert_eq!(
                canonical,
                case["expected"]["canonical_json"]
                    .as_str()
                    .unwrap()
                    .as_bytes(),
                "{name}"
            );
            assert_eq!(
                sha256_digest(&canonical),
                sha256_digest(
                    case["expected"]["canonical_json"]
                        .as_str()
                        .unwrap()
                        .as_bytes()
                ),
                "{name}"
            );
            assert!(
                !String::from_utf8(canonical)
                    .unwrap()
                    .contains("producing_core_version")
            );
            claim_free += 1;
        } else {
            assert!(
                result.is_err(),
                "{name}: historical exemption must fail closed"
            );
        }
    }
    assert_eq!(claim_free, 5);
}

#[test]
fn every_current_manifest_keeps_its_existing_verdict_and_canonical_identity() {
    let corpora: Vec<Value> = CURRENT
        .iter()
        .map(|corpus| serde_json::from_str(corpus).unwrap())
        .collect();
    for case in corpora
        .iter()
        .flat_map(|corpus| corpus["cases"].as_array().unwrap())
        .filter(|case| case["document"] == "manifest")
    {
        let bytes = case["json"].as_str().unwrap().as_bytes();
        // The public assessment has fixed default limits. Cases with narrower
        // test-only limits are compared with the strict parser at those defaults.
        let current = DiscoveryManifest::from_json(bytes, DiscoveryLimits::default());
        let assessment = assess_retained_manifest(bytes);
        match current {
            Ok(manifest) => {
                let (assessment, canonical) = assessment.unwrap();
                assert_eq!(
                    serde_json::to_value(assessment).unwrap(),
                    json!({"revision":"current"}),
                    "{}",
                    case["name"]
                );
                assert_eq!(canonical, manifest.to_canonical_json().unwrap());
            }
            Err(_) => assert!(assessment.is_err(), "{}", case["name"]),
        }
    }
}

#[test]
fn empty_historical_inventory_is_claim_free_but_identity_and_bytes_remain_bound() {
    let corpus: Value = serde_json::from_str(RETIRED).unwrap();
    let case = corpus["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "manifest-with-research-lineage")
        .unwrap();
    let mut value: Value = serde_json::from_str(case["json"].as_str().unwrap()).unwrap();
    value["ontology"] =
        json!({"composition_digest":value["immutable_version"],"modules":[],"bridge_sets":[]});
    let original = serde_json::to_vec(&value).unwrap();
    let (_, canonical) = assess_retained_manifest(&original).unwrap();
    assert_eq!(canonical, original);
    let original_digest = sha256_digest(&canonical);
    value["immutable_version"] = json!(format!("sha256:{}", "f".repeat(64)));
    let (assessment, changed) =
        assess_retained_manifest(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_ne!(sha256_digest(&changed), original_digest);
    assert_eq!(
        serde_json::to_value(assessment).unwrap()["immutable_version"],
        value["immutable_version"]
    );
    // No refs, receipt, writer or origin input exists in this proof: the host
    // must compare the returned identity and digest with its immutable record.
}

#[test]
fn default_response_bound_is_enforced_without_historical_fallback() {
    let corpus: Value = serde_json::from_str(RETIRED).unwrap();
    let mut bytes = corpus["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "manifest-with-research-lineage")
        .unwrap()["json"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    bytes.resize(DiscoveryLimits::default().max_response_bytes + 1, b' ');
    // A valid historical document plus JSON whitespace would otherwise pass.
    assert_eq!(
        assess_retained_manifest(&bytes).unwrap_err().detail(),
        "item admission manifest exceeds its byte bound"
    );
    assert_eq!(
        assess_retained_manifest(&bytes).unwrap_err().code,
        graphforge_hub_publish::HubPublishErrorCode::InvalidInput
    );
}
