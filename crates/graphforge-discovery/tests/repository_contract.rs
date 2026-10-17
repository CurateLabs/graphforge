//! Deterministic repository-move wire artifacts, validated by the native parser.
use graphforge_discovery::{DiscoveryLimits, RepositoryMoved};
use serde_json::{Value, json};
use std::path::PathBuf;

fn artifacts() -> [(String, Value); 2] {
    let identity = json!({"type":"object","additionalProperties":false,"required":["owner","repository"],"properties":{"owner":{"$ref":"#/$defs/slug"},"repository":{"$ref":"#/$defs/slug"}}});
    let valid = json!({"format":"graphforge-repository-moved/1","moved_from":{"owner":"curate","repository":"claims"},"moved_to":{"owner":"research","repository":"renamed"}});
    let mut same = valid.clone();
    same["moved_to"] = same["moved_from"].clone();
    let mut future = valid.clone();
    future["format"] = json!("graphforge-repository-moved/2");
    let mut extra = valid.clone();
    extra["location"] = json!("https://other.example/");
    let mut dots_owner = valid.clone();
    dots_owner["moved_to"]["owner"] = json!("a..b");
    let mut dots_repository = valid.clone();
    dots_repository["moved_to"]["repository"] = json!("a..b");
    let mut single_dot = valid.clone();
    single_dot["moved_to"]["repository"] = json!("a.b");
    [
        (
            "moved.schema.json".into(),
            json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"https://graphforge.sh/schemas/discovery/v1/moved.schema.json","type":"object","additionalProperties":false,"required":["format","moved_from","moved_to"],"properties":{"format":{"const":"graphforge-repository-moved/1"},"moved_from":{"$ref":"#/$defs/identity"},"moved_to":{"$ref":"#/$defs/identity"}},"$defs":{"identity":identity,"slug":{"type":"string","minLength":1,"maxLength":100,"pattern":"^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$","not":{"pattern":r"\.\."}}}}),
        ),
        (
            "moved-conformance.json".into(),
            json!({"format":"graphforge-repository-moved-conformance/1","cases":[{"name":"canonical-move","document":valid,"valid":true},{"name":"same-identity","document":same,"valid":false},{"name":"future-format","document":future,"valid":false},{"name":"unknown-field","document":extra,"valid":false},{"name":"consecutive-dots-owner","document":dots_owner,"valid":false},{"name":"consecutive-dots-repository","document":dots_repository,"valid":false},{"name":"single-dot","document":single_dot,"valid":true}]}),
        ),
    ]
}

#[test]
fn repository_move_artifacts_match_rust_authority() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/reference/discovery/v1");
    for (name, value) in artifacts() {
        let bytes = format!("{}\n", serde_json::to_string_pretty(&value).unwrap());
        if std::env::var_os("GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS").is_some() {
            std::fs::write(root.join(&name), &bytes).unwrap();
        } else {
            assert_eq!(
                std::fs::read(root.join(&name)).unwrap(),
                bytes.as_bytes(),
                "stale {name}"
            );
        }
        if name == "moved-conformance.json" {
            for case in value["cases"].as_array().unwrap() {
                let result = RepositoryMoved::from_json(
                    &serde_json::to_vec(&case["document"]).unwrap(),
                    DiscoveryLimits::default(),
                );
                assert_eq!(
                    result.is_ok(),
                    case["valid"].as_bool().unwrap(),
                    "{}",
                    case["name"]
                );
            }
        }
    }
}
