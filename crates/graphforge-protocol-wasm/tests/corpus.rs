//! The engine behind the WebAssembly exports against the checked-in corpora and
//! the openalex Hub fixture. `packages/protocol-wasm/tests` replays the same
//! corpora through the built module; both sides must agree with the outcomes
//! the corpora record from the native validators.

use graphforge_discovery::DiscoveryLimits;
use graphforge_hub_publish::{ObjectDeclaration, PublishIntent};
use graphforge_protocol_wasm::engine;
use serde_json::{Value, json};

const DISCOVERY: &str = include_str!("../../../docs/reference/discovery/v1/conformance.json");
const MOVED: &str = include_str!("../../../docs/reference/discovery/v1/moved-conformance.json");
const PREVIEWS: &str = include_str!("../../../docs/reference/hub-publish/v1/previews.json");
const FIXTURE: &str = include_str!("../../../tests/fixtures/hub/generated/v1/fixture.json");
const MANIFEST: &str = include_str!("../../../tests/fixtures/hub/generated/v1/manifest.json");
const REFS: &str = include_str!("../../../tests/fixtures/hub/generated/v1/refs.json");
const SUMMARY: &str =
    include_str!("../../../tests/fixtures/hub/generated/v1/objects/openalex-openalex.summary.json");
const COMMITMENT: &str = include_str!("openalex-commitment.json");

fn limits(case: &Value) -> DiscoveryLimits {
    let bytes = case.get("limits").map(|l| serde_json::to_vec(l).unwrap());
    engine::limits(bytes.as_deref()).unwrap()
}

#[test]
fn discovery_corpus_yields_the_recorded_native_outcome() {
    let corpus: Value = serde_json::from_str(DISCOVERY).unwrap();
    let cases = corpus["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 99);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let bytes = case["json"].as_str().unwrap().as_bytes();
        let limits = limits(case);
        let outcome = match case["document"].as_str().unwrap() {
            "manifest" => engine::validate_manifest(bytes, limits).map(|v| v.canonical_json),
            "refs" => engine::validate_refs(bytes, limits).map(|v| v.canonical_json),
            "summary" => engine::validate_summary(bytes, limits).map(|v| v.canonical_json),
            "lineage" => engine::validate_lineage(bytes, limits).map(|v| v.canonical_json),
            other => panic!("{name}: unknown document {other}"),
        };
        let expected = &case["expected"];
        match (expected["result"].as_str().unwrap(), outcome) {
            ("valid", Ok(canonical)) => assert_eq!(canonical, expected["canonical_json"], "{name}"),
            ("invalid", Err(error)) => {
                assert_eq!(error.code, expected["code"], "{name}");
                assert_eq!(error.path.as_deref(), expected["field"].as_str(), "{name}");
                let version = error.version.map(|v| serde_json::to_value(v).unwrap());
                assert_eq!(version.as_ref(), expected.get("version"), "{name}");
            }
            (result, outcome) => panic!("{name}: recorded {result}, got {outcome:?}"),
        }
    }
}

#[test]
fn repository_move_corpus_yields_the_recorded_native_outcome() {
    let corpus: Value = serde_json::from_str(MOVED).unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let bytes = serde_json::to_vec(&case["document"]).unwrap();
        let accepted =
            engine::validate_repository_moved(&bytes, DiscoveryLimits::default()).is_ok();
        assert_eq!(accepted, case["valid"], "{}", case["name"]);
    }
}

#[test]
fn preview_corpus_yields_the_recorded_native_outcome() {
    let corpus: Vec<Value> = serde_json::from_str(PREVIEWS).unwrap();
    for case in corpus {
        let bytes = match case.get("hex") {
            Some(hex) => (0..hex.as_str().unwrap().len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex.as_str().unwrap()[i..i + 2], 16).unwrap())
                .collect(),
            None => case["utf8"].as_str().unwrap().as_bytes().to_vec(),
        };
        let outcome = engine::validate_image(case["media_type"].as_str().unwrap(), &bytes);
        match case["error"].as_str() {
            None => assert!(outcome.is_ok(), "{}", case["name"]),
            Some(error) => assert_eq!(
                outcome.err().map(|e| e.code),
                Some(format!("preview.{error}")),
                "{}",
                case["name"]
            ),
        }
    }
}

/// The publish intent of the openalex fixture: its object inventory at the
/// fixture's own locations, one `main` ref, creating the repository.
fn openalex_intent() -> (Value, PublishIntent) {
    let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
    let manifest: Value = serde_json::from_str(MANIFEST).unwrap();
    let mut objects: Vec<ObjectDeclaration> = manifest["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|object| ObjectDeclaration {
            digest: object["digest"].as_str().unwrap().into(),
            length: object["length"].as_u64().unwrap(),
            media_type: object["media_type"].as_str().unwrap().into(),
        })
        .collect();
    objects.sort();
    let repository = json!({"owner": "openalex", "repository": "openalex"});
    let intent_digest = fixture["package_digest"].as_str().unwrap();
    let manifest_validator = fixture["manifest_digest"].as_str().unwrap();
    let input = json!({
        "repository": repository,
        "intent_digest": intent_digest,
        "objects": objects,
        "manifest_validator": manifest_validator,
        "refs": ["main"],
        "expected_revision": null,
    });
    let intent = PublishIntent {
        rewrite: false,
        prune: false,
        repository: serde_json::from_value(repository).unwrap(),
        intent_digest: intent_digest.into(),
        objects,
        manifest_validator: manifest_validator.into(),
        refs: vec!["main".into()],
        expected_revision: None,
    };
    (input, intent)
}

#[test]
fn openalex_fixture_validates_and_commits_to_the_native_commitment() {
    let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
    let limits = DiscoveryLimits::default();

    let manifest = engine::validate_manifest(MANIFEST.as_bytes(), limits).unwrap();
    assert_eq!(manifest.digest, fixture["manifest_digest"]);
    let summary = engine::validate_summary(SUMMARY.as_bytes(), limits).unwrap();
    assert_eq!(summary.digest, fixture["summary_digest"]);
    let refs = engine::validate_refs(REFS.as_bytes(), limits).unwrap();
    assert_eq!(refs.document.refs[0].validator.0, manifest.digest);
    engine::bind_summary(MANIFEST.as_bytes(), SUMMARY.as_bytes(), limits).unwrap();
    engine::bind_refs(REFS.as_bytes(), MANIFEST.as_bytes(), limits).unwrap();

    let (input, intent) = openalex_intent();
    let native = intent.request_commitment();
    assert_eq!(
        engine::request_commitment(&serde_json::to_vec(&input).unwrap()).unwrap(),
        native
    );
    let recorded: Value = serde_json::from_str(COMMITMENT).unwrap();
    assert_eq!(recorded["request_commitment"], native);
}
