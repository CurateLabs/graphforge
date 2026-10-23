//! Deterministic item descriptor artifacts and native conformance evidence.

use graphforge_discovery::{DiscoveryLimits, ItemDescriptor};
use serde_json::{Value, json};
use std::path::PathBuf;

fn document(prefix: &str, kind: &str) -> Value {
    let id = format!("{prefix}_7k2m9x4q8r3t5v1w");
    json!({
        "format":"graphforge-item/1", "id":id, "kind":kind,
        "repository":{"owner":"opendata","repository":"openalex"},
        "module":{"id":format!("https://graphforge.sh/{id}"),"version":"2026.01",
            "content_digest":format!("sha256:{}", "a".repeat(64))},
        "discovery":{"refs_revision":format!("sha256:{}", "b".repeat(64)),
            "manifest_validator":format!("sha256:{}", "c".repeat(64))}
    })
}

fn valid(name: &str, document: Value) -> Value {
    json!({"name":name,"json":serde_json::to_string(&document).unwrap(),
        "expected":{"result":"valid","document":document}})
}

fn invalid(name: &str, text: String, code: &str, field: Option<&str>) -> Value {
    json!({"name":name,"json":text,"expected":{"result":"invalid","code":code,"field":field}})
}

fn corpus() -> Value {
    let base = document("ont", "ontology_module");
    let mut cases = vec![
        valid("ontology-module", base.clone()),
        valid("bridge-set", document("brg", "bridge_set")),
    ];
    for (name, pointer, value, code, field) in [
        (
            "future-format",
            "/format",
            json!("graphforge-item/2"),
            "unsupported_future",
            Some("format"),
        ),
        (
            "uppercase-id",
            "/id",
            json!("ONT_7K2M9X4Q8R3T5V1W"),
            "invalid_identity",
            Some("id"),
        ),
        (
            "uri-in-id",
            "/id",
            base["module"]["id"].clone(),
            "invalid_identity",
            Some("id"),
        ),
        (
            "unknown-prefix",
            "/id",
            json!("abc_7k2m9x4q8r3t5v1w"),
            "invalid_identity",
            Some("id"),
        ),
        (
            "alias-character",
            "/id",
            json!("ont_7k2m9x4q8r3t5v1o"),
            "invalid_identity",
            Some("id"),
        ),
        (
            "unicode-id",
            "/id",
            json!("éx_0000000000000000"),
            "invalid_identity",
            Some("id"),
        ),
        (
            "kind-disagrees",
            "/kind",
            json!("bridge_set"),
            "invalid_identity",
            Some("kind"),
        ),
        (
            "unknown-kind",
            "/kind",
            json!("project"),
            "malformed_response",
            None,
        ),
        (
            "repository-invalid",
            "/repository/owner",
            json!("a..b"),
            "invalid_identity",
            Some("repository"),
        ),
        (
            "different-module-id",
            "/module/id",
            json!("https://graphforge.sh/ont_0000000000000000"),
            "integrity_failure",
            Some("module.id"),
        ),
        (
            "bare-module-id",
            "/module/id",
            base["id"].clone(),
            "integrity_failure",
            Some("module.id"),
        ),
        (
            "uppercase-module-uri",
            "/module/id",
            json!("HTTPS://GRAPHFORGE.SH/ONT_7K2M9X4Q8R3T5V1W"),
            "integrity_failure",
            Some("module.id"),
        ),
        (
            "invalid-content-digest",
            "/module/content_digest",
            json!("sha256:bad"),
            "integrity_failure",
            Some("digest"),
        ),
        (
            "empty-version",
            "/module/version",
            json!(""),
            "limit_exceeded",
            Some("module.version"),
        ),
        (
            "invalid-refs-revision",
            "/discovery/refs_revision",
            json!("sha256:bad"),
            "integrity_failure",
            Some("digest"),
        ),
        (
            "invalid-manifest-validator",
            "/discovery/manifest_validator",
            json!("sha256:bad"),
            "integrity_failure",
            Some("digest"),
        ),
    ] {
        let mut changed = base.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        cases.push(invalid(
            name,
            serde_json::to_string(&changed).unwrap(),
            code,
            field,
        ));
    }
    for (name, pointer) in [
        ("unknown-top-member", ""),
        ("unknown-module-member", "/module"),
        ("unknown-discovery-member", "/discovery"),
    ] {
        let mut changed = base.clone();
        changed.pointer_mut(pointer).unwrap()["extra"] = json!(true);
        cases.push(invalid(
            name,
            serde_json::to_string(&changed).unwrap(),
            "malformed_response",
            None,
        ));
    }
    let mut missing = base.clone();
    missing.as_object_mut().unwrap().remove("discovery");
    cases.push(invalid(
        "missing-discovery-snapshot",
        serde_json::to_string(&missing).unwrap(),
        "malformed_response",
        None,
    ));
    let mut missing = base.clone();
    missing["discovery"]
        .as_object_mut()
        .unwrap()
        .remove("refs_revision");
    cases.push(invalid(
        "missing-refs-revision",
        serde_json::to_string(&missing).unwrap(),
        "malformed_response",
        None,
    ));
    let text = serde_json::to_string(&base).unwrap();
    cases.push(invalid(
        "duplicate-id",
        text.replacen("\"id\":", "\"id\":\"ignored\",\"id\":", 1),
        "malformed_response",
        None,
    ));
    cases.push(invalid(
        "malformed-unicode",
        text.replacen("2026.01", "\\ud800", 1),
        "malformed_response",
        None,
    ));
    cases.push(invalid(
        "trailing-json",
        format!("{text} null"),
        "malformed_response",
        None,
    ));
    for (name, limits, field) in [
        (
            "response-bound",
            json!({"max_response_bytes":1}),
            "response",
        ),
        ("string-bound", json!({"max_string_bytes":5}), "id"),
    ] {
        let mut case = invalid(name, text.clone(), "limit_exceeded", Some(field));
        case["limits"] = limits;
        cases.push(case);
    }
    json!({"format":"graphforge-item-conformance/1","cases":cases})
}

fn schema() -> Value {
    let alphabet = "[0-9a-hjkmnp-tv-z]{16}";
    let mut schema = json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "$id":"https://graphforge.sh/schemas/discovery/v1/item.schema.json",
        "type":"object","additionalProperties":false,
        "required":["format","id","kind","repository","module","discovery"],
        "properties":{
            "format":{"const":"graphforge-item/1"},
            "id":{"type":"string","pattern":format!("^(ont|brg)_{alphabet}$")},
            "kind":{"enum":["ontology_module","bridge_set"]},
            "repository":{"$ref":"#/$defs/repository"},
            "discovery":{"type":"object","additionalProperties":false,"required":["refs_revision","manifest_validator"],"properties":{
                "refs_revision":{"$ref":"#/$defs/digest"},"manifest_validator":{"$ref":"#/$defs/digest"}}},
            "module":{"type":"object","additionalProperties":false,
                "required":["id","version","content_digest"],"properties":{
                    "id":{"type":"string","pattern":format!("^https://graphforge\\.sh/(ont|brg)_{alphabet}$")},
                    "version":{"type":"string","minLength":1,"maxLength":4096},
                    "content_digest":{"type":"string","pattern":"^sha256:[0-9a-f]{64}$"}}}},
        "$defs":{
            "digest":{"type":"string","pattern":"^sha256:[0-9a-f]{64}$"},
            "repository":{"type":"object","additionalProperties":false,"required":["owner","repository"],"properties":{
                "owner":{"$ref":"#/$defs/slug"},"repository":{"$ref":"#/$defs/slug"}}},
            "slug":{"type":"string","minLength":1,"maxLength":100,
                "pattern":"^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$","not":{"pattern":r"\.\."}}}
    });
    schema["allOf"] = Value::Array([
        ("ont", "ontology_module"),
        ("brg", "bridge_set"),
    ].into_iter().map(|(prefix, kind)| json!({
        "if":{"properties":{"kind":{"const":kind}}},
        "then":{"properties":{
            "id":{"pattern":format!("^{prefix}_{alphabet}$")},
            "module":{"properties":{"id":{"pattern":format!("^https://graphforge\\.sh/{prefix}_{alphabet}$")}}}}}
    })).collect());
    schema
}

#[test]
fn item_artifacts_match_rust_authority_and_native_parser() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/reference/discovery/v1");
    let corpus = corpus();
    for (name, artifact) in [
        ("item.schema.json", schema()),
        ("item-conformance.json", corpus.clone()),
    ] {
        let bytes = format!("{}\n", serde_json::to_string_pretty(&artifact).unwrap());
        if std::env::var_os("GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS").is_some() {
            std::fs::write(root.join(name), &bytes).unwrap();
        } else {
            assert_eq!(
                std::fs::read(root.join(name)).unwrap(),
                bytes.as_bytes(),
                "stale {name}"
            );
        }
    }
    for case in corpus["cases"].as_array().unwrap() {
        let overrides = &case["limits"];
        let limits = DiscoveryLimits {
            max_response_bytes: overrides["max_response_bytes"]
                .as_u64()
                .map_or(DiscoveryLimits::default().max_response_bytes, |n| {
                    n as usize
                }),
            max_string_bytes: overrides["max_string_bytes"]
                .as_u64()
                .map_or(DiscoveryLimits::default().max_string_bytes, |n| n as usize),
            ..DiscoveryLimits::default()
        };
        let outcome = ItemDescriptor::from_json(case["json"].as_str().unwrap().as_bytes(), limits);
        let expected = &case["expected"];
        if expected["result"] == "valid" {
            let descriptor = outcome.unwrap();
            assert_eq!(
                serde_json::to_value(&descriptor).unwrap(),
                expected["document"],
                "{}",
                case["name"]
            );
            assert_eq!(
                ItemDescriptor::from_json(&descriptor.to_canonical_json().unwrap(), limits)
                    .unwrap(),
                descriptor
            );
        } else {
            let error = outcome.unwrap_err();
            assert_eq!(
                serde_json::to_value(error.code).unwrap(),
                expected["code"],
                "{}",
                case["name"]
            );
            assert_eq!(
                serde_json::to_value(error.field).unwrap(),
                expected["field"],
                "{}",
                case["name"]
            );
        }
    }
}
