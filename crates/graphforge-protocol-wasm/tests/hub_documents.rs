//! Native outcome of every control document the hub-publish v1 corpus carries.
//!
//! The corpus records HTTP responses from a stateful reference Hub, which says
//! nothing about a document the Hub refused for state. This test runs each
//! document through the same engine functions the WebAssembly module exports
//! and records the verdict (`accept` or the error code) in
//! `tests/hub-publish-documents.json`. The Node replay in
//! `packages/protocol-wasm/tests` asserts that file in both directions, so a
//! module that accepts what native Rust refuses (or refuses what it accepts)
//! fails. Without `GRAPHFORGE_UPDATE_PROTOCOL_WASM_EXPECTED` the test compares
//! the checked-in file with a fresh generation.

#![allow(clippy::too_many_lines)]

use graphforge_discovery::DiscoveryLimits;
use graphforge_protocol_wasm::engine::{self, Outcome};
use serde_json::{Value, json};

const CORPUS: &str = include_str!("../../../docs/reference/hub-publish/v1/conformance.json");
const RECORDED: &str = "tests/hub-publish-documents.json";
const UPDATE: &str = "GRAPHFORGE_UPDATE_PROTOCOL_WASM_EXPECTED";

fn verdict<T>(outcome: Outcome<T>) -> String {
    outcome.map_or_else(|error| error.code, |_| "accept".into())
}

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

fn success(expect: &Value) -> bool {
    expect["status"]
        .as_u64()
        .is_some_and(|s| (200..300).contains(&s))
}

/// The capabilities document of the repository `owner/repository`, derived
/// from the corpus's own capabilities response.
fn capabilities(corpus: &Value, owner: &str, repository: &str) -> Value {
    let mut document = corpus["cases"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|case| case["steps"].as_array().unwrap())
        .find(|step| {
            step["request"]["url"]
                .as_str()
                .is_some_and(|url| url.ends_with("/.gf/publish"))
                && step["expect"].get("json").is_some()
        })
        .map(|step| step["expect"]["json"].clone())
        .expect("the corpus carries a capabilities response");
    document["repository"] = json!({"owner": owner, "repository": repository});
    document["authorization"]["scope"] = json!(format!("publish:{owner}/{repository}"));
    document["object_location_template"] = json!(format!(
        "https://hub.example/{owner}/{repository}/.gf/objects/{{digest}}"
    ));
    document
}

/// What each single-document kind validates with.
fn validate(kind: &str, document: &Value) -> String {
    let bytes = bytes(document);
    match kind {
        "capabilities" => verdict(engine::validate_capabilities(&bytes)),
        "session_request" => verdict(engine::validate_session_request(&bytes)),
        "commit_request" => verdict(engine::validate_commit_request(&bytes)),
        "operation_status" => verdict(engine::validate_operation_status(&bytes)),
        "receipt" => verdict(engine::validate_receipt(&bytes)),
        other => panic!("no mutation for {other}"),
    }
}

/// Documents the corpus never refuses, made invalid in one way each: a future
/// format, an unknown member, or a dropped required-nullable member.
fn mutations(corpus: &Value, entries: &[Value]) -> Vec<Value> {
    let mutations = [
        (
            "session_request",
            json!({"set": {"format": "graphforge-hub-publish/2"}}),
        ),
        ("session_request", json!({"set": {"unexpected_member": 1}})),
        ("commit_request", json!({"set": {"unexpected_member": 1}})),
        ("commit_request", json!({"remove": ["expected_revision"]})),
        (
            "capabilities",
            json!({"set": {"format": "graphforge-hub-publish/2"}}),
        ),
        ("capabilities", json!({"set": {"unexpected_member": 1}})),
        (
            "operation_status",
            json!({"set": {"format": "graphforge-hub-publish/2"}}),
        ),
        (
            "receipt",
            json!({"set": {"format": "graphforge-hub-publish/2"}}),
        ),
        ("receipt", json!({"remove": ["previous_revision"]})),
        ("receipt", json!({"set": {"revision": "sha256:ABC"}})),
    ];
    let mut out = Vec::new();
    for (kind, mutation) in mutations {
        let entry = entries
            .iter()
            .find(|entry| entry["kind"] == kind && entry["outcome"] == "accept")
            .unwrap();
        let case = corpus["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == entry["case"])
            .unwrap();
        let mut document = case["steps"][usize::try_from(entry["step"].as_u64().unwrap()).unwrap()]
            .pointer(entry["pointer"].as_str().unwrap())
            .unwrap()
            .clone();
        for (name, value) in mutation["set"].as_object().into_iter().flatten() {
            document[name] = value.clone();
        }
        for name in mutation["remove"].as_array().into_iter().flatten() {
            document
                .as_object_mut()
                .unwrap()
                .remove(name.as_str().unwrap());
        }
        let outcome = validate(kind, &document);
        assert_ne!(outcome, "accept", "{kind} {mutation} must be refused");
        let mut entry = entry.clone();
        entry["mutation"] = mutation;
        entry["outcome"] = json!(outcome);
        out.push(entry);
    }
    out
}

fn generate() -> Value {
    let corpus: Value = serde_json::from_str(CORPUS).unwrap();
    let mut entries = Vec::new();
    for case in corpus["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        // captured session variable -> step index of the accepted open request
        let mut sessions = std::collections::BTreeMap::new();
        for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            let (request, expect) = (&step["request"], &step["expect"]);
            let Some(url) = request["url"].as_str() else {
                continue;
            };
            let method = request["method"].as_str().unwrap();
            let Some(target) = url.strip_prefix("https://hub.example/").and_then(|rest| {
                let mut parts = rest.splitn(3, '/');
                let (owner, repository, tail) = (parts.next()?, parts.next()?, parts.next()?);
                Some((owner, repository, format!("/{tail}")))
            }) else {
                continue;
            };
            let (owner, repository, target) = (target.0, target.1, target.2.as_str());
            let at = |kind: &str, pointer: String, verdict: String| json!({"case": name, "step": index, "kind": kind, "pointer": pointer, "outcome": verdict});
            let document = &request["json"];
            let has_variable = document.to_string().contains("${");

            if method == "GET" && target == "/.gf/publish" && expect.get("json").is_some() {
                let outcome = verdict(engine::validate_capabilities(&bytes(&expect["json"])));
                assert_eq!(outcome, "accept", "{name}#{index}: a Hub response");
                entries.push(at("capabilities", "/expect/json".into(), outcome));
            } else if method == "GET" && target == "/.gf/refs" && expect.get("body_text").is_some()
            {
                let outcome = verdict(engine::validate_refs(
                    expect["body_text"].as_str().unwrap().as_bytes(),
                    DiscoveryLimits::default(),
                ));
                assert_eq!(outcome, "accept", "{name}#{index}: a Hub response");
                entries.push(at("refs", "/expect/body_text".into(), outcome));
            } else if method == "GET"
                && target.starts_with("/.gf/publish/operations/")
                && expect.get("json").is_some()
            {
                let outcome = verdict(engine::validate_operation_status(&bytes(&expect["json"])));
                assert_eq!(outcome, "accept", "{name}#{index}: a Hub response");
                entries.push(at("operation_status", "/expect/json".into(), outcome));
            } else if method == "POST" && target == "/.gf/publish/sessions" && !has_variable {
                let outcome = verdict(engine::validate_session_request(&bytes(document)));
                if success(expect) {
                    assert_eq!(outcome, "accept", "{name}#{index}: the Hub accepted it");
                    for (variable, pointer) in expect["capture"].as_object().into_iter().flatten() {
                        if pointer == "/session_id" {
                            sessions.insert(variable.clone(), index);
                        }
                    }
                }
                entries.push(at("session_request", "/request/json".into(), outcome));
            } else if method == "POST" && target.ends_with("/commit") && !has_variable {
                let outcome = verdict(engine::validate_commit_request(&bytes(document)));
                if success(expect) {
                    assert_eq!(outcome, "accept", "{name}#{index}: the Hub accepted it");
                }
                entries.push(at(
                    "commit_request",
                    "/request/json".into(),
                    outcome.clone(),
                ));
                let variable = target
                    .strip_prefix("/.gf/publish/sessions/${")
                    .and_then(|rest| rest.strip_suffix("}/commit"));
                let open = variable.and_then(|v| sessions.get(v)).copied();
                if let (Some(open), "accept") = (open, outcome.as_str()) {
                    let open_document = &case["steps"][open]["request"]["json"];
                    let manifest = engine::validate_manifest(
                        &bytes(&document["manifest"]),
                        DiscoveryLimits::default(),
                    )
                    .unwrap();
                    let check = verdict(engine::check_commit_manifest(&bytes(&json!({
                        "manifest": manifest.document,
                        "repository": {"owner": owner, "repository": repository},
                        "capabilities": capabilities(&corpus, owner, repository),
                        "inventory": open_document["objects"],
                        "refs": document["refs"],
                    }))));
                    if success(expect) {
                        assert_eq!(check, "accept", "{name}#{index}: the Hub accepted it");
                    }
                    let mut entry = at("commit_manifest", "/request/json".into(), check);
                    entry["open_step"] = json!(open);
                    entries.push(entry);
                }
                if success(expect) && expect["json"].get("receipt").is_some() {
                    let outcome =
                        verdict(engine::validate_receipt(&bytes(&expect["json"]["receipt"])));
                    assert_eq!(outcome, "accept", "{name}#{index}: a Hub response");
                    entries.push(at("receipt", "/expect/json/receipt".into(), outcome));
                }
            }
        }
    }
    entries.extend(mutations(&corpus, &entries));
    json!({
        "format": "graphforge-protocol-wasm-hub-documents/1",
        "corpus": "docs/reference/hub-publish/v1/conformance.json",
        "documents": entries,
    })
}

#[test]
fn every_corpus_document_has_the_recorded_native_verdict() {
    let generated = generate();
    let kinds: std::collections::BTreeSet<_> = generated["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds.len(), 7, "{kinds:?}");
    let refused = generated["documents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["outcome"] != "accept")
        .count();
    assert!(
        refused > 0,
        "the corpus must carry documents native Rust refuses"
    );

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(RECORDED);
    let text = serde_json::to_string_pretty(&generated).unwrap() + "\n";
    if std::env::var_os(UPDATE).is_some() {
        std::fs::write(&path, &text).unwrap();
    }
    let recorded = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        recorded, text,
        "run with {UPDATE}=1 to regenerate {RECORDED}"
    );
}
