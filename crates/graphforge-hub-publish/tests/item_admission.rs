//! Native oracle for the bounded host admission seam and its public WASM corpus.
use graphforge_discovery::{DiscoveryLimits, DiscoveryManifest};
use graphforge_hub_publish::{
    HubPublishErrorCode, ItemAdmissionInput, MAX_ITEM_ADMISSION_BYTES, item_admission_requirements,
    plan_item_admission,
};
use serde_json::{Value, json};
use std::path::PathBuf;

fn input() -> Value {
    let mut manifest: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/hub/generated/v1/manifest.json"
    ))
    .unwrap();
    manifest["ontology"]["modules"][0]["id"] = json!("https://graphforge.sh/ont_aaaaaaaaaaaaaaaa");
    manifest["ontology"]["modules"][0]["version"] = json!("same-version");
    json!({"manifest": manifest, "publisher_account_id": "opaque:publisher",
        "facts": [{"id":"ont_aaaaaaaaaaaaaaaa", "claim":{"state":"unclaimed"},
            "owner_visible":false, "publish_as_owner":false,
            "tuples":[{"version":"same-version",
                "content_digest":manifest["ontology"]["modules"][0]["content_digest"], "published":false}]}]})
}

fn claimed(value: &mut Value, account: &str) {
    value["facts"][0]["claim"] = json!({"state":"claimed", "owner_account_id":account,
        "owner_handle":"current-owner"});
}

fn cases() -> Vec<(&'static str, Value, &'static str)> {
    let mut cases = vec![("new-claim", input(), "new_claim")];
    let mut owned = input();
    claimed(&mut owned, "opaque:publisher");
    cases.push(("owner-same-version-new-digest", owned, "owner_update"));
    let mut reuse = input();
    claimed(&mut reuse, "opaque:foreign");
    reuse["facts"][0]["tuples"][0]["published"] = json!(true);
    cases.push(("foreign-exact-reuse", reuse, "exact_reuse"));
    for (name, visible, permission) in [
        ("hidden-owner", false, false),
        ("visible-owner", true, false),
        ("hidden-owner-org-hint", false, true),
        ("visible-owner-org-hint", true, true),
    ] {
        let mut value = input();
        claimed(&mut value, "opaque:foreign");
        value["facts"][0]["owner_visible"] = json!(visible);
        value["facts"][0]["publish_as_owner"] = json!(permission);
        cases.push((name, value, "id_taken"));
    }
    let mut duplicate_alias = input();
    let mut alias = duplicate_alias["manifest"]["ontology"]["modules"][0].clone();
    alias["id"] = json!("https://graphforge.sh/ONT_AAAAAAAAAAAAAAAA");
    duplicate_alias["manifest"]["ontology"]["modules"]
        .as_array_mut()
        .unwrap()
        .insert(0, alias);
    cases.push(("canonical-alias-one-tuple", duplicate_alias, "new_claim"));
    let mut legacy = input();
    legacy["manifest"]["ontology"]["modules"][0]["id"] = json!("https://example.org/legacy");
    legacy["facts"] = json!([]);
    cases.push(("legacy-iri-ignored", legacy, "empty"));
    let mut bridge = input();
    let mut entry = bridge["manifest"]["ontology"]["modules"][0].clone();
    entry["id"] = json!("https://graphforge.sh/brg_aaaaaaaaaaaaaaaa");
    entry.as_object_mut().unwrap().remove("package");
    bridge["manifest"]["ontology"]["modules"] = json!([]);
    bridge["manifest"]["ontology"]["bridge_sets"] = json!([entry]);
    bridge["facts"][0]["id"] = json!("brg_aaaaaaaaaaaaaaaa");
    cases.push(("bridge-claim", bridge, "new_claim"));
    let mut multiple = input();
    claimed(&mut multiple, "opaque:foreign");
    let mut bridge = multiple["manifest"]["ontology"]["modules"][0].clone();
    bridge["id"] = json!("https://graphforge.sh/brg_aaaaaaaaaaaaaaaa");
    bridge.as_object_mut().unwrap().remove("package");
    multiple["manifest"]["ontology"]["bridge_sets"] = json!([bridge]);
    let mut row = multiple["facts"][0].clone();
    row["id"] = json!("brg_aaaaaaaaaaaaaaaa");
    multiple["facts"].as_array_mut().unwrap().push(row);
    cases.push(("sorted-multiple-kind-refusals", multiple, "id_taken"));
    let mut tuples = input();
    let mut second = tuples["manifest"]["ontology"]["modules"][0].clone();
    second["content_digest"] = json!(format!("sha256:{}", "f".repeat(64)));
    tuples["manifest"]["ontology"]["modules"]
        .as_array_mut()
        .unwrap()
        .push(second.clone());
    let mut row = tuples["facts"][0]["tuples"][0].clone();
    row["content_digest"] = second["content_digest"].clone();
    tuples["facts"][0]["tuples"]
        .as_array_mut()
        .unwrap()
        .push(row);
    cases.push((
        "same-id-version-distinct-digests",
        tuples.clone(),
        "new_claim",
    ));
    claimed(&mut tuples, "opaque:foreign");
    tuples["facts"][0]["tuples"][0]["published"] = json!(true);
    cases.push(("partial-exact-reuse-refuses-whole-plan", tuples, "id_taken"));
    for (name, pointer, replacement) in [
        (
            "wrong-kind",
            "/manifest/ontology/modules/0/id",
            json!("brg_aaaaaaaaaaaaaaaa"),
        ),
        ("missing-fact", "/facts", json!([])),
        ("missing-tuple", "/facts/0/tuples", json!([])),
        (
            "noncanonical-fact",
            "/facts/0/id",
            json!("ONT_AAAAAAAAAAAAAAAA"),
        ),
        ("extra-fact", "/facts/0/id", json!("ont_bbbbbbbbbbbbbbbb")),
        ("missing-explicit-absence", "/facts/0/claim", json!({})),
        (
            "unclaimed-published",
            "/facts/0/tuples/0/published",
            json!(true),
        ),
        (
            "unclaimed-permission",
            "/facts/0/publish_as_owner",
            json!(true),
        ),
        (
            "different-tuple",
            "/facts/0/tuples/0/version",
            json!("not-advertised"),
        ),
        ("empty-account", "/publisher_account_id", json!("")),
        (
            "control-account",
            "/publisher_account_id",
            json!("unsafe\naccount"),
        ),
        (
            "overlong-account",
            "/publisher_account_id",
            json!("a".repeat(4097)),
        ),
        (
            "unknown-field",
            "/facts/0/claim",
            json!({"state":"unclaimed","unexpected":true}),
        ),
    ] {
        let mut value = input();
        *value.pointer_mut(pointer).unwrap() = replacement;
        cases.push((name, value, "invalid_input"));
    }
    let mut absent_bool = input();
    absent_bool["facts"][0]
        .as_object_mut()
        .unwrap()
        .remove("owner_visible");
    cases.push(("missing-explicit-visibility", absent_bool, "invalid_input"));
    let mut visible_missing = input();
    claimed(&mut visible_missing, "opaque:foreign");
    visible_missing["facts"][0]["claim"]
        .as_object_mut()
        .unwrap()
        .remove("owner_handle");
    visible_missing["facts"][0]["owner_visible"] = json!(true);
    cases.push((
        "visible-owner-without-handle",
        visible_missing,
        "invalid_input",
    ));
    let mut long_handle = input();
    claimed(&mut long_handle, "opaque:foreign");
    long_handle["facts"][0]["claim"]["owner_handle"] = json!("a".repeat(101));
    cases.push(("overlong-display-handle", long_handle, "invalid_input"));
    let mut unicode = input();
    unicode["publisher_account_id"] = json!("opaque:é账户");
    cases.push(("opaque-unicode-account", unicode, "new_claim"));
    let mut duplicate = input();
    let fact = duplicate["facts"][0].clone();
    duplicate["facts"].as_array_mut().unwrap().push(fact);
    cases.push(("duplicate-id", duplicate, "invalid_input"));
    let mut duplicate = input();
    let tuple = duplicate["facts"][0]["tuples"][0].clone();
    duplicate["facts"][0]["tuples"]
        .as_array_mut()
        .unwrap()
        .push(tuple);
    cases.push(("duplicate-tuple", duplicate, "invalid_input"));
    cases
}

fn outcome(bytes: &[u8]) -> Result<Value, graphforge_hub_publish::HubPublishError> {
    let parsed = ItemAdmissionInput::from_json(bytes)?;
    Ok(serde_json::to_value(plan_item_admission(&parsed)?).unwrap())
}

#[test]
fn native_admission_oracle_and_checked_in_wasm_corpus() {
    let mut corpus = Vec::new();
    for (name, input, expected) in cases() {
        let result = outcome(&serde_json::to_vec(&input).unwrap());
        let oracle = match result {
            Ok(plan) => {
                assert_ne!(expected, "invalid_input", "{name}");
                assert_ne!(expected, "id_taken", "{name}");
                if expected == "empty" {
                    assert_eq!(plan["decisions"], json!([]));
                } else {
                    assert_eq!(plan["decisions"][0]["action"], expected, "{name}");
                }
                json!({"plan":plan})
            }
            Err(error) => {
                assert_eq!(error.code.as_str(), expected, "{name}: {error:?}");
                let body = serde_json::to_value(error.body()).unwrap();
                if expected == "id_taken" {
                    assert_eq!(error.code.http_status(), 409);
                    assert_eq!(
                        body["items"][0].get("owner").is_some(),
                        input["facts"][0]["owner_visible"] == true
                    );
                    assert_eq!(
                        body["items"][0].get("publish_as_owner").is_some(),
                        input["facts"][0]["publish_as_owner"] == true
                    );
                    assert!(!body.to_string().contains("opaque:foreign"));
                } else {
                    assert!(body.get("items").is_none());
                }
                json!({"error":body})
            }
        };
        corpus.push(json!({"name":name,"input":input,"expected":oracle}));
    }
    artifact(
        "item-admission-conformance.json",
        &json!({"format":"graphforge-item-admission-conformance/1","cases":corpus}),
    );
}

fn artifact(name: &str, value: &Value) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/reference/hub-publish/v1")
        .join(name);
    let encoded = format!("{}\n", serde_json::to_string_pretty(value).unwrap());
    if std::env::var_os("GRAPHFORGE_UPDATE_ITEM_ADMISSION_ARTIFACTS").is_some() {
        std::fs::write(&path, &encoded).unwrap();
    }
    assert_eq!(std::fs::read_to_string(path).unwrap(), encoded);
}

#[test]
fn typed_native_and_json_entrypoints_share_the_encoded_byte_limit() {
    let mut value = input();
    // Each opaque account is valid alone; repeated per-ID owner facts cross the
    // aggregate byte bound while remaining within both ontology count bounds.
    let template = value["manifest"]["ontology"]["modules"][0].clone();
    let fact = value["facts"][0].clone();
    let mut modules = Vec::new();
    let mut facts = Vec::new();
    for n in 0..4096_u32 {
        // Hex payloads are an injective subset of Crockford base32.
        let id = format!("ont_{n:016x}");
        let mut module = template.clone();
        module["id"] = json!(format!("https://graphforge.sh/{id}"));
        modules.push(module);
        let mut row = fact.clone();
        row["id"] = json!(id);
        row["claim"] = json!({"state":"claimed", "owner_account_id":"opaque:publisher"});
        facts.push(row);
    }
    value["manifest"]["ontology"]["modules"] = json!(modules);
    value["facts"] = json!(facts);
    let control: ItemAdmissionInput = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(
        item_admission_requirements(&control.manifest)
            .unwrap()
            .len(),
        control.facts.len()
    );
    assert_eq!(plan_item_admission(&control).unwrap().decisions.len(), 4096);
    value["publisher_account_id"] = json!("z".repeat(4096));
    for fact in value["facts"].as_array_mut().unwrap() {
        fact["claim"]["owner_account_id"] = json!("z".repeat(4096));
    }
    let bytes = serde_json::to_vec(&value).unwrap();
    assert!(bytes.len() > MAX_ITEM_ADMISSION_BYTES);
    let typed: ItemAdmissionInput = serde_json::from_value(value).unwrap();
    assert_eq!(
        item_admission_requirements(&typed.manifest).unwrap().len(),
        typed.facts.len()
    );
    for error in [
        plan_item_admission(&typed).unwrap_err(),
        ItemAdmissionInput::from_json(&bytes).unwrap_err(),
    ] {
        assert_eq!(error.code, HubPublishErrorCode::InvalidInput);
        assert_eq!(
            error.detail(),
            "item admission input exceeds its byte bound"
        );
    }
}

#[test]
fn duplicate_json_keys_and_malformed_unicode_fail_closed() {
    for bytes in [
        b"{\"facts\":[],\"facts\":[]}".as_slice(),
        b"{\"publisher_account_id\":\"\\ud800\"}",
        &[0xff],
    ] {
        assert_eq!(
            ItemAdmissionInput::from_json(bytes).unwrap_err().code,
            HubPublishErrorCode::InvalidInput
        );
    }
    let value = input();
    let manifest: DiscoveryManifest = DiscoveryManifest::from_json(
        &serde_json::to_vec(&value["manifest"]).unwrap(),
        DiscoveryLimits::default(),
    )
    .unwrap();
    assert_eq!(
        item_admission_requirements(&manifest).unwrap()[0].id,
        "ont_aaaaaaaaaaaaaaaa"
    );
}

#[test]
fn native_query_bounds_valid_typed_manifests_before_returning_keys() {
    let mut value = input()["manifest"].clone();
    let template = value["ontology"]["modules"][0].clone();
    let modules: Vec<_> = (0..4096_u32)
        .map(|n| {
            let mut module = template.clone();
            module["id"] = json!(format!("https://graphforge.sh/ont_{n:016x}"));
            module["version"] = json!("v".repeat(4096));
            module
        })
        .collect();
    value["ontology"]["modules"] = json!(modules);
    let manifest: DiscoveryManifest = serde_json::from_value(value).unwrap();
    manifest.validate(DiscoveryLimits::default()).unwrap();
    assert!(
        serde_json::to_vec(&manifest).unwrap().len()
            > DiscoveryLimits::default().max_response_bytes
    );
    let error = item_admission_requirements(&manifest).unwrap_err();
    assert_eq!(error.code, HubPublishErrorCode::InvalidInput);
    assert_eq!(
        error.detail(),
        "item admission manifest exceeds its byte bound"
    );
}

#[test]
fn generated_item_admission_schema_matches_the_native_contract() {
    let text = json!({"type":"string","minLength":1,"maxLength":4096});
    let digest = json!({"type":"string","pattern":"^sha256:[0-9a-f]{64}$"});
    let id = json!({"type":"string","pattern":"^(ont|brg)_[0-9a-hjkmnp-tv-z]{16}$"});
    let kind = json!({"enum":["ontology_module","bridge_set"]});
    let tuple = json!({"type":"object","additionalProperties":false,"required":["version","content_digest"],"properties":{"version":text,"content_digest":digest}});
    let fact_tuple = json!({"type":"object","additionalProperties":false,"required":["version","content_digest","published"],"properties":{"version":text,"content_digest":digest,"published":{"type":"boolean"}}});
    let schema = json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "$id":"https://graphforge.sh/schemas/hub-publish/v1/item-admission.schema.json",
        "title":"Internal host item admission input",
        "description":"Host-owned fresh complete facts only; never client HTTP authorization. Rust enforces UTF-8 byte bounds, control-free stable IDs, canonical query set equality, privacy and policy.",
        "type":"object","additionalProperties":false,
        "required":["manifest","publisher_account_id","facts"],
        "properties":{
            "manifest":{"$ref":"../../discovery/v1/manifest.schema.json"},
            "publisher_account_id":{"$ref":"#/$defs/stableAccount"},
            "facts":{"type":"array","maxItems":8192,"items":{"$ref":"#/$defs/fact"}}
        },
        "$defs":{
            "stableAccount":text,
            "fact":{"type":"object","additionalProperties":false,"required":["id","claim","owner_visible","publish_as_owner","tuples"],"properties":{
                "id":id,"claim":{"oneOf":[
                    {"type":"object","additionalProperties":false,"required":["state"],"properties":{"state":{"const":"unclaimed"}}},
                    {"type":"object","additionalProperties":false,"required":["state","owner_account_id"],"properties":{"state":{"const":"claimed"},"owner_account_id":{"$ref":"#/$defs/stableAccount"},"owner_handle":{"type":"string","minLength":1,"maxLength":100,"pattern":"^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$"}}}
                ]},"owner_visible":{"type":"boolean"},"publish_as_owner":{"type":"boolean"},"tuples":{"type":"array","maxItems":8192,"items":fact_tuple}
            }},
            "requirements":{"type":"array","maxItems":8192,"items":{"type":"object","additionalProperties":false,"required":["id","kind","tuples"],"properties":{"id":id,"kind":kind,"tuples":{"type":"array","maxItems":8192,"items":tuple}}}},
            "plan":{"type":"object","additionalProperties":false,"required":["publisher_account_id","decisions"],"properties":{"publisher_account_id":{"$ref":"#/$defs/stableAccount"},"decisions":{"type":"array","maxItems":8192,"items":{"type":"object","additionalProperties":false,"required":["id","kind","version","content_digest","action"],"properties":{"id":id,"kind":kind,"version":text,"content_digest":digest,"action":{"enum":["new_claim","owner_update","exact_reuse"]}}}}}}
        }
    });
    artifact("item-admission.schema.json", &schema);
}
