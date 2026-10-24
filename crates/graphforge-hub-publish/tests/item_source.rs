//! Native transition oracle; replayed by the real protocol WASM module.
use graphforge_discovery::{DiscoveryManifest, RefSet};
use graphforge_hub_publish::{ItemSourceTransitionInput, plan_item_source_transition};
use serde_json::{Value, json};
use std::path::PathBuf;

fn publication() -> Value {
    let admission: Value = serde_json::from_str(include_str!(
        "../../../docs/reference/hub-publish/v1/item-admission-conformance.json"
    ))
    .unwrap();
    let case = admission["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "new-claim")
        .unwrap();
    let admission = case["input"].clone();
    let mut refs: RefSet = serde_json::from_str(include_str!(
        "../../../tests/fixtures/hub/generated/v1/refs.json"
    ))
    .unwrap();
    let manifest: DiscoveryManifest =
        serde_json::from_value(admission["manifest"].clone()).unwrap();
    for reference in &mut refs.refs {
        if reference.name == manifest.resolved_ref {
            reference.target = manifest.immutable_version.clone();
            reference.validator = manifest.canonical_digest().unwrap();
        }
    }
    json!({"operation":"publication","repository":{"repository_id":"repository:A","repository":manifest.repository,"revision":"binding:1"},"admission":admission,"refs":refs,"item_facts":[{"id":"ont_aaaaaaaaaaaaaaaa","ownership_revision":"ownership:1","lifecycle":"active","source":{"state":"absent"}}]})
}
fn owned(input: &mut Value) {
    input["admission"]["facts"][0]["claim"] =
        json!({"state":"claimed","owner_account_id":"opaque:publisher"});
    input["item_facts"][0]["source"] = json!({"state":"designated","designation":{"id":"ont_aaaaaaaaaaaaaaaa","repository_id":"repository:A","revision":"source:1"}});
}
fn rehome() -> Value {
    let mut publication = publication();
    owned(&mut publication);
    json!({"operation":"rehome","verified_owner_account_id":"opaque:publisher","ownership_fact":publication["admission"]["facts"][0],"current":publication["item_facts"][0],"expected_ownership_revision":"ownership:1","expected_source_revision":"source:1","expected_repository_id":"repository:A","target_repository":{"repository_id":"repository:B","repository":{"owner":"other","repository":"destination"},"revision":"binding:2"}})
}
fn refresh(input: &mut Value) {
    let manifest: DiscoveryManifest =
        serde_json::from_value(input["admission"]["manifest"].clone()).unwrap();
    for reference in input["refs"]["refs"].as_array_mut().unwrap() {
        if reference["name"] == manifest.resolved_ref {
            reference["validator"] = json!(manifest.canonical_digest().unwrap());
            reference["target"] = json!(manifest.immutable_version);
        }
    }
}
fn cases() -> Vec<(&'static str, Value, Option<usize>)> {
    let mut out = vec![
        ("first-owning-publication", publication(), Some(1)),
        ("explicit-owner-rehome", rehome(), Some(1)),
    ];
    let mut unrelated = publication();
    let mut reference = unrelated["refs"]["refs"][0].clone();
    reference["name"] = json!("zz-unrelated");
    reference["target"] = json!(format!("sha256:{}", "0".repeat(64)));
    reference["validator"] = reference["target"].clone();
    unrelated["refs"]["refs"]
        .as_array_mut()
        .unwrap()
        .push(reference);
    out.push((
        "unrelated-ref-target-does-not-bind-this-manifest",
        unrelated,
        Some(1),
    ));
    let mut owner = publication();
    owned(&mut owner);
    out.push(("owning-update-preserves", owner.clone(), Some(0)));
    let mut existing = owner.clone();
    existing["admission"]["facts"][0]["tuples"][0]["published"] = json!(true);
    out.push((
        "republish-existing-tuple-preserves",
        existing.clone(),
        Some(0),
    ));
    existing["admission"]["publisher_account_id"] = json!("foreign-account");
    out.push(("foreign-reuse-preserves", existing, Some(0)));
    let mut elsewhere = owner.clone();
    elsewhere["repository"]["repository_id"] = json!("repository:B");
    elsewhere["repository"]["repository"]["repository"] = json!("elsewhere");
    elsewhere["admission"]["manifest"]["repository"] =
        elsewhere["repository"]["repository"].clone();
    elsewhere["refs"]["repository"] = elsewhere["repository"]["repository"].clone();
    elsewhere["admission"]["manifest"]["ontology"]["modules"][0]["version"] = json!("later/β");
    elsewhere["admission"]["facts"][0]["tuples"][0]["version"] = json!("later/β");
    refresh(&mut elsewhere);
    out.push(("later-version-elsewhere-preserves", elsewhere, Some(0)));
    let mut rename = owner.clone();
    rename["repository"]["repository"]["owner"] = json!("transferred");
    rename["repository"]["revision"] = json!("binding:after-transfer");
    rename["admission"]["manifest"]["repository"] = rename["repository"]["repository"].clone();
    rename["refs"]["repository"] = rename["repository"]["repository"].clone();
    refresh(&mut rename);
    out.push(("repository-rename-transfer-preserves", rename, Some(0)));
    let mut many = publication();
    let mut module = many["admission"]["manifest"]["ontology"]["modules"][0].clone();
    module["version"] = json!("zz-next");
    many["admission"]["manifest"]["ontology"]["modules"]
        .as_array_mut()
        .unwrap()
        .push(module);
    let mut tuple = many["admission"]["facts"][0]["tuples"][0].clone();
    tuple["version"] = json!("zz-next");
    many["admission"]["facts"][0]["tuples"]
        .as_array_mut()
        .unwrap()
        .push(tuple);
    refresh(&mut many);
    out.push(("multiple-tuples-initialize-once-per-id", many, Some(1)));
    let mut bridge = publication();
    let mut b = bridge["admission"]["manifest"]["ontology"]["modules"][0].clone();
    b["id"] = json!("https://graphforge.sh/brg_aaaaaaaaaaaaaaaa");
    b.as_object_mut().unwrap().remove("package");
    bridge["admission"]["manifest"]["ontology"]["modules"] = json!([]);
    bridge["admission"]["manifest"]["ontology"]["bridge_sets"] = json!([b]);
    bridge["admission"]["facts"][0]["id"] = json!("brg_aaaaaaaaaaaaaaaa");
    bridge["item_facts"][0]["id"] = json!("brg_aaaaaaaaaaaaaaaa");
    refresh(&mut bridge);
    out.push(("bridge-source-initialization", bridge, Some(1)));
    let mut legacy = publication();
    legacy["admission"]["manifest"]["ontology"]["modules"][0]["id"] =
        json!("https://example.org/legacy");
    legacy["admission"]["facts"] = json!([]);
    legacy["item_facts"] = json!([]);
    refresh(&mut legacy);
    out.push(("legacy-iri-not-claimed", legacy, Some(0)));
    let mut noop = rehome();
    noop["target_repository"]["repository_id"] = json!("repository:A");
    out.push(("same-stable-repository-is-no-op", noop, Some(0)));
    for (name, base, pointer, value) in [
        (
            "claimed-with-absent-source",
            owner.clone(),
            "/item_facts/0/source",
            json!({"state":"absent"}),
        ),
        (
            "unclaimed-with-designated-source",
            publication(),
            "/item_facts/0/source",
            owner["item_facts"][0]["source"].clone(),
        ),
        (
            "missing-source-fact",
            publication(),
            "/item_facts",
            json!([]),
        ),
        (
            "extra-source-fact",
            publication(),
            "/item_facts/0/id",
            json!("ont_bbbbbbbbbbbbbbbb"),
        ),
        (
            "source-key-mismatch",
            owner.clone(),
            "/item_facts/0/source/designation/id",
            json!("ont_bbbbbbbbbbbbbbbb"),
        ),
        (
            "wrong-postcommit-validator",
            publication(),
            "/refs/refs/0/validator",
            json!(format!("sha256:{}", "0".repeat(64))),
        ),
        (
            "wrong-postcommit-target",
            publication(),
            "/refs/refs/0/target",
            json!(format!("sha256:{}", "0".repeat(64))),
        ),
        (
            "wrong-repository-fact",
            publication(),
            "/repository/repository/repository",
            json!("wrong"),
        ),
        (
            "deleted-publication",
            owner.clone(),
            "/item_facts/0/lifecycle",
            json!("tombstoned"),
        ),
        (
            "stale-ownership-epoch",
            rehome(),
            "/expected_ownership_revision",
            json!("ownership:old"),
        ),
        (
            "ownership-aba",
            rehome(),
            "/current/ownership_revision",
            json!("ownership:3"),
        ),
        (
            "source-aba",
            rehome(),
            "/current/source/designation/revision",
            json!("source:3"),
        ),
        (
            "stale-source-epoch",
            rehome(),
            "/expected_source_revision",
            json!("source:old"),
        ),
        (
            "wrong-expected-repository",
            rehome(),
            "/expected_repository_id",
            json!("repository:C"),
        ),
        (
            "former-owner-after-transfer",
            rehome(),
            "/ownership_fact/claim/owner_account_id",
            json!("new-owner"),
        ),
        (
            "deleted-rehome",
            rehome(),
            "/current/lifecycle",
            json!("tombstoned"),
        ),
        (
            "rehome-key-mismatch",
            rehome(),
            "/ownership_fact/id",
            json!("ont_bbbbbbbbbbbbbbbb"),
        ),
        (
            "empty-target-binding-revision",
            rehome(),
            "/target_repository/revision",
            json!(""),
        ),
        (
            "empty-stable-repository-id",
            publication(),
            "/repository/repository_id",
            json!(""),
        ),
        (
            "control-stable-repository-id",
            publication(),
            "/repository/repository_id",
            json!("repo\nsecret"),
        ),
        (
            "trailing-control-stable-repository-id",
            publication(),
            "/repository/repository_id",
            json!("repository:A\n"),
        ),
        (
            "unknown-lifecycle",
            publication(),
            "/item_facts/0/lifecycle",
            json!("unknown"),
        ),
    ] {
        let mut input = base;
        *input.pointer_mut(pointer).unwrap() = value;
        out.push((name, input, None));
    }
    let mut duplicate = publication();
    let row = duplicate["item_facts"][0].clone();
    duplicate["item_facts"].as_array_mut().unwrap().push(row);
    out.push(("duplicate-source-fact", duplicate, None));
    let mut unknown = publication();
    unknown["unexpected"] = json!(true);
    out.push(("closed-input", unknown, None));
    out
}

fn transition_schema() -> Value {
    fn closed(properties: Value) -> Value {
        let required: Vec<_> = properties.as_object().unwrap().keys().cloned().collect();
        json!({"type":"object","additionalProperties":false,"required":required,"properties":properties})
    }
    let revision = json!({"type":"string","minLength":1,"maxLength":4096,"pattern":r"^[^\u0000-\u001f\u007f-\u009f]+(?![\s\S])"});
    let source_ref = json!({"$ref":"../../discovery/v1/item-source.schema.json"});
    let repo_ref =
        json!({"$ref":"../../discovery/v1/item-source.schema.json#/$defs/repositoryFact"});
    let row = closed(
        json!({"id":{"$ref":"../../discovery/v1/item-source.schema.json#/properties/id"},"ownership_revision":revision,"lifecycle":{"enum":["active","tombstoned"]},"source":{"oneOf":[closed(json!({"state":{"const":"absent"}})),closed(json!({"state":{"const":"designated"},"designation":source_ref}))]}}),
    );
    json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"https://graphforge.sh/schemas/hub-publish/v1/item-source-transition.schema.json","title":"Internal host item source transition","description":"Fresh host facts only. Core validates correlation and policy; the host compares all epochs and applies the plan atomically under its existing lease.","oneOf":[closed(json!({"operation":{"const":"publication"},"admission":{"$ref":"item-admission.schema.json"},"repository":repo_ref,"refs":{"$ref":"../../discovery/v1/refs.schema.json"},"item_facts":{"type":"array","maxItems":8192,"items":row}})),closed(json!({"operation":{"const":"rehome"},"verified_owner_account_id":{"$ref":"item-admission.schema.json#/$defs/stableAccount"},"ownership_fact":{"$ref":"item-admission.schema.json#/$defs/fact"},"current":row,"expected_ownership_revision":revision,"expected_source_revision":revision,"expected_repository_id":revision,"target_repository":repo_ref}))]})
}

#[test]
fn generated_transitions_preserve_sources_and_cas_preconditions() {
    let mut corpus = Vec::new();
    for (name, input, count) in cases() {
        let bytes = serde_json::to_vec(&input).unwrap();
        let result = ItemSourceTransitionInput::from_json(&bytes)
            .and_then(|i| plan_item_source_transition(&i));
        let expected = if let Some(count) = count {
            let plan = result.unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(plan.changes.len(), count, "{name}");
            if input["operation"] == "publication" {
                assert_eq!(
                    plan.preconditions.len(),
                    input["item_facts"].as_array().unwrap().len(),
                    "{name}"
                );
                for pre in &plan.preconditions {
                    assert!(
                        input["item_facts"]
                            .as_array()
                            .unwrap()
                            .contains(&serde_json::to_value(&pre.fact).unwrap())
                    );
                }
            } else {
                assert_eq!(
                    serde_json::to_value(&plan.preconditions[0].fact).unwrap(),
                    input["current"]
                );
            }
            assert_eq!(
                serde_json::to_value(&plan.repository).unwrap(),
                if input["operation"] == "publication" {
                    &input["repository"]
                } else {
                    &input["target_repository"]
                }
                .clone()
            );
            if count == 1 {
                assert_eq!(plan.changes[0].repository_id, plan.repository.repository_id);
            }
            json!({"plan":plan})
        } else {
            let error = result.expect_err(name);
            assert_eq!(
                serde_json::to_value(error.code).unwrap(),
                "invalid_input",
                "{name}"
            );
            json!({"code":error.code})
        };
        corpus.push(
            json!({"name":name,"json":String::from_utf8(bytes).unwrap(),"expected":expected}),
        );
    }
    let input = publication();
    let text = serde_json::to_string(&input).unwrap();
    corpus.push(json!({"name":"duplicate-json-member","json":format!("{},\"operation\":\"publication\"}}",&text[..text.len()-1]),"expected":{"code":"invalid_input"}}));
    let value = json!({"format":"graphforge-item-source-conformance/1","cases":corpus});
    let root =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/reference/hub-publish/v1");
    for (name, value) in [
        ("item-source-conformance.json", value),
        ("item-source-transition.schema.json", transition_schema()),
    ] {
        let bytes = format!("{}\n", serde_json::to_string_pretty(&value).unwrap());
        if std::env::var_os("GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS").is_some() {
            std::fs::write(root.join(name), &bytes).unwrap();
        }
        assert_eq!(
            std::fs::read_to_string(root.join(name)).unwrap(),
            bytes,
            "{name}"
        );
    }
}

#[test]
fn source_inputs_are_bounded_and_revisions_are_not_descriptor_hashes() {
    assert!(ItemSourceTransitionInput::from_json(&vec![b' '; 16 * 1024 * 1024 + 1]).is_err());
    let mut input = rehome();
    input["expected_source_revision"] = json!("x".repeat(4097));
    assert!(plan_item_source_transition(&serde_json::from_value(input).unwrap()).is_err());
    let mut input = rehome();
    input["target_repository"]["revision"] = json!("target-binding:after-aba");
    let plan = plan_item_source_transition(&serde_json::from_value(input).unwrap()).unwrap();
    assert_eq!(plan.repository.revision, "target-binding:after-aba");
}
