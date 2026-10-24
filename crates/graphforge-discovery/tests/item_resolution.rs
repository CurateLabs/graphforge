//! Native authority and generated conformance for digest-free exact versions.
use graphforge_discovery::{
    DiscoveryError, DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest,
    ITEM_VERSION_ABSENCE_FIELD, ITEM_VERSION_AMBIGUITY_FIELD, ITEM_VERSION_GONE_FIELD,
    ItemDescriptor, ItemDiscoverySnapshot, ItemId, ItemRepositoryFact, ItemSourceDesignation,
    ItemVersionInventory, ItemVersionState, MAX_ITEM_VERSION_CANDIDATES, RefSet, Sha256Digest,
    StableRepositoryId, bind_item_version_source, bind_item_version_source_json,
    item_resolution_error_response, item_resolution_error_response_json,
    parse_item_resolution_refusal, resolve_item_version,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

fn fixture() -> (ItemDescriptor, RefSet, DiscoveryManifest) {
    let limits = DiscoveryLimits::default();
    let refs = RefSet::from_json(
        include_bytes!("../../../tests/fixtures/hub/generated/v1/refs.json"),
        limits,
    )
    .unwrap();
    let manifest = DiscoveryManifest::from_json(
        include_bytes!("../../../tests/fixtures/hub/generated/v1/manifest.json"),
        limits,
    )
    .unwrap();
    let module = manifest.ontology.as_ref().unwrap().modules[0].identity();
    let id = ItemId::parse_reference(&module.id).unwrap();
    let descriptor = ItemDescriptor {
        format: "graphforge-item/1".into(),
        id: id.as_str().into(),
        kind: id.kind(),
        repository: manifest.repository.clone(),
        module,
        discovery: ItemDiscoverySnapshot {
            refs_revision: revision(&refs),
            manifest_validator: manifest.canonical_digest().unwrap(),
        },
    };
    (descriptor, refs, manifest)
}

fn revision(refs: &RefSet) -> Sha256Digest {
    let hex: String = Sha256::digest(refs.to_canonical_json().unwrap())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Sha256Digest(format!("sha256:{hex}"))
}

fn inventory(candidates: Vec<ItemDescriptor>) -> ItemVersionInventory {
    let descriptor = fixture().0;
    ItemVersionInventory {
        id: descriptor.id,
        version: descriptor.module.version,
        complete: true,
        state: if candidates.is_empty() {
            ItemVersionState::AbsentOrHidden {}
        } else {
            ItemVersionState::Published {
                designation: ItemSourceDesignation {
                    id: fixture().0.id,
                    repository_id: StableRepositoryId::new("canonical").unwrap(),
                    revision: "source:1".into(),
                },
            }
        },
        repositories: candidates
            .iter()
            .map(|d| d.repository.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|repository| ItemRepositoryFact {
                repository_id: StableRepositoryId::new(if repository == fixture().0.repository {
                    "canonical".into()
                } else {
                    repository.canonical_name()
                })
                .unwrap(),
                repository,
                revision: "repo:1".into(),
            })
            .collect(),
        candidates,
    }
}

fn refusal(code: &str, field: Option<&str>, status: u16, body: &str) -> Value {
    json!({"code":code,"field":field,"status":status,"body":body})
}

fn case(name: &str, input: &Value, expected: Value) -> Value {
    json!({"name":name,"json":serde_json::to_string(input).unwrap(),"expected":expected})
}

fn corpus() -> Value {
    let descriptor = fixture().0;
    let base = serde_json::to_value(inventory(vec![descriptor.clone()])).unwrap();
    let identity = serde_json::to_value(&descriptor.module).unwrap();
    let mut absent = base.clone();
    absent["candidates"] = json!([]);
    absent["repositories"] = json!([]);
    absent["state"] = json!({"state":"absent_or_hidden"});
    let mut other = descriptor.clone();
    other.repository.repository = "equivalent".into();
    other.discovery.refs_revision = Sha256Digest(format!("sha256:{}", "e".repeat(64)));
    other.discovery.manifest_validator = Sha256Digest(format!("sha256:{}", "f".repeat(64)));
    let copies = serde_json::to_value(inventory(vec![
        descriptor.clone(),
        other,
        descriptor.clone(),
    ]))
    .unwrap();
    let mut ambiguous = copies.clone();
    ambiguous["candidates"][1]["module"]["content_digest"] =
        json!(format!("sha256:{}", "0".repeat(64)));
    let mut cases = vec![
        case(
            "empty",
            &absent,
            refusal(
                "missing_object",
                Some(ITEM_VERSION_ABSENCE_FIELD),
                404,
                r#"{"code":"missing_object"}"#,
            ),
        ),
        case("one", &base, json!({"identity":identity})),
        case("equivalent-copies", &copies, json!({"identity":identity})),
        case(
            "distinct-digests",
            &ambiguous,
            refusal(
                "ambiguous_identity",
                Some(ITEM_VERSION_AMBIGUITY_FIELD),
                409,
                r#"{"code":"ambiguous_identity"}"#,
            ),
        ),
    ];
    let mut gone = absent.clone();
    gone["state"] = json!({"state":"tombstoned"});
    cases.push(case(
        "visible-tombstone",
        &gone,
        refusal(
            "gone",
            Some(ITEM_VERSION_GONE_FIELD),
            410,
            r#"{"code":"gone"}"#,
        ),
    ));
    let mut unavailable = base.clone();
    unavailable["candidates"] = json!([]);
    unavailable["repositories"] = json!([]);
    cases.push(case(
        "published-empty-is-unavailable",
        &unavailable,
        refusal("integrity_failure", Some("item_version_source"), 503, ""),
    ));
    let mut mirror_only = copies.clone();
    mirror_only["candidates"] = json!([copies["candidates"][1]]);
    mirror_only["repositories"] = json!([copies["repositories"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["repository_id"] != "canonical")
        .unwrap()]);
    cases.push(case(
        "mirror-only-is-unavailable",
        &mirror_only,
        refusal("integrity_failure", Some("item_version_source"), 503, ""),
    ));
    let mut ambiguous_without_source = ambiguous.clone();
    ambiguous_without_source["state"]["designation"]["repository_id"] =
        json!("unavailable-repository");
    cases.push(case(
        "ambiguity-before-source-availability",
        &ambiguous_without_source,
        refusal(
            "ambiguous_identity",
            Some(ITEM_VERSION_AMBIGUITY_FIELD),
            409,
            r#"{"code":"ambiguous_identity"}"#,
        ),
    ));
    for (name, pointer, value) in [
        ("missing-repository-fact", "/repositories", json!([])),
        (
            "wrong-repository-path",
            "/repositories/0/repository/repository",
            json!("wrong"),
        ),
        (
            "wrong-designation-id",
            "/state/designation/id",
            json!("ont_bbbbbbbbbbbbbbbb"),
        ),
        (
            "empty-source-revision",
            "/state/designation/revision",
            json!(""),
        ),
        (
            "hidden-with-candidates",
            "/state",
            json!({"state":"absent_or_hidden"}),
        ),
        (
            "tombstone-with-candidates",
            "/state",
            json!({"state":"tombstoned"}),
        ),
    ] {
        let mut input = base.clone();
        *input.pointer_mut(pointer).unwrap() = value;
        let error = ItemVersionInventory::from_json(
            &serde_json::to_vec(&input).unwrap(),
            DiscoveryLimits::default(),
        )
        .and_then(|i| resolve_item_version(&i, DiscoveryLimits::default()))
        .unwrap_err();
        cases.push(case(
            name,
            &input,
            refusal(
                serde_json::to_value(error.code).unwrap().as_str().unwrap(),
                error.field,
                503,
                "",
            ),
        ));
    }
    let mut duplicated = base.clone();
    duplicated["repositories"]
        .as_array_mut()
        .unwrap()
        .push(base["repositories"][0].clone());
    cases.push(case(
        "duplicate-repository-fact",
        &duplicated,
        refusal(
            "malformed_response",
            Some("item_version_inventory"),
            503,
            "",
        ),
    ));
    let mut renamed = base.clone();
    renamed["repositories"][0]["repository"]["owner"] = json!("transferred");
    renamed["repositories"][0]["revision"] = json!("binding:after-transfer");
    renamed["candidates"][0]["repository"] = renamed["repositories"][0]["repository"].clone();
    cases.push(case(
        "stable-source-survives-repository-transfer",
        &renamed,
        json!({"identity":identity}),
    ));
    let mut rehomed_missing = base.clone();
    rehomed_missing["state"]["designation"]["repository_id"] = json!("repository:after-rehome");
    cases.push(case(
        "rehomed-target-missing-version-no-fallback",
        &rehomed_missing,
        refusal("integrity_failure", Some("item_version_source"), 503, ""),
    ));
    let mut bridge = base.clone();
    bridge["id"] = json!("brg_aaaaaaaaaaaaaaaa");
    bridge["state"]["designation"]["id"] = bridge["id"].clone();
    bridge["candidates"][0]["id"] = bridge["id"].clone();
    bridge["candidates"][0]["kind"] = json!("bridge_set");
    bridge["candidates"][0]["module"]["id"] = json!("https://graphforge.sh/brg_aaaaaaaaaaaaaaaa");
    cases.push(case(
        "bridge-identity-remains-descriptive",
        &bridge,
        json!({"identity":bridge["candidates"][0]["module"]}),
    ));
    for version in [
        "01",
        "1.0+draft",
        "draft/β?x#y",
        ".",
        "..",
        " e\u{301} ",
        "\u{0000}",
    ] {
        let mut input = base.clone();
        input["version"] = json!(version);
        input["candidates"][0]["module"]["version"] = json!(version);
        let mut expected = identity.clone();
        expected["version"] = json!(version);
        cases.push(case(
            &format!("opaque-version-{}", cases.len()),
            &input,
            json!({"identity":expected}),
        ));
    }
    // The first two publications are individually valid and have distinct
    // digests. An invalid later record must prevent an early ambiguity verdict.
    let a = ambiguous["candidates"][0].clone();
    let b = ambiguous["candidates"][1].clone();
    let mut invalid = a.clone();
    invalid["module"]["version"] = json!("SECRET wrong authored version");
    let candidates = [a, b, invalid];
    for (name, order) in [
        ("invalid-last-after-distinct-digests", [0, 1, 2]),
        ("invalid-last-after-reversed-distinct-digests", [1, 0, 2]),
        ("invalid-middle-between-distinct-digests", [0, 2, 1]),
        (
            "invalid-middle-between-reversed-distinct-digests",
            [1, 2, 0],
        ),
        ("invalid-first-before-distinct-digests", [2, 0, 1]),
        ("invalid-first-before-reversed-distinct-digests", [2, 1, 0]),
    ] {
        let mut input = ambiguous.clone();
        input["candidates"] = json!(order.map(|index| candidates[index].clone()));
        let mut entry = case(
            name,
            &input,
            refusal(
                "malformed_response",
                Some("item_version_inventory"),
                503,
                "",
            ),
        );
        entry["bind_source"] = json!(true);
        cases.push(entry);
    }
    for (name, pointer, value, code, field) in [
        (
            "incomplete-empty",
            "/complete",
            json!(false),
            "malformed_response",
            Some("item_version_inventory"),
        ),
        (
            "unknown-member",
            "/unexpected",
            json!(true),
            "malformed_response",
            None,
        ),
        (
            "empty-version",
            "/version",
            json!(""),
            "limit_exceeded",
            Some("item_version_inventory.version"),
        ),
        (
            "uppercase-id",
            "/id",
            json!(descriptor.id.to_ascii_uppercase()),
            "malformed_response",
            Some("item_version_inventory"),
        ),
        (
            "wrong-version",
            "/candidates/0/module/version",
            json!("another"),
            "malformed_response",
            Some("item_version_inventory"),
        ),
        (
            "bad-digest",
            "/candidates/0/module/content_digest",
            json!("bad"),
            "integrity_failure",
            Some("digest"),
        ),
    ] {
        let mut input = if name == "incomplete-empty" {
            absent.clone()
        } else {
            base.clone()
        };
        if pointer == "/unexpected" {
            input["unexpected"] = value;
        } else {
            *input.pointer_mut(pointer).unwrap() = value;
        }
        cases.push(case(name, &input, refusal(code, field, 503, "")));
    }
    let mut missing_complete = base.clone();
    missing_complete.as_object_mut().unwrap().remove("complete");
    cases.push(case(
        "missing-completeness",
        &missing_complete,
        refusal("malformed_response", None, 503, ""),
    ));
    let mut duplicate = case(
        "duplicate-json-member",
        &base,
        refusal("malformed_response", None, 503, ""),
    );
    let text = serde_json::to_string(&base).unwrap();
    duplicate["json"] = json!(format!("{},\"complete\":true}}", &text[..text.len() - 1]));
    cases.push(duplicate);
    cases.push(json!({"name":"response-budget","json":serde_json::to_string(&base).unwrap(),"limits":{"max_response_bytes":1},"expected":refusal("limit_exceeded",Some("response"),503,"")}));
    json!({"format":"graphforge-item-resolution-conformance/1","cases":cases})
}

fn assert_refusal(error: &DiscoveryError, expected: &Value, name: &Value) {
    assert_eq!(
        serde_json::to_value(error.code).unwrap(),
        expected["code"],
        "{name}"
    );
    assert_eq!(
        serde_json::to_value(error.field).unwrap(),
        expected["field"],
        "{name}"
    );
    let response = error.item_resolution_http_response();
    assert_eq!(
        u64::from(response.status),
        expected["status"].as_u64().unwrap(),
        "{name}"
    );
    assert_eq!(response.body, expected["body"].as_str().unwrap(), "{name}");
    assert!(!error.to_string().contains("SECRET"), "{name}");
}

fn source_schema() -> Value {
    let text = json!({"type":"string","minLength":1,"maxLength":4096,"pattern":r"^[^\u0000-\u001f\u007f-\u009f]+$"});
    json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"https://graphforge.sh/schemas/discovery/v1/item-source.schema.json","title":"Internal host permanent-item source designation","description":"Opaque host keys and ABA-safe revisions; Rust enforces UTF-8 byte limits, canonical ID equality and correlated fresh facts.","type":"object","additionalProperties":false,"required":["id","repository_id","revision"],"properties":{"id":{"type":"string","pattern":"^(ont|brg)_[0-9a-hjkmnp-tv-z]{16}$"},"repository_id":text,"revision":text},"$defs":{"repositoryFact":{"type":"object","additionalProperties":false,"required":["repository_id","repository","revision"],"properties":{"repository_id":text,"revision":text,"repository":{"$ref":"item.schema.json#/properties/repository"}}}}})
}
fn inventory_schema() -> Value {
    json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"https://graphforge.sh/schemas/discovery/v1/item-resolution.schema.json","title":"Internal host item-version inventory","type":"object","additionalProperties":false,"required":["id","version","complete","state","repositories","candidates"],"properties":{"id":{"$ref":"item-source.schema.json#/properties/id"},"version":{"type":"string","minLength":1,"maxLength":4096},"complete":{"type":"boolean"},"state":{"oneOf":[{"type":"object","additionalProperties":false,"required":["state"],"properties":{"state":{"enum":["absent_or_hidden","tombstoned"]}}},{"type":"object","additionalProperties":false,"required":["state","designation"],"properties":{"state":{"const":"published"},"designation":{"$ref":"item-source.schema.json"}}}]},"repositories":{"type":"array","maxItems":4096,"items":{"$ref":"item-source.schema.json#/$defs/repositoryFact"}},"candidates":{"type":"array","maxItems":4096,"items":{"$ref":"item.schema.json"}}}})
}

#[test]
fn generated_contract_and_native_resolution_agree() {
    let corpus = corpus();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/reference/discovery/v1");
    let schema = json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"https://graphforge.sh/schemas/discovery/v1/item-resolution-error.schema.json","title":"item-resolution-error","type":"object","additionalProperties":false,"required":["code"],"properties":{"code":{"enum":["missing_object","ambiguous_identity","gone"]}}});
    let source_schema = source_schema();
    let inventory_schema = inventory_schema();
    for (name, value) in [
        ("item-source.schema.json", &source_schema),
        ("item-resolution.schema.json", &inventory_schema),
        ("item-resolution-conformance.json", &corpus),
        ("item-resolution-error.schema.json", &schema),
    ] {
        let bytes = format!("{}\n", serde_json::to_string_pretty(value).unwrap());
        if std::env::var_os("GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS").is_some() {
            std::fs::write(root.join(name), bytes).unwrap();
        } else {
            assert_eq!(
                std::fs::read_to_string(root.join(name)).unwrap(),
                bytes,
                "stale {name}"
            );
        }
    }
    for case in corpus["cases"].as_array().unwrap() {
        let mut limits = DiscoveryLimits::default();
        if let Some(max) = case["limits"]["max_response_bytes"].as_u64() {
            limits.max_response_bytes = usize::try_from(max).unwrap();
        }
        let result =
            ItemVersionInventory::from_json(case["json"].as_str().unwrap().as_bytes(), limits)
                .and_then(|input| resolve_item_version(&input, limits));
        let expected = &case["expected"];
        match result {
            Ok(identity) => assert_eq!(
                serde_json::to_value(identity).unwrap(),
                expected["identity"],
                "{}",
                case["name"]
            ),
            Err(error) => assert_refusal(&error, expected, &case["name"]),
        }
        if case["bind_source"] == true {
            // Deserialize without the validating JSON constructor as well: the
            // typed primitive itself must validate every candidate before grouping.
            let input: ItemVersionInventory =
                serde_json::from_str(case["json"].as_str().unwrap()).unwrap();
            let (source, refs, manifest) = fixture();
            let mut valid_pair = input.clone();
            valid_pair
                .candidates
                .retain(|candidate| candidate.module.version == valid_pair.version);
            assert_eq!(valid_pair.candidates.len(), 2);
            assert_eq!(
                resolve_item_version(&valid_pair, limits).unwrap_err().code,
                DiscoveryErrorCode::AmbiguousIdentity
            );
            let typed_error = resolve_item_version(&input, limits).unwrap_err();
            let binding_error =
                bind_item_version_source(&input, &source, &refs, &manifest, limits).unwrap_err();
            let json_binding_error = bind_item_version_source_json(
                case["json"].as_str().unwrap().as_bytes(),
                &serde_json::to_vec(&source).unwrap(),
                &serde_json::to_vec(&refs).unwrap(),
                &serde_json::to_vec(&manifest).unwrap(),
                limits,
            )
            .unwrap_err();
            for error in [typed_error, binding_error, json_binding_error] {
                assert_refusal(&error, expected, &case["name"]);
            }
        }
    }
}

#[test]
fn every_permutation_has_the_same_identity_or_refusal() {
    let (a, _, _) = fixture();
    let mut b = a.clone();
    b.repository.repository = "other".into();
    for different in [false, true] {
        let mut c = a.clone();
        c.discovery.refs_revision = Sha256Digest(format!("sha256:{}", "e".repeat(64)));
        if different {
            c.module.content_digest = Sha256Digest(format!("sha256:{}", "f".repeat(64)));
        }
        for candidates in [
            vec![a.clone(), b.clone(), c.clone()],
            vec![a.clone(), c.clone(), b.clone()],
            vec![b.clone(), a.clone(), c.clone()],
            vec![b.clone(), c.clone(), a.clone()],
            vec![c.clone(), a.clone(), b.clone()],
            vec![c.clone(), b.clone(), a.clone()],
        ] {
            let result = resolve_item_version(&inventory(candidates), DiscoveryLimits::default());
            if different {
                assert_eq!(
                    result.unwrap_err().code,
                    DiscoveryErrorCode::AmbiguousIdentity
                );
            } else {
                assert_eq!(result.unwrap(), a.module);
            }
        }
    }
}

#[test]
fn candidate_and_aggregate_and_utf8_bounds_apply_before_deduplication() {
    let (descriptor, refs, manifest) = fixture();
    let mut input = inventory(vec![descriptor.clone(); MAX_ITEM_VERSION_CANDIDATES]);
    resolve_item_version(&input, DiscoveryLimits::default()).unwrap();
    input.candidates.push(descriptor.clone());
    assert_eq!(
        resolve_item_version(&input, DiscoveryLimits::default())
            .unwrap_err()
            .code,
        DiscoveryErrorCode::LimitExceeded
    );
    let mut input = inventory(vec![descriptor.clone()]);
    input.version = "β".repeat(2048);
    input.candidates[0].module.version = input.version.clone();
    resolve_item_version(&input, DiscoveryLimits::default()).unwrap();
    input.version.push('a');
    assert_eq!(
        resolve_item_version(&input, DiscoveryLimits::default())
            .unwrap_err()
            .code,
        DiscoveryErrorCode::LimitExceeded
    );
    let input = inventory(vec![descriptor.clone()]);
    let size = serde_json::to_vec(&input).unwrap().len();
    let limits = DiscoveryLimits {
        max_response_bytes: size,
        ..DiscoveryLimits::default()
    };
    resolve_item_version(&input, limits).unwrap();
    assert_eq!(
        bind_item_version_source(&input, &descriptor, &refs, &manifest, limits)
            .unwrap_err()
            .code,
        DiscoveryErrorCode::LimitExceeded
    );
}

#[test]
fn eligible_host_facts_change_results_without_hidden_publication_disclosure() {
    let (a, _, _) = fixture();
    let mut b = a.clone();
    b.module.content_digest = Sha256Digest(format!("sha256:{}", "f".repeat(64)));
    // These booleans are host facts, not a Core authorization implementation.
    let eligible = |readable_a, retained_a, readable_b| {
        inventory(
            [
                (a.clone(), readable_a && retained_a),
                (b.clone(), readable_b),
            ]
            .into_iter()
            .filter_map(|(d, eligible)| eligible.then_some(d))
            .collect(),
        )
    };
    let limits = DiscoveryLimits::default();
    assert_eq!(
        resolve_item_version(&eligible(true, true, false), limits).unwrap(),
        a.module
    );
    assert_eq!(
        resolve_item_version(&eligible(true, true, true), limits)
            .unwrap_err()
            .code,
        DiscoveryErrorCode::AmbiguousIdentity
    );
    assert_eq!(
        resolve_item_version(&eligible(false, true, true), limits).unwrap(),
        b.module
    );
    let absent = resolve_item_version(&inventory(vec![]), limits)
        .unwrap_err()
        .item_resolution_http_response();
    assert_eq!(
        resolve_item_version(&eligible(false, true, false), limits)
            .unwrap_err()
            .item_resolution_http_response(),
        absent
    );
    assert_eq!(
        resolve_item_version(&eligible(true, false, false), limits)
            .unwrap_err()
            .item_resolution_http_response(),
        absent
    );
}

#[test]
fn designated_repository_binds_but_mirrors_splicing_or_unlisted_sources_do_not() {
    let (a, refs_a, manifest_a) = fixture();
    let mut b = a.clone();
    b.repository.repository = "equivalent".into();
    let mut refs_b = refs_a.clone();
    refs_b.repository = b.repository.clone();
    let mut manifest_b = manifest_a.clone();
    manifest_b.repository = b.repository.clone();
    manifest_b.immutable_version = Sha256Digest(format!("sha256:{}", "e".repeat(64)));
    manifest_b.ontology.as_mut().unwrap().modules[0]
        .package
        .as_mut()
        .unwrap()
        .package_digest = Sha256Digest(format!("sha256:{}", "f".repeat(64)));
    b.discovery.manifest_validator = manifest_b.canonical_digest().unwrap();
    for reference in &mut refs_b.refs {
        reference.target = manifest_b.immutable_version.clone();
        reference.validator = b.discovery.manifest_validator.clone();
    }
    b.discovery.refs_revision = revision(&refs_b);
    let input = inventory(vec![a.clone(), b.clone(), a.clone()]);
    let limits = DiscoveryLimits::default();
    assert_eq!(
        bind_item_version_source(&input, &a, &refs_a, &manifest_a, limits).unwrap(),
        a
    );
    assert_eq!(
        bind_item_version_source(&input, &b, &refs_b, &manifest_b, limits)
            .unwrap_err()
            .item_resolution_http_response()
            .status,
        503
    );
    assert_eq!(
        resolve_item_version(&inventory(vec![b.clone()]), limits)
            .unwrap_err()
            .item_resolution_http_response()
            .status,
        503
    );
    // New retained pins in the SAME stable repository are ordinary witnesses.
    let mut same = b.clone();
    same.repository = a.repository.clone();
    let mut same_refs = refs_b.clone();
    same_refs.repository = a.repository.clone();
    let mut same_manifest = manifest_b.clone();
    same_manifest.repository = a.repository.clone();
    same.discovery.manifest_validator = same_manifest.canonical_digest().unwrap();
    for reference in &mut same_refs.refs {
        reference.validator = same.discovery.manifest_validator.clone();
    }
    same.discovery.refs_revision = revision(&same_refs);
    let same_input = inventory(vec![a.clone(), same.clone()]);
    for (d, r, m) in [
        (&a, &refs_a, &manifest_a),
        (&same, &same_refs, &same_manifest),
    ] {
        assert_eq!(
            bind_item_version_source(&same_input, d, r, m, limits).unwrap(),
            *d
        );
    }
    for (d, r, m) in [
        (&a, &refs_b, &manifest_a),
        (&b, &refs_b, &manifest_a),
        (&a, &refs_a, &manifest_b),
    ] {
        assert_eq!(
            bind_item_version_source(&input, d, r, m, limits)
                .unwrap_err()
                .item_resolution_http_response()
                .status,
            503
        );
    }
    assert_eq!(
        bind_item_version_source(
            &inventory(vec![a.clone()]),
            &b,
            &refs_b,
            &manifest_b,
            limits
        )
        .unwrap_err()
        .item_resolution_http_response()
        .status,
        503
    );
    // All current refs can advance; the original actual retained documents still bind.
    assert_eq!(
        bind_item_version_source(&input, &a, &refs_a, &manifest_a, limits).unwrap(),
        a
    );
    let mut spliced = b.clone();
    spliced.discovery.refs_revision = a.discovery.refs_revision;
    assert!(bind_item_version_source(&input, &spliced, &refs_b, &manifest_b, limits).is_err());
}

#[test]
fn nested_missing_object_is_unavailable_not_absence() {
    let (descriptor, refs, manifest) = fixture();
    let input = inventory(vec![descriptor.clone()]);
    let limits = DiscoveryLimits::default();
    for mutation in [0, 1] {
        let mut broken = manifest.clone();
        if mutation == 0 {
            broken.objects.clear();
        } else {
            let digest = &broken.ontology.as_ref().unwrap().modules[0]
                .package
                .as_ref()
                .unwrap()
                .object_digest;
            broken.objects.retain(|o| &o.digest != digest);
        }
        let error =
            bind_item_version_source(&input, &descriptor, &refs, &broken, limits).unwrap_err();
        assert_eq!(error.code, DiscoveryErrorCode::MissingObject);
        assert_ne!(error.field, Some(ITEM_VERSION_ABSENCE_FIELD));
        assert_eq!(error.item_resolution_http_response().status, 503);
        assert_eq!(error.item_resolution_http_response().body, "");
    }
    assert_eq!(
        resolve_item_version(&inventory(vec![]), limits)
            .unwrap_err()
            .item_resolution_http_response()
            .status,
        404
    );
}

#[test]
fn projection_is_total_and_never_guesses_from_a_code_or_message() {
    for (code, path, status, body) in [
        (
            Some("missing_object"),
            Some(ITEM_VERSION_ABSENCE_FIELD),
            404,
            r#"{"code":"missing_object"}"#,
        ),
        (
            Some("ambiguous_identity"),
            Some(ITEM_VERSION_AMBIGUITY_FIELD),
            409,
            r#"{"code":"ambiguous_identity"}"#,
        ),
        (Some("missing_object"), Some("objects"), 503, ""),
        (
            Some("missing_object"),
            Some("ontology.modules.package.object_digest"),
            503,
            "",
        ),
        (Some("missing_object"), None, 503, ""),
        (Some("ambiguous_identity"), None, 503, ""),
        (
            Some("ambiguous_identity"),
            Some(ITEM_VERSION_ABSENCE_FIELD),
            503,
            "",
        ),
        (
            Some("invalid_argument"),
            Some(ITEM_VERSION_ABSENCE_FIELD),
            503,
            "",
        ),
        (None, Some(ITEM_VERSION_ABSENCE_FIELD), 503, ""),
    ] {
        let expected = item_resolution_error_response(code, path);
        assert_eq!((expected.status, expected.body.as_str()), (status, body));
        let bytes = serde_json::to_vec(
            &json!({"code":code,"path":path,"message":"SECRET candidate owner digest repository"}),
        )
        .unwrap();
        assert_eq!(item_resolution_error_response_json(&bytes), expected);
    }
    for bytes in [b"null".as_slice(),b"{}",b"{",b"[]",br#"{"code":false,"path":"item_version_inventory.candidates"}"#,br#"{"code":"missing_object","path":[]}"#,br#"{"code":"missing_object","path":"item_version_inventory.candidates","extra":true}"#,br#"{"code":"missing_object","code":"missing_object","path":"item_version_inventory.candidates"}"#] {
        let response=item_resolution_error_response_json(bytes);
        assert_eq!(response.status,503);assert!(response.body.is_empty());
    }
    assert_eq!(
        item_resolution_error_response_json(
            br#"{"code":"missing_object","path":"item_version_inventory.candidates","version":{}}"#
        )
        .status,
        503
    );
    assert_eq!(
        item_resolution_error_response_json(&vec![b' '; 1025]).status,
        503
    );
    let error = parse_item_resolution_refusal(409, br#"{"code":"ambiguous_identity"}"#).unwrap();
    assert_eq!(error.item_resolution_http_response().status, 409);
    for (status, body) in [
        (404, br#"{"code":"missing_object"}"#.as_slice()),
        (409, br#"{"code":"missing_object"}"#.as_slice()),
        (404, br#"{"code":"ambiguous_identity"}"#),
        (409, br#"{"code":"ambiguous_identity","owners":["SECRET"]}"#),
        (
            409,
            br#"{"code":"ambiguous_identity","code":"ambiguous_identity"}"#,
        ),
        (409, b"proxy error"),
    ] {
        assert!(parse_item_resolution_refusal(status, body).is_err());
    }
    assert!(parse_item_resolution_refusal(409, &vec![b' '; 1025]).is_err());
}

#[test]
fn source_json_rejects_duplicate_members_and_aggregate_overflow() {
    let (d, r, m) = fixture();
    let i = inventory(vec![d.clone()]);
    let limits = DiscoveryLimits::default();
    let i = serde_json::to_vec(&i).unwrap();
    let d = serde_json::to_vec(&d).unwrap();
    let r = serde_json::to_vec(&r).unwrap();
    let m = serde_json::to_vec(&m).unwrap();
    bind_item_version_source_json(&i, &d, &r, &m, limits).unwrap();
    let duplicate = |bytes: &[u8], member: &str, value: &str| {
        format!(
            "{},\"{member}\":{value}}}",
            std::str::from_utf8(&bytes[..bytes.len() - 1]).unwrap()
        )
        .into_bytes()
    };
    let bad_i = duplicate(&i, "complete", "true");
    let bad_d = duplicate(&d, "id", &json!(fixture().0.id).to_string());
    let bad_r = duplicate(&r, "format", r#""graphforge-discovery/1""#);
    let bad_m = duplicate(&m, "format", r#""graphforge-discovery/1""#);
    for (i, d, r, m) in [
        (&bad_i, &d, &r, &m),
        (&i, &bad_d, &r, &m),
        (&i, &d, &bad_r, &m),
        (&i, &d, &r, &bad_m),
    ] {
        assert_eq!(
            bind_item_version_source_json(i, d, r, m, limits)
                .unwrap_err()
                .item_resolution_http_response()
                .status,
            503
        );
    }
    let limits = DiscoveryLimits {
        max_response_bytes: i.len() + d.len() + r.len() + m.len() - 1,
        ..limits
    };
    assert_eq!(
        bind_item_version_source_json(&i, &d, &r, &m, limits)
            .unwrap_err()
            .code,
        DiscoveryErrorCode::LimitExceeded
    );
}

#[test]
fn renamed_repository_keeps_source_identity_but_requires_actual_rehomed_snapshots() {
    let (mut descriptor, mut refs, mut manifest) = fixture();
    let mut input = inventory(vec![descriptor.clone()]);
    descriptor.repository.owner = "transferred".into();
    refs.repository = descriptor.repository.clone();
    manifest.repository = descriptor.repository.clone();
    descriptor.discovery.manifest_validator = manifest.canonical_digest().unwrap();
    for reference in &mut refs.refs {
        reference.validator = descriptor.discovery.manifest_validator.clone();
    }
    descriptor.discovery.refs_revision = revision(&refs);
    input.candidates = vec![descriptor.clone()];
    input.repositories[0].repository = descriptor.repository.clone();
    input.repositories[0].revision = "binding:after-transfer".into();
    assert_eq!(
        bind_item_version_source(
            &input,
            &descriptor,
            &refs,
            &manifest,
            DiscoveryLimits::default()
        )
        .unwrap(),
        descriptor
    );
    // The stable source is unchanged, but the old path/pins cannot be substituted.
    assert!(
        bind_item_version_source(
            &input,
            &descriptor,
            &fixture().1,
            &fixture().2,
            DiscoveryLimits::default()
        )
        .is_err()
    );
}

#[test]
fn malformed_supplied_binding_records_are_not_masked_by_ambiguity() {
    let (source, refs, manifest) = fixture();
    let mut other = source.clone();
    other.module.content_digest = Sha256Digest(format!("sha256:{}", "f".repeat(64)));
    let input = inventory(vec![source.clone(), other]);
    let limits = DiscoveryLimits::default();
    assert_eq!(
        resolve_item_version(&input, limits)
            .unwrap_err()
            .item_resolution_http_response()
            .status,
        409
    );
    let mut malformed = source.clone();
    malformed.id = "brg_aaaaaaaaaaaaaaaa".into();
    assert_eq!(
        bind_item_version_source(&input, &malformed, &refs, &manifest, limits)
            .unwrap_err()
            .item_resolution_http_response()
            .status,
        503
    );
    assert_eq!(
        bind_item_version_source_json(
            &serde_json::to_vec(&input).unwrap(),
            &serde_json::to_vec(&source).unwrap(),
            b"{",
            &serde_json::to_vec(&manifest).unwrap(),
            limits
        )
        .unwrap_err()
        .item_resolution_http_response()
        .status,
        503
    );
}
