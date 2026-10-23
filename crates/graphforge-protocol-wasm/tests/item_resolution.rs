//! The native-callable adapter preserves Core's decisions and projection provenance.
use graphforge_discovery::{
    DiscoveryLimits, ItemDescriptor, ItemDiscoverySnapshot, ItemId, ItemVersionInventory,
};
use graphforge_protocol_wasm::engine;
use serde_json::{Value, json};

#[test]
fn resolution_corpus_and_http_projection_match_native_authority() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/reference/discovery/v1/item-resolution-conformance.json");
    let corpus: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let overrides = case.get("limits").map(|v| serde_json::to_vec(v).unwrap());
        let limits = engine::item_resolution_limits(overrides.as_deref()).unwrap();
        let result =
            engine::resolve_item_version(case["json"].as_str().unwrap().as_bytes(), limits);
        let expected = &case["expected"];
        match result {
            Ok(identity) => assert_eq!(
                serde_json::to_value(identity).unwrap(),
                expected["identity"]
            ),
            Err(error) => {
                assert_eq!(error.code, expected["code"].as_str().unwrap());
                assert_eq!(
                    serde_json::to_value(&error.path).unwrap(),
                    expected["field"]
                );
                let response =
                    engine::item_resolution_error_response(&serde_json::to_vec(&error).unwrap());
                assert_eq!(
                    u64::from(response.status),
                    expected["status"].as_u64().unwrap()
                );
                assert_eq!(response.body, expected["body"].as_str().unwrap());
            }
        }
    }
}

#[test]
fn binding_preserves_nested_missing_object_provenance() {
    let limits = DiscoveryLimits::default();
    let refs = engine::validate_refs(
        include_bytes!("../../../tests/fixtures/hub/generated/v1/refs.json"),
        limits,
    )
    .unwrap();
    let manifest = engine::validate_manifest(
        include_bytes!("../../../tests/fixtures/hub/generated/v1/manifest.json"),
        limits,
    )
    .unwrap();
    let module = manifest.document.ontology.as_ref().unwrap().modules[0].identity();
    let id = ItemId::parse_reference(&module.id).unwrap();
    let descriptor = ItemDescriptor {
        format: "graphforge-item/1".into(),
        id: id.as_str().into(),
        kind: id.kind(),
        repository: manifest.document.repository.clone(),
        module,
        discovery: ItemDiscoverySnapshot {
            refs_revision: graphforge_discovery::Sha256Digest(refs.revision),
            manifest_validator: graphforge_discovery::Sha256Digest(manifest.digest),
        },
    };
    let inventory = ItemVersionInventory {
        id: descriptor.id.clone(),
        version: descriptor.module.version.clone(),
        complete: true,
        candidates: vec![descriptor.clone()],
    };
    let i = serde_json::to_vec(&inventory).unwrap();
    let d = serde_json::to_vec(&descriptor).unwrap();
    let r = serde_json::to_vec(&refs.document).unwrap();
    engine::bind_item_version_source(
        &i,
        &d,
        &r,
        &serde_json::to_vec(&manifest.document).unwrap(),
        limits,
    )
    .unwrap();
    let mut broken = manifest.document;
    broken.objects.clear();
    let error =
        engine::bind_item_version_source(&i, &d, &r, &serde_json::to_vec(&broken).unwrap(), limits)
            .err()
            .unwrap();
    assert_eq!(error.code, "missing_object");
    assert_eq!(error.path.as_deref(), Some("objects"));
    let projected = engine::item_resolution_error_response(&serde_json::to_vec(&error).unwrap());
    assert_eq!(projected.status, 503);
    assert!(projected.body.is_empty());
}

#[test]
fn malformed_diagnostics_and_limits_fail_closed() {
    for bytes in [b"{}".as_slice(),b"null",b"{",br#"{"code":"missing_object","path":false}"#,br#"{"code":"ambiguous_identity"}"#,br#"{"code":"missing_object","path":"item_version_inventory.candidates","path":"item_version_inventory.candidates"}"#] {
        let response=engine::item_resolution_error_response(bytes);assert_eq!(response.status,503);assert!(response.body.is_empty());
    }
    for bytes in [
        br#"{"max_response_bytes":1,"max_response_bytes":2}"#.as_slice(),
        br#"{"unknown":1}"#,
        &vec![b' '; 1025],
    ] {
        assert!(engine::item_resolution_limits(Some(bytes)).is_err());
    }
    let response=engine::item_resolution_error_response(&serde_json::to_vec(&json!({"code":"ambiguous_identity","path":"item_version_inventory.candidates.content_digest","message":"private"})).unwrap());
    assert_eq!(response.status, 409);
    assert_eq!(response.body, r#"{"code":"ambiguous_identity"}"#);
}
