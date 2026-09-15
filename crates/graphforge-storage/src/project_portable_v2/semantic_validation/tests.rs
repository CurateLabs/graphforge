use super::*;

#[test]
fn runtime_map_rejects_duplicate_and_unknown_schema_members() {
    let duplicate = br#"{"contract":"graphforge-runtime-generation-map/1","contract":"graphforge-runtime-generation-map/1","capabilities":[],"participants":[],"graph_tree":null}"#;
    let error = decode_runtime_map(duplicate).err().unwrap();
    assert_eq!(error.code, PortableV2ErrorCode::Incompatible);

    let unknown = br#"{"contract":"graphforge-runtime-generation-map/1","capabilities":[],"participants":[],"graph_tree":null,"host_path":"/private/source"}"#;
    let error = decode_runtime_map(unknown).err().unwrap();
    assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
}

#[test]
fn current_reader_recognizes_the_m9_capability_before_payload_validation() {
    let mut value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/portable-v2/ontology-only.manifest.json"
    ))
    .unwrap();
    let capabilities = value["requirements"]["capabilities"]
        .as_array_mut()
        .unwrap();
    capabilities.push(Value::String("ontology-composition@1".into()));
    capabilities.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    let manifest: Manifest = serde_json::from_value(value).unwrap();
    validate_semantics(&manifest, PortableV2Limits::default()).unwrap();
}

fn composition_vector() -> (PortableV2OntologyComposition, ManifestComponent) {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/portable-v2/multi-ontology-vectors.json"
    ))
    .unwrap();
    let control = serde_json::from_value(fixture["composition"].clone()).unwrap();
    let component = serde_json::from_value(serde_json::json!({
        "kind": "compatibility",
        "participant_id": "graphforge-ontology-composition",
        "required_dependencies": [
            format!("ontology-bridge-{}", "4".repeat(64)),
            format!("ontology-module-{}", "1".repeat(64)),
            format!("ontology-module-{}", "2".repeat(64)),
            format!("ontology-module-{}", "3".repeat(64))
        ],
        "files": [{
            "media_type": "application/vnd.graphforge.ontology-composition+json",
            "path": ONTOLOGY_COMPOSITION_PATH,
            "length": 1,
            "sha256": format!("sha256:{}", "1".repeat(64))
        }]
    }))
    .unwrap();
    (control, component)
}

#[test]
fn composition_control_rejects_duplicate_dangling_and_unbounded_closure() {
    let (valid, component) = composition_vector();
    validate_ontology_composition_contents(&valid, &component, PortableV2Limits::default())
        .unwrap();

    let mut duplicate = valid.clone();
    duplicate.modules.push(duplicate.modules[0].clone());
    assert_eq!(
        validate_ontology_composition_contents(&duplicate, &component, PortableV2Limits::default())
            .unwrap_err()
            .code,
        PortableV2ErrorCode::Incompatible
    );

    let mut dangling = valid.clone();
    dangling.bridge_sets[0].source_modules[0].content_digest = format!("sha256:{}", "9".repeat(64));
    assert_eq!(
        validate_ontology_composition_contents(&dangling, &component, PortableV2Limits::default())
            .unwrap_err()
            .code,
        PortableV2ErrorCode::Incompatible
    );

    let mut unsupported = valid.clone();
    unsupported
        .required_features
        .push("future-contract@2".into());
    unsupported.required_features.sort();
    assert_eq!(
        validate_ontology_composition_contents(
            &unsupported,
            &component,
            PortableV2Limits::default()
        )
        .unwrap_err()
        .code,
        PortableV2ErrorCode::UnsupportedFuture
    );

    let limits = PortableV2Limits {
        max_components: 1,
        ..PortableV2Limits::default()
    };
    assert_eq!(
        validate_ontology_composition_contents(&valid, &component, limits)
            .unwrap_err()
            .code,
        PortableV2ErrorCode::LimitExceeded
    );
}
