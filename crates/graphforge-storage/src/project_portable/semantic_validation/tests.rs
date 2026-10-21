use super::*;

#[test]
fn runtime_map_rejects_duplicate_and_unknown_schema_members() {
    let duplicate = br#"{"contract":"graphforge-runtime-generation-map/1","contract":"graphforge-runtime-generation-map/1","capabilities":[],"participants":[],"graph_tree":null}"#;
    let error = decode_runtime_map(duplicate).err().unwrap();
    assert_eq!(error.code, PortableErrorCode::Incompatible);

    let unknown = br#"{"contract":"graphforge-runtime-generation-map/1","capabilities":[],"participants":[],"graph_tree":null,"host_path":"/private/source"}"#;
    let error = decode_runtime_map(unknown).err().unwrap();
    assert_eq!(error.code, PortableErrorCode::Incompatible);
}

#[test]
fn current_reader_recognizes_the_m9_capability_before_payload_validation() {
    let mut value: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/portable/ontology-only.manifest.json"
    ))
    .unwrap();
    let capabilities = value["requirements"]["capabilities"]
        .as_array_mut()
        .unwrap();
    capabilities.push(Value::String("ontology-composition@1".into()));
    capabilities.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    let manifest: Manifest = serde_json::from_value(value).unwrap();
    validate_semantics(&manifest, PortableLimits::default()).unwrap();
}

fn composition_vector() -> (PortableOntologyComposition, ManifestComponent) {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/portable/multi-ontology-vectors.json"
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
    validate_ontology_composition_contents(&valid, &component, PortableLimits::default()).unwrap();

    let mut duplicate = valid.clone();
    duplicate.modules.push(duplicate.modules[0].clone());
    assert_eq!(
        validate_ontology_composition_contents(&duplicate, &component, PortableLimits::default())
            .unwrap_err()
            .code,
        PortableErrorCode::Incompatible
    );

    let mut dangling = valid.clone();
    dangling.bridge_sets[0].source_modules[0].content_digest = format!("sha256:{}", "9".repeat(64));
    assert_eq!(
        validate_ontology_composition_contents(&dangling, &component, PortableLimits::default())
            .unwrap_err()
            .code,
        PortableErrorCode::Incompatible
    );

    let mut unsupported = valid.clone();
    unsupported
        .required_features
        .push("future-contract@2".into());
    unsupported.required_features.sort();
    assert_eq!(
        validate_ontology_composition_contents(&unsupported, &component, PortableLimits::default())
            .unwrap_err()
            .code,
        PortableErrorCode::UnsupportedFuture
    );

    let limits = PortableLimits {
        max_components: 1,
        ..PortableLimits::default()
    };
    assert_eq!(
        validate_ontology_composition_contents(&valid, &component, limits)
            .unwrap_err()
            .code,
        PortableErrorCode::LimitExceeded
    );
}

#[test]
fn research_runtime_cannot_be_relabelled_or_downgraded_to_generic_settings() {
    let mut manifest: Manifest = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/portable/ontology-only.manifest.json"
    ))
    .unwrap();
    manifest.requirements.capabilities.push("research@1".into());
    let registry_id = "research-registry";
    for id in [registry_id, "research-content"] {
        manifest.components.push(
            serde_json::from_value(serde_json::json!({
                "kind": "research", "participant_id": id, "required_dependencies": [], "files": []
            }))
            .unwrap(),
        );
    }
    let version = crate::research_versions::RESEARCH_VERSION;
    let mut runtime: RuntimeGenerationMap = serde_json::from_value(serde_json::json!({
        "contract": "graphforge-runtime-generation-map/1",
        "capabilities": [{"capability_id":"research", "capability_version":version}],
        "participants": [{
            "participant_id": registry_id, "capability_id":"research", "capability_version":version,
            "record_family_id":"registry", "record_version":version, "encoding":"json",
            "schema_fingerprint":"0".repeat(64), "row_count":1
        }], "graph_tree":null
    }))
    .unwrap();
    validate_research_runtime(&runtime, &manifest).unwrap();
    manifest.components.last_mut().unwrap().kind = "settings".into();
    assert!(validate_research_runtime(&runtime, &manifest).is_err());
    manifest.components.last_mut().unwrap().kind = "research".into();
    manifest
        .requirements
        .capabilities
        .retain(|v| v != "research@1");
    assert!(validate_research_runtime(&runtime, &manifest).is_err());
    manifest.requirements.capabilities.push("research@1".into());
    runtime.participants[0].record_version = version + 1;
    assert_eq!(
        validate_research_runtime(&runtime, &manifest)
            .unwrap_err()
            .code,
        PortableErrorCode::UnsupportedFuture
    );
}

#[test]
fn a_component_cannot_take_a_research_object_directory_as_its_identity() {
    let manifest_with = |participant_id: &str| -> Manifest {
        let mut value: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/portable/ontology-only.manifest.json"
        ))
        .unwrap();
        value["components"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "kind": "settings",
                "participant_id": participant_id,
                "required_dependencies": [],
                "files": []
            }));
        serde_json::from_value(value).unwrap()
    };
    // The same component under an ordinary identity is not refused for its identity.
    let ordinary = validate_semantics(
        &manifest_with("workspace-settings"),
        PortableLimits::default(),
    )
    .unwrap_err();
    assert!(!ordinary.to_string().contains("component identity"));
    for reserved in ["research-retained-evidence", "research-retained"] {
        let error =
            validate_semantics(&manifest_with(reserved), PortableLimits::default()).unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Incompatible, "{reserved}");
        assert!(
            error.to_string().contains("component identity"),
            "{reserved}"
        );
    }
}
