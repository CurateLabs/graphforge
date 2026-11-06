//! Module text is authenticated authored content, with a compatible absent form.

use graphforge_ontology::{OntologyDoc, OntologyLoader, OntologyValidator, module_document_digest};
use serde_json::json;

fn document() -> OntologyDoc {
    serde_json::from_value(json!({
        "ontology_id":"https://example.org/ontology/text", "version":"1",
        "entity_types":[], "relation_types":[], "properties":[], "constraints":[], "migrations":[]
    }))
    .unwrap()
}

#[test]
fn absent_module_text_preserves_historical_serialization_and_digest() {
    let document = document();
    let value = serde_json::to_value(&document).unwrap();
    assert!(value.get("label").is_none());
    assert!(value.get("description").is_none());
    assert!(value.get("license").is_none());
    assert!(value.get("forked_from").is_none());
    assert_eq!(
        module_document_digest(&document).unwrap(),
        "8193bfdb30f20732936333c4664ad500660991854a65d6822d60fd414461fd06"
    );
}

#[test]
fn authored_text_round_trips_and_changes_module_identity_without_normalization() {
    let mut document = document();
    let absent = module_document_digest(&document).unwrap();
    document.label = Some("研究 e\u{301}".into());
    document.description = Some("Plain text, including <angle brackets>.\n\tSecond line.".into());
    OntologyValidator::validate(&document).unwrap();
    let bytes = serde_json::to_vec(&document).unwrap();
    let parsed = OntologyLoader::load_json(bytes.as_slice()).unwrap();
    assert_eq!(parsed, document);
    assert_ne!(module_document_digest(&parsed).unwrap(), absent);
    let labelled = module_document_digest(&document).unwrap();
    document.label = Some("研究 é".into());
    assert_ne!(
        module_document_digest(&document).unwrap(),
        labelled,
        "authored Unicode is not normalized"
    );
    document.description = Some("Revised description".into());
    assert_ne!(module_document_digest(&document).unwrap(), labelled);
}

#[test]
fn loading_and_native_validation_share_bounded_text_rules() {
    for (label, description) in [
        (Some("\n".into()), None),
        (Some("a".repeat(257)), None),
        (Some("first\u{2028}second".into()), None),
        (None, Some("a".repeat(4097))),
        (None, Some("bad\rtext".into())),
    ] {
        let mut document = document();
        document.label = label;
        document.description = description;
        assert!(OntologyValidator::validate(&document).is_err());
        assert!(
            OntologyLoader::load_json(serde_json::to_vec(&document).unwrap().as_slice()).is_err()
        );
    }
    let mut localized = serde_json::to_value(document()).unwrap();
    localized["label"] = json!({"en":"Works"});
    assert!(serde_json::from_value::<OntologyDoc>(localized).is_err());
}

#[test]
fn module_license_is_exact_nonstructural_content_and_loads_as_json_or_yaml() {
    let original = document();
    let mut licensed = original.clone();
    licensed.license = Some("MIT OR (Apache-2.0 AND BSD-3-Clause)".into());
    OntologyValidator::validate(&licensed).unwrap();
    let bytes = serde_json::to_vec(&licensed).unwrap();
    assert_eq!(
        OntologyLoader::load_json(bytes.as_slice()).unwrap(),
        licensed
    );
    let yaml = serde_yaml::to_string(&licensed).unwrap();
    assert_eq!(
        OntologyLoader::load_yaml(yaml.as_bytes()).unwrap(),
        licensed
    );
    assert!(original.has_same_schema(&licensed));
    assert_ne!(
        module_document_digest(&original).unwrap(),
        module_document_digest(&licensed).unwrap()
    );
    let digest = module_document_digest(&licensed).unwrap();
    licensed.license = Some("MIT OR  (Apache-2.0 AND BSD-3-Clause)".into());
    OntologyValidator::validate(&licensed).unwrap();
    assert_ne!(
        module_document_digest(&licensed).unwrap(),
        digest,
        "SPDX expressions are never reformatted"
    );
}

#[test]
fn native_module_loading_refuses_invalid_or_unbounded_licenses() {
    for license in [
        "".into(),
        "   ".into(),
        "Not-A-License".into(),
        "MIT OR".into(),
        "MIT/Apache-2.0".into(),
        "GPL-2.0".into(),
        "MIT\nOR Apache-2.0".into(),
        "MIT\u{a0}".into(),
        "MIT".to_owned() + &" ".repeat(4094),
    ] {
        let mut document = document();
        document.license = Some(license);
        assert!(OntologyValidator::validate(&document).is_err());
        assert!(
            OntologyLoader::load_json(serde_json::to_vec(&document).unwrap().as_slice()).is_err()
        );
    }
    let mut value = serde_json::to_value(document()).unwrap();
    value["license"] = json!(["MIT"]);
    assert!(serde_json::from_value::<OntologyDoc>(value).is_err());
}

#[test]
fn fork_origin_is_exact_content_and_uses_shared_validation() {
    let original = document();
    let mut fork = original.clone();
    let origin = json!({
        "id":"https://graphforge.sh/ont_aaaaaaaaaaaaaaaa",
        "version":"1",
        "digest":format!("sha256:{}", "a".repeat(64))
    });
    fork.forked_from = Some(serde_json::from_value(origin.clone()).unwrap());
    OntologyValidator::validate(&fork).unwrap();
    assert!(original.has_same_schema(&fork));
    assert_ne!(
        module_document_digest(&original).unwrap(),
        module_document_digest(&fork).unwrap()
    );
    assert_eq!(
        OntologyLoader::load_json(serde_json::to_vec(&fork).unwrap().as_slice()).unwrap(),
        fork
    );
    assert_eq!(
        OntologyLoader::load_yaml(serde_yaml::to_string(&fork).unwrap().as_bytes()).unwrap(),
        fork
    );
    for (field, value) in [
        ("id", json!("ont_aaaaaaaaaaaaaaaa")),
        ("id", json!("https://graphforge.sh/brg_aaaaaaaaaaaaaaaa")),
        ("id", json!("https://graphforge.sh/ONT_AAAAAAAAAAAAAAAA")),
        ("digest", json!("a".repeat(64))),
        ("digest", json!(format!("sha256:{}", "A".repeat(64)))),
        ("unexpected", json!(true)),
    ] {
        let mut invalid = origin.clone();
        invalid[field] = value;
        let mut value = serde_json::to_value(&fork).unwrap();
        value["forked_from"] = invalid;
        assert!(
            OntologyLoader::load_json(serde_json::to_vec(&value).unwrap().as_slice()).is_err(),
            "{field}: {value}"
        );
    }
}

#[test]
fn fork_origin_preserves_admitted_native_versions() {
    use graphforge_ontology::{
        ActivationMode, AuthoredModule, CompositionLimits, InventoryCompileRequest,
        OntologyModuleId, compile_inventory,
    };
    let compile = |doc: OntologyDoc| {
        let module = AuthoredModule {
            id: OntologyModuleId {
                ontology_id: doc.ontology_id.clone(),
                authored_version: doc.version.clone(),
                canonical_digest: module_document_digest(&doc).unwrap(),
            },
            dependencies: Vec::new(),
            doc,
            allow_projected_identity: false,
        };
        let compiled = compile_inventory(InventoryCompileRequest {
            modules: std::slice::from_ref(&module),
            bridges: &[],
            activation: &[],
            profile_default: ActivationMode::Exploratory,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap();
        compiled.modules[0].id.clone()
    };
    for version in ["v\n1".to_owned(), String::new(), "a".repeat(4097)] {
        let mut source = document();
        source.ontology_id = "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa".into();
        source.version = version.clone();
        let source =
            OntologyLoader::load_json(serde_json::to_vec(&source).unwrap().as_slice()).unwrap();
        let source_id = compile(source.clone());
        let mut fork = source;
        fork.ontology_id = "https://graphforge.sh/ont_bbbbbbbbbbbbbbbb".into();
        fork.forked_from = Some((&source_id).into());
        let fork =
            OntologyLoader::load_json(serde_json::to_vec(&fork).unwrap().as_slice()).unwrap();
        assert_eq!(fork.forked_from.as_ref().unwrap().version, version);
        let fork_id = compile(fork);
        assert_ne!(source_id.ontology_id, fork_id.ontology_id);
        assert_eq!(source_id.authored_version, fork_id.authored_version);
    }
}

#[test]
fn declared_version_order_is_optional_authenticated_nonstructural_content() {
    let original = document();
    let mut value = serde_json::to_value(&original).unwrap();
    assert!(value.get("version_order").is_none());
    assert_eq!(
        serde_json::to_string(&original).unwrap(),
        r#"{"ontology_id":"https://example.org/ontology/text","version":"1","entity_types":[],"relation_types":[],"properties":[],"constraints":[],"migrations":[]}"#
    );
    value["version_order"] = json!(null);
    assert_eq!(
        serde_json::from_value::<OntologyDoc>(value).unwrap(),
        original
    );
    for order in [0, u32::MAX] {
        let mut ordered = original.clone();
        ordered.version = "v\n1".into();
        ordered.version_order = Some(order);
        let json = serde_json::to_vec(&ordered).unwrap();
        assert_eq!(OntologyLoader::load_json(json.as_slice()).unwrap(), ordered);
        let yaml = serde_yaml::to_string(&ordered).unwrap();
        assert_eq!(OntologyLoader::load_yaml(yaml.as_bytes()).unwrap(), ordered);
        assert!(original.has_same_schema(&ordered));
        let mut absent = ordered.clone();
        absent.version_order = None;
        assert_ne!(
            module_document_digest(&ordered).unwrap(),
            module_document_digest(&absent).unwrap()
        );
    }
    for invalid in [
        json!(-1),
        json!(1.5),
        json!(1.0),
        json!("1"),
        json!(4294967296u64),
        json!(true),
    ] {
        let mut value = serde_json::to_value(&original).unwrap();
        value["version_order"] = invalid;
        assert!(OntologyLoader::load_json(serde_json::to_vec(&value).unwrap().as_slice()).is_err());
        assert!(
            OntologyLoader::load_yaml(serde_yaml::to_string(&value).unwrap().as_bytes()).is_err()
        );
    }
}
