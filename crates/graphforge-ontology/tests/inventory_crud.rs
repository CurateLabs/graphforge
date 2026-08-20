//! Direct tests for ontology inventory CRUD / import-export (#837).

use graphforge_ontology::{
    ActivationMode, DiagnosticCode, EntityTypeDef, ExportFormat, ImportFormatHint,
    ModuleLifecycleStatus, ModuleSelector, OntologyDoc, OntologyInventory, OntologyModuleId,
};

fn entity_doc(ontology_id: &str, version: &str, entities: &[&str]) -> OntologyDoc {
    OntologyDoc {
        ontology_id: ontology_id.to_owned(),
        version: version.to_owned(),
        entity_types: entities
            .iter()
            .map(|name| EntityTypeDef {
                name: (*name).to_owned(),
                r#abstract: false,
                parent: None,
            })
            .collect(),
        relation_types: vec![],
        properties: vec![],
        constraints: vec![],
        migrations: vec![],
    }
}

#[test]
fn create_adopt_list_inspect_export_round_trip() {
    let mut inv = OntologyInventory::new(ActivationMode::Exploratory, Default::default());
    let doc = entity_doc(
        "https://graphforge.dev/ontology/research",
        "2.0.0",
        &["Study"],
    );
    let id = inv
        .create_register(doc.clone(), vec![], None, "op-create")
        .unwrap();
    assert_eq!(inv.list().len(), 0, "staging must not appear in list");

    let receipt = inv
        .adopt(&ModuleSelector::Exact(id.clone()), 0, "op-adopt")
        .unwrap();
    assert_eq!(receipt.prior_generation, 0);
    assert_eq!(receipt.new_generation, 1);
    assert!(!receipt.idempotent_replay);
    assert_eq!(receipt.affected_module.as_ref(), Some(&id));

    let listed = inv.list();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].status, ModuleLifecycleStatus::Adopted);
    assert_eq!(listed[0].digest, id.canonical_digest);

    let inspect = inv.inspect(&ModuleSelector::Exact(id.clone())).unwrap();
    assert_eq!(inspect.doc, doc);
    assert_eq!(inspect.generation, 1);
    assert_eq!(
        inspect.composition_fingerprint,
        inv.composition_fingerprint()
    );

    let json = inv
        .export_module(&ModuleSelector::Exact(id.clone()), ExportFormat::Json)
        .unwrap();
    let json2 = inv
        .export_module(&ModuleSelector::Exact(id), ExportFormat::Json)
        .unwrap();
    assert_eq!(json, json2);
    assert!(
        !json.contains("/Users/"),
        "export must not include host paths"
    );
    assert!(
        !json.contains("\\\\"),
        "export must not include windows paths"
    );
    assert!(json.contains("Study"));

    let meta = inv.export_metadata();
    assert_eq!(meta.generation, 1);
    assert_eq!(meta.modules.len(), 1);
    assert!(meta.bridges.is_empty());
}

#[test]
fn idempotent_adopt_replay() {
    let mut inv = OntologyInventory::default();
    let doc = entity_doc(
        "https://graphforge.dev/ontology/document",
        "1.0.0",
        &["Document"],
    );
    let id = inv.create_register(doc, vec![], None, "c1").unwrap();
    let first = inv
        .adopt(&ModuleSelector::Exact(id.clone()), 0, "same-op")
        .unwrap();
    let second = inv.adopt(&ModuleSelector::Exact(id), 0, "same-op").unwrap();
    assert!(second.idempotent_replay);
    assert_eq!(first.new_generation, second.new_generation);
    assert_eq!(inv.generation(), 1);
}

#[test]
fn malformed_import_and_validate() {
    let mut inv = OntologyInventory::default();
    let err = inv
        .import_text("{not-json", ImportFormatHint::Json, vec![], "bad")
        .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::InventoryMalformed));
    assert_eq!(inv.generation(), 0);

    let bad = OntologyDoc {
        ontology_id: "https://graphforge.dev/ontology/x".into(),
        version: "1".into(),
        entity_types: vec![
            EntityTypeDef {
                name: "Dup".into(),
                r#abstract: false,
                parent: None,
            },
            EntityTypeDef {
                name: "Dup".into(),
                r#abstract: false,
                parent: None,
            },
        ],
        relation_types: vec![],
        properties: vec![],
        constraints: vec![],
        migrations: vec![],
    };
    assert!(inv.validate_document(&bad).is_err());
}

#[test]
fn ambiguous_ontology_id_selection_fails() {
    let mut inv = OntologyInventory::default();
    let d1 = entity_doc(
        "https://graphforge.dev/ontology/science",
        "1.0.0",
        &["Specimen"],
    );
    let d2 = entity_doc(
        "https://graphforge.dev/ontology/science",
        "2.0.0",
        &["Specimen"],
    );
    let a = inv.create_register(d1, vec![], None, "a").unwrap();
    let b = inv.create_register(d2, vec![], None, "b").unwrap();
    inv.adopt(&ModuleSelector::Exact(a), 0, "adopt-a").unwrap();
    inv.adopt(&ModuleSelector::Exact(b), 1, "adopt-b").unwrap();

    let err = inv
        .inspect(&ModuleSelector::OntologyId(
            "https://graphforge.dev/ontology/science".into(),
        ))
        .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::ResolutionAmbiguous));
}

#[test]
fn dependency_blocked_delete_and_safe_delete() {
    let mut inv = OntologyInventory::default();
    let evidence = entity_doc(
        "https://graphforge.dev/ontology/evidence",
        "1.0.0",
        &["Claim"],
    );
    let eid = inv
        .create_register(evidence, vec![], None, "e-create")
        .unwrap();
    inv.adopt(&ModuleSelector::Exact(eid.clone()), 0, "e-adopt")
        .unwrap();

    let research = entity_doc(
        "https://graphforge.dev/ontology/research",
        "2.0.0",
        &["Study"],
    );
    let rid = inv
        .create_register(research, vec![eid.clone()], None, "r-create")
        .unwrap();
    inv.adopt(&ModuleSelector::Exact(rid.clone()), 1, "r-adopt")
        .unwrap();

    let preview = inv
        .preview_delete(&ModuleSelector::Exact(eid.clone()))
        .unwrap();
    assert!(!preview.safe);
    assert_eq!(preview.dependent_modules, vec![rid.clone()]);

    let err = inv
        .delete(&ModuleSelector::Exact(eid.clone()), 2, "del-e")
        .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::DependencyInUse));
    assert_eq!(inv.generation(), 2);
    assert_eq!(inv.list().len(), 2);

    // Remove dependant first, then evidence.
    inv.delete(&ModuleSelector::Exact(rid), 2, "del-r").unwrap();
    inv.delete(&ModuleSelector::Exact(eid), 3, "del-e2")
        .unwrap();
    assert!(inv.list().is_empty());
}

#[test]
fn update_replacement_and_stale_generation_atomic_failure() {
    let mut inv = OntologyInventory::default();
    let v1 = entity_doc(
        "https://graphforge.dev/ontology/genealogy",
        "3.0.0",
        &["Person"],
    );
    let id = inv.create_register(v1, vec![], None, "c").unwrap();
    inv.adopt(&ModuleSelector::Exact(id.clone()), 0, "a")
        .unwrap();
    let before = inv.composition_fingerprint().to_owned();

    let v2 = entity_doc(
        "https://graphforge.dev/ontology/genealogy",
        "3.1.0",
        &["Person"],
    );
    let err = inv
        .update(
            &ModuleSelector::Exact(id.clone()),
            v2.clone(),
            vec![],
            99,
            "stale",
        )
        .unwrap_err();
    assert_eq!(
        err.code(),
        Some(DiagnosticCode::InventoryGenerationConflict)
    );
    assert_eq!(inv.generation(), 1);
    assert_eq!(inv.composition_fingerprint(), before);

    let receipt = inv
        .update(&ModuleSelector::Exact(id), v2, vec![], 1, "upd")
        .unwrap();
    assert_eq!(receipt.new_generation, 2);
    assert_eq!(inv.list().len(), 1);
    assert_eq!(inv.list()[0].id.authored_version, "3.1.0");
}

#[test]
fn reopen_preserves_authority_and_fingerprint() {
    let mut inv = OntologyInventory::default();
    let doc = entity_doc(
        "https://graphforge.dev/ontology/provenance",
        "1.2.0",
        &["Activity"],
    );
    let id = inv.create_register(doc, vec![], None, "c").unwrap();
    inv.adopt(&ModuleSelector::Exact(id), 0, "a").unwrap();
    // Staging must not survive reopen.
    let _ = inv.create_register(
        entity_doc("https://graphforge.dev/ontology/tmp", "1", &["T"]),
        vec![],
        None,
        "stage",
    );

    let snap = inv.snapshot();
    let reopened = OntologyInventory::reopen(snap).unwrap();
    assert_eq!(reopened.generation(), inv.generation());
    assert_eq!(
        reopened.composition_fingerprint(),
        inv.composition_fingerprint()
    );
    assert_eq!(reopened.list().len(), 1);
}

#[test]
fn import_yaml_then_adopt_without_portable_package() {
    let yaml = r#"
ontology_id: https://graphforge.dev/ontology/evidence
version: "1.0.0"
entity_types:
  - name: Claim
"#;
    let mut inv = OntologyInventory::default();
    let id = inv
        .import_text(yaml, ImportFormatHint::Yaml, vec![], "imp")
        .unwrap();
    assert_eq!(inv.list().len(), 0);
    inv.adopt(&ModuleSelector::Exact(id.clone()), 0, "adopt")
        .unwrap();
    let exported = inv
        .export_module(
            &ModuleSelector::OntologyId(id.ontology_id.clone()),
            ExportFormat::Yaml,
        )
        .unwrap();
    assert!(exported.contains("Claim"));
}

#[test]
fn duplicate_adopt_conflict() {
    let mut inv = OntologyInventory::default();
    let doc = entity_doc("https://graphforge.dev/ontology/a", "1", &["Node"]);
    let id = inv
        .create_register(doc.clone(), vec![], None, "c1")
        .unwrap();
    inv.adopt(&ModuleSelector::Exact(id.clone()), 0, "a1")
        .unwrap();
    let again = inv.create_register(doc, vec![], None, "c2");
    assert_eq!(
        again.unwrap_err().code(),
        Some(DiagnosticCode::InventoryDuplicate)
    );
}

#[test]
fn legacy_single_ontology_projection() {
    let doc = entity_doc("core", "2026.05", &["Person"]);
    let inv = OntologyInventory::from_legacy_single(doc, false, ActivationMode::Advisory).unwrap();
    assert_eq!(inv.list().len(), 1);
    assert_eq!(inv.list()[0].id.ontology_id, "core");
    let _ = OntologyModuleId {
        ontology_id: "core".into(),
        authored_version: "2026.05".into(),
        canonical_digest: inv.list()[0].digest.clone(),
    };
}
