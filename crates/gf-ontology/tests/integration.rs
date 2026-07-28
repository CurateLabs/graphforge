//! End-to-end integration tests for the ontology runtime (issue #564).
//!
//! Covers: load → validate → compile → query, subtype resolution, semantic
//! flags, Parquet round-trip, migration round-trip, validation error collection,
//! checksum stability, and YAML/JSON parity.

use std::path::Path;

use gf_ontology::error::OntologyError;
use gf_ontology::migration::MigrationEngine;
use gf_ontology::{
    EntityTypeDef, MigrationDef, OntologyDoc, PropertyDef, PropertyValueType, RelationTypeDef,
    SemanticFlags,
};
use gf_ontology::{
    OntologyCompiler, OntologyHandle, OntologyLoader, OntologyValidationError, OntologyValidator,
    ValidationErrorKind, load_parquet, save_parquet,
};

// ---------------------------------------------------------------------------
// Fixture path helpers
// ---------------------------------------------------------------------------

fn fixture(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn hr_handle() -> OntologyHandle {
    let doc = OntologyLoader::load_file(&fixture("hr.yaml")).unwrap();
    let rt = OntologyCompiler::compile(&doc).unwrap();
    OntologyHandle::new(rt)
}

// ---------------------------------------------------------------------------
// 1. Load → validate → compile → query
// ---------------------------------------------------------------------------

#[test]
fn load_validate_compile_query() {
    let h = hr_handle();

    // Entity type lookups
    assert!(h.entity_type_id("Person").is_some(), "Person should exist");
    assert!(
        h.entity_type_id("Employee").is_some(),
        "Employee should exist"
    );
    assert!(
        h.entity_type_id("Manager").is_some(),
        "Manager should exist"
    );
    assert!(
        h.entity_type_id("Department").is_some(),
        "Department should exist"
    );
    assert!(
        h.entity_type_id("Project").is_some(),
        "Project should exist"
    );
    assert!(
        h.entity_type_id("Ghost").is_none(),
        "Ghost should not exist"
    );

    // Relation type lookups
    assert!(
        h.relation_type_id("MANAGES").is_some(),
        "MANAGES should exist"
    );
    assert!(
        h.relation_type_id("WORKS_IN").is_some(),
        "WORKS_IN should exist"
    );
    assert!(
        h.relation_type_id("IS_FRIEND_OF").is_some(),
        "IS_FRIEND_OF should exist"
    );
    assert!(h.relation_type_id("GHOST_REL").is_none());

    // Property lookups
    let person_id = h.entity_type_id("Person").unwrap();
    assert!(
        h.property_type_id(person_id, "name").is_some(),
        "Person.name should exist"
    );
    assert!(
        h.property_type_id(person_id, "email").is_some(),
        "Person.email should exist"
    );
    assert!(h.property_type_id(person_id, "ghost_prop").is_none());

    let emp_id = h.entity_type_id("Employee").unwrap();
    assert!(
        h.property_type_id(emp_id, "employee_id").is_some(),
        "Employee.employee_id should exist"
    );

    // Metadata
    assert_eq!(h.id(), "hr");
    assert_eq!(h.version(), "v1");
    assert!(!h.checksum().is_empty());
}

// ---------------------------------------------------------------------------
// 2. Subtype resolution — 3-level inheritance chain
// ---------------------------------------------------------------------------

#[test]
fn subtype_three_level() {
    let h = hr_handle();

    let person = h.entity_type_id("Person").unwrap();
    let employee = h.entity_type_id("Employee").unwrap();
    let manager = h.entity_type_id("Manager").unwrap();
    let department = h.entity_type_id("Department").unwrap();

    // Direct and non-adjacent ancestor checks
    assert!(
        h.is_subtype(employee, person),
        "Employee is subtype of Person"
    );
    assert!(
        h.is_subtype(manager, employee),
        "Manager is subtype of Employee"
    );
    assert!(
        h.is_subtype(manager, person),
        "Manager is subtype of Person (non-adjacent)"
    );

    // Negative cases
    assert!(
        !h.is_subtype(person, employee),
        "Person is NOT subtype of Employee"
    );
    assert!(
        !h.is_subtype(person, manager),
        "Person is NOT subtype of Manager"
    );
    assert!(
        !h.is_subtype(employee, manager),
        "Employee is NOT subtype of Manager"
    );
    assert!(!h.is_subtype(person, person), "Not subtype of self");
    assert!(
        !h.is_subtype(department, person),
        "Department has no relation to Person"
    );
}

// ---------------------------------------------------------------------------
// 3. Semantic flags
// ---------------------------------------------------------------------------

#[test]
fn semantic_flags_correct() {
    let h = hr_handle();

    let manages = h.relation_type_id("MANAGES").unwrap();
    let is_friend = h.relation_type_id("IS_FRIEND_OF").unwrap();
    let works_in = h.relation_type_id("WORKS_IN").unwrap();

    let manages_flags = h.semantic_flags(manages);
    assert!(manages_flags.transitive, "MANAGES should be transitive");
    assert!(!manages_flags.symmetric, "MANAGES should not be symmetric");

    let friend_flags = h.semantic_flags(is_friend);
    assert!(friend_flags.symmetric, "IS_FRIEND_OF should be symmetric");
    assert!(
        !friend_flags.transitive,
        "IS_FRIEND_OF should not be transitive"
    );

    let works_flags = h.semantic_flags(works_in);
    assert!(!works_flags.transitive, "WORKS_IN should not be transitive");
    assert!(!works_flags.symmetric, "WORKS_IN should not be symmetric");
}

// ---------------------------------------------------------------------------
// 4. Parquet round-trip
// ---------------------------------------------------------------------------

#[test]
fn parquet_round_trip() {
    let doc = OntologyLoader::load_file(&fixture("hr.yaml")).unwrap();
    let compiled = OntologyCompiler::compile(&doc).unwrap();

    let dir = tempfile::tempdir().unwrap();
    save_parquet(&compiled, dir.path()).unwrap();

    // Load without checksum verification (None)
    let loaded = load_parquet(dir.path(), None).unwrap();

    // Lookup maps must be identical
    assert_eq!(
        compiled.entity_name_to_id, loaded.entity_name_to_id,
        "entity_name_to_id must survive Parquet round-trip"
    );
    assert_eq!(
        compiled.relation_name_to_id, loaded.relation_name_to_id,
        "relation_name_to_id must survive Parquet round-trip"
    );
    assert_eq!(
        compiled.property_name_to_id, loaded.property_name_to_id,
        "property_name_to_id must survive Parquet round-trip"
    );

    // Row counts
    assert_eq!(
        compiled.entity_types.num_rows(),
        loaded.entity_types.num_rows()
    );
    assert_eq!(
        compiled.relation_types.num_rows(),
        loaded.relation_types.num_rows()
    );
    assert_eq!(
        compiled.property_types.num_rows(),
        loaded.property_types.num_rows()
    );

    // Checksum verification — correct checksum must succeed
    let handle = OntologyHandle::new(compiled);
    let checksum = handle.checksum().to_owned();
    load_parquet(dir.path(), Some(&checksum)).unwrap();

    // Wrong checksum must return ChecksumMismatch
    let result = load_parquet(dir.path(), Some("wrong_checksum"));
    assert!(
        matches!(result, Err(OntologyError::ChecksumMismatch { .. })),
        "wrong checksum must return ChecksumMismatch"
    );
}

#[test]
fn parquet_round_trip_preserves_property_owner_namespaces() {
    let doc = OntologyDoc {
        ontology_id: "owner-namespaces".into(),
        version: "v1".into(),
        entity_types: vec![EntityTypeDef {
            name: "Asset".into(),
            r#abstract: false,
            parent: None,
        }],
        relation_types: vec![RelationTypeDef {
            name: "CONNECTED_TO".into(),
            src: "Asset".into(),
            dst: "Asset".into(),
            inverse: None,
            semantic: SemanticFlags::default(),
        }],
        properties: vec![
            PropertyDef {
                owner: "Asset".into(),
                name: "shared".into(),
                value_type: PropertyValueType::Utf8,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
            PropertyDef {
                owner: "CONNECTED_TO".into(),
                name: "shared".into(),
                value_type: PropertyValueType::Int64,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
        ],
        constraints: vec![],
        migrations: vec![],
    };
    let compiled = OntologyCompiler::compile(&doc).unwrap();
    let dir = tempfile::tempdir().unwrap();
    save_parquet(&compiled, dir.path()).unwrap();
    let loaded = load_parquet(dir.path(), None).unwrap();

    assert_eq!(compiled.property_name_to_id, loaded.property_name_to_id);

    let handle = OntologyHandle::new(loaded);
    let asset = handle.entity_type_id("Asset").unwrap();
    let connected_to = handle.relation_type_id("CONNECTED_TO").unwrap();
    assert_eq!(
        asset, connected_to,
        "numeric owner IDs intentionally collide"
    );
    assert_eq!(
        handle.entity_property_type_id(asset, "shared"),
        Some(gf_ontology::PropId(0))
    );
    assert_eq!(
        handle.relation_property_type_id(connected_to, "shared"),
        Some(gf_ontology::PropId(1))
    );
    assert_eq!(
        handle.property_type_id(asset, "shared"),
        None,
        "kind-free lookup must remain ambiguous after reload"
    );
}

// ---------------------------------------------------------------------------
// 5. Migration round-trip — v1 → v2 (rename Person → Human)
// ---------------------------------------------------------------------------

#[test]
fn migration_round_trip() {
    let doc = OntologyLoader::load_file(&fixture("hr.yaml")).unwrap();
    let steps = MigrationEngine::plan("v1", "v2", &doc.migrations).unwrap();
    assert_eq!(steps.len(), 1, "one migration step from v1 to v2");

    let rt = MigrationEngine::apply(doc, &steps).unwrap();
    let handle_v2 = OntologyHandle::new(rt);

    // After renaming Person → Human:
    assert!(
        handle_v2.entity_type_id("Human").is_some(),
        "Human should exist after migration"
    );
    assert!(
        handle_v2.entity_type_id("Person").is_none(),
        "Person should not exist after migration"
    );

    // Employee was Person's child — its parent reference should be updated
    let human_id = handle_v2.entity_type_id("Human").unwrap();
    let employee_id = handle_v2.entity_type_id("Employee").unwrap();
    assert!(
        handle_v2.is_subtype(employee_id, human_id),
        "Employee should still be subtype of Human (was Person)"
    );

    assert_eq!(handle_v2.version(), "v2");
}

// ---------------------------------------------------------------------------
// 6. Validation error collection
// ---------------------------------------------------------------------------

#[test]
fn validation_error_collection() {
    let doc = OntologyDoc {
        ontology_id: "bad".to_owned(),
        version: "1.0".to_owned(),
        entity_types: vec![
            // Violation 1: duplicate name
            EntityTypeDef {
                name: "Person".to_owned(),
                r#abstract: false,
                parent: None,
            },
            EntityTypeDef {
                name: "Person".to_owned(), // duplicate
                r#abstract: false,
                parent: None,
            },
            EntityTypeDef {
                name: "Employee".to_owned(),
                r#abstract: false,
                parent: Some("Ghost".to_owned()), // Violation 2: unresolved parent
            },
        ],
        relation_types: vec![],
        properties: vec![],
        constraints: vec![],
        migrations: vec![MigrationDef {
            from_version: "2.0".to_owned(),
            to_version: "1.0".to_owned(), // Violation 3: bad migration order
            transform_kind: "add_type:X".to_owned(),
            script_ref: None,
            checksum: None,
        }],
    };

    let result = OntologyValidator::validate(&doc);
    let errors: Vec<OntologyValidationError> = result.unwrap_err();

    assert!(
        errors.len() >= 3,
        "expected at least 3 validation errors, got {}: {:?}",
        errors.len(),
        errors
    );

    let has_duplicate = errors
        .iter()
        .any(|e| e.kind == ValidationErrorKind::DuplicateName);
    let has_unresolved = errors
        .iter()
        .any(|e| e.kind == ValidationErrorKind::UnresolvedReference);
    let has_bad_order = errors
        .iter()
        .any(|e| e.kind == ValidationErrorKind::MigrationVersionOrder);

    assert!(has_duplicate, "should report DuplicateName");
    assert!(has_unresolved, "should report UnresolvedReference");
    assert!(has_bad_order, "should report MigrationVersionOrder");
}

// ---------------------------------------------------------------------------
// 7. Checksum stability
// ---------------------------------------------------------------------------

#[test]
fn checksum_stability() {
    // Same YAML loaded twice → same checksum
    let doc1 = OntologyLoader::load_file(&fixture("hr.yaml")).unwrap();
    let doc2 = OntologyLoader::load_file(&fixture("hr.yaml")).unwrap();
    let rt1 = OntologyCompiler::compile(&doc1).unwrap();
    let rt2 = OntologyCompiler::compile(&doc2).unwrap();
    let h1 = OntologyHandle::new(rt1);
    let h2 = OntologyHandle::new(rt2);
    assert_eq!(
        h1.checksum(),
        h2.checksum(),
        "same input must produce same checksum"
    );

    // Different content → different checksum
    let mut doc_modified = OntologyLoader::load_file(&fixture("hr.yaml")).unwrap();
    doc_modified.version = "changed".to_owned();
    let rt_mod = OntologyCompiler::compile(&doc_modified).unwrap();
    let h_mod = OntologyHandle::new(rt_mod);
    assert_ne!(
        h1.checksum(),
        h_mod.checksum(),
        "different content must produce different checksum"
    );
}

// ---------------------------------------------------------------------------
// 8. YAML / JSON parity
// ---------------------------------------------------------------------------

#[test]
fn yaml_json_parity() {
    let from_yaml = OntologyLoader::load_file(&fixture("hr.yaml")).unwrap();
    let from_json = OntologyLoader::load_file(&fixture("hr.json")).unwrap();

    let rt_yaml = OntologyCompiler::compile(&from_yaml).unwrap();
    let rt_json = OntologyCompiler::compile(&from_json).unwrap();

    // Same entity / relation / property counts
    assert_eq!(
        rt_yaml.entity_types.num_rows(),
        rt_json.entity_types.num_rows(),
        "entity type count must match"
    );
    assert_eq!(
        rt_yaml.relation_types.num_rows(),
        rt_json.relation_types.num_rows(),
        "relation type count must match"
    );
    assert_eq!(
        rt_yaml.property_types.num_rows(),
        rt_json.property_types.num_rows(),
        "property count must match"
    );

    // Same lookup maps
    assert_eq!(
        rt_yaml.entity_name_to_id, rt_json.entity_name_to_id,
        "entity lookup maps must match"
    );
    assert_eq!(
        rt_yaml.relation_name_to_id, rt_json.relation_name_to_id,
        "relation lookup maps must match"
    );
    assert_eq!(
        rt_yaml.property_name_to_id, rt_json.property_name_to_id,
        "property lookup maps must match"
    );
}
