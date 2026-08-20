//! Direct tests for provenance-bearing bridge-set lifecycle (#838).

use std::collections::HashSet;

use graphforge_ontology::{
    ActivationMode, BridgeAssertion, BridgeDocument, BridgeExportFormat, BridgeImportFormatHint,
    BridgeInventory, BridgePredicate, BridgeProvenance, BridgeSelector, BridgeSetId,
    DiagnosticCode, MappingMethod, ModuleSymbolTable, OntologyModuleId, QualifiedSymbol,
    SharedSurfaceHint, SymbolKind, bridge_document_digest,
};

fn module_id(ontology_id: &str, version: &str, digest: &str) -> OntologyModuleId {
    OntologyModuleId {
        ontology_id: ontology_id.to_owned(),
        authored_version: version.to_owned(),
        canonical_digest: digest.to_owned(),
    }
}

fn table(
    id: OntologyModuleId,
    entities: &[&str],
    relations: &[&str],
    properties: &[&str],
) -> ModuleSymbolTable {
    ModuleSymbolTable {
        id,
        entities: entities.iter().map(|s| (*s).to_owned()).collect(),
        relations: relations.iter().map(|s| (*s).to_owned()).collect(),
        properties: properties.iter().map(|s| (*s).to_owned()).collect(),
    }
}

fn q(module: &OntologyModuleId, kind: SymbolKind, local_id: &str) -> QualifiedSymbol {
    QualifiedSymbol {
        module: module.clone(),
        kind,
        local_id: local_id.to_owned(),
    }
}

fn authored_prov(justification: &str) -> BridgeProvenance {
    BridgeProvenance {
        method: MappingMethod::Authored,
        confidence: None,
        justification: justification.to_owned(),
        evidence_refs: vec!["evidence:review-1".into()],
    }
}

fn research_gene_modules() -> (
    OntologyModuleId,
    OntologyModuleId,
    ModuleSymbolTable,
    ModuleSymbolTable,
) {
    let research = module_id(
        "https://graphforge.dev/ontology/research",
        "1.0.0",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    let genealogy = module_id(
        "https://graphforge.dev/ontology/genealogy",
        "3.0.0",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
    let research_table = table(
        research.clone(),
        &["Study", "Person"],
        &["FUNDED_BY"],
        &["title"],
    );
    let genealogy_table = table(
        genealogy.clone(),
        &["Person", "Claim"],
        &["PARENT_OF"],
        &["name"],
    );
    (research, genealogy, research_table, genealogy_table)
}

fn base_inventory() -> (BridgeInventory, OntologyModuleId, OntologyModuleId) {
    let (research, genealogy, research_table, genealogy_table) = research_gene_modules();
    let mut inv = BridgeInventory::new(ActivationMode::Exploratory, Default::default());
    inv.register_module(research_table);
    inv.register_module(genealogy_table);
    (inv, research, genealogy)
}

fn assertion(
    source: QualifiedSymbol,
    target: QualifiedSymbol,
    predicate: BridgePredicate,
) -> BridgeAssertion {
    BridgeAssertion {
        source,
        target,
        predicate,
        directional: true,
        provenance: authored_prov("curated mapping"),
        valid_from: None,
        valid_to: None,
    }
}

fn doc(
    bridge_id: &str,
    version: &str,
    source: &OntologyModuleId,
    target: &OntologyModuleId,
    assertions: Vec<BridgeAssertion>,
) -> BridgeDocument {
    BridgeDocument {
        bridge_id: bridge_id.to_owned(),
        authored_version: version.to_owned(),
        source_modules: vec![source.clone()],
        target_modules: vec![target.clone()],
        dependencies: vec![],
        shared_surfaces: vec![SharedSurfaceHint::Evidence],
        assertions,
        enforcement: None,
    }
}

#[test]
fn equivalent_entity_mapping_crud_and_idempotent_adopt() {
    let (mut inv, research, genealogy) = base_inventory();
    let document = doc(
        "https://graphforge.dev/bridge/research-genealogy",
        "1.0.0",
        &research,
        &genealogy,
        vec![assertion(
            q(&research, SymbolKind::Entity, "Person"),
            q(&genealogy, SymbolKind::Entity, "Person"),
            BridgePredicate::Equivalent,
        )],
    );
    let id = inv.create_register(document, "op-create").expect("create");
    let gen0 = inv.generation();
    let receipt = inv
        .adopt(&BridgeSelector::Exact(id.clone()), gen0, "op-adopt")
        .expect("adopt");
    assert_eq!(receipt.prior_generation, 0);
    assert_eq!(receipt.new_generation, 1);
    assert!(!receipt.idempotent_replay);

    let replay = inv
        .adopt(&BridgeSelector::Exact(id.clone()), gen0, "op-adopt")
        .expect("idempotent");
    assert!(replay.idempotent_replay);
    assert_eq!(replay.new_generation, receipt.new_generation);

    let listed = inv.list();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);
    let inspected = inv.inspect(&BridgeSelector::Exact(id)).unwrap();
    assert_eq!(
        inspected.doc.assertions[0].predicate,
        BridgePredicate::Equivalent
    );
}

#[test]
fn directional_related_broader_narrower_and_disjoint() {
    let (mut inv, research, genealogy) = base_inventory();
    let document = doc(
        "https://graphforge.dev/bridge/directional",
        "1.0.0",
        &research,
        &genealogy,
        vec![
            assertion(
                q(&research, SymbolKind::Entity, "Study"),
                q(&genealogy, SymbolKind::Entity, "Person"),
                BridgePredicate::Related,
            ),
            assertion(
                q(&research, SymbolKind::Entity, "Study"),
                q(&genealogy, SymbolKind::Entity, "Claim"),
                BridgePredicate::Broader,
            ),
            assertion(
                q(&genealogy, SymbolKind::Entity, "Claim"),
                q(&research, SymbolKind::Entity, "Study"),
                BridgePredicate::Narrower,
            ),
            assertion(
                q(&research, SymbolKind::Entity, "Study"),
                q(&genealogy, SymbolKind::Entity, "Person"),
                BridgePredicate::Disjoint,
            ),
        ],
    );
    // Related + Disjoint on same Study↔Person pair is allowed (different predicates
    // that do not form the equivalent∩disjoint contradiction).
    // Broader(Study,Claim) + Narrower(Claim,Study) is coherent (inverse pair).
    let id = inv.create_register(document, "op").unwrap();
    inv.adopt(&BridgeSelector::Exact(id), inv.generation(), "adopt")
        .unwrap();
    assert_eq!(inv.list().len(), 1);
}

#[test]
fn property_and_relation_maps_to() {
    let (mut inv, research, genealogy) = base_inventory();
    let document = doc(
        "https://graphforge.dev/bridge/maps",
        "1.0.0",
        &research,
        &genealogy,
        vec![
            assertion(
                q(&research, SymbolKind::Property, "title"),
                q(&genealogy, SymbolKind::Property, "name"),
                BridgePredicate::MapsTo,
            ),
            assertion(
                q(&research, SymbolKind::Relation, "FUNDED_BY"),
                q(&genealogy, SymbolKind::Relation, "PARENT_OF"),
                BridgePredicate::MapsTo,
            ),
        ],
    );
    let id = inv.create_register(document, "op").unwrap();
    inv.adopt(&BridgeSelector::Exact(id), inv.generation(), "adopt")
        .unwrap();
}

#[test]
fn conflicting_equivalent_and_disjoint_rejected() {
    let (inv, research, genealogy) = base_inventory();
    let document = doc(
        "https://graphforge.dev/bridge/conflict",
        "1.0.0",
        &research,
        &genealogy,
        vec![
            assertion(
                q(&research, SymbolKind::Entity, "Person"),
                q(&genealogy, SymbolKind::Entity, "Person"),
                BridgePredicate::Equivalent,
            ),
            assertion(
                q(&research, SymbolKind::Entity, "Person"),
                q(&genealogy, SymbolKind::Entity, "Person"),
                BridgePredicate::Disjoint,
            ),
        ],
    );
    let err = inv.validate_document(&document).unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::BridgeContradiction));
}

#[test]
fn missing_module_endpoint_rejected() {
    let (inv, research, genealogy) = base_inventory();
    let missing = module_id(
        "https://graphforge.dev/ontology/missing",
        "1.0.0",
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    );
    let document = doc(
        "https://graphforge.dev/bridge/missing",
        "1.0.0",
        &research,
        &genealogy,
        vec![assertion(
            q(&research, SymbolKind::Entity, "Person"),
            q(&missing, SymbolKind::Entity, "Ghost"),
            BridgePredicate::Related,
        )],
    );
    let err = inv.validate_document(&document).unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::BridgeEndpointMissing));
}

#[test]
fn invalid_kind_pair_rejected() {
    let (inv, research, genealogy) = base_inventory();
    let document = doc(
        "https://graphforge.dev/bridge/kinds",
        "1.0.0",
        &research,
        &genealogy,
        vec![assertion(
            q(&research, SymbolKind::Entity, "Person"),
            q(&genealogy, SymbolKind::Property, "name"),
            BridgePredicate::Equivalent,
        )],
    );
    let err = inv.validate_document(&document).unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::ResolutionKindMismatch));
}

#[test]
fn dependency_aware_deletion_and_safe_delete() {
    let (mut inv, research, genealogy) = base_inventory();
    let base = doc(
        "https://graphforge.dev/bridge/base",
        "1.0.0",
        &research,
        &genealogy,
        vec![assertion(
            q(&research, SymbolKind::Entity, "Person"),
            q(&genealogy, SymbolKind::Entity, "Person"),
            BridgePredicate::Equivalent,
        )],
    );
    let base_id = inv.create_register(base, "c1").unwrap();
    inv.adopt(&BridgeSelector::Exact(base_id.clone()), 0, "a1")
        .unwrap();

    let mut dependent = doc(
        "https://graphforge.dev/bridge/dependent",
        "1.0.0",
        &research,
        &genealogy,
        vec![assertion(
            q(&research, SymbolKind::Entity, "Study"),
            q(&genealogy, SymbolKind::Entity, "Claim"),
            BridgePredicate::Related,
        )],
    );
    dependent.dependencies = vec![base_id.clone()];
    let dep_id = inv.create_register(dependent, "c2").unwrap();
    inv.adopt(
        &BridgeSelector::Exact(dep_id.clone()),
        inv.generation(),
        "a2",
    )
    .unwrap();

    let preview = inv
        .preview_delete(&BridgeSelector::Exact(base_id.clone()))
        .unwrap();
    assert!(!preview.safe);
    assert_eq!(preview.dependent_bridges, vec![dep_id.clone()]);

    let err = inv
        .delete(
            &BridgeSelector::Exact(base_id.clone()),
            inv.generation(),
            "del-blocked",
        )
        .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::DependencyInUse));

    inv.delete(&BridgeSelector::Exact(dep_id), inv.generation(), "del-dep")
        .unwrap();
    inv.delete(
        &BridgeSelector::Exact(base_id),
        inv.generation(),
        "del-base",
    )
    .unwrap();
    assert!(inv.list().is_empty());
}

#[test]
fn deterministic_export_and_reopen() {
    let (mut inv, research, genealogy) = base_inventory();
    let document = doc(
        "https://graphforge.dev/bridge/export",
        "1.0.0",
        &research,
        &genealogy,
        vec![assertion(
            q(&research, SymbolKind::Entity, "Person"),
            q(&genealogy, SymbolKind::Entity, "Person"),
            BridgePredicate::Equivalent,
        )],
    );
    let digest_a = bridge_document_digest(&document).unwrap();
    let id = inv.create_register(document.clone(), "c").unwrap();
    assert_eq!(id.canonical_digest, digest_a);
    inv.adopt(&BridgeSelector::Exact(id.clone()), 0, "a")
        .unwrap();

    let json = inv
        .export_bridge(&BridgeSelector::Exact(id.clone()), BridgeExportFormat::Json)
        .unwrap();
    let yaml = inv
        .export_bridge(&BridgeSelector::Exact(id.clone()), BridgeExportFormat::Yaml)
        .unwrap();
    let round_json: BridgeDocument = serde_json::from_str(&json).unwrap();
    assert_eq!(bridge_document_digest(&round_json).unwrap(), digest_a);

    // YAML round-trip via lifecycle import (avoid direct serde_yaml crate name;
    // Bazel crate_universe exposes it as serde_yaml_ng).
    let mut staging = BridgeInventory::new(ActivationMode::Exploratory, Default::default());
    for table in [
        table(research.clone(), &["Study", "Person"], &["FUNDED_BY"], &["title"]),
        table(
            genealogy.clone(),
            &["Person", "Claim"],
            &["PARENT_OF"],
            &["name"],
        ),
    ] {
        staging.register_module(table);
    }
    let yaml_id = staging
        .import_text(&yaml, BridgeImportFormatHint::Yaml, "yaml-round")
        .unwrap();
    assert_eq!(yaml_id.canonical_digest, digest_a);

    let snap = inv.snapshot();
    let reopened = BridgeInventory::reopen(snap).unwrap();
    assert_eq!(reopened.generation(), inv.generation());
    assert_eq!(reopened.list().len(), 1);
    assert_eq!(reopened.list()[0].id, id);
}

#[test]
fn suggested_mappings_remain_non_authoritative() {
    let (mut inv, research, genealogy) = base_inventory();
    let mut document = doc(
        "https://graphforge.dev/bridge/suggested",
        "1.0.0",
        &research,
        &genealogy,
        vec![assertion(
            q(&research, SymbolKind::Entity, "Person"),
            q(&genealogy, SymbolKind::Entity, "Person"),
            BridgePredicate::Equivalent,
        )],
    );
    document.assertions[0].provenance.method = MappingMethod::Suggested;
    document.assertions[0].provenance.justification = "tooling hint".into();

    let err = inv.create_register(document.clone(), "c").unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::LifecycleInvalidTransition));

    let text = serde_json::to_string(&document).unwrap();
    let id = inv
        .import_text(&text, BridgeImportFormatHint::Json, "imp")
        .unwrap();
    let err = inv
        .adopt(&BridgeSelector::Exact(id), inv.generation(), "adopt")
        .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::LifecycleInvalidTransition));
}

#[test]
fn equal_human_names_never_create_a_bridge() {
    let (inv, research, genealogy) = base_inventory();
    // Both modules declare Person; inventory of bridges stays empty.
    assert!(inv.list().is_empty());
    assert!(inv.adopted_ids().is_empty());
    // No auto-bridge API exists; same local name is not an equivalence.
    let left = q(&research, SymbolKind::Entity, "Person");
    let right = q(&genealogy, SymbolKind::Entity, "Person");
    assert_eq!(left.local_id, right.local_id);
    assert_ne!(left.module, right.module);
    let _ = HashSet::<BridgeSetId>::new();
}

#[test]
fn stale_generation_fails_closed() {
    let (mut inv, research, genealogy) = base_inventory();
    let document = doc(
        "https://graphforge.dev/bridge/stale",
        "1.0.0",
        &research,
        &genealogy,
        vec![assertion(
            q(&research, SymbolKind::Entity, "Person"),
            q(&genealogy, SymbolKind::Entity, "Person"),
            BridgePredicate::Equivalent,
        )],
    );
    let id = inv.create_register(document, "c").unwrap();
    let err = inv.adopt(&BridgeSelector::Exact(id), 99, "a").unwrap_err();
    assert_eq!(
        err.code(),
        Some(DiagnosticCode::InventoryGenerationConflict)
    );
}

#[test]
fn missing_symbol_in_module_rejected() {
    let (inv, research, genealogy) = base_inventory();
    let document = doc(
        "https://graphforge.dev/bridge/nosymbol",
        "1.0.0",
        &research,
        &genealogy,
        vec![assertion(
            q(&research, SymbolKind::Entity, "DoesNotExist"),
            q(&genealogy, SymbolKind::Entity, "Person"),
            BridgePredicate::Related,
        )],
    );
    let err = inv.validate_document(&document).unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::BridgeEndpointMissing));
}
