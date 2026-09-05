//! Direct tests for the multi-ontology inventory compiler (#836).

use std::sync::atomic::{AtomicBool, Ordering};

use graphforge_ontology::{
    ActivationMode, ActivationRecord, ActivationScope, AuthoredModule, BridgeSetId,
    CompositionLimits, DiagnosticCode, EntityTypeDef, InventoryCompileRequest, OntologyDoc,
    OntologyModuleId, ResolveRequest, SymbolKind, compile_inventory,
    compile_legacy_single_ontology, module_document_digest,
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

fn authored(doc: OntologyDoc, dependencies: Vec<OntologyModuleId>) -> AuthoredModule {
    let digest = module_document_digest(&doc).expect("digest");
    AuthoredModule {
        id: OntologyModuleId {
            ontology_id: doc.ontology_id.clone(),
            authored_version: doc.version.clone(),
            canonical_digest: digest,
        },
        dependencies,
        doc,
        allow_projected_identity: false,
    }
}

#[test]
fn fingerprint_stable_under_inventory_reorder() {
    let genealogy = authored(
        entity_doc(
            "https://graphforge.dev/ontology/genealogy",
            "3.0.0",
            &["Person"],
        ),
        vec![],
    );
    let provenance = authored(
        entity_doc(
            "https://graphforge.dev/ontology/provenance",
            "1.2.0",
            &["Person", "Activity"],
        ),
        vec![],
    );
    // Pin existing authored documents as well as relative reorder stability.
    // These hashes were independently computed over literal canonical JSON.
    assert_eq!(
        genealogy.id.canonical_digest,
        "cdfae1502134b6ac5509efb01b1711d04ac0d7f3a6504de46628dca860609363"
    );
    assert_eq!(
        provenance.id.canonical_digest,
        "c3b7b56a3d1ae2c60c7de427865c68e963652bd363e9672fdc993ec98a56a3e9"
    );
    let bridges = [BridgeSetId {
        bridge_id: "https://graphforge.dev/bridge/research-document".into(),
        authored_version: "1.0.0".into(),
        canonical_digest: "1af03d417388faf01178eaadb579657ff78c7248e807d588c4f9239686f46079".into(),
    }];
    let activation = [ActivationRecord {
        scope: ActivationScope::Module,
        subject: genealogy.id.display_ref(),
        mode: ActivationMode::Strict,
    }];

    let forward = compile_inventory(InventoryCompileRequest {
        modules: &[genealogy.clone(), provenance.clone()],
        bridges: &bridges,
        activation: &activation,
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .expect("forward");
    let reverse = compile_inventory(InventoryCompileRequest {
        modules: &[provenance, genealogy],
        bridges: &bridges,
        activation: &activation,
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .expect("reverse");

    assert_eq!(forward.fingerprint, reverse.fingerprint);
    assert_eq!(
        forward.fingerprint,
        "82a22a7bb3512ea90074fb4ac100cdeef50ebfbf4c351c46e51a81aff715612c"
    );
    assert_eq!(
        forward.modules.iter().map(|m| &m.id).collect::<Vec<_>>(),
        reverse.modules.iter().map(|m| &m.id).collect::<Vec<_>>()
    );
}

#[test]
fn repeated_compilation_is_deterministic() {
    let module = authored(
        entity_doc(
            "https://graphforge.dev/ontology/research",
            "2.0.0",
            &["Study"],
        ),
        vec![],
    );
    let request = InventoryCompileRequest {
        modules: std::slice::from_ref(&module),
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    };
    let a = compile_inventory(request.clone()).unwrap();
    let b = compile_inventory(request).unwrap();
    assert_eq!(a.fingerprint, b.fingerprint);
    assert_eq!(a.modules[0].symbols, b.modules[0].symbols);
}

#[test]
fn qualified_resolution_succeeds_and_unqualified_ambiguity_fails() {
    let genealogy = authored(
        entity_doc(
            "https://graphforge.dev/ontology/genealogy",
            "3.0.0",
            &["Person"],
        ),
        vec![],
    );
    let provenance = authored(
        entity_doc(
            "https://graphforge.dev/ontology/provenance",
            "1.2.0",
            &["Person"],
        ),
        vec![],
    );
    let composition = compile_inventory(InventoryCompileRequest {
        modules: &[genealogy.clone(), provenance],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();

    let qualified = composition
        .resolve(&ResolveRequest {
            module: Some(&genealogy.id),
            kind: SymbolKind::Entity,
            local_id: "Person",
            max_candidates: 64,
        })
        .unwrap();
    assert_eq!(qualified.symbol.display(), "genealogy:entity:Person");
    assert!(!qualified.via_unqualified);

    let err = composition
        .resolve(&ResolveRequest {
            module: None,
            kind: SymbolKind::Entity,
            local_id: "Person",
            max_candidates: 64,
        })
        .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::ResolutionAmbiguous));
    let diagnostic = &err.diagnostics[0];
    assert_eq!(
        diagnostic.candidates,
        vec![
            "genealogy:Person".to_owned(),
            "provenance:Person".to_owned()
        ]
    );
}

#[test]
fn unique_unqualified_resolution_succeeds() {
    let research = authored(
        entity_doc(
            "https://graphforge.dev/ontology/research",
            "2.0.0",
            &["Study"],
        ),
        vec![],
    );
    let composition = compile_inventory(InventoryCompileRequest {
        modules: std::slice::from_ref(&research),
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    let outcome = composition
        .resolve(&ResolveRequest {
            module: None,
            kind: SymbolKind::Entity,
            local_id: "Study",
            max_candidates: 8,
        })
        .unwrap();
    assert!(outcome.via_unqualified);
    assert_eq!(outcome.symbol.local_id, "Study");
}

#[test]
fn missing_dependency_and_cycle_fail_before_activation() {
    let evidence = authored(
        entity_doc(
            "https://graphforge.dev/ontology/evidence",
            "1.0.0",
            &["Claim"],
        ),
        vec![],
    );
    let research_doc = entity_doc(
        "https://graphforge.dev/ontology/research",
        "2.0.0",
        &["Study"],
    );
    let research_digest = module_document_digest(&research_doc).unwrap();
    let research_id = OntologyModuleId {
        ontology_id: research_doc.ontology_id.clone(),
        authored_version: research_doc.version.clone(),
        canonical_digest: research_digest,
    };
    // Missing dependency: research depends on a digest that is not in the inventory.
    let missing_dep = OntologyModuleId {
        ontology_id: "https://graphforge.dev/ontology/evidence".into(),
        authored_version: "1.0.0".into(),
        canonical_digest: "00".repeat(32),
    };
    let research = AuthoredModule {
        id: research_id.clone(),
        dependencies: vec![missing_dep],
        doc: research_doc.clone(),
        allow_projected_identity: false,
    };
    let err = compile_inventory(InventoryCompileRequest {
        modules: &[research, evidence.clone()],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::DependencyMissing));

    // Cycle: research <-> evidence
    let evidence_id = evidence.id.clone();
    let research_cyc = AuthoredModule {
        id: research_id.clone(),
        dependencies: vec![evidence_id.clone()],
        doc: research_doc,
        allow_projected_identity: false,
    };
    let evidence_cyc = AuthoredModule {
        id: evidence_id,
        dependencies: vec![research_id],
        doc: evidence.doc,
        allow_projected_identity: false,
    };
    let err = compile_inventory(InventoryCompileRequest {
        modules: &[research_cyc, evidence_cyc],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::DependencyCycle));
}

#[test]
fn digest_mismatch_and_runtime_catalog_id_rejected() {
    let mut module = authored(
        entity_doc(
            "https://graphforge.dev/ontology/science",
            "2.1.0",
            &["Specimen"],
        ),
        vec![],
    );
    module.id.canonical_digest = "ff".repeat(32);
    let err = compile_inventory(InventoryCompileRequest {
        modules: std::slice::from_ref(&module),
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::InterchangeIntegrity));

    let mut bad = authored(entity_doc("42", "1", &["Node"]), vec![]);
    // Force numeric ontology_id through identity (document already matches).
    bad.id.ontology_id = "42".into();
    bad.doc.ontology_id = "42".into();
    let digest = module_document_digest(&bad.doc).unwrap();
    bad.id.canonical_digest = digest;
    let err = compile_inventory(InventoryCompileRequest {
        modules: std::slice::from_ref(&bad),
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::CollisionMetadata));
}

#[test]
fn module_limit_and_cancellation() {
    let module = authored(
        entity_doc(
            "https://graphforge.dev/ontology/document",
            "1.0.0",
            &["Document"],
        ),
        vec![],
    );
    let err = compile_inventory(InventoryCompileRequest {
        modules: std::slice::from_ref(&module),
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits {
            modules: 0,
            ..CompositionLimits::default()
        },
        cancelled: None,
    })
    .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::ResourceModules));

    let cancelled = AtomicBool::new(true);
    let err = compile_inventory(InventoryCompileRequest {
        modules: std::slice::from_ref(&module),
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: Some(&cancelled),
    })
    .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::LifecycleCancelled));
    assert!(
        !cancelled.load(Ordering::Relaxed)
            || err.code() == Some(DiagnosticCode::LifecycleCancelled)
    );
}

#[test]
fn fingerprint_excludes_runtime_local_state() {
    let module = authored(
        entity_doc(
            "https://graphforge.dev/ontology/evidence",
            "1.0.0",
            &["Claim"],
        ),
        vec![],
    );
    let composition = compile_inventory(InventoryCompileRequest {
        modules: std::slice::from_ref(&module),
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    // Runtime IDs exist on the compiled module but never enter the fingerprint bytes.
    assert!(
        composition.modules[0]
            .runtime
            .entity_name_to_id
            .contains_key("Claim")
    );
    assert!(!composition.fingerprint.contains("Claim"));
    assert_eq!(composition.fingerprint.len(), 64);
}

#[test]
fn legacy_single_ontology_projection_preserves_behavior() {
    let doc = entity_doc("core", "2026.05", &["Person", "Employee"]);
    let legacy = compile_legacy_single_ontology(&doc, false, CompositionLimits::default()).unwrap();
    assert_eq!(legacy.modules.len(), 1);
    assert_eq!(legacy.modules[0].id.ontology_id, "core");
    let outcome = legacy
        .resolve(&ResolveRequest {
            module: None,
            kind: SymbolKind::Entity,
            local_id: "Person",
            max_candidates: 8,
        })
        .unwrap();
    assert_eq!(outcome.symbol.local_id, "Person");

    let published =
        compile_legacy_single_ontology(&doc, true, CompositionLimits::default()).unwrap();
    assert!(published.modules[0].id.ontology_id.starts_with("legacy:"));
    assert_eq!(published.modules[0].id.authored_version, "legacy-v1");
    // Authored ontology digest is retained in the identity digest field.
    assert_eq!(
        published.modules[0].id.canonical_digest,
        module_document_digest(&doc).unwrap()
    );
}

#[test]
fn modules_retain_separate_authority_not_flattened() {
    let a = authored(
        entity_doc("https://graphforge.dev/ontology/a", "1", &["Shared"]),
        vec![],
    );
    let b = authored(
        entity_doc("https://graphforge.dev/ontology/b", "1", &["Shared"]),
        vec![],
    );
    let composition = compile_inventory(InventoryCompileRequest {
        modules: &[a.clone(), b.clone()],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Advisory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    let left = composition.module(&a.id).unwrap();
    let right = composition.module(&b.id).unwrap();
    assert_eq!(left.doc.ontology_id, "https://graphforge.dev/ontology/a");
    assert_eq!(right.doc.ontology_id, "https://graphforge.dev/ontology/b");
    // Each module keeps its own runtime ID space; equal local names do not merge.
    assert_eq!(left.runtime.entity_name_to_id["Shared"], 0);
    assert_eq!(right.runtime.entity_name_to_id["Shared"], 0);
    assert_ne!(left.id, right.id);
}

#[test]
fn non_nfc_identifier_rejected() {
    let mut module = authored(
        entity_doc("https://graphforge.dev/ontology/cafe", "1", &["Place"]),
        vec![],
    );
    // U+0065 U+0301 (e + combining acute) is not NFC; NFC is U+00E9.
    module.id.ontology_id = "https://graphforge.dev/ontology/caf\u{0065}\u{0301}".into();
    module.doc.ontology_id = module.id.ontology_id.clone();
    module.id.canonical_digest = module_document_digest(&module.doc).unwrap();
    let err = compile_inventory(InventoryCompileRequest {
        modules: std::slice::from_ref(&module),
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Exploratory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap_err();
    assert_eq!(err.code(), Some(DiagnosticCode::CollisionMetadata));
}
