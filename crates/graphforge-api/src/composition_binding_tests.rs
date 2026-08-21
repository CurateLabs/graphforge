use std::sync::{Arc, Mutex};

use graphforge_ir::{
    Binder, BindingDecision, BindingDiagnosticCode, CompositionBindingContext,
    CompositionBindingLimits, RuntimeCatalog,
};
use graphforge_ontology::{
    ActivationMode, ActivationRecord, ActivationScope, AuthoredModule, BridgeAssertion,
    BridgeDocument, BridgePredicate, BridgeProvenance, BridgeSetId, CompositionLimits,
    EntityTypeDef, InventoryCompileRequest, MappingMethod, OntologyDoc, OntologyModuleId,
    PropertyDef, PropertyValueType, QualifiedSymbol, SymbolKind, bridge_document_digest,
    compile_inventory, module_document_digest,
};

use super::GraphForge;
use graphforge_core::OntologyMode;

fn module(name: &str, entities: &[&str]) -> AuthoredModule {
    let doc = OntologyDoc {
        ontology_id: format!("https://graphforge.dev/ontology/{name}"),
        version: "1.0.0".to_owned(),
        entity_types: entities
            .iter()
            .map(|entity| EntityTypeDef {
                name: (*entity).to_owned(),
                r#abstract: false,
                parent: None,
            })
            .collect(),
        relation_types: vec![],
        properties: (name == "research")
            .then_some(PropertyDef {
                owner: "Person".to_owned(),
                name: "name".to_owned(),
                value_type: PropertyValueType::Utf8,
                nullable: true,
                multivalued: false,
                default_json: None,
            })
            .into_iter()
            .collect(),
        constraints: vec![],
        migrations: vec![],
    };
    AuthoredModule {
        id: OntologyModuleId {
            ontology_id: doc.ontology_id.clone(),
            authored_version: doc.version.clone(),
            canonical_digest: module_document_digest(&doc).expect("module digest"),
        },
        dependencies: vec![],
        doc,
        allow_projected_identity: false,
    }
}

fn composed_fixture(
    default: ActivationMode,
) -> (
    Arc<CompositionBindingContext>,
    QualifiedSymbol,
    QualifiedSymbol,
) {
    composed_fixture_with_predicates(default, &[BridgePredicate::Equivalent])
}

fn composed_fixture_with_predicates(
    default: ActivationMode,
    predicates: &[BridgePredicate],
) -> (
    Arc<CompositionBindingContext>,
    QualifiedSymbol,
    QualifiedSymbol,
) {
    let research = module("research", &["Person", "Study"]);
    let genealogy = module("genealogy", &["Person"]);
    let source = QualifiedSymbol {
        module: research.id.clone(),
        kind: SymbolKind::Entity,
        local_id: "Person".to_owned(),
    };
    let target = QualifiedSymbol {
        module: genealogy.id.clone(),
        kind: SymbolKind::Entity,
        local_id: "Person".to_owned(),
    };
    let bridges = predicates
        .iter()
        .enumerate()
        .map(|(index, predicate)| BridgeDocument {
            bridge_id: format!("https://graphforge.dev/bridge/person-{index}"),
            authored_version: "1.0.0".to_owned(),
            source_modules: vec![research.id.clone()],
            target_modules: vec![genealogy.id.clone()],
            dependencies: vec![],
            shared_surfaces: vec![],
            assertions: vec![BridgeAssertion {
                source: source.clone(),
                target: target.clone(),
                predicate: *predicate,
                directional: !predicate.is_symmetric(),
                provenance: BridgeProvenance {
                    method: MappingMethod::Authored,
                    confidence: None,
                    justification: "governed person mapping".to_owned(),
                    evidence_refs: vec![],
                },
                valid_from: None,
                valid_to: None,
            }],
            enforcement: Some(ActivationMode::Strict),
        })
        .collect::<Vec<_>>();
    let bridge_ids = bridges
        .iter()
        .map(|bridge| BridgeSetId {
            bridge_id: bridge.bridge_id.clone(),
            authored_version: bridge.authored_version.clone(),
            canonical_digest: bridge_document_digest(bridge).expect("bridge digest"),
        })
        .collect::<Vec<_>>();
    let activation = vec![ActivationRecord {
        scope: ActivationScope::Module,
        subject: research.id.display_ref(),
        mode: ActivationMode::Advisory,
    }];
    let composition = compile_inventory(InventoryCompileRequest {
        modules: &[research, genealogy],
        bridges: &bridge_ids,
        activation: &activation,
        profile_default: default,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .expect("composition");
    (
        Arc::new(CompositionBindingContext::new(
            Arc::new(composition),
            bridges,
            CompositionBindingLimits::default(),
        )),
        source,
        target,
    )
}

#[test]
fn facade_executes_qualified_and_unique_composed_queries() {
    let forge = GraphForge::new(None).expect("facade");
    let (context, _, _) = composed_fixture(ActivationMode::Strict);

    forge
        .execute_with_composition("MATCH (n:`research:Person`) RETURN n", context.clone())
        .expect("qualified execution");
    forge
        .execute_with_composition("MATCH (n:Study) RETURN n", context)
        .expect("unique shorthand execution");
}

#[test]
fn graph_plan_carries_exact_composition_identity_and_receipts() {
    let (context, _, _) = composed_fixture(ActivationMode::Strict);
    let ast = graphforge_cypher::parse("MATCH (n:`research:Person`) RETURN n").expect("parse");
    let plan = Binder::new(
        None,
        Arc::new(Mutex::new(RuntimeCatalog::new())),
        OntologyMode::Exploratory,
    )
    .with_composition(context.clone())
    .bind(&ast)
    .expect("bind");
    assert_eq!(
        plan.composition_fingerprint.as_deref(),
        Some(context.fingerprint())
    );
    assert_eq!(plan.binding_receipts.len(), 1);
    assert_eq!(
        plan.binding_receipts[0].composition_fingerprint,
        context.fingerprint()
    );
}

#[test]
fn facade_rejects_ambiguity_without_publishing_runtime_observations() {
    let forge = GraphForge::new(None).expect("facade");
    let (context, _, _) = composed_fixture(ActivationMode::Strict);
    let error = forge
        .execute_with_composition("MATCH (n:Person) RETURN n", context)
        .expect_err("ambiguous shorthand must fail");
    assert!(error.to_string().contains("ambiguous_symbol"), "{error:?}");

    let first = forge
        .runtime_catalog()
        .lock()
        .expect("catalog")
        .intern_label("after_failed_bind");
    assert_eq!(first.0, 0, "failed bind published a catalog observation");
}

#[test]
fn bridge_selection_and_explain_are_exact_bounded_and_repeatable() {
    let (context, source, target) = composed_fixture(ActivationMode::Exploratory);
    let first = context.select_bridge(&source, &target).expect("bridge");
    let second = context
        .select_bridge(&target, &source)
        .expect("symmetric bridge");
    assert_eq!(
        first.composition_fingerprint,
        second.composition_fingerprint
    );
    assert_eq!(first.effective_mode, second.effective_mode);
    assert_eq!(first.effective_mode, ActivationMode::Strict);
    assert!(matches!(
        first.decisions.as_slice(),
        [BindingDecision::Bridge { bridge_id, predicate, .. }]
            if bridge_id == "https://graphforge.dev/bridge/person-0"
                && predicate == "equivalent"
    ));
    assert_eq!(
        serde_json::to_string(&first).expect("receipt json"),
        serde_json::to_string(&context.select_bridge(&source, &target).expect("repeat"))
            .expect("repeat json")
    );

    let invalid = QualifiedSymbol {
        local_id: "Missing".to_owned(),
        ..source
    };
    assert_eq!(
        context.select_bridge(&invalid, &target).unwrap_err().code,
        BindingDiagnosticCode::UnknownSymbol
    );
}

#[test]
fn conflicting_bridge_paths_are_rejected_with_bounded_candidates() {
    let (context, source, target) = composed_fixture_with_predicates(
        ActivationMode::Exploratory,
        &[BridgePredicate::Equivalent, BridgePredicate::Related],
    );
    let error = context
        .select_bridge(&source, &target)
        .expect_err("conflicting predicates");
    assert_eq!(error.code, BindingDiagnosticCode::ConflictingBridgePaths);
    assert_eq!(error.candidates, ["equivalent", "related"]);
}

#[test]
fn scoped_advisory_and_exploratory_fallbacks_emit_distinct_receipts() {
    let (context, _, _) = composed_fixture(ActivationMode::Exploratory);
    let (_, advisory) = context
        .resolve(SymbolKind::Entity, "research:Unknown")
        .expect("advisory fallback");
    let (_, exploratory) = context
        .resolve(SymbolKind::Entity, "Unknown")
        .expect("exploratory fallback");
    assert_eq!(advisory.effective_mode, ActivationMode::Advisory);
    assert_eq!(
        advisory.diagnostics[0].code,
        BindingDiagnosticCode::RuntimeFallback
    );
    assert_eq!(exploratory.effective_mode, ActivationMode::Exploratory);
    assert!(exploratory.diagnostics.is_empty());
}

#[test]
fn facade_rejects_a_property_on_the_wrong_composed_owner() {
    let forge = GraphForge::new(None).expect("facade");
    let (context, _, _) = composed_fixture(ActivationMode::Strict);
    let error = forge
        .execute_with_composition("MATCH (n:`research:Study`) RETURN n.name", context)
        .expect_err("wrong-owner property must fail");
    assert!(
        error.to_string().contains("wrong_owner_property"),
        "{error:?}"
    );
}
