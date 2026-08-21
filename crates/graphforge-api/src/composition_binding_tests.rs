use std::sync::{Arc, Mutex};

use graphforge_ir::{
    Binder, BindingDecision, BindingDiagnosticCode, CompositionBindingContext,
    CompositionBindingLimits, RuntimeCatalog,
};
use graphforge_ontology::{
    ActivationMode, ActivationRecord, ActivationScope, AuthoredModule, BridgeAssertion,
    BridgeDocument, BridgePredicate, BridgeProvenance, BridgeSetId, CompositionLimits,
    EntityTypeDef, InventoryCompileRequest, MappingMethod, OntologyDoc, OntologyModuleId,
    PropertyDef, PropertyValueType, QualifiedSymbol, RelationTypeDef, SemanticFlags, SymbolKind,
    bridge_document_digest, compile_inventory, module_document_digest,
};

use super::GraphForge;
use graphforge_core::OntologyMode;

fn install_composition_authority(forge: &GraphForge, fingerprint: &str) {
    use graphforge_storage::{
        ProjectCapability, ProjectGenerationRequest, ProjectParticipant,
        ProjectParticipantEncoding, ProjectStageOutcome,
    };
    use sha2::{Digest, Sha256};

    let parent =
        graphforge_storage::resolve_project_generation(forge.resolved_generation.container_root())
            .unwrap();
    let mut participants = parent
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .map(|snapshot| ProjectParticipant {
            capability_id: snapshot.capability_id,
            capability_version: snapshot.capability_version,
            record_family_id: snapshot.record_family_id,
            record_version: snapshot.record_version,
            encoding: super::participant_encoding(&snapshot.encoding).unwrap(),
            schema_fingerprint: snapshot.schema_fingerprint,
            row_count: snapshot.row_count,
            bytes: snapshot.bytes,
        })
        .collect::<Vec<_>>();
    let mut bytes = serde_json::to_vec(&serde_json::json!({
        "composition_fingerprint": fingerprint
    }))
    .unwrap();
    bytes.push(b'\n');
    participants.push(ProjectParticipant {
        capability_id: "workspace".into(),
        capability_version: 1,
        record_family_id: "ontology_composition".into(),
        record_version: 1,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: Sha256::digest(b"test-composition-authority/1").into(),
        row_count: 1,
        bytes,
    });
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    let request = ProjectGenerationRequest {
        transaction_uuid: uuid::Uuid::now_v7(),
        generation_uuid: uuid::Uuid::now_v7(),
        capabilities: parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        participants,
    };
    match forge.stage_project_generation(&request).unwrap() {
        ProjectStageOutcome::Staged(staged) => staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap(),
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
    };
}

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
        relation_types: vec![RelationTypeDef {
            name: "KNOWS".to_owned(),
            src: "Person".to_owned(),
            dst: "Person".to_owned(),
            inverse: None,
            semantic: SemanticFlags::default(),
        }],
        properties: vec![
            PropertyDef {
                owner: "Person".to_owned(),
                name: "name".to_owned(),
                value_type: PropertyValueType::Utf8,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
            PropertyDef {
                owner: "KNOWS".to_owned(),
                name: "since".to_owned(),
                value_type: PropertyValueType::Int64,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
        ],
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
fn facade_does_not_install_unpublished_semantic_bindings() {
    let forge = GraphForge::new(None).expect("facade");
    let (context, _, _) = composed_fixture(ActivationMode::Strict);

    forge
        .execute_with_composition("MATCH (n:`research:Person`) RETURN n", context.clone())
        .expect("read");
    assert!(forge.semantic_storage_bindings.lock().unwrap().is_none());

    forge
        .execute_with_composition("MATCH (n:Person) RETURN n", context)
        .expect_err("ambiguous bind");
    assert!(forge.semantic_storage_bindings.lock().unwrap().is_none());
}

#[test]
fn stale_parent_publication_leaves_graph_and_bindings_at_old_generation() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("project");
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    let before = graphforge_storage::resolve_project_generation(&path)
        .unwrap()
        .generation_uuid();
    let (context, _, _) = composed_fixture(ActivationMode::Strict);
    *forge.current_generation_uuid.lock().unwrap() = uuid::Uuid::now_v7();

    let error = forge
        .execute_with_composition(
            "CREATE (n:`research:Person` {name:'must-not-publish'})",
            context,
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("generation changed"),
        "{error:?}"
    );
    assert!(forge.semantic_storage_bindings.lock().unwrap().is_none());
    assert_eq!(
        graphforge_storage::resolve_project_generation(&path)
            .unwrap()
            .generation_uuid(),
        before
    );
    drop(forge);
    let reopened = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    assert!(reopened.semantic_storage_bindings.lock().unwrap().is_none());
    assert_eq!(
        reopened
            .execute("MATCH (n) RETURN n")
            .unwrap()
            .batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
}

#[test]
fn generation_hydration_rejects_incomplete_bindings_with_matching_fingerprint() {
    let forge = GraphForge::new(None).unwrap();
    let (context, _, _) = composed_fixture(ActivationMode::Strict);
    let complete =
        graphforge_storage::SemanticStorageBindings::project(context.composition(), None).unwrap();
    let mut incomplete = complete.bindings.clone();
    incomplete.pop();
    *forge.semantic_storage_bindings.lock().unwrap() = Some(
        graphforge_storage::SemanticStorageBindings::new(
            context.fingerprint().to_owned(),
            incomplete,
        )
        .unwrap(),
    );
    assert!(
        forge
            .install_generation_composition_context(&context)
            .is_err()
    );
    assert!(forge.default_composition_context.lock().unwrap().is_none());
}

#[test]
fn semantic_publication_replays_one_durable_operation_idempotently() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("project");
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    let (context, _, _) = composed_fixture(ActivationMode::Strict);
    install_composition_authority(&forge, context.fingerprint());
    drop(forge);
    let forge = GraphForge::new(Some(path.to_str().unwrap())).unwrap();
    forge
        .execute_with_composition(
            "CREATE (n:`research:Person` {name:'first'})",
            context.clone(),
        )
        .unwrap();
    forge
        .install_generation_composition_context(&context)
        .unwrap();
    let result = forge
        .execute_write_without_publish(
            "CREATE (n:`genealogy:Person` {name:'replayed'})",
            &std::collections::HashMap::new(),
        )
        .unwrap();
    let receipt = result.mutation_receipt.unwrap();
    let operation = uuid::Uuid::now_v7();
    forge
        .publish_graph_mutation_with_context(&receipt, operation, None, 1)
        .unwrap();
    let first = graphforge_storage::resolve_project_generation(&path)
        .unwrap()
        .generation_uuid();
    forge
        .publish_graph_mutation_with_context(&receipt, operation, None, 1)
        .unwrap();
    assert_eq!(
        graphforge_storage::resolve_project_generation(&path)
            .unwrap()
            .generation_uuid(),
        first
    );
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

#[test]
fn facade_publishes_reopens_and_queries_exact_colliding_semantic_routes() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("project");
    let path_text = path.to_str().unwrap();
    let first = GraphForge::new(Some(path_text)).unwrap();
    let (context, _, _) = composed_fixture(ActivationMode::Strict);
    install_composition_authority(&first, context.fingerprint());
    drop(first);

    let forge = GraphForge::new(Some(path_text)).unwrap();
    forge
        .execute_with_composition(
            "CREATE (n:`research:Person` {name: 'Ada'})",
            context.clone(),
        )
        .unwrap();
    forge
        .execute_with_composition("CREATE (n:`genealogy:Person`)", context.clone())
        .unwrap();
    drop(forge);

    let reopened = GraphForge::new(Some(path_text)).unwrap();
    reopened
        .install_generation_composition_context(&context)
        .unwrap();
    reopened
        .execute("MATCH (n:`research:Person`) SET n.name = 'Ada Lovelace'")
        .unwrap();
    let research = reopened
        .execute("MATCH (n:`research:Person`) RETURN n.name")
        .unwrap();
    assert_eq!(
        research
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        arrow::util::display::array_value_to_string(research.batches[0].column(0), 0).unwrap(),
        "Ada Lovelace"
    );
    reopened
        .execute("MATCH (n:`research:Person`) REMOVE n.name")
        .unwrap();
    let removed = reopened
        .execute("MATCH (n:`research:Person`) RETURN n.name")
        .unwrap();
    assert_eq!(
        arrow::util::display::array_value_to_string(removed.batches[0].column(0), 0).unwrap(),
        ""
    );
    let genealogy = reopened
        .execute("MATCH (n:`genealogy:Person`) RETURN n")
        .unwrap();
    assert_eq!(
        genealogy
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );

    let generation = graphforge_storage::resolve_project_generation(&path).unwrap();
    let bindings = graphforge_storage::semantic_storage_bindings(&generation)
        .unwrap()
        .unwrap();
    let entity_routes = bindings
        .bindings
        .iter()
        .filter(|binding| binding.route_kind == graphforge_storage::SemanticRouteKind::Entity)
        .map(|binding| binding.route.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(entity_routes.len(), 3);
}

#[test]
fn facade_reopens_relation_edge_property_through_default_context() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("project");
    let path_text = path.to_str().unwrap();
    let bootstrap = GraphForge::new(Some(path_text)).unwrap();
    let (context, _, _) = composed_fixture(ActivationMode::Strict);
    install_composition_authority(&bootstrap, context.fingerprint());
    drop(bootstrap);

    let forge = GraphForge::new(Some(path_text)).unwrap();
    forge
        .execute_with_composition(
            "CREATE (a:`research:Person` {name:'A'}), (b:`research:Person` {name:'B'})",
            context.clone(),
        )
        .unwrap();
    forge
        .execute_with_composition(
            "MATCH (a:`research:Person` {name:'A'}), (b:`research:Person` {name:'B'}) CREATE (a)-[r:`research:KNOWS` {since:2020}]->(b)",
            context.clone(),
        )
        .unwrap();
    forge
        .execute_with_composition(
            "CREATE (a:`genealogy:Person` {name:'G1'}), (b:`genealogy:Person` {name:'G2'})",
            context.clone(),
        )
        .unwrap();
    forge
        .execute_with_composition(
            "MATCH (a:`genealogy:Person` {name:'G1'}), (b:`genealogy:Person` {name:'G2'}) CREATE (a)-[r:`genealogy:KNOWS` {since:1999}]->(b)",
            context.clone(),
        )
        .unwrap();
    let installed = forge
        .semantic_storage_bindings
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    let relation = installed
        .bindings
        .iter()
        .find(|binding| {
            binding.route_kind == graphforge_storage::SemanticRouteKind::Relation
                && binding.symbol.module.ontology_id.ends_with("/research")
        })
        .unwrap();
    let genealogy_relation = installed
        .bindings
        .iter()
        .find(|binding| {
            binding.route_kind == graphforge_storage::SemanticRouteKind::Relation
                && binding.symbol.module.ontology_id.ends_with("/genealogy")
        })
        .unwrap();
    assert_ne!(relation.route, genealogy_relation.route);
    let edge_path = forge
        .dir
        .join("topology/edges")
        .join(format!("{}.parquet", relation.route));
    assert!(
        edge_path.exists(),
        "missing opaque edge route {}",
        edge_path.display()
    );
    let edge_rows = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&edge_path).unwrap(),
    )
    .unwrap()
    .metadata()
    .file_metadata()
    .num_rows();
    assert_eq!(edge_rows, 1);
    let edge_batch = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&edge_path).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    let src_id = edge_batch
        .column_by_name("src_id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap()
        .value(0);
    let dst_id = edge_batch
        .column_by_name("dst_id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap()
        .value(0);
    let node_batch = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(forge.dir.join("topology/nodes.parquet")).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    let node_ids = node_batch
        .column_by_name("node_id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    assert!(node_ids.values().contains(&src_id));
    assert!(node_ids.values().contains(&dst_id));
    drop(forge);

    let reopened = GraphForge::new(Some(path_text)).unwrap();
    reopened
        .install_generation_composition_context(&context)
        .unwrap();
    let reopened_edge = reopened
        .dir
        .join("topology/edges")
        .join(edge_path.file_name().unwrap());
    assert!(
        reopened_edge.exists(),
        "opaque edge route was not materialized"
    );
    let before = reopened
        .execute("MATCH (a:`research:Person`)-[r:`research:KNOWS`]->(b:`research:Person`) RETURN r.since")
        .unwrap();
    let before_batch = before
        .batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("qualified relation query returned no rows");
    assert_eq!(
        arrow::util::display::array_value_to_string(before_batch.column(0), 0).unwrap(),
        "2020"
    );
    let genealogy = reopened
        .execute("MATCH (a:`genealogy:Person`)-[r:`genealogy:KNOWS`]->(b:`genealogy:Person`) RETURN r.since")
        .unwrap();
    let genealogy_batch = genealogy
        .batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("colliding genealogy relation query returned no rows");
    assert_eq!(
        arrow::util::display::array_value_to_string(genealogy_batch.column(0), 0).unwrap(),
        "1999"
    );
}
