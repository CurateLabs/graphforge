use super::*;
use graphforge_ontology::{
    ActivationMode, AuthoredModule, CompositionLimits, EntityTypeDef, InventoryCompileRequest,
    OntologyModuleId, PropertyDef, PropertyValueType, compile_inventory, module_document_digest,
};

fn module(id: &str, entity: &str, properties: &[&str]) -> AuthoredModule {
    let document = OntologyDoc {
        ontology_id: id.into(),
        version: "1.0.0".into(),
        entity_types: vec![EntityTypeDef {
            name: entity.into(),
            r#abstract: false,
            parent: None,
        }],
        relation_types: vec![],
        properties: properties
            .iter()
            .map(|name| PropertyDef {
                owner: entity.into(),
                name: (*name).into(),
                value_type: if *name == "secret" {
                    PropertyValueType::Utf8
                } else {
                    PropertyValueType::Int64
                },
                nullable: true,
                multivalued: *name == "scores",
                default_json: None,
            })
            .collect(),
        constraints: vec![],
        migrations: vec![],
    };
    AuthoredModule {
        id: OntologyModuleId {
            ontology_id: id.into(),
            authored_version: document.version.clone(),
            canonical_digest: module_document_digest(&document).unwrap(),
        },
        dependencies: vec![],
        doc: document,
        allow_projected_identity: false,
    }
}
fn publish(
    graph: &mut GraphForge,
    modules: &[AuthoredModule],
) -> graphforge_storage::WorkspaceOntologyComposition {
    let compiled = compile_inventory(InventoryCompileRequest {
        modules,
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Strict,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    let candidate =
        graphforge_storage::WorkspaceOntologyComposition::from_compiled(&compiled, vec![]);
    let change = CompositionChangeRequest {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        expected_project_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
        expected_composition_fingerprint: graph
            .workspace_ontology_composition()
            .unwrap()
            .map(|composition| composition.composition_fingerprint),
        candidate: candidate.clone(),
        data_disposition: CompositionDataDisposition::RequireConforming,
    };
    let checked = graph
        .preview_ontology_composition_change(&change, None)
        .unwrap();
    assert!(checked.diagnostics.is_empty(), "{:?}", checked.diagnostics);
    graph
        .publish_ontology_composition_change(&change, &checked, None)
        .unwrap();
    candidate
}
#[test]
fn typed_updates_require_reviewed_ontology_and_keep_invalid_retention_unresolved() {
    let mut graph = GraphForge::new(None).unwrap();
    let cancel = CancellationToken::new();
    let initial = module("base-items", "Item", &["x", "y"]);
    publish(&mut graph, std::slice::from_ref(&initial));
    let seed = recovery::prepare(&mut graph);
    let candidate = publish(
        &mut graph,
        &[
            initial,
            module(
                "upstream-characters",
                "Character",
                &["scores", "score", "secret"],
            ),
        ],
    );
    graph
        .execute("CREATE (:Character {scores:[2],score:2,secret:'unselected'})")
        .unwrap();
    let local = graph
        .open_research_branch(seed.preview.branch_uuid)
        .unwrap();
    let old = crate::branches::fields::read(local.graph(), &cancel).unwrap();
    let incoming = crate::branches::fields::read(&graph, &cancel).unwrap();
    let fields = incoming
        .iter()
        .filter(|(key, value)| {
            (key.0.starts_with("ontology") && old.get(*key) != Some(*value))
                || key.0 == "node"
                    && !old.contains_key(*key)
                    && !matches!(key.2.as_str(), "property:score" | "property:secret")
        })
        .map(|(key, _)| ResearchFieldIdentity {
            object_kind: key.0.clone(),
            object_uuid: key.1,
            field: key.2.clone(),
        })
        .collect();
    let preview_request = PreviewResearchUpstreamRequest {
        branch_uuid: seed.preview.branch_uuid,
        scope: ResearchUpstreamScope::Fields { fields },
    };
    let review = preview::load(&graph, &preview_request, &cancel).unwrap();
    let scores = review
        .rows
        .iter()
        .find(|row| row.key.2 == "property:scores")
        .unwrap();
    assert!(
        review.requirements[&scores.key]
            .fields
            .iter()
            .any(|key| key.0.starts_with("ontology"))
    );
    let mut update = UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
        version_uuid: Uuid::now_v7(),
        preview: preview_request,
        preview_sha256: review.digest,
        selection: ResearchUpstreamSelection::Selected {
            decisions: vec![ResearchUpstreamDecision {
                unit: ResearchFieldIdentity {
                    object_kind: scores.key.0.clone(),
                    object_uuid: scores.key.1,
                    field: scores.key.2.clone(),
                },
                resolution: ResearchUpstreamResolution::AdoptUpstream,
            }],
        },
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 4,
        explanation: "Review typed scores and required ontology".into(),
    };
    let current = update.expected_generation_uuid;
    assert!(
        graph
            .update_research_branch(&update, &cancel)
            .unwrap_err()
            .to_string()
            .contains("dependency")
    );
    assert_eq!(
        graph.generation_for_read().unwrap().generation_uuid(),
        current
    );
    update.selection = ResearchUpstreamSelection::AllCompatible;
    graph.update_research_branch(&update, &cancel).unwrap();
    let branch = graph
        .open_research_branch(seed.preview.branch_uuid)
        .unwrap();
    assert_eq!(
        branch
            .graph()
            .workspace_ontology_composition()
            .unwrap()
            .unwrap()
            .composition_fingerprint,
        candidate.composition_fingerprint
    );
    let rows = branch
        .graph()
        .execute("MATCH (n:Item),(c:Character) RETURN n.x AS x,n.y AS y,c.secret AS secret,c.scores AS scores")
        .unwrap();
    let batch = &rows.batches[0];
    for name in ["x", "y"] {
        assert_eq!(
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            0
        );
    }
    let secret = batch.column_by_name("secret").unwrap();
    assert!(secret.data_type() == &arrow::datatypes::DataType::Null || secret.is_null(0));
    assert_eq!(
        graphforge_storage::decode_property_value(batch.column(3), batch.schema().field(3), 0)
            .unwrap(),
        IrLiteral::List(vec![IrLiteral::Int(2)])
    );
    drop(branch);
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
                branch_uuid: seed.preview.branch_uuid,
                version_uuid: Uuid::now_v7(),
                created_at: 5,
                query: "MATCH (n:Character) SET n.scores=[3],n.score=3".into(),
            },
            &cancel,
        )
        .unwrap();
    graph
        .execute("MATCH (n:Character) SET n.scores=[4],n.score=4")
        .unwrap();
    let mut request = seed.preview.clone();
    request.scope = ResearchUpstreamScope::Branch;
    for field in ["property:scores", "property:score"] {
        let review = preview::load(&graph, &request, &cancel).unwrap();
        let row = review.rows.iter().find(|row| row.key.2 == field).unwrap();
        assert_eq!(row.change, "conflict");
        update.operation_uuid = Uuid::now_v7();
        update.version_uuid = Uuid::now_v7();
        update.expected_generation_uuid = graph.generation_for_read().unwrap().generation_uuid();
        update.preview = request.clone();
        update.preview_sha256 = review.digest;
        update.selection = ResearchUpstreamSelection::Selected {
            decisions: vec![ResearchUpstreamDecision {
                unit: ResearchFieldIdentity {
                    object_kind: row.key.0.clone(),
                    object_uuid: row.key.1,
                    field: field.into(),
                },
                resolution: ResearchUpstreamResolution::RetainBoth,
            }],
        };
        if field == "property:scores" {
            graph.update_research_branch(&update, &cancel).unwrap();
            let branch = graph
                .open_research_branch(seed.preview.branch_uuid)
                .unwrap();
            let result = branch
                .graph()
                .execute("MATCH (n:Character) RETURN n.scores AS scores")
                .unwrap();
            let batch = &result.batches[0];
            assert_eq!(
                graphforge_storage::decode_property_value(
                    batch.column(0),
                    batch.schema().field(0),
                    0
                )
                .unwrap(),
                IrLiteral::List(vec![IrLiteral::Int(3), IrLiteral::Int(4)])
            );
        } else {
            let before = graph.research_version_retention().unwrap();
            assert!(
                graph
                    .update_research_branch(&update, &cancel)
                    .unwrap_err()
                    .to_string()
                    .contains("multivalued property")
            );
            assert_eq!(graph.research_version_retention().unwrap(), before);
            assert_eq!(
                graph.generation_for_read().unwrap().generation_uuid(),
                update.expected_generation_uuid
            );
        }
    }
}

#[test]
fn identity_equivalent_ontology_upgrade_preserves_typed_values_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().to_str()).unwrap();
    let original = module("items", "Item", &["x", "y"]);
    publish(&mut graph, std::slice::from_ref(&original));
    graph.execute("CREATE (:Item {x:7,y:9})").unwrap();
    let cancel = CancellationToken::new();
    let node_fields = |graph: &GraphForge| {
        crate::branches::fields::read(graph, &cancel)
            .unwrap()
            .into_iter()
            .filter(|(key, _)| key.0 == "node")
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let before = node_fields(&graph);
    let mut next = original;
    next.doc.version = "2.0.0".into();
    next.id.authored_version = next.doc.version.clone();
    next.id.canonical_digest = module_document_digest(&next.doc).unwrap();
    assert!(next.doc.migrations.is_empty());
    publish(&mut graph, &[next]);
    assert_eq!(node_fields(&graph), before);
    drop(graph);
    let reopened = GraphForge::new(root.path().to_str()).unwrap();
    assert_eq!(node_fields(&reopened), before);
    let result = reopened
        .execute("MATCH (n:Item) RETURN n.x AS x,n.y AS y")
        .unwrap();
    for (name, expected) in [("x", 7), ("y", 9)] {
        assert_eq!(
            result.batches[0]
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
}
