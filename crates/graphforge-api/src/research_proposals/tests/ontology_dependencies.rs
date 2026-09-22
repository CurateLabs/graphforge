//! Required ontology cannot be deferred while its relationship is accepted.
use super::*;
use arrow::array::Array;
use graphforge_ontology::{
    ActivationMode, AuthoredModule, CompositionLimits, EntityTypeDef, InventoryCompileRequest,
    OntologyModuleId, RelationTypeDef, compile_inventory, module_document_digest,
};
use graphforge_storage::research_versions::ResearchProposalDecision::{Accept, Defer};

#[test]
fn relationship_acceptance_requires_reviewed_ontology_and_publishes_both_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character)").unwrap();
    let branch_uuid = branch(&mut graph);
    let before_composition = graph.persisted_workspace_ontology_composition().unwrap();
    let document = OntologyDoc {
        ontology_id: "proposal-relationships".into(),
        version: "1.0.0".into(),
        entity_types: vec![EntityTypeDef {
            name: "Character".into(),
            r#abstract: false,
            parent: None,
        }],
        relation_types: vec![RelationTypeDef {
            name: "RELATED".into(),
            src: "Character".into(),
            dst: "Character".into(),
            inverse: None,
            semantic: Default::default(),
        }],
        properties: vec![],
        constraints: vec![],
        migrations: vec![],
    };
    let module = AuthoredModule {
        id: OntologyModuleId {
            ontology_id: document.ontology_id.clone(),
            authored_version: document.version.clone(),
            canonical_digest: module_document_digest(&document).unwrap(),
        },
        dependencies: vec![],
        doc: document,
        allow_projected_identity: false,
    };
    let compiled = compile_inventory(InventoryCompileRequest {
        modules: &[module],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Strict,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    let candidate =
        graphforge_storage::WorkspaceOntologyComposition::from_compiled(&compiled, vec![]);
    graph
        .change_research_branch_ontology(
            &ChangeResearchBranchOntologyRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid,
                version_uuid: Uuid::now_v7(),
                expected_composition_fingerprint: None,
                candidate: candidate.clone(),
                data_disposition: CompositionDataDisposition::RequireConforming,
                created_at: 2,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let version = edit(
        &mut graph,
        branch_uuid,
        "MATCH (n) CREATE (n)-[:RELATED]->(n)",
    );
    let view = graph.open_research_branch(branch_uuid).unwrap();
    let result = view
        .graph()
        .execute("MATCH ()-[r]->() RETURN r.edge_uuid AS id")
        .unwrap();
    let edge = Uuid::from_slice(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let token = CancellationToken::new();
    let proposed = crate::branches::fields::read(view.graph(), &token).unwrap();
    let parent = crate::branches::fields::read(&graph, &token).unwrap();
    let mut fields: Vec<_> = proposed
        .iter()
        .filter(|(key, value)| key.0.starts_with("ontology") && parent.get(*key) != Some(*value))
        .map(|(key, _)| ResearchFieldIdentity {
            object_kind: key.0.clone(),
            object_uuid: key.1,
            field: key.2.clone(),
        })
        .collect();
    assert!(
        fields
            .iter()
            .any(|field| field.object_kind == "ontology_module")
    );
    fields.extend(
        [
            "$object",
            "$relationship_type",
            "$source_uuid",
            "$target_uuid",
        ]
        .into_iter()
        .map(|field| ResearchFieldIdentity {
            object_kind: "edge".into(),
            object_uuid: edge,
            field: field.into(),
        }),
    );
    drop(view);
    let frozen = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: version,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        edges: [edge].into(),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &token,
        )
        .unwrap();
    let mut frozen_ipc = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut frozen_ipc, &frozen.schema).unwrap();
    for batch in &frozen.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    let submission = SubmitResearchProposalRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        proposal_uuid: Uuid::now_v7(),
        source_branch_uuid: branch_uuid,
        source_version_uuid: version,
        frozen_ipc,
        fields,
        actor_uuid: Uuid::now_v7(),
        created_at: 3,
        motivation: "Relationship requires explicit ontology review".into(),
        policy: String::new(),
    };
    graph.submit_research_proposal(&submission, &token).unwrap();
    let preview = super::super::preview::load(&graph, submission.proposal_uuid, &token).unwrap();
    let ontology_items: std::collections::BTreeSet<_> = preview
        .proposal
        .items
        .iter()
        .filter(|item| item.unit.object_kind.starts_with("ontology"))
        .map(|item| item.item_uuid)
        .collect();
    assert!(!ontology_items.is_empty());
    for item in preview
        .proposal
        .items
        .iter()
        .filter(|item| item.unit.object_kind == "edge")
    {
        let row = preview
            .rows
            .iter()
            .find(|row| row.item_uuid == item.item_uuid)
            .unwrap();
        assert!(ontology_items.is_subset(&row.required_items));
        assert!(row.unavailable.is_empty());
    }
    let mut deferred = decision(&graph, submission.proposal_uuid, |_| Accept);
    for id in &ontology_items {
        deferred.decisions.insert(*id, Defer);
    }
    let generation = current(&graph);
    let registry = graph.research_version_retention().unwrap();
    assert!(graph.review_research_proposal(&deferred, &token).is_err());
    assert_eq!(current(&graph), generation);
    assert_eq!(graph.research_version_retention().unwrap(), registry);
    assert_eq!(
        graph.persisted_workspace_ontology_composition().unwrap(),
        before_composition
    );
    let edges = graph.execute("MATCH ()-[r]->() RETURN count(r)").unwrap();
    assert_eq!(
        edges.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    let accepted = decision(&graph, submission.proposal_uuid, |_| Accept);
    let receipt = graph.review_research_proposal(&accepted, &token).unwrap();
    assert_eq!(current(&graph), receipt.generation_uuid);
    assert_eq!(
        graph.workspace_ontology_composition().unwrap(),
        Some(candidate.clone())
    );
    let result = graph
        .execute("MATCH ()-[r]->() RETURN r.edge_uuid")
        .unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        Uuid::from_slice(
            result.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0)
        )
        .unwrap(),
        edge
    );
    drop(graph);
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        graph.workspace_ontology_composition().unwrap(),
        Some(candidate)
    );
    assert_eq!(
        graph.review_research_proposal(&accepted, &token).unwrap(),
        receipt
    );
    assert_eq!(current(&graph), receipt.generation_uuid);
}

#[test]
fn semantic_secondary_label_and_typed_list_survive_selected_acceptance_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Existing)").unwrap();
    let branch_uuid = branch(&mut graph);
    let document = OntologyDoc {
        ontology_id: "proposal-labels".into(),
        version: "1.0.0".into(),
        entity_types: ["Existing", "Character", "Tagged", "Reviewed"]
            .into_iter()
            .map(|name| EntityTypeDef {
                name: name.into(),
                r#abstract: false,
                parent: None,
            })
            .collect(),
        relation_types: vec![],
        properties: vec![
            graphforge_ontology::PropertyDef {
                owner: "Character".into(),
                name: "scores".into(),
                value_type: graphforge_ontology::PropertyValueType::Int64,
                nullable: true,
                multivalued: true,
                default_json: None,
            },
            graphforge_ontology::PropertyDef {
                owner: "Tagged".into(),
                name: "tag_score".into(),
                value_type: graphforge_ontology::PropertyValueType::Int64,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
        ],
        constraints: vec![],
        migrations: vec![],
    };
    let module = AuthoredModule {
        id: OntologyModuleId {
            ontology_id: document.ontology_id.clone(),
            authored_version: document.version.clone(),
            canonical_digest: module_document_digest(&document).unwrap(),
        },
        dependencies: vec![],
        doc: document,
        allow_projected_identity: false,
    };
    let compiled = compile_inventory(InventoryCompileRequest {
        modules: &[module],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Strict,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    let candidate =
        graphforge_storage::WorkspaceOntologyComposition::from_compiled(&compiled, vec![]);
    graph
        .change_research_branch_ontology(
            &ChangeResearchBranchOntologyRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid,
                version_uuid: Uuid::now_v7(),
                expected_composition_fingerprint: None,
                candidate: candidate.clone(),
                data_disposition: CompositionDataDisposition::RequireConforming,
                created_at: 2,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    edit(
        &mut graph,
        branch_uuid,
        "CREATE (:Character:Tagged {scores:[3,1,4]})",
    );
    let version = edit(
        &mut graph,
        branch_uuid,
        "MATCH (n:Tagged) SET n.tag_score=10",
    );
    let view = graph.open_research_branch(branch_uuid).unwrap();
    let token = CancellationToken::new();
    let fields = crate::branches::fields::read(view.graph(), &token).unwrap();
    let tag = view
        .graph()
        .execute("MATCH (n:Tagged) RETURN n.tag_score AS value")
        .unwrap();
    assert_eq!(
        tag.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    let parent = crate::branches::fields::read(&graph, &token).unwrap();
    let stored = view
        .graph()
        .execute("MATCH (n:Character) RETURN n.scores AS scores")
        .unwrap();
    assert_eq!(
        stored
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let batch = &stored.batches[0];
    assert_eq!(
        graphforge_storage::decode_property_value(batch.column(0), batch.schema().field(0), 0)
            .unwrap(),
        IrLiteral::List(vec![
            IrLiteral::Int(3),
            IrLiteral::Int(1),
            IrLiteral::Int(4)
        ])
    );
    let id = fields
        .keys()
        .find(|key| key.0 == "node" && key.2 == "property:scores")
        .unwrap_or_else(|| {
            panic!(
                "missing scores field in {:?}",
                fields.keys().collect::<Vec<_>>()
            )
        })
        .1;
    let expected: std::collections::BTreeMap<_, _> = fields
        .iter()
        .filter(|(key, _)| key.0 == "node" && key.1 == id)
        .map(|(key, value)| (key.clone(), *value))
        .collect();
    let result = view
        .graph()
        .execute("MATCH (n:Character) RETURN labels(n) AS labels, n.scores AS scores")
        .unwrap();
    let labels = result.batches[0]
        .column_by_name("labels")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap()
        .value(0);
    let labels = labels
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(labels.len(), 2);
    assert!((0..labels.len()).all(|i| labels.value(i).starts_with("proposal-labels:entity:")));
    let selected_fields = fields
        .iter()
        .filter(|(key, value)| {
            (key.0 == "node" && key.1 == id)
                || (key.0.starts_with("ontology") && parent.get(*key) != Some(*value))
        })
        .map(|(key, _)| ResearchFieldIdentity {
            object_kind: key.0.clone(),
            object_uuid: key.1,
            field: key.2.clone(),
        })
        .collect();
    drop(view);
    let frozen = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: version,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        nodes: [id].into(),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &token,
        )
        .unwrap();
    let mut frozen_ipc = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut frozen_ipc, &frozen.schema).unwrap();
    for batch in &frozen.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    let submission = SubmitResearchProposalRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        proposal_uuid: Uuid::now_v7(),
        source_branch_uuid: branch_uuid,
        source_version_uuid: version,
        frozen_ipc,
        fields: selected_fields,
        actor_uuid: Uuid::now_v7(),
        created_at: 3,
        motivation: "Preserve both qualified labels and ordered typed values".into(),
        policy: String::new(),
    };
    graph.submit_research_proposal(&submission, &token).unwrap();
    let review = decision(&graph, submission.proposal_uuid, |_| Accept);
    graph.review_research_proposal(&review, &token).unwrap();
    for reopen in [false, true] {
        if reopen {
            drop(graph);
            graph = GraphForge::new(root.to_str()).unwrap();
        }
        assert_eq!(
            graph.workspace_ontology_composition().unwrap(),
            Some(candidate.clone())
        );
        let actual = crate::branches::fields::read(&graph, &token).unwrap();
        for (key, value) in &expected {
            assert_eq!(actual.get(key), Some(value), "exact semantic field {key:?}");
        }
        let result = graph
            .execute("MATCH (n:Character) RETURN n.scores AS scores")
            .unwrap();
        let batch = &result.batches[0];
        assert_eq!(
            graphforge_storage::decode_property_value(batch.column(0), batch.schema().field(0), 0)
                .unwrap(),
            IrLiteral::List(vec![
                IrLiteral::Int(3),
                IrLiteral::Int(1),
                IrLiteral::Int(4)
            ])
        );
    }
    existing_semantic_label_updates(&mut graph, branch_uuid, id);
    drop(graph);
    let reopened = GraphForge::new(root.to_str()).unwrap();
    assert_semantic_node(&reopened, id, true, &[8, 9]);
    let tag = reopened
        .execute("MATCH (n:Tagged) RETURN n.tag_score")
        .unwrap();
    assert_eq!(
        tag.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        11
    );
}

fn existing_semantic_label_updates(graph: &mut GraphForge, branch_uuid: Uuid, id: Uuid) {
    for (first, query, tagged, scores) in [
        (
            "MATCH (n:Tagged) REMOVE n.tag_score",
            "MATCH (n:Character) SET n.scores=[6,2] REMOVE n:`proposal-labels:Tagged`",
            false,
            &[6, 2][..],
        ),
        (
            "MATCH (n:Character) SET n:`proposal-labels:Tagged`:`proposal-labels:Reviewed`, n.scores=[8,9]",
            "MATCH (n:Tagged) SET n.tag_score=11",
            true,
            &[8, 9][..],
        ),
    ] {
        edit(graph, branch_uuid, first);
        let version = edit(graph, branch_uuid, query);
        let before = crate::branches::fields::read(graph, &CancellationToken::new()).unwrap();
        let proposal = submit(
            graph,
            branch_uuid,
            version,
            id,
            &["$labels", "property:scores", "property:tag_score"],
        );
        let review = decision(graph, proposal.proposal_uuid, |_| Accept);
        let receipt = graph
            .review_research_proposal(&review, &CancellationToken::new())
            .unwrap();
        assert_semantic_node(graph, id, tagged, scores);
        if tagged {
            let result = graph
                .execute("MATCH (n:Tagged) RETURN n.tag_score")
                .unwrap();
            assert_eq!(
                result.batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                11
            );
        }
        let fields = crate::branches::fields::read(graph, &CancellationToken::new()).unwrap();
        assert_eq!(
            fields.contains_key(&("node".into(), id, "property:tag_score".into())),
            tagged
        );
        let after = crate::branches::fields::read(graph, &CancellationToken::new()).unwrap();
        for (key, value) in before
            .iter()
            .filter(|(key, _)| key.0 == "node" && key.1 != id)
        {
            assert_eq!(
                after.get(key),
                Some(value),
                "unrelated object field {key:?}"
            );
        }
        let generation = current(graph);
        assert_eq!(
            graph
                .review_research_proposal(&review, &CancellationToken::new())
                .unwrap(),
            receipt
        );
        assert_eq!(current(graph), generation);
    }
}

fn assert_semantic_node(graph: &GraphForge, id: Uuid, tagged: bool, scores: &[i64]) {
    let result = graph
        .execute(
            "MATCH (n:Character) RETURN n.node_uuid AS id, labels(n) AS labels, n.scores AS scores",
        )
        .unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let batch = &result.batches[0];
    assert_eq!(
        Uuid::from_slice(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0)
        )
        .unwrap(),
        id
    );
    let labels = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap()
        .value(0);
    let labels = labels
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let actual: std::collections::BTreeSet<_> =
        (0..labels.len()).map(|row| labels.value(row)).collect();
    let mut expected = std::collections::BTreeSet::from(["proposal-labels:entity:Character"]);
    if tagged {
        expected.insert("proposal-labels:entity:Tagged");
        expected.insert("proposal-labels:entity:Reviewed");
    }
    assert_eq!(actual, expected);
    assert_eq!(
        graphforge_storage::decode_property_value(batch.column(2), batch.schema().field(2), 0)
            .unwrap(),
        IrLiteral::List(scores.iter().map(|value| IrLiteral::Int(*value)).collect())
    );
}

fn route_fixture_composition(
    mode: ActivationMode,
    distinct_types: bool,
) -> graphforge_storage::WorkspaceOntologyComposition {
    let document = OntologyDoc {
        ontology_id: "route-regression".into(),
        version: "1".into(),
        entity_types: ["A", "B"]
            .into_iter()
            .map(|name| EntityTypeDef {
                name: name.into(),
                r#abstract: false,
                parent: None,
            })
            .collect(),
        relation_types: vec![],
        properties: [
            ("A", graphforge_ontology::PropertyValueType::Int64),
            (
                "B",
                if distinct_types {
                    graphforge_ontology::PropertyValueType::Utf8
                } else {
                    graphforge_ontology::PropertyValueType::Int64
                },
            ),
        ]
        .into_iter()
        .map(|(owner, value_type)| graphforge_ontology::PropertyDef {
            owner: owner.into(),
            name: "score".into(),
            value_type,
            nullable: true,
            multivalued: false,
            default_json: None,
        })
        .collect(),
        constraints: vec![],
        migrations: vec![],
    };
    let module = AuthoredModule {
        id: OntologyModuleId {
            ontology_id: document.ontology_id.clone(),
            authored_version: document.version.clone(),
            canonical_digest: module_document_digest(&document).unwrap(),
        },
        dependencies: vec![],
        doc: document,
        allow_projected_identity: false,
    };
    let compiled = compile_inventory(InventoryCompileRequest {
        modules: &[module],
        bridges: &[],
        activation: &[],
        profile_default: mode,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    graphforge_storage::WorkspaceOntologyComposition::from_compiled(&compiled, vec![])
}
fn publish_route_fixture(
    graph: &mut GraphForge,
    candidate: graphforge_storage::WorkspaceOntologyComposition,
) {
    let before = graph.persisted_workspace_ontology_composition().unwrap();
    let change = CompositionChangeRequest {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        expected_project_generation_uuid: current(graph),
        expected_composition_fingerprint: before.map(|c| c.composition_fingerprint),
        candidate,
        data_disposition: CompositionDataDisposition::RequireConforming,
    };
    let preview = graph
        .preview_ontology_composition_change(&change, None)
        .unwrap();
    graph
        .publish_ontology_composition_change(&change, &preview, None)
        .unwrap();
}
#[test]
fn semantic_routes_keep_unrelated_same_name_property_types_independent() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    publish_route_fixture(
        &mut graph,
        route_fixture_composition(ActivationMode::Strict, true),
    );
    graph.execute("CREATE (:A {score:7})").unwrap();
    graph
        .execute("CREATE (:B {score:'private string'})")
        .unwrap();
    let identity = graph.execute("MATCH (n:A) RETURN n.node_uuid").unwrap();
    assert_eq!(
        identity
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let value = graph.execute("MATCH (n:A) RETURN n.score").unwrap();
    assert_eq!(
        value.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    let value = graph.execute("MATCH (n:B) RETURN n.score").unwrap();
    assert_eq!(
        value.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        "private string"
    );
    let mixed = graph
        .execute("MATCH (n) RETURN properties(n)['score'] AS value")
        .unwrap();
    let mut values = Vec::new();
    for batch in &mixed.batches {
        for row in 0..batch.num_rows() {
            values.push(
                graphforge_storage::decode_property_value(
                    batch.column(0),
                    batch.schema().field(0),
                    row,
                )
                .unwrap(),
            );
        }
    }
    assert_eq!(values.len(), 2);
    assert!(values.contains(&IrLiteral::Int(7)));
    assert!(values.contains(&IrLiteral::Str("private string".into())));
}
#[test]
fn semantic_routes_preserve_retained_values_when_profile_becomes_exploratory() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    publish_route_fixture(
        &mut graph,
        route_fixture_composition(ActivationMode::Strict, false),
    );
    graph.execute("CREATE (:A {score:7})").unwrap();
    let before = graph.execute("MATCH (n:A) RETURN n.score").unwrap();
    assert_eq!(
        before.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    publish_route_fixture(
        &mut graph,
        route_fixture_composition(ActivationMode::Exploratory, false),
    );
    graph.execute("CREATE (:A {score:8})").unwrap();
    for reopen in [false, true] {
        if reopen {
            drop(graph);
            graph = GraphForge::new(root.to_str()).unwrap();
        }
        let value = graph
            .execute("MATCH (n:A) RETURN n.score ORDER BY n.score")
            .unwrap();
        let scores = value
            .batches
            .iter()
            .flat_map(|batch| {
                let scores = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..scores.len())
                    .map(|row| {
                        assert!(!scores.is_null(row));
                        scores.value(row)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(scores, vec![7, 8]);
    }
}
