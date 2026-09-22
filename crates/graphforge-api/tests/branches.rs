//! Real same-container Branch graph and scoped restoration evidence.
use arrow::array::{FixedSizeBinaryArray, Int64Array};
use graphforge_api::*;
use uuid::Uuid;

fn current(graph: &GraphForge) -> Uuid {
    graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}
fn count(graph: &GraphForge, query: &str) -> i64 {
    let result = graph.execute(query).unwrap();
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
fn create(graph: &mut GraphForge, label: &str) -> CreateResearchBranchRequest {
    let request = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: label.into(),
    };
    graph
        .create_research_branch(&request, &CancellationToken::new())
        .unwrap();
    request
}
fn edit(graph: &mut GraphForge, branch_uuid: Uuid, query: &str) -> ExecuteResearchBranchRequest {
    let request = ExecuteResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(graph),
        branch_uuid,
        version_uuid: Uuid::now_v7(),
        query: query.into(),
        created_at: 2,
    };
    graph
        .execute_research_branch(&request, &CancellationToken::new())
        .unwrap();
    request
}
fn shared_uuid(graph: &GraphForge) -> Uuid {
    let result = graph
        .execute("MATCH (n:Character) RETURN n.node_uuid AS id")
        .unwrap();
    Uuid::from_slice(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap()
}
#[test]
fn two_branches_restore_independently_of_parent_and_keep_receipts_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (a:Story {name:'First'}), (b:Story {name:'Second'}), (c:Character {name:'Shared'}), (a)-[:FEATURES]->(c), (b)-[:FEATURES]->(c)").unwrap();
    let inherited = shared_uuid(&graph);
    let a = create(&mut graph, "A");
    let b = create(&mut graph, "B");
    let a_edit = edit(
        &mut graph,
        a.branch_uuid,
        "MATCH (n:Character) SET n.name = 'A local' CREATE (:Story {name:'A only'})",
    );
    edit(
        &mut graph,
        a.branch_uuid,
        "MATCH (s:Story {name:'First'})-[r:FEATURES]->() DELETE r",
    );
    let b_edit = edit(
        &mut graph,
        b.branch_uuid,
        "CREATE (:Story {name:'B one'}), (:Story {name:'B two'})",
    );
    graph
        .execute("CREATE (:Story {name:'Parent only'})")
        .unwrap();
    {
        let a_view = graph.open_research_branch(a.branch_uuid).unwrap();
        let b_view = graph.open_research_branch(b.branch_uuid).unwrap();
        assert_eq!(shared_uuid(a_view.graph()), inherited);
        assert_eq!(shared_uuid(b_view.graph()), inherited);
        assert_eq!(
            a_view
                .graph()
                .rank("Story", RankOptions::default())
                .unwrap()
                .num_rows(),
            3
        );
        assert_eq!(
            b_view
                .graph()
                .rank("Story", RankOptions::default())
                .unwrap()
                .num_rows(),
            4
        );
        assert_eq!(count(a_view.graph(), "MATCH ()-[r]->() RETURN count(r)"), 1);
        assert_eq!(count(&graph, "MATCH ()-[r]->() RETURN count(r)"), 2);
        assert!(
            a_view
                .graph()
                .execute("CREATE (:Story {name:'illegal'})")
                .is_err()
        );
    }
    let restore = RestoreResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: a.branch_uuid,
        source_version_uuid: a.version_uuid,
        version_uuid: Uuid::now_v7(),
        created_at: 3,
    };
    let receipt = graph
        .restore_research_branch(&restore, &CancellationToken::new())
        .unwrap();
    let stable = current(&graph);
    assert_eq!(
        graph
            .restore_research_branch(&restore, &CancellationToken::new())
            .unwrap(),
        receipt
    );
    graph
        .execute_research_branch(&a_edit, &CancellationToken::new())
        .unwrap();
    assert_eq!(current(&graph), stable);
    let mut conflict = a_edit.clone();
    conflict.query = "CREATE (:Story {name:'conflicting replay'})".into();
    assert!(
        graph
            .execute_research_branch(&conflict, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current(&graph), stable);
    let registry = graph.research_version_retention().unwrap();
    assert_eq!(registry.heads[&b.branch_uuid], b_edit.version_uuid);
    assert!(registry.receipts.contains_key(&a_edit.operation_uuid));
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    let a_view = graph.open_research_branch(a.branch_uuid).unwrap();
    let b_view = graph.open_research_branch(b.branch_uuid).unwrap();
    assert_eq!(a_view.version_uuid(), restore.version_uuid);
    assert_eq!(count(a_view.graph(), "MATCH (n:Story) RETURN count(n)"), 2);
    assert_eq!(count(a_view.graph(), "MATCH ()-[r]->() RETURN count(r)"), 2);
    assert_eq!(
        count(
            a_view.graph(),
            "MATCH (n:Character {name:'Shared'}) RETURN count(n)"
        ),
        1
    );
    assert_eq!(count(b_view.graph(), "MATCH (n:Story) RETURN count(n)"), 4);
    assert_eq!(count(&graph, "MATCH (n:Story) RETURN count(n)"), 3);
    assert_eq!(shared_uuid(a_view.graph()), inherited);
    assert_eq!(
        graph.research_version_retention().unwrap().receipts[&restore.operation_uuid],
        receipt
    );
}

#[test]
fn exact_branch_retries_refresh_parent_changes_from_another_facade() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Story {name:'Original'})").unwrap();
    let branch = create(&mut graph, "retry");
    let edit = edit(
        &mut graph,
        branch.branch_uuid,
        "CREATE (:Story {name:'Local'})",
    );
    let restore = RestoreResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch.branch_uuid,
        source_version_uuid: branch.version_uuid,
        version_uuid: Uuid::now_v7(),
        created_at: 3,
    };
    let receipt = graph
        .restore_research_branch(&restore, &CancellationToken::new())
        .unwrap();
    for retry in 0..3 {
        let other = GraphForge::new(root.to_str()).unwrap();
        other
            .execute("CREATE (:Story {name:'Concurrent parent'})")
            .unwrap();
        let advanced = current(&other);
        drop(other);
        match retry {
            0 => {
                graph
                    .create_research_branch(&branch, &CancellationToken::new())
                    .unwrap();
            }
            1 => {
                graph
                    .execute_research_branch(&edit, &CancellationToken::new())
                    .unwrap();
            }
            _ => assert_eq!(
                graph
                    .restore_research_branch(&restore, &CancellationToken::new())
                    .unwrap(),
                receipt
            ),
        }
        assert_eq!(current(&graph), advanced);
        assert_eq!(count(&graph, "MATCH (n:Story) RETURN count(n)"), retry + 2);
        let view = graph.open_research_branch(branch.branch_uuid).unwrap();
        assert_eq!(view.version_uuid(), restore.version_uuid);
        assert_eq!(count(view.graph(), "MATCH (n:Story) RETURN count(n)"), 1);
    }
}

fn capture(graph: &mut GraphForge) -> Uuid {
    let version_uuid = Uuid::now_v7();
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid,
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 1,
            required_versions: Default::default(),
        })
        .unwrap();
    graph
        .commit_research_version_operation(operation, &CancellationToken::new())
        .unwrap();
    version_uuid
}
fn frozen_character(graph: &GraphForge, version_uuid: Uuid) -> Vec<u8> {
    let result = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version { version_uuid },
                selector: SliceSelector::Query {
                    query: "MATCH (n:Character) RETURN n.node_uuid AS node_uuid".into(),
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut bytes = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut bytes, result.schema.as_ref()).unwrap();
    for batch in &result.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    bytes
}
#[test]
fn slice_branch_preserves_selected_identity_without_parent_graph_membership() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {name:'Shared'}), (:Story {name:'Unselected'})")
        .unwrap();
    let inherited = shared_uuid(&graph);
    let version = capture(&mut graph);
    let source = BranchSource::Slice {
        frozen_ipc: frozen_character(&graph, version),
    };
    let request = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source,
        creator_uuid: Uuid::now_v7(),
        created_at: 2,
        label: "Selected character".into(),
    };
    graph
        .create_research_branch(&request, &CancellationToken::new())
        .unwrap();
    graph
        .execute("CREATE (:Story {name:'Later parent'})")
        .unwrap();
    let view = graph.open_research_branch(request.branch_uuid).unwrap();
    assert_eq!(shared_uuid(view.graph()), inherited);
    assert_eq!(count(view.graph(), "MATCH (n) RETURN count(n)"), 1);
    assert_eq!(count(&graph, "MATCH (n) RETURN count(n)"), 3);
    assert_eq!(view.record().origin_version_uuid, version);
    let record = graph.research_version(request.version_uuid).unwrap();
    assert!(record.content.graph_projection.is_some());
    drop(view);
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    let view = graph.open_research_branch(request.branch_uuid).unwrap();
    assert_eq!(shared_uuid(view.graph()), inherited);
    assert_eq!(count(view.graph(), "MATCH (n) RETURN count(n)"), 1);
}

#[test]
fn assertion_slice_branch_retains_only_selected_evidence_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {name:'Shared'}), (:Story {name:'Unselected'})")
        .unwrap();
    let context = || WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    };
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let source = Uuid::now_v7();
    let artifact = Uuid::now_v7();
    let assertion = Uuid::now_v7();
    let shared = shared_uuid(&graph);
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid: source,
            label: "Source".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            source_uuid: source,
            artifact_uuid: artifact,
            artifact_kind: ArtifactKind::RawScan,
            media_type: "application/octet-stream".into(),
            payload: ArtifactPayloadRequest::LocalBytes(b"selected evidence".to_vec()),
            derivation_inputs: vec![],
            run_uuid: None,
        })
        .unwrap();
    graph
        .create_assertion(CreateAssertionRequest {
            context: context(),
            assertion_uuid: assertion,
            claim: "Shared appears".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: shared,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    graph
        .attach_evidence(AttachEvidenceRequest {
            context: context(),
            evidence_uuid: Uuid::now_v7(),
            assertion_uuid: assertion,
            source_uuid: artifact,
            source_kind: EvidenceSourceKind::Artifact,
            role: EvidenceRole::Supports,
            weight: None,
        })
        .unwrap();

    for capability_id in [CapabilityId::Epistemic, CapabilityId::ValidTime] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let confidence = Uuid::now_v7();
    graph
        .assess_confidence(AssessConfidenceRequest {
            context: context(),
            confidence_uuid: confidence,
            assertion_uuid: assertion,
            policy: ConfidencePolicyRequest::Explicit { value: 0.8 },
        })
        .unwrap();
    let assertion_row = graph.assertion(assertion, None).unwrap();
    let provenance = Uuid::from_slice(
        assertion_row.batches[0]
            .column_by_name("provenance_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let reasoning = Uuid::now_v7();
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(),
            reasoning_uuid: reasoning,
            assertion_uuid: assertion,
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: b"Interpret the selected evidence".to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid: provenance,
        })
        .unwrap();
    graph
        .record_assertion_status(RecordAssertionStatusRequest {
            context: context(),
            status_event_uuid: Uuid::now_v7(),
            assertion_uuid: assertion,
            status: AssertionStatus::Supported,
            confidence_uuid: Some(confidence),
            reasoning_uuid: Some(reasoning),
            provenance_uuid: provenance,
        })
        .unwrap();
    graph
        .record_assertion_validity(RecordAssertionValidityRequest {
            context: context(),
            validity_event_uuid: Uuid::now_v7(),
            assertion_uuid: assertion,
            valid_from_micros: Some(1),
            valid_to_micros: Some(10),
            reasoning_uuid: Some(reasoning),
            provenance_uuid: provenance,
        })
        .unwrap();
    let outside = Uuid::now_v7();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            source_uuid: source,
            artifact_uuid: outside,
            artifact_kind: ArtifactKind::RawScan,
            media_type: "application/octet-stream".into(),
            payload: ArtifactPayloadRequest::LocalBytes(
                b"outside evidence must not be retained".to_vec(),
            ),
            derivation_inputs: vec![],
            run_uuid: None,
        })
        .unwrap();
    let version = capture(&mut graph);
    let result = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: version,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        assertions: std::collections::BTreeSet::from([assertion]),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut frozen_ipc = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut frozen_ipc, result.schema.as_ref()).unwrap();
    for batch in &result.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    let request = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Slice { frozen_ipc },
        creator_uuid: Uuid::now_v7(),
        created_at: 2,
        label: "Selected assertion".into(),
    };
    graph
        .create_research_branch(&request, &CancellationToken::new())
        .unwrap();
    drop(graph);
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    let view = graph.open_research_branch(request.branch_uuid).unwrap();
    assert_eq!(count(view.graph(), "MATCH (n) RETURN count(n)"), 1);
    assert!(view.graph().assertion(assertion, None).is_ok());
    assert!(view.graph().artifact(artifact).is_ok());
    assert!(view.graph().confidence_assessment(confidence, None).is_ok());
    assert!(view.graph().assertion_status(assertion).is_ok());
    assert_eq!(
        view.graph()
            .list_assertion_validity(ListAssertionValidityRequest {
                assertion_uuid: Some(assertion),
                page: PageRequest::default()
            })
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
    assert!(view.graph().artifact(outside).is_err());
    let version = graph.open_research_version(request.version_uuid).unwrap();
    assert!(version.artifact_payload(artifact).is_ok());
    assert!(version.artifact_payload(outside).is_err());
    assert!(graph.artifact(outside).is_ok());
    let suppression = SuppressResearchBranchAssertionRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: request.branch_uuid,
        version_uuid: Uuid::now_v7(),
        assertion_uuid: assertion,
        created_at: 3,
    };
    graph
        .suppress_research_branch_assertion(&suppression, &CancellationToken::new())
        .unwrap();
    let suppressed = graph.open_research_branch(request.branch_uuid).unwrap();
    assert!(suppressed.graph().assertion(assertion, None).is_err());
    assert!(
        suppressed
            .graph()
            .confidence_assessment(confidence, None)
            .is_err()
    );
    assert_eq!(count(suppressed.graph(), "MATCH (n) RETURN count(n)"), 1);
    assert_eq!(shared_uuid(suppressed.graph()), shared);
    assert!(graph.assertion(assertion, None).is_ok());
    assert!(view.graph().assertion(assertion, None).is_ok());
    assert_eq!(
        field(&graph, request.branch_uuid, assertion, "$object")[10],
        "suppressed"
    );
}

fn field(graph: &GraphForge, branch: Uuid, object: Uuid, name: &str) -> Vec<String> {
    let result = graph
        .open_research_branch(branch)
        .unwrap()
        .fields()
        .unwrap();
    let batch = &result.batches[0];
    let columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|c| {
            c.as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
        })
        .collect();
    let row = (0..batch.num_rows())
        .find(|i| columns[1].value(*i) == object.to_string() && columns[2].value(*i) == name)
        .unwrap();
    columns.iter().map(|c| c.value(row).into()).collect()
}
#[test]
fn field_origins_and_contributions_survive_local_edits_and_slice_children() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {name:'Shared'})")
        .unwrap();
    let object = shared_uuid(&graph);
    let parent = create(&mut graph, "parent branch");
    let inherited = field(&graph, parent.branch_uuid, object, "property:name");
    edit(
        &mut graph,
        parent.branch_uuid,
        "MATCH (n:Character) SET n.name='Local', n.note='First'",
    );
    let first = field(&graph, parent.branch_uuid, object, "property:note");
    assert_eq!(first[5], first[7]);
    assert!(first[4].is_empty() && first[6].is_empty());
    assert_eq!(first[10], "local");
    let head = edit(
        &mut graph,
        parent.branch_uuid,
        "MATCH (n:Character) SET n.note='Second'",
    );
    let second = field(&graph, parent.branch_uuid, object, "property:note");
    assert_eq!(
        (&first[3], &first[5], &first[8]),
        (&second[3], &second[5], &second[8])
    );
    assert_ne!(second[5], second[7]);
    let changed = field(&graph, parent.branch_uuid, object, "property:name");
    assert_eq!(
        (&changed[3], &changed[5], &changed[6], &changed[8]),
        (&inherited[3], &inherited[5], &inherited[6], &inherited[8])
    );
    assert_eq!(changed[10], "local");
    let child = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Slice {
            frozen_ipc: frozen_character(&graph, head.version_uuid),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 4,
        label: "Slice child".into(),
    };
    graph
        .create_research_branch(&child, &CancellationToken::new())
        .unwrap();
    let child_field = field(&graph, child.branch_uuid, object, "property:note");
    assert_eq!(
        (&child_field[3], &child_field[5], &child_field[8]),
        (&first[3], &first[5], &first[8])
    );
    assert_eq!(child_field[4], head.version_uuid.to_string());
    assert_eq!(child_field[6], second[7]);
    assert_eq!(child_field[10], "inherited");
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        field(&graph, child.branch_uuid, object, "property:note"),
        child_field
    );
}

#[test]
fn source_only_slice_branch_has_empty_graph_and_selected_source_membership() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Story {name:'Outside'})").unwrap();
    let context = || WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    };
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let source = Uuid::now_v7();
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid: source,
            label: "Only source".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap();
    let version = capture(&mut graph);
    let frozen = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: version,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        sources: std::collections::BTreeSet::from([source]),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut frozen_ipc = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut frozen_ipc, frozen.schema.as_ref()).unwrap();
    writer.write(&frozen.batches[0]).unwrap();
    writer.finish().unwrap();
    let request = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Slice { frozen_ipc },
        creator_uuid: Uuid::now_v7(),
        created_at: 2,
        label: "Source only".into(),
    };
    graph
        .create_research_branch(&request, &CancellationToken::new())
        .unwrap();
    let view = graph.open_research_branch(request.branch_uuid).unwrap();
    assert_eq!(count(view.graph(), "MATCH (n) RETURN count(n)"), 0);
    assert!(view.graph().source(source).is_ok());
    assert_eq!(
        field(&graph, request.branch_uuid, source, "$object")[9],
        "active"
    );
    assert_eq!(count(&graph, "MATCH (n) RETURN count(n)"), 1);
}

#[test]
fn local_composition_is_exact_and_does_not_change_parent_or_sibling() {
    use graphforge_ontology::{
        AuthoredModule, CompositionLimits, EntityTypeDef, InventoryCompileRequest,
        OntologyModuleId, compile_inventory, module_document_digest,
    };
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {name:'Shared'})")
        .unwrap();
    let a = create(&mut graph, "A");
    let b = create(&mut graph, "B");
    let document = OntologyDoc {
        ontology_id: "branch-local".into(),
        version: "1.0.0".into(),
        entity_types: vec![
            EntityTypeDef {
                name: "Character".into(),
                r#abstract: false,
                parent: None,
            },
            EntityTypeDef {
                name: "LocalStory".into(),
                r#abstract: false,
                parent: None,
            },
        ],
        relation_types: vec![],
        properties: vec![],
        constraints: vec![],
        migrations: vec![],
    };
    let authored = AuthoredModule {
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
        modules: &[authored],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Advisory,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    let candidate =
        graphforge_storage::WorkspaceOntologyComposition::from_compiled(&compiled, vec![]);
    let request = ChangeResearchBranchOntologyRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: a.branch_uuid,
        version_uuid: Uuid::now_v7(),
        expected_composition_fingerprint: None,
        candidate: candidate.clone(),
        data_disposition: CompositionDataDisposition::RequireConforming,
        created_at: 3,
    };
    graph
        .change_research_branch_ontology(&request, &CancellationToken::new())
        .unwrap();
    let view = graph.open_research_branch(a.branch_uuid).unwrap();
    assert_eq!(
        view.graph()
            .workspace_ontology_composition()
            .unwrap()
            .unwrap(),
        candidate
    );
    assert!(graph.workspace_ontology_composition().unwrap().is_none());
    assert!(
        graph
            .open_research_branch(b.branch_uuid)
            .unwrap()
            .graph()
            .workspace_ontology_composition()
            .unwrap()
            .is_none()
    );
    drop(view);
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        graph
            .open_research_branch(a.branch_uuid)
            .unwrap()
            .graph()
            .workspace_ontology_composition()
            .unwrap()
            .unwrap(),
        candidate
    );
}

#[test]
fn reference_does_not_expand_but_bring_preserves_selected_uuid_and_origin() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {name:'Shared'}), (:Story {name:'Outside'})")
        .unwrap();
    let shared = shared_uuid(&graph);
    let base = capture(&mut graph);
    let branch = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Slice {
            frozen_ipc: frozen_character(&graph, base),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 2,
        label: "Small".into(),
    };
    graph
        .create_research_branch(&branch, &CancellationToken::new())
        .unwrap();
    graph
        .execute("MATCH (c:Character) CREATE (s:Story {name:'New'})-[:FEATURES {weight:2}]->(c)")
        .unwrap();
    let source = capture(&mut graph);
    let reference = ReferenceResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch.branch_uuid,
        version_uuid: Uuid::now_v7(),
        reference_uuid: Uuid::now_v7(),
        source_version_uuid: source,
        label: "Read later".into(),
        created_at: 3,
    };
    graph
        .reference_research_branch(&reference, &CancellationToken::new())
        .unwrap();
    let view = graph.open_research_branch(branch.branch_uuid).unwrap();
    assert_eq!(count(view.graph(), "MATCH (n) RETURN count(n)"), 1);
    assert_eq!(view.references().unwrap().batches[0].num_rows(), 1);
    drop(view);
    let frozen = graph.freeze_slice(&SliceRequest {
        request_uuid: Uuid::now_v7(), source: SliceSource::Version { version_uuid: source },
        selector: SliceSelector::Query { query: "MATCH (s:Story {name:'New'})-[r:FEATURES]->() RETURN s.node_uuid AS node_uuid, r.edge_uuid AS edge_uuid".into() },
        include: Default::default(), exclude: Default::default(), limits: Default::default(),
    }, &CancellationToken::new()).unwrap();
    let mut frozen_ipc = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut frozen_ipc, frozen.schema.as_ref()).unwrap();
    writer.write(&frozen.batches[0]).unwrap();
    writer.finish().unwrap();
    let bring = BringResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch.branch_uuid,
        version_uuid: Uuid::now_v7(),
        frozen_ipc,
        created_at: 4,
    };
    let receipt = graph
        .bring_research_branch(&bring, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        graph
            .bring_research_branch(&bring, &CancellationToken::new())
            .unwrap(),
        receipt
    );
    let view = graph.open_research_branch(branch.branch_uuid).unwrap();
    assert_eq!(count(view.graph(), "MATCH (n) RETURN count(n)"), 2);
    assert_eq!(count(view.graph(), "MATCH ()-[r]->() RETURN count(r)"), 1);
    assert_eq!(shared_uuid(view.graph()), shared);
    let imported = view
        .graph()
        .execute("MATCH (s:Story) RETURN s.node_uuid AS id")
        .unwrap();
    let imported = Uuid::from_slice(
        imported.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let inherited = field(&graph, branch.branch_uuid, imported, "property:name");
    assert_eq!(inherited[3], source.to_string());
    assert_eq!(inherited[4], source.to_string());
    assert_eq!(inherited[10], "inherited");
    assert_eq!(count(&graph, "MATCH (n) RETURN count(n)"), 3);
    drop(view);
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        field(&graph, branch.branch_uuid, imported, "property:name"),
        inherited
    );
}

#[test]
fn selected_branch_releases_large_parent_after_evolution_and_cleanup() {
    fn bytes(path: &std::path::Path) -> u64 {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    bytes(&entry.path())
                } else {
                    entry.metadata().unwrap().len()
                }
            })
            .sum()
    }
    let mut measurements = Vec::new();
    for count in [32, 2048] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("project");
        let mut graph = GraphForge::new(root.to_str()).unwrap();
        graph
            .execute("CREATE (:Character {name:'Shared'})")
            .unwrap();
        graph.execute(&format!("UNWIND range(1,{count}) AS i CREATE (:Noise {{value:i, padding:'outside parent content'}})")).unwrap();
        let source = capture(&mut graph);
        let source_record = graph.research_version(source).unwrap();
        let request = CreateResearchBranchRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&graph),
            branch_uuid: Uuid::now_v7(),
            version_uuid: Uuid::now_v7(),
            source: BranchSource::Slice {
                frozen_ipc: frozen_character(&graph, source),
            },
            creator_uuid: Uuid::now_v7(),
            created_at: 2,
            label: "Fixed character".into(),
        };
        let started = std::time::Instant::now();
        graph
            .create_research_branch(&request, &CancellationToken::new())
            .unwrap();
        let elapsed = started.elapsed();
        let selected = graph.research_version(request.version_uuid).unwrap();
        let projection = selected.content.graph_projection.clone().unwrap();
        let definition = graph
            .research_branch_selection(request.branch_uuid)
            .unwrap();
        assert_eq!(
            definition.schema.metadata()["graphforge.branch.selection_contract"],
            "exact_membership/1"
        );
        assert_eq!(
            definition.schema.metadata()["graphforge.branch.origin_version"],
            source.to_string()
        );
        assert_eq!(
            definition.schema.metadata()["graphforge.branch.base_version"],
            request.version_uuid.to_string()
        );
        assert_eq!(
            definition
                .batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
        edit(
            &mut graph,
            request.branch_uuid,
            "MATCH (n:Character) SET n.name = 'Branch-local change'",
        );
        let after_edit = graph
            .research_branch_selection(request.branch_uuid)
            .unwrap();
        assert_eq!(after_edit.schema, definition.schema);
        assert_eq!(after_edit.batches, definition.batches);
        graph.execute("MATCH (n:Noise) DELETE n").unwrap();
        let replacement = graph
            .prepare_research_version(PrepareResearchVersionRequest {
                operation_uuid: Uuid::now_v7(),
                version_uuid: Uuid::now_v7(),
                context_uuid: source_record.context_uuid,
                label: None,
                description: None,
                created_at: 3,
                required_versions: Default::default(),
            })
            .unwrap();
        graph
            .commit_research_version_operation(replacement, &CancellationToken::new())
            .unwrap();
        graph
            .commit_research_version_operation(
                ResearchOperation {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: current(&graph),
                    mutation: ResearchMutation::DeleteVersion {
                        version_uuid: source,
                    },
                },
                &CancellationToken::new(),
            )
            .unwrap();
        let before = bytes(&root.join("graph-objects/sha256"));
        drop(graph);
        graphforge_storage::execute_project_cleanup(
            &root,
            graphforge_storage::ProjectRetentionPolicy {
                retained_ancestors: 0,
            },
            graphforge_storage::ProjectRetentionLimits::default(),
        )
        .unwrap();
        let retained = bytes(&root.join("graph-objects/sha256"));
        assert!(retained < before);
        let graph = GraphForge::new(root.to_str()).unwrap();
        let view = graph.open_research_branch(request.branch_uuid).unwrap();
        assert_eq!(count_nodes(view.graph()), 1);
        assert!(graph.open_research_version(source).is_err());
        let after_cleanup = graph
            .research_branch_selection(request.branch_uuid)
            .unwrap();
        assert_eq!(after_cleanup.schema, definition.schema);
        assert_eq!(after_cleanup.batches, definition.batches);
        assert_eq!(view.record().origin_version_uuid, source);
        assert_eq!(
            graph.research_version(request.version_uuid).unwrap(),
            selected
        );
        // This facade fixture uses the authenticated generation tree directly.
        assert_eq!(projection.source_materialization_bytes_copied, 0);
        assert!(projection.source_materialization_bytes_copied < projection.source_payload_bytes);
        measurements.push((
            count,
            projection.source_payload_bytes,
            projection.selected_payload_bytes,
            projection.source_materialization_bytes_copied,
            retained,
            elapsed.as_micros(),
        ));
    }
    assert!(measurements[1].1 > measurements[0].1);
    assert!(measurements[1].2.abs_diff(measurements[0].2) < 1024);
    assert!(measurements[1].4 <= measurements[0].4 + 8192);
    eprintln!(
        "native Branch parent nodes/source graph bytes/selected graph bytes/source copies/retained CAS bytes/creation us: {measurements:?}"
    );
}
fn count_nodes(graph: &GraphForge) -> i64 {
    count(graph, "MATCH (n) RETURN count(n)")
}

#[test]
fn required_roles_survive_branch_and_historical_branch_version_creation() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Story)-[:FEATURES]->(:Character)")
        .unwrap();
    let identities = graph
        .execute("MATCH (s)-[r]->(t) RETURN s.node_uuid, r.edge_uuid, t.node_uuid")
        .unwrap();
    let uuid = |column: usize| {
        Uuid::from_slice(
            identities.batches[0]
                .column(column)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
        )
        .unwrap()
    };
    let (source_node, edge, target_node) = (uuid(0), uuid(1), uuid(2));
    let origin = capture(&mut graph);
    let frozen = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: origin,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        edges: std::collections::BTreeSet::from([edge]),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut frozen_ipc = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut frozen_ipc, frozen.schema.as_ref()).unwrap();
    for batch in &frozen.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    let parent = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Slice { frozen_ipc },
        creator_uuid: Uuid::now_v7(),
        created_at: 2,
        label: "Only edge active".into(),
    };
    graph
        .create_research_branch(&parent, &CancellationToken::new())
        .unwrap();
    let assert_roles = |graph: &GraphForge, branch| {
        assert_eq!(field(graph, branch, edge, "$object")[9], "active");
        for endpoint in [source_node, target_node] {
            assert_eq!(field(graph, branch, endpoint, "$object")[9], "required");
        }
        let definition = graph.research_branch_selection(branch).unwrap();
        assert_eq!(
            definition.schema.metadata()["graphforge.branch.selection_contract"],
            "exact_membership/1"
        );
        let mut roles = std::collections::BTreeMap::new();
        for batch in &definition.batches {
            let ids = batch
                .column_by_name("object_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let role = batch
                .column_by_name("role")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(
                    roles
                        .insert(
                            Uuid::parse_str(ids.value(row)).unwrap(),
                            role.value(row).to_owned()
                        )
                        .is_none()
                );
            }
        }
        assert_eq!(
            roles,
            std::collections::BTreeMap::from([
                (edge, "active".to_owned()),
                (source_node, "required".to_owned()),
                (target_node, "required".to_owned()),
            ])
        );
    };
    assert_roles(&graph, parent.branch_uuid);
    // Keep the historical source distinct from the parent's later current head.
    edit(
        &mut graph,
        parent.branch_uuid,
        "MATCH ()-[r:FEATURES]->() SET r.weight = 2",
    );
    let mut children = Vec::new();
    for source in [
        BranchSource::Branch {
            branch_uuid: parent.branch_uuid,
        },
        BranchSource::Version {
            version_uuid: parent.version_uuid,
        },
    ] {
        let child = CreateResearchBranchRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&graph),
            branch_uuid: Uuid::now_v7(),
            version_uuid: Uuid::now_v7(),
            source,
            creator_uuid: Uuid::now_v7(),
            created_at: 3,
            label: "Child preserves roles".into(),
        };
        graph
            .create_research_branch(&child, &CancellationToken::new())
            .unwrap();
        assert_roles(&graph, child.branch_uuid);
        children.push(child.branch_uuid);
    }
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_roles(&graph, parent.branch_uuid);
    for child in children {
        assert_roles(&graph, child);
    }
}

#[test]
fn branch_cancel_stale_current_and_conflicting_retry_preserve_authority() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Story {name:'Original'})").unwrap();
    let create_request = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "Atomic".into(),
    };
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let before = (current(&graph), graph.research_version_retention().unwrap());
    assert_eq!(
        graph
            .create_research_branch(&create_request, &cancelled)
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
    assert_eq!(
        (current(&graph), graph.research_version_retention().unwrap()),
        before
    );
    assert_eq!(count_nodes(&graph), 1);
    assert!(
        graph
            .open_research_branch(create_request.branch_uuid)
            .is_err()
    );
    graph
        .create_research_branch(&create_request, &CancellationToken::new())
        .unwrap();

    let edit_request = ExecuteResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: create_request.branch_uuid,
        version_uuid: Uuid::now_v7(),
        query: "CREATE (:Story {name:'Branch local'})".into(),
        created_at: 2,
    };
    let before = (current(&graph), graph.research_version_retention().unwrap());
    assert_eq!(
        graph
            .execute_research_branch(&edit_request, &cancelled)
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
    assert_eq!(
        (current(&graph), graph.research_version_retention().unwrap()),
        before
    );
    assert_eq!(
        count_nodes(
            graph
                .open_research_branch(create_request.branch_uuid)
                .unwrap()
                .graph()
        ),
        1
    );
    graph
        .execute_research_branch(&edit_request, &CancellationToken::new())
        .unwrap();

    let mut stale_create = create_request.clone();
    stale_create.operation_uuid = Uuid::now_v7();
    stale_create.branch_uuid = Uuid::now_v7();
    stale_create.version_uuid = Uuid::now_v7();
    stale_create.source = BranchSource::Current {
        origin_version_uuid: Uuid::now_v7(),
        context_uuid: Uuid::now_v7(),
    };
    stale_create.expected_generation_uuid = current(&graph);
    let mut stale_edit = edit_request.clone();
    stale_edit.operation_uuid = Uuid::now_v7();
    stale_edit.version_uuid = Uuid::now_v7();
    stale_edit.expected_generation_uuid = current(&graph);
    graph
        .execute("CREATE (:Story {name:'Parent later'})")
        .unwrap();
    let before = (current(&graph), graph.research_version_retention().unwrap());
    assert!(
        graph
            .create_research_branch(&stale_create, &CancellationToken::new())
            .is_err()
    );
    assert!(
        graph
            .execute_research_branch(&stale_edit, &CancellationToken::new())
            .is_err()
    );
    let mut conflicting_create = create_request.clone();
    conflicting_create.label = "Different intent".into();
    assert_eq!(
        graph
            .create_research_branch(&conflicting_create, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    let mut conflicting_edit = edit_request.clone();
    conflicting_edit.query = "CREATE (:Story {name:'Conflicting intent'})".into();
    assert_eq!(
        graph
            .execute_research_branch(&conflicting_edit, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert_eq!(
        (current(&graph), graph.research_version_retention().unwrap()),
        before
    );
    assert_eq!(count_nodes(&graph), 2);
    assert_eq!(
        count_nodes(
            graph
                .open_research_branch(create_request.branch_uuid)
                .unwrap()
                .graph()
        ),
        2
    );
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        (current(&graph), graph.research_version_retention().unwrap()),
        before
    );
    assert_eq!(count_nodes(&graph), 2);
    assert_eq!(
        count_nodes(
            graph
                .open_research_branch(create_request.branch_uuid)
                .unwrap()
                .graph()
        ),
        2
    );
}
