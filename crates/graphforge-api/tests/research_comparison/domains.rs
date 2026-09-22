use super::*;
fn write() -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    }
}
fn enable(g: &mut GraphForge) {
    for capability_id in [
        CapabilityId::Provenance,
        CapabilityId::Knowledge,
        CapabilityId::Epistemic,
    ] {
        g.enable_capability(EnableCapabilityRequest {
            context: write(),
            capability_id,
            capability_version: 1,
        })
        .unwrap();
    }
}
#[test]
fn unrelated_external_evidence_does_not_become_a_parent_update() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item {x:0})").unwrap();
    enable(&mut g);
    let b = branch(&mut g);
    let source = Uuid::now_v7();
    g.register_source(RegisterSourceRequest {
        context: write(),
        source_uuid: source,
        label: "Unselected".into(),
        source_kind: SourceKind::Manuscript,
        identity_uri: None,
    })
    .unwrap();
    let artifact = Uuid::now_v7();
    g.register_artifact(RegisterArtifactRequest {
        context: write(),
        source_uuid: source,
        artifact_uuid: artifact,
        artifact_kind: ArtifactKind::RawScan,
        media_type: "application/octet-stream".into(),
        payload: ArtifactPayloadRequest::ExternalReference {
            uri: "https://example.invalid/evidence".into(),
            fingerprint: None,
        },
        derivation_inputs: vec![],
        run_uuid: None,
    })
    .unwrap();
    let v = capture(&mut g);
    for right in [
        ResearchComparisonEndpoint::Project,
        ResearchComparisonEndpoint::Version { version_uuid: v },
    ] {
        let q = request(ResearchComparisonEndpoint::Branch { branch_uuid: b }, right);
        let result = g.compare_research(&q, &CancellationToken::new()).unwrap();
        assert_eq!(result.batches[0].num_rows(), 0);
    }
    // Complete Project comparisons explicitly disclose missing evidence.
    let q = request(
        ResearchComparisonEndpoint::Project,
        ResearchComparisonEndpoint::Version { version_uuid: v },
    );
    let result = g.compare_research(&q, &CancellationToken::new()).unwrap();
    assert_eq!(
        texts(&result, "detail"),
        vec!["external_only", "external_only"]
    );
}
#[test]
fn explicit_canonical_sequences_compare_without_inventing_version_authority() {
    use graphforge_knowledge::research::{ResearchDecisionKind, ResearchSubjectKind};
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item)").unwrap();
    let batch = g
        .execute("MATCH (n:Item) RETURN n.node_uuid AS id")
        .unwrap();
    let id = Uuid::from_slice(
        batch.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let version = capture(&mut g);
    let decisions = RecordResearchDecisionsRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        context: ResearchContext::Project,
        community_uuid: None,
        creator_uuid: Uuid::now_v7(),
        recorded_at: 9,
        decisions: vec![ResearchDecisionInput {
            decision_uuid: Uuid::now_v7(),
            subject_kind: ResearchSubjectKind::Node,
            subject_uuid: id,
            kind: ResearchDecisionKind::Promote,
            source_version_uuid: None,
        }],
    };
    g.record_research_decisions(&decisions, &CancellationToken::new())
        .unwrap();
    let mut q = request(
        ResearchComparisonEndpoint::Version {
            version_uuid: version,
        },
        ResearchComparisonEndpoint::Project,
    );
    assert_eq!(
        g.compare_research(&q, &CancellationToken::new())
            .unwrap()
            .batches[0]
            .num_rows(),
        0
    );
    q.left_authority = Some(ResearchComparisonAuthority {
        context: ResearchContext::Project,
        community_uuid: None,
        through_sequence: Some(0),
    });
    q.right_authority = Some(ResearchComparisonAuthority {
        context: ResearchContext::Project,
        community_uuid: None,
        through_sequence: None,
    });
    let result = g.compare_research(&q, &CancellationToken::new()).unwrap();
    assert_eq!(texts(&result, "field"), vec!["$canonical"]);
    assert_eq!(texts(&result, "change"), vec!["changed"]);
    q.left_authority.as_mut().unwrap().through_sequence = Some(u64::MAX);
    assert!(g.compare_research(&q, &CancellationToken::new()).is_err());
}
#[test]
fn annotation_challenge_and_suppression_report_upstream_modification_as_conflict() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item)").unwrap();
    enable(&mut g);
    let nodes = g
        .execute("MATCH (n:Item) RETURN n.node_uuid AS id")
        .unwrap();
    let node = Uuid::from_slice(
        nodes.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let claim = CreateResearchClaimRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        assertion_uuid: Uuid::now_v7(),
        claim: "Analyst annotation".into(),
        graph_refs: vec![AssertionGraphRefInput {
            graph_uuid: node,
            graph_kind: GraphObjectKind::Node,
            role: AssertionGraphRole::Subject,
            ordinal: 0,
        }],
        category: graphforge_knowledge::research::ResearchCategory::Annotation,
        creator_uuid: Uuid::now_v7(),
        run_uuid: None,
        created_at: 10,
    };
    g.create_research_claim(&claim, &CancellationToken::new())
        .unwrap();
    let provenance = g.assertion(claim.assertion_uuid, None).unwrap();
    let provenance = Uuid::from_slice(
        provenance.batches[0]
            .column_by_name("provenance_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let b = branch(&mut g);
    for change in [
        ResearchClaimChange::Challenge {
            assertion_uuid: claim.assertion_uuid,
            status_event_uuid: Uuid::now_v7(),
            reasoning_uuid: Uuid::now_v7(),
            rationale: "Alternative reading".into(),
            provenance_uuid: provenance,
        },
        ResearchClaimChange::Suppress {
            suppression_uuid: Uuid::now_v7(),
            assertion_uuid: claim.assertion_uuid,
            provenance_uuid: provenance,
        },
    ] {
        let r = ChangeResearchBranchClaimRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&g),
            branch_uuid: b,
            version_uuid: Uuid::now_v7(),
            creator_uuid: Uuid::now_v7(),
            created_at: 12,
            change,
        };
        g.change_research_branch_claim(&r, &CancellationToken::new())
            .unwrap();
    }
    let q = request(
        ResearchComparisonEndpoint::Branch { branch_uuid: b },
        ResearchComparisonEndpoint::Project,
    );
    let local = g.compare_research(&q, &CancellationToken::new()).unwrap();
    assert!(texts(&local, "change").iter().all(|c| c == "local"));
    assert!(
        texts(&local, "field")
            .iter()
            .any(|f| f.starts_with("$claim_suppressions:"))
    );
    assert!(
        texts(&local, "field")
            .iter()
            .any(|f| f.starts_with("$assertion_status_events:"))
    );
    g.record_assertion_status(RecordAssertionStatusRequest {
        context: write(),
        status_event_uuid: Uuid::now_v7(),
        assertion_uuid: claim.assertion_uuid,
        status: AssertionStatus::Hypothesis,
        confidence_uuid: None,
        reasoning_uuid: None,
        provenance_uuid: provenance,
    })
    .unwrap();
    let before = current(&g);
    let conflict = g.compare_research(&q, &CancellationToken::new()).unwrap();
    assert!(texts(&conflict, "change").iter().any(|c| c == "conflict"));
    assert_eq!(current(&g), before);
    let view = g.open_research_branch(b).unwrap();
    assert_eq!(
        view.graph()
            .assertion(claim.assertion_uuid, None)
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
}

#[test]
fn ontology_changes_have_native_semantic_identity_and_local_provenance() {
    use graphforge_ontology::{
        ActivationMode, AuthoredModule, CompositionLimits, EntityTypeDef, InventoryCompileRequest,
        OntologyModuleId, compile_inventory, module_document_digest,
    };
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Character)").unwrap();
    let b = branch(&mut g);
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
    let r = ChangeResearchBranchOntologyRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: b,
        version_uuid: Uuid::now_v7(),
        expected_composition_fingerprint: None,
        candidate,
        data_disposition: CompositionDataDisposition::RequireConforming,
        created_at: 20,
    };
    g.change_research_branch_ontology(&r, &CancellationToken::new())
        .unwrap();
    let q = request(
        ResearchComparisonEndpoint::Branch { branch_uuid: b },
        ResearchComparisonEndpoint::Project,
    );
    let result = g.compare_research(&q, &CancellationToken::new()).unwrap();
    assert!(
        texts(&result, "object_kind")
            .iter()
            .any(|k| k == "ontology_module")
    );
    assert!(texts(&result, "change").iter().all(|k| k == "local"));
    assert!(
        result.batches[0]
            .column_by_name("contribution_uuid")
            .unwrap()
            .null_count()
            == 0
    );
}
#[test]
fn unrelated_parent_branch_reference_stays_outside_child_comparison() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item)").unwrap();
    let parent = branch(&mut g);
    let create = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Branch {
            branch_uuid: parent,
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "Child".into(),
    };
    g.create_research_branch(&create, &CancellationToken::new())
        .unwrap();
    let cited = capture(&mut g);
    let r = ReferenceResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: parent,
        version_uuid: Uuid::now_v7(),
        reference_uuid: Uuid::now_v7(),
        source_version_uuid: cited,
        label: "Unrelated".into(),
        created_at: 2,
    };
    g.reference_research_branch(&r, &CancellationToken::new())
        .unwrap();
    let q = request(
        ResearchComparisonEndpoint::Branch {
            branch_uuid: create.branch_uuid,
        },
        ResearchComparisonEndpoint::Branch {
            branch_uuid: parent,
        },
    );
    assert_eq!(
        g.compare_research(&q, &CancellationToken::new())
            .unwrap()
            .batches[0]
            .num_rows(),
        0
    );
}
#[test]
fn new_evidence_for_selected_assertion_reports_its_source_and_unavailable_artifact() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item)").unwrap();
    enable(&mut g);
    let nodes = g
        .execute("MATCH (n:Item) RETURN n.node_uuid AS id")
        .unwrap();
    let node = Uuid::from_slice(
        nodes.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let assertion = Uuid::now_v7();
    g.create_assertion(CreateAssertionRequest {
        context: write(),
        assertion_uuid: assertion,
        claim: "Selected claim".into(),
        graph_refs: vec![AssertionGraphRefInput {
            graph_uuid: node,
            graph_kind: GraphObjectKind::Node,
            role: AssertionGraphRole::Subject,
            ordinal: 0,
        }],
    })
    .unwrap();
    let b = branch(&mut g);
    let source = Uuid::now_v7();
    g.register_source(RegisterSourceRequest {
        context: write(),
        source_uuid: source,
        label: "New related source".into(),
        source_kind: SourceKind::Manuscript,
        identity_uri: None,
    })
    .unwrap();
    let artifact = Uuid::now_v7();
    g.register_artifact(RegisterArtifactRequest {
        context: write(),
        source_uuid: source,
        artifact_uuid: artifact,
        artifact_kind: ArtifactKind::RawScan,
        media_type: "application/octet-stream".into(),
        payload: ArtifactPayloadRequest::ExternalReference {
            uri: "https://example.invalid/new-evidence".into(),
            fingerprint: None,
        },
        derivation_inputs: vec![],
        run_uuid: None,
    })
    .unwrap();
    g.attach_evidence(AttachEvidenceRequest {
        context: write(),
        evidence_uuid: Uuid::now_v7(),
        assertion_uuid: assertion,
        source_uuid: artifact,
        source_kind: EvidenceSourceKind::Artifact,
        role: EvidenceRole::Supports,
        weight: None,
    })
    .unwrap();
    let q = request(
        ResearchComparisonEndpoint::Branch { branch_uuid: b },
        ResearchComparisonEndpoint::Project,
    );
    let result = g.compare_research(&q, &CancellationToken::new()).unwrap();
    let kinds = texts(&result, "object_kind");
    assert!(kinds.iter().any(|kind| kind == "source"));
    assert!(kinds.iter().any(|kind| kind == "artifact"));
    assert_eq!(
        texts(&result, "detail")
            .iter()
            .filter(|v| *v == "external_only")
            .count(),
        1
    );
    assert!(
        texts(&result, "field")
            .iter()
            .any(|v| v.starts_with("$evidence:"))
    );
}
