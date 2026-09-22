use super::*;

#[test]
fn individual_assertion_adoption_preserves_unselected_claims_and_graph_values() {
    let mut graph = GraphForge::new(None).unwrap();
    let cancel = CancellationToken::new();
    for capability_id in [
        CapabilityId::Provenance,
        CapabilityId::Knowledge,
        CapabilityId::Epistemic,
    ] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let seed = recovery::prepare(&mut graph);
    let ResearchUpstreamSelection::Selected { decisions } = &seed.selection else {
        unreachable!()
    };
    let node = decisions[0].unit.object_uuid;
    let selected = Uuid::now_v7();
    let private = Uuid::now_v7();
    for assertion_uuid in [selected, private] {
        graph
            .create_research_claim(
                &CreateResearchClaimRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: graph
                        .generation_for_read()
                        .unwrap()
                        .generation_uuid(),
                    assertion_uuid,
                    claim: format!("Interpretation {assertion_uuid}"),
                    graph_refs: vec![AssertionGraphRefInput {
                        graph_uuid: node,
                        graph_kind: graphforge_knowledge::GraphObjectKind::Node,
                        role: graphforge_knowledge::AssertionGraphRole::Subject,
                        ordinal: 0,
                    }],
                    category: graphforge_knowledge::research::ResearchCategory::Interpretation,
                    creator_uuid: Uuid::now_v7(),
                    run_uuid: None,
                    created_at: 3,
                },
                &cancel,
            )
            .unwrap();
    }
    let fields: Vec<_> = crate::branches::fields::read(&graph, &cancel)
        .unwrap()
        .keys()
        .filter(|key| key.0 == "assertion" && key.1 == selected)
        .map(|key| ResearchFieldIdentity {
            object_kind: key.0.clone(),
            object_uuid: key.1,
            field: key.2.clone(),
        })
        .collect();
    let request = PreviewResearchUpstreamRequest {
        branch_uuid: seed.preview.branch_uuid,
        scope: ResearchUpstreamScope::Fields { fields },
    };
    let review = preview::load(&graph, &request, &cancel).unwrap();
    let update = UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
        version_uuid: Uuid::now_v7(),
        preview: request,
        preview_sha256: review.digest,
        selection: ResearchUpstreamSelection::AllCompatible,
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 4,
        explanation: "Incorporate one interpretation".into(),
    };
    graph.update_research_branch(&update, &cancel).unwrap();
    let branch = graph
        .open_research_branch(seed.preview.branch_uuid)
        .unwrap();
    let claims =
        crate::research_claims::ledger::read_claims(&branch.graph().generation_for_read().unwrap())
            .unwrap();
    assert_eq!(claims.claims().len(), 1);
    assert_eq!(claims.claims()[0].assertion_uuid, selected);
    assert_eq!(
        crate::research_claims::ledger::read_claims(&graph.generation_for_read().unwrap())
            .unwrap()
            .claims()
            .len(),
        2
    );
    let result = branch
        .graph()
        .execute("MATCH (n:Item) RETURN n.x AS x,n.y AS y")
        .unwrap();
    for name in ["x", "y"] {
        assert_eq!(
            result.batches[0]
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            0
        );
    }
    drop(branch);
    graph
        .suppress_research_branch_assertion(
            &SuppressResearchBranchAssertionRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
                branch_uuid: seed.preview.branch_uuid,
                version_uuid: Uuid::now_v7(),
                assertion_uuid: selected,
                created_at: 5,
            },
            &cancel,
        )
        .unwrap();
    let provenance = crate::knowledge::ledger::read_ledger(&graph.generation_for_read().unwrap())
        .unwrap()
        .assertions
        .into_iter()
        .find(|assertion| assertion.assertion_uuid == selected)
        .unwrap()
        .provenance_uuid;
    graph
        .record_assertion_status(RecordAssertionStatusRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            status_event_uuid: Uuid::now_v7(),
            assertion_uuid: selected,
            status: AssertionStatus::Disputed,
            confidence_uuid: None,
            reasoning_uuid: None,
            provenance_uuid: provenance,
        })
        .unwrap();
    let scope = PreviewResearchUpstreamRequest {
        branch_uuid: seed.preview.branch_uuid,
        scope: ResearchUpstreamScope::Branch,
    };
    let review = preview::load(&graph, &scope, &cancel).unwrap();
    let changed_status = review
        .rows
        .iter()
        .find(|row| row.key.2.starts_with("$assertion_status_events:"))
        .unwrap();
    assert_eq!(changed_status.change, "conflict");
    let mut update = UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
        version_uuid: Uuid::now_v7(),
        preview: scope.clone(),
        preview_sha256: review.digest,
        selection: ResearchUpstreamSelection::AllCompatible,
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 6,
        explanation: "Incorporate only independent compatible fields".into(),
    };
    assert!(
        selection::validate(&review, &update)
            .unwrap()
            .keys()
            .all(|key| key.0 != "assertion")
    );
    graph.update_research_branch(&update, &cancel).unwrap();
    let review = preview::load(&graph, &scope, &cancel).unwrap();
    let changed_status = review
        .rows
        .iter()
        .find(|row| row.key.2.starts_with("$assertion_status_events:"))
        .unwrap();
    assert_eq!(changed_status.change, "conflict");
    update.operation_uuid = Uuid::now_v7();
    update.version_uuid = Uuid::now_v7();
    update.expected_generation_uuid = graph.generation_for_read().unwrap().generation_uuid();
    update.preview_sha256 = review.digest;
    update.selection = ResearchUpstreamSelection::Selected {
        decisions: vec![ResearchUpstreamDecision {
            unit: ResearchFieldIdentity {
                object_kind: changed_status.key.0.clone(),
                object_uuid: changed_status.key.1,
                field: changed_status.key.2.clone(),
            },
            resolution: ResearchUpstreamResolution::KeepLocal,
        }],
    };
    graph.update_research_branch(&update, &cancel).unwrap();
    let branch = graph
        .open_research_branch(seed.preview.branch_uuid)
        .unwrap();
    assert!(
        crate::research_claims::ledger::read_claims(&branch.graph().generation_for_read().unwrap())
            .unwrap()
            .claims()
            .is_empty()
    );
}
