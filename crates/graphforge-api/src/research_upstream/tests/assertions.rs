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
}
