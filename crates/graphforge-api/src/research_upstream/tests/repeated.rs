use super::*;

fn values(graph: &GraphForge, branch: Uuid) -> (i64, i64) {
    let branch = graph.open_research_branch(branch).unwrap();
    let result = branch
        .graph()
        .execute("MATCH (n:Item) RETURN n.x AS x, n.y AS y")
        .unwrap();
    let row = &result.batches[0];
    let value = |name| {
        row.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0)
    };
    (value("x"), value("y"))
}

#[test]
fn repeated_selective_updates_preserve_independent_baselines_and_original_base() {
    let mut graph = GraphForge::new(None).unwrap();
    let cancel = CancellationToken::new();
    let mut update = recovery::prepare(&mut graph);
    let branch = update.preview.branch_uuid;
    let original_base =
        graph.research_version_retention().unwrap().branches[&branch].base_version_uuid;
    let ResearchUpstreamSelection::Selected { decisions } = &mut update.selection else {
        unreachable!()
    };
    decisions[0].resolution = ResearchUpstreamResolution::AdoptUpstream;
    graph.update_research_branch(&update, &cancel).unwrap();
    assert_eq!(values(&graph, branch), (1, 0));
    graph.execute("MATCH (n:Item) SET n.x=2,n.y=2").unwrap();
    let review = preview::load(&graph, &update.preview, &cancel).unwrap();
    let x = review
        .rows
        .iter()
        .find(|row| row.key.2 == "property:x")
        .unwrap();
    let y = review
        .rows
        .iter()
        .find(|row| row.key.2 == "property:y")
        .unwrap();
    assert_eq!(x.change, "upstream");
    assert_eq!(x.baseline, x.left);
    assert_eq!(y.change, "upstream");
    assert_eq!(y.baseline, y.left);
    assert_ne!(x.incorporated, y.incorporated);
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
                branch_uuid: branch,
                version_uuid: Uuid::now_v7(),
                created_at: 3,
                query: "MATCH (n:Item) SET n.x=3".into(),
            },
            &cancel,
        )
        .unwrap();
    let review = preview::load(&graph, &update.preview, &cancel).unwrap();
    assert_eq!(
        review
            .rows
            .iter()
            .find(|row| row.key.2 == "property:x")
            .unwrap()
            .change,
        "conflict"
    );
    let all = UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
        version_uuid: Uuid::now_v7(),
        preview: update.preview.clone(),
        preview_sha256: review.digest,
        selection: ResearchUpstreamSelection::AllCompatible,
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 4,
        explanation: "Apply compatible y only".into(),
    };
    graph.update_research_branch(&all, &cancel).unwrap();
    assert_eq!(values(&graph, branch), (3, 2));
    let after = preview::load(&graph, &update.preview, &cancel).unwrap();
    assert_eq!(after.branch.base_version_uuid, original_base);
    assert_eq!(
        after
            .rows
            .iter()
            .find(|row| row.key.2 == "property:x")
            .unwrap()
            .change,
        "conflict"
    );
    assert!(!after.rows.iter().any(|row| row.key.2 == "property:y"));
    let original = graph.open_research_version(original_base).unwrap();
    let result = original
        .execute("MATCH (n:Item) RETURN n.x AS x, n.y AS y")
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

#[test]
fn retain_both_preserves_native_list_values_and_refuses_scalar_conflicts_atomically() {
    let mut graph = GraphForge::new(None).unwrap();
    let cancel = CancellationToken::new();
    let seed = recovery::prepare(&mut graph);
    let branch = seed.preview.branch_uuid;
    graph.execute("MATCH (n:Item) SET n.tags=[1,2]").unwrap();
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
                branch_uuid: branch,
                version_uuid: Uuid::now_v7(),
                created_at: 3,
                query: "MATCH (n:Item) SET n.tags=[3,3],n.x=3".into(),
            },
            &cancel,
        )
        .unwrap();
    let review = preview::load(&graph, &seed.preview, &cancel).unwrap();
    let tags = review
        .rows
        .iter()
        .find(|row| row.key.2 == "property:tags")
        .unwrap();
    assert_eq!(tags.change, "conflict");
    let mut request = UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
        version_uuid: Uuid::now_v7(),
        preview: seed.preview.clone(),
        preview_sha256: review.digest,
        selection: ResearchUpstreamSelection::Selected {
            decisions: vec![ResearchUpstreamDecision {
                unit: ResearchFieldIdentity {
                    object_kind: tags.key.0.clone(),
                    object_uuid: tags.key.1,
                    field: tags.key.2.clone(),
                },
                resolution: ResearchUpstreamResolution::RetainBoth,
            }],
        },
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 4,
        explanation: "Keep both ordered lists".into(),
    };
    graph.update_research_branch(&request, &cancel).unwrap();
    let view = graph.open_research_branch(branch).unwrap();
    let result = view
        .graph()
        .execute("MATCH (n:Item) RETURN n.tags AS tags")
        .unwrap();
    let lists = result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let values = lists.value(0);
    let values = values
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[3, 3, 1, 2]);
    assert_eq!(super::repeated::values(&graph, branch), (3, 0));
    let review = preview::load(&graph, &seed.preview, &cancel).unwrap();
    let x = review
        .rows
        .iter()
        .find(|row| row.key.2 == "property:x")
        .unwrap();
    request.operation_uuid = Uuid::now_v7();
    request.version_uuid = Uuid::now_v7();
    request.expected_generation_uuid = graph.generation_for_read().unwrap().generation_uuid();
    request.preview_sha256 = review.digest;
    request.selection = ResearchUpstreamSelection::Selected {
        decisions: vec![ResearchUpstreamDecision {
            unit: ResearchFieldIdentity {
                object_kind: x.key.0.clone(),
                object_uuid: x.key.1,
                field: x.key.2.clone(),
            },
            resolution: ResearchUpstreamResolution::RetainBoth,
        }],
    };
    let before = graph.research_version_retention().unwrap();
    let error = graph.update_research_branch(&request, &cancel).unwrap_err();
    assert!(
        error.to_string().contains("multivalued property"),
        "{error}"
    );
    assert_eq!(
        graph.generation_for_read().unwrap().generation_uuid(),
        request.expected_generation_uuid
    );
    assert_eq!(graph.research_version_retention().unwrap(), before);
}
